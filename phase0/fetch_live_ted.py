#!/usr/bin/env python3
"""
Fetch the frozen Phase 0 TED source through the public TED Search API v3.

Output schema, one row per notice x buyer-country x frozen CPV code:
  notice_id, publication_date, buyer_country, cpv_code

Privacy boundary: only publication-number, publication-date, buyer-country and
classification-cpv are requested. No person/contact fields are requested or persisted.

This file is transport/extraction code only. It does not alter the frozen
preregistration, country registry, CPV definitions, windows or decision thresholds.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

TED_SEARCH_URL = "https://api.ted.europa.eu/v3/notices/search"
FIELDS = ["publication-number", "publication-date", "buyer-country", "classification-cpv"]
EU_ALPHA3_TO_ALPHA2 = {
    "AUT":"AT","BEL":"BE","BGR":"BG","HRV":"HR","CYP":"CY","CZE":"CZ",
    "DNK":"DK","EST":"EE","FIN":"FI","FRA":"FR","DEU":"DE","GRC":"GR",
    "HUN":"HU","IRL":"IE","ITA":"IT","LVA":"LV","LTU":"LT","LUX":"LU",
    "MLT":"MT","NLD":"NL","POL":"PL","PRT":"PT","ROU":"RO","SVK":"SK",
    "SVN":"SI","ESP":"ES","SWE":"SE",
}
EU_ALPHA2 = set(EU_ALPHA3_TO_ALPHA2.values())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def vals(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out = []
        for x in v:
            out.extend(vals(x))
        return out
    if isinstance(v, dict):
        for key in ("value", "code", "id"):
            if key in v:
                return vals(v[key])
        out = []
        for x in v.values():
            out.extend(vals(x))
        return out
    return [str(v).strip()]


def norm_country(v: str) -> str | None:
    x = v.strip().upper()
    if x in EU_ALPHA2:
        return x
    if x in EU_ALPHA3_TO_ALPHA2:
        return EU_ALPHA3_TO_ALPHA2[x]
    return None


def norm_cpv(v: str) -> str | None:
    d = "".join(ch for ch in v if ch.isdigit())[:8]
    return d if len(d) == 8 else None


def norm_date(v: str) -> str:
    x = v.strip()
    if len(x) == 8 and x.isdigit():
        return datetime.strptime(x, "%Y%m%d").date().isoformat()
    return datetime.fromisoformat(x.replace("Z", "+00:00")).date().isoformat()


def post_with_retry(session: requests.Session, payload: dict, *, attempts: int = 7) -> dict:
    last = None
    for attempt in range(attempts):
        try:
            r = session.post(TED_SEARCH_URL, json=payload, timeout=(20, 120))
            if r.status_code == 429 or 500 <= r.status_code < 600:
                last = RuntimeError(f"TED HTTP {r.status_code}: {r.text[:500]}")
            else:
                r.raise_for_status()
                data = r.json()
                if data.get("timedOut") is True:
                    raise RuntimeError("TED Search API reported timedOut=true")
                return data
        except (requests.RequestException, ValueError, RuntimeError) as e:
            last = e
        if attempt + 1 < attempts:
            time.sleep(min(60, 2 ** attempt + random.random()))
    raise RuntimeError(f"TED request failed after {attempts} attempts: {last}")


def page_payload(query: str, token: str | None = None, limit: int = 250) -> dict:
    p = {
        "query": query,
        "fields": FIELDS,
        "limit": limit,
        "scope": "ALL",
        "checkQuerySyntax": False,
        "paginationMode": "ITERATION",
        "onlyLatestVersions": False,
    }
    if token:
        p["iterationNextToken"] = token
    return p


def archive_preflight(session: requests.Session) -> dict:
    q = "publication-date>=20160501 AND publication-date<=20160531"
    p = {
        "query": q,
        "fields": ["publication-number", "publication-date"],
        "page": 1,
        "limit": 1,
        "scope": "ALL",
        "checkQuerySyntax": False,
        "paginationMode": "PAGE_NUMBER",
        "onlyLatestVersions": False,
    }
    data = post_with_retry(session, p)
    total = data.get("totalNoticeCount")
    notices = data.get("notices") or []
    if (isinstance(total, (int, float)) and total <= 0) or not notices:
        raise RuntimeError(
            "ARCHIVE_COVERAGE_FAILURE: TED Search API returned no notices for May 2016; "
            "do not interpret a truncated archive as a Phase 0 outcome."
        )
    return {"query": q, "totalNoticeCount": total, "sample_count": len(notices)}


def iter_years(start: date, end: date):
    for y in range(start.year, end.year + 1):
        ys = max(start, date(y, 1, 1))
        ye = min(end, date(y, 12, 31))
        yield y, ys, ye


def fetch_query(session: requests.Session, query: str) -> tuple[list[dict], dict]:
    token = None
    pages = 0
    all_notices: list[dict] = []
    first_total = None
    while True:
        data = post_with_retry(session, page_payload(query, token=token))
        pages += 1
        if first_total is None:
            first_total = data.get("totalNoticeCount")
        notices = data.get("notices") or []
        if not notices:
            break
        all_notices.extend(notices)
        token = data.get("iterationNextToken")
        if not token:
            if len(notices) >= 250:
                raise RuntimeError(
                    "ITERATION_TOKEN_FAILURE: full TED page returned without iterationNextToken"
                )
            break
    return all_notices, {"pages": pages, "totalNoticeCount": first_total, "returned": len(all_notices)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prereg", required=True, type=Path)
    ap.add_argument("--cpv-definitions", required=True, type=Path)
    ap.add_argument("--out-source", required=True, type=Path)
    ap.add_argument("--out-manifest", required=True, type=Path)
    args = ap.parse_args()

    prereg = json.loads(args.prereg.read_text(encoding="utf-8"))
    cpv = json.loads(args.cpv_definitions.read_text(encoding="utf-8"))
    if prereg.get("frozen") is not True or prereg.get("specification_version") != "PHASE0_LOCKED_V6":
        raise RuntimeError("Expected frozen PHASE0_LOCKED_V6 preregistration")
    if sha256_file(args.cpv_definitions) != prereg["locked_input_hashes"]["cpv_definitions_sha256"]:
        raise RuntimeError("Frozen CPV definitions hash mismatch before TED fetch")

    defs = cpv["definitions"]
    ids = {
        cfg[k]
        for cfg in prereg["benchmarks"].values()
        for k in ("treated_definition_id", "control_definition_id")
    }
    frozen_codes = sorted({code for ident in ids for code in defs[ident]["codes"]})
    code_set = set(frozen_codes)

    start = date(2016, 5, 1)
    end = date.fromisoformat(prereg["benchmarks"]["NIS2"]["analysis_cutoff_date"])
    cpv_expr = " OR ".join(f"classification-cpv={code}" for code in frozen_codes)

    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "RegulatoryDemandPhase0/1.0"})
    preflight = archive_preflight(s)

    rows: set[tuple[str, str, str, str]] = set()
    query_log = []
    ignored_non_eu_countries = 0
    ignored_non_frozen_cpvs = 0
    malformed_notices = 0

    for year, ys, ye in iter_years(start, end):
        query = (
            f"({cpv_expr}) AND publication-date>={ys.strftime('%Y%m%d')} "
            f"AND publication-date<={ye.strftime('%Y%m%d')}"
        )
        notices, stats = fetch_query(s, query)
        query_log.append({"year": year, "start": ys.isoformat(), "end": ye.isoformat(), "query": query, **stats})
        print(f"TED {year}: returned {stats['returned']} notices over {stats['pages']} pages", flush=True)

        for n in notices:
            ids_ = vals(n.get("publication-number"))
            dates_ = vals(n.get("publication-date"))
            countries = {norm_country(x) for x in vals(n.get("buyer-country"))}
            countries.discard(None)
            cpvs = {norm_cpv(x) for x in vals(n.get("classification-cpv"))}
            cpvs.discard(None)
            matching_cpvs = cpvs & code_set
            if not ids_ or not dates_ or not countries or not matching_cpvs:
                malformed_notices += 1
                continue
            if len(countries) < len(set(vals(n.get("buyer-country")))):
                ignored_non_eu_countries += 1
            if len(matching_cpvs) < len(cpvs):
                ignored_non_frozen_cpvs += len(cpvs - matching_cpvs)
            notice_id = ids_[0]
            pub_date = norm_date(dates_[0])
            for country in countries:
                for code in matching_cpvs:
                    rows.add((notice_id, pub_date, country, code))

    if not rows:
        raise RuntimeError("TED fetch produced zero normalized rows for frozen CPV universe")

    args.out_source.parent.mkdir(parents=True, exist_ok=True)
    with args.out_source.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["notice_id", "publication_date", "buyer_country", "cpv_code"])
        w.writerows(sorted(rows, key=lambda x: (x[1], x[0], x[2], x[3])))

    manifest = {
        "fetcher_version": "PHASE0_TED_FETCH_V1",
        "ted_search_url": TED_SEARCH_URL,
        "privacy_fields_requested": FIELDS,
        "scope": "ALL",
        "paginationMode": "ITERATION",
        "onlyLatestVersions": False,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "archive_preflight": preflight,
        "frozen_cpv_code_count": len(frozen_codes),
        "frozen_cpv_codes": frozen_codes,
        "queries": query_log,
        "unique_normalized_rows": len(rows),
        "unique_notice_ids": len({r[0] for r in rows}),
        "earliest_publication_date": min(r[1] for r in rows),
        "latest_publication_date": max(r[1] for r in rows),
        "malformed_or_incomplete_notice_records_skipped": malformed_notices,
        "non_eu_country_values_seen": ignored_non_eu_countries,
        "non_frozen_cpv_values_ignored": ignored_non_frozen_cpvs,
        "prereg_sha256": sha256_file(args.prereg),
        "cpv_definitions_sha256": sha256_file(args.cpv_definitions),
        "source_sha256": None,
    }
    manifest["source_sha256"] = sha256_file(args.out_source)
    args.out_manifest.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({
        "unique_normalized_rows": manifest["unique_normalized_rows"],
        "unique_notice_ids": manifest["unique_notice_ids"],
        "earliest": manifest["earliest_publication_date"],
        "latest": manifest["latest_publication_date"],
        "source_sha256": manifest["source_sha256"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"LIVE_TED_FETCH_FAILURE: {exc}", file=sys.stderr)
        raise SystemExit(3)
