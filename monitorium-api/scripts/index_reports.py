"""
Scrapes KASE documents API and kazatomprom.kz to build a catalog of annual IFRS report PDFs.
Writes to dim_financial_reports in BigQuery.

Run from monitorium-api/:
    python scripts/index_reports.py [--dry-run] [--ticker HSBK]
"""
import sys
import os
import re
import json
import hashlib
import argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import urllib3
from db import client as bq_client, PROJECT, DATASET

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

KASE_TICKERS = [
    "HSBK", "KSPI", "KCEL", "KMGZ", "KZTK",
    "KEGC", "KZTO", "AIRA", "CCBN", "ASBN",
    "AKZM", "RAHT", "BAST", "KMGD", "BSUL",
]
# KZAP: kazatomprom.kz  |  IFDR: skipped

KASE_API = "https://kase.kz/api/companies/documents"
TABLE = f"{PROJECT}.{DATASET}.dim_financial_reports"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Monitorium/1.0)", "Referer": "https://kase.kz/"}

AUDIT_KEYWORDS = ["аудиторский отчет", "auditor"]
ANNUAL_EXCLUDE = [
    "квартал", "полугод",
    "январь–март", "январь-март",
    "январь–июнь", "январь-июнь",
    "январь–сентябрь", "январь-сентябрь",
]


def normalize(s: str) -> str:
    return " ".join(s.split()).lower()


def _report_id(ticker: str, fiscal_year: int, doc_type: str) -> str:
    raw = f"{ticker}:{fiscal_year}:{doc_type}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _infer_fiscal_year(name_norm: str, filing_date: str) -> int | None:
    m = re.search(r"\b(20\d{2})\b", name_norm)
    if not m:
        return None
    stated_year = int(m.group(1))
    if len(filing_date) >= 7:
        filing_year = int(filing_date[:4])
        filing_month = int(filing_date[5:7])
        # KSPI mislabeling fix: "за 2025 год" filed in May 2025 is actually FY2024
        if stated_year == filing_year and filing_month <= 6:
            return stated_year - 1
    return stated_year


def _is_annual_audit(name_norm: str) -> bool:
    if not any(kw in name_norm for kw in AUDIT_KEYWORDS):
        return False
    if any(kw in name_norm for kw in ANNUAL_EXCLUDE):
        return False
    return True


def fetch_kase_reports(ticker: str) -> list[dict]:
    params = {
        "org_code": ticker,
        "category_id": 3,
        "language": "ru",
        "page": 1,
        "per_page": 500,
    }
    try:
        resp = requests.get(KASE_API, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        items = resp.json()
        if not isinstance(items, list):
            items = items.get("results") or items.get("data") or []
    except Exception as e:
        print(f"  [{ticker}] API error: {e}")
        return []

    # Filter to this ticker's category-3 docs only (API may return neighbours)
    items = [i for i in items if i.get("org_code") == ticker and i.get("category_id") == 3]

    seen_links: set[str] = set()
    reports = []

    for doc in items:
        link = doc.get("link") or ""
        if not link or not link.lower().endswith(".pdf"):
            continue
        if link in seen_links:
            continue
        seen_links.add(link)

        name = doc.get("name") or ""
        name_norm = normalize(name)   # normalizes double spaces (Bug 1 fix)

        if not _is_annual_audit(name_norm):
            continue

        filing_date = (doc.get("date0") or "")[:10]
        fiscal_year = _infer_fiscal_year(name_norm, filing_date)
        if not fiscal_year:
            print(f"  [{ticker}] Cannot infer year from: {name!r}")
            continue

        full_url = link if link.startswith("http") else f"https://kase.kz{link}"

        lang = "en" if "english" in name_norm or " en " in name_norm else "ru"

        reports.append({
            "report_id":   _report_id(ticker, fiscal_year, "annual_ifrs"),
            "ticker":      ticker,
            "fiscal_year": fiscal_year,
            "doc_type":    "annual_ifrs",
            "quarter":     None,
            "source_url":  full_url,
            "gcs_path":    None,
            "filing_date": filing_date or None,
            "language":    lang,
            "extracted":   False,
            "indexed_at":  datetime.now(timezone.utc).isoformat(),
        })

    # Deduplicate by fiscal_year: prefer consolidated over standalone, keep latest filing
    by_year: dict[int, dict] = {}
    for r in reports:
        fy = r["fiscal_year"]
        existing = by_year.get(fy)
        if existing is None:
            by_year[fy] = r
        else:
            # Prefer consolidated report
            is_consol = "консолидированной" in normalize(r["source_url"])
            was_consol = "консолидированной" in normalize(existing["source_url"])
            if is_consol and not was_consol:
                by_year[fy] = r

    return list(by_year.values())


KZAP_ANNUAL_KEYWORDS = [
    "za_god", "_god_na", "ye20", "fy20", "31_dekabrya",
    "year_ended", "year-ended", "financial_statements_for_20",
]
KZAP_EXCLUDE = [
    "kvartal", "6_mesyatsev", "9_mesyatsev", "3_mesyatsa", "3_mes",
    "6m_", "_6m", "9m_", "3q", "2q", "1q", "polugodie", "1_pg",
    "trading_update", "sokrashchennaya", "promezhutochnaya",
    "separate", "otdelnaya", "ofo_", "_ofo", "standalone",
    "unconsolidated", "4_formi", "formi_14", "formi_ofo", "formi_kfo",
    "balance_sheet_and_income",
]


def fetch_kzap_reports() -> list[dict]:
    url = "https://www.kazatomprom.kz/en/investors/finansovaya_otchetnost"
    headers = {**HEADERS, "Accept": "text/html"}
    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [KZAP] HTTP error: {e}")
        return []

    html = resp.text
    links = re.findall(r'href="([^"]+\.pdf)"', html, re.IGNORECASE)

    # One consolidated annual report per fiscal year (latest URL wins)
    by_year: dict[int, str] = {}

    for href in links:
        href_low = href.lower()

        if any(ex in href_low for ex in KZAP_EXCLUDE):
            continue
        if not any(kw in href_low for kw in KZAP_ANNUAL_KEYWORDS):
            continue

        m = re.search(r"(20\d{2})", href)
        if not m:
            continue
        fiscal_year = int(m.group(1))
        if fiscal_year < 2018 or fiscal_year > 2025:
            continue

        # Among duplicates prefer the URL with "konsolidirovan" in it
        existing = by_year.get(fiscal_year, "")
        if not existing or ("konsolidirovan" in href_low and "konsolidirovan" not in existing.lower()):
            by_year[fiscal_year] = href

    reports = []
    for fiscal_year, href in sorted(by_year.items()):
        full_url = href if href.startswith("http") else f"https://www.kazatomprom.kz{href}"
        reports.append({
            "report_id":   _report_id("KZAP", fiscal_year, "annual_ifrs"),
            "ticker":      "KZAP",
            "fiscal_year": fiscal_year,
            "doc_type":    "annual_ifrs",
            "quarter":     None,
            "source_url":  full_url,
            "gcs_path":    None,
            "filing_date": None,
            "language":    "en",
            "extracted":   False,
            "indexed_at":  datetime.now(timezone.utc).isoformat(),
        })

    return reports


def upsert_reports(reports: list[dict], dry_run: bool = False):
    if not reports:
        return
    bq = bq_client()
    table_ref = bq.get_table(TABLE)

    existing_ids_sql = f"SELECT report_id FROM `{TABLE}`"
    try:
        existing = {row["report_id"] for row in bq.query(existing_ids_sql).result()}
    except Exception:
        existing = set()

    new_rows = [r for r in reports if r["report_id"] not in existing]
    if not new_rows:
        print("  No new reports to insert.")
        return

    if dry_run:
        for r in new_rows:
            print(f"  [DRY] {r['ticker']} FY{r['fiscal_year']} filed={r['filing_date']} — {r['source_url']}")
        return

    errors = bq.insert_rows_json(table_ref, new_rows)
    if errors:
        print(f"  BQ insert errors: {errors}")
    else:
        print(f"  Inserted {len(new_rows)} rows.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ticker", help="Process a single ticker only")
    args = parser.parse_args()

    tickers = [args.ticker.upper()] if args.ticker else KASE_TICKERS

    for ticker in tickers:
        print(f"Indexing {ticker}...")
        reports = fetch_kase_reports(ticker)
        years = sorted(r["fiscal_year"] for r in reports)
        print(f"  Found {len(reports)} annual IFRS reports: {years}")
        upsert_reports(reports, dry_run=args.dry_run)

    if not args.ticker or args.ticker.upper() == "KZAP":
        print("Indexing KZAP (kazatomprom.kz)...")
        kzap = fetch_kzap_reports()
        years = sorted(r["fiscal_year"] for r in kzap)
        print(f"  Found {len(kzap)} annual IFRS reports: {years}")
        upsert_reports(kzap, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
