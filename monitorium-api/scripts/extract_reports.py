"""
Downloads unextracted PDFs from dim_financial_reports, uploads to GCS,
extracts key financials via Gemini, writes to fact_financial_statements.

Run from monitorium-api/:
    python scripts/extract_reports.py [--ticker KZAP] [--year 2023] [--dry-run] [--limit 10]
"""
import sys
import os
import io
import json
import argparse
import hashlib
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import re as _re
import requests
import urllib3
from pypdf import PdfReader, PdfWriter
from google.cloud import bigquery, storage
from google import genai
from google.genai import types as genai_types
from db import client as bq_client, PROJECT, DATASET

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REPORTS_TABLE = f"{PROJECT}.{DATASET}.dim_financial_reports"
STATEMENTS_TABLE = f"{PROJECT}.{DATASET}.fact_financial_statements"
FLOWS_TABLE = f"{PROJECT}.{DATASET}.fact_financial_flows"
GCS_BUCKET = "monitorium-bronze"
GCS_PREFIX = "reports"
GEMINI_MODEL = "gemini-2.0-flash"
MAX_PAGES = 15

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Monitorium/1.0)"}

EXTRACT_PROMPT = """You are a financial data extraction specialist. The PDF contains an IFRS financial report for a Kazakhstani company.
Extract the following key financial metrics from the financial statements (Income Statement, Balance Sheet, Cash Flow Statement).

Return ONLY valid JSON with no markdown, no explanation. Use null for any metric not found.
All numeric values must be in the original units stated in the report (do NOT convert).
Include the currency (KZT, USD, etc.) and units (millions, thousands, or units) as stated in the report.

{
  "currency": "KZT",
  "units": "millions",
  "revenue": null,
  "gross_profit": null,
  "operating_profit": null,
  "ebitda": null,
  "net_income": null,
  "eps": null,
  "total_assets": null,
  "total_liabilities": null,
  "total_equity": null,
  "total_debt": null,
  "cash_and_equivalents": null,
  "current_assets": null,
  "current_liabilities": null,
  "operating_cash_flow": null,
  "investing_cash_flow": null,
  "financing_cash_flow": null,
  "capex": null,
  "free_cash_flow": null,
  "net_interest_income": null,
  "loan_portfolio": null,
  "deposits": null,
  "npl_ratio": null,
  "capital_adequacy_ratio": null
}

Notes:
- revenue: total revenues or net revenues or interest income for banks
- total_debt: sum of short-term and long-term borrowings
- capex: capital expenditures (purchase of PP&E)
- free_cash_flow: operating_cash_flow + capex (capex is typically negative)
- npl_ratio: non-performing loan ratio as a decimal (e.g. 0.042 for 4.2%)
- capital_adequacy_ratio: as a decimal
- For banks: net_interest_income is the primary revenue metric
- ebitda: if not explicitly stated, look for EBITDA in management notes on first pages
- total_liabilities: total liabilities from the balance sheet (should satisfy: total_assets = total_equity + total_liabilities)
"""

FLOWS_PROMPT = """You are a financial data extraction specialist. The PDF contains an IFRS financial report.

Extract every named line item from the Income Statement (Profit & Loss) as directed flow links for a Sankey diagram.
Use the EXACT labels from the report — do not translate or standardize them.

Rules:
- Revenues flow INTO an aggregate node (e.g. "Revenue" or "Выручка" or "Процентные доходы")
- Cost/expense items flow OUT of that aggregate toward an intermediate node (e.g. "Gross Profit")
- Then that intermediate flows into Operating Profit, then into Pre-tax Profit, then into Net Income
- Values are always POSITIVE (direction is encoded by source→target)
- Skip subtotals that are already implied by the links (don't double-count)
- Use the currency and units stated in the report

Respond with ONLY valid JSON, no markdown:
{
  "currency": "KZT",
  "units": "millions",
  "flows": [
    {"source": "Выручка от реализации нефти", "target": "Выручка", "value": 4200000},
    {"source": "Выручка от реализации газа",  "target": "Выручка", "value": 800000},
    {"source": "Выручка", "target": "Себестоимость", "value": 1800000},
    {"source": "Выручка", "target": "Валовая прибыль", "value": 3200000},
    {"source": "Валовая прибыль", "target": "Общие и адм. расходы", "value": 189000},
    {"source": "Валовая прибыль", "target": "Прибыль от операций", "value": 715000},
    ...
  ]
}

Include ALL named line items visible on the face of the income statement. Do not invent items not present.
"""


MAX_BYTES = 18 * 1024 * 1024  # 18MB — Gemini inline limit is 20MB


def slice_pdf(pdf_bytes: bytes, max_pages: int = MAX_PAGES) -> bytes:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    last_good: bytes = b""
    writer = PdfWriter()
    for i in range(min(max_pages, len(reader.pages))):
        writer.add_page(reader.pages[i])
        out = io.BytesIO()
        writer.write(out)
        if out.tell() > MAX_BYTES:
            return last_good or out.getvalue()
        last_good = out.getvalue()
    return last_good


def download_pdf(url: str) -> bytes | None:
    kzap = "kazatomprom.kz" in url
    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            verify=not kzap,
            timeout=60,
            stream=True,
        )
        resp.raise_for_status()
        data = resp.content
        if data[:4] != b"%PDF":
            print(f"  Not a PDF: {url[:80]}")
            return None
        return data
    except Exception as e:
        print(f"  Download failed: {e}")
        return None


def upload_to_gcs(ticker: str, filename: str, pdf_bytes: bytes) -> str:
    gcs = storage.Client(project=PROJECT)
    bucket = gcs.bucket(GCS_BUCKET)
    blob_path = f"{GCS_PREFIX}/{ticker}/{filename}"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    return f"gs://{GCS_BUCKET}/{blob_path}"


def extract_with_gemini(pdf_bytes: bytes) -> dict | None:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("  No GEMINI_API_KEY in .env")
        return None
    client = genai.Client(api_key=api_key)
    pdf_part = genai_types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")

    for attempt in range(4):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[EXTRACT_PROMPT, pdf_part],
            )
            text = response.text.strip()
            break
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                # Parse retryDelay from error message (e.g. "retryDelay": "55s")
                m = _re.search(r"retryDelay.*?(\d+)s", err_str)
                wait = int(m.group(1)) + 2 if m else 60 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s (attempt {attempt+1}/4)...")
                time.sleep(wait)
                continue
            print(f"  Gemini error: {e}")
            return None
    else:
        print("  Gemini failed after 4 attempts")
        return None

    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1][4:].strip() if parts[1].startswith("json") else parts[1].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}\n  Raw: {text[:200]}")
        return None


def extract_flows_with_gemini(pdf_bytes: bytes) -> dict | None:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return None
    client = genai.Client(api_key=api_key)
    pdf_part = genai_types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")

    for attempt in range(4):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[FLOWS_PROMPT, pdf_part],
            )
            text = response.text.strip()
            break
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                m = _re.search(r"retryDelay.*?(\d+)s", err_str)
                wait = int(m.group(1)) + 2 if m else 60 * (attempt + 1)
                print(f"  [flows] Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            print(f"  [flows] Gemini error: {e}")
            return None
    else:
        print("  [flows] Failed after 4 attempts")
        return None

    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1][4:].strip() if parts[1].startswith("json") else parts[1].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  [flows] JSON parse error: {e}")
        return None


def insert_flows(bq: bigquery.Client, report: dict, data: dict):
    flows = data.get("flows", [])
    if not flows:
        return
    currency = data.get("currency")
    units = data.get("units")
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "report_id":    report["report_id"],
            "ticker":       report["ticker"],
            "fiscal_year":  report["fiscal_year"],
            "source_label": f["source"],
            "target_label": f["target"],
            "value":        abs(float(f["value"])),
            "currency":     currency,
            "units":        units,
            "extracted_at": now,
        }
        for f in flows
        if f.get("source") and f.get("target") and f.get("value") is not None
    ]
    if not rows:
        return
    table_ref = bq.get_table(FLOWS_TABLE)
    errors = bq.insert_rows_json(table_ref, rows)
    if errors:
        print(f"  [flows] BQ insert error: {errors}")
    else:
        print(f"  [flows] {len(rows)} links written")


def mark_extracted(bq: bigquery.Client, report_id: str, gcs_path: str):
    sql = f"""
        UPDATE `{REPORTS_TABLE}`
        SET extracted = TRUE, gcs_path = '{gcs_path}'
        WHERE report_id = '{report_id}'
    """
    bq.query(sql).result()


def get_unextracted(bq: bigquery.Client, ticker: str | None, year: int | None, limit: int) -> list[dict]:
    filters = ["extracted = FALSE"]
    if ticker:
        filters.append(f"ticker = '{ticker}'")
    if year:
        filters.append(f"fiscal_year = {year}")
    where = " AND ".join(filters)
    sql = f"""
        SELECT report_id, ticker, fiscal_year, quarter, source_url, language
        FROM `{REPORTS_TABLE}`
        WHERE {where}
        ORDER BY ticker, fiscal_year
        LIMIT {limit}
    """
    return [dict(row) for row in bq.query(sql).result()]


def insert_statement(bq: bigquery.Client, report: dict, data: dict):
    row = {
        "report_id":              report["report_id"],
        "ticker":                 report["ticker"],
        "fiscal_year":            report["fiscal_year"],
        "quarter":                report.get("quarter"),
        "currency":               data.get("currency"),
        "units":                  data.get("units"),
        "revenue":                data.get("revenue"),
        "gross_profit":           data.get("gross_profit"),
        "operating_profit":       data.get("operating_profit"),
        "ebitda":                 data.get("ebitda"),
        "net_income":             data.get("net_income"),
        "eps":                    data.get("eps"),
        "total_assets":           data.get("total_assets"),
        "total_liabilities":      data.get("total_liabilities"),
        "total_equity":           data.get("total_equity"),
        "total_debt":             data.get("total_debt"),
        "cash_and_equivalents":   data.get("cash_and_equivalents"),
        "current_assets":         data.get("current_assets"),
        "current_liabilities":    data.get("current_liabilities"),
        "operating_cash_flow":    data.get("operating_cash_flow"),
        "investing_cash_flow":    data.get("investing_cash_flow"),
        "financing_cash_flow":    data.get("financing_cash_flow"),
        "capex":                  data.get("capex"),
        "free_cash_flow":         data.get("free_cash_flow"),
        "net_interest_income":    data.get("net_interest_income"),
        "loan_portfolio":         data.get("loan_portfolio"),
        "deposits":               data.get("deposits"),
        "npl_ratio":              data.get("npl_ratio"),
        "capital_adequacy_ratio": data.get("capital_adequacy_ratio"),
        "extracted_at":           datetime.now(timezone.utc).isoformat(),
        "gemini_model":           GEMINI_MODEL,
    }
    table_ref = bq.get_table(STATEMENTS_TABLE)
    errors = bq.insert_rows_json(table_ref, [row])
    if errors:
        print(f"  BQ insert error: {errors}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Only process this ticker")
    parser.add_argument("--year", type=int, help="Only process this fiscal year")
    parser.add_argument("--limit", type=int, default=200, help="Max reports to process")
    parser.add_argument("--dry-run", action="store_true", help="Download and parse but don't write to BQ/GCS")
    args = parser.parse_args()

    bq = bq_client()
    reports = get_unextracted(bq, args.ticker, args.year, args.limit)
    print(f"Found {len(reports)} unextracted reports")

    for i, report in enumerate(reports, 1):
        ticker = report["ticker"]
        fy = report["fiscal_year"]
        url = report["source_url"]
        print(f"\n[{i}/{len(reports)}] {ticker} FY{fy} — {url[:80]}")

        pdf_bytes = download_pdf(url)
        if not pdf_bytes:
            continue

        filename = f"{ticker}_{fy}_annual_ifrs.pdf"

        if not args.dry_run:
            try:
                gcs_path = upload_to_gcs(ticker, filename, pdf_bytes)
                print(f"  Uploaded to {gcs_path}")
            except Exception as e:
                print(f"  GCS upload failed: {e}")
                gcs_path = ""
        else:
            gcs_path = f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{ticker}/{filename}"

        sliced = slice_pdf(pdf_bytes)
        print(f"  Sliced to {len(sliced)//1024}KB ({MAX_PAGES} pages)")

        time.sleep(4)  # stay under rate limits
        data = extract_with_gemini(sliced)
        if not data:
            continue

        print(f"  Extracted: revenue={data.get('revenue')}, net_income={data.get('net_income')}, "
              f"total_assets={data.get('total_assets')} ({data.get('currency')} {data.get('units')})")

        # Balance sheet self-check: assets = equity + liabilities
        ta = data.get('total_assets')
        te = data.get('total_equity')
        tl = data.get('total_liabilities')
        if ta and te and tl:
            implied = te + tl
            pct_diff = abs(ta - implied) / ta * 100
            if pct_diff > 1.0:
                print(f"  WARNING balance sheet mismatch: assets={ta}, equity+liab={implied:.0f} ({pct_diff:.1f}% off)")

        time.sleep(4)
        flows = extract_flows_with_gemini(sliced)

        if not args.dry_run:
            insert_statement(bq, report, data)
            if flows:
                insert_flows(bq, report, flows)
            mark_extracted(bq, report["report_id"], gcs_path)
            print(f"  Written to BQ")
        else:
            print(f"  [DRY] Would write to BQ")

    print(f"\nDone. Processed {len(reports)} reports.")


if __name__ == "__main__":
    main()
