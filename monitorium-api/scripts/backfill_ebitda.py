"""
For every fact_financial_statements row where ebitda IS NULL but operating_profit IS NOT NULL,
re-reads the PDF from GCS and asks Gemini to extract EBITDA + D&A.
Updates ebitda in-place via MERGE.

Run from monitorium-api/:
    python scripts/backfill_ebitda.py [--ticker KZAP] [--dry-run] [--limit 50]
"""
import sys, os, io, json, argparse, time, re as _re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery, storage
from google import genai
from google.genai import types as genai_types
from db import client as bq_client, PROJECT, DATASET

STATEMENTS_TABLE = f"{PROJECT}.{DATASET}.fact_financial_statements"
REPORTS_TABLE    = f"{PROJECT}.{DATASET}.dim_financial_reports"
GCS_BUCKET       = "monitorium-bronze"
GEMINI_MODEL     = "gemini-2.0-flash"
MAX_PAGES        = 15
MAX_BYTES        = 18 * 1024 * 1024

EBITDA_PROMPT = """You are a financial data extraction specialist. The PDF is an IFRS annual report.

Extract ONLY these two values from the Income Statement (P&L):

1. EBITDA — look for it explicitly in management notes or an EBITDA table. If not stated, compute: Operating Profit/Loss + Depreciation & Amortization found on the face of the P&L.
2. depreciation_amortization — the D&A line item from the P&L (NOT from the cash flow statement).

Return ONLY valid JSON, no markdown:
{"ebitda": <number or null>, "depreciation_amortization": <number or null>, "currency": "<KZT/USD/etc>", "units": "<thousands/millions/billions>"}

Use the same currency and units as stated in the report. Use null if genuinely not findable.
"""


def get_missing(bq: bigquery.Client, ticker: str | None, limit: int) -> list[dict]:
    where = "s.ebitda IS NULL AND s.operating_profit IS NOT NULL AND r.gcs_path IS NOT NULL AND r.gcs_path != ''"
    if ticker:
        where += f" AND s.ticker = '{ticker}'"
    sql = f"""
        SELECT s.report_id, s.ticker, s.fiscal_year, s.operating_profit, s.units, r.gcs_path
        FROM `{STATEMENTS_TABLE}` s
        JOIN `{REPORTS_TABLE}` r USING (report_id)
        WHERE {where}
        ORDER BY s.ticker, s.fiscal_year
        LIMIT {limit}
    """
    return [dict(row) for row in bq.query(sql).result()]


def read_from_gcs(gcs_path: str) -> bytes | None:
    # gcs_path like gs://bucket/path/to/file.pdf
    path = gcs_path.removeprefix(f"gs://{GCS_BUCKET}/")
    try:
        gcs = storage.Client(project=PROJECT)
        bucket = gcs.bucket(GCS_BUCKET)
        blob = bucket.blob(path)
        return blob.download_as_bytes()
    except Exception as e:
        print(f"  GCS read failed: {e}")
        return None


def slice_pdf(pdf_bytes: bytes, max_pages: int = MAX_PAGES) -> bytes:
    from pypdf import PdfReader, PdfWriter
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


def ask_gemini(pdf_bytes: bytes) -> dict | None:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("  No GEMINI_API_KEY")
        return None
    client = genai.Client(api_key=api_key)
    pdf_part = genai_types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")

    for attempt in range(4):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[EBITDA_PROMPT, pdf_part],
            )
            text = response.text.strip()
            break
        except Exception as e:
            err = str(e)
            if "429" in err or "RESOURCE_EXHAUSTED" in err:
                m = _re.search(r"retryDelay.*?(\d+)s", err)
                wait = int(m.group(1)) + 2 if m else 60 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            print(f"  Gemini error: {e}")
            return None
    else:
        print("  Failed after 4 attempts")
        return None

    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1][4:].strip() if parts[1].startswith("json") else parts[1].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}  raw: {text[:200]}")
        return None


def normalize_to_stmt_units(value: float, from_units: str, to_units: str) -> float:
    """Convert value to match the units already in fact_financial_statements."""
    scale = {"thousands": 1_000, "millions": 1_000_000, "billions": 1_000_000_000}
    f = scale.get(from_units.lower().split()[0], 1_000_000)
    t = scale.get(to_units.lower().split()[0], 1_000_000)
    return value * f / t


def merge_ebitda(bq: bigquery.Client, report_id: str, ebitda: float):
    sql = f"""
        UPDATE `{STATEMENTS_TABLE}`
        SET ebitda = {ebitda}
        WHERE report_id = '{report_id}' AND ebitda IS NULL
    """
    bq.query(sql).result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Load .env
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        for line in open(env_path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    bq = bq_client()
    rows = get_missing(bq, args.ticker, args.limit)
    print(f"Found {len(rows)} rows with null EBITDA to fill")

    filled = 0
    for i, row in enumerate(rows, 1):
        ticker    = row["ticker"]
        fy        = row["fiscal_year"]
        gcs_path  = row["gcs_path"]
        op_profit = row["operating_profit"]
        stmt_units = row["units"] or "millions"

        print(f"\n[{i}/{len(rows)}] {ticker} FY{fy}  op_profit={op_profit} ({stmt_units})")

        pdf_bytes = read_from_gcs(gcs_path)
        if not pdf_bytes:
            continue

        sliced = slice_pdf(pdf_bytes)
        time.sleep(2)

        data = ask_gemini(sliced)
        if not data:
            continue

        ebitda_raw = data.get("ebitda")
        da_raw     = data.get("depreciation_amortization")
        gemini_units = data.get("units", "millions")

        # If Gemini gave EBITDA directly, use it
        if ebitda_raw is not None:
            ebitda_val = normalize_to_stmt_units(float(ebitda_raw), gemini_units, stmt_units)
            source = "direct"
        # Otherwise derive from op_profit + D&A (both in stmt_units after normalization)
        elif da_raw is not None and op_profit is not None:
            da_normalized = normalize_to_stmt_units(float(da_raw), gemini_units, stmt_units)
            ebitda_val = op_profit + da_normalized
            source = f"op+da ({da_normalized:.0f} D&A)"
        else:
            print(f"  No EBITDA or D&A found — skipping")
            continue

        print(f"  ebitda={ebitda_val:.0f} [{stmt_units}]  source={source}")

        if not args.dry_run:
            try:
                merge_ebitda(bq, row["report_id"], ebitda_val)
                filled += 1
                print(f"  Written to BQ")
            except Exception as e:
                print(f"  BQ update failed: {e}")
        else:
            print(f"  [DRY] Would write ebitda={ebitda_val:.0f}")
            filled += 1

    print(f"\nDone. Filled {filled}/{len(rows)} rows.")


if __name__ == "__main__":
    main()
