# transformation/silver_metadata.py

import os
import sys
from datetime import date

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark, write_silver
from ingestion.transforms import clean_metadata, build_kase_stubs
from ingestion.config import KASE_TICKERS

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-metadata")

    df_raw = spark.read.json(f"gs://{BRONZE}/raw/metadata/{RUN_DATE}.json")
    if df_raw.count() == 0:
        print(f"No metadata for {RUN_DATE} — skipping")
        spark.stop()
        raise SystemExit(0)

    df = clean_metadata(df_raw)

    # Add NULL-filled stubs for KASE tickers not present in yfinance metadata,
    # so dim_company always has a row for every ticker that appears in fact_stock_prices.
    existing = [r["ticker"] for r in df.select("ticker").collect()]
    stubs = build_kase_stubs(spark, RUN_DATE, existing, KASE_TICKERS)
    if stubs is not None:
        df = df.unionByName(stubs)
        print(f"Added {stubs.count()} KASE stubs")

    write_silver(df, SILVER, "metadata", RUN_DATE)
    print(f"Silver metadata written for {RUN_DATE}: {df.count()} rows")
    spark.stop()
