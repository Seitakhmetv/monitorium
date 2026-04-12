# transformation/silver_metadata_backfill.py
#
# Processes all available bronze metadata dates (yfinance companies).
# For each date, stubs out KASE tickers that have no yfinance metadata with NULLs
# so dim_company has a row for every ticker appearing in fact_stock_prices.
# Safe to re-run — dynamic partition overwrite.

import os

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark
from ingestion.transforms import clean_metadata, build_kase_stubs, METADATA_COLS
from ingestion.config import KASE_TICKERS

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")


def list_metadata_dates() -> list:
    from google.cloud import storage as gcs
    dates = set()
    for blob in gcs.Client().bucket(BRONZE).list_blobs(prefix="raw/metadata/"):
        name = blob.name.replace("raw/metadata/", "")
        if name.endswith(".json") and "/" not in name:
            dates.add(name.replace(".json", ""))
    return sorted(dates)


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-metadata-backfill")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    dates = list_metadata_dates()
    print(f"Found {len(dates)} metadata dates in bronze")

    for run_date in dates:
        try:
            df_raw = spark.read.json(f"gs://{BRONZE}/raw/metadata/{run_date}.json")
            if df_raw.count() == 0:
                print(f"  {run_date}: empty — skipping")
                continue

            df = clean_metadata(df_raw)
            existing = [r["ticker"] for r in df.select("ticker").collect()]
            stubs = build_kase_stubs(spark, run_date, existing, KASE_TICKERS)
            if stubs is not None:
                df = df.unionByName(stubs)

            df.write.mode("overwrite").parquet(f"gs://{SILVER}/metadata/run_date={run_date}/")
            print(f"  {run_date}: {df.count()} rows")
        except Exception as e:
            print(f"  {run_date}: FAILED — {e}")

    print("Silver metadata backfill complete")
    spark.stop()
