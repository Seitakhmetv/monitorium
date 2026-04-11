# transformation/silver_metadata_backfill.py
#
# Processes all available bronze metadata dates (yfinance companies).
# For each date, stubs out KASE tickers that have no metadata with NULLs
# so dim_company has a row for every ticker appearing in fact_stock_prices.
# Safe to re-run — dynamic partition overwrite.

import os
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, DateType
from dotenv import load_dotenv
from ingestion.utils import build_spark
from ingestion.config import KASE_TICKERS
from datetime import date

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")

FINAL_COLS = ["ticker", "shortName", "sector", "industry", "country", "marketCap", "currency", "run_date"]


def discover_metadata_dates() -> list:
    """List all date-keyed metadata JSON files in bronze."""
    from google.cloud import storage as gcs

    client = gcs.Client()
    bucket = client.bucket(BRONZE_BUCKET)
    dates = set()
    for blob in bucket.list_blobs(prefix="raw/metadata/"):
        name = blob.name.replace("raw/metadata/", "")
        if name.endswith(".json") and "/" not in name:
            dates.add(name.replace(".json", ""))
    return sorted(dates)


def clean_metadata(df):
    return df \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .withColumnRenamed("symbol", "ticker") \
        .withColumn("marketCap", F.col("marketCap").cast(LongType())) \
        .filter(F.col("ticker").isNotNull()) \
        .dropDuplicates(["ticker", "run_date"]) \
        .select(FINAL_COLS)


def build_kase_stubs(spark, run_date: str, existing_tickers: list):
    """
    Build NULL-filled rows for KASE tickers not already in the metadata for this date.
    currency = KZT, everything else NULL.
    """
    missing = [t for t in KASE_TICKERS if t not in existing_tickers]
    if not missing:
        return None

    rows = [(t,) for t in missing]
    stubs = spark.createDataFrame(rows, ["ticker"]) \
        .withColumn("shortName",  F.lit(None).cast(StringType())) \
        .withColumn("sector",     F.lit(None).cast(StringType())) \
        .withColumn("industry",   F.lit(None).cast(StringType())) \
        .withColumn("country",    F.lit("KZ").cast(StringType())) \
        .withColumn("marketCap",  F.lit(None).cast(LongType())) \
        .withColumn("currency",   F.lit("KZT").cast(StringType())) \
        .withColumn("run_date",   F.to_date(F.lit(run_date)))

    print(f"  Stubbing {len(missing)} KASE tickers: {missing}")
    return stubs.select(FINAL_COLS)


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-metadata-backfill")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    dates = discover_metadata_dates()
    print(f"Found {len(dates)} metadata dates in bronze")

    for run_date in dates:
        print(f"\nProcessing {run_date}...")
        try:
            df_raw = spark.read.json(
                f"gs://{BRONZE_BUCKET}/raw/metadata/{run_date}.json"
            )

            if df_raw.count() == 0:
                print(f"  ⚠ empty — skipping")
                continue

            df_clean = clean_metadata(df_raw)

            # Find which tickers already have metadata for this date
            existing_tickers = [r["ticker"] for r in df_clean.select("ticker").collect()]

            # Build stubs for any KASE tickers not in yfinance metadata
            stubs = build_kase_stubs(spark, run_date, existing_tickers)

            if stubs is not None:
                df_final = df_clean.unionByName(stubs)
            else:
                df_final = df_clean

            df_final.write \
                .mode("overwrite") \
                .parquet(f"gs://{SILVER_BUCKET}/metadata/run_date={run_date}/")

            print(f"  → written {df_final.count()} rows ({len(existing_tickers)} yfinance + {len(KASE_TICKERS) - len([t for t in KASE_TICKERS if t in existing_tickers])} KASE stubs)")

        except Exception as e:
            print(f"  ✗ {run_date}: {e}")
            continue

    print("\nSilver metadata backfill complete")
    spark.stop()