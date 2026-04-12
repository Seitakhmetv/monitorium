# transformation/silver_backfill_all.py
#
# Single Dataproc job that runs all four silver backfills in sequence.
# One cluster cold-start instead of four — cheaper and simpler.
# Safe to re-run: all writes use partition overwrite (idempotent).
#
# Purpose: populate silver layer from scratch so users have full history
# from day one. Run once after `backfill` (bronze ingestion) completes.
#
# Usage (Dataproc Serverless):
#   Submitted automatically by run_silver_backfill Cloud Function.
# Usage (local):
#   python -m transformation.silver_backfill_all

import os
from datetime import date
from functools import reduce

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)  # Spark env props take precedence on Dataproc
except ImportError:
    pass  # python-dotenv not required on Dataproc — env vars come from Spark properties

from ingestion.utils import build_spark
from ingestion.config import KASE_TICKERS
from ingestion.transforms import (
    clean_prices,
    clean_metadata, build_kase_stubs,
    clean_worldbank,
    normalize_news, apply_tags, finalize_news,
)
from pyspark.sql import functions as F

BRONZE = os.getenv("GCS_BRONZE_BUCKET", "monitorium-bronze")
SILVER = os.getenv("GCS_SILVER_BUCKET", "monitorium-silver")

# Bronze paths for news sources — {date} substituted per partition
NEWS_SOURCES = {
    "news":      "raw/news/{date}.json",
    "kapital":   "raw/kapital/{date}.json",
    "kursiv":    "raw/kursiv/{date}.json",
    "kase_news": "raw/kase_news/{date}.json",
    "adilet":    "raw/adilet/{date}.json",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def list_bronze_dates(prefix: str) -> list:
    """Return sorted list of date strings for files directly under a GCS prefix."""
    from google.cloud import storage as gcs
    dates = set()
    for blob in gcs.Client().bucket(BRONZE).list_blobs(prefix=prefix):
        name = blob.name.replace(prefix, "")
        if name.endswith(".json") and "/" not in name:
            dates.add(name.replace(".json", ""))
    return sorted(dates)


def try_read(spark, path: str):
    """Read JSON from GCS; return None on any error or empty result."""
    try:
        df = spark.read.json(path)
        return df if df.count() > 0 else None
    except Exception as e:
        print(f"  skip {path}: {e}")
        return None


# ── backfill stages ───────────────────────────────────────────────────────────

def backfill_prices(spark):
    print("\n=== Silver prices backfill ===")
    df_us   = try_read(spark, f"gs://{BRONZE}/raw/prices/backfill/*.json")
    df_kase = try_read(spark, f"gs://{BRONZE}/raw/kase_prices/backfill/all.json")

    if df_us is None and df_kase is None:
        print("No price backfill data found — skipping")
        return

    parts = [clean_prices(df) for df in [df_us, df_kase] if df is not None]
    df = reduce(lambda a, b: a.unionByName(b), parts).dropDuplicates(["ticker", "date"])
    print(f"Prices: {df.count()} rows | range: {df.agg(F.min('date'), F.max('date')).collect()}")
    df.write.mode("overwrite").partitionBy("run_date").parquet(f"gs://{SILVER}/prices/")
    print("Silver prices done")


def backfill_worldbank(spark):
    print("\n=== Silver worldbank backfill ===")
    df = try_read(spark, f"gs://{BRONZE}/raw/worldbank/backfill/all.json")
    if df is None:
        print("No worldbank backfill data — skipping")
        return
    df = clean_worldbank(df)
    print(f"Worldbank: {df.count()} rows")
    df.write.mode("overwrite").partitionBy("run_date").parquet(f"gs://{SILVER}/worldbank/")
    print("Silver worldbank done")


def backfill_metadata(spark):
    print("\n=== Silver metadata backfill ===")
    dates = list_bronze_dates("raw/metadata/")
    print(f"Found {len(dates)} metadata dates")

    for run_date in dates:
        df = try_read(spark, f"gs://{BRONZE}/raw/metadata/{run_date}.json")
        if df is None:
            print(f"  {run_date}: empty — skipping")
            continue

        df = clean_metadata(df)
        existing = [r["ticker"] for r in df.select("ticker").collect()]
        stubs = build_kase_stubs(spark, run_date, existing, KASE_TICKERS)
        if stubs is not None:
            df = df.unionByName(stubs)

        df.write.mode("overwrite").parquet(f"gs://{SILVER}/metadata/run_date={run_date}/")
        print(f"  {run_date}: {df.count()} rows")

    print("Silver metadata done")


def backfill_news(spark):
    print("\n=== Silver news backfill ===")
    all_dates = set()
    for src in NEWS_SOURCES:
        all_dates.update(list_bronze_dates(f"raw/{src}/"))
    all_dates = sorted(all_dates)
    print(f"Found {len(all_dates)} unique news dates")

    written = 0
    for run_date in all_dates:
        dfs = []
        for src, tmpl in NEWS_SOURCES.items():
            path = f"gs://{BRONZE}/" + tmpl.format(date=run_date)
            df = try_read(spark, path)
            if df is not None:
                dfs.append(normalize_news(df, src))

        if not dfs:
            continue

        combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
        combined = apply_tags(combined).dropDuplicates(["article_id"])
        combined = finalize_news(combined)
        combined.write.mode("overwrite").parquet(f"gs://{SILVER}/news/run_date={run_date}/")
        print(f"  {run_date}: {combined.count()} rows")
        written += 1

    print(f"Silver news done — {written} dates written")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = build_spark("monitorium-silver-backfill-all")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    backfill_prices(spark)
    backfill_worldbank(spark)
    backfill_metadata(spark)
    backfill_news(spark)

    print("\nAll silver backfills complete.")
    spark.stop()
