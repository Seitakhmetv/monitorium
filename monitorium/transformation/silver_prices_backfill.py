import os
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, LongType, StringType
from dotenv import load_dotenv
from ingestion.utils import build_spark
from datetime import date

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")

FINAL_COLS = ["date", "ticker", "open", "high", "low", "close", "volume", "currency", "run_date"]


def clean_prices(df):
    # Drop adj_close only if it exists — KASE data never has it
    if "adj_close" in df.columns:
        df = df.drop("adj_close")

    # currency: yfinance data won't have it, KASE data will
    if "currency" not in df.columns:
        df = df.withColumn("currency", F.lit("USD").cast(StringType()))

    return df \
        .withColumn("date", F.to_date(F.col("date"))) \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .withColumn("open", F.col("open").cast(FloatType())) \
        .withColumn("high", F.col("high").cast(FloatType())) \
        .withColumn("low", F.col("low").cast(FloatType())) \
        .withColumn("close", F.col("close").cast(FloatType())) \
        .withColumn("volume", F.col("volume").cast(LongType())) \
        .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull()) \
        .dropDuplicates(["ticker", "date"]) \
        .select(FINAL_COLS)


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices-backfill")

    # Enable dynamic partition overwrite so only touched partitions are replaced,
    # leaving all existing daily silver partitions intact.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Read US backfill — one JSON per year
    df_us = spark.read.json(f"gs://{BRONZE_BUCKET}/raw/prices/backfill/*.json")

    # Read KASE backfill if it exists — single all.json
    try:
        df_kase = spark.read.json(f"gs://{BRONZE_BUCKET}/raw/kase_prices/backfill/all.json")
        print(f"KASE backfill rows (raw): {df_kase.count()}")
    except Exception as e:
        print(f"No KASE backfill data found, skipping: {e}")
        df_kase = None

    df_us_clean = clean_prices(df_us)

    if df_kase is not None:
        df_kase_clean = clean_prices(df_kase)
        df_clean = df_us_clean.unionByName(df_kase_clean)
    else:
        df_clean = df_us_clean

    # Final dedup across both sources on [ticker, date]
    df_clean = df_clean.dropDuplicates(["ticker", "date"])

    print(f"Total rows: {df_clean.count()}")
    print(f"Date range: {df_clean.agg(F.min('date'), F.max('date')).collect()}")

    # Partitioned overwrite — safe to re-run, won't touch daily silver partitions
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("run_date") \
        .parquet(f"gs://{SILVER_BUCKET}/prices/")

    print("Silver prices backfill complete")
    spark.stop()