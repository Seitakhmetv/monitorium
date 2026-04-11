# transformation/silver_prices.py

import os
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType, StringType
from dotenv import load_dotenv
from ingestion.utils import build_spark, validate, write_silver
from datetime import date

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
import sys
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())

FINAL_COLS = ["date", "ticker", "open", "high", "low", "close", "volume", "currency", "run_date"]


def clean_global(spark):
    path = f"gs://{BRONZE_BUCKET}/raw/prices/{RUN_DATE}.json"
    df = spark.read.json(path)

    if df.count() == 0:
        print("No global prices — skipping")
        return None

    return df \
        .withColumn("date", F.to_date(F.col("date"))) \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .withColumn("open", F.col("open").cast(FloatType())) \
        .withColumn("high", F.col("high").cast(FloatType())) \
        .withColumn("low", F.col("low").cast(FloatType())) \
        .withColumn("close", F.col("close").cast(FloatType())) \
        .withColumn("volume", F.col("volume").cast(LongType())) \
        .withColumn("currency", F.lit("USD").cast(StringType())) \
        .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull()) \
        .select(FINAL_COLS)


def clean_kase(spark):
    path = f"gs://{BRONZE_BUCKET}/raw/kase_prices/{RUN_DATE}.json"
    df = spark.read.json(path)

    if df.count() == 0:
        print("No KASE prices — skipping")
        return None

    return df \
        .withColumn("date", F.to_date(F.col("date"))) \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .withColumn("open", F.col("open").cast(FloatType())) \
        .withColumn("high", F.col("high").cast(FloatType())) \
        .withColumn("low", F.col("low").cast(FloatType())) \
        .withColumn("close", F.col("close").cast(FloatType())) \
        .withColumn("volume", F.col("volume").cast(LongType())) \
        .withColumn("currency", F.lit("KZT").cast(StringType())) \
        .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull()) \
        .select(FINAL_COLS)


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices")

    global_df = clean_global(spark)
    kase_df = clean_kase(spark)

    if global_df is None and kase_df is None:
        print(f"No price data for {RUN_DATE} — skipping")
        spark.stop()
        exit(0)

    # union whichever exist
    if global_df is not None and kase_df is not None:
        df_clean = global_df.unionByName(kase_df)
    elif global_df is not None:
        df_clean = global_df
    else:
        df_clean = kase_df

    df_clean = df_clean.dropDuplicates(["ticker", "date"])

    validate(df_clean, ["ticker", "date"])
    write_silver(df_clean, SILVER_BUCKET, "prices", RUN_DATE)
    print(f"Silver prices written for {RUN_DATE}: {df_clean.count()} rows")
    spark.stop()