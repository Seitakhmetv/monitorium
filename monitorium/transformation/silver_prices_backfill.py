import os
from dotenv import load_dotenv
from pyspark.sql import functions as F

from ingestion.utils import build_spark
from transformation.silver_prices import clean_prices

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices-backfill")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_us = spark.read.json(f"gs://{BRONZE_BUCKET}/raw/prices/backfill/*.json")
    df_us_clean = clean_prices(df_us, "USD")

    try:
        df_kase = spark.read.json(f"gs://{BRONZE_BUCKET}/raw/kase_prices/backfill/*.json")
        df_kase_clean = clean_prices(df_kase, "KZT")
        combined = df_us_clean.unionByName(df_kase_clean)
        print(f"KASE backfill rows: {df_kase_clean.count()}")
    except Exception as e:
        print(f"No KASE backfill data, skipping: {e}")
        combined = df_us_clean

    combined = combined.dropDuplicates(["ticker", "date"])
    print(f"Total rows: {combined.count()}")
    print(f"Date range: {combined.agg(F.min('date'), F.max('date')).collect()}")

    combined.write \
        .mode("overwrite") \
        .partitionBy("run_date") \
        .parquet(f"gs://{SILVER_BUCKET}/prices/")

    print("Silver prices backfill complete")
    spark.stop()
