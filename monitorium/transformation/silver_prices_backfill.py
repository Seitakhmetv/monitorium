import os
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, LongType
from dotenv import load_dotenv
from ingestion.utils import build_spark, write_silver
from datetime import date

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")


def clean_prices(df):
    return df \
        .withColumn("date", F.to_date(F.col("date"))) \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .drop("adj_close") \
        .withColumn("open", F.col("open").cast(FloatType())) \
        .withColumn("high", F.col("high").cast(FloatType())) \
        .withColumn("low", F.col("low").cast(FloatType())) \
        .withColumn("close", F.col("close").cast(FloatType())) \
        .withColumn("volume", F.col("volume").cast(LongType())) \
        .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull()) \
        .dropDuplicates(["ticker", "date"])


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices-backfill")

    # read all backfill years at once
    df_raw = spark.read.json(
        f"gs://{BRONZE_BUCKET}/raw/prices/backfill/*.json"
    )

    df_clean = clean_prices(df_raw)
    print(f"Total rows: {df_clean.count()}")
    print(f"Date range: {df_clean.agg(F.min('date'), F.max('date')).collect()}")

    # write partitioned by run_date — merges cleanly with daily silver
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("run_date") \
        .parquet(f"gs://{SILVER_BUCKET}/prices/")

    print("Silver prices backfill complete")
    spark.stop()