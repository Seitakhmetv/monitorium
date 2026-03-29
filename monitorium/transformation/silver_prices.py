# transformation/silver_prices.py

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, LongType
from dotenv import load_dotenv
from ingestion.utils import build_spark, read_bronze, validate, write_silver

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = os.getenv("RUN_DATE")  # pass this in at runtime

DEV = os.getenv("DEV", "false") == "true"

def clean_prices(df):
    """
    Apply all transformations:
    1. Cast date column: to_date(col("date")) 
    2. Cast run_date: to_date(col("run_date"))
    3. Drop adj_close
    4. Cast open/high/low/close to FloatType
    5. Cast volume to LongType
    6. Filter out rows where ticker or close is null
    7. Deduplicate on ["ticker", "date"] keeping first
    Return cleaned DataFrame.
    """
    # your code here
    df_clean = df.withColumn("date", F.to_date(F.col("date"))) \
    .withColumn("run_date", F.to_date(F.col("run_date"))) \
    .drop("adj_close") \
    .withColumn("open", F.col("open").cast(FloatType())) \
    .withColumn("high", F.col("high").cast(FloatType())) \
    .withColumn("low", F.col("low").cast(FloatType())) \
    .withColumn("close", F.col("close").cast(FloatType())) \
    .withColumn("volume", F.col("volume").cast(LongType())) \
    .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull()) \
    .dropDuplicates(["ticker", "date"])
    return df_clean

if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices")
    df_raw = read_bronze(spark, RUN_DATE, BRONZE_BUCKET, "prices")
    df_clean = clean_prices(df_raw)
    validate(df_clean, ["ticker", "date"])
    write_silver(df_clean, SILVER_BUCKET, "prices", RUN_DATE)
    print(f"Silver prices written for {RUN_DATE}")
    spark.stop()