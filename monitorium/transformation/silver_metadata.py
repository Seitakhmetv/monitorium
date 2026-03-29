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

def clean_metadata(df):
    """
    1. Cast run_date to DateType
    2. Rename symbol -> ticker for consistency across all tables
    3. Cast marketCap to LongType
    4. Filter nulls on ticker, sector
    5. Deduplicate on ["ticker", "run_date"]
    """
    df_clean = df.withColumn("run_date", F.to_date(F.col("run_date"))) \
    .withColumnRenamed("symbol", "ticker") \
    .withColumn("marketCap", F.col("marketCap").cast(LongType())) \
    .filter(F.col("ticker").isNotNull() & F.col("sector").isNotNull()) \
    .dropDuplicates(["ticker", "run_date"])
    return df_clean

if __name__ == "__main__":
    spark = build_spark("monitorium-silver-metadata")
    df_raw = read_bronze(spark, RUN_DATE, BRONZE_BUCKET, "metadata")
    df_clean = clean_metadata(df_raw)
    validate(df_clean, ["ticker", "run_date"])
    write_silver(df_clean, SILVER_BUCKET, "metadata", RUN_DATE)
    print(f"Silver metadata written for {RUN_DATE}")
    spark.stop()