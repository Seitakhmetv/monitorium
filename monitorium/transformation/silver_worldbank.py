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

def clean_worldbank(df):
    """
    1. Cast run_date to DateType
    2. Cast date (year string "2025") to IntegerType, rename to year
    3. Cast value to FloatType
    4. Filter nulls on country, value, year
    5. Deduplicate on ["country", "indicator_name", "year"]
    """
    df_clean = df.withColumn("run_date", F.to_date(F.col("run_date"))) \
    .withColumn("year", F.col("date").cast("int")) \
    .withColumn("value", F.col("value").cast(FloatType())) \
    .filter(F.col("country").isNotNull() & F.col("value").isNotNull() & F.col("year").isNotNull()) \
    .dropDuplicates(["country", "indicator_name", "year"])
    return df_clean

if __name__ == "__main__":
    spark = build_spark("monitorium-silver-worldbank")
    df_raw = read_bronze(spark, RUN_DATE, BRONZE_BUCKET, "worldbank")
    df_clean = clean_worldbank(df_raw)
    validate(df_clean, ["indicator_name", "country"])
    write_silver(df_clean, SILVER_BUCKET, "worldbank", RUN_DATE)
    print(f"Silver worldbank written for {RUN_DATE}")
    spark.stop()