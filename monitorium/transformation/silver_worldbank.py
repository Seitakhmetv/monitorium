import os
import sys
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from dotenv import load_dotenv

from ingestion.utils import build_spark, read_bronze, write_silver

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


def clean_worldbank(df):
    return df \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .withColumn("year",     F.col("date").cast("int")) \
        .withColumn("value",    F.col("value").cast(FloatType())) \
        .filter(
            F.col("country").isNotNull() &
            F.col("value").isNotNull() &
            F.col("year").isNotNull()
        ) \
        .dropDuplicates(["country", "indicator_name", "year", "run_date"])


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-worldbank")
    try:
        df_raw   = read_bronze(spark, RUN_DATE, BRONZE_BUCKET, "worldbank")
        df_clean = clean_worldbank(df_raw)
        write_silver(df_clean, SILVER_BUCKET, "worldbank", RUN_DATE)
        print(f"Silver worldbank written for {RUN_DATE}: {df_clean.count()} rows")
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print(f"No bronze worldbank for {RUN_DATE} — skipping.")
        else:
            raise
    spark.stop()
