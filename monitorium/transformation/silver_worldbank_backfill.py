import os
from pyspark.sql import functions as F
from dotenv import load_dotenv

from ingestion.utils import build_spark
from transformation.silver_worldbank import clean_worldbank

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-worldbank-backfill")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_raw = spark.read.json(f"gs://{BRONZE_BUCKET}/raw/worldbank/backfill/all.json")
    df_clean = clean_worldbank(df_raw)

    print(f"Total rows: {df_clean.count()}")
    print(f"Countries:  {[r[0] for r in df_clean.select('country').distinct().collect()]}")
    print(f"Indicators: {[r[0] for r in df_clean.select('indicator_name').distinct().collect()]}")

    df_clean.write \
        .mode("overwrite") \
        .partitionBy("run_date") \
        .parquet(f"gs://{SILVER_BUCKET}/worldbank/")

    print("Silver worldbank backfill complete")
    spark.stop()
