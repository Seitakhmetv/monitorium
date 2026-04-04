import os
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from dotenv import load_dotenv
from ingestion.utils import build_spark, write_silver
from datetime import date

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")


def clean_worldbank(df):
    return df \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .withColumn("year", F.col("date").cast("int")) \
        .withColumn("value", F.col("value").cast(FloatType())) \
        .filter(
            F.col("country").isNotNull() &
            F.col("value").isNotNull() &
            F.col("year").isNotNull()
        ) \
        .dropDuplicates(["country", "indicator_name", "year"])


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-worldbank-backfill")

    df_raw = spark.read.json(
        f"gs://{BRONZE_BUCKET}/raw/worldbank/backfill/all.json"
    )

    df_clean = clean_worldbank(df_raw)
    print(f"Total rows: {df_clean.count()}")

    # write partitioned by run_date
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("run_date") \
        .parquet(f"gs://{SILVER_BUCKET}/worldbank/")

    print("Silver worldbank backfill complete")
    spark.stop()