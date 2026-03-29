# transformation/gold_dim_country.py

from pyspark.sql import functions as F
from ingestion.utils import build_spark, write_gold
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = os.getenv("RUN_DATE")


def build_dim_country(spark):
    """
    Read all worldbank silver partitions — not just today's.
    We want all countries ever seen, not just today's run.
    Use wildcard path: gs://{SILVER_BUCKET}/worldbank/run_date=*/
    Extract distinct country codes.
    Add country_name by mapping code to full name using F.when():
    US -> United States
    GB -> United Kingdom
    DE -> Germany
    KZ -> Kazakhstan
    FR -> France
    JP -> Japan
    Return DataFrame with: country, country_name
    """
    # your code here
    df = spark.read.parquet(f"gs://{SILVER_BUCKET}/worldbank/run_date={RUN_DATE}/")
    distinct_countries = df.select("country").distinct()

    result_df = distinct_countries.withColumn(
        "country_name",
        F.when(F.col("country") == "US", "United States")
        .when(F.col("country") == "GB", "United Kingdom")
        .when(F.col("country") == "DE", "Germany")
        .when(F.col("country") == "KZ", "Kazakhstan")
        .when(F.col("country") == "FR", "France")
        .when(F.col("country") == "JP", "Japan")
        .otherwise("Unknown")
    )
    return result_df


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-dim-country")
    df = build_dim_country(spark)
    df.show()
    write_gold(df, PROJECT_ID, DATASET, "dim_country")
    print(f"dim_country written: {df.count()} rows")
    spark.stop()