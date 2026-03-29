from pyspark.sql import functions as F
from ingestion.utils import build_spark, write_gold
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = os.getenv("RUN_DATE")


def build_fact_macro(spark):
    """
    Read all worldbank silver partitions — wildcard path.
    Join to dim_country on country = country_code.
    Select final columns:
        country_code, indicator_name, year, value, run_date
    Deduplicate on [country_code, indicator_name, year].
    """
    # your code here
    worldbank_df = spark.read.parquet(f"gs://{SILVER_BUCKET}/worldbank/run_date=*/")
    dim_country_df = spark.read.format("bigquery") \
        .option("project", PROJECT_ID) \
        .option("dataset", DATASET) \
        .option("table", "dim_country") \
        .load()
    df = worldbank_df.join(dim_country_df, worldbank_df["country"] == dim_country_df["country"], "left") \
        .select(
            worldbank_df["country"].alias("country_code"),
            worldbank_df["indicator_name"],
            worldbank_df["year"],
            worldbank_df["value"],
            F.lit(RUN_DATE).alias("run_date")
        ) \
        .dropDuplicates(["country_code", "indicator_name", "year"])
    return df


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-fact-macro")
    df = build_fact_macro(spark)
    df.show(5)
    write_gold(df, PROJECT_ID, DATASET, "fact_macro_indicators", mode="append")
    print(f"fact_macro_indicators written: {df.count()} rows")
    spark.stop()