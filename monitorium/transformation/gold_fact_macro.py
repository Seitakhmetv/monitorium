import os
import sys
from datetime import date

from pyspark.sql import functions as F
from dotenv import load_dotenv

from ingestion.utils import build_spark, write_gold

load_dotenv(dotenv_path=".env")

PROJECT_ID    = os.getenv("GCP_PROJECT_ID")
DATASET       = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


def build_fact_macro(spark):
    worldbank_df = spark.read.parquet(f"gs://{SILVER_BUCKET}/worldbank/run_date=*/")
    dim_country_df = spark.read.format("bigquery") \
        .option("project", PROJECT_ID).option("dataset", DATASET).option("table", "dim_country") \
        .load()

    return worldbank_df \
        .join(dim_country_df, worldbank_df["country"] == dim_country_df["country"], "left") \
        .select(
            worldbank_df["country"].alias("country_code"),
            worldbank_df["indicator_name"],
            worldbank_df["year"],
            worldbank_df["value"],
            F.to_date(F.lit(RUN_DATE if RUN_DATE != "ALL" else str(date.today()))).alias("run_date"),
        ) \
        .dropDuplicates(["country_code", "indicator_name", "year"])


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-fact-macro")
    df = build_fact_macro(spark)
    df.show(5)
    mode = "overwrite" if RUN_DATE == "ALL" else "append"
    write_gold(df, PROJECT_ID, DATASET, "fact_macro_indicators", mode=mode)
    print(f"fact_macro_indicators written ({mode}): {df.count()} rows")
    spark.stop()
