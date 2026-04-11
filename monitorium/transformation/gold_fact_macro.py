from pyspark.sql import functions as F
from ingestion.utils import build_spark, write_gold
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
from datetime import date
import sys
from pyspark.sql.types import DateType

print(f"DEBUG sys.argv: {sys.argv}")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())
print(f"DEBUG RUN_DATE: {RUN_DATE}")

def build_fact_macro(spark):
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
            F.to_date(F.lit(RUN_DATE)).alias("run_date")   # ← cast to DateType, not void
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