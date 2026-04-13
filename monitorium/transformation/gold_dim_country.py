import os
import sys
from datetime import date

from pyspark.sql import functions as F
from dotenv import load_dotenv

from ingestion.config import WORLDBANK_COUNTRIES
from ingestion.utils import build_spark, write_gold

load_dotenv()

PROJECT_ID    = os.getenv("GCP_PROJECT_ID")
DATASET       = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


def build_dim_country(spark):
    df = spark.read.parquet(f"gs://{SILVER_BUCKET}/worldbank/run_date=*/")
    distinct = df.select("country").distinct()

    # Build mapping dynamically from config — adding a country = one dict entry
    mapping = F.lit(None).cast("string")
    for code, name in WORLDBANK_COUNTRIES.items():
        mapping = F.when(F.col("country") == code, name).otherwise(mapping)

    return distinct.withColumn("country_name", mapping.cast("string"))


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-dim-country")
    df = build_dim_country(spark)
    df.show()
    write_gold(df, PROJECT_ID, DATASET, "dim_country")
    print(f"dim_country written: {df.count()} rows")
    spark.stop()
