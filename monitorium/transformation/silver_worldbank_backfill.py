# transformation/silver_worldbank_backfill.py

import os

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark
from ingestion.transforms import clean_worldbank

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-worldbank-backfill")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_raw = spark.read.json(f"gs://{BRONZE}/raw/worldbank/backfill/all.json")
    df = clean_worldbank(df_raw)
    print(f"Worldbank rows: {df.count()}")

    df.write.mode("overwrite").partitionBy("run_date").parquet(f"gs://{SILVER}/worldbank/")
    print("Silver worldbank backfill complete")
    spark.stop()
