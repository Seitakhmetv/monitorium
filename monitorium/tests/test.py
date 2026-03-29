from ingestion.utils import build_spark, write_gold
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = os.getenv("RUN_DATE")


spark = build_spark("test")
df = spark.read.parquet(f"gs://{SILVER_BUCKET}/prices/run_date={RUN_DATE}/")

print(df.show(5))