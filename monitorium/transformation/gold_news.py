# transformation/gold_news.py

from pyspark.sql import functions as F
from ingestion.utils import build_spark, write_gold
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv(override=True)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
import sys
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


def build_news(spark):
    """
    Read silver news for RUN_DATE.
    Select all normalized + tagged columns.
    Deduplicate on article_id.
    """
    df = spark.read.parquet(f"gs://{SILVER_BUCKET}/news/run_date={RUN_DATE}/")

    if df.count() == 0:
        print(f"No silver news for {RUN_DATE} — skipping")
        return None

    return df.select(
        "article_id",
        "source",
        "source_type",
        "title",
        "url",
        "pub_date",
        "language",
        "companies",
        "sectors",
        "countries",
        "topics",
        "impact",
        "weight",
        "is_legal",
        "category",
        "description",
        F.to_date(F.lit(RUN_DATE)).alias("run_date")
    ).dropDuplicates(["article_id"])


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-news")

    df = build_news(spark)

    if df is None:
        spark.stop()
        exit(0)

    write_gold(df, PROJECT_ID, DATASET, "news_articles", mode="append")
    print(f"news_articles written: {df.count()} rows")
    spark.stop()