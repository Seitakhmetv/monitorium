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

COLS = [
    "article_id", "source", "source_type", "title", "url", "pub_date",
    "language", "companies", "sectors", "countries", "topics",
    "impact", "weight", "is_legal", "category", "description", "run_date",
]


def build_news(spark):
    news_path = (
        f"gs://{SILVER_BUCKET}/news/run_date=*/"
        if RUN_DATE == "ALL"
        else f"gs://{SILVER_BUCKET}/news/run_date={RUN_DATE}/"
    )
    df = spark.read.parquet(news_path)
    if df.count() == 0:
        return None

    return df.select(
        *[c for c in COLS if c != "run_date"],
        F.to_date(F.lit(RUN_DATE if RUN_DATE != "ALL" else str(date.today()))).alias("run_date"),
    ).dropDuplicates(["article_id"])


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-news")
    df = build_news(spark)

    if df is None:
        print(f"No silver news for {RUN_DATE} — skipping")
        spark.stop()
        exit(0)

    if RUN_DATE == "ALL":
        write_gold(df, PROJECT_ID, DATASET, "news_articles", mode="overwrite")
        print(f"news_articles written (overwrite ALL): {df.count()} rows")
    else:
        # Daily append: exclude article_ids already in the table to prevent duplicates
        from google.cloud import bigquery as bq
        client = bq.Client(project=PROJECT_ID)
        existing_sql = f"""
            SELECT DISTINCT article_id
            FROM `{PROJECT_ID}.{DATASET}.news_articles`
            WHERE DATE(pub_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        """
        try:
            existing_ids = {row.article_id for row in client.query(existing_sql).result()}
            if existing_ids:
                from pyspark.sql.functions import col
                df = df.filter(~col("article_id").isin(existing_ids))
        except Exception as e:
            print(f"⚠ Could not load existing IDs (table may not exist yet): {e}")
        count = df.count()
        if count > 0:
            write_gold(df, PROJECT_ID, DATASET, "news_articles", mode="append")
            print(f"news_articles appended: {count} new rows")
        else:
            print("news_articles: no new rows to append")
    spark.stop()
