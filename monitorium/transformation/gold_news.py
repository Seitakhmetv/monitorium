from pyspark.sql import functions as F
from ingestion.utils import build_spark, write_gold
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
from datetime import date
RUN_DATE = os.getenv("RUN_DATE") or str(date.today())

def build_news(spark):
    """
    Read silver news for RUN_DATE.
    Select final columns:
        article_id, ticker, title, description, link, pub_date, run_date
    Deduplicate on [article_id] — your hash key from scraper.
    """
    # your code here
    news_df = spark.read.parquet(f"gs://{SILVER_BUCKET}/news/run_date={RUN_DATE}/")
    df = news_df.select(
        news_df["article_id"],
        news_df["ticker"],
        news_df["title"],
        news_df["description"],
        news_df["link"],
        news_df["pub_date"],
        F.lit(RUN_DATE).alias("run_date")
    ).dropDuplicates(["article_id"])
    return df


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-news")
    df = build_news(spark)
    df.show(5)
    write_gold(df, PROJECT_ID, DATASET, "news_articles", mode="append")
    print(f"news_articles written: {df.count()} rows")
    spark.stop()