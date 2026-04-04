import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, LongType
from dotenv import load_dotenv
from ingestion.utils import build_spark, read_bronze, validate, write_silver
from datetime import date

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = os.getenv("RUN_DATE") or str(date.today())

DEV = os.getenv("DEV", "false") == "true"

def clean_news(df):
   """
   1. Parse pub_date from RFC 2822 string to TimestampType
      Use F.to_timestamp(col("pub_date"), "EEE, dd MMM yyyy HH:mm:ss Z")
   2. Cast run_date to DateType
   3. Truncate description to 500 chars
      Use F.col("description").substr(1, 500)
   4. Filter nulls on article_id, title, link
   5. Deduplicate on ["article_id"]
   """
   df_clean = df.withColumn("pub_date", F.to_timestamp(F.col("pub_date"), "EEE, dd MMM yyyy HH:mm:ss Z")) \
   .withColumn("run_date", F.to_date(F.col("run_date"))) \
   .withColumn("description", F.col("description").substr(1, 500)) \
   .filter(F.col("article_id").isNotNull() & F.col("title").isNotNull() & F.col("link").isNotNull()) \
   .dropDuplicates(["article_id"])
   return df_clean

if __name__ == "__main__":
   spark = build_spark("monitorium-silver-news")
   spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
   df_raw = read_bronze(spark, RUN_DATE, BRONZE_BUCKET, "news")
   df_clean = clean_news(df_raw)
   validate(df_clean, ["ticker", "article_id"])
   write_silver(df_clean, SILVER_BUCKET, "news", RUN_DATE)
   print(f"Silver news written for {RUN_DATE}")
   spark.stop()