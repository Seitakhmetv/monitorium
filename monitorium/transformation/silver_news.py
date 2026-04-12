# transformation/silver_news.py

import os
import sys
from datetime import date
from functools import reduce

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark, write_silver
from ingestion.transforms import normalize_news, apply_tags, finalize_news

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())
DEV = os.getenv("DEV", "false") == "true"

SOURCES = {
    "news":      "raw/news/{date}.json",
    "kapital":   "raw/kapital/{date}.json",
    "kursiv":    "raw/kursiv/{date}.json",
    "kase_news": "raw/kase_news/{date}.json",
    "adilet":    "raw/adilet/{date}.json",
}


def get_spark():
    if DEV:
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("monitorium-silver-news-dev").master("local[*]").getOrCreate()
    return build_spark("monitorium-silver-news")


def read_source(spark, source_name: str):
    if DEV:
        path = f"sample/{source_name}.json"
        if not os.path.exists(path):
            print(f"  skip {source_name}: no sample file")
            return None
    else:
        path = f"gs://{BRONZE}/" + SOURCES[source_name].format(date=RUN_DATE)

    try:
        df = spark.read.json(path)
        if df.count() == 0:
            print(f"  skip {source_name}: empty")
            return None
        print(f"  {source_name}: {df.count()} rows")
        return df
    except Exception as e:
        print(f"  skip {source_name}: {e}")
        return None


if __name__ == "__main__":
    print(f"DEV={DEV} | RUN_DATE={RUN_DATE}")
    spark = get_spark()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    dfs = []
    for src in SOURCES:
        df = read_source(spark, src)
        if df is not None:
            dfs.append(normalize_news(df, src))

    if not dfs:
        print(f"No news data for {RUN_DATE} — skipping")
        spark.stop()
        raise SystemExit(0)

    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    combined = apply_tags(combined).dropDuplicates(["article_id"])
    combined = finalize_news(combined)

    if DEV:
        combined.show(10, truncate=False)
        combined.printSchema()
        print(f"DEV: {combined.count()} rows — not writing to GCS")
    else:
        write_silver(combined, SILVER, "news", RUN_DATE)
        print(f"Silver news written for {RUN_DATE}: {combined.count()} rows")

    spark.stop()
