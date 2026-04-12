# transformation/silver_news_backfill.py
#
# Iterates all dates available in bronze news sources and runs the same
# normalization + tagging logic as silver_news.py for each date.
# Safe to re-run — partition overwrite, idempotent.

import os
from functools import reduce

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark
from ingestion.transforms import normalize_news, apply_tags, finalize_news

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")

SOURCES = {
    "news":      "raw/news/{date}.json",
    "kapital":   "raw/kapital/{date}.json",
    "kursiv":    "raw/kursiv/{date}.json",
    "kase_news": "raw/kase_news/{date}.json",
    "adilet":    "raw/adilet/{date}.json",
}


def list_dates(source_name: str) -> list:
    from google.cloud import storage as gcs
    prefix = f"raw/{source_name}/"
    dates = set()
    for blob in gcs.Client().bucket(BRONZE).list_blobs(prefix=prefix):
        name = blob.name.replace(prefix, "")
        if name.endswith(".json") and "/" not in name:
            dates.add(name.replace(".json", ""))
    return sorted(dates)


def process_date(spark, run_date: str) -> bool:
    dfs = []
    for src, tmpl in SOURCES.items():
        path = f"gs://{BRONZE}/" + tmpl.format(date=run_date)
        try:
            df = spark.read.json(path)
            if df.count() == 0:
                print(f"  skip {src}: empty")
                continue
            dfs.append(normalize_news(df, src))
            print(f"  {src}: loaded")
        except Exception as e:
            print(f"  skip {src}: {e}")

    if not dfs:
        return False

    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    combined = apply_tags(combined).dropDuplicates(["article_id"])
    combined = finalize_news(combined)
    combined.write.mode("overwrite").parquet(f"gs://{SILVER}/news/run_date={run_date}/")
    print(f"  → {combined.count()} rows written")
    return True


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-news-backfill")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    all_dates = set()
    for src in SOURCES:
        dates = list_dates(src)
        all_dates.update(dates)
        print(f"{src}: {len(dates)} dates")

    all_dates = sorted(all_dates)
    print(f"\nTotal unique dates: {len(all_dates)}")

    written = skipped = 0
    for run_date in all_dates:
        print(f"\n{run_date}...")
        if process_date(spark, run_date):
            written += 1
        else:
            skipped += 1

    print(f"\nDone. Written: {written} | Skipped (no data): {skipped}")
    spark.stop()
