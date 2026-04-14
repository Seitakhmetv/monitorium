import os
from functools import reduce
from dotenv import load_dotenv
from google.cloud import storage as gcs

from ingestion.utils import build_spark
from transformation.silver_news import (
    normalize, apply_tags, FINAL_COLS, SOURCES, _ensure_final_cols,
)

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")


def discover_dates(source_name: str, prefix: str) -> list:
    """List all date-keyed JSON files in bronze for a source (excludes backfill/ subdir)."""
    client = gcs.Client()
    dates = set()
    for blob in client.bucket(BRONZE_BUCKET).list_blobs(prefix=prefix):
        name = blob.name.replace(prefix, "")
        if name.endswith(".json") and "/" not in name:
            dates.add(name.replace(".json", ""))
    return sorted(dates)


def process_date(spark, run_date: str) -> bool:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    dfs = []
    for source_name, path_template in SOURCES.items():
        path = f"gs://{BRONZE_BUCKET}/" + path_template.format(date=run_date)
        try:
            df = spark.read.json(path)
            if df.count() == 0:
                continue
            dfs.append(normalize(df, source_name))
            print(f"  ✓ {source_name} {run_date}")
        except Exception as e:
            print(f"  ⚠ {source_name} {run_date}: {e}")

    if not dfs:
        return False

    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    combined = apply_tags(combined)
    combined = combined.dropDuplicates(["article_id"])
    combined = _ensure_final_cols(combined)

    combined.write.mode("overwrite").parquet(
        f"gs://{SILVER_BUCKET}/news/run_date={run_date}/"
    )
    print(f"  → {combined.count()} rows written for {run_date}")
    return True


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-news-backfill")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Collect all dates that exist in any bronze news source
    all_dates = set()
    for source_name, path_template in SOURCES.items():
        prefix = path_template.format(date="").rsplit("/", 1)[0] + "/"
        dates = discover_dates(source_name, prefix)
        all_dates.update(dates)
        print(f"{source_name}: {len(dates)} dates found")

    all_dates = sorted(all_dates)
    print(f"\nTotal unique dates to process: {len(all_dates)}")

    written = skipped = 0
    for run_date in all_dates:
        print(f"\nProcessing {run_date}...")
        if process_date(spark, run_date):
            written += 1
        else:
            skipped += 1

    print(f"\nDone. Written: {written}, skipped: {skipped}")
    spark.stop()
