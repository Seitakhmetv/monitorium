# transformation/silver_news_backfill.py
#
# Iterates all dates available in bronze news sources and runs the same
# normalization + tagging logic as silver_news.py for each date.
# Safe to re-run — dynamic partition overwrite, skips already-written partitions
# unless bronze has data for them.

import os
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import IntegerType, BooleanType, StringType, StructType, StructField
from dotenv import load_dotenv
from ingestion.tagger import tag_article
from datetime import date

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")

# Bronze source paths — {date} is substituted per partition
SOURCES = {
    "news":      "raw/news/{date}.json",
    "kapital":   "raw/kapital/{date}.json",
    "kursiv":    "raw/kursiv/{date}.json",
    "kase_news": "raw/kase_news/{date}.json",
    "adilet":    "raw/adilet/{date}.json",
}

FINAL_COLS = [
    "article_id", "source", "source_type", "title", "url",
    "pub_date", "language", "run_date",
    "companies", "sectors", "countries", "topics",
    "impact", "weight", "is_legal", "category",
    "description"
]

TAG_SCHEMA = StructType([
    StructField("companies", StringType()),
    StructField("sectors",   StringType()),
    StructField("countries", StringType()),
    StructField("topics",    StringType()),
    StructField("impact",    StringType()),
    StructField("weight",    IntegerType()),
    StructField("is_legal",  BooleanType()),
    StructField("category",  StringType()),
])

tag_udf = F.udf(
    lambda title, source, source_type: __import__('ingestion.tagger', fromlist=['tag_article']).tag_article(
        title or "", source or "", source_type or "news"
    ),
    TAG_SCHEMA
)


def normalize(df, source_name: str):
    if "link" in df.columns and "url" not in df.columns:
        df = df.withColumnRenamed("link", "url")
    if "source" not in df.columns:
        df = df.withColumn("source", F.lit(source_name))
    if "source_type" not in df.columns:
        source_type = "legal" if source_name == "adilet" else "news"
        df = df.withColumn("source_type", F.lit(source_type))
    if "language" not in df.columns:
        df = df.withColumn("language", F.lit("ru"))
    if "description" not in df.columns:
        df = df.withColumn("description", F.lit(None).cast(StringType()))

    df = df.withColumn("pub_date",
        F.coalesce(
            F.to_timestamp(F.col("pub_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("pub_date"), "yyyy-MM-dd'T'HH:mm:ss+05:00"),
            F.to_timestamp(F.col("pub_date"), "EEE, dd MMM yyyy HH:mm:ss Z"),
            F.to_timestamp(F.col("pub_date"), "yyyy-MM-dd"),
            F.current_timestamp()
        )
    )

    df = df.withColumn("run_date", F.to_date(F.col("run_date")))
    df = df.withColumn("description",
        F.when(F.col("description").isNotNull(),
               F.col("description").substr(1, 500))
        .otherwise(F.lit(None).cast(StringType()))
    )

    return df.filter(
        F.col("article_id").isNotNull() & F.col("title").isNotNull()
    )


def apply_tags(df):
    df = df.withColumn("tags", tag_udf(
        F.col("title"), F.col("source"), F.col("source_type")
    ))
    for field in ["companies", "sectors", "countries", "topics",
                  "impact", "weight", "is_legal", "category"]:
        df = df.withColumn(field, F.col(f"tags.{field}"))
    return df.drop("tags")


def discover_dates(spark, source_name: str) -> list:
    """
    List all date-keyed JSON files in bronze for a given source.
    Returns sorted list of date strings like ['2026-04-01', '2026-04-02', ...]
    Ignores subdirectories like backfill/.
    """
    from google.cloud import storage as gcs

    prefix_map = {
        "news":      "raw/news/",
        "kapital":   "raw/kapital/",
        "kursiv":    "raw/kursiv/",
        "kase_news": "raw/kase_news/",
        "adilet":    "raw/adilet/",
    }

    client = gcs.Client()
    bucket = client.bucket(BRONZE_BUCKET)
    prefix = prefix_map[source_name]

    dates = set()
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name.replace(prefix, "")
        # only direct files like 2026-04-01.json, not backfill/...
        if name.endswith(".json") and "/" not in name:
            dates.add(name.replace(".json", ""))

    return sorted(dates)


def process_date(spark, run_date: str) -> bool:
    """
    Process all news sources for a single date.
    Returns True if any data was written.
    """
    from functools import reduce

    dfs = []
    for source_name, path_template in SOURCES.items():
        path = f"gs://{BRONZE_BUCKET}/" + path_template.format(date=run_date)
        try:
            df = spark.read.json(path)
            if df.count() == 0:
                print(f"  ⚠ {source_name} {run_date}: empty")
                continue
            df = normalize(df, source_name)
            dfs.append(df)
            print(f"  ✓ {source_name} {run_date}: {df.count()} rows")
        except Exception as e:
            print(f"  ⚠ {source_name} {run_date}: {e}")
            continue

    if not dfs:
        return False

    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    combined = apply_tags(combined)
    combined = combined.dropDuplicates(["article_id"])

    for col_name in FINAL_COLS:
        if col_name not in combined.columns:
            combined = combined.withColumn(col_name, F.lit(None).cast(StringType()))

    combined = combined.select(FINAL_COLS)

    combined.write \
        .mode("overwrite") \
        .parquet(f"gs://{SILVER_BUCKET}/news/run_date={run_date}/")

    print(f"  → written {combined.count()} rows to silver/news/run_date={run_date}/")
    return True


if __name__ == "__main__":
    from ingestion.utils import build_spark

    spark = build_spark("monitorium-silver-news-backfill")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # Discover all dates that have at least news or kapital bronze data
    all_dates = set()
    for source_name in SOURCES:
        dates = discover_dates(spark, source_name)
        all_dates.update(dates)
        print(f"{source_name}: {len(dates)} dates found")

    all_dates = sorted(all_dates)
    print(f"\nTotal unique dates to process: {len(all_dates)}")

    written = 0
    skipped = 0
    for run_date in all_dates:
        print(f"\nProcessing {run_date}...")
        ok = process_date(spark, run_date)
        if ok:
            written += 1
        else:
            skipped += 1

    print(f"\nDone. Written: {written} dates, skipped (no data): {skipped} dates")
    spark.stop()