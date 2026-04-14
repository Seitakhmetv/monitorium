import os
import sys
from datetime import date
from functools import reduce

from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType
from dotenv import load_dotenv

from ingestion.config import NEWS_SOURCES
from ingestion.tagger import tag_article

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())
DEV = os.getenv("DEV", "false") == "true"

# ── source paths (derived from config — add sources in config.py only) ────────
SOURCES = {name: f"{cfg['gcs_prefix']}/{{date}}.json" for name, cfg in NEWS_SOURCES.items()}

FINAL_COLS = [
    "article_id", "source", "source_type", "title", "url",
    "pub_date", "language", "run_date",
    "companies", "sectors", "countries", "topics",
    "impact", "weight", "is_legal", "category",
    "description",
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
    lambda title, source, source_type: tag_article(
        title or "", source or "", source_type or "news"
    ),
    TAG_SCHEMA,
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

    df = df.withColumn("pub_date", F.coalesce(
        F.to_timestamp(F.col("pub_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(F.col("pub_date"), "yyyy-MM-dd'T'HH:mm:ss+05:00"),
        F.to_timestamp(F.col("pub_date"), "EEE, dd MMM yyyy HH:mm:ss Z"),
        F.to_timestamp(F.col("pub_date"), "yyyy-MM-dd"),
        F.current_timestamp(),
    ))
    df = df.withColumn("run_date", F.to_date(F.col("run_date")))
    df = df.withColumn("description",
        F.when(F.col("description").isNotNull(), F.col("description").substr(1, 500))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return df.filter(F.col("article_id").isNotNull() & F.col("title").isNotNull())


def apply_tags(df):
    df = df.withColumn("tags", tag_udf(F.col("title"), F.col("source"), F.col("source_type")))
    for field in ["companies", "sectors", "countries", "topics", "impact", "weight", "is_legal", "category"]:
        df = df.withColumn(field, F.col(f"tags.{field}"))
    return df.drop("tags")


def _ensure_final_cols(df):
    for col_name in FINAL_COLS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))
    return df.select(FINAL_COLS)


if __name__ == "__main__":
    print(f"DEV={DEV} | RUN_DATE={RUN_DATE}")

    if DEV:
        spark = SparkSession.builder \
            .appName("monitorium-silver-news-dev") \
            .master("local[*]") \
            .getOrCreate()
    else:
        from ingestion.utils import build_spark
        spark = build_spark("monitorium-silver-news")

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    dfs = []
    for source_name, path_template in SOURCES.items():
        if DEV:
            path = f"sample/{source_name}.json"
            if not os.path.exists(path):
                print(f"⚠ {source_name}: no sample file — skipping")
                continue
        else:
            path = f"gs://{BRONZE_BUCKET}/" + path_template.format(date=RUN_DATE)
        try:
            df = spark.read.json(path)
            if df.count() == 0:
                print(f"⚠ {source_name}: empty")
                continue
            dfs.append(normalize(df, source_name))
            print(f"✓ {source_name}: loaded")
        except Exception as e:
            print(f"⚠ {source_name}: {e}")

    if not dfs:
        print(f"No news data for {RUN_DATE} — skipping")
        spark.stop()
        exit(0)

    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    combined = apply_tags(combined)
    combined = combined.dropDuplicates(["article_id"])
    combined = _ensure_final_cols(combined)

    if DEV:
        combined.show(10, truncate=False)
        combined.printSchema()
        print(f"DEV: {combined.count()} rows — not writing to GCS")
    else:
        from ingestion.utils import write_silver
        write_silver(combined, SILVER_BUCKET, "news", RUN_DATE)
        print(f"Silver news written for {RUN_DATE}: {combined.count()} rows")

    spark.stop()
