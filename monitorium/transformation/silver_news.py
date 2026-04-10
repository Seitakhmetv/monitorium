import os
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import IntegerType, BooleanType, StringType, StructType, StructField
from dotenv import load_dotenv
from ingestion.tagger import tag_article
from datetime import date

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = os.getenv("RUN_DATE") or str(date.today())
DEV = os.getenv("DEV", "false") == "true"

SOURCES = {
    "news":      "raw/news/{date}.json",
    "kz_news":   "raw/kz_news/{date}.json",
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
    lambda title, source, source_type: tag_article(
        title or "", source or "", source_type or "news"
    ),
    TAG_SCHEMA
)


def get_spark() -> SparkSession:
    if DEV:
        return SparkSession.builder \
            .appName("monitorium-silver-news-dev") \
            .master("local[*]") \
            .getOrCreate()
    else:
        from ingestion.utils import build_spark
        return build_spark("monitorium-silver-news")


def read_source(spark, source_name: str):
    if DEV:
        path = f"sample/{source_name}.json"
        if not os.path.exists(path):
            print(f"⚠ {source_name}: no sample file — skipping")
            return None
    else:
        path = f"gs://{BRONZE_BUCKET}/" + SOURCES[source_name].format(date=RUN_DATE)

    try:
        df = spark.read.json(path)
        count = df.count()
        if count == 0:
            print(f"⚠ {source_name}: empty")
            return None
        print(f"✓ {source_name}: {count} rows")
        return df
    except Exception as e:
        print(f"⚠ {source_name}: {e}")
        return None


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


if __name__ == "__main__":
    print(f"DEV={DEV} | RUN_DATE={RUN_DATE}")
    spark = get_spark()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    dfs = []
    for source_name in SOURCES:
        df = read_source(spark, source_name)
        if df is not None:
            df = normalize(df, source_name)
            dfs.append(df)

    if not dfs:
        print(f"No news data for {RUN_DATE} — skipping")
        spark.stop()
        exit(0)

    from functools import reduce
    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    combined = apply_tags(combined)
    combined = combined.dropDuplicates(["article_id"])

    for col_name in FINAL_COLS:
        if col_name not in combined.columns:
            combined = combined.withColumn(col_name, F.lit(None).cast(StringType()))

    combined = combined.select(FINAL_COLS)

    if DEV:
        combined.show(10, truncate=False)
        combined.printSchema()
        print(f"DEV: {combined.count()} rows — not writing to GCS")
    else:
        from ingestion.utils import write_silver
        write_silver(combined, SILVER_BUCKET, "news", RUN_DATE)
        print(f"Silver news written for {RUN_DATE}")

    spark.stop()