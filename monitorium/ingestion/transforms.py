# ingestion/transforms.py
#
# Shared transformation functions used by both daily and backfill silver scripts.
# Lives in the ingestion package so it is distributed to Dataproc via ingestion.zip.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, FloatType, IntegerType,
    LongType, StringType, StructField, StructType,
)
from ingestion.tagger import tag_article


# ── Prices ────────────────────────────────────────────────────────────────────

PRICES_COLS = ["date", "ticker", "open", "high", "low", "close", "volume", "currency", "run_date"]


def clean_prices(df):
    if "adj_close" in df.columns:
        df = df.drop("adj_close")
    if "currency" not in df.columns:
        df = df.withColumn("currency", F.lit("USD").cast(StringType()))
    return (
        df.withColumn("date",    F.to_date(F.col("date")))
          .withColumn("run_date", F.to_date(F.col("run_date")))
          .withColumn("open",    F.col("open").cast(FloatType()))
          .withColumn("high",    F.col("high").cast(FloatType()))
          .withColumn("low",     F.col("low").cast(FloatType()))
          .withColumn("close",   F.col("close").cast(FloatType()))
          .withColumn("volume",  F.col("volume").cast(LongType()))
          .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull())
          .dropDuplicates(["ticker", "date"])
          .select(PRICES_COLS)
    )


# ── Metadata ──────────────────────────────────────────────────────────────────

METADATA_COLS = ["ticker", "shortName", "sector", "industry", "country", "marketCap", "currency", "run_date"]


def clean_metadata(df):
    return (
        df.withColumn("run_date", F.to_date(F.col("run_date")))
          .withColumnRenamed("symbol", "ticker")
          .withColumn("marketCap", F.col("marketCap").cast(LongType()))
          .filter(F.col("ticker").isNotNull())
          .dropDuplicates(["ticker", "run_date"])
          .select(METADATA_COLS)
    )


def build_kase_stubs(spark, run_date: str, existing_tickers: list, kase_tickers: list):
    """Return a DataFrame of NULL-filled rows for KASE tickers absent from yfinance metadata."""
    missing = [t for t in kase_tickers if t not in existing_tickers]
    if not missing:
        return None
    return (
        spark.createDataFrame([(t,) for t in missing], ["ticker"])
             .withColumn("shortName", F.lit(None).cast(StringType()))
             .withColumn("sector",    F.lit(None).cast(StringType()))
             .withColumn("industry",  F.lit(None).cast(StringType()))
             .withColumn("country",   F.lit("KZ").cast(StringType()))
             .withColumn("marketCap", F.lit(None).cast(LongType()))
             .withColumn("currency",  F.lit("KZT").cast(StringType()))
             .withColumn("run_date",  F.to_date(F.lit(run_date)))
             .select(METADATA_COLS)
    )


# ── Worldbank ─────────────────────────────────────────────────────────────────

def clean_worldbank(df):
    return (
        df.withColumn("run_date", F.to_date(F.col("run_date")))
          .withColumn("year",  F.col("date").cast("int"))
          .withColumn("value", F.col("value").cast(FloatType()))
          .filter(
              F.col("country").isNotNull() &
              F.col("value").isNotNull() &
              F.col("year").isNotNull()
          )
          .dropDuplicates(["country", "indicator_name", "year", "run_date"])
    )


# ── News ──────────────────────────────────────────────────────────────────────

NEWS_FINAL_COLS = [
    "article_id", "source", "source_type", "title", "url",
    "pub_date", "language", "run_date",
    "companies", "sectors", "countries", "topics",
    "impact", "weight", "is_legal", "category", "description",
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


def normalize_news(df, source_name: str):
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


def finalize_news(df):
    """Ensure all final columns exist and select in canonical order."""
    for col_name in NEWS_FINAL_COLS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))
    return df.select(NEWS_FINAL_COLS)
