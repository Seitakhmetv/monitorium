from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from ingestion.utils import build_spark, write_gold
import os
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    LongType, DateType, BooleanType
)
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
from datetime import date
import sys
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())

FINAL_COLS = [
    "company_key", "ticker", "shortName", "sector",
    "industry", "country", "marketCap", "currency",
    "valid_from", "valid_to", "is_current"
]

CHANGE_COLS = ["sector", "industry", "country", "marketCap", "currency"]


EMPTY_SCHEMA = StructType([
    StructField("company_key", LongType()),
    StructField("ticker", StringType()),
    StructField("shortName", StringType()),
    StructField("sector", StringType()),
    StructField("industry", StringType()),
    StructField("country", StringType()),
    StructField("marketCap", LongType()),
    StructField("currency", StringType()),
    StructField("valid_from", DateType()),
    StructField("valid_to", DateType()),
    StructField("is_current", BooleanType()),
])

def read_existing_dim(spark):
    try:
        return spark.read.format("bigquery") \
            .option("project", PROJECT_ID) \
            .option("dataset", DATASET) \
            .option("table", "dim_company") \
            .load()
    except Exception as e:
        print(f"dim_company not found, assuming first run. Error: {e}")
        return spark.createDataFrame([], EMPTY_SCHEMA)

def detect_changed_tickers(existing_df, incoming_df):
    """
    Compare incoming vs current existing rows.
    Return DataFrame of tickers where any tracked column changed.
    """
    current = existing_df.filter(F.col("is_current") == True)

    joined = current.alias("e").join(
        incoming_df.alias("i"),
        on="ticker",
        how="inner"
    )

    change_condition = " OR ".join(
        [f"e.{c} != i.{c}" for c in CHANGE_COLS]
    )

    return joined.filter(change_condition).select("e.ticker")


def apply_scd2(existing_df, incoming_df, run_date: str):
    # ── first run ─────────────────────────────────────────────
    if existing_df.count() == 0:
        return incoming_df \
            .withColumn("company_key", F.monotonically_increasing_id()) \
            .withColumn("valid_from", F.lit(run_date).cast(DateType())) \
            .withColumn("valid_to", F.lit("9999-12-31").cast(DateType())) \
            .withColumn("is_current", F.lit(True)) \
            .select(FINAL_COLS)

    # ── subsequent runs ───────────────────────────────────────
    changed_tickers = detect_changed_tickers(existing_df, incoming_df)

    # rows with no changes — keep entirely untouched
    unchanged = existing_df.join(
        changed_tickers,
        on="ticker",
        how="left_anti"  # rows NOT in changed_tickers
    )

    # expire old rows for changed tickers
    changed_old = existing_df \
        .filter(F.col("is_current") == True) \
        .join(changed_tickers, on="ticker", how="inner") \
        .withColumn("valid_to", F.lit(run_date).cast(DateType())) \
        .withColumn("is_current", F.lit(False))

    # new rows for changed tickers
    changed_new = incoming_df \
        .join(changed_tickers, on="ticker", how="inner") \
        .withColumn("company_key", F.monotonically_increasing_id()) \
        .withColumn("valid_from", F.lit(run_date).cast(DateType())) \
        .withColumn("valid_to", F.lit("9999-12-31").cast(DateType())) \
        .withColumn("is_current", F.lit(True)) \
        .select(FINAL_COLS)

    return unchanged \
        .select(FINAL_COLS) \
        .unionByName(changed_old.select(FINAL_COLS)) \
        .unionByName(changed_new)


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-dim-company")

    incoming = spark.read.parquet(
        f"gs://{SILVER_BUCKET}/metadata/run_date={RUN_DATE}/"
    )

    existing = read_existing_dim(spark)
    final_df = apply_scd2(existing, incoming, RUN_DATE)

    final_df.show(5)
    write_gold(final_df, PROJECT_ID, DATASET, "dim_company")
    print(f"dim_company written: {final_df.count()} rows")
    spark.stop()