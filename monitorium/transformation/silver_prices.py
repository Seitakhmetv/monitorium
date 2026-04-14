import os
import sys
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType, StringType
from dotenv import load_dotenv

from ingestion.utils import build_spark, write_silver

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())

FINAL_COLS = ["date", "ticker", "open", "high", "low", "close", "volume", "currency", "run_date"]


def clean_prices(df, currency: str):
    """Shared cleaning logic for both yfinance and KASE price data."""
    if "adj_close" in df.columns:
        df = df.drop("adj_close")
    return df \
        .withColumn("date",     F.to_date(F.col("date"))) \
        .withColumn("run_date", F.to_date(F.col("run_date"))) \
        .withColumn("open",     F.col("open").cast(FloatType())) \
        .withColumn("high",     F.col("high").cast(FloatType())) \
        .withColumn("low",      F.col("low").cast(FloatType())) \
        .withColumn("close",    F.col("close").cast(FloatType())) \
        .withColumn("volume",   F.col("volume").cast(LongType())) \
        .withColumn("currency", F.lit(currency).cast(StringType())) \
        .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull()) \
        .select(FINAL_COLS)


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices")
    dfs = []

    for path, currency in [
        (f"gs://{BRONZE_BUCKET}/raw/prices/{RUN_DATE}.json",      "USD"),
        (f"gs://{BRONZE_BUCKET}/raw/kase_prices/{RUN_DATE}.json", "KZT"),
    ]:
        try:
            df = spark.read.json(path)
            if df.count() == 0:
                print(f"⚠ {path}: empty — skipping")
                continue
            dfs.append(clean_prices(df, currency))
        except Exception as e:
            print(f"⚠ {path}: {e} — skipping")

    if not dfs:
        print(f"No price data for {RUN_DATE} — skipping")
        spark.stop()
        exit(0)

    from functools import reduce
    combined = reduce(lambda a, b: a.unionByName(b), dfs)
    combined = combined.dropDuplicates(["ticker", "date"])

    write_silver(combined, SILVER_BUCKET, "prices", RUN_DATE)
    print(f"Silver prices written for {RUN_DATE}: {combined.count()} rows")
    spark.stop()
