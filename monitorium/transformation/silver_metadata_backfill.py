import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType

from ingestion.utils import build_spark
from ingestion.config import KASE_TICKERS
from transformation.silver_metadata import clean_metadata

load_dotenv(override=True)

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")

FINAL_COLS = ["ticker", "shortName", "sector", "industry", "country", "marketCap", "currency", "run_date"]


def discover_metadata_dates() -> list:
    from google.cloud import storage as gcs
    client = gcs.Client()
    dates = set()
    for blob in client.bucket(BRONZE_BUCKET).list_blobs(prefix="raw/metadata/"):
        name = blob.name.replace("raw/metadata/", "")
        if name.endswith(".json") and "/" not in name:
            dates.add(name.replace(".json", ""))
    return sorted(dates)


def build_kase_stubs(spark, run_date: str, existing_tickers: list):
    missing = [t for t in KASE_TICKERS if t not in existing_tickers]
    if not missing:
        return None
    stubs = spark.createDataFrame([(t,) for t in missing], ["ticker"]) \
        .withColumn("shortName",  F.lit(None).cast(StringType())) \
        .withColumn("sector",     F.lit(None).cast(StringType())) \
        .withColumn("industry",   F.lit(None).cast(StringType())) \
        .withColumn("country",    F.lit("KZ").cast(StringType())) \
        .withColumn("marketCap",  F.lit(None).cast(LongType())) \
        .withColumn("currency",   F.lit("KZT").cast(StringType())) \
        .withColumn("run_date",   F.to_date(F.lit(run_date)))
    print(f"  Stubbing {len(missing)} KASE tickers: {missing}")
    return stubs.select(FINAL_COLS)


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-metadata-backfill")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    dates = discover_metadata_dates()
    print(f"Found {len(dates)} metadata dates in bronze")

    for run_date in dates:
        print(f"\nProcessing {run_date}...")
        try:
            df_raw = spark.read.json(f"gs://{BRONZE_BUCKET}/raw/metadata/{run_date}.json")
            if df_raw.count() == 0:
                print(f"  ⚠ empty — skipping")
                continue

            df_clean = clean_metadata(df_raw).select(FINAL_COLS)
            existing_tickers = [r["ticker"] for r in df_clean.select("ticker").collect()]
            stubs = build_kase_stubs(spark, run_date, existing_tickers)

            df_final = df_clean.unionByName(stubs) if stubs is not None else df_clean
            df_final.write.mode("overwrite").parquet(
                f"gs://{SILVER_BUCKET}/metadata/run_date={run_date}/"
            )
            print(f"  → {df_final.count()} rows written")
        except Exception as e:
            print(f"  ✗ {run_date}: {e}")

    print("\nSilver metadata backfill complete")
    spark.stop()
