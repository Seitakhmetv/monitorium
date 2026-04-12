# transformation/silver_prices_backfill.py

import os
from functools import reduce

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark
from ingestion.transforms import clean_prices
from pyspark.sql import functions as F

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices-backfill")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    dfs = []
    for path, label in [
        (f"gs://{BRONZE}/raw/prices/backfill/*.json",      "yfinance"),
        (f"gs://{BRONZE}/raw/kase_prices/backfill/all.json", "KASE"),
    ]:
        try:
            df = spark.read.json(path)
            if df.count() > 0:
                dfs.append(clean_prices(df))
                print(f"{label}: loaded")
            else:
                print(f"{label}: empty — skipping")
        except Exception as e:
            print(f"{label}: not found — skipping ({e})")

    if not dfs:
        print("No price backfill data — nothing to write")
        spark.stop()
        raise SystemExit(0)

    df = reduce(lambda a, b: a.unionByName(b), dfs).dropDuplicates(["ticker", "date"])
    print(f"Total rows: {df.count()} | range: {df.agg(F.min('date'), F.max('date')).collect()}")

    df.write.mode("overwrite").partitionBy("run_date").parquet(f"gs://{SILVER}/prices/")
    print("Silver prices backfill complete")
    spark.stop()
