# transformation/silver_prices.py

import os
import sys
from datetime import date

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark, write_silver
from ingestion.transforms import clean_prices

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


def read_source(spark, path: str):
    try:
        df = spark.read.json(path)
        return df if df.count() > 0 else None
    except Exception as e:
        print(f"skip {path}: {e}")
        return None


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-prices")

    df_us   = read_source(spark, f"gs://{BRONZE}/raw/prices/{RUN_DATE}.json")
    df_kase = read_source(spark, f"gs://{BRONZE}/raw/kase_prices/{RUN_DATE}.json")

    if df_us is None and df_kase is None:
        print(f"No price data for {RUN_DATE} — skipping")
        spark.stop()
        raise SystemExit(0)

    parts = [clean_prices(df) for df in [df_us, df_kase] if df is not None]
    df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    df = df.dropDuplicates(["ticker", "date"])

    write_silver(df, SILVER, "prices", RUN_DATE)
    print(f"Silver prices written for {RUN_DATE}: {df.count()} rows")
    spark.stop()
