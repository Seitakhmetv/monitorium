# transformation/silver_worldbank.py

import os
import sys
from datetime import date

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

from ingestion.utils import build_spark, write_silver
from ingestion.transforms import clean_worldbank

BRONZE = os.getenv("GCS_BRONZE_BUCKET")
SILVER = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


if __name__ == "__main__":
    spark = build_spark("monitorium-silver-worldbank")

    # Worldbank bronze is written per-indicator: raw/worldbank/{indicator}/{date}.json
    # Use wildcard to read all indicators for the run date in one pass.
    path = f"gs://{BRONZE}/raw/worldbank/*/{RUN_DATE}.json"
    try:
        df_raw = spark.read.json(path)
    except Exception as e:
        print(f"No worldbank data for {RUN_DATE}: {e}")
        spark.stop()
        raise SystemExit(0)

    if df_raw.count() == 0:
        print(f"No worldbank data for {RUN_DATE} — skipping")
        spark.stop()
        raise SystemExit(0)

    df = clean_worldbank(df_raw)
    write_silver(df, SILVER, "worldbank", RUN_DATE)
    print(f"Silver worldbank written for {RUN_DATE}: {df.count()} rows")
    spark.stop()
