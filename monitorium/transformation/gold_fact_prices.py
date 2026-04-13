import os
import sys
from datetime import date

from pyspark.sql import functions as F
from dotenv import load_dotenv

from ingestion.utils import build_spark, write_gold

load_dotenv()

PROJECT_ID    = os.getenv("GCP_PROJECT_ID")
DATASET       = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.getenv("RUN_DATE") or str(date.today())


def build_fact_prices(spark):
    # "ALL" = backfill mode: read every silver partition at once
    prices_path = (
        f"gs://{SILVER_BUCKET}/prices/run_date=*/"
        if RUN_DATE == "ALL"
        else f"gs://{SILVER_BUCKET}/prices/run_date={RUN_DATE}/"
    )
    prices_df = spark.read.parquet(prices_path)

    dim_date_df = spark.read.format("bigquery") \
        .option("project", PROJECT_ID).option("dataset", DATASET).option("table", "dim_date") \
        .load()

    return prices_df \
        .join(dim_date_df, prices_df["date"] == dim_date_df["date"], "left") \
        .select(
            dim_date_df["date_key"],
            prices_df["ticker"],
            prices_df["open"],
            prices_df["high"],
            prices_df["low"],
            prices_df["close"],
            prices_df["volume"],
            prices_df["currency"],
            F.to_date(F.lit(RUN_DATE if RUN_DATE != "ALL" else str(date.today()))).alias("run_date"),
        ) \
        .dropDuplicates(["date_key", "ticker"])


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-fact-prices")
    df = build_fact_prices(spark)
    df.show(5)
    # ALL mode: truncate for a clean backfill; daily: append
    mode = "overwrite" if RUN_DATE == "ALL" else "append"
    write_gold(df, PROJECT_ID, DATASET, "fact_stock_prices", mode=mode)
    print(f"fact_stock_prices written ({mode}): {df.count()} rows")
    spark.stop()
