from pyspark.sql import functions as F
from ingestion.utils import build_spark, write_gold
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET")
RUN_DATE = os.getenv("RUN_DATE")


def build_fact_prices(spark):
    """
    Read silver prices for RUN_DATE.
    Join to dim_date on date = date to get date_key.
    Join to dim_company on ticker where is_current = True to get company_key.
    Select final columns:
        date_key, ticker, open, high, low, close, volume, run_date
    Deduplicate on [date_key, ticker] — idempotent.
    """
    # your code here
    prices_df = spark.read.parquet(f"gs://{SILVER_BUCKET}/prices/run_date={RUN_DATE}/")
    dim_date_df = spark.read.format("bigquery") \
        .option("project", PROJECT_ID) \
        .option("dataset", DATASET) \
        .option("table", "dim_date") \
        .load()
    dim_company_df = spark.read.format("bigquery") \
        .option("project", PROJECT_ID) \
        .option("dataset", DATASET) \
        .option("table", "dim_company") \
        .load() \
        .filter(F.col("is_current") == True)
    df = prices_df.join(dim_date_df, prices_df["date"] == dim_date_df["date"], "left") \
        .join(dim_company_df, prices_df["ticker"] == dim_company_df["ticker"], "left") \
        .select(
            dim_date_df["date_key"],
            prices_df["ticker"],
            prices_df["open"],
            prices_df["high"],
            prices_df["low"],
            prices_df["close"],
            prices_df["volume"],
            F.lit(RUN_DATE).alias("run_date")
        ) \
        .dropDuplicates(["date_key", "ticker"])
    return df


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-fact-prices")
    df = build_fact_prices(spark)
    df.show(5)
    write_gold(df, PROJECT_ID, DATASET, "fact_stock_prices", mode="append")
    print(f"fact_stock_prices written: {df.count()} rows")
    spark.stop()