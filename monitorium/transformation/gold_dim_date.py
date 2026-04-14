import os
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from dotenv import load_dotenv

from ingestion.utils import build_spark, write_gold

load_dotenv(dotenv_path=".env")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET    = os.getenv("BQ_DATASET")


def generate_dim_date(spark):
    return spark.sql(
        "SELECT sequence(to_date('2000-01-01'), to_date('2030-12-31'), interval 1 day) AS date"
    ) \
    .select(F.explode("date").alias("date")) \
    .withColumn("date_key",    F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("year",        F.year("date")) \
    .withColumn("quarter",     F.quarter("date")) \
    .withColumn("month",       F.month("date")) \
    .withColumn("week",        F.weekofyear("date")) \
    .withColumn("day_of_week", F.dayofweek("date")) \
    .withColumn("is_weekend",  F.dayofweek("date").isin(1, 7)) \
    .select("date_key", "date", "year", "quarter", "month", "week", "day_of_week", "is_weekend")


if __name__ == "__main__":
    spark = build_spark("monitorium-gold-dim-date")
    df = generate_dim_date(spark)
    df.show(5)
    write_gold(df, PROJECT_ID, DATASET, "dim_date")
    print(f"dim_date written: {df.count()} rows")
    spark.stop()
