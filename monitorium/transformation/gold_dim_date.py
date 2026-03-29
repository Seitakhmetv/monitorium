from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from monitorium.utils import build_spark, write_gold
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")


def generate_dim_date(spark):
    """
    Generate a row for every date between 2020-01-01 and 2030-12-31.
    Use spark.sql("SELECT sequence(to_date('2020-01-01'), 
                                  to_date('2030-12-31'), 
                                  interval 1 day) as date")
    Then explode the sequence into individual rows.
    Derive all columns from the date:
    - date_key: F.date_format(col("date"), "yyyyMMdd").cast(IntegerType())
    - year:     F.year()
    - quarter:  F.quarter()
    - month:    F.month()
    - week:     F.weekofyear()
    - day_of_week: F.dayofweek()
    - is_weekend: day_of_week isin (1, 7)
    """
    # your code here
    df = spark.sql("SELECT sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day) as date") \
    .select(F.explode("date").alias("date")) \
    .withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("quarter", F.quarter(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("week", F.weekofyear(F.col("date"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("date"))) \
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    return df.select("date_key", "date", "year", "quarter", "month", "week", "day_of_week", "is_weekend")



if __name__ == "__main__":
    spark = build_spark("monitorium-gold-dim-date")
    df = generate_dim_date(spark)
    df.show(5)
    df.printSchema()
    write_gold(df, PROJECT_ID, DATASET, "dim_date")
    print(f"dim_date written: {df.count()} rows")
    spark.stop()