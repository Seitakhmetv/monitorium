from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, LongType
from dotenv import load_dotenv
import os
import json

def upload_to_gcs(data: list, bucket_name: str, blob_path: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    ndjson = "\n".join(json.dumps(record) for record in data)
    
    blob.upload_from_string(ndjson, content_type="application/json")


def build_spark(app_name: str) -> SparkSession:
    env = os.getenv("ENV", "local")

    if env == "dataproc":
        return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26") \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) \
        .getOrCreate()

def read_bronze(spark: SparkSession, run_date: str, bucket_name: str, blob_path: str):
    """
    Read raw/prices/{run_date}.json from GCS bronze bucket.
    Return DataFrame.
    """
    # your code here
    df = spark.read.json(f"gs://{bucket_name}/raw/{blob_path}/{run_date}.json")
    return df

def write_silver(df, bucket_name: str, blob_path: str, run_date: str,) -> None:
    """
    Write to GCS silver bucket as Parquet.
    Partition by run_date.
    Path: gs://{SILVER_BUCKET}/prices/run_date={run_date}/
    Overwrite partition — this is what makes it idempotent.
    Use: df.write.mode("overwrite").parquet(path)
    """
    # your code here
    df.write.mode("overwrite").parquet(f"gs://{bucket_name}/{blob_path}/run_date={run_date}/")

def write_gold(df, project_id: str, dataset: str, table: str, mode: str = "overwrite") -> None:
    df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{table}") \
        .option("temporaryGcsBucket", os.getenv("GCS_SILVER_BUCKET")) \
        .option("writeDisposition", "WRITE_TRUNCATE" if mode == "overwrite" else "WRITE_APPEND") \
        .mode("overwrite" if mode == "overwrite" else "append") \
        .save()

def validate(df, primary_keys: list) -> None:
    print(f"Total rows: {df.count()}")
    null_counts = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}
    print(f"Null counts per column: {null_counts}")
    print(f"Duplicate count: {df.count() - df.dropDuplicates(primary_keys).count()}")