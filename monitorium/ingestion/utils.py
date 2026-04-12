from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType
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
        return SparkSession.builder.appName(app_name).getOrCreate()

    # Local dev: wire up GCS connector with service account credentials.
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26")
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        .getOrCreate()
    )


def write_silver(df, bucket_name: str, blob_path: str, run_date: str) -> None:
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
    """Print basic stats — call only when debugging, not in prod hot paths."""
    count = df.count()
    print(f"Total rows: {count}")
    null_counts = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}
    print(f"Null counts: {null_counts}")
    dupe_count = count - df.dropDuplicates(primary_keys).count()
    print(f"Duplicates on {primary_keys}: {dupe_count}")
