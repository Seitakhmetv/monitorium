#to save costs running through scheduler but implemented DAG below
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# ── config ────────────────────────────────────────────────────────────────────
PROJECT_ID = "monitorium-491507"
REGION = "us-central1"
CLUSTER = "monitorium-cluster"
BUCKET = "gs://monitorium-scripts"
RUN_DATE = "{{ ds }}"  # Airflow macro — injects execution date automatically

WHEEL = "monitorium-latest-py3-none-any.whl"  # symlink to latest — update deploy.sh to also upload this

DEFAULT_ARGS = {
    "owner": "monitorium",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── helpers ───────────────────────────────────────────────────────────────────
def dataproc_task(task_id: str, script: str) -> DataprocSubmitPySparkJobOperator:
    return DataprocSubmitPySparkJobOperator(
        task_id=task_id,
        main=f"{BUCKET}/transformation/{script}",
        pyfiles=[f"{BUCKET}/{WHEEL}"],
        files=[f"{BUCKET}/.env"],
        dataproc_properties={
            "spark.executorEnv.RUN_DATE": RUN_DATE
        },
        cluster_name=CLUSTER,
        region=REGION,
        project_id=PROJECT_ID,
    )


def http_task(task_id: str, url: str) -> SimpleHttpOperator:
    return SimpleHttpOperator(
        task_id=task_id,
        method="POST",
        http_conn_id="http_default",
        endpoint=url,
        response_check=lambda response: response.status_code == 200,
        retries=2,
    )


# ── dag ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="monitorium_pipeline",
    default_args=DEFAULT_ARGS,
    description="Monitorium end-to-end data pipeline",
    schedule_interval="0 0 * * *",  # midnight UTC daily
    start_date=days_ago(1),
    catchup=False,
    tags=["monitorium", "finance", "etl"],
) as dag:

    # ── ingestion ─────────────────────────────────────────────────────────────
    ingest_yfinance = http_task(
        "ingest_yfinance",
        "https://scraper-yfinance-6q2dmzhxyq-uc.a.run.app"
    )

    ingest_worldbank = http_task(
        "ingest_worldbank",
        "https://scraper-worldbank-6q2dmzhxyq-uc.a.run.app"
    )

    ingest_news = http_task(
        "ingest_news",
        "https://scraper-news-6q2dmzhxyq-uc.a.run.app"
    )

    # ── silver ────────────────────────────────────────────────────────────────
    silver_prices    = dataproc_task("silver_prices",    "silver_prices.py")
    silver_metadata  = dataproc_task("silver_metadata",  "silver_metadata.py")
    silver_worldbank = dataproc_task("silver_worldbank", "silver_worldbank.py")
    silver_news      = dataproc_task("silver_news",      "silver_news.py")

    # ── gold ──────────────────────────────────────────────────────────────────
    gold_dim_company  = dataproc_task("gold_dim_company",  "gold_dim_company.py")
    gold_dim_country  = dataproc_task("gold_dim_country",  "gold_dim_country.py")
    gold_fact_prices  = dataproc_task("gold_fact_prices",  "gold_fact_prices.py")
    gold_fact_macro   = dataproc_task("gold_fact_macro",   "gold_fact_macro.py")
    gold_news         = dataproc_task("gold_news",         "gold_news.py")

    # ── dependencies ──────────────────────────────────────────────────────────

    # ingestion runs in parallel
    [ingest_yfinance, ingest_worldbank, ingest_news]

    # silver runs after its source is ingested
    ingest_yfinance  >> [silver_prices, silver_metadata]
    ingest_worldbank >> silver_worldbank
    ingest_news      >> silver_news

    # gold runs after all silver is done
    [silver_prices, silver_metadata] >> gold_dim_company
    [silver_prices, silver_metadata] >> gold_fact_prices
    silver_worldbank >> gold_dim_country
    silver_worldbank >> gold_fact_macro
    silver_news      >> gold_news