# main.py (project root)

import functions_framework
from ingestion.scraper_yfinance import fetch_prices, fetch_metadata
from ingestion.scraper_worldbank import fetch_all, INDICATORS, COUNTRIES
from ingestion.scraper_news import fetch_news, deduplicate
from ingestion.utils import upload_to_gcs
from datetime import date
import os


# ── shared ────────────────────────────────────────────────────────────────────

def submit_dataproc_job(script: str, run_date: str) -> int:
    from google.cloud import dataproc_v1

    project = "monitorium-491507"
    region = "us-central1"
    cluster = "monitorium-cluster"
    bucket = "gs://monitorium-scripts"
    wheel = f"{bucket}/monitorium-latest-py3-none-any.whl"

    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    job = {
        "placement": {"cluster_name": cluster},
        "pyspark_job": {
            "main_python_file_uri": f"{bucket}/{script}",
            "python_file_uris": [wheel],
            "file_uris": [f"{bucket}/.env"],
            "properties": {
                "spark.executorEnv.RUN_DATE": run_date
            }
        }
    }

    submitted_job = job_client.submit_job(
        request={"project_id": project, "region": region, "job": job}
    )

    # poll until done
    import time
    job_id = submitted_job.reference.job_id
    while True:
        job_status = job_client.get_job(
            request={"project_id": project, "region": region, "job_id": job_id}
        )
        state = job_status.status.state
        if state == dataproc_v1.JobStatus.State.DONE:
            return 0
        if state in (
            dataproc_v1.JobStatus.State.ERROR,
            dataproc_v1.JobStatus.State.CANCELLED,
        ):
            print(f"Job {job_id} failed with state: {state}")
            return 1
        print(f"Job {job_id} state: {state} — waiting...")
        time.sleep(15)


# ── scrapers ──────────────────────────────────────────────────────────────────

@functions_framework.http
def scraper_yfinance_run(request):
    run_date = str(date.today())
    tickers = ["AAPL", "MSFT", "JPM"]

    prices = fetch_prices(tickers, start=run_date, end=run_date)
    metadata = fetch_metadata(tickers)

    upload_to_gcs(prices, os.getenv("GCS_BRONZE_BUCKET"),
                  f"raw/prices/{run_date}.json")
    upload_to_gcs(metadata, os.getenv("GCS_BRONZE_BUCKET"),
                  f"raw/metadata/{run_date}.json")

    return f"Uploaded {len(prices)} prices, {len(metadata)} metadata", 200


@functions_framework.http
def scraper_worldbank_run(request):
    run_date = str(date.today())
    all_data = fetch_all(COUNTRIES, INDICATORS)

    for indicator_name, records in all_data.items():
        upload_to_gcs(records, os.getenv("GCS_BRONZE_BUCKET"),
                      f"raw/worldbank/{indicator_name}/{run_date}.json")

    return f"Uploaded {sum(len(v) for v in all_data.values())} records", 200


@functions_framework.http
def scraper_news_run(request):
    run_date = str(date.today())
    tickers = ["AAPL", "MSFT", "JPM"]

    all_articles = []
    for ticker in tickers:
        all_articles.extend(fetch_news(ticker))

    deduped = deduplicate(all_articles)
    upload_to_gcs(deduped, os.getenv("GCS_BRONZE_BUCKET"),
                  f"raw/news/{run_date}.json")

    return f"Uploaded {len(deduped)} articles", 200


# ── orchestration ─────────────────────────────────────────────────────────────

@functions_framework.http
def run_silver(request):
    run_date = str(date.today())

    for script in [
        "transformation/silver_prices.py",
        "transformation/silver_metadata.py",
        "transformation/silver_worldbank.py",
        "transformation/silver_news.py",
    ]:
        print(f"Submitting {script}...")
        result = submit_dataproc_job(script, run_date)
        if result != 0:
            return f"FAILED: {script}", 500
        print(f"✓ {script}")

    return "Silver complete", 200


@functions_framework.http
def run_gold(request):
    run_date = str(date.today())

    for script in [
        "transformation/gold_dim_company.py",
        "transformation/gold_dim_country.py",
        "transformation/gold_fact_prices.py",
        "transformation/gold_fact_macro.py",
        "transformation/gold_news.py",
    ]:
        print(f"Submitting {script}...")
        result = submit_dataproc_job(script, run_date)
        if result != 0:
            return f"FAILED: {script}", 500
        print(f"✓ {script}")

    return "Gold complete", 200