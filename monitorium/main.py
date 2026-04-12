# main.py (project root)

import functions_framework
from ingestion.scraper_yfinance import fetch_prices, fetch_metadata
from ingestion.scraper_worldbank import fetch_all, INDICATORS, COUNTRIES, flatten_for_df
from ingestion.scraper_news import fetch_news, deduplicate
from ingestion.scraper_adilet import fetch_adilet
from ingestion.scraper_kase_news import fetch_kase_news
from ingestion.scraper_kapital import fetch_kapital
from ingestion.scraper_kursiv import fetch_kursiv
from ingestion.utils import upload_to_gcs
from ingestion.scraper_kase import fetch_kase_prices
from ingestion.config import KASE_TICKERS
import requests
from datetime import date, timedelta
from ingestion.config import TICKERS
import os


# ── shared ────────────────────────────────────────────────────────────────────

def submit_dataproc_job(script: str, run_date: str) -> int:
    from google.cloud import dataproc_v1

    project = "monitorium-491507"
    region  = "us-central1"
    bucket  = "gs://monitorium-scripts"

    # ingestion.zip (built by deploy.sh) distributes the ingestion package to Dataproc.
    # Dataproc Serverless python_file_uris only supports .py/.zip/.egg — not .whl.
    ingestion_zip = f"{bucket}/ingestion.zip"

    bronze_bucket = os.getenv("GCS_BRONZE_BUCKET", "monitorium-bronze")
    silver_bucket = os.getenv("GCS_SILVER_BUCKET", "monitorium-silver")
    gcp_project   = os.getenv("GCP_PROJECT_ID", project)
    bq_dataset    = os.getenv("BQ_DATASET", "monitorium")

    batch_client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # All env vars passed explicitly so transformation scripts don't need dotenv on Dataproc.
    env_props = {
        "spark.executorEnv.RUN_DATE":          run_date,
        "spark.driverEnv.RUN_DATE":            run_date,
        "spark.executorEnv.GCS_BRONZE_BUCKET": bronze_bucket,
        "spark.driverEnv.GCS_BRONZE_BUCKET":   bronze_bucket,
        "spark.executorEnv.GCS_SILVER_BUCKET": silver_bucket,
        "spark.driverEnv.GCS_SILVER_BUCKET":   silver_bucket,
        "spark.executorEnv.GCP_PROJECT_ID":    gcp_project,
        "spark.driverEnv.GCP_PROJECT_ID":      gcp_project,
        "spark.executorEnv.BQ_DATASET":        bq_dataset,
        "spark.driverEnv.BQ_DATASET":          bq_dataset,
        "spark.executorEnv.ENV":               "dataproc",
        "spark.driverEnv.ENV":                 "dataproc",
    }

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": f"{bucket}/{script}",
            "python_file_uris":     [ingestion_zip],
            "file_uris":            [f"{bucket}/.env"],
            "args":                 [run_date],
        },
        "environment_config": {
            "execution_config": {
                "service_account": "monitorium-sa@monitorium-491507.iam.gserviceaccount.com"
            }
        },
        "runtime_config": {"properties": env_props},
    }

    operation = batch_client.create_batch(
        parent=f"projects/{project}/locations/{region}",
        batch=batch,
    )

    result = operation.result()  # blocks until completion
    print(f"Batch finished [{script}]: {result.state}")

    return 0 if result.state == dataproc_v1.Batch.State.SUCCEEDED else 1


# ── scrapers ──────────────────────────────────────────────────────────────────

@functions_framework.http
def scraper_yfinance_run(request):
    run_date = str(date.today())

    tickers = TICKERS

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
    adilet_date = str(date.today().strftime("%Y-%m"))
    tickers = TICKERS

    all_articles = []
    for ticker in tickers:
        all_articles.extend(fetch_news(ticker))
    deduped = deduplicate(all_articles)
    upload_to_gcs(deduped, os.getenv("GCS_BRONZE_BUCKET"),
                  f"raw/news/{run_date}.json")
    upload_to_gcs(fetch_kase_news(start_date=run_date, end_date=run_date), os.getenv("GCS_BRONZE_BUCKET"), f"raw/kase_news/{run_date}.json")
    upload_to_gcs(fetch_kapital(), os.getenv("GCS_BRONZE_BUCKET"), f"raw/kapital/{run_date}.json")
    upload_to_gcs(fetch_kursiv(), os.getenv("GCS_BRONZE_BUCKET"), f"raw/kursiv/{run_date}.json")
    upload_to_gcs(fetch_adilet(adilet_date), os.getenv("GCS_BRONZE_BUCKET"), f"raw/adilet/{run_date}.json")

    return f"Uploaded articles", 200

@functions_framework.http
def scraper_kase_run(request):
    from ingestion.scraper_kase import fetch_kase_prices
    from ingestion.config import KASE_TICKERS

    run_date = str(date.today())

    prices = fetch_kase_prices(
        tickers=KASE_TICKERS,
        from_date=str(date.today()),
        to_date=str(date.today()+timedelta(days=1))
    )
    upload_to_gcs(prices, os.getenv("GCS_BRONZE_BUCKET"),
                f"raw/kase_prices/{run_date}.json")

    return f"Uploaded {len(prices)} KASE historical rows", 200

# ── orchestration ─────────────────────────────────────────────────────────────

@functions_framework.http
def run_silver(request):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    scripts = [
        "transformation/silver_prices.py",
        "transformation/silver_metadata.py",
        "transformation/silver_worldbank.py",
        "transformation/silver_news.py",
    ]
    run_date = str(date.today())
    failures = []

    # Silver scripts are independent — submit all in parallel to halve wall-clock time.
    with ThreadPoolExecutor(max_workers=len(scripts)) as executor:
        future_to_script = {
            executor.submit(submit_dataproc_job, script, run_date): script
            for script in scripts
        }
        for future in as_completed(future_to_script):
            script = future_to_script[future]
            try:
                result = future.result()
            except Exception as exc:
                print(f"✗ {script}: {exc}")
                failures.append(script)
                continue
            if result != 0:
                print(f"✗ {script}: FAILED")
                failures.append(script)
            else:
                print(f"✓ {script}")

    if failures:
        return f"FAILED: {', '.join(failures)}", 500
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
        result = submit_dataproc_job(script, run_date)  # run_date passed as arg
        if result != 0:
            return f"FAILED: {script}", 500
        print(f"✓ {script}")
    return "Gold complete", 200


#-------------- backfilling -----------------------------
@functions_framework.http
def run_gold_backfill(request):
    from google.cloud import storage as gcs

    client = gcs.Client()
    silver_bucket = os.getenv("GCS_SILVER_BUCKET")

    # ── one-shot gold scripts (read all partitions themselves) ────────────────
    # for script in [
    #     "transformation/gold_dim_date.py",
    #     "transformation/gold_dim_country.py",
    #     "transformation/gold_fact_macro.py",
    # ]:
    #     print(f"Submitting {script}...")
    #     result = submit_dataproc_job(script, str(date.today()))
    #     if result != 0:
    #         return f"FAILED: {script}", 500

    # ── per-date gold scripts ─────────────────────────────────────────────────
    def get_silver_dates(prefix):
        dates = []
        for blob in client.bucket(silver_bucket).list_blobs(prefix=prefix):
            # blobs look like prices/run_date=2026-04-01/part-000.parquet
            part = blob.name.split("run_date=")
            if len(part) > 1:
                d = part[1].split("/")[0]
                if d not in dates:
                    dates.append(d)
        return sorted(dates)

    # # dim_company must run in date order to build SCD2 history correctly
    # for run_date in get_silver_dates("metadata/"):
    #     result = submit_dataproc_job("transformation/gold_dim_company.py", run_date)
    #     if result != 0:
    #         return f"FAILED: gold_dim_company {run_date}", 500

    # for run_date in get_silver_dates("prices/"):
    #     result = submit_dataproc_job("transformation/gold_fact_prices.py", run_date)
    #     if result != 0:
    #         return f"FAILED: gold_fact_prices {run_date}", 500

    for run_date in get_silver_dates("news/"):
        result = submit_dataproc_job("transformation/gold_news.py", run_date)
        if result != 0:
            return f"FAILED: gold_news {run_date}", 500

    return "Gold backfill complete", 200

@functions_framework.http
def backfill(request):
    BUCKET = os.getenv("GCS_BRONZE_BUCKET")
    START_YEAR = 2000
    END_YEAR = date.today().year
    log = []

    # ── find last trading day for yfinance ────────────────────────────────────
    def find_last_trading_day_yfinance() -> str:
        check_date = date.today() - timedelta(days=1)
        for _ in range(10):
            date_str = str(check_date)
            data = fetch_prices(["AAPL"], start=date_str, end=date_str)
            if data:
                print(f"✓ yfinance last trading day: {date_str}")
                return date_str
            check_date -= timedelta(days=1)
        return str(date.today() - timedelta(days=1))

    # ── find last trading day for kase ────────────────────────────────────────
    def find_last_trading_day_kase() -> str:
        from datetime import datetime
        check_date = date.today() - timedelta(days=1)
        for _ in range(10):
            date_str = str(check_date)
            from_ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())
            to_ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp()) + 86400
            resp = requests.get(
                "https://kase.kz/tv-charts/securities/history",
                params={"symbol": "ALL:HSBK", "resolution": "1D",
                        "from": from_ts, "to": to_ts},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            data = resp.json()
            if data.get("s") == "ok" and data.get("t"):
                print(f"✓ KASE last trading day: {date_str}")
                return date_str
            check_date -= timedelta(days=1)
        return str(date.today() - timedelta(days=1))

    # ── yfinance backfill (year by year up to last trading day) ───────────────
    last_yfinance = find_last_trading_day_yfinance()
    last_year = int(last_yfinance[:4])

    for year in range(START_YEAR, last_year + 1):
        chunk_start = f"{year}-01-01"
        chunk_end = last_yfinance if year == last_year else f"{year}-12-31"

        print(f"Fetching yfinance {chunk_start} → {chunk_end}...")
        prices = fetch_prices(TICKERS, start=chunk_start, end=chunk_end)

        if prices:
            upload_to_gcs(prices, BUCKET, f"raw/prices/backfill/{year}.json")
            log.append(f"✓ yfinance {year}: {len(prices)} rows")
        else:
            log.append(f"⚠ yfinance {year}: no data")
        print(log[-1])

    # ── kase backfill (single call, all history) ──────────────────────────────
    last_kase = find_last_trading_day_kase()

    prices_kase = fetch_kase_prices(
        tickers=KASE_TICKERS,
        from_date="2000-01-01",
        to_date=last_kase
    )

    if prices_kase:
        upload_to_gcs(prices_kase, BUCKET, "raw/kase_prices/backfill/all.json")
        log.append(f"✓ kase backfill: {len(prices_kase)} rows up to {last_kase}")
    else:
        log.append("⚠ kase backfill: no data")
    print(log[-1])

    # ── worldbank (unchanged) ─────────────────────────────────────────────────
    print("Fetching World Bank historical data...")
    all_data = fetch_all(COUNTRIES, INDICATORS)
    flat = flatten_for_df(all_data)
    upload_to_gcs(flat, BUCKET, "raw/worldbank/backfill/all.json")
    log.append(f"✓ worldbank: {len(flat)} records")

    return "\n".join(log), 200


@functions_framework.http
def run_silver_backfill(request):
    # One Dataproc job handles all silver backfills in sequence.
    # Single cold-start instead of four — cheaper and simpler.
    result = submit_dataproc_job("transformation/silver_backfill_all.py", str(date.today()))
    if result != 0:
        return "Silver backfill FAILED", 500
    return "Silver backfill complete", 200