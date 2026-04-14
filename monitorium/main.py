import importlib
import os
from datetime import date, timedelta

import functions_framework

from ingestion.config import (
    KASE_TICKERS, NEWS_SOURCES, SILVER_SCRIPTS, GOLD_SCRIPTS,
    SILVER_BACKFILL_SCRIPTS, TICKERS,
)
from ingestion.scraper_kase import fetch_kase_prices
from ingestion.scraper_worldbank import fetch_all, COUNTRIES, INDICATORS
from ingestion.scraper_yfinance import fetch_prices, fetch_metadata
from ingestion.utils import upload_to_gcs


# ── shared ────────────────────────────────────────────────────────────────────

def submit_dataproc_job(script: str, run_date: str, max_retries: int = 3) -> int:
    import time
    from google.cloud import dataproc_v1
    from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable, Aborted

    project = os.getenv("GCP_PROJECT_ID")
    region  = "us-central1"
    bucket  = f"gs://{os.getenv('GCS_SCRIPTS_BUCKET', 'monitorium-scripts')}"
    wheel   = f"{bucket}/monitorium-latest.zip"

    batch_client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    batch = {
        "pyspark_batch": {
            "main_python_file_uri": f"{bucket}/{script}",
            "python_file_uris": [wheel],
            "file_uris": [f"{bucket}/.env"],
            "args": [run_date],
        },
        "environment_config": {
            "execution_config": {
                "service_account": f"monitorium-sa@{project}.iam.gserviceaccount.com",
            }
        },
        "runtime_config": {
            "properties": {
                "spark.executorEnv.RUN_DATE": run_date,
                "spark.driverEnv.RUN_DATE":   run_date,
            }
        },
    }

    for attempt in range(1, max_retries + 1):
        try:
            operation = batch_client.create_batch(
                parent=f"projects/{project}/locations/{region}",
                batch=batch,
            )
            result = operation.result(timeout=3300)
            print(f"Batch job finished: {result.state}")
            return 0 if result.state == dataproc_v1.Batch.State.SUCCEEDED else 1
        except (ResourceExhausted, ServiceUnavailable) as e:
            if attempt == max_retries:
                print(f"Dataproc unavailable after {max_retries} attempts: {e}")
                return 1
            wait = 60 * attempt  # 60s, 120s
            print(f"Dataproc unavailable (attempt {attempt}/{max_retries}), retrying in {wait}s: {e}")
            time.sleep(wait)
        except Aborted as e:
            # 409 Aborted = job ran but failed (missing file, code error, etc.) — not retriable
            print(f"Batch job failed: {e}")
            return 1


def _run_scripts(scripts: list, run_date: str) -> tuple[bool, str]:
    """Submit a list of Dataproc scripts sequentially. Returns (ok, error_msg)."""
    for script in scripts:
        print(f"Submitting {script}...")
        if submit_dataproc_job(script, run_date) != 0:
            return False, script
        print(f"✓ {script}")
    return True, ""


# ── scrapers ──────────────────────────────────────────────────────────────────

@functions_framework.http
def scraper_yfinance_run(request):
    run_date = str(date.today())
    prices   = fetch_prices(TICKERS, start=run_date, end=run_date)
    metadata = fetch_metadata(TICKERS)
    bucket   = os.getenv("GCS_BRONZE_BUCKET")

    upload_to_gcs(prices,   bucket, f"raw/prices/{run_date}.json")
    upload_to_gcs(metadata, bucket, f"raw/metadata/{run_date}.json")
    return f"Uploaded {len(prices)} prices, {len(metadata)} metadata", 200


@functions_framework.http
def scraper_worldbank_run(request):
    from ingestion.scraper_nbk import fetch as fetch_nbk
    run_date = str(date.today())
    data = fetch_all(COUNTRIES, INDICATORS)
    data += fetch_nbk(run_date)
    upload_to_gcs(data, os.getenv("GCS_BRONZE_BUCKET"), f"raw/worldbank/{run_date}.json")
    return f"Uploaded {len(data)} worldbank records", 200


@functions_framework.http
def scraper_news_run(request):
    run_date = str(date.today())
    bucket   = os.getenv("GCS_BRONZE_BUCKET")

    for source_name, cfg in NEWS_SOURCES.items():
        try:
            mod      = importlib.import_module(cfg["module"])
            articles = mod.fetch(run_date)
            upload_to_gcs(articles, bucket, f"{cfg['gcs_prefix']}/{run_date}.json")
            print(f"✓ {source_name}: {len(articles)} articles")
        except Exception as e:
            print(f"✗ {source_name}: {e}")

    return "News scrape complete", 200


@functions_framework.http
def scraper_kase_run(request):
    run_date = str(date.today())
    prices = fetch_kase_prices(
        tickers=KASE_TICKERS,
        from_date=run_date,
        to_date=str(date.today() + timedelta(days=1)),
    )
    upload_to_gcs(prices, os.getenv("GCS_BRONZE_BUCKET"), f"raw/kase_prices/{run_date}.json")
    return f"Uploaded {len(prices)} KASE prices", 200


# ── daily orchestration ───────────────────────────────────────────────────────

@functions_framework.http
def run_silver(request):
    run_date = (request.get_json(silent=True) or {}).get("date") or str(date.today())
    ok, failed = _run_scripts(SILVER_SCRIPTS, run_date)
    return ("Silver complete", 200) if ok else (f"FAILED: {failed}", 500)


@functions_framework.http
def run_gold(request):
    run_date = (request.get_json(silent=True) or {}).get("date") or str(date.today())
    ok, failed = _run_scripts(GOLD_SCRIPTS, run_date)
    return ("Gold complete", 200) if ok else (f"FAILED: {failed}", 500)


# ── backfill ──────────────────────────────────────────────────────────────────

@functions_framework.http
def backfill(request):
    """Bronze backfill: scrape full history into GCS bronze."""
    bucket     = os.getenv("GCS_BRONZE_BUCKET")
    START_YEAR = 2000
    log        = []

    # ── yfinance: find last trading day ──────────────────────────────────────
    def find_last_trading_day_yfinance() -> str:
        check = date.today() - timedelta(days=1)
        for _ in range(10):
            d = str(check)
            if fetch_prices(["AAPL"], start=d, end=d):
                return d
            check -= timedelta(days=1)
        return str(date.today() - timedelta(days=1))

    # ── kase: find last trading day ───────────────────────────────────────────
    def find_last_trading_day_kase() -> str:
        import requests as req
        from datetime import datetime
        check = date.today() - timedelta(days=1)
        for _ in range(10):
            d = str(check)
            from_ts = int(datetime.strptime(d, "%Y-%m-%d").timestamp())
            to_ts   = from_ts + 86400
            resp = req.get(
                "https://kase.kz/tv-charts/securities/history",
                params={"symbol": "ALL:HSBK", "resolution": "1D",
                        "from": from_ts, "to": to_ts},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            data = resp.json()
            if data.get("s") == "ok" and data.get("t"):
                return d
            check -= timedelta(days=1)
        return str(date.today() - timedelta(days=1))

    # ── yfinance year-by-year ─────────────────────────────────────────────────
    last_yf   = find_last_trading_day_yfinance()
    last_year = int(last_yf[:4])

    for year in range(START_YEAR, last_year + 1):
        start = f"{year}-01-01"
        end   = last_yf if year == last_year else f"{year}-12-31"
        prices = fetch_prices(TICKERS, start=start, end=end)
        if prices:
            upload_to_gcs(prices, bucket, f"raw/prices/backfill/{year}.json")
            log.append(f"✓ yfinance {year}: {len(prices)} rows")
        else:
            log.append(f"⚠ yfinance {year}: no data")
        print(log[-1])

    # ── kase all-time ─────────────────────────────────────────────────────────
    last_kase  = find_last_trading_day_kase()
    kase_prices = fetch_kase_prices(
        tickers=KASE_TICKERS, from_date="2000-01-01", to_date=last_kase
    )
    if kase_prices:
        upload_to_gcs(kase_prices, bucket, "raw/kase_prices/backfill/all.json")
        log.append(f"✓ kase backfill: {len(kase_prices)} rows up to {last_kase}")
    else:
        log.append("⚠ kase backfill: no data")
    print(log[-1])

    # ── worldbank ─────────────────────────────────────────────────────────────
    wb_data = fetch_all(COUNTRIES, INDICATORS)
    upload_to_gcs(wb_data, bucket, "raw/worldbank/backfill/all.json")
    log.append(f"✓ worldbank: {len(wb_data)} records")
    print(log[-1])

    return "\n".join(log), 200


@functions_framework.http
def run_silver_backfill(request):
    ok, failed = _run_scripts(SILVER_BACKFILL_SCRIPTS, str(date.today()))
    return ("Silver backfill complete", 200) if ok else (f"FAILED: {failed}", 500)


@functions_framework.http
def run_gold_backfill(request):
    from google.cloud import storage as gcs

    today        = str(date.today())
    silver_bucket = os.getenv("GCS_SILVER_BUCKET")
    client       = gcs.Client()

    def get_silver_dates(prefix: str) -> list:
        dates = set()
        for blob in client.bucket(silver_bucket).list_blobs(prefix=prefix):
            parts = blob.name.split("run_date=")
            if len(parts) > 1:
                dates.add(parts[1].split("/")[0])
        return sorted(dates)

    # ── one-shot gold jobs ────────────────────────────────────────────────────
    for script in [
        "transformation/gold_dim_date.py",
        "transformation/gold_dim_country.py",
        "transformation/gold_fact_macro.py",
    ]:
        print(f"Submitting {script}...")
        if submit_dataproc_job(script, today) != 0:
            return f"FAILED: {script}", 500
        print(f"✓ {script}")

    # ── dim_company: per metadata date in order (SCD2) ────────────────────────
    for run_date in get_silver_dates("metadata/"):
        if submit_dataproc_job("transformation/gold_dim_company.py", run_date) != 0:
            return f"FAILED: gold_dim_company {run_date}", 500
        print(f"✓ gold_dim_company {run_date}")

    # ── fact_prices and gold_news: wildcard mode ("ALL") ─────────────────────
    for script in [
        "transformation/gold_fact_prices.py",
        "transformation/gold_news.py",
    ]:
        print(f"Submitting {script} ALL...")
        if submit_dataproc_job(script, "ALL") != 0:
            return f"FAILED: {script} ALL", 500
        print(f"✓ {script} ALL")

    return "Gold backfill complete", 200


@functions_framework.http
def full_backfill(request):
    """End-to-end backfill: bronze → silver → gold in one call."""
    print("=== FULL BACKFILL: Bronze ===")
    result, status = backfill(request)
    if status != 200:
        return f"Bronze failed: {result}", 500

    print("=== FULL BACKFILL: Silver ===")
    result, status = run_silver_backfill(request)
    if status != 200:
        return f"Silver failed: {result}", 500

    print("=== FULL BACKFILL: Gold ===")
    result, status = run_gold_backfill(request)
    if status != 200:
        return f"Gold failed: {result}", 500

    return "Full backfill complete: bronze → silver → gold", 200
