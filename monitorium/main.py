# main.py (project root)

import functions_framework
from ingestion.scraper_yfinance import fetch_prices, fetch_metadata
from ingestion.scraper_worldbank import fetch_all, INDICATORS, COUNTRIES
from ingestion.scraper_news import fetch_news, deduplicate
from ingestion.utils import upload_to_gcs
from datetime import date
import os


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