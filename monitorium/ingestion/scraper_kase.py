import requests
from datetime import datetime, date, timedelta
import os
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs
from ingestion.config import KASE_TICKERS

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", str(date.today()))
BASE_URL = "https://kase.kz/tv-charts/securities/history"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def fetch_kase_ticker(ticker: str, from_ts: int, to_ts: int) -> list:
    """
    Fetch OHLCV daily bars for a single KASE ticker.
    Returns list of dicts — one row per trading day.
    """
    params = {
        "symbol": f"ALL:{ticker}",
        "resolution": "1D",
        "from": from_ts,
        "to": to_ts,
        "countback": 9999,
        "chart_language_code": "ru"
    }

    resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("s") != "ok" or not data.get("t"):
        print(f"⚠ {ticker}: no data")
        return []

    records = []
    for i, ts in enumerate(data["t"]):
        records.append({
            "ticker":   ticker,
            "date":     str(datetime.utcfromtimestamp(ts).date()),
            "open":     data["o"][i],
            "high":     data["h"][i],
            "low":      data["l"][i],
            "close":    data["c"][i],
            "volume":   data["v"][i],
            "currency": "KZT",
            "exchange": "KASE",
            "run_date": RUN_DATE
        })
    return records


def fetch_kase_prices(tickers: list, from_date: str, to_date: str) -> list:
    """
    Fetch prices for all tickers in date range.
    """
    from_ts = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp())
    to_ts   = int(datetime.strptime(to_date,   "%Y-%m-%d").timestamp())

    all_records = []
    for ticker in tickers:
        try:
            records = fetch_kase_ticker(ticker, from_ts, to_ts)
            all_records.extend(records)
            print(f"✓ {ticker}: {len(records)} rows")
        except Exception as e:
            print(f"✗ {ticker}: {e}")

    return all_records


if __name__ == "__main__":
    # daily — just today
    prices = fetch_kase_prices(KASE_TICKERS, from_date=RUN_DATE, to_date=str(date.today()+timedelta(days=1)))
    upload_to_gcs(prices, BRONZE_BUCKET, f"raw/kase_prices/{RUN_DATE}.json")
    print(f"Uploaded {len(prices)} KASE price rows for {RUN_DATE}")