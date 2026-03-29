from dotenv import load_dotenv
import yfinance as yf
import os
from datetime import date, timedelta
import pandas as pd
from ingestion.utils import upload_to_gcs

load_dotenv()

TICKERS = []
BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", date.today().isoformat())


def fetch_prices(tickers: list, start: str, end: str) -> list:
    """
    Pull OHLCV data for multiple tickers in one call.
    Return list of dicts (one row per ticker per day).
    """

    # 🔥 FIX: Yahoo end date is exclusive
    end_dt = str(date.fromisoformat(end) + timedelta(days=1))

    df = yf.download(
        tickers=tickers,
        start=start,
        end=end_dt,
        group_by="ticker",
        auto_adjust=False,
        threads=True
    )

    if df.empty:
        return []

    # ✅ Normalize dataframe
    if isinstance(df.columns, pd.MultiIndex):
        df = df.stack(level=0).reset_index()
        df = df.rename(columns={"level_1": "ticker"})
    else:
        df = df.reset_index()
        df["ticker"] = tickers[0]

    # ✅ Use global RUN_DATE (cleaner)
    df["run_date"] = RUN_DATE
    # ✅ Standardize column names
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Ticker": "ticker"
    })

    # ✅ Ensure all expected columns exist (prevents crashes)
    expected_cols = [
        "date", "ticker", "open", "high", "low",
        "close", "adj_close", "volume", "run_date"
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df = df[expected_cols]
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    print(df.head())
    return df.to_dict(orient="records")


def fetch_metadata(tickers: list) -> list:
    """
    Pull company metadata for each ticker.
    """
    results = []

    for t in tickers:
        try:
            info = yf.Ticker(t).info

            results.append({
                "symbol": info.get("symbol"),
                "shortName": info.get("shortName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "country": info.get("country"),
                "marketCap": info.get("marketCap"),
                "currency": info.get("currency"),
                "run_date": RUN_DATE
            })
        except Exception as e:
            print(f"Failed metadata for {t}: {e}")

    return results

if __name__ == "__main__":
    tickers = TICKERS or ["AAPL", "MSFT", "JPM"]

    prices = fetch_prices(tickers, start=RUN_DATE, end=RUN_DATE)
    metadata = fetch_metadata(tickers)

    upload_to_gcs(prices, BRONZE_BUCKET, f"raw/prices/{RUN_DATE}.json")
    upload_to_gcs(metadata, BRONZE_BUCKET, f"raw/metadata/{RUN_DATE}.json")

    print(f"Uploaded {len(prices)} price rows and {len(metadata)} metadata rows")