import requests
import time
import os
from datetime import date
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs
from ingestion.config import WORLDBANK_COUNTRIES, WORLDBANK_INDICATORS

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", date.today().isoformat())

COUNTRIES = list(WORLDBANK_COUNTRIES.keys())
INDICATORS = WORLDBANK_INDICATORS


def fetch_indicator(country: str, indicator_code: str) -> list:
    records = []
    page = 1
    per_page = 1000

    while True:
        url = (
            f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}"
            f"?format=json&per_page={per_page}&page={page}"
        )
        for attempt in range(3):
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                break
            print(f"  {country}/{indicator_code} attempt {attempt+1}: {resp.status_code} — retrying...")
            time.sleep(2 ** attempt)  # 1s, 2s, 4s
        if resp.status_code != 200:
            print(f"Failed fetch {country}-{indicator_code}: {resp.status_code}")
            break

        data = resp.json()
        if not data or len(data) < 2:
            break

        meta, data_records = data
        for rec in data_records:
            records.append({
                "country":        country,
                "indicator_code": indicator_code,
                "date":           rec.get("date"),
                "value":          rec.get("value"),
                "run_date":       RUN_DATE,
            })

        if page >= meta.get("pages", 1):
            break
        page += 1

    return records


def fetch_all(countries: list, indicators: dict) -> list:
    """Fetch all country+indicator combinations. Returns flat list of records."""
    records = []
    for indicator_name, indicator_code in indicators.items():
        for country in countries:
            recs = fetch_indicator(country, indicator_code)
            for r in recs:
                r["indicator_name"] = indicator_name
            records.extend(recs)
            print(f"✓ {country}/{indicator_name}: {len(recs)} records")
            time.sleep(0.5)  # be polite to the API
    return records


if __name__ == "__main__":
    data = fetch_all(COUNTRIES, INDICATORS)
    upload_to_gcs(data, BRONZE_BUCKET, f"raw/worldbank/{RUN_DATE}.json")
    print(f"Uploaded {len(data)} worldbank records")
