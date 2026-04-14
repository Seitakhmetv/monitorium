import requests
import time
import os
from datetime import date
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs
from ingestion.config import WORLDBANK_COUNTRIES, WORLDBANK_INDICATORS

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", date.today().isoformat())

COUNTRIES = list(WORLDBANK_COUNTRIES.keys())
INDICATORS = WORLDBANK_INDICATORS


def fetch_indicator(countries: list, indicator_code: str) -> list:
    """Fetch one indicator for all countries in a single batched API call."""
    records = []
    country_str = ";".join(countries)
    page = 1
    per_page = 1000

    while True:
        url = (
            f"https://api.worldbank.org/v2/country/{country_str}/indicator/{indicator_code}"
            f"?format=json&per_page={per_page}&page={page}"
        )
        for attempt in range(3):
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                break
            print(f"  {indicator_code} attempt {attempt+1}: {resp.status_code} — retrying...")
            time.sleep(2 ** attempt)
        if resp.status_code != 200:
            print(f"Failed fetch {indicator_code}: {resp.status_code}")
            break

        data = resp.json()
        if not data or len(data) < 2:
            break

        meta, data_records = data
        for rec in (data_records or []):
            records.append({
                "country":        rec.get("country", {}).get("id", "") if isinstance(rec.get("country"), dict) else rec.get("countryiso3code", ""),
                "indicator_code": indicator_code,
                "date":           rec.get("date"),
                "value":          rec.get("value"),
                "run_date":       RUN_DATE,
            })

        if page >= meta.get("pages", 1):
            break
        page += 1
        time.sleep(0.3)

    return records


def fetch_all(countries: list, indicators: dict) -> list:
    """Fetch all indicators for all countries. Batches all countries per indicator call."""
    records = []
    for indicator_name, indicator_code in indicators.items():
        recs = fetch_indicator(countries, indicator_code)
        for r in recs:
            r["indicator_name"] = indicator_name
        records.extend(recs)
        print(f"✓ {indicator_name}: {len(recs)} records across {len(countries)} countries")
        time.sleep(0.5)
    return records


if __name__ == "__main__":
    from ingestion.scraper_nbk import fetch as fetch_nbk
    data = fetch_all(COUNTRIES, INDICATORS)
    print("Fetching NBK base rate...")
    data += fetch_nbk(RUN_DATE)
    upload_to_gcs(data, BRONZE_BUCKET, f"raw/worldbank/{RUN_DATE}.json")
    print(f"Uploaded {len(data)} worldbank records")
