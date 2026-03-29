# ingestion/scraper_worldbank.py

import requests
import json
import os
from datetime import date
from dotenv import load_dotenv
from monitorium.utils import upload_to_gcs 

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", date.today().isoformat())  # default to today if not set

COUNTRIES = ["US", "GB", "DE", "KZ", "FR", "JP"]

INDICATORS = {
    "gdp_growth":    "NY.GDP.MKTP.KD.ZG",
    "inflation_cpi": "FP.CPI.TOTL.ZG",
    "unemployment":  "SL.UEM.TOTL.ZS",
    "interest_rate": "FR.INR.RINR"
}


def fetch_indicator(country: str, indicator_code: str) -> list:
    """
    Fetch all pages for a single country + indicator.
    Add country and indicator_name to each record.
    """
    records = []
    page = 1
    per_page = 1000  # max World Bank allows

    while True:
        url = (
            f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}"
            f"?format=json&per_page={per_page}&page={page}"
        )
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f"Failed fetch {country}-{indicator_code}: {resp.status_code}")
            break

        data = resp.json()
        if not data or len(data) < 2:
            print(f"No data returned for {country}-{indicator_code}")
            break

        meta, data_records = data

        # add country + indicator_name to each record, handle None values
        for rec in data_records:
            records.append({
                "country": country,
                "indicator_code": indicator_code,
                "date": rec.get("date"),
                "value": rec.get("value"),
                "run_date": RUN_DATE
            })

        total_pages = meta.get("pages", 1)
        if page >= total_pages:
            break
        page += 1

    return records


def fetch_all(countries: list, indicators: dict) -> dict:
    """
    Loop over all country + indicator combinations.
    Return dict keyed by indicator name, value = list of all records.
    """
    all_data = {name: [] for name in indicators.keys()}

    for indicator_name, indicator_code in indicators.items():
        for country in countries:
            recs = fetch_indicator(country, indicator_code)
            all_data[indicator_name].extend(recs)
            print(f"Fetched {len(recs)} records for {country} / {indicator_name}")

    return all_data

def flatten_for_df(data: dict) -> list:
    """
    Flatten the nested dict into a list of records for DataFrame ingestion.
    Each record should have: country, indicator_name, date, value, run_date.
    Turn it to jsonl-serializable format (e.g. convert date to string if needed).
    """
    records = []
    for indicator_name, recs in data.items():
        for rec in recs:
            records.append({
                "country": rec["country"],
                "indicator_name": indicator_name,
                "date": rec["date"],
                "value": rec["value"],
                "run_date": rec["run_date"]
            })
    return records

if __name__ == "__main__":
    all_data = flatten_for_df(fetch_all(COUNTRIES, INDICATORS))
    print(all_data)
    upload_to_gcs(
            all_data,
            BRONZE_BUCKET,
            f"raw/worldbank/{RUN_DATE}.json"
        )