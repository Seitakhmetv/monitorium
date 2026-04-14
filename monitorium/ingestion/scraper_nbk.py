"""
NBK (National Bank of Kazakhstan) base rate scraper.

Fetches rate decisions from the NBK website and returns annual records
(year-end rate for each year) in the same format as the worldbank scraper
so they flow through silver_worldbank → gold_fact_macro unchanged.
"""
import re
import time
from datetime import date, datetime

import requests

BASE_URL = "https://nationalbank.kz/en/news/grafik-prinyatiya-resheniy-po-bazovoy-stavke/rubrics/{}"

# Rubric ID per year — add new year's rubric ID here each January
RUBRIC_IDS = {
    2015: 1548,
    2016: 1547,
    2017: 1546,
    2018: 1545,
    2019: 1544,
    2020: 1543,
    2021: 1581,
    2022: 1698,
    2023: 1843,
    2024: 2098,
    2025: 2237,
    2026: 2365,
}


def _parse_rubric(rubric_id: int) -> list[tuple[str, float]]:
    """Fetch one rubric page and return [(dd.mm.yyyy, rate), ...] sorted by date."""
    url = BASE_URL.format(rubric_id)
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  ⚠ rubric {rubric_id}: {e}")
        return []

    html = resp.text
    tables = re.findall(r"<table[^>]*>.*?</table>", html, re.S)
    if not tables:
        return []

    cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tables[0], re.S)
    cells = [re.sub(r"<[^>]+>", "", c).replace("&nbsp;", "").replace("\xa0", "").strip() for c in cells]
    data = [c for c in cells if c]

    decisions = []
    i = 1  # skip header row
    while i < len(data):
        if re.match(r"\d{2}\.\d{2}\.\d{4}$", data[i]):
            date_str = data[i]
            rate_str = data[i + 1].strip() if i + 1 < len(data) else ""
            if re.match(r"\d+[.,]\d+", rate_str):
                rate = float(rate_str.replace(",", "."))
                decisions.append((date_str, rate))
            i += 4
        else:
            i += 1

    return decisions


def fetch(run_date: str) -> list:
    """
    Return annual NBK base rate records (year-end rate) for all available years.
    Format matches worldbank bronze: country, indicator_code, indicator_name, date (year), value, run_date.
    """
    current_year = int(run_date[:4])
    records = []

    for year, rubric_id in sorted(RUBRIC_IDS.items()):
        if year > current_year:
            continue

        decisions = _parse_rubric(rubric_id)
        if not decisions:
            print(f"  ⚠ {year}: no data")
            continue

        # Parse and sort by date, take the last decision of the year
        dated = []
        for d_str, rate in decisions:
            try:
                dt = datetime.strptime(d_str, "%d.%m.%Y")
                if dt.year == year:
                    dated.append((dt, rate))
            except ValueError:
                pass

        if not dated:
            continue

        dated.sort(key=lambda x: x[0])
        _, year_end_rate = dated[-1]

        records.append({
            "country":        "KZ",
            "indicator_code": "NBK.BASE.RATE",
            "indicator_name": "interest_rate",
            "date":           str(year),
            "value":          year_end_rate,
            "run_date":       run_date,
        })
        print(f"  ✓ {year}: {year_end_rate}%")
        time.sleep(0.3)

    return records
