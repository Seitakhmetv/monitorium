import requests
from bs4 import BeautifulSoup
import hashlib
import re
from datetime import datetime, date, timedelta
import os
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", str(date.today()))
HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://adilet.zan.kz"

RU_MONTHS = {
    "января": "01", "февраля": "02", "марта": "03",
    "апреля": "04", "мая": "05", "июня": "06",
    "июля": "07", "августа": "08", "сентября": "09",
    "октября": "10", "ноября": "11", "декабря": "12"
}


def parse_adilet_date(text: str) -> str:
    """
    Parse 'от 6 апреля 2026 года' → '2026-04-06'
    """
    try:
        match = re.search(r'от\s+(\d{1,2})\s+(\w+)\s+(\d{4})', text.lower())
        if match:
            day = match.group(1).zfill(2)
            month = RU_MONTHS.get(match.group(2), "01")
            year = match.group(3)
            return f"{year}-{month}-{day}T00:00:00+05:00"
    except Exception:
        pass
    return RUN_DATE


def get_doc_type(text: str) -> str:
    """Extract document type from description."""
    text_lower = text.lower()
    if "постановление" in text_lower:
        return "постановление"
    if "приказ" in text_lower:
        return "приказ"
    if "закон" in text_lower:
        return "закон"
    if "кодекс" in text_lower:
        return "кодекс"
    return "акт"


def fetch_adilet(year_month: str) -> list:
    """
    Fetch legal documents for a given year-month.
    year_month format: '2026-04'
    """
    url = f"{BASE_URL}/rus/index/docs/dt={year_month}"
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    soup = BeautifulSoup(resp.content, "html.parser")
    docs = []

    for item in soup.select("div.post_holder"):
        title_el = item.select_one("h4.post_header a")
        desc_el = item.select_one("p")

        if not title_el:
            continue

        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        doc_url = f"{BASE_URL}{href}" if href.startswith("/") else href
        description = desc_el.get_text(strip=True) if desc_el else ""
        pub_date = parse_adilet_date(description)
        doc_type = get_doc_type(description)
        is_new = bool(item.select_one("span.status_new"))

        docs.append({
            "article_id":  hashlib.md5(doc_url.encode()).hexdigest(),
            "source":      "adilet",
            "source_type": "legal",
            "title":       title,
            "url":         doc_url,
            "description": description,  # keep full description for context
            "pub_date":    pub_date,
            "doc_type":    doc_type,
            "is_new":      is_new,
            "language":    "ru",
            "run_date":    RUN_DATE
        })

    return docs


def fetch(run_date: str) -> list:
    dt = date.fromisoformat(run_date)
    months = [
        dt.strftime("%Y-%m"),
        (dt.replace(day=1) - timedelta(days=1)).strftime("%Y-%m"),
    ]
    all_docs = {}
    for ym in months:
        for d in fetch_adilet(ym):
            all_docs[d["article_id"]] = d
        print(f"✓ adilet {ym}: fetched")
    return list(all_docs.values())


if __name__ == "__main__":
    docs = fetch(RUN_DATE)
    upload_to_gcs(docs, BRONZE_BUCKET, f"raw/adilet/{RUN_DATE}.json")
    print(f"Uploaded {len(docs)} Adilet documents")