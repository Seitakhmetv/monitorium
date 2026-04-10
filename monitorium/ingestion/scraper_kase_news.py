import requests
from bs4 import BeautifulSoup
import hashlib
from datetime import datetime, date, timedelta, timezone
import os
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", str(date.today()))
HEADERS = {"User-Agent": "Mozilla/5.0"}
ALMATY_TZ = timezone(timedelta(hours=5))


def get_almaty_today() -> str:
    return datetime.now(ALMATY_TZ).strftime("%Y-%m-%d")


def parse_kase_date(date_str: str) -> str:
    """
    Parse '09.04.26 15:03' → '2026-04-09T15:03:00+05:00'
    KASE timestamps are in Almaty time (UTC+5)
    """
    try:
        dt = datetime.strptime(date_str.strip(), "%d.%m.%y %H:%M")
        dt = dt.replace(year=dt.year if dt.year > 2000 else dt.year + 2000)
        return dt.strftime("%Y-%m-%dT%H:%M:%S+05:00")
    except Exception:
        return date_str


def fetch_kase_news(start_date: str, end_date: str) -> list:
    url = f"https://kase.kz/ru/information/news/all?startDate={start_date}&endDate={end_date}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(resp.content, "html.parser")
    articles = []

    # find all news links — each article is a link inside the news feed
    for link in soup.select("a[href*='/ru/information/news/show/']"):
        title = link.get_text(strip=True)
        href = link.get("href", "")
        article_url = f"https://kase.kz{href}" if href.startswith("/") else href

        # date is in the preceding text node or sibling
        parent = link.parent
        parent_text = parent.get_text(separator=" ", strip=True) if parent else ""

        # extract date pattern DD.MM.YY HH:MM from surrounding text
        import re
        date_match = re.search(r'\d{2}\.\d{2}\.\d{2}\s+\d{2}:\d{2}', parent_text)
        pub_date = parse_kase_date(date_match.group()) if date_match else RUN_DATE

        if not title or not article_url:
            continue

        articles.append({
            "article_id":  hashlib.md5(article_url.encode()).hexdigest(),
            "source":      "kase_news",
            "source_type": "news",
            "title":       title,
            "url":         article_url,
            "pub_date":    pub_date,
            "language":    "ru",
            "run_date":    RUN_DATE
        })

    return articles

if __name__ == "__main__":
    today = get_almaty_today()
    buffer_start = (datetime.now(ALMATY_TZ) - timedelta(days=3)).strftime("%Y-%m-%d")

    articles = fetch_kase_news(start_date=buffer_start, end_date=today)
    # deduplicate
    seen = {}
    for a in articles:
        seen[a["article_id"]] = a

    # print(seen.values())
    upload_to_gcs(
        list(seen.values()),
        BRONZE_BUCKET,
        f"raw/kase_news/{RUN_DATE}.json"
    )
    print(f"Uploaded {len(seen)} KASE news articles")