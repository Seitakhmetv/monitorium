import requests
from bs4 import BeautifulSoup
import hashlib
from datetime import datetime, date, timedelta
import os
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", str(date.today()))
BUFFER_DAYS = 3
BASE_URL = "https://kapital.kz/news/period/today"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def parse_kapital_date(date_str: str) -> str:
    """
    Parse '09.04.2026 · 15:37' → '2026-04-09T15:37:00+05:00'
    Kapital is in Almaty time (UTC+5)
    """
    try:
        clean = date_str.replace("·", "").strip()
        dt = datetime.strptime(clean, "%d.%m.%Y %H:%M")
        return dt.strftime("%Y-%m-%dT%H:%M:%S+05:00")
    except Exception:
        return date_str


def fetch_kapital_page(page: int) -> list:
    """
    Fetch one page of today's news from Kapital.
    Returns list of article dicts.
    """
    resp = requests.get(
        f"{BASE_URL}?page={page}",
        headers=HEADERS,
        timeout=10
    )
    soup = BeautifulSoup(resp.content, "html.parser")
    articles = []

    for item in soup.select("article, .news-item, .article-card"):
        title_el = item.select_one("h2, h3, .title, a[href]")
        link_el = item.select_one("a[href]")
        date_el = item.select_one(".date, time, .news-date, span")

        if not title_el or not link_el:
            continue

        title = title_el.get_text(strip=True)
        url = link_el.get("href", "")
        if url and not url.startswith("http"):
            url = "https://kapital.kz" + url
        date_text = date_el.get_text(strip=True) if date_el else ""
        pub_date = parse_kapital_date(date_text)

        if not title or not url:
            continue

        articles.append({
            "article_id":  hashlib.md5(url.encode()).hexdigest(),
            "source":      "kapital",
            "source_type": "news",
            "title":       title,
            "url":         url,
            "pub_date":    pub_date,
            "language":    "ru",
            "run_date":    RUN_DATE
        })

    return articles


def fetch_kapital(max_pages: int = 5) -> list:
    all_articles = {}
    for page in range(1, max_pages + 1):
        try:
            articles = fetch_kapital_page(page)
            if not articles:
                break
            for a in articles:
                all_articles[a["article_id"]] = a
            print(f"✓ kapital page {page}: {len(articles)} articles")
        except Exception as e:
            print(f"✗ kapital page {page}: {e}")
            break
    return list(all_articles.values())


def fetch(run_date: str) -> list:
    return fetch_kapital()


if __name__ == "__main__":
    articles = fetch(RUN_DATE)
    upload_to_gcs(articles, BRONZE_BUCKET, f"raw/kapital/{RUN_DATE}.json")
    print(f"Uploaded {len(articles)} Kapital articles")