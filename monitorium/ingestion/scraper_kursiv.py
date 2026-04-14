import requests
from bs4 import BeautifulSoup
import hashlib
from datetime import datetime, date
import os
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", str(date.today()))
HEADERS = {"User-Agent": "Mozilla/5.0"}

KURSIV_SECTIONS = [
    "https://kz.kursiv.media/en/category/economics/",
    "https://kz.kursiv.media/en/category/economics/banks-finance/",
    "https://kz.kursiv.media/en/category/economics/investments/",
    "https://kz.kursiv.media/en/category/economics/resources/",
]


def parse_kursiv_date(date_str: str) -> str:
    """
    Parse 'April 9, 2026' → '2026-04-09T00:00:00+05:00'
    No time on listing page — default to midnight Almaty
    """
    try:
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y")
        return dt.strftime("%Y-%m-%dT00:00:00+05:00")
    except Exception:
        return date_str


def fetch_kursiv_page(url: str) -> list:
    """
    Fetch one listing page of Kursiv articles.
    Handles both the front-page (h2 date headers) and paginated layout
    (per-article .subcat-article__header__date span).
    """
    resp = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(resp.content, "html.parser")
    articles = []
    current_date = None

    # ── paginated layout: each article carries its own date span ─────────────
    subcat_articles = soup.select("article.subcat-article")
    if subcat_articles:
        for el in subcat_articles:
            date_span = el.select_one(".subcat-article__header__date span")
            if date_span:
                current_date = date_span.get_text(strip=True)

            title_el = el.select_one("h2 a, h3 a, .entry-title a")
            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            url_article = title_el.get("href", "")
            if not url_article or not title:
                continue

            pub_date = parse_kursiv_date(current_date) if current_date else RUN_DATE

            articles.append({
                "article_id":  hashlib.md5(url_article.encode()).hexdigest(),
                "source":      "kursiv",
                "source_type": "news",
                "title":       title,
                "url":         url_article,
                "pub_date":    pub_date,
                "language":    "en",
                "run_date":    RUN_DATE
            })
        return articles

    # ── front-page layout: h2 date headers between article blocks ─────────────
    for el in soup.select("h2, article, .post, .entry"):
        if el.name == "h2" and not el.select_one("a"):
            text = el.get_text(strip=True)
            try:
                datetime.strptime(text.strip(), "%B %d, %Y")
                current_date = text.strip()
            except Exception:
                pass
            continue

        title_el = el.select_one("h2 a, h3 a, .entry-title a")
        if not title_el:
            continue

        title = title_el.get_text(strip=True)
        url_article = title_el.get("href", "")
        if not url_article or not title:
            continue

        pub_date = parse_kursiv_date(current_date) if current_date else RUN_DATE

        articles.append({
            "article_id":  hashlib.md5(url_article.encode()).hexdigest(),
            "source":      "kursiv",
            "source_type": "news",
            "title":       title,
            "url":         url_article,
            "pub_date":    pub_date,
            "language":    "en",
            "run_date":    RUN_DATE
        })

    return articles


def fetch_kursiv(max_pages: int = 1) -> list:
    """Fetch up to max_pages pages from each section. Default = 1 (daily run)."""
    all_articles: dict = {}
    for section_url in KURSIV_SECTIONS:
        for page in range(1, max_pages + 1):
            url = section_url if page == 1 else f"{section_url}page/{page}/"
            try:
                articles = fetch_kursiv_page(url)
                if not articles:
                    break
                for a in articles:
                    all_articles[a["article_id"]] = a
                print(f"✓ kursiv {url}: {len(articles)} articles")
            except Exception as e:
                print(f"✗ kursiv {url}: {e}")
                break
    return list(all_articles.values())


def fetch(run_date: str) -> list:
    return fetch_kursiv(max_pages=1)


if __name__ == "__main__":
    articles = fetch(RUN_DATE)
    upload_to_gcs(articles, BRONZE_BUCKET, f"raw/kursiv/{RUN_DATE}.json")
    print(f"Uploaded {len(articles)} Kursiv articles")
