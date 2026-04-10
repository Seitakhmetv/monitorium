import requests
from bs4 import BeautifulSoup
import hashlib
from datetime import datetime, date
import os
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs

load_dotenv()

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


def fetch_kursiv_section(url: str) -> list:
    resp = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(resp.content, "html.parser")
    articles = []
    current_date = None

    for el in soup.select("h2, article, .post, .entry"):
        # date headers like "April 9, 2026"
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


def fetch_kursiv() -> list:
    all_articles = {}
    for section_url in KURSIV_SECTIONS:
        try:
            articles = fetch_kursiv_section(section_url)
            for a in articles:
                all_articles[a["article_id"]] = a
            print(f"✓ kursiv {section_url}: {len(articles)} articles")
        except Exception as e:
            print(f"✗ kursiv {section_url}: {e}")
    return list(all_articles.values())


if __name__ == "__main__":
    articles = fetch_kursiv()
    # print(articles)
    upload_to_gcs(articles, BRONZE_BUCKET, f"raw/kursiv/{RUN_DATE}.json")
    print(f"Uploaded {len(articles)} Kursiv articles")