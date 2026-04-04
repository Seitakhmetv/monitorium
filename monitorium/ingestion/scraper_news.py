import requests
from bs4 import BeautifulSoup
import json
import os
import hashlib
from datetime import date
from google.cloud import storage
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs
from ingestion.config import TICKERS
import time

load_dotenv()

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
RUN_DATE = os.getenv("RUN_DATE", date.today().isoformat())

headers = {
    "User-Agent": "Mozilla/5.0"
}


def fetch_news(ticker: str) -> list:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"

    for attempt in range(3):  # retry 3 times
        try:
            resp = requests.get(url, timeout=10, headers=headers)

            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"429 for {ticker}, retrying in {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                print(f"Failed RSS fetch for {ticker}: {resp.status_code}")
                return []

            # ✅ success → parse
            soup = BeautifulSoup(resp.content, "xml")
            items = soup.find_all("item")

            articles = []

            for item in items:
                link = item.link.text if item.link else None
                if not link:
                    continue

                article_id = hashlib.md5(link.encode("utf-8")).hexdigest()

                articles.append({
                    "article_id": article_id,
                    "ticker": ticker,
                    "title": item.title.text if item.title else None,
                    "link": link,
                    "pub_date": item.pubDate.text if item.pubDate else None,
                    "description": item.description.text if item.description else None,
                    "run_date": RUN_DATE
                })

            return articles

        except Exception as e:
            print(f"Error fetching news for {ticker}: {e}")

    return []

def deduplicate(articles: list) -> list:
    """
    Remove duplicate articles by article_id.
    """
    seen = set()
    deduped = []

    for article in articles:
        aid = article.get("article_id")

        if aid and aid not in seen:
            seen.add(aid)
            deduped.append(article)

    return deduped


if __name__ == "__main__":
    
    tickers = TICKERS

    all_articles = []

    for ticker in tickers:
        articles = fetch_news(ticker)
        all_articles.extend(articles)
        print(f"{ticker}: {len(articles)} articles fetched")

    deduped = deduplicate(all_articles)

    print(f"After dedup: {len(deduped)} unique articles")

    upload_to_gcs(
        deduped,
        BRONZE_BUCKET,
        f"raw/news/{RUN_DATE}.json"
    )