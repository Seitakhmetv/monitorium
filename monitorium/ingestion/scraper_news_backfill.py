"""
Bronze news backfill — run locally or as a Cloud Run function.

Backfills one full year for sources that support historical queries:
  - kase_news  : date-range URL, loops month by month
  - adilet     : month-by-month archive pages

Sources that only serve current content (kursiv, kapital, news/Yahoo) are skipped.
"""

import os
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs
from ingestion.scraper_kase_news import fetch_kase_news
from ingestion.scraper_adilet import fetch_adilet

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
BACKFILL_MONTHS = 12  # how many months back to go


def backfill_kase_news(start_date: str, end_date: str) -> int:
    """
    Fetch all KASE news between start_date and end_date, split by pub_date,
    upload one GCS file per date under raw/kase_news/{date}.json.
    """
    import re
    from collections import defaultdict

    print(f"kase_news: fetching {start_date} → {end_date}")

    # Fetch month-by-month to avoid overloading a single request
    total = 0
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    while current <= end:
        month_end = min(
            (current.replace(day=1) + relativedelta(months=1) - timedelta(days=1)),
            end
        )
        start_str = current.strftime("%Y-%m-%d")
        end_str   = month_end.strftime("%Y-%m-%d")

        try:
            articles = fetch_kase_news(start_str, end_str)
            if not articles:
                print(f"  ⚠ kase_news {start_str}…{end_str}: no articles")
                current = month_end + timedelta(days=1)
                continue

            # Group by pub_date (extract date part only)
            by_date = defaultdict(list)
            for a in articles:
                pub = a.get("pub_date", "")
                # extract YYYY-MM-DD from ISO timestamp
                day = pub[:10] if len(pub) >= 10 else start_str
                # Only keep articles in the requested window
                if start_str <= day <= end_str:
                    by_date[day].append(a)

            for day, day_articles in sorted(by_date.items()):
                upload_to_gcs(day_articles, BRONZE_BUCKET, f"raw/kase_news/{day}.json")
                print(f"  ✓ kase_news {day}: {len(day_articles)} articles")
                total += len(day_articles)

        except Exception as e:
            print(f"  ✗ kase_news {start_str}…{end_str}: {e}")

        current = month_end + timedelta(days=1)

    return total


def backfill_adilet(start_year_month: str, end_year_month: str) -> int:
    """
    Fetch Adilet legal docs for each year-month, upload as raw/adilet/{YYYY-MM-01}.json.
    year_month format: 'YYYY-MM'
    """
    from datetime import datetime

    total = 0
    current = datetime.strptime(start_year_month, "%Y-%m").date()
    end     = datetime.strptime(end_year_month,   "%Y-%m").date()

    while current <= end:
        ym = current.strftime("%Y-%m")
        try:
            docs = fetch_adilet(ym)
            if docs:
                # name the file by first of month so silver_news_backfill picks it up
                gcs_date = current.strftime("%Y-%m-%d")
                upload_to_gcs(docs, BRONZE_BUCKET, f"raw/adilet/{gcs_date}.json")
                print(f"  ✓ adilet {ym}: {len(docs)} docs → raw/adilet/{gcs_date}.json")
                total += len(docs)
            else:
                print(f"  ⚠ adilet {ym}: no docs")
        except Exception as e:
            print(f"  ✗ adilet {ym}: {e}")

        current = (current.replace(day=1) + relativedelta(months=1))

    return total


def run(months_back: int = BACKFILL_MONTHS):
    today = date.today()
    start = today - relativedelta(months=months_back)

    start_date       = start.strftime("%Y-%m-%d")
    end_date         = today.strftime("%Y-%m-%d")
    start_year_month = start.strftime("%Y-%m")
    end_year_month   = today.strftime("%Y-%m")

    print(f"\n=== News Bronze Backfill: {start_date} → {end_date} ===\n")

    n_kase  = backfill_kase_news(start_date, end_date)
    n_adilet = backfill_adilet(start_year_month, end_year_month)

    print(f"\nDone. kase_news: {n_kase} articles | adilet: {n_adilet} docs")


if __name__ == "__main__":
    import sys
    months = int(sys.argv[1]) if len(sys.argv) > 1 else BACKFILL_MONTHS
    run(months)
