"""
Bronze news backfill — run locally.

Backfills historical articles for sources that support it:
  - kase_news  : date-range URL, month by month
  - adilet     : month-by-month archive pages
  - kursiv     : paginated category pages (24 articles/page, ~55 pages back to 2020)
  - news       : Yahoo Finance RSS — run once to capture current ~30 headlines per ticker
  - kapital    : front-page pagination only (JS-rendered archive, no date support)

Usage:
  python -m ingestion.scraper_news_backfill          # last 12 months + kursiv/yahoo
  python -m ingestion.scraper_news_backfill 24       # last 24 months for date-based sources
"""

import os
from collections import defaultdict
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from ingestion.utils import upload_to_gcs
from ingestion.scraper_kase_news import fetch_kase_news
from ingestion.scraper_adilet import fetch_adilet
from ingestion.scraper_kursiv import fetch_kursiv
from ingestion.scraper_news import fetch as fetch_yahoo
from ingestion.scraper_kapital import fetch_kapital

load_dotenv(dotenv_path=".env")

BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET")
BACKFILL_MONTHS = 12


# ── kase_news ─────────────────────────────────────────────────────────────────

def backfill_kase_news(start_date: str, end_date: str) -> int:
    print(f"\nkase_news: fetching {start_date} → {end_date}")
    total = 0
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    while current <= end:
        month_end = min(
            current.replace(day=1) + relativedelta(months=1) - timedelta(days=1),
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

            by_date = defaultdict(list)
            for a in articles:
                pub = a.get("pub_date", "")
                day = pub[:10] if len(pub) >= 10 else start_str
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


# ── adilet ────────────────────────────────────────────────────────────────────

def backfill_adilet(start_year_month: str, end_year_month: str) -> int:
    from datetime import datetime
    print(f"\nadilet: {start_year_month} → {end_year_month}")
    total = 0
    current = datetime.strptime(start_year_month, "%Y-%m").date()
    end     = datetime.strptime(end_year_month,   "%Y-%m").date()

    while current <= end:
        ym = current.strftime("%Y-%m")
        try:
            docs = fetch_adilet(ym)
            if docs:
                gcs_date = current.strftime("%Y-%m-%d")
                upload_to_gcs(docs, BRONZE_BUCKET, f"raw/adilet/{gcs_date}.json")
                print(f"  ✓ adilet {ym}: {len(docs)} docs")
                total += len(docs)
            else:
                print(f"  ⚠ adilet {ym}: no docs")
        except Exception as e:
            print(f"  ✗ adilet {ym}: {e}")
        current = current.replace(day=1) + relativedelta(months=1)

    return total


# ── kursiv ────────────────────────────────────────────────────────────────────

def backfill_kursiv(start_date: str, max_pages: int = 55) -> int:
    """
    Paginate kursiv category sections.
    Groups articles by pub_date and uploads one GCS file per date.
    Stops early if all articles on a page are older than start_date.
    """
    from ingestion.scraper_kursiv import KURSIV_SECTIONS, fetch_kursiv_page

    print(f"\nkursiv: paginating up to {max_pages} pages per section (back to {start_date})")

    by_date: dict = defaultdict(dict)  # date → {article_id: article}
    cutoff = date.fromisoformat(start_date)

    for section_url in KURSIV_SECTIONS:
        for page in range(1, max_pages + 1):
            url = section_url if page == 1 else f"{section_url}page/{page}/"
            try:
                articles = fetch_kursiv_page(url)
                if not articles:
                    break

                older_count = 0
                for a in articles:
                    pub = a.get("pub_date", "")
                    day = pub[:10] if len(pub) >= 10 else str(date.today())
                    a["run_date"] = day  # use pub_date as run_date for backfill
                    by_date[day][a["article_id"]] = a
                    if day < start_date:
                        older_count += 1

                print(f"  ✓ kursiv {url}: {len(articles)} articles")

                # Stop paginating if more than half are older than cutoff
                if older_count > len(articles) // 2:
                    break

            except Exception as e:
                print(f"  ✗ kursiv {url}: {e}")
                break

    # Upload per-date
    total = 0
    for day, art_map in sorted(by_date.items()):
        arts = list(art_map.values())
        upload_to_gcs(arts, BRONZE_BUCKET, f"raw/kursiv/{day}.json")
        print(f"  → uploaded kursiv {day}: {len(arts)} articles")
        total += len(arts)

    return total


# ── Yahoo Finance (current snapshot) ─────────────────────────────────────────

def backfill_yahoo(run_date: str) -> int:
    """
    Yahoo Finance RSS only has ~30 recent articles per ticker.
    Run once and upload as today's file.
    """
    print(f"\nyahoo finance (RSS snapshot for {run_date})")
    try:
        articles = fetch_yahoo(run_date)
        if articles:
            upload_to_gcs(articles, BRONZE_BUCKET, f"raw/news/{run_date}.json")
            print(f"  ✓ yahoo finance: {len(articles)} articles")
            return len(articles)
        else:
            print("  ⚠ yahoo finance: no articles")
            return 0
    except Exception as e:
        print(f"  ✗ yahoo finance: {e}")
        return 0


# ── Kapital (current pages) ───────────────────────────────────────────────────

def backfill_kapital(run_date: str, max_pages: int = 10) -> int:
    """
    Kapital.kz archive is JS-rendered for history — only current-page pagination works.
    Fetch more pages of today's content.
    """
    print(f"\nkapital (scraping {max_pages} pages of current content)")
    try:
        articles = fetch_kapital(max_pages=max_pages)
        if articles:
            upload_to_gcs(articles, BRONZE_BUCKET, f"raw/kapital/{run_date}.json")
            print(f"  ✓ kapital: {len(articles)} articles ({max_pages} pages)")
            return len(articles)
        else:
            print("  ⚠ kapital: no articles")
            return 0
    except Exception as e:
        print(f"  ✗ kapital: {e}")
        return 0


# ── main ──────────────────────────────────────────────────────────────────────

def run(months_back: int = BACKFILL_MONTHS):
    today = date.today()
    start = today - relativedelta(months=months_back)

    start_date       = start.strftime("%Y-%m-%d")
    end_date         = today.strftime("%Y-%m-%d")
    start_year_month = start.strftime("%Y-%m")
    end_year_month   = today.strftime("%Y-%m")

    print(f"\n=== News Bronze Backfill: {start_date} → {end_date} ===")

    results = {}
    results["kase_news"]  = backfill_kase_news(start_date, end_date)
    results["adilet"]     = backfill_adilet(start_year_month, end_year_month)
    results["kursiv"]     = backfill_kursiv(start_date)
    results["yahoo"]      = backfill_yahoo(end_date)
    results["kapital"]    = backfill_kapital(end_date, max_pages=10)

    print("\n=== Summary ===")
    for source, count in results.items():
        print(f"  {source}: {count}")
    print(f"  total: {sum(results.values())}")


if __name__ == "__main__":
    import sys
    months = int(sys.argv[1]) if len(sys.argv) > 1 else BACKFILL_MONTHS
    run(months)
