"""
Telethon-based scraper for KZ financial Telegram channels.
fetch(run_date) is synchronous; async work is in _fetch_async().

Required env vars: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION_STRING
Generate the session string once with: python ingestion/gen_telegram_session.py
"""

import asyncio
import hashlib
import os
import time
from datetime import date, timezone, timedelta

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

from ingestion.config import TELEGRAM_CHANNELS

load_dotenv(dotenv_path=".env")

ALMATY_TZ = timezone(timedelta(hours=5))
API_ID    = int(os.environ["TELEGRAM_API_ID"])
API_HASH  = os.environ["TELEGRAM_API_HASH"]
SESSION   = os.environ["TELEGRAM_SESSION_STRING"]


def _detect_language(text: str) -> str:
    """Heuristic: >20% Cyrillic chars → Russian."""
    if not text:
        return "ru"
    cyrillic = sum(1 for c in text if '\u0400' <= c <= '\u04ff')
    return "ru" if cyrillic / len(text) > 0.2 else "en"


async def _fetch_async(run_date: str) -> list:
    backfill = (run_date == "ALL")
    if not backfill:
        target_date = date.fromisoformat(run_date)

    results: dict[str, dict] = {}  # keyed by article_id for dedup

    client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
    await client.start()

    try:
        for channel in TELEGRAM_CHANNELS:
            try:
                msgs = await client.get_messages(
                    channel,
                    limit=200 if backfill else 100,
                )
                count = 0
                for msg in msgs:
                    text = msg.message or ""
                    if not text.strip():
                        continue
                    # Skip pure forwards with no added caption
                    if msg.fwd_from and not text.strip():
                        continue

                    msg_dt = msg.date.astimezone(ALMATY_TZ)
                    if not backfill and msg_dt.date() != target_date:
                        continue

                    url = f"https://t.me/{channel}/{msg.id}"
                    aid = hashlib.md5(url.encode()).hexdigest()
                    results[aid] = {
                        "article_id":  aid,
                        "source":      channel,
                        "source_type": "news",
                        "title":       text[:200],
                        "url":         url,
                        "pub_date":    msg_dt.strftime("%Y-%m-%dT%H:%M:%S+05:00"),
                        "language":    _detect_language(text),
                        "description": text[:500],
                        "run_date":    run_date,
                    }
                    count += 1

                print(f"✓ telegram/{channel}: {count} articles")

            except Exception as e:
                print(f"✗ telegram/{channel}: {e}")

            time.sleep(1.5)  # avoid Telegram flood limits

    finally:
        await client.disconnect()

    return list(results.values())


def fetch(run_date: str) -> list:
    return asyncio.run(_fetch_async(run_date))


if __name__ == "__main__":
    import os
    from datetime import date as _date
    from ingestion.utils import upload_to_gcs

    run_date = str(_date.today())
    articles = fetch(run_date)
    bucket = os.getenv("GCS_BRONZE_BUCKET")
    if bucket:
        upload_to_gcs(articles, bucket, f"raw/telegram/{run_date}.json")
        print(f"Uploaded {len(articles)} Telegram articles to GCS")
    else:
        print(f"Fetched {len(articles)} articles (GCS_BRONZE_BUCKET not set, skipping upload)")
