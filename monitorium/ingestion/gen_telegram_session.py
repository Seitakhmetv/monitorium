"""
Run this ONCE locally to generate a Telethon StringSession string.
Paste the output into .env as TELEGRAM_SESSION_STRING.

Prerequisites:
  1. Get API_ID and API_HASH from https://my.telegram.org → API development tools
  2. Add to .env:  TELEGRAM_API_ID=...  and  TELEGRAM_API_HASH=...
  3. Run:  python ingestion/gen_telegram_session.py
  4. Follow the phone + 2FA prompts
  5. Copy the printed string into .env as TELEGRAM_SESSION_STRING
"""

import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv(dotenv_path=".env")


async def main():
    api_id   = int(os.environ["TELEGRAM_API_ID"])
    api_hash = os.environ["TELEGRAM_API_HASH"]

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()

    session_string = client.session.save()
    print("\n" + "=" * 60)
    print("COPY THIS INTO .env as TELEGRAM_SESSION_STRING:")
    print("=" * 60)
    print(session_string)
    print("=" * 60 + "\n")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
