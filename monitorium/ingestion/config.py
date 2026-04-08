# ingestion/config.py

import os
from dotenv import load_dotenv

load_dotenv()

TICKERS = os.getenv("TICKERS", "AAPL,MSFT,JPM").split(",")
KASE_TICKERS = os.getenv("KASE_TICKERS", "HSBK,KSPI,KCEL").split(",")