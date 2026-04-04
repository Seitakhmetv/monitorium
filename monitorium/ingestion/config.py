# ingestion/config.py

import os
from dotenv import load_dotenv

load_dotenv()

TICKERS = os.getenv("TICKERS", "AAPL,MSFT,JPM").split(",")