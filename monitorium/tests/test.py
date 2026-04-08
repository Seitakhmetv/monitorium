import requests
from datetime import datetime

def test_kase_api(ticker="HSBK"):
    # from 2000-01-01 to today
    from_ts = int(datetime(2026, 4, 4).timestamp())
    to_ts   = int(datetime.today().timestamp())

    url = "https://kase.kz/tv-charts/securities/history"
    params = {
        "symbol": f"ALL:{ticker}",
        "resolution": "1D",
        "from": from_ts,
        "to": to_ts,
        "countback": 9999,  # max bars
        "chart_language_code": "ru"
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    resp = requests.get(url, params=params, headers=headers)
    print(f"Status: {resp.status_code}")
    print(resp.json())

test_kase_api("HSBK")