from fastapi import APIRouter, HTTPException, Path
from db import query, table
import cache

router = APIRouter(prefix="/causality", tags=["causality"])

TTL = 300


@router.get("/{ticker}/{date}", summary="Causality view: price + news + macro for a ticker on a date")
def get_causality(
    ticker: str = Path(description="Ticker symbol (e.g. KSPI, KMG, HSBK)", example="KSPI"),
    date: str = Path(description="Date (YYYY-MM-DD). Returns price movement vs prior day, nearby news, and macro for that year.", example="2024-04-13"),
):
    key = f"causality:{ticker}:{date}"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    t = ticker.upper()
    year = date[:4]

    price_sql = f"""
        SELECT d.date, p.open, p.high, p.low, p.close, p.volume, p.currency,
               LAG(p.close) OVER (ORDER BY d.date) AS prev_close
        FROM {table('fact_stock_prices')} p
        JOIN {table('dim_date')} d USING (date_key)
        WHERE p.ticker = '{t}'
          AND d.date BETWEEN DATE_SUB('{date}', INTERVAL 5 DAY) AND '{date}'
        ORDER BY d.date DESC
        LIMIT 2
    """

    news_sql = f"""
        SELECT article_id, source, title, url, pub_date, topics, impact, weight
        FROM {table('news_articles')}
        WHERE companies LIKE '%{t}%'
          AND DATE(pub_date) BETWEEN DATE_SUB('{date}', INTERVAL 3 DAY) AND DATE_ADD('{date}', INTERVAL 3 DAY)
        ORDER BY pub_date DESC
        LIMIT 20
    """

    macro_sql = f"""
        SELECT indicator_name, country_code, year, value
        FROM {table('fact_macro_indicators')}
        WHERE year = {year}
        ORDER BY indicator_name, country_code
    """

    price_rows = query(price_sql)
    if not price_rows:
        raise HTTPException(status_code=404, detail=f"No price data for {t} on {date}")

    current = price_rows[0]
    prev_close = price_rows[1]["close"] if len(price_rows) > 1 else None
    change_pct = None
    if prev_close and prev_close != 0:
        change_pct = round((current["close"] - prev_close) / prev_close * 100, 2)

    result = {
        "ticker": t,
        "date": date,
        "price": {
            "open":       current["open"],
            "high":       current["high"],
            "low":        current["low"],
            "close":      current["close"],
            "prev_close": prev_close,
            "change_pct": change_pct,
            "currency":   current["currency"],
        },
        "news":  query(news_sql),
        "macro": query(macro_sql),
    }

    cache.set(key, result)
    return result
