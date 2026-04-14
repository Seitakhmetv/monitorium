from fastapi import APIRouter, HTTPException, Path, Query
from db import query, table
import cache

router = APIRouter(prefix="/prices", tags=["prices"])

TTL = 300  # 5 min


@router.get("", summary="List stock prices")
def get_prices(
    ticker: str | None = Query(None, description="Ticker symbol (e.g. KSPI, AAPL, KMG)", example="KSPI"),
    from_date: str | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)", example="2024-01-01"),
    to_date: str | None = Query(None, alias="to", description="End date (YYYY-MM-DD)", example="2024-12-31"),
):
    key = f"prices:{ticker}:{from_date}:{to_date}"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    filters = []
    if ticker:
        filters.append(f"p.ticker = '{ticker.upper()}'")
    if from_date:
        filters.append(f"d.date >= '{from_date}'")
    if to_date:
        filters.append(f"d.date <= '{to_date}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT d.date, p.ticker, p.open, p.high, p.low, p.close, p.volume, p.currency
        FROM {table('fact_stock_prices')} p
        JOIN {table('dim_date')} d USING (date_key)
        {where}
        ORDER BY d.date DESC
        LIMIT 2000
    """
    result = query(sql)
    cache.set(key, result)
    return result


@router.get("/{ticker}/latest", summary="Latest price for a ticker")
def get_latest(
    ticker: str = Path(description="Ticker symbol (e.g. KSPI, HSBK, KMG)", example="KSPI"),
):
    key = f"prices:latest:{ticker}"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    sql = f"""
        SELECT d.date, p.ticker, p.open, p.high, p.low, p.close, p.volume, p.currency
        FROM {table('fact_stock_prices')} p
        JOIN {table('dim_date')} d USING (date_key)
        WHERE p.ticker = '{ticker.upper()}'
        ORDER BY d.date DESC
        LIMIT 1
    """
    rows = query(sql)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {ticker}")
    result = rows[0]
    cache.set(key, result)
    return result
