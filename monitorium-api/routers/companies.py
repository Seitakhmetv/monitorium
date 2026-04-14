from fastapi import APIRouter, HTTPException, Path
from db import query, table
import cache

router = APIRouter(prefix="/companies", tags=["companies"])

TTL = 3600


@router.get("", summary="List all current companies")
def get_companies():
    key = "companies:all"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    sql = f"""
        SELECT ticker, shortName, sector, industry, country, marketCap, currency, valid_from, valid_to
        FROM {table('dim_company')}
        WHERE is_current = TRUE
        ORDER BY ticker
    """
    result = query(sql)
    cache.set(key, result)
    return result


@router.get("/{ticker}", summary="Get company profile by ticker")
def get_company(
    ticker: str = Path(description="Ticker symbol (e.g. KSPI, HSBK, KMG, AAPL)", example="KSPI"),
):
    key = f"companies:{ticker}"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    sql = f"""
        SELECT ticker, shortName, sector, industry, country, marketCap, currency, valid_from, valid_to
        FROM {table('dim_company')}
        WHERE ticker = '{ticker.upper()}'
        ORDER BY valid_from DESC
        LIMIT 1
    """
    rows = query(sql)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Company {ticker} not found")
    result = rows[0]
    cache.set(key, result)
    return result
