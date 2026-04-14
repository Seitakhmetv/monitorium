from fastapi import APIRouter, Query
from db import query, table
import cache

router = APIRouter(prefix="/macro", tags=["macro"])

TTL = 3600


@router.get("", summary="List macro indicators")
def get_macro(
    indicator: str | None = Query(None, description="Indicator name (e.g. gdp_growth, inflation_cpi, unemployment, interest_rate)", example="gdp_growth"),
    country: str | None = Query(None, description="2-letter country code (e.g. KZ, US, RU, CN)", example="KZ"),
    from_year: int | None = Query(None, alias="from", description="Start year", example=2015),
    to_year: int | None = Query(None, alias="to", description="End year", example=2024),
):
    key = f"macro:{indicator}:{country}:{from_year}:{to_year}"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    filters = []
    if indicator:
        filters.append(f"indicator_name = '{indicator}'")
    if country:
        filters.append(f"country_code = '{country.upper()}'")
    if from_year:
        filters.append(f"year >= {from_year}")
    if to_year:
        filters.append(f"year <= {to_year}")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT indicator_name, country_code, year, value
        FROM {table('fact_macro_indicators')}
        {where}
        ORDER BY year DESC, indicator_name
        LIMIT 2000
    """
    result = query(sql)
    cache.set(key, result)
    return result


@router.get("/indicators", summary="List all available indicator names")
def list_indicators():
    key = "macro:indicators"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    sql = f"SELECT DISTINCT indicator_name FROM {table('fact_macro_indicators')} ORDER BY indicator_name"
    result = [r["indicator_name"] for r in query(sql)]
    cache.set(key, result)
    return result
