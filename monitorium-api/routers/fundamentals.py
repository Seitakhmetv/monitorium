from fastapi import APIRouter, HTTPException, Query
from db import query, table
import cache
try:
    from fundamentals_data import FUNDAMENTALS as _STATIC
except ImportError:
    _STATIC = {}

router = APIRouter(prefix="/fundamentals", tags=["fundamentals"])

TTL = 3600  # 1 h

KASE_TICKERS = [
    "HSBK", "KSPI", "KCEL", "KMGZ", "KZTK",
    "KEGC", "KZTO", "AIRA", "CCBN", "ASBN",
    "AKZM", "RAHT", "BAST", "KMGD", "BSUL", "KZAP",
]


@router.get("/tickers", summary="List of tickers with extracted financials")
def get_tickers():
    cache_key = "fundamentals:tickers"
    cached = cache.get(cache_key, TTL)
    if cached is not None:
        return cached
    try:
        rows = query(f"""
            SELECT DISTINCT ticker
            FROM {table('fact_financial_statements')}
            ORDER BY ticker
        """)
        result = [r["ticker"] for r in rows] or KASE_TICKERS
    except Exception:
        result = KASE_TICKERS
    cache.set(cache_key, result)
    return result


@router.get("/{ticker}/statements", summary="Time-series P&L + balance sheet KPIs")
def get_statements(ticker: str):
    key = ticker.upper()
    cache_key = f"fundamentals:statements:{key}"
    cached = cache.get(cache_key, TTL)
    if cached is not None:
        return cached

    try:
        rows = query(f"""
            SELECT
                fiscal_year, quarter, currency, units,
                revenue, gross_profit, operating_profit, ebitda, net_income, eps,
                total_assets, total_liabilities, total_equity, total_debt, cash_and_equivalents,
                current_assets, current_liabilities,
                operating_cash_flow, investing_cash_flow, financing_cash_flow,
                capex, free_cash_flow,
                net_interest_income, loan_portfolio, deposits,
                npl_ratio, capital_adequacy_ratio
            FROM {table('fact_financial_statements')}
            WHERE ticker = '{key}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY report_id ORDER BY extracted_at DESC) = 1
            ORDER BY fiscal_year DESC, quarter DESC NULLS LAST
        """)
    except Exception:
        rows = []

    if not rows:
        # Fall back to static data if BQ empty
        static = _STATIC.get(key)
        if static:
            result = {"ticker": key, "source": "static", "years": static.get("annual", [])}
        else:
            raise HTTPException(404, f"No financials data for {key}")
    else:
        result = {"ticker": key, "source": "bq", "years": rows}

    cache.set(cache_key, result)
    return result


@router.get("/{ticker}/flows/{year}", summary="Income statement Sankey links for a fiscal year")
def get_flows(ticker: str, year: int):
    key = ticker.upper()
    cache_key = f"fundamentals:flows:{key}:{year}"
    cached = cache.get(cache_key, TTL)
    if cached is not None:
        return cached

    try:
        rows = query(f"""
            SELECT source_label, target_label, value, currency, units
            FROM {table('fact_financial_flows')}
            WHERE ticker = '{key}' AND fiscal_year = {year}
            ORDER BY value DESC
        """)
    except Exception:
        rows = []

    if not rows:
        raise HTTPException(404, f"No flow data for {key} FY{year}")

    currency = rows[0]["currency"]
    units = rows[0]["units"]

    # Build nivo-compatible Sankey structure
    node_ids = list(dict.fromkeys(
        label for row in rows for label in (row["source_label"], row["target_label"])
    ))
    result = {
        "ticker":   key,
        "year":     year,
        "currency": currency,
        "units":    units,
        "nodes":    [{"id": n} for n in node_ids],
        "links":    [
            {
                "source": r["source_label"],
                "target": r["target_label"],
                "value":  r["value"],
            }
            for r in rows
        ],
    }

    cache.set(cache_key, result)
    return result


@router.get("/{ticker}/flows", summary="Available fiscal years with flow data")
def get_flows_years(ticker: str):
    key = ticker.upper()
    cache_key = f"fundamentals:flows:years:{key}"
    cached = cache.get(cache_key, TTL)
    if cached is not None:
        return cached
    try:
        rows = query(f"""
            SELECT DISTINCT fiscal_year
            FROM {table('fact_financial_flows')}
            WHERE ticker = '{key}'
            ORDER BY fiscal_year DESC
        """)
        result = [r["fiscal_year"] for r in rows]
    except Exception:
        result = []
    cache.set(cache_key, result)
    return result
