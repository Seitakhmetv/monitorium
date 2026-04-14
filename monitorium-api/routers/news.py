from fastapi import APIRouter, Query
from db import query, table
import cache

router = APIRouter(prefix="/news", tags=["news"])

TTL = 300


@router.get("", summary="List news articles")
def get_news(
    ticker: str | None = Query(None, description="Filter by ticker mentioned in article (e.g. KSPI, KMG)", example="KSPI"),
    topic: str | None = Query(None, description="Filter by topic tag (e.g. oil_price, currency, privatization)", example="oil_price"),
    source: str | None = Query(None, description="Filter by source (news, kapital, kursiv, kase_news, adilet)", example="kursiv"),
    from_date: str | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)", example="2024-01-01"),
    to_date: str | None = Query(None, alias="to", description="End date (YYYY-MM-DD)", example="2024-12-31"),
    limit: int = Query(50, ge=1, le=200, description="Number of articles to return (max 200)"),
):
    key = f"news:{ticker}:{topic}:{source}:{from_date}:{to_date}:{limit}"
    cached = cache.get(key, TTL)
    if cached is not None:
        return cached

    filters = []
    if ticker:
        filters.append(f"companies LIKE '%{ticker.upper()}%'")
    if topic:
        filters.append(f"topics LIKE '%{topic.lower()}%'")
    if source:
        filters.append(f"source = '{source.lower()}'")
    if from_date:
        filters.append(f"DATE(pub_date) >= '{from_date}'")
    if to_date:
        filters.append(f"DATE(pub_date) <= '{to_date}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"""
        SELECT article_id, source, title, url, pub_date, companies, sectors, topics, impact, weight
        FROM {table('news_articles')}
        {where}
        ORDER BY pub_date DESC
        LIMIT {min(limit, 200)}
    """
    result = query(sql)
    cache.set(key, result)
    return result
