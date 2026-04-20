import json
import os
from datetime import datetime, timezone

from fastapi import APIRouter, Query

import cache
from db import query, table

router = APIRouter(prefix="/summary", tags=["summary"])

GEMINI_TTL = 14400  # 4 h — aligned with news scraper cadence


@router.get("", summary="AI-generated market briefing (Gemini)")
def get_summary(lang: str = Query("en", description="Language: en | ru")):
    cache_key = f"summary:{lang}"
    cached = cache.get(cache_key, GEMINI_TTL)
    if cached is not None:
        return cached

    # Prioritise by non-neutral impact first, then weight descending
    sql = f"""
        SELECT title, source, impact, url
        FROM {table('news_articles')}
        WHERE DATE(pub_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY pub_date DESC) = 1
        ORDER BY
            CASE impact WHEN 'positive' THEN 2 WHEN 'negative' THEN 2 ELSE 1 END DESC,
            weight DESC,
            pub_date DESC
        LIMIT 35
    """
    articles = query(sql)

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key or not articles:
        result = _empty("No API key or no recent articles")
        return result

    try:
        from google import genai  # lazy import
        client_g = genai.Client(api_key=api_key)

        # Number articles so Gemini can reference them by index
        article_lines = "\n".join(
            f"[{i}] [{a['source']}] {a['title']} (impact: {a.get('impact') or 'neutral'})"
            for i, a in enumerate(articles)
        )
        lang_label = "Russian" if lang == "ru" else "English"

        prompt = f"""You are a financial analyst specialising in Kazakhstan and Central Asian markets.
Based on the numbered news articles below, write a concise market briefing in {lang_label}.

Articles:
{article_lines}

Respond with ONLY valid JSON, no markdown, no explanation:
{{
  "headline": "One sentence capturing the single most important development",
  "kz_bullets": [
    {{"text": "bullet text", "ref": <article index integer>}},
    ...
  ],
  "world_bullets": [
    {{"text": "bullet text", "ref": <article index integer>}},
    ...
  ]
}}

Rules:
- headline: ≤ 20 words, factual, no fluff
- kz_bullets: 3–4 items on Kazakhstan markets (KASE, KZT, NBK policy, domestic corporates)
- world_bullets: 2–3 items on global factors affecting KZ (oil, uranium, Russia/China/US)
- Each bullet is one sentence — name companies, numbers, directions when present
- ref must be the integer index of the single most relevant article for that bullet
- Do not invent figures not present in the articles
- If insufficient data for a bucket, use a single bullet: {{"text": "Insufficient recent data", "ref": 0}}
"""
        response = client_g.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt],
        )
        text = response.text.strip()

        if text.startswith("```"):
            parts = text.split("```")
            text = parts[1][4:].strip() if parts[1].startswith("json") else parts[1].strip()

        data = json.loads(text)

        def enrich(bullets: list) -> list:
            out = []
            for b in bullets:
                ref = b.get("ref")
                article = articles[ref] if isinstance(ref, int) and 0 <= ref < len(articles) else None
                out.append({
                    "text":   b.get("text", ""),
                    "url":    article["url"]    if article else "",
                    "source": article["source"] if article else "",
                })
            return out

        result = {
            "headline":      data.get("headline", ""),
            "kz_bullets":    enrich(data.get("kz_bullets", [])),
            "world_bullets": enrich(data.get("world_bullets", [])),
            "generated_at":  datetime.now(timezone.utc).isoformat(),
            "article_count": len(articles),
        }
    except Exception as exc:
        result = _empty(str(exc))

    cache.set(cache_key, result)
    return result


def _empty(reason: str = "") -> dict:
    return {
        "headline":      "",
        "kz_bullets":    [],
        "world_bullets": [],
        "generated_at":  "",
        "article_count": 0,
        "error":         reason,
    }
