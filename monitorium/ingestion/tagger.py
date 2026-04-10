from ingestion.config import (
    COMPANY_TAGS, SECTOR_TAGS, TOPIC_TAGS,
    COUNTRY_TAGS, POSITIVE_SIGNALS, NEGATIVE_SIGNALS,
    HIGH_WEIGHT_SOURCES
)


def tag_article(title: str, source: str, source_type: str) -> dict:
    """
    Rule-based tagger. Returns structured tags for a single article.
    Designed to be swapped with AI later without changing output schema.
    """
    text = title.lower() if title else ""

    companies = list({
        ticker for keyword, ticker in COMPANY_TAGS.items()
        if keyword in text
    })

    sectors = list({
        sector for sector, keywords in SECTOR_TAGS.items()
        if any(kw in text for kw in keywords)
    })

    countries = list({
        code for keyword, code in COUNTRY_TAGS.items()
        if keyword in text
    })

    topics = list({
        topic for topic, keywords in TOPIC_TAGS.items()
        if any(kw in text for kw in keywords)
    })

    pos = any(s in text for s in POSITIVE_SIGNALS)
    neg = any(s in text for s in NEGATIVE_SIGNALS)
    if pos and neg:
        impact = "mixed"
    elif pos:
        impact = "positive"
    elif neg:
        impact = "negative"
    else:
        impact = "unknown"

    combined = text + " " + source.lower()
    weight = 1
    for keyword, w in HIGH_WEIGHT_SOURCES.items():
        if keyword in combined:
            weight = max(weight, w)

    if source_type == "legal":
        weight = max(weight, 2)

    if companies:
        category = "company"
    elif sectors:
        category = "sector"
    elif countries:
        category = "country"
    else:
        category = "general"

    return {
        "companies": ",".join(sorted(set(companies))) if companies else "",
        "sectors":   ",".join(sorted(set(sectors))) if sectors else "",
        "countries": ",".join(sorted(set(countries))) if countries else "",
        "topics":    ",".join(sorted(set(topics))) if topics else "",
        "impact":    impact,
        "weight":    weight,
        "is_legal":  source_type == "legal",
        "category":  category,
    }