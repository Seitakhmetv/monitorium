import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# ── Price tickers ──────────────────────────────────────────────────────────────

TICKERS = os.getenv("TICKERS", "AAPL,MSFT,JPM").split(",")
KASE_TICKERS = os.getenv("KASE_TICKERS", "HSBK,KSPI,KCEL").split(",")

# ── World Bank ─────────────────────────────────────────────────────────────────

WORLDBANK_COUNTRIES = {
    # Existing
    "US": "United States",
    "GB": "United Kingdom",
    "DE": "Germany",
    "KZ": "Kazakhstan",
    "FR": "France",
    "JP": "Japan",
    # Central Asia / neighbors
    "RU": "Russia",
    "CN": "China",
    "TR": "Turkey",
    "AZ": "Azerbaijan",
    "UZ": "Uzbekistan",
    # Commodity economies
    "SA": "Saudi Arabia",
    "NG": "Nigeria",
    "NO": "Norway",
    # Major economies
    "IN": "India",
    "KR": "South Korea",
}

WORLDBANK_INDICATORS = {
    "gdp_growth":    "NY.GDP.MKTP.KD.ZG",
    "inflation_cpi": "FP.CPI.TOTL.ZG",
    "unemployment":  "SL.UEM.TOTL.ZS",
    "interest_rate": "FR.INR.RINR",
}

# ── News sources registry ──────────────────────────────────────────────────────
# To add a new source: create ingestion/scraper_<name>.py with fetch(run_date) -> list
# then add one entry here. Nothing else changes.

NEWS_SOURCES = {
    "news":      {"module": "ingestion.scraper_news",      "gcs_prefix": "raw/news"},
    "kapital":   {"module": "ingestion.scraper_kapital",   "gcs_prefix": "raw/kapital"},
    "kursiv":    {"module": "ingestion.scraper_kursiv",    "gcs_prefix": "raw/kursiv"},
    "kase_news": {"module": "ingestion.scraper_kase_news", "gcs_prefix": "raw/kase_news"},
    "adilet":    {"module": "ingestion.scraper_adilet",    "gcs_prefix": "raw/adilet"},
}

# ── Pipeline script lists (used by main.py orchestration) ─────────────────────

SILVER_SCRIPTS = [
    "transformation/silver_prices.py",
    "transformation/silver_metadata.py",
    "transformation/silver_worldbank.py",
    "transformation/silver_news.py",
]

GOLD_SCRIPTS = [
    "transformation/gold_dim_company.py",
    "transformation/gold_dim_country.py",
    "transformation/gold_fact_prices.py",
    "transformation/gold_fact_macro.py",
    "transformation/gold_news.py",
]

SILVER_BACKFILL_SCRIPTS = [
    "transformation/silver_prices_backfill.py",
    "transformation/silver_worldbank_backfill.py",
    "transformation/silver_metadata_backfill.py",
    "transformation/silver_news_backfill.py",
]

# ── Article tagging ────────────────────────────────────────────────────────────

COMPANY_TAGS = {
    # Halyk Bank
    "halyk": "HSBK", "народный банк": "HSBK", "halyk bank": "HSBK", "халык": "HSBK",
    # Kaspi
    "kaspi": "KSPI", "каспи": "KSPI", "kaspi.kz": "KSPI", "kaspi bank": "KSPI",
    # KazMunayGas
    "казмунайгаз": "KMGZ", "kazmunaygas": "KMGZ", "kmg": "KMGZ", "казмунай": "KMGZ",
    "kmgz": "KMGZ", "кмг": "KMGZ",
    # KazTransOil
    "казтрансойл": "KZTO", "kaztransoil": "KZTO", "kzt oil": "KZTO",
    # Kazakhtelecom
    "казахтелеком": "KZTK", "kazakhtelecom": "KZTK", "қазақтелеком": "KZTK",
    # Kcell
    "kcell": "KCEL", "кселл": "KCEL",
    # CenterCredit
    "centercredit": "CCBN", "центркредит": "CCBN", "банк центркредит": "CCBN",
    # ForteBank
    "fortebank": "ASBN", "форте банк": "ASBN", "forte bank": "ASBN",
    # Air Astana
    "air astana": "AIRA", "эйр астана": "AIRA", "air astana group": "AIRA",
    # KEGOC
    "kegoc": "KEGC", "кегок": "KEGC",
    # Kazatomprom
    "kazatomprom": "KZAP", "казатомпром": "KZAP", "uranium one": "KZAP",
    # Altynalmas
    "altynalmas": "ALMS", "алтыналмас": "ALMS",
    # Samruk-Kazyna (state holding — parent of KMG, KZTK, KEGC)
    "samruk": "SKZ", "самрук": "SKZ", "samruk-kazyna": "SKZ", "самрук-қазына": "SKZ",
    # Freedom Holding
    "freedom holding": "FRHC", "фридом холдинг": "FRHC", "freedom finance": "FRHC",
    # Eurasian Resources Group
    "eurasian resources": "ERG", "eurasian resources group": "ERG", "евразийская группа": "ERG",
    # Banka RBK
    "rbk": "BRBK", "банк rbk": "BRBK",
}

SECTOR_TAGS = {
    "banking":    ["банк", "bank", "кредит", "credit", "депозит", "deposit", "займ", "loan",
                   "ипотек", "mortgage", "микрофинанс", "microfinance", "npl", "нпл"],
    "oil_gas":    ["нефть", "oil", "газ", "gas", "мунай", "munay", "нефтяной", "brent",
                   "crude", "lng", "сжиженный газ", "pipeline", "нефтепровод", "нпз", "refinery"],
    "telecom":    ["телеком", "telecom", "связь", "mobile", "мобильн", "интернет", "5g",
                   "broadband", "fiber", "оптоволокн", "spectrum", "частот"],
    "mining":     ["золот", "gold", "уран", "uranium", "mining", "горнодобыв", "металл",
                   "медь", "copper", "zinc", "цинк", "silver", "серебр", "coal", "уголь",
                   "железн", "iron ore", "алюмин", "aluminium"],
    "finance":    ["биржа", "exchange", "акци", "stock", "инвест", "invest", "фонд", "fund",
                   "ценные бумаги", "securities", "облигац", "bond", "etf", "портфел"],
    "energy":     ["энергет", "energy", "электр", "electric", "kegoc", "кегок", "renewable",
                   "возобновляем", "solar", "солнечн", "wind", "ветер", "hydro", "гидро"],
    "transport":  ["авиа", "aviation", "airline", "аэропорт", "airport", "railway", "железная дорога",
                   "жд", "логистик", "logistics", "freight", "грузоперевоз"],
    "retail":     ["ритейл", "retail", "торговл", "ecommerce", "маркетплейс", "marketplace",
                   "supermarket", "супермаркет"],
    "real_estate":["недвижим", "real estate", "строительств", "construction", "жилищ", "housing",
                   "девелопер", "developer"],
    "legal":      ["закон", "law", "постановлен", "regulation", "кодекс", "норматив",
                   "указ", "decree", "поправк", "amendment", "минюст"],
    "macro":      ["ввп", "gdp", "инфляц", "inflation", "ставк", "rate", "бюджет", "budget",
                   "дефицит", "deficit", "профицит", "surplus", "счет текущих", "current account",
                   "резервы", "reserves", "девальваци", "devaluation", "тенге", "tenge"],
}

TOPIC_TAGS = {
    "earnings":      ["прибыль", "profit", "выручка", "revenue", "результат", "отчетность",
                      "чистый доход", "net income", "ebitda", "eps", "quarterly", "квартальн"],
    "regulation":    ["регулиров", "нацбанк", "nationalbank", "афрр", "министерств",
                      "афн", "агентство", "постановлен", "лицензи", "license"],
    "dividend":      ["дивиденд", "dividend", "выплат", "payout", "дивидендная политика"],
    "ipo":           ["ipo", "листинг", "listing", "размещен", "народное ipo", "spac", "debut"],
    "merger":        ["слияни", "merger", "поглощен", "acquisition", "takeover", "покупка активов",
                      "сделка m&a", "stake", "доля"],
    "sanction":      ["санкц", "sanction", "embargo", "эмбарго", "ограничен", "restriction"],
    "rate":          ["ставк", "rate", "инфляц", "inflation", "нацбанк", "base rate", "базовая ставка",
                      "monetary policy", "денежно-кредитная политика"],
    "rating":        ["рейтинг", "rating", "moody", "fitch", "s&p", "upgrade", "downgrade",
                      "outlook", "прогноз", "кредитный рейтинг"],
    "trading":       ["торги", "trading", "объем", "volume", "курс", "котировк", "kase", "aix"],
    "oil_price":     ["brent", "wti", "crude", "нефт", "opec", "опек", "barrel", "баррел"],
    "currency":      ["тенге", "tenge", "kzt", "usd/kzt", "валют", "forex", "курс доллара",
                      "девальваци", "devaluation", "exchange rate"],
    "debt":          ["облигац", "bond", "eurobond", "евробонд", "займ", "кредит", "debt",
                      "долг", "выпуск", "issuance", "погашен", "redemption"],
    "geopolitics":   ["война", "war", "конфликт", "conflict", "геополитик", "geopolit",
                      "нато", "nato", "сно", "sanction", "санкц", "дипломат"],
    "privatization": ["приватизац", "privatiz", "разгосударствлен", "продажа госдол",
                      "народное ipo", "samruk", "самрук"],
}

COUNTRY_TAGS = {
    # Kazakhstan
    "казахстан": "KZ", "kazakhstan": "KZ", "казахстанск": "KZ", "қазақстан": "KZ",
    # Russia
    "россия": "RU", "russia": "RU", "russian": "RU", "российск": "RU", "москва": "RU",
    # China
    "китай": "CN", "china": "CN", "chinese": "CN", "китайск": "CN", "beijing": "CN", "пекин": "CN",
    # USA
    "сша": "US", "united states": "US", "american": "US", "america": "US", "washington": "US",
    # EU
    "евросоюз": "EU", "europe": "EU", "european": "EU", "брюссел": "EU", "brussels": "EU",
    # UK
    "великобритания": "GB", "britain": "GB", "uk": "GB", "british": "GB", "london": "GB",
    # Germany
    "германия": "DE", "germany": "DE", "german": "DE",
    # Japan
    "япония": "JP", "japan": "JP", "japanese": "JP",
    # Central Asia
    "узбекистан": "UZ", "uzbekistan": "UZ", "uzbek": "UZ", "ташкент": "UZ", "tashkent": "UZ",
    "кыргызстан": "KG", "kyrgyzstan": "KG", "кирги": "KG",
    "таджикистан": "TJ", "tajikistan": "TJ",
    "туркменистан": "TM", "turkmenistan": "TM",
    "азербайджан": "AZ", "azerbaijan": "AZ",
    "грузия": "GE", "georgia": "GE",
    # Turkey
    "турция": "TR", "turkey": "TR", "turkish": "TR", "стамбул": "TR", "istanbul": "TR",
    # UAE / Gulf
    "оаэ": "AE", "uae": "AE", "dubai": "AE", "дубай": "AE", "abu dhabi": "AE",
    "саудовск": "SA", "saudi": "SA", "aramco": "SA",
    # India / Korea
    "индия": "IN", "india": "IN", "indian": "IN",
    "корея": "KR", "korea": "KR", "korean": "KR", "samsung": "KR",
    # Orgs
    "opec": "OPEC", "опек": "OPEC", "opec+": "OPEC",
    "imf": "IMF", "мвф": "IMF",
    "world bank": "WB", "всемирный банк": "WB",
}

POSITIVE_SIGNALS = [
    # Russian
    "рост", "рекорд", "прибыль", "повысил", "увеличен", "расширен", "улучшен",
    "позитив", "повышен", "рейтинг повышен", "выручка выросла", "прибыль выросла",
    "увеличил", "расширяет", "открывает", "запускает", "победил", "выиграл",
    "превысил", "достиг рекорда", "максимум", "сильные результаты",
    # English
    "profit", "growth", "record", "upgraded", "positive", "beats", "beat expectations",
    "wins", "secures", "secured", "acquired", "launches", "launched", "expands", "expanded",
    "raises", "raised", "invests", "invested", "partnership", "dividend", "buyback",
    "surpasses", "exceeds", "strong results", "record high", "all-time high",
    "new contract", "new deal", "deal", "ipo success", "oversubscribed", "rally", "rebound",
    "upgrade", "outperform", "buy rating", "price target raised",
    "rises", "climbs", "jumps", "surges", "soars",
    "purchase", "purchases", "buys", "orders", "signs contract", "awarded",
    "agreement", "approved", "green light", "clears", "listed",
]

NEGATIVE_SIGNALS = [
    # Russian
    "паден", "убыток", "снижен", "понизил", "негатив", "дефолт", "кризис",
    "штраф", "санкц", "убытки", "падение", "сокращен", "увольнен", "потер",
    "ухудшен", "ниже прогноза", "слабые результаты", "задолженност", "просрочк",
    "банкротств", "ликвидац", "арест", "обыск", "расследован", "мошенничеств",
    # English
    "loss", "decline", "downgrade", "sanction", "fine", "negative", "default", "crisis",
    "fails", "failed", "failure", "rejected", "blocked", "unable", "denies",
    "cuts", "cut", "layoffs", "lawsuit", "fraud", "investigation", "ban", "banned",
    "suspended", "halted", "warned", "missed", "below expectations", "disappoints",
    "recall", "debt restructuring", "bankruptcy", "liquidat", "seized", "arrested",
    "probe", "charges", "penalty", "writedown", "write-off", "impairment",
    "sell rating", "underperform", "price target cut", "profit warning",
    "falls", "fell", "drops", "dropped", "plunges", "slumps", "tumbles",
    "devaluation", "девальвац", "weakens", "depreciat",
    "on hold", "paused", "delayed", "postponed", "cancelled", "canceled",
    "exits", "withdraws", "pulls out", "scraps", "shelves",
]

HIGH_WEIGHT_SOURCES = {
    # Tier 3 — rating agencies + supranationals (highest authority)
    "moody": 3, "moody's": 3,
    "s&p": 3, "standard & poor": 3, "fitch": 3,
    "imf": 3, "мвф": 3, "world bank": 3, "всемирный банк": 3,
    "wef": 3, "world economic forum": 3,
    "oecd": 3, "оэср": 3, "ebrd": 3, "ебрр": 3, "adb": 3,
    # Tier 2 — major financial press + regulators
    "reuters": 2, "bloomberg": 2,
    "financial times": 2, "ft.com": 2,
    "wall street journal": 2, "wsj": 2,
    "нацбанк": 2, "nationalbank": 2, "nbk": 2,
    "министерств": 2, "ministry": 2,
    "правительств": 2, "government": 2, "үкімет": 2,
    "kase": 2, "казахстанская фондовая": 2, "aix": 2,
    "афрр": 2, "afsa": 2,
    "самрук": 2, "samruk": 2,
    "минфин": 2, "minfin": 2, "ministry of finance": 2,
    "миннацэкономики": 2, "ministry of national economy": 2,
    "kazmunaygas": 2, "казмунайгаз": 2,
    # Tier 2 — KZ financial media
    "kursiv": 2, "курсив": 2,
    "kapital": 2, "капитал": 2,
    "forbes kazakhstan": 2,
}
