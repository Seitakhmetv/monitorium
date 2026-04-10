# ingestion/config.py

import os
from dotenv import load_dotenv

load_dotenv()

TICKERS = os.getenv("TICKERS", "AAPL,MSFT,JPM").split(",")
KASE_TICKERS = os.getenv("KASE_TICKERS", "HSBK,KSPI,KCEL").split(",")

# ingestion/config.py additions

COMPANY_TAGS = {
    "halyk": "HSBK", "народный банк": "HSBK", "halyk bank": "HSBK",
    "kaspi": "KSPI", "каспи": "KSPI",
    "казмунайгаз": "KMGZ", "kazmunaygas": "KMGZ", "kmg": "KMGZ",
    "казтрансойл": "KZTO", "kaztransoil": "KZTO",
    "казахтелеком": "KZTK", "kazakhtelecom": "KZTK",
    "kcell": "KCEL", "kcell": "KCEL",
    "centercredit": "CCBN", "центркредит": "CCBN",
    "fortebank": "ASBN", "форте банк": "ASBN",
    "air astana": "AIRA", "эйр астана": "AIRA",
    "kegoc": "KEGC", "кегок": "KEGC",
    "kazatomprom": "KZAP", "казатомпром": "KZAP",
    "altynalmas": "ALMS", "алтыналмас": "ALMS",
}

SECTOR_TAGS = {
    "banking":  ["банк", "bank", "кредит", "credit", "депозит", "deposit", "займ", "loan"],
    "oil_gas":  ["нефть", "oil", "газ", "gas", "мунай", "munay", "нефтяной"],
    "telecom":  ["телеком", "telecom", "связь", "mobile", "мобильн", "интернет"],
    "mining":   ["золот", "gold", "уран", "uranium", "mining", "горнодобыв", "металл"],
    "finance":  ["биржа", "exchange", "акци", "stock", "инвест", "invest", "фонд", "fund"],
    "energy":   ["энергет", "energy", "электр", "electric", "kegoc", "кегок"],
    "legal":    ["закон", "law", "постановлен", "regulation", "кодекс", "норматив"],
    "macro":    ["ввп", "gdp", "инфляц", "inflation", "ставк", "rate", "бюджет", "budget"],
}

TOPIC_TAGS = {
    "earnings":    ["прибыль", "profit", "выручка", "revenue", "результат", "отчетность"],
    "regulation":  ["регулиров", "нацбанк", "nationalbank", "афрр", "министерств"],
    "dividend":    ["дивиденд", "dividend"],
    "ipo":         ["ipo", "листинг", "listing", "размещен"],
    "merger":      ["слияни", "merger", "поглощен", "acquisition"],
    "sanction":    ["санкц", "sanction"],
    "rate":        ["ставк", "rate", "инфляц", "inflation", "нацбанк"],
    "rating":      ["рейтинг", "rating", "moody", "fitch", "s&p"],
    "trading":     ["торги", "trading", "объем", "volume", "курс"],
}

COUNTRY_TAGS = {
    "казахстан": "KZ", "kazakhstan": "KZ", "казахстанск": "KZ",
    "россия": "RU", "russia": "RU", "russian": "RU", "российск": "RU",
    "китай": "CN", "china": "CN", "chinese": "CN", "китайск": "CN",
    "сша": "US", "united states": "US", "american": "US", "america": "US",
    "евросоюз": "EU", "europe": "EU", "european": "EU", "евро": "EU",
    "великобритания": "GB", "britain": "GB", "uk": "GB", "british": "GB",
    "германия": "DE", "germany": "DE", "german": "DE",
    "япония": "JP", "japan": "JP", "japanese": "JP",
    "узбекистан": "UZ", "uzbekistan": "UZ",
    "турция": "TR", "turkey": "TR", "turkish": "TR",
    "opec": "OPEC", "опек": "OPEC",
}

POSITIVE_SIGNALS = [
    "рост", "рекорд", "прибыль", "profit", "growth", "record",
    "повысил", "upgraded", "увеличен", "расширен", "улучшен",
    "позитив", "positive", "повышен", "рейтинг повышен"
]

NEGATIVE_SIGNALS = [
    "паден", "убыток", "loss", "снижен", "decline", "понизил",
    "downgrade", "санкц", "sanction", "штраф", "fine",
    "негатив", "negative", "дефолт", "default", "кризис", "crisis"
]

HIGH_WEIGHT_SOURCES = {
    "moody": 3, "moody's": 3,
    "s&p": 3, "standard & poor": 3, "fitch": 3,
    "imf": 3, "мвф": 3, "world bank": 3, "всемирный банк": 3,
    "wef": 3, "world economic forum": 3,
    "oecd": 3, "оэср": 3,
    "reuters": 2, "bloomberg": 2,
    "financial times": 2, "ft.com": 2,
    "wall street journal": 2, "wsj": 2,
    "нацбанк": 2, "nationalbank": 2,
    "министерств": 2, "ministry": 2,
    "правительств": 2, "government": 2,
    "kase": 2, "kaзахстанская фондовая": 2,
    "афрр": 2, "afsa": 2,
}