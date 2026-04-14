import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

PROJECT = os.getenv("GCP_PROJECT_ID", "monitorium-491507")
DATASET = os.getenv("BQ_DATASET", "monitorium_gold")

_client: bigquery.Client | None = None


def client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT)
    return _client


def table(name: str) -> str:
    return f"`{PROJECT}.{DATASET}.{name}`"


def query(sql: str) -> list[dict]:
    rows = client().query(sql).result()
    return [dict(row) for row in rows]
