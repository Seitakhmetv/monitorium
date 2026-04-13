# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Monitorium is a free, public, non-commercial financial intelligence platform for Kazakhstani markets — mini-Bloomberg but causality-first: "something moved — why?" Target audience: general public with mid-level financial literacy. Stack: GCP pipeline + FastAPI + React frontend (pipeline is complete; API/frontend are next).

Built entirely by one developer. Every decision optimizes for maintainability over cleverness. Zero revenue, zero budget — minimize infrastructure cost. Always flag cost impact of any suggestion.

## Commands

All commands run from `monitorium/` (the Python package root, not the repo root):

```bash
# Setup
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt && pip install -e .

# Run a scraper locally
python ingestion/scraper_yfinance.py

# Run a silver/gold transformation locally (requires GCP creds + .env)
python transformation/silver_prices.py 2026-04-11
python transformation/gold_fact_prices.py 2026-04-11

# Build wheel and upload to GCS (also uploads .env and requirements.txt)
./deploy.sh                                     # wheel only
./deploy.sh transformation/silver_prices.py    # wheel + specific script

# Deploy all Cloud Run functions
./deploy_functions.sh
# Deploy a single function
./deploy_functions.sh scraper-news

# Tests (minimal coverage, mostly manual/integration)
pytest
```

**DEV mode**: set `DEV=true` in `.env` to use local Spark and sample data instead of GCS.

## Architecture

Three-layer medallion pipeline: Bronze (raw) → Silver (cleaned Parquet) → Gold (BigQuery warehouse).

### Data flow

```
Cloud Scheduler → Cloud Run (HTTP triggers in main.py)
  ├── Scrapers upload NDJSON to GCS Bronze bucket
  └── Orchestrators submit Dataproc Spark batch jobs
           ↓
Dataproc reads Bronze, writes Silver as Parquet (partitioned by run_date)
           ↓
Dataproc reads Silver, writes Gold to BigQuery (monitorium_gold dataset)
```

### Key files

- `main.py` — all Cloud Functions entry points: scrapers, `run_silver`, `run_gold`, backfill variants, `full_backfill`. `submit_dataproc_job()` is the core orchestration primitive.
- `ingestion/config.py` — single source of truth for tickers, `NEWS_SOURCES` registry, `WORLDBANK_COUNTRIES`, pipeline script lists, and article tagging rules. **Most config changes happen here.**
- `ingestion/` — one scraper per source; each exports `fetch(run_date: str) -> list`
- `ingestion/tagger.py` — rule-based article tagger; marked for AI replacement
- `ingestion/utils.py` — `build_spark()`, `upload_to_gcs()`, `write_silver()`, `write_gold()` helpers. Spark imports are lazy (inside `build_spark`) so Cloud Run scrapers don't pay the pyspark import cost.
- `transformation/silver_*.py` — Spark jobs: read Bronze JSON, apply schema, deduplicate, write Parquet. Each exports its `clean_*()` function so backfill scripts can import it.
- `transformation/silver_*_backfill.py` — backfill variants; import cleaning logic from their daily counterpart, never duplicate it.
- `transformation/gold_*.py` — Spark jobs: join Silver, write BigQuery. Accept `"ALL"` as run_date to read all silver partitions at once (used by `run_gold_backfill`).
- `deploy.sh` — builds wheel versioned by git commit hash, uploads to `gs://monitorium-scripts/`

### BigQuery schema (monitorium_gold)

| Table | Key columns |
|---|---|
| `dim_company` | ticker, name, sector, industry, country, marketCap — SCD2 (valid_from/valid_to) |
| `dim_country` | country_code, name |
| `dim_date` | date_key, date, year, month, day_of_week |
| `fact_stock_prices` | date_key, ticker, open, high, low, close, volume, currency |
| `fact_macro_indicators` | indicator_name, value, date, source_url |
| `fact_news_articles` | article_id, source, title, url, pub_date, companies[], sectors[], topics[], impact, weight |

### Infrastructure constraints

- **Do not redesign the pipeline** without strong justification. It is working.
- Cloud Run scales to zero — no idle cost. Dataproc is serverless batch.
- No Cloud SQL — BigQuery handles all queries including operational ones.
- New API layer: FastAPI on Cloud Run, reads BigQuery, in-memory response caching to mask latency.
- Frontend: React + Vite on Firebase Hosting (free tier). No auth, no accounts — localStorage watchlist only.

## Adding a news source

1. Create `ingestion/scraper_<name>.py` with `def fetch(run_date: str) -> list`
2. Add one entry to `NEWS_SOURCES` in `ingestion/config.py`
3. Build + deploy. Nothing else changes — `main.py` and `silver_news.py` read the registry automatically.

## Adding a ticker

- yfinance/global: add to `TICKERS` in `.env`
- KASE: add to `KASE_TICKERS` in `.env`

## Pipeline rules

Before modifying any scraper or transformation:
1. Inspect existing table schemas and GCS structure first.
2. New scrapers must export `fetch(run_date: str) -> list`.
3. Schema changes require understanding all downstream impacts (Silver → Gold → BigQuery).
4. Idempotency is required: all jobs use partition overwrite and deduplication.
5. Gold scripts accept `"ALL"` as run_date for backfill (wildcard silver read + TRUNCATE write). `gold_dim_company.py` is the exception — it must always run per-date in order (SCD2).

## KZ market context

Key tickers: KMG (KazMunayGas, oil/gas), KZAP (Kazatomprom, uranium), KSPI (Kaspi.kz, fintech), HSBK (Halyk Bank), KZTK (Kazakhtelecom), KEGC (KEGOC, grid). Samruk-Kazyna is the state holding owning KMG, KZTK, KEGC. Macro drivers: Brent oil, uranium spot, USD/KZT, NBK base rate. Dual-listed stocks trade on both KASE (Almaty) and AIX (Astana).

## Planned phases

- **Phase 1 (now)**: API + frontend consumption layer on top of existing pipeline. New data sources: goszakup.gov.kz (contractor graph), stat.gov.kz/BNS, NBK, MinFin, Telegram public channels.
- **Phase 2**: Sector-level Lasso regression (nightly Cloud Run), AI article processing via Gemini API (ticker mapping, cause-chain extraction, legislation tagging), hh.kz job postings.
