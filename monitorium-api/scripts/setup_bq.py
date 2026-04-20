"""
One-time setup: creates dim_financial_reports and fact_financial_statements in BigQuery.
Run from monitorium-api/:
    python scripts/setup_bq.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from db import client, PROJECT, DATASET


TABLES = [
    {
        "table_id": "dim_financial_reports",
        "description": "Catalog of financial report PDFs available for KZ-listed companies",
        "schema": [
            bigquery.SchemaField("report_id",     "STRING",    mode="REQUIRED", description="SHA256 of ticker+fiscal_year+doc_type"),
            bigquery.SchemaField("ticker",         "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("fiscal_year",    "INTEGER",   mode="REQUIRED"),
            bigquery.SchemaField("doc_type",       "STRING",    mode="REQUIRED", description="annual_ifrs | quarterly_ifrs"),
            bigquery.SchemaField("quarter",        "INTEGER",   mode="NULLABLE", description="1-4, null for annual"),
            bigquery.SchemaField("source_url",     "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("gcs_path",       "STRING",    mode="NULLABLE", description="gs://... after download"),
            bigquery.SchemaField("filing_date",    "DATE",      mode="NULLABLE"),
            bigquery.SchemaField("language",       "STRING",    mode="NULLABLE", description="ru | en"),
            bigquery.SchemaField("extracted",      "BOOL",      mode="REQUIRED"),
            bigquery.SchemaField("indexed_at",     "TIMESTAMP", mode="REQUIRED"),
        ],
        "clustering_fields": ["ticker", "fiscal_year"],
    },
    {
        "table_id": "fact_financial_statements",
        "description": "Key financial metrics extracted from IFRS reports via Gemini",
        "schema": [
            bigquery.SchemaField("report_id",              "STRING",  mode="REQUIRED"),
            bigquery.SchemaField("ticker",                 "STRING",  mode="REQUIRED"),
            bigquery.SchemaField("fiscal_year",            "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("quarter",                "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("currency",               "STRING",  mode="NULLABLE"),
            bigquery.SchemaField("units",                  "STRING",  mode="NULLABLE", description="millions | thousands | units"),
            # Income Statement
            bigquery.SchemaField("revenue",                "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("gross_profit",           "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("operating_profit",       "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("ebitda",                 "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("net_income",             "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("eps",                    "FLOAT64", mode="NULLABLE"),
            # Balance Sheet
            bigquery.SchemaField("total_assets",           "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("total_equity",           "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("total_debt",             "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("cash_and_equivalents",   "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("current_assets",         "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("current_liabilities",    "FLOAT64", mode="NULLABLE"),
            # Cash Flow
            bigquery.SchemaField("operating_cash_flow",    "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("investing_cash_flow",    "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("financing_cash_flow",    "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("capex",                  "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("free_cash_flow",         "FLOAT64", mode="NULLABLE"),
            # Bank-specific
            bigquery.SchemaField("net_interest_income",    "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("loan_portfolio",         "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("deposits",               "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("npl_ratio",              "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("capital_adequacy_ratio", "FLOAT64", mode="NULLABLE"),
            # Meta
            bigquery.SchemaField("extracted_at",           "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("gemini_model",           "STRING",    mode="NULLABLE"),
        ],
        "clustering_fields": ["ticker", "fiscal_year"],
    },
    {
        "table_id": "fact_financial_flows",
        "description": "Income statement flows for Sankey/waterfall charts, one row per named line item",
        "schema": [
            bigquery.SchemaField("report_id",    "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("ticker",       "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("fiscal_year",  "INTEGER",   mode="REQUIRED"),
            bigquery.SchemaField("source_label", "STRING",    mode="REQUIRED", description="Upstream node label (exact text from report)"),
            bigquery.SchemaField("target_label", "STRING",    mode="REQUIRED", description="Downstream node label"),
            bigquery.SchemaField("value",        "FLOAT64",   mode="REQUIRED", description="Positive number; sign encoded in direction"),
            bigquery.SchemaField("currency",     "STRING",    mode="NULLABLE"),
            bigquery.SchemaField("units",        "STRING",    mode="NULLABLE"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
        ],
        "clustering_fields": ["ticker", "fiscal_year"],
    },
]


def main():
    bq = client()
    dataset_ref = f"{PROJECT}.{DATASET}"

    for spec in TABLES:
        table_ref = f"{dataset_ref}.{spec['table_id']}"
        table = bigquery.Table(table_ref, schema=spec["schema"])
        table.description = spec["description"]
        table.clustering_fields = spec["clustering_fields"]

        try:
            bq.create_table(table)
            print(f"Created {table_ref}")
        except Exception as e:
            if "Already Exists" in str(e):
                print(f"Already exists: {table_ref}")
            else:
                raise


if __name__ == "__main__":
    main()
