"""
DAG: holidays_us_ny  |  Pattern: ETL  |  Schedule: @once
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generates US federal + New York state public holidays for 2025 and 2026
using the python-holidays library (no API key required).

E  extract_holidays    — generate raw holiday entries per year
T  transform_holidays  — classify federal vs state, merge, sort, deduplicate
L  load_holidays       — write holidays_us_ny.csv (full-refresh)

Output (dags/output/holidays/):
    holidays_us_ny.csv

Columns:
    date, name, year, holiday_type   (federal | state)
"""

from __future__ import annotations

import csv
import os

import pendulum
from airflow.sdk import dag, task
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

BQ_PROJECT_ID = 'is3107-491906'
BQ_DATASET_ID = 'citibike'
BQ_TABLE_ID   = 'holidays_us_ny'

CSV_FIELDNAMES = ["date", "name", "year", "holiday_type"]


@dag(
    dag_id="holidays_us_ny",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@yearly",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "IS3107"},
    tags=["holidays", "citibike", "reference", "etl"],
    doc_md=__doc__,
)
def holidays_us_ny():

    @task
    def extract_holidays(year: int) -> list[dict]:
        """
        EXTRACT — Generate raw holiday entries for one year.
        Compares against the base US federal calendar to classify
        each holiday as 'federal' or 'state'.
        """
        import holidays as hdays

        federal_dates = set(hdays.country_holidays("US", years=year).keys())
        ny_holidays   = hdays.country_holidays("US", subdiv="NY", years=year)

        raw = [
            {
                "date":         d.isoformat(),
                "name":         name,
                "year":         year,
                "holiday_type": "federal" if d in federal_dates else "state",
            }
            for d, name in ny_holidays.items()
        ]
        print(f"[{year}] Extracted {len(raw)} holidays")
        return raw

    @task
    def transform_holidays(all_years: list[list[dict]]) -> list[dict]:
        """
        TRANSFORM — Merge all years, sort by date, deduplicate by (date, name).
        """
        flat = [row for year_rows in all_years for row in year_rows]

        seen: set[tuple] = set()
        deduped: list[dict] = []
        for row in sorted(flat, key=lambda r: r["date"]):
            key = (row["date"], row["name"])
            if key not in seen:
                seen.add(key)
                deduped.append(row)

        print(f"Transformed {len(flat)} raw → {len(deduped)} deduplicated holidays")
        return deduped

    @task
    def load_holidays(rows: list[dict]) -> str:
        """
        LOAD — Write final holiday list to CSV (full-refresh).
        """
        output_dir = os.path.join(os.path.dirname(__file__), "output", "holidays")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "holidays_us_ny.csv")

        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Saved {len(rows)} holidays → {out_path}")
        return out_path

    @task
    def ingest_holidays(csv_path: str) -> None:
        """
        INGEST — Load holidays CSV directly to BigQuery with WRITE_TRUNCATE.
        No staging table, always do a complete full-refresh.
        """
        import pandas as pd

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        client = bigquery.Client(project=BQ_PROJECT_ID)
        prod_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"Read {len(df)} holidays from {csv_path}")

        dataset = bigquery.Dataset(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}")
        dataset.location = "asia-east1"
        client.create_dataset(dataset, exists_ok=True)

        try:
            client.get_table(prod_table_id)
        except NotFound:
            schema = [
                bigquery.SchemaField("date",         "DATE",    mode="REQUIRED"),
                bigquery.SchemaField("name",         "STRING",  mode="REQUIRED"),
                bigquery.SchemaField("year",         "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("holiday_type", "STRING",  mode="REQUIRED"),
            ]
            client.create_table(bigquery.Table(prod_table_id, schema=schema), exists_ok=True)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = client.load_table_from_dataframe(df, prod_table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded {len(df)} holidays to {prod_table_id}")

    # ── Task wiring ──────────────────────────────────────────────────────────
    raw_years = extract_holidays.expand(year=[2025, 2026])
    clean     = transform_holidays(raw_years)
    csv_path  = load_holidays(clean)
    ingest_holidays(csv_path)


holidays_us_ny()
