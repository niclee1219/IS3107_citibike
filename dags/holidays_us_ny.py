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

    # ── Task wiring ──────────────────────────────────────────────────────────
    raw_years = extract_holidays.expand(year=[2025, 2026])
    clean     = transform_holidays(raw_years)
    load_holidays(clean)


holidays_us_ny()
