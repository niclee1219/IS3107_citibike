"""
DAG: citibike_stations  |  Pattern: ETL  |  Schedule: @weekly
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fetches Citibike station reference data from the GBFS API and
stages it as a CSV for BigQuery ingestion. Full-refresh each run.

E  extract_stations    — GET station_information.json from GBFS
T  transform_stations  — keep short_name / name / lat / lon, drop invalid
L  load_stations       — write stations.csv (full-refresh)

Source:
    https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json

Output (dags/output/citibike/):
    stations.csv

Columns:
    short_name, name, lat, lon
"""

from __future__ import annotations

import csv
import os

import pendulum
from airflow.sdk import dag, task

CSV_FIELDNAMES = ["short_name", "name", "lat", "lon"]


@dag(
    dag_id="citibike_stations",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "IS3107"},
    tags=["citibike", "stations", "reference", "etl"],
    doc_md=__doc__,
)
def citibike_stations():

    @task
    def extract_stations() -> list[dict]:
        """
        EXTRACT — Fetch raw station records from the Citibike GBFS feed.
        """
        import requests

        response = requests.get(
            "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json",
            timeout=30,
        )
        response.raise_for_status()
        raw = response.json()["data"]["stations"]
        print(f"Fetched {len(raw)} raw station records")
        return raw

    @task
    def transform_stations(raw_stations: list[dict]) -> list[dict]:
        """
        TRANSFORM — Keep only short_name / name / lat / lon.
        short_name is the join key to start_station_id / end_station_id in trips.
        """
        rows = [
            {"short_name": s["short_name"], "name": s["name"],
             "lat": s["lat"], "lon": s["lon"]}
            for s in raw_stations
            if s.get("short_name")
        ]
        print(f"Retained {len(rows)} stations after filtering")
        return rows

    @task
    def load_stations(rows: list[dict]) -> str:
        """
        LOAD — Write station reference data to CSV. Full-refresh on every run.
        """
        output_dir = os.path.join(os.path.dirname(__file__), "output", "citibike_stations")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "stations.csv")

        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Saved {len(rows)} stations → {out_path}")
        return out_path

    # ── Task wiring ──────────────────────────────────────────────────────────
    raw   = extract_stations()
    clean = transform_stations(raw)
    load_stations(clean)


citibike_stations()
