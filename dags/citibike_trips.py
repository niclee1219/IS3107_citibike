"""
DAG: citibike_trips  |  Pattern: ETLT  |  Schedule: @monthly
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Downloads monthly Citibike trip ZIP files from S3, extracts the CSVs,
filters to the required columns, and stages one CSV per month.

catchup=True with end_date=2026-04-01 automatically backfills all months
from 202501 through 202603.

Each scheduled run processes the month of data_interval_start:
    Run on 2025-02-01  →  pulls 202501-citibike-tripdata.zip
    Run on 2025-03-01  →  pulls 202502-citibike-tripdata.zip …

E  extract_citibike_trips    — download ZIP from S3 → temp file (claim-check)
T  transform_citibike_trips  — unzip, concat CSVs, filter columns, write temp CSV
L  load_citibike_trips       — move temp CSV to output, clean up temp ZIP

Source:
    https://s3.amazonaws.com/tripdata/{YYYYMM}-citibike-tripdata.zip

Exclusions:
    ZIP entries whose filename starts with "jc-" (Jersey City data)

Output (dags/output/citibike_trips/):
    trips_YYYY-MM.csv

Columns:
    rideable_type, started_at, ended_at,
    start_station_id, end_station_id, member_casual
"""

from __future__ import annotations

import os
import pendulum
from airflow.sdk import dag, task

KEEP_COLUMNS = [
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_id",
    "end_station_id",
    "member_casual",
]

@dag(
    dag_id="citibike_trips",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), 
    end_date=pendulum.datetime(2026, 4, 1, tz="UTC"), # This stops it at 202603
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "IS3107"},
    tags=["citibike", "trips", "etlt"],
)
def citibike_trips():

    @task
    def extract_citibike_trips(month_str: str) -> str:
        """
        EXTRACT — Download the monthly trip ZIP from S3.
        Claim-check: writes to a temp file, returns the path.
        """
        import tempfile
        import requests
        from airflow.exceptions import AirflowSkipException

        url = f"https://s3.amazonaws.com/tripdata/{month_str}-citibike-tripdata.zip"
        print(f"Downloading {url}")

        response = requests.get(url, stream=True, timeout=300)
        
        # Gracefully skip if the month's data hasn't been published yet
        if response.status_code == 404:
            raise AirflowSkipException(f"Dataset for {month_str} is not yet available on S3.")
            
        # Fail the task for any other HTTP errors (500, 403, etc.)
        response.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(suffix=f"_{month_str}.zip", delete=False)
        for chunk in response.iter_content(chunk_size=8192):
            tmp.write(chunk)
        tmp.flush()
        tmp.close()

        print(f"Downloaded {os.path.getsize(tmp.name) / 1_000_000:.1f} MB → {tmp.name}")
        return tmp.name
    @task
    def transform_citibike_trips(zip_path: str, month_label: str) -> str:
        """
        TRANSFORM — Unzip, concatenate all CSVs, filter to required columns, 
        drop rows with missing station IDs. 
        Writes output to a temporary CSV to avoid blowing up the Airflow XCom limit.
        """
        import zipfile
        import tempfile
        import pandas as pd

        frames: list[pd.DataFrame] = []

        with zipfile.ZipFile(zip_path) as zf:
            csv_entries = [
                name for name in zf.namelist()
                if name.lower().endswith(".csv")
                and not os.path.basename(name).lower().startswith("jc-")
            ]
            print(f"[{month_label}] ZIP contains {len(csv_entries)} CSV(s): {csv_entries}")

            for entry in csv_entries:
                with zf.open(entry) as f:
                    df = pd.read_csv(f, usecols=lambda c: c in KEEP_COLUMNS, low_memory=False)
                    frames.append(df)

        combined  = pd.concat(frames, ignore_index=True)
        available = [c for c in KEEP_COLUMNS if c in combined.columns]
        combined  = combined[available].dropna(subset=["start_station_id", "end_station_id"])

        print(f"[{month_label}] {len(combined):,} rows after filtering")
        
        # Save to temp file and return the path, NOT the raw data
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        combined.to_csv(tmp.name, index=False)
        return tmp.name

    @task
    def load_citibike_trips(temp_csv_path: str, zip_path: str, month_label: str) -> str:
        """
        LOAD — Move the filtered temporary CSV to the final output directory 
        and clean up the temporary ZIP file.
        """
        import shutil

        output_dir = os.path.join(os.path.dirname(__file__), "output", "citibike_trips")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"trips_{month_label}.csv")

        # Move the temp file to the final destination (fast, low memory)
        shutil.move(temp_csv_path, out_path)
        print(f"[{month_label}] Saved final output → {out_path}")

        # Clean up the original downloaded zip file
        try:
            os.remove(zip_path)
        except OSError:
            pass

        return out_path

    # ── Task wiring ──────────────────────────────────────────────────────────
    # Using Airflow Jinja templates inherently resolves function parameter 
    # conflicts while eliminating the need for an isolated context-fetching task.
    
    month_str = "{{ data_interval_start.strftime('%Y%m') }}"
    month_label = "{{ data_interval_start.strftime('%Y-%m') }}"

    zip_path = extract_citibike_trips(month_str)
    temp_csv_path = transform_citibike_trips(zip_path, month_label)
    load_citibike_trips(temp_csv_path, zip_path, month_label)

citibike_trips()