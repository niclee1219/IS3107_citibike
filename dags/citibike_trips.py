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

I  ingest_citibike_trips     — load to BigQuery staging (WRITE_TRUNCATE), MERGE into production on ride_id

Source:
    https://s3.amazonaws.com/tripdata/{YYYYMM}-citibike-tripdata.zip

Exclusions:
    ZIP entries whose filename starts with "jc-" (Jersey City data)

Output (output/citibike_trips/):
    trips_YYYY-MM.csv

Columns:
    rideable_type, started_at, ended_at,
    start_station_id, end_station_id, member_casual
"""

from __future__ import annotations

import os
import pendulum
from airflow.sdk import dag, task
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

BQ_PROJECT_ID = 'is3107-491906'
BQ_DATASET_ID = 'citibike'
BQ_TABLE_ID   = 'trips'

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

KEEP_COLUMNS = [
    "ride_id",  
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_id",
    "end_station_id",
    "member_casual",
]

@dag(
    dag_id="citibike_trips",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), 
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

        output_dir = os.path.join(_PROJECT_ROOT, "output", "citibike_trips")
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

    @task
    def ingest_citibike_trips(csv_path: str, month_label: str) -> None:
        """
        INGEST — Load month's trips to a staging table (WRITE_TRUNCATE),
        then MERGE into production on ride_id.
        Idempotent: re-running the same month upserts rather than duplicates.
        """
        import pandas as pd

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        client = bigquery.Client(project=BQ_PROJECT_ID)
        staging_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.staging_{BQ_TABLE_ID}"
        prod_table_id    = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype={"start_station_id": str, "end_station_id": str})
        df['started_at'] = pd.to_datetime(df['started_at'])
        df['ended_at']   = pd.to_datetime(df['ended_at'])
        df['start_station_id'] = df['start_station_id'].astype(str)
        df['end_station_id']   = df['end_station_id'].astype(str)
        print(f"[{month_label}] Read {len(df):,} trips from {csv_path}")

        dataset = bigquery.Dataset(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}")
        dataset.location = "asia-east1"
        client.create_dataset(dataset, exists_ok=True)

        schema = [
            bigquery.SchemaField("ride_id",           "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("rideable_type",     "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("started_at",        "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("ended_at",          "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("start_station_id",  "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("end_station_id",    "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("member_casual",     "STRING",    mode="REQUIRED"),
        ]

        try:
            client.get_table(staging_table_id)
            client.get_table(prod_table_id)
        except NotFound:
            staging_table = bigquery.Table(staging_table_id, schema=schema)
            client.create_table(staging_table, exists_ok=True)

            prod_table = bigquery.Table(prod_table_id, schema=schema)
            prod_table.time_partitioning = bigquery.TimePartitioning(field='started_at')
            prod_table.clustering_fields = ['rideable_type', 'member_casual']
            client.create_table(prod_table, exists_ok=True)

        # Load to staging (truncate first so re-runs are clean)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result()
        print(f"[{month_label}] Loaded {len(df):,} trips to {staging_table_id}")

        # Merge staging into production (UPSERT on ride_id)
        merge_query = f"""
        MERGE `{prod_table_id}` TARGET
        USING `{staging_table_id}` SOURCE
        ON TARGET.ride_id = SOURCE.ride_id
        WHEN MATCHED THEN
            UPDATE SET
                rideable_type    = SOURCE.rideable_type,
                started_at       = SOURCE.started_at,
                ended_at         = SOURCE.ended_at,
                start_station_id = SOURCE.start_station_id,
                end_station_id   = SOURCE.end_station_id,
                member_casual    = SOURCE.member_casual
        WHEN NOT MATCHED THEN
            INSERT (ride_id, rideable_type, started_at, ended_at,
                    start_station_id, end_station_id, member_casual)
            VALUES (SOURCE.ride_id, SOURCE.rideable_type, SOURCE.started_at, SOURCE.ended_at,
                    SOURCE.start_station_id, SOURCE.end_station_id, SOURCE.member_casual)
        """
        merge_job = client.query(merge_query)
        merge_job.result()
        print(f"[{month_label}] Merged trips into {prod_table_id}")

    # ── Task wiring ──────────────────────────────────────────────────────────
    # Using Airflow Jinja templates inherently resolves function parameter
    # conflicts while eliminating the need for an isolated context-fetching task.

    month_str = "{{ data_interval_start.strftime('%Y%m') }}"
    month_label = "{{ data_interval_start.strftime('%Y-%m') }}"

    zip_path      = extract_citibike_trips(month_str)
    temp_csv_path = transform_citibike_trips(zip_path, month_label)
    csv_path      = load_citibike_trips(temp_csv_path, zip_path, month_label)
    ingest_citibike_trips(csv_path, month_label)

citibike_trips()