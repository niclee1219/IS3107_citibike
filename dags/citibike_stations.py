"""
DAG: citibike_stations  |  Pattern: ETL + Ingest  |  Schedule: @weekly
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fetches Citibike station reference data from the GBFS API and
loads it into BigQuery with UPSERT logic

E  extract_stations    — GET station_information.json from GBFS
T  transform_stations  — keep short_name / name / lat / lon, drop invalid
L  load_stations       — write stations.csv (full-refresh)

I  ingest_stations     — load to BigQuery staging, MERGE into production

Source:
    https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json

Output (dags/output/citibike/):
    stations.csv

Cloud (BigQuery):
    Project: is3107-491906
    Dataset: citibike
    Table: static_stations (UPSERT via staging_static_stations)

Columns:
    short_name, name, lat, lon
"""

from __future__ import annotations

import csv
import os

import pendulum
from airflow.sdk import dag, task
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

CSV_FIELDNAMES = ["short_name", "name", "lat", "lon"]

# BigQuery configuration
BQ_PROJECT_ID = 'is3107-491906'
BQ_DATASET_ID = 'citibike'
BQ_TABLE_ID = 'static_stations'


@dag(
    dag_id="citibike_stations",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "IS3107"},
    tags=["citibike", "stations", "reference", "etl", "ingest", "bigquery"],
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

    @task
    def ingest_stations(csv_path: str) -> None:
        """
        INGEST — Load CSV to BigQuery staging table, then merge into Prod.
        Uses UPSERT logic: updates existing stations by short_name, inserts new ones.
        """
        # Verify CSV exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Initialize BigQuery client
        client = bigquery.Client(project=BQ_PROJECT_ID)
        staging_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.staging_{BQ_TABLE_ID}"
        prod_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        # Read CSV (remove BOM encoding)
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"LOG: Read {len(df)} stations from {csv_path}")

        ### Start Error handling: create dataset and tables if they dont exist ###
        ## Handle dataset
        dataset = bigquery.Dataset(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}")
        dataset.location = "asia-east1"
        client.create_dataset(dataset, exists_ok=True)

        ## Handle tables
        try:
            client.get_table(staging_table_id)
            client.get_table(prod_table_id)
        except NotFound:
            schema = [
                bigquery.SchemaField("short_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("lat", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("lon", "FLOAT", mode="REQUIRED"),
            ]
            staging_table = bigquery.Table(staging_table_id, schema=schema)
            client.create_table(staging_table, exists_ok=True)

            prod_table = bigquery.Table(prod_table_id, schema=schema)
            client.create_table(prod_table, exists_ok=True)
        ### End error handling ###

        # Load to staging
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # overwrites staging table (if any data)
        )
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result()
        print(f"LOG: Loaded {len(df)} stations to {staging_table_id}")

        # Merge staging into production (UPSERT)
        merge_query = f"""
        MERGE `{prod_table_id}` TARGET
        USING `{staging_table_id}` SOURCE
        ON TARGET.short_name = SOURCE.short_name
        WHEN MATCHED THEN
            UPDATE SET name = SOURCE.name, lat = SOURCE.lat, lon = SOURCE.lon
        WHEN NOT MATCHED THEN
            INSERT (short_name, name, lat, lon) VALUES (SOURCE.short_name, SOURCE.name, SOURCE.lat, SOURCE.lon)
        """
        merge_job = client.query(merge_query)
        merge_job.result()
        print(f"LOG: Merged data into {prod_table_id}")

    # ── Task wiring ──────────────────────────────────────────────────────────
    raw   = extract_stations()
    clean = transform_stations(raw)
    csv_path = load_stations(clean)
    ingest_stations(csv_path)


citibike_stations()
