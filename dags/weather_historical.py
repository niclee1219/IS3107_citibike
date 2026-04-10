"""
DAG: weather_historical  |  Pattern: ETLT  |  Schedule: @once
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pulls hourly weather for 3 Manhattan locations (Jan 2025 – Mar 2026)
from the Open-Meteo archive API, engineers rolling weather features,
and stages one CSV per location-month for incremental BigQuery ingestion.

E  extract_weather     — fetch full time series from Open-Meteo → temp JSON
T  transform_weather   — build DataFrame, compute rolling features → temp CSV
L  load_weather        — split by month, write weather_<loc>_YYYY-MM.csv files

Rolling features are computed across the FULL time series before splitting
so values at month boundaries are accurate.

Locations:
    harlem              (40.8116, -73.9465)
    midtown             (40.7549, -73.9840)
    financial_district  (40.7074, -74.0113)

Output (output/weather/):
    weather_harlem_2025-01.csv  …  weather_harlem_2026-03.csv  (x3 locations)

Columns:
    datetime, location, lat, lon,
    temperature_2m, apparent_temp, precipitation_mm, windspeed,
    snowfall, snow_depth, is_rainy, precip_last_3h, mins_since_rain
"""

from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, task
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

BQ_PROJECT_ID       = 'is3107-491906'
BQ_DATASET_ID       = 'citibike'
BQ_TABLE_ID         = 'weather_hourly'
BQ_STAGING_TABLE_ID = 'staging_weather_hourly'

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

LOCATIONS: list[dict] = [
    {"name": "harlem",             "lat": 40.8116, "lon": -73.9465},
    {"name": "midtown",            "lat": 40.7549, "lon": -73.9840},
    {"name": "financial_district", "lat": 40.7074, "lon": -74.0113},
]

_OM_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "snowfall",
    "snow_depth",
]

CSV_FIELDNAMES = [
    "datetime", "location", "lat", "lon",
    "temperature_2m", "apparent_temp", "precipitation_mm",
    "windspeed", "snowfall", "snow_depth",
    "is_rainy", "precip_last_3h", "mins_since_rain",
]


@dag(
    dag_id="weather_historical",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@monthly",
    catchup=False,
    max_active_tasks=3,
    max_active_runs=1,
    default_args={"owner": "IS3107"},
    tags=["weather", "citibike", "historical", "open-meteo", "etlt"],
    doc_md=__doc__,
)
def weather_historical():

    @task
    def get_location_names() -> list[str]:
        # Expand over simple strings — avoids Airflow 3 XCom dict-serialisation
        # issues when using dicts as dynamic task mapping parameters.
        return [loc["name"] for loc in LOCATIONS]

    @task
    def extract_weather(location_name: str) -> str:
        """
        EXTRACT — Fetch the full hourly time series (Jan 2025 - Mar 2026) for one
        location from the Open-Meteo archive API.

        Location coordinates are looked up from the module-level LOCATIONS constant
        so only a plain string needs to pass through XCom for dynamic task mapping.

        Claim-check: saves raw API response to a temp JSON file and returns the path
        so the large payload never passes through XCom.
        """
        import json
        import tempfile
        import requests

        location = next(loc for loc in LOCATIONS if loc["name"] == location_name)

        response = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude":        location["lat"],
                "longitude":       location["lon"],
                "start_date":      "2025-01-01",
                "end_date":        "2026-03-31",
                "hourly":          ",".join(_OM_VARIABLES),
                "timezone":        "America/New_York",
                "wind_speed_unit": "kmh",
            },
            timeout=120,
        )
        response.raise_for_status()
        hourly = response.json()["hourly"]
        print(f"[{location_name}] Fetched {len(hourly['time'])} hourly records")

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=f"_weather_raw_{location_name}.json", delete=False
        )
        json.dump({"location": location, "hourly": hourly}, tmp)
        tmp.flush()
        tmp.close()
        return tmp.name

    @task
    def transform_weather(raw_path: str) -> str:
        """
        TRANSFORM — Load raw JSON, build a pandas DataFrame, and compute rolling
        weather features across the FULL time series (not month-by-month) so that
        precip_last_3h and mins_since_rain are accurate at month boundaries.
        """
        import json
        import tempfile
        import pandas as pd

        with open(raw_path) as f:
            payload = json.load(f)

        location = payload["location"]
        hourly   = payload["hourly"]

        df = pd.DataFrame({
            "datetime":         [t + ":00" for t in hourly["time"]],
            "location":         location["name"],
            "lat":              location["lat"],
            "lon":              location["lon"],
            "temperature_2m":   hourly["temperature_2m"],
            "apparent_temp":    hourly["apparent_temperature"],
            "precipitation_mm": hourly["precipitation"],
            "windspeed":        hourly["wind_speed_10m"],
            "snowfall":         hourly["snowfall"],
            "snow_depth":       hourly["snow_depth"],
        })

        # is_rainy: measurable precipitation (>0.1 mm threshold)
        df["is_rainy"] = df["precipitation_mm"] > 0.1

        # precip_last_3h: sum of the 3 hours immediately before this row
        df["precip_last_3h"] = (
            df["precipitation_mm"]
            .shift(1)
            .rolling(window=3, min_periods=1)
            .sum()
            .fillna(0)
            .round(2)
        )

        # mins_since_rain: minutes elapsed since the last rainy hour
        mins_since: list[float | None] = []
        last_rain_idx: int | None = None
        for i, precip in enumerate(df["precipitation_mm"]):
            if precip > 0.1:
                last_rain_idx = i
            mins_since.append(
                0 if last_rain_idx is None else (i - last_rain_idx) * 60
            )
        df["mins_since_rain"] = mins_since

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=f"_weather_processed_{location['name']}.csv", delete=False
        )
        df[CSV_FIELDNAMES].to_csv(tmp.name, index=False)
        tmp.close()

        try:
            os.remove(raw_path)
        except OSError:
            pass

        print(f"[{location['name']}] Transformed {len(df)} rows → {tmp.name}")
        return tmp.name

    @task
    def load_weather(processed_path: str) -> list[str]:
        """
        LOAD — Split the processed full-series DataFrame by calendar month and
        write one file per location-month: weather_<location>_YYYY-MM.csv.

        Splitting happens after feature engineering so rolling values are accurate.
        """
        import pandas as pd

        df = pd.read_csv(processed_path)
        df["_month"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m")
        location_name = df["location"].iloc[0]

        output_dir = os.path.join(_PROJECT_ROOT, "output", "weather")
        os.makedirs(output_dir, exist_ok=True)

        saved: list[str] = []
        for month, group in df.groupby("_month"):
            out_path = os.path.join(output_dir, f"weather_{location_name}_{month}.csv")
            group.drop(columns=["_month"])[CSV_FIELDNAMES].to_csv(out_path, index=False)
            saved.append(out_path)

        print(f"[{location_name}] Saved {len(saved)} monthly files ({len(df)} total rows)")

        try:
            os.remove(processed_path)
        except OSError:
            pass

        return saved

    @task
    def ingest_weather(csv_paths: list[str]) -> None:
        """
        INGEST — Replace all rows for one location in BigQuery with fresh data.
        - Insert into staging, then
        - Merge by DELETE location + INSERT pattern so rolling features recomputed across the full series are reflected in BQ on every run.
        """
        import pandas as pd

        frames = [pd.read_csv(p, encoding='utf-8-sig') for p in csv_paths]
        df = pd.concat(frames, ignore_index=True)

        df['datetime'] = pd.to_datetime(df['datetime'])
        df['is_rainy'] = df['is_rainy'].astype(str).map({'True': True, 'False': False}).fillna(False).astype(bool)
        location_name = df['location'].iloc[0]
        print(f"[{location_name}] Read {len(df):,} rows from {len(csv_paths)} monthly files")

        client = bigquery.Client(project=BQ_PROJECT_ID)
        staging_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_STAGING_TABLE_ID}"
        prod_table_id    = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        schema = [
            bigquery.SchemaField("datetime",         "DATETIME", mode="REQUIRED"), #nyc have the same time
            bigquery.SchemaField("location",         "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("lat",              "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("lon",              "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("temperature_2m",   "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("apparent_temp",    "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("precipitation_mm", "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("windspeed",        "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("snowfall",         "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("snow_depth",       "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("is_rainy",         "BOOLEAN",   mode="REQUIRED"),
            bigquery.SchemaField("precip_last_3h",   "FLOAT",     mode="REQUIRED"),
            bigquery.SchemaField("mins_since_rain",  "FLOAT",     mode="REQUIRED"),
        ]

        dataset = bigquery.Dataset(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}")
        dataset.location = "asia-east1"
        client.create_dataset(dataset, exists_ok=True)

        try:
            client.get_table(staging_table_id)
            client.get_table(prod_table_id)
        except NotFound:
            client.create_table(
                bigquery.Table(staging_table_id, schema=schema), exists_ok=True
            )
            prod_table = bigquery.Table(prod_table_id, schema=schema)
            prod_table.time_partitioning = bigquery.TimePartitioning(field='datetime')
            prod_table.clustering_fields = ['location']
            client.create_table(prod_table, exists_ok=True)

        # Load full location data to staging (overwrite)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        load_job.result()
        print(f"[{location_name}] Loaded {len(df):,} rows to {staging_table_id}")

        # Replace this location's rows in prod
        client.query(
            f"DELETE FROM `{prod_table_id}` WHERE location = '{location_name}'"
        ).result()
        client.query(f"""
            INSERT INTO `{prod_table_id}`
            SELECT datetime, location, lat, lon,
                   temperature_2m, apparent_temp, precipitation_mm, windspeed,
                   snowfall, snow_depth, is_rainy, precip_last_3h, mins_since_rain
            FROM `{staging_table_id}`
        """).result()
        print(f"[{location_name}] Replaced rows in {prod_table_id}")

    # ── Task wiring (3 parallel E>T>L>I chains, one per location) ───────────
    location_names  = get_location_names()
    raw_paths       = extract_weather.expand(location_name=location_names)
    processed_paths = transform_weather.expand(raw_path=raw_paths)
    saved_paths     = load_weather.expand(processed_path=processed_paths)
    ingest_weather.expand(csv_paths=saved_paths)


weather_historical()
