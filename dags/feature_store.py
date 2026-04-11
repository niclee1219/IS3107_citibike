"""
DAG: feature_store  |  Pattern: ETL  |  Schedule: @monthly
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Joins Citibike trip data with station info, weather, and holidays to
produce a feature-engineered dataset ready for XGBoost model training.

Depends on upstream DAGs having already staged their CSVs:
    citibike_trips     →  output/citibike_trips/trips_YYYY-MM.csv
    citibike_stations  →  output/citibike_stations/stations.csv
    weather_historical →  output/weather/weather_*_YYYY-MM.csv
    holidays_us_ny     →  output/holidays/holidays_us_ny.csv

E  extract_*          - validate/load each source into a temp file
T  transform_features - join all sources, compute all features
L  load_features      - write features_YYYY-MM.csv, clean up temps

Output (output/feature_store/):
    features_YYYY-MM.csv

Feature columns:
    ride_id, log_duration, rideable_type, is_member, is_ebike,
    started_at, hour, is_weekend, is_rush_hour, month, is_holiday,
    origin_h3_r9, origin_h3_r8, origin_h3_r7,
    dest_h3_r9,   dest_h3_r8,   dest_h3_r7,
    od_pair_r9, od_pair_r8, od_encoded,
    euclidean_dist_m, manhattan_dist_m, dist_ratio,
    actual_temp, apparent_temp, precipitation_mm, is_raining,
    windspeed, snowfall, snow_depth, precip_last_3h, mins_since_rain
"""

from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, task
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

BQ_PROJECT_ID = 'is3107-491906'
BQ_DATASET_ID = 'citibike'
BQ_TABLE_ID   = 'features'

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@dag(
    dag_id="feature_store",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    schedule="@monthly",
    catchup=True,
    max_active_runs=2,
    default_args={"owner": "IS3107"},
    tags=["feature-store", "citibike", "xgboost", "etl"],
    doc_md=__doc__,
)
def feature_store():

    @task
    def get_month_params() -> dict:
        """
        Resolve data_interval_start from context and return month strings.
        Centralising context access avoids Airflow 3 mapped-task conflicts
        when data_interval_start is used as a function parameter.
        """
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        dis = ctx["data_interval_start"]
        return {
            "month_label": dis.strftime("%Y-%m"),   # e.g. "2025-01"
        }

    # -- EXTRACT --------------------------------------------------------------

    @task
    def extract_trips(month_label: str) -> str:
        """
        EXTRACT - Validate the monthly trips CSV exists and return its path.
        """
        path = os.path.join(_PROJECT_ROOT, "output", "citibike_trips", f"trips_{month_label}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"trips_{month_label}.csv not found - run citibike_trips DAG first."
            )
        print(f"[trips] Found {path}")
        return path

    @task
    def extract_stations() -> str:
        """
        EXTRACT - Validate stations.csv exists and return its path.
        """
        path = os.path.join(_PROJECT_ROOT, "output", "citibike_stations", "stations.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(
                "stations.csv not found - run citibike_stations DAG first."
            )
        print(f"[stations] Found {path}")
        return path

    @task
    def extract_weather(month_label: str) -> str:
        """
        EXTRACT - Load weather CSVs for all 3 Manhattan locations for this month,
        average across locations by hour to produce a single hourly Manhattan
        weather record, and save to a temp file.
        """
        import tempfile
        import pandas as pd

        weather_dir    = os.path.join(_PROJECT_ROOT, "output", "weather")
        location_names = ["harlem", "midtown", "financial_district"]

        frames: list[pd.DataFrame] = []
        for loc in location_names:
            p = os.path.join(weather_dir, f"weather_{loc}_{month_label}.csv")
            if not os.path.exists(p):
                raise FileNotFoundError(
                    f"{p} not found - run weather_historical DAG first."
                )
            frames.append(pd.read_csv(p))

        combined = pd.concat(frames, ignore_index=True)

        # Average the 3 locations → one representative Manhattan row per hour
        hourly = (
            combined
            .groupby("datetime", as_index=False)
            .agg({
                "temperature_2m":   "mean",
                "apparent_temp":    "mean",
                "precipitation_mm": "mean",
                "windspeed":        "mean",
                "snowfall":         "mean",
                "snow_depth":       "mean",
                "precip_last_3h":   "mean",
                "mins_since_rain":  "mean",
            })
        )
        hourly["is_raining"] = hourly["precipitation_mm"] > 0.1

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=f"_weather_{month_label}.csv", delete=False
        )
        hourly.to_csv(tmp.name, index=False)
        tmp.close()
        print(f"[weather] {len(hourly)} hourly records averaged → {tmp.name}")
        return tmp.name

    @task
    def extract_holidays() -> str:
        """
        EXTRACT - Validate holidays CSV exists and return its path.
        """
        path = os.path.join(_PROJECT_ROOT, "output", "holidays", "holidays_us_ny.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(
                "holidays_us_ny.csv not found - run holidays_us_ny DAG first."
            )
        print(f"[holidays] Found {path}")
        return path

    # -- TRANSFORM ------------------------------------------------------------

    @task
    def transform_features(
        trips_path: str,
        stations_path: str,
        weather_path: str,
        holidays_path: str,
        month_label: str,
    ) -> str:
        """
        TRANSFORM - Join all four sources and engineer the full feature set.

        Joins:
            trips ⟕ stations  ON start_station_id = short_name  (start lat/lon)
            trips ⟕ stations  ON end_station_id   = short_name  (end lat/lon)
            trips ⟕ weather   ON floor(started_at, 1h) = datetime
            trips ⟕ holidays  ON date(started_at) = date
        """
        import hashlib
        import tempfile
        import numpy as np
        import pandas as pd
        import h3

        # -- Load sources -----------------------------------------------------
        trips    = pd.read_csv(trips_path, dtype={"start_station_id": str, "end_station_id": str})
        stations = pd.read_csv(stations_path, dtype={"short_name": str})
        weather  = pd.read_csv(weather_path)
        holidays = pd.read_csv(holidays_path)

        print(f"[{month_label}] Loaded: {len(trips):,} trips | "
              f"{len(stations)} stations | {len(weather)} weather hrs | "
              f"{len(holidays)} holidays")

        trips["started_at"] = pd.to_datetime(trips["started_at"])
        trips["ended_at"]   = pd.to_datetime(trips["ended_at"])

        # -- Join start station -----------------------------------------------
        start_stations = stations.rename(columns={
            "short_name": "start_station_id",
            "lat": "start_lat", "lon": "start_lon",
        })
        trips = trips.merge(
            start_stations[["start_station_id", "start_lat", "start_lon"]],
            on="start_station_id", how="left",
        )

        # -- Join end station -------------------------------------------------
        end_stations = stations.rename(columns={
            "short_name": "end_station_id",
            "lat": "end_lat", "lon": "end_lon",
        })
        trips = trips.merge(
            end_stations[["end_station_id", "end_lat", "end_lon"]],
            on="end_station_id", how="left",
        )
        trips = trips.dropna(subset=["start_lat", "start_lon", "end_lat", "end_lon"])
        print(f"[{month_label}] {len(trips):,} trips after station join")

        # -- Join weather -----------------------------------------------------
        trips["weather_key"] = (
            trips["started_at"].dt.floor("h").dt.strftime("%Y-%m-%dT%H:00:00")
        )
        trips = trips.merge(
            weather.rename(columns={"datetime": "weather_key"}),
            on="weather_key", how="left",
        )

        # -- Join holidays ----------------------------------------------------
        holiday_dates     = set(holidays["date"].unique())
        trips["date_str"] = trips["started_at"].dt.strftime("%Y-%m-%d")
        trips["is_holiday"] = trips["date_str"].isin(holiday_dates)

        # -- Temporal features ------------------------------------------------
        trips["hour"]         = trips["started_at"].dt.hour
        trips["month"]        = trips["started_at"].dt.month
        trips["is_weekend"]   = trips["started_at"].dt.dayofweek >= 5
        trips["is_rush_hour"] = trips["hour"].isin({7, 8, 9, 17, 18, 19})

        # -- Trip features ----------------------------------------------------
        duration_sec          = (trips["ended_at"] - trips["started_at"]).dt.total_seconds()
        trips["log_duration"] = np.log(duration_sec.clip(lower=1))
        trips["is_member"]    = trips["member_casual"] == "member"
        trips["is_ebike"]     = trips["rideable_type"] == "electric_bike"

        # -- H3 spatial features ----------------------------------------------
        for res in [7, 8, 9]:
            trips[f"origin_h3_r{res}"] = trips.apply(
                lambda r, r_=res: h3.latlng_to_cell(r["start_lat"], r["start_lon"], r_),
                axis=1,
            )
            trips[f"dest_h3_r{res}"] = trips.apply(
                lambda r, r_=res: h3.latlng_to_cell(r["end_lat"], r["end_lon"], r_),
                axis=1,
            )

        trips["od_pair_r9"] = trips["origin_h3_r9"] + "_" + trips["dest_h3_r9"]
        trips["od_pair_r8"] = trips["origin_h3_r8"] + "_" + trips["dest_h3_r8"]

        # Deterministic MD5 hash - same OD pair always maps to the same float
        trips["od_encoded"] = trips["od_pair_r9"].apply(
            lambda od: float(int(hashlib.md5(od.encode()).hexdigest(), 16) % 1_000_000)
        )

        # -- Distance features ------------------------------------------------
        def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            from math import radians, sin, cos, asin, sqrt
            R = 6_371_000.0
            lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
            a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
            return 2 * R * asin(sqrt(a))

        trips["euclidean_dist_m"] = trips.apply(
            lambda r: haversine_m(r["start_lat"], r["start_lon"], r["end_lat"], r["end_lon"]),
            axis=1,
        )
        trips["manhattan_dist_m"] = trips.apply(
            lambda r: (
                haversine_m(r["start_lat"], r["start_lon"], r["end_lat"],   r["start_lon"]) +
                haversine_m(r["start_lat"], r["start_lon"], r["start_lat"], r["end_lon"])
            ),
            axis=1,
        )
        trips["dist_ratio"] = np.where(
            trips["manhattan_dist_m"] > 0,
            trips["euclidean_dist_m"] / trips["manhattan_dist_m"],
            1.0,
        )

        # -- Select & rename final columns ------------------------------------
        base_cols = [
            "log_duration", "rideable_type", "is_member", "is_ebike",
            "started_at", "hour", "is_weekend", "is_rush_hour", "month", "is_holiday",
            "origin_h3_r9", "origin_h3_r8", "origin_h3_r7",
            "dest_h3_r9",   "dest_h3_r8",   "dest_h3_r7",
            "od_pair_r9", "od_pair_r8", "od_encoded",
            "euclidean_dist_m", "manhattan_dist_m", "dist_ratio",
            "temperature_2m", "apparent_temp", "precipitation_mm", "is_raining",
            "windspeed", "snowfall", "snow_depth", "precip_last_3h", "mins_since_rain",
        ]
        if "ride_id" in trips.columns:
            base_cols = ["ride_id"] + base_cols

        result = (
            trips[[c for c in base_cols if c in trips.columns]]
            .rename(columns={"temperature_2m": "actual_temp"})
        )

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=f"_features_{month_label}.csv", delete=False
        )
        result.to_csv(tmp.name, index=False)
        tmp.close()
        print(f"[{month_label}] Feature matrix: {result.shape} → {tmp.name}")
        return tmp.name

    # -- LOAD -----------------------------------------------------------------

    @task
    def load_features(processed_path: str, weather_path: str, month_label: str) -> str:
        """
        LOAD - Write the feature matrix to CSV and clean up all temp files.
        """
        import pandas as pd

        output_dir = os.path.join(_PROJECT_ROOT, "output", "feature_store")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"features_{month_label}.csv")

        df = pd.read_csv(processed_path)
        df.to_csv(out_path, index=False)
        print(f"[{month_label}] Saved {len(df):,} rows × {len(df.columns)} cols → {out_path}")

        for tmp in [processed_path, weather_path]:
            try:
                os.remove(tmp)
            except OSError:
                pass

        return out_path

    @task
    def ingest_features(csv_path: str, month_label: str) -> None:
        """
        INGEST - Delete this month's rows from BigQuery then append fresh features.
        Idempotent: re-running the same month replaces rather than duplicates.
        """
        import pandas as pd

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        client = bigquery.Client(project=BQ_PROJECT_ID)
        prod_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        df['started_at'] = pd.to_datetime(df['started_at'])

        bool_cols = ['is_member', 'is_ebike', 'is_weekend', 'is_rush_hour',
                     'is_holiday', 'is_raining']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower().map(
                    {'true': True, 'false': False}
                )

        month_start = f"{month_label}-01"
        month_end   = pd.Period(month_label, 'M').end_time.strftime('%Y-%m-%d')
        print(f"[{month_label}] Read {len(df):,} feature rows from {csv_path}")

        dataset = bigquery.Dataset(f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}")
        dataset.location = "asia-east1"
        client.create_dataset(dataset, exists_ok=True)

        try:
            client.get_table(prod_table_id)
        except NotFound:
            schema = [
                bigquery.SchemaField("ride_id",           "STRING",    mode="NULLABLE"),
                bigquery.SchemaField("log_duration",      "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("rideable_type",     "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("is_member",         "BOOLEAN",   mode="REQUIRED"),
                bigquery.SchemaField("is_ebike",          "BOOLEAN",   mode="REQUIRED"),
                bigquery.SchemaField("started_at",        "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("hour",              "INTEGER",   mode="REQUIRED"),
                bigquery.SchemaField("is_weekend",        "BOOLEAN",   mode="REQUIRED"),
                bigquery.SchemaField("is_rush_hour",      "BOOLEAN",   mode="REQUIRED"),
                bigquery.SchemaField("month",             "INTEGER",   mode="REQUIRED"),
                bigquery.SchemaField("is_holiday",        "BOOLEAN",   mode="REQUIRED"),
                bigquery.SchemaField("origin_h3_r9",      "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("origin_h3_r8",      "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("origin_h3_r7",      "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("dest_h3_r9",        "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("dest_h3_r8",        "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("dest_h3_r7",        "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("od_pair_r9",        "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("od_pair_r8",        "STRING",    mode="REQUIRED"),
                bigquery.SchemaField("od_encoded",        "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("euclidean_dist_m",  "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("manhattan_dist_m",  "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("dist_ratio",        "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("actual_temp",       "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("apparent_temp",     "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("precipitation_mm",  "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("is_raining",        "BOOLEAN",   mode="REQUIRED"),
                bigquery.SchemaField("windspeed",         "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("snowfall",          "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("snow_depth",        "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("precip_last_3h",    "FLOAT",     mode="REQUIRED"),
                bigquery.SchemaField("mins_since_rain",   "FLOAT",     mode="REQUIRED"),
            ]
            prod_table = bigquery.Table(prod_table_id, schema=schema)
            prod_table.time_partitioning = bigquery.TimePartitioning(field='started_at')
            prod_table.clustering_fields = ['rideable_type', 'is_member', 'is_ebike']
            client.create_table(prod_table, exists_ok=True)

        delete_query = f"""
            DELETE FROM `{prod_table_id}`
            WHERE DATE(started_at) BETWEEN '{month_start}' AND '{month_end}'
        """
        client.query(delete_query).result()
        print(f"[{month_label}] Deleted existing rows for {month_start} - {month_end}")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = client.load_table_from_dataframe(df, prod_table_id, job_config=job_config)
        load_job.result()
        print(f"[{month_label}] Appended {len(df)} feature rows to {prod_table_id}")

    # -- Task wiring ----------------------------------------------------------
    params = get_month_params()

    trips_path    = extract_trips(params["month_label"])
    stations_path = extract_stations()
    weather_path  = extract_weather(params["month_label"])
    holidays_path = extract_holidays()

    processed = transform_features(
        trips_path, stations_path, weather_path, holidays_path, params["month_label"]
    )
    csv_path = load_features(processed, weather_path, params["month_label"])
    ingest_features(csv_path, params["month_label"])


feature_store()
