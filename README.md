# IS3107 Citibike Pipeline

ETLT pipeline built with Apache Airflow to collect, engineer, and stage data for a Citibike demand analysis project targeting XGBoost model training. Data is staged as CSV files locally before loading into BigQuery.

## Team

| Name         | GitHub                                       |
| ------------ | -------------------------------------------- |
| Beverley Teo | [@bevteo](https://github.com/bevteo)         |
| Nathan Kew   | [@nathankkh](https://github.com/nathankkh)   |
| Nicholas Lee | [@niclee1219](https://github.com/niclee1219) |
| Terri Tan    | [@territxl](https://github.com/territxl)     |

## Data Sources

| Source                                                                          | DAG                  | Description                                                                               |
| ------------------------------------------------------------------------------- | -------------------- | ----------------------------------------------------------------------------------------- |
| [Citibike S3 (Lyft)](https://s3.amazonaws.com/tripdata/index.html)              | `citibike_trips`     | Monthly trip ZIPs covering Jan 2025 – Mar 2026                                            |
| [GBFS Lyft API](https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json) | `citibike_stations`  | Live station reference data (short_name, lat, lon, name)                                  |
| [Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api#:~:text=Download%20CSV-,API%20URL,-(Open%20in))                   | `weather_historical` | Hourly historical weather for 3 Manhattan locations (Harlem, Midtown, Financial District) |
| [python-holidays](https://github.com/vacanza/python-holidays)                   | `holidays_us_ny`     | US federal and NY state holidays for 2025–2026                                            |

## Pipeline Architecture

```mermaid
flowchart TD
    subgraph WX["weather_historical  (@monthly)"]
        W1("Extract\nOpen-Meteo Archive API\n3 Manhattan locations") --> W2("Transform\nRolling features on full series\nis_rainy · precip_last_3h · mins_since_rain") --> W3("Load\nweather_&lt;loc&gt;_YYYY-MM.csv × 45")
    end

    subgraph HX["holidays_us_ny  (@yearly)"]
        H1("Extract\npython-holidays\n2025 ∥ 2026") --> H2("Transform\nClassify federal vs state\ndedup + sort") --> H3("Load\nholidays_us_ny.csv")
    end

    subgraph CB["citibike_trips  (@monthly · catchup 202501–202603)"]
        C1("Extract\nDownload ZIP from S3") --> C2("Transform\nUnzip · filter cols · drop nulls") --> C3("Load\ntrips_YYYY-MM.csv")
    end

    subgraph ST["citibike_stations  (@monthly)"]
        S1("Extract\nGBFS Lyft API") --> S2("Transform\nshort_name · lat · lon · name") --> S3("Load\nstations.csv")
    end

    subgraph FS["feature_store  (@monthly · catchup 202501–202603)"]
        F1("Extract\nvalidate trips + stations\naverage weather · holidays") --> F2("Transform\nJoin all sources\nH3 · OD · distances · weather · temporal") --> F3("Load\nfeatures_YYYY-MM.csv")
    end

    W3 & H3 & C3 & S3 --> FS
    W3 & H3 & C3 & S3 & F3 --> BQ[("BigQuery\ncitibike dataset")]
```

## Entity Relationship

```mermaid
erDiagram
    TRIPDATA {
        string ride_id PK
        string rideable_type
        timestamp started_at
        timestamp ended_at
        string start_station_id FK
        string end_station_id FK
        string member_casual
    }

    STATION_INFO {
        string short_name PK
        string name
        float lat
        float lon
    }

    WEATHER_HOURLY {
        timestamp datetime PK
        float temperature_2m
        float apparent_temp
        float precipitation_mm
        float windspeed
        float snowfall
        float snow_depth
        float precip_last_3h
        float mins_since_rain
    }

    HOLIDAYS {
        date date PK
        string name
        int year
        string holiday_type
    }

    FEATURE_STORE {
        string ride_id PK
        float log_duration
        string rideable_type
        bool is_member
        bool is_ebike
        timestamp started_at FK
        int hour
        bool is_weekend
        bool is_rush_hour
        int month
        bool is_holiday
        string origin_h3_r9
        string origin_h3_r8
        string origin_h3_r7
        string dest_h3_r9
        string dest_h3_r8
        string dest_h3_r7
        string od_pair_r9
        string od_pair_r8
        float od_encoded
        float euclidean_dist_m
        float manhattan_dist_m
        float dist_ratio
        float actual_temp
        float apparent_temp
        float precipitation_mm
        bool is_raining
        float windspeed
        float snowfall
        float snow_depth
        float precip_last_3h
        float mins_since_rain
    }

    TRIPDATA ||--o{ FEATURE_STORE : "ride_id"
    STATION_INFO ||--o{ TRIPDATA : "start_station_id"
    STATION_INFO ||--o{ TRIPDATA : "end_station_id"
    WEATHER_HOURLY ||--o{ FEATURE_STORE : "floor(started_at, 1h)"
    HOLIDAYS ||--o{ FEATURE_STORE : "date(started_at)"
```

## DAG Reference

| DAG                  | Schedule   | Pattern | Output                                      |
| -------------------- | ---------- | ------- | ------------------------------------------- |
| `weather_historical` | `@monthly` | ETLT    | `output/weather/weather_<loc>_YYYY-MM.csv`  |
| `holidays_us_ny`     | `@yearly`  | ETL     | `output/holidays/holidays_us_ny.csv`        |
| `citibike_stations`  | `@monthly` | ETL     | `output/citibike/stations.csv`              |
| `citibike_trips`     | `@monthly` | ETLT    | `output/citibike/trips_YYYY-MM.csv`         |
| `feature_store`      | `@monthly` | ETL     | `output/feature_store/features_YYYY-MM.csv` |

## Setup

### 1. Clone repo
```bash
git clone https://github.com/niclee1219/IS3107_citibike.git && cd IS3107_citibike
```

### 2. Install dependencies

```bash
pip install apache-airflow
pip install -r requirements.txt
```

### 3. Point Airflow at this project's DAGs folder

```bash
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### 4. (optional) Setup Google Cloud credentials for BigQuery loading

Download the service account key JSON onto the local device, set the environment variable to point to the file

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
```

### 5. Run DAGs in order

```bash
airflow standalone
```

Trigger in this order (feature_store depends on all others):

1. `citibike_stations` — station reference data (needed before trips & features)
2. `weather_historical` — historical weather with rolling features (run once)
3. `holidays_us_ny` — holiday calendar (run once)
4. `citibike_trips` — enable the DAG; catchup auto-backfills 202501–202603
5. `feature_store` — enable after trips completes; catchup auto-backfills 202501–202603

## Output Structure

```
dags/output/
├── weather/
│   ├── weather_harlem_2025-01.csv          # ~744 rows per file
│   ├── weather_midtown_2025-01.csv
│   ├── weather_financial_district_2025-01.csv
│   └── ...                                 # ×45 files (3 locations × 15 months)
├── holidays/
│   └── holidays_us_ny.csv                  # US federal + NY state, 2025–2026
├── citibike/
│   ├── stations.csv                        # short_name / name / lat / lon
│   ├── trips_2025-01.csv
│   └── ...                                 # one file per month through 2026-03
└── feature_store/
    ├── features_2025-01.csv
    └── ...                                 # one file per month through 2026-03
```

## Feature Engineering Notes

### Weather

Rolling features computed across the **full 15-month time series** per location before splitting into monthly files — ensures accuracy at month boundaries.

| Column                    | Description                                               |
| ------------------------- | --------------------------------------------------------- |
| `is_rainy` / `is_raining` | `True` when `precipitation_mm > 0.1`                      |
| `precip_last_3h`          | Sum of precipitation in the 3 hours before this row       |
| `mins_since_rain`         | Minutes since the last hour with measurable precipitation |

### Spatial (H3)

Uses [Uber H3](https://github.com/uber/h3) hexagonal indexing at three resolutions:

| Resolution | Avg hex size | Use                      |
| ---------- | ------------ | ------------------------ |
| r9         | ~174 m       | Fine-grained pickup zone |
| r8         | ~461 m       | Station neighbourhood    |
| r7         | ~1.2 km      | District-level demand    |

OD pairs are encoded with a deterministic MD5 hash so the same origin-destination always maps to the same float across monthly batches.

### Distances

- **Euclidean** (`euclidean_dist_m`): haversine great-circle distance between start and end stations
- **Manhattan** (`manhattan_dist_m`): sum of north-south leg + east-west leg haversine distances
- **Ratio** (`dist_ratio`): euclidean / manhattan — values near 1.0 mean the route is direct

## BigQuery Loading

BigQuery loading is intentionally deferred — verify staged CSVs first, then uncomment `google-cloud-bigquery` in `requirements.txt`.
