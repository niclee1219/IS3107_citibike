from google.cloud import bigquery
import numpy as np

client = bigquery.Client()

def predict_trip_duration(features: dict):
    query = """
    SELECT predicted_log_duration
    FROM ML.PREDICT(
      MODEL `is3107-491906.ml_datasets.xgb_trip_duration_final_4`,
      (
        SELECT
          @is_member AS is_member,
          @is_ebike AS is_ebike,
          @hour AS hour,
          @month AS month,
          @is_weekend AS is_weekend,
          @is_rush_hour AS is_rush_hour,
          @is_holiday AS is_holiday,
          @origin_h3_r8 AS origin_h3_r8,
          @dest_h3_r8 AS dest_h3_r8,
          @od_encoded AS od_encoded,
          @euclidean_dist_m AS euclidean_dist_m,
          @dist_ratio AS dist_ratio,
          @actual_temp AS actual_temp,
          @precipitation_mm AS precipitation_mm,
          @windspeed AS windspeed,
          @snowfall AS snowfall,    
          @snow_depth AS snow_depth,
          @precip_last_3h AS precip_last_3h,
          @mins_since_rain AS mins_since_rain
      )
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("is_member", "BOOL", features["is_member"]),
            bigquery.ScalarQueryParameter("is_ebike", "BOOL", features["is_ebike"]),
            bigquery.ScalarQueryParameter("hour", "INTEGER", features["hour"]),
            bigquery.ScalarQueryParameter("month", "INTEGER", features["month"]),
            bigquery.ScalarQueryParameter("is_weekend", "BOOL", features["is_weekend"]),
            bigquery.ScalarQueryParameter("is_rush_hour", "BOOL", features["is_rush_hour"]),
            bigquery.ScalarQueryParameter("is_holiday", "BOOL", features["is_holiday"]),
            bigquery.ScalarQueryParameter("origin_h3_r8", "STRING", features["origin_h3_r8"]),
            bigquery.ScalarQueryParameter("dest_h3_r8", "STRING", features["dest_h3_r8"]),
            bigquery.ScalarQueryParameter("od_encoded", "FLOAT64", features["od_encoded"]),
            bigquery.ScalarQueryParameter("euclidean_dist_m", "FLOAT64", features["euclidean_dist_m"]),
            bigquery.ScalarQueryParameter("dist_ratio", "FLOAT64", features["dist_ratio"]),
            bigquery.ScalarQueryParameter("actual_temp", "FLOAT64", features["actual_temp"]),
            bigquery.ScalarQueryParameter("precipitation_mm", "FLOAT64", features["precipitation_mm"]),
            bigquery.ScalarQueryParameter("windspeed", "FLOAT64", features["windspeed"]),
            bigquery.ScalarQueryParameter("snowfall", "FLOAT64", features["snowfall"]),
            bigquery.ScalarQueryParameter("snow_depth", "FLOAT64", features["snow_depth"]),
            bigquery.ScalarQueryParameter("precip_last_3h", "FLOAT64", features["precip_last_3h"]),
            bigquery.ScalarQueryParameter("mins_since_rain", "FLOAT64", features["mins_since_rain"]),
        ]
    )

    result = client.query(query, job_config=job_config).to_dataframe()
    log_duration = result.iloc[0]["predicted_log_duration"]

    return np.exp(log_duration)