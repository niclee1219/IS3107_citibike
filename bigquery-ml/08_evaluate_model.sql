-- ================================================================
-- 8) EVALUATE ON TEST SET
-- ================================================================

-- Metrics in log space
CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trip_eval_default_4` AS
SELECT *
FROM ML.EVALUATE(
  MODEL `is3107-491906.ml_datasets.xgb_trip_duration_final_4`,
  (SELECT * FROM `is3107-491906.ml_datasets.trips_test_input_4`)
);

-- Metrics in original duration scale
CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trip_eval_original_scale_4` AS
SELECT
  SQRT(AVG(POW(predicted_log_duration - log_duration, 2))) AS rmse_log,
  AVG(ABS(predicted_log_duration - log_duration)) AS mae_log,
  AVG(
    ABS(EXP(predicted_log_duration) - EXP(log_duration))
    / EXP(log_duration)
  ) * 100 AS mape_pct,
  1 - VARIANCE(predicted_log_duration - log_duration)
      / VARIANCE(log_duration) AS r_squared
FROM ML.PREDICT(
  MODEL `is3107-491906.ml_datasets.xgb_trip_duration_final_4`,
  (SELECT * FROM `is3107-491906.ml_datasets.trips_test_input_4`)
);

-- Metrics bucketed by trip duration
CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trip_eval_bucketed_4` AS
SELECT
  CASE
    WHEN EXP(log_duration) / 60 < 5   THEN 'under 5 min'
    WHEN EXP(log_duration) / 60 < 10  THEN '5–10 min'
    WHEN EXP(log_duration) / 60 < 20  THEN '10–20 min'
    WHEN EXP(log_duration) / 60 < 40  THEN '20–40 min'
    ELSE                                    'over 40 min'
  END AS trip_length_bucket,
  COUNT(*) AS num_trips,
  ROUND(AVG(EXP(log_duration) / 60), 2) AS avg_actual_minutes,
  ROUND(AVG(EXP(predicted_log_duration) / 60), 2) AS avg_predicted_minutes,
  ROUND(AVG(
    EXP(predicted_log_duration) / 60
    - EXP(log_duration) / 60
  ), 2) AS avg_error_minutes
FROM ML.PREDICT(
  MODEL `is3107-491906.ml_datasets.xgb_trip_duration_final_4`,
  (SELECT * FROM `is3107-491906.ml_datasets.trips_test_input_4`)
)
GROUP BY trip_length_bucket
ORDER BY MIN(EXP(log_duration) / 60);

-- Sanity Check
CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trip_predictions_sanity_check_4` AS
SELECT
  ROUND(log_duration, 4) AS actual_log,
  ROUND(predicted_log_duration, 4) AS predicted_log,
  ROUND(EXP(log_duration), 1) AS actual_seconds,
  ROUND(EXP(predicted_log_duration), 1) AS predicted_seconds,
  ROUND(EXP(log_duration) / 60, 2) AS actual_minutes,
  ROUND(EXP(predicted_log_duration) / 60, 2) AS predicted_minutes,
  ROUND(
    EXP(predicted_log_duration) / 60
    - EXP(log_duration) / 60
  , 2) AS error_minutes,
  ROUND(
    ABS(EXP(predicted_log_duration) - EXP(log_duration))
    / EXP(log_duration) * 100
  , 2) AS abs_pct_error
FROM ML.PREDICT(
  MODEL `is3107-491906.ml_datasets.xgb_trip_duration_final_4`,
  (SELECT * FROM `is3107-491906.ml_datasets.trips_test_input_4`)
)
LIMIT 40;

