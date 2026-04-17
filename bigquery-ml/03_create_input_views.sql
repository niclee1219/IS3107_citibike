-- ================================================================
-- 3) CREATE INPUT VIEWS
-- ================================================================

-- DROPPED COLUMNS:
-- started_at: extracted into temporal features
-- rideable_type: redundant with is_ebike
-- origin/destt r9: too sparse
-- origin/dest r7: too coarse
-- od_pair_r9: too sparse
-- od_pair_r8: avoid redundant representation of origin-destination r/s
-- apparent_temp: highly correlated with actual_temp

CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trips_train_input_4` AS
SELECT
  ride_id,              -- kept for stratified sampling, excluded from model

  -- target
  log_duration,

  -- rider      
  is_member,
  is_ebike,

  -- time 
  hour,
  month,
  is_weekend,
  is_rush_hour,
  is_holiday,

  -- spatial
  origin_h3_r8,
  dest_h3_r8,
  od_encoded,

  -- distance
  euclidean_dist_m,
  dist_ratio,

  -- weather 
  actual_temp,
  precipitation_mm,
  windspeed,
  snowfall,
  snow_depth,
  precip_last_3h,
  mins_since_rain
FROM `is3107-491906.ml_datasets.trips_train`;

CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trips_test_input_4` AS
SELECT
  log_duration,    
  is_member,
  is_ebike,
  hour,
  month,
  is_weekend,
  is_rush_hour,
  is_holiday,
  origin_h3_r8,
  dest_h3_r8,
  od_encoded,
  euclidean_dist_m,
  dist_ratio,
  actual_temp,
  precipitation_mm,
  windspeed,
  snowfall,
  snow_depth,
  precip_last_3h,
  mins_since_rain
FROM `is3107-491906.ml_datasets.trips_test`;
