-- ================================================================
-- 2) CORRELATION CHECK
-- ================================================================
SELECT
  CORR(actual_temp, apparent_temp) AS corr_actual_vs_apparent,
  CORR(snowfall, snow_depth) AS corr_snowfall_vs_snow_depth,
  CORR(euclidean_dist_m, manhattan_dist_m) AS corr_euclidean_vs_manhattan
FROM `is3107-491906.citibike.features`;

-- RESULT NOTES:
-- Correlation actual temp vs apparent temp = 0.99 
-- High multicollinearity
-- Decision: Drop apparent_temp in model training

-- RESULT NOTES:
-- Correlation snowfall depth vs snowfall = 0.06 
-- Low correlation
-- Decision: Keep both in model training

-- RESULT NOTES:
-- Correlation euclidean vs manhattan = 0.99 
-- High multicollinearity
-- Decision: Drop manhattan_dist_m in model training
