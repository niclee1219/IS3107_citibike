-- ================================================================
-- 9) FEATURE IMPORTANCE
-- ================================================================

CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.feature_importance_4` AS
SELECT
  feature,
  importance_weight,
  importance_gain,
  importance_cover
FROM ML.FEATURE_IMPORTANCE(
  MODEL `is3107-491906.ml_datasets.xgb_trip_duration_final_4`
)
ORDER BY importance_weight DESC;
