-- ================================================================
-- 6) INSPECT HPT TRIAL RESULTS
-- ================================================================

CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.hpt_results_4` AS
SELECT
  trial_id,
  hyperparameters.learn_rate,
  hyperparameters.max_tree_depth,
  hyperparameters.l1_reg,
  hyperparameters.l2_reg,
  hparam_tuning_evaluation_metrics.mean_absolute_error AS mae
FROM ML.TRIAL_INFO(MODEL `is3107-491906.ml_datasets.xgb_trip_duration_hpt_4`)
ORDER BY mae ASC
LIMIT 5;