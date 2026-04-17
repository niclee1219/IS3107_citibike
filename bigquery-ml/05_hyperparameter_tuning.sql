-- ================================================================
-- 5) HYPERPARAMETER TUNING ON SAMPLED DATA
-- ================================================================

CREATE OR REPLACE MODEL `is3107-491906.ml_datasets.xgb_trip_duration_hpt_4`
OPTIONS (
  MODEL_TYPE = 'BOOSTED_TREE_REGRESSOR',
  INPUT_LABEL_COLS = ['log_duration'],
  DATA_SPLIT_METHOD = 'NO_SPLIT',
  TREE_METHOD = 'HIST', -- faster histogram-based splitting

  NUM_TRIALS = 10,
  MAX_PARALLEL_TRIALS = 3,
  HPARAM_TUNING_OBJECTIVES = ['MEAN_ABSOLUTE_ERROR'],

  MAX_ITERATIONS = 100,

  LEARN_RATE = HPARAM_RANGE(0.01, 0.2),
  MAX_TREE_DEPTH = HPARAM_RANGE(3, 8),
  L1_REG = HPARAM_RANGE(0.0, 2.0),
  L2_REG = HPARAM_RANGE(0.0, 5.0)
) AS
SELECT * FROM `is3107-491906.ml_datasets.trips_train_input_sampled_4`;
