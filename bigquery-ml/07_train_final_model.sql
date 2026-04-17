-- ================================================================
-- 7) FINAL MODEL ON FULL 50M ROWS
-- ================================================================

CREATE OR REPLACE MODEL `is3107-491906.ml_datasets.xgb_trip_duration_final_4`
OPTIONS (
  MODEL_TYPE = 'BOOSTED_TREE_REGRESSOR',
  INPUT_LABEL_COLS = ['log_duration'],
  DATA_SPLIT_METHOD = 'NO_SPLIT',
  TREE_METHOD = 'HIST',

  MAX_ITERATIONS = 300,

  -- hyperparmeters replaced with best trial value
  LEARN_RATE = 0.2,
  MAX_TREE_DEPTH = 8,      
  L1_REG = 0,    
  L2_REG = 0     
) AS
SELECT * EXCEPT(ride_id) 
FROM `is3107-491906.ml_datasets.trips_train_input_4`;
