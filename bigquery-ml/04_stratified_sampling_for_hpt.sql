-- ================================================================
-- 4) STRATIFIED SAMPLING FOR HYPERPARAMETER TUNING
-- ================================================================
-- Stratified on: month x is_weekend x hour_bucket x is_ebike x is_member
-- hour bucketed into 4 periods to keep strata manageable:
-- Each stratum sampled at 20% 
-- Deterministic via FARM_FINGERPRINT(ride_id)

CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trips_train_input_sampled_4` AS
SELECT * EXCEPT(ride_id, hour_bucket, rn, group_size)
FROM (
  SELECT *,
    CASE
      WHEN hour BETWEEN 0  AND 5  THEN 'night'
      WHEN hour BETWEEN 6  AND 10 THEN 'morning'
      WHEN hour BETWEEN 11 AND 15 THEN 'midday'
      ELSE                             'evening'
    END AS hour_bucket,
    ROW_NUMBER() OVER (
      PARTITION BY
        month,
        is_weekend,
        CASE
          WHEN hour BETWEEN 0  AND 5  THEN 'night'
          WHEN hour BETWEEN 6  AND 10 THEN 'morning'
          WHEN hour BETWEEN 11 AND 15 THEN 'midday'
          ELSE                             'evening'
        END,
        is_ebike,
        is_member
      ORDER BY FARM_FINGERPRINT(ride_id)
    ) AS rn,
    COUNT(*) OVER (
      PARTITION BY
        month,
        is_weekend,
        CASE
          WHEN hour BETWEEN 0  AND 5  THEN 'night'
          WHEN hour BETWEEN 6  AND 10 THEN 'morning'
          WHEN hour BETWEEN 11 AND 15 THEN 'midday'
          ELSE                             'evening'
        END,
        is_ebike,
        is_member
    ) AS group_size
  FROM `is3107-491906.ml_datasets.trips_train_input_4`
)
WHERE rn <= CEIL(group_size * 0.1);

-- Verify sample distribution
CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trips_train_input_sample_distribution_4` AS
SELECT
  month,
  is_weekend,
  CASE
    WHEN hour BETWEEN 0  AND 5  THEN 'night'
    WHEN hour BETWEEN 6  AND 10 THEN 'morning'
    WHEN hour BETWEEN 11 AND 15 THEN 'midday'
    ELSE                             'evening'
  END AS hour_bucket,
  is_ebike,
  is_member,
  COUNT(*) AS sampled_rows,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4) AS pct_of_sample
FROM `is3107-491906.ml_datasets.trips_train_input_sampled_4`
GROUP BY month, is_weekend, hour_bucket, is_ebike, is_member
ORDER BY month, is_weekend, hour_bucket, is_ebike, is_member;