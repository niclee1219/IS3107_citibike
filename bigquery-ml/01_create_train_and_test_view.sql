-- ================================================================
-- 1) CREATE TRAIN AND TEST VIEWS
-- ================================================================

CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trips_train` AS
SELECT *
FROM `is3107-491906.citibike.features`
WHERE EXTRACT(YEAR FROM started_at) = 2025;

CREATE OR REPLACE VIEW `is3107-491906.ml_datasets.trips_test` AS
SELECT *
FROM `is3107-491906.citibike.features`
WHERE EXTRACT(YEAR FROM started_at) = 2026;