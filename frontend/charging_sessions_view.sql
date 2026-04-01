-- Run this query in your BigQuery console to create the view.
CREATE OR REPLACE VIEW `car_data.charging_sessions` AS
WITH charging_state AS (
  SELECT 
    *,
    -- Based on sample.csv, the state is the literal string 'charging'.
    CASE WHEN is_charging = 'charging' THEN 1 ELSE 0 END AS is_charging_int
  FROM `car_data.vehicle_status`
),
session_markers AS (
  SELECT 
    *,
    CASE 
      WHEN is_charging_int = 1 AND LAG(is_charging_int) OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp) = 0 THEN 1 
      WHEN is_charging_int = 1 AND LAG(is_charging_int) OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp) IS NULL THEN 1
      ELSE 0 
    END AS is_session_start
  FROM charging_state
),
session_groups AS (
  SELECT 
    *,
    SUM(is_session_start) OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp) AS session_id
  FROM session_markers
  WHERE is_charging_int = 1
)
SELECT 
  vehicle_id,
  session_id,
  MIN(ingestion_timestamp) AS start_time,
  MAX(ingestion_timestamp) AS end_time,
  TIMESTAMP_DIFF(MAX(ingestion_timestamp), MIN(ingestion_timestamp), MINUTE) AS duration_minutes,
  ARRAY_AGG(soc ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_soc,
  ARRAY_AGG(soc ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_soc,
  (ARRAY_AGG(soc ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] - ARRAY_AGG(soc ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)]) / 100.0 * 77.0 AS kwh_added,
  MAX(charging_power) AS max_charging_power,
  ARRAY_AGG(charging_type IGNORE NULLS ORDER BY ingestion_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS charging_type,
  ARRAY_AGG(latitude ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS latitude,
  ARRAY_AGG(longitude ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS longitude
FROM session_groups
GROUP BY vehicle_id, session_id
-- Filter out any brief charging blips that are less than a minute.
HAVING duration_minutes > 1;