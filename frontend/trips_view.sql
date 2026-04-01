-- Run this query in your BigQuery console to create the view.
CREATE OR REPLACE VIEW `car_data.trips` AS
WITH state_changes AS (
  SELECT 
    *,
    -- Mark a '1' when the car's state changes to 'ignition_on' from something else. This is the start of a trip.
    CASE 
      WHEN state = 'ignition_on' AND LAG(state, 1, 'parked') OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp) != 'ignition_on' THEN 1
      ELSE 0 
    END AS is_trip_start
  FROM `car_data.vehicle_status`
),
trip_groups AS (
  SELECT 
    *,
    -- Create a running total of the 'is_trip_start' markers. This gives each trip a unique ID.
    SUM(is_trip_start) OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp) AS trip_id
  FROM state_changes
  -- We only care about the data points where the car was actually on.
  WHERE state = 'ignition_on'
)
SELECT 
  vehicle_id,
  trip_id,
  MIN(ingestion_timestamp) AS start_time,
  MAX(ingestion_timestamp) AS end_time,
  TIMESTAMP_DIFF(MAX(ingestion_timestamp), MIN(ingestion_timestamp), MINUTE) AS duration_minutes,
  MAX(mileage) - MIN(mileage) AS distance_km,
  
  -- Arrays are used to safely grab the exact first and last SoC of the trip
  ARRAY_AGG(soc ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_soc,
  ARRAY_AGG(soc ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_soc,
  
  -- Energy Consumption Logic: 77kWh battery capacity
  -- Fallback to assumed efficiency (16 kWh/100km) for short trips where SoC change is <= 1%
  CASE 
    WHEN (ARRAY_AGG(soc ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] - ARRAY_AGG(soc ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)]) > 1 
    THEN (ARRAY_AGG(soc ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] - ARRAY_AGG(soc ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)]) / 100.0 * 77.0
    ELSE (MAX(mileage) - MIN(mileage)) / 100.0 * 16.0 
  END AS kwh_consumed,

  AVG(external_temperature) AS avg_external_temp,
  ARRAY_AGG(altitude ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_altitude,
  ARRAY_AGG(altitude ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_altitude
FROM trip_groups
GROUP BY vehicle_id, trip_id
-- Filter out any potential data glitches where a trip has no distance.
HAVING distance_km > 0.1;