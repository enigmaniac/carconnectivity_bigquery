-- Run this query in your BigQuery console to create the view.
CREATE OR REPLACE VIEW `car_data.trips` AS
WITH state_changes AS (
  SELECT 
    *,
    -- Grab the most recent known location. Include timestamp to prevent leaking old locations across drives.
    LAST_VALUE(CASE WHEN latitude IS NOT NULL THEN STRUCT(ingestion_timestamp as loc_time, latitude, longitude, altitude, city, country) END IGNORE NULLS) OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS start_loc_carry,
    -- Grab the next known location at or after this moment (car parked after trip)
    FIRST_VALUE(CASE WHEN latitude IS NOT NULL THEN STRUCT(ingestion_timestamp as loc_time, latitude, longitude, altitude, city, country) END IGNORE NULLS) OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS end_loc_carry,
    -- Mark a '1' when the car's state changes to 'ignition_on'. This is the start of a driving SEGMENT.
    CASE 
      WHEN state = 'ignition_on' AND LAG(state, 1, 'parked') OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp) != 'ignition_on' THEN 1
      ELSE 0 
    END AS is_segment_start
  FROM `car_data.vehicle_status`
),
segment_groups AS (
  SELECT 
    *,
    -- Create a running total to give each segment a unique ID
    SUM(is_segment_start) OVER (PARTITION BY vehicle_id ORDER BY ingestion_timestamp) AS segment_id
  FROM state_changes
  WHERE state = 'ignition_on'
),
segments AS (
  SELECT 
    vehicle_id,
    segment_id,
    MIN(ingestion_timestamp) AS segment_start_time,
    MAX(ingestion_timestamp) AS segment_end_time,
    TIMESTAMP_DIFF(MAX(ingestion_timestamp), MIN(ingestion_timestamp), MINUTE) AS duration_minutes,
    MAX(mileage) - MIN(mileage) AS distance_km,
    ARRAY_AGG(soc ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_soc,
    ARRAY_AGG(soc ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_soc,
    AVG(external_temperature) AS avg_external_temp,
    ARRAY_AGG(start_loc_carry.loc_time ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_loc_time,
    ARRAY_AGG(start_loc_carry.latitude ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_latitude,
    ARRAY_AGG(start_loc_carry.longitude ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_longitude,
    ARRAY_AGG(start_loc_carry.altitude ORDER BY ingestion_timestamp ASC LIMIT 1)[OFFSET(0)] AS start_altitude,
    ARRAY_AGG(start_loc_carry.city ORDER BY ingestion_timestamp ASC LIMIT 1)[SAFE_OFFSET(0)] AS start_city,
    ARRAY_AGG(start_loc_carry.country ORDER BY ingestion_timestamp ASC LIMIT 1)[SAFE_OFFSET(0)] AS start_country,
    ARRAY_AGG(end_loc_carry.loc_time ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_loc_time,
    ARRAY_AGG(end_loc_carry.latitude ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_latitude,
    ARRAY_AGG(end_loc_carry.longitude ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_longitude,
    ARRAY_AGG(end_loc_carry.altitude ORDER BY ingestion_timestamp DESC LIMIT 1)[OFFSET(0)] AS end_altitude,
    ARRAY_AGG(end_loc_carry.city ORDER BY ingestion_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS end_city,
    ARRAY_AGG(end_loc_carry.country ORDER BY ingestion_timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS end_country
  FROM segment_groups
  GROUP BY vehicle_id, segment_id
  -- Filter out tiny glitches
  HAVING distance_km > 0.1
),
trip_markers AS (
  SELECT 
    *,
    -- Prevent location leak: If the carried location timestamp is older than the previous segment ended, it leaked across a drive.
    CASE WHEN start_loc_time <= LAG(segment_end_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE start_latitude END AS clean_start_lat,
    CASE WHEN start_loc_time <= LAG(segment_end_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE start_longitude END AS clean_start_lon,
    CASE WHEN start_loc_time <= LAG(segment_end_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE start_altitude END AS clean_start_alt,
    CASE WHEN start_loc_time <= LAG(segment_end_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE start_city END AS clean_start_city,
    CASE WHEN start_loc_time <= LAG(segment_end_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE start_country END AS clean_start_country,
    
    -- Prevent end location leak: If it's newer than the next segment starts, it leaked across a future drive.
    CASE WHEN end_loc_time >= LEAD(segment_start_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE end_latitude END AS clean_end_lat,
    CASE WHEN end_loc_time >= LEAD(segment_start_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE end_longitude END AS clean_end_lon,
    CASE WHEN end_loc_time >= LEAD(segment_start_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE end_altitude END AS clean_end_alt,
    CASE WHEN end_loc_time >= LEAD(segment_start_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE end_city END AS clean_end_city,
    CASE WHEN end_loc_time >= LEAD(segment_start_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) THEN NULL ELSE end_country END AS clean_end_country,
    
    -- Group segments into a single trip if the gap between them is greater than 2 hours (120 minutes).
    CASE 
      WHEN TIMESTAMP_DIFF(segment_start_time, LAG(segment_end_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time), MINUTE) > 120 THEN 1
      WHEN LAG(segment_end_time) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) IS NULL THEN 1
      ELSE 0
    END AS is_trip_start
  FROM segments
),
trips_assigned AS (
  SELECT 
    *,
    SUM(is_trip_start) OVER (PARTITION BY vehicle_id ORDER BY segment_start_time) AS trip_id
  FROM trip_markers
),
trip_metrics AS (
  SELECT 
    *,
    -- Calculate total SoC drop for the entire trip by summing the drops of individual segments.
    -- This cleanly sidesteps any SoC increases caused by mid-trip charging sessions.
    SUM(start_soc - end_soc) OVER (PARTITION BY vehicle_id, trip_id) AS trip_soc_drop,
    SUM(distance_km) OVER (PARTITION BY vehicle_id, trip_id) AS trip_distance_km
  FROM trips_assigned
),
segment_kwh AS (
  SELECT 
    *,
    -- Trip-level consumption rule: >= 4% SoC drop uses battery math, < 4% uses distance assumption
    CASE 
      WHEN trip_soc_drop >= 4 THEN (trip_soc_drop / 100.0) * 77.0
      ELSE (trip_distance_km / 100.0) * 16.0 
    END AS trip_kwh_consumed
  FROM trip_metrics
),
final_segments AS (
  SELECT
    *,
    -- Segment-level consumption rule
    CASE
      WHEN (start_soc - end_soc) >= 4 THEN ((start_soc - end_soc) / 100.0) * 77.0
      ELSE COALESCE(distance_km * (trip_kwh_consumed / NULLIF(trip_distance_km, 0)), 0)
    END AS segment_kwh_consumed
  FROM segment_kwh
),
trip_summary AS (
  SELECT 
    vehicle_id,
    trip_id,
    MIN(segment_start_time) AS start_time,
    MAX(segment_end_time) AS end_time,
    TIMESTAMP_DIFF(MAX(segment_end_time), MIN(segment_start_time), MINUTE) AS total_duration_minutes,
    SUM(duration_minutes) AS driving_duration_minutes,
    MAX(trip_distance_km) AS distance_km,
    
    ARRAY_AGG(start_soc ORDER BY segment_start_time ASC LIMIT 1)[OFFSET(0)] AS start_soc,
    ARRAY_AGG(end_soc ORDER BY segment_start_time DESC LIMIT 1)[OFFSET(0)] AS end_soc,
    
    MAX(trip_kwh_consumed) AS kwh_consumed,
    MAX(trip_soc_drop) < 4 AS is_consumption_estimated,
    
    AVG(avg_external_temp) AS avg_external_temp,
    ARRAY_AGG(clean_start_lat ORDER BY segment_start_time ASC LIMIT 1)[OFFSET(0)] AS start_latitude,
    ARRAY_AGG(clean_start_lon ORDER BY segment_start_time ASC LIMIT 1)[OFFSET(0)] AS start_longitude,
    ARRAY_AGG(clean_end_lat ORDER BY segment_start_time DESC LIMIT 1)[OFFSET(0)] AS end_latitude,
    ARRAY_AGG(clean_end_lon ORDER BY segment_start_time DESC LIMIT 1)[OFFSET(0)] AS end_longitude,
    ARRAY_AGG(clean_start_alt ORDER BY segment_start_time ASC LIMIT 1)[OFFSET(0)] AS start_altitude,
    ARRAY_AGG(clean_end_alt ORDER BY segment_start_time DESC LIMIT 1)[OFFSET(0)] AS end_altitude,
    ARRAY_AGG(clean_start_city ORDER BY segment_start_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS start_city,
    ARRAY_AGG(clean_start_country ORDER BY segment_start_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS start_country,
    ARRAY_AGG(clean_end_city ORDER BY segment_start_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS end_city,
    ARRAY_AGG(clean_end_country ORDER BY segment_start_time DESC LIMIT 1)[SAFE_OFFSET(0)] AS end_country,
    COUNT(segment_id) AS segment_count,
    
    -- Package segments into a nested array
    ARRAY_AGG(STRUCT(
      segment_id,
      segment_start_time AS start_time,
      segment_end_time AS end_time,
      duration_minutes,
      distance_km,
      start_soc,
      end_soc,
      segment_kwh_consumed AS kwh_consumed,
      clean_start_lat AS start_latitude,
      clean_start_lon AS start_longitude,
      clean_start_city AS start_city,
      clean_start_country AS start_country,
      clean_end_lat AS end_latitude,
      clean_end_lon AS end_longitude,
      clean_end_city AS end_city,
      clean_end_country AS end_country,
      avg_external_temp,
      clean_start_alt AS start_altitude,
      clean_end_alt AS end_altitude
    ) ORDER BY segment_start_time ASC) AS segments
  FROM final_segments
  GROUP BY vehicle_id, trip_id
),
trip_charges AS (
  SELECT 
    t.vehicle_id,
    t.trip_id,
    SUM(c.duration_minutes) AS charging_duration_minutes,
    ARRAY_AGG(
      STRUCT(c.session_id, c.start_time, c.end_time, c.duration_minutes, c.start_soc, c.end_soc, c.kwh_added, c.max_charging_power, c.charging_type, c.city, c.country)
      ORDER BY c.start_time ASC
    ) AS charge_sessions
  FROM trip_summary t
  INNER JOIN `car_data.charging_sessions` c 
    ON c.vehicle_id = t.vehicle_id AND c.start_time BETWEEN t.start_time AND t.end_time
  GROUP BY t.vehicle_id, t.trip_id
)
-- Final output: Combine trip summary with aggregated charge sessions using a LEFT JOIN
SELECT 
  t.*,
  COALESCE(tc.charging_duration_minutes, 0) AS charging_duration_minutes,
  GREATEST(0, t.total_duration_minutes - t.driving_duration_minutes - COALESCE(tc.charging_duration_minutes, 0)) AS parked_duration_minutes,
  tc.charge_sessions
FROM trip_summary t
LEFT JOIN trip_charges tc 
  ON t.vehicle_id = tc.vehicle_id AND t.trip_id = tc.trip_id;