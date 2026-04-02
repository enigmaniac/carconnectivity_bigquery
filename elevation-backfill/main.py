import os
import time
import requests
import re
import google.auth
from google.cloud import bigquery

PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = 'car_data'
TABLE_ID = 'vehicle_status'

def escape_bq_string(val):
    """Escapes backslashes, newlines, and single quotes to prevent BigQuery syntax errors."""
    if not val:
        return "Unknown"
    cleaned = str(val).replace('\n', ', ').replace('\r', '')
    return cleaned.replace('\\', '\\\\').replace("'", "\\'")

def main(event, context):
    """Cloud Function entry point to periodically backfill missing elevation data."""
    global PROJECT_ID
    
    # Fallback to the ambient Google Cloud environment project if not explicitly set
    if not PROJECT_ID:
        try:
            _, PROJECT_ID = google.auth.default()
        except Exception:
            pass
            
        if not PROJECT_ID:
            print("Could not determine GCP Project ID. Assuming local testing.")
            return

    # Validate PROJECT_ID to prevent SQL injection risks from modified environment variables
    if not re.match(r'^[a-z0-9-]+$', PROJECT_ID):
        raise ValueError(f"Invalid GCP_PROJECT format. Only lowercase letters, numbers, and hyphens are allowed.")

    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Add city and country columns if they don't exist
    try:
        schema_query = f"ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` ADD COLUMN IF NOT EXISTS city STRING, ADD COLUMN IF NOT EXISTS country STRING, ADD COLUMN IF NOT EXISTS state STRING, ADD COLUMN IF NOT EXISTS postcode STRING, ADD COLUMN IF NOT EXISTS road STRING"
        bq_client.query(schema_query).result()
    except Exception as e:
        print(f"Schema update notice: {e}")

    # Select DISTINCT coordinates missing altitude or geocoding
    # to manage execution time and API limits, allowing multiple runs to process large backfills.
    query = f"""
        SELECT DISTINCT ROUND(latitude, 5) as latitude, ROUND(longitude, 5) as longitude
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE (altitude IS NULL OR city IS NULL OR country IS NULL OR state IS NULL OR postcode IS NULL OR road IS NULL)
          AND latitude IS NOT NULL 
          AND longitude IS NOT NULL
          -- Add a time buffer to avoid operating on rows in the BigQuery streaming buffer.
          AND ingestion_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    """
    
    rows = list(bq_client.query(query).result())
    if not rows:
        print("No missing elevation data found. System is up to date.")
        return

    print(f"Found {len(rows)} unique coordinates missing altitude. Backfilling...")

    # Process in chunks of 10 to respect standard Open-Elevation API limits and geocoding time
    chunk_size = 10
    total_updated_locations = 0

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        
        locations_payload = {"locations": [{"latitude": r.latitude, "longitude": r.longitude} for r in chunk]}
        
        api_url = "https://api.open-elevation.com/api/v1/lookup"
        elevation_results = []
        for attempt in range(3):
            try:
                response = requests.post(api_url, json=locations_payload, timeout=45)
                response.raise_for_status()
                elevation_results = response.json().get('results', [])
                break
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} - Error fetching elevation data for chunk: {e}")
                if attempt < 2:
                    time.sleep(2)

        if not elevation_results:
            print("API returned no elevation results for this chunk. Proceeding with geocoding only.")
            # Provide dummy elevation data to allow reverse geocoding to continue
            elevation_results = [{'elevation': None} for _ in chunk]

        # Construct MERGE statement mapping exact coordinates back to their new altitudes and locations
        merge_sql_parts = []
        for original_row, result in zip(chunk, elevation_results):
            elev_val = result.get('elevation')
            altitude = round(elev_val, 2) if elev_val is not None else "CAST(NULL AS FLOAT64)"
            
            # Reverse geocoding via Nominatim
            city = "Unknown"
            country = "XX"
            state = "Unknown"
            postcode = "Unknown"
            road = "Unknown"
            try:
                nom_url = f"https://nominatim.openstreetmap.org/reverse?lat={original_row.latitude}&lon={original_row.longitude}&format=json"
                nom_resp = requests.get(nom_url, headers={'User-Agent': 'EV-Data-Pipeline/1.0 (ev-analytics)'}, timeout=10)
                if nom_resp.status_code == 200:
                    addr = nom_resp.json().get('address', {})
                    city = addr.get('city', addr.get('town', addr.get('village', addr.get('county', 'Unknown'))))
                    country = addr.get('country_code', 'XX').upper()
                    state = addr.get('state', addr.get('province', addr.get('region', 'Unknown')))
                    postcode = addr.get('postcode', 'Unknown')
                    road = addr.get('road', addr.get('pedestrian', 'Unknown'))
                time.sleep(1.1)  # Respect OpenStreetMap's 1 request/second limit
            except Exception as e:
                print(f"Geocoding failed for {original_row.latitude}, {original_row.longitude}: {e}")
            
            city_esc = escape_bq_string(city)
            country_esc = escape_bq_string(country)
            state_esc = escape_bq_string(state)
            postcode_esc = escape_bq_string(postcode)
            road_esc = escape_bq_string(road)
            
            part = f"SELECT CAST({original_row.latitude} AS FLOAT64) as latitude, CAST({original_row.longitude} AS FLOAT64) as longitude, {altitude} as altitude, '{city_esc}' as city, '{country_esc}' as country, '{state_esc}' as state, '{postcode_esc}' as postcode, '{road_esc}' as road"
            merge_sql_parts.append(part)

        # Update ALL rows in the table that match these unique coordinates
        # We use ROUND(..., 5) to safely match floats (approx 1 meter precision) 
        # and avoid tiny floating point representation mismatches during the join.
        merge_query = f"""
            MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
            USING ({' UNION ALL '.join(merge_sql_parts)}) S
            ON ROUND(T.latitude, 5) = ROUND(S.latitude, 5) 
               AND ROUND(T.longitude, 5) = ROUND(S.longitude, 5)
               -- Prevent the MERGE from attempting to update rows currently in the streaming buffer
               AND T.ingestion_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
            WHEN MATCHED THEN
            UPDATE SET 
                altitude = COALESCE(S.altitude, T.altitude), 
                city = COALESCE(NULLIF(S.city, 'Unknown'), T.city), 
                country = COALESCE(NULLIF(S.country, 'XX'), T.country), 
                state = COALESCE(NULLIF(S.state, 'Unknown'), T.state), 
                postcode = COALESCE(NULLIF(S.postcode, 'Unknown'), T.postcode), 
                road = COALESCE(NULLIF(S.road, 'Unknown'), T.road);
        """
        
        bq_client.query(merge_query).result()
        total_updated_locations += len(elevation_results)
        print(f"Processed chunk: updated records for {len(elevation_results)} unique locations.")
        
        # Polite pause between API requests
        time.sleep(1)

    print(f"Successfully backfilled missing elevations for {total_updated_locations} unique locations.")

if __name__ == "__main__":
    # This block allows the script to be run directly from the command line
    # for local testing and debugging. The 'event' and 'context' arguments
    # are not used in this execution path, so we pass None.
    main(event=None, context=None)