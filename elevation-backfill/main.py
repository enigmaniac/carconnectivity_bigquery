import os
import time
import requests
import re
import google.auth
from google.cloud import bigquery

PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = 'car_data'
TABLE_ID = 'vehicle_status'

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
    
    # Select DISTINCT coordinates missing altitude. Limit to 1000 unique locations per run 
    # to manage execution time and API limits, allowing multiple runs to process large backfills.
    query = f"""
        SELECT DISTINCT ROUND(latitude, 5) as latitude, ROUND(longitude, 5) as longitude
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE altitude IS NULL 
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

    # Process in chunks of 100 to respect standard Open-Elevation API limits
    chunk_size = 100
    total_updated_locations = 0

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        
        locations_payload = {"locations": [{"latitude": r.latitude, "longitude": r.longitude} for r in chunk]}
        
        api_url = "https://api.open-elevation.com/api/v1/lookup"
        try:
            response = requests.post(api_url, json=locations_payload, timeout=30)
            response.raise_for_status()
            elevation_results = response.json().get('results', [])
        except requests.exceptions.RequestException as e:
            print(f"Error fetching elevation data in bulk for chunk: {e}")
            continue

        if not elevation_results:
            print("API returned no elevation results for this chunk.")
            continue

        # Construct MERGE statement mapping exact coordinates back to their new altitudes
        merge_sql_parts = []
        for original_row, result in zip(chunk, elevation_results):
            altitude = round(result['elevation'], 2)
            part = f"SELECT CAST({original_row.latitude} AS FLOAT64) as latitude, CAST({original_row.longitude} AS FLOAT64) as longitude, {altitude} as altitude"
            merge_sql_parts.append(part)

        # Update ALL rows in the table that match these unique coordinates.
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
                UPDATE SET altitude = S.altitude;
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