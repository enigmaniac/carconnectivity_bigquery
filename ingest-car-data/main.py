import os
import datetime
import json
from google.cloud import bigquery, secretmanager

from carconnectivity.carconnectivity import CarConnectivity

# --- Standard GCP libraries ---
secret_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()

# --- Project Configuration ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
BIGQUERY_DATASET = 'car_data'
BIGQUERY_TABLE = 'vehicle_status'

# --- Helper Functions (no changes) ---
def _get_secret(secret_id: str) -> str:
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()

def _build_config_with_secrets(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        config = json.load(f)
    if 'carConnectivity' in config and 'connectors' in config['carConnectivity']:
        for connector in config['carConnectivity']['connectors']:
            if 'config' in connector:
                for key, value in connector['config'].items():
                    if isinstance(value, str) and value.startswith("SECRET:"):
                        secret_id = value.split(":", 1)[1]
                        retrieved_secret = _get_secret(secret_id)
                        connector['config'][key] = retrieved_secret
    return config

def _insert_into_bigquery(rows: list):
    if not rows:
        print("No rows to insert.")
        return
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if not errors:
        print(f"Successfully loaded {len(rows)} rows into {table_id}")
    else:
        error_payload = {"message": "BigQuery insert errors", "errors": errors}
        print(json.dumps(dict(severity="ERROR", **error_payload)))


# --- Main Function ---
def main(event, context):
    try:
        config = _build_config_with_secrets('config.json')
        print("Initializing CarConnectivity with dynamic config...")
        cc = CarConnectivity(config=config)
        
        # Prerequisite: Fetch all data from the API to populate the garage and vehicles.
        cc.fetch_all()
        
        # Step 1: Retrieve the garage using get_garage()
        garage = cc.get_garage()

        if not garage:
            print("Successfully connected, but no garage object was returned.")
            return

        # Step 2: Retrieve the list of vehicles from the garage using list_vehicles().
        vehicles = garage.list_vehicles()
        
        if not vehicles:
            print("Garage retrieved, but it contains no vehicles.")
            return

        print(f"Found {len(vehicles)} vehicle VIN(s). Fetching full data for each.")

    except Exception as e:
        error_payload = {
            "message": "Failed during initialization or data fetch",
            "exception_type": type(e).__name__,
            "error": str(e)
        }
        print(json.dumps(dict(severity="ERROR", **error_payload)))
        raise e

    ingestion_time = datetime.datetime.now(datetime.timezone.utc)
    rows_to_insert = []
    
    # Step 3: Iterate over the list of vehicles
    for vehicle in vehicles:
        edrive = vehicle.get_electric_drive()
        soc = edrive.level.value if edrive else None

        # Step 4: Extract the relevant fields from the vehicle object.
        if vehicle:
            row = {
                "ingestion_timestamp": ingestion_time.isoformat(),
                "vehicle_id": vehicle.vin.value,
                "mileage": vehicle.odometer.value,
                "soc": soc,
                "charging_power": vehicle.charging.power.value,
                "charging_type": vehicle.charging.type.value.value,
                "is_charging": vehicle.charging.state.value.value,
                "is_online": vehicle.connection_state.value.value,
                "external_temperature": vehicle.outside_temperature.value - 273.15,
                "latitude": vehicle.position.latitude.value if vehicle.position else None,
                "longitude": vehicle.position.longitude.value if vehicle.position else None,
                "state": vehicle.state.value.value,
            }
            rows_to_insert.append(row)
    
    _insert_into_bigquery(rows_to_insert)