# Car Connectivity to BigQuery Data Pipeline

## 1. Overview

This project provides a serverless data pipeline on Google Cloud Platform (GCP) to automatically collect and store data from your connected vehicle. It uses a Python-based Cloud Function to query your vehicle's API on a schedule and ingest the data into a BigQuery table. The core logic is powered by the [CarConnectivity](https://github.com/tillsteinbach/CarConnectivity) library, which provides a unified interface for various vehicle brands.

### Key Features & Benefits

* **Automated Data Logging**: Set up a "set it and forget it" pipeline that runs on a recurring schedule (e.g., every 5 minutes) to build a rich historical dataset of your vehicle's status.

* **Data Analysis & Reporting**: With your data stored in BigQuery, you can easily build dashboards (using Looker Studio), run complex analytical queries, and gain insights into your driving habits, charging patterns, and vehicle health over time.

* **Foundation for Future Applications**: This dataset can serve as the backbone for other applications, such as a custom mobile app to view your car's history, a machine learning model to predict battery degradation, or integrations with home automation systems.

* **Secure & Scalable**: Leverages GCP best practices by using Secret Manager for credentials and a serverless, event-driven architecture that is both highly secure and cost-effective.

## 2. Getting Started

Follow these steps to deploy the data pipeline in your own Google Cloud project.

### Step 1: Set Up Your Local Environment & Code

1.  **Clone the Repository**: Clone this project from GitHub to your local machine or into your Google Cloud Shell environment.
    ```
    git clone [https://github.com/enigmaniac/carconnectivity_bigquery.git](https://github.com/enigmaniac/carconnectivity_bigquery.git)
    cd carconnectivity_bigquery
    ```

2.  **Open Cloud Shell**: The easiest way to proceed is by using [Google Cloud Shell](https://shell.cloud.google.com), a browser-based terminal with all necessary tools pre-installed.

### Step 2: Configure Your GCP Project

1.  **Set Your Project ID**: Tell the `gcloud` command-line tool which project to work on. Replace `your-project-id` with your actual GCP Project ID.
    ```
    gcloud config set project your-project-id
    ```

2.  **Enable Required APIs**: Run the following command to enable all the necessary services for this project.
    ```
    gcloud services enable \
      cloudfunctions.googleapis.com \
      cloudbuild.googleapis.com \
      pubsub.googleapis.com \
      cloudscheduler.googleapis.com \
      secretmanager.googleapis.com \
      bigquery.googleapis.com
    ```

### Step 3: Create the BigQuery Table

1.  **Create the Dataset**: This command creates a container for your table.
    ```
    bq mk --dataset car_data
    ```

2.  **Create the Table**: This command uses the provided `db_schema.json` file to create your table with the correct schema.
    ```
    bq mk --table car_data.vehicle_status ./db_schema.json
    ```

### Step 4: Secure Your Credentials

You must store your vehicle API username and password in Secret Manager.

1.  **Store Username**:
    ```
    echo "your-car-username" | gcloud secrets create CAR_API_USERNAME --replication-policy="automatic" --data-file=-
    ```

2.  **Store Password**:
    ```
    echo "your-secret-password" | gcloud secrets create CAR_API_PASSWORD --replication-policy="automatic" --data-file=-
    ```

### Step 5: Deploy the Cloud Function

1.  **Deploy**: Run this command from the root of the project directory. It will package your code and configuration files, and set up the function.
    ```
    gcloud functions deploy ingest-car-data \
      --gen2 \
      --runtime=python311 \
      --region=europe-west1 \
      --source=. \
      --entry-point=main \
      --trigger-topic=run-car-data-ingestion \
      --set-env-vars=GCP_PROJECT=$(gcloud config get-value project)
    ```

2.  **Grant Permissions**: After deployment, you need to grant the function's service account permission to access secrets and BigQuery.
    * Find your function's service account email in the Cloud Function's "Details" tab. For 2nd Gen functions, it's typically the **Compute Engine default service account** (`PROJECT_NUMBER-compute@developer.gserviceaccount.com`).
    * Go to the **IAM & Admin** page in the console and grant that service account the following two roles:
        * `Secret Manager Secret Accessor`
        * `BigQuery Data Editor`

### Step 6: Create the Scheduled Job

This Cloud Scheduler job will trigger your function every 5 minutes.

1.  **Create a Pub/Sub Topic**: This is the message queue that connects the scheduler to the function.
    ```
    gcloud pubsub topics create run-car-data-ingestion
    ```

2.  **Create the Scheduler Job**:
    ```
    gcloud scheduler jobs create pubsub ingest-car-data-scheduler \
      --schedule="*/5 * * * *" \
      --topic=run-car-data-ingestion \
      --message-body="Run" \
      --time-zone="Europe/Paris" \
      --location=europe-west1
    ```

Your data pipeline is now fully deployed and operational!

## 3. Troubleshooting

If you encounter issues, here are some steps to debug the function.

### Manually Triggering the Function

You can trigger the function outside of its schedule to test it.

```
gcloud pubsub topics publish run-car-data-ingestion --message="Manual test trigger"
```

After running this, check the function's logs in the Cloud Console under **Cloud Functions > `ingest-car-data` > Logs**.

### Local Debugging with Cloud Shell Editor

You can run and debug the function in an interactive session.

1.  **Open the Editor**: In Cloud Shell, run `cloudshell editor` and open your project folder.

2.  **Set Up the Environment**: Open a terminal in the editor (`Ctrl+` \`) and run:
    ```
    # Create and activate a virtual environment
    python3 -m venv .venv
    source .venv/bin/activate
    
    # Install dependencies
    pip install -r requirements.txt
    pip install functions-framework
    ```

3.  **Configure the Debugger**: Use the `launch.json` file provided in the `.vscode` directory. Make sure the `GCP_PROJECT` environment variable is set correctly within that file.

4.  **Start Debugging**:
    * Set a breakpoint in `main.py` by clicking next to a line number.
    * Go to the "Run and Debug" panel, select "Run Cloud Function," and click the green play button. The debugger will start and listen for a trigger.

5.  **Send a Fake Pub/Sub Event**: While the debugger is listening, open a *new* terminal and run the following `curl` command to simulate an event and hit your breakpoint.
    ```
    curl localhost:8080 \
      -X POST \
      -H "Content-Type: application/json" \
      -H "ce-id: 12345" \
      -H "ce-specversion: 1.0" \
      -H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
      -H "ce-source: //[pubsub.googleapis.com/projects/your-project-id/topics/run-car-data-ingestion](https://pubsub.googleapis.com/projects/your-project-id/topics/run-car-data-ingestion)" \
      -d '{"message": {"data": "VGVzdA=="}}'
    
