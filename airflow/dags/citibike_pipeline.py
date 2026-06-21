from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import time
import requests

# =====================================================
# CONFIG
# =====================================================

DATABRICKS_HOST = "https://dbc-0237b708-5197.cloud.databricks.com"

DATABRICKS_TOKEN = "dXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

BRONZE_JOB_ID = 797820579539048
SILVER_JOB_ID = 859024956695062
GOLD_JOB_ID = 919169994459565
EXPORT_JOB_ID = 396881423923017
DOWNLOAD_PREP_JOB_ID = 761922598275032

OUTPUT_FILE = "/opt/airflow/data/output/gold_rides.csv"

LOCAL_RAW_FOLDER = "/opt/airflow/data/raw"

LOCAL_OUTPUT_FILE = "/opt/airflow/data/output/gold_rides.csv"

DBFS_RAW_FOLDER = "/Volumes/citibike/default/layers/raw_files/"

DBFS_OUTPUT_FOLDER = "/Volumes/citibike/default/layers/gold/gold_rides_export/"

# =====================================================
# UPLOAD FILES
# =====================================================

def upload_files():

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    uploaded = 0
    skipped = 0

    for file_name in os.listdir(LOCAL_RAW_FOLDER):

        file_path = os.path.join(
            LOCAL_RAW_FOLDER,
            file_name
        )

        if not os.path.isfile(file_path):
            print(f"Skipping directory: {file_name}")
            continue

        if not file_name.lower().endswith(".csv"):
            print(f"Skipping file: {file_name}")
            continue

        if file_name == "gold_rides.csv":
            print(f"Skipping pipeline output file: {file_name}")
            continue

        # Format clean path syntax for native Files API endpoint
        volume_path = f"{DBFS_RAW_FOLDER.rstrip('/')}/{file_name}"
        url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_path}"

        # ADDED: Check if file already exists in Databricks Volume
        try:
            check_response = requests.head(url, headers=headers)
            if check_response.status_code == 200:
                print(f"File '{file_name}' already exists in Databricks volume. Skipping upload.")
                skipped += 1
                continue
        except Exception as e:
            print(f"Could not verify existence of {file_name}: {e}. Proceeding with upload.")

        print(f"Uploading {file_name} to Unity Catalog Volume path: {volume_path}...")
        
        # Stream raw binary bytes directly instead of using Base64 strings
        with open(file_path, "rb") as f:
            response = requests.put(
                url,
                headers=headers,
                data=f
            )

        response.raise_for_status()

        uploaded += 1
        print(f"Successfully uploaded {file_name}")

    print(f"Task summary -> Total uploaded: {uploaded} | Total skipped: {skipped}")

# =====================================================
# RUN JOB AND WAIT
# =====================================================

def run_job_and_wait(job_id):

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers=headers,
        json={
            "job_id": job_id
        }
    )

    response.raise_for_status()

    run_id = response.json()["run_id"]

    print(f"Started Run ID: {run_id}")

    while True:

        response = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=headers,
            params={
                "run_id": run_id
            }
        )

        response.raise_for_status()

        data = response.json()

        state = data["state"]

        life_cycle = state["life_cycle_state"]

        result = state.get(
            "result_state",
            "RUNNING"
        )

        print(
            f"Run ID={run_id} "
            f"State={life_cycle} "
            f"Result={result}"
        )

        if life_cycle == "TERMINATED":

            if result == "SUCCESS":
                print("Job completed successfully")
                return

            raise Exception(
                f"Job failed. Result={result}"
            )

        time.sleep(30)


# =====================================================
# DOWNLOAD CSV
# =====================================================

def download_gold_csv():
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    
    file_path = "/Volumes/citibike/default/layers/gold/gold_rides.csv"
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{file_path}"
    
    print(f"Downloading final file via Files API from: {file_path}")
    
    # Enable chunk streaming to accommodate production-sized files safely
    response = requests.get(
        url,
        headers=headers,
        stream=True
    )

    response.raise_for_status()

    os.makedirs(
        os.path.dirname(OUTPUT_FILE),
        exist_ok=True
    )

    with open(OUTPUT_FILE, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print(f"Saved to {OUTPUT_FILE}")

# =====================================================
# DAG
# =====================================================

with DAG(
    dag_id="citibike_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["citibike", "capstone"]
) as dag:

    upload = PythonOperator(
        task_id="upload_files",
        python_callable=upload_files
    )

    bronze = PythonOperator(
        task_id="bronze_load",
        python_callable=run_job_and_wait,
        op_args=[BRONZE_JOB_ID]
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_job_and_wait,
        op_args=[SILVER_JOB_ID]
    )

    gold = PythonOperator(
        task_id="gold_rides",
        python_callable=run_job_and_wait,
        op_args=[GOLD_JOB_ID]
    )

    export_gold = PythonOperator(
        task_id="export_gold",
        python_callable=run_job_and_wait,
        op_args=[EXPORT_JOB_ID]
    )

    prepare_download = PythonOperator(
        task_id="prepare_download_file",
        python_callable=run_job_and_wait,
        op_args=[DOWNLOAD_PREP_JOB_ID]
    )
    
    download_csv = PythonOperator(
        task_id="download_gold_csv",
        python_callable=download_gold_csv
    )

    (
        upload
        >> bronze
        >> silver
        >> gold
        >> export_gold
        >> prepare_download
        >> download_csv
    )
