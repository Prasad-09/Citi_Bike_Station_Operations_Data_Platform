from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import os
import time
import requests
import boto3

# =====================================================
# CONFIGURATION
# =====================================================

# Databricks Config
DATABRICKS_HOST = "https://dbc-0237b708-5197.cloud.databricks.com"
DATABRICKS_TOKEN = "dapif0bd33165a47d93c4775631acaafc0f1"

BRONZE_JOB_ID = 797820579539048
SILVER_JOB_ID = 859024956695062
GOLD_JOB_ID = 919169994459565
EXPORT_JOB_ID = 396881423923017
DOWNLOAD_PREP_JOB_ID = 761922598275032

LOCAL_RAW_FOLDER = "/opt/airflow/data/raw"
DBFS_RAW_FOLDER = "/Volumes/citibike/default/layers/raw_files/"

# Amazon S3 Config 
AWS_ACCESS_KEY_ID = "AKIAROZSXIAPXAYVOZGY"
AWS_SECRET_ACCESS_KEY = "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"
AWS_REGION = "ap-south-1" 
S3_BUCKET_NAME = "citibikeprojecthv"
S3_TARGET_KEY = "gold_rides/gold_rides.csv" 
# =====================================================
# UPLOAD RAW LOCAL FILES TO DATABRICKS
# =====================================================

def upload_files():
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    uploaded, skipped = 0, 0

    for file_name in os.listdir(LOCAL_RAW_FOLDER):
        file_path = os.path.join(LOCAL_RAW_FOLDER, file_name)

        if not os.path.isfile(file_path) or not file_name.lower().endswith(".csv"):
            continue

        if file_name == "gold_rides.csv":
            continue

        volume_path = f"{DBFS_RAW_FOLDER.rstrip('/')}/{file_name}"
        url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_path}"

        try:
            check_response = requests.head(url, headers=headers)
            if check_response.status_code == 200:
                print(f"File '{file_name}' already exists in Databricks. Skipping.")
                skipped += 1
                continue
        except Exception as e:
            print(f"Could not verify existence: {e}")

        print(f"Uploading {file_name} to Databricks Volume...")
        with open(file_path, "rb") as f:
            response = requests.put(url, headers=headers, data=f)
        response.raise_for_status()
        uploaded += 1

    print(f"Upload Summary -> Uploaded: {uploaded} | Skipped: {skipped}")

# =====================================================
# RUN DATABRICKS JOB AND WAIT
# =====================================================

def run_job_and_wait(job_id):
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": job_id}
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    print(f"Started Run ID: {run_id}")

    while True:
        response = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id}
        )
        response.raise_for_status()
        state = response.json()["state"]
        life_cycle = state["life_cycle_state"]
        result = state.get("result_state", "RUNNING")

        print(f"Run ID={run_id} State={life_cycle} Result={result}")

        if life_cycle == "TERMINATED":
            if result == "SUCCESS":
                return
            raise Exception(f"Job failed. Result={result}")
        time.sleep(30)

# =====================================================
# STREAM FROM DATABRICKS DIRECTLY TO AMAZON S3
# =====================================================

def upload_gold_to_s3():
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    file_path = "/Volumes/citibike/default/layers/gold/gold_rides.csv"
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{file_path}"
    
    # Secure isolated temporary execution directory path inside the container
    local_staging_path = "/tmp/gold_rides_staging.csv"
    os.makedirs(os.path.dirname(local_staging_path), exist_ok=True)
    
    print(f"Streaming data from Databricks Volume: {file_path}")
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()

    # Save data incrementally to local staging path
    with open(local_staging_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                
    print(f"Data staged successfully. Uploading to Amazon S3 bucket: '{S3_BUCKET_NAME}'...")
    
    # Initialize connection to AWS
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    # Upload the file directly to your S3 bucket target location
    s3_client.upload_file(
        Filename=local_staging_path,
        Bucket=S3_BUCKET_NAME,
        Key=S3_TARGET_KEY
    )
    
    print(f"Success! File available in S3 at: s3://{S3_BUCKET_NAME}/{S3_TARGET_KEY}")
    
    # Clean up container cache file to preserve memory and space
    if os.path.exists(local_staging_path):
        os.remove(local_staging_path)

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="citibike_to_s3_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["citibike", "capstone", "aws_s3"]
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
    
    upload_s3 = PythonOperator(
        task_id="upload_gold_to_s3",
        python_callable=upload_gold_to_s3
    )

    (
        upload
        >> bronze
        >> silver
        >> gold
        >> export_gold
        >> prepare_download
        >> upload_s3
    )