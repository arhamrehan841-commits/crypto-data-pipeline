from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import csv
import os
import boto3

# ==================== DAG CONFIG ====================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='crypto_data_pipeline',
    default_args=default_args,
    description='Fetch crypto data, save locally, and upload to S3',
    schedule_interval='*/2 * * * *',  # every 2 minutes
    start_date=datetime(2025, 10, 3),
    catchup=False,
    max_active_runs=1
)

# ==================== TASK FUNCTION ====================
def fetch_and_upload_crypto_data():
    # ========== API CONFIG ==========
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False
    }

    # ========== FETCH DATA ==========
    response = requests.get(url, params=params)
    data = response.json()

    # ========== TIMESTAMP ==========
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ========== LOCAL PATHS ==========
    local_raw = "/tmp/data/raw"
    local_processed = "/tmp/data/processed"
    os.makedirs(local_raw, exist_ok=True)
    os.makedirs(local_processed, exist_ok=True)

    # JSON filename
    json_filename = f"{local_raw}/crypto_data_{timestamp}.json"
    with open(json_filename, "w") as f:
        json.dump(data, f, indent=4)

    # CSV filename
    csv_filename = f"{local_processed}/crypto_data_{timestamp}.csv"
    with open(csv_filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "symbol", "name", "current_price", "market_cap", "last_updated"])
        for coin in data:
            writer.writerow([
                coin["id"],
                coin["symbol"],
                coin["name"],
                coin["current_price"],
                coin["market_cap"],
                coin["last_updated"]
            ])

    # ========== UPLOAD TO S3 USING IAM ROLE ==========
    s3 = boto3.client("s3")  # automatically uses EC2 IAM role
    bucket_name = "cloud-data-pipeline-arham"

    # Upload raw JSON
    raw_key = f"raw/{os.path.basename(json_filename)}"
    s3.upload_file(json_filename, bucket_name, raw_key)
    print(f"☁️ Uploaded raw → s3://{bucket_name}/{raw_key}")

    # Upload processed CSV
    processed_key = f"processed/{os.path.basename(csv_filename)}"
    s3.upload_file(csv_filename, bucket_name, processed_key)
    print(f"☁️ Uploaded processed → s3://{bucket_name}/{processed_key}")

    # Update pointer file
    latest_pointer_key = "processed/latest_file.txt"
    s3.put_object(
        Bucket=bucket_name,
        Key=latest_pointer_key,
        Body=os.path.basename(csv_filename).encode("utf-8")
    )
    print(f"📌 Updated pointer file: s3://{bucket_name}/{latest_pointer_key} → {os.path.basename(csv_filename)}")

# ==================== DAG TASK ====================
fetch_task = PythonOperator(
    task_id='fetch_and_upload_crypto_data',
    python_callable=fetch_and_upload_crypto_data,
    dag=dag
)

fetch_task