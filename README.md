
# 🚀 Crypto Data Pipeline

End-to-end crypto data pipeline using Airflow, Python, S3, Snowflake, and Power BI

## Overview
This Airflow DAG fetches crypto data from CoinGecko, stores raw JSON in S3, transforms it to CSV, loads into Snowflake, and shows it in Power BI.

## Folder Structure
dags/
  └── crypto_pipeline_dag.py
config/
  └── credentials_sample.env
requirements.txt
README.md
.gitignore

## How to Run
1. Install dependencies: `pip install -r requirements.txt`
2. Put your real credentials in `.env` (never upload real keys)
3. Add DAG to Airflow and trigger it
