import os
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# Get the directory where the DAG file sits, then go one level up to the project root
dag_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(dag_dir, '..')
sys.path.insert(0, project_root) 

from scripts.extract import run_extract
from scripts.transform import transform_crypto_data
from scripts.load import load_to_data
from scripts.quality_check import run_quality_check
from scripts.analytics import load_to_analytics

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_market_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for Cryptocurrency Market data",
    # schedule="@daily",
    schedule="0 11 * * *",
    start_date=datetime(2026, 2, 1),
    end_date=datetime(2026, 12, 31),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "etl", "postgres"],
    fail_fast=True,
) as dag:

    def extract_task_callable(**context):
        data = run_extract()
        context["ti"].xcom_push(key="raw_data", value=data)

    def transform_task_callable(**context):
        raw_data = context["ti"].xcom_pull(task_ids="extract", key="raw_data")
        execution_date = context["ds"]
        df = transform_crypto_data(raw_data, execution_date)
        context["ti"].xcom_push(key="transformed_data", value=df)

    def load_task_callable(**context):
       data = context["ti"].xcom_pull(task_ids="transform", key="transformed_data")
       execution_date = context["ds"]
       load_to_data(data, execution_date)
    
    def load_task_analytics(**context):
        execution_date = context["ds"]
        load_to_analytics(execution_date)

    extract = PythonOperator(
        task_id="extract", 
        python_callable=extract_task_callable
    )

    transform = PythonOperator(
        task_id="transform", 
        python_callable=transform_task_callable
    )

    load = PythonOperator(
        task_id="load", 
        python_callable=load_task_callable
    )
    
    analytics = PythonOperator(
        task_id="analytics", 
        python_callable=load_task_analytics
        # python_callable=lambda **context: load_to_analytics(context["ds"])
    )
    
    quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=lambda **context: run_quality_check(context["ds"])
    )

    extract >> transform >> load >> analytics >> quality
