from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"Dag: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}")

def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    logging.error(f"Dag: {dag_id}, Execution Date: {execution_date}, SLA Miss")

def log_task_status(context):
    task_instance = context['task_instance']
    logging.info(f"Task {task_instance.task_id} started")
    yield
    logging.info(f"Task {task_instance.task_id} ended")

def extract_bronze(**context):
    logging.info("Starting Bronze layer extraction")
    # Code to read CSVs and write to Bronze Parquet
    logging.info("Bronze layer extraction complete")
    raise Exception("Simulated failure")  # Remove for actual use

def transform_silver(**context):
    logging.info("Starting Silver layer transformation")
    # Code to clean, enrich, deduplicate and write to Silver Parquet
    logging.info("Silver layer transformation complete")
    raise Exception("Simulated failure")  # Remove for actual use

def build_gold(**context):
    logging.info("Starting Gold layer build")
    # Code to generate Gold aggregation tables
    logging.info("Gold layer build complete")
    raise Exception("Simulated failure")  # Remove for actual use

with DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
) as dag:

    extract_bronze_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        provide_context=True,
        on_failure_callback=on_failure_callback
    )

    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        provide_context=True,
        on_failure_callback=on_failure_callback
    )

    build_gold_task = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        provide_context=True,
        on_failure_callback=on_failure_callback
    )

    extract_bronze_task >> transform_silver_task >> build_gold_task
