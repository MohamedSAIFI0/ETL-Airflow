from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

default_args = {
    'owner' : 'saifi',
    'depends_on_past':False,
    'start_date':datetime(2025,3,4,15,35),
    'retries':2,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    'market-etl',
    default_args=default_args,
    description='ETL pipeline for stock data',
    schedule_interval='*/5 * * * *',
    catchup=False
)


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag = dag,
)

extract_task >> transform_task >> load_task