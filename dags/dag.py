from dotenv import load_dotenv
import os
import sys

load_dotenv()
work_dir = os.getenv('WORK_DIR')
sys.path.append(work_dir)

from datetime import timedelta
from datetime import datetime
from dags.etl import *
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 9), 
    'email': ['manuel@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'Workshop-2',
    default_args=default_args,
    description='workflow spotify stadistics',
    schedule_interval='@daily',  
) as dag:

    extract_grammy_task = PythonOperator(  
        task_id='extract_grammy_task',
        python_callable=extract_grammy,

    )

    transform_grammy_task = PythonOperator(
        task_id='transform_grammy_task',
        python_callable=transform_grammy,

        )

    extract_spotify_task = PythonOperator(
        task_id='extract_spotify_task',
        python_callable=read_spotify,


        )

    transform_spotify_task = PythonOperator(
        task_id='transform_spotify_task',
        python_callable=tranform_spotify,

        )

    merge_task = PythonOperator(
        task_id='merge_task',
        python_callable=merge_data,


        )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_merged_data,

        )

    store_task = PythonOperator(
        task_id='store_task',
        python_callable=store_data,

        )



    extract_grammy_task >> transform_grammy_task >> merge_task >> load_task >> store_task
    extract_spotify_task >> transform_spotify_task >> merge_task