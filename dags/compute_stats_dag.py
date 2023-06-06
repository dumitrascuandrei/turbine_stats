from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

import compute_turbine_stats
from datetime import datetime,timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id='Turbine_Stats',
         default_args=default_args,
         start_date=datetime(2023, 6, 6),
         schedule_interval='@daily',
         catchup=False) as dag:

    start_pipeline = DummyOperator(
        task_id='start_pipeline',
    )

    compute_stats = PythonOperator(
        task_id="compute_stats",
        python_callable=compute_turbine_stats.start
    )
    start_pipeline >> compute_stats
