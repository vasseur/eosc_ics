import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from eosc.connection import create_connections


default_args = {
    "owner": "me",
    "start_date": dt.datetime(2019, 1, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}


dag = DAG("create_connections", default_args=default_args, schedule_interval="@once")

print_world = PythonOperator(
    task_id="create_connections",
    python_callable=create_connections,
    dag=dag
)
