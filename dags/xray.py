import datetime as dt

from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from eosc.connection import get_connections
from eosc.ftp import load_specimen_files
from eosc.imaging import process_specimen_files

# nb of workers to process images
IMAGE_WORKERS = 10

default_args = {
    "owner": "me",
    "start_date": dates.days_ago(1),
    # "retries": 1,
    # "retry_delay": dt.timedelta(minutes=5),
}


dag = DAG("xray", default_args=default_args, schedule_interval="@once")

for conn_id in get_connections("xray"):

    t1 = PythonOperator(
        task_id="get_specimen_files_{}".format(conn_id),
        python_callable=load_specimen_files,
        op_args=[conn_id],
        dag=dag,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="process_specimen_files_{}".format(conn_id),
        python_callable=process_specimen_files,
        op_args=[conn_id],
        provide_context=True,
        dag=dag
    )

    t1 >> t2
