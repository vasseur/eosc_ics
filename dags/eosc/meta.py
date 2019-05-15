import datetime as dt

from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from eosc.connection import get_connections
from eosc.ftp import load_specimen_files, create_dirs
from eosc.imaging import process_specimen_files
from eosc.ml import train_model

def create_image_dag(dag):

    lasts = []
    for cx in get_connections(dag.dag_id):

        t1 = PythonOperator(
            task_id="create_dirs_{}".format(cx.conn_id),
            python_callable=create_dirs,
            op_args=[cx.conn_id],
            dag=dag,
            provide_context=True
        )

        t2 = PythonOperator(
            task_id="load_specimen_files_{}".format(cx.conn_id),
            python_callable=load_specimen_files,
            op_args=[cx.conn_id],
            dag=dag,
            provide_context=True
        )

        t3 = PythonOperator(
            task_id="process_specimen_files_{}".format(cx.conn_id),
            python_callable=process_specimen_files,
            op_args=[cx.conn_id],
            provide_context=True,
            dag=dag
        )

        t1 >> t2 >> t3
        lasts.append(t3)

    t4 = PythonOperator(
        task_id="train_ml_{}".format(dag.dag_id),
        python_callable=train_model,
        op_args=[dag.dag_id],
        dag=dag
    )

    for last in lasts:
        last >> t4

    return dag
