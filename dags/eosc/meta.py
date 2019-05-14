import datetime as dt

from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from eosc.connection import get_connections
from eosc.ftp import load_specimen_files, create_dirs
from eosc.imaging import process_specimen_files

def create_image_dag(dag):

    for conn_id in get_connections(dag.dag_id):

        t1 = PythonOperator(
            task_id="create_dirs_{}".format(conn_id),
            python_callable=create_dirs,
            op_args=[conn_id],
            dag=dag,
            provide_context=True
        )

        t2 = PythonOperator(
            task_id="get_specimen_files_{}".format(conn_id),
            python_callable=load_specimen_files,
            op_args=[conn_id],
            dag=dag,
            provide_context=True
        )

        t3 = PythonOperator(
            task_id="process_specimen_files_{}".format(conn_id),
            python_callable=process_specimen_files,
            op_args=[conn_id],
            provide_context=True,
            dag=dag
        )

        t1 >> t2 >> t3

    return dag
