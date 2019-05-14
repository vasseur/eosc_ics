import datetime as dt

from airflow import DAG
from airflow.utils import dates
from eosc import meta


default_args = {
    "owner": "eosc_ics",
    "start_date": dates.days_ago(1),
    # "retries": 1,
    # "retry_delay": dt.timedelta(minutes=5),
}

dag = DAG("xray", default_args=default_args, schedule_interval="@once")
meta.create_image_dag(dag)