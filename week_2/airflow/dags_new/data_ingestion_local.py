from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

from ingest_script_for_dag import ingest_callable

dataset_file = "yellow_tripdata_'{{ execution_date.strftime(\"%Y-%m\") }}'.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
CSV_NAME = AIRFLOW_HOME + "/output_{{ execution_date.strftime(\"%Y-%m\") }}.csv"


host = os.getenv("PG_HOST")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
port = os.getenv("PG_PORT")
db = os.getenv("PG_DATABASE")
table_name = "airflow_yellow_taxi_{{ execution_date.strftime(\"%Y-%m\") }}"

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
)

with local_workflow:
    wget_task = BashOperator(
        task_id="wget_task_id", 
        bash_command=f"curl -sSL {dataset_url} > {CSV_NAME}"
        # bash_command="echo '{{ execution_date.strftime(\"%Y-%m\") }}'"
    )

    ingest_task = PythonOperator(
        task_id="ingest_task_id", 
        python_callable=ingest_callable,
        op_kwargs=dict(user=user, password=password, host=host, port=port, db=db, table=table_name, csv_file=CSV_NAME)
    )

    wget_task >> ingest_task
