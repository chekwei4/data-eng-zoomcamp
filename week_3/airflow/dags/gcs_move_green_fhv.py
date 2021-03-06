import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_move_green_fhv",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    #to organise file structure
    move_green_files_task = GCSToGCSOperator(
        task_id='move_green_files_task',
        source_bucket=BUCKET,
        source_object="raw/green_tripdata*.parquet",
        destination_bucket=BUCKET,
        destination_object="green/green_mv_tripdata",
        move_object=False
    )

    move_fhv_files_task = GCSToGCSOperator(
        task_id='move_fhv_files_task',
        source_bucket=BUCKET,
        source_object="raw/fhv*.parquet",
        destination_bucket=BUCKET,
        destination_object="fhv/fhv_mv_tripdata",
        move_object=False
    )

    #run below single task
    move_green_files_task
    move_fhv_files_task

    # gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
    #     task_id="gcs_2_bq_ext_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table_yellowtrip_2019",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/yellow/*"],
    #         },
    #     },
    # )

    # CREATE_PART_TBL_QUERY = f"CREATE OR REPLACE TABLE `de-zoomcamp-339108.trips_data_all.yellow_trip_data_partition_af` \
    #                         PARTITION BY \
    #                         DATE(tpep_pickup_datetime) AS \
    #                         SELECT * FROM `{BIGQUERY_DATASET}.external_table_yellowtrip_2019`"

    # bq_ext_2_partition_task = BigQueryInsertJobOperator(
    #     task_id="bq_ext_2_partition_task",
    #     configuration={
    #         "query": {
    #             "query": CREATE_PART_TBL_QUERY,
    #             "useLegacySql": False,
    #         }
    #     },
    # )


    # gcs_2_gcs_task >> gcs_2_bq_ext_task >> bq_ext_2_partition_task
    