import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

execution_month = '{{ execution_date.strftime(\'%Y-%m\') }}'

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

green_taxi_parquet_file_name = f'green_tripdata_{execution_month}.parquet'

green_taxi_url = URL_PREFIX + green_taxi_parquet_file_name
green_taxi_parquet_file_local = path_to_local_home + green_taxi_parquet_file_name
green_taxi_gcs_file = f'raw/green_taxi/{green_taxi_parquet_file_name}'

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def download_format_upload_gcs_dag(dag, dataset_url, parquet_file_local, gcs_file):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {dataset_url} > {parquet_file_local}"
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_file,
                "local_file": parquet_file_local,
            },
        )

        remove_dataset_task = BashOperator(
            task_id="remove_dataset_task",
            bash_command=f"rm {parquet_file_local}"
        )

        download_dataset_task >> local_to_gcs_task >> remove_dataset_task

default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

green_taxi_data_dag = DAG(
    dag_id = "green_taxi_data_monthly_dag_v1",
    schedule_interval = "0 10 2 * *",
    default_args = default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    catchup = True,
    max_active_runs = 1,
    tags=['dtc-de'],
)

download_format_upload_gcs_dag(green_taxi_data_dag, green_taxi_url, green_taxi_parquet_file_local, green_taxi_gcs_file)