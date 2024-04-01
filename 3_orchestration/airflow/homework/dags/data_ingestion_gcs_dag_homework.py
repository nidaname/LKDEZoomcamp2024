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

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

yellow_taxi_file_name = f'yellow_tripdata_{execution_month}.csv.gz'
yellow_taxi_parquet_file_name = yellow_taxi_file_name.replace('.csv.gz', '.parquet')

yellow_taxi_file_local = path_to_local_home + yellow_taxi_file_name
yellow_taxi_url = URL_PREFIX + "yellow/" + yellow_taxi_file_name
yellow_taxi_parquet_file_local = path_to_local_home + yellow_taxi_parquet_file_name
yellow_taxi_gcs_file = f'raw/yellow_taxi/{yellow_taxi_parquet_file_name}'


green_taxi_file_name = f'green_tripdata_{execution_month}.csv.gz'
green_taxi_parquet_file_name = green_taxi_file_name.replace('.csv.gz', '.parquet')

green_taxi_file_local = path_to_local_home + green_taxi_file_name
green_taxi_url = URL_PREFIX + "green/" + green_taxi_file_name
green_taxi_parquet_file_local = path_to_local_home + green_taxi_parquet_file_name
green_taxi_gcs_file = f'raw/green_taxi/{green_taxi_parquet_file_name}'

fhv_taxi_file_name = f'fhv_tripdata_{execution_month}.csv.gz'
fhv_taxi_parquet_file_name = fhv_taxi_file_name.replace('.csv.gz', '.parquet')

fhv_taxi_file_local = path_to_local_home + fhv_taxi_file_name

fhv_taxi_url = URL_PREFIX + "fhv/" + fhv_taxi_file_name
fhv_taxi_parquet_file_local = path_to_local_home + fhv_taxi_parquet_file_name
fhv_taxi_gcs_file = f'raw/fhv_taxi/{fhv_taxi_parquet_file_name}'

zones_file_name = 'taxi_zone_lookup.csv'
zones_parquet_file_name = zones_file_name.replace('.csv', '.parquet')

zones_file_local = path_to_local_home + zones_file_name
zones_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/{zones_file_name}'
zones_parquet_file_local = path_to_local_home + zones_parquet_file_name
zones_gcs_file = f'raw/zones/{zones_parquet_file_name}'

# unsure why pyarrow is giving OSError: zlib inflate failed: incorrect header check
# only happens in this docker container
def format_to_parquet(src_file, dest_file):
    if src_file.endswith('.csv') or src_file.endswith('.csv.gz'):
        table = pv.read_csv(src_file)
        pq.write_table(table, dest_file)
    else:
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    
# def format_to_parquet(src_file, dest_file):
#     if src_file.endswith('.csv') or src_file.endswith('.csv.gz'):
#         table = pd.read_csv(src_file, compression="gzip", sep=',')
#         table.to_parquet(dest_file)
#     else:
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
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


def download_format_upload_gcs_dag(dag, dataset_url, dataset_file_local, parquet_file_local, gcs_file):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {dataset_url} > {dataset_file_local}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": dataset_file_local,
                "dest_file": parquet_file_local
            },
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
            bash_command=f"rm {dataset_file_local} {parquet_file_local}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_dataset_task

default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

yellow_taxi_data_dag = DAG(
    dag_id = "yellow_taxi_data_dag_v2",
    schedule_interval = "0 8 2 * *",
    default_args = default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    catchup = True,
    max_active_runs = 1,
    tags=['dtc-de'],
)

download_format_upload_gcs_dag(yellow_taxi_data_dag, yellow_taxi_url, yellow_taxi_file_local, yellow_taxi_parquet_file_local, yellow_taxi_gcs_file)

fhv_taxi_data_dag = DAG(
    dag_id = "fhv_taxi_data_dag_v1",
    schedule_interval = "0 7 2 * *",
    default_args = default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    catchup = True,
    max_active_runs = 1,
    tags=['dtc-de'],
)

download_format_upload_gcs_dag(fhv_taxi_data_dag, fhv_taxi_url, fhv_taxi_file_local, fhv_taxi_parquet_file_local, fhv_taxi_gcs_file)

zones_data_dag = DAG(
    dag_id = "zones_data_dag_v1",
    schedule_interval = "@once",
    default_args = default_args,
    start_date=datetime(2019, 1, 1),
    # end_date=datetime(2020, 1, 1),
    catchup = True,
    max_active_runs = 1,
    tags=['dtc-de'],
)

download_format_upload_gcs_dag(zones_data_dag, zones_url, zones_file_local, zones_parquet_file_local, zones_gcs_file)

green_taxi_data_dag = DAG(
    dag_id = "green_taxi_data_dag_v1",
    schedule_interval = "0 10 2 * *",
    default_args = default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    catchup = True,
    max_active_runs = 1,
    tags=['dtc-de'],
)

download_format_upload_gcs_dag(green_taxi_data_dag, green_taxi_url, green_taxi_file_local, green_taxi_parquet_file_local, green_taxi_gcs_file)