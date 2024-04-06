import subprocess
import pandas as pd
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


init_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
BUCKET = 'airflow-lkdezoomcamp'

def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        request_url = f"{init_url}{file_name}"

        subprocess.run(['wget', f'{request_url}'])

        print(f"Local: {file_name}")

        # # upload it to gcs 
        upload_to_gcs(BUCKET, f"raw/{service}_taxi/{file_name}", file_name)
        print(f"uploaded to GCS: {service}/{file_name}")

        subprocess.run(['rm', f'{file_name}'])
        print(f"removing: {file_name}")

web_to_gcs('2019', 'green')