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


# init_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
BUCKET = 'airflow-lkdezoomcamp'

nytaxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'RatecodeID': pd.Int64Dtype(),
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float, 
        'fare_amount': float, 
        'extra': float,
        'mta_tax': float,   
        'tip_amount': float,
        'tolls_amount': float,
        'ehail_fee': float, 
        'improvement_surcharge': float,
        'total_amount': float,
        'payment_type': pd.Int64Dtype(),
        'trip_type': pd.Int64Dtype(),
        'congestion_surcharge': float
    }

fhv_taxi_dtypes = {
        'dispatching_base_num': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'SR_Flag': pd.Int64Dtype(),
        'Affiliated_base_num': str
    }

parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
# parse_dates = ['pickup_datetime', 'dropOff_datetime']

def web_to_gcs(year, service, version='wget'):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        parquet_file_name = file_name.replace(".csv.gz", '.parquet')

        request_url = f"{init_url}{service}/{file_name}"
        # csv file_name
        # file_name = f"{service}_tripdata_{year}-{month}.parquet"
        if version == 'wget':
            subprocess.run(['wget', f'{request_url}'])

            table = pv.read_csv(file_name)
            pq.write_table(table, parquet_file_name)

            print(f"Local: {file_name}")

            # # upload it to gcs 
            upload_to_gcs(BUCKET, f"raw/{service}_taxi/{parquet_file_name}", parquet_file_name)
            print(f"uploaded to GCS: {service}/{parquet_file_name}")

            subprocess.run(['rm', f'{parquet_file_name}', f'{file_name}'])
            print(f"removing: {parquet_file_name} {file_name}")
        else:
           
            df = pd.read_csv(request_url, sep=',', compression='gzip', dtype=nytaxi_dtypes, parse_dates=parse_dates)
            print(df.dtypes)
            df.to_parquet(parquet_file_name)

            print(f"Local: {file_name}")

            # # upload it to gcs 
            upload_to_gcs(BUCKET, f"raw/{service}_taxi/{parquet_file_name}", parquet_file_name)
            print(f"uploaded to GCS: {service}/{parquet_file_name}")

            subprocess.run(['rm', f'{parquet_file_name}'])
            print(f"removing: {parquet_file_name}")

web_to_gcs('2019', 'green', version="abc")

web_to_gcs('2020', 'green', version="abc")