required to export GOOGLE_APPLICATION_CREDENTIALS before running `upload_to_gcs.py`

`export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/LKDEZoomcamp2024/4_datawarehouse/onyx-descent-417702-c2fbbb866ff5-magedezoomcamp.json"`

used `upload_to_gcs.py` to download and export to gcs bucket

note regions for bucket and dataset

note forbidden for files when wget with wrong url

note if parquet file is broken, when creating external table in BQ, it shows as one column `__xml_version__1_0__encoding__UTF_8___` 

# rmb to fix the dtypes
- from actual website, downloading parquet and upload to gcs. Doesn't work for green taxi since `ehail` is multiple dtypes
- download from github, .csv.gz, when uploaded as parquet and create external table, `lpep_*_datetimes` gets read as int
- ~~best way is download from github, fix dtypes on pandas, fix dtypes on  CREATE EXTERNAL STATEMENT~~
- just download the github csv.gz files, create external with `INT64` for datetimes, use below `CREATE` statement
```sql
CREATE OR REPLACE EXTERNAL TABLE `<>`       
  (VendorID INT64,
  lpep_pickup_datetime INT64,
  lpep_dropoff_datetime INT64,
  store_and_fwd_flag STRING,
  RatecodeID INT64,
  PULocationID INT64,
  DOLocationID INT64,
  passenger_count INT64,
  trip_distance FLOAT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee FLOAT64,
  improvement_surcharge FLOAT64,
  total_amount FLOAT64,
  payment_type INT64,
  trip_type INT64,
  congestion_surcharge FLOAT64)   
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://<>/*.parquet']
    );  

CREATE OR REPLACE TABLE `table` AS (
  SELECT
    * EXCEPT(lpep_dropoff_datetime, lpep_pickup_datetime),
    TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS lpep_pickup_datetime,
    TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS lpep_dropoff_datetime
  FROM `table`
)
```

# uploading with mage

to self
- did in the `homework` folder
- made a api data loader that loads from actual website, `https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet`
- used `kwargs.execution_date.strftime('%Y-%m')`
- loaded to gcs

# can be done in a similar way using airflow

- tried with gzip files in `https://github.com/DataTalksClub/nyc-tlc-data/releases`
- yellow trip data is too large for `pd.read_parquet` and `df.to_parquet`
- using `pyarrow.csv.read_csv` with `pyarrow.parquet.write_table` works
- fhv 2019 is ~200mb which sigterms the airflow run