required to export GOOGLE_APPLICATION_CREDENTIALS before running `upload_to_gcs.py`

used `upload_to_gcs.py` to download and export to gcs bucket

note regions for bucket and dataset

note forbidden for files when wget with wrong url

note if parquet file is broken, when creating external table in BQ, it shows as one column `__xml_version__1_0__encoding__UTF_8___` 

# uploading with mage

to self
- did in the `homework` folder
- made a api data loader that loads from actual website, `https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet`
- used `kwargs.execution_date.strftime('%Y-%m')`
- loaded to gcs

can be done in a similar way using airflow

- tried with gzip files in `https://github.com/DataTalksClub/nyc-tlc-data/releases`
- yellow trip data is too large for `pd.read_parquet` and `df.to_parquet`
- using `pyarrow.csv.read_csv` with `pyarrow.parquet.write_table` works