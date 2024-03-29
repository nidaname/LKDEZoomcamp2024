when using files from `https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/`

when using pyarrow, it gives `OSError: zlib inflate failed: incorrect header check`

when using pandas, it gives `gzip.BadGzipFile: Not a gzipped file (b'No')`

not sure why


updated airflow version in Dockerfile to `apache/airflow:2.8.4-python3.11`

edited Dockerfile, shifted requirements.txt to after `AIRFLOW_UID`

AIRFLOW_HOME from /opt/airflow to /opt/airflow/

edited `docker-compose.yml` line 133 from 2.20 to 2.8.4