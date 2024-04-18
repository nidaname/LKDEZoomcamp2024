# notes

note the java version that's needed

Spark runs on Java 8/11/17, Scala 2.12/2.13, Python 3.8+, and R 3.5+. [link](https://spark.apache.org/docs/latest/#downloading)


# note the need for a few export statements

```
export JAVA_HOME="/workspaces/LKDEZoomcamp2024/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="/workspaces/LKDEZoomcamp2024/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

# copying files to gcs

note the need for gsutils, i.e. gcloud cli download

authorising with SA key [link](https://cloud.google.com/sdk/docs/authorizing#key)

command to copy

```
gsutil -m cp -r /workspaces/LKDEZoomcamp2024/6_batch/data/pq gs://airflow-lkdezoomcamp/pq/
```

# commands for .py files

```
python 4_spark_local_pq_test.py \
    --input_green='data/pq/green/*/*' \
    --input_yellow='data/pq/yellow/*/*' \
    --output='data/report/revenue/'
```

```
URL="spark://codespaces-d61839:7077"

spark-submit \
    --master="${URL}" \
    4_spark_local_pq_test.py \
        --input_green=data/pq/green/*/*/ \
        --input_yellow=data/pq/yellow/*/*/ \
        --output=data/report-2020
```

# when creating dataproc cluster

note the need to enable api for 
- vms
- dataproc
- Cloud Resource Manager API (this is when creating the cluster)

## not sure why i have this many errors when creating default dataproc cluster

- default compute engine service account lacks access
    - storage.get...
- default network needs to enable private services access???
    - note need to uncheck internal access only?
    - doesn't seem safe but ok for testing
    - its default unchecked in the video
- default custom staging bucket doesn't have uniform level access???



submitting jobs to dataproc after creating the dataproc cluster
- upload the script to gcs
- run the following

```
gcloud dataproc jobs submit pyspark \
    --cluster=cluster-0a9f \
    --region=asia-southeast1 \
    gs://airflow-lkdezoomcamp/code/4_spark_local_pq_test_dataproc.py \
    -- \
        --input_green=gs://airflow-lkdezoomcamp/pq/green/2020/*/ \
        --input_yellow=gs://airflow-lkdezoomcamp/raw/yellow_taxi/*_2020* \
        --output=gs://airflow-lkdezoomcamp/report-2020
```
note the * for yellow_taxi, only taking 2020 since 2019 is in the same folder

# for dataproc write to bq

```
gcloud dataproc jobs submit pyspark \
    --cluster=cluster-0a9f \
    --region=asia-southeast1 \
    --jars=gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar \
    gs://airflow-lkdezoomcamp/code/4_spark_local_pq_test_dataproc_bq.py \
    -- \
        --input_green=gs://airflow-lkdezoomcamp/pq/green/2020/*/ \
        --input_yellow=gs://airflow-lkdezoomcamp/raw/yellow_taxi/*_2020* \
        --output=ny_taxi_data.report_2020_bq
```

why isit using above version of the jar? no idea

```
gcloud dataproc jobs submit pyspark \
    --cluster=cluster-0a9f \
    --region=asia-southeast1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://airflow-lkdezoomcamp/code/4_spark_local_pq_test_dataproc_bq.py \
    -- \
        --input_green=gs://airflow-lkdezoomcamp/pq/green/2020/*/ \
        --input_yellow=gs://airflow-lkdezoomcamp/raw/yellow_taxi/*_2020* \
        --output=ny_taxi_data.report_2020_bq
```
