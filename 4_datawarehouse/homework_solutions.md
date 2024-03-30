# QUERIES USED TO MAKE TABLES

```sql
CREATE OR REPLACE EXTERNAL TABLE `onyx-descent-417702.test_dataset.green_taxi_data_parquet_2022_all`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://airflow-lkdezoomcamp/green/*.parquet']
    );
```

```sql
CREATE OR REPLACE TABLE `onyx-descent-417702.test_dataset.green_taxi_data_parquet_2022_all_materialised` AS (
  SELECT
    *
  FROM `onyx-descent-417702.test_dataset.green_taxi_data_parquet_2022_all`
)
```

# QUESTION 1

840,402

# QUESTION 2

```sql
SELECT
 count(DISTINCT PULocationID)
FROM
  `onyx-descent-417702.test_dataset.green_taxi_data_parquet_2022_all`
```
0 MB for the External Table and 6.41MB for the Materialized Table

# QUESTION 3

1,622

```sql
SELECT
  SUM(IF(fare_amount = 0, 1, 0))
FROM
  `onyx-descent-417702.test_dataset.green_taxi_data_parquet_2022_all_materialised`
```

# QUESTION 4

Partition by lpep_pickup_datetime Cluster on PUlocationID

```SQL
CREATE OR REPLACE TABLE `onyx-descent-417702.test_dataset.green_taxi_data_parquet_2022_all_materialised_clustered_partitioned`
PARTITION BY lpep_pickup_datetime
CLUSTER BY PUlocationID
 AS (
  SELECT
    *,
  FROM `onyx-descent-417702.test_dataset.green_taxi_data_parquet_2022_all`
)
```

# QUESTION 5

12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

# QUESTION 6

GCP Bucket

# QUESTION 7

False. can't tell the benefit