This follows the DEZoomcamp 2022 version which uses airflow.

link here: `https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/cohorts/2022/week_2_data_ingestion`

I'm using the super lightweight docker-compose.yaml file
- named docker-compose_2.3.4.yaml. here its renamed to docker-compose.yaml

# notes to self
- key in credentials folder is the same key used for mage
- trips_data_all is the dataset for 2022, 2024 its ny_taxi_data
- when did i make ny_taxi_data?


# to dos

maybe use BigQueryGetDatasetOperator and BigQueryCreateEmptyDatasetOperator before inserting into table.

since dataset might not have been created