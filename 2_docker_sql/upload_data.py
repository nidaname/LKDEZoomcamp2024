#!/usr/bin/env python
# coding: utf-8

from time import time
from sqlalchemy import create_engine
import pandas as pd
import os
import argparse



def main(params):
    user = params.user
    password = params.password
    host = params.host
    database = params.database
    port = params.port
    table_name = params.table_name
    url = params.url
    gz_csv = "yellow_taxi_data.csv.gz"
    csv = "yellow_taxi_data.csv"
    
    print("downloading csv file...")
    os.system(f"wget {url} -O {gz_csv}")

    print("unzipping csv file...s")
    os.system(f"gzip -d {csv}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    df_iter = pd.read_csv(csv, iterator=True, chunksize=100000)
    df = next(df_iter)

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    
    print("Inserting chunks...")
    while True:
        start_time = time()
        df = next(df_iter)

        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        df.to_sql(name=table_name, con=engine, if_exists="append")

        end_time = time()

        print(f"inserted one chunk... took {end_time-start_time} seconds")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingestion script')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url to download the csv to ingest')

    args = parser.parse_args()

    

    main(args)
