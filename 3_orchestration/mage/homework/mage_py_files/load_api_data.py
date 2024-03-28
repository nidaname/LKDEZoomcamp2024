import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    nytaxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float, 
        'RatecCodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float, 
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float, 
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
    all_df = []

    for month in ['10', '11', '12']:

        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green//green_tripdata_2020-{month}.csv.gz'
        current_df = pd.read_csv(url, sep=',', compression='gzip', dtype=nytaxi_dtypes, parse_dates=parse_dates)
        all_df.append(current_df)
    

    return pd.concat(all_df)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert set([10, 11, 12]) <= set(output['lpep_dropoff_datetime'].dt.month), 'The output does not have 10 11 12 months'