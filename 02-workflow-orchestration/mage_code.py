# DATA LOADER: homework_load_data block
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    urls = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
     'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
     'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'] 

    taxi_dtypes = {'VendorID': pd.Int64Dtype(),
               'passenger_count': pd.Int64Dtype(),
               'trip_distance': float,   
               'RatecodeID': pd.Int64Dtype(),
               'store_and_fwd_flag': str,
               'PULocationID': pd.Int64Dtype(),
               'DOLocationID': pd.Int64Dtype(),
               'payment_type': pd.Int64Dtype(),
               'fare_amount': float,
               'extra': float,
               'mta_tax': float,
               'tip_amount': float,   
               'tolls_amount': float,
               'improvement_surcharge': float,
               'congestion_surcharge': float}

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime'] 
    df_list = []
    for url in urls:
        df = pd.read_csv(url, dtype=taxi_dtypes, parse_dates=parse_dates, compression='gzip', encoding='utf-8')
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)

    return df


#TRANSFORMER: homeworkd_transform
    
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero
    data = data[(data['trip_distance'] > 0) & (data['passenger_count'] > 0)]

    # Create new date column from datetime 
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns to snake case
    data.rename(columns={'VendorID':'vendor_id'}, 
          inplace=True)
    data.columns = [col.lower().replace(' ', '_') for col in data.columns]
    

    # Rename column names with camel case to snake case
    cols = [col.replace('_', ' ') for col in data.columns]
    snakecase_cols = [col.lower().replace(' ', '_') for col in cols]
    data.columns = snakecase_cols

    # Assertion 1 
    valid_vendor_ids = data['vendor_id'].unique()
    assert data['vendor_id'].isin(valid_vendor_ids).all(), 'Invalid vendor_id found'

    # Assertion 2
    assert (data['passenger_count'] > 0).all(), 'passenger_count contains non-positive values' 

    # Assertion 3
    assert (data['trip_distance'] > 0).all(), 'trip_distance contains non-positive values'

    return data 

#DATA EXPORTER: homework_data_to_postgres

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = 'mage'  # Specify the name of the schema to export data to
    table_name = 'green_taxi'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

#DATA EXPORTER: homework_to_gcs_parquet_patritioned
        
import pyarrow as pa 
import pyarrow.parquet as pq 
import os 



if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/terraform_key.json"

bucket_name = 'mage-zoomcamp-alexdegg'
project_id = 'refined-outlet-411413'

table_name = "green_taxi_data"

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem=gcs
    )


