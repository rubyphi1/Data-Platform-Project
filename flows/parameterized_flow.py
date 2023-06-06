from pathlib import Path
import pandas as pd
import numpy as np
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import fastparquet
from prefect_gcp import GcpCredentials
import os
from prefect.tasks import task_input_hash
from time import time
from datetime import timedelta
from google.cloud import bigquery
from datetime import datetime
from prefect_gcp.bigquery import BigQueryWarehouse


@task (log_prints = True)
#Function to get today id
def get_today_id():
    today = datetime.now()
    # month = 1  #Fetch the data of 10/2022
    # year = 2023
    month = today.month 
    year = today.year
    if month == 1:
        month = 12
        year = year -1
    else:
        month = month -1
    
    month = month -2
    id = int(f"{year}{month:02}")
    return month,year,id

# Function to query the maximum Data ID in the table
@task(log_prints = True)
def query_max_id(gcp_credentials_block) -> int:
    client = bigquery.Client(credentials=gcp_credentials_block.get_credentials_from_service_account())
    query = """
        SELECT MAX(DataID) AS max_id
        FROM `utopian-outlet-384405.data_project.taxi_rides` 
    """
    try:
         # In case the table is not created yet
        result = client.query(query)
        for row in result: 
            max_id = int(row['max_id'])
    except:
        max_id =-1
    print("The max_id is:",max_id)
    return max_id 

@task(log_prints = True)#,retries=3, cache_key_fn =task_input_hash,cache_expiration = timedelta(days=1))    
def fetch_transform(dataset_url: str) -> pd.DataFrame:
    df = pd.read_parquet(dataset_url, engine='fastparquet')
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints = True,retries=3)
def add_id(today_id:int,df:pd.DataFrame) -> pd.DataFrame:
    a_shape = (df.shape[0], 1)
    fill_value = today_id
    id = np.full(a_shape, int(fill_value))
    id_df = pd.DataFrame(id, columns= ['DataID'])
    new_df = pd.concat([id_df, df], axis=1)
    return new_df

@task(log_prints = True)#, cache_key_fn =task_input_hash,cache_expiration = timedelta(days=1))
def write_local(new_df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = f"data/{color}/{dataset_file}"
    new_df.to_parquet(path, compression="gzip")
    # # Checking where merge df is saved
    #print("File location using os.getcwd():", os.getcwd())
    return path


@task(log_prints = True, retries =3)#,cache_key_fn =task_input_hash,cache_expiration = timedelta(days=1))
def write_gcs(path):
    """Upload local parquet file to GCS"""
    gsc_path = path
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=gsc_path)
    return gsc_path

@task()
def write_bq(gcs_path, gcp_credentials_block) -> None:
    """Write DataFrame to BiqQuery"""
    client = bigquery.Client(credentials=gcp_credentials_block.get_credentials_from_service_account())
    table_id = "utopian-outlet-384405.data_project.yellow_tripdata"
    bucket_name = "taxi_ny"
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,)

    uri = f"gs://{bucket_name}/{gcs_path}"
    load_job = client.load_table_from_uri( uri, table_id, job_config=job_config)


@flow()
def etl_web_to_gcs_to_bq(color : str) -> None:
    """The main ETL function"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    month = get_today_id()[0] 
    year = get_today_id()[1]
    today_id = get_today_id()[2]
    
    max_id = query_max_id(gcp_credentials_block)
    
    #Fetch the data of new month which would have the today id > max id
    # If the max_id = -1 which means the table is not created yet, then still fetch the data
    if today_id > max_id:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

        df = fetch_transform(dataset_url) 
            
        new_df = add_id(today_id,df)
            
        path = write_local(new_df, color, dataset_file)
        gcs_path =  write_gcs(path)
        write_bq(gcs_path, gcp_credentials_block)
    else:
        return
    
@flow()
def etl_parent_flow(color: str = "yellow"):
        etl_web_to_gcs_to_bq(color)
        

if __name__ == "__main__":
    color = "yellow"
    # year = 2023
    # months = [1]
    etl_parent_flow(color)