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

@task(log_prints = True)#,retries=3, cache_key_fn =task_input_hash,cache_expiration = timedelta(days=1))    
def fetch_transform(dataset_url: str) -> pd.DataFrame:
    df = pd.read_parquet(dataset_url, engine='fastparquet')
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints = True,retries=3)
def merge_table_id(latest_id:int,df:pd.DataFrame) -> pd.DataFrame:
    a_shape = (df.shape[0], 1)
    fill_value = latest_id
    id = np.full(a_shape, int(fill_value))
    id_df = pd.DataFrame(id, columns= ['DataID'])
    merge_df = pd.concat([id_df, df], axis=1)
    return merge_df

@task(log_prints = True)#, cache_key_fn =task_input_hash,cache_expiration = timedelta(days=1))
def write_local(merge_df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = f"data/{color}/{dataset_file}"
    merge_df.to_parquet(path, compression="gzip")
    # # Checking where merge df is saved
    #print("File location using os.getcwd():", os.getcwd())
    return path


@task(log_prints = True, retries =3)#,cache_key_fn =task_input_hash,cache_expiration = timedelta(days=1))
def write_gcs(path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return gcs_block

@task()
def write_bq(gcs_block, color: str, dataset_file: str) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    client = bigquery.Client(credentials=gcp_credentials_block.get_credentials_from_service_account())
    table_id = "utopian-outlet-384405.data_project.taxi_rides"
    bucket_name = "taxi_ny"
    path = f"data/{color}/{dataset_file}"
    
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,)

    uri = f"gs://{bucket_name}/{path}"
    load_job = client.load_table_from_uri( uri, table_id, job_config=job_config)


@flow()
def etl_web_to_gcs_to_bq(month : int,year : int,  color : str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    
    latest_id = int(f"{year}{month}")
    
    df = fetch_transform(dataset_url) 
    
    merge_df = merge_table_id(latest_id,df)
    
    path = write_local(merge_df, color, dataset_file)
    gcs_block =  write_gcs(path)
    write_bq(gcs_block,color, dataset_file)
 
    
@flow()
def etl_parent_flow(
    months: list[int] = [4], year: int = 2022, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs_to_bq(month,year, color)
        

if __name__ == "__main__":
    color = "yellow"
    year = 2022
    months = [4]
    etl_parent_flow(months, year, color)