from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import fastparquet
from prefect_gcp import GcpCredentials

@task(log_prints = True,retries=3)    
def fetch_transform(dataset_url: str) -> pd.DataFrame:
    df = pd.read_parquet(dataset_url, engine='fastparquet')
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints = True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = f"data/{color}/{dataset_file}"
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints = True)
def write_gcs(path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="data_project.taxi_rides",
        project_id="utopian-outlet-384405",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_web_to_gcs_to_bq(month : int,year : int,  color : str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    df = fetch_transform(dataset_url)
    path = write_local(df, color, dataset_file)
    write_gcs(path)
    write_bq(df)
 
    
@flow()
def etl_parent_flow(
    months: list[int] = [4,5], year: int = 2022, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs_to_bq(month,year, color)

if __name__ == "__main__":
    color = "yellow"
    year = 2022
    months = [4]
    etl_parent_flow(months, year, color)