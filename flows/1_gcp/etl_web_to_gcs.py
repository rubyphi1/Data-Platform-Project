from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import fastparquet


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    df = pd.read_parquet(dataset_url, engine='fastparquet')
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = f"data/{color}/{dataset_file}"
    df.to_parquet(f"../../{path}", compression="gzip")
    return path


@task()
def write_gcs(path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(from_path=f"../../{path}", to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2022
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    df = fetch(dataset_url)
    path = write_local(df, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()