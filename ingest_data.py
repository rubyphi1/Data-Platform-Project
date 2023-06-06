#!/usr/bin/env python
# coding: utf-8

# In[1]:
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow,task
from prefect.tasks import task_input_hash
from time import time
from datetime import timedelta
###
from prefect_sqlalchemy import SqlAlchemyConnector

#In[2]:
@task(log_prints = True, retries =3,cache_key_fn =task_input_hash,cache_expiration = timedelta(days=1))
# Caching refers to the ability of a task run to reflect a finished state without actually running the code that defines the task. 
# This allows you to efficiently reuse results of tasks that may be expensive to run with every flow run, or reuse cached results if the inputs to a task have not changed.

#You can define a task that is cached based on its inputs by using the Prefect task_input_hash. 
# This is a task cache key implementation that hashes all inputs to the task using a JSON or cloudpickle serializer. 
# If the task inputs do not change, the cached results are used rather than running the task until the cache expires.

# cache_key_fn :given the task run context and call parameters, generates a string key. 
# If the key matches a previous completed state, that state result will be restored instead of running the task again.

# cache_expiration	An optional amount of time indicating how long cached states for this task should be restorable; 
# if not provided, cached states will never expire.
def extract_data(url):
    parquet_file = 'output'
    #Download the parquet file
    os.system(f"wget {url} -O {parquet_file}")
    df = pd.read_parquet(parquet_file, engine='fastparquet')
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"Missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    #Drop rows where passenger count =0
    df = df[df['passenger_count'] !=0]
    print(f"There are {df['passenger_count'].isin([0]).sum()} missing passenger rows left ")
    return(df)

@task(log_prints = True,retries =3)
def ingest_data(table_name,df):
    
    connection_block= SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
    
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') #If_exist is how to behave if the table is exist
        print('The column name is imported')

        #Import the data of table to sql
        df.to_sql(name=table_name, con=engine, if_exists='append')
        print('Finish importing the data to sql')

@flow(name="Subflow",log_prints=True)
def log_subflow(table_name:str):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest Flow")
def main_flow(table_name:str):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
    
    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name,data)

#In[3]:
if __name__ == '__main__': 
    main_flow("yellow_taxi_trips")
