#!/usr/bin/env python
# coding: utf-8

# In[1]:
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine

#In[2]:
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_file = 'output'

    #Download the parquet file
    os.system(f"wget {url} -O {parquet_file}")
    df = pd.read_parquet(parquet_file, engine='fastparquet')

    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}') 

    #Import the row name to sql
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') #If_exist is how to behave if the table is exist
    print('The column name is imported')

    #Import the data of table to sql
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print('Finish importing the data to sql')

#In[3]:
if __name__ == '__main__': #We need this line for things that we want to run as scripts
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    # user, password, host, port, database name, table name,
    # url of the parquet
    parser.add_argument('--user' , help = 'user name for postgres')
    parser.add_argument('--password' , help = 'password for postgres')
    parser.add_argument('--host' , help = 'host for postgres')
    parser.add_argument('--port' , help = 'port for postgres')
    parser.add_argument('--db' , help = 'database name for postgres')
    parser.add_argument('--table_name' , help = 'name of the table where we will write the results to')
    parser.add_argument('--url' , help = 'url of the parquet file')                   
                
    args = parser.parse_args()
    #The parsers lines above will parse the arguments to the main method
    main(args) 
