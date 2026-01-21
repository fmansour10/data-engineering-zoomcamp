#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import pyarrow.parquet as pq
import pyarrow.fs as fs
from sqlalchemy import create_engine, table
from tqdm.auto import tqdm
import click


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}
'''
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]
'''
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data to ingest')
@click.option('--month', default=1, type=int, help='Month of the data to ingest')
@click.option('--chunksize', default=100000, type=int, help='Number of rows per chunk to ingest')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    pg_user = pg_user
    pg_pass = pg_pass
    pg_host = pg_host
    pg_port = pg_port
    pg_db = pg_db
    year = year
    month = month

    target_table = target_table
    chunksize = chunksize
    
    '''    # Parquet file URL
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"

    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    # Read parquet (HTTPS supported automatically)
    df = pd.read_parquet(url, engine="pyarrow")

    # Optional: chunk manually for large files
    first = True

    for i in tqdm(range(0, len(df), chunksize)):
        df_chunk = df.iloc[i:i+chunksize]

        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False
            print("Table created")

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=chunksize
        )
    '''

    ## CSV Processing
    # Read a sample of the data
    # prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    # url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'

    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    
    # Create a connection to the Postgres database
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Create an iterator to read the data in chunks
    df_iter = pd.read_csv(
        url, 
        dtype=dtype, 
        #parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    ## write data to postgres in chunks
    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
    



if __name__ == '__main__':
    run()