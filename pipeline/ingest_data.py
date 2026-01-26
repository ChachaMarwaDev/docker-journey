#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import numpy as np
import click


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='green_trip_data', help='Target table name')
@click.option('--month', default='2025_11', help='Month format(YEAR_MONTH)')
@click.option('--data-dir', default='data', help='Directory containing the parquet files')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, month, data_dir):
    # Construct the file path with the month parameter
    trip_path = f"{data_dir}/green_tripdata_{month}.parquet"
    
    print(f"Loading data from: {trip_path}")
    
    df = pd.read_parquet(trip_path)
 
    dtype_spec = {
        "VendorID": "Int64",
        "lpep_pickup_datetime": "datetime64[ns]",
        "lpep_dropoff_datetime": "datetime64[ns]",
        "store_and_fwd_flag": "string",
        "RatecodeID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "ehail_fee": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "payment_type": "Int64",
        "trip_type": "Int64",
        "congestion_surcharge": "float64",
        "cbd_congestion_fee": "float64"
    }

    # Apply conversions for columns that exist in the DataFrame
    for col, target_dtype in dtype_spec.items():
        if col in df.columns:
            if "datetime" in str(target_dtype):
                df[col] = pd.to_datetime(df[col])
            elif target_dtype == "string":
                df[col] = df[col].astype("string")
            else:
                df[col] = df[col].astype(target_dtype)

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    pd.io.sql.get_schema(df, name=target_table, con=engine)

    df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')

    # Read the entire Parquet file first
    df = pd.read_parquet(trip_path)

    # Define chunk size (e.g., 100,000 rows per chunk)
    chunk_size = 10000

    # Calculate number of chunks
    n_chunks = int(np.ceil(len(df) / chunk_size))

    first = True

    # Loop through chunks
    for i in tqdm(range(n_chunks)):
        # Get chunk
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        df_chunk = df.iloc[start_idx:end_idx]

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

        print(f"Inserted chunk {i+1}/{n_chunks}: {len(df_chunk)} rows")


if __name__ == '__main__':
    run()