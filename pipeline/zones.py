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
@click.option('--target-table', default='taxi_zone', help='Target table name')
@click.option('--data-dir', default='data', help='Directory containing the CSV files')
@click.option('--csv-file', default='taxi_zone_lookup.csv', help='CSV filename')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, data_dir, csv_file):
    # Construct the file path
    csv_path = f"{data_dir}/{csv_file}"

    print(f"Loading data from: {csv_path}")

    # Define dtype specification for optimization
    dtype = {
        'LocationID': 'int64',
        'Borough': 'category',
        'Zone': 'category', 
        'service_zone': 'category'
    }

    # Read the entire CSV file first
    df = pd.read_csv(csv_path, dtype=dtype)
    print(f"Total rows: {len(df)}")

    # Create database engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    (pd.io.sql.get_schema(df, name=target_table, con=engine))

    # Define chunk size
    chunk_size = 53  # Adjust based on your needs
    n_chunks = int(np.ceil(len(df) / chunk_size))

    first = True

    # Loop through chunks
    for i in tqdm(range(n_chunks), desc="Processing chunks"):
        # Get chunk
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        df_chunk = df.iloc[start_idx:end_idx]

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )

    print(f"\n Successfully inserted {len(df)} rows into '{target_table}' table")


if __name__ == '__main__':
    run()

