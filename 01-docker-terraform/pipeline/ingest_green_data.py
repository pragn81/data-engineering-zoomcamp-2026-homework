#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def ingest_taxi_zone_lookup(engine):
    """Ingest taxi zone lookup CSV data into PostgreSQL."""
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    table_name = 'taxi_zone_lookup'
    
    dtype = {
        "LocationID": "Int64",
        "Borough": "string",
        "Zone": "string",
        "service_zone": "string"
    }
    
    print(f"Ingesting taxi zone lookup data from: {url}")
    
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        iterator=True,
        chunksize=100000
    )
    
    first = True
    
    for df_chunk in tqdm(df_iter, desc="Loading taxi zones"):
        if first:
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False
        
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )
    
    print(f"Successfully ingested taxi zone lookup data into {table_name}")


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    """Ingest NYC green taxi data (parquet format) and taxi zone lookup into PostgreSQL database."""
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # First, ingest taxi zone lookup data
    ingest_taxi_zone_lookup(engine)
    
    # Then, ingest green taxi trip data
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    
    print(f"\nDownloading green taxi data from: {url}")
    
    # Read parquet file in chunks
    df = pd.read_parquet(url)
    
    # Split dataframe into chunks for processing
    total_rows = len(df)
    chunks = [df[i:i+chunksize] for i in range(0, total_rows, chunksize)]
    
    print(f"Total rows: {total_rows}, Processing in {len(chunks)} chunks")
    
    first = True
    
    for df_chunk in tqdm(chunks, desc="Ingesting green taxi data"):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False
        
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )
    
    print(f"Successfully ingested {total_rows} rows into {target_table}")
    print("\n✅ All data ingestion completed successfully!")


if __name__ == '__main__':
    main()
