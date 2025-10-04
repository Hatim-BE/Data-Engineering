import psycopg2
from psycopg2 import errors
import pyarrow.parquet as pq
import io
import pandas as pd
import time
import argparse

def main(params):
    host = params.host
    port = params.port
    dbname = params.dbname
    user = params.user
    password = params.password
    # parquet_file = params.parquet_file
    table_name = params.table  

    # ---- CONFIG ----
    parquet_file = "data/yellow_tripdata_2025-01.parquet"
    db_params = {
        "dbname": dbname,
        "user": user,
        "password": password,
        "host": host,
        "port": port
    }

    # ---- CONNECT TO POSTGRES ----
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # ---- READ PARQUET ----
    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    print(f"Loaded {len(df)} rows from Parquet file.")

    # ---- OUPTUT SQL QUERY ----
    # print(pd.io.sql.get_schema(df, name=table_name))

    try:
        cur.execute(pd.io.sql.get_schema(df, name=table_name))
    except errors.DuplicateTable:
        print("Table already exists")
    except Exception as e:
        print(f"Exception occured: {e}")

    conn.commit()

    parquet = pq.ParquetFile(parquet_file)
    print(f"File has {parquet.num_row_groups} row groups")

    total_rows = 0

    # Data ingesting chunk by chunk (groups for parquet files)
    for chunk in range(parquet.num_row_groups):
        df_chunk = parquet.read_row_group(chunk).to_pandas()
        output = io.StringIO()
        df_chunk.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        start = time.time()
        cur.copy_from(output, table_name, null="", sep='\t')
        conn.commit()
        end = time.time()

        total_rows += len(df_chunk)
        print(f"chunk {chunk} of {len(df_chunk)} rows ingested in {end - start} seconds")

    print(f"Successfully inserted {total_rows} rows into {table_name}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    parser= argparse.ArgumentParser(description="Ingest parquet data to Postgres")

    parser.add_argument("host", help="Database host (e.g., localhost")
    parser.add_argument("port", type=int, help="Database port (default 5432)")
    parser.add_argument("dbname", help="Database name")
    parser.add_argument("user", help="Database username")
    parser.add_argument("password", help="Database password")
    parser.add_argument("parquet_file", help="Path to the Parquet file")
    parser.add_argument("--table", default="yellow_taxi_data", help="table name (default: yellow_taxi_data)")

    args = parser.parse_args()

    main(args)


    