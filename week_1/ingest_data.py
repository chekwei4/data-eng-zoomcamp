#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
import argparse


def main(args):
    user = args.username
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table = args.table
    url = args.url
    csv_name = "output.csv"

    # download the csv
    os.system(f"wget {url} -O {csv_name}")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000
    )
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create an empty table first, and go back to terminal to check if exits
    df.head(n=0).to_sql(name=table, con=engine, if_exists="replace")
    df.to_sql(name=table, con=engine, if_exists="append")

    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table, con=engine, if_exists="append")
        t_end = time()
        print("inserted another chunk, took %.3f second" % (t_end - t_start))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to PG")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--db")
    parser.add_argument("--table")
    parser.add_argument("--url")

    args = parser.parse_args()
    main(args)