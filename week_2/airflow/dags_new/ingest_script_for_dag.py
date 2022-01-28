import pandas as pd
import os
from sqlalchemy import create_engine
from time import time


def ingest_callable(user, password, host, port, db, table, csv_file, execution_date):
    print(f"user:{user}, pw:{password}, host:{host}, port:{port}, db:{db}, table:{table}, csvFile:{csv_file}, execution_date:{execution_date}")
    # download the csv
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    engine.connect()

    print("connection established...")

    print("inserting data...")

    # t_start = time()

    # df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000
    # )
    # df = next(df_iter)
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # # create an empty table first, and go back to terminal to check if exits
    # df.head(n=0).to_sql(name=table, con=engine, if_exists="replace")
    # df.to_sql(name=table, con=engine, if_exists="append")

    # t_end = time()

    # print("inserted first chunk, took %.3f second" % (t_end - t_start))

    # while True:
    #     t_start = time()
    #     df = next(df_iter)
    #     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #     df.to_sql(name=table, con=engine, if_exists="append")
    #     t_end = time()
    #     print("inserted another chunk, took %.3f second" % (t_end - t_start))

