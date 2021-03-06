# Week 1 Notes
By Chekwei Chia

## Create Dockerfile
from: base image

run: bash command

entrypoint: bash
```
FROM python:3.9

RUN pip install pandas

ENTRYPOINT [ "bash" ]
```

## Running sample docker with python code

`Dockerfile`
```
FROM python:3.9

RUN pip install pandas

WORKDIR /app

COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]
```

`pipeline.py`

```
import sys

import pandas as pd

# some fancy stuff with 

print(sys.argv)

day = sys.argv[1]

print(f"job finished successfully for day={day}")
```

Commands to run:

1. `docker build -t test:pandas .`

2. `docker run -it test:pandas 2022-01-19`

<img src="image/output1.png">

## Run Postgres with Docker
Command for Mac OS

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

## Create connection to Postgres DB
`pgcli -h localhost -p 5432 -u root -d ny_taxi`

`-h`: host

`-p`: port

`-d`: database


## Ingest Data into Postgres DB
More details on jupyter notebook via [here](https://github.com/chekwei4/data-eng-zoomcamp/blob/master/Docker_Tutorial/upload-data-chekwei.ipynb)

## Useful Postgres SQL Commands 

To see all available tables in the schema

`\dt`


To get all the columns in the table, with their datatypes

`\d yellow_taxi_data` 

To query data that is of datetime format, we can use [`colName::date`] syntax

Example:
```
select * 
from ...
where tpep_pickup_datetime::date = '2021-01-14'
```

## Connecting pgAdmin and Postgres DB
`pgcli` is a command line interface (cli) way to interact with Postgres DB, but it might not be the most convenient way to do things. Another way to go about is to use pgAdmin. We do not need to install it on our machine, since we already have docker. We can just pull a docker image that contains the tool and we can use pgAdmin with just a docker container.

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

## Concept of connecting two containers
pdAdmin will try to find Postgres in the same container, but it is unable to locate that. Because pgAdmin and Postgres SQL are residing in two different running containers.

The idea here is to link the two containers by putting two containers in the same network.

1. Create a network 

`docker network create pg-network-1`
  

1. Run Postgres, but now with a new appended flag that states the `--network` and `--name`

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network-1 \
  --name pg-database-1 \
  postgres:13
```


3. Run pgAdmin

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network-1 \
  --name pgadmin-3 \
  dpage/pgadmin4
```

## Ingesting data through Docker
1. Create a ingest_data.py file

- To take in params like `username`, `password`, `host`, `port`, `table_name`, `db_name`, and `url` to download data

2. Modify Dockerfile to include more packages like
`sqlalchemy`, `psycopg2`, `wget`

3. Build the docker image
   
`docker build -t taxi_ingest:v001 .`

4. Run the docker image as a container

Things to note:

- `--network` must be the same as earlier declared, so that all containers can talk to each other

- `--host` and `--port` here must also be the same as earlier declared

```
URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

docker run -it \
  --network=pg-network-1 \
  taxi_ingest:v1 \
    --user=root \
    --password=root \
    --host=pg-database-1 \
    --port=5432 \
    --db=ny_taxi \
    --table=yellow_taxi_docker \
    --url=${URL}
```