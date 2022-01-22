# Week 1 Notes

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


## Useful Postgres SQL Commands 

To see all available tables in the schema

`\dt`


To get all the columns in the table, with their datatypes

`\d yellow_taxi_data` 

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