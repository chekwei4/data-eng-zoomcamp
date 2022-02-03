# Ingesting data to local Postgres with Airflow

## Create DAG
DAG will have a `name`, `schedule_interval`, and `start_date`.
- `schedule_interval` is the one we specify the crontab patter. Check out [here](https://crontab.guru/) for crontab cheatsheet. 
- inside the DAG object, we will specify the task (technically, it's called the operators)
  - eg. `BashOperator` that takes in `task_id` and `bash_command`, and it will execute the bash command accordingly.
    - This `BashOperator` object will download the data file from a url via `curl` command
  - We also have a `PythonOperator`, which we will run a python function to ingest the data file downloaded
  - We do so by importing the python module
- Lastly, we also specify the sequence of which the operators should run

## Python Module
After the data file has been downloaded via `curl` command, we create a python module which will ingest data into Postgres DB. The concept is similar to week 1's `ingest_data.py`, but now, we are doing it via airflow. 
1. take in input parameters, which we will read via `op_kwargs` in the `PythonOperator` object when we created the DAG. This inputs will be the DB connection details (user, password, host, port etc.)
2. create sqlalchemy connection engine and start connection
3. insert data into DB in chunks of 100k

## Airflow Worker
We can get the `CONTAINER ID` of our airflow worker via 
```
docker ps
```

To run bash in the worker container, we run 
```
docker exec -it [container_id] bash
```

With that, we enter bash mode inside the container, and we can see what are the data files downloaded. It will be residing inside parent directory. This is because we have specify the data files to be downloaded into:

```
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

CSV_NAME = AIRFLOW_HOME + "/output_{{ execution_date.strftime(\"%Y-%m\") }}.csv"

```

FYI: `{{execution_date.strftime(\"%Y-%m\")}}` is a way to capture the year and month of the airflow execution datetime. 

## Postgres

Within week_1's .yml file, we have added below. It is understood that by adding the network, we can allow multiple docker-compose files to communicate. 

```
networks:
  airflow:
    external:
      name: airflow_default
```

By running `docker network ls` we can find out the network which the container with airflow is running on. 


## pgAdmin
Two things we need to do. 

1. Setup Postgres and pgAdmin, by ensuring that they both run in the same network. Here, we declare `network=airflow_default`

2. Recall that our Postgres has already set up `network=airflow_default`

To spin up pgAdmin, we could run below:

```
docker run -it \  
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \  
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8081:80 \ 
    --network=network=airflow_default \
    --name pgadmin \ 
    dpage/pgadmin4
```
Double check that `network=airflow_default`

The password and user name for pgAdmin is set as user=`admin@admin.com` and pw='root`.

Access pdAdmin UI via: `http://localhost:8081/` 

Next, We need to configure the Servers

1. Right click `Servers`
2. Select `Server...`
3. Name could be `docker localhost`
4. Under the `Connection` tab, hostname must be `pgdatabase` and port is `5432`. All these are based on the way Postgres was spin up when we run `docker-compose up` on week_1's .yml file. 