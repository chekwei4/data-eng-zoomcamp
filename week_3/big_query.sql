--SQL codes for Homework Week3 
--Question 1: What is count for fhv vehicles data for year 2019 *

-- first, we create an external table referring to gcs path
-- in this case, we only want fhv data from 2019, and we need to specify where those parquet files are located
-- gs://dtc_data_lake_de-zoomcamp-339108/raw/fhv_tripdata_2019-*.parquet under GCS
CREATE OR REPLACE EXTERNAL TABLE 'de-zoomcamp-339108.trips_data_all.external_table_fhv_2019'
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_de-zoomcamp-339108/raw/fhv_tripdata_2019-*.parquet']
);

SELECT count(*) FROM `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019`
--Ans: 42084899

-- Question 2: How many distinct dispatching_base_num we have in fhv for 2019 *
SELECT count(distinct dispatching_base_num) FROM `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019`
--Ans: 792

-- Question 3: Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num *
-- Trial 1: without any partitioning and clustering
-- Query complete (26.9 sec elapsed, 1.5 GB processed)
select * from `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019`
where dropoff_datetime BETWEEN '2019-01-01' and '2019-10-31'
order by dispatching_base_num

-- Trial 2: creating a table by partitioning dropoff_datetime
CREATE OR REPLACE TABLE `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff`
PARTITION BY 
    DATE(dropoff_datetime) AS
SELECT * FROM `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019`

-- now, let's run the same query on this newly created partition table
-- Query complete (12.0 sec elapsed, 1.5 GB processed)
select * from `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff`
where dropoff_datetime BETWEEN '2019-01-01' and '2019-10-31'
order by dispatching_base_num

-- Trial 3: creating a table by partitioning dropoff_datetime PLUS clustering on dispatching_base_num
CREATE OR REPLACE TABLE `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff_clust_dispatch`
PARTITION BY 
    DATE(dropoff_datetime) 
CLUSTER BY 
    dispatching_base_num AS
SELECT * FROM `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019`

-- REMEMBER TO TURN OFF CACHE
-- now, let's run the same query again
-- Query complete (17.1 sec elapsed, 1.5 GB processed)
select * from `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff_clust_dispatch`
where dropoff_datetime BETWEEN '2019-01-01' and '2019-10-31'
order by dispatching_base_num


--Question 4: What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 *

--Create a new table where we partition by dropoff_datetime
--and cluster by dispatching_base_num
CREATE OR REPLACE TABLE `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff_clust_dispatch`
PARTITION BY 
    DATE(dropoff_datetime) 
CLUSTER BY 
    dispatching_base_num AS
SELECT * FROM `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019`

--then we will run the query to get the required data
--which we see that approximated data process is 400.1MB (top right of console) and
--actual 145.6 MB processed in 0.4 seconds
--total count is 26558
select count(*) from `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff_clust_dispatch`
where dropoff_datetime between '2019-01-01' and '2019-03-31'
and dispatching_base_num in ('B00987', 'B02060', 'B02279');

--Question 5: What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag *
--Answer in readme.md


--Question 6: What improvements can be seen by partitioning and clustering for data size less than 1 GB *
--Answer in readme.md


--Question 7: In which format does BigQuery save data
--Answer in readme.md