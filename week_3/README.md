# Week 3 Notes
By Chekwei Chia

## Syntax for creating an external table on BigQeury
1. Specify where we are going to pull the data from. In this case, we are pulling data from a GCS bucket, under `raw` folder and we want fhv trip data from all months in year 2019
2. Format of the files in GCS bucket are all `parquet`. 
3. Specify the name of this external table which we want to create. In this case, the name of the table is `external_table_fhv_2019`

```
CREATE OR REPLACE EXTERNAL TABLE 'de-zoomcamp-339108.trips_data_all.external_table_fhv_2019'
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_de-zoomcamp-339108/raw/fhv_tripdata_2019-*.parquet']
);
```
Note: Such an external table actually does not have data sitting in BigQuery. Data is inside GCS, so when we inspect the external table's details, we will see that the table size is actually 0 and number of rows is empty.

## Creating partitions
Here, we create a new table with partitions called `external_table_fhv_2019_part_dropoff` where we will create partitions on `dropoff_datetime`
```
CREATE OR REPLACE TABLE `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff`
PARTITION BY 
    DATE(dropoff_datetime) AS
SELECT * FROM `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019`
```

## Creating clusters
Here, we create a new table called `external_table_fhv_2019_part_dropoff_clust_dispatch` with partitions created on `dropoff_datetime` and also clusters on `dispatching_base_num`
```
CREATE OR REPLACE TABLE `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019_part_dropoff_clust_dispatch`
PARTITION BY 
    DATE(dropoff_datetime) 
CLUSTER BY 
    dispatching_base_num AS
SELECT * FROM `de-zoomcamp-339108.trips_data_all.external_table_fhv_2019
```


## Partition vs Clustering
Clustering can provide more granularity

Check for cardinality of a column, before deciding on clustering or partitioning
- because there's a limit of 4000 partitions per table
- if a column has more than 4000 unique values, then clustering will be a better choice

When a partition results in a small amount of data per partition (eg. Less than 1GB per partition), then using clustering is also a better choice

According to [google cloud documentation](https://cloud.google.com/bigquery/docs/partitioned-tables)
- clustering is preferred over partitioning when partitioning results in a small amount of data per partition
- as such, if a column like SR_FLAG has cardinality of 43, but values like SR_FLAG=35 only contains 3 records, and SR_FLAG=42 only contains 1 record, partitioning is not ideal for SR_FLAG
- clustering on SR_FLAG is preferred

## Internals of BigQuery
Big query uses column oriented structure

## BigQuery with Machine Learning
Allows us to build models in the data warehouse itself
- no need to export the data out