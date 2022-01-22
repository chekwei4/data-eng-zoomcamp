-- question3 (Count records):
select count(*)
from yellow_taxi_data
where tpep_pickup_datetime >= '2021-01-15'
and tpep_pickup_datetime < '2021-01-16'

--question4 (Largest tip for each day):
select tpep_pickup_datetime, tpep_dropoff_datetime, tip_amount from yellow_taxi_data where tip_amount in (select max(tip_amount) from yellow_taxi_data)

--question5 (Most popular destination)
--According to https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv, central park 
--location id is 43
