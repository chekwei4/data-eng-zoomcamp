-- Below code answers for HW1 for SQL portion

-- question3 (Count records):
select count(*)
from yellow_taxi_data
where tpep_pickup_datetime >= '2021-01-15'
and tpep_pickup_datetime < '2021-01-16'

--question4 (Largest tip for each day):
select tpep_pickup_datetime, tpep_dropoff_datetime, tip_amount from yellow_taxi_data where tip_amount in (select max(tip_amount) from yellow_taxi_data)
--amount was 1140.44 on 20 Jan

--question5 (Most popular destination)
--According to https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv, central park 
--location id is 43
select count(*) as total_cnt, "DOLocationID"
from yellow_taxi_data
where "PULocationID" = 43
and 
tpep_pickup_datetime::date = '2021-01-14'
group by "DOLocationID"
order by total_cnt desc
--Zone for 237 is "Upper East Side South" according to lookup.csv
--237,"Manhattan","Upper East Side South","Yellow Zone"

--question6 (Most expensive route)
select "PULocationID", "DOLocationID", avg(total_amount) as avg_total_amt
from yellow_taxi_data
group by "PULocationID", "DOLocationID"
order by avg_total_amt desc
--4,"Manhattan","Alphabet City","Yellow Zone"
--265,"Unknown","NA","N/A"