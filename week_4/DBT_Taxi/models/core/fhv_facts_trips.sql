{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'fhv' as service_type 
    from {{ ref('stg_fhv_trip_data') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
fhv_data.SR_Flag,
fhv_data.dispatching_base_num,
fhv_data.PULocationID,
fhv_data.DOLocationID,
fhv_data.pickup_datetime
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PULocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.DOLocationID = dropoff_zone.locationid
