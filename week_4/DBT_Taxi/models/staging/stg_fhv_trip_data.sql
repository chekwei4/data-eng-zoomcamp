{{ config(materialized='view') }}

select *,
from {{ source('staging','external_table_fhv_2019') }}
