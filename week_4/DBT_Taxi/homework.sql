--question1
SELECT count(*) FROM `de-zoomcamp-339108.dbt_cchia.fact_trips`
where pickup_datetime between "2019-01-01" and "2020-12-31";
--ans: 61635151

--question3
SELECT count(*) FROM `de-zoomcamp-339108.dbt_cchia.stg_fhv_trip_data`;
--ans: 42084899

--question4
SELECT count(*) FROM `de-zoomcamp-339108.dbt_cchia.fhv_facts_trips`;
--ans: 22676253