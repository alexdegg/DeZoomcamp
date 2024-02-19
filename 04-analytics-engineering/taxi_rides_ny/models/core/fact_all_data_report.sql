{{
    config(
        materialized='table'
    )
}}

with fhv_add as (
    select 
    cast('' as string) as tripid,
    NULL as vendorid, 
    service_type,
    NULL as ratecodeid, 
    PUlocationID as pickup_locationid,
    pickup_borough, 
    pickup_zone, 
    DOlocationID as dropoff_locationid,
    dropoff_borough, 
    dropoff_zone,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime, 
    cast('' as string) as store_and_fwd_flag, 
    NULL as passenger_count, 
    NULL as trip_distance, 
    NULL as trip_type, 
    NULL as fare_amount, 
    NULL as extra, 
    NULL as mta_tax, 
    NULL as tip_amount, 
    NULL as tolls_amount, 
    NULL as ehail_fee, 
    NULL as improvement_surcharge, 
    NULL as total_amount, 
    NULL as payment_type, 
    cast('' as string) as payment_type_description,
    dispatching_base_num,
    SR_Flag,
    Affiliated_base_number
    from {{ ref('fact_fhv') }}
),
trips_add as (
    select 
    tripid,
    vendorid, 
    service_type,
    ratecodeid, 
    pickup_locationid, 
    pickup_borough, 
    pickup_zone, 
    dropoff_locationid,
    dropoff_borough, 
    dropoff_zone,  
    pickup_datetime, 
    dropoff_datetime, 
    store_and_fwd_flag, 
    passenger_count, 
    trip_distance, 
    trip_type, 
    fare_amount, 
    extra, 
    mta_tax, 
    tip_amount, 
    tolls_amount, 
    ehail_fee, 
    improvement_surcharge, 
    total_amount, 
    payment_type, 
    payment_type_description,
    cast('' as string) as dispatching_base_num,
    cast('' as string) as SR_Flag,
    cast('' as string) as Affiliated_base_number
    from {{ ref('fact_trips') }}
),
all_data_add as (
    select * from fhv_add
    union all select * from trips_add
)

select *
from all_data_add
