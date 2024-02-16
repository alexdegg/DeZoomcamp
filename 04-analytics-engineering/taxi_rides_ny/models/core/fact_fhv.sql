{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata_clean as (
    select 
        dispatching_base_num ,
        pickup_datetime,
        dropOff_datetime,
        round(cast(PUlocationID as numeric), 0) as PUlocationID,
        round(cast(DOlocationID as numeric), 0) as DOlocationID,
        SR_Flag,
        Affiliated_base_number
    from {{ ref('stg_fhv_tripdata') }}
    where PULocationID != 'nan' and DOLocationID != 'nan'
),
fhv_tripdata as(
    select
        dispatching_base_num ,
        pickup_datetime,
        dropOff_datetime,
        cast(PUlocationID as integer) as PUlocationID,
        cast(DOlocationID as integer) as DOlocationID,
        SR_Flag,
        Affiliated_base_number
    from fhv_tripdata_clean
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropOff_datetime,
    fhv_tripdata.SR_Flag,
    fhv_tripdata.Affiliated_base_number,
    cast(fhv_tripdata.PUlocationID as integer) as PUlocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    cast(fhv_tripdata.DOlocationID as integer) as DOlocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata
inner join dim_zones as pickup_zone
on cast(fhv_tripdata.PUlocationID as integer) = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on cast(fhv_tripdata.DOlocationID as integer) = dropoff_zone.locationid