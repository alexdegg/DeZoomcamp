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
        PUlocationID,
        DOlocationID,
        SR_Flag,
        Affiliated_base_number,
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
    where PUlocationID is not null and DOlocationID is not null
),
fhv_tripdata as(
    select
        dispatching_base_num ,
        pickup_datetime,
        dropOff_datetime,
        PUlocationID,
        DOlocationID,
        SR_Flag,
        Affiliated_base_number,
        service_type
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
    PUlocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    DOlocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    service_type
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.DOlocationID = dropoff_zone.locationid