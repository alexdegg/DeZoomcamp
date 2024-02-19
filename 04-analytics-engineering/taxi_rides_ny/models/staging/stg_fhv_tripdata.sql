{{
    config(
        materialized='view'
    )
}}

select
    cast(dispatching_base_num as string) as dispatching_base_num ,
    pickup_datetime,
    dropOff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as PUlocationID,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as DOlocationID,
    cast(SR_Flag AS string) as SR_Flag,
    cast(Affiliated_base_number AS string) as Affiliated_base_number
from {{ source('staging','fhv_trips')}}
where cast(pickup_datetime as string) like '%2019%'

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}