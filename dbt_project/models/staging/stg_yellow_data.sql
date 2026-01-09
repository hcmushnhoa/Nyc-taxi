with tripdata as (
    select  *,
             row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
    from {{ source('staging','trips_yellow') }}
        where VendorID not null
)
select
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast('VendorID',api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast('passenger_count',api.Column.translate_type("integer")) }} as count_passenger,
    {{ dbt.safe_cast('RatecodeID',api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast('PULocationID',api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast('DOLocationID',api.Column.translate_type("integer")) }} as dropoff_locationid,
-- time data
    {{ dbt.safe_cast('tpep_pickup_datetime',api.Column.translate_type("timestamp")) }} as pickup_datetime,
    {{ dbt.safe_cast('tpep_dropoff_datetime',api.Column.translate_type("timestamp")) }} as dropoff_datetime,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    store_and_fwd_flag,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_describe_payment_type(payment_type) }} as payment_type_describe,
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    cast(airport_fee as numeric) as airport_fee
from tripdata
where rn = 1
-- dbt run --var('is_test_run', 'false') -> bỏ dòng limit 100 để run real(no test)
-- Bỏ luôn đoạn này khi chạy real
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}