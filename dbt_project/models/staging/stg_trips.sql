with source as (
    select * from {{ source('tlc_raw', 'fct_trips_raw') }}
)
select
    {{ dbt_utils.generate_surrogate_key([
        'pickup_datetime', 'dropoff_datetime',
        'pickup_zone_id', 'dropoff_zone_id', 'total_amount']) }} as trip_sk,
    pickup_datetime,
    dropoff_datetime,
    datediff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,
    pickup_zone_id,
    dropoff_zone_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    year,
    month
from source
