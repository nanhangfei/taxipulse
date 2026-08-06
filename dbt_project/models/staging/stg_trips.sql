-- Staging: clean + conform TLC trips to the trip grain.
-- Reads the gold-fed table (Stage 2 lake path). The Spark silver step already
-- quarantines invalid rows, so these filters are now harmless no-ops kept as a
-- defensive backstop / documentation of the trip-grain contract.
with source as (
    select * from {{ source('tlc_raw', 'fct_trips_gold') }}
    where pickup_datetime < dropoff_datetime      -- positive trip duration
      and fare_amount  >= 0                        -- no negative fares
      and total_amount >= 0
      and payment_type in (1, 2, 3, 4, 5, 6)       -- valid payment types
      and pickup_zone_id  is not null
      and dropoff_zone_id is not null
),

-- One row per trip grain: drop exact duplicates that would collide on trip_sk.
deduped as (
    select *
    from source
    qualify row_number() over (
        partition by pickup_datetime, dropoff_datetime,
                     pickup_zone_id, dropoff_zone_id, total_amount
        order by pickup_datetime
    ) = 1
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
from deduped
