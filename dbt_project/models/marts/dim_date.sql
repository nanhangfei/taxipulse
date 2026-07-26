-- Date dimension covering the loaded data range (2024). Materialized as a
-- table; grain is one row per calendar day. Built from a dbt_utils date spine
-- so it has no upstream data dependency.
{{ config(materialized='table') }}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-01-01' as date)"
    ) }}
)

select
    cast(date_day as date)                       as date_day,
    year(date_day)                               as year,
    month(date_day)                              as month,
    day(date_day)                                as day_of_month,
    dayofweekiso(date_day)                       as day_of_week,   -- 1=Mon .. 7=Sun
    dayname(date_day)                            as day_name,
    monthname(date_day)                          as month_name,
    quarter(date_day)                            as quarter,
    weekofyear(date_day)                         as week_of_year,
    (dayofweekiso(date_day) >= 6)                as is_weekend
from spine
