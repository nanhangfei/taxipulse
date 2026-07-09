{{ config(
    materialized='incremental',
    unique_key='trip_sk',
    incremental_strategy='merge',
    cluster_by=['to_date(pickup_datetime)']
) }}

select * from {{ ref('stg_trips') }}
{% if is_incremental() %}
  -- reprocess only the month being run: idempotent backfills
  where year  = year(to_date('{{ var("run_month", "2024-01-01") }}'))
    and month = month(to_date('{{ var("run_month", "2024-01-01") }}'))
{% endif %}
