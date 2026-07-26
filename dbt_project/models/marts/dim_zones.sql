-- Small conformed dimension: full-refresh table, not incremental
-- (the marts/ default is incremental, which would append duplicates here).
{{ config(materialized='table') }}

select
    "LocationID"::int as zone_id,
    "Borough"         as borough,
    "Zone"            as zone_name,
    "service_zone"    as service_zone
from {{ source('tlc_raw', 'zone_lookup') }}
