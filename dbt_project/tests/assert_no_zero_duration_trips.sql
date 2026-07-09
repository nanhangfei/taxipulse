-- Singular test: fails if any trip has zero (or negative) duration.
-- (Should be impossible post-Spark-quarantine — this is the belt to that suspender.)
select *
from {{ ref('fct_trips') }}
where pickup_datetime >= dropoff_datetime
