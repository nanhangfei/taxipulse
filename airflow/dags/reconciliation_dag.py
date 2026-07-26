"""Lambda-architecture reconciliation: does the speed layer agree with batch?

Why this exists (gap fix #7): in any Lambda design the streaming path (Flink)
and the batch path (Spark->dbt) compute overlapping facts from the same source.
They WILL drift — late events, dedup differences, watermark discards. A
production platform measures that drift instead of hoping. This DAG recomputes
Flink's 5-minute zone counts from the batch fact table and compares.

Tolerance: 1% relative difference per (window, zone). Above that -> Slack alert
and the run is marked failed so it shows up in dashboards.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Asset
from alerting import slack_alert  # reuse the same alerting

GOLD_ASSET = Asset("s3a://gold/fct_trips")
TOLERANCE = 0.01

RECON_SQL = """
WITH batch AS (
    SELECT
        TIME_SLICE(dropoff_datetime, 5, 'MINUTE') AS window_start,
        pickup_zone_id,
        COUNT(*) AS batch_count
    FROM ANALYTICS_DB.MARTS.FCT_TRIPS
    WHERE dropoff_datetime >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
speed AS (
    SELECT window_start, pickup_zone_id, trip_count AS speed_count
    FROM RT_DB.PUBLIC.RT_ZONE_DEMAND_5MIN
    WHERE window_start >= DATEADD(day, -1, CURRENT_TIMESTAMP())
)
SELECT
    COALESCE(b.window_start, s.window_start)     AS window_start,
    COALESCE(b.pickup_zone_id, s.pickup_zone_id) AS zone_id,
    IFNULL(b.batch_count, 0)                     AS batch_count,
    IFNULL(s.speed_count, 0)                     AS speed_count,
    ABS(IFNULL(b.batch_count,0) - IFNULL(s.speed_count,0))
        / NULLIF(GREATEST(IFNULL(b.batch_count,0), IFNULL(s.speed_count,0)), 0)
                                                 AS rel_diff
FROM batch b
FULL OUTER JOIN speed s
  ON b.window_start = s.window_start AND b.pickup_zone_id = s.pickup_zone_id
QUALIFY rel_diff > %(tolerance)s
ORDER BY rel_diff DESC
LIMIT 50
"""


@dag(
    dag_id="lambda_reconciliation",
    schedule=[GOLD_ASSET],            # data-aware: runs whenever gold updates
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1,
                  "retry_delay": timedelta(minutes=2),
                  "on_failure_callback": slack_alert},
    tags=["quality", "lambda", "reconciliation"],
)
def lambda_reconciliation():

    @task
    def compare_layers() -> None:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = hook.get_records(RECON_SQL, parameters={"tolerance": TOLERANCE})
        if rows:
            sample = "\n".join(
                f"  window={r[0]} zone={r[1]} batch={r[2]} speed={r[3]} diff={r[4]:.2%}"
                for r in rows[:10])
            raise ValueError(
                f"Speed/batch drift beyond {TOLERANCE:.0%} in {len(rows)} "
                f"(window, zone) pairs:\n{sample}")
        print("Reconciliation OK: batch and speed layers agree within tolerance.")

    compare_layers()


lambda_reconciliation()
