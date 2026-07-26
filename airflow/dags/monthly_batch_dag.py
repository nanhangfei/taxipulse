"""Monthly batch pipeline: TLC parquet -> Spark medallion -> Snowflake -> dbt -> Soda.

Production behaviors implemented here (not just mentioned):
  * catchup=True + logical-date params: any month is backfillable via
    `airflow dags backfill -s 2024-01-01 -e 2024-03-01 monthly_batch_pipeline`
  * Idempotent per-partition semantics end-to-end (Spark dynamic overwrite,
    Snowflake DELETE+COPY scoped to the month, dbt incremental merge)
  * dbt BUILD (tests gate downstream models), then Soda as an independent
    post-load operational gate
  * Slack on_failure_callback on every task (gap fix #5, minimal viable alerting)
  * Airflow Asset emitted on completion so downstream DAGs are data-aware
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset
from alerting import slack_alert

GOLD_ASSET = Asset("s3a://gold/fct_trips")
DBT_DIR = "/opt/airflow/dbt_project"
SODA_DIR = "/opt/airflow/soda"


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": slack_alert,
}


@dag(
    dag_id="monthly_batch_pipeline",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "tlc", "medallion"],
)
def monthly_batch_pipeline():

    @task
    def resolve_partition(ds: str | None = None) -> dict:
        d = datetime.strptime(ds, "%Y-%m-%d")
        return {"year": d.year, "month": d.month,
                "src": f"/data/yellow_tripdata_{d.year}-{d.month:02d}.parquet"}

    p = resolve_partition()

    download = BashOperator(
        task_id="download_tlc_parquet",
        bash_command=(
            "mkdir -p /data && curl -fL --retry 3 -o {{ ti.xcom_pull(task_ids='resolve_partition')['src'] }} "
            "https://d37ci6vzurychx.cloudfront.net/trip-data/"
            "yellow_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
        ),
    )

    def spark_task(name: str, args: str) -> BashOperator:
        return BashOperator(
            task_id=name,
            bash_command=(
                "spark-submit --master spark://spark-master:7077 "
                "--packages org.apache.hadoop:hadoop-aws:3.3.4,io.openlineage:openlineage-spark_2.12:1.9.1 "
                f"/opt/airflow/spark_jobs/{name}.py {args}"
            ),
            env={  # creds via env, inherited from compose — never inline
                "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER", ""),
                "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", ""),
            },
            append_env=True,
        )

    bronze = spark_task(
        "ingest_bronze",
        "{{ ti.xcom_pull(task_ids='resolve_partition')['src'] }} "
        "{{ logical_date.year }} {{ logical_date.month }}")
    silver = spark_task("transform_silver", "{{ logical_date.year }} {{ logical_date.month }}")
    gold = spark_task("load_gold", "{{ logical_date.year }} {{ logical_date.month }}")

    # Idempotent Snowflake load: delete the month, then COPY it back in.
    snowflake_load = SQLExecuteQueryOperator(
        task_id="snowflake_copy_into_raw",
        conn_id="snowflake_default",
        sql="""
            DELETE FROM RAW_DB.TLC.FCT_TRIPS_RAW
             WHERE year = {{ logical_date.year }} AND month = {{ logical_date.month }};

            COPY INTO RAW_DB.TLC.FCT_TRIPS_RAW
            FROM @RAW_DB.TLC.MINIO_GOLD_STAGE/fct_trips/year={{ logical_date.year }}/month={{ logical_date.month }}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt deps && dbt build --target prod "
                     "--vars '{run_month: \"{{ ds }}\"}'",
    )

    soda_scan = BashOperator(
        task_id="soda_scan_marts",
        bash_command=f"soda scan -d snowflake_prod -c {SODA_DIR}/configuration.yml {SODA_DIR}/checks/",
    )

    publish = BashOperator(
        task_id="publish_gold_dataset",
        bash_command="echo gold updated",
        outlets=[GOLD_ASSET],
    )

    p >> download >> bronze >> silver >> gold >> snowflake_load >> dbt_build >> soda_scan >> publish


monthly_batch_pipeline()
