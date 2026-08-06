# Project Context
Lambda-architecture data platform on NYC TLC data.
Stack: MinIO, Kafka+Schema Registry, Airflow, Spark, Flink, Snowflake, dbt, Soda,
Prometheus/Grafana, OpenLineage/Marquez, Docker Compose, GitHub Actions, Metabase.

## Design decisions already made
- Lambda not Kappa: replay producer simulates streaming from batch TLC data
- Idempotency end-to-end: Spark dynamic partition overwrite, DELETE+COPY per month, dbt merge
- dbt tests = build-time gate; Soda = runtime operational gate (freshness/volume/drift)
- Quarantine bucket with reject reasons, never silent drops
- Reconciliation DAG compares Flink 5-min counts vs batch, 1% tolerance
- Roadmap ordered for NZ data-engineer JDs: warehouse+dbt first (Stage 1),
  streaming last (Stage 7, behind compose profile `streaming`)
- Snowflake objects via snowflake/setup.sql, not Terraform (removed — rarely
  in target JDs; recoverable from git history)
- BI = Metabase (Stage 6); powerbi/DESIGN.md is a design doc only

## Known TODOs
- Flink image needs Kafka/Avro connector JARs (custom Dockerfile)
- No pytest suite for Spark jobs yet (Stage 5)
- Pinned versions may need bumps

## Resolved
- Spark→MinIO S3A jars: spark_jobs/Dockerfile adds hadoop-aws + aws-sdk-bundle
- Gold→Snowflake: done via write_pandas PUT+COPY (internal stage) in
  scripts/load_gold_to_snowflake.py, not an external CREATE STAGE (Stage 2)
- dim_date model written (Stage 1)