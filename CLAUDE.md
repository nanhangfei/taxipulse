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
- Snowflake MINIO_GOLD_STAGE needs CREATE STAGE (or switch to real S3)
- dim_date model not yet written
- No pytest suite for Spark jobs yet
- Pinned versions may need bumps