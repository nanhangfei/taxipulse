# Project Context
Lambda-architecture data platform on NYC TLC data.
Stack: MinIO, Kafka+Schema Registry, Airflow, Spark, Flink, Snowflake, dbt, Soda,
Terraform, Prometheus/Grafana, OpenLineage/Marquez, Docker Compose, GitHub Actions.

## Design decisions already made
- Lambda not Kappa: replay producer simulates streaming from batch TLC data
- Idempotency end-to-end: Spark dynamic partition overwrite, DELETE+COPY per month, dbt merge
- dbt tests = build-time gate; Soda = runtime operational gate (freshness/volume/drift)
- Quarantine bucket with reject reasons, never silent drops
- Reconciliation DAG compares Flink 5-min counts vs batch, 1% tolerance

## Known TODOs
- Flink image needs Kafka/Avro connector JARs (custom Dockerfile)
- Snowflake MINIO_GOLD_STAGE needs CREATE STAGE (or switch to real S3)
- dim_date model not yet written
- No pytest suite for Spark jobs yet
- Pinned versions may need bumps