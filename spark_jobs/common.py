"""Shared Spark session factory — MinIO creds come from env, never hardcoded."""
import os
from pyspark.sql import SparkSession

def spark_session(app: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app)
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # MinIO over http
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # idempotent reruns
    )
    # OpenLineage -> Marquez is wired in Stage 4; only enable it when the
    # collector URL is set, so Stage 2 batch runs need neither Marquez nor the
    # openlineage-spark jar on the classpath.
    ol_url = os.environ.get("OPENLINEAGE_URL")
    if ol_url:
        builder = (
            builder
            .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
            .config("spark.openlineage.transport.type", "http")
            .config("spark.openlineage.transport.url", ol_url)
            .config("spark.openlineage.namespace", os.environ.get("OPENLINEAGE_NAMESPACE", "nyc_taxi_platform"))
        )
    return builder.getOrCreate()
