"""Shared Spark session factory — MinIO creds come from env, never hardcoded."""
import os
from pyspark.sql import SparkSession

def spark_session(app: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # idempotent reruns
        # OpenLineage Spark listener -> Marquez (gap fix #4)
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://marquez:5000")
        .config("spark.openlineage.namespace", "nyc_taxi_platform")
        .getOrCreate()
    )
