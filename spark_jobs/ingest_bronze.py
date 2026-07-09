"""Bronze ingestion: schema-enforced landing of TLC parquet into MinIO.

Idempotency: dynamic partition overwrite on (year, month) — rerunning the same
logical date replaces exactly that partition, never duplicates it.
Schema drift: columns are selected & cast EXPLICITLY. TLC added/renamed columns
over the years (e.g. airport_fee from 2021+); we normalize here, once.
"""
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from common import spark_session

def main(src_path: str, year: int, month: int) -> None:
    spark = spark_session(f"bronze-{year}-{month:02d}")
    df = spark.read.parquet(src_path)

    normalized = df.select(
        F.col("tpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        F.col("PULocationID").cast(IntegerType()).alias("pickup_zone_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("dropoff_zone_id"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("trip_distance").cast(DoubleType()).alias("trip_distance"),
        F.col("fare_amount").cast(DoubleType()).alias("fare_amount"),
        F.col("tip_amount").cast(DoubleType()).alias("tip_amount"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col("payment_type").cast(IntegerType()).alias("payment_type"),
        # airport_fee only exists in newer files — tolerate absence explicitly
        (F.col("airport_fee") if "airport_fee" in df.columns else F.lit(None))
            .cast(DoubleType()).alias("airport_fee"),
    ).withColumn("year", F.lit(year)).withColumn("month", F.lit(month)) \
     .withColumn("_ingested_at", F.current_timestamp())

    (normalized.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet("s3a://bronze/yellow_taxi"))
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
