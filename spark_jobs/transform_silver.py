"""Silver: clean + dedup. Invalid rows are QUARANTINED, never silently dropped.

Quarantine contract (gap fix #6): every rejected row is written with a
`_reject_reason`, partitioned the same way, so failures are auditable and
replayable after a rule fix.
"""
import sys
from pyspark.sql import functions as F
from common import spark_session

# Quality rules, in priority order. Built lazily inside main() because F.col()
# needs an active Spark JVM — evaluating it at import time raises AssertionError.
def _rules():
    return {
        "null_pickup": F.col("pickup_datetime").isNull(),
        "null_dropoff": F.col("dropoff_datetime").isNull(),
        "zero_or_negative_duration": F.col("dropoff_datetime") <= F.col("pickup_datetime"),
        "negative_fare": F.col("fare_amount") < 0,
        "absurd_distance": F.col("trip_distance") > 200,
    }

def main(year: int, month: int) -> None:
    spark = spark_session(f"silver-{year}-{month:02d}")
    df = (spark.read.parquet("s3a://bronze/yellow_taxi")
          .where((F.col("year") == year) & (F.col("month") == month)))

    reject_reason = F.lit(None).cast("string")
    for name, cond in _rules().items():
        reject_reason = F.when(cond & reject_reason.isNull(), F.lit(name)).otherwise(reject_reason)
    df = df.withColumn("_reject_reason", reject_reason)

    quarantined = df.where(F.col("_reject_reason").isNotNull())
    clean = df.where(F.col("_reject_reason").isNull()).drop("_reject_reason")

    # dedup on the natural key of a trip
    clean = clean.dropDuplicates(
        ["pickup_datetime", "dropoff_datetime", "pickup_zone_id",
         "dropoff_zone_id", "total_amount"])

    (quarantined.write.mode("overwrite").partitionBy("year", "month")
        .parquet("s3a://quarantine/yellow_taxi"))
    (clean.write.mode("overwrite").partitionBy("year", "month")
        .parquet("s3a://silver/trips"))

    # emit counts for the Airflow task to log / push to metrics
    total, bad = df.count(), quarantined.count()
    print(f"METRIC silver_total={total} silver_quarantined={bad} "
          f"quarantine_pct={bad / max(total, 1):.4f}")
    spark.stop()

if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
