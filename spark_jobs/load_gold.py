"""Gold: analysis-ready aggregates + zone dimension join, written for Snowflake load."""
import sys
from pyspark.sql import functions as F
from common import spark_session

def main(year: int, month: int) -> None:
    spark = spark_session(f"gold-{year}-{month:02d}")
    trips = (spark.read.parquet("s3a://silver/trips")
             .where((F.col("year") == year) & (F.col("month") == month)))

    (trips.write.mode("overwrite").partitionBy("year", "month")
        .parquet("s3a://gold/fct_trips"))

    daily = (trips.groupBy(F.to_date("pickup_datetime").alias("trip_date"), "pickup_zone_id")
             .agg(F.count("*").alias("trip_count"),
                  F.sum("total_amount").alias("total_revenue"),
                  F.avg("fare_amount").alias("avg_fare"),
                  F.avg("tip_amount").alias("avg_tip"))
             .withColumn("year", F.lit(year)).withColumn("month", F.lit(month)))
    (daily.write.mode("overwrite").partitionBy("year", "month")
        .parquet("s3a://gold/daily_zone_agg"))
    spark.stop()

if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
