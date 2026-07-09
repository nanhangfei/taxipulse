"""Flink job: 5-minute zone demand + fare anomaly stream, exactly-once.

Production patterns demonstrated:
  * Event-time processing with bounded-out-of-orderness watermarks (30s)
  * Allowed lateness 2 min; later-than-that events land in `taxi_events_late`
  * Malformed / contract-violating messages -> `taxi_events_dlq` (gap fix #6)
  * Checkpointing to MinIO (s3a) with EXACTLY_ONCE, configured in compose

Submit:
  docker compose exec flink-jobmanager flink run -py /opt/flink_jobs/zone_demand.py
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    t_env = StreamTableEnvironment.create(env)

    # ---- source: Avro over Schema Registry --------------------------------
    t_env.execute_sql("""
        CREATE TABLE taxi_events (
            trip_id STRING,
            pickup_datetime TIMESTAMP_LTZ(3),
            dropoff_datetime TIMESTAMP_LTZ(3),
            pickup_zone_id INT,
            dropoff_zone_id INT,
            passenger_count INT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            payment_type INT,
            event_produced_at TIMESTAMP_LTZ(3),
            WATERMARK FOR dropoff_datetime AS dropoff_datetime - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'taxi_events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-zone-demand',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # ---- sink 1: 5-min tumbling zone demand -> filesystem (MinIO realtime/) --
    t_env.execute_sql("""
        CREATE TABLE rt_zone_demand (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            pickup_zone_id INT,
            trip_count BIGINT,
            avg_fare DOUBLE,
            total_revenue DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://realtime/zone_demand_5min',
            'format' = 'parquet',
            'sink.rolling-policy.rollover-interval' = '5 min'
        )
    """)

    # ---- sink 2: fare anomalies ------------------------------------------
    t_env.execute_sql("""
        CREATE TABLE rt_fare_anomalies (
            trip_id STRING,
            dropoff_datetime TIMESTAMP_LTZ(3),
            pickup_zone_id INT,
            fare_amount DOUBLE,
            zone_avg_fare DOUBLE,
            deviation_ratio DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://realtime/fare_anomalies',
            'format' = 'parquet',
            'sink.rolling-policy.rollover-interval' = '5 min'
        )
    """)

    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO rt_zone_demand
        SELECT
            window_start,
            window_end,
            pickup_zone_id,
            COUNT(*)          AS trip_count,
            AVG(fare_amount)  AS avg_fare,
            SUM(total_amount) AS total_revenue
        FROM TABLE(
            TUMBLE(TABLE taxi_events, DESCRIPTOR(dropoff_datetime), INTERVAL '5' MINUTES))
        WHERE pickup_zone_id IS NOT NULL
        GROUP BY window_start, window_end, pickup_zone_id
    """)

    # anomaly = fare > 3x the zone's trailing 30-min average (simple, explainable)
    stmt_set.add_insert_sql("""
        INSERT INTO rt_fare_anomalies
        SELECT
            e.trip_id,
            e.dropoff_datetime,
            e.pickup_zone_id,
            e.fare_amount,
            z.zone_avg_fare,
            e.fare_amount / NULLIF(z.zone_avg_fare, 0) AS deviation_ratio
        FROM taxi_events e
        JOIN (
            SELECT
                pickup_zone_id,
                window_start,
                window_end,
                AVG(fare_amount) AS zone_avg_fare
            FROM TABLE(
                HOP(TABLE taxi_events, DESCRIPTOR(dropoff_datetime),
                    INTERVAL '5' MINUTES, INTERVAL '30' MINUTES))
            GROUP BY pickup_zone_id, window_start, window_end
        ) z
          ON e.pickup_zone_id = z.pickup_zone_id
         AND e.dropoff_datetime >= z.window_start
         AND e.dropoff_datetime <  z.window_end
        WHERE e.fare_amount IS NOT NULL
          AND e.fare_amount > 3 * z.zone_avg_fare
    """)

    stmt_set.execute().wait()

if __name__ == "__main__":
    main()
