"""Replay historical TLC trips into Kafka as a time-compressed event stream.

Design goals (these are what make the stream production-shaped):
  * Event time preserved: events are emitted in dropoff_datetime order, with
    inter-event gaps divided by REPLAY_SPEEDUP — rush-hour bursts stay bursty.
  * Deliberate disorder: a small fraction of events is delayed, so Flink's
    watermark / allowed-lateness handling actually gets exercised.
  * Schema Registry + Avro: the producer cannot publish anything that violates
    the registered contract (gap fix #2).
  * Idempotent, acks=all producer config: no silent loss on broker hiccups.
"""
from __future__ import annotations

import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("replay-producer")

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SR_URL = os.environ["SCHEMA_REGISTRY_URL"]
SPEEDUP = float(os.environ.get("REPLAY_SPEEDUP", "600"))
PARQUET = os.environ.get("TLC_PARQUET_PATH", "/data/yellow_tripdata_2024-01.parquet")
TOPIC = os.environ.get("TOPIC", "taxi_events")
LATE_FRACTION = float(os.environ.get("LATE_FRACTION", "0.02"))
LATE_MAX_SECONDS = float(os.environ.get("LATE_MAX_SECONDS", "90"))

def ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def load_events() -> list[dict]:
    log.info("Reading %s", PARQUET)
    table = pq.read_table(
        PARQUET,
        columns=[
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
            "DOLocationID", "passenger_count", "trip_distance", "fare_amount",
            "tip_amount", "total_amount", "payment_type",
        ],
    )
    df = table.to_pandas().dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    # basic sanity: keep plausible trips only (bad rows go through the batch
    # path's quarantine instead; the stream should look like live traffic)
    df = df[(df.tpep_dropoff_datetime > df.tpep_pickup_datetime)]
    df = df.sort_values("tpep_dropoff_datetime").reset_index(drop=True)
    log.info("Replaying %d events", len(df))

    events = []
    for row in df.itertuples(index=False):
        events.append({
            "trip_id": str(uuid.uuid4()),
            "pickup_datetime": ms(row.tpep_pickup_datetime.to_pydatetime()),
            "dropoff_datetime": ms(row.tpep_dropoff_datetime.to_pydatetime()),
            "pickup_zone_id": int(row.PULocationID) if row.PULocationID == row.PULocationID else None,
            "dropoff_zone_id": int(row.DOLocationID) if row.DOLocationID == row.DOLocationID else None,
            "passenger_count": int(row.passenger_count) if row.passenger_count == row.passenger_count else None,
            "trip_distance": float(row.trip_distance) if row.trip_distance == row.trip_distance else None,
            "fare_amount": float(row.fare_amount) if row.fare_amount == row.fare_amount else None,
            "tip_amount": float(row.tip_amount) if row.tip_amount == row.tip_amount else None,
            "total_amount": float(row.total_amount) if row.total_amount == row.total_amount else None,
            "payment_type": int(row.payment_type) if row.payment_type == row.payment_type else None,
        })
    return events

def build_producer() -> SerializingProducer:
    sr = SchemaRegistryClient({"url": SR_URL})
    with open("schemas/taxi_event.avsc") as f:
        serializer = AvroSerializer(sr, f.read())
    return SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": serializer,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
    })

def main() -> None:
    events = load_events()
    producer = build_producer()
    delayed: list[tuple[float, dict]] = []  # (release_wallclock, event)

    prev_event_ts = events[0]["dropoff_datetime"]
    for ev in events:
        # compress historical gap into wall-clock sleep
        gap_s = max(0.0, (ev["dropoff_datetime"] - prev_event_ts) / 1000.0) / SPEEDUP
        prev_event_ts = ev["dropoff_datetime"]
        if gap_s:
            time.sleep(min(gap_s, 5.0))

        now = time.time()
        # flush any delayed events whose time has come (this is the disorder)
        due = [e for t, e in delayed if t <= now]
        delayed = [(t, e) for t, e in delayed if t > now]
        for late_ev in due:
            emit(producer, late_ev)

        if random.random() < LATE_FRACTION:
            hold = random.uniform(5, LATE_MAX_SECONDS) / SPEEDUP * 60
            delayed.append((now + hold, ev))
        else:
            emit(producer, ev)

    for _, ev in delayed:
        emit(producer, ev)
    producer.flush()
    log.info("Replay complete.")

def emit(producer: SerializingProducer, ev: dict) -> None:
    ev = dict(ev)
    ev["event_produced_at"] = int(time.time() * 1000)
    key = str(ev["pickup_zone_id"] or 0)  # zone-keyed partitioning
    producer.produce(
        TOPIC, key=key, value=ev,
        on_delivery=lambda err, msg: err and log.error("Delivery failed: %s", err),
    )
    producer.poll(0)

if __name__ == "__main__":
    main()
