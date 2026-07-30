#!/usr/bin/env python3
import json
import os
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events-in")
EVENTS_PER_SEC = int(os.getenv("EVENTS_PER_SEC", "50"))

RUNNING = True
DEVICE_IDS = [f"device-{i:03d}" for i in range(200)]
EVENT_TYPES = ["temp", "pressure", "vibration", "humidity"]


def stop_handler(signum, frame):
    del signum, frame
    global RUNNING
    RUNNING = False


def build_producer() -> Producer:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "flink-elastic-loadgen",
        "enable.idempotence": True,
        "linger.ms": 5,
        "batch.size": 32768,
        "compression.type": "lz4",
        "acks": "all",
    }
    return Producer(conf)


def make_event() -> dict:
    return {
        "device_id": random.choice(DEVICE_IDS),
        "event_type": random.choice(EVENT_TYPES),
        "value": round(random.uniform(1.0, 100.0), 3),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    producer = build_producer()
    print(
        f"Starting loadgen: bootstrap={BOOTSTRAP} topic={TOPIC} rate={EVENTS_PER_SEC} ev/s",
        flush=True,
    )

    interval = 1.0
    sent_total = 0
    while RUNNING:
        start = time.time()
        for _ in range(EVENTS_PER_SEC):
            event = make_event()
            key = event["device_id"].encode("utf-8")
            value = json.dumps(event).encode("utf-8")
            producer.produce(TOPIC, key=key, value=value)
            sent_total += 1
        producer.poll(0)

        elapsed = time.time() - start
        sleep_for = max(0.0, interval - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)

        if sent_total % (EVENTS_PER_SEC * 10) == 0:
            print(f"sent_total={sent_total}", flush=True)

    producer.flush(30)
    print("Loadgen stopped", flush=True)


if __name__ == "__main__":
    main()
