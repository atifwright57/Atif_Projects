"""Publish simulated business events to a Kafka topic."""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from kafka import KafkaProducer


LOGGER = logging.getLogger("kafka-producer")


def build_event(randomizer: random.Random, late: bool) -> dict[str, object]:
    event_time = datetime.now(timezone.utc)
    if late:
        event_time -= timedelta(minutes=60)

    return {
        "event_id": str(uuid4()),
        "customer_id": f"customer-{randomizer.randint(1, 50):03d}",
        "event_ts": event_time.isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        ),
        "event_type": randomizer.choice(["created", "updated", "completed"]),
        "amount": round(randomizer.uniform(1, 500), 2),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "business-events"))
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--interval-seconds", type=float, default=0.1)
    parser.add_argument("--duplicate-every", type=int, default=0)
    parser.add_argument("--late-every", type=int, default=0)
    parser.add_argument("--seed", type=int, default=None)
    arguments = parser.parse_args()
    if arguments.events < 1:
        parser.error("--events must be at least 1")
    if arguments.interval_seconds < 0:
        parser.error("--interval-seconds cannot be negative")
    if arguments.duplicate_every < 0 or arguments.late_every < 0:
        parser.error("--duplicate-every and --late-every cannot be negative")
    return arguments


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    randomizer = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )

    previous_event: dict[str, object] | None = None
    futures = []
    try:
        for index in range(args.events):
            is_duplicate = (
                args.duplicate_every > 0
                and index > 0
                and (index + 1) % args.duplicate_every == 0
            )
            if is_duplicate and previous_event is not None:
                event = previous_event
            else:
                is_late = args.late_every > 0 and (index + 1) % args.late_every == 0
                event = build_event(randomizer, is_late)
                previous_event = event

            futures.append(producer.send(args.topic, key=event["event_id"], value=event))
            LOGGER.info(
                "Queued event %s%s",
                event["event_id"],
                " (replay)" if is_duplicate else "",
            )
            if args.interval_seconds:
                time.sleep(args.interval_seconds)

        producer.flush(timeout=30)
        for future in futures:
            metadata = future.get(timeout=10)
            LOGGER.debug(
                "Published to %s partition=%s offset=%s",
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )
        LOGGER.info("Published %s messages to %s", len(futures), args.topic)
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
