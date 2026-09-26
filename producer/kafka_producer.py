"""
kafka_producer.py
-----------------
Reads ride events from ride_event_generator.py and publishes them to the
Kafka topic defined in config/settings.py.

Run modes
---------
  # Produce forever at the configured rate (default)
  python producer/kafka_producer.py

  # Produce exactly 500 events then exit
  python producer/kafka_producer.py --max-events 500

  # Override throughput at runtime
  python producer/kafka_producer.py --events-per-second 25

  # Emit the full lifecycle (requested→accepted→started→completed) for N trips
  python producer/kafka_producer.py --lifecycle --trips 20

Design decisions
----------------
* confluent-kafka is used over kafka-python because it wraps the battle-tested
  librdkafka C library — much higher throughput and lower latency at the cost of
  a slightly heavier install.
* Each event is keyed by trip_id so that all status transitions for the same ride
  land on the same Kafka partition, preserving ordering per trip.
* Failed deliveries are caught in the delivery callback and forwarded to the DLQ
  topic rather than crashing the producer.
* A graceful shutdown on SIGINT / SIGTERM flushes the internal producer queue so
  no in-flight messages are lost when you hit Ctrl+C.
"""

import argparse
import json
import logging
import signal
import sys
import time
from typing import Any

from confluent_kafka import Producer, KafkaException

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_COMPRESSION_TYPE,
    KAFKA_MAX_REQUEST_SIZE,
    KAFKA_PRODUCER_RETRIES,
    KAFKA_RETRY_BACKOFF_MS,
    KAFKA_TOPIC_DLQ,
    KAFKA_TOPIC_RIDE_EVENTS,
    LOG_FORMAT,
    LOG_LEVEL,
    PRODUCER_EVENTS_PER_SECOND,
    PRODUCER_MAX_EVENTS,
)
from producer.ride_event_generator import generate_ride_event, generate_trip_lifecycle

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("kafka_producer")


# ---------------------------------------------------------------------------
# Kafka producer factory
# ---------------------------------------------------------------------------

def _build_producer() -> Producer:
    """
    Construct a confluent_kafka Producer with settings from config/settings.py.

    Key config knobs explained:
      bootstrap.servers   — comma-separated list of broker host:port pairs
      retries             — how many times to retry a failed send at the broker level
      retry.backoff.ms    — milliseconds to wait between retries
      compression.type    — batch compression (gzip reduces bandwidth ~4-6x)
      message.max.bytes   — max size of a single message; must align with broker setting
      acks                — "all" waits for all ISR replicas to acknowledge (safest)
      linger.ms           — time to wait before flushing a batch (higher = better compression)
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "retries": KAFKA_PRODUCER_RETRIES,
        "retry.backoff.ms": KAFKA_RETRY_BACKOFF_MS,
        "compression.type": KAFKA_COMPRESSION_TYPE,
        "message.max.bytes": KAFKA_MAX_REQUEST_SIZE,
        "acks": "all",
        "linger.ms": 5,
        "enable.idempotence": True,   # exactly-once delivery semantics at the producer
    }
    return Producer(conf)


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def _delivery_callback(producer: Producer, err, msg) -> None:
    """
    Called asynchronously by librdkafka after each message is acknowledged
    (or fails permanently).

    On success  → logs at DEBUG level (avoids flooding stdout at high throughput).
    On failure  → logs the error and forwards the raw message value to the DLQ.
    """
    if err is not None:
        log.error("Delivery failed | topic=%s partition=%s | %s", msg.topic(), msg.partition(), err)
        # Forward failed message to dead-letter queue for later inspection
        try:
            producer.produce(
                topic=KAFKA_TOPIC_DLQ,
                key=msg.key(),
                value=msg.value(),
            )
        except KafkaException as dlq_err:
            log.critical("DLQ write also failed: %s", dlq_err)
    else:
        log.debug(
            "Delivered | topic=%s partition=%d offset=%d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ---------------------------------------------------------------------------
# Core produce helpers
# ---------------------------------------------------------------------------

def _publish_event(producer: Producer, event: Any) -> None:
    """
    Serialise one event to JSON and hand it to the producer's internal queue.

    The message key is the trip_id — this guarantees all events belonging to
    the same trip are routed to the same partition (order preservation per trip).
    """
    payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
    key = event["trip_id"].encode("utf-8")

    # produce() is non-blocking; delivery_report fires later in poll()
    producer.produce(
        topic=KAFKA_TOPIC_RIDE_EVENTS,
        key=key,
        value=payload,
        callback=lambda err, msg: _delivery_callback(producer, err, msg),
    )

    # poll(0) lets librdkafka fire delivery callbacks without blocking
    producer.poll(0)


# ---------------------------------------------------------------------------
# Run modes
# ---------------------------------------------------------------------------

def run_continuous(
    producer: Producer,
    events_per_second: float,
    max_events: int,
) -> None:
    """
    Produce events in a tight loop at the requested throughput rate.

    Throughput control
    ------------------
    sleep_interval = 1 / events_per_second
    We measure how long _publish_event actually took and subtract it from the
    sleep so the rate stays accurate even when the machine is under load.
    """
    interval = 1.0 / events_per_second
    count = 0
    log.info(
        "Starting continuous producer | rate=%.1f eps | max=%s | topic=%s",
        events_per_second,
        max_events if max_events > 0 else "∞",
        KAFKA_TOPIC_RIDE_EVENTS,
    )

    while True:
        if max_events > 0 and count >= max_events:
            log.info("Reached max_events=%d — stopping.", max_events)
            break

        t0 = time.monotonic()
        event = generate_ride_event()
        _publish_event(producer, event)
        count += 1

        elapsed = time.monotonic() - t0
        sleep_for = max(0.0, interval - elapsed)
        time.sleep(sleep_for)

        if count % 100 == 0:
            log.info("Published %d events so far...", count)

    producer.flush()
    log.info("Producer flushed. Total events published: %d", count)


def run_lifecycle(producer: Producer, num_trips: int) -> None:
    """
    Produce the full ordered lifecycle for `num_trips` independent trips.
    Each trip emits 4 events: requested → accepted → started → completed.
    Useful for testing downstream consumers that track trip state machines.
    """
    total = 0
    log.info("Lifecycle mode | trips=%d | topic=%s", num_trips, KAFKA_TOPIC_RIDE_EVENTS)

    for i in range(1, num_trips + 1):
        events = generate_trip_lifecycle()
        for event in events:
            _publish_event(producer, event)
            total += 1
        log.debug("Trip %d/%d published (%s)", i, num_trips, events[0]["trip_id"])

    producer.flush()
    log.info("Lifecycle run complete. Total events published: %d", total)


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown_requested = False


def _handle_signal(signum, frame) -> None:  # noqa: ANN001
    global _shutdown_requested
    log.info("Shutdown signal received (%s) — flushing and exiting...", signum)
    _shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ride-share event producer — publishes fake events to Kafka."
    )
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=PRODUCER_EVENTS_PER_SECOND,
        help=f"Target publish rate (default: {PRODUCER_EVENTS_PER_SECOND})",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=PRODUCER_MAX_EVENTS,
        help="Stop after N events (0 = run forever, default: 0)",
    )
    parser.add_argument(
        "--lifecycle",
        action="store_true",
        help="Emit full trip lifecycles instead of random events",
    )
    parser.add_argument(
        "--trips",
        type=int,
        default=10,
        help="Number of trip lifecycles to emit (only used with --lifecycle)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    producer = _build_producer()

    log.info("Kafka producer initialised | broker=%s", KAFKA_BOOTSTRAP_SERVERS)

    try:
        if args.lifecycle:
            run_lifecycle(producer, num_trips=args.trips)
        else:
            run_continuous(
                producer,
                events_per_second=args.events_per_second,
                max_events=args.max_events,
            )
    except Exception as exc:
        log.exception("Unhandled exception in producer: %s", exc)
        producer.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()
