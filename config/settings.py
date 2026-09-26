"""
Central configuration for the Real-Time Ride Analytics Pipeline.
All values fall back to sensible defaults so the project runs locally
without any environment setup. Override via a .env file or shell exports.

NOTE: Type annotations use Python 3.9-compatible syntax (no `X | Y`,
no built-in generics like list[str]) so this file works inside the
Apache Spark Docker image which ships with Python 3.9.
"""

import os
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

# Load variables from a .env file if one exists (silently ignored if absent)
load_dotenv()


# ---------------------------------------------------------------------------
# Kafka / Redpanda
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

KAFKA_TOPIC_RIDE_EVENTS: str = os.getenv("KAFKA_TOPIC_RIDE_EVENTS", "ride_events")

KAFKA_TOPIC_DLQ: str = os.getenv("KAFKA_TOPIC_DLQ", "ride_events_dlq")

KAFKA_PRODUCER_RETRIES: int = int(os.getenv("KAFKA_PRODUCER_RETRIES", "3"))

KAFKA_RETRY_BACKOFF_MS: int = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "500"))

KAFKA_COMPRESSION_TYPE: str = os.getenv("KAFKA_COMPRESSION_TYPE", "gzip")

KAFKA_MAX_REQUEST_SIZE: int = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", "1048576"))


# ---------------------------------------------------------------------------
# Ride Event Simulator
# ---------------------------------------------------------------------------

PRODUCER_EVENTS_PER_SECOND: float = float(os.getenv("PRODUCER_EVENTS_PER_SECOND", "10"))

PRODUCER_MAX_EVENTS: int = int(os.getenv("PRODUCER_MAX_EVENTS", "0"))

# Optional seed — use Optional[int] instead of int | None for Python 3.9
PRODUCER_RANDOM_SEED: Optional[int] = (
    int(os.getenv("PRODUCER_RANDOM_SEED"))
    if os.getenv("PRODUCER_RANDOM_SEED")
    else None
)

# Use Dict/Tuple from typing instead of dict[]/tuple[] built-in generics
CITY_BOUNDING_BOXES: Dict[str, Tuple[float, float, float, float]] = {
    "New York":      (40.477399, 40.917577, -74.259090, -73.700272),
    "San Francisco": (37.708305, 37.832238, -122.517971, -122.374895),
    "Chicago":       (41.644335, 42.023135, -87.940267, -87.524044),
    "Los Angeles":   (33.703652, 34.337306, -118.668176, -118.155289),
    "Austin":        (30.098659, 30.516863, -97.938383, -97.560767),
}

# Use List[str] instead of list[str]
RIDE_STATUS_CHOICES: List[str] = ["requested", "accepted", "started", "completed", "cancelled"]
RIDE_STATUS_WEIGHTS: List[int] = [10, 10, 15, 55, 10]

FARE_MIN_USD: float = float(os.getenv("FARE_MIN_USD", "3.50"))
FARE_MAX_USD: float = float(os.getenv("FARE_MAX_USD", "85.00"))

SURGE_MIN: float = float(os.getenv("SURGE_MIN", "1.0"))
SURGE_MAX: float = float(os.getenv("SURGE_MAX", "3.5"))


# ---------------------------------------------------------------------------
# PostgreSQL (replaces Delta Lake — Spark writes KPIs here, Grafana reads here)
# ---------------------------------------------------------------------------

PG_HOST:     str = os.getenv("PG_HOST",     "postgres")
PG_PORT:     str = os.getenv("PG_PORT",     "5432")
PG_DATABASE: str = os.getenv("PG_DATABASE", "rideanalytics")
PG_USER:     str = os.getenv("PG_USER",     "spark")
PG_PASSWORD: str = os.getenv("PG_PASSWORD", "spark")


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "ride-analytics")

SPARK_TRIGGER_INTERVAL: str = os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")

SPARK_WINDOW_DURATION: str = os.getenv("SPARK_WINDOW_DURATION", "1 minute")

SPARK_WATERMARK_DELAY: str = os.getenv("SPARK_WATERMARK_DELAY", "2 minutes")

SPARK_PACKAGES: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
