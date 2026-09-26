"""
Central configuration for the Real-Time Ride Analytics Pipeline.
All values fall back to sensible defaults so the project runs locally
without any environment setup. Override via a .env file or shell exports.
"""

import os

from dotenv import load_dotenv

# Load variables from a .env file if one exists (silently ignored if absent)
load_dotenv()


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Primary topic that the producer writes to
KAFKA_TOPIC_RIDE_EVENTS: str = os.getenv("KAFKA_TOPIC_RIDE_EVENTS", "ride_events")

# Dead-letter queue — malformed / undeliverable messages land here
KAFKA_TOPIC_DLQ: str = os.getenv("KAFKA_TOPIC_DLQ", "ride_events_dlq")

# How many times the producer retries a failed send before giving up
KAFKA_PRODUCER_RETRIES: int = int(os.getenv("KAFKA_PRODUCER_RETRIES", "3"))

# Milliseconds the producer waits before retrying
KAFKA_RETRY_BACKOFF_MS: int = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "500"))

# Compression applied to Kafka message batches (none | gzip | snappy | lz4)
KAFKA_COMPRESSION_TYPE: str = os.getenv("KAFKA_COMPRESSION_TYPE", "gzip")

# Maximum bytes per request sent to the broker
KAFKA_MAX_REQUEST_SIZE: int = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", "1048576"))  # 1 MB


# ---------------------------------------------------------------------------
# Ride Event Simulator
# ---------------------------------------------------------------------------

# Target events published per second (float allowed, e.g. 0.5 = 1 every 2 s)
PRODUCER_EVENTS_PER_SECOND: float = float(os.getenv("PRODUCER_EVENTS_PER_SECOND", "10"))

# Total events to produce before the script exits (0 = run forever)
PRODUCER_MAX_EVENTS: int = int(os.getenv("PRODUCER_MAX_EVENTS", "0"))

# Seed for the Faker / random number generator (set to reproduce a run exactly)
PRODUCER_RANDOM_SEED: int | None = (
    int(os.getenv("PRODUCER_RANDOM_SEED"))
    if os.getenv("PRODUCER_RANDOM_SEED")
    else None
)

# Cities that events are generated for, with rough bounding boxes
# Format: city_name -> (lat_min, lat_max, lon_min, lon_max)
CITY_BOUNDING_BOXES: dict[str, tuple[float, float, float, float]] = {
    "New York":     (40.477399, 40.917577, -74.259090, -73.700272),
    "San Francisco":(37.708305, 37.832238, -122.517971, -122.374895),
    "Chicago":      (41.644335, 42.023135, -87.940267, -87.524044),
    "Los Angeles":  (33.703652, 34.337306, -118.668176, -118.155289),
    "Austin":       (30.098659, 30.516863, -97.938383, -97.560767),
}

# Possible ride statuses and their relative probability weights
RIDE_STATUS_CHOICES: list[str] = ["requested", "accepted", "started", "completed", "cancelled"]
RIDE_STATUS_WEIGHTS: list[int] = [10, 10, 15, 55, 10]   # must sum to 100

# Fare range in USD
FARE_MIN_USD: float = float(os.getenv("FARE_MIN_USD", "3.50"))
FARE_MAX_USD: float = float(os.getenv("FARE_MAX_USD", "85.00"))

# Surge multiplier range (1.0 = no surge)
SURGE_MIN: float = float(os.getenv("SURGE_MIN", "1.0"))
SURGE_MAX: float = float(os.getenv("SURGE_MAX", "3.5"))


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "ride-analytics")

# How often each streaming micro-batch runs
SPARK_TRIGGER_INTERVAL: str = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")

# Tumbling window size for Gold aggregations
SPARK_WINDOW_DURATION: str = os.getenv("SPARK_WINDOW_DURATION", "1 minute")

# How long to wait for late-arriving events before closing a window
SPARK_WATERMARK_DELAY: str = os.getenv("SPARK_WATERMARK_DELAY", "2 minutes")

# Maven coordinates for Spark packages (downloaded at spark-submit time)
# Versions must align with the Spark version used in the Docker image (3.5.x)
SPARK_PACKAGES: str = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",   # Kafka connector
    "io.delta:delta-spark_2.12:3.1.0",                     # Delta Lake
])


# ---------------------------------------------------------------------------
# Delta Lake paths (inside the Spark container at /app/delta_data/)
# ---------------------------------------------------------------------------

_DELTA_BASE: str = os.getenv("DELTA_BASE_PATH", "/app/delta_data")

DELTA_BRONZE_PATH: str            = f"{_DELTA_BASE}/bronze"
DELTA_SILVER_PATH: str            = f"{_DELTA_BASE}/silver"
DELTA_GOLD_RIDES_PATH: str        = f"{_DELTA_BASE}/gold/rides_per_city"
DELTA_GOLD_FARE_PATH: str         = f"{_DELTA_BASE}/gold/fare_metrics"
DELTA_GOLD_CANCELLATION_PATH: str = f"{_DELTA_BASE}/gold/cancellation"
DELTA_CHECKPOINT_PATH: str        = f"{_DELTA_BASE}/checkpoints"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
