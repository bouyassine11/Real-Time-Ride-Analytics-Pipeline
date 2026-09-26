"""
delta/schemas.py
----------------
Defines PySpark StructType schemas for each Delta Lake layer.

Having schemas in one place means:
- All three Spark jobs import from here — no duplication
- Schema changes are made once and propagate everywhere
- Easy to version (add event_version checks in Silver)

Layers
------
BRONZE  — raw Kafka message, minimal typing (string JSON + metadata)
SILVER  — fully typed, parsed, validated ride event
GOLD_*  — aggregated metric schemas (one per Gold table)
"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Bronze schema
# Represents the raw Kafka message envelope.
# The actual ride event lives inside the `value` column as a JSON string.
# ---------------------------------------------------------------------------

BRONZE_SCHEMA = StructType([
    StructField("value",               StringType(),    nullable=False),  # raw JSON string
    StructField("kafka_topic",         StringType(),    nullable=True),
    StructField("kafka_partition",     LongType(),      nullable=True),
    StructField("kafka_offset",        LongType(),      nullable=True),
    StructField("kafka_timestamp",     TimestampType(), nullable=True),   # Kafka broker timestamp
    StructField("ingestion_timestamp", TimestampType(), nullable=False),  # when Spark read it
])

# ---------------------------------------------------------------------------
# Silver schema
# Fully parsed and typed ride event. Matches the JSON schema from Phase 1.
# ---------------------------------------------------------------------------

# Nested location struct (used for both pickup and dropoff)
LOCATION_SCHEMA = StructType([
    StructField("lat",     DoubleType(),  nullable=True),
    StructField("lon",     DoubleType(),  nullable=True),
    StructField("address", StringType(),  nullable=True),
])

SILVER_SCHEMA = StructType([
    StructField("event_id",          StringType(),    nullable=False),
    StructField("trip_id",           StringType(),    nullable=False),
    StructField("driver_id",         StringType(),    nullable=True),
    StructField("rider_id",          StringType(),    nullable=True),
    StructField("status",            StringType(),    nullable=True),
    StructField("city",              StringType(),    nullable=True),
    StructField("pickup",            LOCATION_SCHEMA, nullable=True),
    StructField("dropoff",           LOCATION_SCHEMA, nullable=True),
    StructField("fare_usd",          DoubleType(),    nullable=True),
    StructField("surge_multiplier",  DoubleType(),    nullable=True),
    StructField("total_fare_usd",    DoubleType(),    nullable=True),
    StructField("distance_miles",    DoubleType(),    nullable=True),
    StructField("event_timestamp",   TimestampType(), nullable=False),  # parsed from event JSON
    StructField("event_version",     StringType(),    nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=True), # carried from Bronze
])

# ---------------------------------------------------------------------------
# Gold schemas — one per aggregated metric table
# ---------------------------------------------------------------------------

# Rides per city per time window
GOLD_RIDES_PER_CITY_SCHEMA = StructType([
    StructField("window_start",  TimestampType(), nullable=False),
    StructField("window_end",    TimestampType(), nullable=False),
    StructField("city",          StringType(),    nullable=False),
    StructField("ride_count",    LongType(),      nullable=False),
    StructField("processed_at",  TimestampType(), nullable=False),
])

# Average fare and surge per city per time window
GOLD_FARE_METRICS_SCHEMA = StructType([
    StructField("window_start",       TimestampType(), nullable=False),
    StructField("window_end",         TimestampType(), nullable=False),
    StructField("city",               StringType(),    nullable=False),
    StructField("avg_fare_usd",       DoubleType(),    nullable=True),
    StructField("avg_surge",          DoubleType(),    nullable=True),
    StructField("avg_distance_miles", DoubleType(),    nullable=True),
    StructField("total_revenue_usd",  DoubleType(),    nullable=True),
    StructField("processed_at",       TimestampType(), nullable=False),
])

# Cancellation rate per city per time window
GOLD_CANCELLATION_SCHEMA = StructType([
    StructField("window_start",        TimestampType(), nullable=False),
    StructField("window_end",          TimestampType(), nullable=False),
    StructField("city",                StringType(),    nullable=False),
    StructField("total_rides",         LongType(),      nullable=False),
    StructField("cancelled_rides",     LongType(),      nullable=False),
    StructField("cancellation_rate",   DoubleType(),    nullable=True),  # 0.0 – 1.0
    StructField("processed_at",        TimestampType(), nullable=False),
])
