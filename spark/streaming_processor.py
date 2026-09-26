"""
spark/streaming_processor.py
-----------------------------
Single PySpark Structured Streaming job.

Flow:
  Redpanda (Kafka protocol) → parse JSON events → aggregate KPIs → PostgreSQL

This replaces the three-layer Bronze/Silver/Gold architecture with a single
job that writes aggregated results directly to PostgreSQL tables every batch.
Grafana connects to PostgreSQL directly — no API layer needed.

PostgreSQL tables written:
  rides_per_city    — ride count per city per 1-minute window
  fare_metrics      — avg fare, surge multiplier, total revenue per city per window
  cancellation_rate — cancellation rate per city per window
  latest_events     — last 1000 raw events for debugging/inspection

Run:
  spark-submit spark/streaming_processor.py
"""

import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, "/app")

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RIDE_EVENTS,
    PG_DATABASE,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    SPARK_APP_NAME,
    SPARK_TRIGGER_INTERVAL,
    SPARK_WATERMARK_DELAY,
    SPARK_WINDOW_DURATION,
)

# ---------------------------------------------------------------------------
# PostgreSQL connection properties
# ---------------------------------------------------------------------------

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
PG_PROPERTIES = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ---------------------------------------------------------------------------
# Event JSON schema — matches producer/ride_event_generator.py output
# ---------------------------------------------------------------------------

LOCATION_SCHEMA = StructType([
    StructField("lat",     DoubleType(), True),
    StructField("lon",     DoubleType(), True),
    StructField("address", StringType(), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(), True),
    StructField("trip_id",          StringType(), True),
    StructField("driver_id",        StringType(), True),
    StructField("rider_id",         StringType(), True),
    StructField("status",           StringType(), True),
    StructField("city",             StringType(), True),
    StructField("pickup",           LOCATION_SCHEMA, True),
    StructField("dropoff",          LOCATION_SCHEMA, True),
    StructField("fare_usd",         DoubleType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("total_fare_usd",   DoubleType(), True),
    StructField("distance_miles",   DoubleType(), True),
    StructField("timestamp",        StringType(), True),
    StructField("event_version",    StringType(), True),
])


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Read from Redpanda/Kafka
# ---------------------------------------------------------------------------

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RIDE_EVENTS)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "5000")
        .option("failOnDataLoss", "false")
        .load()
    )


# ---------------------------------------------------------------------------
# Parse raw Kafka messages into typed event rows
# ---------------------------------------------------------------------------

def parse_events(df: DataFrame) -> DataFrame:
    """
    Parse the JSON value from Kafka into typed columns.
    Filter out records with missing required fields.
    """
    parsed = df.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        F.col("timestamp").alias("kafka_ts"),
    )

    return (
        parsed
        .select(
            F.col("e.event_id"),
            F.col("e.trip_id"),
            F.col("e.status"),
            F.col("e.city"),
            F.col("e.fare_usd"),
            F.col("e.surge_multiplier"),
            F.col("e.total_fare_usd"),
            F.col("e.distance_miles"),
            F.to_timestamp(F.col("e.timestamp")).alias("event_timestamp"),
        )
        .filter(
            F.col("event_id").isNotNull() &
            F.col("city").isNotNull() &
            F.col("event_timestamp").isNotNull() &
            F.col("status").isin(
                "requested", "accepted", "started", "completed", "cancelled"
            )
        )
        .withWatermark("event_timestamp", SPARK_WATERMARK_DELAY)
    )


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

def agg_rides_per_city(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy(
            F.window("event_timestamp", SPARK_WINDOW_DURATION).alias("w"),
            "city",
        )
        .agg(F.count("*").alias("ride_count"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "city",
            "ride_count",
            F.current_timestamp().alias("updated_at"),
        )
    )


def agg_fare_metrics(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("status") == "completed")
        .groupBy(
            F.window("event_timestamp", SPARK_WINDOW_DURATION).alias("w"),
            "city",
        )
        .agg(
            F.round(F.avg("total_fare_usd"), 2).alias("avg_fare_usd"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge"),
            F.round(F.avg("distance_miles"), 2).alias("avg_distance_miles"),
            F.round(F.sum("total_fare_usd"), 2).alias("total_revenue_usd"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "city",
            "avg_fare_usd",
            "avg_surge",
            "avg_distance_miles",
            "total_revenue_usd",
            F.current_timestamp().alias("updated_at"),
        )
    )


def agg_cancellation(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy(
            F.window("event_timestamp", SPARK_WINDOW_DURATION).alias("w"),
            "city",
        )
        .agg(
            F.count("*").alias("total_rides"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_rides"),
        )
        .withColumn(
            "cancellation_rate",
            F.round(F.col("cancelled_rides") / F.col("total_rides"), 4),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "city",
            "total_rides",
            "cancelled_rides",
            "cancellation_rate",
            F.current_timestamp().alias("updated_at"),
        )
    )


# ---------------------------------------------------------------------------
# Write batch to PostgreSQL
# ---------------------------------------------------------------------------

def make_pg_writer(table: str, pk_cols: list):
    """
    Returns a foreachBatch function that upserts rows into PostgreSQL.
    Uses INSERT ... ON CONFLICT DO UPDATE so history accumulates over time
    and Grafana timeseries panels show a proper time range of data.
    """
    def write_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        count = batch_df.count()
        print(f"[{table}] Upserting {count} rows (batch {batch_id})")

        # Write to a temp table then upsert into the real table
        temp_table = f"{table}_tmp_{batch_id}"
        (
            batch_df.write
            .jdbc(
                url=PG_URL,
                table=temp_table,
                mode="overwrite",
                properties=PG_PROPERTIES,
            )
        )

        # Build the upsert SQL
        cols = batch_df.columns
        set_clause = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in cols if c not in pk_cols
        )
        pk_clause = ", ".join(pk_cols)
        col_list  = ", ".join(cols)

        upsert_sql = f"""
            INSERT INTO {table} ({col_list})
            SELECT {col_list} FROM {temp_table}
            ON CONFLICT ({pk_clause}) DO UPDATE SET {set_clause};
            DROP TABLE IF EXISTS {temp_table};
        """

        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
        try:
            with conn.cursor() as cur:
                cur.execute(upsert_sql)
            conn.commit()
        finally:
            conn.close()

    return write_batch


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def wait_for_postgres(max_wait: int = 120) -> None:
    """Wait until PostgreSQL is accepting connections and init.sql has run."""
    import time
    import psycopg2
    waited = 0
    while waited < max_wait:
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
                user=PG_USER, password=PG_PASSWORD, connect_timeout=5
            )
            conn.close()
            print(f"[Processor] PostgreSQL ready at {PG_HOST}:{PG_PORT}")
            return
        except Exception as e:
            print(f"[Processor] Waiting for PostgreSQL... ({waited}s) — {e}")
            time.sleep(10)
            waited += 10
    raise RuntimeError(f"PostgreSQL not ready after {max_wait}s")


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Processor] Connecting to Redpanda: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[Processor] Writing to PostgreSQL: {PG_URL}")

    wait_for_postgres()

    raw = read_kafka_stream(spark)
    events = parse_events(raw)

    # Three aggregation streams from the same parsed source
    rides_df  = agg_rides_per_city(events)
    fare_df   = agg_fare_metrics(events)
    cancel_df = agg_cancellation(events)

    # Start all three streaming queries
    q1 = (
        rides_df.writeStream
        .foreachBatch(make_pg_writer("rides_per_city", ["window_start", "city"]))
        .option("checkpointLocation", "/tmp/checkpoints/rides_per_city")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )

    q2 = (
        fare_df.writeStream
        .foreachBatch(make_pg_writer("fare_metrics", ["window_start", "city"]))
        .option("checkpointLocation", "/tmp/checkpoints/fare_metrics")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )

    q3 = (
        cancel_df.writeStream
        .foreachBatch(make_pg_writer("cancellation_rate", ["window_start", "city"]))
        .option("checkpointLocation", "/tmp/checkpoints/cancellation_rate")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )

    print("[Processor] All 3 streaming queries started:")
    print("  → rides_per_city")
    print("  → fare_metrics")
    print("  → cancellation_rate")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
