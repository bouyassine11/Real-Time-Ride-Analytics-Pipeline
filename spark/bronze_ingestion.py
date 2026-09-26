"""
spark/bronze_ingestion.py
--------------------------
Kafka → Delta Lake Bronze

This is the first Spark job in the pipeline. It reads raw messages from the
`ride_events` Kafka topic and writes them to the Bronze Delta table with
minimal transformation — just the raw JSON value plus Kafka metadata.

Bronze layer philosophy
-----------------------
Never throw data away at ingestion. Land everything as-is so you always have
the original record if something goes wrong downstream. Schema enforcement and
cleaning happen in Silver, not here.

What this job does
------------------
1. Connect to Kafka as a Structured Streaming source
2. Read each message (value = raw JSON bytes, key = trip_id bytes)
3. Add metadata columns: kafka_partition, kafka_offset, kafka_timestamp,
   ingestion_timestamp (when Spark actually processed it)
4. Write each micro-batch to the Bronze Delta table (append mode)
5. Checkpoint progress so the job can resume after a restart without
   re-reading already-processed messages

Run this job
------------
  spark-submit spark/bronze_ingestion.py

Or via Docker Compose:
  docker compose run spark spark-submit /app/spark/bronze_ingestion.py
"""

import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Append project root to sys.path so config/delta are importable
sys.path.insert(0, "/app")

from config.settings import (
    DELTA_BRONZE_PATH,
    DELTA_CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RIDE_EVENTS,
    SPARK_APP_NAME,
    SPARK_TRIGGER_INTERVAL,
)


def build_spark_session() -> SparkSession:
    """
    Create a SparkSession configured for:
    - Delta Lake (delta-spark package)
    - Kafka structured streaming (spark-sql-kafka connector)
    Both packages are loaded via spark.jars.packages and downloaded at runtime.
    """
    return (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Store Delta logs and checkpoints on local filesystem (works locally + Databricks)
        .config("spark.sql.warehouse.dir", "/app/delta_warehouse")
        # Reduce shuffle partitions for local single-node Spark (default 200 is too many)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """
    Create a streaming DataFrame from the Kafka topic.

    Kafka source columns returned by Spark:
      key        — message key bytes (trip_id)
      value      — message value bytes (raw JSON ride event)
      topic      — topic name string
      partition  — partition number int
      offset     — message offset long
      timestamp  — Kafka broker timestamp
      timestampType — 0=CreateTime, 1=LogAppendTime
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RIDE_EVENTS)
        # earliest: re-read from beginning on first run (no checkpoint yet)
        # latest: only new messages after checkpoint exists
        .option("startingOffsets", "earliest")
        # Limit records per micro-batch to avoid overwhelming local Spark
        .option("maxOffsetsPerTrigger", "1000")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_to_bronze(df):
    """
    Minimal transformation: cast bytes to strings and add metadata columns.
    The ride event JSON is kept intact in the `value` column — no parsing here.
    """
    return df.select(
        # Cast binary value to UTF-8 string — this is the raw JSON
        F.col("value").cast("string").alias("value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").cast("long").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        # ingestion_timestamp = wall-clock time when Spark processed this record
        F.current_timestamp().alias("ingestion_timestamp"),
    )


def write_bronze_stream(df, checkpoint_path: str, output_path: str):
    """
    Write the streaming DataFrame to Delta Lake in append mode.

    checkpoint_path: stores Spark streaming state (offsets, progress).
                     Without this, a restart would re-read all Kafka messages.
    output_path:     where the Delta table files are written.

    trigger processingTime: Spark waits SPARK_TRIGGER_INTERVAL between
    micro-batches. "10 seconds" is a good balance for near-real-time
    without overwhelming local resources.
    """
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/bronze")
        .option("mergeSchema", "true")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start(output_path)
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Bronze] Starting ingestion from topic: {KAFKA_TOPIC_RIDE_EVENTS}")
    print(f"[Bronze] Writing to: {DELTA_BRONZE_PATH}")

    raw_stream = read_kafka_stream(spark)
    bronze_df = transform_to_bronze(raw_stream)

    query = write_bronze_stream(
        bronze_df,
        checkpoint_path=DELTA_CHECKPOINT_PATH,
        output_path=DELTA_BRONZE_PATH,
    )

    print("[Bronze] Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
