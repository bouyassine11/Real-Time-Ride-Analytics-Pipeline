"""
spark/silver_transform.py
--------------------------
Delta Bronze → Delta Lake Silver

Reads from the Bronze Delta table (raw JSON strings) and produces a clean,
typed, deduplicated Silver table.

What this job does
------------------
1. Read the Bronze Delta table as a streaming source
2. Parse the JSON `value` column into typed struct columns using SILVER_SCHEMA
3. Flatten nested structs (pickup/dropoff) for easier querying
4. Drop records with null event_id or trip_id (can't deduplicate without them)
5. Deduplicate by event_id within each micro-batch (handles producer retries)
6. Write to Silver Delta table (append mode with Delta's MERGE for dedup)

Silver layer philosophy
-----------------------
Silver is the "single source of truth" for clean ride events. Every record here
is valid, typed, and unique by event_id. Downstream Gold jobs can trust this data
without any further cleaning.

Run this job
------------
  spark-submit spark/silver_transform.py
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

sys.path.insert(0, "/app")

from config.settings import (
    DELTA_BRONZE_PATH,
    DELTA_CHECKPOINT_PATH,
    DELTA_SILVER_PATH,
    SPARK_APP_NAME,
    SPARK_TRIGGER_INTERVAL,
)
from delta.schemas import SILVER_SCHEMA


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/app/delta_warehouse")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_bronze_stream(spark: SparkSession):
    """
    Read the Bronze Delta table as a streaming source.
    Using `ignoreChanges=true` so Delta schema evolution doesn't break the stream.
    """
    return (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(DELTA_BRONZE_PATH)
    )


def parse_json_to_silver(df, schema: StructType):
    """
    Parse the raw JSON string in `value` into typed columns.

    Steps:
    1. from_json() parses the JSON string using SILVER_SCHEMA
       — unknown fields are ignored, missing fields become null
    2. Alias the parsed struct as `event`
    3. Flatten all fields from the struct to top-level columns
    4. Parse the ISO-8601 `timestamp` string into a proper TimestampType
    5. Carry over `ingestion_timestamp` from Bronze for lineage tracking
    """
    # Build the inner JSON schema (without the metadata columns)
    # We reuse SILVER_SCHEMA but exclude ingestion_timestamp (it comes from Bronze)
    json_schema = StructType([
        f for f in schema.fields
        if f.name not in ("event_timestamp", "ingestion_timestamp")
    ])

    parsed = df.select(
        F.from_json(F.col("value"), json_schema).alias("event"),
        F.col("ingestion_timestamp"),
    )

    return parsed.select(
        F.col("event.event_id"),
        F.col("event.trip_id"),
        F.col("event.driver_id"),
        F.col("event.rider_id"),
        F.col("event.status"),
        F.col("event.city"),
        F.col("event.pickup"),
        F.col("event.dropoff"),
        F.col("event.fare_usd"),
        F.col("event.surge_multiplier"),
        F.col("event.total_fare_usd"),
        F.col("event.distance_miles"),
        # Parse ISO-8601 string → TimestampType
        F.to_timestamp(F.col("event.timestamp")).alias("event_timestamp"),
        F.col("event.event_version"),
        F.col("ingestion_timestamp"),
    )


def apply_quality_filters(df):
    """
    Drop records that can't be used downstream:
    - null event_id → can't deduplicate
    - null trip_id  → can't track trip lifecycle
    - null event_timestamp → can't do windowed aggregations
    - status not in the valid set → malformed event
    """
    valid_statuses = ["requested", "accepted", "started", "completed", "cancelled"]

    return df.filter(
        F.col("event_id").isNotNull() &
        F.col("trip_id").isNotNull() &
        F.col("event_timestamp").isNotNull() &
        F.col("status").isin(valid_statuses)
    )


def deduplicate_batch(batch_df, batch_id: int):
    """
    Called once per micro-batch by foreachBatch.
    Deduplicates by event_id within the batch, then merges into Silver Delta table
    using MERGE (upsert) — so even if the same event_id appears across batches,
    it won't be written twice.

    This is the standard pattern for exactly-once writes with Delta Lake.
    """
    from delta.tables import DeltaTable  # imported here to avoid Spark init issues

    # Step 1: drop duplicates within this micro-batch
    deduped = batch_df.dropDuplicates(["event_id"])

    if deduped.isEmpty():
        return

    spark = batch_df.sparkSession

    # Step 2: check if Silver table already exists
    if DeltaTable.isDeltaTable(spark, DELTA_SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, DELTA_SILVER_PATH)

        # MERGE: if event_id already exists → skip (do nothing)
        #        if event_id is new → insert
        (
            silver_table.alias("existing")
            .merge(
                deduped.alias("incoming"),
                "existing.event_id = incoming.event_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run — write the table fresh
        (
            deduped.write
            .format("delta")
            .mode("overwrite")
            .save(DELTA_SILVER_PATH)
        )


def write_silver_stream(df, checkpoint_path: str):
    """
    Use foreachBatch to handle deduplication logic per micro-batch.
    foreachBatch gives us full DataFrame API access inside each batch,
    which is required for Delta MERGE operations.
    """
    return (
        df.writeStream
        .format("delta")
        .foreachBatch(deduplicate_batch)
        .option("checkpointLocation", f"{checkpoint_path}/silver")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Silver] Reading from Bronze: {DELTA_BRONZE_PATH}")
    print(f"[Silver] Writing to Silver: {DELTA_SILVER_PATH}")

    bronze_stream = read_bronze_stream(spark)
    parsed_df = parse_json_to_silver(bronze_stream, SILVER_SCHEMA)
    clean_df = apply_quality_filters(parsed_df)

    query = write_silver_stream(clean_df, checkpoint_path=DELTA_CHECKPOINT_PATH)

    print("[Silver] Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
