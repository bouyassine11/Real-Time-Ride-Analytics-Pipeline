"""
spark/gold_aggregations.py
---------------------------
Delta Silver → Delta Lake Gold

Reads clean ride events from Silver and computes three business metric tables:

  1. gold_rides_per_city   — ride count per city per 1-minute window
  2. gold_fare_metrics     — avg fare, avg surge, total revenue per city per window
  3. gold_cancellation     — cancellation rate per city per window

Gold layer philosophy
---------------------
Gold tables are query-ready aggregations. They answer specific business questions
directly — a BI tool or dashboard queries Gold, never Silver or Bronze.
Each Gold table is optimised for one type of query (counts, financials, quality).

Windowing strategy
------------------
We use 1-minute tumbling windows on `event_timestamp` (the time the event
happened in the ride app, not when Spark processed it — this is event time).

Watermark of 2 minutes: Spark will wait up to 2 minutes for late-arriving
events before finalising a window. Events arriving more than 2 minutes late
are dropped. This is acceptable for operational dashboards.

Run this job
------------
  spark-submit spark/gold_aggregations.py
"""

import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, "/app")

from config.settings import (
    DELTA_CHECKPOINT_PATH,
    DELTA_GOLD_CANCELLATION_PATH,
    DELTA_GOLD_FARE_PATH,
    DELTA_GOLD_RIDES_PATH,
    DELTA_SILVER_PATH,
    SPARK_APP_NAME,
    SPARK_TRIGGER_INTERVAL,
    SPARK_WINDOW_DURATION,
    SPARK_WATERMARK_DELAY,
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/app/delta_warehouse")
        .config("spark.sql.shuffle.partitions", "4")
        # Enable streaming aggregations with watermark
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .getOrCreate()
    )


def read_silver_stream(spark: SparkSession) -> DataFrame:
    """
    Read Silver Delta table as a streaming source.
    All fields are already typed — no JSON parsing needed here.
    """
    return (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(DELTA_SILVER_PATH)
    )


def apply_watermark(df: DataFrame) -> DataFrame:
    """
    Apply event-time watermark on event_timestamp.

    Watermark tells Spark: "once the max event_timestamp seen exceeds
    window_end + WATERMARK_DELAY, finalise that window and stop accepting
    late events for it." Without a watermark, Spark would keep all window
    state in memory forever.
    """
    return df.withWatermark("event_timestamp", SPARK_WATERMARK_DELAY)


# ---------------------------------------------------------------------------
# Gold 1 — Rides per city per window
# ---------------------------------------------------------------------------

def compute_rides_per_city(df: DataFrame) -> DataFrame:
    """
    Count total ride events per city per time window.
    Includes all statuses (requested, completed, cancelled, etc.).
    """
    return (
        df
        .groupBy(
            F.window(F.col("event_timestamp"), SPARK_WINDOW_DURATION).alias("window"),
            F.col("city"),
        )
        .agg(
            F.count("*").alias("ride_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("city"),
            F.col("ride_count"),
            F.current_timestamp().alias("processed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Gold 2 — Fare metrics per city per window
# ---------------------------------------------------------------------------

def compute_fare_metrics(df: DataFrame) -> DataFrame:
    """
    Compute financial metrics for completed rides only.
    Cancelled or requested rides don't have meaningful fare data.
    """
    return (
        df
        .filter(F.col("status") == "completed")
        .groupBy(
            F.window(F.col("event_timestamp"), SPARK_WINDOW_DURATION).alias("window"),
            F.col("city"),
        )
        .agg(
            F.round(F.avg("total_fare_usd"), 2).alias("avg_fare_usd"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge"),
            F.round(F.avg("distance_miles"), 2).alias("avg_distance_miles"),
            F.round(F.sum("total_fare_usd"), 2).alias("total_revenue_usd"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("city"),
            F.col("avg_fare_usd"),
            F.col("avg_surge"),
            F.col("avg_distance_miles"),
            F.col("total_revenue_usd"),
            F.current_timestamp().alias("processed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Gold 3 — Cancellation rate per city per window
# ---------------------------------------------------------------------------

def compute_cancellation_rate(df: DataFrame) -> DataFrame:
    """
    Compute the fraction of rides that were cancelled per city per window.
    cancellation_rate = cancelled_rides / total_rides (0.0 to 1.0)

    Uses conditional aggregation:
      sum(CASE WHEN status='cancelled' THEN 1 ELSE 0 END)
    This avoids a separate filter + join, keeping it in one pass.
    """
    return (
        df
        .groupBy(
            F.window(F.col("event_timestamp"), SPARK_WINDOW_DURATION).alias("window"),
            F.col("city"),
        )
        .agg(
            F.count("*").alias("total_rides"),
            F.sum(
                F.when(F.col("status") == "cancelled", 1).otherwise(0)
            ).alias("cancelled_rides"),
        )
        .withColumn(
            "cancellation_rate",
            F.round(F.col("cancelled_rides") / F.col("total_rides"), 4),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("city"),
            F.col("total_rides"),
            F.col("cancelled_rides"),
            F.col("cancellation_rate"),
            F.current_timestamp().alias("processed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def write_gold_stream(df: DataFrame, name: str, output_path: str, checkpoint_path: str):
    """
    Write a Gold aggregation stream to Delta Lake.
    Uses 'complete' output mode because windowed aggregations need to update
    previously written windows when late data arrives within the watermark.
    """
    return (
        df.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{checkpoint_path}/gold_{name}")
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
        .start(output_path)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Gold] Reading from Silver: {DELTA_SILVER_PATH}")

    silver_stream = read_silver_stream(spark)
    watermarked = apply_watermark(silver_stream)

    # Compute all three Gold aggregations from the same Silver stream
    rides_df      = compute_rides_per_city(watermarked)
    fare_df       = compute_fare_metrics(watermarked)
    cancel_df     = compute_cancellation_rate(watermarked)

    # Start all three streaming queries concurrently
    q1 = write_gold_stream(rides_df,  "rides_per_city", DELTA_GOLD_RIDES_PATH,        DELTA_CHECKPOINT_PATH)
    q2 = write_gold_stream(fare_df,   "fare_metrics",   DELTA_GOLD_FARE_PATH,         DELTA_CHECKPOINT_PATH)
    q3 = write_gold_stream(cancel_df, "cancellation",   DELTA_GOLD_CANCELLATION_PATH, DELTA_CHECKPOINT_PATH)

    print("[Gold] All 3 streaming queries started:")
    print(f"  → rides_per_city  : {DELTA_GOLD_RIDES_PATH}")
    print(f"  → fare_metrics    : {DELTA_GOLD_FARE_PATH}")
    print(f"  → cancellation    : {DELTA_GOLD_CANCELLATION_PATH}")

    # Wait for all queries to finish (runs until manually stopped)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
