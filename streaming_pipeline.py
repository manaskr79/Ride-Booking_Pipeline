"""
Spark Structured Streaming Pipeline
Processes ride booking events from Kafka in real-time and writes to Delta Lake.

Topics consumed:
  - ride-requests
  - ride-completed
  - driver-locations

Aggregations:
  - Ride demand by zone (5-min tumbling window)
  - Surge pricing metrics (10-min sliding window)
  - Location density heatmap (5-min tumbling window)
  - Revenue analytics (hourly)
  - Driver utilization (10-min)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    count, avg, sum as _sum, max as _max, min as _min,
    when, expr, round as _round,
    year, month, dayofweek, hour,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType,
)

# ─── Schemas ────────────────────────────────────────────────────────────────

LOCATION_SCHEMA = StructType([
    StructField("zone_id",   StringType(),  True),
    StructField("zone_name", StringType(),  True),
    StructField("latitude",  DoubleType(),  True),
    StructField("longitude", DoubleType(),  True),
])

RIDE_REQUEST_SCHEMA = StructType([
    StructField("event_id",          StringType(), True),
    StructField("event_type",        StringType(), True),
    StructField("timestamp",         StringType(), True),
    StructField("ride_id",           StringType(), True),
    StructField("user_id",           StringType(), True),
    StructField("ride_type",         StringType(), True),
    StructField("status",            StringType(), True),
    StructField("pickup",            LOCATION_SCHEMA, True),
    StructField("dropoff",           LOCATION_SCHEMA, True),
    StructField("distance_km",       DoubleType(), True),
    StructField("estimated_fare",    DoubleType(), True),
    StructField("surge_multiplier",  DoubleType(), True),
    StructField("payment_mode",      StringType(), True),
    StructField("hour_of_day",       DoubleType(), True),
    StructField("day_of_week",       StringType(), True),
])

RIDE_COMPLETED_SCHEMA = StructType([
    StructField("event_id",           StringType(), True),
    StructField("event_type",         StringType(), True),
    StructField("timestamp",          StringType(), True),
    StructField("ride_id",            StringType(), True),
    StructField("user_id",            StringType(), True),
    StructField("driver_id",          StringType(), True),
    StructField("ride_type",          StringType(), True),
    StructField("status",             StringType(), True),
    StructField("pickup_zone_id",     StringType(), True),
    StructField("dropoff_zone_id",    StringType(), True),
    StructField("distance_km",        DoubleType(), True),
    StructField("duration_minutes",   DoubleType(), True),
    StructField("wait_time_minutes",  DoubleType(), True),
    StructField("estimated_fare",     DoubleType(), True),
    StructField("actual_fare",        DoubleType(), True),
    StructField("surge_multiplier",   DoubleType(), True),
    StructField("payment_mode",       StringType(), True),
    StructField("driver_rating",      DoubleType(), True),
    StructField("user_rating",        DoubleType(), True),
    StructField("cancellation_reason", StringType(), True),
])

DRIVER_LOCATION_SCHEMA = StructType([
    StructField("event_id",        StringType(), True),
    StructField("event_type",      StringType(), True),
    StructField("timestamp",       StringType(), True),
    StructField("driver_id",       StringType(), True),
    StructField("latitude",        DoubleType(), True),
    StructField("longitude",       DoubleType(), True),
    StructField("zone_id",         StringType(), True),
    StructField("zone_name",       StringType(), True),
    StructField("is_available",    BooleanType(), True),
    StructField("current_speed_kmh", DoubleType(), True),
])

# ─── Config ─────────────────────────────────────────────────────────────────

KAFKA_SERVERS      = "localhost:9092"
CHECKPOINT_BASE    = "abfss://checkpoints@<your_storage>.dfs.core.windows.net/ride-pipeline"
DELTA_BASE         = "abfss://delta@<your_storage>.dfs.core.windows.net/ride-pipeline"

# For local dev / testing, override with:
# CHECKPOINT_BASE = "/tmp/checkpoints/ride-pipeline"
# DELTA_BASE      = "/tmp/delta/ride-pipeline"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RideBookingStreamingPipeline")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Kafka connector (adjust version if needed)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "io.delta:delta-core_2.12:2.4.0")
        # Tune for streaming
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topic: str):
    """Create a Kafka streaming DataFrame for a topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


def parse_ride_requests(spark: SparkSession):
    raw = read_kafka_stream(spark, "ride-requests")
    return (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), RIDE_REQUEST_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("pickup_zone_id",   col("pickup.zone_id"))
        .withColumn("pickup_zone_name", col("pickup.zone_name"))
        .withColumn("dropoff_zone_id",  col("dropoff.zone_id"))
    )


def parse_ride_completed(spark: SparkSession):
    raw = read_kafka_stream(spark, "ride-completed")
    return (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), RIDE_COMPLETED_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp(col("timestamp")))
    )


def parse_driver_locations(spark: SparkSession):
    raw = read_kafka_stream(spark, "driver-locations")
    return (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), DRIVER_LOCATION_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp(col("timestamp")))
    )


# ─── Aggregation Queries ──────────────────────────────────────────────────

def write_stream(df, path: str, checkpoint: str, output_mode: str = "append",
                 trigger_secs: int = 30):
    return (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint)
        .option("path", path)
        .trigger(processingTime=f"{trigger_secs} seconds")
        .start()
    )


def ride_demand_by_zone(requests_df):
    """Ride demand per zone per 5-minute tumbling window."""
    agg = (
        requests_df
        .withWatermark("event_ts", "2 minutes")
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            col("pickup_zone_id"),
            col("pickup_zone_name"),
        )
        .agg(
            count("*").alias("total_requests"),
            avg("estimated_fare").alias("avg_estimated_fare"),
            avg("surge_multiplier").alias("avg_surge"),
            _sum("distance_km").alias("total_distance_km"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("pickup_zone_id").alias("zone_id"),
            col("pickup_zone_name").alias("zone_name"),
            _round("total_requests", 0).alias("total_requests"),
            _round("avg_estimated_fare", 2).alias("avg_estimated_fare"),
            _round("avg_surge", 3).alias("avg_surge"),
            _round("total_distance_km", 2).alias("total_distance_km"),
        )
    )
    return write_stream(
        agg,
        path=f"{DELTA_BASE}/ride_demand_by_zone",
        checkpoint=f"{CHECKPOINT_BASE}/ride_demand_by_zone",
        output_mode="append",
    )


def surge_pricing_metrics(requests_df):
    """Surge pricing per ride type per 10-min sliding window."""
    agg = (
        requests_df
        .withWatermark("event_ts", "5 minutes")
        .groupBy(
            window(col("event_ts"), "10 minutes", "5 minutes"),   # sliding
            col("ride_type"),
        )
        .agg(
            avg("surge_multiplier").alias("avg_surge"),
            _max("surge_multiplier").alias("max_surge"),
            count(when(col("surge_multiplier") > 1.5, True)).alias("high_surge_count"),
            count("*").alias("total_rides"),
            avg("estimated_fare").alias("avg_fare"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "ride_type",
            _round("avg_surge", 3).alias("avg_surge"),
            _round("max_surge", 3).alias("max_surge"),
            "high_surge_count",
            "total_rides",
            _round("avg_fare", 2).alias("avg_fare"),
        )
    )
    return write_stream(
        agg,
        path=f"{DELTA_BASE}/surge_pricing_metrics",
        checkpoint=f"{CHECKPOINT_BASE}/surge_pricing_metrics",
        output_mode="append",
    )


def location_density_heatmap(driver_df):
    """Driver density per zone per 5-min window for heatmap."""
    agg = (
        driver_df
        .withWatermark("event_ts", "2 minutes")
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            col("zone_id"),
            col("zone_name"),
        )
        .agg(
            count("driver_id").alias("driver_pings"),
            count(when(col("is_available"), True)).alias("available_drivers"),
            avg("current_speed_kmh").alias("avg_speed_kmh"),
            avg("latitude").alias("centroid_lat"),
            avg("longitude").alias("centroid_lng"),
        )
        .select(
            col("window.start").alias("window_start"),
            "zone_id", "zone_name",
            "driver_pings", "available_drivers",
            _round("avg_speed_kmh", 1).alias("avg_speed_kmh"),
            _round("centroid_lat", 6).alias("centroid_lat"),
            _round("centroid_lng", 6).alias("centroid_lng"),
        )
    )
    return write_stream(
        agg,
        path=f"{DELTA_BASE}/location_density",
        checkpoint=f"{CHECKPOINT_BASE}/location_density",
        output_mode="append",
    )


def revenue_analytics(completed_df):
    """Hourly revenue per zone and ride type."""
    enriched = (
        completed_df
        .filter(col("status") == "COMPLETED")
        .withColumn("ride_hour", hour(col("event_ts")))
    )
    agg = (
        enriched
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            window(col("event_ts"), "1 hour"),
            col("pickup_zone_id"),
            col("ride_type"),
            col("payment_mode"),
        )
        .agg(
            count("*").alias("completed_rides"),
            _sum("actual_fare").alias("total_revenue"),
            avg("actual_fare").alias("avg_fare"),
            avg("driver_rating").alias("avg_driver_rating"),
            avg("wait_time_minutes").alias("avg_wait_time"),
            avg("duration_minutes").alias("avg_trip_duration"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("pickup_zone_id").alias("zone_id"),
            "ride_type", "payment_mode",
            "completed_rides",
            _round("total_revenue", 2).alias("total_revenue"),
            _round("avg_fare", 2).alias("avg_fare"),
            _round("avg_driver_rating", 2).alias("avg_driver_rating"),
            _round("avg_wait_time", 2).alias("avg_wait_time"),
            _round("avg_trip_duration", 2).alias("avg_trip_duration"),
        )
    )
    return write_stream(
        agg,
        path=f"{DELTA_BASE}/revenue_analytics",
        checkpoint=f"{CHECKPOINT_BASE}/revenue_analytics",
        output_mode="append",
    )


def cancellation_analytics(completed_df):
    """Track cancellation rates per zone per 15-min window."""
    agg = (
        completed_df
        .withWatermark("event_ts", "5 minutes")
        .groupBy(
            window(col("event_ts"), "15 minutes"),
            col("pickup_zone_id"),
        )
        .agg(
            count("*").alias("total"),
            count(when(col("status") == "CANCELLED", True)).alias("cancelled"),
            count(when(col("status") == "COMPLETED", True)).alias("completed"),
        )
        .withColumn(
            "cancellation_rate",
            _round(col("cancelled") / col("total"), 4)
        )
        .select(
            col("window.start").alias("window_start"),
            col("pickup_zone_id").alias("zone_id"),
            "total", "cancelled", "completed", "cancellation_rate",
        )
    )
    return write_stream(
        agg,
        path=f"{DELTA_BASE}/cancellation_analytics",
        checkpoint=f"{CHECKPOINT_BASE}/cancellation_analytics",
        output_mode="append",
    )


# ─── Raw Event Sink (for audit / replay) ─────────────────────────────────

def sink_raw_events(df, topic_name: str):
    return write_stream(
        df,
        path=f"{DELTA_BASE}/raw/{topic_name}",
        checkpoint=f"{CHECKPOINT_BASE}/raw/{topic_name}",
        output_mode="append",
        trigger_secs=60,
    )


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Parse streams
    requests_df = parse_ride_requests(spark)
    completed_df = parse_ride_completed(spark)
    driver_df = parse_driver_locations(spark)

    # Launch all streaming queries
    queries = [
        ride_demand_by_zone(requests_df),
        surge_pricing_metrics(requests_df),
        location_density_heatmap(driver_df),
        revenue_analytics(completed_df),
        cancellation_analytics(completed_df),
        sink_raw_events(requests_df, "ride_requests"),
        sink_raw_events(completed_df, "ride_completed"),
        sink_raw_events(driver_df, "driver_locations"),
    ]

    print(f"\n✅ {len(queries)} streaming queries running. Press Ctrl+C to stop.\n")

    # Keep alive — await termination of any query
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
