"""
Delta Lake Table Manager
Creates, optimizes, and manages Delta Lake tables for the ride booking pipeline.
Run this ONCE before starting the streaming pipeline.
"""

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

DELTA_BASE = "abfss://delta@<your_storage>.dfs.core.windows.net/ride-pipeline"
# Local dev: DELTA_BASE = "/tmp/delta/ride-pipeline"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("DeltaLakeTableManager")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


TABLE_DEFINITIONS = {
    "ride_demand_by_zone": StructType([
        StructField("window_start",       TimestampType(), False),
        StructField("window_end",         TimestampType(), True),
        StructField("zone_id",            StringType(),    False),
        StructField("zone_name",          StringType(),    True),
        StructField("total_requests",     LongType(),      True),
        StructField("avg_estimated_fare", DoubleType(),    True),
        StructField("avg_surge",          DoubleType(),    True),
        StructField("total_distance_km",  DoubleType(),    True),
    ]),

    "surge_pricing_metrics": StructType([
        StructField("window_start",    TimestampType(), False),
        StructField("window_end",      TimestampType(), True),
        StructField("ride_type",       StringType(),    False),
        StructField("avg_surge",       DoubleType(),    True),
        StructField("max_surge",       DoubleType(),    True),
        StructField("high_surge_count", LongType(),     True),
        StructField("total_rides",     LongType(),      True),
        StructField("avg_fare",        DoubleType(),    True),
    ]),

    "location_density": StructType([
        StructField("window_start",      TimestampType(), False),
        StructField("zone_id",           StringType(),    False),
        StructField("zone_name",         StringType(),    True),
        StructField("driver_pings",      LongType(),      True),
        StructField("available_drivers", LongType(),      True),
        StructField("avg_speed_kmh",     DoubleType(),    True),
        StructField("centroid_lat",      DoubleType(),    True),
        StructField("centroid_lng",      DoubleType(),    True),
    ]),

    "revenue_analytics": StructType([
        StructField("window_start",      TimestampType(), False),
        StructField("window_end",        TimestampType(), True),
        StructField("zone_id",           StringType(),    False),
        StructField("ride_type",         StringType(),    False),
        StructField("payment_mode",      StringType(),    True),
        StructField("completed_rides",   LongType(),      True),
        StructField("total_revenue",     DoubleType(),    True),
        StructField("avg_fare",          DoubleType(),    True),
        StructField("avg_driver_rating", DoubleType(),    True),
        StructField("avg_wait_time",     DoubleType(),    True),
        StructField("avg_trip_duration", DoubleType(),    True),
    ]),

    "cancellation_analytics": StructType([
        StructField("window_start",       TimestampType(), False),
        StructField("zone_id",            StringType(),    False),
        StructField("total",              LongType(),      True),
        StructField("cancelled",          LongType(),      True),
        StructField("completed",          LongType(),      True),
        StructField("cancellation_rate",  DoubleType(),    True),
    ]),
}

# Partition columns per table (improves query performance in Power BI)
PARTITION_COLS = {
    "ride_demand_by_zone":    ["zone_id"],
    "surge_pricing_metrics":  ["ride_type"],
    "location_density":       ["zone_id"],
    "revenue_analytics":      ["zone_id", "ride_type"],
    "cancellation_analytics": ["zone_id"],
}

# Z-ORDER columns for file-level data skipping
ZORDER_COLS = {
    "ride_demand_by_zone":    ["window_start"],
    "surge_pricing_metrics":  ["window_start", "avg_surge"],
    "location_density":       ["window_start"],
    "revenue_analytics":      ["window_start", "total_revenue"],
    "cancellation_analytics": ["window_start", "cancellation_rate"],
}


def create_table(spark: SparkSession, name: str, schema: StructType,
                 partitions: list, path: str):
    if DeltaTable.isDeltaTable(spark, path):
        logger.info(f"Table '{name}' already exists at {path}")
        return
    (
        spark.createDataFrame([], schema)
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy(*partitions)
        .save(path)
    )
    # Enable Auto Optimize
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
              f"('delta.autoOptimize.optimizeWrite' = 'true', "
              f"'delta.autoOptimize.autoCompact' = 'true')")
    logger.info(f"✅ Created table '{name}' at {path}")


def optimize_table(spark: SparkSession, name: str, path: str, zorder_cols: list):
    logger.info(f"Running OPTIMIZE + ZORDER on '{name}'...")
    zorder_str = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zorder_str})")
    logger.info(f"✅ Optimized '{name}'")


def vacuum_table(spark: SparkSession, name: str, path: str,
                 retention_hours: int = 168):  # 7 days
    logger.info(f"Running VACUUM on '{name}' (retain {retention_hours}h)...")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    logger.info(f"✅ Vacuumed '{name}'")


def show_table_history(spark: SparkSession, name: str, path: str, limit: int = 5):
    print(f"\n=== History: {name} ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{path}`").show(limit, truncate=False)


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n🔧 Creating Delta Lake tables...\n")
    for table_name, schema in TABLE_DEFINITIONS.items():
        path = f"{DELTA_BASE}/{table_name}"
        create_table(
            spark, table_name, schema,
            PARTITION_COLS.get(table_name, []),
            path,
        )

    print("\n⚡ Running initial OPTIMIZE pass...\n")
    for table_name in TABLE_DEFINITIONS:
        path = f"{DELTA_BASE}/{table_name}"
        try:
            optimize_table(spark, table_name, path, ZORDER_COLS.get(table_name, []))
        except Exception as e:
            logger.warning(f"OPTIMIZE skipped for '{table_name}': {e}")

    print("\n✅ Delta Lake setup complete!\n")
    for table_name in TABLE_DEFINITIONS:
        path = f"{DELTA_BASE}/{table_name}"
        show_table_history(spark, table_name, path, limit=3)


if __name__ == "__main__":
    main()
