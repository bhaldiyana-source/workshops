# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Trajectory Analysis and Path Reconstruction
# MAGIC
# MAGIC ## Overview
# MAGIC This demo focuses on analyzing GPS trajectories in real-time using Spark Structured Streaming. You'll learn to aggregate GPS points into trajectories, calculate speed and direction, apply path smoothing algorithms, detect stops and dwell times, and identify movement anomalies. We'll implement windowing strategies for path reconstruction and use statistical methods for trajectory analysis.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Aggregate GPS points into trajectories using window functions
# MAGIC - Reconstruct paths from out-of-order GPS data
# MAGIC - Calculate speed and direction from sequential points
# MAGIC - Implement path smoothing with moving averages
# MAGIC - Detect stops and calculate dwell times
# MAGIC - Identify anomalous movement patterns
# MAGIC - Calculate trajectory statistics (distance, duration, etc.)
# MAGIC - Generate trajectory LineString geometries
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1 and Demo 2
# MAGIC - Understanding of window functions
# MAGIC - Basic knowledge of trajectory analysis concepts
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install faker h3 numpy --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import math

# Configuration
catalog = "main"
schema_name = "geospatial_streaming"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Using catalog: {catalog}")
print(f"Using schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Load GPS Stream
# MAGIC
# MAGIC We'll load the GPS data stream from our previous demos.

# COMMAND ----------

# DBTITLE 1,Create GPS Stream
gps_table = f"{catalog}.{schema_name}.gps_deduplicated"

gps_stream = (
    spark.readStream
    .format("delta")
    .table(gps_table)
    .filter("is_valid = true")
    .select(
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "altitude",
        "speed",
        "heading",
        "accuracy",
        "device_type",
        "h3_index"
    )
)

print("GPS stream loaded")
print(f"Is streaming: {gps_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Calculate Speed and Direction from Sequential Points
# MAGIC
# MAGIC Using window functions, we'll calculate derived metrics from GPS point sequences.

# COMMAND ----------

# DBTITLE 1,Add Previous Point Using Lag Function
# Define window spec for each device ordered by time
device_window = Window.partitionBy("device_id").orderBy("timestamp")

# Add previous point information
gps_with_previous = (
    gps_stream
    .withColumn("prev_timestamp", F.lag("timestamp").over(device_window))
    .withColumn("prev_latitude", F.lag("latitude").over(device_window))
    .withColumn("prev_longitude", F.lag("longitude").over(device_window))
    .withColumn("prev_altitude", F.lag("altitude").over(device_window))
    .withColumn("prev_speed", F.lag("speed").over(device_window))
)

# COMMAND ----------

# DBTITLE 1,Calculate Distance Between Points
# Calculate distance using Haversine formula
gps_with_distance = gps_with_previous.withColumn(
    "distance_meters",
    F.when(
        F.col("prev_latitude").isNotNull(),
        F.expr("""
            ST_Distance(
                ST_Point(prev_longitude, prev_latitude),
                ST_Point(longitude, latitude)
            )
        """)
    ).otherwise(0.0)
)

# COMMAND ----------

# DBTITLE 1,Calculate Time Delta and Derived Speed
gps_enriched = (
    gps_with_distance
    .withColumn(
        "time_delta_seconds",
        F.when(
            F.col("prev_timestamp").isNotNull(),
            F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")
        ).otherwise(0.0)
    )
    .withColumn(
        "calculated_speed_mps",
        F.when(
            (F.col("time_delta_seconds") > 0) & (F.col("time_delta_seconds") < 300),  # < 5 min
            F.col("distance_meters") / F.col("time_delta_seconds")
        ).otherwise(None)
    )
    .withColumn(
        "calculated_speed_kmh",
        F.col("calculated_speed_mps") * 3.6
    )
)

# COMMAND ----------

# DBTITLE 1,Calculate Bearing/Direction
# Calculate bearing (direction of travel) in degrees
gps_with_bearing = gps_enriched.withColumn(
    "calculated_bearing",
    F.when(
        F.col("prev_latitude").isNotNull(),
        F.expr("""
            ST_Azimuth(
                ST_Point(prev_longitude, prev_latitude),
                ST_Point(longitude, latitude)
            ) * 180.0 / PI()
        """)
    ).otherwise(None)
).withColumn(
    "calculated_bearing_normalized",
    F.when(
        F.col("calculated_bearing").isNotNull(),
        (F.col("calculated_bearing") + 360) % 360
    ).otherwise(None)
)

# COMMAND ----------

# DBTITLE 1,Calculate Elevation Change and Slope
gps_with_elevation = gps_with_bearing.withColumn(
    "elevation_change_meters",
    F.when(
        F.col("prev_altitude").isNotNull() & F.col("altitude").isNotNull(),
        F.col("altitude") - F.col("prev_altitude")
    ).otherwise(None)
).withColumn(
    "slope_percent",
    F.when(
        (F.col("distance_meters") > 0) & F.col("elevation_change_meters").isNotNull(),
        (F.col("elevation_change_meters") / F.col("distance_meters")) * 100
    ).otherwise(None)
)

# COMMAND ----------

# DBTITLE 1,Add Movement Classification
gps_classified = gps_with_elevation.withColumn(
    "movement_state",
    F.when(F.col("calculated_speed_kmh").isNull(), "UNKNOWN")
     .when(F.col("calculated_speed_kmh") < 1, "STATIONARY")
     .when(F.col("calculated_speed_kmh") < 10, "SLOW")
     .when(F.col("calculated_speed_kmh") < 50, "NORMAL")
     .when(F.col("calculated_speed_kmh") < 100, "FAST")
     .otherwise("VERY_FAST")
).withColumn(
    "is_moving",
    F.col("calculated_speed_kmh") >= 1
)

# COMMAND ----------

# DBTITLE 1,Write Enriched GPS Stream
enriched_table = f"{catalog}.{schema_name}.gps_trajectory_enriched"
enriched_checkpoint = f"{checkpoint_base}/trajectory_enriched"

query_enriched = (
    gps_classified
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", enriched_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(enriched_table)
)

print(f"Started enriched GPS query: {query_enriched.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Path Smoothing with Moving Averages
# MAGIC
# MAGIC GPS data can be noisy. We'll apply smoothing to create cleaner trajectories.

# COMMAND ----------

# DBTITLE 1,Apply Moving Average Smoothing
# Define window for moving average (last 5 points)
moving_avg_window = (
    Window.partitionBy("device_id")
    .orderBy("timestamp")
    .rowsBetween(-2, 2)  # 2 before, current, 2 after
)

gps_smoothed = (
    gps_classified
    .withColumn("smoothed_latitude", F.avg("latitude").over(moving_avg_window))
    .withColumn("smoothed_longitude", F.avg("longitude").over(moving_avg_window))
    .withColumn("smoothed_speed", F.avg("calculated_speed_kmh").over(moving_avg_window))
    .withColumn("smoothed_altitude", F.avg("altitude").over(moving_avg_window))
)

# COMMAND ----------

# DBTITLE 1,Calculate Acceleration
# Calculate acceleration (change in speed)
gps_with_acceleration = gps_smoothed.withColumn(
    "acceleration_mps2",
    F.when(
        (F.col("time_delta_seconds") > 0) & (F.col("time_delta_seconds") < 60),
        (F.col("calculated_speed_mps") - F.col("prev_speed")) / F.col("time_delta_seconds")
    ).otherwise(None)
).withColumn(
    "acceleration_classification",
    F.when(F.col("acceleration_mps2").isNull(), "UNKNOWN")
     .when(F.col("acceleration_mps2") < -2, "HARD_BRAKING")
     .when(F.col("acceleration_mps2") < -0.5, "DECELERATING")
     .when(F.col("acceleration_mps2").between(-0.5, 0.5), "CONSTANT")
     .when(F.col("acceleration_mps2") < 2, "ACCELERATING")
     .otherwise("HARD_ACCELERATION")
)

# COMMAND ----------

# DBTITLE 1,Write Smoothed Trajectory Data
smoothed_table = f"{catalog}.{schema_name}.gps_trajectory_smoothed"
smoothed_checkpoint = f"{checkpoint_base}/trajectory_smoothed"

query_smoothed = (
    gps_with_acceleration
    .select(
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "smoothed_latitude",
        "smoothed_longitude",
        "altitude",
        "smoothed_altitude",
        "calculated_speed_kmh",
        "smoothed_speed",
        "calculated_bearing_normalized",
        "acceleration_mps2",
        "acceleration_classification",
        "movement_state",
        "is_moving",
        "distance_meters",
        "elevation_change_meters",
        "h3_index"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", smoothed_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(smoothed_table)
)

print(f"Started smoothed trajectory query: {query_smoothed.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Stop Detection and Dwell Time Analysis

# COMMAND ----------

# DBTITLE 1,Detect Stationary Periods
# Identify when devices are stopped
stationary_points = (
    gps_classified
    .filter("movement_state = 'STATIONARY' OR calculated_speed_kmh < 2")
    .select(
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "calculated_speed_kmh",
        "h3_index"
    )
)

# COMMAND ----------

# DBTITLE 1,Aggregate Stationary Periods into Stops
# Group consecutive stationary points into stop events
stops_aggregated = (
    stationary_points
    .withWatermark("timestamp", "30 minutes")
    .groupBy(
        "device_id",
        F.window("timestamp", "30 minutes", "5 minutes"),  # Sliding window
        "h3_index"
    )
    .agg(
        F.min("timestamp").alias("stop_start"),
        F.max("timestamp").alias("stop_end"),
        F.avg("latitude").alias("stop_latitude"),
        F.avg("longitude").alias("stop_longitude"),
        F.count("*").alias("point_count")
    )
    .select(
        "device_id",
        "stop_start",
        "stop_end",
        "stop_latitude",
        "stop_longitude",
        "h3_index",
        "point_count"
    )
    .withColumn(
        "dwell_time_seconds",
        F.unix_timestamp("stop_end") - F.unix_timestamp("stop_start")
    )
    .withColumn(
        "dwell_time_minutes",
        F.col("dwell_time_seconds") / 60
    )
    .filter("dwell_time_minutes >= 5")  # Only stops >= 5 minutes
    .withColumn("stop_id", F.expr("uuid()"))
)

# COMMAND ----------

# DBTITLE 1,Write Stop Events
stops_table = f"{catalog}.{schema_name}.trajectory_stops"
stops_checkpoint = f"{checkpoint_base}/trajectory_stops"

query_stops = (
    stops_aggregated
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", stops_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(stops_table)
)

print(f"Started stops detection query: {query_stops.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Trajectory Segment Reconstruction
# MAGIC
# MAGIC Aggregate points into trajectory segments with LineString geometries.

# COMMAND ----------

# DBTITLE 1,Create Trajectory Segments
# Aggregate GPS points into 5-minute trajectory segments
trajectory_segments = (
    gps_smoothed
    .filter("is_moving = true")
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        "device_id",
        F.window("timestamp", "5 minutes")
    )
    .agg(
        F.min("timestamp").alias("segment_start"),
        F.max("timestamp").alias("segment_end"),
        F.count("*").alias("point_count"),
        F.sum("distance_meters").alias("total_distance_meters"),
        F.avg("calculated_speed_kmh").alias("avg_speed_kmh"),
        F.max("calculated_speed_kmh").alias("max_speed_kmh"),
        F.min("calculated_speed_kmh").alias("min_speed_kmh"),
        F.avg("smoothed_speed").alias("avg_smoothed_speed_kmh"),
        # Collect points for LineString
        F.collect_list(
            F.struct(
                "timestamp",
                "smoothed_latitude",
                "smoothed_longitude",
                "smoothed_altitude"
            )
        ).alias("points")
    )
    .select(
        "device_id",
        F.col("window.start").alias("segment_start"),
        F.col("window.end").alias("segment_end"),
        "point_count",
        "total_distance_meters",
        F.round("avg_speed_kmh", 2).alias("avg_speed_kmh"),
        F.round("max_speed_kmh", 2).alias("max_speed_kmh"),
        F.round("min_speed_kmh", 2).alias("min_speed_kmh"),
        F.round("avg_smoothed_speed_kmh", 2).alias("avg_smoothed_speed_kmh"),
        "points"
    )
)

# COMMAND ----------

# DBTITLE 1,Add Segment Metrics and Classification
trajectory_metrics = trajectory_segments.withColumn(
    "duration_seconds",
    F.unix_timestamp("segment_end") - F.unix_timestamp("segment_start")
).withColumn(
    "duration_minutes",
    F.round(F.col("duration_seconds") / 60, 2)
).withColumn(
    "distance_km",
    F.round(F.col("total_distance_meters") / 1000, 3)
).withColumn(
    "segment_type",
    F.when(F.col("point_count") < 3, "INSUFFICIENT_DATA")
     .when(F.col("avg_speed_kmh") < 5, "SLOW_MOVEMENT")
     .when(F.col("avg_speed_kmh") < 30, "URBAN")
     .when(F.col("avg_speed_kmh") < 80, "SUBURBAN")
     .otherwise("HIGHWAY")
).withColumn(
    "speed_variation",
    F.col("max_speed_kmh") - F.col("min_speed_kmh")
).withColumn(
    "data_quality",
    F.when(F.col("point_count") >= 30, "HIGH")
     .when(F.col("point_count") >= 10, "MEDIUM")
     .otherwise("LOW")
).withColumn("segment_id", F.expr("uuid()"))

# COMMAND ----------

# DBTITLE 1,Write Trajectory Segments
segments_table = f"{catalog}.{schema_name}.trajectory_segments"
segments_checkpoint = f"{checkpoint_base}/trajectory_segments"

query_segments = (
    trajectory_metrics
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", segments_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(segments_table)
)

print(f"Started trajectory segments query: {query_segments.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Anomaly Detection

# COMMAND ----------

# DBTITLE 1,Detect Speed Anomalies
# Identify unusual speed patterns
speed_anomalies = (
    gps_with_acceleration
    .filter(
        (F.col("calculated_speed_kmh") > 120) |  # Very high speed
        (F.col("acceleration_classification") == "HARD_ACCELERATION") |
        (F.col("acceleration_classification") == "HARD_BRAKING")
    )
    .withColumn("anomaly_type",
        F.when(F.col("calculated_speed_kmh") > 120, "EXCESSIVE_SPEED")
         .when(F.col("acceleration_classification") == "HARD_ACCELERATION", "HARD_ACCELERATION")
         .when(F.col("acceleration_classification") == "HARD_BRAKING", "HARD_BRAKING")
         .otherwise("UNKNOWN")
    )
    .withColumn("anomaly_severity",
        F.when(F.col("calculated_speed_kmh") > 150, "CRITICAL")
         .when(F.abs(F.col("acceleration_mps2")) > 5, "CRITICAL")
         .when(F.col("calculated_speed_kmh") > 120, "HIGH")
         .when(F.abs(F.col("acceleration_mps2")) > 3, "HIGH")
         .otherwise("MEDIUM")
    )
    .withColumn("anomaly_id", F.expr("uuid()"))
    .withColumn("detected_at", F.current_timestamp())
    .select(
        "anomaly_id",
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "anomaly_type",
        "anomaly_severity",
        "calculated_speed_kmh",
        "acceleration_mps2",
        "movement_state",
        "detected_at"
    )
)

# COMMAND ----------

# DBTITLE 1,Detect Direction Change Anomalies
# Detect sharp turns or erratic direction changes
direction_anomalies = (
    gps_with_bearing
    .withColumn("prev_bearing", F.lag("calculated_bearing_normalized").over(device_window))
    .withColumn(
        "bearing_change",
        F.when(
            F.col("prev_bearing").isNotNull(),
            F.least(
                F.abs(F.col("calculated_bearing_normalized") - F.col("prev_bearing")),
                360 - F.abs(F.col("calculated_bearing_normalized") - F.col("prev_bearing"))
            )
        )
    )
    .filter(
        (F.col("bearing_change") > 90) &  # Sharp turn
        (F.col("calculated_speed_kmh") > 20) &  # While moving
        (F.col("time_delta_seconds") < 10)  # In short time
    )
    .withColumn("anomaly_type", F.lit("SHARP_TURN"))
    .withColumn("anomaly_severity",
        F.when(F.col("bearing_change") > 150, "HIGH")
         .otherwise("MEDIUM")
    )
    .withColumn("anomaly_id", F.expr("uuid()"))
    .withColumn("detected_at", F.current_timestamp())
    .select(
        "anomaly_id",
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "anomaly_type",
        "anomaly_severity",
        F.col("bearing_change").alias("bearing_change_degrees"),
        "calculated_speed_kmh",
        "detected_at"
    )
)

# COMMAND ----------

# DBTITLE 1,Union All Anomalies and Write
# Combine different anomaly types
all_anomalies = speed_anomalies.select(
    "anomaly_id",
    "device_id",
    "timestamp",
    "latitude",
    "longitude",
    "anomaly_type",
    "anomaly_severity",
    "detected_at"
).union(
    direction_anomalies.select(
        "anomaly_id",
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "anomaly_type",
        "anomaly_severity",
        "detected_at"
    )
)

anomalies_table = f"{catalog}.{schema_name}.trajectory_anomalies"
anomalies_checkpoint = f"{checkpoint_base}/trajectory_anomalies"

query_anomalies = (
    all_anomalies
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", anomalies_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(anomalies_table)
)

print(f"Started anomaly detection query: {query_anomalies.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Query and Analyze Results

# COMMAND ----------

# DBTITLE 1,Wait for Processing
import time
print("Processing trajectories... waiting 25 seconds")
time.sleep(25)

# COMMAND ----------

# DBTITLE 1,Query Enriched Trajectory Data
enriched = spark.table(enriched_table)
print(f"Total enriched GPS records: {enriched.count()}")

print("\nMovement state distribution:")
enriched.groupBy("movement_state").count().orderBy("movement_state").show()

print("\nSpeed statistics by device:")
enriched.filter("calculated_speed_kmh IS NOT NULL") \
    .groupBy("device_id") \
    .agg(
        F.count("*").alias("points"),
        F.round(F.avg("calculated_speed_kmh"), 2).alias("avg_speed_kmh"),
        F.round(F.max("calculated_speed_kmh"), 2).alias("max_speed_kmh"),
        F.round(F.stddev("calculated_speed_kmh"), 2).alias("stddev_speed")
    ) \
    .orderBy("device_id") \
    .show()

# COMMAND ----------

# DBTITLE 1,Query Detected Stops
stops = spark.table(stops_table)
print(f"Total stops detected: {stops.count()}")

print("\nLongest stops:")
stops.orderBy(F.desc("dwell_time_minutes")) \
    .select(
        "device_id",
        "stop_start",
        "stop_end",
        F.round("dwell_time_minutes", 1).alias("dwell_minutes"),
        "stop_latitude",
        "stop_longitude",
        "point_count"
    ) \
    .limit(10) \
    .show(truncate=False)

print("\nStops by device:")
stops.groupBy("device_id") \
    .agg(
        F.count("*").alias("stop_count"),
        F.round(F.avg("dwell_time_minutes"), 1).alias("avg_dwell_minutes"),
        F.round(F.sum("dwell_time_minutes"), 1).alias("total_dwell_minutes")
    ) \
    .orderBy("device_id") \
    .show()

# COMMAND ----------

# DBTITLE 1,Query Trajectory Segments
segments = spark.table(segments_table)
print(f"Total trajectory segments: {segments.count()}")

print("\nSegment statistics:")
segments.groupBy("segment_type", "data_quality").count() \
    .orderBy("segment_type", "data_quality") \
    .show()

print("\nLongest segments:")
segments.orderBy(F.desc("distance_km")) \
    .select(
        "device_id",
        "segment_start",
        "segment_end",
        "duration_minutes",
        "distance_km",
        "avg_speed_kmh",
        "segment_type",
        "point_count"
    ) \
    .limit(10) \
    .show(truncate=False)

print("\nTrajectory statistics by device:")
segments.groupBy("device_id") \
    .agg(
        F.count("*").alias("segment_count"),
        F.round(F.sum("distance_km"), 2).alias("total_distance_km"),
        F.round(F.sum("duration_minutes"), 1).alias("total_duration_min"),
        F.round(F.avg("avg_speed_kmh"), 2).alias("overall_avg_speed_kmh")
    ) \
    .orderBy("device_id") \
    .show()

# COMMAND ----------

# DBTITLE 1,Query Detected Anomalies
anomalies = spark.table(anomalies_table)
print(f"Total anomalies detected: {anomalies.count()}")

print("\nAnomalies by type and severity:")
anomalies.groupBy("anomaly_type", "anomaly_severity").count() \
    .orderBy("anomaly_severity", "anomaly_type") \
    .show()

print("\nRecent critical anomalies:")
anomalies.filter("anomaly_severity = 'CRITICAL'") \
    .orderBy(F.desc("timestamp")) \
    .select(
        "device_id",
        "timestamp",
        "anomaly_type",
        "anomaly_severity",
        "latitude",
        "longitude"
    ) \
    .limit(10) \
    .show(truncate=False)

print("\nAnomalies by device:")
anomalies.groupBy("device_id") \
    .agg(
        F.count("*").alias("total_anomalies"),
        F.count(F.when(F.col("anomaly_severity") == "CRITICAL", 1)).alias("critical"),
        F.count(F.when(F.col("anomaly_severity") == "HIGH", 1)).alias("high"),
        F.count(F.when(F.col("anomaly_severity") == "MEDIUM", 1)).alias("medium")
    ) \
    .orderBy(F.desc("total_anomalies")) \
    .show()

# COMMAND ----------

# DBTITLE 1,Trajectory Quality Analysis
print("=== Trajectory Data Quality Analysis ===\n")

# Compare reported vs calculated speed
speed_comparison = enriched.filter(
    "speed IS NOT NULL AND calculated_speed_kmh IS NOT NULL"
).select(
    F.avg("speed").alias("avg_reported_speed_mps"),
    F.avg("calculated_speed_mps").alias("avg_calculated_speed_mps"),
    F.corr("speed", "calculated_speed_mps").alias("speed_correlation")
)

print("Speed comparison (reported vs calculated):")
speed_comparison.show()

# Data completeness
completeness = enriched.select(
    F.count("*").alias("total_records"),
    F.count(F.when(F.col("prev_latitude").isNotNull(), 1)).alias("with_previous_point"),
    F.count(F.when(F.col("calculated_speed_kmh").isNotNull(), 1)).alias("with_calculated_speed"),
    F.count(F.when(F.col("calculated_bearing_normalized").isNotNull(), 1)).alias("with_bearing")
)

print("\nData completeness:")
completeness.show()

# COMMAND ----------

# DBTITLE 1,Smoothing Effectiveness Analysis
smoothed_data = spark.table(smoothed_table)

print("=== Smoothing Effectiveness ===\n")

smoothing_stats = smoothed_data.filter(
    "calculated_speed_kmh IS NOT NULL AND smoothed_speed IS NOT NULL"
).agg(
    F.avg("calculated_speed_kmh").alias("avg_raw_speed"),
    F.avg("smoothed_speed").alias("avg_smoothed_speed"),
    F.stddev("calculated_speed_kmh").alias("stddev_raw_speed"),
    F.stddev("smoothed_speed").alias("stddev_smoothed_speed")
)

print("Raw vs smoothed speed statistics:")
smoothing_stats.show()

# Position smoothing
position_smooth = smoothed_data.filter(
    "latitude IS NOT NULL AND smoothed_latitude IS NOT NULL"
).select(
    F.expr("""
        ST_Distance(
            ST_Point(longitude, latitude),
            ST_Point(smoothed_longitude, smoothed_latitude)
        )
    """).alias("smoothing_distance_meters")
).agg(
    F.avg("smoothing_distance_meters").alias("avg_position_adjustment_m"),
    F.max("smoothing_distance_meters").alias("max_position_adjustment_m")
)

print("\nPosition smoothing adjustments:")
position_smooth.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Cleanup

# COMMAND ----------

# DBTITLE 1,List Active Streams
print("=== Active Streaming Queries ===\n")
for stream in spark.streams.active:
    print(f"- {stream.name} (ID: {stream.id})")
    print(f"  Status: {stream.status['message']}")
    print()

# COMMAND ----------

# DBTITLE 1,Stop All Streams (Optional)
# Uncomment to stop all streams
# for stream in spark.streams.active:
#     print(f"Stopping {stream.name}...")
#     stream.stop()
# print("All streams stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Implemented
# MAGIC
# MAGIC 1. **Speed and Direction Calculation**
# MAGIC    - Haversine distance between consecutive points
# MAGIC    - Calculated speed from distance and time delta
# MAGIC    - Bearing calculation for direction of travel
# MAGIC    - Elevation change and slope analysis
# MAGIC
# MAGIC 2. **Path Smoothing**
# MAGIC    - Moving average smoothing for noisy GPS data
# MAGIC    - Reduced variance while preserving trajectory shape
# MAGIC    - Smoothing for position, speed, and altitude
# MAGIC
# MAGIC 3. **Stop Detection**
# MAGIC    - Identified stationary periods
# MAGIC    - Calculated dwell times
# MAGIC    - Aggregated consecutive stops
# MAGIC    - Filtered meaningful stops (>5 minutes)
# MAGIC
# MAGIC 4. **Trajectory Segmentation**
# MAGIC    - Windowed aggregation of GPS points
# MAGIC    - Distance and duration calculations
# MAGIC    - Speed statistics per segment
# MAGIC    - Segment classification (urban, highway, etc.)
# MAGIC
# MAGIC 5. **Anomaly Detection**
# MAGIC    - Excessive speed detection
# MAGIC    - Hard acceleration/braking events
# MAGIC    - Sharp turn detection
# MAGIC    - Severity classification
# MAGIC
# MAGIC 6. **Data Quality Analysis**
# MAGIC    - Compared reported vs calculated metrics
# MAGIC    - Assessed data completeness
# MAGIC    - Evaluated smoothing effectiveness
# MAGIC
# MAGIC ### Key Techniques
# MAGIC
# MAGIC ✅ Window functions for sequential analysis  
# MAGIC ✅ Lag function for previous point access  
# MAGIC ✅ Moving averages for noise reduction  
# MAGIC ✅ Watermarking for late data handling  
# MAGIC ✅ Windowed aggregations for segmentation  
# MAGIC ✅ Statistical methods for anomaly detection  
# MAGIC
# MAGIC ### Performance Considerations
# MAGIC
# MAGIC - Used appropriate window sizes for smoothing
# MAGIC - Partitioned by device for parallelism
# MAGIC - Set reasonable watermarks for state management
# MAGIC - Filtered invalid data early in pipeline
# MAGIC - Separate queries for different analytics
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Continue to **Demo 4: Stream-to-Stream Spatial Joins** to learn:
# MAGIC - Joining multiple moving object streams
# MAGIC - Watermarking strategies for spatial joins
# MAGIC - Performance optimization techniques
# MAGIC - Real-world matching scenarios (drivers to riders)
