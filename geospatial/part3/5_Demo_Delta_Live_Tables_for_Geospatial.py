# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Delta Live Tables for Geospatial
# MAGIC
# MAGIC ## Overview
# MAGIC This demo showcases Delta Live Tables (DLT) for building robust geospatial data pipelines. You'll implement the medallion architecture (Bronze, Silver, Gold) for location data, define data quality expectations specific to geospatial data, handle CDC with spatial updates, and create incremental processing patterns for efficient streaming pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Build medallion architecture for geospatial streaming data
# MAGIC - Implement data quality expectations for spatial data
# MAGIC - Handle Change Data Capture (CDC) with location updates
# MAGIC - Create incremental processing patterns
# MAGIC - Apply APPLY CHANGES for SCD Type 1 and 2
# MAGIC - Monitor DLT pipeline health
# MAGIC - Optimize geospatial DLT pipelines
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1-4
# MAGIC - Understanding of Delta Live Tables concepts
# MAGIC - Knowledge of medallion architecture
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes
# MAGIC
# MAGIC ## Note
# MAGIC This notebook defines a Delta Live Tables pipeline. To execute:
# MAGIC 1. Create a new DLT pipeline in the Databricks UI
# MAGIC 2. Add this notebook as the pipeline source
# MAGIC 3. Configure target catalog and schema
# MAGIC 4. Start the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration
# MAGIC
# MAGIC This DLT pipeline will process GPS data through three layers:
# MAGIC - **Bronze**: Raw ingestion with minimal transformation
# MAGIC - **Silver**: Validated, enriched, and deduplicated data
# MAGIC - **Gold**: Business-level aggregations and insights

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Ingestion
# MAGIC
# MAGIC Ingest raw GPS data from cloud storage with schema enforcement.

# COMMAND ----------

@dlt.table(
    name="gps_bronze",
    comment="Raw GPS location data ingested from cloud storage",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gps_bronze():
    """
    Bronze layer: Raw GPS data ingestion
    - No quality checks (accept all data)
    - Add metadata columns
    - Preserve original structure
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/tmp/geospatial_streaming/dlt_schema/gps")
            .schema("""
                device_id STRING,
                timestamp TIMESTAMP,
                latitude DOUBLE,
                longitude DOUBLE,
                altitude DOUBLE,
                speed DOUBLE,
                heading DOUBLE,
                accuracy DOUBLE,
                device_type STRING,
                battery_level INT,
                metadata MAP<STRING, STRING>
            """)
            .load("/tmp/geospatial_streaming/data/landing/gps")
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("ingestion_date", F.current_date())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Validated and Enriched
# MAGIC
# MAGIC Apply data quality rules and enrich with spatial features.

# COMMAND ----------

@dlt.table(
    name="gps_silver",
    comment="Validated and enriched GPS data with spatial indexing",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_device_id", "device_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_latitude", "latitude BETWEEN -90 AND 90")
@dlt.expect_or_drop("valid_longitude", "longitude BETWEEN -180 AND 180")
@dlt.expect("reasonable_speed", "speed IS NULL OR speed BETWEEN 0 AND 100", on_violation="WARN")
@dlt.expect("reasonable_accuracy", "accuracy IS NULL OR accuracy < 100", on_violation="WARN")
def gps_silver():
    """
    Silver layer: Clean and enriched GPS data
    - Validate coordinates
    - Add spatial indexes (H3, geohash)
    - Calculate derived fields
    - Deduplicate records
    """
    return (
        dlt.read_stream("gps_bronze")
            # Add H3 spatial index (resolution 9 for ~105m² cells)
            .withColumn("h3_index", F.expr("h3_latlngtocell(latitude, longitude, 9)"))
            
            # Add geohash for alternative indexing
            .withColumn("geohash", F.expr("ST_GeoHash(ST_Point(longitude, latitude), 7)"))
            
            # Create point geometry
            .withColumn("point_wkt", F.expr("ST_AsText(ST_Point(longitude, latitude))"))
            
            # Add processing metadata
            .withColumn("processing_timestamp", F.current_timestamp())
            
            # Calculate processing lag
            .withColumn("processing_lag_seconds",
                       F.unix_timestamp("processing_timestamp") - F.unix_timestamp("timestamp"))
            
            # Classify data quality
            .withColumn("quality_score",
                F.when(F.col("accuracy") < 10, 100)
                 .when(F.col("accuracy") < 25, 80)
                 .when(F.col("accuracy") < 50, 60)
                 .otherwise(40))
            
            # Deduplicate on device_id and timestamp
            .withWatermark("timestamp", "10 minutes")
            .dropDuplicates(["device_id", "timestamp"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Trajectory Enrichment
# MAGIC
# MAGIC Calculate movement metrics from sequential points.

# COMMAND ----------

@dlt.table(
    name="gps_trajectory_silver",
    comment="GPS data enriched with trajectory analytics (speed, bearing, distance)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("has_previous_point", "prev_timestamp IS NOT NULL")
def gps_trajectory_silver():
    """
    Silver layer: Trajectory enrichment
    - Calculate speed from distance/time
    - Calculate bearing (direction)
    - Calculate distance between points
    - Classify movement state
    """
    from pyspark.sql.window import Window
    
    # Define window for lag operations
    window_spec = Window.partitionBy("device_id").orderBy("timestamp")
    
    return (
        dlt.read_stream("gps_silver")
            # Add previous point for calculations
            .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
            .withColumn("prev_latitude", F.lag("latitude").over(window_spec))
            .withColumn("prev_longitude", F.lag("longitude").over(window_spec))
            
            # Calculate distance
            .withColumn("distance_meters",
                F.when(F.col("prev_latitude").isNotNull(),
                    F.expr("""
                        ST_Distance(
                            ST_Point(prev_longitude, prev_latitude),
                            ST_Point(longitude, latitude)
                        )
                    """)
                ).otherwise(0.0))
            
            # Calculate time delta
            .withColumn("time_delta_seconds",
                F.when(F.col("prev_timestamp").isNotNull(),
                    F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")
                ).otherwise(0.0))
            
            # Calculate derived speed
            .withColumn("calculated_speed_mps",
                F.when((F.col("time_delta_seconds") > 0) & (F.col("time_delta_seconds") < 300),
                    F.col("distance_meters") / F.col("time_delta_seconds")
                ).otherwise(None))
            
            .withColumn("calculated_speed_kmh", F.col("calculated_speed_mps") * 3.6)
            
            # Calculate bearing
            .withColumn("bearing_degrees",
                F.when(F.col("prev_latitude").isNotNull(),
                    F.expr("""
                        ST_Azimuth(
                            ST_Point(prev_longitude, prev_latitude),
                            ST_Point(longitude, latitude)
                        ) * 180.0 / PI()
                    """)
                ).otherwise(None))
            
            # Classify movement
            .withColumn("movement_state",
                F.when(F.col("calculated_speed_kmh").isNull(), "UNKNOWN")
                 .when(F.col("calculated_speed_kmh") < 1, "STATIONARY")
                 .when(F.col("calculated_speed_kmh") < 10, "SLOW")
                 .when(F.col("calculated_speed_kmh") < 50, "NORMAL")
                 .when(F.col("calculated_speed_kmh") < 100, "FAST")
                 .otherwise("VERY_FAST"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Business Aggregations
# MAGIC
# MAGIC Create business-level metrics and aggregations.

# COMMAND ----------

@dlt.table(
    name="device_hourly_summary_gold",
    comment="Hourly summary statistics per device",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def device_hourly_summary_gold():
    """
    Gold layer: Hourly device activity summary
    - Aggregate by device and hour
    - Calculate distance traveled
    - Compute average speed
    - Count data points
    """
    return (
        dlt.read_stream("gps_trajectory_silver")
            .withWatermark("timestamp", "1 hour")
            .groupBy(
                F.window("timestamp", "1 hour"),
                "device_id",
                "device_type"
            )
            .agg(
                F.count("*").alias("point_count"),
                F.sum("distance_meters").alias("total_distance_meters"),
                F.avg("calculated_speed_kmh").alias("avg_speed_kmh"),
                F.max("calculated_speed_kmh").alias("max_speed_kmh"),
                F.avg("quality_score").alias("avg_quality_score"),
                F.countDistinct("h3_index").alias("unique_locations"),
                F.min("timestamp").alias("first_seen"),
                F.max("timestamp").alias("last_seen")
            )
            .select(
                F.col("window.start").alias("hour_start"),
                F.col("window.end").alias("hour_end"),
                "device_id",
                "device_type",
                "point_count",
                F.round(F.col("total_distance_meters") / 1000, 2).alias("distance_km"),
                F.round("avg_speed_kmh", 2).alias("avg_speed_kmh"),
                F.round("max_speed_kmh", 2).alias("max_speed_kmh"),
                F.round("avg_quality_score", 1).alias("avg_quality_score"),
                "unique_locations",
                "first_seen",
                "last_seen"
            )
    )

# COMMAND ----------

@dlt.table(
    name="spatial_density_gold",
    comment="Spatial density heatmap by H3 cell",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def spatial_density_gold():
    """
    Gold layer: Spatial density analysis
    - Aggregate by H3 cell and time window
    - Count devices and points
    - Calculate average speed per cell
    """
    return (
        dlt.read_stream("gps_silver")
            .withWatermark("timestamp", "30 minutes")
            .groupBy(
                F.window("timestamp", "15 minutes"),
                "h3_index"
            )
            .agg(
                F.countDistinct("device_id").alias("unique_devices"),
                F.count("*").alias("total_points"),
                F.avg("speed").alias("avg_speed"),
                F.avg("quality_score").alias("avg_quality")
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "h3_index",
                # Add H3 cell center coordinates
                F.expr("h3_celltolatlng(h3_index)[0]").alias("cell_latitude"),
                F.expr("h3_celltolatlng(h3_index)[1]").alias("cell_longitude"),
                "unique_devices",
                "total_points",
                F.round("avg_speed", 2).alias("avg_speed"),
                F.round("avg_quality", 1).alias("avg_quality")
            )
    )

# COMMAND ----------

@dlt.table(
    name="movement_patterns_gold",
    comment="Movement pattern classification and statistics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def movement_patterns_gold():
    """
    Gold layer: Movement pattern analysis
    - Aggregate by movement state and time window
    - Calculate statistics per pattern type
    """
    return (
        dlt.read_stream("gps_trajectory_silver")
            .filter("movement_state != 'UNKNOWN'")
            .withWatermark("timestamp", "30 minutes")
            .groupBy(
                F.window("timestamp", "10 minutes"),
                "device_id",
                "movement_state"
            )
            .agg(
                F.count("*").alias("occurrence_count"),
                F.sum("distance_meters").alias("distance_in_state"),
                F.avg("calculated_speed_kmh").alias("avg_speed"),
                F.min("timestamp").alias("state_start"),
                F.max("timestamp").alias("state_end")
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "device_id",
                "movement_state",
                "occurrence_count",
                F.round(F.col("distance_in_state") / 1000, 3).alias("distance_km"),
                F.round("avg_speed", 2).alias("avg_speed_kmh"),
                "state_start",
                "state_end",
                F.round(
                    (F.unix_timestamp("state_end") - F.unix_timestamp("state_start")) / 60,
                    1
                ).alias("duration_minutes")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Monitoring Tables

# COMMAND ----------

@dlt.table(
    name="data_quality_metrics_gold",
    comment="Real-time data quality metrics and SLA tracking",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def data_quality_metrics_gold():
    """
    Gold layer: Data quality monitoring
    - Track quality metrics over time
    - Identify data quality issues
    - Monitor SLA compliance
    """
    return (
        dlt.read_stream("gps_silver")
            .withWatermark("timestamp", "1 hour")
            .groupBy(
                F.window("timestamp", "5 minutes"),
                "device_id"
            )
            .agg(
                F.count("*").alias("total_points"),
                F.avg("quality_score").alias("avg_quality_score"),
                F.avg("processing_lag_seconds").alias("avg_lag_seconds"),
                F.max("processing_lag_seconds").alias("max_lag_seconds"),
                F.avg("accuracy").alias("avg_accuracy_meters"),
                F.count(F.when(F.col("quality_score") >= 80, 1)).alias("high_quality_points"),
                F.count(F.when(F.col("processing_lag_seconds") < 60, 1)).alias("timely_points")
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "device_id",
                "total_points",
                F.round("avg_quality_score", 1).alias("avg_quality_score"),
                F.round("avg_lag_seconds", 1).alias("avg_lag_seconds"),
                F.round("max_lag_seconds", 1).alias("max_lag_seconds"),
                F.round("avg_accuracy_meters", 1).alias("avg_accuracy_meters"),
                # Calculate SLA percentages
                F.round((F.col("high_quality_points") / F.col("total_points")) * 100, 2)
                    .alias("high_quality_pct"),
                F.round((F.col("timely_points") / F.col("total_points")) * 100, 2)
                    .alias("timely_pct")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Pattern: Device State Table
# MAGIC
# MAGIC Maintain current state of each device using APPLY CHANGES.

# COMMAND ----------

# Note: APPLY CHANGES requires a separate streaming table as source
# This is a conceptual example - in production, use with proper CDC source

@dlt.table(
    name="device_current_state",
    comment="Current state of each device (SCD Type 1)",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def device_current_state():
    """
    Gold layer: Current device state (SCD Type 1)
    - Latest known location per device
    - Most recent status
    - Updated on each new location
    """
    from pyspark.sql.window import Window
    
    # Get latest record per device
    window_latest = Window.partitionBy("device_id").orderBy(F.desc("timestamp"))
    
    return (
        dlt.read_stream("gps_silver")
            .withColumn("row_num", F.row_number().over(window_latest))
            .filter("row_num = 1")
            .select(
                "device_id",
                "timestamp",
                "latitude",
                "longitude",
                "altitude",
                "speed",
                "heading",
                "h3_index",
                "geohash",
                "point_wkt",
                "quality_score",
                "device_type",
                "battery_level",
                F.current_timestamp().alias("last_updated")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Detection Table

# COMMAND ----------

@dlt.table(
    name="trajectory_anomalies_gold",
    comment="Detected trajectory anomalies and unusual patterns",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def trajectory_anomalies_gold():
    """
    Gold layer: Anomaly detection
    - Excessive speed
    - Unusual acceleration
    - Erratic movement
    """
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("device_id").orderBy("timestamp")
    
    base_data = (
        dlt.read_stream("gps_trajectory_silver")
            # Calculate acceleration
            .withColumn("prev_speed", F.lag("calculated_speed_mps").over(window_spec))
            .withColumn("acceleration_mps2",
                F.when(
                    (F.col("time_delta_seconds") > 0) & (F.col("time_delta_seconds") < 60),
                    (F.col("calculated_speed_mps") - F.col("prev_speed")) / F.col("time_delta_seconds")
                ).otherwise(None))
    )
    
    return (
        base_data
            .filter(
                (F.col("calculated_speed_kmh") > 120) |  # Excessive speed
                (F.abs(F.col("acceleration_mps2")) > 3)  # Hard acceleration/braking
            )
            .withColumn("anomaly_type",
                F.when(F.col("calculated_speed_kmh") > 120, "EXCESSIVE_SPEED")
                 .when(F.col("acceleration_mps2") > 3, "HARD_ACCELERATION")
                 .when(F.col("acceleration_mps2") < -3, "HARD_BRAKING")
                 .otherwise("UNKNOWN"))
            .withColumn("anomaly_severity",
                F.when(F.col("calculated_speed_kmh") > 150, "CRITICAL")
                 .when(F.abs(F.col("acceleration_mps2")) > 5, "CRITICAL")
                 .when(F.col("calculated_speed_kmh") > 120, "HIGH")
                 .when(F.abs(F.col("acceleration_mps2")) > 3, "HIGH")
                 .otherwise("MEDIUM"))
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
                F.round("calculated_speed_kmh", 2).alias("speed_kmh"),
                F.round("acceleration_mps2", 2).alias("acceleration_mps2"),
                "movement_state",
                "detected_at"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### DLT Pipeline Architecture
# MAGIC
# MAGIC This Delta Live Tables pipeline implements:
# MAGIC
# MAGIC **Bronze Layer (1 table)**
# MAGIC - Raw GPS data ingestion
# MAGIC - Schema enforcement
# MAGIC - Metadata addition
# MAGIC
# MAGIC **Silver Layer (2 tables)**
# MAGIC - `gps_silver`: Validated and spatially indexed GPS data
# MAGIC - `gps_trajectory_silver`: Enriched with trajectory metrics
# MAGIC
# MAGIC **Gold Layer (6 tables)**
# MAGIC - `device_hourly_summary_gold`: Per-device hourly statistics
# MAGIC - `spatial_density_gold`: Geospatial heatmap data
# MAGIC - `movement_patterns_gold`: Movement classification analytics
# MAGIC - `data_quality_metrics_gold`: Quality monitoring and SLA tracking
# MAGIC - `device_current_state`: Latest device state (SCD Type 1)
# MAGIC - `trajectory_anomalies_gold`: Detected anomalies
# MAGIC
# MAGIC ### Data Quality Expectations
# MAGIC
# MAGIC ✅ Coordinate validation (latitude/longitude bounds)  
# MAGIC ✅ Required field checks (device_id, timestamp)  
# MAGIC ✅ Reasonable value ranges (speed, accuracy)  
# MAGIC ✅ Deduplication on device + timestamp  
# MAGIC ✅ Quality score calculation  
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC - **Automatic schema evolution** with cloudFiles
# MAGIC - **Watermarking** for stateful operations
# MAGIC - **H3 and geohash indexing** for spatial queries
# MAGIC - **Real-time aggregations** with windowing
# MAGIC - **Anomaly detection** in streaming pipeline
# MAGIC - **Data quality monitoring** with SLA tracking
# MAGIC
# MAGIC ### To Deploy This Pipeline
# MAGIC
# MAGIC 1. Go to Databricks UI → Workflows → Delta Live Tables
# MAGIC 2. Click "Create Pipeline"
# MAGIC 3. Configure:
# MAGIC    - **Name**: Geospatial Streaming Pipeline
# MAGIC    - **Notebook**: This notebook
# MAGIC    - **Target**: main.geospatial_streaming
# MAGIC    - **Cluster Mode**: Enhanced Autoscaling
# MAGIC 4. Click "Create" and then "Start"
# MAGIC
# MAGIC ### Monitoring
# MAGIC
# MAGIC Once deployed, monitor via:
# MAGIC - DLT pipeline UI for data lineage
# MAGIC - Event logs for quality metrics
# MAGIC - SQL queries on gold tables for business metrics
