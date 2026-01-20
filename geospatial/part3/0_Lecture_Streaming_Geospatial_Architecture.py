# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Streaming Geospatial Architecture
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces real-time geospatial analytics using Apache Spark Structured Streaming and Delta Live Tables. We'll explore architectural patterns for processing streaming location data, managing state for moving objects, handling late-arriving events, and optimizing for low-latency geospatial operations. You'll learn how to build scalable, fault-tolerant systems for fleet tracking, IoT sensor networks, and smart city applications.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand streaming geospatial architecture patterns
# MAGIC - Design state management strategies for moving objects
# MAGIC - Implement watermarking for late data handling
# MAGIC - Optimize streaming queries for low latency
# MAGIC - Apply fault tolerance and exactly-once processing guarantees
# MAGIC - Choose appropriate checkpoint strategies
# MAGIC - Design medallion architecture for geospatial streams
# MAGIC - Implement real-time spatial indexing strategies
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Streaming Geospatial Analytics?
# MAGIC
# MAGIC ### Traditional Batch vs Real-Time Geospatial
# MAGIC
# MAGIC **Batch Processing Limitations:**
# MAGIC - High latency (minutes to hours)
# MAGIC - Cannot support real-time alerts
# MAGIC - Inefficient for time-sensitive decisions
# MAGIC - Complex state management across batches
# MAGIC
# MAGIC **Streaming Benefits:**
# MAGIC - Sub-second to second latency
# MAGIC - Immediate anomaly detection
# MAGIC - Real-time geofencing and alerts
# MAGIC - Continuous trajectory analysis
# MAGIC - Live dashboards and visualizations
# MAGIC
# MAGIC ### Real-World Use Cases
# MAGIC
# MAGIC **Fleet Management**
# MAGIC - Real-time vehicle tracking
# MAGIC - Route deviation alerts
# MAGIC - ETA predictions
# MAGIC - Fuel optimization
# MAGIC
# MAGIC **Smart Cities**
# MAGIC - Traffic flow analysis
# MAGIC - Public transit optimization
# MAGIC - Emergency response coordination
# MAGIC - Environmental monitoring
# MAGIC
# MAGIC **Delivery Services**
# MAGIC - Driver-customer matching
# MAGIC - Dynamic routing
# MAGIC - Demand prediction
# MAGIC - Service area optimization
# MAGIC
# MAGIC **IoT & Sensors**
# MAGIC - Asset tracking
# MAGIC - Environmental monitoring
# MAGIC - Security and surveillance
# MAGIC - Predictive maintenance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Geospatial Architecture Overview
# MAGIC
# MAGIC ### High-Level Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Data Sources                              │
# MAGIC │  GPS Devices │ IoT Sensors │ Mobile Apps │ Vehicle Systems │
# MAGIC └────────────┬─────────────┬────────────┬────────────────────┘
# MAGIC              │             │            │
# MAGIC              ▼             ▼            ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Message Broker                            │
# MAGIC │          Kafka / Kinesis / Event Hubs / Auto Loader         │
# MAGIC └────────────────────────────┬────────────────────────────────┘
# MAGIC                               │
# MAGIC                               ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │              Spark Structured Streaming                      │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
# MAGIC │  │ Ingest   │→ │ Transform│→ │ Enrich   │→ │ Aggregate│   │
# MAGIC │  │ & Parse  │  │ & Filter │  │ & Join   │  │ & Window │   │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
# MAGIC │       │             │              │              │         │
# MAGIC │       └─────────────┴──────────────┴──────────────┘         │
# MAGIC │                          │                                   │
# MAGIC │                 Checkpoint Storage                           │
# MAGIC └────────────────────────────┬────────────────────────────────┘
# MAGIC                               │
# MAGIC                               ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Delta Lake Storage                        │
# MAGIC │    Bronze (Raw) │ Silver (Validated) │ Gold (Aggregated)   │
# MAGIC └────────────────────────────┬────────────────────────────────┘
# MAGIC                               │
# MAGIC              ┌────────────────┼────────────────┐
# MAGIC              ▼                ▼                ▼
# MAGIC       ┌──────────┐    ┌──────────┐    ┌──────────┐
# MAGIC       │Dashboards│    │  Alerts  │    │   APIs   │
# MAGIC       └──────────┘    └──────────┘    └──────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Concepts: Structured Streaming for Geospatial
# MAGIC
# MAGIC ### Stream Processing Model
# MAGIC
# MAGIC **Unbounded Tables**
# MAGIC - Treat streams as append-only tables
# MAGIC - New data continuously arrives
# MAGIC - Query against current state of table
# MAGIC
# MAGIC **Micro-batch Processing**
# MAGIC - Process small batches at regular intervals
# MAGIC - Default trigger: as fast as possible
# MAGIC - Balance latency vs throughput
# MAGIC
# MAGIC **Continuous Processing**
# MAGIC - For ultra-low latency (experimental)
# MAGIC - ~1ms latency achievable
# MAGIC - Limited operator support
# MAGIC
# MAGIC ### Geospatial Data in Streams
# MAGIC
# MAGIC **Schema Considerations**
# MAGIC ```python
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC gps_schema = StructType([
# MAGIC     StructField("device_id", StringType(), False),
# MAGIC     StructField("timestamp", TimestampType(), False),
# MAGIC     StructField("latitude", DoubleType(), False),
# MAGIC     StructField("longitude", DoubleType(), False),
# MAGIC     StructField("altitude", DoubleType(), True),
# MAGIC     StructField("speed", DoubleType(), True),
# MAGIC     StructField("heading", DoubleType(), True),
# MAGIC     StructField("accuracy", DoubleType(), True)
# MAGIC ])
# MAGIC ```
# MAGIC
# MAGIC **Geometry Representation**
# MAGIC - WKT (Well-Known Text): String representation
# MAGIC - WKB (Well-Known Binary): Binary format
# MAGIC - GeoJSON: JSON representation
# MAGIC - Separate lat/lon columns: Most efficient for streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermarking and Late Data Handling
# MAGIC
# MAGIC ### The Late Data Problem
# MAGIC
# MAGIC **Causes of Late-Arriving Data:**
# MAGIC - Network delays and retries
# MAGIC - Device offline periods
# MAGIC - Clock skew between devices
# MAGIC - Batch uploads from cached data
# MAGIC - Out-of-order message delivery
# MAGIC
# MAGIC **Impact:**
# MAGIC - Incorrect aggregations
# MAGIC - Missed geofence events
# MAGIC - Trajectory gaps
# MAGIC - Growing state size
# MAGIC
# MAGIC ### Watermarking Strategy
# MAGIC
# MAGIC **What is a Watermark?**
# MAGIC - Threshold for event time progress
# MAGIC - Determines when to drop late data
# MAGIC - Enables state cleanup
# MAGIC
# MAGIC **Setting Watermarks:**
# MAGIC ```python
# MAGIC # Allow 10 minutes of late data
# MAGIC df_with_watermark = gps_stream.withWatermark("timestamp", "10 minutes")
# MAGIC
# MAGIC # Watermark = max(event_time) - threshold
# MAGIC # Data older than watermark is dropped
# MAGIC ```
# MAGIC
# MAGIC **Choosing Watermark Thresholds:**
# MAGIC - **Short (1-5 min)**: Low latency, may drop valid data
# MAGIC - **Medium (10-30 min)**: Balance accuracy and latency
# MAGIC - **Long (1+ hour)**: Maximum accuracy, higher state memory
# MAGIC
# MAGIC ### Event Time vs Processing Time
# MAGIC
# MAGIC **Event Time:**
# MAGIC - When event occurred (GPS timestamp)
# MAGIC - Used for business logic
# MAGIC - Subject to skew and delays
# MAGIC
# MAGIC **Processing Time:**
# MAGIC - When Spark processes the event
# MAGIC - Used for monitoring
# MAGIC - Always increasing
# MAGIC
# MAGIC **Best Practice:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import current_timestamp
# MAGIC
# MAGIC df = gps_stream.withColumn("processing_time", current_timestamp()) \
# MAGIC                .withColumn("lag_seconds", 
# MAGIC                            (col("processing_time").cast("long") - 
# MAGIC                             col("timestamp").cast("long")))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## State Management for Moving Objects
# MAGIC
# MAGIC ### Stateless vs Stateful Operations
# MAGIC
# MAGIC **Stateless Operations (No Memory)**
# MAGIC - Filtering by location bounds
# MAGIC - Schema transformations
# MAGIC - Point-in-polygon tests (with broadcast)
# MAGIC - Distance calculations
# MAGIC
# MAGIC **Stateful Operations (Require Memory)**
# MAGIC - Aggregations (windowed counts)
# MAGIC - Trajectory reconstruction
# MAGIC - Geofence entry/exit detection
# MAGIC - Stream-to-stream joins
# MAGIC - Deduplication
# MAGIC
# MAGIC ### State Store Architecture
# MAGIC
# MAGIC **State Storage:**
# MAGIC - In-memory with disk spillover
# MAGIC - Persisted to checkpoint location
# MAGIC - Fault-tolerant and recoverable
# MAGIC - Partitioned for parallelism
# MAGIC
# MAGIC **State Management Patterns:**
# MAGIC
# MAGIC **1. Windowed Aggregations**
# MAGIC ```python
# MAGIC # Tumbling window: non-overlapping fixed intervals
# MAGIC df.groupBy(
# MAGIC     window("timestamp", "5 minutes"),
# MAGIC     "geohash"
# MAGIC ).count()
# MAGIC
# MAGIC # Sliding window: overlapping intervals
# MAGIC df.groupBy(
# MAGIC     window("timestamp", "10 minutes", "5 minutes"),
# MAGIC     "zone_id"
# MAGIC ).avg("speed")
# MAGIC ```
# MAGIC
# MAGIC **2. Stateful Tracking with flatMapGroupsWithState**
# MAGIC ```python
# MAGIC # Track trajectory state per device
# MAGIC def update_trajectory(device_id, events, state):
# MAGIC     # Custom state management logic
# MAGIC     # Return updated events and new state
# MAGIC     pass
# MAGIC
# MAGIC df.groupByKey(lambda x: x.device_id) \
# MAGIC   .flatMapGroupsWithState(
# MAGIC       outputMode=OutputMode.Append(),
# MAGIC       timeoutConf=GroupStateTimeout.EventTimeTimeout()
# MAGIC   )(update_trajectory)
# MAGIC ```
# MAGIC
# MAGIC **3. Stream Deduplication**
# MAGIC ```python
# MAGIC # Remove duplicate GPS points within window
# MAGIC df.withWatermark("timestamp", "1 hour") \
# MAGIC   .dropDuplicates(["device_id", "timestamp"])
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Indexing in Streaming
# MAGIC
# MAGIC ### Why Spatial Indexing Matters
# MAGIC
# MAGIC **Challenges in Streaming Geospatial:**
# MAGIC - Need fast point-in-polygon tests
# MAGIC - Efficient nearest neighbor queries
# MAGIC - Spatial joins on continuous data
# MAGIC - Grouping by geographic region
# MAGIC
# MAGIC ### H3: Hierarchical Hexagonal Indexing
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Uniform hexagonal cells
# MAGIC - Multiple resolution levels (0-15)
# MAGIC - Parent-child relationships
# MAGIC - Neighbor queries in O(1)
# MAGIC
# MAGIC **Using H3 in Streaming:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import expr
# MAGIC
# MAGIC # Add H3 index to streaming data
# MAGIC df_indexed = gps_stream.withColumn(
# MAGIC     "h3_index",
# MAGIC     expr("h3_latlngtocell(latitude, longitude, 9)")
# MAGIC )
# MAGIC
# MAGIC # Group by H3 cell for spatial aggregations
# MAGIC traffic_density = df_indexed \
# MAGIC     .withWatermark("timestamp", "5 minutes") \
# MAGIC     .groupBy(
# MAGIC         window("timestamp", "1 minute"),
# MAGIC         "h3_index"
# MAGIC     ).count()
# MAGIC ```
# MAGIC
# MAGIC **Resolution Guidelines:**
# MAGIC - **Res 5**: ~252 km² (country/state level)
# MAGIC - **Res 7**: ~5.2 km² (city level)
# MAGIC - **Res 9**: ~105 m² (neighborhood)
# MAGIC - **Res 11**: ~2.6 m² (building level)
# MAGIC
# MAGIC ### Geohash: Alternative Indexing
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Base-32 encoded strings
# MAGIC - Variable precision (1-12 chars)
# MAGIC - Simpler but less uniform than H3
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import expr
# MAGIC
# MAGIC # Add geohash to stream
# MAGIC df.withColumn("geohash", 
# MAGIC              expr("ST_GeoHash(ST_Point(longitude, latitude), 7)"))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Management
# MAGIC
# MAGIC ### What are Checkpoints?
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Track streaming query progress
# MAGIC - Enable fault tolerance
# MAGIC - Store state information
# MAGIC - Allow exactly-once semantics
# MAGIC
# MAGIC **Checkpoint Contents:**
# MAGIC - Offset information (which data processed)
# MAGIC - State store data (aggregations, etc.)
# MAGIC - Metadata (schema, configuration)
# MAGIC
# MAGIC ### Checkpoint Strategies
# MAGIC
# MAGIC **1. Single Checkpoint Location**
# MAGIC ```python
# MAGIC query = df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/mnt/checkpoints/gps_stream") \
# MAGIC     .start("/mnt/delta/gps_data")
# MAGIC ```
# MAGIC
# MAGIC **2. Separate Checkpoints per Query**
# MAGIC - Isolate failures
# MAGIC - Independent restarts
# MAGIC - Clearer organization
# MAGIC
# MAGIC **3. Checkpoint Cleanup**
# MAGIC ```python
# MAGIC # Old checkpoints can grow large
# MAGIC # Manual cleanup when restarting from scratch
# MAGIC dbutils.fs.rm("/mnt/checkpoints/gps_stream", recurse=True)
# MAGIC ```
# MAGIC
# MAGIC ### Checkpoint Best Practices
# MAGIC
# MAGIC ✅ **Do:**
# MAGIC - Use reliable storage (DBFS, S3, ADLS)
# MAGIC - One checkpoint per streaming query
# MAGIC - Include query name in path
# MAGIC - Monitor checkpoint size
# MAGIC - Plan for schema evolution
# MAGIC
# MAGIC ❌ **Don't:**
# MAGIC - Share checkpoints between queries
# MAGIC - Use local disk for checkpoints
# MAGIC - Manually modify checkpoint files
# MAGIC - Delete checkpoints unless restarting fresh

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geofencing in Streaming
# MAGIC
# MAGIC ### Geofence Architecture Patterns
# MAGIC
# MAGIC **Pattern 1: Broadcast Static Geofences**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC # Small set of geofences (< 1000)
# MAGIC geofences = spark.table("geofence_definitions")
# MAGIC
# MAGIC # Broadcast to all executors
# MAGIC stream_with_fences = gps_stream.join(
# MAGIC     broadcast(geofences),
# MAGIC     expr("ST_Contains(geofences.geometry, ST_Point(longitude, latitude))")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Pattern 2: H3 Pre-filtering**
# MAGIC ```python
# MAGIC # Index geofences with H3
# MAGIC geofences_h3 = geofences.select("geofence_id", explode("h3_cells"))
# MAGIC
# MAGIC # Fast join on H3 index, then precise geometry test
# MAGIC stream_filtered = gps_stream \
# MAGIC     .join(broadcast(geofences_h3), "h3_index") \
# MAGIC     .filter("ST_Contains(geometry, point)")
# MAGIC ```
# MAGIC
# MAGIC **Pattern 3: Stateful Entry/Exit Detection**
# MAGIC ```python
# MAGIC # Track previous location state
# MAGIC def detect_geofence_events(device_id, locations, state):
# MAGIC     previous_fence = state.get("last_geofence")
# MAGIC     current_fence = locations[-1].geofence_id
# MAGIC     
# MAGIC     events = []
# MAGIC     if previous_fence != current_fence:
# MAGIC         if previous_fence:
# MAGIC             events.append(("EXIT", previous_fence))
# MAGIC         if current_fence:
# MAGIC             events.append(("ENTER", current_fence))
# MAGIC     
# MAGIC     state.update("last_geofence", current_fence)
# MAGIC     return events, state
# MAGIC ```
# MAGIC
# MAGIC ### Dynamic Geofence Updates
# MAGIC
# MAGIC **Challenge:**
# MAGIC - Geofences change over time
# MAGIC - Need to update broadcast variable
# MAGIC - Cannot restart stream for each update
# MAGIC
# MAGIC **Solution: Delta Lake + Stream-Static Join**
# MAGIC ```python
# MAGIC # Geofences stored in Delta table
# MAGIC # Auto-refreshes on each micro-batch
# MAGIC current_geofences = spark.read.format("delta").table("geofences")
# MAGIC
# MAGIC gps_stream.join(
# MAGIC     broadcast(current_geofences),
# MAGIC     geofence_condition
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trajectory Analysis Patterns
# MAGIC
# MAGIC ### Trajectory Reconstruction
# MAGIC
# MAGIC **Challenge:**
# MAGIC - GPS points arrive out of order
# MAGIC - Need to group by device and time window
# MAGIC - Connect points into paths
# MAGIC - Handle gaps in data
# MAGIC
# MAGIC **Window-based Approach:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import collect_list, sort_array
# MAGIC
# MAGIC # Aggregate points into trajectories
# MAGIC trajectories = gps_stream \
# MAGIC     .withWatermark("timestamp", "10 minutes") \
# MAGIC     .groupBy(
# MAGIC         "device_id",
# MAGIC         window("timestamp", "5 minutes")
# MAGIC     ) \
# MAGIC     .agg(
# MAGIC         sort_array(collect_list(struct("timestamp", "latitude", "longitude")))
# MAGIC             .alias("points")
# MAGIC     ) \
# MAGIC     .withColumn("trajectory", 
# MAGIC                 expr("ST_MakeLine(points)"))
# MAGIC ```
# MAGIC
# MAGIC ### Speed and Direction Calculation
# MAGIC
# MAGIC **From Sequential Points:**
# MAGIC ```python
# MAGIC # Using lag to get previous point
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import lag
# MAGIC
# MAGIC window_spec = Window.partitionBy("device_id").orderBy("timestamp")
# MAGIC
# MAGIC df_with_prev = gps_stream \
# MAGIC     .withColumn("prev_lat", lag("latitude").over(window_spec)) \
# MAGIC     .withColumn("prev_lon", lag("longitude").over(window_spec)) \
# MAGIC     .withColumn("prev_time", lag("timestamp").over(window_spec))
# MAGIC
# MAGIC # Calculate speed and bearing
# MAGIC df_enriched = df_with_prev.withColumn(
# MAGIC     "calculated_speed",
# MAGIC     expr("ST_Distance(ST_Point(prev_lon, prev_lat), ST_Point(longitude, latitude)) / " +
# MAGIC          "(unix_timestamp(timestamp) - unix_timestamp(prev_time))")
# MAGIC ).withColumn(
# MAGIC     "bearing",
# MAGIC     expr("ST_Azimuth(ST_Point(prev_lon, prev_lat), ST_Point(longitude, latitude))")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Stop Detection
# MAGIC
# MAGIC **Identifying Stationary Periods:**
# MAGIC ```python
# MAGIC # Detect when speed drops below threshold for duration
# MAGIC stops = gps_stream \
# MAGIC     .filter("speed < 1.0") \
# MAGIC     .withWatermark("timestamp", "30 minutes") \
# MAGIC     .groupBy("device_id") \
# MAGIC     .agg(
# MAGIC         min("timestamp").alias("stop_start"),
# MAGIC         max("timestamp").alias("stop_end"),
# MAGIC         avg("latitude").alias("stop_lat"),
# MAGIC         avg("longitude").alias("stop_lon")
# MAGIC     ) \
# MAGIC     .filter("(unix_timestamp(stop_end) - unix_timestamp(stop_start)) > 300")  # 5 min
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream-to-Stream Joins
# MAGIC
# MAGIC ### Types of Streaming Joins
# MAGIC
# MAGIC **1. Stream-Static Join**
# MAGIC - Stream joined with batch table
# MAGIC - No special requirements
# MAGIC - Static side refreshed per micro-batch
# MAGIC
# MAGIC **2. Stream-Stream Join**
# MAGIC - Both sides are streaming
# MAGIC - Requires watermarks on both sides
# MAGIC - State managed automatically
# MAGIC
# MAGIC ### Spatial Stream-Stream Join Example
# MAGIC
# MAGIC **Use Case: Match Drivers to Riders**
# MAGIC ```python
# MAGIC # Driver location stream
# MAGIC drivers = kafka_stream("drivers") \
# MAGIC     .withWatermark("timestamp", "2 minutes") \
# MAGIC     .withColumn("h3_driver", expr("h3_latlngtocell(latitude, longitude, 9)"))
# MAGIC
# MAGIC # Rider request stream
# MAGIC riders = kafka_stream("riders") \
# MAGIC     .withWatermark("timestamp", "2 minutes") \
# MAGIC     .withColumn("h3_rider", expr("h3_latlngtocell(latitude, longitude, 9)"))
# MAGIC
# MAGIC # Join on nearby H3 cells
# MAGIC matches = drivers.join(
# MAGIC     riders,
# MAGIC     expr("""
# MAGIC         h3_driver = h3_rider AND
# MAGIC         timestamp BETWEEN rider_timestamp - INTERVAL 2 MINUTES 
# MAGIC                      AND rider_timestamp + INTERVAL 2 MINUTES
# MAGIC     """)
# MAGIC ).filter("ST_Distance(driver_point, rider_point) < 1000")  # Within 1km
# MAGIC ```
# MAGIC
# MAGIC ### Join Performance Optimization
# MAGIC
# MAGIC **Key Techniques:**
# MAGIC 1. **Spatial indexing first** (H3, geohash)
# MAGIC 2. **Tight watermarks** (reduce state)
# MAGIC 3. **Partition by spatial key** (h3_index)
# MAGIC 4. **Broadcast small streams** if applicable
# MAGIC 5. **Filter before join** (reduce data volume)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Tables for Geospatial
# MAGIC
# MAGIC ### Medallion Architecture Pattern
# MAGIC
# MAGIC **Bronze Layer: Raw Ingestion**
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Raw GPS data from IoT devices"
# MAGIC )
# MAGIC def gps_bronze():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC             .format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "json")
# MAGIC             .load("/mnt/landing/gps")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Silver Layer: Validated & Enriched**
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     comment="Validated GPS with spatial indexing"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_coords", "latitude BETWEEN -90 AND 90")
# MAGIC @dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
# MAGIC def gps_silver():
# MAGIC     return (
# MAGIC         dlt.read_stream("gps_bronze")
# MAGIC             .filter("longitude BETWEEN -180 AND 180")
# MAGIC             .withColumn("h3_index", 
# MAGIC                        expr("h3_latlngtocell(latitude, longitude, 9)"))
# MAGIC             .withColumn("point", 
# MAGIC                        expr("ST_Point(longitude, latitude)"))
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Gold Layer: Aggregated Insights**
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     comment="Hourly traffic density by region"
# MAGIC )
# MAGIC def traffic_density_gold():
# MAGIC     return (
# MAGIC         dlt.read_stream("gps_silver")
# MAGIC             .withWatermark("timestamp", "1 hour")
# MAGIC             .groupBy(
# MAGIC                 window("timestamp", "1 hour"),
# MAGIC                 "h3_index"
# MAGIC             )
# MAGIC             .agg(
# MAGIC                 count("*").alias("vehicle_count"),
# MAGIC                 avg("speed").alias("avg_speed")
# MAGIC             )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### Data Quality Expectations
# MAGIC
# MAGIC **Geospatial-specific Checks:**
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_latitude", "latitude BETWEEN -90 AND 90")
# MAGIC @dlt.expect_or_drop("valid_longitude", "longitude BETWEEN -180 AND 180")
# MAGIC @dlt.expect_or_drop("positive_speed", "speed >= 0")
# MAGIC @dlt.expect_or_drop("valid_heading", "heading BETWEEN 0 AND 360 OR heading IS NULL")
# MAGIC @dlt.expect("reasonable_speed", "speed < 200", on_violation="WARN")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Strategies
# MAGIC
# MAGIC ### Latency Optimization
# MAGIC
# MAGIC **1. Trigger Configuration**
# MAGIC ```python
# MAGIC # Process immediately (lowest latency)
# MAGIC .trigger(availableNow=True)
# MAGIC
# MAGIC # Fixed interval (predictable)
# MAGIC .trigger(processingTime="10 seconds")
# MAGIC
# MAGIC # Continuous (experimental, ~1ms latency)
# MAGIC .trigger(continuous="1 second")
# MAGIC ```
# MAGIC
# MAGIC **2. Partition Strategy**
# MAGIC ```python
# MAGIC # Partition by spatial key for locality
# MAGIC df.repartition("h3_index")
# MAGIC
# MAGIC # Partition by device for trajectory processing
# MAGIC df.repartition("device_id")
# MAGIC ```
# MAGIC
# MAGIC **3. Broadcast Optimization**
# MAGIC ```python
# MAGIC # Broadcast small lookup tables
# MAGIC geofences_broadcast = broadcast(spark.table("geofences"))
# MAGIC
# MAGIC # Reuse across executors
# MAGIC stream.join(geofences_broadcast, join_condition)
# MAGIC ```
# MAGIC
# MAGIC ### Throughput Optimization
# MAGIC
# MAGIC **1. Batch Size Tuning**
# MAGIC ```python
# MAGIC # Kafka configuration
# MAGIC .option("maxOffsetsPerTrigger", 100000)  # Records per batch
# MAGIC .option("startingOffsets", "latest")
# MAGIC ```
# MAGIC
# MAGIC **2. Shuffle Optimization**
# MAGIC ```python
# MAGIC # Reduce shuffle for spatial operations
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 200)
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", True)
# MAGIC ```
# MAGIC
# MAGIC **3. State Store Configuration**
# MAGIC ```python
# MAGIC # RocksDB for large state
# MAGIC spark.conf.set("spark.sql.streaming.stateStore.providerClass",
# MAGIC                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
# MAGIC ```
# MAGIC
# MAGIC ### Memory Optimization
# MAGIC
# MAGIC **State Cleanup:**
# MAGIC ```python
# MAGIC # Set appropriate watermark for state cleanup
# MAGIC df.withWatermark("timestamp", "1 hour")  # Drop state older than 1 hour
# MAGIC
# MAGIC # Use event time timeout for custom state
# MAGIC .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Observability
# MAGIC
# MAGIC ### Streaming Query Metrics
# MAGIC
# MAGIC **Built-in Metrics:**
# MAGIC ```python
# MAGIC # Start query and monitor
# MAGIC query = df.writeStream.start()
# MAGIC
# MAGIC # Get recent progress
# MAGIC query.recentProgress  # Last few micro-batch statistics
# MAGIC
# MAGIC # Current status
# MAGIC query.status  # Message, isDataAvailable, isTriggerActive
# MAGIC
# MAGIC # Latest progress details
# MAGIC query.lastProgress
# MAGIC ```
# MAGIC
# MAGIC **Key Metrics to Monitor:**
# MAGIC - **Input Rate**: Records/second ingested
# MAGIC - **Processing Rate**: Records/second processed
# MAGIC - **Batch Duration**: Time per micro-batch
# MAGIC - **Trigger Latency**: Delay between batches
# MAGIC - **State Memory**: Size of state stores
# MAGIC - **Watermark**: Current watermark value
# MAGIC
# MAGIC ### Custom Instrumentation
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import current_timestamp, unix_timestamp
# MAGIC
# MAGIC # Add processing metrics
# MAGIC df_monitored = gps_stream \
# MAGIC     .withColumn("processing_time", current_timestamp()) \
# MAGIC     .withColumn("latency_seconds",
# MAGIC                 unix_timestamp("processing_time") - unix_timestamp("timestamp"))
# MAGIC
# MAGIC # Log metrics to Delta table
# MAGIC df_monitored.writeStream \
# MAGIC     .foreachBatch(lambda batch, epoch: log_metrics(batch, epoch)) \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC ### Alert Patterns
# MAGIC
# MAGIC **Geofence Violations:**
# MAGIC ```python
# MAGIC def send_alerts(batch_df, batch_id):
# MAGIC     violations = batch_df.filter("event_type = 'ENTER' AND geofence_id = 'restricted_zone'")
# MAGIC     if violations.count() > 0:
# MAGIC         # Send to notification system
# MAGIC         publish_to_sns(violations)
# MAGIC
# MAGIC geofence_events.writeStream \
# MAGIC     .foreachBatch(send_alerts) \
# MAGIC     .start()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fault Tolerance and Recovery
# MAGIC
# MAGIC ### Exactly-Once Guarantees
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. **Replayable source** (Kafka, Delta, files)
# MAGIC 2. **Idempotent sink** (Delta Lake, JDBC with keys)
# MAGIC 3. **Checkpoint location** (tracks offsets)
# MAGIC
# MAGIC **Example Configuration:**
# MAGIC ```python
# MAGIC query = df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/checkpoints/gps") \
# MAGIC     .start("/delta/gps_processed")
# MAGIC ```
# MAGIC
# MAGIC ### Failure Scenarios and Recovery
# MAGIC
# MAGIC **Executor Failure:**
# MAGIC - Spark automatically retries failed tasks
# MAGIC - Checkpoint ensures no data loss
# MAGIC - Processing continues from last committed offset
# MAGIC
# MAGIC **Driver Failure:**
# MAGIC - Requires external restart mechanism
# MAGIC - Structured Streaming on Databricks auto-restarts
# MAGIC - Resume from checkpoint on restart
# MAGIC
# MAGIC **Source Failure:**
# MAGIC - Kafka: Reconnect and continue from committed offset
# MAGIC - File sources: Monitor directory for new files
# MAGIC - Delta: Read from latest version
# MAGIC
# MAGIC ### Best Practices for Reliability
# MAGIC
# MAGIC ✅ **Do:**
# MAGIC - Always set checkpoint locations
# MAGIC - Use Delta Lake for stateful operations
# MAGIC - Implement idempotent operations
# MAGIC - Monitor query health
# MAGIC - Test recovery procedures
# MAGIC
# MAGIC ❌ **Avoid:**
# MAGIC - Non-deterministic operations in streams
# MAGIC - Sharing checkpoint locations
# MAGIC - Manual checkpoint manipulation
# MAGIC - Unbounded state growth

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Patterns
# MAGIC
# MAGIC ### Pattern 1: Multi-Level Geofencing
# MAGIC
# MAGIC **Use Case:** City → District → Neighborhood hierarchy
# MAGIC ```python
# MAGIC # Use H3 parent-child relationships
# MAGIC df_with_hierarchy = gps_stream \
# MAGIC     .withColumn("h3_neighborhood", expr("h3_latlngtocell(lat, lon, 11)")) \
# MAGIC     .withColumn("h3_district", expr("h3_celltoparent(h3_neighborhood, 9)")) \
# MAGIC     .withColumn("h3_city", expr("h3_celltoparent(h3_district, 7)"))
# MAGIC
# MAGIC # Efficient hierarchical aggregations
# MAGIC city_stats = df_with_hierarchy.groupBy("h3_city").count()
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Adaptive Spatial Resolution
# MAGIC
# MAGIC **Use Case:** Higher resolution in dense areas
# MAGIC ```python
# MAGIC def adaptive_h3_index(lat, lon, density):
# MAGIC     if density > 1000:  # High density
# MAGIC         return h3_latlngtocell(lat, lon, 11)  # Fine resolution
# MAGIC     elif density > 100:  # Medium density
# MAGIC         return h3_latlngtocell(lat, lon, 9)
# MAGIC     else:  # Low density
# MAGIC         return h3_latlngtocell(lat, lon, 7)   # Coarse resolution
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Predictive Geofencing
# MAGIC
# MAGIC **Use Case:** Alert before entering geofence
# MAGIC ```python
# MAGIC # Calculate trajectory projection
# MAGIC df_with_prediction = gps_stream \
# MAGIC     .withColumn("projected_lat", 
# MAGIC                 col("latitude") + (col("speed") * cos(col("bearing")) / 111000 * 60)) \
# MAGIC     .withColumn("projected_lon",
# MAGIC                 col("longitude") + (col("speed") * sin(col("bearing")) / 111000 * 60))
# MAGIC
# MAGIC # Check if projected point enters geofence
# MAGIC predictive_alerts = df_with_prediction \
# MAGIC     .join(broadcast(geofences),
# MAGIC           expr("ST_Contains(geometry, ST_Point(projected_lon, projected_lat))"))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Pitfalls and Solutions
# MAGIC
# MAGIC ### Pitfall 1: State Memory Explosion
# MAGIC
# MAGIC **Problem:** State grows indefinitely without cleanup
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Always use watermarks for stateful operations
# MAGIC df.withWatermark("timestamp", "1 hour") \
# MAGIC   .groupBy(window("timestamp", "10 minutes"), "device_id") \
# MAGIC   .count()
# MAGIC ```
# MAGIC
# MAGIC ### Pitfall 2: Coordinate System Confusion
# MAGIC
# MAGIC **Problem:** Mixing lat/lon order or using wrong projection
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Always use (longitude, latitude) for ST_Point (x, y convention)
# MAGIC correct = expr("ST_Point(longitude, latitude)")
# MAGIC
# MAGIC # Document coordinate reference system
# MAGIC # WGS84 (EPSG:4326) for GPS data
# MAGIC ```
# MAGIC
# MAGIC ### Pitfall 3: Inefficient Spatial Joins
# MAGIC
# MAGIC **Problem:** Cartesian product on large streams
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Pre-filter with spatial index before expensive geometry operations
# MAGIC df.withColumn("h3", expr("h3_latlngtocell(lat, lon, 9)")) \
# MAGIC   .join(geofences_h3, "h3")  # Fast join on index \
# MAGIC   .filter("ST_Contains(geometry, point)")  # Precise test on subset
# MAGIC ```
# MAGIC
# MAGIC ### Pitfall 4: Ignoring Time Zones
# MAGIC
# MAGIC **Problem:** GPS timestamps in different time zones
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Convert all timestamps to UTC
# MAGIC df.withColumn("timestamp_utc", 
# MAGIC              to_utc_timestamp(col("local_timestamp"), col("timezone")))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Streaming Architecture**: Unbounded tables with micro-batch processing
# MAGIC 2. **Watermarking**: Essential for state management and late data handling
# MAGIC 3. **Spatial Indexing**: H3 and geohash for efficient spatial operations
# MAGIC 4. **State Management**: Choose appropriate patterns for your use case
# MAGIC 5. **Checkpointing**: Critical for fault tolerance and exactly-once semantics
# MAGIC 6. **Performance**: Balance latency, throughput, and resource usage
# MAGIC 7. **Monitoring**: Track metrics and implement alerting
# MAGIC 8. **Delta Live Tables**: Medallion architecture for structured pipelines
# MAGIC
# MAGIC ### Architecture Decision Guide
# MAGIC
# MAGIC **Use Structured Streaming when:**
# MAGIC - Need custom business logic
# MAGIC - Complex stateful processing
# MAGIC - Dynamic query modifications
# MAGIC - Integration with external systems
# MAGIC
# MAGIC **Use Delta Live Tables when:**
# MAGIC - Standard ETL patterns
# MAGIC - Data quality requirements
# MAGIC - Medallion architecture
# MAGIC - Declarative pipeline definition
# MAGIC
# MAGIC ### What's Next?
# MAGIC
# MAGIC In the following demos and labs, you'll implement:
# MAGIC
# MAGIC **Demo 1: Setting Up Streaming Pipelines**
# MAGIC - Kafka integration for GPS data
# MAGIC - Schema management and validation
# MAGIC - Checkpoint configuration
# MAGIC - Monitoring and debugging
# MAGIC
# MAGIC **Demo 2: Real-Time Geofencing**
# MAGIC - Dynamic geofence definitions
# MAGIC - Entry/exit event detection
# MAGIC - Alert generation
# MAGIC - Stateful processing
# MAGIC
# MAGIC **Demo 3: Trajectory Analysis**
# MAGIC - Path reconstruction
# MAGIC - Speed and direction calculations
# MAGIC - Anomaly detection
# MAGIC - Smoothing algorithms
# MAGIC
# MAGIC **Demo 4: Stream-to-Stream Spatial Joins**
# MAGIC - Joining moving objects
# MAGIC - Watermark strategies
# MAGIC - Performance optimization
# MAGIC
# MAGIC **Demo 5: Delta Live Tables for Geospatial**
# MAGIC - Medallion architecture implementation
# MAGIC - Data quality expectations
# MAGIC - Incremental processing
# MAGIC
# MAGIC **Labs 6-9: Real-World Applications**
# MAGIC - Fleet tracking system
# MAGIC - IoT sensor analytics
# MAGIC - Ride sharing platform
# MAGIC - Smart city traffic management

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC - [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
# MAGIC - [Apache Sedona Documentation](https://sedona.apache.org/latest/)
# MAGIC - [H3 Spatial Index](https://h3geo.org/)
# MAGIC
# MAGIC ### Research Papers
# MAGIC - "Moving Object Databases" - Güting & Schneider
# MAGIC - "Geofencing: A Survey" - IEEE
# MAGIC - "Trajectory Data Mining" - Zheng & Zhou
# MAGIC
# MAGIC ### Tools and Libraries
# MAGIC - **Apache Sedona**: Spatial analytics on Spark
# MAGIC - **H3-py**: Python bindings for H3
# MAGIC - **GeoPandas**: Python geospatial data analysis
# MAGIC - **Folium**: Interactive map visualizations
# MAGIC
# MAGIC ### Community
# MAGIC - Databricks Community Forums
# MAGIC - Apache Sedona Slack
# MAGIC - H3 GitHub Discussions
