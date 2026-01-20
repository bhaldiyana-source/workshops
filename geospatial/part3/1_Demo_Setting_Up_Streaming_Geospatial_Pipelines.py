# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Setting Up Streaming Geospatial Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC This demo walks through setting up production-ready streaming geospatial pipelines using Spark Structured Streaming. You'll learn to ingest GPS data from Kafka and file sources, define schemas for streaming geometries, configure checkpoints for fault tolerance, and monitor streaming queries with built-in metrics. We'll cover both real-time and batch streaming patterns for geospatial data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Set up Kafka integration for GPS data streams
# MAGIC - Define schemas for streaming geospatial data
# MAGIC - Implement checkpoint management for fault tolerance
# MAGIC - Configure Auto Loader for file-based streaming
# MAGIC - Monitor streaming queries with metrics and dashboards
# MAGIC - Handle schema evolution in streaming pipelines
# MAGIC - Optimize streaming ingestion performance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lecture 0: Streaming Geospatial Architecture
# MAGIC - Access to Databricks workspace with DBR 15.0+
# MAGIC - Basic knowledge of Spark Structured Streaming
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install faker h3 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json

# Configuration
catalog = "main"
schema_name = "geospatial_streaming"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

# Create schema if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Using catalog: {catalog}")
print(f"Using schema: {schema_name}")
print(f"Checkpoint location: {checkpoint_base}")
print(f"Data location: {data_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Schema Definition for Streaming Geometries
# MAGIC
# MAGIC Proper schema definition is critical for streaming geospatial data. We'll define schemas for:
# MAGIC - Raw GPS point data
# MAGIC - Trajectory/path data
# MAGIC - Geofence definitions

# COMMAND ----------

# DBTITLE 1,Define GPS Point Schema
# Schema for incoming GPS data streams
gps_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("altitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),          # meters/second
    StructField("heading", DoubleType(), True),        # degrees 0-360
    StructField("accuracy", DoubleType(), True),       # meters
    StructField("device_type", StringType(), True),    # vehicle, phone, tracker
    StructField("battery_level", IntegerType(), True), # percentage
    StructField("metadata", MapType(StringType(), StringType()), True)
])

# Display schema
print("GPS Point Schema:")
print(gps_schema.simpleString())

# COMMAND ----------

# DBTITLE 1,Define Geofence Schema
# Schema for geofence definitions
geofence_schema = StructType([
    StructField("geofence_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("geometry_wkt", StringType(), False),  # Well-Known Text
    StructField("geometry_type", StringType(), False), # POLYGON, CIRCLE
    StructField("center_lat", DoubleType(), True),
    StructField("center_lon", DoubleType(), True),
    StructField("radius_meters", DoubleType(), True),
    StructField("category", StringType(), True),       # restricted, delivery, parking
    StructField("active", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

print("Geofence Schema:")
print(geofence_schema.simpleString())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Generate Synthetic GPS Data Stream
# MAGIC
# MAGIC For demonstration purposes, we'll generate synthetic GPS data that simulates real-world patterns:
# MAGIC - Multiple devices moving through space
# MAGIC - Realistic timestamps
# MAGIC - Occasional data quality issues
# MAGIC - Out-of-order events

# COMMAND ----------

# DBTITLE 1,Create Synthetic GPS Data Generator
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

def generate_gps_records(num_devices=10, records_per_device=100):
    """Generate synthetic GPS data for testing"""
    records = []
    
    # San Francisco bay area bounds
    lat_min, lat_max = 37.7, 37.8
    lon_min, lon_max = -122.5, -122.4
    
    for device_num in range(num_devices):
        device_id = f"device_{device_num:03d}"
        device_type = random.choice(["vehicle", "phone", "tracker"])
        
        # Start position
        lat = random.uniform(lat_min, lat_max)
        lon = random.uniform(lon_min, lon_max)
        base_time = datetime.now() - timedelta(hours=2)
        
        for i in range(records_per_device):
            # Simulate movement
            lat += random.uniform(-0.001, 0.001)
            lon += random.uniform(-0.001, 0.001)
            
            # Keep within bounds
            lat = max(lat_min, min(lat_max, lat))
            lon = max(lon_min, min(lon_max, lon))
            
            # Generate timestamp with occasional out-of-order
            if random.random() < 0.05:  # 5% out of order
                ts = base_time + timedelta(seconds=i * 10 - random.randint(30, 120))
            else:
                ts = base_time + timedelta(seconds=i * 10)
            
            record = {
                "device_id": device_id,
                "timestamp": ts,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "altitude": round(random.uniform(0, 100), 1) if random.random() > 0.1 else None,
                "speed": round(random.uniform(0, 30), 2),
                "heading": round(random.uniform(0, 360), 1),
                "accuracy": round(random.uniform(5, 50), 1),
                "device_type": device_type,
                "battery_level": random.randint(20, 100),
                "metadata": {"source": "synthetic", "version": "1.0"}
            }
            
            # Introduce some data quality issues
            if random.random() < 0.02:  # 2% bad coordinates
                record["latitude"] = 999.0
            
            records.append(record)
    
    return records

# Generate sample data
sample_records = generate_gps_records(num_devices=5, records_per_device=50)
print(f"Generated {len(sample_records)} GPS records")
print("\nSample record:")
print(json.dumps(sample_records[0], indent=2, default=str))

# COMMAND ----------

# DBTITLE 1,Write Sample Data to Landing Zone
# Write as JSON files to simulate incoming data stream
landing_path = f"{data_base}/landing/gps"

# Clear previous data
dbutils.fs.rm(landing_path, recurse=True)

# Write in batches to simulate streaming
batch_size = 50
for i in range(0, len(sample_records), batch_size):
    batch = sample_records[i:i+batch_size]
    batch_df = spark.createDataFrame(batch, schema=gps_schema)
    
    batch_path = f"{landing_path}/batch_{i//batch_size:03d}"
    batch_df.write.mode("overwrite").json(batch_path)
    print(f"Wrote batch {i//batch_size} with {len(batch)} records to {batch_path}")

print(f"\nTotal files in landing zone: {len(dbutils.fs.ls(landing_path))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: File-Based Streaming with Auto Loader
# MAGIC
# MAGIC Auto Loader provides an easy way to ingest files from cloud storage incrementally and efficiently.
# MAGIC It automatically:
# MAGIC - Detects new files
# MAGIC - Handles schema inference and evolution
# MAGIC - Scales to millions of files
# MAGIC - Provides exactly-once guarantees

# COMMAND ----------

# DBTITLE 1,Setup Auto Loader Stream
def create_autoloader_stream(source_path, checkpoint_path):
    """Create streaming DataFrame using Auto Loader"""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", "timestamp TIMESTAMP")
            .schema(gps_schema)  # Explicit schema for production
            .load(source_path)
    )

# Create the stream
gps_stream = create_autoloader_stream(
    source_path=landing_path,
    checkpoint_path=f"{checkpoint_base}/autoloader"
)

# Verify stream is created
print("Stream created successfully")
print(f"Is streaming: {gps_stream.isStreaming}")
print("\nStream schema:")
gps_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Initial Data Validation and Enrichment
# Add data quality checks and spatial enrichment
gps_validated = (
    gps_stream
    # Add processing timestamp
    .withColumn("processing_time", F.current_timestamp())
    
    # Calculate processing lag
    .withColumn("lag_seconds", 
                F.unix_timestamp("processing_time") - F.unix_timestamp("timestamp"))
    
    # Validate coordinates
    .withColumn("valid_latitude", 
                F.col("latitude").between(-90, 90))
    .withColumn("valid_longitude",
                F.col("longitude").between(-180, 180))
    .withColumn("is_valid",
                F.col("valid_latitude") & F.col("valid_longitude"))
    
    # Add H3 spatial index (resolution 9 ~ 105m²)
    .withColumn("h3_index",
                F.expr("h3_latlngtocell(latitude, longitude, 9)"))
    
    # Add geohash for alternative indexing
    .withColumn("geohash",
                F.expr("ST_GeoHash(ST_Point(longitude, latitude), 7)"))
    
    # Create point geometry
    .withColumn("point_wkt",
                F.expr("ST_AsText(ST_Point(longitude, latitude))"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Checkpoint Management
# MAGIC
# MAGIC Checkpoints are critical for:
# MAGIC - Fault tolerance
# MAGIC - Exactly-once processing
# MAGIC - State management
# MAGIC - Progress tracking

# COMMAND ----------

# DBTITLE 1,Write Stream to Delta Lake with Checkpoint
# Define output table
output_table = f"{catalog}.{schema_name}.gps_raw"
checkpoint_path = f"{checkpoint_base}/gps_raw"

# Start streaming query with checkpoint
query_gps_raw = (
    gps_validated
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")  # Allow schema evolution
    .trigger(processingTime="5 seconds")  # Micro-batch interval
    .toTable(output_table)
)

print(f"Started streaming query: {query_gps_raw.name}")
print(f"Query ID: {query_gps_raw.id}")
print(f"Checkpoint location: {checkpoint_path}")

# COMMAND ----------

# DBTITLE 1,Monitor Stream Progress
import time

# Wait for some data to be processed
print("Processing stream... (waiting 15 seconds)")
time.sleep(15)

# Get query status
status = query_gps_raw.status
print("\n=== Query Status ===")
print(f"Message: {status['message']}")
print(f"Is Data Available: {status['isDataAvailable']}")
print(f"Is Trigger Active: {status['isTriggerActive']}")

# Get recent progress
if query_gps_raw.recentProgress:
    latest_progress = query_gps_raw.lastProgress
    print("\n=== Latest Progress ===")
    print(f"Batch ID: {latest_progress.get('batchId', 'N/A')}")
    print(f"Input Rows: {latest_progress.get('numInputRows', 0)}")
    print(f"Processed Rows: {latest_progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
    print(f"Batch Duration: {latest_progress.get('durationMs', {}).get('triggerExecution', 0)} ms")
    
    # State metrics
    if 'stateOperators' in latest_progress and latest_progress['stateOperators']:
        state_info = latest_progress['stateOperators'][0]
        print(f"State Memory: {state_info.get('memoryUsedBytes', 0) / 1024 / 1024:.2f} MB")

# COMMAND ----------

# DBTITLE 1,Inspect Checkpoint Structure
# Examine checkpoint directory structure
checkpoint_contents = dbutils.fs.ls(checkpoint_path)
print("=== Checkpoint Contents ===")
for item in checkpoint_contents:
    print(f"{item.name:<30} {item.size:>15,} bytes")

# Show commits
commits_path = f"{checkpoint_path}/commits"
if any(item.path.endswith("commits/") for item in checkpoint_contents):
    commits = dbutils.fs.ls(commits_path)
    print(f"\n=== Commits ({len(commits)}) ===")
    for commit in commits[:5]:  # Show first 5
        print(f"  {commit.name}")

# Show offsets
offsets_path = f"{checkpoint_path}/offsets"
if any(item.path.endswith("offsets/") for item in checkpoint_contents):
    offsets = dbutils.fs.ls(offsets_path)
    print(f"\n=== Offsets ({len(offsets)}) ===")
    for offset in offsets[:5]:  # Show first 5
        print(f"  {offset.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Query the Streaming Results
# MAGIC
# MAGIC While the stream is running, we can query the Delta table to see the results.

# COMMAND ----------

# DBTITLE 1,Query Ingested Data
# Read from Delta table
gps_table = spark.table(output_table)

print(f"Total records ingested: {gps_table.count()}")
print(f"\nRecords per device:")
gps_table.groupBy("device_id").count().orderBy("device_id").show()

print("\nData quality summary:")
gps_table.groupBy("is_valid").count().show()

print("\nProcessing lag statistics:")
gps_table.select(
    F.min("lag_seconds").alias("min_lag"),
    F.avg("lag_seconds").alias("avg_lag"),
    F.max("lag_seconds").alias("max_lag")
).show()

# COMMAND ----------

# DBTITLE 1,Sample Valid Records
print("Sample of valid GPS records:")
gps_table.filter("is_valid = true") \
    .select(
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "speed",
        "heading",
        "h3_index",
        "geohash"
    ) \
    .orderBy("timestamp") \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Spatial Distribution Analysis
print("Spatial distribution by H3 cell:")
gps_table.filter("is_valid = true") \
    .groupBy("h3_index") \
    .agg(
        F.count("*").alias("point_count"),
        F.countDistinct("device_id").alias("unique_devices"),
        F.avg("speed").alias("avg_speed"),
        F.min("timestamp").alias("first_seen"),
        F.max("timestamp").alias("last_seen")
    ) \
    .orderBy(F.desc("point_count")) \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced Streaming Patterns

# COMMAND ----------

# DBTITLE 1,Streaming Aggregation with Watermark
# Create windowed aggregation stream
aggregated_stream = (
    gps_validated
    .filter("is_valid = true")
    .withWatermark("timestamp", "10 minutes")  # Handle late data up to 10 min
    .groupBy(
        F.window("timestamp", "1 minute"),  # 1-minute tumbling windows
        "device_id",
        "h3_index"
    )
    .agg(
        F.count("*").alias("point_count"),
        F.avg("speed").alias("avg_speed"),
        F.max("speed").alias("max_speed"),
        F.avg("accuracy").alias("avg_accuracy"),
        F.min("timestamp").alias("window_start"),
        F.max("timestamp").alias("window_end")
    )
    .select(
        "device_id",
        "h3_index",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "point_count",
        "avg_speed",
        "max_speed",
        "avg_accuracy"
    )
)

# Write aggregated stream
agg_table = f"{catalog}.{schema_name}.gps_aggregated"
agg_checkpoint = f"{checkpoint_base}/gps_aggregated"

query_aggregated = (
    aggregated_stream
    .writeStream
    .format("delta")
    .outputMode("append")  # Append mode for windowed aggregations with watermark
    .option("checkpointLocation", agg_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(agg_table)
)

print(f"Started aggregation query: {query_aggregated.name}")

# COMMAND ----------

# DBTITLE 1,Monitor Aggregation Stream
# Wait for processing
time.sleep(15)

# Query aggregated results
agg_results = spark.table(agg_table)
print(f"Total aggregation windows: {agg_results.count()}")
print("\nSample aggregations:")
agg_results.orderBy(F.desc("window_start")).limit(10).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Deduplication Stream
# Remove duplicate GPS points within watermark window
deduplicated_stream = (
    gps_validated
    .filter("is_valid = true")
    .withWatermark("timestamp", "5 minutes")
    .dropDuplicates(["device_id", "timestamp", "latitude", "longitude"])
)

# Write deduplicated stream
dedup_table = f"{catalog}.{schema_name}.gps_deduplicated"
dedup_checkpoint = f"{checkpoint_base}/gps_deduplicated"

query_dedup = (
    deduplicated_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", dedup_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(dedup_table)
)

print(f"Started deduplication query: {query_dedup.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Monitoring Dashboard Metrics

# COMMAND ----------

# DBTITLE 1,Create Monitoring Metrics Function
def get_stream_metrics(query):
    """Extract key metrics from streaming query"""
    if not query.lastProgress:
        return None
    
    progress = query.lastProgress
    
    metrics = {
        "query_name": query.name,
        "query_id": str(query.id),
        "batch_id": progress.get("batchId", -1),
        "timestamp": progress.get("timestamp", ""),
        
        # Input metrics
        "input_rows": progress.get("numInputRows", 0),
        "input_rate": progress.get("inputRowsPerSecond", 0),
        
        # Processing metrics
        "processed_rows": progress.get("processedRowsPerSecond", 0),
        "batch_duration_ms": progress.get("durationMs", {}).get("triggerExecution", 0),
        
        # Source metrics
        "sources": progress.get("sources", []),
        
        # Sink metrics
        "sink": progress.get("sink", {}),
        
        # State metrics
        "state_operators": progress.get("stateOperators", [])
    }
    
    return metrics

# COMMAND ----------

# DBTITLE 1,Display Metrics for All Queries
import pandas as pd

# Collect metrics from all running queries
all_queries = [query_gps_raw, query_aggregated, query_dedup]
metrics_data = []

for query in all_queries:
    metrics = get_stream_metrics(query)
    if metrics:
        metrics_data.append({
            "Query Name": metrics["query_name"],
            "Batch ID": metrics["batch_id"],
            "Input Rows": metrics["input_rows"],
            "Input Rate": f"{metrics['input_rate']:.2f}",
            "Process Rate": f"{metrics['processed_rows']:.2f}",
            "Duration (ms)": metrics["batch_duration_ms"]
        })

if metrics_data:
    df_metrics = pd.DataFrame(metrics_data)
    print("=== Streaming Query Metrics ===")
    print(df_metrics.to_string(index=False))
else:
    print("No metrics available yet")

# COMMAND ----------

# DBTITLE 1,Check for Data Quality Issues
print("=== Data Quality Report ===\n")

# Invalid coordinates
invalid_count = gps_table.filter("is_valid = false").count()
total_count = gps_table.count()
print(f"Invalid coordinates: {invalid_count} / {total_count} ({invalid_count/total_count*100:.2f}%)")

# Duplicate check (before deduplication)
duplicate_check = gps_table.groupBy("device_id", "timestamp", "latitude", "longitude") \
    .count() \
    .filter("count > 1")
dup_count = duplicate_check.count()
print(f"Duplicate records: {dup_count}")

# Late data analysis
print("\nLate data analysis (lag > 60 seconds):")
late_data = gps_table.filter("lag_seconds > 60") \
    .select(
        "device_id",
        "timestamp",
        "processing_time",
        "lag_seconds"
    ) \
    .orderBy(F.desc("lag_seconds"))

late_count = late_data.count()
print(f"Late arrivals: {late_count}")
if late_count > 0:
    late_data.limit(5).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Stream Management and Cleanup

# COMMAND ----------

# DBTITLE 1,List Active Streams
print("=== Active Streaming Queries ===\n")
active_streams = spark.streams.active

for i, stream in enumerate(active_streams, 1):
    print(f"{i}. Query: {stream.name}")
    print(f"   ID: {stream.id}")
    print(f"   Status: {stream.status['message']}")
    print(f"   Running: {stream.isActive}")
    print()

# COMMAND ----------

# DBTITLE 1,Stop Specific Query (Optional)
# Uncomment to stop a specific query
# query_gps_raw.stop()
# print("Stopped query: query_gps_raw")

# COMMAND ----------

# DBTITLE 1,Stop All Streaming Queries
# Stop all queries for cleanup
def stop_all_streams():
    """Stop all active streaming queries"""
    for stream in spark.streams.active:
        print(f"Stopping query: {stream.name}")
        stream.stop()
        print(f"  ✓ Stopped")
    print("\nAll streaming queries stopped")

# Uncomment to stop all streams
# stop_all_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Kafka Integration (Advanced)
# MAGIC
# MAGIC For production systems, Kafka is commonly used as the message broker.
# MAGIC Below is a template for Kafka integration (requires Kafka cluster).

# COMMAND ----------

# DBTITLE 1,Kafka Stream Configuration Template
# Template for reading from Kafka (not executed in this demo)
kafka_config_template = """
# Kafka bootstrap servers
kafka_bootstrap_servers = "your-kafka-broker:9092"
kafka_topic = "gps_data"

# Create Kafka stream
kafka_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")  # or "earliest"
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)  # Rate limiting
        .load()
)

# Parse JSON from Kafka value
from pyspark.sql.functions import from_json, col

gps_from_kafka = (
    kafka_stream
    .select(
        col("key").cast("string").alias("message_key"),
        from_json(col("value").cast("string"), gps_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    )
    .select("data.*", "kafka_timestamp", "partition", "offset")
)

# Write to Delta with checkpoint
query = (
    gps_from_kafka
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/kafka_gps")
    .trigger(processingTime="5 seconds")
    .toTable("gps_from_kafka")
)
"""

print("Kafka Integration Template:")
print(kafka_config_template)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC 1. **Schema Definition**
# MAGIC    - Defined structured schemas for GPS and geofence data
# MAGIC    - Used appropriate data types for geospatial fields
# MAGIC
# MAGIC 2. **Auto Loader**
# MAGIC    - Set up file-based streaming with cloudFiles
# MAGIC    - Configured schema inference and evolution
# MAGIC    - Demonstrated incremental file processing
# MAGIC
# MAGIC 3. **Checkpoint Management**
# MAGIC    - Configured checkpoint locations for fault tolerance
# MAGIC    - Examined checkpoint directory structure
# MAGIC    - Understood offset and state tracking
# MAGIC
# MAGIC 4. **Data Enrichment**
# MAGIC    - Added H3 and geohash spatial indexes
# MAGIC    - Implemented data quality validation
# MAGIC    - Calculated processing lag metrics
# MAGIC
# MAGIC 5. **Streaming Patterns**
# MAGIC    - Windowed aggregations with watermarks
# MAGIC    - Deduplication with state management
# MAGIC    - Multiple output sinks from same source
# MAGIC
# MAGIC 6. **Monitoring**
# MAGIC    - Extracted streaming query metrics
# MAGIC    - Monitored data quality issues
# MAGIC    - Tracked processing performance
# MAGIC
# MAGIC ### Best Practices Applied
# MAGIC
# MAGIC ✅ Explicit schema definition for production reliability  
# MAGIC ✅ Separate checkpoint per streaming query  
# MAGIC ✅ Watermarking for late data handling  
# MAGIC ✅ Data validation and enrichment early in pipeline  
# MAGIC ✅ Delta Lake for ACID transactions  
# MAGIC ✅ Monitoring and observability built-in  
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Continue to **Demo 2: Real-Time Geofencing** to learn how to:
# MAGIC - Define dynamic geofence boundaries
# MAGIC - Detect entry/exit events
# MAGIC - Generate real-time alerts
# MAGIC - Implement stateful geofence tracking
