# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02: Reading from Streaming Sources
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook demonstrates how to read streaming data from various sources in Spark Structured Streaming. You'll learn to connect to Auto Loader, Kafka, Delta Lake, and configure schema handling for production workloads.
# MAGIC
# MAGIC **Duration**: 45 minutes
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Read streaming data from cloud files using Auto Loader
# MAGIC - Connect to Kafka topics with authentication
# MAGIC - Stream from Delta Lake tables with Change Data Feed
# MAGIC - Handle schema inference and evolution
# MAGIC - Configure rate limiting and backpressure

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader - Cloud Files Source
# MAGIC
# MAGIC **Auto Loader** is the recommended way to ingest files from cloud storage:
# MAGIC - Automatically processes new files as they arrive
# MAGIC - Handles schema inference and evolution
# MAGIC - Scalable for millions of files
# MAGIC - Supports JSON, CSV, Parquet, Avro, ORC, text, binary

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Setup: Create Sample Data Directory
# Create a directory for our sample streaming data
dbutils.fs.mkdirs("/tmp/streaming_source/json_data/")

# Generate sample JSON data
sample_data = [
    {"sensor_id": 1, "temperature": 72.5, "humidity": 45, "timestamp": "2024-01-10T10:00:00"},
    {"sensor_id": 2, "temperature": 68.3, "humidity": 52, "timestamp": "2024-01-10T10:00:01"},
    {"sensor_id": 3, "temperature": 75.2, "humidity": 48, "timestamp": "2024-01-10T10:00:02"}
]

# Write sample data
import json
for i, data in enumerate(sample_data):
    dbutils.fs.put(f"/tmp/streaming_source/json_data/batch_{i}.json", 
                   json.dumps(data), overwrite=True)

print("✅ Sample data created")

# COMMAND ----------

# DBTITLE 1,Auto Loader: Basic Example
# Read streaming JSON files with Auto Loader
checkpoint_location = "/tmp/checkpoints/autoloader_demo"

autoloader_df = (spark.readStream
    .format("cloudFiles")  # Auto Loader format
    .option("cloudFiles.format", "json")  # Source file format
    .option("cloudFiles.schemaLocation", checkpoint_location)  # Store inferred schema
    .load("/tmp/streaming_source/json_data/"))

# Display schema
print("Inferred Schema:")
autoloader_df.printSchema()

# Write to console for demonstration
query = (autoloader_df
    .writeStream
    .format("console")
    .option("truncate", False)
    .trigger(availableNow=True)  # Process all available data
    .start())

query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Auto Loader: With Explicit Schema
# Define explicit schema for better performance
sensor_schema = StructType([
    StructField("sensor_id", IntegerType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=False),
    StructField("humidity", IntegerType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False)
])

# Read with explicit schema
autoloader_explicit = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(sensor_schema)  # Provide schema explicitly
    .load("/tmp/streaming_source/json_data/"))

print("✅ Auto Loader configured with explicit schema")

# COMMAND ----------

# DBTITLE 1,Auto Loader: Schema Evolution
# Enable schema evolution to handle new columns
autoloader_evolution = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/schema_evolution")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Automatically add new columns
    .option("cloudFiles.inferColumnTypes", "true")  # Infer types automatically
    .load("/tmp/streaming_source/json_data/"))

# Add new file with additional column
new_data = {"sensor_id": 4, "temperature": 70.1, "humidity": 50, 
            "timestamp": "2024-01-10T10:00:03", "pressure": 1013.25}
dbutils.fs.put("/tmp/streaming_source/json_data/batch_new.json", 
               json.dumps(new_data), overwrite=True)

print("✅ Schema evolution enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Kafka Source
# MAGIC
# MAGIC Kafka is the most common source for real-time event streaming.
# MAGIC
# MAGIC ### Connection Parameters
# MAGIC - `kafka.bootstrap.servers`: Kafka broker addresses
# MAGIC - `subscribe`: Topic name(s) to subscribe
# MAGIC - `startingOffsets`: Where to start reading (earliest, latest, specific offsets)

# COMMAND ----------

# DBTITLE 1,Kafka: Basic Configuration
# NOTE: This assumes Kafka is running. For demo purposes, we'll show the configuration.

kafka_config = """
# Reading from Kafka
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "sensor_events")  # Topic name
    .option("startingOffsets", "latest")  # Start from latest messages
    .load())

# Kafka DataFrame has these columns:
# - key: binary
# - value: binary (your actual data)
# - topic: string
# - partition: int
# - offset: long
# - timestamp: timestamp
# - timestampType: int
"""

print("Kafka Configuration Example:")
print(kafka_config)

# COMMAND ----------

# DBTITLE 1,Kafka: Parsing JSON Messages
# Simulated Kafka-like structure for demonstration
from pyspark.sql.functions import from_json, col

# Define the schema for your Kafka message value
kafka_value_schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("event_time", StringType())
])

# Example of parsing Kafka messages (using rate source as simulation)
kafka_simulation = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .select(
        col("timestamp").cast("string").alias("key"),
        to_json(struct(
            (col("value") % 10 + 1).alias("sensor_id"),
            ((col("value") % 30) + 60).cast("double").alias("temperature"),
            ((col("value") % 20) + 40).cast("int").alias("humidity"),
            col("timestamp").cast("string").alias("event_time")
        )).alias("value")
    ))

# Parse JSON from Kafka value
parsed_kafka = (kafka_simulation
    .select(
        col("key"),
        from_json(col("value"), kafka_value_schema).alias("data")
    )
    .select("key", "data.*"))

# Display parsed data
display(parsed_kafka)

# COMMAND ----------

# DBTITLE 1,Kafka: With Authentication (SASL/SSL)
kafka_secure_config = """
# Secure Kafka connection with SASL/SSL
kafka_secure_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9093")
    .option("subscribe", "secure_topic")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", 
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";')
    .option("startingOffsets", "earliest")
    .load())
"""

print("Secure Kafka Configuration:")
print(kafka_secure_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Lake Source with Change Data Feed
# MAGIC
# MAGIC Read changes from Delta tables using Change Data Feed (CDF).
# MAGIC
# MAGIC ### Benefits
# MAGIC - Efficient incremental processing
# MAGIC - Track inserts, updates, deletes
# MAGIC - ACID guarantees
# MAGIC - Time travel capability

# COMMAND ----------

# DBTITLE 1,Setup: Create Delta Table with CDF
# Create a Delta table with Change Data Feed enabled
delta_table_path = "/tmp/delta_source_table"

# Sample data
initial_data = spark.createDataFrame([
    (1, "Alice", 100),
    (2, "Bob", 200),
    (3, "Charlie", 150)
], ["id", "name", "amount"])

# Write as Delta table with CDF enabled
(initial_data.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .save(delta_table_path))

print("✅ Delta table with CDF created")

# COMMAND ----------

# DBTITLE 1,Delta: Stream Changes with CDF
# Read changes from Delta table
delta_stream = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")  # Enable CDF reading
    .option("startingVersion", 0)  # Start from version 0
    .load(delta_table_path))

print("Delta CDF Schema:")
delta_stream.printSchema()

# CDF adds these columns:
# - _change_type: insert, update_preimage, update_postimage, delete
# - _commit_version: Delta version number
# - _commit_timestamp: When change occurred

# COMMAND ----------

# DBTITLE 1,Delta: Process Incremental Changes
# Make some changes to the Delta table
from delta.tables import DeltaTable

# Update
spark.sql(f"""
    UPDATE delta.`{delta_table_path}`
    SET amount = amount + 50
    WHERE id = 1
""")

# Insert
new_record = spark.createDataFrame([(4, "Diana", 175)], ["id", "name", "amount"])
new_record.write.format("delta").mode("append").save(delta_table_path)

# Delete
spark.sql(f"""
    DELETE FROM delta.`{delta_table_path}`
    WHERE id = 3
""")

print("✅ Changes applied to Delta table")

# COMMAND ----------

# DBTITLE 1,Delta: Query Changes
# Read and filter changes
changes_df = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)  # Read from version 1 onwards
    .load(delta_table_path))

# Process changes by type
query = (changes_df
    .select("id", "name", "amount", "_change_type", "_commit_version")
    .writeStream
    .format("console")
    .option("truncate", False)
    .trigger(availableNow=True)
    .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema Handling Strategies
# MAGIC
# MAGIC ### Schema Inference
# MAGIC - Automatic but can be slow
# MAGIC - Good for development
# MAGIC
# MAGIC ### Explicit Schema
# MAGIC - Faster and more predictable
# MAGIC - Recommended for production
# MAGIC
# MAGIC ### Schema Evolution
# MAGIC - Handle schema changes gracefully
# MAGIC - Add new columns automatically

# COMMAND ----------

# DBTITLE 1,Schema Evolution Example
# Create initial schema
v1_data = [{"id": 1, "value": "A"}]
dbutils.fs.put("/tmp/streaming_source/schema_test/v1.json", json.dumps(v1_data[0]), overwrite=True)

# Read with schema evolution
evolving_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/schema_test")
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # Rescue mode
    .option("rescuedDataColumn", "_rescued_data")  # Column for unexpected data
    .load("/tmp/streaming_source/schema_test/"))

print("Initial schema:")
evolving_stream.printSchema()

# Add file with new column
v2_data = [{"id": 2, "value": "B", "new_column": "X"}]
dbutils.fs.put("/tmp/streaming_source/schema_test/v2.json", json.dumps(v2_data[0]), overwrite=True)

print("✅ Schema evolution configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Rate Limiting and Backpressure
# MAGIC
# MAGIC Control the rate of data ingestion to prevent overwhelming downstream systems.

# COMMAND ----------

# DBTITLE 1,Rate Limiting with maxFilesPerTrigger
# Limit number of files processed per trigger
rate_limited = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/rate_limited")
    .option("maxFilesPerTrigger", 10)  # Process max 10 files per trigger
    .load("/tmp/streaming_source/json_data/"))

print("✅ Rate limiting configured: max 10 files per trigger")

# COMMAND ----------

# DBTITLE 1,Kafka Rate Limiting
kafka_rate_limit_config = """
# Limit records processed per partition per trigger
kafka_rate_limited = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .option("maxOffsetsPerTrigger", 10000)  # Max records per trigger
    .option("minPartitions", 4)  # Minimum partitions for parallelism
    .load())
"""

print("Kafka Rate Limiting Example:")
print(kafka_rate_limit_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. File Sources (Alternative to Auto Loader)
# MAGIC
# MAGIC Direct file reading (less recommended than Auto Loader).

# COMMAND ----------

# DBTITLE 1,Direct File Source
# Read files directly (not recommended for production)
direct_file_stream = (spark.readStream
    .format("json")  # Direct format
    .schema(sensor_schema)  # Must provide schema
    .load("/tmp/streaming_source/json_data/"))

print("Direct file streaming configured")
print("Note: Auto Loader is preferred for production use")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Socket Source (Testing Only)
# MAGIC
# MAGIC Socket source for development and testing.

# COMMAND ----------

# DBTITLE 1,Socket Source Example
socket_config = """
# Socket source (for testing only)
socket_stream = (spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load())

# Receives text lines from socket
# Use netcat for testing: nc -lk 9999
"""

print("Socket Source Example:")
print(socket_config)
print("\n⚠️  Socket source is for testing only, not for production")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Hands-On Exercise
# MAGIC
# MAGIC **Task**: Create a streaming pipeline that:
# MAGIC 1. Reads JSON files using Auto Loader
# MAGIC 2. Infers schema automatically
# MAGIC 3. Limits to 5 files per trigger
# MAGIC 4. Writes to a Delta table

# COMMAND ----------

# DBTITLE 1,Exercise Solution
# Generate more sample files
for i in range(10):
    data = {
        "sensor_id": i + 10,
        "temperature": 65 + (i * 0.5),
        "humidity": 40 + i,
        "timestamp": f"2024-01-10T10:{i:02d}:00"
    }
    dbutils.fs.put(f"/tmp/streaming_source/exercise/file_{i}.json", 
                   json.dumps(data), overwrite=True)

# Solution
exercise_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/checkpoints/exercise")
    .option("maxFilesPerTrigger", 5)  # Rate limiting
    .load("/tmp/streaming_source/exercise/"))

# Write to Delta
exercise_query = (exercise_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/checkpoints/exercise_output")
    .outputMode("append")
    .trigger(availableNow=True)
    .start("/tmp/delta_output/exercise"))

exercise_query.awaitTermination()

# Verify results
result_count = spark.read.format("delta").load("/tmp/delta_output/exercise").count()
print(f"✅ Exercise completed. Records processed: {result_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook, you learned:
# MAGIC
# MAGIC ✅ Reading from cloud files with Auto Loader  
# MAGIC ✅ Connecting to Kafka topics  
# MAGIC ✅ Streaming from Delta Lake with Change Data Feed  
# MAGIC ✅ Schema inference, evolution, and explicit schemas  
# MAGIC ✅ Rate limiting and backpressure control  
# MAGIC ✅ Different source types and their use cases  
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Proceed to **Notebook 03: Stateless Transformations** to learn how to process and transform streaming data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Stop all active queries
for query in spark.streams.active:
    print(f"Stopping: {query.name}")
    query.stop()

# Optional: Clean up test data
# dbutils.fs.rm("/tmp/streaming_source", recurse=True)
# dbutils.fs.rm("/tmp/checkpoints", recurse=True)
# dbutils.fs.rm("/tmp/delta_source_table", recurse=True)
# dbutils.fs.rm("/tmp/delta_output", recurse=True)

print("✅ Cleanup completed")
