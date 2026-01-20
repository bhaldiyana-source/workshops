# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01: Structured Streaming Fundamentals
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook introduces the fundamental concepts of **Spark Structured Streaming**, Apache Spark's scalable and fault-tolerant stream processing engine. You'll learn the core architecture, concepts, and APIs that form the foundation for building production streaming applications.
# MAGIC
# MAGIC **Duration**: 30 minutes
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand the differences between batch and streaming processing
# MAGIC - Learn Structured Streaming architecture and the incremental query model
# MAGIC - Explore sources, transformations, and sinks
# MAGIC - Understand triggers and checkpoint mechanisms
# MAGIC - Grasp fault tolerance and exactly-once semantics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Streaming vs. Batch Processing
# MAGIC
# MAGIC ### Batch Processing
# MAGIC - Processes finite datasets
# MAGIC - Scheduled execution (hourly, daily)
# MAGIC - High latency (minutes to hours)
# MAGIC - Simple to reason about
# MAGIC
# MAGIC ### Stream Processing
# MAGIC - Processes unbounded datasets (continuous data)
# MAGIC - Continuous execution
# MAGIC - Low latency (seconds to milliseconds)
# MAGIC - Complex state management
# MAGIC
# MAGIC ### When to Use Streaming?
# MAGIC - Real-time dashboards and monitoring
# MAGIC - Fraud detection and alerting
# MAGIC - IoT sensor data processing
# MAGIC - Clickstream analytics
# MAGIC - Log aggregation and analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Structured Streaming Architecture
# MAGIC
# MAGIC ### Key Components
# MAGIC
# MAGIC 1. **Input Source**: Where data comes from (Kafka, files, sockets, Delta Lake)
# MAGIC 2. **Query**: Transformations applied to streaming data
# MAGIC 3. **Result Table**: Logical table updated with each trigger
# MAGIC 4. **Output Sink**: Where results are written (Delta Lake, Kafka, console)
# MAGIC 5. **Checkpoint**: Fault tolerance mechanism
# MAGIC
# MAGIC ### Incremental Query Model
# MAGIC
# MAGIC Structured Streaming treats streaming data as an unbounded table that continuously grows. New data = new rows appended to the input table.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Display Spark version
print(f"Spark Version: {spark.version}")
print(f"Structured Streaming is built on Spark SQL engine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating Your First Streaming DataFrame
# MAGIC
# MAGIC Let's create a simple streaming DataFrame from a rate source (generates data automatically).

# COMMAND ----------

# DBTITLE 1,Simple Streaming DataFrame
# Rate source generates rows with timestamp and value columns
streaming_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)  # Generate 5 rows per second
    .load())

# Check if it's a streaming DataFrame
print(f"Is Streaming: {streaming_df.isStreaming}")
print(f"\nSchema:")
streaming_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Understanding Triggers
# MAGIC
# MAGIC Triggers control when the streaming query should process data.
# MAGIC
# MAGIC ### Trigger Types
# MAGIC
# MAGIC 1. **Default (Micro-batch)**: Process as fast as possible
# MAGIC 2. **Fixed Interval**: Process at specified intervals (e.g., every 10 seconds)
# MAGIC 3. **Once**: Process all available data once, then stop
# MAGIC 4. **Continuous**: Ultra-low latency (~1ms) - experimental

# COMMAND ----------

# DBTITLE 1,Example: Fixed Interval Trigger
# This will process data every 5 seconds
streaming_query = (streaming_df
    .writeStream
    .format("memory")  # Write to in-memory table
    .queryName("rate_demo")
    .trigger(processingTime="5 seconds")  # Process every 5 seconds
    .start())

# Let it run for a moment
import time
time.sleep(10)

# Query the in-memory table
result_df = spark.sql("SELECT * FROM rate_demo ORDER BY timestamp DESC LIMIT 10")
display(result_df)

# Stop the query
streaming_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Output Modes
# MAGIC
# MAGIC Output modes determine what gets written to the sink.
# MAGIC
# MAGIC ### Three Output Modes
# MAGIC
# MAGIC 1. **Append** (default): Only new rows since last trigger
# MAGIC    - Use for: Stateless queries, aggregations with watermark
# MAGIC
# MAGIC 2. **Update**: Only updated rows since last trigger
# MAGIC    - Use for: Aggregations with groupBy
# MAGIC
# MAGIC 3. **Complete**: Entire result table (all rows)
# MAGIC    - Use for: Simple aggregations without groupBy on many dimensions

# COMMAND ----------

# DBTITLE 1,Append Mode Example
append_query = (streaming_df
    .select(
        col("timestamp"),
        col("value"),
        (col("value") * 2).alias("doubled")
    )
    .writeStream
    .format("memory")
    .queryName("append_example")
    .outputMode("append")  # Only new rows
    .trigger(processingTime="3 seconds")
    .start())

time.sleep(8)
display(spark.sql("SELECT * FROM append_example LIMIT 10"))
append_query.stop()

# COMMAND ----------

# DBTITLE 1,Update Mode Example
# Aggregation query with update mode
update_query = (streaming_df
    .groupBy(window(col("timestamp"), "10 seconds"))
    .agg(
        count("*").alias("count"),
        avg("value").alias("avg_value")
    )
    .writeStream
    .format("memory")
    .queryName("update_example")
    .outputMode("update")  # Only updated aggregations
    .trigger(processingTime="5 seconds")
    .start())

time.sleep(15)
display(spark.sql("SELECT * FROM update_example ORDER BY window"))
update_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Checkpointing and Fault Tolerance
# MAGIC
# MAGIC ### What is Checkpointing?
# MAGIC
# MAGIC Checkpointing is the mechanism that provides fault tolerance in Structured Streaming:
# MAGIC - Stores query metadata and progress
# MAGIC - Enables exactly-once semantics
# MAGIC - Allows recovery from failures
# MAGIC - Tracks processed offsets
# MAGIC
# MAGIC ### Checkpoint Location
# MAGIC - Always specify a checkpoint location for production
# MAGIC - Use DBFS or cloud storage (S3, ADLS, GCS)
# MAGIC - Don't change query logic drastically (breaks checkpoint compatibility)

# COMMAND ----------

# DBTITLE 1,Streaming with Checkpoint
# Setup checkpoint location
checkpoint_path = "/tmp/streaming_checkpoint_demo"

# Create streaming query with checkpoint
checkpointed_query = (streaming_df
    .select(
        col("timestamp"),
        col("value"),
        (col("value") % 10).alias("value_mod_10")
    )
    .writeStream
    .format("memory")
    .queryName("checkpointed_demo")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="5 seconds")
    .start())

time.sleep(10)

# Check query status
print(f"Query ID: {checkpointed_query.id}")
print(f"Status: {checkpointed_query.status}")
print(f"Is Active: {checkpointed_query.isActive}")

checkpointed_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Monitoring Streaming Queries
# MAGIC
# MAGIC ### Query Progress
# MAGIC
# MAGIC Structured Streaming provides detailed progress information after each micro-batch.

# COMMAND ----------

# DBTITLE 1,Query Progress Monitoring
monitored_query = (streaming_df
    .writeStream
    .format("memory")
    .queryName("monitored_query")
    .option("checkpointLocation", "/tmp/monitored_checkpoint")
    .trigger(processingTime="3 seconds")
    .start())

# Let it process a few batches
time.sleep(10)

# Get progress information
last_progress = monitored_query.lastProgress

if last_progress:
    print("=== Query Progress ===")
    print(f"Batch ID: {last_progress['batchId']}")
    print(f"Input Rows: {last_progress['numInputRows']}")
    print(f"Processing Rate: {last_progress.get('processedRowsPerSecond', 'N/A')} rows/sec")
    print(f"Batch Duration: {last_progress['batchDuration']} ms")
    print(f"Trigger: {last_progress['trigger']}")
    
monitored_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exactly-Once Semantics
# MAGIC
# MAGIC Structured Streaming provides **exactly-once processing guarantees** through:
# MAGIC
# MAGIC 1. **Idempotent Sinks**: Write operations can be repeated safely
# MAGIC 2. **Checkpointing**: Tracks processed data
# MAGIC 3. **Transactional Writes**: Delta Lake provides ACID transactions
# MAGIC
# MAGIC ### Key Points
# MAGIC - Each input record is processed exactly once
# MAGIC - No duplicates, no data loss
# MAGIC - Requires proper checkpoint and sink configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Stateless vs. Stateful Operations
# MAGIC
# MAGIC ### Stateless Operations
# MAGIC - Each record processed independently
# MAGIC - Examples: filter, select, map, flatMap
# MAGIC - No memory of previous records
# MAGIC - Simple and efficient
# MAGIC
# MAGIC ### Stateful Operations
# MAGIC - Maintain state across records
# MAGIC - Examples: aggregations, joins, deduplication
# MAGIC - Require state management
# MAGIC - More complex but powerful

# COMMAND ----------

# DBTITLE 1,Stateless Transformation Example
# Simple filter and transformation (stateless)
stateless_query = (streaming_df
    .filter(col("value") > 500)  # Stateless filter
    .withColumn("value_squared", col("value") * col("value"))  # Stateless transformation
    .writeStream
    .format("memory")
    .queryName("stateless_example")
    .trigger(processingTime="5 seconds")
    .start())

time.sleep(10)
display(spark.sql("SELECT * FROM stateless_example LIMIT 10"))
stateless_query.stop()

# COMMAND ----------

# DBTITLE 1,Stateful Aggregation Example
# Aggregation requires maintaining state
stateful_query = (streaming_df
    .groupBy(
        window(col("timestamp"), "20 seconds"),
        (col("value") / 100).cast("int").alias("value_bucket")
    )
    .agg(
        count("*").alias("count"),
        sum("value").alias("sum_value"),
        avg("value").alias("avg_value")
    )
    .writeStream
    .format("memory")
    .queryName("stateful_example")
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start())

time.sleep(15)
display(spark.sql("SELECT * FROM stateful_example ORDER BY window"))
stateful_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Common Streaming Sources
# MAGIC
# MAGIC Structured Streaming supports various sources:
# MAGIC
# MAGIC 1. **File Sources**
# MAGIC    - JSON, CSV, Parquet, ORC, text
# MAGIC    - Auto Loader (cloudFiles) - recommended
# MAGIC
# MAGIC 2. **Kafka**
# MAGIC    - Most common for event streaming
# MAGIC    - High throughput, distributed
# MAGIC
# MAGIC 3. **Delta Lake**
# MAGIC    - Change Data Feed (CDF)
# MAGIC    - ACID transactions
# MAGIC
# MAGIC 4. **Socket** (for testing)
# MAGIC 5. **Rate Source** (for testing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Best Practices
# MAGIC
# MAGIC ### Development
# MAGIC - Start with small datasets and console sink
# MAGIC - Use `.explain()` to understand query plans
# MAGIC - Test with historical data before going live
# MAGIC - Monitor query progress regularly
# MAGIC
# MAGIC ### Production
# MAGIC - Always set checkpoint location
# MAGIC - Use appropriate trigger intervals
# MAGIC - Implement error handling and alerting
# MAGIC - Monitor cluster resources
# MAGIC - Use Delta Lake for sinks (ACID guarantees)
# MAGIC
# MAGIC ### Performance
# MAGIC - Partition appropriately
# MAGIC - Use Photon for better performance
# MAGIC - Configure shuffle partitions correctly
# MAGIC - Avoid expensive UDFs in hot path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Hands-On Exercise
# MAGIC
# MAGIC Create a streaming query that:
# MAGIC 1. Reads from rate source (10 rows/sec)
# MAGIC 2. Filters values greater than 500
# MAGIC 3. Adds a column indicating if value is even or odd
# MAGIC 4. Writes to memory sink with 5-second trigger
# MAGIC 5. Query the results

# COMMAND ----------

# DBTITLE 1,Exercise Solution
# Create streaming DataFrame
exercise_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load())

# Apply transformations
transformed_df = (exercise_df
    .filter(col("value") > 500)
    .withColumn("is_even", 
                when(col("value") % 2 == 0, "even").otherwise("odd")))

# Write stream
exercise_query = (transformed_df
    .writeStream
    .format("memory")
    .queryName("exercise_results")
    .trigger(processingTime="5 seconds")
    .start())

# Wait and query
time.sleep(12)
result = spark.sql("""
    SELECT 
        is_even,
        COUNT(*) as count,
        MIN(value) as min_value,
        MAX(value) as max_value,
        AVG(value) as avg_value
    FROM exercise_results
    GROUP BY is_even
""")
display(result)

exercise_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook, you learned:
# MAGIC
# MAGIC ✅ Differences between batch and streaming processing  
# MAGIC ✅ Structured Streaming architecture and incremental query model  
# MAGIC ✅ How to create streaming DataFrames  
# MAGIC ✅ Trigger types and output modes  
# MAGIC ✅ Checkpointing for fault tolerance  
# MAGIC ✅ Monitoring streaming queries  
# MAGIC ✅ Exactly-once semantics  
# MAGIC ✅ Stateless vs stateful operations  
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Proceed to **Notebook 02: Reading from Streaming Sources** to learn how to connect to real data sources like Kafka, cloud files, and Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Stop any remaining queries and clean up checkpoints.

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping query: {query.name}")
    query.stop()

print("✅ All streaming queries stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC - [Databricks Structured Streaming](https://docs.databricks.com/structured-streaming/index.html)
# MAGIC - [PySpark Streaming API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/index.html)
