# Spark Structured Streaming with Python Workshop

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 240   | Estimated duration to complete the lab(s). |
| Level           | 200/300  | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | Databricks Runtime 13.3+, Python 3.10+, PySpark 3.4+          | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  
This workshop provides comprehensive hands-on training in **Spark Structured Streaming**, Apache Spark's scalable and fault-tolerant stream processing engine built on the Spark SQL engine. Using **Python** as the primary language, you'll learn to build production-grade streaming applications that process real-time data from sources like Kafka, cloud storage, and Delta Lake.

Across seven progressive modules, you'll master the complete streaming pipeline lifecycle: from understanding streaming concepts and reading from various sources, to implementing complex transformations with windowing and watermarks, managing state, handling late data, and building end-to-end streaming ETL pipelines with Delta Live Tables.

By the end, you'll be proficient in designing and deploying resilient, scalable streaming applications on Databricks that handle millions of events per second with exactly-once semantics, complete monitoring, and automatic recovery—all using Python and the Structured Streaming API.

## Learning Objectives
- Understand Structured Streaming fundamentals: micro-batching, continuous processing, triggers, and the incremental query model
- Read streaming data from multiple sources (Kafka, cloud storage, Delta Lake, Auto Loader) using Python DataFrames
- Implement stateless transformations (filter, select, map) and stateful operations (aggregations, joins, deduplication) on streaming data
- Apply windowing techniques (tumbling, sliding, session windows) and watermarking to handle event-time processing and late-arriving data
- Build streaming joins (stream-stream, stream-static) and implement exactly-once semantics with idempotent writes
- Write streaming results to sinks (Delta Lake, Kafka, memory, console) with appropriate output modes (append, update, complete)
- Monitor streaming queries using query progress, metrics, and Databricks dashboards for performance optimization
- Handle failures, checkpointing, and state recovery to build fault-tolerant production streaming applications

## Requirements & Prerequisites  
Before starting this workshop, ensure you have:  
- Access to a **Databricks workspace** with Unity Catalog enabled
- An available **All-purpose cluster** or **Serverless compute** (DBR 13.3+ recommended)
- **CREATE TABLE** and **READ/WRITE** permissions in Unity Catalog
- **Intermediate Python skills** - Comfortable with functions, lambdas, list comprehensions, and working with DataFrames
- **Intermediate PySpark knowledge** - Familiar with DataFrame API, transformations, and actions
- **Basic SQL skills** - Understanding of SELECT, JOIN, GROUP BY, and aggregate functions
- **Understanding of streaming concepts** - Familiarity with event-driven architectures, data streams, and real-time processing
- **Basic knowledge of Delta Lake** - Understanding of ACID transactions and table formats (helpful but not required)

## Contents  
This repository includes:
- **1 Lecture - Structured Streaming Fundamentals and Architecture** notebook
- **2 Lab - Reading from Streaming Sources (Auto Loader, Kafka, Delta)** notebook
- **3 Lab - Stateless Transformations and Basic Streaming Queries** notebook
- **4 Lab - Windowing and Watermarks for Event-Time Processing** notebook
- **5 Lab - Stateful Operations: Aggregations, Joins, and Deduplication** notebook
- **6 Lab - Writing to Sinks and Managing Output Modes** notebook
- **7 Lab - Monitoring, Debugging, and Production Best Practices** notebook
- **8 Demo - Building a Complete Streaming ETL Pipeline** notebook
- Sample streaming data generators (Python scripts)
- Images and architecture diagrams
- Streaming troubleshooting guide

## Getting Started
1. Complete all notebooks in order.
   - **NOTE:** Completion of **Lab - Reading from Streaming Sources** is required before starting Labs 3-7.
2. Follow the notebook instructions step by step.
3. Execute all code cells and observe the streaming outputs.
4. Use provided Python scripts to generate sample streaming data.

## Workshop Structure

### Module 1: Structured Streaming Fundamentals (30 minutes)
**Lecture notebook covering:**
- Streaming vs. batch processing paradigms
- Structured Streaming architecture: micro-batching and continuous processing
- Key concepts: sources, transformations, sinks, triggers, and checkpoints
- Understanding the incremental query model and fault tolerance
- When to use streaming vs. batch processing

**Topics:**
```python
# Core concepts demonstrated
- Input sources and output sinks
- DataFrame operations on streaming data
- Triggers: fixed interval, once, continuous
- Checkpointing and state management
- Exactly-once processing guarantees
```

### Module 2: Reading from Streaming Sources (45 minutes)
**Hands-on lab using Python:**
- Configure and read from Auto Loader (cloud files)
- Connect to Kafka topics with authentication
- Stream from Delta Lake tables with change data feed
- Handle schema inference and evolution
- Configure rate limits and backpressure

**Python code examples:**
```python
# Reading from cloud storage with Auto Loader
streaming_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load("/path/to/data"))

# Reading from Kafka
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "topic_name")
    .load())

# Reading from Delta Lake
delta_stream = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table("catalog.schema.table_name"))
```

**Expected Outcome:** Successfully reading streaming data from multiple sources

### Module 3: Stateless Transformations (40 minutes)
**Hands-on lab:**
- Apply filter, select, and drop operations on streams
- Parse JSON and nested structures from Kafka
- Use map and flatMap with Python UDFs
- Explode arrays and work with complex types
- Chain multiple transformations efficiently

**Python examples:**
```python
# Parsing and transforming streaming JSON
from pyspark.sql.functions import col, from_json, explode

parsed_df = (streaming_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .filter(col("temperature") > 75)
    .withColumn("temp_fahrenheit", col("temperature") * 9/5 + 32))

# Using Python UDFs on streams
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def classify_severity(temp):
    if temp > 100: return "Critical"
    elif temp > 80: return "Warning"
    else: return "Normal"

enriched_df = parsed_df.withColumn("severity", classify_severity(col("temperature")))
```

**Expected Outcome:** Transform streaming data with Python logic

### Module 4: Windowing and Watermarks (50 minutes)
**Hands-on lab:**
- Implement tumbling windows for time-based aggregations
- Create sliding windows for moving averages
- Apply session windows for user activity tracking
- Configure watermarks to handle late-arriving data
- Balance latency vs. completeness with watermark tuning

**Python examples:**
```python
from pyspark.sql.functions import window, col, count, avg

# Tumbling window aggregation
windowed_counts = (streaming_df
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("device_id")
    )
    .agg(
        count("*").alias("event_count"),
        avg("temperature").alias("avg_temperature")
    ))

# Sliding window for moving averages
sliding_avg = (streaming_df
    .withWatermark("timestamp", "1 hour")
    .groupBy(
        window(col("timestamp"), "15 minutes", "5 minutes"),
        col("sensor_id")
    )
    .agg(avg("value").alias("moving_avg")))

# Session window for user sessions
session_analysis = (clickstream_df
    .withWatermark("event_time", "30 minutes")
    .groupBy(
        col("user_id"),
        session_window(col("event_time"), "10 minutes")
    )
    .count())
```

**Expected Outcome:** Time-based aggregations with proper late data handling

### Module 5: Stateful Operations (50 minutes)
**Hands-on lab:**
- Perform streaming aggregations (count, sum, avg, min, max)
- Implement streaming-to-streaming joins
- Create streaming-to-static joins for enrichment
- Deduplicate streaming events with watermarks
- Manage state size and performance

**Python examples:**
```python
# Streaming aggregation
aggregated = (streaming_df
    .groupBy("category", "region")
    .agg(
        count("*").alias("total_events"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction")
    ))

# Stream-to-stream join
enriched_stream = (orders_stream
    .withWatermark("order_time", "10 minutes")
    .join(
        shipments_stream.withWatermark("ship_time", "15 minutes"),
        expr("""
            order_id = shipment_order_id AND
            ship_time >= order_time AND
            ship_time <= order_time + interval 1 hour
        """)
    ))

# Deduplication
deduplicated = (streaming_df
    .withWatermark("timestamp", "1 hour")
    .dropDuplicates(["event_id", "user_id"]))

# Stream-to-static join for enrichment
enriched = (streaming_df
    .join(
        spark.table("catalog.schema.product_catalog"),
        "product_id",
        "left"
    ))
```

**Expected Outcome:** Complex stateful processing with managed state

### Module 6: Writing to Sinks (40 minutes)
**Hands-on lab:**
- Write to Delta Lake with append, update, and complete modes
- Configure foreachBatch for custom sink logic
- Write to Kafka for downstream processing
- Use console and memory sinks for debugging
- Implement exactly-once semantics with idempotent writes

**Python examples:**
```python
# Writing to Delta Lake
query = (streaming_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/stream1")
    .trigger(processingTime="10 seconds")
    .table("catalog.schema.streaming_output"))

# foreachBatch for custom logic
def process_batch(batch_df, batch_id):
    # Custom processing
    batch_df.write.format("delta").mode("append").saveAsTable("target_table")
    
    # Send notifications, call APIs, etc.
    if batch_df.count() > 1000:
        send_alert("High volume detected")

query = (streaming_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/checkpoints/custom")
    .start())

# Writing to Kafka
kafka_output = (processed_df
    .selectExpr("event_id as key", "to_json(struct(*)) as value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "output_topic")
    .option("checkpointLocation", "/checkpoints/kafka_out")
    .start())

# Console sink for debugging
debug_query = (streaming_df
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .start())
```

**Expected Outcome:** Multiple sink configurations working correctly

### Module 7: Monitoring and Production Best Practices (35 minutes)
**Hands-on lab:**
- Query streaming query progress and status
- Monitor processing rates and latency metrics
- Debug common streaming errors and exceptions
- Implement proper checkpoint management
- Configure auto-scaling and resource optimization
- Set up alerting for streaming failures

**Python monitoring code:**
```python
# Monitor query progress
query = streaming_df.writeStream.start()

# Access real-time metrics
progress = query.lastProgress
print(f"Input rows: {progress['numInputRows']}")
print(f"Processing rate: {progress['processedRowsPerSecond']}")
print(f"Batch duration: {progress['batchDuration']} ms")

# Check query status
status = query.status
print(f"Query is active: {query.isActive}")
print(f"Query ID: {query.id}")

# Stream monitoring function
def monitor_stream(query, interval=10):
    import time
    while query.isActive:
        progress = query.lastProgress
        if progress:
            print(f"""
            Batch: {progress['batchId']}
            Input rows: {progress['numInputRows']}
            Processing rate: {progress['processedRowsPerSecond']:.2f} rows/sec
            Latency: {progress['batchDuration']} ms
            """)
        time.sleep(interval)

# Handle failures gracefully
from pyspark.sql.streaming import StreamingQueryException

try:
    query.awaitTermination()
except StreamingQueryException as e:
    print(f"Query failed: {e.message}")
    # Implement retry logic or alerting
```

**Expected Outcome:** Comprehensive monitoring and debugging capabilities

### Module 8: Complete Streaming ETL Pipeline (30 minutes)
**Demo notebook:**
- End-to-end clickstream analytics pipeline
- Multi-stage transformations with Bronze-Silver-Gold architecture
- Streaming aggregations with multiple windows
- Real-time dashboard integration
- Production deployment patterns

**Complete pipeline example:**
```python
# Bronze: Raw ingestion
bronze_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/raw/clickstream")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/bronze")
    .table("clickstream_bronze"))

# Silver: Cleaned and enriched
silver_stream = (spark.readStream
    .table("clickstream_bronze")
    .filter(col("event_type").isNotNull())
    .withColumn("parsed_timestamp", to_timestamp(col("timestamp")))
    .join(user_dim, "user_id")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/silver")
    .outputMode("append")
    .table("clickstream_silver"))

# Gold: Aggregated metrics
gold_stream = (spark.readStream
    .table("clickstream_silver")
    .withWatermark("parsed_timestamp", "30 minutes")
    .groupBy(
        window("parsed_timestamp", "5 minutes"),
        "page_category",
        "user_segment"
    )
    .agg(
        count("*").alias("page_views"),
        countDistinct("user_id").alias("unique_users"),
        avg("time_on_page").alias("avg_time_on_page")
    )
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/gold")
    .outputMode("complete")
    .table("clickstream_metrics"))
```

**Expected Outcome:** Production-ready streaming pipeline

## Key Concepts Covered

### Streaming Fundamentals
- **Micro-batching:** Process data in small batches (default mode)
- **Continuous processing:** Low-latency processing with ~1ms latency
- **Triggers:** Control when batches are processed
- **Checkpointing:** Fault tolerance and exactly-once guarantees
- **Watermarks:** Handle late-arriving data in event-time processing

### State Management
- **Stateless operations:** Simple transformations without state
- **Stateful aggregations:** Count, sum, average with maintained state
- **Stream-stream joins:** Join two streaming sources
- **Deduplication:** Remove duplicate events efficiently
- **State store:** RocksDB-backed persistent state

### Output Modes
- **Append:** Only new rows (for aggregations with watermark)
- **Update:** New and updated rows (for aggregations)
- **Complete:** Entire result table (for simple aggregations)

### Performance Optimization
- Partition streaming data appropriately
- Use column pruning and filter pushdown
- Configure shuffle partitions for streaming workloads
- Optimize checkpoint frequency
- Monitor and tune state store size

## Best Practices

### Development
- Start with console sink for debugging before production sinks
- Use display() in Databricks for interactive streaming development
- Test watermark settings with historical data first
- Implement schema validation early in the pipeline
- Use structured schemas instead of schema inference for production

### Production Deployment
- Always specify checkpoint locations
- Use Delta Lake for sinks to get ACID guarantees
- Configure appropriate trigger intervals (balance latency vs. cost)
- Implement monitoring and alerting for stream failures
- Plan for checkpoint cleanup and maintenance
- Version control your streaming code and configurations

### Error Handling
```python
# Implement robust error handling
query = (streaming_df
    .writeStream
    .foreachBatch(lambda df, batch_id: process_with_retry(df, batch_id))
    .option("checkpointLocation", checkpoint_path)
    .start())

def process_with_retry(df, batch_id, max_retries=3):
    for attempt in range(max_retries):
        try:
            df.write.format("delta").mode("append").saveAsTable("target")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                log_error(f"Batch {batch_id} failed after {max_retries} attempts: {e}")
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
```

### Resource Management
- Configure cluster autoscaling for variable loads
- Use Photon for improved streaming performance
- Set appropriate shuffle partitions: `spark.sql.shuffle.partitions`
- Monitor memory usage and adjust driver/executor sizes
- Clean up old checkpoints periodically

## Troubleshooting Guide

Common issues and solutions:

### Query Performance Issues
- **Symptom:** Increasing batch processing time
- **Solutions:**
  - Check if state size is growing unbounded (add watermarks)
  - Optimize shuffle partitions configuration
  - Review and optimize expensive operations (UDFs, complex joins)
  - Enable Adaptive Query Execution (AQE)

### Checkpoint Issues
- **Symptom:** Checkpoint incompatibility errors
- **Solutions:**
  - Don't change query logic drastically (breaks checkpoint)
  - For major changes, use new checkpoint location
  - Clean up old checkpoints regularly
  - Back up checkpoints before major upgrades

### Late Data Handling
- **Symptom:** Missing data or incomplete aggregations
- **Solutions:**
  - Configure watermarks appropriately for your SLA
  - Balance between completeness and latency
  - Monitor dropped late data in metrics
  - Consider using longer watermark for critical data

### Memory Issues
- **Symptom:** OOM errors or executor crashes
- **Solutions:**
  - Reduce state size with appropriate watermarks
  - Increase executor memory
  - Optimize state store configurations
  - Use deduplication with watermarks to limit state

### Connection Failures
- **Symptom:** Kafka/source connection errors
- **Solutions:**
  - Implement retry logic with exponential backoff
  - Verify network connectivity and firewall rules
  - Check authentication credentials
  - Configure appropriate timeouts

## Python Code Snippets Library

### Common Patterns
```python
# Pattern 1: Parse JSON from Kafka
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", StringType()),
    StructField("value", IntegerType()),
    StructField("timestamp", StringType())
])

parsed = (kafka_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*"))

# Pattern 2: Running aggregation with watermark
running_agg = (stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(window("event_time", "5 minutes"), "category")
    .agg(
        count("*").alias("count"),
        sum("amount").alias("total"),
        avg("amount").alias("average")
    ))

# Pattern 3: Deduplication
deduped = (stream
    .withWatermark("timestamp", "2 hours")
    .dropDuplicates(["event_id"]))

# Pattern 4: Multiple output streams from one source
def fan_out_processing(source_stream):
    # Cache the source to avoid reading twice
    source = source_stream.cache()
    
    # Write to multiple destinations
    query1 = source.filter(col("type") == "A").writeStream.table("table_a")
    query2 = source.filter(col("type") == "B").writeStream.table("table_b")
    
    return query1, query2
```

## Additional Resources
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks Structured Streaming Best Practices](https://docs.databricks.com/structured-streaming/index.html)
- [PySpark Structured Streaming API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/index.html)
- [Delta Lake Streaming](https://docs.delta.io/latest/delta-streaming.html)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)

## Support
For questions or issues with this workshop:
- Open an issue in this repository
- Contact: Ajit Kalura
- Databricks Community Forums: [community.databricks.com](https://community.databricks.com/)
- Stack Overflow: [spark-structured-streaming tag](https://stackoverflow.com/questions/tagged/spark-structured-streaming)

---

**Ready to master real-time data processing with Python and Spark?** Start with Module 1 to understand Structured Streaming fundamentals! 🚀
