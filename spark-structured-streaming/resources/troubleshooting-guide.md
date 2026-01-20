# Spark Structured Streaming Troubleshooting Guide

## Table of Contents
1. [Query Performance Issues](#query-performance-issues)
2. [Checkpoint Problems](#checkpoint-problems)
3. [Late Data and Watermark Issues](#late-data-and-watermark-issues)
4. [Memory Problems](#memory-problems)
5. [Source Connection Issues](#source-connection-issues)
6. [State Management Issues](#state-management-issues)
7. [Output Sink Problems](#output-sink-problems)
8. [Common Error Messages](#common-error-messages)

---

## Query Performance Issues

### Symptom: Increasing Batch Processing Time

**Description**: Batch processing time keeps increasing, causing lag to grow over time.

**Possible Causes**:
- Unbounded state growth (no watermarks)
- Growing shuffle operations
- Inefficient transformations
- Resource constraints

**Solutions**:

1. **Add watermarks to limit state**:
```python
# Before (unbounded state)
df.groupBy("user_id").agg(count("*"))

# After (bounded state with watermark)
df.withWatermark("timestamp", "1 hour") \
  .groupBy("user_id") \
  .agg(count("*"))
```

2. **Optimize shuffle partitions**:
```python
# Configure based on data volume
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Adjust as needed
```

3. **Enable Adaptive Query Execution**:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

4. **Review expensive operations**:
- Minimize UDF usage (use built-in functions)
- Avoid collect() on streaming DataFrames
- Optimize complex joins

### Symptom: Low Throughput

**Description**: Processing rate is much lower than expected.

**Solutions**:

1. **Increase parallelism**:
```python
# Repartition input
df = df.repartition(100)

# Increase source parallelism
.option("maxFilesPerTrigger", 50)  # For file sources
.option("minPartitions", 20)       # For Kafka
```

2. **Use Photon** (Databricks):
- Enable Photon on cluster configuration
- 3-10x faster for many workloads

3. **Optimize trigger interval**:
```python
# Too frequent (more overhead)
.trigger(processingTime="1 second")

# Better balance
.trigger(processingTime="10 seconds")
```

---

## Checkpoint Problems

### Symptom: Checkpoint Incompatibility Error

**Error Message**:
```
org.apache.spark.sql.AnalysisException: Checkpoint schema is incompatible with current schema
```

**Causes**:
- Query logic changed significantly
- Schema evolved incompatibly
- Different Spark version

**Solutions**:

1. **Start with new checkpoint** (for development):
```python
# Use a new checkpoint location
.option("checkpointLocation", "/checkpoints/my_query_v2")
```

2. **Back up and restore** (for production):
```bash
# Back up existing checkpoint
dbutils.fs.cp("/checkpoints/prod", "/checkpoints/prod_backup", recurse=True)

# If needed, restore
dbutils.fs.rm("/checkpoints/prod", recurse=True)
dbutils.fs.cp("/checkpoints/prod_backup", "/checkpoints/prod", recurse=True)
```

3. **Avoid incompatible changes**:
- Don't drastically change aggregation keys
- Don't change window durations
- Maintain consistent column names

### Symptom: Checkpoint Directory Growing Too Large

**Solutions**:

1. **Enable checkpoint cleanup**:
```python
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "10")
```

2. **Manually clean old files**:
```bash
# Keep only recent checkpoints
# BE CAREFUL - only do this when query is stopped
dbutils.fs.rm("/checkpoints/my_query/offsets/999", recurse=True)  # Old batches
```

3. **Monitor checkpoint size**:
```python
def check_checkpoint_size(path):
    files = dbutils.fs.ls(path)
    total_size = sum(f.size for f in files if not f.isDir())
    print(f"Checkpoint size: {total_size / 1e9:.2f} GB")
```

---

## Late Data and Watermark Issues

### Symptom: Missing or Incomplete Aggregations

**Description**: Aggregations seem to be missing data or showing incomplete results.

**Causes**:
- Watermark too aggressive (dropping valid late data)
- No watermark configured
- Clock skew in event times

**Solutions**:

1. **Configure appropriate watermark**:
```python
# Too aggressive (drops data within 5 minutes)
df.withWatermark("timestamp", "5 minutes")

# Better (allows 1 hour of late data)
df.withWatermark("timestamp", "1 hour")
```

2. **Monitor dropped late data**:
```python
progress = query.lastProgress
if progress and 'watermark' in progress:
    print(f"Current watermark: {progress['watermark']}")
    # Check for late data in metrics
```

3. **Balance latency vs. completeness**:
```
Shorter watermark = Lower latency, more dropped data
Longer watermark = Higher latency, more complete data
```

### Symptom: Watermark Not Advancing

**Causes**:
- No new data arriving
- Event timestamps in the future
- Incorrect timestamp column

**Solutions**:

1. **Verify timestamp column**:
```python
# Check timestamp distribution
df.select(
    min("timestamp").alias("min_ts"),
    max("timestamp").alias("max_ts"),
    current_timestamp().alias("now")
).show()
```

2. **Handle future timestamps**:
```python
# Cap timestamps to current time
from pyspark.sql.functions import least, current_timestamp

df = df.withColumn("timestamp", 
    least(col("timestamp"), current_timestamp()))
```

---

## Memory Problems

### Symptom: Out of Memory (OOM) Errors

**Error Message**:
```
java.lang.OutOfMemoryError: Java heap space
```

**Causes**:
- Unbounded state growth
- Large batch sizes
- Insufficient executor memory
- Memory leaks in UDFs

**Solutions**:

1. **Limit state with watermarks**:
```python
# Prevent unbounded state
df.withWatermark("timestamp", "2 hours") \
  .dropDuplicates(["event_id"])
```

2. **Increase executor memory**:
```python
# Cluster configuration
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")
```

3. **Reduce batch size**:
```python
# Limit data per trigger
.option("maxFilesPerTrigger", 10)  # For files
.option("maxOffsetsPerTrigger", 10000)  # For Kafka
```

4. **Use approximate aggregations**:
```python
# Instead of exact count distinct
from pyspark.sql.functions import approx_count_distinct

df.groupBy("user_id") \
  .agg(approx_count_distinct("event_id").alias("approx_count"))
```

### Symptom: Executor Crashes

**Solutions**:

1. **Check for data skew**:
```python
# Identify skewed keys
df.groupBy("key").count().orderBy(desc("count")).show()

# Use salting for skewed keys
df.withColumn("salt", (rand() * 10).cast("int")) \
  .groupBy("key", "salt") \
  .agg(...)
```

2. **Optimize memory overhead**:
```python
spark.conf.set("spark.executor.memoryOverhead", "1g")
```

---

## Source Connection Issues

### Symptom: Kafka Connection Failures

**Error Messages**:
```
Failed to resolve brokers
Connection refused
Authentication failed
```

**Solutions**:

1. **Verify connectivity**:
```python
# Test basic connection
try:
    test_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "test_topic") \
        .load()
    print("✅ Connection successful")
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

2. **Check authentication**:
```python
# Correct SASL configuration
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.mechanism", "PLAIN")
.option("kafka.sasl.jaas.config", 
    'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";')
```

3. **Configure timeouts**:
```python
.option("kafka.request.timeout.ms", "60000")
.option("kafka.session.timeout.ms", "30000")
```

### Symptom: Auto Loader Not Detecting Files

**Solutions**:

1. **Check file paths**:
```python
# Verify files exist
files = dbutils.fs.ls("/path/to/data/")
print(f"Found {len(files)} files")
```

2. **Configure schema location**:
```python
# Ensure schema location is writable
.option("cloudFiles.schemaLocation", "/tmp/schemas/my_stream")
```

3. **Check file formats**:
```python
# Verify format matches files
.option("cloudFiles.format", "json")  # Must match actual files
```

---

## State Management Issues

### Symptom: State Store Growing Unbounded

**Description**: State store size keeps growing, slowing down queries.

**Solutions**:

1. **Always use watermarks for stateful operations**:
```python
# Required for bounded state
df.withWatermark("timestamp", "24 hours") \
  .groupBy(...) \
  .agg(...)
```

2. **Monitor state size**:
```python
progress = query.lastProgress
if progress and 'stateOperators' in progress:
    for op in progress['stateOperators']:
        print(f"State store: {op.get('numRowsTotal', 0)} rows")
        print(f"Memory: {op.get('memoryUsedBytes', 0) / 1e6:.2f} MB")
```

3. **Use appropriate state timeout**:
```python
# For sessionization, set timeout
from pyspark.sql.streaming.state import GroupStateTimeout

df.groupByKey(...) \
  .mapGroupsWithState(
      func=process_group,
      outputMode="update",
      timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
  )
```

---

## Output Sink Problems

### Symptom: Data Not Appearing in Sink

**Causes**:
- Wrong output mode
- Sink configuration errors
- Query not started/active

**Solutions**:

1. **Verify query is active**:
```python
print(f"Query active: {query.isActive}")
print(f"Query status: {query.status}")
```

2. **Check output mode**:
```python
# For aggregations, use update or complete
.outputMode("update")  # Only changed rows

# For append-only, use append
.outputMode("append")  # Only new rows
```

3. **Test with console sink first**:
```python
# Debug with console
df.writeStream \
  .format("console") \
  .option("truncate", False) \
  .start()
```

### Symptom: Delta Lake Write Conflicts

**Error Message**:
```
ConcurrentAppendException: Files were added to the root of the table by a concurrent update
```

**Solutions**:

1. **Use one query per table**:
- Don't write to same Delta table from multiple streaming queries
- Use different target tables or partitions

2. **Enable optimistic concurrency**:
```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

---

## Common Error Messages

### "Analysis Exception: Append output mode not supported"

**Cause**: Using append mode with aggregation without watermark.

**Solution**:
```python
# Add watermark
df.withWatermark("timestamp", "10 minutes") \
  .groupBy(...) \
  .agg(...) \
  .writeStream \
  .outputMode("update")  # Use update, not append
```

### "Illegal State Exception: Cannot perform operation after query has been stopped"

**Cause**: Trying to access stopped query.

**Solution**:
```python
if query.isActive:
    progress = query.lastProgress
else:
    print("Query is stopped")
```

### "Timeout waiting for query initialization"

**Cause**: Query taking too long to start.

**Solutions**:
- Check checkpoint directory (might be large)
- Verify source connectivity
- Review cluster resources

---

## Debug Checklist

When troubleshooting streaming queries:

```
□ Check query is active (query.isActive)
□ Review last progress (query.lastProgress)
□ Check for errors in driver logs
□ Verify source data is arriving
□ Confirm checkpoint location is accessible
□ Review state store size
□ Check memory usage (executors)
□ Verify network connectivity to sources
□ Test with console sink for debugging
□ Review Spark UI for bottlenecks
```

---

## Getting Help

### Log Locations
- **Driver logs**: Check cluster driver logs in Databricks
- **Executor logs**: Check executor logs for task failures
- **Query progress**: Use `query.lastProgress` for metrics

### Useful Debug Commands

```python
# Query progress
print(json.dumps(query.lastProgress, indent=2))

# Query status
print(query.status)

# Explain plan
df.explain(mode="formatted")

# Check active queries
for q in spark.streams.active:
    print(f"{q.name}: {q.status}")
```

### Community Resources
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks Forums](https://community.databricks.com/)
- [Stack Overflow - spark-streaming tag](https://stackoverflow.com/questions/tagged/spark-streaming)

---

**Last Updated**: January 2026  
**Author**: Ajit Kalura
