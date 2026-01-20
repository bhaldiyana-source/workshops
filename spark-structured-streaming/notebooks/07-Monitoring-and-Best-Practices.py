# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 07: Monitoring and Best Practices
# MAGIC
# MAGIC ## Overview
# MAGIC Learn to monitor streaming queries, debug issues, and implement production best practices.
# MAGIC
# MAGIC **Duration**: 35 minutes

# COMMAND ----------

from pyspark.sql.functions import *
import time
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Query Progress Monitoring

# COMMAND ----------

# Create streaming query
streaming_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load())

monitored_query = (streaming_df
    .writeStream
    .format("memory")
    .queryName("monitored")
    .option("checkpointLocation", "/tmp/checkpoints/monitored")
    .trigger(processingTime="5 seconds")
    .start())

# Let it process a few batches
time.sleep(15)

# Get progress information
progress = monitored_query.lastProgress

if progress:
    print("=== Query Progress Metrics ===")
    print(f"Batch ID: {progress['batchId']}")
    print(f"Input Rows: {progress['numInputRows']}")
    print(f"Processing Rate: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec")
    print(f"Batch Duration: {progress['batchDuration']} ms")
    print(f"Sources: {json.dumps(progress['sources'], indent=2)}")
    print(f"Sink: {json.dumps(progress['sink'], indent=2)}")

monitored_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Streaming Query Status

# COMMAND ----------

# Create query
status_query = (streaming_df
    .writeStream
    .format("memory")
    .queryName("status_demo")
    .start())

time.sleep(5)

# Check status
status = status_query.status
print(f"Query ID: {status_query.id}")
print(f"Run ID: {status_query.runId}")
print(f"Name: {status_query.name}")
print(f"Is Active: {status_query.isActive}")
print(f"Message: {status['message']}")
print(f"Is Trigger Active: {status['isTriggerActive']}")
print(f"Is Data Available: {status['isDataAvailable']}")

status_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Monitoring Function

# COMMAND ----------

def monitor_streaming_query(query, duration=30, interval=5):
    """Monitor a streaming query and report metrics"""
    start_time = time.time()
    
    print(f"Monitoring query: {query.name}")
    print(f"Duration: {duration} seconds")
    print("=" * 60)
    
    while time.time() - start_time < duration and query.isActive:
        progress = query.lastProgress
        
        if progress:
            print(f"\n[{time.strftime('%H:%M:%S')}] Batch {progress['batchId']}")
            print(f"  Input rows: {progress['numInputRows']}")
            print(f"  Processing rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
            print(f"  Batch duration: {progress['batchDuration']} ms")
            
            # Check for issues
            if progress['batchDuration'] > 30000:  # > 30 seconds
                print("  ⚠️  WARNING: Slow batch processing")
            
            if progress.get('processedRowsPerSecond', 0) < 10:
                print("  ⚠️  WARNING: Low throughput")
        
        time.sleep(interval)
    
    print("\n" + "=" * 60)
    print("Monitoring completed")

# Test monitoring
test_query = (streaming_df
    .writeStream
    .format("memory")
    .queryName("test_monitoring")
    .start())

monitor_streaming_query(test_query, duration=20, interval=5)
test_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Checkpoint Management

# COMMAND ----------

# Best practices for checkpoints
checkpoint_example = """
# Good checkpoint practices:

# 1. Use DBFS or cloud storage
checkpoint_location = "dbfs:/checkpoints/my_query"

# 2. One checkpoint per query
# Don't reuse checkpoint locations

# 3. Organize by environment
dev_checkpoint = "/checkpoints/dev/query_name"
prod_checkpoint = "/checkpoints/prod/query_name"

# 4. Back up before schema changes
# dbutils.fs.cp(checkpoint_location, backup_location, recurse=True)

# 5. Clean up old checkpoints periodically
# But keep for recovery if needed
"""

print(checkpoint_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Error Handling

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryException

def safe_streaming_query(df, output_path, checkpoint_path):
    """Streaming query with error handling"""
    try:
        query = (df
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .start(output_path))
        
        # Monitor for failures
        query.awaitTermination(timeout=60)
        
        return query
        
    except StreamingQueryException as e:
        print(f"❌ Streaming query failed: {e.message}")
        print(f"Cause: {e.cause}")
        # Implement retry logic or alerting
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise

print("✅ Error handling pattern defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Performance Optimization

# COMMAND ----------

# Performance best practices
print("=== Performance Optimization Tips ===\n")

print("1. Configure shuffle partitions:")
print("   spark.conf.set('spark.sql.shuffle.partitions', '200')")

print("\n2. Use Photon:")
print("   Enable on cluster configuration")

print("\n3. Optimize file sizes:")
print("   - Target 128 MB - 1 GB per file")
print("   - Use Delta table optimize")

print("\n4. Partition appropriately:")
print("   - Partition by date/hour for time-series")
print("   - Avoid over-partitioning")

print("\n5. Use Adaptive Query Execution:")
spark.conf.set("spark.sql.adaptive.enabled", "true")
print("   spark.sql.adaptive.enabled = true")

print("\n6. Checkpoint frequency:")
print("   - Trigger interval: balance latency vs cost")
print("   - More frequent = lower latency, higher cost")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Production Deployment Checklist

# COMMAND ----------

checklist = """
📋 PRODUCTION DEPLOYMENT CHECKLIST

✅ Configuration
  □ Checkpoint location set to DBFS/cloud storage
  □ Appropriate trigger interval configured
  □ Output mode matches use case
  □ Watermarks configured for stateful ops

✅ Error Handling
  □ foreachBatch with try-catch implemented
  □ Alerting on query failures
  □ Retry logic for transient errors
  □ Monitoring dashboard set up

✅ Performance
  □ Cluster sized appropriately
  □ Shuffle partitions configured
  □ Photon enabled (if available)
  □ Auto-scaling configured

✅ Data Quality
  □ Schema validation in place
  □ Null handling implemented
  □ Data quality checks added
  □ Bad records handling

✅ Operations
  □ Runbook documented
  □ On-call rotation defined
  □ Backup/recovery tested
  □ Cost monitoring enabled

✅ Testing
  □ Load testing completed
  □ Failure scenarios tested
  □ Recovery verified
  □ Performance benchmarked
"""

print(checklist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Common Issues and Solutions

# COMMAND ----------

troubleshooting = """
🔧 COMMON ISSUES

Issue: Increasing batch processing time
Solution:
  - Check if state size is growing unbounded
  - Add watermarks to limit state
  - Optimize expensive operations
  - Increase cluster resources

Issue: Out of memory errors
Solution:
  - Reduce state size with watermarks
  - Increase executor memory
  - Use deduplication carefully
  - Check for data skew

Issue: Checkpoint incompatibility
Solution:
  - Don't change query logic drastically
  - Use new checkpoint for major changes
  - Back up checkpoints before changes

Issue: Slow startup
Solution:
  - Check checkpoint directory size
  - Verify source connectivity
  - Review cluster startup time

Issue: Missing data
Solution:
  - Check watermark settings
  - Verify source connectivity
  - Review trigger intervals
  - Check for errors in logs
"""

print(troubleshooting)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Query progress monitoring
# MAGIC ✅ Streaming query status checks
# MAGIC ✅ Custom monitoring functions
# MAGIC ✅ Checkpoint management
# MAGIC ✅ Error handling patterns
# MAGIC ✅ Performance optimization
# MAGIC ✅ Production deployment checklist
# MAGIC
# MAGIC **Next**: Notebook 08 - Complete Streaming ETL Pipeline

# COMMAND ----------

for query in spark.streams.active:
    query.stop()
