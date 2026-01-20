# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06: Writing to Sinks
# MAGIC
# MAGIC ## Overview
# MAGIC Learn to write streaming results to various sinks with appropriate output modes.
# MAGIC
# MAGIC **Duration**: 40 minutes

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Writing to Delta Lake

# COMMAND ----------

# Create streaming data
streaming_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("sensor_id", (col("value") % 5).cast("int"))
    .withColumn("reading", (col("value") % 100).cast("double")))

# Write to Delta with append mode
delta_query = (streaming_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/delta_sink")
    .trigger(processingTime="10 seconds")
    .start("/tmp/delta_output/sensor_data"))

import time
time.sleep(15)
delta_query.stop()

# Verify
result = spark.read.format("delta").load("/tmp/delta_output/sensor_data")
print(f"Records written: {result.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. foreach Batch for Custom Logic

# COMMAND ----------

def process_batch(batch_df, batch_id):
    """Custom batch processing logic"""
    print(f"Processing batch {batch_id}")
    
    # Write to Delta
    batch_df.write.format("delta").mode("append").save("/tmp/delta_output/custom")
    
    # Additional custom logic (e.g., send alerts, call APIs)
    count = batch_df.count()
    if count > 50:
        print(f"⚠️ High volume alert: {count} records")

# Use foreachBatch
custom_query = (streaming_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/checkpoints/custom")
    .trigger(processingTime="5 seconds")
    .start())

time.sleep(12)
custom_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Writing to Kafka

# COMMAND ----------

# Prepare data for Kafka
kafka_ready = (streaming_df
    .select(
        col("sensor_id").cast("string").alias("key"),
        to_json(struct("sensor_id", "reading", "timestamp")).alias("value")
    ))

kafka_config = """
# Write to Kafka
kafka_query = (kafka_ready
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "sensor_output")
    .option("checkpointLocation", "/tmp/checkpoints/kafka_out")
    .start())
"""

print("Kafka sink configuration:")
print(kafka_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Console and Memory Sinks (Debugging)

# COMMAND ----------

# Console sink
console_query = (streaming_df
    .select("sensor_id", "reading", "timestamp")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 10)
    .trigger(processingTime="5 seconds")
    .start())

time.sleep(8)
console_query.stop()

# COMMAND ----------

# Memory sink
memory_query = (streaming_df
    .writeStream
    .format("memory")
    .queryName("sensor_memory")
    .outputMode("append")
    .start())

time.sleep(10)
display(spark.sql("SELECT * FROM sensor_memory LIMIT 20"))
memory_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Output Modes Comparison

# COMMAND ----------

# Aggregation for output mode demo
aggregated = (streaming_df
    .groupBy("sensor_id")
    .agg(
        count("*").alias("count"),
        avg("reading").alias("avg_reading")
    ))

# Complete mode (entire result table)
complete_query = (aggregated
    .writeStream
    .format("memory")
    .queryName("complete_output")
    .outputMode("complete")
    .start())

time.sleep(10)
display(spark.sql("SELECT * FROM complete_output"))
complete_query.stop()

# Update mode (only updated rows)
update_query = (aggregated
    .writeStream
    .format("memory")
    .queryName("update_output")
    .outputMode("update")
    .start())

time.sleep(10)
display(spark.sql("SELECT * FROM update_output"))
update_query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exactly-Once Semantics

# COMMAND ----------

# Delta Lake provides exactly-once guarantees
exactly_once = (streaming_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/exactly_once")
    .trigger(processingTime="10 seconds")
    .start("/tmp/delta_output/exactly_once"))

print("✅ Exactly-once semantics with Delta Lake")
print("- Idempotent writes")
print("- ACID transactions")
print("- Checkpoint-based recovery")

time.sleep(12)
exactly_once.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Writing to Delta Lake (recommended)
# MAGIC ✅ foreachBatch for custom logic
# MAGIC ✅ Kafka output sink
# MAGIC ✅ Console and memory for debugging
# MAGIC ✅ Output modes: append, update, complete
# MAGIC ✅ Exactly-once semantics
# MAGIC
# MAGIC **Next**: Notebook 07 - Monitoring and Best Practices

# COMMAND ----------

for query in spark.streams.active:
    query.stop()
