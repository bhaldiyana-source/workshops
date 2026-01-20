# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04: Windowing and Watermarks
# MAGIC
# MAGIC ## Overview
# MAGIC Master time-based operations with windows and watermarks for handling late-arriving data.
# MAGIC
# MAGIC **Duration**: 50 minutes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tumbling Windows

# COMMAND ----------

# Create streaming data with timestamps
streaming_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("event_time", col("timestamp"))
    .withColumn("sensor_id", (col("value") % 5).cast("int")))

# Tumbling window aggregation
tumbling = (streaming_df
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("sensor_id")
    )
    .agg(
        count("*").alias("count"),
        avg("value").alias("avg_value")
    ))

display(tumbling)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sliding Windows

# COMMAND ----------

# Sliding window (15 min window, sliding every 5 min)
sliding = (streaming_df
    .groupBy(
        window(col("event_time"), "15 minutes", "5 minutes"),
        col("sensor_id")
    )
    .agg(
        count("*").alias("count"),
        avg("value").alias("moving_avg")
    ))

print("Sliding window: 15 min window, 5 min slide")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Watermarks for Late Data

# COMMAND ----------

# Add watermark to handle late data
with_watermark = (streaming_df
    .withWatermark("event_time", "10 minutes")  # Allow 10 min late data
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor_id")
    )
    .agg(
        count("*").alias("count"),
        sum("value").alias("total")
    ))

print("✅ Watermark configured: 10 minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Session Windows

# COMMAND ----------

# Session windows (gap-based)
session_win = (streaming_df
    .withWatermark("event_time", "30 minutes")
    .groupBy(
        col("sensor_id"),
        session_window(col("event_time"), "5 minutes")  # 5 min gap
    )
    .count())

print("Session window with 5-minute timeout")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Windowing Best Practices

# COMMAND ----------

# Best practice example
optimized = (streaming_df
    .withWatermark("event_time", "1 hour")  # Appropriate watermark
    .groupBy(
        window(col("event_time"), "15 minutes"),  # Reasonable window size
        col("sensor_id")
    )
    .agg(
        count("*").alias("event_count"),
        avg("value").alias("avg_value"),
        max("value").alias("max_value"),
        min("value").alias("min_value")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("event_count"),
        col("avg_value")
    ))

# Query
query = (optimized
    .writeStream
    .format("memory")
    .queryName("windowed_metrics")
    .outputMode("update")
    .start())

import time
time.sleep(20)

display(spark.sql("SELECT * FROM windowed_metrics ORDER BY window_start DESC"))
query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Tumbling windows (fixed, non-overlapping)
# MAGIC ✅ Sliding windows (overlapping)  
# MAGIC ✅ Watermarks for late data handling
# MAGIC ✅ Session windows (activity-based)
# MAGIC
# MAGIC **Next**: Notebook 05 - Stateful Operations

# COMMAND ----------

for query in spark.streams.active:
    query.stop()
