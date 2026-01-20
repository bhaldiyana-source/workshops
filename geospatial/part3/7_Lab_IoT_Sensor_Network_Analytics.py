# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: IoT Sensor Network Analytics
# MAGIC
# MAGIC ## Overview
# MAGIC Build a real-time IoT sensor network analytics system that processes environmental sensor data, performs spatial correlation analysis, generates heatmaps, and detects spatial anomalies.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Process multi-sensor spatial data streams
# MAGIC - Perform spatial correlation analysis
# MAGIC - Create real-time environmental heatmaps
# MAGIC - Detect and alert on spatial anomalies
# MAGIC - Implement time-series aggregations on spatial data
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %pip install faker h3 numpy --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import numpy as np

catalog = "main"
schema_name = "iot_sensor_lab"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Lab environment ready: {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Generate Sensor Network Data

# COMMAND ----------

def generate_sensor_network(num_sensors=20, readings_per_sensor=100):
    """Generate synthetic environmental sensor data"""
    sensors = []
    readings = []
    
    # San Francisco area
    lat_min, lat_max = 37.72, 37.82
    lon_min, lon_max = -122.50, -122.38
    
    # Create sensor grid
    for i in range(num_sensors):
        sensor_id = f"SENSOR_{i+1:03d}"
        lat = random.uniform(lat_min, lat_max)
        lon = random.uniform(lon_min, lon_max)
        sensor_type = random.choice(["temperature", "air_quality", "noise", "humidity"])
        
        sensors.append({
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "installation_date": datetime.now() - timedelta(days=random.randint(30, 365)),
            "status": "active"
        })
        
        # Generate readings
        base_time = datetime.now() - timedelta(hours=1)
        for j in range(readings_per_sensor):
            ts = base_time + timedelta(seconds=j * 30)
            
            # Generate realistic sensor values with spatial patterns
            if sensor_type == "temperature":
                base_val = 20 + (lat - lat_min) * 5  # Temperature gradient
                value = base_val + random.uniform(-2, 2)
                unit = "celsius"
            elif sensor_type == "air_quality":
                base_val = 50 + random.uniform(-20, 40)
                value = max(0, base_val + np.random.normal(0, 10))
                unit = "aqi"
            elif sensor_type == "noise":
                base_val = 60 + random.uniform(-10, 20)
                value = base_val + np.random.normal(0, 5)
                unit = "decibels"
            else:  # humidity
                base_val = 65 + random.uniform(-15, 15)
                value = max(0, min(100, base_val + np.random.normal(0, 5)))
                unit = "percent"
            
            # Occasional anomalies
            if random.random() < 0.05:
                value *= random.uniform(1.5, 2.5)
            
            readings.append({
                "sensor_id": sensor_id,
                "timestamp": ts,
                "value": round(value, 2),
                "unit": unit,
                "quality": random.choice(["good", "good", "good", "fair"])
            })
    
    return sensors, readings

sensors, readings = generate_sensor_network(20, 100)
print(f"Generated {len(sensors)} sensors and {len(readings)} readings")

# Save to tables
sensors_df = spark.createDataFrame(sensors)
sensors_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.sensors")

# Write readings to landing zone
readings_landing = f"{data_base}/landing/sensor_readings"
dbutils.fs.rm(readings_landing, recurse=True)

readings_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("quality", StringType())
])

batch_size = 100
for i in range(0, len(readings), batch_size):
    batch = readings[i:i+batch_size]
    spark.createDataFrame(batch, schema=readings_schema).write.mode("overwrite").json(
        f"{readings_landing}/batch_{i//batch_size:03d}"
    )

print("Data written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Real-Time Sensor Data Processing

# COMMAND ----------

# Create sensor readings stream
sensor_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(readings_schema)
    .load(readings_landing)
)

# Enrich with sensor metadata
sensors_static = spark.table(f"{catalog}.{schema_name}.sensors")

enriched_readings = (
    sensor_stream
    .join(
        F.broadcast(sensors_static),
        "sensor_id",
        "inner"
    )
    .withColumn("h3_index", F.expr("h3_latlngtocell(latitude, longitude, 9)"))
    .withColumn("processing_time", F.current_timestamp())
)

# Write enriched stream
enriched_table = f"{catalog}.{schema_name}.sensor_readings_enriched"
enriched_checkpoint = f"{checkpoint_base}/enriched_readings"

query_enriched = (
    enriched_readings
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", enriched_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(enriched_table)
)

print(f"Started enriched readings: {query_enriched.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Spatial Heatmap Generation

# COMMAND ----------

# Generate heatmap data by H3 cell
heatmap_stream = (
    enriched_readings
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "h3_index",
        "sensor_type"
    )
    .agg(
        F.avg("value").alias("avg_value"),
        F.max("value").alias("max_value"),
        F.min("value").alias("min_value"),
        F.stddev("value").alias("stddev_value"),
        F.count("*").alias("reading_count")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "h3_index",
        F.expr("h3_celltolatlng(h3_index)[0]").alias("latitude"),
        F.expr("h3_celltolatlng(h3_index)[1]").alias("longitude"),
        "sensor_type",
        F.round("avg_value", 2).alias("avg_value"),
        F.round("max_value", 2).alias("max_value"),
        F.round("min_value", 2).alias("min_value"),
        F.round("stddev_value", 2).alias("stddev_value"),
        "reading_count"
    )
)

heatmap_table = f"{catalog}.{schema_name}.environmental_heatmap"
heatmap_checkpoint = f"{checkpoint_base}/heatmap"

query_heatmap = (
    heatmap_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", heatmap_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(heatmap_table)
)

print(f"Started heatmap generation: {query_heatmap.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Spatial Anomaly Detection

# COMMAND ----------

# Detect anomalies by comparing to spatial neighborhood
from pyspark.sql.window import Window

anomaly_detection = (
    enriched_readings
    .withWatermark("timestamp", "15 minutes")
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "sensor_type",
        "h3_index"
    )
    .agg(
        F.avg("value").alias("cell_avg"),
        F.stddev("value").alias("cell_stddev"),
        F.count("*").alias("sample_count")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "sensor_type",
        "h3_index",
        "cell_avg",
        "cell_stddev",
        "sample_count"
    )
)

# Write anomaly detection results
anomaly_table = f"{catalog}.{schema_name}.spatial_anomalies"
anomaly_checkpoint = f"{checkpoint_base}/anomalies"

query_anomalies = (
    anomaly_detection
    .filter("cell_stddev > 5")  # High variation indicates anomaly
    .withColumn("anomaly_type", F.lit("HIGH_VARIATION"))
    .withColumn("anomaly_severity",
        F.when(F.col("cell_stddev") > 10, "HIGH")
         .when(F.col("cell_stddev") > 7, "MEDIUM")
         .otherwise("LOW"))
    .withColumn("anomaly_id", F.expr("uuid()"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", anomaly_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(anomaly_table)
)

print(f"Started anomaly detection: {query_anomalies.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Multi-Sensor Correlation

# COMMAND ----------

# Pivot sensor data for correlation analysis
correlation_data = (
    enriched_readings
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        F.window("timestamp", "10 minutes"),
        "h3_index"
    )
    .pivot("sensor_type")
    .agg(F.avg("value"))
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "h3_index",
        F.col("temperature").alias("avg_temperature"),
        F.col("air_quality").alias("avg_air_quality"),
        F.col("noise").alias("avg_noise"),
        F.col("humidity").alias("avg_humidity")
    )
)

correlation_table = f"{catalog}.{schema_name}.sensor_correlation"
correlation_checkpoint = f"{checkpoint_base}/correlation"

query_correlation = (
    correlation_data
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", correlation_checkpoint)
    .trigger(processingTime="15 seconds")
    .toTable(correlation_table)
)

print(f"Started correlation analysis: {query_correlation.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Results

# COMMAND ----------

import time
print("Processing sensor data... waiting 25 seconds")
time.sleep(25)

# COMMAND ----------

# Sensor network overview
enriched = spark.table(enriched_table)
print(f"=== Sensor Network Overview ===")
print(f"Total readings: {enriched.count()}")

print("\nReadings per sensor type:")
enriched.groupBy("sensor_type").agg(
    F.count("*").alias("readings"),
    F.avg("value").alias("avg_value"),
    F.min("value").alias("min_value"),
    F.max("value").alias("max_value")
).show()

# Heatmap data
heatmap = spark.table(heatmap_table)
print(f"\n=== Heatmap Data ===")
print(f"Total heatmap cells: {heatmap.count()}")

print("\nHotspots by sensor type:")
heatmap.filter("avg_value IS NOT NULL") \
    .orderBy(F.desc("avg_value")) \
    .limit(10) \
    .show(truncate=False)

# Anomalies
anomalies = spark.table(anomaly_table)
print(f"\n=== Detected Anomalies ===")
print(f"Total anomalies: {anomalies.count()}")

if anomalies.count() > 0:
    anomalies.groupBy("sensor_type", "anomaly_severity").count().show()

# Correlation
correlation = spark.table(correlation_table)
print(f"\n=== Sensor Correlations ===")
print(f"Correlation records: {correlation.count()}")

if correlation.count() > 0:
    correlation.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Complete!
# MAGIC
# MAGIC You've built an IoT sensor network analytics system with:
# MAGIC - Real-time sensor data processing
# MAGIC - Spatial heatmap generation
# MAGIC - Anomaly detection
# MAGIC - Multi-sensor correlation analysis
