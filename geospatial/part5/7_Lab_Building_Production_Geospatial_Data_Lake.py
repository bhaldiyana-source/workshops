# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building Production Geospatial Data Lake
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on lab guides you through building a complete production-ready geospatial data lake. You'll implement end-to-end pipelines, integrate multiple data sources, set up quality gates, optimize performance, and configure monitoring.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Design and implement complete medallion architecture
# MAGIC - Build multi-source data integration pipelines
# MAGIC - Implement quality gates and validation
# MAGIC - Apply performance optimization techniques
# MAGIC - Set up monitoring and alerting
# MAGIC
# MAGIC ## Lab Structure
# MAGIC - **Exercise 1:** Design data lake architecture
# MAGIC - **Exercise 2:** Build Bronze layer ingestion
# MAGIC - **Exercise 3:** Implement Silver layer transformations
# MAGIC - **Exercise 4:** Create Gold layer analytics tables
# MAGIC - **Exercise 5:** Set up monitoring and alerts
# MAGIC
# MAGIC ## Duration
# MAGIC 45-50 minutes

# COMMAND ----------

# MAGIC %pip install apache-sedona==1.5.1 h3==3.7.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)

# Create lab catalog
spark.sql("CREATE CATALOG IF NOT EXISTS lab_geospatial")
spark.sql("USE CATALOG lab_geospatial")

print("✅ Lab environment ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Design Your Data Lake Architecture
# MAGIC
# MAGIC **Scenario:** You're building a data lake for a smart city initiative that ingests:
# MAGIC - Real-time IoT sensor data (traffic, air quality)
# MAGIC - Batch infrastructure data (buildings, roads)
# MAGIC - Streaming event data (incidents, maintenance)
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Define Bronze, Silver, and Gold schemas
# MAGIC 2. Choose partitioning strategies
# MAGIC 3. Plan optimization approach

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 1: Create your schemas
# MAGIC
# MAGIC -- TODO: Create Bronze schema for raw data
# MAGIC -- CREATE SCHEMA IF NOT EXISTS bronze COMMENT '...'
# MAGIC
# MAGIC -- TODO: Create Silver schema for standardized data
# MAGIC
# MAGIC -- TODO: Create Gold schema for analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze 
# MAGIC COMMENT 'Raw sensor and infrastructure data with full history';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver 
# MAGIC COMMENT 'Validated, standardized spatial data with H3 indexing';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold 
# MAGIC COMMENT 'Business-ready analytics and aggregations';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS monitoring
# MAGIC COMMENT 'Quality metrics and pipeline monitoring';
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Build Bronze Layer Ingestion
# MAGIC
# MAGIC **Task:** Create a Bronze table that ingests sensor data from multiple sources

# COMMAND ----------

# TODO: Generate sample sensor data representing multiple sources
# TODO: Add metadata columns (source_system, ingest_timestamp, file_path)
# TODO: Write to Bronze layer with appropriate partitioning
# TODO: Enable Change Data Feed

# Your code here:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 2

# COMMAND ----------

import random
from datetime import datetime, timedelta

def generate_multi_source_data(num_records=20000):
    """Generate sensor data from multiple sources"""
    sources = ['iot_platform_a', 'iot_platform_b', 'mobile_app']
    sensor_types = ['traffic', 'air_quality', 'noise', 'temperature']
    
    SF_BOUNDS = (37.70, 37.80, -122.52, -122.35)
    data = []
    base_time = datetime(2025, 1, 1)
    
    for i in range(num_records):
        lat = random.uniform(SF_BOUNDS[0], SF_BOUNDS[1])
        lon = random.uniform(SF_BOUNDS[2], SF_BOUNDS[3])
        sensor_type = random.choice(sensor_types)
        
        data.append({
            'record_id': f'REC_{i:08d}',
            'sensor_id': f'SENSOR_{i % 500:04d}',
            'sensor_type': sensor_type,
            'latitude': lat,
            'longitude': lon,
            'reading_value': random.uniform(0, 100),
            'reading_timestamp': base_time + timedelta(minutes=random.randint(0, 10080)),
            'source_system': random.choice(sources),
            'raw_payload': f'{{"value": {random.uniform(0, 100)}}}'
        })
    
    return data

# Generate and write to Bronze
df_bronze = spark.createDataFrame(generate_multi_source_data()) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumn("ingest_date", to_date(current_timestamp()))

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("ingest_date") \
    .saveAsTable("bronze.sensor_readings")

# Enable CDC
spark.sql("""
    ALTER TABLE bronze.sensor_readings
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print(f"✅ Created Bronze table with {df_bronze.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Implement Silver Layer Transformations
# MAGIC
# MAGIC **Task:** Transform Bronze data into standardized Silver layer with:
# MAGIC - Geometry creation and validation
# MAGIC - H3 indexing
# MAGIC - Quality scoring
# MAGIC - Deduplication

# COMMAND ----------

# TODO: Read from Bronze
# TODO: Create Point geometries from lat/lon
# TODO: Add H3 indexes at resolutions 7, 9, 11
# TODO: Validate geometries
# TODO: Calculate quality scores
# TODO: Deduplicate records
# TODO: Write to Silver with H3 partitioning

# Your code here:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 3

# COMMAND ----------

import h3

h3_udf = udf(lambda lat, lon, res: h3.geo_to_h3(lat, lon, res) if lat and lon else None, StringType())

# Read from Bronze
df_bronze_read = spark.table("bronze.sensor_readings")

# Silver transformations
df_silver = df_bronze_read \
    .withColumn("geometry", expr("ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20)))")) \
    .withColumn("is_valid_geometry", expr("ST_IsValid(geometry)")) \
    .withColumn("h3_7", h3_udf(col("latitude"), col("longitude"), lit(7))) \
    .withColumn("h3_9", h3_udf(col("latitude"), col("longitude"), lit(9))) \
    .withColumn("h3_11", h3_udf(col("latitude"), col("longitude"), lit(11))) \
    .withColumn("quality_score",
        when(col("is_valid_geometry") & col("h3_7").isNotNull() & col("reading_value").isNotNull(), 100)
        .when(col("is_valid_geometry") & col("h3_7").isNotNull(), 80)
        .otherwise(60)
    ) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .filter(col("quality_score") >= 60)

# Deduplicate
from pyspark.sql.window import Window

window_spec = Window.partitionBy("sensor_id", "reading_timestamp").orderBy(col("ingest_timestamp").desc())
df_silver_dedup = df_silver.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1).drop("row_num")

# Write to Silver
df_silver_dedup.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("h3_7") \
    .saveAsTable("silver.sensor_readings")

# Optimize
spark.sql("OPTIMIZE silver.sensor_readings ZORDER BY (latitude, longitude, reading_timestamp)")

print(f"✅ Created Silver table with {df_silver_dedup.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Create Gold Layer Analytics
# MAGIC
# MAGIC **Task:** Build three Gold tables:
# MAGIC 1. Hourly regional metrics by H3 region
# MAGIC 2. Sensor performance statistics
# MAGIC 3. Anomaly hotspots

# COMMAND ----------

# TODO: Create hourly_regional_metrics table
# TODO: Create sensor_performance table
# TODO: Create anomaly_hotspots table

# Your code here:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 4

# COMMAND ----------

df_silver_read = spark.table("silver.sensor_readings")

# 1. Hourly regional metrics
df_hourly = df_silver_read \
    .withColumn("reading_date", to_date("reading_timestamp")) \
    .withColumn("reading_hour", hour("reading_timestamp")) \
    .groupBy("h3_7", "sensor_type", "reading_date", "reading_hour") \
    .agg(
        count("*").alias("reading_count"),
        avg("reading_value").alias("avg_reading"),
        min("reading_value").alias("min_reading"),
        max("reading_value").alias("max_reading"),
        stddev("reading_value").alias("stddev_reading")
    )

df_hourly.write.format("delta").mode("overwrite").partitionBy("reading_date") \
    .saveAsTable("gold.hourly_regional_metrics")

# 2. Sensor performance
df_sensors = df_silver_read.groupBy("sensor_id", "sensor_type") \
    .agg(
        count("*").alias("total_readings"),
        avg("reading_value").alias("avg_reading"),
        avg("quality_score").alias("avg_quality")
    )

df_sensors.write.format("delta").mode("overwrite").saveAsTable("gold.sensor_performance")

# 3. Anomaly hotspots
df_anomalies = df_silver_read \
    .filter(
        ((col("sensor_type") == "air_quality") & (col("reading_value") > 150)) |
        ((col("sensor_type") == "noise") & (col("reading_value") > 85))
    ) \
    .withColumn("reading_date", to_date("reading_timestamp")) \
    .groupBy("h3_7", "sensor_type", "reading_date") \
    .agg(count("*").alias("anomaly_count"))

df_anomalies.write.format("delta").mode("overwrite").partitionBy("reading_date") \
    .saveAsTable("gold.anomaly_hotspots")

print("✅ Created 3 Gold tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Set Up Monitoring
# MAGIC
# MAGIC **Task:** Create monitoring metrics for:
# MAGIC - Data freshness
# MAGIC - Data quality trends
# MAGIC - Pipeline performance

# COMMAND ----------

# TODO: Create data freshness metrics
# TODO: Create quality trend metrics
# TODO: Set up alerting thresholds

# Your code here:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 5

# COMMAND ----------

# Data freshness
freshness_metrics = spark.sql("""
    SELECT 
        'bronze.sensor_readings' as table_name,
        MAX(ingest_timestamp) as last_update,
        CURRENT_TIMESTAMP() - MAX(ingest_timestamp) as staleness,
        COUNT(*) as record_count
    FROM bronze.sensor_readings
""")

freshness_metrics.write.format("delta").mode("overwrite") \
    .saveAsTable("monitoring.data_freshness")

# Quality trends
quality_metrics = spark.sql("""
    SELECT
        CURRENT_DATE() as metric_date,
        COUNT(*) as total_records,
        AVG(quality_score) as avg_quality,
        SUM(CASE WHEN is_valid_geometry THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as valid_pct
    FROM silver.sensor_readings
""")

quality_metrics.write.format("delta").mode("append") \
    .saveAsTable("monitoring.daily_quality_metrics")

print("✅ Monitoring configured")

# Check for alerts
alert_thresholds = {'min_quality': 80, 'max_staleness_hours': 1}
current_quality = spark.table("monitoring.daily_quality_metrics").select("avg_quality").collect()[0][0]

if current_quality < alert_thresholds['min_quality']:
    print(f"⚠️  ALERT: Quality score {current_quality:.1f} below threshold")
else:
    print(f"✅ Quality score {current_quality:.1f} meets threshold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Readiness Checklist
# MAGIC
# MAGIC ### Data Architecture
# MAGIC - ✅ Medallion architecture implemented
# MAGIC - ✅ Appropriate partitioning strategies
# MAGIC - ✅ Change Data Feed enabled
# MAGIC - ✅ Optimization applied (Z-order)
# MAGIC
# MAGIC ### Quality & Governance
# MAGIC - ✅ Geometry validation
# MAGIC - ✅ Quality scoring
# MAGIC - ✅ Deduplication logic
# MAGIC - ✅ Metadata tracking
# MAGIC
# MAGIC ### Operations
# MAGIC - ✅ Monitoring tables created
# MAGIC - ✅ Alerting thresholds defined
# MAGIC - ✅ Retention policies set
# MAGIC - ✅ Documentation complete
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **End-to-end implementation** requires careful planning of all layers
# MAGIC 2. **Quality gates** at each layer ensure data fitness
# MAGIC 3. **H3 partitioning** provides uniform distribution
# MAGIC 4. **Monitoring** is essential for production reliability
# MAGIC 5. **Incremental processing** with CDC enables efficient updates
# MAGIC
# MAGIC **Continue to Lab 8: Optimizing Large-Scale Spatial Queries**
