# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Medallion Architecture for Geospatial Data
# MAGIC
# MAGIC ## Overview
# MAGIC This demo walks through implementing a complete medallion architecture (Bronze → Silver → Gold) for geospatial data on Databricks. You'll build a real pipeline for city infrastructure data, implementing incremental processing, quality checks, and optimizations at each layer.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Implement Bronze layer for raw spatial data ingestion
# MAGIC - Build Silver layer with standardization and quality checks
# MAGIC - Create Gold layer with business-ready aggregations
# MAGIC - Optimize layer transitions with Delta Lake features
# MAGIC - Handle incremental processing efficiently
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - DBR 15.0+ with Apache Sedona installed
# MAGIC - Write access to Unity Catalog
# MAGIC
# MAGIC ## Duration
# MAGIC 35-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install Dependencies

# COMMAND ----------

# Install Apache Sedona and H3
%pip install apache-sedona==1.5.1 h3==3.7.6 geopandas==0.14.1
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Spark for Sedona

# COMMAND ----------

from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# Configure Sedona
spark.conf.set("spark.serializer", KryoSerializer.getName)
spark.conf.set("spark.kryo.registrator", SedonaKryoRegistrator.getName)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Register Sedona SQL functions
SedonaRegistrator.registerAll(spark)

print("✅ Sedona configured and registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Catalog and Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog and schemas for medallion architecture
# MAGIC CREATE CATALOG IF NOT EXISTS geospatial_demo;
# MAGIC USE CATALOG geospatial_demo;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw spatial data';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Standardized and validated spatial data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Business-ready spatial analytics';
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case: City Infrastructure Data Pipeline
# MAGIC
# MAGIC **Scenario:** You manage geospatial data for a smart city initiative
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - IoT sensors (traffic, air quality, noise)
# MAGIC - Mobile devices (movement patterns)
# MAGIC - City infrastructure (buildings, roads, utilities)
# MAGIC - Emergency services (incidents, response times)
# MAGIC
# MAGIC **Goal:** Build a data lake that supports:
# MAGIC - Real-time dashboards for city operations
# MAGIC - Historical analytics for urban planning
# MAGIC - ML models for predictive maintenance
# MAGIC - Public APIs for civic applications

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC ### Characteristics
# MAGIC - Preserve original format and structure
# MAGIC - Capture all data (no filtering)
# MAGIC - Add metadata for lineage
# MAGIC - Support time travel
# MAGIC - Partition by ingestion date

# COMMAND ----------

# Generate sample IoT sensor data
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Set seed for reproducibility
random.seed(42)

# Define San Francisco bounding box
SF_LAT_MIN, SF_LAT_MAX = 37.70, 37.80
SF_LON_MIN, SF_LON_MAX = -122.52, -122.35

# Generate sample sensor data
def generate_sensor_data(num_records=10000):
    data = []
    base_time = datetime(2025, 1, 1, 0, 0, 0)
    
    sensor_types = ['traffic', 'air_quality', 'noise', 'temperature']
    
    for i in range(num_records):
        lat = random.uniform(SF_LAT_MIN, SF_LAT_MAX)
        lon = random.uniform(SF_LON_MIN, SF_LON_MAX)
        sensor_type = random.choice(sensor_types)
        
        # Generate sensor-specific readings
        if sensor_type == 'traffic':
            reading = random.randint(0, 100)  # vehicles per minute
        elif sensor_type == 'air_quality':
            reading = random.uniform(0, 500)  # AQI
        elif sensor_type == 'noise':
            reading = random.uniform(30, 120)  # decibels
        else:  # temperature
            reading = random.uniform(-5, 40)  # celsius
        
        timestamp = base_time + timedelta(hours=random.randint(0, 168))  # 1 week
        
        data.append({
            'sensor_id': f'SENSOR_{i % 1000:04d}',
            'sensor_type': sensor_type,
            'latitude': lat,
            'longitude': lon,
            'reading_value': reading,
            'reading_timestamp': timestamp,
            'raw_payload': f'{{"sensor_id":"SENSOR_{i % 1000:04d}","value":{reading}}}',
            'source_system': 'iot_platform_v1',
            'ingest_timestamp': datetime.now()
        })
    
    return data

# Create DataFrame
sensor_data = generate_sensor_data(10000)
df_raw = spark.createDataFrame(sensor_data)

display(df_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Write Raw Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date

# Add bronze metadata columns
df_bronze = df_raw \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumn("ingest_date", to_date(current_timestamp())) \
    .withColumn("source_file", lit("manual_upload")) \
    .withColumn("bronze_id", monotonically_increasing_id())

# Write to Bronze layer with partitioning
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingest_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze.sensor_readings_raw")

print(f"✅ Wrote {df_bronze.count()} records to Bronze layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer: Enable Change Data Feed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable CDC for incremental processing
# MAGIC ALTER TABLE bronze.sensor_readings_raw
# MAGIC SET TBLPROPERTIES (
# MAGIC   delta.enableChangeDataFeed = true,
# MAGIC   delta.logRetentionDuration = '30 days',
# MAGIC   delta.deletedFileRetentionDuration = '7 days'
# MAGIC );
# MAGIC
# MAGIC DESCRIBE EXTENDED bronze.sensor_readings_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Standardization and Validation
# MAGIC
# MAGIC ### Transformations
# MAGIC 1. **Geometry Creation:** Convert lat/lon to Point geometries
# MAGIC 2. **CRS Standardization:** Ensure all data is in EPSG:4326
# MAGIC 3. **H3 Indexing:** Add hierarchical spatial indexes
# MAGIC 4. **Validation:** Check geometry validity and data quality
# MAGIC 5. **Deduplication:** Remove duplicate sensor readings
# MAGIC 6. **Enrichment:** Add derived fields

# COMMAND ----------

from pyspark.sql.functions import expr, col
import h3

# Register H3 UDF
def lat_lon_to_h3(lat, lon, resolution):
    try:
        return h3.geo_to_h3(lat, lon, resolution)
    except:
        return None

h3_udf = udf(lat_lon_to_h3, StringType())

# Read from Bronze
df_bronze_read = spark.read.table("bronze.sensor_readings_raw")

# Silver transformations
df_silver = df_bronze_read \
    .withColumn("geometry", expr("ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20)))")) \
    .withColumn("is_valid_geometry", expr("ST_IsValid(geometry)")) \
    .withColumn("h3_index_7", h3_udf(col("latitude"), col("longitude"), lit(7))) \
    .withColumn("h3_index_9", h3_udf(col("latitude"), col("longitude"), lit(9))) \
    .withColumn("h3_index_11", h3_udf(col("latitude"), col("longitude"), lit(11))) \
    .withColumn("quality_score", 
        when(col("is_valid_geometry") & col("h3_index_7").isNotNull() & 
             col("reading_value").isNotNull(), 100)
        .when(col("is_valid_geometry") & col("h3_index_7").isNotNull(), 80)
        .when(col("is_valid_geometry"), 60)
        .otherwise(20)
    ) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .filter(col("quality_score") >= 60)  # Quality gate

# Show validation stats
validation_stats = df_silver.groupBy("sensor_type") \
    .agg(
        count("*").alias("total_records"),
        avg("quality_score").alias("avg_quality"),
        sum(when(col("is_valid_geometry"), 1).otherwise(0)).alias("valid_geometries")
    )

display(validation_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer: Deduplication

# COMMAND ----------

from pyspark.sql.window import Window

# Define deduplication window
window_spec = Window.partitionBy("sensor_id", "reading_timestamp") \
    .orderBy(col("ingest_timestamp").desc())

# Deduplicate - keep latest ingested record
df_silver_dedup = df_silver \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"Before deduplication: {df_silver.count()} records")
print(f"After deduplication: {df_silver_dedup.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer: Write with Optimization

# COMMAND ----------

# Write to Silver with optimization
df_silver_dedup.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("h3_index_7") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.sensor_readings_standardized")

# Optimize Silver table
spark.sql("""
    OPTIMIZE silver.sensor_readings_standardized
    ZORDER BY (latitude, longitude, reading_timestamp)
""")

print("✅ Silver layer written and optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer: Add Bloom Filter

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add Bloom filter for fast sensor_id lookups
# MAGIC CREATE BLOOMFILTER INDEX
# MAGIC ON TABLE silver.sensor_readings_standardized
# MAGIC FOR COLUMNS(sensor_id OPTIONS (fpp=0.01, numItems=1000000));
# MAGIC
# MAGIC -- Describe table
# MAGIC DESCRIBE EXTENDED silver.sensor_readings_standardized;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Business-Ready Analytics
# MAGIC
# MAGIC ### Gold Tables to Create
# MAGIC 1. **Hourly Regional Metrics:** Aggregated sensor readings by H3 region
# MAGIC 2. **Sensor Performance:** Statistics per sensor
# MAGIC 3. **Time Series Trends:** Daily/hourly trends
# MAGIC 4. **Hotspot Analysis:** Identify areas with concerning readings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1: Hourly Regional Metrics

# COMMAND ----------

# Read from Silver
df_silver_read = spark.read.table("silver.sensor_readings_standardized")

# Create hourly regional aggregations
df_gold_regional = df_silver_read \
    .withColumn("reading_date", to_date("reading_timestamp")) \
    .withColumn("reading_hour", hour("reading_timestamp")) \
    .groupBy("h3_index_7", "sensor_type", "reading_date", "reading_hour") \
    .agg(
        count("*").alias("reading_count"),
        avg("reading_value").alias("avg_reading"),
        min("reading_value").alias("min_reading"),
        max("reading_value").alias("max_reading"),
        stddev("reading_value").alias("stddev_reading"),
        countDistinct("sensor_id").alias("unique_sensors"),
        avg("quality_score").alias("avg_quality_score")
    ) \
    .withColumn("created_timestamp", current_timestamp())

# Write to Gold
df_gold_regional.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("reading_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.hourly_regional_metrics")

display(df_gold_regional.orderBy("reading_date", "reading_hour").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: Sensor Performance Statistics

# COMMAND ----------

# Calculate sensor-level statistics
df_gold_sensors = df_silver_read \
    .groupBy("sensor_id", "sensor_type") \
    .agg(
        count("*").alias("total_readings"),
        min("reading_timestamp").alias("first_reading"),
        max("reading_timestamp").alias("last_reading"),
        avg("reading_value").alias("avg_reading"),
        stddev("reading_value").alias("stddev_reading"),
        avg("quality_score").alias("avg_quality_score"),
        first("latitude").alias("sensor_latitude"),
        first("longitude").alias("sensor_longitude"),
        first("h3_index_7").alias("sensor_h3_region")
    ) \
    .withColumn("days_active", 
        datediff(col("last_reading"), col("first_reading"))
    ) \
    .withColumn("readings_per_day", 
        col("total_readings") / (col("days_active") + 1)
    ) \
    .withColumn("updated_timestamp", current_timestamp())

# Write to Gold
df_gold_sensors.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.sensor_performance")

display(df_gold_sensors.orderBy(col("total_readings").desc()).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 3: Anomaly Detection - Hotspots

# COMMAND ----------

# Identify hotspots (areas with concerning readings)
df_gold_hotspots = df_silver_read \
    .withColumn("is_anomaly",
        when(
            (col("sensor_type") == "air_quality") & (col("reading_value") > 150), True
        ).when(
            (col("sensor_type") == "noise") & (col("reading_value") > 85), True
        ).when(
            (col("sensor_type") == "traffic") & (col("reading_value") > 80), True
        ).otherwise(False)
    ) \
    .filter(col("is_anomaly") == True) \
    .withColumn("reading_date", to_date("reading_timestamp")) \
    .groupBy("h3_index_7", "sensor_type", "reading_date") \
    .agg(
        count("*").alias("anomaly_count"),
        avg("reading_value").alias("avg_anomaly_value"),
        max("reading_value").alias("max_anomaly_value"),
        collect_list("sensor_id").alias("affected_sensors")
    ) \
    .withColumn("severity",
        when(col("anomaly_count") > 100, "CRITICAL")
        .when(col("anomaly_count") > 50, "HIGH")
        .when(col("anomaly_count") > 20, "MEDIUM")
        .otherwise("LOW")
    ) \
    .withColumn("detected_timestamp", current_timestamp())

# Write to Gold
df_gold_hotspots.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("reading_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.anomaly_hotspots")

display(df_gold_hotspots.filter(col("severity").isin("CRITICAL", "HIGH")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Processing with Delta Lake
# MAGIC
# MAGIC ### Using Change Data Feed for Silver Updates

# COMMAND ----------

# Example: Incremental processing from Bronze to Silver
# In production, this would run on a schedule

from delta.tables import DeltaTable

# Check if Silver table exists
if DeltaTable.isDeltaTable(spark, "spark_catalog.geospatial_demo.silver.sensor_readings_standardized"):
    # Get last processed version
    last_version = spark.sql("""
        SELECT MAX(bronze_id) as max_id 
        FROM silver.sensor_readings_standardized
    """).collect()[0]["max_id"]
    
    if last_version is None:
        last_version = 0
    
    print(f"Last processed bronze_id: {last_version}")
    
    # Read only new records from Bronze
    df_new_bronze = spark.read.table("bronze.sensor_readings_raw") \
        .filter(col("bronze_id") > last_version)
    
    new_count = df_new_bronze.count()
    print(f"New records to process: {new_count}")
    
    if new_count > 0:
        # Apply Silver transformations
        df_new_silver = df_new_bronze \
            .withColumn("geometry", expr("ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20)))")) \
            .withColumn("is_valid_geometry", expr("ST_IsValid(geometry)")) \
            .withColumn("h3_index_7", h3_udf(col("latitude"), col("longitude"), lit(7))) \
            .withColumn("h3_index_9", h3_udf(col("latitude"), col("longitude"), lit(9))) \
            .withColumn("h3_index_11", h3_udf(col("latitude"), col("longitude"), lit(11))) \
            .withColumn("quality_score", 
                when(col("is_valid_geometry") & col("h3_index_7").isNotNull() & 
                     col("reading_value").isNotNull(), 100)
                .when(col("is_valid_geometry") & col("h3_index_7").isNotNull(), 80)
                .otherwise(60)
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .filter(col("quality_score") >= 60)
        
        # Append to Silver
        df_new_silver.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("silver.sensor_readings_standardized")
        
        print(f"✅ Processed {df_new_silver.count()} new records to Silver")
else:
    print("Silver table doesn't exist yet - this is expected on first run")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Gold Updates

# COMMAND ----------

# Incremental update for Gold layer
# Read latest Silver data (last 24 hours)
df_silver_latest = spark.read.table("silver.sensor_readings_standardized") \
    .filter(col("processing_timestamp") >= current_timestamp() - expr("INTERVAL 24 HOURS"))

if df_silver_latest.count() > 0:
    # Update regional metrics
    df_gold_update = df_silver_latest \
        .withColumn("reading_date", to_date("reading_timestamp")) \
        .withColumn("reading_hour", hour("reading_timestamp")) \
        .groupBy("h3_index_7", "sensor_type", "reading_date", "reading_hour") \
        .agg(
            count("*").alias("reading_count"),
            avg("reading_value").alias("avg_reading"),
            min("reading_value").alias("min_reading"),
            max("reading_value").alias("max_reading"),
            stddev("reading_value").alias("stddev_reading"),
            countDistinct("sensor_id").alias("unique_sensors"),
            avg("quality_score").alias("avg_quality_score")
        ) \
        .withColumn("created_timestamp", current_timestamp())
    
    # Use merge for upsert
    from delta.tables import DeltaTable
    
    gold_table = DeltaTable.forName(spark, "gold.hourly_regional_metrics")
    
    gold_table.alias("target").merge(
        df_gold_update.alias("source"),
        """
        target.h3_index_7 = source.h3_index_7 AND
        target.sensor_type = source.sensor_type AND
        target.reading_date = source.reading_date AND
        target.reading_hour = source.reading_hour
        """
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print("✅ Gold layer updated incrementally")
else:
    print("No new Silver data to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Monitoring

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table statistics
# MAGIC SELECT 
# MAGIC   'Bronze' as layer,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT ingest_date) as partition_count
# MAGIC FROM bronze.sensor_readings_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Silver' as layer,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT h3_index_7) as partition_count
# MAGIC FROM silver.sensor_readings_standardized
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Gold-Regional' as layer,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT reading_date) as partition_count
# MAGIC FROM gold.hourly_regional_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Performance Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 1: Count sensors by type (should use statistics)
# MAGIC SELECT sensor_type, COUNT(*) as sensor_count
# MAGIC FROM silver.sensor_readings_standardized
# MAGIC GROUP BY sensor_type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 2: Find sensors in specific region (benefits from H3 partitioning)
# MAGIC SELECT sensor_id, sensor_type, AVG(reading_value) as avg_reading
# MAGIC FROM silver.sensor_readings_standardized
# MAGIC WHERE h3_index_7 = '87283082fffffff'  -- Replace with actual H3 from your data
# MAGIC GROUP BY sensor_id, sensor_type
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Dashboard

# COMMAND ----------

# Create quality metrics
quality_metrics = spark.sql("""
    SELECT 
        'Silver' as layer,
        COUNT(*) as total_records,
        AVG(quality_score) as avg_quality_score,
        SUM(CASE WHEN quality_score >= 80 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as high_quality_pct,
        SUM(CASE WHEN is_valid_geometry THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as valid_geometry_pct,
        COUNT(DISTINCT sensor_id) as unique_sensors,
        COUNT(DISTINCT h3_index_7) as unique_regions
    FROM silver.sensor_readings_standardized
""")

display(quality_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Maintenance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set table properties for automatic maintenance
# MAGIC ALTER TABLE bronze.sensor_readings_raw
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE silver.sensor_readings_standardized
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Bronze Layer
# MAGIC ✅ Preserve raw data exactly as received
# MAGIC ✅ Add metadata for lineage (source, timestamp)
# MAGIC ✅ Partition by ingestion date for easy management
# MAGIC ✅ Enable Change Data Feed for incremental processing
# MAGIC ✅ Set appropriate retention policies
# MAGIC
# MAGIC ### Silver Layer
# MAGIC ✅ Standardize CRS to EPSG:4326
# MAGIC ✅ Add H3 indexes for spatial partitioning
# MAGIC ✅ Validate geometries and assign quality scores
# MAGIC ✅ Deduplicate based on business keys
# MAGIC ✅ Partition by H3 for uniform distribution
# MAGIC ✅ Z-order by spatial and temporal columns
# MAGIC ✅ Add Bloom filters for ID lookups
# MAGIC
# MAGIC ### Gold Layer
# MAGIC ✅ Pre-aggregate for specific use cases
# MAGIC ✅ Denormalize for query performance
# MAGIC ✅ Partition by frequently filtered columns
# MAGIC ✅ Use MERGE for upsert patterns
# MAGIC ✅ Document table purpose and SLAs
# MAGIC
# MAGIC ### Optimization
# MAGIC ✅ Enable Auto Optimize for automatic maintenance
# MAGIC ✅ Use Liquid Clustering for dynamic workloads
# MAGIC ✅ Monitor table statistics regularly
# MAGIC ✅ Vacuum old files periodically
# MAGIC ✅ Set appropriate retention durations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Medallion architecture** provides clear data quality progression and separation of concerns
# MAGIC
# MAGIC 2. **Bronze layer** preserves raw data with metadata for full lineage and time travel
# MAGIC
# MAGIC 3. **Silver layer** standardizes CRS, validates geometries, and adds H3 indexes for efficient querying
# MAGIC
# MAGIC 4. **Gold layer** provides business-ready analytics with pre-aggregated metrics
# MAGIC
# MAGIC 5. **Delta Lake features** (Change Data Feed, OPTIMIZE, ZORDER) enable efficient incremental processing
# MAGIC
# MAGIC 6. **H3 partitioning** provides uniform data distribution better than geographic boundaries
# MAGIC
# MAGIC 7. **Quality gates** at each layer ensure data quality improves through the pipeline
# MAGIC
# MAGIC 8. **Incremental processing** reduces compute costs and enables near-real-time updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Continue to **Demo 2: Delta Lake Optimization for Spatial Queries**
# MAGIC
# MAGIC You'll learn:
# MAGIC - Advanced Z-ordering strategies
# MAGIC - Bloom filter optimization
# MAGIC - Liquid Clustering setup
# MAGIC - Query performance benchmarking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You've implemented a complete medallion architecture for geospatial data.
