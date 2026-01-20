# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Schema Evolution and Data Transformations
# MAGIC
# MAGIC ## Overview
# MAGIC In this lab, you'll learn how to handle evolving event schemas in Zerobus ingestion pipelines. Real-world applications frequently add new fields, change data types, or restructure event payloads. You'll implement automatic schema detection, merging strategies, data quality checks, and transformations using Delta Live Tables.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Handle schema evolution as event structures change over time
# MAGIC - Configure automatic schema merging in Delta tables
# MAGIC - Add and modify columns without breaking existing pipelines
# MAGIC - Implement data quality constraints and validation rules
# MAGIC - Transform streaming data using Delta Live Tables (DLT)
# MAGIC - Handle schema mismatches and incompatible data types
# MAGIC - Create bronze, silver, and gold layers for streaming data
# MAGIC - Monitor schema changes and data quality metrics
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Labs 2 and 3
# MAGIC - Understanding of Delta Lake table properties
# MAGIC - Basic knowledge of Delta Live Tables
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Understanding Schema Evolution in Delta Lake
# MAGIC
# MAGIC Delta Lake supports schema evolution through table properties:
# MAGIC - **Schema Merge**: Automatically add new columns
# MAGIC - **Schema Overwrite**: Replace entire schema (destructive)
# MAGIC - **Type Widening**: Promote compatible types (int → long, float → double)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current schema of sensor_events table
# MAGIC DESCRIBE EXTENDED zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table properties related to schema
# MAGIC SHOW TBLPROPERTIES zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Enable Schema Evolution
# MAGIC
# MAGIC Let's configure our table to automatically handle schema changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable schema evolution features
# MAGIC ALTER TABLE zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Column mapping allows column renaming and type changes without rewriting data files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Send Events with New Fields
# MAGIC
# MAGIC Let's simulate schema evolution by sending events with additional fields.

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone
import random

# Configuration
ENDPOINT_URL = dbutils.widgets.get("endpoint_url") if dbutils.widgets.get("endpoint_url") else ""
AUTH_TOKEN = dbutils.widgets.get("auth_token") if dbutils.widgets.get("auth_token") else ""

if not ENDPOINT_URL or not AUTH_TOKEN:
    dbutils.widgets.text("endpoint_url", "", "Zerobus Endpoint URL")
    dbutils.widgets.text("auth_token", "", "Authentication Token")
    print("⚠️  Please enter your endpoint URL and authentication token!")
else:
    print("✅ Configuration loaded")

# COMMAND ----------

# Original schema events
original_events = [
    {
        "sensor_id": "EVOLVE-001",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": 22.5,
        "humidity": 45.0,
        "pressure": 1013.25,
        "location": "Building A - Floor 1",
        "ingestion_time": datetime.now(timezone.utc).isoformat()
    }
]

# New schema events with additional fields
evolved_events = [
    {
        "sensor_id": "EVOLVE-002",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": 23.1,
        "humidity": 42.5,
        "pressure": 1012.80,
        "location": "Building A - Floor 2",
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        # NEW FIELDS
        "battery_level": 85.5,
        "signal_strength": -45,
        "firmware_version": "2.1.0",
        "device_status": "active"
    }
]

# Function to send events
def send_events(events, description):
    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    
    print(f"\n{description}")
    print(f"Event structure: {json.dumps(events[0], indent=2)}")
    print("-" * 80)
    
    try:
        response = requests.post(
            ENDPOINT_URL,
            headers=headers,
            json=events,
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        
        if response.status_code == 200:
            print("✅ Events sent successfully")
        else:
            print(f"❌ Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")

# Send original format
send_events(original_events, "Sending ORIGINAL schema events:")

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️ Note:** Zerobus will reject events with new fields by default. We need to use schema merging options.
# MAGIC
# MAGIC For Zerobus to accept new fields, we have two approaches:
# MAGIC 1. Pre-add columns to the table schema
# MAGIC 2. Use a staging pattern with a flexible schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Pre-Add New Columns
# MAGIC
# MAGIC Let's add the new columns to our table schema proactively.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add new columns to the table
# MAGIC ALTER TABLE zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC ADD COLUMNS (
# MAGIC   battery_level DOUBLE COMMENT 'Battery level percentage (0-100)',
# MAGIC   signal_strength INT COMMENT 'Signal strength in dBm',
# MAGIC   firmware_version STRING COMMENT 'Device firmware version',
# MAGIC   device_status STRING COMMENT 'Current device status'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify new schema
# MAGIC DESCRIBE zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# Now send events with new fields
send_events(evolved_events, "Sending EVOLVED schema events (after ALTER TABLE):")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Schema Evolution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check that new fields are populated
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   timestamp,
# MAGIC   temperature,
# MAGIC   battery_level,
# MAGIC   signal_strength,
# MAGIC   firmware_version,
# MAGIC   device_status
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE sensor_id LIKE 'EVOLVE-%'
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify that old records have NULL for new columns
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   timestamp,
# MAGIC   temperature,
# MAGIC   battery_level,
# MAGIC   signal_strength,
# MAGIC   firmware_version,
# MAGIC   device_status,
# MAGIC   CASE 
# MAGIC     WHEN battery_level IS NULL THEN 'Old Schema'
# MAGIC     ELSE 'New Schema'
# MAGIC   END as schema_version
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Schema-Flexible Bronze Table
# MAGIC
# MAGIC For maximum flexibility, create a bronze table that accepts any JSON structure.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create flexible bronze table with JSON storage
# MAGIC CREATE TABLE IF NOT EXISTS zerobus_workshop.streaming_ingestion.sensor_events_bronze (
# MAGIC   event_id STRING NOT NULL,
# MAGIC   event_data STRING NOT NULL COMMENT 'Raw JSON event payload',
# MAGIC   ingestion_time TIMESTAMP NOT NULL,
# MAGIC   source_system STRING COMMENT 'Source system identifier'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(ingestion_time))
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )
# MAGIC COMMENT 'Bronze layer - Raw JSON events from any source';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Implement Delta Live Tables Pipeline
# MAGIC
# MAGIC Delta Live Tables (DLT) provides declarative ETL with built-in quality checks. Let's create a DLT pipeline for our streaming data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Pipeline Definition
# MAGIC
# MAGIC Below is a complete DLT pipeline. To use it:
# MAGIC 1. Create a new DLT pipeline in your workspace
# MAGIC 2. Add this notebook as a source
# MAGIC 3. Configure target catalog/schema
# MAGIC 4. Start the pipeline

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Note: This section only runs in DLT context
# For this lab, we'll show the code structure

# Bronze Layer - Raw ingestion
@dlt.table(
    name="sensor_bronze",
    comment="Bronze layer - Raw sensor events from Zerobus",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "sensor_id"
    }
)
def sensor_bronze():
    return (
        spark.readStream
        .table("zerobus_workshop.streaming_ingestion.sensor_events")
    )

# Silver Layer - Cleaned and validated
@dlt.table(
    name="sensor_silver",
    comment="Silver layer - Validated and enriched sensor data",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_sensor_id", "sensor_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_temperature", "temperature BETWEEN -50 AND 150")
@dlt.expect_or_drop("valid_humidity", "humidity BETWEEN 0 AND 100")
@dlt.expect_or_drop("valid_pressure", "pressure BETWEEN 900 AND 1100")
def sensor_silver():
    return (
        dlt.read_stream("sensor_bronze")
        .withColumn("processing_time", current_timestamp())
        .withColumn("event_date", to_date(col("timestamp")))
        .withColumn("event_hour", hour(col("timestamp")))
        # Handle NULL values in new fields
        .withColumn("battery_level", coalesce(col("battery_level"), lit(100.0)))
        .withColumn("device_status", coalesce(col("device_status"), lit("unknown")))
        # Add derived fields
        .withColumn("temp_fahrenheit", col("temperature") * 9/5 + 32)
        .withColumn("needs_maintenance", 
                   when(col("battery_level") < 20, lit(True))
                   .otherwise(lit(False)))
    )

# Gold Layer - Aggregated metrics
@dlt.table(
    name="sensor_gold_hourly",
    comment="Gold layer - Hourly aggregated sensor metrics"
)
def sensor_gold_hourly():
    return (
        dlt.read_stream("sensor_silver")
        .groupBy(
            window(col("timestamp"), "1 hour"),
            col("sensor_id"),
            col("location")
        )
        .agg(
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("pressure").alias("avg_pressure"),
            avg("battery_level").alias("avg_battery_level"),
            count("*").alias("event_count")
        )
        .select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            col("sensor_id"),
            col("location"),
            col("avg_temperature"),
            col("min_temperature"),
            col("max_temperature"),
            col("avg_humidity"),
            col("avg_pressure"),
            col("avg_battery_level"),
            col("event_count")
        )
    )

print("✅ DLT pipeline definitions created")
print("📝 To use: Create a DLT pipeline and add this notebook as a source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Manual Transformation Without DLT
# MAGIC
# MAGIC If DLT is not available, we can implement transformations using standard Structured Streaming.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create silver table
spark.sql("""
CREATE TABLE IF NOT EXISTS zerobus_workshop.streaming_ingestion.sensor_events_silver (
  sensor_id STRING,
  timestamp TIMESTAMP,
  event_date DATE,
  event_hour INT,
  temperature DOUBLE,
  temp_fahrenheit DOUBLE,
  humidity DOUBLE,
  pressure DOUBLE,
  location STRING,
  battery_level DOUBLE,
  signal_strength INT,
  firmware_version STRING,
  device_status STRING,
  needs_maintenance BOOLEAN,
  processing_time TIMESTAMP,
  ingestion_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
""")

print("✅ Silver table created")

# COMMAND ----------

# Read from bronze (sensor_events) and write to silver
# This would run as a continuous streaming job

bronze_df = (
    spark.readStream
    .table("zerobus_workshop.streaming_ingestion.sensor_events")
)

silver_df = (
    bronze_df
    # Add derived columns
    .withColumn("event_date", to_date(col("timestamp")))
    .withColumn("event_hour", hour(col("timestamp")))
    .withColumn("processing_time", current_timestamp())
    # Handle NULLs
    .withColumn("battery_level", coalesce(col("battery_level"), lit(100.0)))
    .withColumn("device_status", coalesce(col("device_status"), lit("unknown")))
    .withColumn("signal_strength", coalesce(col("signal_strength"), lit(0)))
    .withColumn("firmware_version", coalesce(col("firmware_version"), lit("unknown")))
    # Add business logic
    .withColumn("temp_fahrenheit", col("temperature") * 9/5 + 32)
    .withColumn("needs_maintenance", 
               when(col("battery_level") < 20, lit(True))
               .otherwise(lit(False)))
    # Filter invalid data
    .filter(col("sensor_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter((col("temperature") >= -50) & (col("temperature") <= 150))
    .filter((col("humidity") >= 0) & (col("humidity") <= 100))
)

# Display sample (for notebook demo - in production use writeStream)
display(silver_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Implement Data Quality Checks
# MAGIC
# MAGIC Let's add explicit data quality validation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add CHECK constraints to enforce data quality
# MAGIC ALTER TABLE zerobus_workshop.streaming_ingestion.sensor_events_silver
# MAGIC ADD CONSTRAINT valid_temperature CHECK (temperature BETWEEN -50 AND 150);
# MAGIC
# MAGIC ALTER TABLE zerobus_workshop.streaming_ingestion.sensor_events_silver
# MAGIC ADD CONSTRAINT valid_humidity CHECK (humidity BETWEEN 0 AND 100);
# MAGIC
# MAGIC ALTER TABLE zerobus_workshop.streaming_ingestion.sensor_events_silver
# MAGIC ADD CONSTRAINT valid_battery CHECK (battery_level BETWEEN 0 AND 100);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all constraints on the table
# MAGIC SHOW TBLPROPERTIES zerobus_workshop.streaming_ingestion.sensor_events_silver;

# COMMAND ----------

# Python-based quality checks
def validate_sensor_event(event_df):
    """
    Apply data quality checks and return quality metrics.
    """
    from pyspark.sql.functions import col, count, when
    
    quality_checks = event_df.select(
        count("*").alias("total_records"),
        
        # Completeness checks
        count(when(col("sensor_id").isNull(), 1)).alias("missing_sensor_id"),
        count(when(col("timestamp").isNull(), 1)).alias("missing_timestamp"),
        count(when(col("temperature").isNull(), 1)).alias("missing_temperature"),
        
        # Validity checks
        count(when((col("temperature") < -50) | (col("temperature") > 150), 1)).alias("invalid_temperature"),
        count(when((col("humidity") < 0) | (col("humidity") > 100), 1)).alias("invalid_humidity"),
        count(when((col("pressure") < 900) | (col("pressure") > 1100), 1)).alias("invalid_pressure"),
        
        # Consistency checks
        count(when(col("ingestion_time") < col("timestamp"), 1)).alias("future_events"),
        
        # Battery health
        count(when(col("battery_level") < 20, 1)).alias("low_battery_count"),
        count(when(col("battery_level") < 10, 1)).alias("critical_battery_count")
    ).collect()[0]
    
    return quality_checks

# Run quality checks on recent data
recent_events = spark.read.table("zerobus_workshop.streaming_ingestion.sensor_events") \
    .where("ingestion_time >= current_timestamp() - INTERVAL 1 HOUR")

if recent_events.count() > 0:
    quality_results = validate_sensor_event(recent_events)
    
    print("DATA QUALITY REPORT")
    print("=" * 80)
    print(f"Total records: {quality_results['total_records']}")
    print(f"\nCompleteness:")
    print(f"  Missing sensor_id: {quality_results['missing_sensor_id']}")
    print(f"  Missing timestamp: {quality_results['missing_timestamp']}")
    print(f"  Missing temperature: {quality_results['missing_temperature']}")
    print(f"\nValidity:")
    print(f"  Invalid temperature: {quality_results['invalid_temperature']}")
    print(f"  Invalid humidity: {quality_results['invalid_humidity']}")
    print(f"  Invalid pressure: {quality_results['invalid_pressure']}")
    print(f"\nConsistency:")
    print(f"  Future events: {quality_results['future_events']}")
    print(f"\nDevice Health:")
    print(f"  Low battery (<20%): {quality_results['low_battery_count']}")
    print(f"  Critical battery (<10%): {quality_results['critical_battery_count']}")
else:
    print("No recent events to analyze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Handle Type Changes
# MAGIC
# MAGIC Let's test type widening (e.g., int → long, float → double).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current data type
# MAGIC DESCRIBE zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake supports automatic type widening for compatible types:
# MAGIC - `BYTE` → `SHORT` → `INT` → `LONG`
# MAGIC - `FLOAT` → `DOUBLE`
# MAGIC - `DATE` → `TIMESTAMP`

# COMMAND ----------

# Send event with larger numeric values to test type widening
large_value_event = {
    "sensor_id": "TYPE-TEST-001",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "temperature": 25.123456789,  # High precision
    "humidity": 55.0,
    "pressure": 1015.0,
    "location": "Type Test Lab",
    "ingestion_time": datetime.now(timezone.utc).isoformat(),
    "battery_level": 90.5,
    "signal_strength": -35,
    "firmware_version": "3.0.0",
    "device_status": "active"
}

headers = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Content-Type": "application/json"
}

print("Sending event with high-precision values...")
response = requests.post(ENDPOINT_URL, headers=headers, json=[large_value_event], timeout=30)
print(f"Status: {response.status_code}")
print(f"Response: {response.json() if response.status_code == 200 else response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Create Quarantine Table for Invalid Data
# MAGIC
# MAGIC Handle schema violations by routing invalid events to a quarantine table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create quarantine table for rejected events
# MAGIC CREATE TABLE IF NOT EXISTS zerobus_workshop.streaming_ingestion.sensor_events_quarantine (
# MAGIC   event_payload STRING COMMENT 'Original JSON payload',
# MAGIC   rejection_reason STRING COMMENT 'Why this event was quarantined',
# MAGIC   rejection_time TIMESTAMP COMMENT 'When event was rejected',
# MAGIC   source_table STRING COMMENT 'Intended destination table'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(rejection_time))
# MAGIC COMMENT 'Quarantine table for invalid or rejected events';

# COMMAND ----------

# Function to quarantine invalid events
def quarantine_event(event_json, reason):
    """Store invalid event in quarantine table."""
    quarantine_record = spark.createDataFrame([{
        "event_payload": json.dumps(event_json),
        "rejection_reason": reason,
        "rejection_time": datetime.now(timezone.utc).isoformat(),
        "source_table": "sensor_events"
    }])
    
    quarantine_record.write.mode("append").saveAsTable(
        "zerobus_workshop.streaming_ingestion.sensor_events_quarantine"
    )
    
print("✅ Quarantine function defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Monitor Schema Changes Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track schema evolution using table history
# MAGIC DESCRIBE HISTORY zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get schema at different versions
# MAGIC SELECT 
# MAGIC   version,
# MAGIC   timestamp,
# MAGIC   operation,
# MAGIC   operationParameters
# MAGIC FROM (DESCRIBE HISTORY zerobus_workshop.streaming_ingestion.sensor_events)
# MAGIC WHERE operation IN ('ADD COLUMNS', 'CHANGE COLUMN', 'CREATE TABLE')
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ Enabled schema evolution in Delta Lake tables  
# MAGIC ✅ Added new columns without breaking existing pipelines  
# MAGIC ✅ Sent events with evolved schemas  
# MAGIC ✅ Created bronze, silver, and gold layer tables  
# MAGIC ✅ Implemented data quality checks and constraints  
# MAGIC ✅ Built Delta Live Tables transformations  
# MAGIC ✅ Handled type changes and NULL values  
# MAGIC ✅ Created quarantine table for invalid data  
# MAGIC
# MAGIC ### Schema Evolution Best Practices
# MAGIC
# MAGIC **1. Additive Changes (Safe):**
# MAGIC - Add new optional columns
# MAGIC - Provide default values for backward compatibility
# MAGIC - Use NULL for historical records
# MAGIC
# MAGIC **2. Type Changes (Careful):**
# MAGIC - Only widen types (int → long, float → double)
# MAGIC - Never narrow types (will fail)
# MAGIC - Test thoroughly before production
# MAGIC
# MAGIC **3. Breaking Changes (Avoid):**
# MAGIC - Don't rename columns (use column mapping)
# MAGIC - Don't delete columns with data
# MAGIC - Don't change semantics of existing fields
# MAGIC
# MAGIC **4. Data Quality:**
# MAGIC - Implement CHECK constraints for validation
# MAGIC - Use Delta Live Tables expectations
# MAGIC - Route invalid data to quarantine tables
# MAGIC - Monitor quality metrics continuously
# MAGIC
# MAGIC ### Layered Architecture Benefits
# MAGIC
# MAGIC **Bronze Layer:**
# MAGIC - Raw, immutable data
# MAGIC - Schema flexibility
# MAGIC - Complete audit trail
# MAGIC
# MAGIC **Silver Layer:**
# MAGIC - Validated and cleaned
# MAGIC - Enriched with derived fields
# MAGIC - Business logic applied
# MAGIC
# MAGIC **Gold Layer:**
# MAGIC - Aggregated metrics
# MAGIC - Optimized for analytics
# MAGIC - Ready for dashboards and ML
# MAGIC
# MAGIC ### Next Lab
# MAGIC
# MAGIC Proceed to **Lab 5: Monitoring and Managing Zerobus Ingestion Pipelines** to learn:
# MAGIC - Query Zerobus system tables
# MAGIC - Monitor ingestion performance
# MAGIC - Set up alerts and notifications
# MAGIC - Troubleshoot common issues
# MAGIC - Performance tuning and optimization
