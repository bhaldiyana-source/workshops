# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 08: Complete Streaming ETL Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC Build an end-to-end production-ready streaming ETL pipeline using the Bronze-Silver-Gold medallion architecture.
# MAGIC
# MAGIC **Duration**: 30 minutes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline Architecture
# MAGIC
# MAGIC ### Medallion Architecture
# MAGIC
# MAGIC **Bronze Layer** (Raw Data)
# MAGIC - Ingest data as-is from source
# MAGIC - Minimal transformation
# MAGIC - Full audit trail
# MAGIC
# MAGIC **Silver Layer** (Cleaned & Validated)
# MAGIC - Data quality checks
# MAGIC - Deduplication
# MAGIC - Schema enforcement
# MAGIC - Business logic
# MAGIC
# MAGIC **Gold Layer** (Business Aggregates)
# MAGIC - Aggregated metrics
# MAGIC - Business KPIs
# MAGIC - Ready for analytics/ML

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup: Generate Sample Clickstream Data

# COMMAND ----------

# Create sample clickstream data
import json
import random

sample_events = []
for i in range(50):
    event = {
        "event_id": f"evt_{i:05d}",
        "user_id": random.randint(1, 20),
        "session_id": f"sess_{random.randint(1, 10):03d}",
        "event_type": random.choice(["page_view", "click", "purchase"]),
        "page_url": random.choice(["/home", "/products", "/cart", "/checkout"]),
        "timestamp": f"2024-01-10T10:{i//2:02d}:{(i%2)*30:02d}",
        "user_agent": "Mozilla/5.0",
        "ip_address": f"192.168.1.{random.randint(1, 255)}"
    }
    sample_events.append(event)
    
    # Write to files
    dbutils.fs.put(f"/tmp/clickstream/raw/batch_{i:03d}.json", 
                   json.dumps(event), overwrite=True)

print(f"✅ Generated {len(sample_events)} sample events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. BRONZE LAYER - Raw Ingestion

# COMMAND ----------

# DBTITLE 1,Bronze: Ingest Raw Data
# Schema for incoming data
clickstream_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("page_url", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_agent", StringType()),
    StructField("ip_address", StringType())
])

# Read raw data
bronze_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(clickstream_schema)
    .load("/tmp/clickstream/raw/"))

# Add ingestion metadata
bronze_with_metadata = (bronze_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name()))

# Write to Bronze Delta table
bronze_query = (bronze_with_metadata
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/bronze")
    .trigger(processingTime="10 seconds")
    .start("/tmp/delta/clickstream_bronze"))

print("✅ Bronze layer ingestion started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SILVER LAYER - Cleaned & Validated

# COMMAND ----------

# DBTITLE 1,Silver: Clean and Validate
import time
time.sleep(15)  # Let bronze ingest some data
bronze_query.stop()

# Read from Bronze
silver_df = (spark.readStream
    .format("delta")
    .load("/tmp/delta/clickstream_bronze"))

# Cleansing and validation
silver_cleaned = (silver_df
    # Data quality: filter nulls
    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("event_type").isNotNull())
    
    # Parse timestamp
    .withColumn("event_time", to_timestamp(col("timestamp")))
    
    # Derive additional fields
    .withColumn("event_date", to_date(col("event_time")))
    .withColumn("event_hour", hour(col("event_time")))
    
    # Categorize events
    .withColumn("is_conversion", 
        when(col("event_type") == "purchase", True).otherwise(False))
    
    # Extract domain from page_url
    .withColumn("page_category",
        when(col("page_url").like("%/home%"), "home")
        .when(col("page_url").like("%/products%"), "catalog")
        .when(col("page_url").like("%/cart%"), "cart")
        .when(col("page_url").like("%/checkout%"), "checkout")
        .otherwise("other"))
)

# Deduplication
silver_deduped = (silver_cleaned
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["event_id"]))

# Write to Silver Delta table
silver_query = (silver_deduped
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/silver")
    .trigger(processingTime="10 seconds")
    .start("/tmp/delta/clickstream_silver"))

print("✅ Silver layer processing started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. GOLD LAYER - Business Aggregates

# COMMAND ----------

# DBTITLE 1,Gold: Aggregate Metrics
time.sleep(15)
silver_query.stop()

# Read from Silver
gold_df = (spark.readStream
    .format("delta")
    .load("/tmp/delta/clickstream_silver"))

# Aggregate metrics
gold_metrics = (gold_df
    .withWatermark("event_time", "30 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("page_category"),
        col("event_type")
    )
    .agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions"),
        sum(when(col("is_conversion"), 1).otherwise(0)).alias("conversions")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("page_category"),
        col("event_type"),
        col("event_count"),
        col("unique_users"),
        col("unique_sessions"),
        col("conversions"),
        (col("conversions") / col("event_count") * 100).alias("conversion_rate_pct")
    ))

# Write to Gold Delta table
gold_query = (gold_metrics
    .writeStream
    .format("delta")
    .outputMode("update")  # Update existing aggregations
    .option("checkpointLocation", "/tmp/checkpoints/gold")
    .trigger(processingTime="10 seconds")
    .start("/tmp/delta/clickstream_gold"))

print("✅ Gold layer aggregation started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Query Results

# COMMAND ----------

time.sleep(20)

# Query Bronze layer
bronze_count = spark.read.format("delta").load("/tmp/delta/clickstream_bronze").count()
print(f"Bronze Layer: {bronze_count} raw events")

# Query Silver layer
silver_count = spark.read.format("delta").load("/tmp/delta/clickstream_silver").count()
print(f"Silver Layer: {silver_count} cleaned events")

# Query Gold layer
gold_results = spark.read.format("delta").load("/tmp/delta/clickstream_gold")
print(f"Gold Layer: {gold_results.count()} aggregated metrics")
display(gold_results.orderBy("window_start", "page_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pipeline Monitoring

# COMMAND ----------

# Check all query statuses
print("=== Pipeline Status ===\n")

for query in spark.streams.active:
    print(f"Query: {query.name}")
    print(f"  Status: {'Active' if query.isActive else 'Stopped'}")
    progress = query.lastProgress
    if progress:
        print(f"  Last Batch: {progress['batchId']}")
        print(f"  Input Rows: {progress['numInputRows']}")
        print(f"  Processing Rate: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pipeline Orchestration Function

# COMMAND ----------

def run_streaming_pipeline(source_path, checkpoint_base, output_base):
    """
    Run complete Bronze-Silver-Gold pipeline
    """
    try:
        print("🚀 Starting streaming pipeline...")
        
        # Bronze: Ingest
        print("📥 Bronze: Ingesting raw data...")
        bronze_query = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(clickstream_schema)
            .load(source_path)
            .withColumn("ingestion_timestamp", current_timestamp())
            .writeStream
            .format("delta")
            .option("checkpointLocation", f"{checkpoint_base}/bronze")
            .start(f"{output_base}/bronze"))
        
        # Silver: Clean
        print("🧹 Silver: Cleaning and validating...")
        silver_query = (spark.readStream
            .format("delta")
            .load(f"{output_base}/bronze")
            .filter(col("event_id").isNotNull())
            .withColumn("event_time", to_timestamp(col("timestamp")))
            .withWatermark("event_time", "1 hour")
            .dropDuplicates(["event_id"])
            .writeStream
            .format("delta")
            .option("checkpointLocation", f"{checkpoint_base}/silver")
            .start(f"{output_base}/silver"))
        
        # Gold: Aggregate
        print("📊 Gold: Aggregating metrics...")
        gold_query = (spark.readStream
            .format("delta")
            .load(f"{output_base}/silver")
            .withWatermark("event_time", "30 minutes")
            .groupBy(window("event_time", "5 minutes"), "event_type")
            .count()
            .writeStream
            .format("delta")
            .outputMode("update")
            .option("checkpointLocation", f"{checkpoint_base}/gold")
            .start(f"{output_base}/gold"))
        
        print("✅ Pipeline started successfully")
        return [bronze_query, silver_query, gold_query]
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

# Example usage (commented out)
# queries = run_streaming_pipeline(
#     "/tmp/clickstream/raw",
#     "/tmp/checkpoints/pipeline",
#     "/tmp/delta/pipeline"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Checks

# COMMAND ----------

# Data quality validation
def validate_silver_quality():
    """Run data quality checks on Silver layer"""
    silver_data = spark.read.format("delta").load("/tmp/delta/clickstream_silver")
    
    print("=== Data Quality Report ===\n")
    
    # Check for nulls
    null_checks = silver_data.select([
        count(when(col(c).isNull(), 1)).alias(c) 
        for c in ["event_id", "user_id", "event_type"]
    ]).collect()[0]
    
    print("Null Counts:")
    for field, null_count in null_checks.asDict().items():
        status = "✅" if null_count == 0 else "❌"
        print(f"  {status} {field}: {null_count}")
    
    # Check duplicates
    total = silver_data.count()
    unique = silver_data.select("event_id").distinct().count()
    print(f"\nDuplication Check:")
    print(f"  Total: {total}, Unique: {unique}")
    print(f"  {'✅ No duplicates' if total == unique else '❌ Duplicates found'}")
    
    # Check data freshness
    from datetime import datetime, timedelta
    recent = silver_data.filter(
        col("event_time") > (datetime.now() - timedelta(hours=1))
    ).count()
    print(f"\nFreshness Check:")
    print(f"  Events in last hour: {recent}")

validate_silver_quality()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What We Built
# MAGIC
# MAGIC ✅ **Bronze Layer**: Raw data ingestion with audit trail  
# MAGIC ✅ **Silver Layer**: Cleaned, validated, and deduplicated data  
# MAGIC ✅ **Gold Layer**: Business-ready aggregated metrics  
# MAGIC ✅ **Monitoring**: Query status and performance tracking  
# MAGIC ✅ **Data Quality**: Automated validation checks  
# MAGIC ✅ **Pipeline Function**: Reusable orchestration code  
# MAGIC
# MAGIC ### Production Readiness
# MAGIC
# MAGIC This pipeline demonstrates:
# MAGIC - Proper layering and separation of concerns
# MAGIC - Incremental processing and deduplication
# MAGIC - Error handling and monitoring
# MAGIC - Data quality validation
# MAGIC - ACID guarantees with Delta Lake
# MAGIC
# MAGIC ### Next Steps for Production
# MAGIC
# MAGIC 1. Add alerting on failures
# MAGIC 2. Implement auto-recovery
# MAGIC 3. Set up scheduled optimization (OPTIMIZE, VACUUM)
# MAGIC 4. Create dashboards from Gold layer
# MAGIC 5. Document runbook procedures
# MAGIC 6. Load test with production volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Stop all queries
for query in spark.streams.active:
    print(f"Stopping: {query.name}")
    query.stop()

print("\n✅ All queries stopped")

# Optional: Clean up data
# dbutils.fs.rm("/tmp/clickstream", recurse=True)
# dbutils.fs.rm("/tmp/checkpoints", recurse=True)
# dbutils.fs.rm("/tmp/delta", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the Spark Structured Streaming Workshop!
# MAGIC
# MAGIC You now know how to:
# MAGIC - Build streaming pipelines with PySpark
# MAGIC - Handle various sources and sinks
# MAGIC - Apply transformations and aggregations
# MAGIC - Manage state and handle late data
# MAGIC - Monitor and optimize streaming queries
# MAGIC - Deploy production-ready pipelines
# MAGIC
# MAGIC ### Keep Learning
# MAGIC - Experiment with your own data sources
# MAGIC - Build custom transformations
# MAGIC - Integrate with ML models
# MAGIC - Explore Delta Live Tables
# MAGIC - Join the Databricks Community!
