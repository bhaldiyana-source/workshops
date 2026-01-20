# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge Lab: Petabyte-Scale Geospatial Processing
# MAGIC
# MAGIC ## Overview
# MAGIC This advanced challenge lab presents real-world scenarios requiring optimization techniques for processing petabyte-scale geospatial datasets. You'll apply all concepts learned to solve complex performance and scalability problems.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Optimize ultra-large dataset processing
# MAGIC - Apply advanced partitioning strategies
# MAGIC - Manage compute resources efficiently
# MAGIC - Balance cost and performance trade-offs
# MAGIC - Implement advanced techniques (bucketing, salting)
# MAGIC
# MAGIC ## Challenge Level
# MAGIC Expert - Open-ended problems with multiple valid solutions
# MAGIC
# MAGIC ## Duration
# MAGIC 60+ minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 1: Process 10B Events with Limited Resources
# MAGIC
# MAGIC **Scenario:** You have 10 billion geospatial events (simulated with sampling) and need to:
# MAGIC - Calculate hourly aggregations by H3 region
# MAGIC - Keep processing time under 30 minutes
# MAGIC - Use a maximum of 100 workers
# MAGIC - Minimize storage costs
# MAGIC
# MAGIC **Constraints:**
# MAGIC - Budget: Limited compute budget
# MAGIC - Latency: Results needed within SLA
# MAGIC - Quality: No data loss acceptable
# MAGIC
# MAGIC **Your approach:**

# COMMAND ----------

# TODO: Design your solution
# Considerations:
# - What partitioning strategy?
# - What file size targets?
# - What optimization techniques?
# - How to parallelize efficiently?

# Your code here:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Solution Approach

# COMMAND ----------

# MAGIC %pip install apache-sedona==1.5.1 h3==3.7.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from sedona.register import SedonaRegistrator
import h3

SedonaRegistrator.registerAll(spark)

# Configuration for large-scale processing
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB

print("✅ Optimized configuration set")

# COMMAND ----------

# Strategy 1: Aggressive partitioning + Z-ordering
# - Partition by date + H3 (level 7)
# - Z-order by H3 level 9 for locality
# - Target 1GB files
# - Use Photon for acceleration

# Simulate processing pipeline
def process_large_scale(sample_size=1000000):
    """
    Scalable processing pipeline
    """
    import random
    from datetime import datetime, timedelta
    
    # Generate sample data
    data = []
    base_time = datetime(2025, 1, 1)
    
    for i in range(sample_size):
        lat = random.uniform(-90, 90)
        lon = random.uniform(-180, 180)
        h3_7 = h3.geo_to_h3(lat, lon, 7)
        h3_9 = h3.geo_to_h3(lat, lon, 9)
        
        data.append({
            'event_id': f'E{i:010d}',
            'latitude': lat,
            'longitude': lon,
            'h3_7': h3_7,
            'h3_9': h3_9,
            'event_timestamp': base_time + timedelta(hours=random.randint(0, 720)),
            'value': random.uniform(0, 1000)
        })
    
    df = spark.createDataFrame(data)
    
    # Add temporal partition
    df = df.withColumn("event_date", to_date("event_timestamp"))
    
    # Write with optimization
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date", "h3_7") \
        .option("dataChange", "false") \
        .saveAsTable("challenge.large_scale_events")
    
    # Optimize
    spark.sql("OPTIMIZE challenge.large_scale_events ZORDER BY (h3_9, event_timestamp)")
    
    return df.count()

# Process
spark.sql("CREATE CATALOG IF NOT EXISTS challenge")
spark.sql("USE CATALOG challenge")
spark.sql("CREATE SCHEMA IF NOT EXISTS default")

records_processed = process_large_scale()
print(f"✅ Processed {records_processed:,} records")

# COMMAND ----------

# Aggregation query
result = spark.sql("""
    SELECT 
        h3_7,
        DATE_TRUNC('hour', event_timestamp) as hour,
        COUNT(*) as event_count,
        AVG(value) as avg_value,
        MIN(value) as min_value,
        MAX(value) as max_value
    FROM challenge.large_scale_events
    GROUP BY h3_7, DATE_TRUNC('hour', event_timestamp)
    ORDER BY hour, event_count DESC
""")

result.write.format("delta").mode("overwrite") \
    .partitionBy("hour") \
    .saveAsTable("challenge.hourly_aggregations")

print(f"✅ Created {result.count():,} hourly aggregations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 2: Real-Time Spatial Join at Scale
# MAGIC
# MAGIC **Scenario:** Join 1B streaming events with 100M POIs in real-time
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Latency < 5 seconds per micro-batch
# MAGIC - Find events within 1km of POIs
# MAGIC - Handle late-arriving data
# MAGIC - No data duplication
# MAGIC
# MAGIC **Your solution:**

# COMMAND ----------

# TODO: Design streaming spatial join
# Considerations:
# - Broadcast or shuffle join?
# - Windowing strategy?
# - State management?
# - Checkpointing?

# Your code here:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Solution Approach

# COMMAND ----------

# Strategy: Spatial partitioning + broadcast
# - Partition both datasets by H3
# - Use broadcast for POI lookups within H3
# - Implement distance calculation only within same H3

def setup_spatial_join():
    """
    Setup optimized spatial join
    """
    # Create sample POI dataset
    poi_data = []
    for i in range(10000):  # Simulate POI dataset
        lat = random.uniform(37.70, 37.80)
        lon = random.uniform(-122.52, -122.35)
        h3_7 = h3.geo_to_h3(lat, lon, 7)
        
        poi_data.append({
            'poi_id': f'POI_{i:06d}',
            'poi_name': f'Location_{i}',
            'latitude': lat,
            'longitude': lon,
            'h3_7': h3_7
        })
    
    df_pois = spark.createDataFrame(poi_data)
    df_pois.write.format("delta").mode("overwrite").saveAsTable("challenge.pois")
    
    # Perform join
    df_events = spark.table("challenge.large_scale_events")
    df_pois_read = spark.table("challenge.pois")
    
    # Join within same H3, then filter by distance
    from pyspark.sql.functions import broadcast
    
    df_joined = df_events.join(
        broadcast(df_pois_read),
        df_events.h3_7 == df_pois_read.h3_7
    ).filter(
        expr("""
            ST_Distance(
                ST_Point(CAST(challenge.large_scale_events.longitude AS DECIMAL(24,20)), 
                         CAST(challenge.large_scale_events.latitude AS DECIMAL(24,20))),
                ST_Point(CAST(challenge.pois.longitude AS DECIMAL(24,20)), 
                         CAST(challenge.pois.latitude AS DECIMAL(24,20)))
            ) <= 1000
        """)
    )
    
    return df_joined.count()

nearby_count = setup_spatial_join()
print(f"✅ Found {nearby_count:,} events near POIs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 3: Cost Optimization for Archive
# MAGIC
# MAGIC **Scenario:** 5-year historical dataset (50TB) rarely queried
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Reduce storage costs by 70%
# MAGIC - Maintain query capability
# MAGIC - Query latency can be 10x slower
# MAGIC - Retain full audit trail
# MAGIC
# MAGIC **Optimization ideas:**

# COMMAND ----------

# TODO: Design cost-optimized archive strategy
# Considerations:
# - Compression codec?
# - Partition strategy for cold data?
# - Vacuum strategy?
# - Shallow clones?

# Your approach:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Solution

# COMMAND ----------

# Strategy: Aggressive compression + compaction + archival tier
# 1. Use ZSTD compression (best ratio)
# 2. Compact old partitions
# 3. Move to cold storage tier
# 4. Keep only critical columns in hot tier

# Example implementation
spark.sql("""
    CREATE TABLE IF NOT EXISTS challenge.events_archive
    USING DELTA
    TBLPROPERTIES (
        'delta.compression' = 'zstd',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    AS SELECT 
        event_id,
        h3_7,
        h3_9,
        event_date,
        event_timestamp,
        value
    FROM challenge.large_scale_events
    WHERE event_date < CURRENT_DATE() - INTERVAL '180' DAY
""")

print("✅ Archive table created with ZSTD compression")

# Vacuum original table
spark.sql("VACUUM challenge.large_scale_events RETAIN 0 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 4: Global Dataset Deduplication
# MAGIC
# MAGIC **Scenario:** Deduplicate 100B records from 50 sources globally
# MAGIC
# MAGIC **Complexity:**
# MAGIC - Records may arrive out of order
# MAGIC - Same event from multiple sources
# MAGIC - Different field names/schemas
# MAGIC - Time zone differences
# MAGIC
# MAGIC **Your deduplication strategy:**

# COMMAND ----------

# TODO: Design deduplication pipeline
# Key decisions:
# - What is the dedup key?
# - Window-based or full scan?
# - How to handle schema variations?
# - Performance optimization?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge 5: Design Your Own
# MAGIC
# MAGIC **Task:** Identify a geospatial processing challenge in your domain and design a solution
# MAGIC
# MAGIC **Document:**
# MAGIC - Problem statement
# MAGIC - Scale (data volume, query frequency)
# MAGIC - Constraints (budget, latency, accuracy)
# MAGIC - Solution architecture
# MAGIC - Optimization techniques applied
# MAGIC - Expected performance
# MAGIC - Cost estimates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation Criteria
# MAGIC
# MAGIC Your solutions will be evaluated on:
# MAGIC
# MAGIC 1. **Correctness** - Does it produce accurate results?
# MAGIC 2. **Scalability** - Can it handle the specified data volume?
# MAGIC 3. **Performance** - Does it meet latency requirements?
# MAGIC 4. **Cost Efficiency** - Is it cost-optimized?
# MAGIC 5. **Maintainability** - Is the solution operationally viable?
# MAGIC 6. **Innovation** - Creative use of optimization techniques?
# MAGIC
# MAGIC ## Advanced Techniques Reference
# MAGIC
# MAGIC **Bucketing:**
# MAGIC ```python
# MAGIC df.write.bucketBy(100, "h3_9").sortBy("event_timestamp").saveAsTable("bucketed_events")
# MAGIC ```
# MAGIC
# MAGIC **Salting for Skew:**
# MAGIC ```python
# MAGIC df.withColumn("salt", (rand() * 10).cast("int"))
# MAGIC ```
# MAGIC
# MAGIC **Dynamic Partition Pruning:**
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
# MAGIC ```
# MAGIC
# MAGIC **Adaptive Query Execution:**
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
# MAGIC ```
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Multiple valid solutions** exist for petabyte-scale problems
# MAGIC 2. **Trade-offs** between cost, performance, and complexity are inevitable
# MAGIC 3. **Experimentation** and benchmarking are essential
# MAGIC 4. **Monitoring** reveals optimization opportunities
# MAGIC 5. **Iterative improvement** is more effective than big-bang optimization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You've completed all labs in the Enterprise Geospatial Data Engineering workshop!
