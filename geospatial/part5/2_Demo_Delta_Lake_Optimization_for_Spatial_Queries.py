# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Delta Lake Optimization for Spatial Queries
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores advanced Delta Lake optimization techniques specifically for geospatial queries. You'll learn how to use Z-ordering, Bloom filters, Liquid Clustering, and other optimization strategies to achieve 10-100x query performance improvements.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Apply Z-ordering for spatial columns
# MAGIC - Implement Bloom filters for point lookups
# MAGIC - Configure Liquid Clustering for dynamic workloads
# MAGIC - Optimize file sizes for spatial data
# MAGIC - Benchmark query performance improvements
# MAGIC - Use data skipping effectively
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1 (Medallion Architecture)
# MAGIC - DBR 15.0+ with Photon enabled
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install apache-sedona==1.5.1 h3==3.7.6 geopandas==0.14.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
import time

# Configure Sedona
spark.conf.set("spark.serializer", KryoSerializer.getName)
spark.conf.set("spark.kryo.registrator", SedonaKryoRegistrator.getName)
SedonaRegistrator.registerAll(spark)

# Use catalog from previous demo
spark.sql("USE CATALOG geospatial_demo")
spark.sql("USE SCHEMA silver")

print("✅ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Technique 1: Z-Ordering
# MAGIC
# MAGIC ### What is Z-Ordering?
# MAGIC Z-ordering co-locates related data in the same files by mapping multi-dimensional data to one dimension while preserving locality.
# MAGIC
# MAGIC ### Benefits for Geospatial Data
# MAGIC - 10-100x faster range queries
# MAGIC - Effective data skipping
# MAGIC - Reduced I/O for spatial joins
# MAGIC - Works with latitude, longitude, H3 indexes

# COMMAND ----------

# Create a larger dataset for optimization testing
import random
from datetime import datetime, timedelta

def generate_large_dataset(num_records=100000):
    data = []
    SF_LAT_MIN, SF_LAT_MAX = 37.70, 37.80
    SF_LON_MIN, SF_LON_MAX = -122.52, -122.35
    base_time = datetime(2025, 1, 1)
    
    for i in range(num_records):
        lat = random.uniform(SF_LAT_MIN, SF_LAT_MAX)
        lon = random.uniform(SF_LON_MIN, SF_LON_MAX)
        timestamp = base_time + timedelta(minutes=random.randint(0, 10080))
        
        data.append({
            'event_id': f'EVT_{i:08d}',
            'latitude': lat,
            'longitude': lon,
            'event_timestamp': timestamp,
            'event_type': random.choice(['traffic', 'incident', 'construction']),
            'severity': random.randint(1, 5),
            'value': random.uniform(0, 100)
        })
    
    return data

# Generate data
print("Generating test dataset...")
test_data = generate_large_dataset(100000)
df_test = spark.createDataFrame(test_data)

# Write unoptimized table
df_test.write.format("delta").mode("overwrite").saveAsTable("test_events_unoptimized")

print(f"✅ Created test table with {df_test.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline Query Performance (Unoptimized)

# COMMAND ----------

# Query 1: Range query (spatial filter)
start_time = time.time()
result1 = spark.sql("""
    SELECT COUNT(*), AVG(value)
    FROM test_events_unoptimized
    WHERE latitude BETWEEN 37.75 AND 37.78
      AND longitude BETWEEN -122.45 AND -122.42
""").collect()
baseline_time_1 = time.time() - start_time

print(f"Baseline Query 1 (range): {baseline_time_1:.3f} seconds")
print(f"Result: {result1}")

# COMMAND ----------

# Query 2: Point lookup
start_time = time.time()
result2 = spark.sql("""
    SELECT *
    FROM test_events_unoptimized
    WHERE event_id = 'EVT_00050000'
""").collect()
baseline_time_2 = time.time() - start_time

print(f"Baseline Query 2 (point lookup): {baseline_time_2:.3f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Z-Ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create optimized copy
# MAGIC CREATE OR REPLACE TABLE test_events_zorder
# MAGIC AS SELECT * FROM test_events_unoptimized;
# MAGIC
# MAGIC -- Apply Z-ordering on spatial columns
# MAGIC OPTIMIZE test_events_zorder
# MAGIC ZORDER BY (latitude, longitude);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Measure Improvement

# COMMAND ----------

# Query 1 with Z-ordering
start_time = time.time()
result1_opt = spark.sql("""
    SELECT COUNT(*), AVG(value)
    FROM test_events_zorder
    WHERE latitude BETWEEN 37.75 AND 37.78
      AND longitude BETWEEN -122.45 AND -122.42
""").collect()
zorder_time_1 = time.time() - start_time

print(f"Z-Order Query 1: {zorder_time_1:.3f} seconds")
print(f"Speedup: {baseline_time_1/zorder_time_1:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-Ordering with H3 Index
# MAGIC
# MAGIC For even better performance, Z-order by H3 index instead of lat/lon

# COMMAND ----------

import h3

h3_udf = udf(lambda lat, lon, res: h3.geo_to_h3(lat, lon, res) if lat and lon else None, StringType())

# Add H3 indexes
df_with_h3 = spark.table("test_events_unoptimized") \
    .withColumn("h3_7", h3_udf(col("latitude"), col("longitude"), lit(7))) \
    .withColumn("h3_9", h3_udf(col("latitude"), col("longitude"), lit(9)))

# Write and optimize
df_with_h3.write.format("delta").mode("overwrite").saveAsTable("test_events_h3_zorder")

spark.sql("""
    OPTIMIZE test_events_h3_zorder
    ZORDER BY (h3_9, event_timestamp)
""")

print("✅ Created H3-optimized table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Technique 2: Bloom Filters
# MAGIC
# MAGIC ### When to Use Bloom Filters
# MAGIC - High-cardinality columns (IDs, hashes, geohashes)
# MAGIC - Frequent equality predicates
# MAGIC - Sparse data lookups

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bloom filter on event_id
# MAGIC CREATE BLOOMFILTER INDEX
# MAGIC ON TABLE test_events_zorder
# MAGIC FOR COLUMNS(event_id OPTIONS (fpp=0.01, numItems=1000000));

# COMMAND ----------

# Test point lookup with Bloom filter
start_time = time.time()
result_bloom = spark.sql("""
    SELECT *
    FROM test_events_zorder
    WHERE event_id = 'EVT_00050000'
""").collect()
bloom_time = time.time() - start_time

print(f"Bloom Filter Query: {bloom_time:.3f} seconds")
print(f"Speedup: {baseline_time_2/bloom_time:.1f}x faster than baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Technique 3: Liquid Clustering
# MAGIC
# MAGIC ### Benefits
# MAGIC - Automatic clustering maintenance
# MAGIC - No manual OPTIMIZE needed
# MAGIC - Adapts to query patterns
# MAGIC - Simpler than partitioning + Z-order

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with Liquid Clustering (DBR 13.0+)
# MAGIC CREATE OR REPLACE TABLE test_events_liquid
# MAGIC CLUSTER BY (h3_9, event_type)
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   h3_7,
# MAGIC   h3_9,
# MAGIC   event_timestamp,
# MAGIC   event_type,
# MAGIC   severity,
# MAGIC   value
# MAGIC FROM test_events_h3_zorder;
# MAGIC
# MAGIC -- Liquid Clustering automatically maintains clustering on writes
# MAGIC DESCRIBE EXTENDED test_events_liquid;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Technique 4: File Size Tuning

# COMMAND ----------

# Check current file sizes
file_stats = spark.sql("""
    SELECT 
        COUNT(*) as num_files,
        AVG(size_in_bytes) / 1024 / 1024 as avg_file_size_mb,
        MIN(size_in_bytes) / 1024 / 1024 as min_file_size_mb,
        MAX(size_in_bytes) / 1024 / 1024 as max_file_size_mb
    FROM (
        SELECT input_file_name(), COUNT(*) as size_in_bytes
        FROM test_events_zorder
        GROUP BY input_file_name()
    )
""").collect()

print(f"File Statistics: {file_stats}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compact Small Files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact to target 1GB file size
# MAGIC OPTIMIZE test_events_zorder;
# MAGIC
# MAGIC -- Check results
# MAGIC DESCRIBE HISTORY test_events_zorder LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Technique 5: Data Skipping with Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data skipping statistics
# MAGIC SELECT 
# MAGIC     minValues.latitude as min_lat,
# MAGIC     maxValues.latitude as max_lat,
# MAGIC     minValues.longitude as min_lon,
# MAGIC     maxValues.longitude as max_lon,
# MAGIC     stats.numRecords
# MAGIC FROM 
# MAGIC     (SELECT * FROM (DESCRIBE DETAIL test_events_zorder)) AS detail,
# MAGIC     LATERAL VIEW json_tuple(stats, 'minValues', 'maxValues', 'numRecords') AS minValues, maxValues, stats;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprehensive Performance Benchmark

# COMMAND ----------

# Define test queries
test_queries = {
    "Range Query": """
        SELECT COUNT(*), AVG(value)
        FROM {table}
        WHERE latitude BETWEEN 37.75 AND 37.78
          AND longitude BETWEEN -122.45 AND -122.42
    """,
    "Point Lookup": """
        SELECT *
        FROM {table}
        WHERE event_id = 'EVT_00050000'
    """,
    "Aggregation by Type": """
        SELECT event_type, COUNT(*), AVG(severity)
        FROM {table}
        WHERE latitude BETWEEN 37.72 AND 37.79
        GROUP BY event_type
    """,
    "Time Range": """
        SELECT DATE(event_timestamp), COUNT(*)
        FROM {table}
        WHERE event_timestamp >= '2025-01-01'
          AND event_timestamp < '2025-01-08'
        GROUP BY DATE(event_timestamp)
    """
}

# Tables to test
tables = [
    "test_events_unoptimized",
    "test_events_zorder",
    "test_events_h3_zorder",
    "test_events_liquid"
]

# Run benchmarks
results = []
for table in tables:
    for query_name, query_template in test_queries.items():
        query = query_template.format(table=table)
        
        # Warm-up run
        spark.sql(query).collect()
        
        # Timed run
        start = time.time()
        spark.sql(query).collect()
        duration = time.time() - start
        
        results.append({
            'table': table,
            'query': query_name,
            'duration_sec': duration
        })

# Create results DataFrame
df_results = spark.createDataFrame(results)
display(df_results.orderBy("query", "duration_sec"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualization: Performance Comparison

# COMMAND ----------

# Pivot results for comparison
df_pivot = df_results.groupBy("query").pivot("table").avg("duration_sec")
display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Spatial Query Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Choose the Right Clustering Strategy
# MAGIC
# MAGIC | Strategy | Best For | Maintenance |
# MAGIC |----------|----------|-------------|
# MAGIC | Z-Order by lat/lon | Static datasets, range queries | Manual OPTIMIZE |
# MAGIC | Z-Order by H3 | Large datasets, uniform distribution | Manual OPTIMIZE |
# MAGIC | Liquid Clustering | Dynamic workloads, mixed queries | Automatic |
# MAGIC | Partitioning + Z-Order | Very large datasets (>10TB) | Manual OPTIMIZE per partition |
# MAGIC
# MAGIC ### 2. File Size Targets
# MAGIC - **Target:** 1GB per file
# MAGIC - **Too small:** Metadata overhead, slow queries
# MAGIC - **Too large:** Poor parallelism, slow data skipping
# MAGIC
# MAGIC ### 3. Bloom Filter Guidelines
# MAGIC - Use for columns with >100K distinct values
# MAGIC - Set fpp (false positive probability) to 0.01
# MAGIC - Only for equality predicates (=, IN)
# MAGIC - Monitor index size vs. benefit
# MAGIC
# MAGIC ### 4. Z-Order Column Selection
# MAGIC ```python
# MAGIC # Good: Columns frequently used in WHERE clauses
# MAGIC ZORDER BY (h3_index, timestamp)
# MAGIC
# MAGIC # Bad: Too many columns (diminishing returns after 3-4)
# MAGIC ZORDER BY (lat, lon, timestamp, type, severity)  # Don't do this
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-Optimize for Automatic Maintenance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable Auto Optimize
# MAGIC ALTER TABLE test_events_zorder
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Auto Optimize will:
# MAGIC -- 1. Optimize file sizes during writes
# MAGIC -- 2. Automatically compact small files
# MAGIC -- 3. Reduce need for manual OPTIMIZE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Query Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check recent query performance
# MAGIC SELECT 
# MAGIC     query_id,
# MAGIC     query_text,
# MAGIC     execution_time_ms / 1000 as execution_time_sec,
# MAGIC     rows_produced,
# MAGIC     query_start_time
# MAGIC FROM system.query.history
# MAGIC WHERE catalog_name = 'geospatial_demo'
# MAGIC   AND query_start_time >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
# MAGIC ORDER BY execution_time_ms DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze storage costs
# MAGIC DESCRIBE DETAIL test_events_unoptimized;
# MAGIC DESCRIBE DETAIL test_events_zorder;
# MAGIC DESCRIBE DETAIL test_events_h3_zorder;

# COMMAND ----------

# Compare table sizes
size_comparison = spark.sql("""
    SELECT 
        'Unoptimized' as version,
        COUNT(DISTINCT input_file_name()) as num_files,
        SUM(size) / 1024 / 1024 as size_mb
    FROM (
        SELECT input_file_name() as file, 1 as size
        FROM test_events_unoptimized
    )
    
    UNION ALL
    
    SELECT 
        'Z-Ordered' as version,
        COUNT(DISTINCT input_file_name()) as num_files,
        SUM(size) / 1024 / 1024 as size_mb
    FROM (
        SELECT input_file_name() as file, 1 as size
        FROM test_events_zorder
    )
""")

display(size_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Z-ordering** can provide 10-100x speedup for range queries on spatial data
# MAGIC
# MAGIC 2. **H3-based Z-ordering** often outperforms lat/lon Z-ordering for large datasets
# MAGIC
# MAGIC 3. **Bloom filters** are essential for fast ID lookups (100x+ speedup)
# MAGIC
# MAGIC 4. **Liquid Clustering** simplifies maintenance with automatic optimization
# MAGIC
# MAGIC 5. **Target 1GB file sizes** for optimal balance of parallelism and I/O
# MAGIC
# MAGIC 6. **Data skipping** with min/max statistics is automatic and highly effective
# MAGIC
# MAGIC 7. **Auto Optimize** reduces operational burden for production tables
# MAGIC
# MAGIC 8. **Benchmark your specific queries** - different patterns benefit from different optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Continue to **Demo 3: Geospatial Data Partitioning Strategies**
# MAGIC
# MAGIC You'll learn:
# MAGIC - H3-based partitioning for uniform distribution
# MAGIC - Geographic boundary partitioning
# MAGIC - Composite temporal + spatial partitioning
# MAGIC - Dynamic repartitioning strategies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Great work!** You've mastered Delta Lake optimization for spatial queries.
