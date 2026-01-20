# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Optimizing Large-Scale Spatial Queries
# MAGIC
# MAGIC ## Overview
# MAGIC Hands-on lab to optimize spatial queries for petabyte-scale datasets. Analyze query plans, tune indexes, implement caching, and measure performance improvements.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Analyze spatial query execution plans
# MAGIC - Apply optimal indexing strategies
# MAGIC - Implement effective caching
# MAGIC - Optimize spatial joins
# MAGIC - Benchmark performance improvements
# MAGIC
# MAGIC ## Duration
# MAGIC 40 minutes

# COMMAND ----------

# MAGIC %pip install apache-sedona==1.5.1 h3==3.7.6
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from sedona.register import SedonaRegistrator
import time

SedonaRegistrator.registerAll(spark)
spark.sql("USE CATALOG lab_geospatial")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Query Plan Analysis
# MAGIC
# MAGIC **Task:** Analyze the execution plan of a slow spatial query and identify bottlenecks

# COMMAND ----------

# Create large test dataset
def generate_large_spatial_data(num_records=500000):
    import random
    from datetime import datetime, timedelta
    
    data = []
    base_time = datetime(2025, 1, 1)
    
    for i in range(num_records):
        lat = random.uniform(37.70, 37.80)
        lon = random.uniform(-122.52, -122.35)
        
        data.append({
            'event_id': f'EVT_{i:09d}',
            'latitude': lat,
            'longitude': lon,
            'event_timestamp': base_time + timedelta(minutes=random.randint(0, 43200)),
            'category': random.choice(['A', 'B', 'C', 'D']),
            'value': random.uniform(0, 1000)
        })
    
    return spark.createDataFrame(data)

df_large = generate_large_spatial_data()
df_large.write.format("delta").mode("overwrite").saveAsTable("silver.large_events_unoptimized")

print("✅ Created large test dataset")

# COMMAND ----------

# TODO: Run a slow query and examine its plan
# TODO: Identify data skipping opportunities
# TODO: Find partition pruning issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 1

# COMMAND ----------

# Analyze slow query
slow_query = """
    SELECT category, COUNT(*), AVG(value)
    FROM silver.large_events_unoptimized
    WHERE latitude BETWEEN 37.75 AND 37.78
      AND longitude BETWEEN -122.45 AND -122.42
    GROUP BY category
"""

# Show explain plan
spark.sql(f"EXPLAIN COST {slow_query}").show(truncate=False)

# Measure baseline performance
start = time.time()
result = spark.sql(slow_query).collect()
baseline_time = time.time() - start

print(f"Baseline query time: {baseline_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Apply Optimizations
# MAGIC
# MAGIC **Task:** Apply Z-ordering, add H3 index, and remeasure

# COMMAND ----------

# TODO: Add H3 index column
# TODO: Repartition by H3
# TODO: Apply Z-ordering
# TODO: Add Bloom filter on event_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 2

# COMMAND ----------

import h3

h3_udf = udf(lambda lat, lon, res: h3.geo_to_h3(lat, lon, res), StringType())

# Add H3 and optimize
df_optimized = spark.table("silver.large_events_unoptimized") \
    .withColumn("h3_9", h3_udf(col("latitude"), col("longitude"), lit(9)))

df_optimized.write.format("delta").mode("overwrite") \
    .partitionBy("category") \
    .saveAsTable("silver.large_events_optimized")

# Apply optimizations
spark.sql("""
    OPTIMIZE silver.large_events_optimized
    ZORDER BY (latitude, longitude, event_timestamp)
""")

spark.sql("""
    CREATE BLOOMFILTER INDEX ON TABLE silver.large_events_optimized
    FOR COLUMNS(event_id OPTIONS (fpp=0.01, numItems=1000000))
""")

print("✅ Optimizations applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Benchmark Improvements

# COMMAND ----------

# Measure optimized query
optimized_query = """
    SELECT category, COUNT(*), AVG(value)
    FROM silver.large_events_optimized
    WHERE latitude BETWEEN 37.75 AND 37.78
      AND longitude BETWEEN -122.45 AND -122.42
    GROUP BY category
"""

start = time.time()
result_opt = spark.sql(optimized_query).collect()
optimized_time = time.time() - start

print(f"Optimized query time: {optimized_time:.2f} seconds")
print(f"Speedup: {baseline_time/optimized_time:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Optimize Spatial Join

# COMMAND ----------

# Create POI dataset
pois = [
    ('POI_001', 'Restaurant', 37.7649, -122.4294),
    ('POI_002', 'Park', 37.7749, -122.4194),
    ('POI_003', 'Store', 37.7549, -122.4394)
]

df_pois = spark.createDataFrame(pois, ['poi_id', 'poi_name', 'latitude', 'longitude'])
df_pois.write.format("delta").mode("overwrite").saveAsTable("silver.pois")

# TODO: Optimize join to find events within 500m of POIs
# TODO: Use broadcast join if appropriate
# TODO: Apply spatial partitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Exercise 4

# COMMAND ----------

from pyspark.sql.functions import broadcast

# Spatial join with optimization
df_events = spark.table("silver.large_events_optimized")
df_pois_read = spark.table("silver.pois")

# Add geometries
df_events_geom = df_events.withColumn("event_geom", 
    expr("ST_Point(CAST(longitude AS DECIMAL(24,20)), CAST(latitude AS DECIMAL(24,20)))"))

df_pois_geom = df_pois_read.withColumn("poi_geom",
    expr("ST_Point(CAST(longitude AS DECIMAL(24,20)), CAST(latitude AS DECIMAL(24,20)))"))

# Broadcast small POI table
df_join = df_events_geom.crossJoin(broadcast(df_pois_geom)) \
    .filter(expr("ST_Distance(event_geom, poi_geom) <= 500")) \
    .select("event_id", "poi_id", "poi_name", 
           expr("ST_Distance(event_geom, poi_geom) as distance"))

print(f"Events near POIs: {df_join.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Query plans** reveal optimization opportunities
# MAGIC 2. **Z-ordering** dramatically improves range query performance
# MAGIC 3. **H3 indexing** enables efficient spatial partitioning
# MAGIC 4. **Broadcast joins** work well for small reference datasets
# MAGIC 5. **Bloom filters** accelerate point lookups
# MAGIC 6. **Benchmarking** validates optimization impact
# MAGIC
# MAGIC **Continue to Lab 9: Multi-Region Geospatial Platform**
