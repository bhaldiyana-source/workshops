# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Spatial Joins at Scale
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores different strategies for performing spatial joins on large datasets in Databricks. You'll learn when to use broadcast joins, partitioned joins, and H3-based joins, and how to optimize each approach for maximum performance.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Implement broadcast joins for small reference datasets
# MAGIC - Use partitioned joins for large-to-large joins
# MAGIC - Optimize joins using H3 spatial indexing
# MAGIC - Analyze query execution plans
# MAGIC - Benchmark and compare join strategies
# MAGIC - Select the optimal strategy for your data characteristics
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1: H3 Hexagonal Indexing
# MAGIC - Understanding of Spark DataFrame operations
# MAGIC - Familiarity with join concepts
# MAGIC
# MAGIC ## Duration
# MAGIC 45-50 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install and Import Libraries

# COMMAND ----------

# Install required libraries
%pip install h3 geopandas shapely --quiet

#dbutils.library.restartPython()

# COMMAND ----------

import h3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, box
from shapely.wkt import loads as wkt_loads, dumps as wkt_dumps
import time
import numpy as np
from pyspark.sql.functions import udf, col, broadcast, explode, array, lit
from pyspark.sql.types import StringType, ArrayType, BooleanType, DoubleType

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Generate Sample Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Points Dataset (Customers)

# COMMAND ----------

# Generate 100,000 customer locations across San Francisco Bay Area
import random
random.seed(42)

num_customers = 100000

customers = []
for i in range(num_customers):
    # Bay Area: roughly 37.2 to 38.0 N, -122.6 to -121.8 W
    lat = 37.2 + random.random() * 0.8
    lon = -122.6 + random.random() * 0.8
    customer_id = f"C{i:06d}"
    customers.append((customer_id, lat, lon))

customers_df = spark.createDataFrame(customers, ['customer_id', 'latitude', 'longitude'])

print(f"Created {customers_df.count():,} customer locations")
customers_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Polygons Dataset (Service Zones)

# COMMAND ----------

# Generate 50 service zones (hexagonal grid using H3)
zone_resolution = 6  # City-scale zones

# Get center of Bay Area
center_lat, center_lon = 37.6, -122.2
center_h3 = h3.geo_to_h3(center_lat, center_lon, zone_resolution)

# Get surrounding zones
zones_h3 = h3.k_ring(center_h3, 3)  # Get 3-ring of zones

zones = []
for i, zone_h3 in enumerate(zones_h3):
    zone_id = f"Z{i:03d}"
    boundary = h3.h3_to_geo_boundary(zone_h3)
    
    # Convert to WKT
    polygon_coords = [(lon, lat) for lat, lon in boundary]
    polygon_coords.append(polygon_coords[0])  # Close the polygon
    
    polygon = Polygon(polygon_coords)
    wkt = wkt_dumps(polygon)
    
    center = h3.h3_to_geo(zone_h3)
    
    zones.append((zone_id, zone_h3, wkt, center[0], center[1]))

zones_df = spark.createDataFrame(zones, ['zone_id', 'h3_cell', 'geometry_wkt', 'center_lat', 'center_lon'])

print(f"Created {zones_df.count()} service zones")
zones_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Strategy 1 - Broadcast Join

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Broadcast Joins
# MAGIC
# MAGIC **When to use:**
# MAGIC - Small dataset (typically < 10MB)
# MAGIC - One-to-many relationship
# MAGIC - Want to avoid shuffle
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Small dataset copied to all executors
# MAGIC 2. Large dataset stays partitioned
# MAGIC 3. Join happens locally on each executor

# COMMAND ----------

# Create UDF for point-in-polygon test
def point_in_polygon_udf():
    def check_contains(point_lat, point_lon, polygon_wkt):
        try:
            point = Point(point_lon, point_lat)
            polygon = wkt_loads(polygon_wkt)
            return polygon.contains(point)
        except:
            return False
    return udf(check_contains, BooleanType())

contains_udf = point_in_polygon_udf()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Broadcast Join

# COMMAND ----------

# Method 1: Broadcast Join (suitable when zones_df is small)
print("=" * 60)
print("BROADCAST JOIN")
print("=" * 60)

start_time = time.time()

# Broadcast the zones dataset
zones_broadcast = broadcast(zones_df)

# Cross join (cartesian product) with broadcast
joined = customers_df.crossJoin(zones_broadcast)

# Filter using point-in-polygon test
result_broadcast = joined.filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select('customer_id', 'zone_id', 'latitude', 'longitude')

# Trigger computation
count_broadcast = result_broadcast.count()
duration_broadcast = time.time() - start_time

print(f"Customers assigned to zones: {count_broadcast:,}")
print(f"Duration: {duration_broadcast:.2f} seconds")
print()

result_broadcast.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Broadcast Join Execution Plan

# COMMAND ----------

# Show execution plan
result_broadcast.explain(mode='simple')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Strategy 2 - H3-Based Join (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding H3-Based Joins
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Fast: Hash join on string/integer IDs
# MAGIC - Scalable: Works with huge datasets
# MAGIC - Efficient: Reduces candidates dramatically
# MAGIC
# MAGIC **Two-phase approach:**
# MAGIC 1. **Phase 1**: H3 join (fast, approximate)
# MAGIC 2. **Phase 2**: Precise geometric test (only on candidates)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add H3 Index to Customers

# COMMAND ----------

# UDF to convert lat/lon to H3
def geo_to_h3_udf(resolution):
    def convert(lat, lon):
        try:
            return h3.geo_to_h3(lat, lon, resolution)
        except:
            return None
    return udf(convert, StringType())

# Add H3 column to customers at zone resolution
join_resolution = 6  # Match the zones resolution
customers_with_h3 = customers_df.withColumn(
    'h3_cell', 
    geo_to_h3_udf(join_resolution)(col('latitude'), col('longitude'))
)

print("Customers with H3 index:")
customers_with_h3.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform H3-Based Join

# COMMAND ----------

print("=" * 60)
print("H3-BASED JOIN")
print("=" * 60)

start_time = time.time()

# Phase 1: Fast H3 join (exact match on cell ID)
h3_joined = customers_with_h3.join(
    zones_df,
    customers_with_h3.h3_cell == zones_df.h3_cell,
    'inner'
)

# Phase 2: Precise geometric test (post-filter)
result_h3 = h3_joined.filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select(
    customers_with_h3.customer_id,
    zones_df.zone_id,
    customers_with_h3.latitude,
    customers_with_h3.longitude
)

# Trigger computation
count_h3 = result_h3.count()
duration_h3 = time.time() - start_time

print(f"Customers assigned to zones: {count_h3:,}")
print(f"Duration: {duration_h3:.2f} seconds")
print(f"Speedup vs Broadcast: {duration_broadcast/duration_h3:.1f}x")
print()

result_h3.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze H3 Join Execution Plan

# COMMAND ----------

result_h3.explain(mode='simple')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Strategy 3 - H3 with Buffer for Proximity

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3 Join with Expanded Search
# MAGIC
# MAGIC For cases where points might be near zone boundaries, we can expand the search using H3 neighbors

# COMMAND ----------

# UDF to get H3 cell and its neighbors
def h3_with_neighbors_udf(resolution, k_ring=1):
    def get_cells(lat, lon):
        try:
            center = h3.geo_to_h3(lat, lon, resolution)
            neighbors = h3.k_ring(center, k_ring)
            return list(neighbors)
        except:
            return []
    return udf(get_cells, ArrayType(StringType()))

# Add H3 cells with neighbors
customers_expanded = customers_df.withColumn(
    'h3_cells',
    h3_with_neighbors_udf(join_resolution, k_ring=1)(col('latitude'), col('longitude'))
).withColumn(
    'h3_cell',
    explode(col('h3_cells'))
)

print("Customers with expanded H3 search (including neighbors):")
customers_expanded.show(5)

# COMMAND ----------

# Perform H3 join with expanded search
print("=" * 60)
print("H3 JOIN WITH BUFFER (K-RING)")
print("=" * 60)

start_time = time.time()

# Join with expanded cells
h3_expanded_joined = customers_expanded.join(
    zones_df,
    customers_expanded.h3_cell == zones_df.h3_cell,
    'inner'
)

# Post-filter with precise test
result_h3_expanded = h3_expanded_joined.filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select(
    customers_expanded.customer_id,
    zones_df.zone_id,
    customers_expanded.latitude,
    customers_expanded.longitude
).distinct()  # Remove duplicates from expanded search

count_h3_expanded = result_h3_expanded.count()
duration_h3_expanded = time.time() - start_time

print(f"Customers assigned to zones: {count_h3_expanded:,}")
print(f"Duration: {duration_h3_expanded:.2f} seconds")
print(f"Additional matches found: {count_h3_expanded - count_h3}")
print()

result_h3_expanded.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Strategy 4 - Range Join with Bounding Boxes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Bounding Boxes for Pre-filtering

# COMMAND ----------

# Calculate bounding boxes for zones
def get_bbox_from_wkt(wkt_str):
    try:
        polygon = wkt_loads(wkt_str)
        bounds = polygon.bounds  # (minx, miny, maxx, maxy)
        return bounds
    except:
        return (None, None, None, None)

# UDF for bounding box extraction
bbox_udf = udf(get_bbox_from_wkt, ArrayType(DoubleType()))

# Add bounding boxes to zones
zones_with_bbox = zones_df.withColumn('bbox', bbox_udf(col('geometry_wkt'))) \
    .withColumn('min_lon', col('bbox')[0]) \
    .withColumn('min_lat', col('bbox')[1]) \
    .withColumn('max_lon', col('bbox')[2]) \
    .withColumn('max_lat', col('bbox')[3]) \
    .drop('bbox')

zones_with_bbox.show(5)

# COMMAND ----------

# Perform range join using bounding boxes
print("=" * 60)
print("RANGE JOIN WITH BOUNDING BOXES")
print("=" * 60)

start_time = time.time()

# Join using bounding box range conditions
range_joined = customers_df.join(
    broadcast(zones_with_bbox),
    (col('longitude') >= col('min_lon')) & 
    (col('longitude') <= col('max_lon')) &
    (col('latitude') >= col('min_lat')) & 
    (col('latitude') <= col('max_lat'))
)

# Post-filter with precise test
result_range = range_joined.filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select('customer_id', 'zone_id', 'latitude', 'longitude')

count_range = result_range.count()
duration_range = time.time() - start_time

print(f"Customers assigned to zones: {count_range:,}")
print(f"Duration: {duration_range:.2f} seconds")
print()

result_range.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Performance Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare All Strategies

# COMMAND ----------

# Summary of results
comparison_data = [
    ('Broadcast Join', count_broadcast, duration_broadcast, 1.0),
    ('H3 Join', count_h3, duration_h3, duration_broadcast/duration_h3),
    ('H3 + Buffer', count_h3_expanded, duration_h3_expanded, duration_broadcast/duration_h3_expanded),
    ('Range Join (BBox)', count_range, duration_range, duration_broadcast/duration_range)
]

comparison_df = spark.createDataFrame(
    comparison_data,
    ['Strategy', 'Results', 'Duration (s)', 'Speedup']
)

print("PERFORMANCE COMPARISON")
print("=" * 60)
comparison_df.show(truncate=False)

# COMMAND ----------

# Visualize performance comparison
import matplotlib.pyplot as plt

strategies = [row['Strategy'] for row in comparison_data]
durations = [row[2] for row in comparison_data]

plt.figure(figsize=(12, 6))
plt.subplot(1, 2, 1)
plt.bar(strategies, durations, color=['blue', 'green', 'orange', 'red'])
plt.ylabel('Duration (seconds)')
plt.title('Join Strategy Performance')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

plt.subplot(1, 2, 2)
speedups = [row[3] for row in comparison_data]
plt.bar(strategies, speedups, color=['blue', 'green', 'orange', 'red'])
plt.ylabel('Speedup (relative to broadcast)')
plt.title('Relative Performance')
plt.axhline(y=1, color='black', linestyle='--', label='Baseline')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.tight_layout()

display(plt.gcf())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Advanced H3 Join Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-Resolution H3 Join
# MAGIC
# MAGIC Use different resolutions for different query types

# COMMAND ----------

# Create multi-resolution H3 index
customers_multi_res = customers_df \
    .withColumn('h3_res5', geo_to_h3_udf(5)(col('latitude'), col('longitude'))) \
    .withColumn('h3_res6', geo_to_h3_udf(6)(col('latitude'), col('longitude'))) \
    .withColumn('h3_res7', geo_to_h3_udf(7)(col('latitude'), col('longitude')))

print("Customers with multi-resolution H3:")
customers_multi_res.show(5)

# COMMAND ----------

# Can now join at different resolutions depending on zone size
# Large zones -> lower resolution
# Small zones -> higher resolution

# Example: Identify zone sizes and select appropriate resolution
zone_sizes = zones_df.select('zone_id', 'h3_cell').collect()

print(f"Using resolution 6 for {len(zone_sizes)} zones")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Partitioning for Spatial Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition Data by H3 for Faster Joins

# COMMAND ----------

# Repartition both datasets by H3 cell
customers_partitioned = customers_with_h3.repartition(50, 'h3_cell')
zones_partitioned = zones_df.repartition(50, 'h3_cell')

print("Partitioned datasets by H3 cell")
print(f"Customers partitions: {customers_partitioned.rdd.getNumPartitions()}")
print(f"Zones partitions: {zones_partitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# Perform join on partitioned data
print("=" * 60)
print("PARTITIONED H3 JOIN")
print("=" * 60)

start_time = time.time()

result_partitioned = customers_partitioned.join(
    zones_partitioned,
    'h3_cell',
    'inner'
).filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select('customer_id', 'zone_id', 'latitude', 'longitude')

count_partitioned = result_partitioned.count()
duration_partitioned = time.time() - start_time

print(f"Customers assigned: {count_partitioned:,}")
print(f"Duration: {duration_partitioned:.2f} seconds")
print(f"Speedup vs broadcast: {duration_broadcast/duration_partitioned:.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Join Strategy Selection Guide

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decision Matrix
# MAGIC
# MAGIC | Scenario | Recommended Strategy | Why |
# MAGIC |----------|---------------------|-----|
# MAGIC | **Small zones (<10MB)** | Broadcast Join | Simple, no shuffle, good for small reference data |
# MAGIC | **Large datasets, uniform density** | H3 Join | Fast hash join, minimal overhead |
# MAGIC | **Points near boundaries** | H3 + K-Ring Buffer | Catches edge cases, still fast |
# MAGIC | **Complex polygons, varied sizes** | Range Join (BBox) + precise test | Good pre-filter, works with any geometry |
# MAGIC | **Very large both sides** | Partitioned H3 Join | Co-locates data, minimizes shuffle |
# MAGIC | **Real-time queries** | Pre-computed H3 index | Instant lookups |
# MAGIC
# MAGIC ### Performance Characteristics
# MAGIC
# MAGIC ```
# MAGIC Broadcast:     O(n × m) but m is small and local
# MAGIC H3:            O(n + m) with hash join
# MAGIC H3 + Buffer:   O(n × k + m) where k is small (neighbors)
# MAGIC Range (BBox):  O(n × log m) with index
# MAGIC Partitioned:   O(n/p × m/p) where p is partitions
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### 1. Data Preparation
# MAGIC - Pre-compute H3 indices and store with data
# MAGIC - Validate geometries before joins
# MAGIC - Simplify complex polygons when possible
# MAGIC - Cache reference datasets that are reused
# MAGIC
# MAGIC ### 2. Resolution Selection
# MAGIC - Match H3 resolution to smallest geometry
# MAGIC - Use lower resolution for large zones
# MAGIC - Test different resolutions with sample data
# MAGIC - Consider using multiple resolutions
# MAGIC
# MAGIC ### 3. Performance Optimization
# MAGIC - Use broadcast for small datasets (<10MB)
# MAGIC - Use H3 join for most production cases
# MAGIC - Add k-ring buffer for boundary precision
# MAGIC - Partition large datasets by H3 cell
# MAGIC - Cache intermediate results
# MAGIC
# MAGIC ### 4. Accuracy Considerations
# MAGIC - H3 join alone is approximate
# MAGIC - Always follow with precise geometric test
# MAGIC - Use k-ring buffer for boundary cases
# MAGIC - Validate results against known test cases

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Pitfalls
# MAGIC
# MAGIC ### Pitfall 1: Wrong Resolution
# MAGIC ```python
# MAGIC # Bad: Resolution too low, many false positives
# MAGIC h3_cell = h3.geo_to_h3(lat, lon, 3)
# MAGIC
# MAGIC # Good: Match resolution to geometry size
# MAGIC h3_cell = h3.geo_to_h3(lat, lon, 7)  # For city zones
# MAGIC ```
# MAGIC
# MAGIC ### Pitfall 2: Forgetting Precise Test
# MAGIC ```python
# MAGIC # Bad: H3 join without geometric verification
# MAGIC result = points.join(zones, 'h3_cell')
# MAGIC
# MAGIC # Good: Add precise point-in-polygon test
# MAGIC result = points.join(zones, 'h3_cell') \
# MAGIC     .filter(contains_udf(...))
# MAGIC ```
# MAGIC
# MAGIC ### Pitfall 3: Not Using Broadcast
# MAGIC ```python
# MAGIC # Bad: Full shuffle for small reference data
# MAGIC result = large.join(small, condition)
# MAGIC
# MAGIC # Good: Broadcast small dataset
# MAGIC result = large.join(broadcast(small), condition)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue with:
# MAGIC - **Demo 3**: Advanced Geometric Operations
# MAGIC - **Demo 4**: Spatial Aggregations
# MAGIC - **Lab 5**: Optimizing Large-Scale Spatial Joins (hands-on practice)
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC - Choose join strategy based on data characteristics
# MAGIC - H3 indexing dramatically improves performance
# MAGIC - Always validate approximate joins with precise tests
# MAGIC - Partition data for optimal spatial locality
# MAGIC - Benchmark strategies with your actual data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Spark SQL Join Strategies](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
# MAGIC - [H3 Spatial Joins](https://h3geo.org/docs/highlights/joins)
# MAGIC - [Apache Sedona Spatial Joins](https://sedona.apache.org/tutorial/sql/#spatial-join)
# MAGIC - [Databricks Query Optimization](https://docs.databricks.com/optimizations/index.html)
