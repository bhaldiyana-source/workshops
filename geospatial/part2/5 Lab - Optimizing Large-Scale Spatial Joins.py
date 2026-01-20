# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Optimizing Large-Scale Spatial Joins
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll apply spatial join optimization techniques to real-world scenarios. You'll work with large datasets, benchmark different strategies, and optimize queries to meet performance requirements.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Analyze spatial join requirements and select optimal strategies
# MAGIC - Implement and benchmark multiple join approaches
# MAGIC - Optimize queries using H3 indexing and partitioning
# MAGIC - Interpret execution plans to identify bottlenecks
# MAGIC - Apply best practices for production-scale spatial joins
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1: H3 Hexagonal Indexing
# MAGIC - Completion of Demo 2: Spatial Joins at Scale
# MAGIC - Understanding of Spark DataFrame operations
# MAGIC
# MAGIC ## Duration
# MAGIC 60-75 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

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
from pyspark.sql.functions import udf, col, broadcast, explode, array, lit, count as spark_count
from pyspark.sql.types import StringType, ArrayType, BooleanType, DoubleType, IntegerType
import random

print("✓ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Generate Large-Scale Dataset
# MAGIC
# MAGIC **Task**: Create realistic large-scale datasets for join operations
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Generate 1,000,000 customer locations across a metropolitan area
# MAGIC - Create 500 service zones of varying sizes
# MAGIC - Include realistic geographic clustering

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.1: Generate Customer Locations
# MAGIC
# MAGIC Create 1M customer locations with realistic clustering around multiple city centers

# COMMAND ----------

# STARTER CODE
random.seed(42)
np.random.seed(42)

# Define metropolitan area centers (San Francisco Bay Area)
metro_centers = [
    {'name': 'San Francisco', 'lat': 37.7749, 'lon': -122.4194, 'weight': 0.30},
    {'name': 'Oakland', 'lat': 37.8044, 'lon': -122.2712, 'weight': 0.20},
    {'name': 'San Jose', 'lat': 37.3382, 'lon': -121.8863, 'weight': 0.25},
    {'name': 'Fremont', 'lat': 37.5485, 'lon': -121.9886, 'weight': 0.15},
    {'name': 'Berkeley', 'lat': 37.8715, 'lon': -122.2730, 'weight': 0.10}
]

# TODO: Generate 1M customer locations
# Hint: Use random.choices() to select centers based on weights
# Hint: Add Gaussian noise for clustering (std_dev ~0.02 degrees)

num_customers = 1_000_000

# YOUR CODE HERE
customers = []
for i in range(num_customers):
    # Select a center based on weights
    center = random.choices(metro_centers, weights=[c['weight'] for c in metro_centers])[0]
    
    # Add Gaussian noise
    lat = center['lat'] + np.random.randn() * 0.02
    lon = center['lon'] + np.random.randn() * 0.02
    
    customers.append((f'CUST{i:07d}', lat, lon))

# Create DataFrame
customers_df = spark.createDataFrame(customers, ['customer_id', 'latitude', 'longitude'])

# Validate
assert customers_df.count() == num_customers, "Should have 1M customers"
print(f"✓ Created {customers_df.count():,} customer locations")
customers_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.2: Generate Service Zones
# MAGIC
# MAGIC Create 500 service zones using H3 cells at resolution 7

# COMMAND ----------

# TODO: Generate 500 service zones
# Hint: Use H3 k_ring to create zones of varying sizes
# Hint: Convert H3 cells to polygon boundaries

# YOUR CODE HERE
zone_resolution = 7
zones = []

# Start with seed cells across the metro area
for i, center in enumerate(metro_centers):
    seed_h3 = h3.geo_to_h3(center['lat'], center['lon'], zone_resolution)
    
    # Create zones with varying sizes (k-rings from 0 to 3)
    for k in range(4):
        for cell in h3.k_ring(seed_h3, k):
            if len(zones) >= 500:
                break
            
            zone_id = f'ZONE{len(zones):03d}'
            boundary = h3.h3_to_geo_boundary(cell)
            polygon_coords = [(lon, lat) for lat, lon in boundary]
            polygon_coords.append(polygon_coords[0])
            
            polygon = Polygon(polygon_coords)
            wkt = wkt_dumps(polygon)
            
            zones.append((zone_id, cell, wkt))
        
        if len(zones) >= 500:
            break

zones_df = spark.createDataFrame(zones[:500], ['zone_id', 'h3_cell', 'geometry_wkt'])

# Validate
assert zones_df.count() == 500, "Should have 500 zones"
print(f"✓ Created {zones_df.count()} service zones")
zones_df.show(5, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Benchmark Naive Join
# MAGIC
# MAGIC **Task**: Implement and benchmark a naive spatial join without optimization
# MAGIC
# MAGIC **Expected Performance**: This will be slow! Use for baseline comparison.

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2.1: Implement Point-in-Polygon UDF

# COMMAND ----------

# TODO: Create UDF for point-in-polygon test
# YOUR CODE HERE

def point_in_polygon_check(point_lat, point_lon, polygon_wkt):
    """Check if point is inside polygon"""
    try:
        point = Point(point_lon, point_lat)
        polygon = wkt_loads(polygon_wkt)
        return polygon.contains(point)
    except:
        return False

contains_udf = udf(point_in_polygon_check, BooleanType())

print("✓ Created point-in-polygon UDF")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2.2: Perform Naive Broadcast Join
# MAGIC
# MAGIC Perform spatial join using broadcast and point-in-polygon test

# COMMAND ----------

# TODO: Implement naive broadcast join
# Warning: This will take several minutes with 1M customers!
# For testing, use a sample first

# Test with sample
print("Testing with 10K sample...")
customers_sample = customers_df.sample(fraction=0.01, seed=42)  # 10K customers

# YOUR CODE HERE
start_time = time.time()

# Broadcast zones (small dataset)
zones_broadcast = broadcast(zones_df)

# Cross join
joined = customers_sample.crossJoin(zones_broadcast)

# Filter with point-in-polygon
result_naive = joined.filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select('customer_id', 'zone_id')

# Trigger computation
count_naive = result_naive.count()
duration_naive = time.time() - start_time

print(f"✓ Naive join completed")
print(f"  Customers: {customers_sample.count():,}")
print(f"  Zones: {zones_df.count()}")
print(f"  Results: {count_naive:,}")
print(f"  Duration: {duration_naive:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Optimize with H3 Indexing
# MAGIC
# MAGIC **Task**: Implement H3-based join and measure improvement
# MAGIC
# MAGIC **Target**: 10-100x faster than naive approach

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3.1: Add H3 Index to Customers

# COMMAND ----------

# TODO: Add H3 column to customers
# Hint: Use same resolution as zones (7)

# YOUR CODE HERE
def geo_to_h3_func(resolution):
    def convert(lat, lon):
        try:
            return h3.geo_to_h3(lat, lon, resolution)
        except:
            return None
    return udf(convert, StringType())

customers_with_h3 = customers_df.withColumn(
    'h3_cell',
    geo_to_h3_func(zone_resolution)(col('latitude'), col('longitude'))
)

print("✓ Added H3 index to customers")
customers_with_h3.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3.2: Perform H3-Based Join

# COMMAND ----------

# TODO: Implement H3 join with precise post-filter
# YOUR CODE HERE

print("Performing H3-based join on full dataset...")
start_time = time.time()

# Phase 1: H3 join (fast)
h3_joined = customers_with_h3.join(
    zones_df,
    customers_with_h3.h3_cell == zones_df.h3_cell,
    'inner'
)

# Phase 2: Precise geometric test
result_h3 = h3_joined.filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select(
    customers_with_h3.customer_id,
    zones_df.zone_id
)

# Trigger computation
count_h3 = result_h3.count()
duration_h3 = time.time() - start_time

print(f"✓ H3 join completed")
print(f"  Customers: {customers_df.count():,}")
print(f"  Zones: {zones_df.count()}")
print(f"  Results: {count_h3:,}")
print(f"  Duration: {duration_h3:.2f}s")

# Calculate speedup (extrapolated from sample)
estimated_naive_duration = duration_naive * (customers_df.count() / customers_sample.count())
speedup = estimated_naive_duration / duration_h3

print(f"\n✓ Performance Improvement:")
print(f"  Estimated naive duration: {estimated_naive_duration:.2f}s")
print(f"  H3 join duration: {duration_h3:.2f}s")
print(f"  Speedup: {speedup:.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Analyze Query Execution Plans
# MAGIC
# MAGIC **Task**: Compare execution plans to understand performance differences

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4.1: Analyze Naive Join Plan

# COMMAND ----------

# TODO: Display and analyze execution plan for naive join
# YOUR CODE HERE

print("Naive Join Execution Plan:")
print("=" * 60)

# Create plan (without executing)
naive_plan = customers_sample.crossJoin(broadcast(zones_df)).filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
)

naive_plan.explain(mode='simple')

# QUESTION: What do you notice about:
# - Number of stages?
# - Shuffle operations?
# - Broadcast operations?

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4.2: Analyze H3 Join Plan

# COMMAND ----------

# TODO: Display and analyze H3 join execution plan
# YOUR CODE HERE

print("H3 Join Execution Plan:")
print("=" * 60)

result_h3.explain(mode='simple')

# QUESTION: Compare with naive plan:
# - What's different?
# - Why is it faster?
# - What are the key operations?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Optimize with Partitioning
# MAGIC
# MAGIC **Task**: Further optimize using spatial partitioning
# MAGIC
# MAGIC **Target**: Additional 2-5x improvement through better data locality

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 5.1: Repartition by H3 Cell

# COMMAND ----------

# TODO: Repartition both datasets by H3 cell
# Hint: Choose partition count based on distinct H3 cells

# YOUR CODE HERE

# Calculate optimal partition count
distinct_cells = customers_with_h3.select('h3_cell').distinct().count()
optimal_partitions = min(200, max(50, distinct_cells // 1000))

print(f"Distinct H3 cells: {distinct_cells}")
print(f"Using {optimal_partitions} partitions")

# Repartition
customers_partitioned = customers_with_h3.repartition(optimal_partitions, 'h3_cell')
zones_partitioned = zones_df.repartition(optimal_partitions, 'h3_cell')

print(f"✓ Repartitioned datasets")
print(f"  Customers partitions: {customers_partitioned.rdd.getNumPartitions()}")
print(f"  Zones partitions: {zones_partitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 5.2: Perform Partitioned Join

# COMMAND ----------

# TODO: Execute join on partitioned data and measure performance
# YOUR CODE HERE

print("Performing partitioned H3 join...")
start_time = time.time()

result_partitioned = customers_partitioned.join(
    zones_partitioned,
    'h3_cell',
    'inner'
).filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select('customer_id', 'zone_id')

count_partitioned = result_partitioned.count()
duration_partitioned = time.time() - start_time

print(f"✓ Partitioned join completed")
print(f"  Results: {count_partitioned:,}")
print(f"  Duration: {duration_partitioned:.2f}s")
print(f"  Speedup vs H3 join: {duration_h3/duration_partitioned:.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Handle Edge Cases
# MAGIC
# MAGIC **Task**: Implement robust join that handles boundary cases
# MAGIC
# MAGIC **Requirement**: Points near zone boundaries should be correctly assigned

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 6.1: Implement K-Ring Buffer Join

# COMMAND ----------

# TODO: Expand search to include neighboring cells
# Hint: Use h3.k_ring() with k=1

# YOUR CODE HERE

def h3_with_neighbors(resolution, k=1):
    def get_neighbors(lat, lon):
        try:
            center = h3.geo_to_h3(lat, lon, resolution)
            neighbors = h3.k_ring(center, k)
            return list(neighbors)
        except:
            return []
    return udf(get_neighbors, ArrayType(StringType()))

# Add expanded H3 cells
customers_expanded = customers_df.withColumn(
    'h3_cells',
    h3_with_neighbors(zone_resolution, k=1)(col('latitude'), col('longitude'))
).withColumn(
    'h3_cell',
    explode(col('h3_cells'))
)

print("✓ Created expanded customer H3 cells")
print(f"  Original customers: {customers_df.count():,}")
print(f"  Expanded rows: {customers_expanded.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 6.2: Perform Buffer Join with Deduplication

# COMMAND ----------

# TODO: Join with expanded cells and remove duplicates
# YOUR CODE HERE

print("Performing buffer join with k-ring...")
start_time = time.time()

result_buffer = customers_expanded.join(
    zones_df,
    'h3_cell',
    'inner'
).filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select('customer_id', 'zone_id').distinct()  # Remove duplicates!

count_buffer = result_buffer.count()
duration_buffer = time.time() - start_time

print(f"✓ Buffer join completed")
print(f"  Results: {count_buffer:,}")
print(f"  Duration: {duration_buffer:.2f}s")
print(f"  Additional matches vs standard H3: {count_buffer - count_h3:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Performance Summary and Recommendations
# MAGIC
# MAGIC **Task**: Analyze results and provide optimization recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 7.1: Create Performance Comparison

# COMMAND ----------

# TODO: Compile all benchmark results
# YOUR CODE HERE

comparison_data = [
    ('Naive (Broadcast)', customers_sample.count(), duration_naive, count_naive, 1.0),
    ('H3 Index', customers_df.count(), duration_h3, count_h3, speedup),
    ('Partitioned H3', customers_df.count(), duration_partitioned, count_partitioned, estimated_naive_duration/duration_partitioned),
    ('H3 + Buffer', customers_df.count(), duration_buffer, count_buffer, estimated_naive_duration/duration_buffer)
]

comparison_df = spark.createDataFrame(
    comparison_data,
    ['Strategy', 'Customers', 'Duration_s', 'Results', 'Speedup']
)

print("PERFORMANCE COMPARISON")
print("=" * 80)
comparison_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 7.2: Visualize Performance

# COMMAND ----------

# TODO: Create performance visualization
# YOUR CODE HERE

import matplotlib.pyplot as plt

strategies = [row['Strategy'] for row in comparison_data]
durations = [row[2] for row in comparison_data]
speedups = [row[4] for row in comparison_data]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Duration comparison
ax1.barh(strategies, durations, color=['red', 'orange', 'green', 'blue'])
ax1.set_xlabel('Duration (seconds)')
ax1.set_title('Join Strategy Performance')
ax1.grid(axis='x', alpha=0.3)

# Speedup comparison
ax2.barh(strategies, speedups, color=['red', 'orange', 'green', 'blue'])
ax2.set_xlabel('Speedup (vs Naive)')
ax2.set_title('Relative Performance')
ax2.axvline(x=1, color='black', linestyle='--', label='Baseline')
ax2.grid(axis='x', alpha=0.3)
ax2.legend()

plt.tight_layout()
display(plt.gcf())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Challenge - Production Optimization
# MAGIC
# MAGIC **Challenge**: Optimize for a production scenario with specific requirements
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Process 1M customers in < 30 seconds
# MAGIC - Guarantee 100% accuracy (no missed boundaries)
# MAGIC - Minimize memory usage
# MAGIC - Support incremental updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 8.1: Design Optimal Solution

# COMMAND ----------

# TODO: Implement production-ready solution
# Consider:
# - Caching strategy
# - Partition sizing
# - Buffer approach
# - Error handling

# YOUR CODE HERE

print("Production-Optimized Spatial Join")
print("=" * 60)

# Step 1: Cache zones (small, reused)
zones_cached = zones_df.cache()
zones_cached.count()  # Force cache
print("✓ Cached zones")

# Step 2: Optimal partitioning
customers_optimized = customers_with_h3.repartition(100, 'h3_cell')
print("✓ Optimized partitioning")

# Step 3: Efficient join with validation
start_time = time.time()

result_production = customers_optimized.join(
    zones_cached,
    'h3_cell',
    'inner'
).filter(
    contains_udf(col('latitude'), col('longitude'), col('geometry_wkt'))
).select('customer_id', 'zone_id')

# Add quality checks
result_with_qa = result_production.cache()
final_count = result_with_qa.count()

# Check for duplicates
duplicate_count = (
    result_with_qa.groupBy('customer_id').count()
    .filter(col('count') > 1)
    .count()
)

duration_production = time.time() - start_time

print(f"\n✓ Production join completed")
print(f"  Duration: {duration_production:.2f}s")
print(f"  Results: {final_count:,}")
print(f"  Duplicates: {duplicate_count}")
print(f"  Meets < 30s requirement: {duration_production < 30}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution Summary
# MAGIC
# MAGIC ### Key Learnings
# MAGIC
# MAGIC 1. **Indexing is Critical**
# MAGIC    - H3 indexing provides 10-100x speedup
# MAGIC    - Exact match on cell ID is much faster than geometry comparison
# MAGIC
# MAGIC 2. **Partitioning Matters**
# MAGIC    - Co-locating data by spatial key reduces shuffle
# MAGIC    - Optimal partition count balances parallelism and overhead
# MAGIC
# MAGIC 3. **Two-Phase Approach**
# MAGIC    - Phase 1: Fast approximate filter (H3)
# MAGIC    - Phase 2: Precise geometric test (on candidates only)
# MAGIC
# MAGIC 4. **Edge Cases**
# MAGIC    - K-ring buffer catches boundary cases
# MAGIC    - Always deduplicate after buffer joins
# MAGIC
# MAGIC 5. **Production Considerations**
# MAGIC    - Cache small, reused datasets
# MAGIC    - Add quality checks (duplicate detection)
# MAGIC    - Monitor and tune partition sizes
# MAGIC    - Plan for incremental updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenge: Real-Time Updates
# MAGIC
# MAGIC **Bonus Task**: Design a system for real-time customer-to-zone assignment

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO Bonus: Implement Lookup Table Approach

# COMMAND ----------

# TODO: Create materialized H3-to-zone mapping for instant lookups
# YOUR CODE HERE

# Create lookup table
print("Creating H3-to-zone lookup table...")

h3_to_zone_lookup = zones_df.select('h3_cell', 'zone_id', 'geometry_wkt')

# For even faster lookups, explode multi-cell zones
# (In production, store in key-value store like Redis)

h3_to_zone_lookup.write.mode('overwrite').parquet('/tmp/h3_zone_lookup')
print("✓ Saved lookup table")

# Simulate real-time lookup
new_customer_lat, new_customer_lon = 37.7749, -122.4194
new_h3 = h3.geo_to_h3(new_customer_lat, new_customer_lon, zone_resolution)

# Instant lookup
lookup_table = spark.read.parquet('/tmp/h3_zone_lookup')
zone_match = lookup_table.filter(col('h3_cell') == new_h3).first()

if zone_match:
    print(f"\n✓ Real-time lookup for ({new_customer_lat}, {new_customer_lon}):")
    print(f"  H3 cell: {new_h3}")
    print(f"  Zone: {zone_match['zone_id']}")
else:
    print(f"\n✗ No zone found for customer location")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've completed the spatial join optimization lab. You should now be able to:
# MAGIC - ✓ Benchmark and compare join strategies
# MAGIC - ✓ Implement H3-based optimizations
# MAGIC - ✓ Use partitioning for better performance
# MAGIC - ✓ Handle edge cases with buffer joins
# MAGIC - ✓ Design production-ready spatial join pipelines
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Continue to Lab 6: Building H3-Based Analytics Pipeline
# MAGIC - Explore Lab 7: Geofencing and Proximity Analysis
# MAGIC - Apply these techniques to your own datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Spark Join Strategies](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
# MAGIC - [H3 Spatial Joins](https://h3geo.org/docs/highlights/joins)
# MAGIC - [Query Plan Analysis](https://docs.databricks.com/optimizations/spark-ui-guide/index.html)
