# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Spatial Aggregations
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores spatial aggregation techniques for analyzing patterns and trends in geospatial data. You'll learn grid-based aggregations, hot spot analysis, density calculations, and multi-resolution analytics using H3 hexagonal grids.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Perform grid-based spatial aggregations using H3
# MAGIC - Create density heatmaps from point data
# MAGIC - Conduct hot spot analysis (Getis-Ord Gi*)
# MAGIC - Build multi-resolution aggregation hierarchies
# MAGIC - Analyze time-space patterns
# MAGIC - Visualize aggregated spatial data effectively
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1: H3 Hexagonal Indexing
# MAGIC - Understanding of statistical concepts
# MAGIC - Python and Spark proficiency
# MAGIC
# MAGIC ## Duration
# MAGIC 45-50 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install and Import Libraries

# COMMAND ----------

# Install required libraries
%pip install h3 geopandas shapely folium keplergl scipy --quiet

#dbutils.library.restartPython()

# COMMAND ----------

import h3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import folium
from folium import plugins
import numpy as np
from scipy import stats
from pyspark.sql.functions import udf, col, explode, count, sum as spark_sum, avg, stddev, percentile_approx
from pyspark.sql.types import StringType, ArrayType, DoubleType, IntegerType
from pyspark.sql.window import Window
import time
from datetime import datetime, timedelta

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Generate Sample Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Realistic Point Data (Ride-sharing pickups)

# COMMAND ----------

import random
random.seed(42)

# Generate 50,000 ride pickups in San Francisco
num_rides = 50000

# Define hot spots (popular areas)
hotspots = [
    {'name': 'Financial District', 'lat': 37.7946, 'lon': -122.3999, 'weight': 0.25},
    {'name': 'Mission District', 'lat': 37.7599, 'lon': -122.4148, 'weight': 0.20},
    {'name': 'SOMA', 'lat': 37.7786, 'lon': -122.3893, 'weight': 0.15},
    {'name': 'Marina', 'lat': 37.8021, 'lon': -122.4360, 'weight': 0.10},
    {'name': 'Castro', 'lat': 37.7609, 'lon': -122.4350, 'weight': 0.10},
    {'name': 'Haight', 'lat': 37.7699, 'lon': -122.4469, 'weight': 0.10},
    {'name': 'Nob Hill', 'lat': 37.7920, 'lon': -122.4156, 'weight': 0.10}
]

rides = []
for i in range(num_rides):
    # Select hotspot based on weights
    weights = [h['weight'] for h in hotspots]
    hotspot = random.choices(hotspots, weights=weights)[0]
    
    # Generate point near hotspot with some randomness
    lat = hotspot['lat'] + random.gauss(0, 0.005)  # ~500m std dev
    lon = hotspot['lon'] + random.gauss(0, 0.005)
    
    # Add temporal component (24 hours)
    hour = int(random.triangular(6, 22, 18))  # Peak at 6pm
    timestamp = datetime(2024, 1, 15, hour, random.randint(0, 59))
    
    # Add value (fare)
    fare = max(5, random.gauss(15, 8))
    
    rides.append((
        f'R{i:06d}',
        lat,
        lon,
        timestamp,
        hour,
        fare
    ))

# Create Spark DataFrame
rides_df = spark.createDataFrame(
    rides,
    ['ride_id', 'latitude', 'longitude', 'timestamp', 'hour', 'fare']
)

print(f"Created {rides_df.count():,} ride records")
rides_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Grid-Based Aggregation with H3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic H3 Aggregation

# COMMAND ----------

# UDF to convert lat/lon to H3
def geo_to_h3_udf(resolution):
    def convert(lat, lon):
        try:
            return h3.geo_to_h3(lat, lon, resolution)
        except:
            return None
    return udf(convert, StringType())

# Add H3 index at resolution 8 (neighborhood level)
rides_with_h3 = rides_df.withColumn(
    'h3_cell',
    geo_to_h3_udf(8)(col('latitude'), col('longitude'))
)

print("Rides with H3 index:")
rides_with_h3.show(5)

# COMMAND ----------

# Aggregate by H3 cell
h3_aggregated = rides_with_h3.groupBy('h3_cell').agg(
    count('*').alias('ride_count'),
    spark_sum('fare').alias('total_fare'),
    avg('fare').alias('avg_fare')
)

print("H3 Cell Aggregations:")
h3_aggregated.orderBy(col('ride_count').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Grid Aggregation

# COMMAND ----------

# Collect aggregated data for visualization
h3_data = h3_aggregated.collect()

# Create choropleth map
m = folium.Map(location=[37.7749, -122.4194], zoom_start=12)

# Color scale based on ride count
max_rides = max([row['ride_count'] for row in h3_data])
min_rides = min([row['ride_count'] for row in h3_data])

def get_color(count):
    """Get color based on count"""
    normalized = (count - min_rides) / (max_rides - min_rides) if max_rides > min_rides else 0
    
    # Color scale: blue (low) -> yellow -> red (high)
    if normalized < 0.33:
        return '#3288bd'
    elif normalized < 0.67:
        return '#fee08b'
    else:
        return '#d53e4f'

# Add H3 cells to map
for row in h3_data:
    h3_cell = row['h3_cell']
    if h3_cell:
        boundary = h3.h3_to_geo_boundary(h3_cell)
        
        folium.Polygon(
            locations=[(lat, lon) for lat, lon in boundary],
            popup=f"Cell: {h3_cell}<br>Rides: {row['ride_count']}<br>Avg Fare: ${row['avg_fare']:.2f}",
            color=get_color(row['ride_count']),
            fill=True,
            fillColor=get_color(row['ride_count']),
            fillOpacity=0.6,
            weight=1
        ).add_to(m)

m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Multi-Resolution Aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Aggregations at Multiple Resolutions

# COMMAND ----------

# Add multiple H3 resolutions
rides_multi_res = rides_df
for res in [6, 7, 8, 9]:
    rides_multi_res = rides_multi_res.withColumn(
        f'h3_res{res}',
        geo_to_h3_udf(res)(col('latitude'), col('longitude'))
    )

print("Rides with multiple H3 resolutions:")
rides_multi_res.select('ride_id', 'h3_res6', 'h3_res7', 'h3_res8', 'h3_res9').show(5, truncate=False)

# COMMAND ----------

# Aggregate at each resolution
aggregations = {}

print("Multi-Resolution Aggregation Summary:")
print("=" * 60)

for res in [6, 7, 8, 9]:
    agg = rides_multi_res.groupBy(f'h3_res{res}').agg(
        count('*').alias('ride_count'),
        avg('fare').alias('avg_fare')
    )
    
    cell_count = agg.count()
    total_rides = rides_multi_res.count()
    avg_rides_per_cell = total_rides / cell_count
    
    aggregations[res] = agg
    
    print(f"Resolution {res}:")
    print(f"  Cells: {cell_count}")
    print(f"  Avg rides per cell: {avg_rides_per_cell:.1f}")
    print(f"  Cell area: {h3.hex_area(res, 'km^2'):.3f} km²")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hierarchical Drill-Down Analysis

# COMMAND ----------

# Find top cell at resolution 6
top_cell_res6 = aggregations[6].orderBy(col('ride_count').desc()).first()
print(f"Top cell at resolution 6: {top_cell_res6['h3_res6']}")
print(f"Rides: {top_cell_res6['ride_count']}")

# Get children cells at resolution 7
children_res7 = h3.h3_to_children(top_cell_res6['h3_res6'], 7)
print(f"\nChildren at resolution 7: {len(children_res7)} cells")

# Filter rides in children cells
children_data = rides_multi_res.filter(
    col('h3_res7').isin(list(children_res7))
).groupBy('h3_res7').agg(
    count('*').alias('ride_count'),
    avg('fare').alias('avg_fare')
).orderBy(col('ride_count').desc())

print("\nChildren cell statistics:")
children_data.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Density Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Point Density

# COMMAND ----------

# Calculate density (rides per km²) for each cell
h3_density = h3_aggregated.rdd.map(lambda row: (
    row['h3_cell'],
    row['ride_count'],
    row['ride_count'] / h3.hex_area(8, 'km^2'),  # Density
    row['total_fare'],
    row['avg_fare']
)).toDF(['h3_cell', 'ride_count', 'density', 'total_fare', 'avg_fare'])

print("Density Analysis:")
h3_density.orderBy(col('density').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Density Heatmap

# COMMAND ----------

# Convert to pandas for heatmap
rides_pd = rides_df.select('latitude', 'longitude').toPandas()

# Create heatmap
m = folium.Map(location=[37.7749, -122.4194], zoom_start=12)

heat_data = [[row['latitude'], row['longitude']] for _, row in rides_pd.iterrows()]

plugins.HeatMap(
    heat_data,
    min_opacity=0.3,
    max_zoom=13,
    radius=15,
    blur=20,
    gradient={
        0.0: 'blue',
        0.5: 'lime',
        0.7: 'yellow',
        1.0: 'red'
    }
).add_to(m)

m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Hot Spot Analysis (Getis-Ord Gi*)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Local Moran's I and Gi* Statistics

# COMMAND ----------

# Get H3 cells with neighbors
h3_cells = [row['h3_cell'] for row in h3_aggregated.collect()]

# Create adjacency information
adjacency_data = []
for cell in h3_cells[:100]:  # Limit for demo
    neighbors = h3.hex_ring(cell, 1)
    for neighbor in neighbors:
        if neighbor in h3_cells:
            adjacency_data.append((cell, neighbor))

print(f"Created adjacency relationships for {len(set([a[0] for a in adjacency_data]))} cells")

# COMMAND ----------

# Calculate Gi* statistic for hot spot analysis
# Join cell values with neighbor values

from pyspark.sql import Window
from pyspark.sql.functions import collect_list

# Create neighbor mapping
neighbors_df = spark.createDataFrame(adjacency_data, ['h3_cell', 'neighbor_cell'])

# Join with aggregated data
cell_with_neighbors = neighbors_df.join(
    h3_density.select('h3_cell', col('ride_count').alias('value')),
    'h3_cell'
).join(
    h3_density.select(col('h3_cell').alias('neighbor_cell'), col('ride_count').alias('neighbor_value')),
    'neighbor_cell'
)

# Calculate statistics for each cell
gi_stats = cell_with_neighbors.groupBy('h3_cell', 'value').agg(
    count('neighbor_cell').alias('neighbor_count'),
    spark_sum('neighbor_value').alias('neighbor_sum'),
    avg('neighbor_value').alias('neighbor_avg')
)

print("Hot Spot Analysis (Gi* approximation):")
gi_stats.orderBy(col('neighbor_sum').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify Statistically Significant Hot Spots

# COMMAND ----------

# Calculate z-scores for hot spot identification
# Get global statistics
global_stats = h3_density.agg(
    avg('ride_count').alias('global_mean'),
    stddev('ride_count').alias('global_std')
).first()

global_mean = global_stats['global_mean']
global_std = global_stats['global_std']

print(f"Global statistics:")
print(f"  Mean rides per cell: {global_mean:.2f}")
print(f"  Std dev: {global_std:.2f}")

# Calculate z-scores
from pyspark.sql.functions import lit

h3_with_zscore = h3_density.withColumn(
    'z_score',
    (col('ride_count') - lit(global_mean)) / lit(global_std)
)

# Classify hot spots
h3_classified = h3_with_zscore.withColumn(
    'classification',
    when(col('z_score') > 2.58, 'Hot Spot (99% confidence)')
    .when(col('z_score') > 1.96, 'Hot Spot (95% confidence)')
    .when(col('z_score') < -2.58, 'Cold Spot (99% confidence)')
    .when(col('z_score') < -1.96, 'Cold Spot (95% confidence)')
    .otherwise('Not Significant')
)

print("\nHot and Cold Spot Classification:")
h3_classified.groupBy('classification').count().show()

print("\nTop Hot Spots:")
h3_classified.filter(col('classification').contains('Hot')).orderBy(col('z_score').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Temporal Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time-Space Analysis

# COMMAND ----------

# Aggregate by H3 cell and hour
temporal_agg = rides_with_h3.groupBy('h3_cell', 'hour').agg(
    count('*').alias('ride_count'),
    avg('fare').alias('avg_fare')
)

print("Temporal-Spatial Aggregation:")
temporal_agg.orderBy('h3_cell', 'hour').show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify Peak Hours by Location

# COMMAND ----------

# Find peak hour for each cell
from pyspark.sql.functions import when

window_spec = Window.partitionBy('h3_cell').orderBy(col('ride_count').desc())
from pyspark.sql.functions import row_number

peak_hours = temporal_agg.withColumn(
    'rank',
    row_number().over(window_spec)
).filter(col('rank') == 1).drop('rank')

print("Peak Hours by Location:")
peak_hours.orderBy(col('ride_count').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Series by Cell

# COMMAND ----------

# Select a high-activity cell
top_cell = h3_aggregated.orderBy(col('ride_count').desc()).first()['h3_cell']

# Get hourly time series for that cell
time_series = temporal_agg.filter(col('h3_cell') == top_cell).orderBy('hour')

print(f"Hourly activity for cell {top_cell}:")
time_series.show(24)

# COMMAND ----------

# Visualize time series
ts_data = time_series.toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))
plt.bar(ts_data['hour'], ts_data['ride_count'], color='steelblue', alpha=0.7)
plt.xlabel('Hour of Day')
plt.ylabel('Number of Rides')
plt.title(f'Hourly Ride Pattern for Cell {top_cell}')
plt.xticks(range(0, 24))
plt.grid(axis='y', alpha=0.3)
display(plt.gcf())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Advanced Aggregation Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rolling Aggregations

# COMMAND ----------

# Create moving average over neighboring cells
def get_k_ring_cells(center_cell, k=1):
    """Get k-ring of cells"""
    try:
        return list(h3.k_ring(center_cell, k))
    except:
        return []

k_ring_udf = udf(lambda cell: get_k_ring_cells(cell, 1), ArrayType(StringType()))

# Add k-ring cells
h3_with_neighbors = h3_aggregated.withColumn(
    'neighbor_cells',
    k_ring_udf(col('h3_cell'))
)

# Explode neighbors
h3_neighbors_exploded = h3_with_neighbors.select(
    col('h3_cell').alias('center_cell'),
    explode(col('neighbor_cells')).alias('neighbor_cell')
)

# Join to get neighbor values
neighbor_values = h3_neighbors_exploded.join(
    h3_aggregated.select(
        col('h3_cell').alias('neighbor_cell'),
        col('ride_count').alias('neighbor_ride_count')
    ),
    'neighbor_cell',
    'left'
)

# Calculate smoothed values (average of cell + neighbors)
smoothed = neighbor_values.groupBy('center_cell').agg(
    avg('neighbor_ride_count').alias('smoothed_ride_count')
)

print("Smoothed aggregation (including neighbors):")
smoothed.orderBy(col('smoothed_ride_count').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Percentile Aggregations

# COMMAND ----------

# Calculate percentiles by H3 cell
percentile_agg = rides_with_h3.groupBy('h3_cell').agg(
    count('*').alias('ride_count'),
    avg('fare').alias('avg_fare'),
    percentile_approx('fare', 0.5).alias('median_fare'),
    percentile_approx('fare', 0.25).alias('p25_fare'),
    percentile_approx('fare', 0.75).alias('p75_fare'),
    percentile_approx('fare', 0.95).alias('p95_fare')
)

print("Fare Percentiles by Location:")
percentile_agg.orderBy(col('ride_count').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Optimized Aggregation Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-aggregated Lookup Tables

# COMMAND ----------

# Create pre-aggregated table for fast queries
# Cache for reuse
h3_lookup = h3_aggregated.cache()

print(f"Cached aggregation table with {h3_lookup.count()} cells")

# Query performance
start = time.time()
result = h3_lookup.filter(col('ride_count') > 100).count()
duration = time.time() - start

print(f"Query filtered {result} cells in {duration:.3f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialized Aggregations

# COMMAND ----------

# Save aggregated data for reuse
output_path = "/tmp/h3_aggregations"

h3_aggregated.write.mode('overwrite').parquet(output_path)
print(f"Saved aggregations to {output_path}")

# Read back
h3_materialized = spark.read.parquet(output_path)
print(f"Loaded {h3_materialized.count()} aggregated cells")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Grid Selection
# MAGIC - Choose H3 resolution based on analysis scale
# MAGIC - Lower resolution (6-7) for regional patterns
# MAGIC - Higher resolution (8-10) for local patterns
# MAGIC - Use multiple resolutions for drill-down analysis
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC - Pre-compute and cache aggregations
# MAGIC - Materialize frequently-used aggregations
# MAGIC - Use appropriate partition sizes
# MAGIC - Leverage H3 hierarchical structure
# MAGIC
# MAGIC ### Statistical Analysis
# MAGIC - Calculate z-scores for hot spot identification
# MAGIC - Use neighbor values for spatial autocorrelation
# MAGIC - Consider temporal patterns in analysis
# MAGIC - Validate statistical significance
# MAGIC
# MAGIC ### Visualization
# MAGIC - Use choropleth maps for discrete aggregations
# MAGIC - Use heatmaps for continuous density
# MAGIC - Color scales should reflect data distribution
# MAGIC - Add interactive popups with details

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Demand Forecasting
# MAGIC ```python
# MAGIC # Aggregate historical data by location and time
# MAGIC historical = rides.groupBy('h3_cell', 'hour', 'day_of_week').agg(
# MAGIC     avg('ride_count').alias('avg_demand')
# MAGIC )
# MAGIC
# MAGIC # Use for real-time predictions
# MAGIC predicted_demand = historical.filter(
# MAGIC     (col('h3_cell') == current_cell) &
# MAGIC     (col('hour') == current_hour) &
# MAGIC     (col('day_of_week') == current_dow)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Service Coverage Analysis
# MAGIC ```python
# MAGIC # Identify underserved areas
# MAGIC coverage = rides.groupBy('h3_cell').agg(
# MAGIC     count('*').alias('ride_count')
# MAGIC )
# MAGIC
# MAGIC # All possible cells in region
# MAGIC all_cells = generate_h3_grid(region, resolution)
# MAGIC
# MAGIC # Left join to find gaps
# MAGIC gaps = all_cells.join(coverage, 'h3_cell', 'left') \
# MAGIC     .filter(col('ride_count').isNull())
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Revenue Optimization
# MAGIC ```python
# MAGIC # Find high-value areas
# MAGIC revenue_density = rides.groupBy('h3_cell').agg(
# MAGIC     sum('fare').alias('total_revenue'),
# MAGIC     count('*').alias('ride_count'),
# MAGIC     (sum('fare') / hex_area(resolution)).alias('revenue_density')
# MAGIC )
# MAGIC
# MAGIC # Prioritize resources
# MAGIC priority_areas = revenue_density.orderBy(
# MAGIC     col('revenue_density').desc()
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue with:
# MAGIC - **Lab 5**: Optimizing Large-Scale Spatial Joins
# MAGIC - **Lab 6**: Building H3-Based Analytics Pipeline (hands-on with aggregations)
# MAGIC - **Lab 7**: Geofencing and Proximity Analysis
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC - H3 grids enable efficient spatial aggregations
# MAGIC - Multi-resolution analysis provides insights at different scales
# MAGIC - Hot spot analysis identifies statistically significant patterns
# MAGIC - Temporal aggregations reveal time-space dynamics
# MAGIC - Pre-computed aggregations improve query performance
# MAGIC - Choose visualization type based on data characteristics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [H3 Aggregation Examples](https://h3geo.org/docs/highlights/aggregation)
# MAGIC - [Spatial Statistics in Python](https://pysal.org/)
# MAGIC - [Getis-Ord Gi* Statistic](https://en.wikipedia.org/wiki/Getis%E2%80%93Ord_statistics)
# MAGIC - [Kepler.gl Aggregation Layers](https://docs.kepler.gl/docs/user-guides/c-types-of-layers/k-hexagon)
# MAGIC - [Spark Aggregation Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
