# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: H3 Hexagonal Indexing
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo explores the H3 hexagonal hierarchical spatial indexing system. You'll learn how to convert locations to H3 cells, traverse the grid, perform proximity analysis, and leverage H3 for efficient geospatial operations at scale.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Convert geographic coordinates to H3 cell indices
# MAGIC - Select appropriate H3 resolutions for different use cases
# MAGIC - Perform grid traversal and neighbor operations
# MAGIC - Use k-ring functions for proximity analysis
# MAGIC - Apply polyfill to convert polygons to H3 cells
# MAGIC - Optimize spatial queries using H3 indexing
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Part 1: Foundations of Geospatial Analytics
# MAGIC - Understanding of basic spatial concepts
# MAGIC - Python proficiency
# MAGIC
# MAGIC ## Duration
# MAGIC 40-45 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install and Import Libraries

# COMMAND ----------

# Install H3 and geospatial libraries
%pip install h3 geopandas --quiet

# Restart Python kernel to use new packages
#dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install shapely folium keplergl --quiet

# COMMAND ----------

# Import required libraries
import h3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, mapping
import folium
import json
from pyspark.sql.functions import udf, col, explode, array
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField

print(f"H3 version: {h3.__version__}")
print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Basic H3 Operations
# MAGIC
# MAGIC ### Converting Coordinates to H3 Cells

# COMMAND ----------

# Example: San Francisco location
lat = 37.7749
lon = -122.4194

# Convert to H3 cells at different resolutions
print("San Francisco H3 Cells at Different Resolutions:")
print("=" * 60)

for resolution in range(0, 16):
    h3_cell = h3.geo_to_h3(lat, lon, resolution)
    # Get cell stats
    area_km2 = h3.hex_area(resolution, unit='km^2')
    edge_km = h3.edge_length(resolution, unit='km')
    
    print(f"Res {resolution:2d}: {h3_cell:15s} | Area: {area_km2:12,.2f} km² | Edge: {edge_km:8.3f} km")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding H3 Cell IDs
# MAGIC
# MAGIC H3 cell IDs encode:
# MAGIC - Resolution level
# MAGIC - Position on Earth
# MAGIC - Hierarchical relationships

# COMMAND ----------

# Explore H3 cell properties
target_resolution = 7
h3_cell = h3.geo_to_h3(lat, lon, target_resolution)

print(f"H3 Cell: {h3_cell}")
print(f"Resolution: {h3.h3_get_resolution(h3_cell)}")
print(f"Is valid: {h3.h3_is_valid(h3_cell)}")
print(f"Is pentagon: {h3.h3_is_pentagon(h3_cell)}")

# Get cell center (lat, lon)
center = h3.h3_to_geo(h3_cell)
print(f"Cell center: {center}")

# Get cell boundary
boundary = h3.h3_to_geo_boundary(h3_cell)
print(f"Boundary vertices: {len(boundary)} points")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing H3 Cells

# COMMAND ----------

def visualize_h3_cell(lat, lon, resolution, zoom=12):
    """Create a map showing an H3 cell"""
    # Get H3 cell
    h3_cell = h3.geo_to_h3(lat, lon, resolution)
    
    # Get boundary
    boundary = h3.h3_to_geo_boundary(h3_cell)
    boundary_coords = [(lat, lon) for lat, lon in boundary]
    
    # Create map
    m = folium.Map(location=[lat, lon], zoom_start=zoom)
    
    # Add point
    folium.Marker(
        [lat, lon],
        popup=f"Original Point<br>Lat: {lat}<br>Lon: {lon}",
        icon=folium.Icon(color='red', icon='info-sign')
    ).add_to(m)
    
    # Add H3 cell
    folium.Polygon(
        locations=boundary_coords,
        popup=f"H3 Cell: {h3_cell}<br>Resolution: {resolution}",
        color='blue',
        fill=True,
        fillOpacity=0.3
    ).add_to(m)
    
    return m

# Visualize at different resolutions
print("Visualizing H3 cells at resolution 7, 9, and 11")
map_res7 = visualize_h3_cell(lat, lon, 7, zoom=11)
map_res7

# COMMAND ----------

map_res9 = visualize_h3_cell(lat, lon, 9, zoom=13)
map_res9

# COMMAND ----------

map_res11 = visualize_h3_cell(lat, lon, 11, zoom=15)
map_res11

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: H3 Grid Traversal
# MAGIC
# MAGIC ### Neighbor Operations

# COMMAND ----------

# Get neighbors of a cell
h3_cell = h3.geo_to_h3(lat, lon, 9)
neighbors = h3.hex_ring(h3_cell, 1)  # 1-ring neighbors

print(f"Center cell: {h3_cell}")
print(f"Number of neighbors: {len(neighbors)}")
print(f"Neighbors: {neighbors}")

# COMMAND ----------

def visualize_neighbors(lat, lon, resolution):
    """Visualize H3 cell with its neighbors"""
    h3_cell = h3.geo_to_h3(lat, lon, resolution)
    neighbors = h3.hex_ring(h3_cell, 1)
    
    m = folium.Map(location=[lat, lon], zoom_start=13)
    
    # Draw center cell
    boundary = h3.h3_to_geo_boundary(h3_cell)
    folium.Polygon(
        locations=[(lat, lon) for lat, lon in boundary],
        popup=f"Center: {h3_cell}",
        color='red',
        fill=True,
        fillOpacity=0.5
    ).add_to(m)
    
    # Draw neighbors
    for neighbor in neighbors:
        boundary = h3.h3_to_geo_boundary(neighbor)
        folium.Polygon(
            locations=[(lat, lon) for lat, lon in boundary],
            popup=f"Neighbor: {neighbor}",
            color='blue',
            fill=True,
            fillOpacity=0.3
        ).add_to(m)
    
    return m

neighbor_map = visualize_neighbors(lat, lon, 9)
neighbor_map

# COMMAND ----------

# MAGIC %md
# MAGIC ### K-Ring Operations
# MAGIC
# MAGIC K-ring gets all cells within k steps from center cell

# COMMAND ----------

# Get k-ring at different distances
h3_cell = h3.geo_to_h3(lat, lon, 9)

for k in [1, 2, 3]:
    k_ring = h3.k_ring(h3_cell, k)
    print(f"K-ring {k}: {len(k_ring)} cells")

# COMMAND ----------

def visualize_k_ring(lat, lon, resolution, k):
    """Visualize k-ring around a cell"""
    h3_cell = h3.geo_to_h3(lat, lon, resolution)
    
    m = folium.Map(location=[lat, lon], zoom_start=12)
    
    # Get k-ring
    k_ring = h3.k_ring(h3_cell, k)
    
    # Color scale
    colors = ['red', 'orange', 'yellow', 'green', 'blue', 'purple']
    
    # Draw all cells
    for cell in k_ring:
        # Calculate distance from center
        distance = h3.h3_distance(h3_cell, cell)
        color = colors[min(distance, len(colors)-1)]
        
        boundary = h3.h3_to_geo_boundary(cell)
        folium.Polygon(
            locations=[(lat, lon) for lat, lon in boundary],
            popup=f"Distance: {distance}",
            color=color,
            fill=True,
            fillOpacity=0.4
        ).add_to(m)
    
    return m

k_ring_map = visualize_k_ring(lat, lon, 9, k=3)
k_ring_map

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Hierarchical Operations
# MAGIC
# MAGIC ### Parent-Child Relationships

# COMMAND ----------

# Get parent and children cells
h3_cell = h3.geo_to_h3(lat, lon, 8)

# Parent cell (lower resolution)
parent = h3.h3_to_parent(h3_cell, 7)
print(f"Cell at res 8: {h3_cell}")
print(f"Parent at res 7: {parent}")

# Children cells (higher resolution)
children = h3.h3_to_children(h3_cell, 9)
print(f"Children at res 9: {len(children)} cells")
print(f"First 5 children: {list(children)[:5]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolution Changes
# MAGIC
# MAGIC H3 cells have a 7:1 parent-child ratio (except at pentagons)

# COMMAND ----------

# Demonstrate hierarchical structure
base_cell = h3.geo_to_h3(lat, lon, 6)

print("Hierarchical structure:")
for res in range(6, 10):
    if res == 6:
        cell = base_cell
    else:
        cell = h3.h3_to_children(cell, res)
        cell = list(cell)[0]  # Take first child
    
    print(f"Resolution {res}: {cell}")
    
    # Get children count
    if res < 9:
        children = h3.h3_to_children(cell, res + 1)
        print(f"  → {len(children)} children at resolution {res + 1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Polyfill - Polygons to H3 Cells

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting Polygons to H3 Cells

# COMMAND ----------

# Create a sample polygon (downtown San Francisco area)
downtown_sf = Polygon([
    (-122.4194, 37.7749),  # Union Square
    (-122.4000, 37.7749),
    (-122.4000, 37.7900),
    (-122.4194, 37.7900),
    (-122.4194, 37.7749)
])

# Convert to H3 GeoJSON format
geojson = mapping(downtown_sf)

# Polyfill at different resolutions
print("Polyfill results:")
for res in [7, 8, 9, 10]:
    h3_cells = h3.polyfill(geojson, res, geo_json_conformant=True)
    print(f"Resolution {res}: {len(h3_cells)} cells")

# COMMAND ----------

def visualize_polyfill(polygon, resolution):
    """Visualize polygon with its H3 polyfill"""
    geojson = mapping(polygon)
    h3_cells = h3.polyfill(geojson, resolution, geo_json_conformant=True)
    
    # Get center for map
    center = polygon.centroid
    m = folium.Map(location=[center.y, center.x], zoom_start=13)
    
    # Draw original polygon
    coords = list(polygon.exterior.coords)
    folium.Polygon(
        locations=[(lat, lon) for lon, lat in coords],
        popup="Original Polygon",
        color='red',
        fill=False,
        weight=3
    ).add_to(m)
    
    # Draw H3 cells
    for cell in h3_cells:
        boundary = h3.h3_to_geo_boundary(cell)
        folium.Polygon(
            locations=[(lat, lon) for lat, lon in boundary],
            popup=f"Cell: {cell}",
            color='blue',
            fill=True,
            fillOpacity=0.3
        ).add_to(m)
    
    return m

polyfill_map = visualize_polyfill(downtown_sf, 9)
polyfill_map

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Compact and Uncompact Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compact: Optimize Cell Storage
# MAGIC
# MAGIC Compact replaces child cells with parent cells when all children are present

# COMMAND ----------

# Get H3 cells for a large area
large_area = Polygon([
    (-122.45, 37.75),
    (-122.35, 37.75),
    (-122.35, 37.82),
    (-122.45, 37.82),
    (-122.45, 37.75)
])

geojson = mapping(large_area)
h3_cells = h3.polyfill(geojson, 9, geo_json_conformant=True)

print(f"Original cells (res 9): {len(h3_cells)}")

# Compact cells
compacted = h3.compact(h3_cells)
print(f"Compacted cells: {len(compacted)}")
print(f"Space savings: {(1 - len(compacted)/len(h3_cells))*100:.1f}%")

# Check resolutions in compacted set
resolutions = [h3.h3_get_resolution(cell) for cell in compacted]
print(f"Resolutions in compacted set: {set(resolutions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uncompact: Expand to Uniform Resolution

# COMMAND ----------

# Uncompact back to resolution 9
uncompacted = h3.uncompact(compacted, 9)
print(f"Uncompacted cells: {len(uncompacted)}")
print(f"Matches original: {h3_cells == uncompacted}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Practical Applications with Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sample Dataset

# COMMAND ----------

# Generate random locations in San Francisco
import random

random.seed(42)

# Generate 10,000 random points in SF area
locations = []
for i in range(10000):
    lat = 37.7 + random.random() * 0.15  # 37.7 to 37.85
    lon = -122.5 + random.random() * 0.15  # -122.5 to -122.35
    value = random.randint(1, 100)
    locations.append((i, lat, lon, value))

# Create Spark DataFrame
df = spark.createDataFrame(locations, ['id', 'latitude', 'longitude', 'value'])
print(f"Created dataset with {df.count()} locations")
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add H3 Index to DataFrame

# COMMAND ----------

# Create UDF to convert lat/lon to H3
def geo_to_h3_udf(resolution):
    def convert(lat, lon):
        try:
            return h3.geo_to_h3(lat, lon, resolution)
        except:
            return None
    return udf(convert, StringType())

# Add H3 columns at multiple resolutions
df_with_h3 = df \
    .withColumn('h3_res7', geo_to_h3_udf(7)(col('latitude'), col('longitude'))) \
    .withColumn('h3_res8', geo_to_h3_udf(8)(col('latitude'), col('longitude'))) \
    .withColumn('h3_res9', geo_to_h3_udf(9)(col('latitude'), col('longitude')))

df_with_h3.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3-Based Aggregations

# COMMAND ----------

# Aggregate by H3 cells at different resolutions
print("Aggregation by H3 Resolution:")
print("=" * 60)

for res in [7, 8, 9]:
    agg_df = df_with_h3.groupBy(f'h3_res{res}').agg(
        {'value': 'count', 'value': 'sum'}
    ).withColumnRenamed('count(value)', 'count') \
      .withColumnRenamed('sum(value)', 'total_value')
    
    cell_count = agg_df.count()
    print(f"Resolution {res}: {cell_count} cells")
    
    # Show top 5 cells by count
    print(f"Top 5 cells by count:")
    agg_df.orderBy(col('count').desc()).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Proximity Queries with K-Ring

# COMMAND ----------

# Find all points within 2km of a target location
target_lat, target_lon = 37.7749, -122.4194
target_resolution = 9

# Get target H3 cell and k-ring
target_h3 = h3.geo_to_h3(target_lat, target_lon, target_resolution)
search_cells = h3.k_ring(target_h3, 2)  # 2-ring ≈ 2km at res 9

print(f"Target cell: {target_h3}")
print(f"Search cells: {len(search_cells)}")

# Filter DataFrame to search cells
nearby_points = df_with_h3.filter(col('h3_res9').isin(search_cells))
nearby_count = nearby_points.count()

print(f"Found {nearby_count} points in search area")
nearby_points.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Performance Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare: Naive Distance vs H3 K-Ring

# COMMAND ----------

import time
from math import radians, sin, cos, sqrt, atan2

# Naive approach: Calculate distance for all points
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km"""
    R = 6371  # Earth radius in km
    
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c

# Register UDF
haversine_udf = udf(lambda lat, lon: haversine_distance(target_lat, target_lon, lat, lon), DoubleType())

# Method 1: Naive distance calculation
start = time.time()
df_with_distance = df.withColumn('distance', haversine_udf(col('latitude'), col('longitude')))
nearby_naive = df_with_distance.filter(col('distance') <= 2.0)
count_naive = nearby_naive.count()
time_naive = time.time() - start

print(f"Naive approach: {count_naive} points found in {time_naive:.3f}s")

# Method 2: H3 k-ring filter
start = time.time()
nearby_h3 = df_with_h3.filter(col('h3_res9').isin(search_cells))
count_h3 = nearby_h3.count()
time_h3 = time.time() - start

print(f"H3 approach: {count_h3} points found in {time_h3:.3f}s")
print(f"Speedup: {time_naive/time_h3:.1f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Resolution Selection Guidelines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Data Distribution Across Resolutions

# COMMAND ----------

def analyze_resolution(df, lat_col, lon_col, resolution):
    """Analyze data distribution at a given H3 resolution"""
    
    # Add H3 column
    df_h3 = df.withColumn('h3_cell', geo_to_h3_udf(resolution)(col(lat_col), col(lon_col)))
    
    # Aggregate by cell
    stats = df_h3.groupBy('h3_cell').agg(
        {'*': 'count'}
    ).withColumnRenamed('count(1)', 'point_count')
    
    # Calculate statistics
    cell_count = stats.count()
    total_points = df.count()
    avg_points = total_points / cell_count if cell_count > 0 else 0
    
    # Get distribution
    distribution = stats.agg({
        'point_count': 'min',
        'point_count': 'max',
        'point_count': 'avg',
        'point_count': 'stddev'
    }).collect()[0]
    
    return {
        'resolution': resolution,
        'num_cells': cell_count,
        'total_points': total_points,
        'avg_points_per_cell': avg_points,
        'min_points': distribution['min(point_count)'],
        'max_points': distribution['max(point_count)'],
        'mean_points': distribution['avg(point_count)'],
        'stddev_points': distribution['stddev_samp(point_count)']
    }

# Analyze multiple resolutions
print("H3 Resolution Analysis:")
print("=" * 80)

results = []
for res in range(6, 11):
    result = analyze_resolution(df, 'latitude', 'longitude', res)
    results.append(result)
    
    print(f"Resolution {res}:")
    print(f"  Cells: {result['num_cells']:,}")
    print(f"  Avg points/cell: {result['avg_points_per_cell']:.1f}")
    print(f"  Min/Max: {result['min_points']}/{result['max_points']}")
    print(f"  Std Dev: {result['stddev_points']:.1f}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Resolution Selection
# MAGIC - **Too low**: Large cells, poor granularity, less effective filtering
# MAGIC - **Too high**: Many cells, overhead increases, diminishing returns
# MAGIC - **Sweet spot**: 10-100 points per cell on average
# MAGIC
# MAGIC ### Performance Tips
# MAGIC - Use H3 for initial filtering, then precise geometry for accuracy
# MAGIC - Cache H3 indices when reusing across queries
# MAGIC - Use compact() for storage, uncompact() for querying
# MAGIC - Partition Spark DataFrames by H3 cell for spatial locality
# MAGIC
# MAGIC ### Common Use Cases
# MAGIC - **Res 6-7**: Regional analysis, city-level aggregations
# MAGIC - **Res 8-9**: Neighborhood, delivery zones, store catchment areas
# MAGIC - **Res 10-11**: Building-level, precise location tracking
# MAGIC - **Res 12+**: Indoor positioning, high-precision applications

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand H3 indexing, continue with:
# MAGIC - **Demo 2**: Spatial Joins at Scale using H3
# MAGIC - **Demo 3**: Advanced Geometric Operations
# MAGIC - **Demo 4**: Spatial Aggregations with H3
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC - H3 provides fast, hierarchical spatial indexing
# MAGIC - Resolution selection depends on use case and data density
# MAGIC - K-ring operations enable efficient proximity queries
# MAGIC - Polyfill converts polygons to H3 cells
# MAGIC - Compact/uncompact optimize storage and querying
# MAGIC - H3 + Spark = scalable geospatial analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [H3 Documentation](https://h3geo.org/)
# MAGIC - [H3-py GitHub](https://github.com/uber/h3-py)
# MAGIC - [Uber H3 Blog](https://eng.uber.com/h3/)
# MAGIC - [H3 Resolution Table](https://h3geo.org/docs/core-library/restable/)
