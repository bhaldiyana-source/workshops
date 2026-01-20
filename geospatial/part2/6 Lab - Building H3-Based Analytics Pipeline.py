# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building H3-Based Analytics Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll build a complete analytics pipeline using H3 hexagonal indexing. You'll aggregate spatial data at multiple resolutions, create drill-down capabilities, and build interactive visualizations for geospatial insights.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Design multi-resolution H3 aggregation pipelines
# MAGIC - Implement hierarchical drill-down analytics
# MAGIC - Create materialized aggregation layers for performance
# MAGIC - Build interactive geospatial visualizations
# MAGIC - Optimize pipeline for production workloads
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1: H3 Hexagonal Indexing
# MAGIC - Completion of Demo 4: Spatial Aggregations
# MAGIC - Understanding of data pipeline concepts
# MAGIC
# MAGIC ## Duration
# MAGIC 60-75 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install required libraries
%pip install h3 geopandas shapely folium keplergl --quiet

#dbutils.library.restartPython()

# COMMAND ----------

import h3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import folium
from folium import plugins
import json
import time
from datetime import datetime, timedelta
import numpy as np
import random
from pyspark.sql.functions import (
    udf, col, count, sum as spark_sum, avg, stddev, min as spark_min, 
    max as spark_max, lit, when, explode, array, struct, to_json
)
from pyspark.sql.types import StringType, ArrayType, DoubleType, IntegerType, StructType, StructField
from pyspark.sql.window import Window

print("✓ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Generate Event Stream Data
# MAGIC
# MAGIC **Task**: Create a realistic event dataset for a delivery service
# MAGIC
# MAGIC **Requirements**:
# MAGIC - 100,000 delivery events over 7 days
# MAGIC - Multiple delivery types (food, grocery, package)
# MAGIC - Temporal patterns (rush hours, weekdays vs weekends)
# MAGIC - Geographic clustering around urban areas

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.1: Generate Delivery Events

# COMMAND ----------

# TODO: Create synthetic delivery event data
# STARTER CODE PROVIDED

random.seed(42)
np.random.seed(42)

# Service areas (San Francisco neighborhoods)
service_areas = [
    {'name': 'Financial District', 'lat': 37.7946, 'lon': -122.3999, 'density': 0.25},
    {'name': 'Mission', 'lat': 37.7599, 'lon': -122.4148, 'density': 0.20},
    {'name': 'SOMA', 'lat': 37.7786, 'lon': -122.3893, 'density': 0.15},
    {'name': 'Marina', 'lat': 37.8021, 'lon': -122.4360, 'density': 0.10},
    {'name': 'Castro', 'lat': 37.7609, 'lon': -122.4350, 'density': 0.10},
    {'name': 'Richmond', 'lat': 37.7806, 'lon': -122.4639, 'density': 0.10},
    {'name': 'Sunset', 'lat': 37.7516, 'lon': -122.4668, 'density': 0.10}
]

delivery_types = [
    {'type': 'food', 'weight': 0.50, 'avg_value': 35},
    {'type': 'grocery', 'weight': 0.30, 'avg_value': 85},
    {'type': 'package', 'weight': 0.20, 'avg_value': 55}
]

# YOUR CODE HERE: Generate 100K delivery events
num_events = 100000
start_date = datetime(2024, 1, 1)

events = []
for i in range(num_events):
    # Select service area
    area = random.choices(service_areas, weights=[a['density'] for a in service_areas])[0]
    
    # Generate location with clustering
    lat = area['lat'] + np.random.randn() * 0.008
    lon = area['lon'] + np.random.randn() * 0.008
    
    # Generate timestamp with daily patterns
    day_offset = random.randint(0, 6)
    hour = int(random.triangular(8, 22, 12))  # Peak at noon
    minute = random.randint(0, 59)
    timestamp = start_date + timedelta(days=day_offset, hours=hour, minutes=minute)
    
    # Select delivery type
    delivery = random.choices(delivery_types, weights=[d['weight'] for d in delivery_types])[0]
    delivery_value = max(5, np.random.normal(delivery['avg_value'], 15))
    
    # Delivery time (minutes)
    delivery_time = max(10, int(np.random.normal(30, 10)))
    
    events.append((
        f'DEL{i:06d}',
        timestamp,
        timestamp.date(),
        hour,
        timestamp.strftime('%A'),
        lat,
        lon,
        delivery['type'],
        round(delivery_value, 2),
        delivery_time
    ))

# Create DataFrame
events_df = spark.createDataFrame(
    events,
    ['delivery_id', 'timestamp', 'date', 'hour', 'day_of_week', 'latitude', 'longitude', 'delivery_type', 'value', 'delivery_time_min']
)

# Validate
assert events_df.count() == num_events, f"Should have {num_events} events"
print(f"✓ Generated {events_df.count():,} delivery events")
events_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Build Multi-Resolution H3 Index
# MAGIC
# MAGIC **Task**: Add H3 indices at multiple resolutions for hierarchical analysis
# MAGIC
# MAGIC **Resolutions to add**: 6 (city), 7 (district), 8 (neighborhood), 9 (block)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2.1: Add Multi-Resolution H3 Columns

# COMMAND ----------

# TODO: Add H3 columns at resolutions 6, 7, 8, 9
# YOUR CODE HERE

def geo_to_h3_func(resolution):
    def convert(lat, lon):
        try:
            return h3.geo_to_h3(lat, lon, resolution)
        except:
            return None
    return udf(convert, StringType())

# Add multiple resolutions
events_with_h3 = events_df
for res in [6, 7, 8, 9]:
    events_with_h3 = events_with_h3.withColumn(
        f'h3_res{res}',
        geo_to_h3_func(res)(col('latitude'), col('longitude'))
    )

print("✓ Added multi-resolution H3 indices")
events_with_h3.select('delivery_id', 'h3_res6', 'h3_res7', 'h3_res8', 'h3_res9').show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Create Aggregation Pipeline
# MAGIC
# MAGIC **Task**: Build aggregation layers at each resolution
# MAGIC
# MAGIC **Metrics to calculate**:
# MAGIC - Delivery count
# MAGIC - Total revenue
# MAGIC - Average delivery time
# MAGIC - Delivery type distribution

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3.1: Implement Aggregation Function

# COMMAND ----------

# TODO: Create reusable aggregation function
# YOUR CODE HERE

def aggregate_by_h3(df, resolution, additional_groupby=[]):
    """Aggregate metrics by H3 cell"""
    h3_col = f'h3_res{resolution}'
    
    groupby_cols = [h3_col] + additional_groupby
    
    aggregated = df.groupBy(*groupby_cols).agg(
        count('*').alias('delivery_count'),
        spark_sum('value').alias('total_revenue'),
        avg('value').alias('avg_order_value'),
        avg('delivery_time_min').alias('avg_delivery_time'),
        stddev('value').alias('stddev_value')
    )
    
    # Add resolution metadata
    aggregated = aggregated.withColumn('resolution', lit(resolution))
    
    # Add area
    cell_area_km2 = h3.hex_area(resolution, 'km^2')
    aggregated = aggregated.withColumn('cell_area_km2', lit(cell_area_km2))
    
    # Calculate density
    aggregated = aggregated.withColumn(
        'delivery_density',
        col('delivery_count') / col('cell_area_km2')
    )
    
    return aggregated

print("✓ Created aggregation function")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3.2: Generate Aggregations at All Resolutions

# COMMAND ----------

# TODO: Create aggregations for resolutions 6-9
# YOUR CODE HERE

print("Generating multi-resolution aggregations...")
aggregations = {}

for res in [6, 7, 8, 9]:
    print(f"  Processing resolution {res}...")
    agg = aggregate_by_h3(events_with_h3, res)
    aggregations[res] = agg
    
    cell_count = agg.count()
    print(f"    {cell_count} cells")

print("\n✓ Generated all aggregations")

# Show sample from resolution 8
print("\nSample aggregation (resolution 8):")
aggregations[8].orderBy(col('delivery_count').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Implement Hierarchical Navigation
# MAGIC
# MAGIC **Task**: Build functions for drilling down and rolling up aggregations
# MAGIC
# MAGIC **Features**:
# MAGIC - Drill down: From parent cell to children
# MAGIC - Roll up: From children to parent
# MAGIC - Navigate: Between sibling cells

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4.1: Implement Drill-Down Function

# COMMAND ----------

# TODO: Create drill-down function
# YOUR CODE HERE

def drill_down(parent_cell, parent_resolution, child_resolution):
    """Get child cells and their aggregated metrics"""
    # Get children H3 cells
    children = h3.h3_to_children(parent_cell, child_resolution)
    
    # Get aggregations for children
    child_col = f'h3_res{child_resolution}'
    child_agg = aggregations[child_resolution].filter(
        col(child_col).isin(list(children))
    )
    
    return child_agg

print("✓ Created drill-down function")

# Test drill-down
top_cell_res7 = aggregations[7].orderBy(col('delivery_count').desc()).first()
print(f"\nTop cell at resolution 7: {top_cell_res7[f'h3_res7']}")
print(f"Deliveries: {top_cell_res7['delivery_count']}")

# Drill down to resolution 8
children_res8 = drill_down(top_cell_res7['h3_res7'], 7, 8)
print(f"\nChildren at resolution 8:")
children_res8.orderBy(col('delivery_count').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4.2: Implement Roll-Up Function

# COMMAND ----------

# TODO: Create roll-up aggregation function
# YOUR CODE HERE

def roll_up(child_cells, child_resolution, parent_resolution):
    """Aggregate child cells to parent level"""
    
    # Get parent cells
    parent_cells = set()
    for child in child_cells:
        parent = h3.h3_to_parent(child, parent_resolution)
        parent_cells.add(parent)
    
    # Get parent aggregations
    parent_col = f'h3_res{parent_resolution}'
    parent_agg = aggregations[parent_resolution].filter(
        col(parent_col).isin(list(parent_cells))
    )
    
    return parent_agg

print("✓ Created roll-up function")

# Test roll-up
child_cells = [row[f'h3_res8'] for row in children_res8.collect()]
parent_agg = roll_up(child_cells, 8, 7)
print("\nRolled up to parent:")
parent_agg.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Temporal Analysis
# MAGIC
# MAGIC **Task**: Add temporal dimension to spatial aggregations
# MAGIC
# MAGIC **Analysis**:
# MAGIC - Hourly patterns by location
# MAGIC - Day-of-week trends
# MAGIC - Peak vs off-peak comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 5.1: Create Temporal-Spatial Aggregations

# COMMAND ----------

# TODO: Aggregate by H3 cell, hour, and day of week
# YOUR CODE HERE

# Hourly aggregation
hourly_spatial = events_with_h3.groupBy('h3_res8', 'hour').agg(
    count('*').alias('delivery_count'),
    avg('value').alias('avg_value'),
    avg('delivery_time_min').alias('avg_time')
)

print("Hourly-Spatial Aggregation:")
hourly_spatial.orderBy('h3_res8', 'hour').show(20)

# Day of week aggregation
weekly_spatial = events_with_h3.groupBy('h3_res8', 'day_of_week').agg(
    count('*').alias('delivery_count'),
    avg('value').alias('avg_value')
)

print("\nWeekly-Spatial Aggregation:")
weekly_spatial.orderBy('h3_res8', 'day_of_week').show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 5.2: Identify Peak Hours by Location

# COMMAND ----------

# TODO: Find peak hour for each H3 cell
# YOUR CODE HERE

from pyspark.sql.functions import row_number

window_spec = Window.partitionBy('h3_res8').orderBy(col('delivery_count').desc())

peak_hours = hourly_spatial.withColumn(
    'rank',
    row_number().over(window_spec)
).filter(col('rank') == 1).drop('rank')

print("Peak Hours by Location:")
peak_hours.orderBy(col('delivery_count').desc()).show(15)

# Analyze peak hour distribution
peak_hour_dist = peak_hours.groupBy('hour').count().orderBy('hour')
print("\nPeak Hour Distribution:")
peak_hour_dist.show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Build Interactive Visualization
# MAGIC
# MAGIC **Task**: Create interactive map with drill-down capabilities
# MAGIC
# MAGIC **Features**:
# MAGIC - Choropleth map colored by delivery density
# MAGIC - Click to drill down to next resolution
# MAGIC - Display metrics in popups

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 6.1: Prepare Data for Visualization

# COMMAND ----------

# TODO: Convert aggregations to GeoDataFrame format
# YOUR CODE HERE

def h3_agg_to_geodataframe(h3_agg_df, resolution):
    """Convert H3 aggregation to GeoDataFrame"""
    h3_col = f'h3_res{resolution}'
    
    # Collect data
    data = h3_agg_df.collect()
    
    # Create polygons
    geometries = []
    properties = []
    
    for row in data:
        h3_cell = row[h3_col]
        if h3_cell:
            # Get boundary
            boundary = h3.h3_to_geo_boundary(h3_cell)
            polygon = Polygon([(lon, lat) for lat, lon in boundary])
            
            geometries.append(polygon)
            properties.append({
                'h3_cell': h3_cell,
                'delivery_count': row['delivery_count'],
                'total_revenue': row['total_revenue'],
                'avg_order_value': row['avg_order_value'],
                'avg_delivery_time': row['avg_delivery_time'],
                'delivery_density': row['delivery_density']
            })
    
    gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs='EPSG:4326')
    return gdf

print("✓ Created GeoDataFrame conversion function")

# Convert resolution 7 for visualization
gdf_res7 = h3_agg_to_geodataframe(aggregations[7], 7)
print(f"\nCreated GeoDataFrame with {len(gdf_res7)} cells")
gdf_res7.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 6.2: Create Choropleth Map

# COMMAND ----------

# TODO: Build interactive choropleth map
# YOUR CODE HERE

def create_choropleth_map(gdf, metric='delivery_density', title='Delivery Density'):
    """Create choropleth map from GeoDataFrame"""
    
    # Calculate center
    center_lat = gdf.geometry.centroid.y.mean()
    center_lon = gdf.geometry.centroid.x.mean()
    
    # Create map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=11)
    
    # Color scale
    vmin = gdf[metric].quantile(0.05)
    vmax = gdf[metric].quantile(0.95)
    
    # Add choropleth
    for idx, row in gdf.iterrows():
        # Normalize value for color
        value = row[metric]
        normalized = (value - vmin) / (vmax - vmin) if vmax > vmin else 0.5
        normalized = max(0, min(1, normalized))
        
        # Color scale: blue -> yellow -> red
        if normalized < 0.33:
            color = '#3288bd'
        elif normalized < 0.67:
            color = '#fee08b'
        else:
            color = '#d53e4f'
        
        # Create popup
        popup_html = f"""
        <b>H3 Cell:</b> {row['h3_cell']}<br>
        <b>Deliveries:</b> {row['delivery_count']:,}<br>
        <b>Revenue:</b> ${row['total_revenue']:,.2f}<br>
        <b>Avg Order:</b> ${row['avg_order_value']:.2f}<br>
        <b>Avg Time:</b> {row['avg_delivery_time']:.1f} min<br>
        <b>Density:</b> {row['delivery_density']:.1f} del/km²
        """
        
        folium.GeoJson(
            row['geometry'].__geo_interface__,
            style_function=lambda x, color=color: {
                'fillColor': color,
                'color': 'black',
                'weight': 1,
                'fillOpacity': 0.6
            },
            popup=folium.Popup(popup_html, max_width=300)
        ).add_to(m)
    
    # Add title
    title_html = f'<h3 align="center">{title}</h3>'
    m.get_root().html.add_child(folium.Element(title_html))
    
    return m

# Create map
map_viz = create_choropleth_map(gdf_res7, 'delivery_density', 'Delivery Density (Resolution 7)')
map_viz

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Performance Optimization
# MAGIC
# MAGIC **Task**: Materialize aggregations for production queries
# MAGIC
# MAGIC **Optimization strategies**:
# MAGIC - Cache frequently-accessed resolutions
# MAGIC - Partition by H3 cell
# MAGIC - Pre-compute common metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 7.1: Materialize Aggregation Layers

# COMMAND ----------

# TODO: Save aggregations to persistent storage
# YOUR CODE HERE

# Define output paths
base_path = "/tmp/h3_aggregations"

for res in [6, 7, 8, 9]:
    output_path = f"{base_path}/resolution_{res}"
    
    print(f"Materializing resolution {res}...")
    aggregations[res].write \
        .mode('overwrite') \
        .partitionBy(f'h3_res{res}') \
        .parquet(output_path)
    
    print(f"  ✓ Saved to {output_path}")

print("\n✓ All aggregations materialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 7.2: Benchmark Query Performance

# COMMAND ----------

# TODO: Compare performance of materialized vs on-demand aggregations
# YOUR CODE HERE

# Test query: Get top 10 cells by delivery count at resolution 8

# Method 1: From materialized aggregation
print("Method 1: Materialized aggregation")
start = time.time()
materialized = spark.read.parquet(f"{base_path}/resolution_8")
top_10_materialized = materialized.orderBy(col('delivery_count').desc()).limit(10).collect()
duration_materialized = time.time() - start
print(f"  Duration: {duration_materialized:.3f}s")

# Method 2: From raw events
print("\nMethod 2: On-demand aggregation")
start = time.time()
on_demand = events_with_h3.groupBy('h3_res8').agg(
    count('*').alias('delivery_count')
).orderBy(col('delivery_count').desc()).limit(10).collect()
duration_on_demand = time.time() - start
print(f"  Duration: {duration_on_demand:.3f}s")

print(f"\nSpeedup: {duration_on_demand/duration_materialized:.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Challenge - Real-Time Dashboard
# MAGIC
# MAGIC **Challenge**: Design a real-time analytics dashboard
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Sub-second query response
# MAGIC - Support drill-down from city to block level
# MAGIC - Show temporal trends
# MAGIC - Handle 1000+ queries per minute

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 8.1: Design Caching Strategy

# COMMAND ----------

# TODO: Implement intelligent caching for dashboard queries
# YOUR CODE HERE

print("Implementing caching strategy...")

# Cache most-queried resolutions
cache_resolutions = [7, 8]  # District and neighborhood

for res in cache_resolutions:
    print(f"  Caching resolution {res}...")
    aggregations[res].cache()
    aggregations[res].count()  # Force cache
    print(f"    ✓ Cached {aggregations[res].count()} cells")

# Cache temporal aggregations
print("  Caching temporal aggregations...")
hourly_spatial.cache()
hourly_spatial.count()
print(f"    ✓ Cached hourly data")

print("\n✓ Caching complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 8.2: Implement Dashboard Query Functions

# COMMAND ----------

# TODO: Create optimized query functions for common dashboard operations
# YOUR CODE HERE

class H3AnalyticsDashboard:
    """Dashboard query interface"""
    
    def __init__(self, aggregations_dict, hourly_data):
        self.aggregations = aggregations_dict
        self.hourly = hourly_data
    
    def get_overview(self, resolution=7):
        """Get high-level metrics"""
        agg = self.aggregations[resolution]
        
        total_deliveries = agg.agg(spark_sum('delivery_count')).collect()[0][0]
        total_revenue = agg.agg(spark_sum('total_revenue')).collect()[0][0]
        avg_order_value = agg.agg(avg('avg_order_value')).collect()[0][0]
        
        return {
            'total_deliveries': total_deliveries,
            'total_revenue': total_revenue,
            'avg_order_value': avg_order_value,
            'active_cells': agg.count()
        }
    
    def get_top_cells(self, resolution=8, n=10, metric='delivery_count'):
        """Get top N cells by metric"""
        return self.aggregations[resolution] \
            .orderBy(col(metric).desc()) \
            .limit(n) \
            .collect()
    
    def get_cell_detail(self, h3_cell, resolution):
        """Get detailed metrics for a specific cell"""
        h3_col = f'h3_res{resolution}'
        return self.aggregations[resolution] \
            .filter(col(h3_col) == h3_cell) \
            .first()
    
    def get_hourly_pattern(self, h3_cell):
        """Get hourly delivery pattern for a cell"""
        return self.hourly \
            .filter(col('h3_res8') == h3_cell) \
            .orderBy('hour') \
            .collect()

# Initialize dashboard
dashboard = H3AnalyticsDashboard(aggregations, hourly_spatial)

# Test queries
print("Dashboard Queries:")
print("=" * 60)

# Overview
start = time.time()
overview = dashboard.get_overview(7)
duration = time.time() - start
print(f"Overview (resolution 7): {duration:.3f}s")
for key, value in overview.items():
    print(f"  {key}: {value:,.0f}" if isinstance(value, (int, float)) else f"  {key}: {value}")

# Top cells
start = time.time()
top_cells = dashboard.get_top_cells(8, 5)
duration = time.time() - start
print(f"\nTop 5 Cells (resolution 8): {duration:.3f}s")
for cell in top_cells:
    print(f"  {cell['h3_res8']}: {cell['delivery_count']} deliveries")

# Cell detail
if top_cells:
    start = time.time()
    detail = dashboard.get_cell_detail(top_cells[0]['h3_res8'], 8)
    duration = time.time() - start
    print(f"\nCell Detail: {duration:.3f}s")
    print(f"  Revenue: ${detail['total_revenue']:,.2f}")
    print(f"  Avg Order: ${detail['avg_order_value']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution Summary
# MAGIC
# MAGIC ### Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC Raw Events
# MAGIC     ↓
# MAGIC Add Multi-Resolution H3 Indices
# MAGIC     ↓
# MAGIC Aggregate at Each Resolution
# MAGIC     ├─ Resolution 6 (City)
# MAGIC     ├─ Resolution 7 (District)  
# MAGIC     ├─ Resolution 8 (Neighborhood)
# MAGIC     └─ Resolution 9 (Block)
# MAGIC     ↓
# MAGIC Materialize to Storage
# MAGIC     ↓
# MAGIC Cache Frequently-Accessed Layers
# MAGIC     ↓
# MAGIC Dashboard Queries (< 1s response)
# MAGIC ```
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC
# MAGIC 1. **Multi-Resolution Index**
# MAGIC    - Enables flexible drill-down
# MAGIC    - Small storage overhead (~10% per resolution)
# MAGIC    - Supports different analysis scales
# MAGIC
# MAGIC 2. **Materialized Aggregations**
# MAGIC    - 10-100x faster than on-demand
# MAGIC    - Update frequency based on data freshness needs
# MAGIC    - Partition by H3 cell for efficient queries
# MAGIC
# MAGIC 3. **Intelligent Caching**
# MAGIC    - Cache most-accessed resolutions (7-8)
# MAGIC    - Cache temporal aggregations
# MAGIC    - Monitor cache hit rates
# MAGIC
# MAGIC 4. **Hierarchical Navigation**
# MAGIC    - Parent-child relationships via H3 API
# MAGIC    - Efficient drill-down and roll-up
# MAGIC    - Preserve metric consistency

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Advanced Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO Bonus: Anomaly Detection

# COMMAND ----------

# TODO: Identify cells with unusual delivery patterns
# YOUR CODE HERE

# Calculate z-scores for delivery density
from pyspark.sql.functions import stddev, avg

stats = aggregations[8].select(
    avg('delivery_density').alias('mean_density'),
    stddev('delivery_density').alias('std_density')
).first()

mean = stats['mean_density']
std = stats['std_density']

# Identify anomalies
anomalies = aggregations[8].withColumn(
    'z_score',
    (col('delivery_density') - lit(mean)) / lit(std)
).filter(
    (col('z_score') > 3) | (col('z_score') < -3)
)

print("Anomalous Cells (|z-score| > 3):")
anomalies.orderBy(col('z_score').desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've built a complete H3-based analytics pipeline! You should now be able to:
# MAGIC - ✓ Design multi-resolution spatial aggregations
# MAGIC - ✓ Implement hierarchical navigation
# MAGIC - ✓ Create interactive visualizations
# MAGIC - ✓ Optimize for production performance
# MAGIC - ✓ Build real-time analytics dashboards
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Continue to Lab 7: Geofencing and Proximity Analysis
# MAGIC - Apply this pipeline to your own spatial datasets
# MAGIC - Explore advanced H3 features (compacting, edge operations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [H3 Aggregation Best Practices](https://h3geo.org/docs/highlights/aggregation)
# MAGIC - [Building Data Pipelines with Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC - [Kepler.gl for Geospatial Visualization](https://docs.kepler.gl/)
# MAGIC - [Databricks Delta Lake](https://docs.databricks.com/delta/index.html)
