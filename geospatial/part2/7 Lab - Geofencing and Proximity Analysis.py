# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Geofencing and Proximity Analysis
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll build a dynamic geofencing system with real-time proximity detection capabilities. You'll implement zone-based analytics, proximity alerts, and optimize for low-latency queries at scale.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Design and implement dynamic geofence systems
# MAGIC - Perform efficient proximity queries using H3
# MAGIC - Build real-time proximity alert systems
# MAGIC - Optimize geofence lookups for sub-millisecond response
# MAGIC - Analyze zone entry/exit events
# MAGIC - Handle complex geofence scenarios
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1: H3 Hexagonal Indexing
# MAGIC - Completion of Demo 2: Spatial Joins at Scale
# MAGIC - Understanding of real-time systems
# MAGIC
# MAGIC ## Duration
# MAGIC 60-75 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install required libraries
%pip install h3 geopandas shapely folium --quiet

#dbutils.library.restartPython()

# COMMAND ----------

import h3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, box
from shapely.wkt import loads as wkt_loads, dumps as wkt_dumps
import folium
from folium import plugins
import time
from datetime import datetime, timedelta
import numpy as np
import random
from collections import defaultdict
from pyspark.sql.functions import (
    udf, col, count, sum as spark_sum, avg, lit, when, explode, 
    array, struct, lag, lead, unix_timestamp, from_unixtime
)
from pyspark.sql.types import StringType, ArrayType, BooleanType, DoubleType, IntegerType, StructType, StructField
from pyspark.sql.window import Window

print("✓ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Define Geofence Zones
# MAGIC
# MAGIC **Task**: Create a set of geofence zones for different use cases
# MAGIC
# MAGIC **Zone Types**:
# MAGIC - Delivery zones (large, ~5km radius)
# MAGIC - Restricted areas (airports, military bases)
# MAGIC - Premium service areas (downtown, business districts)
# MAGIC - Parking zones (small, ~100m radius)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.1: Create Delivery Zones

# COMMAND ----------

# TODO: Create delivery zones using H3 cells
# STARTER CODE PROVIDED

random.seed(42)

# Define zone centers (San Francisco area)
zone_centers = [
    {'name': 'Downtown SF', 'lat': 37.7879, 'lon': -122.4074, 'type': 'delivery', 'radius_km': 5},
    {'name': 'Mission Bay', 'lat': 37.7706, 'lon': -122.3922, 'type': 'delivery', 'radius_km': 3},
    {'name': 'Marina District', 'lat': 37.8021, 'lon': -122.4360, 'type': 'delivery', 'radius_km': 2},
    {'name': 'SFO Airport', 'lat': 37.6213, 'lon': -122.3790, 'type': 'restricted', 'radius_km': 3},
    {'name': 'Financial District', 'lat': 37.7946, 'lon': -122.3999, 'type': 'premium', 'radius_km': 1.5},
]

# YOUR CODE HERE: Convert zones to H3 cells
zones = []

for zone in zone_centers:
    zone_id = f"ZONE_{zone['type'].upper()}_{len(zones):03d}"
    
    # Determine H3 resolution based on radius
    if zone['radius_km'] >= 5:
        resolution = 6
    elif zone['radius_km'] >= 2:
        resolution = 7
    elif zone['radius_km'] >= 1:
        resolution = 8
    else:
        resolution = 9
    
    # Get center H3 cell
    center_h3 = h3.geo_to_h3(zone['lat'], zone['lon'], resolution)
    
    # Calculate k-ring size for radius
    avg_edge_km = h3.edge_length(resolution, 'km')
    k = int(zone['radius_km'] / avg_edge_km) + 1
    
    # Get all cells in radius
    zone_cells = list(h3.k_ring(center_h3, k))
    
    # Create polygon from cells
    all_boundaries = []
    for cell in zone_cells:
        boundary = h3.h3_to_geo_boundary(cell)
        all_boundaries.extend([(lon, lat) for lat, lon in boundary])
    
    # Use center cell boundary for now (simplification)
    boundary = h3.h3_to_geo_boundary(center_h3)
    polygon = Polygon([(lon, lat) for lat, lon in boundary])
    
    zones.append({
        'zone_id': zone_id,
        'zone_name': zone['name'],
        'zone_type': zone['type'],
        'h3_cells': zone_cells,
        'center_lat': zone['lat'],
        'center_lon': zone['lon'],
        'radius_km': zone['radius_km'],
        'resolution': resolution,
        'geometry_wkt': wkt_dumps(polygon)
    })

print(f"✓ Created {len(zones)} geofence zones")
for zone in zones:
    print(f"  {zone['zone_id']}: {zone['zone_name']} ({len(zone['h3_cells'])} cells)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 1.2: Create H3 Lookup Table

# COMMAND ----------

# TODO: Create H3 cell to zone mapping for fast lookups
# YOUR CODE HERE

# Explode zones to cell-level mapping
cell_mappings = []

for zone in zones:
    for h3_cell in zone['h3_cells']:
        cell_mappings.append((
            h3_cell,
            zone['zone_id'],
            zone['zone_name'],
            zone['zone_type'],
            zone['resolution']
        ))

# Create DataFrame
zones_lookup_df = spark.createDataFrame(
    cell_mappings,
    ['h3_cell', 'zone_id', 'zone_name', 'zone_type', 'resolution']
)

print(f"✓ Created lookup table with {zones_lookup_df.count():,} H3 cell mappings")
zones_lookup_df.show(10)

# Cache for fast queries
zones_lookup_df.cache()
zones_lookup_df.count()
print("✓ Cached lookup table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Generate Movement Data
# MAGIC
# MAGIC **Task**: Simulate vehicle movements for proximity analysis
# MAGIC
# MAGIC **Requirements**:
# MAGIC - 1,000 vehicles
# MAGIC - Movement paths over 1 hour
# MAGIC - Location updates every 30 seconds
# MAGIC - Realistic movement patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 2.1: Generate Vehicle Trajectories

# COMMAND ----------

# TODO: Create vehicle movement data
# YOUR CODE HERE

np.random.seed(42)

num_vehicles = 1000
update_interval_seconds = 30
duration_hours = 1
num_updates = int((duration_hours * 3600) / update_interval_seconds)

# Generate starting positions
vehicle_starts = []
for i in range(num_vehicles):
    # Random start in SF area
    lat = 37.7 + np.random.random() * 0.15
    lon = -122.5 + np.random.random() * 0.15
    vehicle_starts.append((lat, lon))

# Generate trajectories
vehicle_updates = []
start_time = datetime(2024, 1, 15, 10, 0, 0)

for vehicle_id in range(num_vehicles):
    current_lat, current_lon = vehicle_starts[vehicle_id]
    
    # Random movement direction and speed
    direction_lat = np.random.randn() * 0.0001
    direction_lon = np.random.randn() * 0.0001
    speed_factor = np.random.uniform(0.5, 2.0)  # Varying speeds
    
    for update_num in range(num_updates):
        timestamp = start_time + timedelta(seconds=update_num * update_interval_seconds)
        
        # Update position
        current_lat += direction_lat * speed_factor
        current_lon += direction_lon * speed_factor
        
        # Add some randomness
        current_lat += np.random.randn() * 0.00005
        current_lon += np.random.randn() * 0.00005
        
        # Occasional direction changes
        if random.random() < 0.1:
            direction_lat = np.random.randn() * 0.0001
            direction_lon = np.random.randn() * 0.0001
        
        vehicle_updates.append((
            f'VEH{vehicle_id:04d}',
            timestamp,
            current_lat,
            current_lon
        ))

# Create DataFrame
movements_df = spark.createDataFrame(
    vehicle_updates,
    ['vehicle_id', 'timestamp', 'latitude', 'longitude']
)

print(f"✓ Generated {movements_df.count():,} vehicle location updates")
print(f"  Vehicles: {movements_df.select('vehicle_id').distinct().count()}")
print(f"  Time span: {duration_hours} hour(s)")
movements_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Real-Time Zone Detection
# MAGIC
# MAGIC **Task**: Implement efficient zone membership detection
# MAGIC
# MAGIC **Requirements**:
# MAGIC - < 100ms per lookup
# MAGIC - Handle multi-zone membership
# MAGIC - Support all resolution levels

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3.1: Add H3 Index to Movement Data

# COMMAND ----------

# TODO: Add H3 indices for all zone resolutions
# YOUR CODE HERE

def geo_to_h3_func(resolution):
    def convert(lat, lon):
        try:
            return h3.geo_to_h3(lat, lon, resolution)
        except:
            return None
    return udf(convert, StringType())

# Add H3 columns for all resolutions used by zones
movements_with_h3 = movements_df
for res in [6, 7, 8, 9]:
    movements_with_h3 = movements_with_h3.withColumn(
        f'h3_res{res}',
        geo_to_h3_func(res)(col('latitude'), col('longitude'))
    )

print("✓ Added H3 indices to movements")
movements_with_h3.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 3.2: Perform Zone Lookup

# COMMAND ----------

# TODO: Join movements with zone lookup to detect zone membership
# YOUR CODE HERE

print("Performing zone lookups...")
start_time = time.time()

# Need to join on appropriate H3 resolution
# Strategy: Join on all resolutions and union results

zone_detections = None

for res in [6, 7, 8, 9]:
    h3_col = f'h3_res{res}'
    
    # Filter zones for this resolution
    zones_at_res = zones_lookup_df.filter(col('resolution') == res)
    
    # Join movements with zones
    detected = movements_with_h3.join(
        zones_at_res,
        movements_with_h3[h3_col] == zones_at_res['h3_cell'],
        'left'
    ).select(
        movements_with_h3.vehicle_id,
        movements_with_h3.timestamp,
        movements_with_h3.latitude,
        movements_with_h3.longitude,
        zones_at_res.zone_id,
        zones_at_res.zone_name,
        zones_at_res.zone_type
    )
    
    if zone_detections is None:
        zone_detections = detected
    else:
        zone_detections = zone_detections.union(detected)

# Remove duplicates and null zones
zone_detections = zone_detections.filter(col('zone_id').isNotNull()).distinct()

duration = time.time() - start_time
detection_count = zone_detections.count()

print(f"✓ Zone detection completed")
print(f"  Duration: {duration:.2f}s")
print(f"  Detections: {detection_count:,}")
print(f"  Avg lookup time: {(duration*1000)/movements_df.count():.2f}ms per update")

zone_detections.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Zone Entry/Exit Events
# MAGIC
# MAGIC **Task**: Detect when vehicles enter or exit zones
# MAGIC
# MAGIC **Events to detect**:
# MAGIC - Entry: Vehicle enters a zone
# MAGIC - Exit: Vehicle leaves a zone
# MAGIC - Dwell time: Time spent in zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4.1: Detect Zone Transitions

# COMMAND ----------

# TODO: Identify zone entry and exit events
# YOUR CODE HERE

from pyspark.sql.functions import lag, lead

# Sort by vehicle and timestamp
window_spec = Window.partitionBy('vehicle_id').orderBy('timestamp')

# Get previous zone
zone_transitions = zone_detections.withColumn(
    'prev_zone_id',
    lag('zone_id', 1).over(window_spec)
).withColumn(
    'next_zone_id',
    lead('zone_id', 1).over(window_spec)
)

# Classify events
zone_events = zone_transitions.withColumn(
    'event_type',
    when(col('prev_zone_id').isNull(), 'ENTRY')
    .when(col('next_zone_id').isNull(), 'EXIT')
    .when(col('zone_id') != col('prev_zone_id'), 'ENTRY')
    .when(col('zone_id') != col('next_zone_id'), 'EXIT')
    .otherwise('IN_ZONE')
).filter(col('event_type').isin(['ENTRY', 'EXIT']))

print("Zone Entry/Exit Events:")
zone_events.show(20)

# Summary by event type
print("\nEvent Summary:")
zone_events.groupBy('event_type', 'zone_type').count().orderBy('zone_type', 'event_type').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 4.2: Calculate Dwell Time

# COMMAND ----------

# TODO: Calculate time spent in each zone
# YOUR CODE HERE

# Get entry and exit pairs
entries = zone_events.filter(col('event_type') == 'ENTRY') \
    .select(
        col('vehicle_id'),
        col('zone_id'),
        col('zone_name'),
        col('zone_type'),
        col('timestamp').alias('entry_time')
    )

exits = zone_events.filter(col('event_type') == 'EXIT') \
    .select(
        col('vehicle_id'),
        col('zone_id'),
        col('timestamp').alias('exit_time')
    )

# Join entries with subsequent exits
window_spec = Window.partitionBy('vehicle_id', 'zone_id').orderBy('entry_time')

dwell_times = entries.join(
    exits,
    (entries.vehicle_id == exits.vehicle_id) & (entries.zone_id == exits.zone_id),
    'inner'
).filter(col('exit_time') > col('entry_time'))

# Calculate dwell time in minutes
dwell_times = dwell_times.withColumn(
    'dwell_minutes',
    (unix_timestamp('exit_time') - unix_timestamp('entry_time')) / 60
)

print("Dwell Time Analysis:")
dwell_times.orderBy('dwell_minutes', ascending=False).show(15)

# Average dwell time by zone
print("\nAverage Dwell Time by Zone:")
dwell_times.groupBy('zone_name', 'zone_type').agg(
    count('*').alias('visits'),
    avg('dwell_minutes').alias('avg_dwell_minutes'),
    spark_sum('dwell_minutes').alias('total_dwell_minutes')
).orderBy('avg_dwell_minutes', ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Proximity Alerts
# MAGIC
# MAGIC **Task**: Implement proximity alert system
# MAGIC
# MAGIC **Alert Types**:
# MAGIC - Approaching zone (within 1km)
# MAGIC - Entering restricted zone
# MAGIC - Exceeding dwell time limit
# MAGIC - Multiple vehicles in same area

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 5.1: Detect Approaching Vehicles

# COMMAND ----------

# TODO: Identify vehicles approaching zones (using k-ring buffer)
# YOUR CODE HERE

def h3_with_buffer(resolution, k=1):
    """Get H3 cell with k-ring buffer"""
    def get_cells(lat, lon):
        try:
            center = h3.geo_to_h3(lat, lon, resolution)
            return list(h3.k_ring(center, k))
        except:
            return []
    return udf(get_cells, ArrayType(StringType()))

# Add buffered H3 cells (1-ring = approaching zone)
movements_buffered = movements_with_h3.withColumn(
    'h3_cells_buffered',
    h3_with_buffer(8, k=1)(col('latitude'), col('longitude'))
).withColumn(
    'h3_cell_buf',
    explode(col('h3_cells_buffered'))
)

# Join with zones
approaching = movements_buffered.join(
    zones_lookup_df.filter(col('resolution') == 8),
    movements_buffered.h3_cell_buf == zones_lookup_df.h3_cell,
    'inner'
).select(
    movements_buffered.vehicle_id,
    movements_buffered.timestamp,
    movements_buffered.latitude,
    movements_buffered.longitude,
    zones_lookup_df.zone_id,
    zones_lookup_df.zone_name,
    zones_lookup_df.zone_type
).distinct()

# Exclude vehicles already in zone (they're not "approaching")
currently_in_zone = zone_detections.select('vehicle_id', 'timestamp', 'zone_id')

approaching_alerts = approaching.join(
    currently_in_zone,
    (approaching.vehicle_id == currently_in_zone.vehicle_id) &
    (approaching.timestamp == currently_in_zone.timestamp) &
    (approaching.zone_id == currently_in_zone.zone_id),
    'left_anti'
)

print(f"✓ Generated {approaching_alerts.count():,} approaching zone alerts")
approaching_alerts.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 5.2: Restricted Zone Violations

# COMMAND ----------

# TODO: Detect vehicles entering restricted zones
# YOUR CODE HERE

# Filter for restricted zone entries
restricted_violations = zone_events.filter(
    (col('event_type') == 'ENTRY') & (col('zone_type') == 'restricted')
).select(
    col('vehicle_id'),
    col('timestamp'),
    col('zone_name').alias('restricted_zone'),
    lit('RESTRICTED_ZONE_ENTRY').alias('alert_type')
)

print(f"✓ Detected {restricted_violations.count()} restricted zone violations")
restricted_violations.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Proximity to Other Vehicles
# MAGIC
# MAGIC **Task**: Detect when vehicles are near each other
# MAGIC
# MAGIC **Use Cases**:
# MAGIC - Fleet coordination
# MAGIC - Collision avoidance
# MAGIC - Clustering analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 6.1: Find Nearby Vehicles

# COMMAND ----------

# TODO: Identify vehicles in same H3 cell
# YOUR CODE HERE

# Get vehicles by H3 cell and timestamp
# Use 5-minute windows for proximity
movements_with_h3_snapshot = movements_with_h3.withColumn(
    'time_bucket',
    (unix_timestamp('timestamp') / 300).cast('int')  # 5-minute buckets
)

# Group by H3 cell and time bucket
vehicle_proximity = movements_with_h3_snapshot.groupBy('h3_res9', 'time_bucket').agg(
    collect_list(struct('vehicle_id', 'latitude', 'longitude', 'timestamp')).alias('vehicles')
).withColumn(
    'vehicle_count',
    col('vehicles').cast('array<struct<vehicle_id:string,latitude:double,longitude:double,timestamp:timestamp>>').getItem(0)
)

# Filter for cells with multiple vehicles
from pyspark.sql.functions import size, collect_list

vehicle_clusters = movements_with_h3_snapshot.groupBy('h3_res9', 'time_bucket').agg(
    collect_list('vehicle_id').alias('vehicle_list'),
    count('*').alias('vehicle_count'),
    avg('latitude').alias('center_lat'),
    avg('longitude').alias('center_lon')
).filter(col('vehicle_count') > 1).orderBy(col('vehicle_count').desc())

print("Vehicle Proximity Clusters:")
print(f"  Found {vehicle_clusters.count()} locations with multiple vehicles")
vehicle_clusters.show(15, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Performance Optimization
# MAGIC
# MAGIC **Task**: Optimize geofence system for production
# MAGIC
# MAGIC **Requirements**:
# MAGIC - < 10ms per zone lookup
# MAGIC - Support 10,000+ vehicles
# MAGIC - Real-time alert generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 7.1: Benchmark Lookup Performance

# COMMAND ----------

# TODO: Test lookup performance with different strategies
# YOUR CODE HERE

from pyspark.sql.functions import current_timestamp

# Test case: Single vehicle lookup
test_lat, test_lon = 37.7879, -122.4074
test_h3_res8 = h3.geo_to_h3(test_lat, test_lon, 8)

# Method 1: Direct H3 lookup
print("Method 1: H3 Lookup")
start = time.time()
zone_result = zones_lookup_df.filter(col('h3_cell') == test_h3_res8).collect()
duration_h3 = time.time() - start
print(f"  Duration: {duration_h3*1000:.2f}ms")
print(f"  Zones found: {len(zone_result)}")

# Method 2: With k-ring buffer (more accurate)
print("\nMethod 2: H3 with Buffer")
start = time.time()
buffered_cells = h3.k_ring(test_h3_res8, 1)
zone_result_buffered = zones_lookup_df.filter(col('h3_cell').isin(list(buffered_cells))).collect()
duration_buffered = time.time() - start
print(f"  Duration: {duration_buffered*1000:.2f}ms")
print(f"  Zones found: {len(zone_result_buffered)}")

# Benchmark batch lookups
print("\nBatch Lookup Benchmark (1000 locations):")
sample_movements = movements_with_h3.limit(1000).cache()
sample_movements.count()

start = time.time()
batch_result = sample_movements.join(
    zones_lookup_df.filter(col('resolution') == 8),
    sample_movements.h3_res8 == zones_lookup_df.h3_cell,
    'left'
).count()
duration_batch = time.time() - start

print(f"  Duration: {duration_batch:.3f}s")
print(f"  Per-lookup: {(duration_batch*1000)/1000:.2f}ms")
print(f"  Results: {batch_result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 7.2: Create Materialized Alert Table

# COMMAND ----------

# TODO: Pre-compute common alerts for fast queries
# YOUR CODE HERE

# Combine all alert types
all_alerts = None

# Restricted zone alerts
restricted_alerts = restricted_violations.select(
    col('vehicle_id'),
    col('timestamp'),
    col('alert_type'),
    col('restricted_zone').alias('zone_name'),
    lit('high').alias('priority')
)

# Approaching zone alerts
approaching_alerts_formatted = approaching_alerts.select(
    col('vehicle_id'),
    col('timestamp'),
    lit('APPROACHING_ZONE').alias('alert_type'),
    col('zone_name'),
    when(col('zone_type') == 'restricted', 'high')
    .when(col('zone_type') == 'premium', 'medium')
    .otherwise('low').alias('priority')
)

# Union all alerts
all_alerts = restricted_alerts.union(approaching_alerts_formatted)

# Add alert ID
from pyspark.sql.functions import monotonically_increasing_id
all_alerts = all_alerts.withColumn('alert_id', monotonically_increasing_id())

print(f"✓ Created unified alert table with {all_alerts.count():,} alerts")

# Materialize
output_path = "/tmp/geofence_alerts"
all_alerts.write.mode('overwrite').partitionBy('priority').parquet(output_path)
print(f"✓ Materialized alerts to {output_path}")

all_alerts.groupBy('alert_type', 'priority').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Visualization
# MAGIC
# MAGIC **Task**: Create interactive map showing zones and alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO 8.1: Visualize Geofences and Vehicle Movements

# COMMAND ----------

# TODO: Create map with zones, vehicle paths, and alerts
# YOUR CODE HERE

# Create map centered on SF
m = folium.Map(location=[37.7749, -122.4194], zoom_start=12)

# Add geofence zones
for zone in zones:
    # Get zone boundary
    if len(zone['h3_cells']) > 0:
        # Simplified: draw center cell
        boundary = h3.h3_to_geo_boundary(zone['h3_cells'][0])
        
        # Color by type
        if zone['zone_type'] == 'restricted':
            color = 'red'
            fillColor = 'red'
        elif zone['zone_type'] == 'premium':
            color = 'gold'
            fillColor = 'yellow'
        else:
            color = 'blue'
            fillColor = 'lightblue'
        
        folium.Polygon(
            locations=[(lat, lon) for lat, lon in boundary],
            popup=f"<b>{zone['zone_name']}</b><br>Type: {zone['zone_type']}<br>Cells: {len(zone['h3_cells'])}",
            color=color,
            fill=True,
            fillColor=fillColor,
            fillOpacity=0.3,
            weight=2
        ).add_to(m)

# Add sample vehicle paths
sample_vehicles = movements_df.filter(col('vehicle_id').isin(['VEH0001', 'VEH0002', 'VEH0003'])).toPandas()

for vehicle_id in sample_vehicles['vehicle_id'].unique():
    vehicle_path = sample_vehicles[sample_vehicles['vehicle_id'] == vehicle_id]
    
    # Create path
    path_coords = [(row['latitude'], row['longitude']) for _, row in vehicle_path.iterrows()]
    
    folium.PolyLine(
        path_coords,
        popup=f"Vehicle: {vehicle_id}",
        color='green',
        weight=3,
        opacity=0.7
    ).add_to(m)
    
    # Add start/end markers
    if len(path_coords) > 0:
        folium.CircleMarker(
            path_coords[0],
            radius=5,
            popup=f"{vehicle_id} Start",
            color='green',
            fill=True
        ).add_to(m)
        
        folium.CircleMarker(
            path_coords[-1],
            radius=5,
            popup=f"{vehicle_id} End",
            color='red',
            fill=True
        ).add_to(m)

# Add layer control
folium.LayerControl().add_to(m)

m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution Summary
# MAGIC
# MAGIC ### Geofencing Architecture
# MAGIC
# MAGIC ```
# MAGIC Zone Definition
# MAGIC     ↓
# MAGIC H3 Cell Mapping (Pre-computed)
# MAGIC     ↓
# MAGIC Lookup Table (Cached)
# MAGIC     ↓
# MAGIC Real-Time Vehicle Updates
# MAGIC     ↓
# MAGIC H3 Index Assignment
# MAGIC     ↓
# MAGIC Fast Hash Join on H3
# MAGIC     ↓
# MAGIC Zone Detection (< 10ms)
# MAGIC     ↓
# MAGIC Alert Generation
# MAGIC     ├─ Entry/Exit Events
# MAGIC     ├─ Proximity Alerts
# MAGIC     ├─ Violation Detection
# MAGIC     └─ Dwell Time Analysis
# MAGIC ```
# MAGIC
# MAGIC ### Key Optimizations
# MAGIC
# MAGIC 1. **H3 Lookup Table**
# MAGIC    - Pre-compute zone → H3 cell mappings
# MAGIC    - O(1) lookups via hash join
# MAGIC    - Cache in memory for speed
# MAGIC
# MAGIC 2. **Multi-Resolution Support**
# MAGIC    - Different resolutions for different zone sizes
# MAGIC    - Larger zones use lower resolution
# MAGIC    - Reduces lookup table size
# MAGIC
# MAGIC 3. **K-Ring Buffer**
# MAGIC    - Add 1-ring buffer for boundary accuracy
# MAGIC    - Detect approaching vehicles
# MAGIC    - Minimal performance impact
# MAGIC
# MAGIC 4. **Event-Driven Alerts**
# MAGIC    - Only trigger on state changes
# MAGIC    - Materialize for historical analysis
# MAGIC    - Priority-based processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenge: Real-Time System Design

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO Bonus: Design Streaming Geofence System

# COMMAND ----------

# TODO: Design architecture for real-time geofencing
# YOUR CODE HERE

print("Real-Time Geofence System Design")
print("=" * 60)

print("""
Architecture Components:

1. INGESTION LAYER
   - Kafka/Kinesis for vehicle location streams
   - Schema validation
   - Deduplication
   
2. PROCESSING LAYER (Spark Structured Streaming)
   - Add H3 index to incoming locations
   - Stream-stream join with zone lookup
   - Stateful processing for entry/exit detection
   - Window-based aggregations
   
3. STORAGE LAYER
   - Delta Lake for historical events
   - Redis for hot lookup cache
   - Zone definitions in key-value store
   
4. ALERT LAYER
   - Priority queue for alerts
   - Webhook/notification service
   - Alert deduplication logic
   
5. API LAYER
   - REST API for queries
   - WebSocket for real-time updates
   - GraphQL for complex queries
   
Performance Targets:
- Ingestion: 10,000+ updates/sec
- Lookup latency: < 10ms p99
- Alert latency: < 100ms end-to-end
- Query latency: < 50ms p99

Scaling Strategy:
- Partition by H3 cell for data locality
- Scale horizontally based on throughput
- Cache hot zones in memory
- Use broadcast for small zone datasets
""")

# Sample streaming code structure (conceptual)
print("\nStreaming Code Pattern:")
print("""
# Read vehicle stream
vehicle_stream = spark.readStream \\
    .format("kafka") \\
    .option("subscribe", "vehicle-locations") \\
    .load()

# Add H3 index
indexed_stream = vehicle_stream.withColumn(
    'h3_cell',
    h3_udf(col('lat'), col('lon'))
)

# Join with zones (broadcast for small zone dataset)
alerts = indexed_stream.join(
    broadcast(zones_lookup),
    'h3_cell'
).filter(
    # Alert conditions
    col('zone_type') == 'restricted'
)

# Write alerts
alerts.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", "/checkpoints") \\
    .start("/alerts")
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've built a complete geofencing and proximity analysis system! You should now be able to:
# MAGIC - ✓ Design dynamic geofence zones
# MAGIC - ✓ Implement efficient zone lookups using H3
# MAGIC - ✓ Detect entry/exit events
# MAGIC - ✓ Generate proximity alerts
# MAGIC - ✓ Optimize for real-time performance
# MAGIC - ✓ Visualize geofences and movements
# MAGIC
# MAGIC ### Production Considerations
# MAGIC - Implement caching strategy
# MAGIC - Add monitoring and alerting
# MAGIC - Handle late-arriving data
# MAGIC - Plan for zone updates
# MAGIC - Scale horizontally

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [H3 for Ride-Sharing](https://eng.uber.com/h3/)
# MAGIC - [Geofencing Best Practices](https://www.mapbox.com/guides/geofencing)
# MAGIC - [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC - [Real-Time Analytics Architecture](https://databricks.com/solutions/accelerators/real-time-analytics)
