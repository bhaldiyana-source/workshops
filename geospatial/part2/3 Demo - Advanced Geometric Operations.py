# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Advanced Geometric Operations
# MAGIC
# MAGIC ## Overview
# MAGIC This demo covers advanced geometric operations essential for geospatial analysis, including buffer creation, simplification, convex hulls, transformations, and topology operations. You'll learn how to optimize these operations for distributed processing at scale.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Create buffers around points, lines, and polygons
# MAGIC - Simplify complex geometries using Douglas-Peucker algorithm
# MAGIC - Generate convex hulls and bounding boxes
# MAGIC - Transform geometries between coordinate reference systems
# MAGIC - Validate and repair invalid geometries
# MAGIC - Optimize geometric operations for performance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Part 1: Foundations of Geospatial Analytics
# MAGIC - Understanding of coordinate reference systems
# MAGIC - Python and Spark proficiency
# MAGIC
# MAGIC ## Duration
# MAGIC 45-50 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install and Import Libraries

# COMMAND ----------

# Install required libraries
%pip install geopandas shapely pyproj folium --quiet

#dbutils.library.restartPython()

# COMMAND ----------

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, Polygon, MultiPolygon, box
from shapely.ops import transform, unary_union
from shapely import wkt, wkb
from shapely.validation import make_valid
import pyproj
from functools import partial
import folium
import numpy as np
from pyspark.sql.functions import udf, col, pandas_udf
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
import time

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Buffer Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Buffer Creation

# COMMAND ----------

# Create sample points (San Francisco landmarks)
landmarks = [
    ('Golden Gate Bridge', 37.8199, -122.4783),
    ('Alcatraz Island', 37.8267, -122.4233),
    ('Fishermans Wharf', 37.8080, -122.4177),
    ('Union Square', 37.7879, -122.4074),
    ('AT&T Park', 37.7786, -122.3893)
]

# Create GeoDataFrame
gdf_points = gpd.GeoDataFrame(
    landmarks,
    columns=['name', 'lat', 'lon'],
    geometry=[Point(lon, lat) for name, lat, lon in landmarks],
    crs='EPSG:4326'
)

print("San Francisco Landmarks:")
gdf_points.head()

# COMMAND ----------

# Create buffers in degrees (wrong approach - for demonstration)
buffer_distance_degrees = 0.01  # ~1.1 km at SF latitude

gdf_points['buffer_degrees'] = gdf_points.geometry.buffer(buffer_distance_degrees)

print("Buffer in degrees (not recommended):")
print(f"Original point area: {gdf_points.geometry.iloc[0].area:.10f} square degrees")
print(f"Buffer area: {gdf_points['buffer_degrees'].iloc[0].area:.10f} square degrees")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correct Buffer in Meters (Using Projection)

# COMMAND ----------

# Project to UTM Zone 10N (appropriate for San Francisco)
gdf_points_utm = gdf_points.to_crs('EPSG:32610')  # UTM Zone 10N

# Create 1km buffer in meters
buffer_distance_meters = 1000

gdf_points_utm['buffer_1km'] = gdf_points_utm.geometry.buffer(buffer_distance_meters)

print("Buffer in meters (correct approach):")
print(f"Buffer area: {gdf_points_utm['buffer_1km'].iloc[0].area:,.0f} square meters")
print(f"Expected: ~{np.pi * buffer_distance_meters**2:,.0f} square meters")

# COMMAND ----------

# Convert back to WGS84 for visualization
gdf_points['buffer_1km'] = gdf_points_utm['buffer_1km'].to_crs('EPSG:4326')

# Visualize
m = folium.Map(location=[37.8, -122.43], zoom_start=12)

# Add points
for idx, row in gdf_points.iterrows():
    folium.Marker(
        location=[row['lat'], row['lon']],
        popup=row['name'],
        icon=folium.Icon(color='red', icon='info-sign')
    ).add_to(m)
    
    # Add buffer
    folium.GeoJson(
        row['buffer_1km'].__geo_interface__,
        style_function=lambda x: {
            'fillColor': 'blue',
            'color': 'blue',
            'weight': 2,
            'fillOpacity': 0.2
        }
    ).add_to(m)

m

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variable Distance Buffers

# COMMAND ----------

# Create buffers with different distances based on attributes
gdf_points_utm['importance'] = [5, 4, 3, 2, 1]  # Priority ranking
gdf_points_utm['buffer_variable'] = gdf_points_utm.apply(
    lambda row: row.geometry.buffer(row['importance'] * 500),  # 500m per importance level
    axis=1
)

print("Variable distance buffers:")
for idx, row in gdf_points_utm.iterrows():
    area_km2 = row['buffer_variable'].area / 1_000_000
    print(f"{row['name']}: {row['importance']}x importance = {area_km2:.2f} km²")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dissolving Overlapping Buffers

# COMMAND ----------

# Combine all buffers into one multipolygon
all_buffers = gdf_points_utm['buffer_1km'].tolist()
dissolved = unary_union(all_buffers)

print(f"Individual buffers: {len(all_buffers)}")
print(f"Dissolved result type: {dissolved.geom_type}")
print(f"Total area (dissolved): {dissolved.area / 1_000_000:.2f} km²")
print(f"Sum of individual areas: {sum([b.area for b in all_buffers]) / 1_000_000:.2f} km²")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Simplification Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Douglas-Peucker Simplification

# COMMAND ----------

# Create a complex polygon (San Francisco Bay)
# Simplified coastline with many vertices
bay_coords = []
num_points = 1000

# Generate wavy coastline
for i in range(num_points):
    angle = 2 * np.pi * i / num_points
    r = 0.05 + 0.01 * np.sin(10 * angle)  # Add waviness
    x = -122.4 + r * np.cos(angle)
    y = 37.8 + r * np.sin(angle)
    bay_coords.append((x, y))

bay_polygon = Polygon(bay_coords)

print(f"Original polygon:")
print(f"  Vertices: {len(bay_polygon.exterior.coords)}")
print(f"  Area: {bay_polygon.area:.6f} square degrees")

# COMMAND ----------

# Simplify with different tolerance values
tolerances = [0.0001, 0.001, 0.005, 0.01]

simplified_polygons = {}
for tolerance in tolerances:
    simplified = bay_polygon.simplify(tolerance, preserve_topology=True)
    simplified_polygons[tolerance] = simplified
    
    reduction = (1 - len(simplified.exterior.coords) / len(bay_polygon.exterior.coords)) * 100
    
    print(f"Tolerance {tolerance}:")
    print(f"  Vertices: {len(simplified.exterior.coords)}")
    print(f"  Reduction: {reduction:.1f}%")
    print(f"  Area: {simplified.area:.6f} square degrees")
    print(f"  Area difference: {abs(simplified.area - bay_polygon.area) / bay_polygon.area * 100:.2f}%")
    print()

# COMMAND ----------

# Visualize simplification
m = folium.Map(location=[37.8, -122.4], zoom_start=11)

# Original
folium.GeoJson(
    bay_polygon.__geo_interface__,
    name='Original (1000 vertices)',
    style_function=lambda x: {'fillColor': 'red', 'color': 'red', 'weight': 2, 'fillOpacity': 0.2}
).add_to(m)

# Simplified with tolerance 0.005
folium.GeoJson(
    simplified_polygons[0.005].__geo_interface__,
    name=f'Simplified ({len(simplified_polygons[0.005].exterior.coords)} vertices)',
    style_function=lambda x: {'fillColor': 'blue', 'color': 'blue', 'weight': 2, 'fillOpacity': 0.3}
).add_to(m)

folium.LayerControl().add_to(m)
m

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Impact of Simplification

# COMMAND ----------

# Benchmark intersection operation with and without simplification
test_point = Point(-122.4, 37.8).buffer(0.02)

# Original polygon
start = time.time()
for _ in range(1000):
    result = bay_polygon.intersects(test_point)
duration_original = time.time() - start

# Simplified polygon
simplified = bay_polygon.simplify(0.005, preserve_topology=True)
start = time.time()
for _ in range(1000):
    result = simplified.intersects(test_point)
duration_simplified = time.time() - start

print("Performance comparison (1000 intersection tests):")
print(f"Original ({len(bay_polygon.exterior.coords)} vertices): {duration_original:.3f}s")
print(f"Simplified ({len(simplified.exterior.coords)} vertices): {duration_simplified:.3f}s")
print(f"Speedup: {duration_original/duration_simplified:.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Convex Hull and Bounding Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convex Hull Generation

# COMMAND ----------

# Create random points
np.random.seed(42)
num_points = 50

random_points = [
    Point(-122.4 + np.random.randn() * 0.02, 37.8 + np.random.randn() * 0.02)
    for _ in range(num_points)
]

# Create MultiPoint
from shapely.geometry import MultiPoint
multi_point = MultiPoint(random_points)

# Generate convex hull
convex_hull = multi_point.convex_hull

print(f"Original points: {len(random_points)}")
print(f"Convex hull vertices: {len(convex_hull.exterior.coords)}")
print(f"Area coverage: {convex_hull.area:.6f} square degrees")

# COMMAND ----------

# Visualize convex hull
m = folium.Map(location=[37.8, -122.4], zoom_start=13)

# Add points
for point in random_points:
    folium.CircleMarker(
        location=[point.y, point.x],
        radius=3,
        color='red',
        fill=True,
        fillColor='red'
    ).add_to(m)

# Add convex hull
folium.GeoJson(
    convex_hull.__geo_interface__,
    style_function=lambda x: {'fillColor': 'blue', 'color': 'blue', 'weight': 2, 'fillOpacity': 0.2}
).add_to(m)

m

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bounding Box Operations

# COMMAND ----------

# Different types of bounding boxes
bbox = multi_point.bounds  # (minx, miny, maxx, maxy)
bbox_polygon = box(*bbox)

# Envelope (same as bounding box)
envelope = multi_point.envelope

# Minimum rotated rectangle
from shapely.geometry import MultiPoint
minimum_rotated_rectangle = multi_point.minimum_rotated_rectangle

print(f"Bounding box area: {bbox_polygon.area:.6f}")
print(f"Minimum rotated rectangle area: {minimum_rotated_rectangle.area:.6f}")
print(f"Convex hull area: {convex_hull.area:.6f}")
print(f"Efficiency (MRR vs BBox): {minimum_rotated_rectangle.area / bbox_polygon.area:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Coordinate Reference System Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding CRS Transformations

# COMMAND ----------

# Create a test point
test_point = Point(-122.4194, 37.7749)  # San Francisco

# Common CRS transformations
crs_systems = {
    'WGS84': 'EPSG:4326',
    'Web Mercator': 'EPSG:3857',
    'UTM Zone 10N': 'EPSG:32610',
    'California Albers': 'EPSG:3310'
}

print("Coordinate transformations from San Francisco:")
print("=" * 60)

for name, epsg in crs_systems.items():
    # Create transformer
    transformer = pyproj.Transformer.from_crs('EPSG:4326', epsg, always_xy=True)
    
    # Transform point
    x, y = transformer.transform(test_point.x, test_point.y)
    
    print(f"{name} ({epsg}):")
    print(f"  X: {x:,.2f}, Y: {y:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Impact of CRS on Distance Calculations

# COMMAND ----------

# Two points in San Francisco
point1 = Point(-122.4194, 37.7749)  # Downtown SF
point2 = Point(-122.5194, 37.8749)  # 10 km away approximately

# Calculate distance in different CRS
print("Distance calculations in different coordinate systems:")
print("=" * 60)

# WGS84 (degrees - not meaningful)
dist_wgs84 = point1.distance(point2)
print(f"WGS84 (degrees): {dist_wgs84:.6f} degrees")

# UTM (meters - accurate for local areas)
transformer = pyproj.Transformer.from_crs('EPSG:4326', 'EPSG:32610', always_xy=True)
p1_utm = transform(transformer.transform, point1)
p2_utm = transform(transformer.transform, point2)
dist_utm = p1_utm.distance(p2_utm)
print(f"UTM Zone 10N (meters): {dist_utm:,.2f} meters ({dist_utm/1000:.2f} km)")

# Web Mercator (meters - distorted at high latitudes)
transformer_3857 = pyproj.Transformer.from_crs('EPSG:4326', 'EPSG:3857', always_xy=True)
p1_merc = transform(transformer_3857.transform, point1)
p2_merc = transform(transformer_3857.transform, point2)
dist_merc = p1_merc.distance(p2_merc)
print(f"Web Mercator (meters): {dist_merc:,.2f} meters ({dist_merc/1000:.2f} km)")

# Geodesic (true Earth distance)
from geopy.distance import geodesic
dist_geodesic = geodesic((point1.y, point1.x), (point2.y, point2.x)).meters
print(f"Geodesic (true): {dist_geodesic:,.2f} meters ({dist_geodesic/1000:.2f} km)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Topology Validation and Repair

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Topology Issues

# COMMAND ----------

# Create invalid geometries
invalid_geometries = {
    'Self-intersecting polygon': Polygon([
        (0, 0), (2, 2), (2, 0), (0, 2), (0, 0)  # Bowtie shape
    ]),
    'Unclosed ring': Polygon([
        (0, 0), (1, 0), (1, 1), (0, 1)  # Missing closing point
    ]),
    'Spike': Polygon([
        (0, 0), (1, 0), (1, 1), (0.5, 0.5), (1, 1), (0, 1), (0, 0)  # Has spike
    ])
}

print("Geometry Validation:")
print("=" * 60)

for name, geom in invalid_geometries.items():
    is_valid = geom.is_valid
    print(f"{name}:")
    print(f"  Valid: {is_valid}")
    if not is_valid:
        from shapely.validation import explain_validity
        print(f"  Issue: {explain_validity(geom)}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Repairing Invalid Geometries

# COMMAND ----------

# Repair invalid geometries
print("Repairing Invalid Geometries:")
print("=" * 60)

repaired = {}
for name, geom in invalid_geometries.items():
    if not geom.is_valid:
        # Use make_valid to fix topology issues
        fixed = make_valid(geom)
        repaired[name] = fixed
        
        print(f"{name}:")
        print(f"  Original valid: {geom.is_valid}")
        print(f"  Repaired valid: {fixed.is_valid}")
        print(f"  Original type: {geom.geom_type}")
        print(f"  Repaired type: {fixed.geom_type}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced Geometric Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rotation, Scaling, Translation

# COMMAND ----------

from shapely.affinity import rotate, scale, translate

# Create a simple square
square = box(0, 0, 1, 1)

print("Original square:")
print(f"  Bounds: {square.bounds}")
print(f"  Area: {square.area}")

# Rotate 45 degrees
rotated = rotate(square, 45, origin='center')
print(f"\nRotated 45°:")
print(f"  Bounds: {rotated.bounds}")
print(f"  Area: {rotated.area:.4f}")

# Scale 2x in x, 0.5x in y
scaled = scale(square, xfact=2.0, yfact=0.5, origin='center')
print(f"\nScaled (2x, 0.5x):")
print(f"  Bounds: {scaled.bounds}")
print(f"  Area: {scaled.area:.4f}")

# Translate
translated = translate(square, xoff=5, yoff=3)
print(f"\nTranslated (+5, +3):")
print(f"  Bounds: {translated.bounds}")
print(f"  Area: {translated.area:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Spark UDFs for Geometric Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Geometric Operation UDFs

# COMMAND ----------

# UDF for buffer operation
def buffer_geometry_udf(distance_meters):
    def buffer_func(lat, lon):
        try:
            # Create point in WGS84
            point = Point(lon, lat)
            
            # Transform to UTM
            transformer_to_utm = pyproj.Transformer.from_crs('EPSG:4326', 'EPSG:32610', always_xy=True)
            point_utm = transform(transformer_to_utm.transform, point)
            
            # Buffer in meters
            buffered_utm = point_utm.buffer(distance_meters)
            
            # Transform back to WGS84
            transformer_to_wgs = pyproj.Transformer.from_crs('EPSG:32610', 'EPSG:4326', always_xy=True)
            buffered_wgs = transform(transformer_to_wgs.transform, buffered_utm)
            
            # Return as WKT
            return buffered_wgs.wkt
        except Exception as e:
            return None
    
    return udf(buffer_func, StringType())

# UDF for simplification
def simplify_geometry_udf(tolerance):
    def simplify_func(wkt_geom):
        try:
            geom = wkt.loads(wkt_geom)
            simplified = geom.simplify(tolerance, preserve_topology=True)
            return simplified.wkt
        except:
            return None
    
    return udf(simplify_func, StringType())

# UDF for validation
@udf(returnType=BooleanType())
def is_valid_geometry(wkt_geom):
    try:
        geom = wkt.loads(wkt_geom)
        return geom.is_valid
    except:
        return False

print("✓ Geometric UDFs registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Operations at Scale

# COMMAND ----------

# Create sample dataset
import random
random.seed(42)

locations = [
    (i, 37.7 + random.random() * 0.15, -122.5 + random.random() * 0.15)
    for i in range(10000)
]

df_locations = spark.createDataFrame(locations, ['id', 'latitude', 'longitude'])

print(f"Created {df_locations.count():,} locations")
df_locations.show(5)

# COMMAND ----------

# Apply buffer operation
df_with_buffers = df_locations.withColumn(
    'buffer_500m',
    buffer_geometry_udf(500)(col('latitude'), col('longitude'))
)

print("Applied 500m buffers:")
df_with_buffers.select('id', 'latitude', 'longitude', 'buffer_500m').show(5, truncate=50)

# COMMAND ----------

# Validate geometries
df_validated = df_with_buffers.withColumn(
    'is_valid',
    is_valid_geometry(col('buffer_500m'))
)

valid_count = df_validated.filter(col('is_valid') == True).count()
invalid_count = df_validated.filter(col('is_valid') == False).count()

print(f"Valid geometries: {valid_count:,}")
print(f"Invalid geometries: {invalid_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Buffer Operations
# MAGIC - Always use appropriate projected CRS for distance-based operations
# MAGIC - Transform to local projection (UTM) before buffering
# MAGIC - Transform back to WGS84 for storage/visualization
# MAGIC - Consider dissolving overlapping buffers for analysis
# MAGIC
# MAGIC ### Simplification
# MAGIC - Test different tolerance values with sample data
# MAGIC - Preserve topology for accurate spatial relationships
# MAGIC - Balance between precision and performance
# MAGIC - Simplify before expensive operations (joins, intersections)
# MAGIC
# MAGIC ### CRS Transformations
# MAGIC - Use appropriate CRS for your analysis region
# MAGIC - UTM for local accurate measurements
# MAGIC - Web Mercator for web mapping
# MAGIC - Always document CRS in your code
# MAGIC
# MAGIC ### Topology Validation
# MAGIC - Validate geometries before complex operations
# MAGIC - Use make_valid() to repair issues
# MAGIC - Handle edge cases (null geometries, empty geometries)
# MAGIC - Log validation failures for debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Tips
# MAGIC
# MAGIC ### 1. Pre-compute and Cache
# MAGIC ```python
# MAGIC # Bad: Repeated transformations
# MAGIC for query in queries:
# MAGIC     result = df.transform().buffer().query()
# MAGIC
# MAGIC # Good: Transform once, cache
# MAGIC df_transformed = df.transform().cache()
# MAGIC for query in queries:
# MAGIC     result = df_transformed.buffer().query()
# MAGIC ```
# MAGIC
# MAGIC ### 2. Simplify Before Operations
# MAGIC ```python
# MAGIC # Bad: Complex polygons in expensive operations
# MAGIC result = complex_polygons.join(points)
# MAGIC
# MAGIC # Good: Simplify first
# MAGIC simplified = complex_polygons.simplify(0.001)
# MAGIC result = simplified.join(points)  # Much faster
# MAGIC ```
# MAGIC
# MAGIC ### 3. Use Appropriate Precision
# MAGIC ```python
# MAGIC # Bad: Sub-meter precision for city analysis
# MAGIC buffer_distance = 1000.001  # Unnecessarily precise
# MAGIC
# MAGIC # Good: Appropriate precision
# MAGIC buffer_distance = 1000  # Meters, suitable for analysis
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue with:
# MAGIC - **Demo 4**: Spatial Aggregations
# MAGIC - **Lab 5**: Optimizing Large-Scale Spatial Joins
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC - Always use projected CRS for metric operations
# MAGIC - Simplification dramatically improves performance
# MAGIC - Validate and repair geometries proactively
# MAGIC - Choose appropriate geometric operations for your use case
# MAGIC - Test transformations with sample data first

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Shapely Documentation](https://shapely.readthedocs.io/)
# MAGIC - [Pyproj CRS Transformations](https://pyproj4.github.io/pyproj/)
# MAGIC - [EPSG.io - CRS Database](https://epsg.io/)
# MAGIC - [Douglas-Peucker Algorithm](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm)
# MAGIC - [OGC Simple Features Specification](https://www.ogc.org/standards/sfa)
