# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Working with Points and Basic Geometries
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo teaches you how to work with point geometries and perform fundamental geospatial operations. You'll create points, calculate distances, perform buffer operations, and create compelling visualizations using real-world data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Create Point geometries from coordinate data
# MAGIC - Calculate distances between points using different methods
# MAGIC - Perform buffer operations to create zones around points
# MAGIC - Create centroids and bounding boxes
# MAGIC - Work with coordinate precision and validation
# MAGIC - Visualize points on interactive maps
# MAGIC - Understand when to use geographic vs. projected coordinates
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Demo 1 (Environment Setup)
# MAGIC - GeoPandas, Shapely, Folium installed
# MAGIC - Basic Python knowledge
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Libraries

# COMMAND ----------

!pip install geopandas

# COMMAND ----------

!pip install shapely h3 keplergl folium geopy pyproj rtree

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiPoint
from geopy.distance import geodesic
import folium
from keplergl import KeplerGl
import numpy as np

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Creating Point Geometries
# MAGIC
# MAGIC ### Method 1: Single Point Creation

# COMMAND ----------

# Create a point for San Francisco
# Format: Point(longitude, latitude)
# Note: Shapely uses (X, Y) order which is (longitude, latitude) for geographic coordinates

sf_point = Point(-122.4194, 37.7749)

print("San Francisco Point:")
print(f"  Geometry: {sf_point}")
print(f"  Type: {sf_point.geom_type}")
print(f"  Longitude (X): {sf_point.x}")
print(f"  Latitude (Y): {sf_point.y}")
print(f"  Is Valid: {sf_point.is_valid}")
print(f"  Is Empty: {sf_point.is_empty}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Creating Multiple Points from DataFrame

# COMMAND ----------

# Sample data: Coffee shops in San Francisco
coffee_shops = pd.DataFrame({
    'name': [
        'Blue Bottle Coffee',
        'Philz Coffee',
        'Sightglass Coffee',
        'Four Barrel Coffee',
        'Ritual Coffee Roasters',
        'Verve Coffee Roasters',
        'Equator Coffees',
        'Saint Frank Coffee'
    ],
    'neighborhood': [
        'Mission', 'Mission', 'SOMA', 'Mission',
        'Mission', 'Pacific Heights', 'Civic Center', 'Russian Hill'
    ],
    'longitude': [
        -122.4177, -122.4214, -122.4075, -122.4241,
        -122.4258, -122.4302, -122.4195, -122.4215
    ],
    'latitude': [
        37.7614, 37.7545, 37.7789, 37.7632,
        37.7649, 37.7939, 37.7794, 37.8009
    ],
    'avg_rating': [4.5, 4.7, 4.4, 4.6, 4.5, 4.6, 4.3, 4.5]
})

print("Coffee shops dataset:")
display(coffee_shops)

# COMMAND ----------

# Convert to GeoDataFrame
geometry = [Point(xy) for xy in zip(coffee_shops['longitude'], coffee_shops['latitude'])]

coffee_gdf = gpd.GeoDataFrame(
    coffee_shops,
    geometry=geometry,
    crs='EPSG:4326'  # WGS84 coordinate system
)

print(f"GeoDataFrame created:")
print(f"  Shape: {coffee_gdf.shape}")
print(f"  CRS: {coffee_gdf.crs}")
print(f"  Geometry column: {coffee_gdf.geometry.name}")
print(f"\nFirst few rows:")
display(coffee_gdf.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Distance Calculations
# MAGIC
# MAGIC ### Method 1: Geodesic Distance (Great Circle)
# MAGIC Best for geographic coordinates (WGS84)

# COMMAND ----------

# Calculate distance between two coffee shops
shop1_coords = (coffee_gdf.iloc[0]['latitude'], coffee_gdf.iloc[0]['longitude'])
shop2_coords = (coffee_gdf.iloc[1]['latitude'], coffee_gdf.iloc[1]['longitude'])

# GeoPy expects (latitude, longitude) order
distance_km = geodesic(shop1_coords, shop2_coords).kilometers
distance_miles = geodesic(shop1_coords, shop2_coords).miles
distance_meters = geodesic(shop1_coords, shop2_coords).meters

print(f"Distance from {coffee_gdf.iloc[0]['name']} to {coffee_gdf.iloc[1]['name']}:")
print(f"  {distance_km:.3f} kilometers")
print(f"  {distance_miles:.3f} miles")
print(f"  {distance_meters:.1f} meters")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Shapely Distance (Euclidean)
# MAGIC Works in the coordinate system's units (degrees for WGS84, meters for projected)

# COMMAND ----------

# Calculate Euclidean distance in degrees (not recommended for real distances)
euclidean_distance_degrees = coffee_gdf.iloc[0].geometry.distance(coffee_gdf.iloc[1].geometry)
print(f"Euclidean distance (in degrees): {euclidean_distance_degrees:.6f}°")
print("⚠️ Note: This is not a real-world distance - it's in degree units\n")

# For accurate distance, transform to projected CRS
coffee_gdf_projected = coffee_gdf.to_crs('EPSG:3857')  # Web Mercator (meters)
euclidean_distance_meters = coffee_gdf_projected.iloc[0].geometry.distance(
    coffee_gdf_projected.iloc[1].geometry
)

print(f"Euclidean distance (in meters): {euclidean_distance_meters:.1f} m")
print(f"Geodesic distance (in meters): {distance_meters:.1f} m")
print(f"Difference: {abs(euclidean_distance_meters - distance_meters):.1f} m")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Distance Matrix
# MAGIC Find distances between all pairs of coffee shops

# COMMAND ----------

def calculate_distance_matrix(gdf):
    """Calculate pairwise geodesic distances between all points"""
    n = len(gdf)
    distances = np.zeros((n, n))
    
    for i in range(n):
        for j in range(n):
            if i != j:
                coord1 = (gdf.iloc[i]['latitude'], gdf.iloc[i]['longitude'])
                coord2 = (gdf.iloc[j]['latitude'], gdf.iloc[j]['longitude'])
                distances[i, j] = geodesic(coord1, coord2).kilometers
    
    return pd.DataFrame(
        distances,
        index=gdf['name'],
        columns=gdf['name']
    )

# Calculate distance matrix
distance_matrix = calculate_distance_matrix(coffee_gdf)

print("Distance Matrix (kilometers):")
print("Showing distances from first 3 shops to all shops:\n")
display(distance_matrix.iloc[:3, :].round(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find Nearest Neighbor

# COMMAND ----------

def find_nearest_shop(gdf, target_idx):
    """Find the nearest coffee shop to a given shop"""
    target_shop = gdf.iloc[target_idx]
    target_coords = (target_shop['latitude'], target_shop['longitude'])
    
    min_distance = float('inf')
    nearest_idx = None
    
    for idx, row in gdf.iterrows():
        if idx != target_idx:
            shop_coords = (row['latitude'], row['longitude'])
            distance = geodesic(target_coords, shop_coords).kilometers
            
            if distance < min_distance:
                min_distance = distance
                nearest_idx = idx
    
    nearest_shop = gdf.iloc[nearest_idx]
    
    return nearest_shop, min_distance

# Find nearest shop to Blue Bottle Coffee
nearest, distance = find_nearest_shop(coffee_gdf, 0)

print(f"Nearest shop to {coffee_gdf.iloc[0]['name']}:")
print(f"  Name: {nearest['name']}")
print(f"  Neighborhood: {nearest['neighborhood']}")
print(f"  Distance: {distance:.3f} km")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Buffer Operations
# MAGIC
# MAGIC Buffers create zones around geometries - useful for service areas, proximity analysis, etc.

# COMMAND ----------

# Create 500-meter buffer around a coffee shop
# First, transform to projected CRS for accurate meter-based buffers
coffee_gdf_projected = coffee_gdf.to_crs('EPSG:3857')

# Create buffer (500 meters)
buffer_distance = 500  # meters
coffee_gdf_projected['buffer_500m'] = coffee_gdf_projected.geometry.buffer(buffer_distance)

# Transform back to WGS84 for visualization
coffee_with_buffers = coffee_gdf_projected.to_crs('EPSG:4326')

print(f"Created 500-meter buffers around {len(coffee_gdf)} coffee shops")
print(f"\nSample buffer (Blue Bottle Coffee):")
print(f"  Original point: {coffee_with_buffers.iloc[0].geometry}")
print(f"  Buffer geometry type: {coffee_with_buffers.iloc[0]['buffer_500m'].geom_type}")
print(f"  Buffer area: {coffee_gdf_projected.iloc[0]['buffer_500m'].area:.0f} square meters")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Buffers

# COMMAND ----------

# Create map centered on San Francisco
m = folium.Map(
    location=[37.7749, -122.4194],
    zoom_start=13,
    tiles='OpenStreetMap'
)

# Add buffer circles
for idx, row in coffee_with_buffers.iterrows():
    # Add buffer polygon
    folium.GeoJson(
        row['buffer_500m'].__geo_interface__,
        style_function=lambda x: {
            'fillColor': 'blue',
            'color': 'darkblue',
            'weight': 1,
            'fillOpacity': 0.2
        }
    ).add_to(m)
    
    # Add coffee shop marker
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup=f"<b>{row['name']}</b><br>Rating: {row['avg_rating']}<br>500m service area",
        tooltip=row['name'],
        icon=folium.Icon(color='red', icon='coffee', prefix='fa')
    ).add_to(m)

# Add legend
legend_html = '''
<div style="position: fixed; 
     top: 10px; right: 10px; width: 200px; height: 90px; 
     background-color: white; border:2px solid grey; z-index:9999; 
     font-size:14px; padding: 10px">
     <p><b>Coffee Shop Coverage</b></p>
     <p><i class="fa fa-coffee" style="color:red"></i> Coffee Shop</p>
     <p><span style="color:darkblue">○</span> 500m Service Area</p>
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Buffer Overlaps

# COMMAND ----------

# Check which buffers overlap
overlaps = []

for i in range(len(coffee_gdf_projected)):
    for j in range(i + 1, len(coffee_gdf_projected)):
        buffer1 = coffee_gdf_projected.iloc[i]['buffer_500m']
        buffer2 = coffee_gdf_projected.iloc[j]['buffer_500m']
        
        if buffer1.intersects(buffer2):
            shop1 = coffee_gdf.iloc[i]['name']
            shop2 = coffee_gdf.iloc[j]['name']
            overlap_area = buffer1.intersection(buffer2).area
            overlaps.append({
                'shop1': shop1,
                'shop2': shop2,
                'overlap_area_sqm': overlap_area
            })

overlaps_df = pd.DataFrame(overlaps)
print(f"Found {len(overlaps)} buffer overlaps:\n")
display(overlaps_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Point Aggregation and Analysis
# MAGIC
# MAGIC ### Centroid Calculation

# COMMAND ----------

# Create MultiPoint from all coffee shops
all_points = MultiPoint(coffee_gdf.geometry.tolist())

# Calculate centroid
centroid = all_points.centroid

print(f"Centroid of all coffee shops:")
print(f"  Coordinates: ({centroid.x:.6f}, {centroid.y:.6f})")
print(f"  Geometry type: {centroid.geom_type}")

# Find which shop is closest to the centroid
centroid_coords = (centroid.y, centroid.x)
closest_to_centroid = None
min_dist = float('inf')

for idx, row in coffee_gdf.iterrows():
    shop_coords = (row['latitude'], row['longitude'])
    dist = geodesic(centroid_coords, shop_coords).kilometers
    if dist < min_dist:
        min_dist = dist
        closest_to_centroid = row['name']

print(f"\nClosest shop to centroid: {closest_to_centroid} ({min_dist:.3f} km away)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bounding Box

# COMMAND ----------

# Get bounding box (envelope)
bounds = coffee_gdf.total_bounds  # Returns [minx, miny, maxx, maxy]
bbox = all_points.envelope

print(f"Bounding box:")
print(f"  Min Longitude: {bounds[0]:.6f}")
print(f"  Min Latitude: {bounds[1]:.6f}")
print(f"  Max Longitude: {bounds[2]:.6f}")
print(f"  Max Latitude: {bounds[3]:.6f}")
print(f"\n  Width: {(bounds[2] - bounds[0]) * 111:.2f} km (approximate)")
print(f"  Height: {(bounds[3] - bounds[1]) * 111:.2f} km (approximate)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Centroid and Bounding Box

# COMMAND ----------

# Create visualization map
m2 = folium.Map(
    location=[centroid.y, centroid.x],
    zoom_start=12,
    tiles='CartoDB positron'
)

# Add bounding box
bbox_coords = [
    [bounds[1], bounds[0]],  # SW corner
    [bounds[3], bounds[0]],  # NW corner
    [bounds[3], bounds[2]],  # NE corner
    [bounds[1], bounds[2]],  # SE corner
    [bounds[1], bounds[0]]   # Close the box
]
folium.PolyLine(
    bbox_coords,
    color='green',
    weight=3,
    opacity=0.7,
    popup='Bounding Box'
).add_to(m2)

# Add centroid
folium.Marker(
    location=[centroid.y, centroid.x],
    popup='<b>Centroid</b><br>Geographic center of all shops',
    icon=folium.Icon(color='purple', icon='star', prefix='fa'),
    tooltip='Centroid'
).add_to(m2)

# Add coffee shops
for idx, row in coffee_gdf.iterrows():
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=6,
        popup=row['name'],
        color='red',
        fill=True,
        fillColor='red',
        fillOpacity=0.7
    ).add_to(m2)

displayHTML(m2._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Coordinate Validation and Precision

# COMMAND ----------

# Check coordinate precision
print("Coordinate Precision Analysis:\n")

for idx, row in coffee_gdf.head(3).iterrows():
    lon = row['longitude']
    lat = row['latitude']
    
    # Count decimal places
    lon_str = str(lon).split('.')[-1]
    lat_str = str(lat).split('.')[-1]
    
    print(f"{row['name']}:")
    print(f"  Longitude: {lon} ({len(lon_str)} decimal places)")
    print(f"  Latitude: {lat} ({len(lat_str)} decimal places)")
    
    # Estimate precision
    # Each decimal place of latitude ≈ 11 meters
    # Each decimal place of longitude ≈ 11 meters * cos(latitude)
    lat_precision = 111000 / (10 ** len(lat_str))  # meters
    lon_precision = 111000 * np.cos(np.radians(lat)) / (10 ** len(lon_str))
    
    print(f"  Approximate precision: ±{lat_precision:.1f}m (lat), ±{lon_precision:.1f}m (lon)\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Geometries

# COMMAND ----------

# Check geometry validity
print("Geometry Validation:\n")
print(f"Total points: {len(coffee_gdf)}")
print(f"Valid geometries: {coffee_gdf.geometry.is_valid.sum()}")
print(f"Empty geometries: {coffee_gdf.geometry.is_empty.sum()}")
print(f"Null geometries: {coffee_gdf.geometry.isna().sum()}")

# Check if coordinates are within valid ranges
coffee_gdf['valid_lon'] = coffee_gdf['longitude'].between(-180, 180)
coffee_gdf['valid_lat'] = coffee_gdf['latitude'].between(-90, 90)

print(f"\nCoordinate Range Validation:")
print(f"Valid longitudes: {coffee_gdf['valid_lon'].sum()}/{len(coffee_gdf)}")
print(f"Valid latitudes: {coffee_gdf['valid_lat'].sum()}/{len(coffee_gdf)}")

# Clean up validation columns
coffee_gdf = coffee_gdf.drop(columns=['valid_lon', 'valid_lat'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced Point Operations
# MAGIC
# MAGIC ### Find Points Within Distance

# COMMAND ----------

def find_shops_within_distance(gdf, target_idx, max_distance_km):
    """Find all shops within a specified distance from a target shop"""
    target_shop = gdf.iloc[target_idx]
    target_coords = (target_shop['latitude'], target_shop['longitude'])
    
    nearby_shops = []
    
    for idx, row in gdf.iterrows():
        if idx != target_idx:
            shop_coords = (row['latitude'], row['longitude'])
            distance = geodesic(target_coords, shop_coords).kilometers
            
            if distance <= max_distance_km:
                nearby_shops.append({
                    'name': row['name'],
                    'neighborhood': row['neighborhood'],
                    'distance_km': distance,
                    'rating': row['avg_rating']
                })
    
    return pd.DataFrame(nearby_shops).sort_values('distance_km')

# Find all shops within 2 km of Blue Bottle Coffee
nearby = find_shops_within_distance(coffee_gdf, 0, 2.0)

print(f"Coffee shops within 2 km of {coffee_gdf.iloc[0]['name']}:\n")
display(nearby)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Distance-Based Categories

# COMMAND ----------

# Categorize shops by distance from centroid
centroid_coords = (centroid.y, centroid.x)

def categorize_by_distance(row):
    shop_coords = (row['latitude'], row['longitude'])
    distance = geodesic(centroid_coords, shop_coords).kilometers
    
    if distance < 1.5:
        return 'Central'
    elif distance < 3.0:
        return 'Mid-range'
    else:
        return 'Outer'

coffee_gdf['distance_category'] = coffee_gdf.apply(categorize_by_distance, axis=1)
coffee_gdf['distance_from_center'] = coffee_gdf.apply(
    lambda row: geodesic(centroid_coords, (row['latitude'], row['longitude'])).kilometers,
    axis=1
)

print("Shops categorized by distance from center:\n")
display(coffee_gdf[['name', 'neighborhood', 'distance_from_center', 'distance_category']].sort_values('distance_from_center'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Interactive Visualization with Kepler.gl

# COMMAND ----------

# Prepare data for Kepler.gl
kepler_data = coffee_gdf.copy()
kepler_data['lon'] = kepler_data.geometry.x
kepler_data['lat'] = kepler_data.geometry.y

# Create Kepler map
map_kepler = KeplerGl(height=600)
map_kepler.add_data(data=kepler_data, name='Coffee Shops')

# Display
map_kepler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Summary Statistics

# COMMAND ----------

print("=" * 60)
print("COFFEE SHOPS SPATIAL ANALYSIS SUMMARY")
print("=" * 60)
print(f"\nTotal Shops: {len(coffee_gdf)}")
print(f"Neighborhoods Covered: {coffee_gdf['neighborhood'].nunique()}")
print(f"Average Rating: {coffee_gdf['avg_rating'].mean():.2f}")

print(f"\n📍 Geographic Extent:")
print(f"  Longitude range: {bounds[0]:.4f}° to {bounds[2]:.4f}°")
print(f"  Latitude range: {bounds[1]:.4f}° to {bounds[3]:.4f}°")

print(f"\n📏 Distance Analysis:")
print(f"  Minimum distance between shops: {distance_matrix[distance_matrix > 0].min().min():.3f} km")
print(f"  Maximum distance between shops: {distance_matrix.max().max():.3f} km")
print(f"  Average distance to nearest neighbor: {distance_matrix[distance_matrix > 0].min(axis=1).mean():.3f} km")

print(f"\n🎯 Centroid:")
print(f"  Location: ({centroid.y:.6f}°, {centroid.x:.6f}°)")

print(f"\n📊 Distance Categories:")
print(coffee_gdf['distance_category'].value_counts().to_string())

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### ✓ What We Learned
# MAGIC
# MAGIC 1. **Point Creation**
# MAGIC    - Create individual points with `Point(lon, lat)`
# MAGIC    - Convert DataFrames to GeoDataFrames with geometry column
# MAGIC    - Always specify CRS explicitly
# MAGIC
# MAGIC 2. **Distance Calculations**
# MAGIC    - Use `geodesic()` for accurate great-circle distances
# MAGIC    - Transform to projected CRS for Euclidean distances in meters
# MAGIC    - Build distance matrices for multiple points
# MAGIC
# MAGIC 3. **Buffer Operations**
# MAGIC    - Use projected CRS (EPSG:3857) for meter-based buffers
# MAGIC    - Transform back to WGS84 for visualization
# MAGIC    - Check for buffer overlaps with `intersects()`
# MAGIC
# MAGIC 4. **Point Aggregation**
# MAGIC    - Calculate centroids with `MultiPoint().centroid`
# MAGIC    - Find bounding boxes with `total_bounds` or `envelope`
# MAGIC    - Aggregate by spatial proximity
# MAGIC
# MAGIC 5. **Validation**
# MAGIC    - Check geometry validity with `is_valid`
# MAGIC    - Validate coordinate ranges
# MAGIC    - Understand coordinate precision implications
# MAGIC
# MAGIC ### 🎯 Best Practices
# MAGIC - Always use geodesic distance for geographic coordinates
# MAGIC - Transform to projected CRS for accurate meter/km calculations
# MAGIC - Validate geometries before operations
# MAGIC - Document coordinate system assumptions
# MAGIC - Consider coordinate precision requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC **Next Demo:** Polygons and Spatial Relationships
# MAGIC
# MAGIC You'll learn to:
# MAGIC - Create and manipulate polygon geometries
# MAGIC - Perform point-in-polygon tests
# MAGIC - Calculate polygon intersections and unions
# MAGIC - Use spatial predicates (contains, intersects, within, touches)
# MAGIC - Build complex spatial queries
# MAGIC
# MAGIC ### Quick Reference
# MAGIC
# MAGIC ```python
# MAGIC # Create point
# MAGIC point = Point(lon, lat)
# MAGIC
# MAGIC # Calculate geodesic distance
# MAGIC from geopy.distance import geodesic
# MAGIC dist_km = geodesic((lat1, lon1), (lat2, lon2)).kilometers
# MAGIC
# MAGIC # Create buffer (in projected CRS)
# MAGIC gdf_proj = gdf.to_crs('EPSG:3857')
# MAGIC gdf_proj['buffer'] = gdf_proj.geometry.buffer(500)  # 500 meters
# MAGIC
# MAGIC # Find centroid
# MAGIC from shapely.geometry import MultiPoint
# MAGIC centroid = MultiPoint(gdf.geometry.tolist()).centroid
# MAGIC
# MAGIC # Get bounding box
# MAGIC bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
# MAGIC ```
