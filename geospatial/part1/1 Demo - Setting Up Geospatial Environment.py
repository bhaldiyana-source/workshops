# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Setting Up Geospatial Environment
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo walks you through setting up a complete geospatial analytics environment in Databricks. You'll install essential libraries, verify your setup, and load sample datasets to prepare for geospatial analysis.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Install geospatial libraries (GeoPandas, Shapely, H3, Kepler.gl, Folium)
# MAGIC - Verify library installations and versions
# MAGIC - Configure your Databricks cluster for geospatial work
# MAGIC - Import and validate sample geospatial datasets
# MAGIC - Troubleshoot common installation issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with DBR 14.3+ cluster
# MAGIC - Cluster with at least 8GB memory recommended
# MAGIC - Basic Python knowledge
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Core Geospatial Libraries
# MAGIC
# MAGIC We'll install the essential libraries for geospatial analytics. This may take 2-3 minutes.
# MAGIC
# MAGIC ### Libraries to Install:
# MAGIC - **geopandas**: Spatial operations on DataFrames
# MAGIC - **shapely**: Geometry manipulation
# MAGIC - **h3**: Hexagonal spatial indexing
# MAGIC - **keplergl**: Interactive visualization
# MAGIC - **folium**: Map creation
# MAGIC - **geopy**: Geocoding and distance calculations
# MAGIC - **pyproj**: Coordinate system transformations
# MAGIC - **rtree**: Spatial indexing (optional but recommended)

# COMMAND ----------

# MAGIC %pip install --quiet geopandas 

# COMMAND ----------

# Install geospatial packages
%pip install shapely h3 keplergl folium geopy pyproj rtree

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart Python Environment
# MAGIC After installing packages, we need to restart the Python environment to load the new libraries.

# COMMAND ----------

# Restart Python to load newly installed packages
#dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Installation
# MAGIC
# MAGIC Let's verify that all libraries are properly installed and check their versions.

# COMMAND ----------

# Import libraries and check versions
import sys
print(f"Python version: {sys.version}\n")

try:
    import geopandas as gpd
    print(f"✓ GeoPandas: {gpd.__version__}")
except ImportError as e:
    print(f"✗ GeoPandas: Not installed - {e}")

try:
    import shapely
    print(f"✓ Shapely: {shapely.__version__}")
except ImportError as e:
    print(f"✗ Shapely: Not installed - {e}")

try:
    import h3
    print(f"✓ H3: {h3.__version__}")
except ImportError as e:
    print(f"✗ H3: Not installed - {e}")

try:
    import folium
    print(f"✓ Folium: {folium.__version__}")
except ImportError as e:
    print(f"✗ Folium: Not installed - {e}")

try:
    from keplergl import KeplerGl
    print(f"✓ Kepler.gl: Installed")
except ImportError as e:
    print(f"✗ Kepler.gl: Not installed - {e}")

try:
    import geopy
    print(f"✓ GeoPy: {geopy.__version__}")
except ImportError as e:
    print(f"✗ GeoPy: Not installed - {e}")

try:
    import pyproj
    print(f"✓ PyProj: {pyproj.__version__}")
except ImportError as e:
    print(f"✗ PyProj: Not installed - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test Basic Functionality
# MAGIC
# MAGIC Let's test basic geospatial operations to ensure everything works correctly.

# COMMAND ----------

# Test Shapely - Create basic geometries
from shapely.geometry import Point, LineString, Polygon

# Create a point
point = Point(-122.4194, 37.7749)
print(f"Point created: {point}")
print(f"Point coordinates: {point.x}, {point.y}\n")

# Create a line
line = LineString([(-122.5, 37.7), (-122.4, 37.8), (-122.3, 37.75)])
print(f"LineString created: {line}")
print(f"Line length (degrees): {line.length:.4f}\n")

# Create a polygon
polygon = Polygon([(-122.5, 37.7), (-122.3, 37.7), (-122.3, 37.8), (-122.5, 37.8)])
print(f"Polygon created: {polygon}")
print(f"Polygon area (square degrees): {polygon.area:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Spatial Operations

# COMMAND ----------

# Test point-in-polygon
is_inside = polygon.contains(point)
print(f"Is point inside polygon? {is_inside}\n")

# Test buffer operation
buffered_point = point.buffer(0.01)
print(f"Buffer created around point")
print(f"Buffer area: {buffered_point.area:.6f} square degrees\n")

# Test intersection
intersection = buffered_point.intersection(polygon)
print(f"Intersection area: {intersection.area:.6f} square degrees")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Sample GeoDataFrame
# MAGIC
# MAGIC Let's create a GeoDataFrame with sample data to test GeoPandas functionality.

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Create sample data - Major US cities
cities_data = {
    'name': ['San Francisco', 'Los Angeles', 'New York', 'Chicago', 'Houston', 'Phoenix'],
    'state': ['California', 'California', 'New York', 'Illinois', 'Texas', 'Arizona'],
    'population': [873965, 3979576, 8336817, 2693976, 2320268, 1680992],
    'longitude': [-122.4194, -118.2437, -74.0060, -87.6298, -95.3698, -112.0740],
    'latitude': [37.7749, 34.0522, 40.7128, 41.8781, 29.7604, 33.4484]
}

# Create DataFrame
df = pd.DataFrame(cities_data)

# Create geometry column
geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]

# Create GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

print("GeoDataFrame created successfully!")
print(f"\nShape: {gdf.shape}")
print(f"CRS: {gdf.crs}")
print(f"\nFirst few rows:")
display(gdf.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Coordinate Reference System (CRS) Operations

# COMMAND ----------

# Check current CRS
print(f"Current CRS: {gdf.crs}\n")

# Transform to Web Mercator (EPSG:3857) for better distance calculations
gdf_mercator = gdf.to_crs('EPSG:3857')
print(f"Transformed CRS: {gdf_mercator.crs}\n")

# Compare coordinates
print("Original WGS84 coordinates (San Francisco):")
print(f"  Longitude: {gdf.loc[0, 'geometry'].x}")
print(f"  Latitude: {gdf.loc[0, 'geometry'].y}\n")

print("Web Mercator coordinates (San Francisco):")
print(f"  X: {gdf_mercator.loc[0, 'geometry'].x:.2f} meters")
print(f"  Y: {gdf_mercator.loc[0, 'geometry'].y:.2f} meters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test Distance Calculations

# COMMAND ----------

from geopy.distance import geodesic

# Calculate distance between San Francisco and Los Angeles
sf_coords = (37.7749, -122.4194)
la_coords = (34.0522, -118.2437)

distance_km = geodesic(sf_coords, la_coords).kilometers
distance_miles = geodesic(sf_coords, la_coords).miles

print(f"Distance from San Francisco to Los Angeles:")
print(f"  {distance_km:.2f} kilometers")
print(f"  {distance_miles:.2f} miles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test H3 Hexagonal Indexing

# COMMAND ----------

import h3

# Convert San Francisco coordinates to H3 index
sf_lat, sf_lon = 37.7749, -122.4194
resolution = 9  # Resolution level (0-15, higher = smaller hexagons)

h3_index = h3.geo_to_h3(sf_lat, sf_lon, resolution)
print(f"H3 index for San Francisco (resolution {resolution}): {h3_index}\n")

# Get neighboring hexagons
neighbors = h3.k_ring(h3_index, k=1)
print(f"Number of neighbors (including center): {len(neighbors)}\n")

# Get hexagon boundary
boundary = h3.h3_to_geo_boundary(h3_index)
print(f"Hexagon boundary (first 3 points):")
for i, coord in enumerate(boundary[:3]):
    print(f"  Point {i+1}: lat={coord[0]:.6f}, lon={coord[1]:.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Simple Visualization with Folium

# COMMAND ----------

import folium

# Create base map centered on San Francisco
m = folium.Map(
    location=[37.7749, -122.4194],
    zoom_start=6,
    tiles='OpenStreetMap'
)

# Add markers for all cities
for idx, row in gdf.iterrows():
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup=f"{row['name']}<br>Population: {row['population']:,}",
        tooltip=row['name'],
        icon=folium.Icon(color='blue', icon='info-sign')
    ).add_to(m)

# Add circle markers with size based on population
for idx, row in gdf.iterrows():
    folium.Circle(
        location=[row['latitude'], row['longitude']],
        radius=row['population'] / 100,  # Scale down for visualization
        color='red',
        fill=True,
        fillColor='red',
        fillOpacity=0.3,
        popup=f"{row['name']}<br>Population: {row['population']:,}"
    ).add_to(m)

# Display map
displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Test Kepler.gl Visualization

# COMMAND ----------

from keplergl import KeplerGl

# Create Kepler map
map_1 = KeplerGl(height=600)

# Add GeoDataFrame
map_1.add_data(data=gdf, name='US Cities')

# Display map
map_1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Validate Geometries

# COMMAND ----------

# Check if all geometries are valid
print("Geometry Validation:")
print(f"Total geometries: {len(gdf)}")
print(f"Valid geometries: {gdf.geometry.is_valid.sum()}")
print(f"Invalid geometries: {(~gdf.geometry.is_valid).sum()}\n")

# Check geometry types
print(f"Geometry types:")
print(gdf.geometry.type.value_counts())

# Check bounds
print(f"\nBounds (min_lon, min_lat, max_lon, max_lat):")
print(gdf.total_bounds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Create Sample Polygon Data

# COMMAND ----------

# Create sample service areas around cities (circular buffers)
# First transform to projected CRS for accurate distance-based buffers
gdf_projected = gdf.to_crs('EPSG:3857')

# Create 50km buffer around each city
buffer_distance = 50000  # 50 kilometers in meters
gdf_projected['service_area'] = gdf_projected.geometry.buffer(buffer_distance)

# Create separate GeoDataFrame for service areas
service_areas = gpd.GeoDataFrame(
    gdf_projected[['name', 'state', 'population']],
    geometry=gdf_projected['service_area'],
    crs='EPSG:3857'
)

# Transform back to WGS84 for visualization
service_areas_wgs84 = service_areas.to_crs('EPSG:4326')

print("Service areas created!")
print(f"\nSample service area (San Francisco):")
print(f"  Area: {service_areas.loc[0, 'service_area'].area / 1_000_000:.2f} square kilometers")
display(service_areas_wgs84.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Visualize Service Areas

# COMMAND ----------

# Create map with service areas
m2 = folium.Map(location=[37.7749, -122.4194], zoom_start=5)

# Add service area polygons
for idx, row in service_areas_wgs84.iterrows():
    folium.GeoJson(
        row['geometry'].__geo_interface__,
        style_function=lambda x: {
            'fillColor': 'blue',
            'color': 'darkblue',
            'weight': 2,
            'fillOpacity': 0.2
        },
        tooltip=f"{row['name']} Service Area"
    ).add_to(m2)

# Add city markers
for idx, row in gdf.iterrows():
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup=f"{row['name']}",
        icon=folium.Icon(color='red', icon='star')
    ).add_to(m2)

displayHTML(m2._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Test Spatial Join

# COMMAND ----------

# Create random points to test which service area they fall into
import random

# Generate random points within the bounds
random.seed(42)
bounds = gdf.total_bounds  # min_lon, min_lat, max_lon, max_lat

random_points = []
for i in range(20):
    lon = random.uniform(bounds[0], bounds[2])
    lat = random.uniform(bounds[1], bounds[3])
    random_points.append(Point(lon, lat))

# Create GeoDataFrame for random points
points_gdf = gpd.GeoDataFrame(
    {'id': range(len(random_points))},
    geometry=random_points,
    crs='EPSG:4326'
)

# Perform spatial join
joined = gpd.sjoin(points_gdf, service_areas_wgs84, how='left', predicate='within')

print(f"Spatial join completed!")
print(f"Total points: {len(points_gdf)}")
print(f"Points within service areas: {joined['name'].notna().sum()}")
print(f"\nPoints by service area:")
print(joined['name'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Summary of Installed Capabilities
# MAGIC
# MAGIC ### ✓ Installation Complete
# MAGIC
# MAGIC You now have a fully functional geospatial environment with:
# MAGIC
# MAGIC 1. **Core Libraries**
# MAGIC    - GeoPandas for spatial DataFrames
# MAGIC    - Shapely for geometry operations
# MAGIC    - PyProj for coordinate transformations
# MAGIC
# MAGIC 2. **Visualization**
# MAGIC    - Folium for interactive maps
# MAGIC    - Kepler.gl for advanced visualizations
# MAGIC
# MAGIC 3. **Spatial Indexing**
# MAGIC    - H3 for hexagonal indexing
# MAGIC    - RTree for spatial indexing
# MAGIC
# MAGIC 4. **Utilities**
# MAGIC    - GeoPy for geocoding and distances
# MAGIC
# MAGIC ### Verified Functionality
# MAGIC ✓ Geometry creation (Point, LineString, Polygon)
# MAGIC ✓ Spatial operations (buffer, intersection, contains)
# MAGIC ✓ GeoDataFrame operations
# MAGIC ✓ CRS transformations
# MAGIC ✓ Distance calculations
# MAGIC ✓ H3 hexagonal indexing
# MAGIC ✓ Interactive visualizations
# MAGIC ✓ Spatial joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Guide
# MAGIC
# MAGIC ### Common Issues and Solutions
# MAGIC
# MAGIC #### Issue 1: Package Installation Fails
# MAGIC **Symptom:** Error during `%pip install`
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Ensure cluster has internet access
# MAGIC - Try installing packages one at a time
# MAGIC - Check cluster has sufficient memory (8GB+ recommended)
# MAGIC - Try: `%pip install --upgrade pip` first
# MAGIC
# MAGIC #### Issue 2: Import Errors After Installation
# MAGIC **Symptom:** `ModuleNotFoundError` despite successful installation
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Make sure you ran `dbutils.library.restartPython()`
# MAGIC - Detach and reattach notebook to cluster
# MAGIC - Restart cluster if issue persists
# MAGIC
# MAGIC #### Issue 3: Visualization Not Displaying
# MAGIC **Symptom:** Blank maps or no output
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Ensure JavaScript is enabled in browser
# MAGIC - Try refreshing the browser page
# MAGIC - Check data has valid geometries
# MAGIC - Verify CRS is set correctly
# MAGIC
# MAGIC #### Issue 4: CRS Transformation Errors
# MAGIC **Symptom:** `CRSError` or incorrect coordinates
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Verify input CRS is correct: `gdf.crs`
# MAGIC - Explicitly set CRS: `gdf = gdf.set_crs('EPSG:4326')`
# MAGIC - Check EPSG code is valid at https://epsg.io/
# MAGIC
# MAGIC #### Issue 5: Performance Issues
# MAGIC **Symptom:** Slow operations or timeouts
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Sample data for testing: `gdf.sample(1000)`
# MAGIC - Use spatial indexes: `gdf.sindex`
# MAGIC - Simplify geometries: `gdf.geometry.simplify(0.001)`
# MAGIC - Increase cluster size

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Validation Function

# COMMAND ----------

def validate_geospatial_environment():
    """
    Validate that the geospatial environment is properly configured.
    Returns True if all checks pass, False otherwise.
    """
    checks_passed = 0
    checks_total = 0
    
    print("=" * 60)
    print("GEOSPATIAL ENVIRONMENT VALIDATION")
    print("=" * 60 + "\n")
    
    # Check 1: GeoPandas
    checks_total += 1
    try:
        import geopandas as gpd
        print(f"✓ GeoPandas {gpd.__version__}")
        checks_passed += 1
    except ImportError:
        print("✗ GeoPandas - NOT INSTALLED")
    
    # Check 2: Shapely
    checks_total += 1
    try:
        import shapely
        print(f"✓ Shapely {shapely.__version__}")
        checks_passed += 1
    except ImportError:
        print("✗ Shapely - NOT INSTALLED")
    
    # Check 3: H3
    checks_total += 1
    try:
        import h3
        print(f"✓ H3 {h3.__version__}")
        checks_passed += 1
    except ImportError:
        print("✗ H3 - NOT INSTALLED")
    
    # Check 4: Folium
    checks_total += 1
    try:
        import folium
        print(f"✓ Folium {folium.__version__}")
        checks_passed += 1
    except ImportError:
        print("✗ Folium - NOT INSTALLED")
    
    # Check 5: Kepler.gl
    checks_total += 1
    try:
        from keplergl import KeplerGl
        print(f"✓ Kepler.gl")
        checks_passed += 1
    except ImportError:
        print("✗ Kepler.gl - NOT INSTALLED")
    
    # Check 6: GeoPy
    checks_total += 1
    try:
        import geopy
        print(f"✓ GeoPy {geopy.__version__}")
        checks_passed += 1
    except ImportError:
        print("✗ GeoPy - NOT INSTALLED")
    
    print("\n" + "=" * 60)
    print(f"RESULT: {checks_passed}/{checks_total} checks passed")
    print("=" * 60)
    
    if checks_passed == checks_total:
        print("\n✓ Environment is ready for geospatial analytics!")
        return True
    else:
        print("\n✗ Some components are missing. Please install missing packages.")
        return False

# Run validation
validate_geospatial_environment()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Congratulations! Your geospatial environment is now set up and ready to use.
# MAGIC
# MAGIC **Next Demo:** Working with Points and Basic Geometries
# MAGIC
# MAGIC In the next demo, you'll learn to:
# MAGIC - Create and manipulate point geometries
# MAGIC - Calculate distances between locations
# MAGIC - Perform buffer operations
# MAGIC - Create custom visualizations
# MAGIC - Work with real-world coordinate data
# MAGIC
# MAGIC ### Quick Reference: Key Functions
# MAGIC
# MAGIC ```python
# MAGIC # Create geometries
# MAGIC from shapely.geometry import Point, LineString, Polygon
# MAGIC point = Point(lon, lat)
# MAGIC
# MAGIC # Create GeoDataFrame
# MAGIC import geopandas as gpd
# MAGIC gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
# MAGIC
# MAGIC # Transform CRS
# MAGIC gdf_projected = gdf.to_crs('EPSG:3857')
# MAGIC
# MAGIC # Calculate distance
# MAGIC from geopy.distance import geodesic
# MAGIC distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
# MAGIC
# MAGIC # Create map
# MAGIC import folium
# MAGIC m = folium.Map(location=[lat, lon], zoom_start=10)
# MAGIC ```
