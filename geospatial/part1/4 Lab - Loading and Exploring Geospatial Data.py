# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Loading and Exploring Geospatial Data
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll apply what you've learned to load, validate, and explore real-world geospatial data. You'll work with multiple data formats, perform quality assessments, execute spatial queries, and create visualizations.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Load geospatial data from multiple formats (GeoJSON, CSV with coordinates, WKT)
# MAGIC - Validate and clean geospatial data
# MAGIC - Perform coordinate reference system transformations
# MAGIC - Execute spatial queries to answer business questions
# MAGIC - Create professional visualizations
# MAGIC - Handle common data quality issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Demos 1-3
# MAGIC - Understanding of points and polygons
# MAGIC - Familiarity with pandas and GeoPandas
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Libraries

# COMMAND ----------

# MAGIC %pip install --quiet geopandas 

# COMMAND ----------

# MAGIC %pip install shapely h3 keplergl folium geopy pyproj rtree

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
from shapely.wkt import loads as wkt_loads
from shapely.validation import make_valid
import folium
from keplergl import KeplerGl
import json
from geopy.distance import geodesic
import warnings
warnings.filterwarnings('ignore')

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Load Data from GeoJSON
# MAGIC
# MAGIC ### Task 1.1: Create Sample GeoJSON Data
# MAGIC First, let's create sample GeoJSON data representing city boundaries

# COMMAND ----------

# Sample GeoJSON data for major US cities
cities_geojson = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "name": "San Francisco",
                "state": "California",
                "population": 873965,
                "founded": 1776
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-122.52, 37.70], [-122.35, 37.70],
                    [-122.35, 37.83], [-122.52, 37.83],
                    [-122.52, 37.70]
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {
                "name": "Los Angeles",
                "state": "California",
                "population": 3979576,
                "founded": 1781
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-118.67, 33.70], [-118.15, 33.70],
                    [-118.15, 34.34], [-118.67, 34.34],
                    [-118.67, 33.70]
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {
                "name": "New York",
                "state": "New York",
                "population": 8336817,
                "founded": 1624
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-74.26, 40.50], [-73.70, 40.50],
                    [-73.70, 40.92], [-74.26, 40.92],
                    [-74.26, 40.50]
                ]]
            }
        }
    ]
}

# Load as GeoDataFrame
cities_gdf = gpd.GeoDataFrame.from_features(cities_geojson['features'], crs='EPSG:4326')

print("✓ Cities GeoDataFrame loaded from GeoJSON")
print(f"\nShape: {cities_gdf.shape}")
print(f"CRS: {cities_gdf.crs}")
print(f"Columns: {list(cities_gdf.columns)}")
display(cities_gdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Validate the Data
# MAGIC **TODO:** Check for data quality issues

# COMMAND ----------

# TODO: Complete the validation checks
print("Data Validation Report")
print("=" * 60)

# Check 1: Missing values
print(f"\n1. Missing Values:")
# YOUR CODE HERE: Check for null values in each column
missing_values = cities_gdf.isnull().sum()
print(missing_values)

# Check 2: Geometry validity
print(f"\n2. Geometry Validation:")
# YOUR CODE HERE: Check if all geometries are valid
valid_geoms = cities_gdf.geometry.is_valid.sum()
total_geoms = len(cities_gdf)
print(f"   Valid geometries: {valid_geoms}/{total_geoms}")

# Check 3: CRS
print(f"\n3. Coordinate Reference System:")
# YOUR CODE HERE: Print the CRS
print(f"   CRS: {cities_gdf.crs}")

# Check 4: Bounds
print(f"\n4. Geographic Bounds:")
# YOUR CODE HERE: Print the total bounds
bounds = cities_gdf.total_bounds
print(f"   Min Longitude: {bounds[0]:.4f}")
print(f"   Min Latitude: {bounds[1]:.4f}")
print(f"   Max Longitude: {bounds[2]:.4f}")
print(f"   Max Latitude: {bounds[3]:.4f}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Load Points from CSV
# MAGIC
# MAGIC ### Task 2.1: Create and Load Point Data

# COMMAND ----------

# Sample data: Points of interest (POIs)
pois_data = {
    'id': ['POI001', 'POI002', 'POI003', 'POI004', 'POI005', 'POI006', 'POI007', 'POI008'],
    'name': [
        'Golden Gate Bridge', 'Alcatraz Island', 'Griffith Observatory',
        'Santa Monica Pier', 'Statue of Liberty', 'Central Park',
        'Times Square', 'Brooklyn Bridge'
    ],
    'city': [
        'San Francisco', 'San Francisco', 'Los Angeles', 'Los Angeles',
        'New York', 'New York', 'New York', 'New York'
    ],
    'category': ['Landmark', 'Landmark', 'Attraction', 'Attraction', 'Landmark', 'Park', 'Attraction', 'Landmark'],
    'latitude': [37.8199, 37.8267, 34.1184, 34.0094, 40.6892, 40.7829, 40.7580, 40.7061],
    'longitude': [-122.4783, -122.4233, -118.3004, -118.4973, -74.0445, -73.9654, -73.9855, -73.9969],
    'visitors_per_year': [10000000, 1500000, 1000000, 8000000, 4500000, 42000000, 50000000, 4000000]
}

# Create DataFrame
pois_df = pd.DataFrame(pois_data)

# TODO: Convert to GeoDataFrame
# YOUR CODE HERE: Create Point geometries from latitude/longitude
geometry = [Point(xy) for xy in zip(pois_df['longitude'], pois_df['latitude'])]

# YOUR CODE HERE: Create GeoDataFrame
pois_gdf = gpd.GeoDataFrame(pois_df, geometry=geometry, crs='EPSG:4326')

print("✓ Points of Interest GeoDataFrame created")
print(f"\nShape: {pois_gdf.shape}")
display(pois_gdf.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Calculate Basic Statistics

# COMMAND ----------

# TODO: Calculate statistics for POIs
print("Points of Interest Statistics")
print("=" * 60)

# YOUR CODE HERE: Count POIs by city
print("\nPOIs by City:")
city_counts = pois_gdf['city'].value_counts()
print(city_counts)

# YOUR CODE HERE: Count POIs by category
print("\nPOIs by Category:")
category_counts = pois_gdf['category'].value_counts()
print(category_counts)

# YOUR CODE HERE: Calculate average visitors per city
print("\nAverage Annual Visitors by City:")
avg_visitors = pois_gdf.groupby('city')['visitors_per_year'].mean().sort_values(ascending=False)
print(avg_visitors.apply(lambda x: f"{x:,.0f}"))

# YOUR CODE HERE: Find most visited POI
most_visited = pois_gdf.loc[pois_gdf['visitors_per_year'].idxmax()]
print(f"\nMost Visited POI:")
print(f"  {most_visited['name']} ({most_visited['city']})")
print(f"  {most_visited['visitors_per_year']:,} visitors/year")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Spatial Queries
# MAGIC
# MAGIC ### Task 3.1: Find POIs Within Cities

# COMMAND ----------

# TODO: Perform spatial join to find which POIs are in which cities
# YOUR CODE HERE: Use gpd.sjoin to join pois with cities
pois_in_cities = gpd.sjoin(pois_gdf, cities_gdf, how='left', predicate='within')

print("Spatial Join Results")
print("=" * 60)

# Display results
print("\nPOIs matched to cities:")
display(pois_in_cities[['name', 'city', 'name_right', 'population']])

# Count POIs per city
print("\nPOIs per city boundary:")
city_poi_counts = pois_in_cities['name_right'].value_counts()
print(city_poi_counts)

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Calculate Distances Between POIs

# COMMAND ----------

# TODO: Calculate distance matrix between POIs in San Francisco
sf_pois = pois_gdf[pois_gdf['city'] == 'San Francisco']

print(f"San Francisco POIs: {len(sf_pois)}")
print()

if len(sf_pois) >= 2:
    # YOUR CODE HERE: Calculate distances between SF POIs
    poi1 = sf_pois.iloc[0]
    poi2 = sf_pois.iloc[1]
    
    coords1 = (poi1['latitude'], poi1['longitude'])
    coords2 = (poi2['latitude'], poi2['longitude'])
    
    distance_km = geodesic(coords1, coords2).kilometers
    distance_miles = geodesic(coords1, coords2).miles
    
    print(f"Distance between:")
    print(f"  {poi1['name']} and {poi2['name']}")
    print(f"  {distance_km:.2f} kilometers")
    print(f"  {distance_miles:.2f} miles")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Find Nearest POI to City Centroid

# COMMAND ----------

# TODO: For each city, find the POI closest to its centroid
print("Nearest POI to Each City Center")
print("=" * 60)

for idx, city in cities_gdf.iterrows():
    city_name = city['name']
    city_centroid = city.geometry.centroid
    centroid_coords = (city_centroid.y, city_centroid.x)
    
    # Get POIs in this city
    city_pois = pois_gdf[pois_gdf['city'] == city_name]
    
    if len(city_pois) > 0:
        # YOUR CODE HERE: Find nearest POI
        min_distance = float('inf')
        nearest_poi = None
        
        for poi_idx, poi in city_pois.iterrows():
            poi_coords = (poi['latitude'], poi['longitude'])
            distance = geodesic(centroid_coords, poi_coords).kilometers
            
            if distance < min_distance:
                min_distance = distance
                nearest_poi = poi
        
        print(f"\n{city_name}:")
        print(f"  Nearest POI: {nearest_poi['name']}")
        print(f"  Distance from center: {min_distance:.2f} km")
        print(f"  Category: {nearest_poi['category']}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Working with Well-Known Text (WKT)
# MAGIC
# MAGIC ### Task 4.1: Load Data from WKT Format

# COMMAND ----------

# Sample data with WKT geometries
districts_data = {
    'district_id': ['D001', 'D002', 'D003'],
    'district_name': ['Financial District', 'Mission District', 'Marina District'],
    'city': ['San Francisco', 'San Francisco', 'San Francisco'],
    'wkt_geometry': [
        'POLYGON ((-122.408 37.788, -122.391 37.788, -122.391 37.800, -122.408 37.800, -122.408 37.788))',
        'POLYGON ((-122.424 37.748, -122.408 37.748, -122.408 37.770, -122.424 37.770, -122.424 37.748))',
        'POLYGON ((-122.450 37.795, -122.430 37.795, -122.430 37.810, -122.450 37.810, -122.450 37.795))'
    ]
}

districts_df = pd.DataFrame(districts_data)

# TODO: Parse WKT strings and create GeoDataFrame
# YOUR CODE HERE: Convert WKT strings to geometries
geometry = [wkt_loads(wkt) for wkt in districts_df['wkt_geometry']]

# YOUR CODE HERE: Create GeoDataFrame
districts_gdf = gpd.GeoDataFrame(
    districts_df.drop(columns=['wkt_geometry']),
    geometry=geometry,
    crs='EPSG:4326'
)

print("✓ Districts GeoDataFrame created from WKT")
display(districts_gdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Calculate District Metrics

# COMMAND ----------

# TODO: Calculate area and perimeter for each district
# YOUR CODE HERE: Transform to projected CRS for accurate measurements
districts_proj = districts_gdf.to_crs('EPSG:3857')

# YOUR CODE HERE: Calculate metrics
districts_proj['area_sqkm'] = districts_proj.geometry.area / 1_000_000
districts_proj['perimeter_km'] = districts_proj.geometry.length / 1_000

# Transform back to WGS84
districts_with_metrics = districts_proj.to_crs('EPSG:4326')

print("District Metrics:")
display(districts_with_metrics[['district_name', 'area_sqkm', 'perimeter_km']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Data Quality and Cleaning
# MAGIC
# MAGIC ### Task 5.1: Handle Invalid Geometries

# COMMAND ----------

# Create a dataset with potential issues
problematic_data = {
    'id': ['G001', 'G002', 'G003', 'G004'],
    'name': ['Valid Point', 'Valid Polygon', 'Self-Intersecting', 'Valid Point 2'],
    'geometry': [
        Point(-122.4, 37.8),
        Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8), (-122.5, 37.7)]),
        Polygon([(-122.5, 37.7), (-122.4, 37.8), (-122.4, 37.7), (-122.5, 37.8), (-122.5, 37.7)]),  # Self-intersecting
        Point(-122.3, 37.7)
    ]
}

problematic_gdf = gpd.GeoDataFrame(problematic_data, geometry='geometry', crs='EPSG:4326')

# TODO: Identify and fix invalid geometries
print("Geometry Validation and Repair")
print("=" * 60)

# YOUR CODE HERE: Check which geometries are invalid
print("\nInitial validation:")
for idx, row in problematic_gdf.iterrows():
    validity = "✓ Valid" if row.geometry.is_valid else "✗ Invalid"
    print(f"  {row['name']}: {validity}")

# YOUR CODE HERE: Fix invalid geometries
print("\nFixing invalid geometries...")
problematic_gdf['geometry'] = problematic_gdf.geometry.apply(
    lambda geom: make_valid(geom) if not geom.is_valid else geom
)

# YOUR CODE HERE: Verify all are now valid
print("\nAfter repair:")
for idx, row in problematic_gdf.iterrows():
    validity = "✓ Valid" if row.geometry.is_valid else "✗ Invalid"
    print(f"  {row['name']}: {validity}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Handle Missing Coordinates

# COMMAND ----------

# Create dataset with missing values
incomplete_data = {
    'location_id': ['L001', 'L002', 'L003', 'L004', 'L005'],
    'name': ['Complete Location', 'Missing Lat', 'Missing Lon', 'Both Missing', 'Complete Location 2'],
    'latitude': [37.7749, None, 37.8, None, 40.7],
    'longitude': [-122.4194, -122.3, None, None, -74.0]
}

incomplete_df = pd.DataFrame(incomplete_data)

# TODO: Handle missing coordinates
print("Handling Missing Coordinates")
print("=" * 60)

# YOUR CODE HERE: Identify rows with missing coordinates
print("\nOriginal data:")
print(f"  Total rows: {len(incomplete_df)}")
print(f"  Missing latitude: {incomplete_df['latitude'].isna().sum()}")
print(f"  Missing longitude: {incomplete_df['longitude'].isna().sum()}")

# YOUR CODE HERE: Filter to complete records
complete_df = incomplete_df.dropna(subset=['latitude', 'longitude'])

print(f"\nAfter removing incomplete records:")
print(f"  Remaining rows: {len(complete_df)}")
print(f"  Removed rows: {len(incomplete_df) - len(complete_df)}")

# YOUR CODE HERE: Create GeoDataFrame from complete records
geometry = [Point(xy) for xy in zip(complete_df['longitude'], complete_df['latitude'])]
complete_gdf = gpd.GeoDataFrame(complete_df, geometry=geometry, crs='EPSG:4326')

print(f"\n✓ GeoDataFrame created with {len(complete_gdf)} valid locations")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Visualization
# MAGIC
# MAGIC ### Task 6.1: Create Multi-Layer Map

# COMMAND ----------

# TODO: Create a comprehensive map showing cities, districts, and POIs

# YOUR CODE HERE: Create base map
m = folium.Map(
    location=[37.77, -122.42],
    zoom_start=10,
    tiles='CartoDB positron'
)

# YOUR CODE HERE: Add city boundaries
for idx, city in cities_gdf.iterrows():
    if city['name'] == 'San Francisco':  # Focus on SF
        folium.GeoJson(
            city.geometry.__geo_interface__,
            style_function=lambda x: {
                'fillColor': 'lightblue',
                'color': 'blue',
                'weight': 2,
                'fillOpacity': 0.2
            },
            tooltip=f"{city['name']}<br>Population: {city['population']:,}"
        ).add_to(m)

# YOUR CODE HERE: Add districts
colors = ['red', 'green', 'orange']
for idx, (i, district) in enumerate(districts_gdf.iterrows()):
    folium.GeoJson(
        district.geometry.__geo_interface__,
        style_function=lambda x, color=colors[idx]: {
            'fillColor': color,
            'color': color,
            'weight': 2,
            'fillOpacity': 0.3
        },
        tooltip=district['district_name']
    ).add_to(m)

# YOUR CODE HERE: Add POIs
sf_pois_only = pois_gdf[pois_gdf['city'] == 'San Francisco']
for idx, poi in sf_pois_only.iterrows():
    icon_map = {
        'Landmark': 'star',
        'Attraction': 'camera',
        'Park': 'tree'
    }
    
    folium.Marker(
        location=[poi['latitude'], poi['longitude']],
        popup=f"<b>{poi['name']}</b><br>Category: {poi['category']}<br>Visitors: {poi['visitors_per_year']:,}/year",
        tooltip=poi['name'],
        icon=folium.Icon(color='red', icon=icon_map.get(poi['category'], 'info-sign'))
    ).add_to(m)

# Add legend
legend_html = '''
<div style="position: fixed; 
     top: 10px; right: 10px; width: 220px; 
     background-color: white; border:2px solid grey; z-index:9999; 
     font-size:12px; padding: 10px">
     <p><b>Map Legend</b></p>
     <p><span style="color:blue">▭</span> City Boundary</p>
     <p><span style="color:red">▭</span> Financial District</p>
     <p><span style="color:green">▭</span> Mission District</p>
     <p><span style="color:orange">▭</span> Marina District</p>
     <p><i class="fa fa-map-marker" style="color:red"></i> Points of Interest</p>
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2: Create Interactive Kepler.gl Visualization

# COMMAND ----------

# TODO: Create Kepler.gl map with POIs
# YOUR CODE HERE: Prepare data
kepler_data = pois_gdf.copy()
kepler_data['lon'] = kepler_data.geometry.x
kepler_data['lat'] = kepler_data.geometry.y

# YOUR CODE HERE: Create and display map
map_kepler = KeplerGl(height=600)
map_kepler.add_data(data=kepler_data, name='Points of Interest')

# Optional: Add cities as polygons
cities_for_kepler = cities_gdf.copy()
map_kepler.add_data(data=cities_for_kepler, name='Cities')

map_kepler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Advanced Analysis
# MAGIC
# MAGIC ### Task 7.1: Calculate Tourism Density

# COMMAND ----------

# TODO: Calculate visitor density (visitors per square kilometer) for each city

print("Tourism Density Analysis")
print("=" * 60)

# Transform to projected CRS
cities_proj = cities_gdf.to_crs('EPSG:3857')

for idx, city in cities_proj.iterrows():
    city_name = city['name']
    
    # Get POIs in this city
    city_pois = pois_in_cities[pois_in_cities['name_right'] == city_name]
    
    # Calculate total visitors
    total_visitors = city_pois['visitors_per_year'].sum()
    
    # Calculate area
    area_sqkm = city.geometry.area / 1_000_000
    
    # Calculate density
    if area_sqkm > 0:
        density = total_visitors / area_sqkm
        
        print(f"\n{city_name}:")
        print(f"  Area: {area_sqkm:.2f} sq km")
        print(f"  Total POI Visitors: {total_visitors:,.0f}/year")
        print(f"  Tourism Density: {density:,.0f} visitors/sq km/year")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7.2: Find Optimal New Location
# MAGIC Find the best location for a new attraction (maximize distance from existing POIs)

# COMMAND ----------

# TODO: Generate candidate points and find the one farthest from existing POIs

print("Optimal New Location Analysis")
print("=" * 60)

# Focus on San Francisco
sf_city = cities_gdf[cities_gdf['name'] == 'San Francisco'].iloc[0]
sf_bounds = sf_city.geometry.bounds  # min_lon, min_lat, max_lon, max_lat

# Generate candidate points within SF
import random
random.seed(42)

num_candidates = 20
candidates = []

for i in range(num_candidates):
    lon = random.uniform(sf_bounds[0], sf_bounds[2])
    lat = random.uniform(sf_bounds[1], sf_bounds[3])
    point = Point(lon, lat)
    
    # Check if point is within city boundary
    if sf_city.geometry.contains(point):
        candidates.append(point)

print(f"\nGenerated {len(candidates)} candidate locations within San Francisco")

# For each candidate, find minimum distance to existing POIs
sf_pois_only = pois_gdf[pois_gdf['city'] == 'San Francisco']

best_location = None
max_min_distance = 0

for candidate in candidates:
    candidate_coords = (candidate.y, candidate.x)
    
    # Find minimum distance to any existing POI
    min_dist = float('inf')
    for idx, poi in sf_pois_only.iterrows():
        poi_coords = (poi['latitude'], poi['longitude'])
        dist = geodesic(candidate_coords, poi_coords).kilometers
        min_dist = min(min_dist, dist)
    
    # Track candidate with maximum minimum distance
    if min_dist > max_min_distance:
        max_min_distance = min_dist
        best_location = candidate

print(f"\nOptimal Location Found:")
print(f"  Coordinates: ({best_location.y:.6f}, {best_location.x:.6f})")
print(f"  Distance to nearest POI: {max_min_distance:.2f} km")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Summary Report
# MAGIC
# MAGIC ### Task 8.1: Generate Comprehensive Report

# COMMAND ----------

# TODO: Create a comprehensive summary report

print("=" * 70)
print(" " * 20 + "GEOSPATIAL DATA ANALYSIS REPORT")
print("=" * 70)

print("\n📊 DATA SUMMARY")
print("-" * 70)
print(f"  Cities loaded: {len(cities_gdf)}")
print(f"  Points of Interest: {len(pois_gdf)}")
print(f"  Districts: {len(districts_gdf)}")

print("\n🏙️  CITIES")
print("-" * 70)
for idx, city in cities_gdf.iterrows():
    print(f"  {city['name']}, {city['state']}")
    print(f"    Population: {city['population']:,}")
    print(f"    POIs: {len(pois_gdf[pois_gdf['city'] == city['name']])}")

print("\n📍 POINTS OF INTEREST")
print("-" * 70)
print(f"  Total POIs: {len(pois_gdf)}")
print(f"  Categories: {', '.join(pois_gdf['category'].unique())}")
print(f"  Total annual visitors: {pois_gdf['visitors_per_year'].sum():,}")
print(f"\n  Top 3 Most Visited:")
top_3 = pois_gdf.nlargest(3, 'visitors_per_year')
for idx, poi in top_3.iterrows():
    print(f"    {idx+1}. {poi['name']} - {poi['visitors_per_year']:,} visitors")

print("\n🗺️  SPATIAL ANALYSIS")
print("-" * 70)
print(f"  Total geographic extent:")
bounds = pois_gdf.total_bounds
print(f"    Longitude: {bounds[0]:.4f}° to {bounds[2]:.4f}°")
print(f"    Latitude: {bounds[1]:.4f}° to {bounds[3]:.4f}°")

print("\n✅ DATA QUALITY")
print("-" * 70)
print(f"  Cities: {cities_gdf.geometry.is_valid.sum()}/{len(cities_gdf)} valid geometries")
print(f"  POIs: {pois_gdf.geometry.is_valid.sum()}/{len(pois_gdf)} valid geometries")
print(f"  Districts: {districts_gdf.geometry.is_valid.sum()}/{len(districts_gdf)} valid geometries")
print(f"  Missing values: {cities_gdf.isnull().sum().sum() + pois_gdf.isnull().sum().sum()}")

print("\n" + "=" * 70)
print(" " * 25 + "END OF REPORT")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Complete! 🎉
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✓ **Data Loading**
# MAGIC - Loaded data from GeoJSON format
# MAGIC - Created geometries from CSV with coordinates
# MAGIC - Parsed Well-Known Text (WKT) geometries
# MAGIC
# MAGIC ✓ **Data Validation**
# MAGIC - Checked geometry validity
# MAGIC - Handled missing coordinates
# MAGIC - Fixed invalid geometries
# MAGIC - Validated coordinate ranges
# MAGIC
# MAGIC ✓ **Spatial Queries**
# MAGIC - Performed point-in-polygon tests
# MAGIC - Executed spatial joins
# MAGIC - Calculated distances between features
# MAGIC - Found nearest neighbors
# MAGIC
# MAGIC ✓ **Analysis**
# MAGIC - Calculated area and perimeter metrics
# MAGIC - Analyzed tourism density
# MAGIC - Identified optimal locations
# MAGIC - Generated summary statistics
# MAGIC
# MAGIC ✓ **Visualization**
# MAGIC - Created multi-layer Folium maps
# MAGIC - Built interactive Kepler.gl visualizations
# MAGIC - Added custom legends and tooltips
# MAGIC
# MAGIC ### Key Skills Developed
# MAGIC - Loading multiple geospatial formats
# MAGIC - Data quality assessment and cleaning
# MAGIC - Spatial relationship queries
# MAGIC - CRS transformations
# MAGIC - Professional visualization creation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Lab
# MAGIC
# MAGIC **Lab 5: Building Your First Spatial Application**
# MAGIC
# MAGIC You'll build a complete store locator application with:
# MAGIC - Distance-based search
# MAGIC - Service area analysis
# MAGIC - Interactive dashboard
# MAGIC - Performance optimization
# MAGIC
# MAGIC ### Bonus Challenges (Optional)
# MAGIC
# MAGIC If you want more practice, try these:
# MAGIC
# MAGIC 1. **Challenge 1**: Add more POIs and calculate which district has the highest visitor density
# MAGIC
# MAGIC 2. **Challenge 2**: Create a function that returns all POIs within X kilometers of a given point
# MAGIC
# MAGIC 3. **Challenge 3**: Generate a heat map showing POI concentration
# MAGIC
# MAGIC 4. **Challenge 4**: Find pairs of POIs that are within 5km of each other
# MAGIC
# MAGIC 5. **Challenge 5**: Calculate the centroid of all Landmarks and find the closest attraction to it
