# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Polygons and Spatial Relationships
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores polygon geometries and spatial relationships between geometries. You'll learn to create polygons, perform spatial operations (intersection, union, difference), and use spatial predicates to answer complex geospatial questions.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Create Polygon and MultiPolygon geometries
# MAGIC - Perform point-in-polygon tests
# MAGIC - Calculate polygon intersections, unions, and differences
# MAGIC - Use spatial predicates (contains, intersects, within, touches, crosses)
# MAGIC - Handle polygon holes and complex shapes
# MAGIC - Calculate polygon areas and perimeters
# MAGIC - Simplify complex polygons for performance
# MAGIC - Visualize polygon relationships
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Demos 1-2
# MAGIC - Understanding of point geometries
# MAGIC - GeoPandas, Shapely installed
# MAGIC
# MAGIC ## Duration
# MAGIC 35-40 minutes

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
from shapely.geometry import Point, Polygon, MultiPolygon, LineString
from shapely.ops import unary_union
import folium
from keplergl import KeplerGl
import numpy as np

print("✓ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Creating Polygon Geometries
# MAGIC
# MAGIC ### Method 1: Simple Polygon from Coordinates

# COMMAND ----------

# Create a simple polygon - a square in San Francisco
# Coordinates: list of (longitude, latitude) tuples
# First and last coordinates must be the same (closed polygon)

square_coords = [
    (-122.45, 37.75),  # SW corner
    (-122.40, 37.75),  # SE corner
    (-122.40, 37.80),  # NE corner
    (-122.45, 37.80),  # NW corner
    (-122.45, 37.75)   # Close the polygon (same as first point)
]

square = Polygon(square_coords)

print("Simple Polygon Created:")
print(f"  Geometry type: {square.geom_type}")
print(f"  Is valid: {square.is_valid}")
print(f"  Is simple: {square.is_simple}")
print(f"  Number of points: {len(square.exterior.coords)}")
print(f"  Bounds: {square.bounds}")
print(f"  Area (square degrees): {square.area:.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Polygon with Holes

# COMMAND ----------

# Create a polygon with a hole (like a donut)
# Outer boundary
outer = [
    (-122.45, 37.75),
    (-122.40, 37.75),
    (-122.40, 37.80),
    (-122.45, 37.80),
    (-122.45, 37.75)
]

# Inner hole
hole = [
    (-122.44, 37.76),
    (-122.41, 37.76),
    (-122.41, 37.79),
    (-122.44, 37.79),
    (-122.44, 37.76)
]

# Create polygon with hole
polygon_with_hole = Polygon(outer, [hole])

print("Polygon with Hole Created:")
print(f"  Area with hole: {polygon_with_hole.area:.6f} square degrees")
print(f"  Area without hole: {Polygon(outer).area:.6f} square degrees")
print(f"  Hole area: {Polygon(hole).area:.6f} square degrees")
print(f"  Number of interior rings: {len(polygon_with_hole.interiors)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 3: Creating Polygons from Real-World Regions

# COMMAND ----------

# Create delivery zones for a logistics company in San Francisco

zones_data = {
    'zone_name': ['Mission District', 'Financial District', 'Marina District'],
    'zone_id': ['Z001', 'Z002', 'Z003'],
    'delivery_fee': [5.99, 7.99, 6.99]
}

# Define polygon coordinates for each zone
mission_coords = [
    (-122.424, 37.748), (-122.408, 37.748), (-122.408, 37.770),
    (-122.424, 37.770), (-122.424, 37.748)
]

financial_coords = [
    (-122.408, 37.788), (-122.391, 37.788), (-122.391, 37.800),
    (-122.408, 37.800), (-122.408, 37.788)
]

marina_coords = [
    (-122.450, 37.795), (-122.430, 37.795), (-122.430, 37.810),
    (-122.450, 37.810), (-122.450, 37.795)
]

# Create geometries
geometries = [
    Polygon(mission_coords),
    Polygon(financial_coords),
    Polygon(marina_coords)
]

# Create GeoDataFrame
zones_gdf = gpd.GeoDataFrame(
    zones_data,
    geometry=geometries,
    crs='EPSG:4326'
)

print("Delivery Zones GeoDataFrame:")
display(zones_gdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Point-in-Polygon Tests
# MAGIC
# MAGIC Determine if points fall within polygons - crucial for location-based queries

# COMMAND ----------

# Sample customer locations
customers = pd.DataFrame({
    'customer_id': ['C001', 'C002', 'C003', 'C004', 'C005'],
    'address': [
        '123 Mission St',
        '456 Market St',
        '789 Lombard St',
        '321 Valencia St',
        '654 Montgomery St'
    ],
    'longitude': [-122.418, -122.398, -122.437, -122.421, -122.402],
    'latitude': [37.755, 37.793, 37.802, 37.762, 37.794]
})

# Convert to GeoDataFrame
customer_geometry = [Point(xy) for xy in zip(customers['longitude'], customers['latitude'])]
customers_gdf = gpd.GeoDataFrame(
    customers,
    geometry=customer_geometry,
    crs='EPSG:4326'
)

print("Customer Locations:")
display(customers_gdf)

# COMMAND ----------

# Method 1: Manual point-in-polygon test
print("Manual Point-in-Polygon Tests:\n")

for idx, customer in customers_gdf.iterrows():
    customer_point = customer.geometry
    print(f"{customer['customer_id']} ({customer['address']}):")
    
    for zone_idx, zone in zones_gdf.iterrows():
        if zone.geometry.contains(customer_point):
            print(f"  ✓ Inside {zone['zone_name']} - Delivery Fee: ${zone['delivery_fee']}")
            break
    else:
        print(f"  ✗ Outside all delivery zones")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spatial Join: Automated Point-in-Polygon

# COMMAND ----------

# Perform spatial join to assign zones to customers
customers_with_zones = gpd.sjoin(
    customers_gdf,
    zones_gdf,
    how='left',
    predicate='within'
)

print("Customers with Assigned Zones:")
display(customers_with_zones[['customer_id', 'address', 'zone_name', 'delivery_fee']])

# Count customers per zone
print("\nCustomers per Zone:")
zone_counts = customers_with_zones['zone_name'].value_counts()
print(zone_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Spatial Predicates
# MAGIC
# MAGIC Spatial predicates test relationships between geometries

# COMMAND ----------

# Create test geometries
poly1 = Polygon([(-122.42, 37.76), (-122.40, 37.76), (-122.40, 37.78), (-122.42, 37.78), (-122.42, 37.76)])
poly2 = Polygon([(-122.41, 37.77), (-122.39, 37.77), (-122.39, 37.79), (-122.41, 37.79), (-122.41, 37.77)])
poly3 = Polygon([(-122.44, 37.74), (-122.42, 37.74), (-122.42, 37.76), (-122.44, 37.76), (-122.44, 37.74)])
test_point = Point(-122.41, 37.77)

print("Spatial Predicate Tests:\n")

# Test: contains
print(f"1. CONTAINS")
print(f"   poly1.contains(test_point): {poly1.contains(test_point)}")
print(f"   poly2.contains(test_point): {poly2.contains(test_point)}\n")

# Test: intersects
print(f"2. INTERSECTS (overlaps or touches)")
print(f"   poly1.intersects(poly2): {poly1.intersects(poly2)}")
print(f"   poly1.intersects(poly3): {poly1.intersects(poly3)}\n")

# Test: within
print(f"3. WITHIN (inverse of contains)")
print(f"   test_point.within(poly1): {test_point.within(poly1)}")
print(f"   test_point.within(poly2): {test_point.within(poly2)}\n")

# Test: touches
print(f"4. TOUCHES (shares boundary but not interior)")
print(f"   poly1.touches(poly3): {poly1.touches(poly3)}")
print(f"   poly2.touches(poly3): {poly2.touches(poly3)}\n")

# Test: disjoint
print(f"5. DISJOINT (no spatial relationship)")
print(f"   poly1.disjoint(poly2): {poly1.disjoint(poly2)}")
print(f"   poly1.disjoint(poly3): {poly1.disjoint(poly3)}\n")

# Test: overlaps
print(f"6. OVERLAPS (partial overlap)")
print(f"   poly1.overlaps(poly2): {poly1.overlaps(poly2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Spatial Relationships

# COMMAND ----------

# Create visualization of spatial predicates
m = folium.Map(location=[37.77, -122.41], zoom_start=14)

# Add polygons
folium.GeoJson(
    poly1.__geo_interface__,
    style_function=lambda x: {'fillColor': 'blue', 'color': 'darkblue', 'weight': 2, 'fillOpacity': 0.3},
    tooltip='Polygon 1'
).add_to(m)

folium.GeoJson(
    poly2.__geo_interface__,
    style_function=lambda x: {'fillColor': 'red', 'color': 'darkred', 'weight': 2, 'fillOpacity': 0.3},
    tooltip='Polygon 2 (intersects Poly 1)'
).add_to(m)

folium.GeoJson(
    poly3.__geo_interface__,
    style_function=lambda x: {'fillColor': 'green', 'color': 'darkgreen', 'weight': 2, 'fillOpacity': 0.3},
    tooltip='Polygon 3 (touches Poly 1)'
).add_to(m)

# Add test point
folium.Marker(
    location=[test_point.y, test_point.x],
    popup='Test Point',
    icon=folium.Icon(color='purple', icon='star')
).add_to(m)

displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Geometric Operations on Polygons
# MAGIC
# MAGIC ### Intersection

# COMMAND ----------

# Calculate intersection of two polygons
intersection = poly1.intersection(poly2)

print(f"Intersection:")
print(f"  Geometry type: {intersection.geom_type}")
print(f"  Area: {intersection.area:.8f} square degrees")
print(f"  Polygon 1 area: {poly1.area:.8f} square degrees")
print(f"  Polygon 2 area: {poly2.area:.8f} square degrees")
print(f"  Overlap percentage: {(intersection.area / poly1.area * 100):.2f}% of Poly1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union

# COMMAND ----------

# Calculate union of two polygons
union = poly1.union(poly2)

print(f"Union:")
print(f"  Geometry type: {union.geom_type}")
print(f"  Combined area: {union.area:.8f} square degrees")
print(f"  Sum of areas: {(poly1.area + poly2.area):.8f} square degrees")
print(f"  Overlap removed: {(poly1.area + poly2.area - union.area):.8f} square degrees")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Difference

# COMMAND ----------

# Calculate difference (poly1 minus poly2)
difference = poly1.difference(poly2)

print(f"Difference (Poly1 - Poly2):")
print(f"  Geometry type: {difference.geom_type}")
print(f"  Remaining area: {difference.area:.8f} square degrees")
print(f"  Area removed: {(poly1.area - difference.area):.8f} square degrees")
print(f"  Percentage remaining: {(difference.area / poly1.area * 100):.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Symmetric Difference

# COMMAND ----------

# Calculate symmetric difference (areas in either but not both)
sym_diff = poly1.symmetric_difference(poly2)

print(f"Symmetric Difference:")
print(f"  Geometry type: {sym_diff.geom_type}")
print(f"  Area: {sym_diff.area:.8f} square degrees")
print(f"  This equals: (Poly1 + Poly2) - (Poly1 ∩ Poly2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Geometric Operations

# COMMAND ----------

# Create multi-map visualization
m2 = folium.Map(location=[37.77, -122.41], zoom_start=14)

# Original polygons (semi-transparent)
folium.GeoJson(
    poly1.__geo_interface__,
    style_function=lambda x: {'fillColor': 'blue', 'color': 'blue', 'weight': 1, 'fillOpacity': 0.2},
    tooltip='Original Poly 1'
).add_to(m2)

folium.GeoJson(
    poly2.__geo_interface__,
    style_function=lambda x: {'fillColor': 'red', 'color': 'red', 'weight': 1, 'fillOpacity': 0.2},
    tooltip='Original Poly 2'
).add_to(m2)

# Intersection (highlighted)
if not intersection.is_empty:
    folium.GeoJson(
        intersection.__geo_interface__,
        style_function=lambda x: {'fillColor': 'purple', 'color': 'purple', 'weight': 3, 'fillOpacity': 0.6},
        tooltip=f'Intersection (Area: {intersection.area:.8f})'
    ).add_to(m2)

displayHTML(m2._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Working with Delivery Zones
# MAGIC
# MAGIC ### Calculate Zone Metrics

# COMMAND ----------

# Transform to projected CRS for accurate area calculations
zones_projected = zones_gdf.to_crs('EPSG:3857')

# Calculate areas in square kilometers
zones_projected['area_sqkm'] = zones_projected.geometry.area / 1_000_000

# Calculate perimeters in kilometers
zones_projected['perimeter_km'] = zones_projected.geometry.length / 1_000

# Transform back for display
zones_with_metrics = zones_projected.to_crs('EPSG:4326')

print("Zone Metrics:")
display(zones_with_metrics[['zone_name', 'zone_id', 'delivery_fee', 'area_sqkm', 'perimeter_km']])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Zone Overlaps

# COMMAND ----------

# Check if any zones overlap
overlaps = []

for i in range(len(zones_gdf)):
    for j in range(i + 1, len(zones_gdf)):
        zone1 = zones_gdf.iloc[i]
        zone2 = zones_gdf.iloc[j]
        
        if zone1.geometry.intersects(zone2.geometry):
            intersection_area = zone1.geometry.intersection(zone2.geometry).area
            overlaps.append({
                'zone1': zone1['zone_name'],
                'zone2': zone2['zone_name'],
                'overlap_area_deg': intersection_area
            })

if overlaps:
    print("⚠️ Zone overlaps detected:")
    display(pd.DataFrame(overlaps))
else:
    print("✓ No zone overlaps - zones are properly defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge All Zones into Service Area

# COMMAND ----------

# Create unified service area
total_service_area = unary_union(zones_gdf.geometry)

print(f"Total Service Area:")
print(f"  Geometry type: {total_service_area.geom_type}")
print(f"  Total area: {total_service_area.area:.6f} square degrees")
print(f"  Sum of individual zones: {zones_gdf.geometry.area.sum():.6f} square degrees")

# Calculate in square kilometers
total_service_area_proj = zones_gdf.to_crs('EPSG:3857').geometry.union_all()
print(f"  Total area: {total_service_area_proj.area / 1_000_000:.2f} square kilometers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced Polygon Operations
# MAGIC
# MAGIC ### Buffer Polygons

# COMMAND ----------

# Create buffer around zones (expand service areas)
zones_proj = zones_gdf.to_crs('EPSG:3857')

# Add 500-meter buffer
buffer_distance = 500  # meters
zones_proj['buffered'] = zones_proj.geometry.buffer(buffer_distance)

# Create GeoDataFrame for buffered zones
buffered_zones = gpd.GeoDataFrame(
    zones_proj[['zone_name', 'zone_id', 'delivery_fee']],
    geometry=zones_proj['buffered'],
    crs='EPSG:3857'
).to_crs('EPSG:4326')

print("Buffered Zones Created:")
print(f"  Buffer distance: {buffer_distance} meters")
print(f"  Original total area: {zones_proj.geometry.area.sum() / 1_000_000:.2f} sq km")
print(f"  Buffered total area: {zones_proj['buffered'].area.sum() / 1_000_000:.2f} sq km")
print(f"  Area increase: {((zones_proj['buffered'].area.sum() - zones_proj.geometry.area.sum()) / zones_proj.geometry.area.sum() * 100):.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simplify Polygons

# COMMAND ----------

# Create a complex polygon by buffering and simplifying
complex_poly = zones_gdf.iloc[0].geometry.buffer(0.005)

# Simplify with different tolerances
simplified_01 = complex_poly.simplify(0.001, preserve_topology=True)
simplified_02 = complex_poly.simplify(0.002, preserve_topology=True)

print("Polygon Simplification:")
print(f"  Original vertices: {len(complex_poly.exterior.coords)}")
print(f"  Simplified (0.001): {len(simplified_01.exterior.coords)} vertices")
print(f"  Simplified (0.002): {len(simplified_02.exterior.coords)} vertices")
print(f"\n  Original area: {complex_poly.area:.8f}")
print(f"  Simplified (0.001) area: {simplified_01.area:.8f}")
print(f"  Simplified (0.002) area: {simplified_02.area:.8f}")
print(f"\n  Area loss (0.001): {((complex_poly.area - simplified_01.area) / complex_poly.area * 100):.2f}%")
print(f"  Area loss (0.002): {((complex_poly.area - simplified_02.area) / complex_poly.area * 100):.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convex Hull

# COMMAND ----------

# Create convex hull (smallest convex polygon containing all points)
mission_zone = zones_gdf.iloc[0].geometry
convex_hull = mission_zone.convex_hull

print(f"Convex Hull:")
print(f"  Original area: {mission_zone.area:.8f} square degrees")
print(f"  Convex hull area: {convex_hull.area:.8f} square degrees")
print(f"  Area increase: {((convex_hull.area - mission_zone.area) / mission_zone.area * 100):.2f}%")
print(f"  Original vertices: {len(mission_zone.exterior.coords)}")
print(f"  Convex hull vertices: {len(convex_hull.exterior.coords)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: MultiPolygon Geometries

# COMMAND ----------

# Create a MultiPolygon (collection of disconnected polygons)
# Example: A delivery company with service in multiple disconnected areas

area1 = Polygon([(-122.45, 37.75), (-122.42, 37.75), (-122.42, 37.77), (-122.45, 37.77), (-122.45, 37.75)])
area2 = Polygon([(-122.40, 37.79), (-122.37, 37.79), (-122.37, 37.81), (-122.40, 37.81), (-122.40, 37.79)])

multi_zone = MultiPolygon([area1, area2])

print(f"MultiPolygon Created:")
print(f"  Geometry type: {multi_zone.geom_type}")
print(f"  Number of polygons: {len(multi_zone.geoms)}")
print(f"  Total area: {multi_zone.area:.8f} square degrees")
print(f"  Is valid: {multi_zone.is_valid}")

# Access individual polygons
print(f"\n  Polygon 1 area: {multi_zone.geoms[0].area:.8f}")
print(f"  Polygon 2 area: {multi_zone.geoms[1].area:.8f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Complete Visualization

# COMMAND ----------

# Create comprehensive map showing all zones and customers
m3 = folium.Map(
    location=[37.78, -122.42],
    zoom_start=12,
    tiles='CartoDB positron'
)

# Add delivery zones
colors = ['blue', 'red', 'green']
for idx, (_, zone) in enumerate(zones_gdf.iterrows()):
    folium.GeoJson(
        zone.geometry.__geo_interface__,
        style_function=lambda x, color=colors[idx]: {
            'fillColor': color,
            'color': color,
            'weight': 2,
            'fillOpacity': 0.3
        },
        tooltip=f"{zone['zone_name']}<br>Fee: ${zone['delivery_fee']}"
    ).add_to(m3)

# Add zone labels (centroids)
for idx, zone in zones_gdf.iterrows():
    centroid = zone.geometry.centroid
    folium.Marker(
        location=[centroid.y, centroid.x],
        icon=folium.DivIcon(html=f'''
            <div style="font-size: 12pt; color: black; font-weight: bold;">
                {zone['zone_name']}
            </div>
        ''')
    ).add_to(m3)

# Add customers
for idx, customer in customers_gdf.iterrows():
    # Determine which zone (if any)
    customer_zone = customers_with_zones[customers_with_zones['customer_id'] == customer['customer_id']]
    
    if not customer_zone.empty and pd.notna(customer_zone.iloc[0]['zone_name']):
        icon_color = 'green'
        popup_text = f"<b>{customer['address']}</b><br>Zone: {customer_zone.iloc[0]['zone_name']}<br>Fee: ${customer_zone.iloc[0]['delivery_fee']}"
    else:
        icon_color = 'red'
        popup_text = f"<b>{customer['address']}</b><br>⚠️ Outside service area"
    
    folium.Marker(
        location=[customer['latitude'], customer['longitude']],
        popup=popup_text,
        tooltip=customer['customer_id'],
        icon=folium.Icon(color=icon_color, icon='home')
    ).add_to(m3)

displayHTML(m3._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Performance Optimization

# COMMAND ----------

# Create spatial index for faster queries
print("Creating spatial index...")
spatial_index = zones_gdf.sindex

print(f"✓ Spatial index created")
print(f"  Type: {type(spatial_index)}")
print(f"  Indexed geometries: {len(zones_gdf)}")

# Use spatial index for point-in-polygon queries
test_point = Point(-122.418, 37.755)

# Method 1: Without spatial index (checks all polygons)
import time

start = time.time()
for _ in range(1000):
    for idx, zone in zones_gdf.iterrows():
        if zone.geometry.contains(test_point):
            break
time_without_index = time.time() - start

# Method 2: With spatial index (checks only candidates)
start = time.time()
for _ in range(1000):
    possible_matches_idx = list(spatial_index.intersection(test_point.bounds))
    for idx in possible_matches_idx:
        if zones_gdf.iloc[idx].geometry.contains(test_point):
            break
time_with_index = time.time() - start

print(f"\nPerformance Comparison (1000 queries):")
print(f"  Without spatial index: {time_without_index:.4f} seconds")
print(f"  With spatial index: {time_with_index:.4f} seconds")
print(f"  Speedup: {time_without_index / time_with_index:.2f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("POLYGON OPERATIONS SUMMARY")
print("=" * 60)

print(f"\n📐 Delivery Zones:")
print(f"  Total zones: {len(zones_gdf)}")
print(f"  Total coverage: {total_service_area_proj.area / 1_000_000:.2f} square kilometers")

print(f"\n👥 Customers:")
print(f"  Total customers: {len(customers_gdf)}")
print(f"  Customers in zones: {customers_with_zones['zone_name'].notna().sum()}")
print(f"  Customers outside zones: {customers_with_zones['zone_name'].isna().sum()}")

print(f"\n💰 Revenue Potential:")
zone_revenue = customers_with_zones.groupby('zone_name')['delivery_fee'].agg(['count', 'sum'])
zone_revenue.columns = ['orders', 'revenue']
print(zone_revenue.to_string())

print(f"\n✓ Operations Demonstrated:")
print(f"  • Polygon creation and validation")
print(f"  • Point-in-polygon testing")
print(f"  • Spatial predicates (contains, intersects, touches, etc.)")
print(f"  • Geometric operations (union, intersection, difference)")
print(f"  • Spatial joins")
print(f"  • Buffer operations")
print(f"  • Polygon simplification")
print(f"  • Spatial indexing for performance")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### ✓ What We Learned
# MAGIC
# MAGIC 1. **Polygon Creation**
# MAGIC    - Simple polygons from coordinate lists
# MAGIC    - Polygons with holes (interior rings)
# MAGIC    - MultiPolygon for disconnected areas
# MAGIC
# MAGIC 2. **Spatial Predicates**
# MAGIC    - `contains`: A fully contains B
# MAGIC    - `intersects`: A and B share space
# MAGIC    - `within`: A is fully inside B
# MAGIC    - `touches`: A and B share boundary
# MAGIC    - `disjoint`: No relationship
# MAGIC
# MAGIC 3. **Geometric Operations**
# MAGIC    - `intersection`: Shared area
# MAGIC    - `union`: Combined area
# MAGIC    - `difference`: A minus B
# MAGIC    - `buffer`: Expand/shrink
# MAGIC    - `simplify`: Reduce complexity
# MAGIC
# MAGIC 4. **Spatial Joins**
# MAGIC    - Use `gpd.sjoin()` for automated queries
# MAGIC    - Predicate options: within, contains, intersects
# MAGIC    - More efficient than manual iteration
# MAGIC
# MAGIC 5. **Performance**
# MAGIC    - Use spatial indexes (`.sindex`) for large datasets
# MAGIC    - Simplify geometries when appropriate
# MAGIC    - Transform to projected CRS for accurate measurements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC **Next Lab:** Loading and Exploring Geospatial Data
# MAGIC
# MAGIC You'll practice:
# MAGIC - Importing various geospatial formats
# MAGIC - Data quality assessment
# MAGIC - Complex spatial queries
# MAGIC - Creating professional visualizations
# MAGIC
# MAGIC ### Quick Reference
# MAGIC
# MAGIC ```python
# MAGIC # Create polygon
# MAGIC poly = Polygon([(x1, y1), (x2, y2), (x3, y3), (x1, y1)])
# MAGIC
# MAGIC # Point-in-polygon
# MAGIC is_inside = polygon.contains(point)
# MAGIC
# MAGIC # Spatial join
# MAGIC result = gpd.sjoin(points_gdf, polygons_gdf, predicate='within')
# MAGIC
# MAGIC # Geometric operations
# MAGIC intersection = poly1.intersection(poly2)
# MAGIC union = poly1.union(poly2)
# MAGIC difference = poly1.difference(poly2)
# MAGIC
# MAGIC # Buffer (in projected CRS)
# MAGIC gdf_proj = gdf.to_crs('EPSG:3857')
# MAGIC gdf_proj['buffer'] = gdf_proj.geometry.buffer(500)  # 500 meters
# MAGIC
# MAGIC # Simplify
# MAGIC simplified = polygon.simplify(tolerance=0.001, preserve_topology=True)
# MAGIC
# MAGIC # Spatial index
# MAGIC sindex = gdf.sindex
# MAGIC candidates = list(sindex.intersection(point.bounds))
# MAGIC ```
