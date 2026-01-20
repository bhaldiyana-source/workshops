# Databricks notebook source
# MAGIC %md
# MAGIC # Overlay Functions and Geometry Processing
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook covers:
# MAGIC - Overlay operations (union, intersection, difference)
# MAGIC - Aggregation functions
# MAGIC - Advanced processing (buffer, convex hull, concave hull)
# MAGIC - Simplification and centroid operations
# MAGIC - Boundary extraction
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed notebook 05_Topological_Relationships.py
# MAGIC - Understanding of spatial relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Overlay Functions
# MAGIC
# MAGIC Combine geometries using set operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Union - Combine Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union: Combine two geometries into one
# MAGIC SELECT st_astext(
# MAGIC   st_union(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
# MAGIC     st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
# MAGIC   )
# MAGIC ) AS union_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union of non-overlapping polygons
# MAGIC SELECT st_astext(
# MAGIC   st_union(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
# MAGIC     st_geomfromtext('POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))')
# MAGIC   )
# MAGIC ) AS union_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union of adjacent polygons
# MAGIC SELECT st_astext(
# MAGIC   st_union(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
# MAGIC     st_geomfromtext('POLYGON((1 0, 1 1, 2 1, 2 0, 1 0))')
# MAGIC   )
# MAGIC ) AS union_result;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Intersection - Get Overlap

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Intersection: Get the overlap between geometries
# MAGIC SELECT st_astext(
# MAGIC   st_intersection(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
# MAGIC     st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
# MAGIC   )
# MAGIC ) AS intersection_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Intersection of two lines
# MAGIC SELECT st_astext(
# MAGIC   st_intersection(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 10 10)'),
# MAGIC     st_geomfromtext('LINESTRING(0 10, 10 0)')
# MAGIC   )
# MAGIC ) AS line_intersection;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Intersection of line and polygon
# MAGIC SELECT st_astext(
# MAGIC   st_intersection(
# MAGIC     st_geomfromtext('LINESTRING(-1 5, 11 5)'),
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC   )
# MAGIC ) AS line_polygon_intersection;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Difference - Remove Overlap

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Difference: Get the part of A that doesn't overlap with B
# MAGIC SELECT st_astext(
# MAGIC   st_difference(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
# MAGIC     st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
# MAGIC   )
# MAGIC ) AS difference_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Difference is not commutative (A - B != B - A)
# MAGIC SELECT 
# MAGIC   st_astext(
# MAGIC     st_difference(
# MAGIC       st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
# MAGIC       st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
# MAGIC     )
# MAGIC   ) AS a_minus_b,
# MAGIC   st_astext(
# MAGIC     st_difference(
# MAGIC       st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))'),
# MAGIC       st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')
# MAGIC     )
# MAGIC   ) AS b_minus_a;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a "donut" by subtracting inner polygon
# MAGIC SELECT st_astext(
# MAGIC   st_difference(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC     st_geomfromtext('POLYGON((3 3, 3 7, 7 7, 7 3, 3 3))')
# MAGIC   )
# MAGIC ) AS donut_polygon;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Union Aggregate - Combine Multiple Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union Aggregate: Combine multiple geometries into one
# MAGIC WITH shapes AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
# MAGIC   UNION ALL
# MAGIC   SELECT st_geomfromtext('POLYGON((1 0, 1 1, 2 1, 2 0, 1 0))') AS geom
# MAGIC   UNION ALL
# MAGIC   SELECT st_geomfromtext('POLYGON((2 0, 2 1, 3 1, 3 0, 2 0))') AS geom
# MAGIC )
# MAGIC SELECT st_astext(st_union_agg(geom)) AS combined_geometry
# MAGIC FROM shapes;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Union aggregate with overlapping regions
# MAGIC WITH circles AS (
# MAGIC   SELECT st_buffer(st_point(0, 0), 1) AS geom
# MAGIC   UNION ALL
# MAGIC   SELECT st_buffer(st_point(1, 0), 1) AS geom
# MAGIC   UNION ALL
# MAGIC   SELECT st_buffer(st_point(0.5, 0.866), 1) AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_geometrytype(st_union_agg(geom)) AS result_type,
# MAGIC   ROUND(st_area(st_union_agg(geom)), 4) AS total_area
# MAGIC FROM circles;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Geometry Processing Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Buffer - Create Area Around Geometry

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Buffer: Create a buffer around a point
# MAGIC SELECT st_astext(
# MAGIC   st_buffer(st_point(0, 0), 5.0)
# MAGIC ) AS buffered_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Buffer around a line (creates parallel lines)
# MAGIC SELECT st_astext(
# MAGIC   st_buffer(st_geomfromtext('LINESTRING(0 0, 10 10)'), 1.0)
# MAGIC ) AS buffered_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Buffer analysis - create service zones
# MAGIC WITH location AS (
# MAGIC   SELECT st_point(-122.4194, 37.7749) AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   '1km zone' AS zone_type,
# MAGIC   st_astext(st_buffer(geom, 0.01)) AS boundary
# MAGIC FROM location
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '3km zone',
# MAGIC   st_astext(st_buffer((SELECT geom FROM location), 0.03))
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '5km zone',
# MAGIC   st_astext(st_buffer((SELECT geom FROM location), 0.05));

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Convex Hull - Smallest Convex Polygon

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convex Hull: Get the smallest convex polygon containing the geometry
# MAGIC SELECT st_astext(
# MAGIC   st_convexhull(st_geomfromtext('MULTIPOINT(0 0, 1 1, 2 0, 1 2)'))
# MAGIC ) AS convex_hull;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convex hull of a complex linestring
# MAGIC SELECT st_astext(
# MAGIC   st_convexhull(st_geomfromtext('LINESTRING(0 0, 1 1, 2 0, 1 2, 3 1)'))
# MAGIC ) AS convex_hull;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare original polygon with its convex hull
# MAGIC WITH concave_shape AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 2 0, 2 1, 1 1, 1 2, 2 2, 2 3, 0 3, 0 0))') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_astext(geom) AS original_polygon,
# MAGIC   st_astext(st_convexhull(geom)) AS convex_hull,
# MAGIC   ROUND(st_area(geom), 4) AS original_area,
# MAGIC   ROUND(st_area(st_convexhull(geom)), 4) AS convex_hull_area
# MAGIC FROM concave_shape;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Concave Hull - More Detailed Hull

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Concave Hull: Get a concave hull (more detailed than convex)
# MAGIC -- Third parameter is allow_holes (false = no holes allowed)
# MAGIC SELECT st_astext(
# MAGIC   st_concavehull(
# MAGIC     st_geomfromtext('MULTIPOINT(0 0, 1 0, 2 0, 0 1, 1 1, 2 1)'),
# MAGIC     0.5,
# MAGIC     false
# MAGIC   )
# MAGIC ) AS concave_hull;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Different concavity parameters
# MAGIC WITH points AS (
# MAGIC   SELECT st_geomfromtext('MULTIPOINT(0 0, 3 0, 6 0, 0 3, 3 3, 6 3, 1.5 1.5, 4.5 1.5)') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Convex Hull' AS hull_type,
# MAGIC   st_astext(st_convexhull(geom)) AS hull
# MAGIC FROM points
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Concave (0.3)',
# MAGIC   st_astext(st_concavehull(geom, 0.3, false))
# MAGIC FROM points
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Concave (0.7)',
# MAGIC   st_astext(st_concavehull(geom, 0.7, false))
# MAGIC FROM points;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Centroid - Center Point

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Centroid: Get the center point of a geometry
# MAGIC SELECT st_astext(
# MAGIC   st_centroid(st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))
# MAGIC ) AS centroid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Centroid of irregular polygon
# MAGIC SELECT st_astext(
# MAGIC   st_centroid(st_geomfromtext('POLYGON((0 0, 4 0, 4 4, 2 4, 2 2, 0 2, 0 0))'))
# MAGIC ) AS centroid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate centroids for multiple zones
# MAGIC WITH zones AS (
# MAGIC   SELECT 'Zone A' AS name, st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))') AS boundary
# MAGIC   UNION ALL
# MAGIC   SELECT 'Zone B', st_geomfromtext('POLYGON((10 10, 10 15, 15 15, 15 10, 10 10))')
# MAGIC   UNION ALL
# MAGIC   SELECT 'Zone C', st_geomfromtext('POLYGON((5 5, 5 10, 10 10, 10 5, 5 5))')
# MAGIC )
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   st_astext(st_centroid(boundary)) AS center_point,
# MAGIC   st_x(st_centroid(boundary)) AS center_x,
# MAGIC   st_y(st_centroid(boundary)) AS center_y,
# MAGIC   ROUND(st_area(boundary), 2) AS area
# MAGIC FROM zones;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Boundary - Extract Boundary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Boundary: Get the boundary of a geometry
# MAGIC SELECT st_astext(
# MAGIC   st_boundary(st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))
# MAGIC ) AS boundary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Boundary of a linestring (endpoints)
# MAGIC SELECT st_astext(
# MAGIC   st_boundary(st_geomfromtext('LINESTRING(0 0, 5 5, 10 0)'))
# MAGIC ) AS boundary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare geometry with its boundary
# MAGIC WITH polygon AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_geometrytype(geom) AS original_type,
# MAGIC   st_geometrytype(st_boundary(geom)) AS boundary_type,
# MAGIC   st_dimension(geom) AS original_dimension,
# MAGIC   st_dimension(st_boundary(geom)) AS boundary_dimension
# MAGIC FROM polygon;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Simplify - Reduce Complexity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simplify: Simplify a geometry using Douglas-Peucker algorithm
# MAGIC SELECT st_astext(
# MAGIC   st_simplify(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 0.1, 2 0, 3 0, 4 0)'),
# MAGIC     0.5
# MAGIC   )
# MAGIC ) AS simplified_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simplify with different tolerance values
# MAGIC WITH complex_line AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(0 0, 0.5 0.1, 1 0, 1.5 0.1, 2 0, 2.5 0.1, 3 0)') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Original' AS version,
# MAGIC   st_astext(geom) AS geometry,
# MAGIC   st_npoints(geom) AS point_count
# MAGIC FROM complex_line
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Simplified (0.05)',
# MAGIC   st_astext(st_simplify(geom, 0.05)),
# MAGIC   st_npoints(st_simplify(geom, 0.05))
# MAGIC FROM complex_line
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Simplified (0.2)',
# MAGIC   st_astext(st_simplify(geom, 0.2)),
# MAGIC   st_npoints(st_simplify(geom, 0.2))
# MAGIC FROM complex_line;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Practical Example: Service Area Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create overlapping service areas and analyze coverage
# MAGIC WITH stores AS (
# MAGIC   SELECT 'Store A' AS name, st_point(-122.42, 37.78) AS location, 0.02 AS service_radius
# MAGIC   UNION ALL SELECT 'Store B', st_point(-122.40, 37.79), 0.025
# MAGIC   UNION ALL SELECT 'Store C', st_point(-122.41, 37.77), 0.015
# MAGIC ),
# MAGIC service_areas AS (
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     location,
# MAGIC     st_buffer(location, service_radius) AS coverage
# MAGIC   FROM stores
# MAGIC ),
# MAGIC total_coverage AS (
# MAGIC   SELECT st_union_agg(coverage) AS total_area
# MAGIC   FROM service_areas
# MAGIC ),
# MAGIC individual_stats AS (
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     st_astext(location) AS location_wkt,
# MAGIC     ROUND(st_area(coverage), 6) AS individual_area
# MAGIC   FROM service_areas
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Total Coverage' AS metric,
# MAGIC   NULL AS store_name,
# MAGIC   NULL AS location,
# MAGIC   ROUND(st_area(total_area), 6) AS area_value
# MAGIC FROM total_coverage
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Individual Store',
# MAGIC   name,
# MAGIC   location_wkt,
# MAGIC   individual_area
# MAGIC FROM individual_stats;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Practical Example: Delivery Zone Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find optimal delivery zones by merging overlapping areas
# MAGIC WITH delivery_points AS (
# MAGIC   SELECT 'Point 1' AS name, st_point(-122.45, 37.80) AS location
# MAGIC   UNION ALL SELECT 'Point 2', st_point(-122.43, 37.81)
# MAGIC   UNION ALL SELECT 'Point 3', st_point(-122.41, 37.79)
# MAGIC   UNION ALL SELECT 'Point 4', st_point(-122.40, 37.77)
# MAGIC ),
# MAGIC buffered_zones AS (
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     st_buffer(location, 0.02) AS zone
# MAGIC   FROM delivery_points
# MAGIC ),
# MAGIC merged_zone AS (
# MAGIC   SELECT st_union_agg(zone) AS total_zone
# MAGIC   FROM buffered_zones
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_geometrytype(total_zone) AS zone_type,
# MAGIC   ROUND(st_area(total_zone), 6) AS total_area,
# MAGIC   ROUND(st_perimeter(total_zone), 4) AS perimeter,
# MAGIC   st_astext(st_centroid(total_zone)) AS zone_center,
# MAGIC   st_astext(st_convexhull(total_zone)) AS bounding_hull
# MAGIC FROM merged_zone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practical Example: Route Corridor Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create corridors around routes and find intersections
# MAGIC WITH routes AS (
# MAGIC   SELECT 
# MAGIC     'Route North' AS name,
# MAGIC     st_geomfromtext('LINESTRING(-122.45 37.75, -122.40 37.85)') AS path
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'Route South',
# MAGIC     st_geomfromtext('LINESTRING(-122.44 37.76, -122.39 37.70)')
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'Route East',
# MAGIC     st_geomfromtext('LINESTRING(-122.50 37.78, -122.35 37.79)')
# MAGIC ),
# MAGIC corridors AS (
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     path,
# MAGIC     st_buffer(path, 0.01) AS corridor
# MAGIC   FROM routes
# MAGIC )
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   ROUND(st_length(path), 4) AS route_length,
# MAGIC   ROUND(st_area(corridor), 6) AS corridor_area,
# MAGIC   st_astext(st_centroid(corridor)) AS corridor_center,
# MAGIC   st_npoints(path) AS waypoints
# MAGIC FROM corridors
# MAGIC ORDER BY route_length DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Comprehensive Processing Example
# MAGIC
# MAGIC
# MAGIC Combine multiple processing operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complete geometry processing workflow
# MAGIC WITH raw_points AS (
# MAGIC   -- Collection of survey points
# MAGIC   SELECT st_geomfromtext('MULTIPOINT(-122.42 37.78, -122.41 37.79, -122.40 37.78, -122.41 37.77, -122.43 37.79, -122.415 37.785)') AS points
# MAGIC ),
# MAGIC convex_boundary AS (
# MAGIC   -- Step 1: Create convex hull
# MAGIC   SELECT st_convexhull(points) AS boundary
# MAGIC   FROM raw_points
# MAGIC ),
# MAGIC buffered_area AS (
# MAGIC   -- Step 2: Add buffer zone
# MAGIC   SELECT st_buffer(boundary, 0.01) AS area
# MAGIC   FROM convex_boundary
# MAGIC ),
# MAGIC simplified_area AS (
# MAGIC   -- Step 3: Simplify if complex
# MAGIC   SELECT st_simplify(area, 0.001) AS area
# MAGIC   FROM buffered_area
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_geometrytype(area) AS geometry_type,
# MAGIC   ROUND(st_area(area), 6) AS total_area,
# MAGIC   ROUND(st_perimeter(area), 4) AS perimeter,
# MAGIC   st_astext(st_centroid(area)) AS center_point,
# MAGIC   st_npoints(st_boundary(area)) AS boundary_points
# MAGIC FROM simplified_area;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Overlay Operations**:
# MAGIC    - `st_union()` combines geometries (removes overlaps)
# MAGIC    - `st_intersection()` returns only the overlapping parts
# MAGIC    - `st_difference()` removes overlap (A minus B)
# MAGIC    - `st_union_agg()` combines multiple geometries from a group
# MAGIC
# MAGIC 2. **Processing Functions**:
# MAGIC    - `st_buffer()` creates area around geometry (distance-based)
# MAGIC    - `st_convexhull()` creates smallest convex polygon containing geometry
# MAGIC    - `st_concavehull()` creates more detailed hull with concave areas
# MAGIC    - `st_centroid()` finds geometric center
# MAGIC    - `st_boundary()` extracts boundary lines/points
# MAGIC    - `st_simplify()` reduces complexity while preserving shape
# MAGIC
# MAGIC 3. **Performance Considerations**:
# MAGIC    - Buffer operations can be expensive on complex geometries
# MAGIC    - Simplify complex geometries when high precision isn't needed
# MAGIC    - Union aggregate can be memory-intensive for many large geometries
# MAGIC
# MAGIC 4. **Common Use Cases**:
# MAGIC    - Service area analysis (buffer + union)
# MAGIC    - Coverage optimization (intersection + difference)
# MAGIC    - Route corridors (buffer on linestrings)
# MAGIC    - Facility planning (convex hull + centroid)
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next notebook: **07_Transformations_and_Validation.py** to learn about affine transformations and geometry validation.
