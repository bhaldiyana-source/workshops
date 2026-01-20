# Databricks notebook source
# MAGIC %md
# MAGIC # Geometry Constructors and Accessors
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook covers:
# MAGIC - Creating points (2D, 3D, with measures)
# MAGIC - Creating complex geometries (linestrings, polygons)
# MAGIC - Extracting information from geometries
# MAGIC - Accessing geometry properties
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed notebook 01_Introduction_and_Data_Import.py
# MAGIC - Understanding of basic geometry types

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating Points
# MAGIC
# MAGIC Points can be created with various dimensions:
# MAGIC - 2D: X, Y coordinates
# MAGIC - 3D (Z): X, Y, Z coordinates
# MAGIC - With Measure (M): X, Y, M value
# MAGIC - 4D (ZM): X, Y, Z, M values

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 2D Points

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a 2D point
# MAGIC SELECT st_point(1.0, 2.0) AS point_2d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display as WKT for better readability
# MAGIC SELECT st_astext(st_point(1.0, 2.0)) AS point_wkt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create multiple points
# MAGIC SELECT 
# MAGIC   st_astext(st_point(-122.4194, 37.7749)) AS san_francisco,
# MAGIC   st_astext(st_point(-118.2437, 34.0522)) AS los_angeles,
# MAGIC   st_astext(st_point(-74.0060, 40.7128)) AS new_york;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 3D Points with Z Coordinate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a 3D point with Z coordinate (elevation)
# MAGIC SELECT st_pointz(1.0, 2.0, 3.0) AS point_3d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display 3D point as WKT
# MAGIC SELECT st_astext(st_pointz(1.0, 2.0, 3.0)) AS point_3d_wkt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create points with elevation data
# MAGIC SELECT 
# MAGIC   st_astext(st_pointz(-122.4194, 37.7749, 16)) AS sf_sea_level,
# MAGIC   st_astext(st_pointz(-105.2705, 39.7392, 1609)) AS denver_elevation;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Points with Measure Value

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create point with measure value (M)
# MAGIC -- M can represent time, distance along route, or any other measure
# MAGIC SELECT st_pointm(1.0, 2.0, 5.0) AS point_with_measure;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display point with measure as WKT
# MAGIC SELECT st_astext(st_pointm(1.0, 2.0, 5.0)) AS point_m_wkt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 4D Points (X, Y, Z, M)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create 4D point with both Z and M values
# MAGIC SELECT st_pointzm(1.0, 2.0, 3.0, 4.0) AS point_4d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display 4D point as WKT
# MAGIC SELECT st_astext(st_pointzm(1.0, 2.0, 3.0, 4.0)) AS point_4d_wkt;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating Complex Geometries
# MAGIC
# MAGIC Build linestrings, polygons, and other complex shapes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Creating Linestrings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a linestring from two points
# MAGIC SELECT st_astext(
# MAGIC   st_makeline(
# MAGIC     st_point(-122.4194, 37.7749),
# MAGIC     st_point(-118.2437, 34.0522)
# MAGIC   )
# MAGIC ) AS line_sf_to_la;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a linestring from multiple points using array
# MAGIC SELECT st_astext(
# MAGIC   st_makeline(
# MAGIC     array(
# MAGIC       st_point(-122.4194, 37.7749),
# MAGIC       st_point(-121.0, 37.0),
# MAGIC       st_point(-118.2437, 34.0522)
# MAGIC     )
# MAGIC   )
# MAGIC ) AS multi_segment_route;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Creating Polygons

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a simple polygon from a closed linestring
# MAGIC SELECT st_astext(
# MAGIC   st_makepolygon(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)')
# MAGIC   )
# MAGIC ) AS simple_polygon;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a triangle
# MAGIC SELECT st_astext(
# MAGIC   st_makepolygon(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 4 0, 2 3, 0 0)')
# MAGIC   )
# MAGIC ) AS triangle;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Creating Polygons with Holes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create polygon with holes (donut shape)
# MAGIC SELECT st_astext(
# MAGIC   st_makepolygon(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)'),
# MAGIC     array(st_geomfromtext('LINESTRING(2 2, 2 4, 4 4, 4 2, 2 2)'))
# MAGIC   )
# MAGIC ) AS polygon_with_hole;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create polygon with multiple holes
# MAGIC SELECT st_astext(
# MAGIC   st_makepolygon(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)'),
# MAGIC     array(
# MAGIC       st_geomfromtext('LINESTRING(2 2, 2 4, 4 4, 4 2, 2 2)'),
# MAGIC       st_geomfromtext('LINESTRING(6 6, 6 8, 8 8, 8 6, 6 6)')
# MAGIC     )
# MAGIC   )
# MAGIC ) AS polygon_with_multiple_holes;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Geometry Accessors
# MAGIC
# MAGIC Extract information and properties from existing geometries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Extracting Coordinates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get X coordinate of a point
# MAGIC SELECT st_x(st_point(1, 2)) AS x_coord;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get Y coordinate of a point
# MAGIC SELECT st_y(st_point(1, 2)) AS y_coord;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get Z coordinate of a 3D point
# MAGIC SELECT st_z(st_pointz(1, 2, 3)) AS z_coord;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get M value of a point with measure
# MAGIC SELECT st_m(st_pointm(1, 2, 5)) AS m_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract all coordinates from a point
# MAGIC SELECT 
# MAGIC   st_x(st_pointzm(1, 2, 3, 4)) AS x,
# MAGIC   st_y(st_pointzm(1, 2, 3, 4)) AS y,
# MAGIC   st_z(st_pointzm(1, 2, 3, 4)) AS z,
# MAGIC   st_m(st_pointzm(1, 2, 3, 4)) AS m;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Geometry Type and Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the dimension of a geometry (0=point, 1=line, 2=polygon)
# MAGIC SELECT 
# MAGIC   st_dimension(st_geomfromtext('POINT(0 0)')) AS point_dim,
# MAGIC   st_dimension(st_geomfromtext('LINESTRING(0 0, 1 1)')) AS line_dim,
# MAGIC   st_dimension(st_geomfromtext('POLYGON((0 0, 1 0, 1 1, 0 0))')) AS polygon_dim;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the geometry type
# MAGIC SELECT st_geometrytype(st_geomfromtext('POLYGON((0 0, 1 0, 1 1, 0 0))')) AS geom_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get geometry types for various shapes
# MAGIC SELECT 
# MAGIC   st_geometrytype(st_geomfromtext('POINT(0 0)')) AS point_type,
# MAGIC   st_geometrytype(st_geomfromtext('LINESTRING(0 0, 1 1)')) AS line_type,
# MAGIC   st_geometrytype(st_geomfromtext('POLYGON((0 0, 1 0, 1 1, 0 0))')) AS polygon_type,
# MAGIC   st_geometrytype(st_geomfromtext('MULTIPOINT(0 0, 1 1)')) AS multipoint_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Counting Elements

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count number of geometries in a collection
# MAGIC SELECT st_numgeometries(st_geomfromtext('MULTIPOINT(0 0, 1 1)')) AS num_geoms;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count number of points in a linestring
# MAGIC SELECT st_npoints(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')) AS num_points;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alternative function for counting points
# MAGIC SELECT st_numpoints(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')) AS num_points_alt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare point counts for different geometries
# MAGIC SELECT 
# MAGIC   st_npoints(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')) AS line_points,
# MAGIC   st_npoints(st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) AS polygon_points;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Extracting Specific Points

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the start point of a linestring
# MAGIC SELECT st_astext(
# MAGIC   st_startpoint(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'))
# MAGIC ) AS start_pt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the end point of a linestring
# MAGIC SELECT st_astext(
# MAGIC   st_endpoint(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'))
# MAGIC ) AS end_pt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the Nth point of a linestring (1-indexed)
# MAGIC SELECT st_astext(
# MAGIC   st_pointn(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'), 2)
# MAGIC ) AS nth_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract all key points from a linestring
# MAGIC SELECT 
# MAGIC   st_astext(st_startpoint(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2, 3 3)'))) AS start,
# MAGIC   st_astext(st_pointn(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2, 3 3)'), 2)) AS second,
# MAGIC   st_astext(st_pointn(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2, 3 3)'), 3)) AS third,
# MAGIC   st_astext(st_endpoint(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2, 3 3)'))) AS end;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Checking Geometry Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if a linestring is closed (start point = end point)
# MAGIC SELECT 
# MAGIC   st_isclosed(st_geomfromtext('LINESTRING(0 0, 1 1, 0 0)')) AS is_closed,
# MAGIC   st_isclosed(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')) AS is_not_closed;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if a geometry is empty
# MAGIC SELECT 
# MAGIC   st_isempty(st_geomfromtext('POINT EMPTY')) AS is_empty,
# MAGIC   st_isempty(st_geomfromtext('POINT(1 1)')) AS is_not_empty;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if a geometry is simple (no self-intersections)
# MAGIC SELECT st_issimple(st_geomfromtext('LINESTRING(0 0, 1 1)')) AS is_simple;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check multiple properties at once
# MAGIC WITH line AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_geometrytype(geom) AS type,
# MAGIC   st_dimension(geom) AS dimension,
# MAGIC   st_npoints(geom) AS num_points,
# MAGIC   st_isclosed(geom) AS is_closed,
# MAGIC   st_issimple(geom) AS is_simple,
# MAGIC   st_isempty(geom) AS is_empty
# MAGIC FROM line;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Practical Example: Route Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a delivery route and analyze its properties
# MAGIC WITH waypoints AS (
# MAGIC   SELECT array(
# MAGIC     st_point(-122.4194, 37.7749),  -- San Francisco
# MAGIC     st_point(-122.3, 37.8),         -- Waypoint 1
# MAGIC     st_point(-122.2, 37.85),        -- Waypoint 2
# MAGIC     st_point(-122.1, 37.9)          -- Final destination
# MAGIC   ) AS points
# MAGIC ),
# MAGIC route AS (
# MAGIC   SELECT st_makeline(points) AS path
# MAGIC   FROM waypoints
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_geometrytype(path) AS geometry_type,
# MAGIC   st_npoints(path) AS waypoint_count,
# MAGIC   st_astext(st_startpoint(path)) AS start_location,
# MAGIC   st_astext(st_endpoint(path)) AS end_location,
# MAGIC   ROUND(st_length(path), 6) AS route_length_degrees,
# MAGIC   st_isclosed(path) AS is_round_trip
# MAGIC FROM route;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practical Example: Service Area Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create and analyze service areas
# MAGIC WITH service_area AS (
# MAGIC   SELECT st_makepolygon(
# MAGIC     st_geomfromtext('LINESTRING(-122.5 37.5, -122.5 38.0, -122.0 38.0, -122.0 37.5, -122.5 37.5)')
# MAGIC   ) AS boundary
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_geometrytype(boundary) AS type,
# MAGIC   st_dimension(boundary) AS dimension,
# MAGIC   st_npoints(boundary) AS boundary_points,
# MAGIC   ROUND(st_area(boundary), 6) AS area_sq_degrees,
# MAGIC   ROUND(st_perimeter(boundary), 6) AS perimeter_degrees,
# MAGIC   st_astext(st_centroid(boundary)) AS center_point
# MAGIC FROM service_area;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Practical Example: Building Complex Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a complex multi-zone delivery area
# MAGIC WITH zones AS (
# MAGIC   SELECT 'Zone 1' AS name, st_point(-122.4, 37.8) AS center, 0.05 AS radius
# MAGIC   UNION ALL SELECT 'Zone 2', st_point(-122.5, 37.7), 0.03
# MAGIC   UNION ALL SELECT 'Zone 3', st_point(-122.3, 37.9), 0.04
# MAGIC ),
# MAGIC zone_boundaries AS (
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     center,
# MAGIC     st_buffer(center, radius) AS boundary
# MAGIC   FROM zones
# MAGIC )
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   st_astext(center) AS center_point,
# MAGIC   st_x(center) AS center_lon,
# MAGIC   st_y(center) AS center_lat,
# MAGIC   st_geometrytype(boundary) AS boundary_type,
# MAGIC   ROUND(st_area(boundary), 6) AS coverage_area
# MAGIC FROM zone_boundaries
# MAGIC ORDER BY coverage_area DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Point Constructors**:
# MAGIC    - `st_point(x, y)` for 2D points
# MAGIC    - `st_pointz(x, y, z)` for 3D points with elevation
# MAGIC    - `st_pointm(x, y, m)` for points with measure values
# MAGIC    - `st_pointzm(x, y, z, m)` for 4D points
# MAGIC
# MAGIC 2. **Complex Geometry Constructors**:
# MAGIC    - `st_makeline()` creates linestrings from points
# MAGIC    - `st_makepolygon()` creates polygons from closed linestrings
# MAGIC    - Can create polygons with holes using array of interior rings
# MAGIC
# MAGIC 3. **Coordinate Accessors**:
# MAGIC    - `st_x()`, `st_y()`, `st_z()`, `st_m()` extract coordinates
# MAGIC    - `st_startpoint()`, `st_endpoint()`, `st_pointn()` extract specific points
# MAGIC
# MAGIC 4. **Property Accessors**:
# MAGIC    - `st_geometrytype()` returns the type of geometry
# MAGIC    - `st_dimension()` returns the topological dimension
# MAGIC    - `st_npoints()` counts points in a geometry
# MAGIC    - `st_isclosed()`, `st_isempty()`, `st_issimple()` check properties
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next notebook: **04_Geometry_Editors_and_SRID.py** to learn about modifying geometries and working with coordinate reference systems.
