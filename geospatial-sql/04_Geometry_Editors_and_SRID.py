# Databricks notebook source
# MAGIC %md
# MAGIC # Geometry Editors and Spatial Reference Systems
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook covers:
# MAGIC - Modifying existing geometries (add, remove, set points)
# MAGIC - Reversing and forcing dimensions
# MAGIC - Working with SRIDs (Spatial Reference System Identifiers)
# MAGIC - Transforming between coordinate systems
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed notebook 03_Geometry_Constructors_and_Accessors.py
# MAGIC - Understanding of coordinate reference systems (helpful)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Geometry Editors
# MAGIC
# MAGIC Functions to modify existing geometries without creating entirely new ones.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Adding Points to Linestrings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a point to a linestring at a specific position (0-indexed)
# MAGIC SELECT st_astext(
# MAGIC   st_addpoint(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 2 2)'),
# MAGIC     st_point(1, 1),
# MAGIC     1  -- Insert at index 1 (between first and second point)
# MAGIC   )
# MAGIC ) AS modified_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a point at the end (position -1)
# MAGIC SELECT st_astext(
# MAGIC   st_addpoint(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'),
# MAGIC     st_point(3, 3),
# MAGIC     -1
# MAGIC   )
# MAGIC ) AS line_with_endpoint;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add multiple waypoints to a route
# MAGIC WITH original_route AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(-122.4 37.8, -122.0 38.0)') AS path
# MAGIC ),
# MAGIC with_waypoint_1 AS (
# MAGIC   SELECT st_addpoint(path, st_point(-122.3, 37.85), 1) AS path
# MAGIC   FROM original_route
# MAGIC ),
# MAGIC with_waypoint_2 AS (
# MAGIC   SELECT st_addpoint(path, st_point(-122.2, 37.9), 2) AS path
# MAGIC   FROM with_waypoint_1
# MAGIC )
# MAGIC SELECT st_astext(path) AS enhanced_route
# MAGIC FROM with_waypoint_2;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Removing Points from Linestrings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove a point from a linestring by index (0-indexed)
# MAGIC SELECT st_astext(
# MAGIC   st_removepoint(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'),
# MAGIC     1  -- Remove the middle point
# MAGIC   )
# MAGIC ) AS line_with_point_removed;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove the first point
# MAGIC SELECT st_astext(
# MAGIC   st_removepoint(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 1, 2 2, 3 3)'),
# MAGIC     0
# MAGIC   )
# MAGIC ) AS without_first_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove the last point
# MAGIC WITH line AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(0 0, 1 1, 2 2, 3 3)') AS geom
# MAGIC )
# MAGIC SELECT st_astext(
# MAGIC   st_removepoint(geom, st_npoints(geom) - 1)
# MAGIC ) AS without_last_point
# MAGIC FROM line;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Setting/Replacing Points in Linestrings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set a specific point in a linestring to a new value
# MAGIC SELECT st_astext(
# MAGIC   st_setpoint(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'),
# MAGIC     1,  -- Index of point to replace
# MAGIC     st_point(1, 5)  -- New point value
# MAGIC   )
# MAGIC ) AS line_with_modified_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Adjust a waypoint in a route
# MAGIC SELECT st_astext(
# MAGIC   st_setpoint(
# MAGIC     st_geomfromtext('LINESTRING(-122.4 37.8, -122.3 37.85, -122.2 37.9)'),
# MAGIC     1,
# MAGIC     st_point(-122.35, 37.88)  -- Adjusted waypoint
# MAGIC   )
# MAGIC ) AS adjusted_route;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace first and last points
# MAGIC WITH line AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)') AS geom
# MAGIC ),
# MAGIC new_start AS (
# MAGIC   SELECT st_setpoint(geom, 0, st_point(-1, -1)) AS geom
# MAGIC   FROM line
# MAGIC )
# MAGIC SELECT st_astext(
# MAGIC   st_setpoint(geom, 2, st_point(3, 3))
# MAGIC ) AS modified_endpoints
# MAGIC FROM new_start;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Reversing Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reverse the order of points in a linestring
# MAGIC SELECT st_astext(
# MAGIC   st_reverse(st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)'))
# MAGIC ) AS reversed_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demonstrate reversing a route
# MAGIC WITH route AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(-122.4 37.8, -122.3 37.85, -122.2 37.9)') AS path
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_astext(path) AS original_route,
# MAGIC   st_astext(st_reverse(path)) AS return_route
# MAGIC FROM route;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Forcing Dimension Changes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Force 2D (remove Z and M coordinates)
# MAGIC SELECT st_astext(
# MAGIC   st_force2d(st_pointz(1, 2, 3))
# MAGIC ) AS point_2d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Force 2D on a 4D point
# MAGIC SELECT st_astext(
# MAGIC   st_force2d(st_pointzm(1, 2, 3, 4))
# MAGIC ) AS point_2d_from_4d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare dimensions
# MAGIC SELECT 
# MAGIC   st_astext(st_pointzm(1, 2, 3, 4)) AS original_4d,
# MAGIC   st_astext(st_force2d(st_pointzm(1, 2, 3, 4))) AS forced_2d;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Spatial Reference Systems (SRID)
# MAGIC
# MAGIC SRID defines the coordinate reference system for your geometries.
# MAGIC
# MAGIC **Common SRIDs:**
# MAGIC - 4326: WGS84 (GPS coordinates, lat/lon)
# MAGIC - 3857: Web Mercator (used by Google Maps, OpenStreetMap)
# MAGIC - 0: Arbitrary/undefined coordinate system

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Getting and Setting SRID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the SRID of a geometry
# MAGIC SELECT st_srid(st_geomfromewkt('SRID=4326;POINT(1 2)')) AS srid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check SRID of different geometry creation methods
# MAGIC SELECT 
# MAGIC   st_srid(st_geomfromtext('POINT(1 2)')) AS from_wkt_srid,
# MAGIC   st_srid(st_geomfromewkt('SRID=4326;POINT(1 2)')) AS from_ewkt_srid,
# MAGIC   st_srid(st_point(1, 2)) AS from_point_srid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the SRID of a geometry
# MAGIC SELECT st_asewkt(
# MAGIC   st_setsrid(st_point(1, 2), 4326)
# MAGIC ) AS geom_with_srid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set SRID for real-world coordinates
# MAGIC SELECT st_asewkt(
# MAGIC   st_setsrid(st_point(-122.4194, 37.7749), 4326)
# MAGIC ) AS san_francisco_with_srid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change the SRID of an existing geometry
# MAGIC WITH original AS (
# MAGIC   SELECT st_geomfromtext('POINT(-122.4194 37.7749)') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_srid(geom) AS original_srid,
# MAGIC   st_asewkt(geom) AS original_ewkt,
# MAGIC   st_asewkt(st_setsrid(geom, 4326)) AS with_wgs84_srid
# MAGIC FROM original;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Transforming Between Coordinate Systems

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transform from WGS84 (4326) to Web Mercator (3857)
# MAGIC SELECT st_astext(
# MAGIC   st_transform(
# MAGIC     st_setsrid(st_point(-122.4194, 37.7749), 4326),
# MAGIC     3857
# MAGIC   )
# MAGIC ) AS transformed_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare coordinates in different systems
# MAGIC WITH wgs84 AS (
# MAGIC   SELECT st_setsrid(st_point(-122.4194, 37.7749), 4326) AS geom
# MAGIC ),
# MAGIC web_mercator AS (
# MAGIC   SELECT st_transform(geom, 3857) AS geom
# MAGIC   FROM wgs84
# MAGIC )
# MAGIC SELECT 
# MAGIC   'WGS84 (4326)' AS coordinate_system,
# MAGIC   st_x((SELECT geom FROM wgs84)) AS x,
# MAGIC   st_y((SELECT geom FROM wgs84)) AS y
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Web Mercator (3857)',
# MAGIC   st_x(geom),
# MAGIC   st_y(geom)
# MAGIC FROM web_mercator;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transform back and forth
# MAGIC WITH original AS (
# MAGIC   SELECT st_setsrid(st_point(-122.4194, 37.7749), 4326) AS geom
# MAGIC ),
# MAGIC to_mercator AS (
# MAGIC   SELECT st_transform(geom, 3857) AS geom
# MAGIC   FROM original
# MAGIC ),
# MAGIC back_to_wgs84 AS (
# MAGIC   SELECT st_transform(geom, 4326) AS geom
# MAGIC   FROM to_mercator
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_astext((SELECT geom FROM original)) AS original_wgs84,
# MAGIC   st_astext((SELECT geom FROM to_mercator)) AS web_mercator,
# MAGIC   st_astext(geom) AS back_to_wgs84
# MAGIC FROM back_to_wgs84;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Practical Example: Route Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create and optimize a delivery route
# MAGIC WITH original_route AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(-122.4 37.8, -122.3 37.85, -122.25 37.88, -122.2 37.9)') AS path
# MAGIC ),
# MAGIC route_analysis AS (
# MAGIC   SELECT 
# MAGIC     path,
# MAGIC     st_npoints(path) AS waypoint_count,
# MAGIC     st_length(path) AS original_length
# MAGIC   FROM original_route
# MAGIC ),
# MAGIC -- Remove inefficient middle waypoint
# MAGIC optimized_route AS (
# MAGIC   SELECT 
# MAGIC     st_removepoint(path, 2) AS path,
# MAGIC     original_length
# MAGIC   FROM route_analysis
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_astext(path) AS optimized_path,
# MAGIC   st_npoints(path) AS new_waypoint_count,
# MAGIC   st_length(path) AS new_length,
# MAGIC   original_length,
# MAGIC   ROUND((original_length - st_length(path)) * 100 / original_length, 2) AS savings_percent
# MAGIC FROM optimized_route;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Practical Example: SRID Management for Multi-Source Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Handling data from multiple sources with different SRIDs
# MAGIC WITH gps_data AS (
# MAGIC   -- GPS data in WGS84
# MAGIC   SELECT 
# MAGIC     'GPS Device' AS source,
# MAGIC     st_setsrid(st_point(-122.4194, 37.7749), 4326) AS location,
# MAGIC     4326 AS srid
# MAGIC ),
# MAGIC web_map_data AS (
# MAGIC   -- Web map data in Web Mercator
# MAGIC   SELECT 
# MAGIC     'Web Map' AS source,
# MAGIC     st_setsrid(st_point(-13628308, 4549405), 3857) AS location,
# MAGIC     3857 AS srid
# MAGIC ),
# MAGIC -- Normalize all to WGS84
# MAGIC normalized AS (
# MAGIC   SELECT source, location, srid FROM gps_data
# MAGIC   UNION ALL
# MAGIC   SELECT source, st_transform(location, 4326) AS location, srid FROM web_map_data
# MAGIC )
# MAGIC SELECT 
# MAGIC   source,
# MAGIC   srid AS original_srid,
# MAGIC   st_astext(location) AS wgs84_coordinates,
# MAGIC   ROUND(st_x(location), 4) AS longitude,
# MAGIC   ROUND(st_y(location), 4) AS latitude
# MAGIC FROM normalized;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practical Example: Building a Corrected Route

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Build a route with corrections
# MAGIC WITH planned_route AS (
# MAGIC   SELECT st_makeline(array(
# MAGIC     st_point(-122.4, 37.8),
# MAGIC     st_point(-122.3, 37.85),
# MAGIC     st_point(-122.2, 37.9),
# MAGIC     st_point(-122.1, 37.95)
# MAGIC   )) AS path
# MAGIC ),
# MAGIC -- Correct waypoint 2 (index 1)
# MAGIC corrected_waypoint_2 AS (
# MAGIC   SELECT st_setpoint(path, 1, st_point(-122.32, 37.86)) AS path
# MAGIC   FROM planned_route
# MAGIC ),
# MAGIC -- Add an additional waypoint
# MAGIC with_extra_waypoint AS (
# MAGIC   SELECT st_addpoint(path, st_point(-122.25, 37.88), 2) AS path
# MAGIC   FROM corrected_waypoint_2
# MAGIC ),
# MAGIC -- Set SRID
# MAGIC final_route AS (
# MAGIC   SELECT st_setsrid(path, 4326) AS path
# MAGIC   FROM with_extra_waypoint
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_asewkt(path) AS route_with_srid,
# MAGIC   st_npoints(path) AS total_waypoints,
# MAGIC   ROUND(st_length(path), 4) AS length_degrees,
# MAGIC   st_astext(st_startpoint(path)) AS start,
# MAGIC   st_astext(st_endpoint(path)) AS destination
# MAGIC FROM final_route;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Comprehensive Geometry Editing Example

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complete workflow: create, edit, and transform a geometry
# MAGIC WITH step1_create AS (
# MAGIC   SELECT 
# MAGIC     'Step 1: Create' AS step,
# MAGIC     st_makeline(array(
# MAGIC       st_point(0, 0),
# MAGIC       st_point(1, 1),
# MAGIC       st_point(2, 2),
# MAGIC       st_point(3, 3)
# MAGIC     )) AS geom
# MAGIC ),
# MAGIC step2_reverse AS (
# MAGIC   SELECT 
# MAGIC     'Step 2: Reverse' AS step,
# MAGIC     st_reverse(geom) AS geom
# MAGIC   FROM step1_create
# MAGIC ),
# MAGIC step3_remove AS (
# MAGIC   SELECT 
# MAGIC     'Step 3: Remove point' AS step,
# MAGIC     st_removepoint(geom, 1) AS geom
# MAGIC   FROM step2_reverse
# MAGIC ),
# MAGIC step4_add AS (
# MAGIC   SELECT 
# MAGIC     'Step 4: Add point' AS step,
# MAGIC     st_addpoint(geom, st_point(2.5, 2.5), 2) AS geom
# MAGIC   FROM step3_remove
# MAGIC ),
# MAGIC step5_srid AS (
# MAGIC   SELECT 
# MAGIC     'Step 5: Set SRID' AS step,
# MAGIC     st_setsrid(geom, 4326) AS geom
# MAGIC   FROM step4_add
# MAGIC )
# MAGIC SELECT 
# MAGIC   step,
# MAGIC   st_asewkt(geom) AS geometry,
# MAGIC   st_npoints(geom) AS point_count,
# MAGIC   st_srid(geom) AS srid
# MAGIC FROM step1_create
# MAGIC UNION ALL SELECT step, st_asewkt(geom), st_npoints(geom), st_srid(geom) FROM step2_reverse
# MAGIC UNION ALL SELECT step, st_asewkt(geom), st_npoints(geom), st_srid(geom) FROM step3_remove
# MAGIC UNION ALL SELECT step, st_asewkt(geom), st_npoints(geom), st_srid(geom) FROM step4_add
# MAGIC UNION ALL SELECT step, st_asewkt(geom), st_npoints(geom), st_srid(geom) FROM step5_srid;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Geometry Editors**:
# MAGIC    - `st_addpoint()` adds points to linestrings at specific positions
# MAGIC    - `st_removepoint()` removes points by index
# MAGIC    - `st_setpoint()` replaces a point at a specific index
# MAGIC    - `st_reverse()` reverses the order of points
# MAGIC    - `st_force2d()` removes Z and M coordinates
# MAGIC
# MAGIC 2. **SRID Management**:
# MAGIC    - `st_srid()` gets the SRID of a geometry
# MAGIC    - `st_setsrid()` assigns an SRID (doesn't transform coordinates)
# MAGIC    - Always set SRID 4326 for GPS/lat-lon data
# MAGIC
# MAGIC 3. **Coordinate Transformations**:
# MAGIC    - `st_transform()` converts between coordinate systems
# MAGIC    - Common SRIDs: 4326 (WGS84), 3857 (Web Mercator)
# MAGIC    - Always normalize to a common SRID when combining data from multiple sources
# MAGIC
# MAGIC 4. **Best Practices**:
# MAGIC    - Set SRIDs explicitly for geographic data
# MAGIC    - Transform to a common SRID before spatial operations
# MAGIC    - Use appropriate SRID for your use case (4326 for global, projected for local)
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next notebook: **05_Topological_Relationships.py** to learn about testing spatial relationships between geometries.
