# Databricks notebook source
# MAGIC %md
# MAGIC # Topological Relationships and Distance Tests
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook covers:
# MAGIC - Spatial relationship predicates (contains, within, intersects, etc.)
# MAGIC - Equality and boundary tests
# MAGIC - Distance-based relationships
# MAGIC - Practical filtering and querying patterns
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed notebook 04_Geometry_Editors_and_SRID.py
# MAGIC - Understanding of basic geometry types and operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Containment Relationships
# MAGIC
# MAGIC Test whether one geometry contains or is within another.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Contains

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contains: Does geometry A completely contain geometry B?
# MAGIC SELECT st_contains(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC   st_geomfromtext('POINT(5 5)')
# MAGIC ) AS contains_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point outside the polygon
# MAGIC SELECT st_contains(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC   st_geomfromtext('POINT(15 15)')
# MAGIC ) AS contains_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Polygon contains a line
# MAGIC SELECT st_contains(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC   st_geomfromtext('LINESTRING(2 2, 8 8)')
# MAGIC ) AS polygon_contains_line;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Within

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Within: Is geometry A completely within geometry B?
# MAGIC SELECT st_within(
# MAGIC   st_geomfromtext('POINT(5 5)'),
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS within_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point outside the polygon
# MAGIC SELECT st_within(
# MAGIC   st_geomfromtext('POINT(15 15)'),
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS within_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contains and Within are inverse relationships
# MAGIC SELECT 
# MAGIC   st_contains(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC     st_geomfromtext('POINT(5 5)')
# MAGIC   ) AS polygon_contains_point,
# MAGIC   st_within(
# MAGIC     st_geomfromtext('POINT(5 5)'),
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC   ) AS point_within_polygon;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Intersection Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Intersects

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Intersects: Do the geometries have any points in common?
# MAGIC SELECT st_intersects(
# MAGIC   st_geomfromtext('LINESTRING(0 0, 10 10)'),
# MAGIC   st_geomfromtext('LINESTRING(0 10, 10 0)')
# MAGIC ) AS intersects_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lines that don't intersect
# MAGIC SELECT st_intersects(
# MAGIC   st_geomfromtext('LINESTRING(0 0, 5 5)'),
# MAGIC   st_geomfromtext('LINESTRING(10 10, 15 15)')
# MAGIC ) AS intersects_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point intersects with polygon
# MAGIC SELECT st_intersects(
# MAGIC   st_geomfromtext('POINT(5 5)'),
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS point_intersects_polygon;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Disjoint

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disjoint: Are the geometries completely separate (don't touch)?
# MAGIC SELECT st_disjoint(
# MAGIC   st_geomfromtext('POINT(0 0)'),
# MAGIC   st_geomfromtext('POINT(10 10)')
# MAGIC ) AS disjoint_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same point is not disjoint
# MAGIC SELECT st_disjoint(
# MAGIC   st_geomfromtext('POINT(5 5)'),
# MAGIC   st_geomfromtext('POINT(5 5)')
# MAGIC ) AS disjoint_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disjoint is the inverse of Intersects
# MAGIC SELECT 
# MAGIC   st_intersects(
# MAGIC     st_geomfromtext('POINT(0 0)'),
# MAGIC     st_geomfromtext('POINT(10 10)')
# MAGIC   ) AS intersects,
# MAGIC   st_disjoint(
# MAGIC     st_geomfromtext('POINT(0 0)'),
# MAGIC     st_geomfromtext('POINT(10 10)')
# MAGIC   ) AS disjoint;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Equality and Boundary Tests

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Equals

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Equals: Are the geometries spatially equal?
# MAGIC SELECT st_equals(
# MAGIC   st_geomfromtext('POINT(1 1)'),
# MAGIC   st_geomfromtext('POINT(1 1)')
# MAGIC ) AS equals_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Different points are not equal
# MAGIC SELECT st_equals(
# MAGIC   st_geomfromtext('POINT(1 1)'),
# MAGIC   st_geomfromtext('POINT(2 2)')
# MAGIC ) AS equals_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same polygon with different vertex order
# MAGIC SELECT st_equals(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC   st_geomfromtext('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')
# MAGIC ) AS equals_result;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Touches

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Touches: Do the geometries touch at boundaries but not overlap?
# MAGIC SELECT st_touches(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
# MAGIC   st_geomfromtext('POLYGON((1 0, 1 1, 2 1, 2 0, 1 0))')
# MAGIC ) AS touches_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point touches polygon boundary
# MAGIC SELECT st_touches(
# MAGIC   st_geomfromtext('POINT(0 5)'),
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS point_touches_boundary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overlapping polygons don't just "touch"
# MAGIC SELECT st_touches(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
# MAGIC   st_geomfromtext('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
# MAGIC ) AS touches_result;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Covers
# MAGIC
# MAGIC
# MAGIC Covers is similar to Contains, but includes boundary points.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Covers: Does geometry A cover geometry B?
# MAGIC SELECT st_covers(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC   st_geomfromtext('LINESTRING(0 0, 5 5)')
# MAGIC ) AS covers_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point on the boundary is covered
# MAGIC SELECT st_covers(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC   st_geomfromtext('POINT(0 5)')
# MAGIC ) AS covers_boundary_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare Contains vs Covers for boundary points
# MAGIC SELECT 
# MAGIC   st_contains(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC     st_geomfromtext('POINT(0 5)')
# MAGIC   ) AS contains_boundary,
# MAGIC   st_covers(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC     st_geomfromtext('POINT(0 5)')
# MAGIC   ) AS covers_boundary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Distance-Based Relationships

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 DWithin

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DWithin: Are two geometries within a specified distance?
# MAGIC SELECT st_dwithin(
# MAGIC   st_geomfromtext('POINT(0 0)'),
# MAGIC   st_geomfromtext('POINT(3 4)'),
# MAGIC   6.0
# MAGIC ) AS within_distance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Points NOT within distance
# MAGIC SELECT st_dwithin(
# MAGIC   st_geomfromtext('POINT(0 0)'),
# MAGIC   st_geomfromtext('POINT(3 4)'),
# MAGIC   4.0
# MAGIC ) AS within_distance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify with actual distance calculation
# MAGIC SELECT 
# MAGIC   st_distance(
# MAGIC     st_geomfromtext('POINT(0 0)'),
# MAGIC     st_geomfromtext('POINT(3 4)')
# MAGIC   ) AS actual_distance,
# MAGIC   st_dwithin(
# MAGIC     st_geomfromtext('POINT(0 0)'),
# MAGIC     st_geomfromtext('POINT(3 4)'),
# MAGIC     5.0
# MAGIC   ) AS within_5,
# MAGIC   st_dwithin(
# MAGIC     st_geomfromtext('POINT(0 0)'),
# MAGIC     st_geomfromtext('POINT(3 4)'),
# MAGIC     5.1
# MAGIC   ) AS within_5_1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practical Example: Store Locator with Filters

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find stores within service areas and distance thresholds
# MAGIC WITH service_area AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((-122.5 37.7, -122.5 37.9, -122.3 37.9, -122.3 37.7, -122.5 37.7))') AS boundary
# MAGIC ),
# MAGIC stores AS (
# MAGIC   SELECT 'Store A' AS name, st_geomfromtext('POINT(-122.4194 37.7749)') AS location
# MAGIC   UNION ALL SELECT 'Store B', st_geomfromtext('POINT(-122.4000 37.7900)')
# MAGIC   UNION ALL SELECT 'Store C', st_geomfromtext('POINT(-122.4500 37.7600)')
# MAGIC   UNION ALL SELECT 'Store D', st_geomfromtext('POINT(-122.2000 37.8000)')
# MAGIC   UNION ALL SELECT 'Store E', st_geomfromtext('POINT(-122.6000 37.8500)')
# MAGIC ),
# MAGIC user_location AS (
# MAGIC   SELECT st_geomfromtext('POINT(-122.4100 37.7800)') AS location
# MAGIC )
# MAGIC SELECT 
# MAGIC   s.name,
# MAGIC   st_astext(s.location) AS store_location,
# MAGIC   st_within(s.location, sa.boundary) AS in_service_area,
# MAGIC   st_dwithin(s.location, u.location, 0.05) AS within_5km_approx,
# MAGIC   ROUND(st_distance(s.location, u.location), 4) AS distance_degrees
# MAGIC FROM stores s
# MAGIC CROSS JOIN service_area sa
# MAGIC CROSS JOIN user_location u
# MAGIC WHERE st_within(s.location, sa.boundary)
# MAGIC   AND st_dwithin(s.location, u.location, 0.05)
# MAGIC ORDER BY st_distance(s.location, u.location);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Practical Example: Delivery Zone Assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assign customers to delivery zones
# MAGIC WITH delivery_zones AS (
# MAGIC   SELECT 'Zone North' AS zone_name, 
# MAGIC          st_geomfromtext('POLYGON((-122.5 37.8, -122.5 38.0, -122.3 38.0, -122.3 37.8, -122.5 37.8))') AS boundary
# MAGIC   UNION ALL 
# MAGIC   SELECT 'Zone South',
# MAGIC          st_geomfromtext('POLYGON((-122.5 37.6, -122.5 37.8, -122.3 37.8, -122.3 37.6, -122.5 37.6))')
# MAGIC   UNION ALL
# MAGIC   SELECT 'Zone East',
# MAGIC          st_geomfromtext('POLYGON((-122.3 37.7, -122.3 37.9, -122.1 37.9, -122.1 37.7, -122.3 37.7))')
# MAGIC ),
# MAGIC customers AS (
# MAGIC   SELECT 'Customer 1' AS name, st_geomfromtext('POINT(-122.4194 37.7749)') AS location
# MAGIC   UNION ALL SELECT 'Customer 2', st_geomfromtext('POINT(-122.4000 37.9000)')
# MAGIC   UNION ALL SELECT 'Customer 3', st_geomfromtext('POINT(-122.2500 37.8000)')
# MAGIC   UNION ALL SELECT 'Customer 4', st_geomfromtext('POINT(-122.4500 37.6500)')
# MAGIC   UNION ALL SELECT 'Customer 5', st_geomfromtext('POINT(-122.1500 37.8500)')
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.name AS customer_name,
# MAGIC   dz.zone_name,
# MAGIC   st_astext(c.location) AS customer_location,
# MAGIC   st_contains(dz.boundary, c.location) AS is_in_zone,
# MAGIC   st_distance(c.location, st_centroid(dz.boundary)) AS distance_to_zone_center
# MAGIC FROM customers c
# MAGIC CROSS JOIN delivery_zones dz
# MAGIC WHERE st_contains(dz.boundary, c.location)
# MAGIC ORDER BY c.name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Practical Example: Intersection Detection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find routes that intersect with restricted areas
# MAGIC WITH restricted_areas AS (
# MAGIC   SELECT 'Construction Zone' AS area_name,
# MAGIC          st_geomfromtext('POLYGON((-122.42 37.77, -122.42 37.78, -122.41 37.78, -122.41 37.77, -122.42 37.77))') AS boundary
# MAGIC   UNION ALL
# MAGIC   SELECT 'School Zone',
# MAGIC          st_geomfromtext('POLYGON((-122.40 37.79, -122.40 37.80, -122.39 37.80, -122.39 37.79, -122.40 37.79))')
# MAGIC ),
# MAGIC planned_routes AS (
# MAGIC   SELECT 'Route A' AS route_name,
# MAGIC          st_geomfromtext('LINESTRING(-122.43 37.76, -122.38 37.81)') AS path
# MAGIC   UNION ALL
# MAGIC   SELECT 'Route B',
# MAGIC          st_geomfromtext('LINESTRING(-122.45 37.75, -122.44 37.76)')
# MAGIC   UNION ALL
# MAGIC   SELECT 'Route C',
# MAGIC          st_geomfromtext('LINESTRING(-122.41 37.775, -122.395 37.795)')
# MAGIC )
# MAGIC SELECT 
# MAGIC   pr.route_name,
# MAGIC   ra.area_name,
# MAGIC   st_intersects(pr.path, ra.boundary) AS intersects_restricted,
# MAGIC   st_disjoint(pr.path, ra.boundary) AS avoids_area,
# MAGIC   CASE 
# MAGIC     WHEN st_intersects(pr.path, ra.boundary) THEN 'REVIEW REQUIRED'
# MAGIC     ELSE 'APPROVED'
# MAGIC   END AS route_status
# MAGIC FROM planned_routes pr
# MAGIC CROSS JOIN restricted_areas ra
# MAGIC WHERE st_intersects(pr.path, ra.boundary)
# MAGIC ORDER BY pr.route_name, ra.area_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Comprehensive Relationship Matrix

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test all relationships between two geometries
# MAGIC WITH geom_a AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))') AS geom
# MAGIC ),
# MAGIC geom_b AS (
# MAGIC   SELECT st_geomfromtext('POINT(5 5)') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Point in Polygon' AS scenario,
# MAGIC   st_contains((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS a_contains_b,
# MAGIC   st_within((SELECT geom FROM geom_b), (SELECT geom FROM geom_a)) AS b_within_a,
# MAGIC   st_intersects((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS intersects,
# MAGIC   st_disjoint((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS disjoint,
# MAGIC   st_touches((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS touches,
# MAGIC   st_equals((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS equals,
# MAGIC   st_covers((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS a_covers_b;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with adjacent polygons
# MAGIC WITH geom_a AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
# MAGIC ),
# MAGIC geom_b AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((1 0, 1 1, 2 1, 2 0, 1 0))') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Adjacent Polygons' AS scenario,
# MAGIC   st_contains((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS a_contains_b,
# MAGIC   st_within((SELECT geom FROM geom_b), (SELECT geom FROM geom_a)) AS b_within_a,
# MAGIC   st_intersects((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS intersects,
# MAGIC   st_disjoint((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS disjoint,
# MAGIC   st_touches((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS touches,
# MAGIC   st_equals((SELECT geom FROM geom_a), (SELECT geom FROM geom_b)) AS equals;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Containment Relationships**:
# MAGIC    - `st_contains(A, B)` - A completely contains B
# MAGIC    - `st_within(A, B)` - A is completely within B (inverse of contains)
# MAGIC    - `st_covers(A, B)` - Like contains but includes boundary points
# MAGIC
# MAGIC 2. **Intersection Relationships**:
# MAGIC    - `st_intersects(A, B)` - A and B share any points
# MAGIC    - `st_disjoint(A, B)` - A and B share no points (inverse of intersects)
# MAGIC    - `st_touches(A, B)` - Boundaries touch but interiors don't overlap
# MAGIC
# MAGIC 3. **Equality and Other Tests**:
# MAGIC    - `st_equals(A, B)` - Geometries are spatially equal
# MAGIC
# MAGIC 4. **Distance Relationships**:
# MAGIC    - `st_dwithin(A, B, distance)` - A and B are within specified distance
# MAGIC    - More efficient than `st_distance(A, B) < threshold` for filtering
# MAGIC
# MAGIC 5. **Performance Tips**:
# MAGIC    - Use `st_dwithin()` instead of `st_distance() < X` in WHERE clauses
# MAGIC    - Spatial predicates can leverage spatial indexes
# MAGIC    - Combine multiple predicates to narrow results efficiently
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next notebook: **06_Overlay_and_Processing.py** to learn about combining and processing geometries.
