# Databricks notebook source
# MAGIC %md
# MAGIC # Practical Geospatial Examples
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook demonstrates four complete real-world use cases:
# MAGIC 1. Store Locator - Find nearby stores and calculate distances
# MAGIC 2. Delivery Zone Analysis - Create and manage delivery boundaries
# MAGIC 3. Area Coverage Analysis - Analyze overlapping zones and coverage
# MAGIC 4. Route Analysis - Plan and analyze delivery routes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed notebooks 01-07
# MAGIC - Understanding of all geospatial functions covered so far

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Store Locator
# MAGIC
# MAGIC Build a complete store locator system with distance calculations and proximity filtering.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Create Sample Store Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a stores table
# MAGIC CREATE OR REPLACE TEMPORARY VIEW stores AS
# MAGIC SELECT 1 AS store_id, 'Downtown Store' AS store_name, 
# MAGIC        st_geomfromtext('POINT(-122.4194 37.7749)') AS location,
# MAGIC        'Electronics, Groceries' AS categories
# MAGIC UNION ALL
# MAGIC SELECT 2, 'Uptown Store',
# MAGIC        st_geomfromtext('POINT(-122.4000 37.7900)'),
# MAGIC        'Groceries, Pharmacy'
# MAGIC UNION ALL
# MAGIC SELECT 3, 'Suburban Store',
# MAGIC        st_geomfromtext('POINT(-122.5000 37.7500)'),
# MAGIC        'Electronics, Home Goods'
# MAGIC UNION ALL
# MAGIC SELECT 4, 'Bayview Store',
# MAGIC        st_geomfromtext('POINT(-122.3900 37.7300)'),
# MAGIC        'Groceries, Pharmacy'
# MAGIC UNION ALL
# MAGIC SELECT 5, 'Marina Store',
# MAGIC        st_geomfromtext('POINT(-122.4350 37.8050)'),
# MAGIC        'Electronics, Home Goods, Groceries';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all stores
# MAGIC SELECT 
# MAGIC   store_id,
# MAGIC   store_name,
# MAGIC   st_astext(location) AS coordinates,
# MAGIC   categories
# MAGIC FROM stores
# MAGIC ORDER BY store_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Find Stores Within Distance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find stores within 10km of a user location
# MAGIC WITH user_location AS (
# MAGIC   SELECT st_geomfromtext('POINT(-122.4100 37.7800)') AS location
# MAGIC )
# MAGIC SELECT 
# MAGIC   s.store_name,
# MAGIC   s.categories,
# MAGIC   ROUND(st_distancesphere(s.location, u.location) / 1000, 2) AS distance_km,
# MAGIC   st_astext(s.location) AS store_coordinates
# MAGIC FROM stores s
# MAGIC CROSS JOIN user_location u
# MAGIC WHERE st_dwithin(s.location, u.location, 0.1)  -- Approximately 10km in degrees
# MAGIC ORDER BY distance_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Find Nearest Store

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the 3 nearest stores to user location
# MAGIC WITH user_location AS (
# MAGIC   SELECT st_geomfromtext('POINT(-122.4100 37.7800)') AS location
# MAGIC )
# MAGIC SELECT 
# MAGIC   s.store_name,
# MAGIC   s.categories,
# MAGIC   ROUND(st_distancesphere(s.location, u.location) / 1000, 2) AS distance_km,
# MAGIC   ROUND(DEGREES(st_azimuth(u.location, s.location)), 1) AS bearing_degrees,
# MAGIC   CASE 
# MAGIC     WHEN DEGREES(st_azimuth(u.location, s.location)) BETWEEN 0 AND 45 THEN 'North'
# MAGIC     WHEN DEGREES(st_azimuth(u.location, s.location)) BETWEEN 45 AND 135 THEN 'East'
# MAGIC     WHEN DEGREES(st_azimuth(u.location, s.location)) BETWEEN 135 AND 225 THEN 'South'
# MAGIC     WHEN DEGREES(st_azimuth(u.location, s.location)) BETWEEN 225 AND 315 THEN 'West'
# MAGIC     ELSE 'North'
# MAGIC   END AS direction
# MAGIC FROM stores s
# MAGIC CROSS JOIN user_location u
# MAGIC ORDER BY st_distance(s.location, u.location)
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Delivery Zone Analysis
# MAGIC
# MAGIC Create and manage delivery zones with buffer analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Create Delivery Zones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delivery zones around warehouses
# MAGIC CREATE OR REPLACE TEMPORARY VIEW delivery_zones AS
# MAGIC WITH warehouses AS (
# MAGIC   SELECT 1 AS zone_id, 'Zone A' AS zone_name,
# MAGIC          st_geomfromtext('POINT(-122.4194 37.7749)') AS warehouse_location,
# MAGIC          0.05 AS delivery_radius_degrees
# MAGIC   UNION ALL
# MAGIC   SELECT 2, 'Zone B',
# MAGIC          st_geomfromtext('POINT(-122.4000 37.7900)'),
# MAGIC          0.05
# MAGIC   UNION ALL
# MAGIC   SELECT 3, 'Zone C',
# MAGIC          st_geomfromtext('POINT(-122.4500 37.7600)'),
# MAGIC          0.03
# MAGIC )
# MAGIC SELECT 
# MAGIC   zone_id,
# MAGIC   zone_name,
# MAGIC   warehouse_location,
# MAGIC   st_buffer(warehouse_location, delivery_radius_degrees) AS boundary,
# MAGIC   delivery_radius_degrees
# MAGIC FROM warehouses;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View delivery zone statistics
# MAGIC SELECT 
# MAGIC   zone_name,
# MAGIC   st_astext(warehouse_location) AS warehouse_coords,
# MAGIC   ROUND(st_area(boundary), 6) AS coverage_area_sq_degrees,
# MAGIC   ROUND(st_perimeter(boundary), 4) AS perimeter_degrees,
# MAGIC   delivery_radius_degrees
# MAGIC FROM delivery_zones
# MAGIC ORDER BY coverage_area_sq_degrees DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Check Address Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if addresses are within delivery zones
# MAGIC WITH customer_addresses AS (
# MAGIC   SELECT 'Customer 1' AS name, st_geomfromtext('POINT(-122.4150 37.7800)') AS address
# MAGIC   UNION ALL SELECT 'Customer 2', st_geomfromtext('POINT(-122.4050 37.7950)')
# MAGIC   UNION ALL SELECT 'Customer 3', st_geomfromtext('POINT(-122.4600 37.7550)')
# MAGIC   UNION ALL SELECT 'Customer 4', st_geomfromtext('POINT(-122.3800 37.7400)')
# MAGIC   UNION ALL SELECT 'Customer 5', st_geomfromtext('POINT(-122.4500 37.7650)')
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.name,
# MAGIC   st_astext(c.address) AS address,
# MAGIC   dz.zone_name,
# MAGIC   st_contains(dz.boundary, c.address) AS is_in_zone,
# MAGIC   ROUND(st_distancesphere(dz.warehouse_location, c.address) / 1000, 2) AS distance_to_warehouse_km
# MAGIC FROM customer_addresses c
# MAGIC LEFT JOIN delivery_zones dz ON st_contains(dz.boundary, c.address)
# MAGIC ORDER BY c.name, dz.zone_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Identify Uncovered Areas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find customers not covered by any zone
# MAGIC WITH customer_addresses AS (
# MAGIC   SELECT 'Customer 1' AS name, st_geomfromtext('POINT(-122.4150 37.7800)') AS address
# MAGIC   UNION ALL SELECT 'Customer 2', st_geomfromtext('POINT(-122.4050 37.7950)')
# MAGIC   UNION ALL SELECT 'Customer 3', st_geomfromtext('POINT(-122.4600 37.7550)')
# MAGIC   UNION ALL SELECT 'Customer 4', st_geomfromtext('POINT(-122.3800 37.7400)')
# MAGIC   UNION ALL SELECT 'Customer 5', st_geomfromtext('POINT(-122.4500 37.7650)')
# MAGIC ),
# MAGIC coverage_check AS (
# MAGIC   SELECT 
# MAGIC     c.name,
# MAGIC     c.address,
# MAGIC     MAX(CASE WHEN st_contains(dz.boundary, c.address) THEN 1 ELSE 0 END) AS is_covered
# MAGIC   FROM customer_addresses c
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC   GROUP BY c.name, c.address
# MAGIC )
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   st_astext(address) AS address,
# MAGIC   CASE WHEN is_covered = 1 THEN 'Covered' ELSE 'NOT COVERED' END AS coverage_status
# MAGIC FROM coverage_check
# MAGIC WHERE is_covered = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Area Coverage Analysis
# MAGIC
# MAGIC Analyze overlapping zones and calculate total coverage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Calculate Total Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total coverage area (accounting for overlaps)
# MAGIC WITH individual_areas AS (
# MAGIC   SELECT 
# MAGIC     zone_name,
# MAGIC     st_area(boundary) AS individual_area
# MAGIC   FROM delivery_zones
# MAGIC ),
# MAGIC total_coverage AS (
# MAGIC   SELECT st_union_agg(boundary) AS merged_boundary
# MAGIC   FROM delivery_zones
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Individual Zones (Sum)' AS metric,
# MAGIC   ROUND(SUM(individual_area), 6) AS area_value
# MAGIC FROM individual_areas
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Total Coverage (Merged)',
# MAGIC   ROUND(st_area(merged_boundary), 6)
# MAGIC FROM total_coverage
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Overlap Area',
# MAGIC   ROUND((SELECT SUM(individual_area) FROM individual_areas) - 
# MAGIC         (SELECT st_area(merged_boundary) FROM total_coverage), 6);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Find Overlapping Zones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find which zones overlap and by how much
# MAGIC SELECT 
# MAGIC   a.zone_name AS zone_a,
# MAGIC   b.zone_name AS zone_b,
# MAGIC   st_intersects(a.boundary, b.boundary) AS zones_overlap,
# MAGIC   ROUND(st_area(st_intersection(a.boundary, b.boundary)), 6) AS overlap_area,
# MAGIC   ROUND(st_area(st_intersection(a.boundary, b.boundary)) / st_area(a.boundary) * 100, 2) AS overlap_pct_of_a,
# MAGIC   st_astext(st_centroid(st_intersection(a.boundary, b.boundary))) AS overlap_center
# MAGIC FROM delivery_zones a
# MAGIC CROSS JOIN delivery_zones b
# MAGIC WHERE a.zone_id < b.zone_id
# MAGIC   AND st_intersects(a.boundary, b.boundary);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Calculate Zone Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comprehensive zone statistics
# MAGIC WITH zone_stats AS (
# MAGIC   SELECT 
# MAGIC     dz.zone_name,
# MAGIC     st_area(dz.boundary) AS zone_area,
# MAGIC     st_perimeter(dz.boundary) AS zone_perimeter,
# MAGIC     dz.warehouse_location
# MAGIC   FROM delivery_zones dz
# MAGIC ),
# MAGIC overlaps AS (
# MAGIC   SELECT 
# MAGIC     a.zone_name,
# MAGIC     SUM(st_area(st_intersection(a.boundary, b.boundary))) AS total_overlap
# MAGIC   FROM delivery_zones a
# MAGIC   CROSS JOIN delivery_zones b
# MAGIC   WHERE a.zone_id != b.zone_id
# MAGIC     AND st_intersects(a.boundary, b.boundary)
# MAGIC   GROUP BY a.zone_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   zs.zone_name,
# MAGIC   ROUND(zs.zone_area, 6) AS total_area,
# MAGIC   ROUND(COALESCE(o.total_overlap, 0), 6) AS overlap_with_others,
# MAGIC   ROUND(zs.zone_area - COALESCE(o.total_overlap, 0), 6) AS exclusive_area,
# MAGIC   ROUND(COALESCE(o.total_overlap, 0) / zs.zone_area * 100, 2) AS overlap_percentage,
# MAGIC   ROUND(zs.zone_perimeter, 4) AS perimeter
# MAGIC FROM zone_stats zs
# MAGIC LEFT JOIN overlaps o ON zs.zone_name = o.zone_name
# MAGIC ORDER BY total_area DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Route Analysis
# MAGIC
# MAGIC Plan and analyze delivery routes with waypoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Create Delivery Routes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delivery routes with multiple stops
# MAGIC CREATE OR REPLACE TEMPORARY VIEW delivery_routes AS
# MAGIC WITH route_waypoints AS (
# MAGIC   -- Route 1: Downtown circuit
# MAGIC   SELECT 1 AS route_id, 'Route 1' AS route_name,
# MAGIC          st_makeline(array(
# MAGIC            st_geomfromtext('POINT(-122.4194 37.7749)'),
# MAGIC            st_geomfromtext('POINT(-122.4100 37.7800)'),
# MAGIC            st_geomfromtext('POINT(-122.4000 37.7900)'),
# MAGIC            st_geomfromtext('POINT(-122.4150 37.7950)'),
# MAGIC            st_geomfromtext('POINT(-122.4194 37.7749)')
# MAGIC          )) AS path
# MAGIC   UNION ALL
# MAGIC   -- Route 2: Suburban route
# MAGIC   SELECT 2, 'Route 2',
# MAGIC          st_makeline(array(
# MAGIC            st_geomfromtext('POINT(-122.5000 37.7500)'),
# MAGIC            st_geomfromtext('POINT(-122.4800 37.7400)'),
# MAGIC            st_geomfromtext('POINT(-122.4600 37.7550)'),
# MAGIC            st_geomfromtext('POINT(-122.4750 37.7650)'),
# MAGIC            st_geomfromtext('POINT(-122.5000 37.7500)')
# MAGIC          )) AS path
# MAGIC   UNION ALL
# MAGIC   -- Route 3: Express route
# MAGIC   SELECT 3, 'Route 3',
# MAGIC          st_makeline(array(
# MAGIC            st_geomfromtext('POINT(-122.4350 37.8050)'),
# MAGIC            st_geomfromtext('POINT(-122.4200 37.7850)'),
# MAGIC            st_geomfromtext('POINT(-122.4350 37.8050)')
# MAGIC          )) AS path
# MAGIC )
# MAGIC SELECT * FROM route_waypoints;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze route statistics
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   st_npoints(path) AS waypoint_count,
# MAGIC   ROUND(st_length(path), 4) AS route_length_degrees,
# MAGIC   st_isclosed(path) AS is_round_trip,
# MAGIC   st_astext(st_startpoint(path)) AS start_location,
# MAGIC   st_astext(st_endpoint(path)) AS end_location,
# MAGIC   st_astext(st_centroid(path)) AS route_center
# MAGIC FROM delivery_routes
# MAGIC ORDER BY route_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Calculate Route Distances

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate actual driving distances (approximate)
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   ROUND(st_length(path), 4) AS length_degrees,
# MAGIC   ROUND(st_length(path) * 111.32, 2) AS approx_length_km,
# MAGIC   st_npoints(path) - 1 AS number_of_segments,
# MAGIC   ROUND((st_length(path) * 111.32) / (st_npoints(path) - 1), 2) AS avg_segment_km
# MAGIC FROM delivery_routes
# MAGIC ORDER BY approx_length_km DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Create Route Corridors

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create buffer zones around routes (service corridors)
# MAGIC WITH route_corridors AS (
# MAGIC   SELECT 
# MAGIC     route_name,
# MAGIC     path,
# MAGIC     st_buffer(path, 0.01) AS corridor
# MAGIC   FROM delivery_routes
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   ROUND(st_area(corridor), 6) AS corridor_area,
# MAGIC   ROUND(st_perimeter(corridor), 4) AS corridor_perimeter,
# MAGIC   ROUND(st_length(path) * 111.32, 2) AS route_length_km,
# MAGIC   st_npoints(path) AS stops
# MAGIC FROM route_corridors
# MAGIC ORDER BY corridor_area DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Find Intersecting Routes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find where routes intersect
# MAGIC SELECT 
# MAGIC   a.route_name AS route_a,
# MAGIC   b.route_name AS route_b,
# MAGIC   st_intersects(a.path, b.path) AS routes_intersect,
# MAGIC   CASE 
# MAGIC     WHEN st_intersects(a.path, b.path) 
# MAGIC     THEN st_astext(st_intersection(a.path, b.path))
# MAGIC     ELSE NULL
# MAGIC   END AS intersection_point
# MAGIC FROM delivery_routes a
# MAGIC CROSS JOIN delivery_routes b
# MAGIC WHERE a.route_id < b.route_id
# MAGIC   AND st_intersects(a.path, b.path);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Route Optimization Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze route efficiency
# MAGIC WITH route_analysis AS (
# MAGIC   SELECT 
# MAGIC     route_name,
# MAGIC     path,
# MAGIC     st_startpoint(path) AS start_pt,
# MAGIC     st_endpoint(path) AS end_pt,
# MAGIC     st_length(path) AS actual_path_length,
# MAGIC     st_npoints(path) AS waypoints
# MAGIC   FROM delivery_routes
# MAGIC ),
# MAGIC efficiency_metrics AS (
# MAGIC   SELECT 
# MAGIC     route_name,
# MAGIC     waypoints,
# MAGIC     ROUND(actual_path_length * 111.32, 2) AS actual_distance_km,
# MAGIC     ROUND(st_distance(start_pt, end_pt) * 111.32, 2) AS direct_distance_km,
# MAGIC     ROUND((actual_path_length / NULLIF(st_distance(start_pt, end_pt), 0)), 2) AS route_efficiency_ratio,
# MAGIC     st_isclosed(path) AS is_round_trip
# MAGIC   FROM route_analysis
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   waypoints,
# MAGIC   actual_distance_km,
# MAGIC   direct_distance_km,
# MAGIC   route_efficiency_ratio,
# MAGIC   CASE 
# MAGIC     WHEN is_round_trip THEN 'Round Trip'
# MAGIC     ELSE 'One Way'
# MAGIC   END AS trip_type,
# MAGIC   CASE 
# MAGIC     WHEN route_efficiency_ratio <= 1.2 THEN 'Efficient'
# MAGIC     WHEN route_efficiency_ratio <= 1.5 THEN 'Acceptable'
# MAGIC     ELSE 'Needs Optimization'
# MAGIC   END AS efficiency_rating
# MAGIC FROM efficiency_metrics
# MAGIC ORDER BY route_efficiency_ratio;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprehensive Integration Example
# MAGIC
# MAGIC Combine all concepts: stores, zones, routes, and analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complete logistics analysis: stores, zones, and routes
# MAGIC WITH route_zone_intersections AS (
# MAGIC   SELECT 
# MAGIC     dr.route_name,
# MAGIC     dz.zone_name,
# MAGIC     st_intersects(dr.path, dz.boundary) AS route_enters_zone,
# MAGIC     st_length(st_intersection(dr.path, dz.boundary)) AS path_length_in_zone
# MAGIC   FROM delivery_routes dr
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC   WHERE st_intersects(dr.path, dz.boundary)
# MAGIC ),
# MAGIC stores_on_routes AS (
# MAGIC   SELECT 
# MAGIC     dr.route_name,
# MAGIC     s.store_name,
# MAGIC     st_dwithin(s.location, dr.path, 0.01) AS store_near_route,
# MAGIC     st_distance(s.location, dr.path) * 111.32 AS distance_to_route_km
# MAGIC   FROM delivery_routes dr
# MAGIC   CROSS JOIN stores s
# MAGIC   WHERE st_dwithin(s.location, dr.path, 0.01)
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Route-Zone Intersections' AS analysis_type,
# MAGIC   route_name AS route_or_zone,
# MAGIC   zone_name AS related_entity,
# MAGIC   CAST(NULL AS DOUBLE) AS distance_km,
# MAGIC   'Intersects' AS relationship
# MAGIC FROM route_zone_intersections
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Stores Near Routes',
# MAGIC   route_name,
# MAGIC   store_name,
# MAGIC   ROUND(distance_to_route_km, 2),
# MAGIC   'Within 1km'
# MAGIC FROM stores_on_routes
# MAGIC ORDER BY analysis_type, route_or_zone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Store Locator Pattern**:
# MAGIC    - Use `st_dwithin()` for proximity filtering
# MAGIC    - Calculate distances with `st_distancesphere()` for accuracy
# MAGIC    - Add bearing/direction with `st_azimuth()` for user guidance
# MAGIC    - Sort by distance for "nearest" queries
# MAGIC
# MAGIC 2. **Delivery Zone Pattern**:
# MAGIC    - Create zones with `st_buffer()` around points
# MAGIC    - Check containment with `st_contains()`
# MAGIC    - Find uncovered areas by checking all zones
# MAGIC    - Calculate zone statistics (area, perimeter)
# MAGIC
# MAGIC 3. **Coverage Analysis Pattern**:
# MAGIC    - Merge zones with `st_union_agg()`
# MAGIC    - Find overlaps with `st_intersection()`
# MAGIC    - Calculate exclusive areas with `st_difference()`
# MAGIC    - Compute overlap percentages for optimization
# MAGIC
# MAGIC 4. **Route Analysis Pattern**:
# MAGIC    - Build routes with `st_makeline()` from waypoints
# MAGIC    - Calculate route metrics (length, waypoints, closed)
# MAGIC    - Create corridors with `st_buffer()` on lines
# MAGIC    - Analyze efficiency by comparing actual vs direct distance
# MAGIC
# MAGIC 5. **Integration Best Practices**:
# MAGIC    - Combine multiple geospatial operations in CTEs
# MAGIC    - Use appropriate distance functions for your use case
# MAGIC    - Validate geometries before complex operations
# MAGIC    - Index spatial columns for performance at scale
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the exercise notebooks to practice these concepts:
# MAGIC - **09_Exercise_01_Basic_Point_Operations.py**
# MAGIC - **10_Exercise_02_Polygon_Analysis.py**
# MAGIC - **11_Exercise_03_Route_Planning.py**
# MAGIC - **12_Exercise_04_Spatial_Joins.py**
# MAGIC - **13_Exercise_05_Geohash_Analysis.py**
