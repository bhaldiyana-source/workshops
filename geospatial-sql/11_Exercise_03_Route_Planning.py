# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3: Route Planning
# MAGIC
# MAGIC ## Exercise Overview
# MAGIC Build delivery routes as linestrings, calculate total distances, create buffers along routes, and find intersections with service zones.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Create linestring geometries from waypoints
# MAGIC - Calculate route metrics (length, waypoints, endpoints)
# MAGIC - Create route corridors with buffers
# MAGIC - Analyze route efficiency
# MAGIC - Find intersections between routes and zones
# MAGIC
# MAGIC ## Tasks
# MAGIC 1. Create delivery routes with multiple waypoints
# MAGIC 2. Calculate route distances and statistics
# MAGIC 3. Create buffer corridors along routes
# MAGIC 4. Find routes intersecting with specific zones
# MAGIC 5. Optimize routes and suggest improvements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Create Delivery Routes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delivery routes with waypoints
# MAGIC CREATE OR REPLACE TEMPORARY VIEW delivery_routes AS
# MAGIC SELECT 1 AS route_id, 'Morning Route 1' AS route_name, 'Groceries' AS cargo_type,
# MAGIC        st_makeline(array(
# MAGIC          st_geomfromtext('POINT(-122.4200 37.7900)'),  -- Warehouse
# MAGIC          st_geomfromtext('POINT(-122.4150 37.7850)'),  -- Stop 1
# MAGIC          st_geomfromtext('POINT(-122.4100 37.7800)'),  -- Stop 2
# MAGIC          st_geomfromtext('POINT(-122.4050 37.7750)'),  -- Stop 3
# MAGIC          st_geomfromtext('POINT(-122.4000 37.7700)'),  -- Stop 4
# MAGIC          st_geomfromtext('POINT(-122.4200 37.7900)')   -- Return to warehouse
# MAGIC        )) AS path
# MAGIC UNION ALL
# MAGIC SELECT 2, 'Morning Route 2', 'Electronics',
# MAGIC        st_makeline(array(
# MAGIC          st_geomfromtext('POINT(-122.4200 37.7900)'),
# MAGIC          st_geomfromtext('POINT(-122.4300 37.7950)'),
# MAGIC          st_geomfromtext('POINT(-122.4400 37.8000)'),
# MAGIC          st_geomfromtext('POINT(-122.4500 37.7950)'),
# MAGIC          st_geomfromtext('POINT(-122.4200 37.7900)')
# MAGIC        )) AS path
# MAGIC UNION ALL
# MAGIC SELECT 3, 'Afternoon Route 1', 'Mixed Goods',
# MAGIC        st_makeline(array(
# MAGIC          st_geomfromtext('POINT(-122.4300 37.7800)'),
# MAGIC          st_geomfromtext('POINT(-122.4250 37.7750)'),
# MAGIC          st_geomfromtext('POINT(-122.4200 37.7700)'),
# MAGIC          st_geomfromtext('POINT(-122.4150 37.7650)'),
# MAGIC          st_geomfromtext('POINT(-122.4100 37.7600)'),
# MAGIC          st_geomfromtext('POINT(-122.4050 37.7550)'),
# MAGIC          st_geomfromtext('POINT(-122.4300 37.7800)')
# MAGIC        )) AS path
# MAGIC UNION ALL
# MAGIC SELECT 4, 'Express Route', 'Priority',
# MAGIC        st_makeline(array(
# MAGIC          st_geomfromtext('POINT(-122.4400 37.7900)'),
# MAGIC          st_geomfromtext('POINT(-122.4300 37.7850)'),
# MAGIC          st_geomfromtext('POINT(-122.4400 37.7900)')
# MAGIC        )) AS path
# MAGIC UNION ALL
# MAGIC SELECT 5, 'Evening Route', 'Furniture',
# MAGIC        st_makeline(array(
# MAGIC          st_geomfromtext('POINT(-122.4500 37.7850)'),
# MAGIC          st_geomfromtext('POINT(-122.4600 37.7800)'),
# MAGIC          st_geomfromtext('POINT(-122.4700 37.7850)'),
# MAGIC          st_geomfromtext('POINT(-122.4800 37.7900)'),
# MAGIC          st_geomfromtext('POINT(-122.4700 37.7950)'),
# MAGIC          st_geomfromtext('POINT(-122.4600 37.8000)'),
# MAGIC          st_geomfromtext('POINT(-122.4500 37.7850)')
# MAGIC        )) AS path;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View basic route information
# MAGIC SELECT 
# MAGIC   route_id,
# MAGIC   route_name,
# MAGIC   cargo_type,
# MAGIC   st_npoints(path) AS total_waypoints,
# MAGIC   st_npoints(path) - 1 AS number_of_stops,
# MAGIC   st_isclosed(path) AS is_round_trip,
# MAGIC   st_astext(st_startpoint(path)) AS start_location,
# MAGIC   st_astext(st_endpoint(path)) AS end_location
# MAGIC FROM delivery_routes
# MAGIC ORDER BY route_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Calculate Route Distances and Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate comprehensive route statistics
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   cargo_type,
# MAGIC   st_npoints(path) - 1 AS stops,
# MAGIC   ROUND(st_length(path), 6) AS length_degrees,
# MAGIC   ROUND(st_length(path) * 111.32, 2) AS approx_length_km,
# MAGIC   ROUND(st_length(path) * 69.17, 2) AS approx_length_miles,
# MAGIC   st_isclosed(path) AS is_round_trip,
# MAGIC   st_astext(st_centroid(path)) AS route_center,
# MAGIC   ROUND((st_length(path) * 111.32) / (st_npoints(path) - 1), 2) AS avg_segment_km
# MAGIC FROM delivery_routes
# MAGIC ORDER BY approx_length_km DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate route efficiency (actual distance vs direct distance)
# MAGIC WITH route_metrics AS (
# MAGIC   SELECT 
# MAGIC     route_name,
# MAGIC     path,
# MAGIC     st_length(path) AS actual_length,
# MAGIC     st_distance(st_startpoint(path), st_endpoint(path)) AS direct_length,
# MAGIC     st_isclosed(path) AS is_round_trip,
# MAGIC     st_npoints(path) - 1 AS stops
# MAGIC   FROM delivery_routes
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   stops,
# MAGIC   ROUND(actual_length * 111.32, 2) AS actual_km,
# MAGIC   ROUND(direct_length * 111.32, 2) AS direct_km,
# MAGIC   CASE 
# MAGIC     WHEN is_round_trip THEN 'N/A (Round Trip)'
# MAGIC     ELSE CAST(ROUND(actual_length / NULLIF(direct_length, 0), 2) AS STRING)
# MAGIC   END AS efficiency_ratio,
# MAGIC   CASE 
# MAGIC     WHEN is_round_trip THEN 'Round Trip'
# MAGIC     WHEN actual_length / NULLIF(direct_length, 0) <= 1.2 THEN 'Efficient'
# MAGIC     WHEN actual_length / NULLIF(direct_length, 0) <= 1.5 THEN 'Acceptable'
# MAGIC     ELSE 'Needs Optimization'
# MAGIC   END AS efficiency_rating
# MAGIC FROM route_metrics
# MAGIC ORDER BY actual_km DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate time estimates (assuming 40 km/h average speed + 5 min per stop)
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   cargo_type,
# MAGIC   ROUND(st_length(path) * 111.32, 2) AS distance_km,
# MAGIC   st_npoints(path) - 2 AS delivery_stops,  -- Exclude start and end
# MAGIC   ROUND((st_length(path) * 111.32) / 40 * 60, 0) AS driving_time_min,
# MAGIC   (st_npoints(path) - 2) * 5 AS stop_time_min,
# MAGIC   ROUND((st_length(path) * 111.32) / 40 * 60, 0) + (st_npoints(path) - 2) * 5 AS total_time_min,
# MAGIC   CONCAT(
# MAGIC     CAST(FLOOR((ROUND((st_length(path) * 111.32) / 40 * 60, 0) + (st_npoints(path) - 2) * 5) / 60) AS STRING),
# MAGIC     'h ',
# MAGIC     CAST(MOD(CAST(ROUND((st_length(path) * 111.32) / 40 * 60, 0) + (st_npoints(path) - 2) * 5 AS INT), 60) AS STRING),
# MAGIC     'm'
# MAGIC   ) AS estimated_duration
# MAGIC FROM delivery_routes
# MAGIC ORDER BY total_time_min DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Create Buffer Corridors Along Routes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create service corridors around routes
# MAGIC CREATE OR REPLACE TEMPORARY VIEW route_corridors AS
# MAGIC SELECT 
# MAGIC   route_id,
# MAGIC   route_name,
# MAGIC   cargo_type,
# MAGIC   path,
# MAGIC   st_buffer(path, 0.005) AS narrow_corridor,   -- ~500m
# MAGIC   st_buffer(path, 0.01) AS standard_corridor,  -- ~1km
# MAGIC   st_buffer(path, 0.02) AS wide_corridor       -- ~2km
# MAGIC FROM delivery_routes;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze corridor sizes
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   ROUND(st_length(path) * 111.32, 2) AS route_length_km,
# MAGIC   ROUND(st_area(narrow_corridor) * 12321.0, 2) AS narrow_corridor_sq_km,
# MAGIC   ROUND(st_area(standard_corridor) * 12321.0, 2) AS standard_corridor_sq_km,
# MAGIC   ROUND(st_area(wide_corridor) * 12321.0, 2) AS wide_corridor_sq_km,
# MAGIC   ROUND(st_perimeter(standard_corridor) * 111.32, 2) AS standard_perimeter_km
# MAGIC FROM route_corridors
# MAGIC ORDER BY route_length_km DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find overlapping route corridors
# MAGIC SELECT 
# MAGIC   a.route_name AS route_a,
# MAGIC   b.route_name AS route_b,
# MAGIC   st_intersects(a.standard_corridor, b.standard_corridor) AS corridors_overlap,
# MAGIC   ROUND(st_area(st_intersection(a.standard_corridor, b.standard_corridor)) * 12321.0, 2) AS overlap_area_sq_km,
# MAGIC   st_astext(st_centroid(st_intersection(a.standard_corridor, b.standard_corridor))) AS overlap_center
# MAGIC FROM route_corridors a
# MAGIC CROSS JOIN route_corridors b
# MAGIC WHERE a.route_id < b.route_id
# MAGIC   AND st_intersects(a.standard_corridor, b.standard_corridor)
# MAGIC ORDER BY overlap_area_sq_km DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Find Routes Intersecting with Zones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create service zones for intersection analysis
# MAGIC CREATE OR REPLACE TEMPORARY VIEW service_zones AS
# MAGIC SELECT 'Residential Zone A' AS zone_name,
# MAGIC        st_geomfromtext('POLYGON((-122.425 37.780, -122.425 37.795, -122.410 37.795, -122.410 37.780, -122.425 37.780))') AS boundary,
# MAGIC        'Residential' AS zone_type
# MAGIC UNION ALL
# MAGIC SELECT 'Commercial District B',
# MAGIC        st_geomfromtext('POLYGON((-122.445 37.795, -122.445 37.810, -122.430 37.810, -122.430 37.795, -122.445 37.795))'),
# MAGIC        'Commercial'
# MAGIC UNION ALL
# MAGIC SELECT 'Industrial Area C',
# MAGIC        st_geomfromtext('POLYGON((-122.480 37.785, -122.480 37.805, -122.460 37.805, -122.460 37.785, -122.480 37.785))'),
# MAGIC        'Industrial'
# MAGIC UNION ALL
# MAGIC SELECT 'School Zone D',
# MAGIC        st_geomfromtext('POLYGON((-122.415 37.755, -122.415 37.770, -122.405 37.770, -122.405 37.755, -122.415 37.755))'),
# MAGIC        'School';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find which routes pass through which zones
# MAGIC SELECT 
# MAGIC   dr.route_name,
# MAGIC   dr.cargo_type,
# MAGIC   sz.zone_name,
# MAGIC   sz.zone_type,
# MAGIC   st_intersects(dr.path, sz.boundary) AS route_enters_zone,
# MAGIC   ROUND(st_length(st_intersection(dr.path, sz.boundary)) * 111.32, 2) AS distance_in_zone_km,
# MAGIC   ROUND(st_length(st_intersection(dr.path, sz.boundary)) / st_length(dr.path) * 100, 1) AS pct_of_route
# MAGIC FROM delivery_routes dr
# MAGIC CROSS JOIN service_zones sz
# MAGIC WHERE st_intersects(dr.path, sz.boundary)
# MAGIC ORDER BY dr.route_name, distance_in_zone_km DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check route corridors against restricted zones
# MAGIC WITH restricted_zones AS (
# MAGIC   SELECT zone_name, boundary
# MAGIC   FROM service_zones
# MAGIC   WHERE zone_type = 'School'
# MAGIC )
# MAGIC SELECT 
# MAGIC   rc.route_name,
# MAGIC   rc.cargo_type,
# MAGIC   rz.zone_name AS restricted_zone,
# MAGIC   st_intersects(rc.standard_corridor, rz.boundary) AS corridor_in_restricted,
# MAGIC   CASE 
# MAGIC     WHEN st_intersects(rc.standard_corridor, rz.boundary) THEN 'WARNING: Review Route'
# MAGIC     ELSE 'OK'
# MAGIC   END AS compliance_status,
# MAGIC   ROUND(st_distance(rc.path, rz.boundary) * 111.32, 3) AS min_distance_km
# MAGIC FROM route_corridors rc
# MAGIC CROSS JOIN restricted_zones rz
# MAGIC ORDER BY min_distance_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Optimize Routes and Suggest Improvements

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify routes with optimization opportunities
# MAGIC WITH route_analysis AS (
# MAGIC   SELECT 
# MAGIC     route_name,
# MAGIC     cargo_type,
# MAGIC     path,
# MAGIC     st_length(path) AS total_length,
# MAGIC     st_npoints(path) - 1 AS stops,
# MAGIC     st_isclosed(path) AS is_round_trip
# MAGIC   FROM delivery_routes
# MAGIC ),
# MAGIC segment_analysis AS (
# MAGIC   SELECT 
# MAGIC     route_name,
# MAGIC     total_length,
# MAGIC     stops,
# MAGIC     total_length / stops AS avg_segment_length,
# MAGIC     is_round_trip
# MAGIC   FROM route_analysis
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   stops,
# MAGIC   ROUND(total_length * 111.32, 2) AS total_km,
# MAGIC   ROUND(avg_segment_length * 111.32, 2) AS avg_segment_km,
# MAGIC   CASE 
# MAGIC     WHEN NOT is_round_trip THEN 'Consider round trip for efficiency'
# MAGIC     WHEN stops > 6 THEN 'Consider splitting into multiple routes'
# MAGIC     WHEN avg_segment_length * 111.32 > 5 THEN 'Long segments - consider adding intermediate stops'
# MAGIC     WHEN avg_segment_length * 111.32 < 1 THEN 'Short segments - route is well optimized'
# MAGIC     ELSE 'Route appears optimal'
# MAGIC   END AS recommendation
# MAGIC FROM segment_analysis
# MAGIC ORDER BY total_km DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Suggest route consolidation opportunities
# MAGIC WITH route_proximity AS (
# MAGIC   SELECT 
# MAGIC     a.route_name AS route_a,
# MAGIC     b.route_name AS route_b,
# MAGIC     a.cargo_type AS cargo_a,
# MAGIC     b.cargo_type AS cargo_b,
# MAGIC     st_distance(a.path, b.path) AS min_distance,
# MAGIC     st_distance(st_centroid(a.path), st_centroid(b.path)) AS center_distance
# MAGIC   FROM delivery_routes a
# MAGIC   CROSS JOIN delivery_routes b
# MAGIC   WHERE a.route_id < b.route_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_a,
# MAGIC   route_b,
# MAGIC   cargo_a,
# MAGIC   cargo_b,
# MAGIC   ROUND(min_distance * 111.32, 2) AS min_distance_km,
# MAGIC   ROUND(center_distance * 111.32, 2) AS center_distance_km,
# MAGIC   CASE 
# MAGIC     WHEN cargo_a = cargo_b AND center_distance * 111.32 < 3 THEN 'HIGH PRIORITY: Consider consolidation'
# MAGIC     WHEN center_distance * 111.32 < 2 THEN 'MEDIUM: Review for potential consolidation'
# MAGIC     ELSE 'LOW: Routes are geographically separated'
# MAGIC   END AS consolidation_priority
# MAGIC FROM route_proximity
# MAGIC WHERE center_distance * 111.32 < 5  -- Only show routes within 5km
# MAGIC ORDER BY center_distance_km;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate potential savings from route optimization
# MAGIC WITH current_metrics AS (
# MAGIC   SELECT 
# MAGIC     SUM(st_length(path) * 111.32) AS total_distance_km,
# MAGIC     COUNT(*) AS total_routes,
# MAGIC     AVG(st_npoints(path) - 1) AS avg_stops_per_route
# MAGIC   FROM delivery_routes
# MAGIC ),
# MAGIC simulated_optimized AS (
# MAGIC   -- Simulate 15% improvement through optimization
# MAGIC   SELECT 
# MAGIC     total_distance_km * 0.85 AS optimized_distance_km,
# MAGIC     total_distance_km * 0.15 AS savings_km,
# MAGIC     total_routes
# MAGIC   FROM current_metrics
# MAGIC )
# MAGIC SELECT 
# MAGIC   cm.total_routes,
# MAGIC   ROUND(cm.total_distance_km, 2) AS current_total_km,
# MAGIC   ROUND(cm.avg_stops_per_route, 1) AS avg_stops,
# MAGIC   ROUND(so.optimized_distance_km, 2) AS optimized_total_km,
# MAGIC   ROUND(so.savings_km, 2) AS potential_savings_km,
# MAGIC   CONCAT(ROUND(so.savings_km / cm.total_distance_km * 100, 1), '%') AS savings_percentage,
# MAGIC   ROUND(so.savings_km * 2.5, 2) AS estimated_cost_savings_usd  -- Assuming $2.50/km
# MAGIC FROM current_metrics cm
# MAGIC CROSS JOIN simulated_optimized so;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Route Coverage Heat Map

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate coverage intensity (how many routes serve each area)
# MAGIC WITH all_corridors AS (
# MAGIC   SELECT route_name, standard_corridor AS corridor
# MAGIC   FROM route_corridors
# MAGIC ),
# MAGIC corridor_overlaps AS (
# MAGIC   SELECT 
# MAGIC     a.route_name,
# MAGIC     COUNT(DISTINCT b.route_name) AS overlapping_routes,
# MAGIC     st_area(a.corridor) * 12321.0 AS corridor_area_sq_km
# MAGIC   FROM all_corridors a
# MAGIC   LEFT JOIN all_corridors b 
# MAGIC     ON st_intersects(a.corridor, b.corridor) AND a.route_name != b.route_name
# MAGIC   GROUP BY a.route_name, a.corridor
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   overlapping_routes,
# MAGIC   ROUND(corridor_area_sq_km, 2) AS area_sq_km,
# MAGIC   CASE 
# MAGIC     WHEN overlapping_routes >= 3 THEN 'HIGH COVERAGE'
# MAGIC     WHEN overlapping_routes >= 1 THEN 'MEDIUM COVERAGE'
# MAGIC     ELSE 'LOW COVERAGE'
# MAGIC   END AS coverage_intensity
# MAGIC FROM corridor_overlaps
# MAGIC ORDER BY overlapping_routes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Extract Individual Waypoints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract all waypoints from routes for analysis
# MAGIC WITH route_waypoints AS (
# MAGIC   SELECT 
# MAGIC     route_id,
# MAGIC     route_name,
# MAGIC     SEQUENCE(1, st_npoints(path)) AS waypoint_indices,
# MAGIC     path
# MAGIC   FROM delivery_routes
# MAGIC ),
# MAGIC exploded_waypoints AS (
# MAGIC   SELECT 
# MAGIC     route_id,
# MAGIC     route_name,
# MAGIC     waypoint_idx,
# MAGIC     st_pointn(path, waypoint_idx) AS waypoint
# MAGIC   FROM route_waypoints
# MAGIC   LATERAL VIEW EXPLODE(waypoint_indices) AS waypoint_idx
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   waypoint_idx,
# MAGIC   st_astext(waypoint) AS location,
# MAGIC   ROUND(st_x(waypoint), 6) AS longitude,
# MAGIC   ROUND(st_y(waypoint), 6) AS latitude,
# MAGIC   CASE 
# MAGIC     WHEN waypoint_idx = 1 THEN 'START'
# MAGIC     WHEN waypoint_idx = (SELECT MAX(wi) FROM (SELECT EXPLODE(waypoint_indices) AS wi FROM route_waypoints WHERE route_id = ew.route_id)) THEN 'END'
# MAGIC     ELSE 'STOP'
# MAGIC   END AS waypoint_type
# MAGIC FROM exploded_waypoints ew
# MAGIC ORDER BY route_id, waypoint_idx;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Route Reversal Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare routes with their reversed versions
# MAGIC WITH reversed_routes AS (
# MAGIC   SELECT 
# MAGIC     route_name,
# MAGIC     path AS original_path,
# MAGIC     st_reverse(path) AS reversed_path
# MAGIC   FROM delivery_routes
# MAGIC )
# MAGIC SELECT 
# MAGIC   route_name,
# MAGIC   st_astext(st_startpoint(original_path)) AS original_start,
# MAGIC   st_astext(st_endpoint(original_path)) AS original_end,
# MAGIC   st_astext(st_startpoint(reversed_path)) AS reversed_start,
# MAGIC   st_astext(st_endpoint(reversed_path)) AS reversed_end,
# MAGIC   ROUND(st_length(original_path), 6) AS original_length,
# MAGIC   ROUND(st_length(reversed_path), 6) AS reversed_length,
# MAGIC   ROUND(st_length(original_path), 6) = ROUND(st_length(reversed_path), 6) AS lengths_equal
# MAGIC FROM reversed_routes
# MAGIC ORDER BY route_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Summary
# MAGIC
# MAGIC ### Key Functions Used
# MAGIC - `st_makeline()` - Create routes from waypoint arrays
# MAGIC - `st_length()` - Calculate route distances
# MAGIC - `st_npoints()`, `st_pointn()` - Access waypoints
# MAGIC - `st_startpoint()`, `st_endpoint()` - Get route endpoints
# MAGIC - `st_buffer()` - Create route corridors
# MAGIC - `st_isclosed()` - Check if route is round trip
# MAGIC - `st_reverse()` - Reverse route direction
# MAGIC - `st_intersects()`, `st_intersection()` - Route-zone analysis
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC 1. Creating linestring routes from waypoints
# MAGIC 2. Calculating route metrics and statistics
# MAGIC 3. Analyzing route efficiency
# MAGIC 4. Creating and analyzing buffer corridors
# MAGIC 5. Route-zone intersection analysis
# MAGIC 6. Identifying optimization opportunities
# MAGIC 7. Route consolidation analysis
# MAGIC 8. Waypoint extraction and manipulation
# MAGIC
# MAGIC ### Real-World Applications
# MAGIC - Delivery route planning
# MAGIC - Fleet optimization
# MAGIC - Service coverage analysis
# MAGIC - Compliance checking (restricted zones)
# MAGIC - Cost estimation
# MAGIC - Route consolidation
# MAGIC - Time estimation
# MAGIC
# MAGIC ## Next Exercise
# MAGIC Continue to **12_Exercise_04_Spatial_Joins.py** to practice complex spatial joins and customer assignment.
