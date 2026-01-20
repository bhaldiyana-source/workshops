# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 2: Polygon Analysis
# MAGIC
# MAGIC ## Exercise Overview
# MAGIC Create delivery zones for multiple locations, identify overlapping coverage areas, and calculate comprehensive zone statistics.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Create polygon geometries using buffers
# MAGIC - Analyze polygon properties (area, perimeter)
# MAGIC - Detect overlapping regions
# MAGIC - Calculate coverage statistics
# MAGIC - Perform containment queries
# MAGIC
# MAGIC ## Tasks
# MAGIC 1. Create delivery zones around warehouse locations
# MAGIC 2. Calculate zone statistics (area, perimeter, etc.)
# MAGIC 3. Identify overlapping zones and quantify overlaps
# MAGIC 4. Assign customers to appropriate zones
# MAGIC 5. Optimize zone coverage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Create Delivery Zones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create warehouse locations and delivery zones
# MAGIC CREATE OR REPLACE TEMPORARY VIEW warehouses AS
# MAGIC SELECT 1 AS warehouse_id, 'North Warehouse' AS name,
# MAGIC        st_geomfromtext('POINT(-122.4200 37.8000)') AS location,
# MAGIC        'Electronics, General Goods' AS specialties
# MAGIC UNION ALL SELECT 2, 'South Warehouse',
# MAGIC        st_geomfromtext('POINT(-122.4300 37.7500)'),
# MAGIC        'Groceries, Pharmacy'
# MAGIC UNION ALL SELECT 3, 'East Warehouse',
# MAGIC        st_geomfromtext('POINT(-122.3800 37.7750)'),
# MAGIC        'Furniture, Home Goods'
# MAGIC UNION ALL SELECT 4, 'West Warehouse',
# MAGIC        st_geomfromtext('POINT(-122.4800 37.7800)'),
# MAGIC        'Electronics, Groceries'
# MAGIC UNION ALL SELECT 5, 'Central Warehouse',
# MAGIC        st_geomfromtext('POINT(-122.4300 37.7800)'),
# MAGIC        'General Goods, Pharmacy';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delivery zones with different radii based on capacity
# MAGIC CREATE OR REPLACE TEMPORARY VIEW delivery_zones AS
# MAGIC WITH zone_radii AS (
# MAGIC   SELECT 
# MAGIC     warehouse_id,
# MAGIC     name,
# MAGIC     location,
# MAGIC     specialties,
# MAGIC     CASE warehouse_id
# MAGIC       WHEN 1 THEN 0.04  -- 4km radius approx
# MAGIC       WHEN 2 THEN 0.05  -- 5km radius
# MAGIC       WHEN 3 THEN 0.03  -- 3km radius
# MAGIC       WHEN 4 THEN 0.045 -- 4.5km radius
# MAGIC       WHEN 5 THEN 0.06  -- 6km radius (largest)
# MAGIC     END AS radius_degrees
# MAGIC   FROM warehouses
# MAGIC )
# MAGIC SELECT 
# MAGIC   warehouse_id,
# MAGIC   name AS zone_name,
# MAGIC   location AS warehouse_location,
# MAGIC   st_buffer(location, radius_degrees) AS zone_boundary,
# MAGIC   radius_degrees,
# MAGIC   specialties
# MAGIC FROM zone_radii;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View created zones
# MAGIC SELECT 
# MAGIC   zone_name,
# MAGIC   st_astext(warehouse_location) AS warehouse_coords,
# MAGIC   specialties,
# MAGIC   ROUND(radius_degrees * 111.32, 2) AS approx_radius_km
# MAGIC FROM delivery_zones
# MAGIC ORDER BY warehouse_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Calculate Zone Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate comprehensive zone statistics
# MAGIC SELECT 
# MAGIC   zone_name,
# MAGIC   ROUND(st_area(zone_boundary), 6) AS area_sq_degrees,
# MAGIC   ROUND(st_area(zone_boundary) * 12321.0, 2) AS approx_area_sq_km,
# MAGIC   ROUND(st_perimeter(zone_boundary), 4) AS perimeter_degrees,
# MAGIC   ROUND(st_perimeter(zone_boundary) * 111.32, 2) AS approx_perimeter_km,
# MAGIC   st_npoints(st_boundary(zone_boundary)) AS boundary_points,
# MAGIC   st_astext(st_centroid(zone_boundary)) AS zone_center,
# MAGIC   st_isvalid(zone_boundary) AS is_valid_geometry
# MAGIC FROM delivery_zones
# MAGIC ORDER BY approx_area_sq_km DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total coverage area
# MAGIC WITH individual_areas AS (
# MAGIC   SELECT SUM(st_area(zone_boundary)) AS total_individual
# MAGIC   FROM delivery_zones
# MAGIC ),
# MAGIC merged_coverage AS (
# MAGIC   SELECT st_area(st_union_agg(zone_boundary)) AS total_merged
# MAGIC   FROM delivery_zones
# MAGIC )
# MAGIC SELECT 
# MAGIC   ROUND(ia.total_individual, 6) AS sum_individual_areas,
# MAGIC   ROUND(mc.total_merged, 6) AS merged_coverage_area,
# MAGIC   ROUND(ia.total_individual - mc.total_merged, 6) AS total_overlap_area,
# MAGIC   ROUND((ia.total_individual - mc.total_merged) / ia.total_individual * 100, 2) AS overlap_percentage
# MAGIC FROM individual_areas ia
# MAGIC CROSS JOIN merged_coverage mc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Identify Overlapping Zones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all overlapping zone pairs
# MAGIC SELECT 
# MAGIC   a.zone_name AS zone_a,
# MAGIC   b.zone_name AS zone_b,
# MAGIC   st_intersects(a.zone_boundary, b.zone_boundary) AS zones_overlap,
# MAGIC   ROUND(st_area(st_intersection(a.zone_boundary, b.zone_boundary)), 6) AS overlap_area_sq_deg,
# MAGIC   ROUND(st_area(st_intersection(a.zone_boundary, b.zone_boundary)) * 12321.0, 2) AS overlap_area_sq_km,
# MAGIC   ROUND(st_area(st_intersection(a.zone_boundary, b.zone_boundary)) / st_area(a.zone_boundary) * 100, 2) AS pct_of_zone_a,
# MAGIC   ROUND(st_area(st_intersection(a.zone_boundary, b.zone_boundary)) / st_area(b.zone_boundary) * 100, 2) AS pct_of_zone_b,
# MAGIC   st_astext(st_centroid(st_intersection(a.zone_boundary, b.zone_boundary))) AS overlap_center
# MAGIC FROM delivery_zones a
# MAGIC CROSS JOIN delivery_zones b
# MAGIC WHERE a.warehouse_id < b.warehouse_id
# MAGIC   AND st_intersects(a.zone_boundary, b.zone_boundary)
# MAGIC ORDER BY overlap_area_sq_km DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate exclusive (non-overlapping) area for each zone
# MAGIC WITH overlapping_areas AS (
# MAGIC   SELECT 
# MAGIC     a.warehouse_id,
# MAGIC     a.zone_name,
# MAGIC     st_union_agg(
# MAGIC       CASE 
# MAGIC         WHEN st_intersects(a.zone_boundary, b.zone_boundary) 
# MAGIC         THEN st_intersection(a.zone_boundary, b.zone_boundary)
# MAGIC         ELSE st_geomfromtext('POLYGON EMPTY')
# MAGIC       END
# MAGIC     ) AS overlap_geom
# MAGIC   FROM delivery_zones a
# MAGIC   CROSS JOIN delivery_zones b
# MAGIC   WHERE a.warehouse_id != b.warehouse_id
# MAGIC   GROUP BY a.warehouse_id, a.zone_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   dz.zone_name,
# MAGIC   ROUND(st_area(dz.zone_boundary), 6) AS total_area,
# MAGIC   ROUND(COALESCE(st_area(oa.overlap_geom), 0), 6) AS overlapping_area,
# MAGIC   ROUND(st_area(dz.zone_boundary) - COALESCE(st_area(oa.overlap_geom), 0), 6) AS exclusive_area,
# MAGIC   ROUND((st_area(dz.zone_boundary) - COALESCE(st_area(oa.overlap_geom), 0)) / st_area(dz.zone_boundary) * 100, 2) AS exclusive_pct
# MAGIC FROM delivery_zones dz
# MAGIC LEFT JOIN overlapping_areas oa ON dz.warehouse_id = oa.warehouse_id
# MAGIC ORDER BY exclusive_area DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Assign Customers to Zones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customer locations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customers AS
# MAGIC SELECT 1 AS customer_id, 'Alice' AS customer_name,
# MAGIC        st_geomfromtext('POINT(-122.4250 37.7900)') AS location
# MAGIC UNION ALL SELECT 2, 'Bob',
# MAGIC        st_geomfromtext('POINT(-122.4100 37.7650)')
# MAGIC UNION ALL SELECT 3, 'Charlie',
# MAGIC        st_geomfromtext('POINT(-122.3900 37.7700)')
# MAGIC UNION ALL SELECT 4, 'Diana',
# MAGIC        st_geomfromtext('POINT(-122.4700 37.7850)')
# MAGIC UNION ALL SELECT 5, 'Eve',
# MAGIC        st_geomfromtext('POINT(-122.4400 37.7750)')
# MAGIC UNION ALL SELECT 6, 'Frank',
# MAGIC        st_geomfromtext('POINT(-122.4200 37.8050)')
# MAGIC UNION ALL SELECT 7, 'Grace',
# MAGIC        st_geomfromtext('POINT(-122.4500 37.7450)')
# MAGIC UNION ALL SELECT 8, 'Henry',
# MAGIC        st_geomfromtext('POINT(-122.3700 37.7800)')
# MAGIC UNION ALL SELECT 9, 'Iris',
# MAGIC        st_geomfromtext('POINT(-122.5000 37.7900)')
# MAGIC UNION ALL SELECT 10, 'Jack',
# MAGIC        st_geomfromtext('POINT(-122.4300 37.7850)');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assign customers to all zones they fall within
# MAGIC SELECT 
# MAGIC   c.customer_name,
# MAGIC   st_astext(c.location) AS customer_location,
# MAGIC   dz.zone_name,
# MAGIC   dz.specialties,
# MAGIC   st_contains(dz.zone_boundary, c.location) AS is_in_zone,
# MAGIC   ROUND(st_distancesphere(dz.warehouse_location, c.location) / 1000, 2) AS distance_to_warehouse_km
# MAGIC FROM customers c
# MAGIC CROSS JOIN delivery_zones dz
# MAGIC WHERE st_contains(dz.zone_boundary, c.location)
# MAGIC ORDER BY c.customer_name, distance_to_warehouse_km;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find customers in multiple zones (overlapping coverage)
# MAGIC WITH customer_zone_count AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.location,
# MAGIC     COUNT(*) AS zone_count,
# MAGIC     STRING_AGG(dz.zone_name, ', ') AS available_zones
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC   WHERE st_contains(dz.zone_boundary, c.location)
# MAGIC   GROUP BY c.customer_id, c.customer_name, c.location
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   zone_count,
# MAGIC   available_zones,
# MAGIC   CASE 
# MAGIC     WHEN zone_count = 0 THEN 'NO COVERAGE'
# MAGIC     WHEN zone_count = 1 THEN 'Single Zone'
# MAGIC     ELSE 'Multiple Zones'
# MAGIC   END AS coverage_type
# MAGIC FROM customer_zone_count
# MAGIC ORDER BY zone_count DESC, customer_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assign each customer to their nearest warehouse
# MAGIC WITH customer_distances AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     dz.zone_name,
# MAGIC     dz.specialties,
# MAGIC     st_distancesphere(dz.warehouse_location, c.location) / 1000 AS distance_km,
# MAGIC     st_contains(dz.zone_boundary, c.location) AS is_in_zone,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY st_distance(dz.warehouse_location, c.location)) AS rank
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   zone_name AS assigned_warehouse,
# MAGIC   specialties,
# MAGIC   ROUND(distance_km, 2) AS distance_km,
# MAGIC   is_in_zone,
# MAGIC   CASE 
# MAGIC     WHEN is_in_zone THEN 'Standard Delivery'
# MAGIC     ELSE 'Extended Delivery'
# MAGIC   END AS service_type
# MAGIC FROM customer_distances
# MAGIC WHERE rank = 1
# MAGIC ORDER BY customer_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Optimize Zone Coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify gaps in coverage (customers not covered)
# MAGIC WITH covered_customers AS (
# MAGIC   SELECT DISTINCT c.customer_id
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC   WHERE st_contains(dz.zone_boundary, c.location)
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   st_astext(c.location) AS location,
# MAGIC   'NOT COVERED' AS coverage_status,
# MAGIC   MIN(st_distancesphere(dz.warehouse_location, c.location) / 1000) AS nearest_warehouse_km
# MAGIC FROM customers c
# MAGIC CROSS JOIN delivery_zones dz
# MAGIC WHERE c.customer_id NOT IN (SELECT customer_id FROM covered_customers)
# MAGIC GROUP BY c.customer_id, c.customer_name, c.location
# MAGIC ORDER BY nearest_warehouse_km;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Suggest zone expansion to cover uncovered customers
# MAGIC WITH covered_customers AS (
# MAGIC   SELECT DISTINCT c.customer_id
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC   WHERE st_contains(dz.zone_boundary, c.location)
# MAGIC ),
# MAGIC uncovered AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.location,
# MAGIC     dz.warehouse_id,
# MAGIC     dz.zone_name,
# MAGIC     st_distance(dz.warehouse_location, c.location) AS distance_deg,
# MAGIC     dz.radius_degrees,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY st_distance(dz.warehouse_location, c.location)) AS rank
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC   WHERE c.customer_id NOT IN (SELECT customer_id FROM covered_customers)
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   zone_name AS nearest_warehouse,
# MAGIC   ROUND(radius_degrees * 111.32, 2) AS current_radius_km,
# MAGIC   ROUND(distance_deg * 111.32, 2) AS required_radius_km,
# MAGIC   ROUND((distance_deg - radius_degrees) * 111.32, 2) AS expansion_needed_km,
# MAGIC   ROUND((distance_deg / radius_degrees - 1) * 100, 1) AS expansion_pct
# MAGIC FROM uncovered
# MAGIC WHERE rank = 1
# MAGIC ORDER BY expansion_needed_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Zone Efficiency Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze zone efficiency: customers served per unit area
# MAGIC WITH customers_per_zone AS (
# MAGIC   SELECT 
# MAGIC     dz.zone_name,
# MAGIC     st_area(dz.zone_boundary) * 12321.0 AS area_sq_km,
# MAGIC     COUNT(DISTINCT c.customer_id) AS customers_served
# MAGIC   FROM delivery_zones dz
# MAGIC   LEFT JOIN customers c ON st_contains(dz.zone_boundary, c.location)
# MAGIC   GROUP BY dz.zone_name, dz.zone_boundary
# MAGIC )
# MAGIC SELECT 
# MAGIC   zone_name,
# MAGIC   ROUND(area_sq_km, 2) AS area_sq_km,
# MAGIC   customers_served,
# MAGIC   CASE 
# MAGIC     WHEN customers_served > 0 
# MAGIC     THEN ROUND(area_sq_km / customers_served, 2)
# MAGIC     ELSE NULL
# MAGIC   END AS sq_km_per_customer,
# MAGIC   CASE 
# MAGIC     WHEN customers_served > 0 
# MAGIC     THEN ROUND(customers_served / area_sq_km, 3)
# MAGIC     ELSE 0
# MAGIC   END AS customers_per_sq_km,
# MAGIC   CASE 
# MAGIC     WHEN customers_served = 0 THEN 'UNDERUTILIZED'
# MAGIC     WHEN customers_served / area_sq_km > 0.015 THEN 'HIGH EFFICIENCY'
# MAGIC     WHEN customers_served / area_sq_km > 0.010 THEN 'MEDIUM EFFICIENCY'
# MAGIC     ELSE 'LOW EFFICIENCY'
# MAGIC   END AS efficiency_rating
# MAGIC FROM customers_per_zone
# MAGIC ORDER BY customers_per_sq_km DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Create Optimal Convex Hull Zone

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create minimal convex hull covering all customers
# MAGIC WITH all_customer_points AS (
# MAGIC   SELECT st_union_agg(location) AS all_points
# MAGIC   FROM customers
# MAGIC ),
# MAGIC hull_zone AS (
# MAGIC   SELECT st_convexhull(all_points) AS hull_boundary
# MAGIC   FROM all_customer_points
# MAGIC ),
# MAGIC buffered_hull AS (
# MAGIC   SELECT st_buffer(hull_boundary, 0.01) AS buffered_boundary
# MAGIC   FROM hull_zone
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Optimal Convex Hull Zone' AS zone_type,
# MAGIC   ROUND(st_area((SELECT hull_boundary FROM hull_zone)), 6) AS hull_area,
# MAGIC   ROUND(st_area(buffered_boundary), 6) AS buffered_area,
# MAGIC   ROUND(st_perimeter((SELECT hull_boundary FROM hull_zone)), 4) AS hull_perimeter,
# MAGIC   st_astext(st_centroid((SELECT hull_boundary FROM hull_zone))) AS hull_center,
# MAGIC   (SELECT COUNT(*) FROM customers) AS customers_covered
# MAGIC FROM buffered_hull;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Specialty Coverage Matrix

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which specialties are available to each customer?
# MAGIC WITH customer_specialties AS (
# MAGIC   SELECT 
# MAGIC     c.customer_name,
# MAGIC     dz.zone_name,
# MAGIC     dz.specialties
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN delivery_zones dz
# MAGIC   WHERE st_contains(dz.zone_boundary, c.location)
# MAGIC ),
# MAGIC specialty_array AS (
# MAGIC   SELECT 
# MAGIC     customer_name,
# MAGIC     ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(SPLIT(specialties, ', ')))) AS available_specialties,
# MAGIC     COUNT(DISTINCT zone_name) AS zones_available
# MAGIC   FROM customer_specialties
# MAGIC   GROUP BY customer_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   zones_available,
# MAGIC   ARRAY_JOIN(available_specialties, ', ') AS available_specialties,
# MAGIC   SIZE(available_specialties) AS specialty_count
# MAGIC FROM specialty_array
# MAGIC ORDER BY specialty_count DESC, customer_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Summary
# MAGIC
# MAGIC ### Key Functions Used
# MAGIC - `st_buffer()` - Create zones around points
# MAGIC - `st_area()`, `st_perimeter()` - Calculate polygon metrics
# MAGIC - `st_intersects()`, `st_intersection()` - Find overlaps
# MAGIC - `st_contains()` - Test point-in-polygon
# MAGIC - `st_union_agg()` - Merge multiple polygons
# MAGIC - `st_difference()` - Calculate exclusive areas
# MAGIC - `st_convexhull()` - Create minimal covering polygon
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC 1. Creating and managing polygon zones
# MAGIC 2. Calculating area and perimeter statistics
# MAGIC 3. Detecting and quantifying overlaps
# MAGIC 4. Point-in-polygon queries
# MAGIC 5. Coverage optimization analysis
# MAGIC 6. Zone efficiency calculations
# MAGIC 7. Gap analysis and expansion planning
# MAGIC
# MAGIC ### Real-World Applications
# MAGIC - Delivery zone management
# MAGIC - Service area planning
# MAGIC - Territory optimization
# MAGIC - Coverage gap analysis
# MAGIC - Resource allocation
# MAGIC - Facility planning
# MAGIC
# MAGIC ## Next Exercise
# MAGIC Continue to **11_Exercise_03_Route_Planning.py** to practice route creation and analysis.
