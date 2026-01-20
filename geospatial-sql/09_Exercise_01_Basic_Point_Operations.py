# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 1: Basic Point Operations
# MAGIC
# MAGIC ## Exercise Overview
# MAGIC Create a table of major US cities with their coordinates and perform various distance and proximity calculations.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Create and manage point geometries
# MAGIC - Calculate distances between points
# MAGIC - Find nearest neighbors
# MAGIC - Calculate bearings and directions
# MAGIC - Perform proximity analysis
# MAGIC
# MAGIC ## Tasks
# MAGIC 1. Create a table of major US cities with their lat/lon coordinates
# MAGIC 2. Calculate distances between all city pairs
# MAGIC 3. Find the 3 nearest cities for each city
# MAGIC 4. Calculate average distance between cities
# MAGIC 5. Identify cities within certain distance ranges

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Create Cities Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table of major US cities with coordinates
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_cities AS
# MAGIC SELECT 'San Francisco' AS city_name, 'CA' AS state, 
# MAGIC        st_geomfromtext('POINT(-122.4194 37.7749)') AS location,
# MAGIC        884363 AS population
# MAGIC UNION ALL SELECT 'Los Angeles', 'CA', 
# MAGIC        st_geomfromtext('POINT(-118.2437 34.0522)'), 3980400
# MAGIC UNION ALL SELECT 'San Diego', 'CA',
# MAGIC        st_geomfromtext('POINT(-117.1611 32.7157)'), 1423851
# MAGIC UNION ALL SELECT 'Seattle', 'WA',
# MAGIC        st_geomfromtext('POINT(-122.3321 47.6062)'), 753675
# MAGIC UNION ALL SELECT 'Portland', 'OR',
# MAGIC        st_geomfromtext('POINT(-122.6765 45.5231)'), 652503
# MAGIC UNION ALL SELECT 'Phoenix', 'AZ',
# MAGIC        st_geomfromtext('POINT(-112.0740 33.4484)'), 1680992
# MAGIC UNION ALL SELECT 'Denver', 'CO',
# MAGIC        st_geomfromtext('POINT(-104.9903 39.7392)'), 716492
# MAGIC UNION ALL SELECT 'Las Vegas', 'NV',
# MAGIC        st_geomfromtext('POINT(-115.1398 36.1699)'), 641903
# MAGIC UNION ALL SELECT 'New York', 'NY',
# MAGIC        st_geomfromtext('POINT(-74.0060 40.7128)'), 8336817
# MAGIC UNION ALL SELECT 'Chicago', 'IL',
# MAGIC        st_geomfromtext('POINT(-87.6298 41.8781)'), 2716000
# MAGIC UNION ALL SELECT 'Boston', 'MA',
# MAGIC        st_geomfromtext('POINT(-71.0589 42.3601)'), 692600
# MAGIC UNION ALL SELECT 'Miami', 'FL',
# MAGIC        st_geomfromtext('POINT(-80.1918 25.7617)'), 467963
# MAGIC UNION ALL SELECT 'Dallas', 'TX',
# MAGIC        st_geomfromtext('POINT(-96.7970 32.7767)'), 1343573
# MAGIC UNION ALL SELECT 'Houston', 'TX',
# MAGIC        st_geomfromtext('POINT(-95.3698 29.7604)'), 2320268
# MAGIC UNION ALL SELECT 'Austin', 'TX',
# MAGIC        st_geomfromtext('POINT(-97.7431 30.2672)'), 978908;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all cities
# MAGIC SELECT 
# MAGIC   city_name,
# MAGIC   state,
# MAGIC   st_astext(location) AS coordinates,
# MAGIC   st_x(location) AS longitude,
# MAGIC   st_y(location) AS latitude,
# MAGIC   FORMAT_NUMBER(population, 0) AS population
# MAGIC FROM us_cities
# MAGIC ORDER BY population DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Calculate Distances Between All City Pairs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate distances between all pairs of cities
# MAGIC SELECT 
# MAGIC   a.city_name AS from_city,
# MAGIC   a.state AS from_state,
# MAGIC   b.city_name AS to_city,
# MAGIC   b.state AS to_state,
# MAGIC   ROUND(st_distancesphere(a.location, b.location) / 1000, 2) AS distance_km,
# MAGIC   ROUND(st_distancesphere(a.location, b.location) / 1609.34, 2) AS distance_miles
# MAGIC FROM us_cities a
# MAGIC CROSS JOIN us_cities b
# MAGIC WHERE a.city_name < b.city_name
# MAGIC ORDER BY distance_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Find the 3 Nearest Cities for Each City

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find 3 nearest cities for each city
# MAGIC WITH city_distances AS (
# MAGIC   SELECT 
# MAGIC     a.city_name AS from_city,
# MAGIC     b.city_name AS to_city,
# MAGIC     b.state AS to_state,
# MAGIC     st_distancesphere(a.location, b.location) / 1609.34 AS distance_miles,
# MAGIC     DEGREES(st_azimuth(a.location, b.location)) AS bearing_degrees,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY a.city_name ORDER BY st_distance(a.location, b.location)) AS rank
# MAGIC   FROM us_cities a
# MAGIC   CROSS JOIN us_cities b
# MAGIC   WHERE a.city_name != b.city_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   from_city,
# MAGIC   to_city,
# MAGIC   to_state,
# MAGIC   ROUND(distance_miles, 2) AS distance_miles,
# MAGIC   ROUND(bearing_degrees, 1) AS bearing_degrees,
# MAGIC   CASE 
# MAGIC     WHEN bearing_degrees BETWEEN 0 AND 22.5 OR bearing_degrees > 337.5 THEN 'North'
# MAGIC     WHEN bearing_degrees BETWEEN 22.5 AND 67.5 THEN 'Northeast'
# MAGIC     WHEN bearing_degrees BETWEEN 67.5 AND 112.5 THEN 'East'
# MAGIC     WHEN bearing_degrees BETWEEN 112.5 AND 157.5 THEN 'Southeast'
# MAGIC     WHEN bearing_degrees BETWEEN 157.5 AND 202.5 THEN 'South'
# MAGIC     WHEN bearing_degrees BETWEEN 202.5 AND 247.5 THEN 'Southwest'
# MAGIC     WHEN bearing_degrees BETWEEN 247.5 AND 292.5 THEN 'West'
# MAGIC     ELSE 'Northwest'
# MAGIC   END AS direction,
# MAGIC   rank AS nearest_rank
# MAGIC FROM city_distances
# MAGIC WHERE rank <= 3
# MAGIC ORDER BY from_city, rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Calculate Average Distance Between Cities

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate average, minimum, and maximum distances
# MAGIC WITH city_distances AS (
# MAGIC   SELECT 
# MAGIC     a.city_name AS from_city,
# MAGIC     b.city_name AS to_city,
# MAGIC     st_distancesphere(a.location, b.location) / 1609.34 AS distance_miles
# MAGIC   FROM us_cities a
# MAGIC   CROSS JOIN us_cities b
# MAGIC   WHERE a.city_name < b.city_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_city_pairs,
# MAGIC   ROUND(AVG(distance_miles), 2) AS avg_distance_miles,
# MAGIC   ROUND(MIN(distance_miles), 2) AS min_distance_miles,
# MAGIC   ROUND(MAX(distance_miles), 2) AS max_distance_miles,
# MAGIC   ROUND(STDDEV(distance_miles), 2) AS stddev_distance_miles
# MAGIC FROM city_distances;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the closest and farthest city pairs
# MAGIC WITH city_distances AS (
# MAGIC   SELECT 
# MAGIC     a.city_name AS from_city,
# MAGIC     b.city_name AS to_city,
# MAGIC     st_distancesphere(a.location, b.location) / 1609.34 AS distance_miles
# MAGIC   FROM us_cities a
# MAGIC   CROSS JOIN us_cities b
# MAGIC   WHERE a.city_name < b.city_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Closest Cities' AS category,
# MAGIC   from_city,
# MAGIC   to_city,
# MAGIC   ROUND(distance_miles, 2) AS distance_miles
# MAGIC FROM city_distances
# MAGIC WHERE distance_miles = (SELECT MIN(distance_miles) FROM city_distances)
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Farthest Cities',
# MAGIC   from_city,
# MAGIC   to_city,
# MAGIC   ROUND(distance_miles, 2)
# MAGIC FROM city_distances
# MAGIC WHERE distance_miles = (SELECT MAX(distance_miles) FROM city_distances);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Identify Cities Within Distance Ranges

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count cities within different distance ranges from each city
# MAGIC WITH distance_ranges AS (
# MAGIC   SELECT 
# MAGIC     a.city_name AS from_city,
# MAGIC     b.city_name AS to_city,
# MAGIC     st_distancesphere(a.location, b.location) / 1609.34 AS distance_miles,
# MAGIC     CASE 
# MAGIC       WHEN st_distancesphere(a.location, b.location) / 1609.34 <= 200 THEN 'Within 200 miles'
# MAGIC       WHEN st_distancesphere(a.location, b.location) / 1609.34 <= 500 THEN '200-500 miles'
# MAGIC       WHEN st_distancesphere(a.location, b.location) / 1609.34 <= 1000 THEN '500-1000 miles'
# MAGIC       WHEN st_distancesphere(a.location, b.location) / 1609.34 <= 2000 THEN '1000-2000 miles'
# MAGIC       ELSE 'Over 2000 miles'
# MAGIC     END AS distance_range
# MAGIC   FROM us_cities a
# MAGIC   CROSS JOIN us_cities b
# MAGIC   WHERE a.city_name != b.city_name
# MAGIC )
# MAGIC SELECT 
# MAGIC   from_city,
# MAGIC   distance_range,
# MAGIC   COUNT(*) AS cities_count,
# MAGIC   ROUND(AVG(distance_miles), 2) AS avg_distance_miles
# MAGIC FROM distance_ranges
# MAGIC GROUP BY from_city, distance_range
# MAGIC ORDER BY from_city, 
# MAGIC   CASE distance_range
# MAGIC     WHEN 'Within 200 miles' THEN 1
# MAGIC     WHEN '200-500 miles' THEN 2
# MAGIC     WHEN '500-1000 miles' THEN 3
# MAGIC     WHEN '1000-2000 miles' THEN 4
# MAGIC     ELSE 5
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Geographic Center of All Cities

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the geographic center of all cities
# MAGIC WITH all_points AS (
# MAGIC   SELECT st_union_agg(location) AS all_locations
# MAGIC   FROM us_cities
# MAGIC ),
# MAGIC center_point AS (
# MAGIC   SELECT st_centroid(all_locations) AS center
# MAGIC   FROM all_points
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_astext(center) AS geographic_center,
# MAGIC   st_x(center) AS longitude,
# MAGIC   st_y(center) AS latitude,
# MAGIC   'Approximate center of selected US cities' AS description
# MAGIC FROM center_point;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Cities Within Radius of San Francisco

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all cities within 800 miles of San Francisco
# MAGIC WITH sf_location AS (
# MAGIC   SELECT location 
# MAGIC   FROM us_cities 
# MAGIC   WHERE city_name = 'San Francisco'
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.city_name,
# MAGIC   c.state,
# MAGIC   ROUND(st_distancesphere(c.location, sf.location) / 1609.34, 2) AS distance_from_sf_miles,
# MAGIC   ROUND(DEGREES(st_azimuth(sf.location, c.location)), 1) AS bearing_from_sf
# MAGIC FROM us_cities c
# MAGIC CROSS JOIN sf_location sf
# MAGIC WHERE c.city_name != 'San Francisco'
# MAGIC   AND st_distancesphere(c.location, sf.location) / 1609.34 <= 800
# MAGIC ORDER BY distance_from_sf_miles;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Population Density Analysis by Region

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze population by geographic region
# MAGIC WITH regional_classification AS (
# MAGIC   SELECT 
# MAGIC     city_name,
# MAGIC     state,
# MAGIC     population,
# MAGIC     location,
# MAGIC     CASE 
# MAGIC       WHEN st_x(location) < -100 AND st_y(location) > 40 THEN 'Pacific Northwest'
# MAGIC       WHEN st_x(location) < -115 AND st_y(location) BETWEEN 32 AND 40 THEN 'Southwest'
# MAGIC       WHEN st_x(location) BETWEEN -115 AND -100 AND st_y(location) BETWEEN 32 AND 40 THEN 'Mountain West'
# MAGIC       WHEN st_x(location) > -100 AND st_y(location) > 40 THEN 'Midwest/Northeast'
# MAGIC       WHEN st_x(location) > -100 AND st_y(location) < 35 THEN 'South'
# MAGIC       ELSE 'Other'
# MAGIC     END AS region
# MAGIC   FROM us_cities
# MAGIC )
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   COUNT(*) AS cities_count,
# MAGIC   FORMAT_NUMBER(SUM(population), 0) AS total_population,
# MAGIC   FORMAT_NUMBER(AVG(population), 0) AS avg_population,
# MAGIC   STRING_AGG(city_name, ', ') AS cities
# MAGIC FROM regional_classification
# MAGIC GROUP BY region
# MAGIC ORDER BY SUM(population) DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Summary
# MAGIC
# MAGIC ### Key Functions Used
# MAGIC - `st_geomfromtext()` - Create point geometries from coordinates
# MAGIC - `st_distancesphere()` - Calculate great circle distances
# MAGIC - `st_azimuth()` - Calculate bearing between points
# MAGIC - `st_x()`, `st_y()` - Extract coordinates
# MAGIC - `st_centroid()` - Find center point
# MAGIC - `st_union_agg()` - Aggregate multiple geometries
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC 1. Creating and managing point geometries
# MAGIC 2. Distance calculations in multiple units
# MAGIC 3. Nearest neighbor analysis
# MAGIC 4. Direction/bearing calculations
# MAGIC 5. Proximity queries and filtering
# MAGIC 6. Statistical analysis of spatial data
# MAGIC 7. Geographic classification and grouping
# MAGIC
# MAGIC ### Real-World Applications
# MAGIC - Store/facility locators
# MAGIC - Supply chain optimization
# MAGIC - Market analysis
# MAGIC - Service area planning
# MAGIC - Transportation routing
# MAGIC
# MAGIC ## Next Exercise
# MAGIC Continue to **10_Exercise_02_Polygon_Analysis.py** to practice polygon operations and zone analysis.
