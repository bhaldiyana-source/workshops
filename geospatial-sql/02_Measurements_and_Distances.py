# Databricks notebook source
# MAGIC %md
# MAGIC # Measurements and Distance Calculations
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook covers:
# MAGIC - Distance calculations (Cartesian, Spherical, Spheroid)
# MAGIC - Area calculations for polygons
# MAGIC - Length and perimeter measurements
# MAGIC - Azimuth, closest point, and max distance calculations
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed notebook 01_Introduction_and_Data_Import.py
# MAGIC - Understanding of GEOGRAPHY vs GEOMETRY data types

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Distance Functions
# MAGIC
# MAGIC Different distance functions are appropriate for different use cases:
# MAGIC - **st_distance**: 2D Cartesian distance (planar)
# MAGIC - **st_distancesphere**: Great circle distance on a sphere (faster, less accurate)
# MAGIC - **st_distancespheroid**: Distance on Earth's spheroid (most accurate for Earth)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Cartesian Distance (st_distance)
# MAGIC
# MAGIC For planar geometries, calculates Euclidean distance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2D Cartesian distance between geometries
# MAGIC -- Using Pythagorean theorem: sqrt(3^2 + 4^2) = 5.0
# MAGIC SELECT st_distance(
# MAGIC   st_geomfromtext('POINT(0 0)'),
# MAGIC   st_geomfromtext('POINT(3 4)')
# MAGIC ) AS cartesian_distance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distance from a point to a line
# MAGIC SELECT st_distance(
# MAGIC   st_geomfromtext('POINT(5 0)'),
# MAGIC   st_geomfromtext('LINESTRING(0 0, 10 10)')
# MAGIC ) AS point_to_line_distance;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Spherical Distance (st_distancesphere)
# MAGIC
# MAGIC Calculates great circle distance on a sphere. Returns distance in meters.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spherical distance in meters (great circle distance)
# MAGIC -- From San Francisco to Los Angeles
# MAGIC SELECT st_distancesphere(
# MAGIC   st_geomfromtext('POINT(-122.4194 37.7749)'), -- San Francisco
# MAGIC   st_geomfromtext('POINT(-118.2437 34.0522)')  -- Los Angeles
# MAGIC ) AS distance_meters;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert to kilometers for easier reading
# MAGIC SELECT 
# MAGIC   st_distancesphere(
# MAGIC     st_geomfromtext('POINT(-122.4194 37.7749)'),
# MAGIC     st_geomfromtext('POINT(-118.2437 34.0522)')
# MAGIC   ) / 1000 AS distance_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Spheroid Distance (st_distancespheroid)
# MAGIC
# MAGIC Most accurate distance calculation for Earth. Accounts for Earth's oblate spheroid shape.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spheroid distance (most accurate for Earth)
# MAGIC SELECT st_distancespheroid(
# MAGIC   st_geomfromtext('POINT(-122.4194 37.7749)'),
# MAGIC   st_geomfromtext('POINT(-118.2437 34.0522)')
# MAGIC ) AS accurate_distance_meters;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare all three distance methods
# MAGIC SELECT 
# MAGIC   st_distance(
# MAGIC     st_geomfromtext('POINT(-122.4194 37.7749)'),
# MAGIC     st_geomfromtext('POINT(-118.2437 34.0522)')
# MAGIC   ) AS cartesian_distance,
# MAGIC   st_distancesphere(
# MAGIC     st_geomfromtext('POINT(-122.4194 37.7749)'),
# MAGIC     st_geomfromtext('POINT(-118.2437 34.0522)')
# MAGIC   ) AS sphere_distance_m,
# MAGIC   st_distancespheroid(
# MAGIC     st_geomfromtext('POINT(-122.4194 37.7749)'),
# MAGIC     st_geomfromtext('POINT(-118.2437 34.0522)')
# MAGIC   ) AS spheroid_distance_m;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Multiple City Distances

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate distances between major US cities
# MAGIC WITH cities AS (
# MAGIC   SELECT 'San Francisco' AS city, st_geomfromtext('POINT(-122.4194 37.7749)') AS location
# MAGIC   UNION ALL SELECT 'Los Angeles', st_geomfromtext('POINT(-118.2437 34.0522)')
# MAGIC   UNION ALL SELECT 'New York', st_geomfromtext('POINT(-74.0060 40.7128)')
# MAGIC   UNION ALL SELECT 'Chicago', st_geomfromtext('POINT(-87.6298 41.8781)')
# MAGIC   UNION ALL SELECT 'Seattle', st_geomfromtext('POINT(-122.3321 47.6062)')
# MAGIC )
# MAGIC SELECT 
# MAGIC   a.city AS from_city,
# MAGIC   b.city AS to_city,
# MAGIC   ROUND(st_distancesphere(a.location, b.location) / 1000, 2) AS distance_km
# MAGIC FROM cities a
# MAGIC CROSS JOIN cities b
# MAGIC WHERE a.city < b.city
# MAGIC ORDER BY distance_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Area Calculations
# MAGIC
# MAGIC Calculate the area of polygons and multi-polygons.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Basic Area Calculation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Area of a simple square polygon (10x10 units)
# MAGIC SELECT st_area(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS area;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Area of a triangle
# MAGIC SELECT st_area(
# MAGIC   st_geomfromtext('POLYGON((0 0, 4 0, 2 3, 0 0))')
# MAGIC ) AS triangle_area;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Geographic Area (in square meters)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For geographic areas, use GEOGRAPHY type or appropriate SRID
# MAGIC -- Area of a region in the Bay Area (returns square meters)
# MAGIC SELECT st_area(
# MAGIC   st_geogfromtext('POLYGON((-122.5 37.5, -122.5 38.0, -122.0 38.0, -122.0 37.5, -122.5 37.5))')
# MAGIC ) AS area_square_meters;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert to square kilometers
# MAGIC SELECT 
# MAGIC   st_area(
# MAGIC     st_geogfromtext('POLYGON((-122.5 37.5, -122.5 38.0, -122.0 38.0, -122.0 37.5, -122.5 37.5))')
# MAGIC   ) / 1000000 AS area_square_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Polygon with Holes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate area of a polygon with a hole
# MAGIC -- Outer ring minus inner ring
# MAGIC SELECT st_area(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))')
# MAGIC ) AS area_with_hole;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Length and Perimeter
# MAGIC
# MAGIC Calculate the length of lines and perimeter of polygons.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Length of Linestrings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Length of a linestring (using Pythagorean theorem: sqrt(3^2 + 4^2) = 5.0)
# MAGIC SELECT st_length(
# MAGIC   st_geomfromtext('LINESTRING(0 0, 3 4)')
# MAGIC ) AS line_length;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Length of a multi-segment line
# MAGIC SELECT st_length(
# MAGIC   st_geomfromtext('LINESTRING(0 0, 1 0, 1 1, 2 1)')
# MAGIC ) AS multi_segment_length;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Perimeter of Polygons

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perimeter of a square (4 sides × 10 units = 40.0)
# MAGIC SELECT st_perimeter(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS perimeter;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perimeter of a triangle
# MAGIC SELECT st_perimeter(
# MAGIC   st_geomfromtext('POLYGON((0 0, 3 0, 0 4, 0 0))')
# MAGIC ) AS triangle_perimeter;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Other Measurements
# MAGIC
# MAGIC Additional spatial measurement functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Azimuth (Bearing)
# MAGIC
# MAGIC Calculate the bearing/azimuth from one point to another in radians.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate bearing/azimuth from point A to point B (in radians)
# MAGIC SELECT st_azimuth(
# MAGIC   st_geomfromtext('POINT(0 0)'),
# MAGIC   st_geomfromtext('POINT(1 1)')
# MAGIC ) AS azimuth_radians;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert azimuth to degrees
# MAGIC SELECT 
# MAGIC   st_azimuth(
# MAGIC     st_geomfromtext('POINT(0 0)'),
# MAGIC     st_geomfromtext('POINT(1 1)')
# MAGIC   ) AS azimuth_radians,
# MAGIC   DEGREES(st_azimuth(
# MAGIC     st_geomfromtext('POINT(0 0)'),
# MAGIC     st_geomfromtext('POINT(1 1)')
# MAGIC   )) AS azimuth_degrees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Azimuth from San Francisco to Los Angeles
# MAGIC SELECT 
# MAGIC   DEGREES(st_azimuth(
# MAGIC     st_geomfromtext('POINT(-122.4194 37.7749)'),
# MAGIC     st_geomfromtext('POINT(-118.2437 34.0522)')
# MAGIC   )) AS bearing_to_la_degrees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Closest Point

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the closest point on geometry A to geometry B
# MAGIC SELECT st_astext(
# MAGIC   st_closestpoint(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 10 10)'),
# MAGIC     st_geomfromtext('POINT(5 0)')
# MAGIC   )
# MAGIC ) AS closest_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Closest point on a polygon to an external point
# MAGIC SELECT st_astext(
# MAGIC   st_closestpoint(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'),
# MAGIC     st_geomfromtext('POINT(10 10)')
# MAGIC   )
# MAGIC ) AS closest_point_on_polygon;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Maximum Distance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Maximum distance between any two points in two geometries
# MAGIC SELECT st_maxdistance(
# MAGIC   st_geomfromtext('LINESTRING(0 0, 10 0)'),
# MAGIC   st_geomfromtext('LINESTRING(0 5, 10 5)')
# MAGIC ) AS max_distance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Maximum distance within a polygon (diagonal)
# MAGIC SELECT st_maxdistance(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS max_distance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practical Example: Delivery Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze delivery zones and distances
# MAGIC WITH warehouse AS (
# MAGIC   SELECT st_geomfromtext('POINT(-122.4194 37.7749)') AS location
# MAGIC ),
# MAGIC delivery_locations AS (
# MAGIC   SELECT 'Customer A' AS name, st_geomfromtext('POINT(-122.4100 37.7800)') AS location
# MAGIC   UNION ALL SELECT 'Customer B', st_geomfromtext('POINT(-122.4300 37.7700)')
# MAGIC   UNION ALL SELECT 'Customer C', st_geomfromtext('POINT(-122.4500 37.7600)')
# MAGIC   UNION ALL SELECT 'Customer D', st_geomfromtext('POINT(-122.4000 37.7900)')
# MAGIC ),
# MAGIC distances AS (
# MAGIC   SELECT 
# MAGIC     d.name,
# MAGIC     st_distancesphere(w.location, d.location) AS distance_m,
# MAGIC     st_azimuth(w.location, d.location) AS azimuth_rad
# MAGIC   FROM warehouse w
# MAGIC   CROSS JOIN delivery_locations d
# MAGIC )
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   ROUND(distance_m, 2) AS distance_meters,
# MAGIC   ROUND(distance_m / 1000, 2) AS distance_km,
# MAGIC   ROUND(DEGREES(azimuth_rad), 2) AS bearing_degrees,
# MAGIC   CASE 
# MAGIC     WHEN distance_m < 1000 THEN 'Very Close'
# MAGIC     WHEN distance_m < 3000 THEN 'Close'
# MAGIC     WHEN distance_m < 5000 THEN 'Medium'
# MAGIC     ELSE 'Far'
# MAGIC   END AS proximity_category
# MAGIC FROM distances
# MAGIC ORDER BY distance_m;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary Statistics Example

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate comprehensive statistics for service areas
# MAGIC WITH service_zones AS (
# MAGIC   SELECT 
# MAGIC     'Zone A' AS zone_name,
# MAGIC     st_buffer(st_geomfromtext('POINT(-122.4194 37.7749)'), 0.05) AS boundary
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'Zone B',
# MAGIC     st_buffer(st_geomfromtext('POINT(-122.4000 37.7900)'), 0.03)
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'Zone C',
# MAGIC     st_buffer(st_geomfromtext('POINT(-122.4300 37.7700)'), 0.04)
# MAGIC )
# MAGIC SELECT 
# MAGIC   zone_name,
# MAGIC   ROUND(st_area(boundary), 6) AS area_sq_degrees,
# MAGIC   ROUND(st_perimeter(boundary), 4) AS perimeter_degrees,
# MAGIC   st_astext(st_centroid(boundary)) AS center_point
# MAGIC FROM service_zones
# MAGIC ORDER BY area_sq_degrees DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Distance Functions**:
# MAGIC    - Use `st_distance` for planar/Cartesian calculations
# MAGIC    - Use `st_distancesphere` for faster Earth distance calculations
# MAGIC    - Use `st_distancespheroid` for most accurate Earth distances
# MAGIC
# MAGIC 2. **Area and Length**:
# MAGIC    - `st_area` for polygon areas
# MAGIC    - `st_length` for linestring lengths
# MAGIC    - `st_perimeter` for polygon perimeters
# MAGIC
# MAGIC 3. **Advanced Measurements**:
# MAGIC    - `st_azimuth` for bearing/direction calculations
# MAGIC    - `st_closestpoint` for proximity analysis
# MAGIC    - `st_maxdistance` for extent calculations
# MAGIC
# MAGIC 4. **Unit Conversions**: Remember to convert meters to kilometers or miles as needed for readability
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next notebook: **03_Geometry_Constructors_and_Accessors.py** to learn about creating and accessing geometry properties.
