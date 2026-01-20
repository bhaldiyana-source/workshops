# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 5: Geohash Analysis
# MAGIC
# MAGIC ## Exercise Overview
# MAGIC Convert point data to geohashes for efficient spatial indexing and aggregation. Learn to use geohashes for clustering, proximity analysis, and performance optimization.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand geohash encoding and precision levels
# MAGIC - Convert coordinates to geohashes
# MAGIC - Aggregate data by geohash
# MAGIC - Perform proximity queries using geohashes
# MAGIC - Analyze spatial patterns with geohash clustering
# MAGIC
# MAGIC ## Tasks
# MAGIC 1. Generate geohashes for point data at different precision levels
# MAGIC 2. Aggregate events by geohash for heat map analysis
# MAGIC 3. Find neighbors using geohash prefixes
# MAGIC 4. Perform hierarchical spatial aggregation
# MAGIC 5. Optimize queries using geohash indexing patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geohash Overview
# MAGIC
# MAGIC Geohash is a hierarchical spatial encoding system that converts latitude/longitude coordinates into short alphanumeric strings.
# MAGIC
# MAGIC **Key Properties:**
# MAGIC - Longer geohashes = more precise location
# MAGIC - Common prefixes = geographic proximity
# MAGIC - Each character adds ~5x precision
# MAGIC
# MAGIC **Precision Levels:**
# MAGIC - 1 char: ±2,500 km
# MAGIC - 3 chars: ±78 km  
# MAGIC - 5 chars: ±2.4 km
# MAGIC - 7 chars: ±76 m
# MAGIC - 9 chars: ±2.4 m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Generate Geohashes at Different Precision Levels

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample location data
# MAGIC CREATE OR REPLACE TEMPORARY VIEW event_locations AS
# MAGIC SELECT 1 AS event_id, 'Concert at Park' AS event_name, TIMESTAMP '2024-06-15 19:00:00' AS event_time,
# MAGIC        st_geomfromtext('POINT(-122.4194 37.7749)') AS location, 5000 AS attendance
# MAGIC UNION ALL SELECT 2, 'Food Festival', TIMESTAMP '2024-06-16 12:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4186 37.7750)'), 3000
# MAGIC UNION ALL SELECT 3, 'Art Exhibition', TIMESTAMP '2024-06-17 10:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4100 37.7800)'), 1500
# MAGIC UNION ALL SELECT 4, 'Tech Conference', TIMESTAMP '2024-06-18 09:00:00',
# MAGIC        st_geomfromtext('POINT(-122.3988 37.7933)'), 8000
# MAGIC UNION ALL SELECT 5, 'Street Fair', TIMESTAMP '2024-06-19 11:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4300 37.7600)'), 2000
# MAGIC UNION ALL SELECT 6, 'Marathon Start', TIMESTAMP '2024-06-20 07:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4500 37.7850)'), 12000
# MAGIC UNION ALL SELECT 7, 'Farmers Market', TIMESTAMP '2024-06-21 08:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4150 37.7825)'), 4000
# MAGIC UNION ALL SELECT 8, 'Outdoor Movie', TIMESTAMP '2024-06-22 20:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4220 37.7780)'), 2500
# MAGIC UNION ALL SELECT 9, 'Bike Race', TIMESTAMP '2024-06-23 08:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4050 37.7700)'), 1000
# MAGIC UNION ALL SELECT 10, 'Community Picnic', TIMESTAMP '2024-06-24 12:00:00',
# MAGIC        st_geomfromtext('POINT(-122.4400 37.7900)'), 800;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate geohashes at multiple precision levels
# MAGIC SELECT 
# MAGIC   event_name,
# MAGIC   st_astext(location) AS coordinates,
# MAGIC   st_geohash(location, 5) AS geohash_5,  -- ~2.4km precision
# MAGIC   st_geohash(location, 6) AS geohash_6,  -- ~610m precision
# MAGIC   st_geohash(location, 7) AS geohash_7,  -- ~76m precision
# MAGIC   st_geohash(location, 8) AS geohash_8,  -- ~19m precision
# MAGIC   st_geohash(location, 9) AS geohash_9   -- ~2.4m precision
# MAGIC FROM event_locations
# MAGIC ORDER BY event_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualize geohash bounding boxes
# MAGIC WITH geohash_boxes AS (
# MAGIC   SELECT 
# MAGIC     event_name,
# MAGIC     st_geohash(location, 6) AS geohash,
# MAGIC     st_geomfromgeohash(st_geohash(location, 6)) AS geohash_bbox,
# MAGIC     location AS actual_point
# MAGIC   FROM event_locations
# MAGIC )
# MAGIC SELECT 
# MAGIC   event_name,
# MAGIC   geohash,
# MAGIC   st_astext(geohash_bbox) AS bbox_polygon,
# MAGIC   ROUND(st_area(geohash_bbox) * 12321.0, 4) AS bbox_area_sq_km,
# MAGIC   st_contains(geohash_bbox, actual_point) AS point_in_bbox
# MAGIC FROM geohash_boxes
# MAGIC ORDER BY event_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Aggregate Events by Geohash for Heat Map Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregate events by geohash (precision 6 = ~610m)
# MAGIC WITH geohash_aggregation AS (
# MAGIC   SELECT 
# MAGIC     st_geohash(location, 6) AS geohash,
# MAGIC     st_pointfromgeohash(st_geohash(location, 6)) AS geohash_center,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(attendance) AS total_attendance,
# MAGIC     ROUND(AVG(attendance), 0) AS avg_attendance,
# MAGIC     STRING_AGG(event_name, ', ') AS events
# MAGIC   FROM event_locations
# MAGIC   GROUP BY st_geohash(location, 6)
# MAGIC )
# MAGIC SELECT 
# MAGIC   geohash,
# MAGIC   st_astext(geohash_center) AS center_point,
# MAGIC   event_count,
# MAGIC   total_attendance,
# MAGIC   avg_attendance,
# MAGIC   events,
# MAGIC   CASE 
# MAGIC     WHEN total_attendance > 10000 THEN 'Very High Activity'
# MAGIC     WHEN total_attendance > 5000 THEN 'High Activity'
# MAGIC     WHEN total_attendance > 2000 THEN 'Medium Activity'
# MAGIC     ELSE 'Low Activity'
# MAGIC   END AS activity_level
# MAGIC FROM geohash_aggregation
# MAGIC ORDER BY total_attendance DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Multi-level aggregation (different precision levels)
# MAGIC WITH precision_5 AS (
# MAGIC   SELECT 
# MAGIC     'Precision 5 (~2.4km)' AS precision_level,
# MAGIC     st_geohash(location, 5) AS geohash,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(attendance) AS total_attendance
# MAGIC   FROM event_locations
# MAGIC   GROUP BY st_geohash(location, 5)
# MAGIC ),
# MAGIC precision_6 AS (
# MAGIC   SELECT 
# MAGIC     'Precision 6 (~610m)' AS precision_level,
# MAGIC     st_geohash(location, 6) AS geohash,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(attendance) AS total_attendance
# MAGIC   FROM event_locations
# MAGIC   GROUP BY st_geohash(location, 6)
# MAGIC ),
# MAGIC precision_7 AS (
# MAGIC   SELECT 
# MAGIC     'Precision 7 (~76m)' AS precision_level,
# MAGIC     st_geohash(location, 7) AS geohash,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(attendance) AS total_attendance
# MAGIC   FROM event_locations
# MAGIC   GROUP BY st_geohash(location, 7)
# MAGIC )
# MAGIC SELECT precision_level, geohash, event_count, total_attendance
# MAGIC FROM precision_5
# MAGIC UNION ALL
# MAGIC SELECT * FROM precision_6
# MAGIC UNION ALL
# MAGIC SELECT * FROM precision_7
# MAGIC ORDER BY precision_level, total_attendance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Find Neighbors Using Geohash Prefixes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find events in the same geohash area
# MAGIC WITH event_geohashes AS (
# MAGIC   SELECT 
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     location,
# MAGIC     attendance,
# MAGIC     st_geohash(location, 6) AS geohash_6
# MAGIC   FROM event_locations
# MAGIC )
# MAGIC SELECT 
# MAGIC   a.event_name AS event_a,
# MAGIC   b.event_name AS event_b,
# MAGIC   a.geohash_6 AS shared_geohash,
# MAGIC   ROUND(st_distancesphere(a.location, b.location), 2) AS distance_meters,
# MAGIC   a.attendance + b.attendance AS combined_attendance
# MAGIC FROM event_geohashes a
# MAGIC JOIN event_geohashes b ON a.geohash_6 = b.geohash_6 AND a.event_id < b.event_id
# MAGIC ORDER BY distance_meters;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find events with similar geohash prefixes (nearby areas)
# MAGIC WITH event_geohashes AS (
# MAGIC   SELECT 
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     st_geohash(location, 7) AS geohash_7,
# MAGIC     SUBSTRING(st_geohash(location, 7), 1, 5) AS geohash_prefix_5,
# MAGIC     location,
# MAGIC     attendance
# MAGIC   FROM event_locations
# MAGIC )
# MAGIC SELECT 
# MAGIC   a.event_name AS event_a,
# MAGIC   b.event_name AS event_b,
# MAGIC   a.geohash_prefix_5 AS shared_prefix,
# MAGIC   a.geohash_7 AS geohash_a,
# MAGIC   b.geohash_7 AS geohash_b,
# MAGIC   ROUND(st_distancesphere(a.location, b.location) / 1000, 2) AS distance_km
# MAGIC FROM event_geohashes a
# MAGIC JOIN event_geohashes b ON a.geohash_prefix_5 = b.geohash_prefix_5 AND a.event_id < b.event_id
# MAGIC ORDER BY shared_prefix, distance_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Perform Hierarchical Spatial Aggregation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create hierarchical geohash summary
# MAGIC WITH geohash_hierarchy AS (
# MAGIC   SELECT 
# MAGIC     event_id,
# MAGIC     event_name,
# MAGIC     location,
# MAGIC     attendance,
# MAGIC     st_geohash(location, 4) AS geohash_4,
# MAGIC     st_geohash(location, 5) AS geohash_5,
# MAGIC     st_geohash(location, 6) AS geohash_6,
# MAGIC     st_geohash(location, 7) AS geohash_7
# MAGIC   FROM event_locations
# MAGIC )
# MAGIC SELECT 
# MAGIC   4 AS precision_level,
# MAGIC   geohash_4 AS geohash,
# MAGIC   COUNT(*) AS events,
# MAGIC   SUM(attendance) AS attendance
# MAGIC FROM geohash_hierarchy
# MAGIC GROUP BY geohash_4
# MAGIC UNION ALL
# MAGIC SELECT 5, geohash_5, COUNT(*), SUM(attendance)
# MAGIC FROM geohash_hierarchy
# MAGIC GROUP BY geohash_5
# MAGIC UNION ALL
# MAGIC SELECT 6, geohash_6, COUNT(*), SUM(attendance)
# MAGIC FROM geohash_hierarchy
# MAGIC GROUP BY geohash_6
# MAGIC UNION ALL
# MAGIC SELECT 7, geohash_7, COUNT(*), SUM(attendance)
# MAGIC FROM geohash_hierarchy
# MAGIC GROUP BY geohash_7
# MAGIC ORDER BY precision_level, attendance DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Roll up from detailed to coarse geohash levels
# MAGIC WITH detailed_data AS (
# MAGIC   SELECT 
# MAGIC     st_geohash(location, 7) AS geohash_7,
# MAGIC     event_name,
# MAGIC     attendance
# MAGIC   FROM event_locations
# MAGIC ),
# MAGIC rollup_6 AS (
# MAGIC   SELECT 
# MAGIC     SUBSTRING(geohash_7, 1, 6) AS geohash_6,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(attendance) AS total_attendance
# MAGIC   FROM detailed_data
# MAGIC   GROUP BY SUBSTRING(geohash_7, 1, 6)
# MAGIC ),
# MAGIC rollup_5 AS (
# MAGIC   SELECT 
# MAGIC     SUBSTRING(geohash_6, 1, 5) AS geohash_5,
# MAGIC     SUM(event_count) AS event_count,
# MAGIC     SUM(total_attendance) AS total_attendance
# MAGIC   FROM rollup_6
# MAGIC   GROUP BY SUBSTRING(geohash_6, 1, 5)
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Level 5' AS aggregation_level,
# MAGIC   geohash_5 AS geohash,
# MAGIC   event_count,
# MAGIC   total_attendance
# MAGIC FROM rollup_5
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Level 6',
# MAGIC   geohash_6,
# MAGIC   event_count,
# MAGIC   total_attendance
# MAGIC FROM rollup_6
# MAGIC ORDER BY aggregation_level, total_attendance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Optimize Queries Using Geohash Indexing Patterns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Efficient proximity search using geohash prefix matching
# MAGIC -- Find all events near a target location
# MAGIC WITH target AS (
# MAGIC   SELECT 
# MAGIC     st_geomfromtext('POINT(-122.4200 37.7800)') AS location,
# MAGIC     st_geohash(st_geomfromtext('POINT(-122.4200 37.7800)'), 5) AS target_geohash_5
# MAGIC ),
# MAGIC candidate_events AS (
# MAGIC   SELECT 
# MAGIC     e.event_id,
# MAGIC     e.event_name,
# MAGIC     e.location,
# MAGIC     e.attendance,
# MAGIC     st_geohash(e.location, 5) AS event_geohash_5,
# MAGIC     SUBSTRING(st_geohash(e.location, 6), 1, 5) AS event_geohash_prefix
# MAGIC   FROM event_locations e
# MAGIC   CROSS JOIN target t
# MAGIC   -- First filter: geohash prefix match (fast)
# MAGIC   WHERE SUBSTRING(st_geohash(e.location, 6), 1, 5) = t.target_geohash_5
# MAGIC      OR st_geohash(e.location, 5) = t.target_geohash_5
# MAGIC )
# MAGIC SELECT 
# MAGIC   ce.event_name,
# MAGIC   ce.attendance,
# MAGIC   ce.event_geohash_5,
# MAGIC   ROUND(st_distancesphere(ce.location, t.location) / 1000, 2) AS distance_km,
# MAGIC   'Geohash Match' AS match_type
# MAGIC FROM candidate_events ce
# MAGIC CROSS JOIN target t
# MAGIC -- Second filter: actual distance (precise)
# MAGIC WHERE st_distancesphere(ce.location, t.location) <= 5000  -- Within 5km
# MAGIC ORDER BY distance_km;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create geohash-based spatial index simulation
# MAGIC CREATE OR REPLACE TEMPORARY VIEW geohash_index AS
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   event_name,
# MAGIC   location,
# MAGIC   attendance,
# MAGIC   st_geohash(location, 5) AS gh_5,
# MAGIC   st_geohash(location, 6) AS gh_6,
# MAGIC   st_geohash(location, 7) AS gh_7,
# MAGIC   -- Store multiple geohash levels for flexible querying
# MAGIC   ARRAY(
# MAGIC     st_geohash(location, 4),
# MAGIC     st_geohash(location, 5),
# MAGIC     st_geohash(location, 6),
# MAGIC     st_geohash(location, 7)
# MAGIC   ) AS geohash_array
# MAGIC FROM event_locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query using the geohash index
# MAGIC SELECT 
# MAGIC   event_name,
# MAGIC   gh_6 AS geohash,
# MAGIC   attendance,
# MAGIC   geohash_array AS all_precisions
# MAGIC FROM geohash_index
# MAGIC WHERE gh_6 LIKE '9q8yy%'  -- Efficient prefix search
# MAGIC ORDER BY attendance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Geohash Grid Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a grid of geohash cells and calculate statistics
# MAGIC WITH geohash_grid AS (
# MAGIC   SELECT DISTINCT
# MAGIC     st_geohash(location, 6) AS geohash
# MAGIC   FROM event_locations
# MAGIC ),
# MAGIC grid_stats AS (
# MAGIC   SELECT 
# MAGIC     gg.geohash,
# MAGIC     st_pointfromgeohash(gg.geohash) AS cell_center,
# MAGIC     st_geomfromgeohash(gg.geohash) AS cell_boundary,
# MAGIC     COUNT(el.event_id) AS events_in_cell,
# MAGIC     COALESCE(SUM(el.attendance), 0) AS total_attendance
# MAGIC   FROM geohash_grid gg
# MAGIC   LEFT JOIN event_locations el ON st_geohash(el.location, 6) = gg.geohash
# MAGIC   GROUP BY gg.geohash
# MAGIC )
# MAGIC SELECT 
# MAGIC   geohash,
# MAGIC   st_astext(cell_center) AS center,
# MAGIC   ROUND(st_area(cell_boundary) * 12321.0, 4) AS cell_area_sq_km,
# MAGIC   events_in_cell,
# MAGIC   total_attendance,
# MAGIC   CASE 
# MAGIC     WHEN events_in_cell = 0 THEN 0
# MAGIC     ELSE ROUND(total_attendance / events_in_cell, 0)
# MAGIC   END AS avg_attendance_per_event,
# MAGIC   CASE 
# MAGIC     WHEN total_attendance > 5000 THEN '🔥 Hot Spot'
# MAGIC     WHEN total_attendance > 2000 THEN '⚡ Active'
# MAGIC     WHEN events_in_cell > 0 THEN '✓ Some Activity'
# MAGIC     ELSE '○ Empty'
# MAGIC   END AS activity_indicator
# MAGIC FROM grid_stats
# MAGIC ORDER BY total_attendance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Temporal-Spatial Analysis with Geohash

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze event patterns by time and location (geohash)
# MAGIC WITH temporal_spatial AS (
# MAGIC   SELECT 
# MAGIC     event_name,
# MAGIC     event_time,
# MAGIC     DAYOFWEEK(event_time) AS day_of_week,
# MAGIC     HOUR(event_time) AS hour_of_day,
# MAGIC     st_geohash(location, 6) AS geohash,
# MAGIC     attendance
# MAGIC   FROM event_locations
# MAGIC )
# MAGIC SELECT 
# MAGIC   geohash,
# MAGIC   COUNT(*) AS total_events,
# MAGIC   SUM(attendance) AS total_attendance,
# MAGIC   ROUND(AVG(HOUR(event_time)), 1) AS avg_start_hour,
# MAGIC   MIN(event_time) AS first_event,
# MAGIC   MAX(event_time) AS last_event,
# MAGIC   DATEDIFF(MAX(event_time), MIN(event_time)) AS days_span,
# MAGIC   CASE 
# MAGIC     WHEN AVG(HOUR(event_time)) < 12 THEN 'Morning Events'
# MAGIC     WHEN AVG(HOUR(event_time)) < 18 THEN 'Afternoon Events'
# MAGIC     ELSE 'Evening Events'
# MAGIC   END AS time_pattern
# MAGIC FROM temporal_spatial
# MAGIC GROUP BY geohash
# MAGIC ORDER BY total_attendance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Geohash Density Heatmap

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate density metrics for visualization
# MAGIC WITH geohash_stats AS (
# MAGIC   SELECT 
# MAGIC     st_geohash(location, 5) AS geohash_5,
# MAGIC     st_geohash(location, 6) AS geohash_6,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     SUM(attendance) AS total_attendance
# MAGIC   FROM event_locations
# MAGIC   GROUP BY st_geohash(location, 5), st_geohash(location, 6)
# MAGIC ),
# MAGIC density_calculation AS (
# MAGIC   SELECT 
# MAGIC     geohash_6,
# MAGIC     event_count,
# MAGIC     total_attendance,
# MAGIC     st_geomfromgeohash(geohash_6) AS bbox,
# MAGIC     st_area(st_geomfromgeohash(geohash_6)) * 12321.0 AS area_sq_km
# MAGIC   FROM geohash_stats
# MAGIC )
# MAGIC SELECT 
# MAGIC   geohash_6,
# MAGIC   event_count,
# MAGIC   total_attendance,
# MAGIC   ROUND(area_sq_km, 4) AS area_sq_km,
# MAGIC   ROUND(event_count / area_sq_km, 2) AS events_per_sq_km,
# MAGIC   ROUND(total_attendance / area_sq_km, 0) AS attendance_density,
# MAGIC   CASE 
# MAGIC     WHEN total_attendance / area_sq_km > 10000 THEN 5
# MAGIC     WHEN total_attendance / area_sq_km > 5000 THEN 4
# MAGIC     WHEN total_attendance / area_sq_km > 2000 THEN 3
# MAGIC     WHEN total_attendance / area_sq_km > 1000 THEN 2
# MAGIC     ELSE 1
# MAGIC   END AS heat_intensity
# MAGIC FROM density_calculation
# MAGIC ORDER BY attendance_density DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 4: Geohash-Based Recommendation System

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recommend events based on geohash proximity and similarity
# MAGIC WITH user_interest AS (
# MAGIC   -- Simulated: User attended event at this location
# MAGIC   SELECT 
# MAGIC     st_geomfromtext('POINT(-122.4150 37.7800)') AS user_location,
# MAGIC     st_geohash(st_geomfromtext('POINT(-122.4150 37.7800)'), 6) AS user_geohash
# MAGIC ),
# MAGIC event_scores AS (
# MAGIC   SELECT 
# MAGIC     e.event_name,
# MAGIC     e.event_time,
# MAGIC     e.attendance,
# MAGIC     st_geohash(e.location, 6) AS event_geohash,
# MAGIC     st_distancesphere(e.location, ui.user_location) / 1000 AS distance_km,
# MAGIC     -- Scoring: proximity + popularity
# MAGIC     (100 - LEAST(st_distancesphere(e.location, ui.user_location) / 100, 100)) +
# MAGIC     (e.attendance / 100.0) +
# MAGIC     (CASE WHEN st_geohash(e.location, 5) = SUBSTRING(ui.user_geohash, 1, 5) THEN 50 ELSE 0 END) AS recommendation_score
# MAGIC   FROM event_locations e
# MAGIC   CROSS JOIN user_interest ui
# MAGIC )
# MAGIC SELECT 
# MAGIC   event_name,
# MAGIC   DATE_FORMAT(event_time, 'yyyy-MM-dd HH:mm') AS event_time,
# MAGIC   ROUND(distance_km, 2) AS distance_km,
# MAGIC   attendance,
# MAGIC   ROUND(recommendation_score, 1) AS score,
# MAGIC   CASE 
# MAGIC     WHEN recommendation_score > 150 THEN '⭐⭐⭐ Highly Recommended'
# MAGIC     WHEN recommendation_score > 100 THEN '⭐⭐ Recommended'
# MAGIC     ELSE '⭐ Worth Checking'
# MAGIC   END AS recommendation
# MAGIC FROM event_scores
# MAGIC ORDER BY recommendation_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Summary
# MAGIC
# MAGIC ### Key Functions Used
# MAGIC - `st_geohash()` - Convert point to geohash string
# MAGIC - `st_geomfromgeohash()` - Get bounding box polygon from geohash
# MAGIC - `st_pointfromgeohash()` - Get center point from geohash
# MAGIC - `SUBSTRING()` - Extract geohash prefixes for proximity
# MAGIC - Spatial aggregation with GROUP BY geohash
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC 1. Converting coordinates to geohashes at multiple precisions
# MAGIC 2. Aggregating spatial data using geohash grouping
# MAGIC 3. Finding neighbors with geohash prefix matching
# MAGIC 4. Hierarchical spatial aggregation and rollups
# MAGIC 5. Efficient proximity queries using geohash indexing
# MAGIC 6. Density analysis and heatmap generation
# MAGIC 7. Temporal-spatial pattern analysis
# MAGIC 8. Recommendation systems using geohash similarity
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC **Geohash Advantages:**
# MAGIC - Fast prefix-based proximity searches
# MAGIC - Hierarchical aggregation
# MAGIC - Efficient indexing
# MAGIC - Simple string comparison
# MAGIC
# MAGIC **Geohash Limitations:**
# MAGIC - Edge cases at geohash boundaries
# MAGIC - Not uniform at all latitudes
# MAGIC - Prefix matching gives rectangular, not circular, proximity
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - Use appropriate precision for your use case
# MAGIC - Combine geohash filtering with exact distance calculations
# MAGIC - Store multiple precision levels for flexibility
# MAGIC - Consider geohash boundaries in proximity queries
# MAGIC
# MAGIC ### Real-World Applications
# MAGIC - Location-based recommendations
# MAGIC - Heatmap generation
# MAGIC - Spatial indexing and partitioning
# MAGIC - Event clustering
# MAGIC - Real-time proximity searches
# MAGIC - Geographic data aggregation
# MAGIC - Cache key generation
# MAGIC - Distributed spatial indexing
# MAGIC
# MAGIC ## Workshop Complete!
# MAGIC
# MAGIC Congratulations on completing all 5 exercises! You now have practical experience with:
# MAGIC - Point operations and distance calculations
# MAGIC - Polygon analysis and zone management
# MAGIC - Route planning and optimization
# MAGIC - Spatial joins and entity assignment
# MAGIC - Geohash-based spatial indexing
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Apply these techniques to your own geospatial datasets
# MAGIC - Explore more advanced spatial algorithms
# MAGIC - Integrate with visualization tools
# MAGIC - Optimize for production workloads
# MAGIC - Combine with machine learning for predictive spatial analytics
