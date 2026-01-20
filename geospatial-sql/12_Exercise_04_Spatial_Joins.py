# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 4: Spatial Joins
# MAGIC
# MAGIC ## Exercise Overview
# MAGIC Join tables of customer locations with store locations to assign each customer to their nearest store, calculate service coverage, and perform advanced spatial relationship queries.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Perform spatial joins based on distance
# MAGIC - Assign entities using nearest neighbor logic
# MAGIC - Calculate coverage statistics
# MAGIC - Perform point-in-polygon joins
# MAGIC - Analyze spatial relationships across datasets
# MAGIC
# MAGIC ## Tasks
# MAGIC 1. Create customer and store datasets
# MAGIC 2. Assign customers to nearest stores
# MAGIC 3. Calculate service coverage metrics
# MAGIC 4. Perform zone-based assignments
# MAGIC 5. Analyze customer distribution and accessibility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Create Customer and Store Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create stores table
# MAGIC CREATE OR REPLACE TEMPORARY VIEW stores AS
# MAGIC SELECT 1 AS store_id, 'Downtown Superstore' AS store_name,
# MAGIC        st_geomfromtext('POINT(-122.4200 37.7850)') AS location,
# MAGIC        'Full Service' AS store_type,
# MAGIC        50 AS capacity,
# MAGIC        8 AS opening_hour,
# MAGIC        22 AS closing_hour
# MAGIC UNION ALL SELECT 2, 'Bayview Market',
# MAGIC        st_geomfromtext('POINT(-122.3900 37.7350)'),
# MAGIC        'Grocery', 30, 7, 23
# MAGIC UNION ALL SELECT 3, 'Richmond Plaza',
# MAGIC        st_geomfromtext('POINT(-122.4700 37.7800)'),
# MAGIC        'Full Service', 45, 8, 22
# MAGIC UNION ALL SELECT 4, 'Mission District Store',
# MAGIC        st_geomfromtext('POINT(-122.4190 37.7599)'),
# MAGIC        'Grocery', 25, 6, 24
# MAGIC UNION ALL SELECT 5, 'North Beach Shop',
# MAGIC        st_geomfromtext('POINT(-122.4100 37.8000)'),
# MAGIC        'Convenience', 15, 6, 24
# MAGIC UNION ALL SELECT 6, 'Sunset Market',
# MAGIC        st_geomfromtext('POINT(-122.4950 37.7600)'),
# MAGIC        'Grocery', 35, 7, 23
# MAGIC UNION ALL SELECT 7, 'Financial District Express',
# MAGIC        st_geomfromtext('POINT(-122.3988 37.7933)'),
# MAGIC        'Convenience', 20, 6, 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customers table with demographics
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customers AS
# MAGIC SELECT 1 AS customer_id, 'Alice Johnson' AS customer_name,
# MAGIC        st_geomfromtext('POINT(-122.4150 37.7800)') AS location,
# MAGIC        'Premium' AS membership_tier,
# MAGIC        120 AS avg_monthly_spend
# MAGIC UNION ALL SELECT 2, 'Bob Smith',
# MAGIC        st_geomfromtext('POINT(-122.4250 37.7900)'),
# MAGIC        'Standard', 80
# MAGIC UNION ALL SELECT 3, 'Carol Williams',
# MAGIC        st_geomfromtext('POINT(-122.3950 37.7400)'),
# MAGIC        'Premium', 150
# MAGIC UNION ALL SELECT 4, 'David Brown',
# MAGIC        st_geomfromtext('POINT(-122.4800 37.7750)'),
# MAGIC        'Standard', 65
# MAGIC UNION ALL SELECT 5, 'Eve Davis',
# MAGIC        st_geomfromtext('POINT(-122.4100 37.7650)'),
# MAGIC        'Premium', 140
# MAGIC UNION ALL SELECT 6, 'Frank Miller',
# MAGIC        st_geomfromtext('POINT(-122.4300 37.8050)'),
# MAGIC        'Standard', 75
# MAGIC UNION ALL SELECT 7, 'Grace Wilson',
# MAGIC        st_geomfromtext('POINT(-122.4500 37.7600)'),
# MAGIC        'Basic', 45
# MAGIC UNION ALL SELECT 8, 'Henry Moore',
# MAGIC        st_geomfromtext('POINT(-122.3850 37.7950)'),
# MAGIC        'Premium', 130
# MAGIC UNION ALL SELECT 9, 'Iris Taylor',
# MAGIC        st_geomfromtext('POINT(-122.5000 37.7650)'),
# MAGIC        'Standard', 70
# MAGIC UNION ALL SELECT 10, 'Jack Anderson',
# MAGIC        st_geomfromtext('POINT(-122.4200 37.7550)'),
# MAGIC        'Basic', 50
# MAGIC UNION ALL SELECT 11, 'Karen Thomas',
# MAGIC        st_geomfromtext('POINT(-122.4050 37.7700)'),
# MAGIC        'Premium', 125
# MAGIC UNION ALL SELECT 12, 'Leo Jackson',
# MAGIC        st_geomfromtext('POINT(-122.4400 37.7950)'),
# MAGIC        'Standard', 85
# MAGIC UNION ALL SELECT 13, 'Maria White',
# MAGIC        st_geomfromtext('POINT(-122.3800 37.7450)'),
# MAGIC        'Basic', 40
# MAGIC UNION ALL SELECT 14, 'Nathan Harris',
# MAGIC        st_geomfromtext('POINT(-122.4900 37.7850)'),
# MAGIC        'Premium', 135
# MAGIC UNION ALL SELECT 15, 'Olivia Martin',
# MAGIC        st_geomfromtext('POINT(-122.4150 37.7950)'),
# MAGIC        'Standard', 90;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View datasets
# MAGIC SELECT 
# MAGIC   'Stores' AS dataset,
# MAGIC   COUNT(*) AS record_count,
# MAGIC   COUNT(DISTINCT store_type) AS categories
# MAGIC FROM stores
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Customers',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT membership_tier)
# MAGIC FROM customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Assign Customers to Nearest Stores

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate distances between all customers and stores
# MAGIC SELECT 
# MAGIC   c.customer_name,
# MAGIC   c.membership_tier,
# MAGIC   s.store_name,
# MAGIC   s.store_type,
# MAGIC   ROUND(st_distancesphere(c.location, s.location) / 1000, 2) AS distance_km,
# MAGIC   ROUND(DEGREES(st_azimuth(c.location, s.location)), 1) AS bearing_degrees
# MAGIC FROM customers c
# MAGIC CROSS JOIN stores s
# MAGIC ORDER BY c.customer_name, distance_km
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assign each customer to their nearest store
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_store_assignments AS
# MAGIC WITH customer_distances AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.location AS customer_location,
# MAGIC     c.membership_tier,
# MAGIC     c.avg_monthly_spend,
# MAGIC     s.store_id,
# MAGIC     s.store_name,
# MAGIC     s.location AS store_location,
# MAGIC     s.store_type,
# MAGIC     s.capacity,
# MAGIC     st_distancesphere(c.location, s.location) / 1000 AS distance_km,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY st_distance(c.location, s.location)) AS rank
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN stores s
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   customer_location,
# MAGIC   membership_tier,
# MAGIC   avg_monthly_spend,
# MAGIC   store_id,
# MAGIC   store_name,
# MAGIC   store_location,
# MAGIC   store_type,
# MAGIC   capacity,
# MAGIC   distance_km
# MAGIC FROM customer_distances
# MAGIC WHERE rank = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View customer assignments
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   membership_tier,
# MAGIC   store_name,
# MAGIC   store_type,
# MAGIC   ROUND(distance_km, 2) AS distance_km,
# MAGIC   CASE 
# MAGIC     WHEN distance_km < 1 THEN 'Very Close (<1km)'
# MAGIC     WHEN distance_km < 3 THEN 'Close (1-3km)'
# MAGIC     WHEN distance_km < 5 THEN 'Moderate (3-5km)'
# MAGIC     ELSE 'Far (>5km)'
# MAGIC   END AS distance_category
# MAGIC FROM customer_store_assignments
# MAGIC ORDER BY distance_km;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Calculate Service Coverage Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate load and revenue by store
# MAGIC SELECT 
# MAGIC   store_name,
# MAGIC   store_type,
# MAGIC   capacity,
# MAGIC   COUNT(*) AS assigned_customers,
# MAGIC   ROUND(COUNT(*) * 100.0 / capacity, 1) AS capacity_utilization_pct,
# MAGIC   SUM(avg_monthly_spend) AS total_monthly_revenue,
# MAGIC   ROUND(AVG(distance_km), 2) AS avg_customer_distance_km,
# MAGIC   ROUND(MAX(distance_km), 2) AS max_customer_distance_km,
# MAGIC   ROUND(MIN(distance_km), 2) AS min_customer_distance_km
# MAGIC FROM customer_store_assignments
# MAGIC GROUP BY store_name, store_type, capacity
# MAGIC ORDER BY total_monthly_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze customer distribution by membership tier
# MAGIC SELECT 
# MAGIC   membership_tier,
# MAGIC   COUNT(*) AS customer_count,
# MAGIC   ROUND(AVG(distance_km), 2) AS avg_distance_to_store_km,
# MAGIC   ROUND(AVG(avg_monthly_spend), 2) AS avg_monthly_spend,
# MAGIC   SUM(avg_monthly_spend) AS total_tier_revenue,
# MAGIC   COUNT(DISTINCT store_id) AS stores_serving
# MAGIC FROM customer_store_assignments
# MAGIC GROUP BY membership_tier
# MAGIC ORDER BY total_tier_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify underserved customer segments
# MAGIC WITH customer_access AS (
# MAGIC   SELECT 
# MAGIC     membership_tier,
# MAGIC     customer_name,
# MAGIC     distance_km,
# MAGIC     avg_monthly_spend,
# MAGIC     CASE 
# MAGIC       WHEN distance_km > 5 THEN 'Poor Access'
# MAGIC       WHEN distance_km > 3 THEN 'Moderate Access'
# MAGIC       ELSE 'Good Access'
# MAGIC     END AS access_category
# MAGIC   FROM customer_store_assignments
# MAGIC )
# MAGIC SELECT 
# MAGIC   access_category,
# MAGIC   membership_tier,
# MAGIC   COUNT(*) AS customers,
# MAGIC   ROUND(AVG(distance_km), 2) AS avg_distance_km,
# MAGIC   ROUND(AVG(avg_monthly_spend), 2) AS avg_spend,
# MAGIC   SUM(avg_monthly_spend) AS total_revenue_at_risk
# MAGIC FROM customer_access
# MAGIC GROUP BY access_category, membership_tier
# MAGIC ORDER BY 
# MAGIC   CASE access_category
# MAGIC     WHEN 'Poor Access' THEN 1
# MAGIC     WHEN 'Moderate Access' THEN 2
# MAGIC     ELSE 3
# MAGIC   END,
# MAGIC   total_revenue_at_risk DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Perform Zone-Based Assignments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create service zones around stores
# MAGIC CREATE OR REPLACE TEMPORARY VIEW store_service_zones AS
# MAGIC SELECT 
# MAGIC   store_id,
# MAGIC   store_name,
# MAGIC   location,
# MAGIC   store_type,
# MAGIC   capacity,
# MAGIC   st_buffer(location, 0.025) AS primary_zone,    -- ~2.5km
# MAGIC   st_buffer(location, 0.050) AS secondary_zone,  -- ~5km
# MAGIC   st_buffer(location, 0.075) AS extended_zone    -- ~7.5km
# MAGIC FROM stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assign customers based on zone containment
# MAGIC WITH zone_assignments AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.membership_tier,
# MAGIC     s.store_id,
# MAGIC     s.store_name,
# MAGIC     CASE 
# MAGIC       WHEN st_contains(s.primary_zone, c.location) THEN 'Primary'
# MAGIC       WHEN st_contains(s.secondary_zone, c.location) THEN 'Secondary'
# MAGIC       WHEN st_contains(s.extended_zone, c.location) THEN 'Extended'
# MAGIC       ELSE 'Outside'
# MAGIC     END AS service_zone,
# MAGIC     st_distancesphere(c.location, s.location) / 1000 AS distance_km
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN store_service_zones s
# MAGIC   WHERE st_contains(s.extended_zone, c.location)
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   membership_tier,
# MAGIC   store_name,
# MAGIC   service_zone,
# MAGIC   ROUND(distance_km, 2) AS distance_km,
# MAGIC   CASE service_zone
# MAGIC     WHEN 'Primary' THEN 'Standard Delivery (Free)'
# MAGIC     WHEN 'Secondary' THEN 'Standard Delivery ($5)'
# MAGIC     WHEN 'Extended' THEN 'Extended Delivery ($10)'
# MAGIC     ELSE 'Not Available'
# MAGIC   END AS delivery_option
# MAGIC FROM zone_assignments
# MAGIC ORDER BY customer_name, distance_km;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate zone coverage statistics
# MAGIC WITH zone_coverage AS (
# MAGIC   SELECT 
# MAGIC     s.store_name,
# MAGIC     s.store_type,
# MAGIC     CASE 
# MAGIC       WHEN st_contains(s.primary_zone, c.location) THEN 'Primary'
# MAGIC       WHEN st_contains(s.secondary_zone, c.location) THEN 'Secondary'
# MAGIC       WHEN st_contains(s.extended_zone, c.location) THEN 'Extended'
# MAGIC     END AS zone_type,
# MAGIC     c.customer_id,
# MAGIC     c.avg_monthly_spend
# MAGIC   FROM store_service_zones s
# MAGIC   CROSS JOIN customers c
# MAGIC   WHERE st_contains(s.extended_zone, c.location)
# MAGIC )
# MAGIC SELECT 
# MAGIC   store_name,
# MAGIC   store_type,
# MAGIC   zone_type,
# MAGIC   COUNT(*) AS customers_in_zone,
# MAGIC   SUM(avg_monthly_spend) AS potential_revenue
# MAGIC FROM zone_coverage
# MAGIC GROUP BY store_name, store_type, zone_type
# MAGIC ORDER BY store_name, 
# MAGIC   CASE zone_type
# MAGIC     WHEN 'Primary' THEN 1
# MAGIC     WHEN 'Secondary' THEN 2
# MAGIC     ELSE 3
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Analyze Customer Distribution and Accessibility

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geographic distribution of customers vs stores
# MAGIC WITH customer_center AS (
# MAGIC   SELECT st_centroid(st_union_agg(location)) AS center
# MAGIC   FROM customers
# MAGIC ),
# MAGIC store_center AS (
# MAGIC   SELECT st_centroid(st_union_agg(location)) AS center
# MAGIC   FROM stores
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Customers' AS entity_type,
# MAGIC   st_astext(center) AS geographic_center,
# MAGIC   st_x(center) AS longitude,
# MAGIC   st_y(center) AS latitude
# MAGIC FROM customer_center
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Stores',
# MAGIC   st_astext(center),
# MAGIC   st_x(center),
# MAGIC   st_y(center)
# MAGIC FROM store_center
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Offset (Stores to Customers)',
# MAGIC   NULL,
# MAGIC   st_x((SELECT center FROM store_center)) - st_x((SELECT center FROM customer_center)),
# MAGIC   st_y((SELECT center FROM store_center)) - st_y((SELECT center FROM customer_center));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find customers with multiple nearby store options
# MAGIC WITH nearby_stores AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.membership_tier,
# MAGIC     s.store_id,
# MAGIC     s.store_name,
# MAGIC     s.store_type,
# MAGIC     st_distancesphere(c.location, s.location) / 1000 AS distance_km
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN stores s
# MAGIC   WHERE st_dwithin(c.location, s.location, 0.05)  -- Within ~5km
# MAGIC ),
# MAGIC customer_options AS (
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     membership_tier,
# MAGIC     COUNT(DISTINCT store_id) AS nearby_store_count,
# MAGIC     COUNT(DISTINCT store_type) AS store_type_variety,
# MAGIC     ROUND(AVG(distance_km), 2) AS avg_distance_km,
# MAGIC     STRING_AGG(store_name, ', ') AS available_stores
# MAGIC   FROM nearby_stores
# MAGIC   GROUP BY customer_id, customer_name, membership_tier
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   membership_tier,
# MAGIC   nearby_store_count,
# MAGIC   store_type_variety,
# MAGIC   avg_distance_km,
# MAGIC   available_stores,
# MAGIC   CASE 
# MAGIC     WHEN nearby_store_count >= 3 THEN 'Excellent Choice'
# MAGIC     WHEN nearby_store_count = 2 THEN 'Good Choice'
# MAGIC     WHEN nearby_store_count = 1 THEN 'Limited Choice'
# MAGIC     ELSE 'No Nearby Options'
# MAGIC   END AS accessibility_rating
# MAGIC FROM customer_options
# MAGIC ORDER BY nearby_store_count DESC, avg_distance_km;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify potential new store locations
# MAGIC WITH customer_clusters AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.location,
# MAGIC     c.avg_monthly_spend,
# MAGIC     MIN(st_distancesphere(c.location, s.location) / 1000) AS nearest_store_km
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN stores s
# MAGIC   GROUP BY c.customer_id, c.location, c.avg_monthly_spend
# MAGIC ),
# MAGIC underserved AS (
# MAGIC   SELECT *
# MAGIC   FROM customer_clusters
# MAGIC   WHERE nearest_store_km > 3
# MAGIC )
# MAGIC SELECT 
# MAGIC   st_astext(st_centroid(st_union_agg(location))) AS suggested_new_store_location,
# MAGIC   COUNT(*) AS underserved_customers,
# MAGIC   SUM(avg_monthly_spend) AS potential_revenue,
# MAGIC   ROUND(AVG(nearest_store_km), 2) AS avg_current_distance_km,
# MAGIC   'HIGH PRIORITY' AS expansion_priority
# MAGIC FROM underserved
# MAGIC HAVING COUNT(*) >= 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Multi-Criteria Store Assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assign stores based on distance, capacity, and store type preference
# MAGIC WITH customer_preferences AS (
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     membership_tier,
# MAGIC     CASE membership_tier
# MAGIC       WHEN 'Premium' THEN 'Full Service'
# MAGIC       WHEN 'Standard' THEN 'Grocery'
# MAGIC       ELSE 'Convenience'
# MAGIC     END AS preferred_store_type
# MAGIC   FROM customers
# MAGIC ),
# MAGIC scored_assignments AS (
# MAGIC   SELECT 
# MAGIC     cp.customer_id,
# MAGIC     cp.customer_name,
# MAGIC     cp.membership_tier,
# MAGIC     s.store_id,
# MAGIC     s.store_name,
# MAGIC     s.store_type,
# MAGIC     st_distancesphere(c.location, s.location) / 1000 AS distance_km,
# MAGIC     -- Multi-criteria score
# MAGIC     (100 - st_distancesphere(c.location, s.location) / 100) +  -- Distance score
# MAGIC     (CASE WHEN s.store_type = cp.preferred_store_type THEN 50 ELSE 0 END) +  -- Type match bonus
# MAGIC     (s.capacity / 2) AS match_score  -- Capacity score
# MAGIC   FROM customer_preferences cp
# MAGIC   JOIN customers c ON cp.customer_id = c.customer_id
# MAGIC   CROSS JOIN stores s
# MAGIC ),
# MAGIC best_matches AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY match_score DESC) AS rank
# MAGIC   FROM scored_assignments
# MAGIC )
# MAGIC SELECT 
# MAGIC   customer_name,
# MAGIC   membership_tier,
# MAGIC   store_name,
# MAGIC   store_type,
# MAGIC   ROUND(distance_km, 2) AS distance_km,
# MAGIC   ROUND(match_score, 1) AS match_score
# MAGIC FROM best_matches
# MAGIC WHERE rank = 1
# MAGIC ORDER BY match_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Competitive Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulate competitor stores and analyze market share
# MAGIC WITH competitor_stores AS (
# MAGIC   SELECT 1 AS comp_id, 'Competitor A' AS comp_name,
# MAGIC          st_geomfromtext('POINT(-122.4350 37.7750)') AS location
# MAGIC   UNION ALL SELECT 2, 'Competitor B',
# MAGIC          st_geomfromtext('POINT(-122.4100 37.7450)')
# MAGIC   UNION ALL SELECT 3, 'Competitor C',
# MAGIC          st_geomfromtext('POINT(-122.4600 37.7950)')
# MAGIC ),
# MAGIC customer_nearest_overall AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.avg_monthly_spend,
# MAGIC     'Our Store' AS winner_type,
# MAGIC     s.store_name AS winner_name,
# MAGIC     st_distancesphere(c.location, s.location) / 1000 AS distance_km
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN stores s
# MAGIC   WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM competitor_stores cs
# MAGIC     WHERE st_distance(c.location, cs.location) < st_distance(c.location, s.location)
# MAGIC   )
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.avg_monthly_spend,
# MAGIC     'Competitor',
# MAGIC     cs.comp_name,
# MAGIC     st_distancesphere(c.location, cs.location) / 1000
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN competitor_stores cs
# MAGIC   WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM stores s
# MAGIC     WHERE st_distance(c.location, s.location) < st_distance(c.location, cs.location)
# MAGIC   )
# MAGIC )
# MAGIC SELECT 
# MAGIC   winner_type,
# MAGIC   COUNT(*) AS customers_captured,
# MAGIC   SUM(avg_monthly_spend) AS monthly_revenue,
# MAGIC   ROUND(AVG(distance_km), 2) AS avg_customer_distance_km,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 1) AS market_share_pct
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     avg_monthly_spend,
# MAGIC     winner_type,
# MAGIC     distance_km,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY distance_km) AS rank
# MAGIC   FROM customer_nearest_overall
# MAGIC )
# MAGIC WHERE rank = 1
# MAGIC GROUP BY winner_type
# MAGIC ORDER BY monthly_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Dynamic Service Area Visualization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Voronoi-like partitioning (simplified)
# MAGIC -- Each customer point gets assigned to exactly one store
# MAGIC WITH customer_assignments_all AS (
# MAGIC   SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.location AS customer_location,
# MAGIC     s.store_id,
# MAGIC     s.location AS store_location,
# MAGIC     st_distance(c.location, s.location) AS distance,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY st_distance(c.location, s.location)) AS rank
# MAGIC   FROM customers c
# MAGIC   CROSS JOIN stores s
# MAGIC ),
# MAGIC service_territories AS (
# MAGIC   SELECT 
# MAGIC     store_id,
# MAGIC     st_union_agg(customer_location) AS customer_points
# MAGIC   FROM customer_assignments_all
# MAGIC   WHERE rank = 1
# MAGIC   GROUP BY store_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   s.store_name,
# MAGIC   s.store_type,
# MAGIC   st_astext(st_convexhull(st.customer_points)) AS territory_hull,
# MAGIC   st_astext(st_centroid(st.customer_points)) AS territory_center,
# MAGIC   COUNT(caa.customer_id) AS customers_in_territory
# MAGIC FROM service_territories st
# MAGIC JOIN stores s ON st.store_id = s.store_id
# MAGIC JOIN customer_assignments_all caa ON st.store_id = caa.store_id AND caa.rank = 1
# MAGIC GROUP BY s.store_name, s.store_type, st.customer_points
# MAGIC ORDER BY customers_in_territory DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise Summary
# MAGIC
# MAGIC ### Key Functions Used
# MAGIC - `st_distancesphere()` - Calculate accurate Earth distances
# MAGIC - `st_dwithin()` - Proximity filtering
# MAGIC - `ROW_NUMBER()` - Nearest neighbor assignment
# MAGIC - `st_contains()` - Point-in-polygon joins
# MAGIC - `st_buffer()` - Create service zones
# MAGIC - `st_union_agg()`, `st_centroid()` - Aggregate spatial analysis
# MAGIC - `st_convexhull()` - Territory delineation
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC 1. Performing spatial joins with distance calculations
# MAGIC 2. Nearest neighbor assignment using window functions
# MAGIC 3. Zone-based containment joins
# MAGIC 4. Multi-criteria spatial matching
# MAGIC 5. Coverage and accessibility analysis
# MAGIC 6. Service territory definition
# MAGIC 7. Competitive analysis with spatial data
# MAGIC 8. Gap analysis and expansion planning
# MAGIC
# MAGIC ### Real-World Applications
# MAGIC - Customer-store assignment
# MAGIC - Service territory management
# MAGIC - Market share analysis
# MAGIC - Accessibility studies
# MAGIC - Capacity planning
# MAGIC - Site selection
# MAGIC - Competitive intelligence
# MAGIC - Revenue optimization
# MAGIC
# MAGIC ## Next Exercise
# MAGIC Continue to **13_Exercise_05_Geohash_Analysis.py** to practice geohash-based spatial indexing and aggregation.
