# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Advanced Spatial Operations
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture covers advanced geospatial concepts essential for processing large-scale spatial datasets in Databricks. We'll explore sophisticated spatial indexing techniques, particularly H3 hexagonal indexing, spatial join strategies, and optimization approaches for distributed geospatial computing.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand the limitations of basic spatial operations at scale
# MAGIC - Explain how spatial indexing improves query performance
# MAGIC - Compare different spatial join strategies and their use cases
# MAGIC - Describe the H3 hexagonal indexing system and its advantages
# MAGIC - Recognize when to apply specific optimization techniques
# MAGIC - Design efficient geospatial pipelines for production workloads
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Part 1: Foundations of Geospatial Analytics
# MAGIC - Understanding of basic spatial operations (buffers, intersections, etc.)
# MAGIC - Familiarity with distributed computing concepts
# MAGIC - Python and Spark SQL proficiency
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Challenge of Geospatial Data at Scale
# MAGIC
# MAGIC ### Why Basic Operations Don't Scale
# MAGIC
# MAGIC When working with geospatial data, naive approaches quickly become impractical:
# MAGIC
# MAGIC **Example: Point-in-Polygon Query**
# MAGIC - 1 million customer locations
# MAGIC - 10,000 delivery zones (polygons)
# MAGIC - Naive approach: 10 billion comparisons!
# MAGIC
# MAGIC **The Problem:**
# MAGIC ```python
# MAGIC # Inefficient approach - cartesian product
# MAGIC for customer in customers:      # 1M iterations
# MAGIC     for zone in zones:          # 10K iterations each
# MAGIC         if zone.contains(customer.location):  # 10B geometry tests!
# MAGIC             # assign customer to zone
# MAGIC ```
# MAGIC
# MAGIC **Why This Fails:**
# MAGIC - Computational complexity: O(n × m) where n and m are dataset sizes
# MAGIC - Each geometric test is expensive (especially for complex polygons)
# MAGIC - No parallelization in naive loops
# MAGIC - Memory constraints with large datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Indexing: The Foundation of Performance
# MAGIC
# MAGIC ### What is Spatial Indexing?
# MAGIC
# MAGIC Spatial indexing organizes geographic data to quickly answer the question:
# MAGIC **"What objects are near this location?"**
# MAGIC
# MAGIC ### Common Spatial Index Types
# MAGIC
# MAGIC | Index Type | Structure | Best For | Complexity |
# MAGIC |------------|-----------|----------|------------|
# MAGIC | **Grid Index** | Fixed rectangular cells | Uniform distributions | O(1) lookup |
# MAGIC | **Quadtree** | Hierarchical squares | Variable density | O(log n) |
# MAGIC | **R-tree** | Bounding boxes | Complex geometries | O(log n) |
# MAGIC | **H3** | Hexagonal cells | Multi-resolution analysis | O(1) lookup |
# MAGIC | **S2** | Spherical geometry | Global coverage | O(log n) |
# MAGIC
# MAGIC ### How Indexes Improve Performance
# MAGIC
# MAGIC **Without Index:**
# MAGIC - Must compare every point with every polygon
# MAGIC - Time complexity: O(n × m)
# MAGIC
# MAGIC **With Spatial Index:**
# MAGIC - Quickly filter to nearby candidates
# MAGIC - Only test geometries that might intersect
# MAGIC - Time complexity: O(n × log m) or better

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 Hexagonal Indexing System
# MAGIC
# MAGIC ### What is H3?
# MAGIC
# MAGIC H3 is a hierarchical hexagonal geospatial indexing system developed by Uber Technologies.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Hexagonal grid cells covering the entire Earth
# MAGIC - 16 resolution levels (0-15)
# MAGIC - Hierarchical parent-child relationships
# MAGIC - Constant cell edge count (6 edges per cell)
# MAGIC
# MAGIC ### Why Hexagons?
# MAGIC
# MAGIC **Advantages over Square Grids:**
# MAGIC
# MAGIC 1. **Uniform Neighbor Distance**
# MAGIC    - All neighbors equidistant from center
# MAGIC    - Squares have 8 neighbors at 2 different distances
# MAGIC    - Better for proximity analysis
# MAGIC
# MAGIC 2. **Better Angular Resolution**
# MAGIC    - 6 neighbors vs 8 (squares) reduces edge cases
# MAGIC    - More natural for radial queries
# MAGIC
# MAGIC 3. **Reduced Edge Effects**
# MAGIC    - Hexagons tile more naturally on curved surfaces
# MAGIC    - Less distortion near poles
# MAGIC
# MAGIC 4. **Optimal Packing**
# MAGIC    - Best coverage-to-perimeter ratio (after circles)
# MAGIC    - Efficient for aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 Resolution Levels
# MAGIC
# MAGIC ### Understanding Resolution Trade-offs
# MAGIC
# MAGIC | Resolution | Avg Cell Area | Avg Edge Length | Total Cells (Earth) | Use Case |
# MAGIC |------------|---------------|-----------------|---------------------|----------|
# MAGIC | 0 | 4,357,449 km² | 1,107 km | 122 | Continental analysis |
# MAGIC | 1 | 609,788 km² | 418 km | 842 | Country-level |
# MAGIC | 2 | 86,801 km² | 158 km | 5,882 | State/province |
# MAGIC | 3 | 12,393 km² | 59.8 km | 41,162 | Metropolitan areas |
# MAGIC | 4 | 1,770 km² | 22.6 km | 288,122 | City districts |
# MAGIC | 5 | 252.9 km² | 8.54 km | 2,016,842 | Neighborhoods |
# MAGIC | 6 | 36.1 km² | 3.23 km | 14,117,882 | Large zones |
# MAGIC | 7 | 5.16 km² | 1.22 km | 98,825,162 | Service areas |
# MAGIC | 8 | 0.737 km² | 461 m | 691,776,122 | Block groups |
# MAGIC | 9 | 0.105 km² | 174 m | 4,842,432,842 | City blocks |
# MAGIC | 10 | 15,047 m² | 66 m | 33,897,029,882 | Building clusters |
# MAGIC | 11 | 2,149 m² | 25 m | 237,279,209,162 | Large buildings |
# MAGIC | 12 | 307 m² | 9.4 m | 1,660,954,464,122 | Buildings |
# MAGIC | 13 | 43.9 m² | 3.5 m | 11,626,681,248,842 | Rooms |
# MAGIC | 14 | 6.27 m² | 1.3 m | 81,386,768,741,882 | Precise locations |
# MAGIC | 15 | 0.895 m² | 0.5 m | 569,707,381,193,162 | Sub-meter precision |
# MAGIC
# MAGIC ### Resolution Selection Guidelines
# MAGIC
# MAGIC **Consider:**
# MAGIC - **Analysis scale**: City-wide? Building-level?
# MAGIC - **Data density**: Points per square km
# MAGIC - **Performance**: Higher resolution = more cells = more computation
# MAGIC - **Aggregation needs**: Multi-resolution analysis?
# MAGIC
# MAGIC **Rule of Thumb:**
# MAGIC - Start with resolution that gives 10-100 points per cell
# MAGIC - Test performance with sample data
# MAGIC - Adjust based on query patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Join Strategies
# MAGIC
# MAGIC ### Strategy 1: Broadcast Join
# MAGIC
# MAGIC **When to Use:**
# MAGIC - One dataset is small (< 10MB typically)
# MAGIC - Can fit in executor memory
# MAGIC - Example: Join 1M points to 100 city boundaries
# MAGIC
# MAGIC **How It Works:**
# MAGIC ```
# MAGIC 1. Small dataset copied to all executors
# MAGIC 2. Large dataset partitioned across cluster
# MAGIC 3. Each partition performs local joins
# MAGIC 4. No shuffle required!
# MAGIC ```
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Fast - no network shuffle
# MAGIC - Simple implementation
# MAGIC - Predictable performance
# MAGIC
# MAGIC **Limitations:**
# MAGIC - Small dataset size constraint
# MAGIC - Memory pressure if broadcast too large
# MAGIC
# MAGIC ### Strategy 2: Partitioned Join (Range-based)
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Both datasets are large
# MAGIC - Data has spatial clustering
# MAGIC - Need to minimize shuffle
# MAGIC
# MAGIC **How It Works:**
# MAGIC ```
# MAGIC 1. Partition both datasets by spatial region
# MAGIC 2. Co-locate related partitions
# MAGIC 3. Join within partitions
# MAGIC 4. Combine results
# MAGIC ```
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Scalable to very large datasets
# MAGIC - Reduces data movement
# MAGIC - Leverages data locality
# MAGIC
# MAGIC **Considerations:**
# MAGIC - Requires good partitioning key
# MAGIC - Data skew can cause hot partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3-Based Spatial Joins
# MAGIC
# MAGIC ### The H3 Join Strategy
# MAGIC
# MAGIC **Concept:** Use H3 cells as join keys to avoid expensive geometric comparisons
# MAGIC
# MAGIC **Process:**
# MAGIC ```
# MAGIC 1. Convert points to H3 cells
# MAGIC 2. Convert polygons to covering H3 cells (polyfill)
# MAGIC 3. Join on H3 cell ID (exact match - very fast!)
# MAGIC 4. Post-filter with precise geometric test
# MAGIC ```
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC # Without H3: 1M × 10K = 10B comparisons
# MAGIC # With H3: 1M H3 lookups + ~10M precise checks
# MAGIC # Speed improvement: 100-1000x
# MAGIC ```
# MAGIC
# MAGIC ### Why H3 Joins Are Fast
# MAGIC
# MAGIC 1. **Exact Key Match**: H3 cell ID is just a string/integer
# MAGIC 2. **Hash Join**: Standard database join optimization applies
# MAGIC 3. **Reduced Candidates**: Filter out ~99% of comparisons
# MAGIC 4. **Parallelizable**: Each H3 cell processed independently
# MAGIC
# MAGIC ### H3 Join Accuracy
# MAGIC
# MAGIC **Important:** H3 join is an approximation!
# MAGIC
# MAGIC - Point at cell boundary might be in wrong cell
# MAGIC - Polygon edges might cross cell boundaries
# MAGIC - **Always follow with precise geometric test for critical applications**
# MAGIC
# MAGIC **Two-Phase Approach:**
# MAGIC ```python
# MAGIC # Phase 1: H3 filter (fast, approximate)
# MAGIC candidates = points.join(polygons, on='h3_cell')
# MAGIC
# MAGIC # Phase 2: Precise test (only on candidates)
# MAGIC results = candidates.filter(polygon.contains(point))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Geometric Operations
# MAGIC
# MAGIC ### Buffer Operations
# MAGIC
# MAGIC **Purpose:** Create zones around geometries
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Service area definition (e.g., 5km delivery radius)
# MAGIC - Proximity analysis
# MAGIC - Catchment areas
# MAGIC
# MAGIC **Considerations:**
# MAGIC - Buffer distance depends on coordinate system
# MAGIC - Large buffers on many geometries = expensive
# MAGIC - Consider simplifying before buffering
# MAGIC
# MAGIC **Performance Tips:**
# MAGIC ```python
# MAGIC # Bad: Buffer in lat/long
# MAGIC buffered = point.buffer(0.05)  # What distance is this?
# MAGIC
# MAGIC # Good: Transform to projected CRS first
# MAGIC point_projected = transform(point, 'EPSG:3857')
# MAGIC buffered = point_projected.buffer(5000)  # 5000 meters
# MAGIC ```
# MAGIC
# MAGIC ### Simplification
# MAGIC
# MAGIC **Purpose:** Reduce geometry complexity while preserving shape
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Visualization at small scales
# MAGIC - Reducing computation time
# MAGIC - File size reduction
# MAGIC
# MAGIC **Douglas-Peucker Algorithm:**
# MAGIC - Tolerance parameter controls simplification
# MAGIC - Higher tolerance = more simplification
# MAGIC - Test to find optimal balance
# MAGIC
# MAGIC **Example:**
# MAGIC ```python
# MAGIC # Original: 10,000 vertices
# MAGIC simplified = polygon.simplify(tolerance=100, preserve_topology=True)
# MAGIC # Result: 500 vertices, 20x faster operations
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Aggregations
# MAGIC
# MAGIC ### Grid-Based Aggregation Pattern
# MAGIC
# MAGIC **Concept:** Aggregate point data into spatial grid cells
# MAGIC
# MAGIC **Common Use Cases:**
# MAGIC - Heatmaps
# MAGIC - Density analysis
# MAGIC - Hot spot detection
# MAGIC - Statistical summaries by area
# MAGIC
# MAGIC **Implementation:**
# MAGIC ```python
# MAGIC # Using H3
# MAGIC points_with_h3 = points.assign(
# MAGIC     h3_cell=lambda df: df.apply(lambda row: h3.geo_to_h3(row.lat, row.lon, resolution), axis=1)
# MAGIC )
# MAGIC
# MAGIC # Aggregate by cell
# MAGIC aggregated = points_with_h3.groupby('h3_cell').agg({
# MAGIC     'value': ['count', 'sum', 'mean'],
# MAGIC     'h3_cell': 'first'
# MAGIC })
# MAGIC ```
# MAGIC
# MAGIC ### Hot Spot Analysis (Getis-Ord Gi*)
# MAGIC
# MAGIC **Purpose:** Identify statistically significant spatial clusters
# MAGIC
# MAGIC **What It Detects:**
# MAGIC - High-value clusters (hot spots)
# MAGIC - Low-value clusters (cold spots)
# MAGIC - Statistical significance (not just high values)
# MAGIC
# MAGIC **Applications:**
# MAGIC - Crime analysis
# MAGIC - Disease outbreak detection
# MAGIC - Retail site selection
# MAGIC - Traffic accident patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Principles
# MAGIC
# MAGIC ### 1. Reduce Data Early
# MAGIC
# MAGIC **Spatial Filtering:**
# MAGIC ```python
# MAGIC # Bad: Load all data, then filter
# MAGIC all_data = spark.read.parquet("large_dataset")
# MAGIC filtered = all_data.filter(within_bounds(geometry))
# MAGIC
# MAGIC # Good: Filter with predicate pushdown
# MAGIC filtered = spark.read.parquet("large_dataset") \
# MAGIC     .filter((col("lon") > west) & (col("lon") < east)) \
# MAGIC     .filter((col("lat") > south) & (col("lat") < north))
# MAGIC ```
# MAGIC
# MAGIC ### 2. Partition Strategically
# MAGIC
# MAGIC **Spatial Partitioning:**
# MAGIC - Partition by H3 cells at appropriate resolution
# MAGIC - Co-locate spatially related data
# MAGIC - Balance partition sizes
# MAGIC
# MAGIC ```python
# MAGIC # Partition by H3 for spatial queries
# MAGIC df.write \
# MAGIC     .partitionBy("h3_cell_res5") \
# MAGIC     .parquet("output_path")
# MAGIC ```
# MAGIC
# MAGIC ### 3. Choose Right Data Types
# MAGIC
# MAGIC | Format | Use Case | Size | Speed |
# MAGIC |--------|----------|------|-------|
# MAGIC | WKT | Human readable, debugging | Large | Slow |
# MAGIC | WKB | Binary storage | Medium | Fast |
# MAGIC | GeoJSON | Web APIs, interop | Large | Medium |
# MAGIC | Coordinates | Simple points | Small | Fastest |
# MAGIC
# MAGIC ### 4. Cache Wisely
# MAGIC
# MAGIC ```python
# MAGIC # Cache expensive spatial computations
# MAGIC polygons_with_h3 = polygons.assign(
# MAGIC     h3_cells=lambda df: df.geometry.apply(h3_polyfill)
# MAGIC ).cache()  # Reuse in multiple operations
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Optimization Workflow
# MAGIC
# MAGIC ### Step 1: Profile Current Performance
# MAGIC ```python
# MAGIC # Enable timing
# MAGIC import time
# MAGIC start = time.time()
# MAGIC result = expensive_spatial_query()
# MAGIC print(f"Duration: {time.time() - start:.2f}s")
# MAGIC
# MAGIC # Check execution plan
# MAGIC result.explain()
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Identify Bottlenecks
# MAGIC - Is it a shuffle? → Better partitioning needed
# MAGIC - Is it UDF-heavy? → Vectorize operations
# MAGIC - Too many tasks? → Increase partition size
# MAGIC - Too few tasks? → Repartition for parallelism
# MAGIC
# MAGIC ### Step 3: Apply Optimizations
# MAGIC 1. Add spatial index (H3, R-tree)
# MAGIC 2. Optimize join strategy
# MAGIC 3. Simplify geometries if possible
# MAGIC 4. Use appropriate resolution
# MAGIC
# MAGIC ### Step 4: Validate and Benchmark
# MAGIC ```python
# MAGIC # Compare strategies
# MAGIC strategies = [broadcast_join, partitioned_join, h3_join]
# MAGIC for strategy in strategies:
# MAGIC     start = time.time()
# MAGIC     result = strategy()
# MAGIC     duration = time.time() - start
# MAGIC     print(f"{strategy.__name__}: {duration:.2f}s")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Pitfalls and Solutions
# MAGIC
# MAGIC ### Pitfall 1: Wrong H3 Resolution
# MAGIC
# MAGIC **Problem:** Too high = billions of cells, too low = no benefit
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Test multiple resolutions
# MAGIC for res in range(6, 10):
# MAGIC     cell_count = data.assign(h3=lambda df: h3_index(df, res))['h3'].nunique()
# MAGIC     avg_points = len(data) / cell_count
# MAGIC     print(f"Res {res}: {cell_count} cells, {avg_points:.1f} points/cell")
# MAGIC # Choose resolution with 10-100 points per cell
# MAGIC ```
# MAGIC
# MAGIC ### Pitfall 2: Forgetting Coordinate Systems
# MAGIC
# MAGIC **Problem:** Buffer in degrees, get inconsistent distances
# MAGIC
# MAGIC **Solution:** Always project to appropriate CRS for measurements
# MAGIC ```python
# MAGIC # Buffer in meters using projected CRS
# MAGIC gdf_projected = gdf.to_crs('EPSG:3857')
# MAGIC buffered = gdf_projected.buffer(1000)  # 1km
# MAGIC buffered_latlon = buffered.to_crs('EPSG:4326')
# MAGIC ```
# MAGIC
# MAGIC ### Pitfall 3: Invalid Geometries
# MAGIC
# MAGIC **Problem:** Operations fail or produce incorrect results
# MAGIC
# MAGIC **Solution:** Validate and repair
# MAGIC ```python
# MAGIC from shapely.validation import make_valid
# MAGIC
# MAGIC # Check validity
# MAGIC invalid = gdf[~gdf.geometry.is_valid]
# MAGIC
# MAGIC # Repair
# MAGIC gdf['geometry'] = gdf.geometry.apply(
# MAGIC     lambda g: make_valid(g) if not g.is_valid else g
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-World Use Cases
# MAGIC
# MAGIC ### Use Case 1: Last-Mile Delivery Optimization
# MAGIC
# MAGIC **Challenge:** Assign 100K daily orders to delivery zones
# MAGIC
# MAGIC **Solution:**
# MAGIC 1. Represent zones as H3 cells (resolution 8)
# MAGIC 2. Convert order locations to H3
# MAGIC 3. Join on H3 cell ID
# MAGIC 4. Calculate optimal routes within zones
# MAGIC
# MAGIC **Result:** 500x faster than geometric intersection
# MAGIC
# MAGIC ### Use Case 2: Retail Site Selection
# MAGIC
# MAGIC **Challenge:** Find optimal locations for 50 new stores
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. Grid market area with H3 (resolution 7)
# MAGIC 2. Aggregate customer data by cell
# MAGIC 3. Calculate population density, income, competition
# MAGIC 4. Hot spot analysis to identify high-value areas
# MAGIC 5. Multi-criteria optimization for site selection
# MAGIC
# MAGIC ### Use Case 3: Real-Time Geofencing
# MAGIC
# MAGIC **Challenge:** Alert when vehicles enter/exit 10K zones
# MAGIC
# MAGIC **Solution:**
# MAGIC 1. Pre-compute H3 cells for each zone (resolution 9)
# MAGIC 2. Convert vehicle location to H3 in real-time
# MAGIC 3. Hash lookup for zone membership
# MAGIC 4. Precise boundary test only when near edges
# MAGIC
# MAGIC **Result:** < 1ms per location check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Data Preparation
# MAGIC - ✓ Validate geometries before operations
# MAGIC - ✓ Use consistent coordinate reference systems
# MAGIC - ✓ Simplify geometries when precision allows
# MAGIC - ✓ Partition data by spatial keys (H3)
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC - ✓ Use H3 indexing for large-scale operations
# MAGIC - ✓ Choose appropriate join strategy for data sizes
# MAGIC - ✓ Cache intermediate results that are reused
# MAGIC - ✓ Profile queries and optimize bottlenecks
# MAGIC
# MAGIC ### Accuracy and Quality
# MAGIC - ✓ Follow H3 filtering with precise geometric tests
# MAGIC - ✓ Handle edge cases (null geometries, boundaries)
# MAGIC - ✓ Test with sample data before full scale
# MAGIC - ✓ Document CRS and transformations
# MAGIC
# MAGIC ### Production Readiness
# MAGIC - ✓ Monitor query performance over time
# MAGIC - ✓ Handle data quality issues gracefully
# MAGIC - ✓ Implement error handling and logging
# MAGIC - ✓ Document expected data volumes and SLAs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand the theory, let's put it into practice:
# MAGIC
# MAGIC 1. **Demo 1: H3 Hexagonal Indexing** - Hands-on with H3 operations
# MAGIC 2. **Demo 2: Spatial Joins at Scale** - Compare join strategies
# MAGIC 3. **Demo 3: Advanced Geometric Operations** - Buffers, simplification, more
# MAGIC 4. **Demo 4: Spatial Aggregations** - Grid-based analytics
# MAGIC 5. **Labs 5-7** - Apply techniques to real-world scenarios
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC - Spatial indexing is essential for performance at scale
# MAGIC - H3 provides fast, approximate spatial joins
# MAGIC - Always validate with precise geometric tests
# MAGIC - Choose resolution based on analysis needs
# MAGIC - Profile and optimize iteratively
# MAGIC
# MAGIC **Ready to dive into hands-on demos? Let's go!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [H3 Documentation](https://h3geo.org/)
# MAGIC - [Uber Engineering: H3](https://eng.uber.com/h3/)
# MAGIC - [Apache Sedona](https://sedona.apache.org/)
# MAGIC - [GeoPandas Performance Guide](https://geopandas.org/en/stable/docs/user_guide/mergingdata.html)
# MAGIC
# MAGIC ### Research Papers
# MAGIC - "Efficient Spatial Joins Using R-trees"
# MAGIC - "The Quadtree and Related Hierarchical Data Structures"
# MAGIC - "Spatial Join Strategies in Distributed Systems"
# MAGIC
# MAGIC ### Tools and Libraries
# MAGIC - H3-py: Python bindings for H3
# MAGIC - Shapely: Geometric operations
# MAGIC - GeoPandas: Spatial data in pandas
# MAGIC - Kepler.gl: Visualization
