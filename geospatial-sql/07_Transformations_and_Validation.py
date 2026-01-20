# Databricks notebook source
# MAGIC %md
# MAGIC # Affine Transformations and Geometry Validation
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook covers:
# MAGIC - Affine transformations (translate, scale, rotate)
# MAGIC - Geometry validation
# MAGIC - Best practices for valid geometries
# MAGIC - Debugging invalid geometries
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed notebook 06_Overlay_and_Processing.py
# MAGIC - Understanding of coordinate systems

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Affine Transformations
# MAGIC
# MAGIC Affine transformations modify geometries through mathematical operations while preserving certain properties like parallelism.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Translate - Move Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Translate: Move a geometry by X and Y offset
# MAGIC SELECT st_astext(
# MAGIC   st_translate(
# MAGIC     st_point(1, 1),
# MAGIC     5,  -- X offset
# MAGIC     10  -- Y offset
# MAGIC   )
# MAGIC ) AS translated_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Translate a polygon
# MAGIC SELECT st_astext(
# MAGIC   st_translate(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'),
# MAGIC     10,  -- Move 10 units right
# MAGIC     20   -- Move 20 units up
# MAGIC   )
# MAGIC ) AS translated_polygon;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Translate a linestring
# MAGIC SELECT st_astext(
# MAGIC   st_translate(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 5 5, 10 0)'),
# MAGIC     -3,  -- Move left
# MAGIC     2    -- Move up
# MAGIC   )
# MAGIC ) AS translated_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare original and translated
# MAGIC WITH original AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Original' AS version,
# MAGIC   st_astext(geom) AS geometry,
# MAGIC   st_x(st_centroid(geom)) AS center_x,
# MAGIC   st_y(st_centroid(geom)) AS center_y
# MAGIC FROM original
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Translated',
# MAGIC   st_astext(st_translate(geom, 10, 20)),
# MAGIC   st_x(st_centroid(st_translate(geom, 10, 20))),
# MAGIC   st_y(st_centroid(st_translate(geom, 10, 20)))
# MAGIC FROM original;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Scale - Resize Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Scale: Scale a geometry by X and Y factors
# MAGIC SELECT st_astext(
# MAGIC   st_scale(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 1)'),
# MAGIC     2,  -- X factor (double width)
# MAGIC     3   -- Y factor (triple height)
# MAGIC   )
# MAGIC ) AS scaled_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Scale a polygon uniformly
# MAGIC SELECT st_astext(
# MAGIC   st_scale(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'),
# MAGIC     2,  -- Double in both dimensions
# MAGIC     2
# MAGIC   )
# MAGIC ) AS scaled_polygon;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Scale down (shrink)
# MAGIC SELECT st_astext(
# MAGIC   st_scale(
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
# MAGIC     0.5,  -- Half size
# MAGIC     0.5
# MAGIC   )
# MAGIC ) AS scaled_down_polygon;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare areas before and after scaling
# MAGIC WITH original AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))') AS geom
# MAGIC ),
# MAGIC scaled AS (
# MAGIC   SELECT st_scale(geom, 2, 2) AS geom
# MAGIC   FROM original
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Original' AS version,
# MAGIC   ROUND(st_area((SELECT geom FROM original)), 2) AS area
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Scaled 2x',
# MAGIC   ROUND(st_area(geom), 2)
# MAGIC FROM scaled;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Rotate - Rotate Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rotate: Rotate a geometry around origin (angle in radians)
# MAGIC -- 90 degrees = π/2 radians ≈ 1.5708
# MAGIC SELECT st_astext(
# MAGIC   st_rotate(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 0)'),
# MAGIC     1.5708  -- 90 degrees in radians
# MAGIC   )
# MAGIC ) AS rotated_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rotate 45 degrees (π/4 radians)
# MAGIC SELECT st_astext(
# MAGIC   st_rotate(
# MAGIC     st_geomfromtext('LINESTRING(0 0, 1 0)'),
# MAGIC     0.7854  -- 45 degrees
# MAGIC   )
# MAGIC ) AS rotated_45_degrees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rotate a square polygon
# MAGIC SELECT st_astext(
# MAGIC   st_rotate(
# MAGIC     st_geomfromtext('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'),
# MAGIC     0.7854  -- 45 degrees
# MAGIC   )
# MAGIC ) AS rotated_square;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rotate at different angles
# MAGIC WITH original AS (
# MAGIC   SELECT st_geomfromtext('LINESTRING(0 0, 2 0)') AS geom
# MAGIC )
# MAGIC SELECT 
# MAGIC   '0 degrees' AS rotation,
# MAGIC   st_astext(geom) AS geometry
# MAGIC FROM original
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '45 degrees',
# MAGIC   st_astext(st_rotate(geom, 0.7854))
# MAGIC FROM original
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '90 degrees',
# MAGIC   st_astext(st_rotate(geom, 1.5708))
# MAGIC FROM original
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '180 degrees',
# MAGIC   st_astext(st_rotate(geom, 3.1416))
# MAGIC FROM original;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Combined Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combine multiple transformations
# MAGIC WITH original AS (
# MAGIC   SELECT st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))') AS geom
# MAGIC ),
# MAGIC scaled AS (
# MAGIC   SELECT st_scale(geom, 1.5, 1.5) AS geom
# MAGIC   FROM original
# MAGIC ),
# MAGIC rotated AS (
# MAGIC   SELECT st_rotate(geom, 0.7854) AS geom
# MAGIC   FROM scaled
# MAGIC ),
# MAGIC translated AS (
# MAGIC   SELECT st_translate(geom, 10, 10) AS geom
# MAGIC   FROM rotated
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Original' AS step,
# MAGIC   st_astext((SELECT geom FROM original)) AS geometry,
# MAGIC   ROUND(st_area((SELECT geom FROM original)), 4) AS area
# MAGIC UNION ALL
# MAGIC SELECT 'After Scale', st_astext(geom), ROUND(st_area(geom), 4) FROM scaled
# MAGIC UNION ALL
# MAGIC SELECT 'After Rotate', st_astext(geom), ROUND(st_area(geom), 4) FROM rotated
# MAGIC UNION ALL
# MAGIC SELECT 'After Translate', st_astext(geom), ROUND(st_area(geom), 4) FROM translated;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Geometry Validation
# MAGIC
# MAGIC Ensure geometries are valid according to OGC standards.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Valid Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if a geometry is valid
# MAGIC SELECT st_isvalid(
# MAGIC   st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
# MAGIC ) AS is_valid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Valid point
# MAGIC SELECT st_isvalid(st_point(1, 2)) AS is_valid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Valid linestring
# MAGIC SELECT st_isvalid(
# MAGIC   st_geomfromtext('LINESTRING(0 0, 1 1, 2 2)')
# MAGIC ) AS is_valid;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Invalid Geometries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Invalid polygon (self-intersecting bowtie)
# MAGIC SELECT st_isvalid(
# MAGIC   st_geomfromtext('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))')
# MAGIC ) AS is_valid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check multiple geometries for validity
# MAGIC WITH test_geometries AS (
# MAGIC   SELECT 
# MAGIC     'Valid Square' AS name,
# MAGIC     st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))') AS geom
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'Self-Intersecting',
# MAGIC     st_geomfromtext('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))')
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'Valid Triangle',
# MAGIC     st_geomfromtext('POLYGON((0 0, 4 0, 2 3, 0 0))')
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'Valid Line',
# MAGIC     st_geomfromtext('LINESTRING(0 0, 5 5, 10 0)')
# MAGIC )
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   st_isvalid(geom) AS is_valid,
# MAGIC   st_geometrytype(geom) AS geometry_type
# MAGIC FROM test_geometries;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Common Validation Issues

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Issue 1: Self-intersection
# MAGIC SELECT 
# MAGIC   'Self-Intersecting Polygon' AS issue_type,
# MAGIC   st_isvalid(st_geomfromtext('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))')) AS is_valid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Issue 2: Unclosed polygon (actually creates invalid WKT)
# MAGIC -- This will be caught during parsing, not validation
# MAGIC SELECT 
# MAGIC   'Valid Closed Polygon' AS issue_type,
# MAGIC   st_isvalid(st_geomfromtext('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')) AS is_valid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Issue 3: Spike or duplicate consecutive points are typically valid
# MAGIC SELECT 
# MAGIC   'Line with Spike' AS issue_type,
# MAGIC   st_isvalid(st_geomfromtext('LINESTRING(0 0, 5 5, 2 2, 10 10)')) AS is_valid;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Practical Example: Data Quality Check

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate a collection of geometries
# MAGIC WITH geographic_features AS (
# MAGIC   SELECT 1 AS id, 'Park' AS name, 
# MAGIC          st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))') AS boundary
# MAGIC   UNION ALL
# MAGIC   SELECT 2, 'Trail', 
# MAGIC          st_geomfromtext('LINESTRING(0 0, 5 5, 10 10)')
# MAGIC   UNION ALL
# MAGIC   SELECT 3, 'Viewpoint', 
# MAGIC          st_point(5, 5)
# MAGIC   UNION ALL
# MAGIC   SELECT 4, 'Invalid Area',
# MAGIC          st_geomfromtext('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))')
# MAGIC )
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   name,
# MAGIC   st_geometrytype(boundary) AS geom_type,
# MAGIC   st_isvalid(boundary) AS is_valid,
# MAGIC   st_isempty(boundary) AS is_empty,
# MAGIC   st_issimple(boundary) AS is_simple,
# MAGIC   CASE 
# MAGIC     WHEN NOT st_isvalid(boundary) THEN 'NEEDS REPAIR'
# MAGIC     WHEN st_isempty(boundary) THEN 'EMPTY GEOMETRY'
# MAGIC     ELSE 'OK'
# MAGIC   END AS status
# MAGIC FROM geographic_features;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Practical Example: Transformation Pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complete transformation pipeline for data normalization
# MAGIC WITH source_data AS (
# MAGIC   -- Original data in arbitrary coordinate system
# MAGIC   SELECT 'Feature A' AS name, st_geomfromtext('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))') AS geom
# MAGIC   UNION ALL
# MAGIC   SELECT 'Feature B', st_geomfromtext('POLYGON((5 5, 5 7, 7 7, 7 5, 5 5))')
# MAGIC   UNION ALL
# MAGIC   SELECT 'Feature C', st_geomfromtext('POLYGON((10 0, 10 3, 13 3, 13 0, 10 0))')
# MAGIC ),
# MAGIC normalized AS (
# MAGIC   -- Step 1: Translate to origin
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     st_translate(geom, -st_x(st_centroid(geom)), -st_y(st_centroid(geom))) AS geom
# MAGIC   FROM source_data
# MAGIC ),
# MAGIC scaled AS (
# MAGIC   -- Step 2: Normalize scale
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     st_scale(geom, 10, 10) AS geom
# MAGIC   FROM normalized
# MAGIC ),
# MAGIC final AS (
# MAGIC   -- Step 3: Translate to target location
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     st_translate(geom, 100, 100) AS geom
# MAGIC   FROM scaled
# MAGIC )
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   st_astext(geom) AS transformed_geometry,
# MAGIC   st_isvalid(geom) AS is_valid,
# MAGIC   ROUND(st_area(geom), 2) AS area,
# MAGIC   st_astext(st_centroid(geom)) AS center
# MAGIC FROM final;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practical Example: Coordinate System Alignment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Align geometries from different coordinate systems
# MAGIC WITH dataset_a AS (
# MAGIC   -- Data in system A (needs rotation and translation)
# MAGIC   SELECT 'Site 1' AS name, st_geomfromtext('POINT(10 5)') AS location
# MAGIC   UNION ALL
# MAGIC   SELECT 'Site 2', st_geomfromtext('POINT(15 8)')
# MAGIC ),
# MAGIC dataset_b AS (
# MAGIC   -- Data in system B (reference system)
# MAGIC   SELECT 'Reference 1' AS name, st_geomfromtext('POINT(100 100)') AS location
# MAGIC ),
# MAGIC aligned_a AS (
# MAGIC   -- Align dataset A to dataset B coordinate system
# MAGIC   -- Step 1: Rotate 45 degrees
# MAGIC   -- Step 2: Scale to match
# MAGIC   -- Step 3: Translate to reference
# MAGIC   SELECT 
# MAGIC     name,
# MAGIC     st_translate(
# MAGIC       st_scale(
# MAGIC         st_rotate(location, 0.7854),
# MAGIC         2, 2
# MAGIC       ),
# MAGIC       80, 85
# MAGIC     ) AS location
# MAGIC   FROM dataset_a
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Dataset A (aligned)' AS dataset,
# MAGIC   name,
# MAGIC   st_astext(location) AS location
# MAGIC FROM aligned_a
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Dataset B (reference)',
# MAGIC   name,
# MAGIC   st_astext(location)
# MAGIC FROM dataset_b;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation Best Practices

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comprehensive validation check
# MAGIC WITH test_data AS (
# MAGIC   SELECT 1 AS id, st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))') AS geom
# MAGIC   UNION ALL
# MAGIC   SELECT 2, st_geomfromtext('LINESTRING(0 0, 5 5)')
# MAGIC   UNION ALL
# MAGIC   SELECT 3, st_point(1, 2)
# MAGIC   UNION ALL
# MAGIC   SELECT 4, st_geomfromtext('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))')
# MAGIC ),
# MAGIC validation_results AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     geom,
# MAGIC     st_isvalid(geom) AS is_valid,
# MAGIC     st_isempty(geom) AS is_empty,
# MAGIC     st_issimple(geom) AS is_simple,
# MAGIC     st_geometrytype(geom) AS geom_type,
# MAGIC     st_dimension(geom) AS dimension
# MAGIC   FROM test_data
# MAGIC )
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   geom_type,
# MAGIC   dimension,
# MAGIC   is_valid,
# MAGIC   is_empty,
# MAGIC   is_simple,
# MAGIC   CASE 
# MAGIC     WHEN is_empty THEN 'WARNING: Empty geometry'
# MAGIC     WHEN NOT is_valid THEN 'ERROR: Invalid geometry'
# MAGIC     WHEN NOT is_simple THEN 'WARNING: Non-simple geometry'
# MAGIC     ELSE 'PASS: All checks passed'
# MAGIC   END AS validation_status
# MAGIC FROM validation_results
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN NOT is_valid THEN 1
# MAGIC     WHEN is_empty THEN 2
# MAGIC     WHEN NOT is_simple THEN 3
# MAGIC     ELSE 4
# MAGIC   END,
# MAGIC   id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Affine Transformations**:
# MAGIC    - `st_translate(geom, x_offset, y_offset)` moves geometry
# MAGIC    - `st_scale(geom, x_factor, y_factor)` resizes geometry
# MAGIC    - `st_rotate(geom, angle_radians)` rotates around origin
# MAGIC    - Transformations can be combined in sequence
# MAGIC    - Transformations preserve certain geometric properties
# MAGIC
# MAGIC 2. **Geometry Validation**:
# MAGIC    - `st_isvalid()` checks OGC validity standards
# MAGIC    - `st_isempty()` checks for empty geometries
# MAGIC    - `st_issimple()` checks for self-intersections
# MAGIC    - Always validate geometries from external sources
# MAGIC
# MAGIC 3. **Common Invalid Geometries**:
# MAGIC    - Self-intersecting polygons (bowtie shapes)
# MAGIC    - Polygons with unclosed rings
# MAGIC    - Duplicate consecutive points (sometimes)
# MAGIC    - Zero-area polygons
# MAGIC
# MAGIC 4. **Best Practices**:
# MAGIC    - Validate geometries before complex operations
# MAGIC    - Check for empty geometries in your data
# MAGIC    - Test transformations on sample data first
# MAGIC    - Document coordinate system assumptions
# MAGIC    - Use validation in data quality pipelines
# MAGIC
# MAGIC 5. **Coordinate System Alignment**:
# MAGIC    - Use transformations to align data from different sources
# MAGIC    - Combine rotation, scaling, and translation as needed
# MAGIC    - Always validate after transformations
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next notebook: **08_Practical_Examples.py** to see complete real-world use cases combining all the functions learned so far.
