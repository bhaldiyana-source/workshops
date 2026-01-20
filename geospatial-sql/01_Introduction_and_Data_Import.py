# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to Geospatial Data Types and Data Import/Export
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook covers:
# MAGIC - Understanding GEOGRAPHY vs GEOMETRY data types
# MAGIC - Key geospatial concepts (WKT, WKB, EWKT, GeoJSON, SRID)
# MAGIC - Importing geospatial data from various formats
# MAGIC - Exporting geospatial data to different formats
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks Runtime 17.1 or above
# MAGIC - Basic SQL knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Introduction to Geospatial Data Types
# MAGIC
# MAGIC ### GEOGRAPHY vs GEOMETRY
# MAGIC
# MAGIC **GEOGRAPHY**
# MAGIC - Uses latitude/longitude on a spherical model (Earth)
# MAGIC - Default SRID is 4326 (WGS84)
# MAGIC - Best for global-scale, Earth-based data
# MAGIC - Distance calculations account for Earth's curvature
# MAGIC
# MAGIC **GEOMETRY**
# MAGIC - Uses planar (flat) coordinate system
# MAGIC - Can have various SRIDs or 0 for arbitrary coordinates
# MAGIC - Best for local projections or non-Earth spatial data
# MAGIC - Faster for planar calculations
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **WKT** (Well-Known Text): Human-readable text format for geometries
# MAGIC - **WKB** (Well-Known Binary): Binary format for efficient storage
# MAGIC - **EWKT/EWKB**: Extended formats that include SRID information
# MAGIC - **GeoJSON**: JSON-based format for geographic data structures
# MAGIC - **SRID**: Spatial Reference System Identifier (e.g., 4326 for WGS84)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating Geometries from Text Formats

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 From WKT (Well-Known Text)
# MAGIC
# MAGIC WKT is a human-readable format for representing geometries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a GEOMETRY point
# MAGIC SELECT st_geomfromtext('POINT(1 2)') AS point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a GEOMETRY polygon with SRID
# MAGIC SELECT st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 4326) AS polygon;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a GEOGRAPHY from WKT (San Francisco coordinates)
# MAGIC SELECT st_geogfromtext('POINT(-122.4194 37.7749)') AS sf_location;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 From EWKT (Extended Well-Known Text)
# MAGIC
# MAGIC EWKT includes SRID in the text representation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EWKT includes SRID in the text
# MAGIC SELECT st_geomfromewkt('SRID=4326;POINT(-122.4194 37.7749)') AS sf_point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a line from San Francisco to Los Angeles
# MAGIC SELECT st_geogfromewkt('SRID=4326;LINESTRING(-122.4194 37.7749, -118.2437 34.0522)') AS ca_line;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 From WKB (Well-Known Binary)
# MAGIC
# MAGIC WKB is a binary format for efficient storage and transmission.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert WKB to GEOMETRY
# MAGIC -- This WKB represents POINT(1 2)
# MAGIC SELECT st_geomfromwkb(unhex('0101000000000000000000F03F0000000000000040')) AS point_from_wkb;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert WKB to GEOGRAPHY
# MAGIC SELECT st_geogfromwkb(unhex('0101000000000000000000F03F0000000000000040')) AS geog_from_wkb;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 From GeoJSON
# MAGIC
# MAGIC GeoJSON is a popular JSON-based format for geographic data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create GEOMETRY from GeoJSON point
# MAGIC SELECT st_geomfromgeojson('{"type":"Point","coordinates":[-122.4194,37.7749]}') AS point;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create GEOGRAPHY from GeoJSON polygon
# MAGIC SELECT st_geogfromgeojson('{"type":"Polygon","coordinates":[[[-122.5,37.5],[-122.5,38.0],[-122.0,38.0],[-122.0,37.5],[-122.5,37.5]]]}') AS area;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 From Geohash
# MAGIC
# MAGIC Geohash is a geocoding system that encodes a geographic location into a short string.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the polygon bounding box for a geohash (San Francisco area)
# MAGIC SELECT st_geomfromgeohash('9q8yy') AS geohash_polygon;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the center point for a geohash
# MAGIC SELECT st_pointfromgeohash('9q8yy') AS geohash_center;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Generic Conversion Functions
# MAGIC
# MAGIC Auto-detect format and convert to GEOGRAPHY or GEOMETRY.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Auto-detect format and convert to GEOGRAPHY
# MAGIC SELECT to_geography('POINT(-122.4194 37.7749)') AS geog;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Auto-detect format and convert to GEOMETRY
# MAGIC SELECT to_geometry('{"type":"Point","coordinates":[1,2]}') AS geom;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Safe conversions that return NULL on error
# MAGIC SELECT 
# MAGIC   try_to_geography('INVALID WKT') AS safe_geog,
# MAGIC   try_to_geometry('INVALID WKT') AS safe_geom;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exporting Geospatial Data
# MAGIC
# MAGIC Convert geometries to various output formats.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Export to WKT (Well-Known Text)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To WKT (Well-Known Text)
# MAGIC SELECT st_astext(st_geomfromtext('POINT(1 2)')) AS wkt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alternative function name
# MAGIC SELECT st_aswkt(st_geomfromtext('POINT(1 2)')) AS wkt_alt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Export to EWKT (Extended Well-Known Text)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To EWKT (Extended Well-Known Text with SRID)
# MAGIC SELECT st_asewkt(st_geomfromewkt('SRID=4326;POINT(1 2)')) AS ewkt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Export to WKB (Well-Known Binary)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To WKB (Well-Known Binary)
# MAGIC SELECT st_aswkb(st_geomfromtext('POINT(1 2)')) AS wkb;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alternative function name
# MAGIC SELECT st_asbinary(st_geomfromtext('POINT(1 2)')) AS wkb_alt;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Export to EWKB (Extended Well-Known Binary)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To EWKB (Extended Well-Known Binary)
# MAGIC SELECT st_asewkb(st_geomfromewkt('SRID=4326;POINT(1 2)')) AS ewkb;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Export to GeoJSON

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To GeoJSON
# MAGIC SELECT st_asgeojson(st_geomfromtext('POINT(-122.4194 37.7749)')) AS geojson;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GeoJSON for a polygon
# MAGIC SELECT st_asgeojson(st_geomfromtext('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) AS geojson_polygon;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Export to Geohash

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To Geohash (San Francisco with precision 8)
# MAGIC SELECT st_geohash(st_geomfromtext('POINT(-122.4194 37.7749)'), 8) AS geohash;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Different precision levels
# MAGIC SELECT 
# MAGIC   st_geohash(st_geomfromtext('POINT(-122.4194 37.7749)'), 5) AS geohash_5,
# MAGIC   st_geohash(st_geomfromtext('POINT(-122.4194 37.7749)'), 8) AS geohash_8,
# MAGIC   st_geohash(st_geomfromtext('POINT(-122.4194 37.7749)'), 12) AS geohash_12;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Practical Demonstration: Round-trip Conversions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demonstrate round-trip conversion: WKT -> GEOMETRY -> GeoJSON -> GEOMETRY -> WKT
# MAGIC WITH original AS (
# MAGIC   SELECT st_geomfromtext('POINT(-122.4194 37.7749)') AS geom
# MAGIC ),
# MAGIC as_geojson AS (
# MAGIC   SELECT st_asgeojson(geom) AS geojson
# MAGIC   FROM original
# MAGIC ),
# MAGIC back_to_geom AS (
# MAGIC   SELECT st_geomfromgeojson(geojson) AS geom
# MAGIC   FROM as_geojson
# MAGIC )
# MAGIC SELECT st_astext(geom) AS final_wkt
# MAGIC FROM back_to_geom;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Format Comparison Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare all export formats for the same point
# MAGIC SELECT 
# MAGIC   'San Francisco' AS location,
# MAGIC   st_astext(st_geomfromtext('POINT(-122.4194 37.7749)')) AS wkt,
# MAGIC   st_asewkt(st_setsrid(st_geomfromtext('POINT(-122.4194 37.7749)'), 4326)) AS ewkt,
# MAGIC   st_asgeojson(st_geomfromtext('POINT(-122.4194 37.7749)')) AS geojson,
# MAGIC   st_geohash(st_geomfromtext('POINT(-122.4194 37.7749)'), 8) AS geohash;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **GEOGRAPHY vs GEOMETRY**: Choose GEOGRAPHY for lat/lon Earth-based data, GEOMETRY for planar coordinates
# MAGIC 2. **Multiple Input Formats**: Databricks supports WKT, WKB, EWKT, EWKB, GeoJSON, and Geohash
# MAGIC 3. **Flexible Output**: Convert to any format needed for interoperability
# MAGIC 4. **Safe Conversions**: Use `try_to_geography()` and `try_to_geometry()` for error handling
# MAGIC 5. **SRID Management**: Always be aware of your coordinate reference system
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next notebook: **02_Measurements_and_Distances.py** to learn about spatial calculations.
