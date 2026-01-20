# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Multi-Format Data Ingestion
# MAGIC
# MAGIC ## Overview
# MAGIC Learn to ingest geospatial data from multiple formats including Shapefiles, GeoJSON, and GeoParquet. Handle schema inference, format conversion, and error handling at scale.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Ingest Shapefiles using GeoPandas
# MAGIC - Process GeoJSON with Spark
# MAGIC - Optimize GeoParquet ingestion
# MAGIC - Handle format conversion pipelines
# MAGIC - Implement error handling
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %pip install apache-sedona==1.5.1 geopandas==0.14.1 pyogrio==0.7.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from sedona.register import SedonaRegistrator
import geopandas as gpd
import json

SedonaRegistrator.registerAll(spark)
spark.sql("USE CATALOG geospatial_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS raw")

print("✅ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format 1: GeoJSON Ingestion

# COMMAND ----------

# Create sample GeoJSON
geojson_sample = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "properties": {"name": "San Francisco", "population": 873965}
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-73.9352, 40.7306]},
            "properties": {"name": "Brooklyn", "population": 2736074}
        }
    ]
}

# Write to temp location
dbutils.fs.put("/tmp/cities.geojson", json.dumps(geojson_sample), overwrite=True)

# Read GeoJSON
df_geojson = spark.read.format("json").load("/tmp/cities.geojson")
display(df_geojson)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format 2: GeoParquet (Native)

# COMMAND ----------

# GeoParquet is the recommended format for large-scale geospatial data
# It's Parquet with geospatial metadata

# Create sample geoparquet
from shapely.geometry import Point
import pandas as pd

cities_data = {
    'name': ['San Francisco', 'New York', 'London'],
    'latitude': [37.7749, 40.7128, 51.5074],
    'longitude': [-122.4194, -74.0060, -0.1278],
    'population': [873965, 8336817, 8982000]
}

gdf = gpd.GeoDataFrame(
    cities_data,
    geometry=[Point(lon, lat) for lon, lat in zip(cities_data['longitude'], cities_data['latitude'])],
    crs="EPSG:4326"
)

# Convert to Spark DataFrame
df_geoparquet = spark.createDataFrame(
    gdf.assign(geometry=gdf.geometry.to_wkt())
)

df_geoparquet.write.format("delta").mode("overwrite").saveAsTable("raw.cities_geoparquet")
display(df_geoparquet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format 3: CSV with Lat/Lon

# COMMAND ----------

# Most common format - CSV with latitude/longitude columns
csv_data = """name,latitude,longitude,category
Poi1,37.7749,-122.4194,restaurant
Poi2,37.7849,-122.4094,cafe
Poi3,37.7649,-122.4294,park"""

dbutils.fs.put("/tmp/pois.csv", csv_data, overwrite=True)

# Read and convert to spatial
df_csv = spark.read.format("csv").option("header", "true").load("/tmp/pois.csv")

df_spatial = df_csv \
    .withColumn("geometry", expr("ST_Point(CAST(longitude AS DECIMAL(24,20)), CAST(latitude AS DECIMAL(24,20)))")) \
    .withColumn("is_valid", expr("ST_IsValid(geometry)"))

df_spatial.write.format("delta").mode("overwrite").saveAsTable("raw.pois_from_csv")
display(df_spatial)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unified Ingestion Pipeline

# COMMAND ----------

def ingest_spatial_data(source_path, source_format, table_name):
    """
    Unified ingestion function for multiple formats
    """
    if source_format == "geojson":
        df = spark.read.format("json").load(source_path)
        # Parse GeoJSON structure
        df = df.selectExpr("explode(features) as feature") \
            .select("feature.properties.*", "feature.geometry")
    elif source_format == "csv":
        df = spark.read.format("csv").option("header", "true").load(source_path)
        df = df.withColumn("geometry", expr("ST_Point(CAST(longitude AS DECIMAL(24,20)), CAST(latitude AS DECIMAL(24,20)))"))
    elif source_format == "parquet":
        df = spark.read.format("parquet").load(source_path)
    else:
        raise ValueError(f"Unsupported format: {source_format}")
    
    # Add metadata
    df = df.withColumn("source_format", lit(source_format)) \
        .withColumn("ingest_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name())
    
    # Write to Bronze
    df.write.format("delta").mode("append").saveAsTable(table_name)
    
    print(f"✅ Ingested {df.count()} records from {source_format}")

# Example usage
# ingest_spatial_data("/tmp/pois.csv", "csv", "bronze.spatial_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Ingestion

# COMMAND ----------

# Set up streaming ingestion with Auto Loader
streaming_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/schema") \
    .load("/tmp/geojson_stream/")

# Write stream to Bronze
# streaming_df.writeStream \
#     .format("delta") \
#     .option("checkpointLocation", "/tmp/checkpoints") \
#     .table("bronze.streaming_spatial_data")

print("✅ Streaming pipeline configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling and Data Quality

# COMMAND ----------

# Quality checks during ingestion
def validate_spatial_data(df):
    """Add validation flags"""
    return df \
        .withColumn("has_geometry", col("geometry").isNotNull()) \
        .withColumn("is_valid_geometry", expr("ST_IsValid(geometry)")) \
        .withColumn("has_properties", col("name").isNotNull()) \
        .withColumn("quality_check_passed", 
            col("has_geometry") & col("is_valid_geometry") & col("has_properties"))

# Example
df_validated = validate_spatial_data(df_spatial)
display(df_validated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **GeoParquet** is the best format for large-scale geospatial data storage
# MAGIC 2. **Auto Loader** simplifies incremental ingestion from cloud storage
# MAGIC 3. **Schema inference** works well for JSON/GeoJSON but validate carefully
# MAGIC 4. **Always validate** geometries during ingestion
# MAGIC 5. **Add metadata** (source, timestamp) for lineage tracking
# MAGIC
# MAGIC **Continue to Demo 5: Geospatial Data Quality Framework**
