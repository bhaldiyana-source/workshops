# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC Ingests raw data from source systems into bronze tables

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Get configuration
catalog = spark.conf.get("catalog")
source_path = spark.conf.get("source_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables

# COMMAND ----------

# TODO: Add your bronze tables here
# Example template:

@dlt.table(
    name="bronze_{{entity_name}}",
    comment="Raw {{entity_name}} data from source",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_entity():
    """Ingest raw data using Auto Loader"""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")  # or csv, parquet, etc.
        .option("cloudFiles.schemaLocation", f"{source_path}/schemas/{{entity_name}}")
        .load(f"{source_path}/{{entity_name}}")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

# Add more bronze tables as needed
# Copy and modify the template above for each source table
