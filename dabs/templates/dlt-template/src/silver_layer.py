# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Cleaned and Validated Data
# MAGIC
# MAGIC Applies data quality rules and transformations

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables

# COMMAND ----------

# TODO: Add your silver tables here
# Example template:

@dlt.table(
    name="silver_{{entity_name}}",
    comment="Cleaned and validated {{entity_name}} data"
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect("valid_status", "status IN ('active', 'inactive', 'pending')")
def silver_entity():
    """Clean and validate bronze data"""
    return (
        dlt.read_stream("bronze_{{entity_name}}")
        .select(
            "id",
            "name",
            "timestamp",
            "status",
            # Add your columns here
        )
        .withColumn("_processing_timestamp", F.current_timestamp())
        .dropDuplicates(["id"])  # Remove duplicates
    )

# COMMAND ----------

# Add more silver tables as needed
