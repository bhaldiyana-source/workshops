# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business-Level Aggregates
# MAGIC
# MAGIC Creates business-ready aggregated tables

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Tables

# COMMAND ----------

# TODO: Add your gold tables here
# Example template:

@dlt.table(
    name="gold_{{metric_name}}_summary",
    comment="{{metric_name}} summary for analytics"
)
def gold_metric_summary():
    """Create aggregated summary"""
    return (
        dlt.read("silver_{{entity_name}}")
        .groupBy("status", F.date_trunc("day", "timestamp").alias("date"))
        .agg(
            F.count("*").alias("total_count"),
            F.countDistinct("id").alias("unique_count"),
            # Add your aggregations here
        )
        .withColumn("_created_at", F.current_timestamp())
    )

# COMMAND ----------

# Add more gold tables as needed
