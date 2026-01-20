# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3 Demo: DLT Pipeline with Unity Catalog
# MAGIC
# MAGIC ## Overview
# MAGIC Build a complete medallion architecture using:
# MAGIC - Delta Live Tables for data pipelines
# MAGIC - Unity Catalog for governance
# MAGIC - DABs for deployment
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Raw Data (Volume) → Bronze (DLT) → Silver (DLT) → Gold (DLT) → Analytics (Job)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Bundle Structure
# MAGIC
# MAGIC ```bash
# MAGIC mkdir customer-pipeline-bundle
# MAGIC cd customer-pipeline-bundle
# MAGIC mkdir -p src/pipelines src/jobs
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define databricks.yml

# COMMAND ----------

bundle_config = '''bundle:
  name: customer-pipeline

variables:
  catalog:
    description: "Unity Catalog"
    default: "dev_catalog"
  
  source_path:
    description: "Source data path"
    default: "/Volumes/dev_catalog/bronze/raw_data"

targets:
  dev:
    variables:
      catalog: "dev_catalog"
      source_path: "/Volumes/dev_catalog/bronze/raw_data"
  
  prod:
    variables:
      catalog: "prod_catalog"
      source_path: "/Volumes/prod_catalog/bronze/raw_data"

resources:
  # Schemas
  schemas:
    bronze:
      name: "${var.catalog}.${bundle.target}_bronze"
      comment: "Bronze layer - raw data"
    
    silver:
      name: "${var.catalog}.${bundle.target}_silver"
      comment: "Silver layer - cleaned"
    
    gold:
      name: "${var.catalog}.${bundle.target}_gold"
      comment: "Gold layer - aggregated"
  
  # Volume for raw data
  volumes:
    raw_data:
      name: "${var.catalog}.${bundle.target}_bronze.raw_data"
      volume_type: "MANAGED"
  
  # DLT Pipeline
  pipelines:
    customer_pipeline:
      name: "Customer Pipeline - ${bundle.target}"
      target: "${var.catalog}.${bundle.target}_silver"
      continuous: false
      development: ${var.is_dev}
      
      clusters:
        - label: "default"
          num_workers: 2
          
      libraries:
        - notebook:
            path: ./src/pipelines/bronze.py
        - notebook:
            path: ./src/pipelines/silver.py
        - notebook:
            path: ./src/pipelines/gold.py
      
      configuration:
        catalog: ${var.catalog}
        environment: ${bundle.target}
  
  # Analytics Job
  jobs:
    customer_analytics:
      name: "Customer Analytics - ${bundle.target}"
      
      tasks:
        - task_key: "generate_report"
          notebook_task:
            notebook_path: ./src/jobs/analytics.py
          new_cluster:
            num_workers: 2
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
'''

print(bundle_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create DLT Bronze Layer

# COMMAND ----------

bronze_notebook = '''# Databricks notebook source
import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data"
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"/Volumes/{catalog}/bronze/raw_data/customers")
        .withColumn("_ingested_at", F.current_timestamp())
    )

@dlt.table(name="bronze_orders")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"/Volumes/{catalog}/bronze/raw_data/orders")
        .withColumn("_ingested_at", F.current_timestamp())
    )
'''

print("Create as: src/pipelines/bronze.py")
print(bronze_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deploy and Run
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy -t dev
# MAGIC databricks bundle run customer_pipeline -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Created a complete DLT pipeline with:
# MAGIC - Unity Catalog schemas and volumes
# MAGIC - Bronze, silver, gold layers
# MAGIC - Analytics job
# MAGIC - Deployed via DABs
