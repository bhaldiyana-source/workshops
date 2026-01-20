# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3 Lab: Complex Resource Management
# MAGIC
# MAGIC ## Objectives
# MAGIC - Create schemas, volumes, DLT pipeline, and jobs
# MAGIC - Implement medallion architecture
# MAGIC - Deploy and validate
# MAGIC
# MAGIC ## Tasks
# MAGIC 1. Create Unity Catalog resources
# MAGIC 2. Build DLT pipeline (bronze → silver → gold)
# MAGIC 3. Create analytics job
# MAGIC 4. Deploy and test
# MAGIC
# MAGIC ## Estimated Time
# MAGIC 60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create Bundle Structure
# MAGIC
# MAGIC Create a bundle for an e-commerce analytics platform with:
# MAGIC - Product catalog data
# MAGIC - Order transactions
# MAGIC - Customer analytics
# MAGIC
# MAGIC ### Steps
# MAGIC 1. Create bundle directory
# MAGIC 2. Create databricks.yml with all resources
# MAGIC 3. Create DLT notebooks for each layer
# MAGIC 4. Create analytics job
# MAGIC 5. Deploy and test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provided: Bundle Configuration Template

# COMMAND ----------

template = '''bundle:
  name: ecommerce-analytics

variables:
  catalog:
    default: "dev_catalog"

targets:
  dev:
    variables:
      catalog: "dev_catalog"

resources:
  schemas:
    bronze:
      name: "${var.catalog}.${bundle.target}_bronze"
    silver:
      name: "${var.catalog}.${bundle.target}_silver"
    gold:
      name: "${var.catalog}.${bundle.target}_gold"
  
  volumes:
    raw_data:
      name: "${var.catalog}.${bundle.target}_bronze.raw_data"
      volume_type: "MANAGED"
  
  pipelines:
    ecommerce_pipeline:
      name: "E-Commerce Pipeline - ${bundle.target}"
      # TODO: Complete pipeline configuration
  
  jobs:
    analytics:
      name: "E-Commerce Analytics - ${bundle.target}"
      # TODO: Complete job configuration
'''

print("Complete this template in your databricks.yml")
print(template)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Create DLT Notebooks
# MAGIC
# MAGIC Create bronze, silver, and gold layer notebooks
# MAGIC
# MAGIC ### Success Criteria
# MAGIC - [ ] Bronze layer ingests raw data
# MAGIC - [ ] Silver layer cleanses and joins
# MAGIC - [ ] Gold layer creates aggregates
# MAGIC - [ ] Data quality checks implemented
# MAGIC - [ ] Pipeline runs successfully

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Validate and Deploy
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: Add Quality Monitoring
# MAGIC
# MAGIC Add a quality monitor to track data quality over time
