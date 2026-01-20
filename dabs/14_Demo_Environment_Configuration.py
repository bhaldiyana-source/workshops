# Databricks notebook source
# MAGIC %md
# MAGIC # Module 5 Demo: Environment Configuration
# MAGIC
# MAGIC ## Overview
# MAGIC Set up dev, staging, and production environments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commands
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy to dev
# MAGIC databricks bundle deploy -t dev
# MAGIC
# MAGIC # Deploy to staging
# MAGIC databricks bundle deploy -t staging
# MAGIC
# MAGIC # Deploy to prod
# MAGIC databricks bundle deploy -t prod
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - Single source, multiple environments
# MAGIC - Environment-specific configuration
# MAGIC - Safe promotion path
