# Databricks notebook source
# MAGIC %md
# MAGIC # Module 5: Multi-Environment Deployment
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Configure multiple deployment environments
# MAGIC - Manage environment-specific settings
# MAGIC - Handle secrets across environments
# MAGIC - Implement promotion strategies
# MAGIC
# MAGIC ## Environment Strategy

# COMMAND ----------

multi_env_config = '''bundle:
  name: my-app

variables:
  catalog:
    default: "dev_catalog"
  num_workers:
    default: 2

targets:
  dev:
    workspace:
      host: https://dev-workspace.cloud.databricks.com
    variables:
      catalog: "dev_catalog"
      num_workers: 1
  
  staging:
    workspace:
      host: https://staging-workspace.cloud.databricks.com
    variables:
      catalog: "staging_catalog"
      num_workers: 2
  
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    variables:
      catalog: "prod_catalog"
      num_workers: 4
    
    # Production-only settings
    run_as:
      service_principal_name: "prod-service-principal"

resources:
  jobs:
    my_job:
      name: "My Job - ${bundle.target}"
      tasks:
        - notebook_task:
            notebook_path: ./src/main.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            num_workers: ${var.num_workers}
'''

print(multi_env_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - Targets define environment-specific configs
# MAGIC - Variables customize per environment
# MAGIC - Service principals for production
# MAGIC - Secrets managed separately
