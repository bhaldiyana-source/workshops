# Databricks notebook source
# MAGIC %md
# MAGIC # Module 8: Production Best Practices
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement monitoring and observability
# MAGIC - Handle errors and failures
# MAGIC - Secure production deployments
# MAGIC - Optimize performance
# MAGIC
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### 1. Security
# MAGIC - Use service principals for production
# MAGIC - Rotate secrets regularly
# MAGIC - Implement least-privilege access
# MAGIC - Audit deployments
# MAGIC
# MAGIC ### 2. Monitoring
# MAGIC - Log all deployments
# MAGIC - Track job failures
# MAGIC - Monitor resource usage
# MAGIC - Set up alerts
# MAGIC
# MAGIC ### 3. Performance
# MAGIC - Use autoscaling
# MAGIC - Optimize cluster sizes
# MAGIC - Cache frequently used data
# MAGIC - Monitor costs

# COMMAND ----------

production_config = '''bundle:
  name: production-app

targets:
  prod:
    # Use service principal
    run_as:
      service_principal_name: "prod-sp"
    
    # Workspace configuration
    workspace:
      host: https://prod.cloud.databricks.com
      root_path: /Production/.bundle/${bundle.name}
    
    # Production variables
    variables:
      catalog: "prod_catalog"
      num_workers: 4
      enable_autoscaling: true
    
    # Strict permissions
    permissions:
      - level: CAN_VIEW
        group_name: "prod-viewers"
      - level: CAN_MANAGE
        group_name: "prod-admins"

resources:
  jobs:
    prod_job:
      name: "Production Job"
      
      # Email alerts
      email_notifications:
        on_start: ["oncall@company.com"]
        on_success: ["ops@company.com"]
        on_failure: ["oncall@company.com", "critical@company.com"]
      
      # Retry policy
      max_retries: 3
      min_retry_interval_millis: 60000
      
      # Timeout
      timeout_seconds: 7200
      
      tasks:
        - notebook_task:
            notebook_path: ./src/production.py
          
          # Autoscaling cluster
          new_cluster:
            autoscale:
              min_workers: 2
              max_workers: 10
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
'''

print(production_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - Production requires extra care
# MAGIC - Security, monitoring, performance
# MAGIC - Automated recovery and alerts
# MAGIC - Cost optimization
