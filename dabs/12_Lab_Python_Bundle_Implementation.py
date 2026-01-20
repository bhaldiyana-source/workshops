# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4 Lab: Python Bundle Implementation
# MAGIC
# MAGIC ## Objectives
# MAGIC - Create a Python-defined bundle
# MAGIC - Implement dynamic task generation
# MAGIC - Use mutators
# MAGIC
# MAGIC ## Tasks
# MAGIC 1. Create bundle.py
# MAGIC 2. Generate tasks dynamically
# MAGIC 3. Deploy and test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Template

# COMMAND ----------

task_template = '''# bundle.py
def create_bundle():
    """Create bundle with dynamic tasks"""
    
    # Generate tasks for multiple environments
    tasks = []
    for env in ["dev", "staging", "prod"]:
        tasks.append({
            "task_key": f"deploy_{env}",
            "notebook_task": {
                "notebook_path": "./src/deploy.py",
                "base_parameters": {"environment": env}
            }
        })
    
    return {
        "bundle": {"name": "dynamic-deployment"},
        "resources": {
            "jobs": {
                "deployment_job": {
                    "name": "Dynamic Deployment",
                    "tasks": tasks
                }
            }
        }
    }
'''

print(task_template)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Success Criteria
# MAGIC - [ ] Python bundle creates valid configuration
# MAGIC - [ ] Dynamic tasks generated correctly
# MAGIC - [ ] Bundle deploys successfully
