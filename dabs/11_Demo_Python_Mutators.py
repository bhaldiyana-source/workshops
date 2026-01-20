# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4 Demo: Python Mutators
# MAGIC
# MAGIC ## Overview
# MAGIC Demonstrates creating dynamic bundles using Python

# COMMAND ----------

# Example mutator
mutator_example = '''# mutator.py
def apply_mutator(config):
    """Add custom tags to all jobs"""
    for job_name, job_config in config.get("resources", {}).get("jobs", {}).items():
        if "tags" not in job_config:
            job_config["tags"] = {}
        job_config["tags"]["managed_by"] = "dabs"
        job_config["tags"]["created_at"] = datetime.now().isoformat()
    return config
'''

print(mutator_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - Mutators transform bundle configurations
# MAGIC - Python enables complex logic
# MAGIC - Reusable across bundles
