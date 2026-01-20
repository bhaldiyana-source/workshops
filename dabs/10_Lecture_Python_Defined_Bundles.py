# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4: Python-Defined Bundles
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand Python support for DABs
# MAGIC - Create bundles programmatically
# MAGIC - Implement custom mutators
# MAGIC - Generate dynamic configurations
# MAGIC - Use Python for complex logic
# MAGIC
# MAGIC ## Why Python-Defined Bundles?
# MAGIC
# MAGIC ### YAML Limitations
# MAGIC - Static configurations
# MAGIC - Limited conditional logic
# MAGIC - Repetitive definitions
# MAGIC - No type checking
# MAGIC
# MAGIC ### Python Advantages
# MAGIC - Dynamic generation
# MAGIC - Complex logic
# MAGIC - Type safety
# MAGIC - Code reuse
# MAGIC - IDE support

# COMMAND ----------

# Example: Python-defined bundle
python_bundle = '''# bundle.py
from databricks.sdk.service.jobs import Task, NotebookTask, NewCluster

def get_bundle():
    """Generate bundle configuration dynamically"""
    
    # Dynamic task generation
    tasks = []
    for region in ["us-east", "us-west", "eu-central"]:
        tasks.append(Task(
            task_key=f"process_{region}",
            notebook_task=NotebookTask(
                notebook_path=f"./src/process_region.py",
                base_parameters={"region": region}
            ),
            new_cluster=NewCluster(
                num_workers=2 if region.startswith("us") else 4,
                spark_version="13.3.x-scala2.12",
                node_type_id="i3.xlarge"
            )
        ))
    
    return {
        "bundle": {"name": "regional-processing"},
        "resources": {
            "jobs": {
                "regional_job": {
                    "name": "Regional Processing",
                    "tasks": tasks
                }
            }
        }
    }
'''

print(python_bundle)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - Python enables dynamic bundle generation
# MAGIC - Use mutators for advanced transformations
# MAGIC - Type safety improves reliability
# MAGIC - Reusable components reduce duplication
