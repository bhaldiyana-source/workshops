# Databricks notebook source
# MAGIC %md
# MAGIC # {{job_display_name}}
# MAGIC
# MAGIC Description: {{job_description}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Define parameters
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("environment", "dev", "Environment")

# Get parameter values
catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")

print(f"Environment: {environment}")
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Logic

# COMMAND ----------

# TODO: Add your logic here

from pyspark.sql import functions as F

# Example: Create sample data
data = [
    ("example1", 100),
    ("example2", 200),
    ("example3", 300),
]

df = spark.createDataFrame(data, ["name", "value"])
df = df.withColumn("processing_date", F.current_date())
df = df.withColumn("environment", F.lit(environment))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# TODO: Save results to target table

# Example:
# output_table = f"{catalog}.schema.output_table"
# df.write.mode("append").saveAsTable(output_table)
# print(f"Results saved to {output_table}")

print("✅ Job completed successfully!")
