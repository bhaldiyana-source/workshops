# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Job Example
# MAGIC
# MAGIC This notebook demonstrates a basic DABs-managed job.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("environment", "dev", "Environment")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")

print(f"Running in environment: {environment}")
print(f"Using catalog: {catalog}")

# COMMAND ----------

# Create sample data
from pyspark.sql.functions import current_date, lit

data = [
    ("Product A", 100, 29.99),
    ("Product B", 50, 49.99),
    ("Product C", 75, 19.99),
]

df = spark.createDataFrame(data, ["product_name", "quantity", "price"])
df = df.withColumn("date", current_date())
df = df.withColumn("environment", lit(environment))

display(df)

# COMMAND ----------

# Calculate totals
from pyspark.sql.functions import sum, col

df_with_total = df.withColumn("total_value", col("quantity") * col("price"))

display(df_with_total)

print(f"Total revenue: ${df_with_total.agg(sum('total_value')).collect()[0][0]:.2f}")

# COMMAND ----------

print("✅ Job completed successfully!")
