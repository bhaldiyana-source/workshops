# Databricks notebook source
import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

# COMMAND ----------

@dlt.table(
    name="bronze_sales",
    comment="Raw sales data"
)
def bronze_sales():
    return spark.createDataFrame([
        (1, "2026-01-01", "Product A", 100),
        (2, "2026-01-01", "Product B", 200),
        (3, "2026-01-02", "Product A", 150),
    ], ["id", "date", "product", "amount"])
