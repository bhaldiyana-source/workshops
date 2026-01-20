# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(name="gold_sales_summary")
def gold_sales_summary():
    return (
        dlt.read("silver_sales")
        .groupBy("product")
        .agg(F.sum("amount").alias("total_amount"))
    )
