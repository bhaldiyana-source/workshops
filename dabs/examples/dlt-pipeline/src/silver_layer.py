# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(name="silver_sales")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_sales():
    return dlt.read("bronze_sales")
