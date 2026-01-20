# Databricks notebook source
import dlt
from pyspark.sql import functions as F

@dlt.table(name="bronze_data")
def bronze_data():
    """Bronze layer ingestion"""
    # TODO: Add your ingestion logic
    return spark.createDataFrame([], schema="id INT, name STRING")
