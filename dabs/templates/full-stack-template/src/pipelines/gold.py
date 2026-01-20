# Databricks notebook source
import dlt
from pyspark.sql import functions as F

@dlt.table(name="gold_summary")
def gold_summary():
    """Gold layer aggregation"""
    return dlt.read("silver_data").groupBy("name").count()
