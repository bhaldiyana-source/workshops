# Databricks notebook source
import dlt
from pyspark.sql import functions as F

@dlt.table(name="silver_data")
def silver_data():
    """Silver layer transformation"""
    return dlt.read("bronze_data")
