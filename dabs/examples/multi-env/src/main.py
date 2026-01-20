# Databricks notebook source
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("environment", "dev")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("environment")

print(f"Environment: {env}")
print(f"Catalog: {catalog}")
