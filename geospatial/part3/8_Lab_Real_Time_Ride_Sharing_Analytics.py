# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Real-Time Ride Sharing Analytics
# MAGIC
# MAGIC ## Overview
# MAGIC Build a real-time ride sharing analytics platform with driver-rider matching, surge pricing, demand forecasting, and service area optimization.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement proximity-based driver-rider matching
# MAGIC - Calculate dynamic surge pricing zones
# MAGIC - Forecast demand by geographic region
# MAGIC - Optimize service areas
# MAGIC - Analyze wait time and match efficiency
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %pip install faker h3 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random

catalog = "main"
schema_name = "rideshare_lab"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Lab environment ready: {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Generate Ride Sharing Data

# COMMAND ----------

def generate_rideshare_data():
    """Generate driver and rider data"""
    lat_min, lat_max = 37.73, 37.82
    lon_min, lon_max = -122.48, -122.38
    
    drivers = []
    riders = []
    
    # Generate driver positions (moving)
    for i in range(15):
        driver_id = f"DRIVER_{i+1:03d}"
        base_time = datetime.now() - timedelta(minutes=20)
        lat = random.uniform(lat_min, lat_max)
        lon = random.uniform(lon_min, lon_max)
        
        for j in range(40):
            # Simulate movement
            lat += random.uniform(-0.001, 0.001)
            lon += random.uniform(-0.001, 0.001)
            lat = max(lat_min, min(lat_max, lat))
            lon = max(lon_min, min(lon_max, lon))
            
            drivers.append({
                "driver_id": driver_id,
                "timestamp": base_time + timedelta(seconds=j*30),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "available": random.choice([True, True, True, False]),
                "rating": round(random.uniform(4.2, 5.0), 2),
                "vehicle_type": random.choice(["standard", "premium", "xl"])
            })
    
    # Generate rider requests
    for i in range(25):
        rider_id = f"RIDER_{i+1:03d}"
        base_time = datetime.now() - timedelta(minutes=18)
        
        for j in range(20):
            pickup_lat = random.uniform(lat_min, lat_max)
            pickup_lon = random.uniform(lon_min, lon_max)
            dropoff_lat = random.uniform(lat_min, lat_max)
            dropoff_lon = random.uniform(lon_min, lon_max)
            
            riders.append({
                "rider_id": rider_id,
                "request_timestamp": base_time + timedelta(seconds=j*40),
                "pickup_latitude": round(pickup_lat, 6),
                "pickup_longitude": round(pickup_lon, 6),
                "dropoff_latitude": round(dropoff_lat, 6),
                "dropoff_longitude": round(dropoff_lon, 6),
                "passenger_count": random.randint(1, 4),
                "ride_type": random.choice(["standard", "premium"]),
                "max_wait_minutes": random.randint(5, 15)
            })
    
    return drivers, riders

drivers, riders = generate_rideshare_data()
print(f"Generated {len(drivers)} driver positions and {len(riders)} rider requests")

# Write to landing zones
drivers_landing = f"{data_base}/landing/drivers"
riders_landing = f"{data_base}/landing/riders"

dbutils.fs.rm(drivers_landing, recurse=True)
dbutils.fs.rm(riders_landing, recurse=True)

driver_schema = StructType([
    StructField("driver_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("available", BooleanType()),
    StructField("rating", DoubleType()),
    StructField("vehicle_type", StringType())
])

rider_schema = StructType([
    StructField("rider_id", StringType()),
    StructField("request_timestamp", TimestampType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("pickup_longitude", DoubleType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("dropoff_longitude", DoubleType()),
    StructField("passenger_count", IntegerType()),
    StructField("ride_type", StringType()),
    StructField("max_wait_minutes", IntegerType())
])

# Write batches
for i in range(0, len(drivers), 50):
    spark.createDataFrame(drivers[i:i+50], driver_schema).write.mode("overwrite").json(
        f"{drivers_landing}/batch_{i//50:03d}"
    )

for i in range(0, len(riders), 50):
    spark.createDataFrame(riders[i:i+50], rider_schema).write.mode("overwrite").json(
        f"{riders_landing}/batch_{i//50:03d}"
    )

print("Data written to landing zones")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Driver-Rider Matching

# COMMAND ----------

# Create streams
driver_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(driver_schema)
    .load(drivers_landing)
    .filter("available = true")
    .withColumn("h3_driver", F.expr("h3_latlngtocell(latitude, longitude, 9)"))
    .withWatermark("timestamp", "3 minutes")
)

rider_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(rider_schema)
    .load(riders_landing)
    .withColumn("h3_rider", F.expr("h3_latlngtocell(pickup_latitude, pickup_longitude, 9)"))
    .withWatermark("request_timestamp", "3 minutes")
)

# Join streams for matching
matches = (
    driver_stream
    .join(
        rider_stream,
        F.expr("""
            h3_driver = h3_rider AND
            timestamp BETWEEN request_timestamp - INTERVAL 2 MINUTES 
                         AND request_timestamp + INTERVAL 2 MINUTES
        """),
        "inner"
    )
    .withColumn(
        "distance_meters",
        F.expr("ST_Distance(ST_Point(longitude, latitude), ST_Point(pickup_longitude, pickup_latitude))")
    )
    .filter("distance_meters <= 2000")  # Within 2km
    .withColumn("eta_minutes", F.round(F.col("distance_meters") / 250 / 60, 1))
    .withColumn(
        "match_score",
        F.round((2000 - F.col("distance_meters")) / 2000 * 50 + F.col("rating") * 10, 2)
    )
)

# Rank matches
match_window = Window.partitionBy("rider_id", "request_timestamp").orderBy(
    "distance_meters", F.desc("rating")
)

ranked_matches = (
    matches
    .withColumn("match_rank", F.row_number().over(match_window))
    .filter("match_rank <= 3")
    .withColumn("match_id", F.expr("uuid()"))
    .select(
        "match_id",
        "driver_id",
        "rider_id",
        "timestamp",
        "request_timestamp",
        F.round("distance_meters", 0).alias("distance_meters"),
        "eta_minutes",
        "rating",
        "vehicle_type",
        "ride_type",
        "match_rank",
        "match_score"
    )
)

# Write matches
matches_table = f"{catalog}.{schema_name}.driver_rider_matches"
matches_checkpoint = f"{checkpoint_base}/matches"

query_matches = (
    ranked_matches
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", matches_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(matches_table)
)

print(f"Started matching: {query_matches.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Demand and Surge Pricing

# COMMAND ----------

# Calculate demand by region
demand_analysis = (
    rider_stream
    .groupBy(
        F.window("request_timestamp", "5 minutes"),
        "h3_rider",
        "ride_type"
    )
    .agg(
        F.count("*").alias("request_count"),
        F.avg("passenger_count").alias("avg_passengers")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("h3_rider").alias("h3_index"),
        "ride_type",
        "request_count",
        F.round("avg_passengers", 1).alias("avg_passengers")
    )
    .withColumn(
        "surge_multiplier",
        F.when(F.col("request_count") > 10, 2.0)
         .when(F.col("request_count") > 5, 1.5)
         .when(F.col("request_count") > 2, 1.2)
         .otherwise(1.0)
    )
    .withColumn(
        "demand_level",
        F.when(F.col("request_count") > 10, "VERY_HIGH")
         .when(F.col("request_count") > 5, "HIGH")
         .when(F.col("request_count") > 2, "MEDIUM")
         .otherwise("LOW")
    )
)

demand_table = f"{catalog}.{schema_name}.demand_surge_pricing"
demand_checkpoint = f"{checkpoint_base}/demand"

query_demand = (
    demand_analysis
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", demand_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(demand_table)
)

print(f"Started demand analysis: {query_demand.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Supply-Demand Balance

# COMMAND ----------

# Driver supply by region
supply_analysis = (
    driver_stream
    .filter("available = true")
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "h3_driver"
    )
    .agg(
        F.countDistinct("driver_id").alias("available_drivers")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("h3_driver").alias("h3_index"),
        "available_drivers"
    )
)

supply_table = f"{catalog}.{schema_name}.driver_supply"
supply_checkpoint = f"{checkpoint_base}/supply"

query_supply = (
    supply_analysis
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", supply_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(supply_table)
)

print(f"Started supply analysis: {query_supply.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Results

# COMMAND ----------

import time
print("Processing rideshare data... waiting 25 seconds")
time.sleep(25)

# COMMAND ----------

matches_df = spark.table(matches_table)
print(f"=== Driver-Rider Matches ===")
print(f"Total matches: {matches_df.count()}")

print("\nMatches by rank:")
matches_df.groupBy("match_rank").count().orderBy("match_rank").show()

print("\nTop matches:")
matches_df.orderBy(F.desc("match_score")).limit(10).show(truncate=False)

# COMMAND ----------

demand_df = spark.table(demand_table)
print(f"\n=== Demand & Surge Pricing ===")
print(f"Total demand records: {demand_df.count()}")

print("\nSurge pricing zones:")
demand_df.filter("surge_multiplier > 1.0") \
    .orderBy(F.desc("surge_multiplier")) \
    .show(truncate=False)

print("\nDemand by level:")
demand_df.groupBy("demand_level").agg(
    F.count("*").alias("zones"),
    F.avg("surge_multiplier").alias("avg_surge")
).show()

# COMMAND ----------

supply_df = spark.table(supply_table)
print(f"\n=== Driver Supply ===")
print(f"Total supply records: {supply_df.count()}")

print("\nSupply distribution:")
supply_df.groupBy("available_drivers").count().orderBy("available_drivers").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Complete!
# MAGIC
# MAGIC You've built a ride sharing analytics platform with:
# MAGIC - Real-time driver-rider matching
# MAGIC - Dynamic surge pricing
# MAGIC - Demand forecasting
# MAGIC - Supply-demand balance tracking
