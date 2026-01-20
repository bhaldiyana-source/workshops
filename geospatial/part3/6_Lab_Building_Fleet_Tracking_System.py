# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building Fleet Tracking System
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll build a complete real-time fleet tracking system using the concepts from previous demos. You'll implement vehicle tracking, route adherence monitoring, ETA calculations, and create live dashboards for fleet management.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement real-time vehicle position tracking
# MAGIC - Monitor route adherence and deviations
# MAGIC - Calculate dynamic ETAs with traffic consideration
# MAGIC - Build live dashboard data pipelines
# MAGIC - Create alert systems for route deviations
# MAGIC - Optimize fleet operations with geospatial analytics
# MAGIC
# MAGIC ## Lab Structure
# MAGIC This lab is divided into exercises with solution code provided.
# MAGIC Try to implement each exercise before looking at the solution!
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install faker h3 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import and Configure
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random

catalog = "main"
schema_name = "fleet_tracking_lab"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Lab environment ready: {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Generate Fleet Data
# MAGIC
# MAGIC **Task**: Create synthetic data for a delivery fleet with:
# MAGIC - 10 vehicles
# MAGIC - Planned routes
# MAGIC - Real-time GPS positions
# MAGIC - Delivery stops

# COMMAND ----------

# DBTITLE 1,Solution: Generate Fleet Data
from faker import Faker
fake = Faker()

def generate_routes(num_routes=10):
    """Generate planned delivery routes"""
    routes = []
    lat_min, lat_max = 37.70, 37.82
    lon_min, lon_max = -122.50, -122.37
    
    for i in range(num_routes):
        # Generate 5-8 stops per route
        num_stops = random.randint(5, 8)
        stops = []
        
        for j in range(num_stops):
            stops.append({
                "stop_sequence": j + 1,
                "latitude": round(random.uniform(lat_min, lat_max), 6),
                "longitude": round(random.uniform(lon_min, lon_max), 6),
                "planned_arrival": datetime.now() + timedelta(minutes=j*20),
                "planned_duration_minutes": random.randint(5, 15),
                "stop_type": random.choice(["pickup", "delivery", "delivery", "delivery"])
            })
        
        routes.append({
            "route_id": f"ROUTE_{i+1:03d}",
            "vehicle_id": f"VEH_{i+1:03d}",
            "driver_id": f"DRV_{i+1:03d}",
            "route_date": datetime.now().date(),
            "planned_start": datetime.now(),
            "planned_end": datetime.now() + timedelta(hours=3),
            "stops": stops,
            "total_stops": len(stops)
        })
    
    return routes

def generate_vehicle_positions(routes, points_per_route=50):
    """Generate GPS positions along routes"""
    positions = []
    
    for route in routes:
        stops = route["stops"]
        current_time = route["planned_start"]
        
        # Generate positions moving through stops
        for i in range(len(stops) - 1):
            start_stop = stops[i]
            end_stop = stops[i + 1]
            
            # Interpolate positions between stops
            for j in range(points_per_route // len(stops)):
                ratio = j / (points_per_route // len(stops))
                
                # Add some randomness to simulate real movement
                noise_lat = random.uniform(-0.001, 0.001)
                noise_lon = random.uniform(-0.001, 0.001)
                
                lat = start_stop["latitude"] + (end_stop["latitude"] - start_stop["latitude"]) * ratio + noise_lat
                lon = start_stop["longitude"] + (end_stop["longitude"] - start_stop["longitude"]) * ratio + noise_lon
                
                positions.append({
                    "vehicle_id": route["vehicle_id"],
                    "route_id": route["route_id"],
                    "timestamp": current_time,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "speed": round(random.uniform(5, 25), 2),  # m/s
                    "heading": round(random.uniform(0, 360), 1),
                    "current_stop_sequence": i + 1
                })
                
                current_time += timedelta(seconds=30)
    
    return positions

# Generate data
routes = generate_routes(10)
vehicle_positions = generate_vehicle_positions(routes, 50)

print(f"Generated {len(routes)} routes")
print(f"Generated {len(vehicle_positions)} GPS positions")

# COMMAND ----------

# DBTITLE 1,Write Routes and Positions to Delta
# Save routes
routes_data = []
for route in routes:
    route_copy = route.copy()
    route_copy.pop("stops")  # Remove stops for main table
    routes_data.append(route_copy)

routes_df = spark.createDataFrame(routes_data)
routes_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.routes")

# Save route stops
stops_data = []
for route in routes:
    for stop in route["stops"]:
        stop_record = stop.copy()
        stop_record["route_id"] = route["route_id"]
        stop_record["vehicle_id"] = route["vehicle_id"]
        stops_data.append(stop_record)

stops_df = spark.createDataFrame(stops_data)
stops_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.route_stops")

# Write positions to landing zone for streaming
positions_landing = f"{data_base}/landing/vehicle_positions"
dbutils.fs.rm(positions_landing, recurse=True)

positions_schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("route_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("heading", DoubleType()),
    StructField("current_stop_sequence", IntegerType())
])

batch_size = 50
for i in range(0, len(vehicle_positions), batch_size):
    batch = vehicle_positions[i:i+batch_size]
    batch_df = spark.createDataFrame(batch, schema=positions_schema)
    batch_df.write.mode("overwrite").json(f"{positions_landing}/batch_{i//batch_size:03d}")

print(f"\nRoutes: {routes_df.count()}")
print(f"Stops: {stops_df.count()}")
print("Vehicle positions written to landing zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Real-Time Vehicle Tracking
# MAGIC
# MAGIC **Task**: Create a streaming pipeline to:
# MAGIC 1. Ingest vehicle GPS positions
# MAGIC 2. Enrich with route information
# MAGIC 3. Track current location of each vehicle

# COMMAND ----------

# DBTITLE 1,Solution: Vehicle Tracking Stream
# Create vehicle position stream
vehicle_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(positions_schema)
    .load(positions_landing)
    .withColumn("h3_index", F.expr("h3_latlngtocell(latitude, longitude, 9)"))
    .withColumn("point_wkt", F.expr("ST_AsText(ST_Point(longitude, latitude))"))
)

# Enrich with route info
routes_static = spark.table(f"{catalog}.{schema_name}.routes")

vehicle_tracking = (
    vehicle_stream
    .join(
        F.broadcast(routes_static.select("route_id", "vehicle_id", "driver_id", "planned_start", "planned_end")),
        ["route_id", "vehicle_id"],
        "left"
    )
    .withColumn("processing_time", F.current_timestamp())
)

# Write to tracking table
tracking_table = f"{catalog}.{schema_name}.vehicle_tracking"
tracking_checkpoint = f"{checkpoint_base}/vehicle_tracking"

query_tracking = (
    vehicle_tracking
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", tracking_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(tracking_table)
)

print(f"Started vehicle tracking: {query_tracking.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Route Adherence Monitoring
# MAGIC
# MAGIC **Task**: Monitor if vehicles are following planned routes by:
# MAGIC 1. Comparing current position to planned stops
# MAGIC 2. Calculating deviation distance
# MAGIC 3. Detecting when vehicles go off-route

# COMMAND ----------

# DBTITLE 1,Solution: Route Adherence Monitoring
# Join vehicle positions with planned stops
stops_static = spark.table(f"{catalog}.{schema_name}.route_stops")

route_adherence = (
    vehicle_tracking
    .join(
        F.broadcast(stops_static.select(
            "route_id",
            "stop_sequence",
            F.col("latitude").alias("stop_latitude"),
            F.col("longitude").alias("stop_longitude"),
            "planned_arrival",
            "stop_type"
        )),
        [
            vehicle_tracking.route_id == stops_static.route_id,
            vehicle_tracking.current_stop_sequence == stops_static.stop_sequence
        ],
        "inner"
    )
    .withColumn(
        "distance_to_stop_meters",
        F.expr("""
            ST_Distance(
                ST_Point(longitude, latitude),
                ST_Point(stop_longitude, stop_latitude)
            )
        """)
    )
    .withColumn(
        "time_diff_minutes",
        (F.unix_timestamp("timestamp") - F.unix_timestamp("planned_arrival")) / 60
    )
    .withColumn(
        "status",
        F.when(F.col("distance_to_stop_meters") <= 100, "AT_STOP")
         .when(F.col("distance_to_stop_meters") <= 500, "APPROACHING")
         .when(F.col("distance_to_stop_meters") <= 2000, "ON_ROUTE")
         .otherwise("OFF_ROUTE")
    )
    .withColumn(
        "on_time_status",
        F.when(F.abs(F.col("time_diff_minutes")) <= 5, "ON_TIME")
         .when(F.col("time_diff_minutes") > 5, "LATE")
         .otherwise("EARLY")
    )
    .select(
        "vehicle_id",
        "route_id",
        "driver_id",
        "timestamp",
        "latitude",
        "longitude",
        "current_stop_sequence",
        "stop_type",
        F.round("distance_to_stop_meters", 1).alias("distance_to_stop_m"),
        F.round("time_diff_minutes", 1).alias("time_diff_min"),
        "status",
        "on_time_status",
        "speed"
    )
)

# Write adherence data
adherence_table = f"{catalog}.{schema_name}.route_adherence"
adherence_checkpoint = f"{checkpoint_base}/route_adherence"

query_adherence = (
    route_adherence
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", adherence_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(adherence_table)
)

print(f"Started route adherence monitoring: {query_adherence.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: ETA Calculations
# MAGIC
# MAGIC **Task**: Calculate estimated time of arrival (ETA) for each stop based on:
# MAGIC 1. Current vehicle position
# MAGIC 2. Distance to stop
# MAGIC 3. Current speed and traffic conditions

# COMMAND ----------

# DBTITLE 1,Solution: Dynamic ETA Calculation
# Calculate ETA for next stops
eta_calculation = (
    route_adherence
    .filter("status IN ('ON_ROUTE', 'APPROACHING')")
    .withColumn(
        "estimated_travel_time_minutes",
        F.when(
            F.col("speed") > 1,
            (F.col("distance_to_stop_m") / F.col("speed")) / 60
        ).otherwise(
            F.col("distance_to_stop_m") / 5 / 60  # Assume 5 m/s if stopped
        )
    )
    .withColumn(
        "traffic_factor",
        F.lit(1.2)  # 20% delay for traffic (would be dynamic in production)
    )
    .withColumn(
        "adjusted_eta_minutes",
        F.col("estimated_travel_time_minutes") * F.col("traffic_factor")
    )
    .withColumn(
        "estimated_arrival",
        F.expr("timestamp + INTERVAL adjusted_eta_minutes MINUTES")
    )
    .withColumn(
        "eta_accuracy",
        F.when(F.col("distance_to_stop_m") < 500, "HIGH")
         .when(F.col("distance_to_stop_m") < 2000, "MEDIUM")
         .otherwise("LOW")
    )
    .select(
        "vehicle_id",
        "route_id",
        "timestamp",
        "current_stop_sequence",
        "stop_type",
        F.round("distance_to_stop_m", 0).alias("distance_to_stop_m"),
        F.round("adjusted_eta_minutes", 1).alias("eta_minutes"),
        "estimated_arrival",
        "eta_accuracy",
        "on_time_status"
    )
)

# Write ETA data
eta_table = f"{catalog}.{schema_name}.vehicle_eta"
eta_checkpoint = f"{checkpoint_base}/vehicle_eta"

query_eta = (
    eta_calculation
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", eta_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(eta_table)
)

print(f"Started ETA calculation: {query_eta.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Alert System
# MAGIC
# MAGIC **Task**: Create alerts for:
# MAGIC 1. Vehicles going off-route
# MAGIC 2. Significant delays
# MAGIC 3. Stopped vehicles

# COMMAND ----------

# DBTITLE 1,Solution: Fleet Alert System
# Define alert conditions
alerts = (
    route_adherence
    .filter(
        (F.col("status") == "OFF_ROUTE") |
        (F.col("on_time_status") == "LATE") & (F.abs(F.col("time_diff_min")) > 15) |
        (F.col("speed") < 0.5) & (F.col("status") != "AT_STOP")
    )
    .withColumn(
        "alert_type",
        F.when(F.col("status") == "OFF_ROUTE", "OFF_ROUTE")
         .when((F.col("on_time_status") == "LATE") & (F.abs(F.col("time_diff_min")) > 15), "DELAYED")
         .when((F.col("speed") < 0.5) & (F.col("status") != "AT_STOP"), "STOPPED")
         .otherwise("UNKNOWN")
    )
    .withColumn(
        "alert_severity",
        F.when(F.col("distance_to_stop_m") > 5000, "CRITICAL")
         .when(F.abs(F.col("time_diff_min")) > 30, "CRITICAL")
         .when(F.col("alert_type") == "OFF_ROUTE", "HIGH")
         .when(F.col("alert_type") == "DELAYED", "MEDIUM")
         .otherwise("LOW")
    )
    .withColumn("alert_id", F.expr("uuid()"))
    .withColumn("alert_timestamp", F.current_timestamp())
    .withColumn(
        "alert_message",
        F.concat(
            F.lit("Vehicle "), F.col("vehicle_id"),
            F.lit(" "), F.lower(F.col("alert_type")),
            F.lit(" on route "), F.col("route_id"),
            F.lit(" at stop "), F.col("current_stop_sequence")
        )
    )
    .select(
        "alert_id",
        "vehicle_id",
        "route_id",
        "driver_id",
        "timestamp",
        "latitude",
        "longitude",
        "alert_type",
        "alert_severity",
        "alert_message",
        "distance_to_stop_m",
        "time_diff_min",
        "status",
        "alert_timestamp"
    )
)

# Write alerts
alerts_table = f"{catalog}.{schema_name}.fleet_alerts"
alerts_checkpoint = f"{checkpoint_base}/fleet_alerts"

query_alerts = (
    alerts
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", alerts_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(alerts_table)
)

print(f"Started fleet alert system: {query_alerts.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Live Dashboard Data
# MAGIC
# MAGIC **Task**: Create aggregated tables for dashboard visualization

# COMMAND ----------

# DBTITLE 1,Solution: Dashboard Data Pipeline
# Fleet summary statistics
fleet_summary = (
    vehicle_tracking
    .withWatermark("timestamp", "5 minutes")
    .groupBy(F.window("timestamp", "1 minute"))
    .agg(
        F.countDistinct("vehicle_id").alias("active_vehicles"),
        F.countDistinct("route_id").alias("active_routes"),
        F.avg("speed").alias("avg_speed_mps"),
        F.count("*").alias("total_positions")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "active_vehicles",
        "active_routes",
        F.round("avg_speed_mps", 2).alias("avg_speed_mps"),
        "total_positions"
    )
)

summary_table = f"{catalog}.{schema_name}.fleet_summary_dashboard"
summary_checkpoint = f"{checkpoint_base}/fleet_summary"

query_summary = (
    fleet_summary
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", summary_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(summary_table)
)

print(f"Started fleet summary dashboard: {query_summary.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Results and Analysis

# COMMAND ----------

# DBTITLE 1,Wait for Processing
import time
print("Processing fleet data... waiting 25 seconds")
time.sleep(25)

# COMMAND ----------

# DBTITLE 1,Fleet Status Overview
tracking = spark.table(tracking_table)
print(f"=== Fleet Status ===")
print(f"Total position updates: {tracking.count()}")

print("\nVehicles tracked:")
tracking.groupBy("vehicle_id", "route_id").agg(
    F.count("*").alias("positions"),
    F.min("timestamp").alias("first_seen"),
    F.max("timestamp").alias("last_seen"),
    F.avg("speed").alias("avg_speed")
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Route Adherence Analysis
adherence = spark.table(adherence_table)
print(f"\n=== Route Adherence ===")
print(f"Total adherence records: {adherence.count()}")

print("\nStatus distribution:")
adherence.groupBy("status", "on_time_status").count().orderBy("status").show()

print("\nVehicles off-route or late:")
adherence.filter("status = 'OFF_ROUTE' OR on_time_status = 'LATE'") \
    .select("vehicle_id", "route_id", "timestamp", "status", "on_time_status", "distance_to_stop_m", "time_diff_min") \
    .orderBy(F.desc("timestamp")) \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,ETA Analysis
eta_data = spark.table(eta_table)
print(f"\n=== ETA Analysis ===")
print(f"Total ETA calculations: {eta_data.count()}")

print("\nAverage ETA by vehicle:")
eta_data.groupBy("vehicle_id", "route_id").agg(
    F.avg("eta_minutes").alias("avg_eta_minutes"),
    F.avg("distance_to_stop_m").alias("avg_distance_m"),
    F.count("*").alias("calculations")
).orderBy("vehicle_id").show(truncate=False)

print("\nUpcoming arrivals:")
eta_data.orderBy("estimated_arrival") \
    .select("vehicle_id", "current_stop_sequence", "stop_type", "eta_minutes", "estimated_arrival", "eta_accuracy") \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Alerts Summary
alerts_data = spark.table(alerts_table)
print(f"\n=== Fleet Alerts ===")
print(f"Total alerts generated: {alerts_data.count()}")

print("\nAlerts by type and severity:")
alerts_data.groupBy("alert_type", "alert_severity").count().orderBy("alert_severity", "alert_type").show()

print("\nCritical alerts:")
alerts_data.filter("alert_severity = 'CRITICAL'") \
    .orderBy(F.desc("timestamp")) \
    .select("vehicle_id", "alert_type", "alert_message", "timestamp") \
    .limit(10) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Dashboard Summary
dashboard = spark.table(summary_table)
print(f"\n=== Dashboard Summary ===")
print(f"Summary records: {dashboard.count()}")

if dashboard.count() > 0:
    print("\nFleet activity over time:")
    dashboard.orderBy(F.desc("window_start")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# DBTITLE 1,Stop All Streams (Optional)
# Uncomment to stop all streams
# for stream in spark.streams.active:
#     print(f"Stopping {stream.name}...")
#     stream.stop()
# print("All streams stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Built
# MAGIC
# MAGIC A complete real-time fleet tracking system with:
# MAGIC
# MAGIC 1. **Vehicle Tracking**
# MAGIC    - Real-time GPS position ingestion
# MAGIC    - Route association
# MAGIC    - Spatial indexing
# MAGIC
# MAGIC 2. **Route Adherence**
# MAGIC    - Distance to planned stops
# MAGIC    - On-route/off-route detection
# MAGIC    - Schedule adherence monitoring
# MAGIC
# MAGIC 3. **ETA Calculations**
# MAGIC    - Dynamic arrival time estimates
# MAGIC    - Traffic factor adjustments
# MAGIC    - Accuracy classification
# MAGIC
# MAGIC 4. **Alert System**
# MAGIC    - Off-route alerts
# MAGIC    - Delay notifications
# MAGIC    - Stopped vehicle detection
# MAGIC    - Severity classification
# MAGIC
# MAGIC 5. **Dashboard Data**
# MAGIC    - Real-time fleet summary
# MAGIC    - Performance metrics
# MAGIC    - Visual analytics support
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC ✅ Structured Streaming for real-time tracking  
# MAGIC ✅ Broadcast joins for route data  
# MAGIC ✅ Dynamic calculations with UDFs  
# MAGIC ✅ Alert generation patterns  
# MAGIC ✅ Dashboard data preparation  
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Enhance with predictive ETA using ML models
# MAGIC - Add driver behavior analytics
# MAGIC - Integrate with optimization algorithms
# MAGIC - Build interactive dashboards with BI tools
# MAGIC
# MAGIC **Congratulations!** You've built a production-ready fleet tracking system!
