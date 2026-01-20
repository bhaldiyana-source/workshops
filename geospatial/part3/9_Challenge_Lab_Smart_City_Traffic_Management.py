# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge Lab: Smart City Traffic Management
# MAGIC
# MAGIC ## Overview
# MAGIC This comprehensive challenge lab integrates all concepts from the workshop. Build a complete smart city traffic management system with real-time traffic flow analysis, congestion detection, dynamic routing, and multi-modal transportation integration.
# MAGIC
# MAGIC ## Challenge Objectives
# MAGIC - Implement real-time traffic flow analysis across city grid
# MAGIC - Detect and predict traffic congestion
# MAGIC - Generate dynamic routing recommendations
# MAGIC - Integrate multiple transportation modes (vehicles, public transit, bikes)
# MAGIC - Create predictive traffic patterns
# MAGIC - Build comprehensive monitoring dashboards
# MAGIC
# MAGIC ## Challenge Requirements
# MAGIC
# MAGIC This is an open-ended challenge. You should:
# MAGIC 1. Design the data model
# MAGIC 2. Implement streaming pipelines
# MAGIC 3. Build analytics and ML models
# MAGIC 4. Create visualization-ready data
# MAGIC
# MAGIC ## Duration
# MAGIC 90-120 minutes
# MAGIC
# MAGIC ## Hints and Starter Code Provided Below

# COMMAND ----------

# MAGIC %pip install faker h3 numpy --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random
import numpy as np

catalog = "main"
schema_name = "smart_city_challenge"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Challenge environment ready: {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Data Generation (Starter Code)
# MAGIC
# MAGIC Generate synthetic smart city data including:
# MAGIC - Vehicle traffic sensors
# MAGIC - Public transit vehicles (buses, trains)
# MAGIC - Bike sharing stations
# MAGIC - Traffic signals
# MAGIC - Incident reports

# COMMAND ----------

def generate_smart_city_data():
    """Generate comprehensive smart city transportation data"""
    
    # Define city grid (San Francisco-like)
    lat_min, lat_max = 37.70, 37.85
    lon_min, lon_max = -122.52, -122.35
    
    # 1. Traffic Sensors (fixed locations)
    sensors = []
    for i in range(50):
        sensors.append({
            "sensor_id": f"TRAFFIC_SENSOR_{i+1:03d}",
            "latitude": round(random.uniform(lat_min, lat_max), 6),
            "longitude": round(random.uniform(lon_min, lon_max), 6),
            "sensor_type": "traffic_flow",
            "road_type": random.choice(["highway", "arterial", "local"]),
            "lanes": random.randint(2, 6)
        })
    
    # 2. Traffic Flow Data
    traffic_flow = []
    base_time = datetime.now() - timedelta(hours=1)
    
    for sensor in sensors:
        for j in range(120):  # 2 hours of data
            ts = base_time + timedelta(seconds=j*30)
            hour = ts.hour
            
            # Traffic patterns by time of day
            if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hour
                base_volume = 80
            elif 22 <= hour or hour <= 5:  # Night
                base_volume = 15
            else:  # Normal hours
                base_volume = 40
            
            # Add road type factor
            if sensor["road_type"] == "highway":
                base_volume *= 2
            elif sensor["road_type"] == "local":
                base_volume *= 0.5
            
            volume = int(base_volume + np.random.normal(0, 10))
            avg_speed = max(5, 60 - (volume / sensor["lanes"]) + np.random.normal(0, 5))
            
            traffic_flow.append({
                "sensor_id": sensor["sensor_id"],
                "timestamp": ts,
                "vehicle_count": max(0, volume),
                "avg_speed_kmh": round(avg_speed, 1),
                "occupancy_percent": round(min(100, (volume / sensor["lanes"]) * 10), 1)
            })
    
    # 3. Public Transit Vehicles
    transit_vehicles = []
    for i in range(20):
        vehicle_id = f"BUS_{i+1:03d}"
        route_id = f"ROUTE_{random.randint(1, 10)}"
        lat = random.uniform(lat_min, lat_max)
        lon = random.uniform(lon_min, lon_max)
        
        for j in range(120):
            lat += random.uniform(-0.002, 0.002)
            lon += random.uniform(-0.002, 0.002)
            lat = max(lat_min, min(lat_max, lat))
            lon = max(lon_min, min(lon_max, lon))
            
            transit_vehicles.append({
                "vehicle_id": vehicle_id,
                "route_id": route_id,
                "timestamp": base_time + timedelta(seconds=j*30),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "speed_kmh": round(random.uniform(10, 40), 1),
                "passenger_count": random.randint(5, 50),
                "on_schedule": random.choice([True, True, True, False])
            })
    
    # 4. Bike Share Stations
    bike_stations = []
    for i in range(30):
        station_id = f"BIKE_STATION_{i+1:03d}"
        capacity = random.randint(10, 30)
        
        for j in range(120):
            available = random.randint(0, capacity)
            
            bike_stations.append({
                "station_id": station_id,
                "timestamp": base_time + timedelta(seconds=j*60),
                "latitude": round(random.uniform(lat_min, lat_max), 6),
                "longitude": round(random.uniform(lon_min, lon_max), 6),
                "bikes_available": available,
                "docks_available": capacity - available,
                "total_capacity": capacity
            })
    
    # 5. Traffic Incidents
    incidents = []
    for i in range(15):
        incident_time = base_time + timedelta(minutes=random.randint(0, 120))
        incidents.append({
            "incident_id": f"INCIDENT_{i+1:03d}",
            "timestamp": incident_time,
            "latitude": round(random.uniform(lat_min, lat_max), 6),
            "longitude": round(random.uniform(lon_min, lon_max), 6),
            "incident_type": random.choice(["accident", "construction", "weather", "event"]),
            "severity": random.choice(["minor", "moderate", "major"]),
            "estimated_duration_minutes": random.randint(15, 180),
            "lanes_affected": random.randint(1, 3)
        })
    
    return {
        "sensors": sensors,
        "traffic_flow": traffic_flow,
        "transit_vehicles": transit_vehicles,
        "bike_stations": bike_stations,
        "incidents": incidents
    }

# Generate all data
print("Generating smart city data...")
city_data = generate_smart_city_data()

for key, value in city_data.items():
    print(f"{key}: {len(value)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: YOUR CHALLENGE - Design and Implement
# MAGIC
# MAGIC ### Tasks:
# MAGIC
# MAGIC 1. **Data Ingestion**
# MAGIC    - Write data to landing zones
# MAGIC    - Create streaming readers
# MAGIC    - Implement schema validation
# MAGIC
# MAGIC 2. **Traffic Flow Analysis**
# MAGIC    - Calculate traffic density by region (use H3)
# MAGIC    - Identify congestion points
# MAGIC    - Track congestion trends over time
# MAGIC
# MAGIC 3. **Congestion Detection**
# MAGIC    - Define congestion thresholds
# MAGIC    - Detect real-time congestion
# MAGIC    - Generate congestion alerts
# MAGIC
# MAGIC 4. **Multi-Modal Integration**
# MAGIC    - Combine vehicle, transit, and bike data
# MAGIC    - Calculate transportation mode shares
# MAGIC    - Identify transit-rich vs car-dependent areas
# MAGIC
# MAGIC 5. **Dynamic Routing**
# MAGIC    - Identify fastest routes considering current traffic
# MAGIC    - Recommend alternative routes
# MAGIC    - Calculate time savings
# MAGIC
# MAGIC 6. **Predictive Analytics**
# MAGIC    - Build simple congestion prediction model
# MAGIC    - Forecast traffic patterns
# MAGIC    - Predict incident impacts
# MAGIC
# MAGIC 7. **Dashboard Data**
# MAGIC    - Real-time traffic heatmap
# MAGIC    - Congestion timeline
# MAGIC    - Mode share analysis
# MAGIC    - Incident impact visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Starter Code: Data Persistence

# COMMAND ----------

# Save sensor metadata
sensors_df = spark.createDataFrame(city_data["sensors"])
sensors_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.traffic_sensors")

# Write streaming data to landing zones
def write_stream_data(data, path, schema):
    """Helper to write streaming data"""
    dbutils.fs.rm(path, recurse=True)
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        spark.createDataFrame(batch, schema).write.mode("overwrite").json(
            f"{path}/batch_{i//batch_size:03d}"
        )

# Traffic flow
traffic_flow_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed_kmh", DoubleType()),
    StructField("occupancy_percent", DoubleType())
])
write_stream_data(city_data["traffic_flow"], f"{data_base}/landing/traffic_flow", traffic_flow_schema)

# Transit vehicles
transit_schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("route_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("passenger_count", IntegerType()),
    StructField("on_schedule", BooleanType())
])
write_stream_data(city_data["transit_vehicles"], f"{data_base}/landing/transit", transit_schema)

# Bike stations
bike_schema = StructType([
    StructField("station_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("bikes_available", IntegerType()),
    StructField("docks_available", IntegerType()),
    StructField("total_capacity", IntegerType())
])
write_stream_data(city_data["bike_stations"], f"{data_base}/landing/bike_stations", bike_schema)

# Incidents
incidents_df = spark.createDataFrame(city_data["incidents"])
incidents_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.traffic_incidents")

print("All data written successfully")
print("\nNow implement your solution!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE
# MAGIC
# MAGIC Use the code cells below to implement your smart city traffic management system.
# MAGIC
# MAGIC ### Suggested Approach:
# MAGIC 1. Create streaming DataFrames for each data source
# MAGIC 2. Enrich with spatial indexes (H3)
# MAGIC 3. Implement traffic density calculation
# MAGIC 4. Build congestion detection logic
# MAGIC 5. Create multi-modal analysis
# MAGIC 6. Generate routing recommendations
# MAGIC 7. Build predictive models
# MAGIC 8. Create dashboard-ready aggregations

# COMMAND ----------

# YOUR CODE HERE - Traffic Flow Stream
# Hint: Read from traffic_flow landing zone, add H3 index, join with sensor metadata

# COMMAND ----------

# YOUR CODE HERE - Congestion Detection
# Hint: Calculate congestion score based on speed and occupancy

# COMMAND ----------

# YOUR CODE HERE - Multi-Modal Integration
# Hint: Combine traffic, transit, and bike data by spatial region

# COMMAND ----------

# YOUR CODE HERE - Dynamic Routing
# Hint: Use graph algorithms or heuristics to find optimal routes

# COMMAND ----------

# YOUR CODE HERE - Predictive Analytics
# Hint: Use historical patterns to predict future congestion

# COMMAND ----------

# YOUR CODE HERE - Dashboard Data
# Hint: Create windowed aggregations for visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Solution (Reference Only)
# MAGIC
# MAGIC Below is one possible solution approach. Try to implement your own first!

# COMMAND ----------

# Sample: Traffic Flow Processing
traffic_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(traffic_flow_schema)
    .load(f"{data_base}/landing/traffic_flow")
)

sensors = spark.table(f"{catalog}.{schema_name}.traffic_sensors")

traffic_enriched = (
    traffic_stream
    .join(F.broadcast(sensors), "sensor_id")
    .withColumn("h3_index", F.expr("h3_latlngtocell(latitude, longitude, 9)"))
    .withColumn(
        "congestion_score",
        F.when(F.col("avg_speed_kmh") < 10, 100)
         .when(F.col("avg_speed_kmh") < 20, 80)
         .when(F.col("avg_speed_kmh") < 30, 60)
         .when(F.col("avg_speed_kmh") < 40, 40)
         .otherwise(20)
    )
    .withColumn(
        "congestion_level",
        F.when(F.col("congestion_score") >= 80, "SEVERE")
         .when(F.col("congestion_score") >= 60, "HEAVY")
         .when(F.col("congestion_score") >= 40, "MODERATE")
         .otherwise("LIGHT")
    )
)

# Write enriched traffic
traffic_table = f"{catalog}.{schema_name}.traffic_flow_enriched"
traffic_checkpoint = f"{checkpoint_base}/traffic_flow"

query_traffic = (
    traffic_enriched
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", traffic_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(traffic_table)
)

print(f"Started traffic flow processing: {query_traffic.name}")

# COMMAND ----------

# Sample: Traffic Density Heatmap
traffic_density = (
    traffic_enriched
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "h3_index",
        "road_type"
    )
    .agg(
        F.avg("congestion_score").alias("avg_congestion"),
        F.avg("avg_speed_kmh").alias("avg_speed"),
        F.sum("vehicle_count").alias("total_vehicles"),
        F.avg("occupancy_percent").alias("avg_occupancy")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "h3_index",
        F.expr("h3_celltolatlng(h3_index)[0]").alias("latitude"),
        F.expr("h3_celltolatlng(h3_index)[1]").alias("longitude"),
        "road_type",
        F.round("avg_congestion", 1).alias("congestion_score"),
        F.round("avg_speed", 1).alias("avg_speed_kmh"),
        "total_vehicles",
        F.round("avg_occupancy", 1).alias("occupancy_pct")
    )
)

density_table = f"{catalog}.{schema_name}.traffic_density_heatmap"
density_checkpoint = f"{checkpoint_base}/density"

query_density = (
    traffic_density
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", density_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(density_table)
)

print(f"Started density heatmap: {query_density.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation Criteria
# MAGIC
# MAGIC Your solution will be evaluated on:
# MAGIC
# MAGIC 1. **Completeness** (40%)
# MAGIC    - All data sources integrated
# MAGIC    - Multiple analysis types implemented
# MAGIC    - Dashboard data generated
# MAGIC
# MAGIC 2. **Technical Quality** (30%)
# MAGIC    - Proper streaming patterns used
# MAGIC    - Efficient spatial indexing
# MAGIC    - Appropriate watermarking
# MAGIC    - Error handling
# MAGIC
# MAGIC 3. **Insights Generated** (20%)
# MAGIC    - Meaningful congestion detection
# MAGIC    - Useful routing recommendations
# MAGIC    - Actionable predictions
# MAGIC
# MAGIC 4. **Code Quality** (10%)
# MAGIC    - Clean, readable code
# MAGIC    - Proper documentation
# MAGIC    - Efficient queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Complete!
# MAGIC
# MAGIC Congratulations on completing the Smart City Traffic Management Challenge!
# MAGIC
# MAGIC You've demonstrated mastery of:
# MAGIC - Complex streaming pipeline design
# MAGIC - Multi-source data integration
# MAGIC - Real-time geospatial analytics
# MAGIC - Predictive modeling
# MAGIC - Dashboard data preparation
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Enhance with ML models for better predictions
# MAGIC - Add real-time visualization with BI tools
# MAGIC - Implement advanced routing algorithms
# MAGIC - Deploy to production with monitoring
