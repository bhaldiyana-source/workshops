# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Stream-to-Stream Spatial Joins
# MAGIC
# MAGIC ## Overview
# MAGIC This demo covers advanced stream-to-stream spatial joins using Spark Structured Streaming. You'll learn to join multiple moving object streams, implement watermarking strategies for spatial data, optimize performance with spatial indexing, and handle real-world scenarios like driver-rider matching and asset tracking.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Join moving objects with static geometries
# MAGIC - Join multiple streaming sources
# MAGIC - Implement watermarking for stream-stream joins
# MAGIC - Optimize spatial joins with H3 indexing
# MAGIC - Handle temporal constraints in spatial matching
# MAGIC - Implement proximity-based matching
# MAGIC - Optimize join performance and manage state
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1-3
# MAGIC - Understanding of stream-stream joins
# MAGIC - Knowledge of watermarking concepts
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install faker h3 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configure
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import random

# Configuration
catalog = "main"
schema_name = "geospatial_streaming"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Using catalog: {catalog}")
print(f"Using schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create Multiple Streams (Driver and Rider)
# MAGIC
# MAGIC We'll simulate a ride-sharing scenario with driver and rider streams.

# COMMAND ----------

# DBTITLE 1,Generate Driver Stream Data
from faker import Faker
fake = Faker()

def generate_driver_data(num_drivers=5, records_per_driver=30):
    """Generate synthetic driver location data"""
    records = []
    
    # San Francisco bounds
    lat_min, lat_max = 37.75, 37.80
    lon_min, lon_max = -122.45, -122.40
    
    for driver_num in range(num_drivers):
        driver_id = f"driver_{driver_num:03d}"
        vehicle_type = random.choice(["sedan", "suv", "compact"])
        
        # Start position
        lat = random.uniform(lat_min, lat_max)
        lon = random.uniform(lon_min, lon_max)
        base_time = datetime.now() - timedelta(minutes=30)
        
        for i in range(records_per_driver):
            # Simulate movement
            lat += random.uniform(-0.002, 0.002)
            lon += random.uniform(-0.002, 0.002)
            
            # Keep within bounds
            lat = max(lat_min, min(lat_max, lat))
            lon = max(lon_min, min(lon_max, lon))
            
            ts = base_time + timedelta(seconds=i * 20)
            
            record = {
                "driver_id": driver_id,
                "timestamp": ts,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "vehicle_type": vehicle_type,
                "available": random.choice([True, True, True, False]),  # 75% available
                "rating": round(random.uniform(4.0, 5.0), 2),
                "trips_today": random.randint(0, 20)
            }
            records.append(record)
    
    return records

driver_records = generate_driver_data(num_drivers=5, records_per_driver=30)
print(f"Generated {len(driver_records)} driver records")

# COMMAND ----------

# DBTITLE 1,Generate Rider Request Stream Data
def generate_rider_data(num_riders=8, records_per_rider=20):
    """Generate synthetic rider request data"""
    records = []
    
    # San Francisco bounds
    lat_min, lat_max = 37.75, 37.80
    lon_min, lon_max = -122.45, -122.40
    
    for rider_num in range(num_riders):
        rider_id = f"rider_{rider_num:03d}"
        
        # Request location
        lat = random.uniform(lat_min, lat_max)
        lon = random.uniform(lon_min, lon_max)
        
        # Destination
        dest_lat = random.uniform(lat_min, lat_max)
        dest_lon = random.uniform(lon_min, lon_max)
        
        base_time = datetime.now() - timedelta(minutes=25)
        
        for i in range(records_per_rider):
            ts = base_time + timedelta(seconds=i * 30)
            
            record = {
                "rider_id": rider_id,
                "request_timestamp": ts,
                "pickup_latitude": round(lat, 6),
                "pickup_longitude": round(lon, 6),
                "dropoff_latitude": round(dest_lat, 6),
                "dropoff_longitude": round(dest_lon, 6),
                "passenger_count": random.randint(1, 4),
                "ride_type": random.choice(["standard", "premium", "shared"]),
                "max_wait_minutes": random.randint(5, 15)
            }
            records.append(record)
    
    return records

rider_records = generate_rider_data(num_riders=8, records_per_rider=20)
print(f"Generated {len(rider_records)} rider request records")

# COMMAND ----------

# DBTITLE 1,Write Driver and Rider Data to Landing Zones
# Write driver data
driver_landing = f"{data_base}/landing/drivers"
dbutils.fs.rm(driver_landing, recurse=True)

driver_schema = StructType([
    StructField("driver_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("vehicle_type", StringType(), True),
    StructField("available", BooleanType(), False),
    StructField("rating", DoubleType(), True),
    StructField("trips_today", IntegerType(), True)
])

batch_size = 30
for i in range(0, len(driver_records), batch_size):
    batch = driver_records[i:i+batch_size]
    batch_df = spark.createDataFrame(batch, schema=driver_schema)
    batch_df.write.mode("overwrite").json(f"{driver_landing}/batch_{i//batch_size:03d}")

# Write rider data
rider_landing = f"{data_base}/landing/riders"
dbutils.fs.rm(rider_landing, recurse=True)

rider_schema = StructType([
    StructField("rider_id", StringType(), False),
    StructField("request_timestamp", TimestampType(), False),
    StructField("pickup_latitude", DoubleType(), False),
    StructField("pickup_longitude", DoubleType(), False),
    StructField("dropoff_latitude", DoubleType(), False),
    StructField("dropoff_longitude", DoubleType(), False),
    StructField("passenger_count", IntegerType(), True),
    StructField("ride_type", StringType(), True),
    StructField("max_wait_minutes", IntegerType(), True)
])

for i in range(0, len(rider_records), batch_size):
    batch = rider_records[i:i+batch_size]
    batch_df = spark.createDataFrame(batch, schema=rider_schema)
    batch_df.write.mode("overwrite").json(f"{rider_landing}/batch_{i//batch_size:03d}")

print("Data written to landing zones")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Create Streaming DataFrames

# COMMAND ----------

# DBTITLE 1,Create Driver Stream with Spatial Index
driver_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(driver_schema)
    .load(driver_landing)
    .filter("available = true")  # Only available drivers
    .withColumn("h3_driver", F.expr("h3_latlngtocell(latitude, longitude, 9)"))
    .withColumn("driver_point", F.expr("ST_Point(longitude, latitude)"))
    .withWatermark("timestamp", "5 minutes")
)

print("Driver stream created")

# COMMAND ----------

# DBTITLE 1,Create Rider Stream with Spatial Index
rider_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(rider_schema)
    .load(rider_landing)
    .withColumn("h3_rider", F.expr("h3_latlngtocell(pickup_latitude, pickup_longitude, 9)"))
    .withColumn("rider_point", F.expr("ST_Point(pickup_longitude, pickup_latitude)"))
    .withWatermark("request_timestamp", "5 minutes")
)

print("Rider stream created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Stream-to-Stream Spatial Join
# MAGIC
# MAGIC Join drivers with riders based on proximity and time window.

# COMMAND ----------

# DBTITLE 1,H3-Based Pre-filtering Join
# First, expand H3 cells to include neighbors for proximity matching
driver_with_neighbors = driver_stream.withColumn(
    "h3_cells",
    F.expr("h3_gridring(h3_driver, 1)")  # Include neighboring cells
).select(
    "driver_id",
    "timestamp",
    "latitude",
    "longitude",
    "vehicle_type",
    "rating",
    "trips_today",
    "driver_point",
    F.explode("h3_cells").alias("h3_match")
)

rider_for_match = rider_stream.select(
    "rider_id",
    "request_timestamp",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude",
    "passenger_count",
    "ride_type",
    "max_wait_minutes",
    "rider_point",
    F.col("h3_rider").alias("h3_match")
)

# COMMAND ----------

# DBTITLE 1,Perform Stream-Stream Join with Spatial and Temporal Constraints
# Join on H3 cells and time window
matches = (
    driver_with_neighbors
    .join(
        rider_for_match,
        [
            driver_with_neighbors.h3_match == rider_for_match.h3_match,
            # Time constraint: driver location within ±2 minutes of request
            driver_with_neighbors.timestamp.between(
                F.expr("request_timestamp - INTERVAL 2 MINUTES"),
                F.expr("request_timestamp + INTERVAL 2 MINUTES")
            )
        ],
        "inner"
    )
)

# COMMAND ----------

# DBTITLE 1,Calculate Precise Distance and Filter by Proximity
matches_with_distance = matches.withColumn(
    "distance_meters",
    F.expr("ST_Distance(driver_point, rider_point)")
).filter(
    "distance_meters <= 1000"  # Within 1km
).withColumn(
    "eta_minutes",
    F.round(F.col("distance_meters") / 250 / 60, 1)  # Assume 15 km/h average speed
)

# COMMAND ----------

# DBTITLE 1,Rank Matches and Select Best Driver for Each Rider
# For each rider, rank drivers by distance and rating
match_window = Window.partitionBy("rider_id", "request_timestamp").orderBy(
    "distance_meters", F.desc("rating")
)

ranked_matches = matches_with_distance.withColumn(
    "match_rank",
    F.row_number().over(match_window)
).filter("match_rank <= 3")  # Keep top 3 matches per rider

# COMMAND ----------

# DBTITLE 1,Add Match Scoring
final_matches = ranked_matches.withColumn(
    "match_score",
    F.round(
        (1000 - F.col("distance_meters")) / 1000 * 50 +  # Distance component (max 50 points)
        F.col("rating") * 10,  # Rating component (max 50 points)
        2
    )
).withColumn(
    "match_id",
    F.expr("uuid()")
).withColumn(
    "matched_at",
    F.current_timestamp()
).select(
    "match_id",
    "driver_id",
    "rider_id",
    "timestamp",
    "request_timestamp",
    "distance_meters",
    "eta_minutes",
    "rating",
    "vehicle_type",
    "ride_type",
    "passenger_count",
    "match_rank",
    "match_score",
    "matched_at",
    F.col("latitude").alias("driver_lat"),
    F.col("longitude").alias("driver_lon"),
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude"
)

# COMMAND ----------

# DBTITLE 1,Write Matches to Delta Table
matches_table = f"{catalog}.{schema_name}.driver_rider_matches"
matches_checkpoint = f"{checkpoint_base}/driver_rider_matches"

query_matches = (
    final_matches
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", matches_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(matches_table)
)

print(f"Started matches query: {query_matches.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Static-Stream Spatial Join
# MAGIC
# MAGIC Join moving vehicles with static points of interest (POI).

# COMMAND ----------

# DBTITLE 1,Create Static POI Data
poi_data = [
    {"poi_id": "poi_001", "name": "Airport", "category": "transport", "latitude": 37.773, "longitude": -122.425, "radius_meters": 500},
    {"poi_id": "poi_002", "name": "Ferry Building", "category": "landmark", "latitude": 37.795, "longitude": -122.393, "radius_meters": 200},
    {"poi_id": "poi_003", "name": "Golden Gate Park", "category": "park", "latitude": 37.769, "longitude": -122.486, "radius_meters": 1000},
    {"poi_id": "poi_004", "name": "Union Square", "category": "shopping", "latitude": 37.788, "longitude": -122.407, "radius_meters": 300},
    {"poi_id": "poi_005", "name": "Fisherman's Wharf", "category": "tourist", "latitude": 37.808, "longitude": -122.415, "radius_meters": 400}
]

poi_df = spark.createDataFrame(poi_data)
poi_table = f"{catalog}.{schema_name}.points_of_interest"
poi_df.write.mode("overwrite").saveAsTable(poi_table)

# Add H3 index to POIs
poi_with_h3 = poi_df.withColumn(
    "h3_index",
    F.expr("h3_latlngtocell(latitude, longitude, 9)")
).withColumn(
    "poi_point",
    F.expr("ST_Point(longitude, latitude)")
)

print(f"Created {poi_df.count()} points of interest")
poi_with_h3.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Join Driver Stream with Static POIs
# Load the GPS stream (reusing from earlier demos)
gps_table = f"{catalog}.{schema_name}.gps_deduplicated"

vehicle_stream = (
    spark.readStream
    .format("delta")
    .table(gps_table)
    .filter("is_valid = true")
    .withColumn("vehicle_point", F.expr("ST_Point(longitude, latitude)"))
)

# Join with POIs (broadcast static data)
vehicle_near_poi = (
    vehicle_stream
    .join(
        F.broadcast(poi_with_h3),
        vehicle_stream.h3_index == poi_with_h3.h3_index,
        "inner"
    )
    .withColumn(
        "distance_to_poi",
        F.expr("ST_Distance(vehicle_point, poi_point)")
    )
    .filter("distance_to_poi <= radius_meters")
    .select(
        "device_id",
        "timestamp",
        F.col("latitude").alias("vehicle_lat"),
        F.col("longitude").alias("vehicle_lon"),
        "poi_id",
        F.col("name").alias("poi_name"),
        "category",
        F.round("distance_to_poi", 2).alias("distance_meters"),
        "speed"
    )
    .withColumn("event_id", F.expr("uuid()"))
    .withColumn("detected_at", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Write Vehicle-POI Proximity Events
vehicle_poi_table = f"{catalog}.{schema_name}.vehicle_poi_proximity"
vehicle_poi_checkpoint = f"{checkpoint_base}/vehicle_poi_proximity"

query_vehicle_poi = (
    vehicle_near_poi
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", vehicle_poi_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(vehicle_poi_table)
)

print(f"Started vehicle-POI query: {query_vehicle_poi.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Optimize Join Performance

# COMMAND ----------

# DBTITLE 1,Monitor Join Performance
import time
print("Processing streams... waiting 20 seconds")
time.sleep(20)

# Check matches
matches_df = spark.table(matches_table)
print(f"\n=== Driver-Rider Matches ===")
print(f"Total matches: {matches_df.count()}")

print("\nTop matches by score:")
matches_df.orderBy(F.desc("match_score")).limit(10).show(truncate=False)

print("\nMatches per rider:")
matches_df.groupBy("rider_id").agg(
    F.count("*").alias("total_matches"),
    F.min("distance_meters").alias("closest_driver_m"),
    F.avg("distance_meters").alias("avg_distance_m"),
    F.max("match_score").alias("best_score")
).orderBy("rider_id").show()

# COMMAND ----------

# DBTITLE 1,Vehicle-POI Proximity Analysis
vehicle_poi_df = spark.table(vehicle_poi_table)
print(f"\n=== Vehicle-POI Proximity ===")
print(f"Total proximity events: {vehicle_poi_df.count()}")

print("\nEvents by POI:")
vehicle_poi_df.groupBy("poi_name", "category").agg(
    F.countDistinct("device_id").alias("unique_vehicles"),
    F.count("*").alias("total_detections"),
    F.avg("distance_meters").alias("avg_distance_m"),
    F.avg("speed").alias("avg_speed_mps")
).orderBy("poi_name").show(truncate=False)

print("\nVehicles visiting multiple POIs:")
vehicle_poi_df.groupBy("device_id").agg(
    F.countDistinct("poi_id").alias("pois_visited"),
    F.collect_set("poi_name").alias("poi_names")
).filter("pois_visited > 1").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Join Performance Metrics
print("\n=== Join Performance Analysis ===")

# Get query metrics
for query in [query_matches, query_vehicle_poi]:
    if query.lastProgress:
        progress = query.lastProgress
        print(f"\nQuery: {query.name}")
        print(f"  Batch ID: {progress.get('batchId', 'N/A')}")
        print(f"  Input Rows: {progress.get('numInputRows', 0)}")
        print(f"  Processed Rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
        print(f"  Duration: {progress.get('durationMs', {}).get('triggerExecution', 0)} ms")
        
        # State info
        if 'stateOperators' in progress and progress['stateOperators']:
            state = progress['stateOperators'][0]
            print(f"  State Rows: {state.get('numRowsTotal', 0)}")
            print(f"  State Memory: {state.get('memoryUsedBytes', 0) / 1024 / 1024:.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced Join Patterns

# COMMAND ----------

# DBTITLE 1,Aggregated Match Statistics
# Real-time statistics on matching performance
match_stats = (
    final_matches
    .withWatermark("matched_at", "10 minutes")
    .groupBy(
        F.window("matched_at", "2 minutes"),
        "ride_type"
    )
    .agg(
        F.count("*").alias("total_matches"),
        F.countDistinct("driver_id").alias("unique_drivers"),
        F.countDistinct("rider_id").alias("unique_riders"),
        F.avg("distance_meters").alias("avg_distance_m"),
        F.avg("eta_minutes").alias("avg_eta_minutes"),
        F.avg("match_score").alias("avg_score")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "ride_type",
        "total_matches",
        "unique_drivers",
        "unique_riders",
        F.round("avg_distance_m", 1).alias("avg_distance_m"),
        F.round("avg_eta_minutes", 1).alias("avg_eta_min"),
        F.round("avg_score", 2).alias("avg_score")
    )
)

stats_table = f"{catalog}.{schema_name}.match_statistics"
stats_checkpoint = f"{checkpoint_base}/match_statistics"

query_stats = (
    match_stats
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", stats_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(stats_table)
)

print(f"Started match statistics query: {query_stats.name}")

# COMMAND ----------

# DBTITLE 1,Query Match Statistics
time.sleep(15)

stats = spark.table(stats_table)
print(f"Total statistic records: {stats.count()}")

if stats.count() > 0:
    print("\nMatching statistics by time window:")
    stats.orderBy(F.desc("window_start")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Cleanup

# COMMAND ----------

# DBTITLE 1,List All Active Streams
print("=== Active Streaming Queries ===\n")
for stream in spark.streams.active:
    print(f"- {stream.name} (ID: {stream.id})")
    print(f"  Status: {stream.status['message']}")
    print()

# COMMAND ----------

# DBTITLE 1,Stop All Streams (Optional)
# Uncomment to stop all streams
# for stream in spark.streams.active:
#     print(f"Stopping {stream.name}...")
#     stream.stop()
# print("All streams stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Implemented
# MAGIC
# MAGIC 1. **Stream-to-Stream Join**
# MAGIC    - Joined driver and rider streams in real-time
# MAGIC    - Applied temporal constraints (time windows)
# MAGIC    - Calculated proximity-based matching
# MAGIC
# MAGIC 2. **Static-Stream Join**
# MAGIC    - Joined moving vehicles with static POIs
# MAGIC    - Detected proximity events
# MAGIC    - Broadcast optimization for static data
# MAGIC
# MAGIC 3. **Spatial Optimization**
# MAGIC    - H3 indexing for pre-filtering
# MAGIC    - Neighbor cell expansion for proximity
# MAGIC    - Precise distance calculation after filtering
# MAGIC
# MAGIC 4. **Watermarking**
# MAGIC    - 5-minute watermarks on both streams
# MAGIC    - State cleanup for bounded memory
# MAGIC    - Late data handling
# MAGIC
# MAGIC 5. **Match Ranking and Scoring**
# MAGIC    - Multiple matches per request
# MAGIC    - Distance and rating-based scoring
# MAGIC    - Top-N selection per rider
# MAGIC
# MAGIC 6. **Real-time Analytics**
# MAGIC    - Windowed match statistics
# MAGIC    - Performance monitoring
# MAGIC    - Quality metrics
# MAGIC
# MAGIC ### Performance Optimization Techniques
# MAGIC
# MAGIC ✅ H3 spatial indexing for O(1) lookups  
# MAGIC ✅ Broadcast joins for small static data  
# MAGIC ✅ Watermarking for state management  
# MAGIC ✅ Pre-filtering before expensive operations  
# MAGIC ✅ Appropriate window sizes for joins  
# MAGIC ✅ Partitioning by spatial keys  
# MAGIC
# MAGIC ### Key Learnings
# MAGIC
# MAGIC - **Watermarks are essential** for stream-stream joins
# MAGIC - **H3 indexing dramatically improves** spatial join performance
# MAGIC - **State management is critical** for long-running joins
# MAGIC - **Broadcast static data** when possible
# MAGIC - **Monitor state size** to prevent memory issues
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Continue to **Demo 5: Delta Live Tables for Geospatial** to learn:
# MAGIC - Building medallion architecture for location data
# MAGIC - Implementing data quality expectations
# MAGIC - CDC patterns with spatial updates
# MAGIC - Incremental processing strategies
