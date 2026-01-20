# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Real-Time Geofencing
# MAGIC
# MAGIC ## Overview
# MAGIC This demo demonstrates real-time geofencing using Spark Structured Streaming. You'll learn to define dynamic geofence boundaries, detect entry and exit events, generate alerts, and manage stateful processing for tracking devices across geofence boundaries. We'll implement both static and dynamic geofencing patterns with efficient spatial indexing.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Define geofence boundaries using various geometry types
# MAGIC - Implement point-in-polygon testing at stream time
# MAGIC - Detect geofence entry and exit events with state management
# MAGIC - Generate real-time alerts for geofence violations
# MAGIC - Optimize geofence lookups with H3 indexing
# MAGIC - Handle dynamic geofence updates
# MAGIC - Implement time-based geofence rules
# MAGIC - Build alert throttling and deduplication
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1: Setting Up Streaming Pipelines
# MAGIC - Understanding of stateful streaming operations
# MAGIC - Basic knowledge of spatial geometries
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install faker h3 shapely --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries and Configure Environment
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import GroupState, GroupStateTimeout, OutputMode
from delta.tables import DeltaTable
import json
from datetime import datetime, timedelta

# Configuration
catalog = "main"
schema_name = "geospatial_streaming"
checkpoint_base = f"/tmp/{schema_name}/checkpoints"
data_base = f"/tmp/{schema_name}/data"

# Use existing catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Using catalog: {catalog}")
print(f"Using schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Define Geofences
# MAGIC
# MAGIC We'll create various types of geofences:
# MAGIC - Circular zones (simple radius-based)
# MAGIC - Polygonal zones (complex boundaries)
# MAGIC - Time-restricted zones (active only during certain hours)

# COMMAND ----------

# DBTITLE 1,Create Geofence Definitions
from shapely.geometry import Point, Polygon
from shapely import wkt

# Define geofences in San Francisco area
geofences_data = [
    {
        "geofence_id": "zone_001",
        "name": "Financial District",
        "geometry_type": "POLYGON",
        "geometry_wkt": "POLYGON((-122.405 37.791, -122.395 37.791, -122.395 37.795, -122.405 37.795, -122.405 37.791))",
        "center_lat": 37.793,
        "center_lon": -122.400,
        "radius_meters": None,
        "category": "commercial",
        "alert_on_entry": True,
        "alert_on_exit": False,
        "active": True,
        "time_restriction_start": None,
        "time_restriction_end": None,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    },
    {
        "geofence_id": "zone_002",
        "name": "Restricted Area - Port",
        "geometry_type": "CIRCLE",
        "geometry_wkt": None,
        "center_lat": 37.780,
        "center_lon": -122.390,
        "radius_meters": 500.0,
        "category": "restricted",
        "alert_on_entry": True,
        "alert_on_exit": True,
        "active": True,
        "time_restriction_start": None,
        "time_restriction_end": None,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    },
    {
        "geofence_id": "zone_003",
        "name": "Delivery Zone - North Beach",
        "geometry_type": "POLYGON",
        "geometry_wkt": "POLYGON((-122.415 37.798, -122.405 37.798, -122.405 37.808, -122.415 37.808, -122.415 37.798))",
        "center_lat": 37.803,
        "center_lon": -122.410,
        "radius_meters": None,
        "category": "delivery",
        "alert_on_entry": False,
        "alert_on_exit": True,
        "active": True,
        "time_restriction_start": None,
        "time_restriction_end": None,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    },
    {
        "geofence_id": "zone_004",
        "name": "Night Restricted Zone",
        "geometry_type": "CIRCLE",
        "geometry_wkt": None,
        "center_lat": 37.775,
        "center_lon": -122.420,
        "radius_meters": 800.0,
        "category": "restricted",
        "alert_on_entry": True,
        "alert_on_exit": False,
        "active": True,
        "time_restriction_start": 22,  # 10 PM
        "time_restriction_end": 6,      # 6 AM
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    },
    {
        "geofence_id": "zone_005",
        "name": "City Center",
        "geometry_type": "POLYGON",
        "geometry_wkt": "POLYGON((-122.430 37.770, -122.380 37.770, -122.380 37.810, -122.430 37.810, -122.430 37.770))",
        "center_lat": 37.790,
        "center_lon": -122.405,
        "radius_meters": None,
        "category": "monitoring",
        "alert_on_entry": False,
        "alert_on_exit": False,
        "active": True,
        "time_restriction_start": None,
        "time_restriction_end": None,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }
]

# Create schema for geofences
geofence_schema = StructType([
    StructField("geofence_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("geometry_type", StringType(), False),
    StructField("geometry_wkt", StringType(), True),
    StructField("center_lat", DoubleType(), True),
    StructField("center_lon", DoubleType(), True),
    StructField("radius_meters", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("alert_on_entry", BooleanType(), False),
    StructField("alert_on_exit", BooleanType(), False),
    StructField("active", BooleanType(), False),
    StructField("time_restriction_start", IntegerType(), True),
    StructField("time_restriction_end", IntegerType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

# Create DataFrame and save to Delta
geofences_df = spark.createDataFrame(geofences_data, schema=geofence_schema)

geofence_table = f"{catalog}.{schema_name}.geofences"
geofences_df.write.mode("overwrite").saveAsTable(geofence_table)

print(f"Created {len(geofences_data)} geofences")
geofences_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Add H3 Index to Geofences for Fast Lookups
# For polygon geofences, we'll generate H3 cells that cover the polygon
# For circle geofences, we'll generate H3 cells within the radius

def get_h3_cells_for_circle(center_lat, center_lon, radius_meters, resolution=9):
    """Generate H3 cells covering a circular geofence"""
    import h3
    
    # Get center H3 cell
    center_h3 = h3.latlng_to_cell(center_lat, center_lon, resolution)
    
    # Get k-ring (neighboring cells)
    # Approximate k based on radius
    k = max(1, int(radius_meters / 200))  # ~200m per resolution 9 cell
    
    cells = list(h3.grid_disk(center_h3, k))
    return cells

def get_h3_cells_for_polygon(wkt_str, resolution=9):
    """Generate H3 cells covering a polygon geofence"""
    import h3
    from shapely import wkt
    
    poly = wkt.loads(wkt_str)
    
    # Get bounding box
    minx, miny, maxx, maxy = poly.bounds
    
    # Sample points across the polygon
    cells = set()
    lat_step = (maxy - miny) / 10
    lon_step = (maxx - minx) / 10
    
    for lat in [miny + i * lat_step for i in range(11)]:
        for lon in [minx + i * lon_step for i in range(11)]:
            point = Point(lon, lat)
            if poly.contains(point):
                cell = h3.latlng_to_cell(lat, lon, resolution)
                cells.add(cell)
                # Add neighbors to ensure coverage
                cells.update(h3.grid_disk(cell, 1))
    
    return list(cells)

# Generate H3 cells for each geofence
geofence_h3_data = []

for gf in geofences_data:
    if gf["geometry_type"] == "CIRCLE":
        cells = get_h3_cells_for_circle(
            gf["center_lat"],
            gf["center_lon"],
            gf["radius_meters"]
        )
    else:  # POLYGON
        cells = get_h3_cells_for_polygon(gf["geometry_wkt"])
    
    for cell in cells:
        geofence_h3_data.append({
            "geofence_id": gf["geofence_id"],
            "h3_index": cell
        })

# Create H3 lookup table
geofence_h3_df = spark.createDataFrame(geofence_h3_data)
geofence_h3_table = f"{catalog}.{schema_name}.geofence_h3_index"
geofence_h3_df.write.mode("overwrite").saveAsTable(geofence_h3_table)

print(f"Generated {len(geofence_h3_data)} H3 cell mappings for geofences")
print("\nH3 cells per geofence:")
geofence_h3_df.groupBy("geofence_id").count().orderBy("geofence_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: GPS Stream with Geofence Testing
# MAGIC
# MAGIC We'll read the GPS stream and test each point against geofences.

# COMMAND ----------

# DBTITLE 1,Load GPS Stream
# Read from the table created in Demo 1
gps_table = f"{catalog}.{schema_name}.gps_deduplicated"

# Create a streaming DataFrame from the Delta table
gps_stream = (
    spark.readStream
    .format("delta")
    .table(gps_table)
    .filter("is_valid = true")
    .select(
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "speed",
        "heading",
        "h3_index",
        "point_wkt"
    )
)

print("GPS stream loaded")
print(f"Is streaming: {gps_stream.isStreaming}")

# COMMAND ----------

# DBTITLE 1,Join GPS Stream with Geofences (Using H3 Pre-filtering)
# Load geofence data for broadcast
geofences_static = spark.table(geofence_table)
geofence_h3_static = spark.table(geofence_h3_table)

# Step 1: Pre-filter with H3 index (fast)
gps_with_candidate_fences = (
    gps_stream
    .join(
        F.broadcast(geofence_h3_static),
        "h3_index",
        "inner"
    )
    # Join with full geofence details
    .join(
        F.broadcast(geofences_static),
        "geofence_id",
        "inner"
    )
    .filter("active = true")  # Only active geofences
)

# Step 2: Precise geometry testing
gps_in_geofences = gps_with_candidate_fences.withColumn(
    "is_inside",
    F.when(
        F.col("geometry_type") == "CIRCLE",
        # Circle: distance test
        F.expr("""
            ST_Distance(
                ST_Point(longitude, latitude),
                ST_Point(center_lon, center_lat)
            ) <= radius_meters
        """)
    ).when(
        F.col("geometry_type") == "POLYGON",
        # Polygon: contains test
        F.expr("ST_Contains(ST_GeomFromText(geometry_wkt), ST_Point(longitude, latitude))")
    ).otherwise(False)
).filter("is_inside = true")

# Step 3: Apply time restrictions
gps_in_geofences = gps_in_geofences.withColumn(
    "current_hour",
    F.hour("timestamp")
).withColumn(
    "time_allowed",
    F.when(
        F.col("time_restriction_start").isNull(),
        True  # No time restriction
    ).when(
        F.col("time_restriction_start") <= F.col("time_restriction_end"),
        # Normal range (e.g., 9 AM to 5 PM)
        F.col("current_hour").between(F.col("time_restriction_start"), F.col("time_restriction_end"))
    ).otherwise(
        # Overnight range (e.g., 10 PM to 6 AM)
        (F.col("current_hour") >= F.col("time_restriction_start")) |
        (F.col("current_hour") <= F.col("time_restriction_end"))
    )
)

# Keep only geofence matches that pass time restrictions
gps_in_geofences_filtered = gps_in_geofences.filter("time_allowed = true")

# COMMAND ----------

# DBTITLE 1,Write Current Geofence Positions
# This shows which devices are currently in which geofences
current_positions_table = f"{catalog}.{schema_name}.current_geofence_positions"
current_positions_checkpoint = f"{checkpoint_base}/current_positions"

query_positions = (
    gps_in_geofences_filtered
    .select(
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "geofence_id",
        F.col("name").alias("geofence_name"),
        "category",
        "speed"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", current_positions_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(current_positions_table)
)

print(f"Started current positions query: {query_positions.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Stateful Entry/Exit Event Detection
# MAGIC
# MAGIC We'll use stateful processing to track when devices enter and exit geofences.

# COMMAND ----------

# DBTITLE 1,Define State Schema and Update Function
# State for tracking device geofence status
class DeviceGeofenceState:
    def __init__(self, geofence_id=None, entry_time=None, last_seen=None):
        self.geofence_id = geofence_id
        self.entry_time = entry_time
        self.last_seen = last_seen

# Schema for output events
event_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("event_type", StringType(), False),  # ENTER or EXIT
    StructField("event_timestamp", TimestampType(), False),
    StructField("geofence_id", StringType(), False),
    StructField("geofence_name", StringType(), False),
    StructField("category", StringType(), True),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("speed", DoubleType(), True),
    StructField("dwell_time_seconds", DoubleType(), True)
])

# COMMAND ----------

# DBTITLE 1,Implement Stateful Event Detection with FlatMapGroupsWithState
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

def detect_geofence_events(device_id, locations, state):
    """
    Detect ENTER and EXIT events for a device
    
    State stores:
    - current_geofence: which geofence the device is currently in
    - entry_time: when the device entered the current geofence
    """
    events = []
    
    # Get current state
    if state.exists:
        state_data = state.get
        current_fence = state_data[0]
        entry_time = state_data[1]
    else:
        current_fence = None
        entry_time = None
    
    # Process each location update
    for loc in sorted(locations, key=lambda x: x.timestamp):
        new_fence = loc.geofence_id if hasattr(loc, 'geofence_id') and loc.geofence_id else None
        
        # Check if fence changed
        if new_fence != current_fence:
            # EXIT event
            if current_fence is not None:
                dwell_seconds = (loc.timestamp - entry_time).total_seconds() if entry_time else 0
                
                # Create EXIT event
                events.append({
                    "device_id": device_id,
                    "event_type": "EXIT",
                    "event_timestamp": loc.timestamp,
                    "geofence_id": current_fence,
                    "geofence_name": getattr(loc, 'geofence_name', current_fence),
                    "category": getattr(loc, 'category', None),
                    "latitude": loc.latitude,
                    "longitude": loc.longitude,
                    "speed": getattr(loc, 'speed', None),
                    "dwell_time_seconds": dwell_seconds
                })
            
            # ENTER event
            if new_fence is not None:
                events.append({
                    "device_id": device_id,
                    "event_type": "ENTER",
                    "event_timestamp": loc.timestamp,
                    "geofence_id": new_fence,
                    "geofence_name": getattr(loc, 'geofence_name', new_fence),
                    "category": getattr(loc, 'category', None),
                    "latitude": loc.latitude,
                    "longitude": loc.longitude,
                    "speed": getattr(loc, 'speed', None),
                    "dwell_time_seconds": None
                })
                entry_time = loc.timestamp
            
            current_fence = new_fence
    
    # Update state
    if current_fence is not None:
        state.update((current_fence, entry_time))
    else:
        state.remove()
    
    return iter(events)

# Note: The above function shows the logic pattern
# In practice, we'll use a simpler windowed approach for this demo

# COMMAND ----------

# DBTITLE 1,Simpler Approach - Window-Based Event Detection
# For this demo, we'll use a windowed approach to detect changes
# This is simpler than full stateful processing but still effective

# First, create a stream with ALL geofence states (in or out)
from pyspark.sql.window import Window

# Create a complete timeline with geofence status
gps_timeline = gps_stream.select(
    "device_id",
    "timestamp",
    "latitude",
    "longitude",
    "speed"
)

# Join to get geofences (or null if not in any)
gps_with_fences = (
    gps_timeline
    .join(
        F.broadcast(geofence_h3_static),
        gps_timeline.h3_index == geofence_h3_static.h3_index,
        "left"
    )
    .join(
        F.broadcast(geofences_static),
        ["geofence_id"],
        "left"
    )
    .withColumn(
        "is_inside",
        F.when(
            F.col("geofence_id").isNotNull(),
            F.when(
                F.col("geometry_type") == "CIRCLE",
                F.expr("ST_Distance(ST_Point(longitude, latitude), ST_Point(center_lon, center_lat)) <= radius_meters")
            ).when(
                F.col("geometry_type") == "POLYGON",
                F.expr("ST_Contains(ST_GeomFromText(geometry_wkt), ST_Point(longitude, latitude))")
            )
        ).otherwise(False)
    )
    .filter(F.col("is_inside") | F.col("geofence_id").isNull())
    .select(
        "device_id",
        "timestamp",
        "latitude",
        "longitude",
        "speed",
        F.coalesce("geofence_id", F.lit("OUTSIDE")).alias("geofence_id"),
        F.coalesce("name", F.lit("Outside")).alias("geofence_name"),
        "category"
    )
)

# Detect transitions using lag window function
window_spec = Window.partitionBy("device_id", "geofence_id").orderBy("timestamp")

gps_with_transitions = gps_with_fences.withColumn(
    "prev_fence",
    F.lag("geofence_id").over(Window.partitionBy("device_id").orderBy("timestamp"))
).withColumn(
    "is_transition",
    (F.col("geofence_id") != F.col("prev_fence")) | F.col("prev_fence").isNull()
)

# Create events for transitions
geofence_events = gps_with_transitions.filter("is_transition = true").select(
    "device_id",
    F.when(F.col("geofence_id") != "OUTSIDE", F.lit("ENTER"))
     .otherwise(F.lit("EXIT"))
     .alias("event_type"),
    F.col("timestamp").alias("event_timestamp"),
    "geofence_id",
    "geofence_name",
    "category",
    "latitude",
    "longitude",
    "speed"
)

# COMMAND ----------

# DBTITLE 1,Write Geofence Events
events_table = f"{catalog}.{schema_name}.geofence_events"
events_checkpoint = f"{checkpoint_base}/geofence_events"

query_events = (
    geofence_events
    .filter("geofence_id != 'OUTSIDE'")  # Only care about actual geofences
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", events_checkpoint)
    .trigger(processingTime="5 seconds")
    .toTable(events_table)
)

print(f"Started geofence events query: {query_events.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Generate Real-Time Alerts

# COMMAND ----------

# DBTITLE 1,Filter for Alert-Worthy Events
# Join events with geofence config to determine which need alerts
alerts_stream = (
    geofence_events
    .filter("geofence_id != 'OUTSIDE'")
    .join(
        F.broadcast(geofences_static.select(
            "geofence_id",
            "alert_on_entry",
            "alert_on_exit"
        )),
        "geofence_id"
    )
    .filter(
        (F.col("event_type") == "ENTER") & (F.col("alert_on_entry")) |
        (F.col("event_type") == "EXIT") & (F.col("alert_on_exit"))
    )
    .withColumn("alert_id", F.expr("uuid()"))
    .withColumn("alert_generated_at", F.current_timestamp())
    .withColumn(
        "alert_priority",
        F.when(F.col("category") == "restricted", "HIGH")
         .when(F.col("category") == "delivery", "MEDIUM")
         .otherwise("LOW")
    )
    .withColumn(
        "alert_message",
        F.concat(
            F.lit("Device "), F.col("device_id"),
            F.lit(" "), F.lower(F.col("event_type")),
            F.lit("ed geofence: "), F.col("geofence_name"),
            F.lit(" ("), F.col("category"), F.lit(")")
        )
    )
    .select(
        "alert_id",
        "device_id",
        "event_type",
        "event_timestamp",
        "geofence_id",
        "geofence_name",
        "category",
        "alert_priority",
        "alert_message",
        "latitude",
        "longitude",
        "speed",
        "alert_generated_at"
    )
)

# COMMAND ----------

# DBTITLE 1,Write Alerts to Delta Table
alerts_table = f"{catalog}.{schema_name}.geofence_alerts"
alerts_checkpoint = f"{checkpoint_base}/geofence_alerts"

query_alerts = (
    alerts_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", alerts_checkpoint)
    .trigger(processingTime="2 seconds")  # More frequent for alerts
    .toTable(alerts_table)
)

print(f"Started alerts query: {query_alerts.name}")

# COMMAND ----------

# DBTITLE 1,Alert Notification Function (Simulation)
def send_alert_notifications(batch_df, batch_id):
    """
    Process a batch of alerts and send notifications
    This is a simulation - in production, you'd integrate with:
    - SNS/SQS
    - PagerDuty
    - Slack/Teams
    - Email service
    - SMS gateway
    """
    alerts = batch_df.collect()
    
    for alert in alerts:
        priority = alert["alert_priority"]
        message = alert["alert_message"]
        timestamp = alert["event_timestamp"]
        
        # Simulation: print to console
        print(f"[{priority}] {timestamp}: {message}")
        
        # In production:
        # if priority == "HIGH":
        #     send_to_pagerduty(alert)
        # elif priority == "MEDIUM":
        #     send_to_slack(alert)
        # send_to_logging_service(alert)

# Apply to alerts stream (optional - uncomment to enable)
# query_alert_notifications = (
#     alerts_stream
#     .writeStream
#     .foreachBatch(send_alert_notifications)
#     .option("checkpointLocation", f"{checkpoint_base}/alert_notifications")
#     .trigger(processingTime="5 seconds")
#     .start()
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Monitor and Query Results

# COMMAND ----------

# DBTITLE 1,Wait for Processing
import time
print("Processing streams... waiting 20 seconds")
time.sleep(20)

# COMMAND ----------

# DBTITLE 1,Query Current Positions
current_pos = spark.table(current_positions_table)
print(f"Total position records: {current_pos.count()}")
print("\nCurrent devices in geofences:")
current_pos.groupBy("geofence_name", "category").agg(
    F.countDistinct("device_id").alias("unique_devices"),
    F.count("*").alias("total_positions"),
    F.avg("speed").alias("avg_speed")
).orderBy("geofence_name").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Query Geofence Events
events = spark.table(events_table)
print(f"Total geofence events: {events.count()}")

print("\nEvents by type and geofence:")
events.groupBy("geofence_name", "event_type").count() \
    .orderBy("geofence_name", "event_type") \
    .show(truncate=False)

print("\nRecent events:")
events.orderBy(F.desc("event_timestamp")).limit(10).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Query Alerts
alerts = spark.table(alerts_table)
print(f"Total alerts generated: {alerts.count()}")

print("\nAlerts by priority:")
alerts.groupBy("alert_priority").count().orderBy("alert_priority").show()

print("\nAlerts by geofence:")
alerts.groupBy("geofence_name", "category", "event_type").count() \
    .orderBy("geofence_name") \
    .show(truncate=False)

print("\nRecent HIGH priority alerts:")
alerts.filter("alert_priority = 'HIGH'") \
    .orderBy(F.desc("event_timestamp")) \
    .select("device_id", "event_type", "geofence_name", "alert_message", "event_timestamp") \
    .limit(5) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Device Dwell Time Analysis
# Calculate how long devices stayed in each geofence
device_sessions = events.groupBy("device_id", "geofence_id", "geofence_name").agg(
    F.min(F.when(F.col("event_type") == "ENTER", F.col("event_timestamp"))).alias("entry_time"),
    F.max(F.when(F.col("event_type") == "EXIT", F.col("event_timestamp"))).alias("exit_time")
).filter("entry_time IS NOT NULL AND exit_time IS NOT NULL")

device_sessions = device_sessions.withColumn(
    "dwell_time_minutes",
    (F.unix_timestamp("exit_time") - F.unix_timestamp("entry_time")) / 60
)

print("Device dwell times:")
device_sessions.select(
    "device_id",
    "geofence_name",
    "entry_time",
    "exit_time",
    F.round("dwell_time_minutes", 2).alias("dwell_minutes")
).orderBy(F.desc("dwell_minutes")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Advanced Patterns

# COMMAND ----------

# DBTITLE 1,Alert Throttling - Prevent Alert Spam
# Deduplicate alerts within a time window to prevent spam

throttled_alerts = (
    alerts_stream
    .withWatermark("event_timestamp", "5 minutes")
    .dropDuplicates([
        "device_id",
        "geofence_id",
        "event_type"
    ], watermarkDelayThreshold="5 minutes")  # Only one alert per device/fence/type per 5 min
    .withColumn("throttle_window", F.window("event_timestamp", "5 minutes"))
)

print("Alert throttling configured")

# COMMAND ----------

# DBTITLE 1,Geofence Occupancy Real-Time Aggregation
# Count devices currently in each geofence
occupancy_stream = (
    gps_in_geofences_filtered
    .withWatermark("timestamp", "2 minutes")
    .groupBy(
        F.window("timestamp", "1 minute"),
        "geofence_id",
        "name",
        "category"
    )
    .agg(
        F.countDistinct("device_id").alias("device_count"),
        F.avg("speed").alias("avg_speed"),
        F.count("*").alias("total_points")
    )
    .select(
        "geofence_id",
        F.col("name").alias("geofence_name"),
        "category",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "device_count",
        F.round("avg_speed", 2).alias("avg_speed"),
        "total_points"
    )
)

occupancy_table = f"{catalog}.{schema_name}.geofence_occupancy"
occupancy_checkpoint = f"{checkpoint_base}/geofence_occupancy"

query_occupancy = (
    occupancy_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", occupancy_checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(occupancy_table)
)

print(f"Started occupancy query: {query_occupancy.name}")

# COMMAND ----------

# DBTITLE 1,Query Occupancy Metrics
time.sleep(15)

occupancy = spark.table(occupancy_table)
print(f"Total occupancy records: {occupancy.count()}")

print("\nCurrent geofence occupancy:")
occupancy.orderBy(F.desc("window_start")).limit(20).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Dynamic Geofence Updates

# COMMAND ----------

# DBTITLE 1,Update Geofence Definition
# Simulate updating a geofence - the stream will pick up changes automatically
from delta.tables import DeltaTable

# Update zone_002 to be inactive
delta_geofences = DeltaTable.forName(spark, geofence_table)

delta_geofences.update(
    condition="geofence_id = 'zone_002'",
    set={
        "active": "false",
        "updated_at": "current_timestamp()"
    }
)

print("Updated zone_002 to inactive")
print("\nCurrent geofence status:")
spark.table(geofence_table).select("geofence_id", "name", "active", "updated_at").show()

# Note: The stream will pick up this change in the next micro-batch
# because we're using broadcast with fresh reads each time

# COMMAND ----------

# DBTITLE 1,Add New Geofence Dynamically
new_geofence = spark.createDataFrame([{
    "geofence_id": "zone_006",
    "name": "Emergency Zone",
    "geometry_type": "CIRCLE",
    "geometry_wkt": None,
    "center_lat": 37.785,
    "center_lon": -122.410,
    "radius_meters": 300.0,
    "category": "emergency",
    "alert_on_entry": True,
    "alert_on_exit": True,
    "active": True,
    "time_restriction_start": None,
    "time_restriction_end": None,
    "created_at": datetime.now(),
    "updated_at": datetime.now()
}])

new_geofence.write.mode("append").saveAsTable(geofence_table)

print("Added new emergency geofence")

# Also add H3 index for the new geofence
new_cells = get_h3_cells_for_circle(37.785, -122.410, 300.0)
new_h3_data = [{"geofence_id": "zone_006", "h3_index": cell} for cell in new_cells]
spark.createDataFrame(new_h3_data).write.mode("append").saveAsTable(geofence_h3_table)

print(f"Added {len(new_cells)} H3 cells for new geofence")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Cleanup

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
# MAGIC 1. **Geofence Definition**
# MAGIC    - Multiple geometry types (circles, polygons)
# MAGIC    - Category-based classification
# MAGIC    - Time-based restrictions
# MAGIC    - Dynamic updates
# MAGIC
# MAGIC 2. **Spatial Indexing**
# MAGIC    - H3 pre-filtering for performance
# MAGIC    - Precise geometry testing
# MAGIC    - Broadcast optimization
# MAGIC
# MAGIC 3. **Event Detection**
# MAGIC    - Entry/exit event generation
# MAGIC    - State tracking across boundaries
# MAGIC    - Dwell time calculation
# MAGIC
# MAGIC 4. **Alert System**
# MAGIC    - Priority-based alerting
# MAGIC    - Alert throttling/deduplication
# MAGIC    - Real-time notification patterns
# MAGIC
# MAGIC 5. **Analytics**
# MAGIC    - Current position tracking
# MAGIC    - Occupancy monitoring
# MAGIC    - Dwell time analysis
# MAGIC
# MAGIC ### Performance Optimizations
# MAGIC
# MAGIC ✅ H3 indexing for fast spatial lookups  
# MAGIC ✅ Broadcast joins for small dimension tables  
# MAGIC ✅ Watermarking for state cleanup  
# MAGIC ✅ Separate checkpoints per query  
# MAGIC ✅ Appropriate trigger intervals  
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Continue to **Demo 3: Trajectory Analysis** to learn:
# MAGIC - GPS point aggregation into paths
# MAGIC - Speed and direction calculations
# MAGIC - Path smoothing algorithms
# MAGIC - Anomaly detection in trajectories
