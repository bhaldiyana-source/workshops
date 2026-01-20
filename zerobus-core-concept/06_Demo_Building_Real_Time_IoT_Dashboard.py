# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building a Real-Time IoT Dashboard with Zerobus
# MAGIC
# MAGIC ## Overview
# MAGIC In this comprehensive demo, you'll build a complete end-to-end real-time IoT monitoring application using Zerobus. This demo brings together all concepts from previous labs to create a production-ready solution with streaming ingestion, real-time analytics, automated alerting, and interactive visualization.
# MAGIC
# MAGIC ## What You'll Build
# MAGIC
# MAGIC **Smart Building Monitoring System:**
# MAGIC - Ingest real-time sensor data from multiple buildings
# MAGIC - Track temperature, humidity, pressure, and air quality
# MAGIC - Detect anomalies and equipment failures
# MAGIC - Send real-time alerts for critical conditions
# MAGIC - Visualize metrics on interactive dashboards
# MAGIC - Generate predictive maintenance recommendations
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC IoT Sensors → Zerobus HTTP Endpoint → Delta Lake (Bronze)
# MAGIC                                            ↓
# MAGIC                                    Delta Live Tables
# MAGIC                                            ↓
# MAGIC                                   Silver Layer (Validated)
# MAGIC                                            ↓
# MAGIC                                   Gold Layer (Aggregated)
# MAGIC                                            ↓
# MAGIC                          Databricks SQL Dashboard + Alerts
# MAGIC ```
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set Up the Smart Building Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for smart building demo
# MAGIC CREATE SCHEMA IF NOT EXISTS zerobus_workshop.smart_building
# MAGIC COMMENT 'Smart building IoT monitoring demo';
# MAGIC
# MAGIC USE CATALOG zerobus_workshop;
# MAGIC USE SCHEMA smart_building;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Bronze Layer Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze layer: Raw sensor telemetry
# MAGIC CREATE TABLE IF NOT EXISTS sensor_telemetry_bronze (
# MAGIC   -- Identification
# MAGIC   sensor_id STRING NOT NULL COMMENT 'Unique sensor identifier',
# MAGIC   building_id STRING NOT NULL COMMENT 'Building identifier',
# MAGIC   floor INT COMMENT 'Floor number',
# MAGIC   zone STRING COMMENT 'Zone within floor (e.g., North Wing, South Wing)',
# MAGIC   sensor_type STRING COMMENT 'Type of sensor (temperature, humidity, air_quality)',
# MAGIC   
# MAGIC   -- Timestamp
# MAGIC   event_timestamp TIMESTAMP NOT NULL COMMENT 'Sensor reading timestamp',
# MAGIC   ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'Databricks ingestion time',
# MAGIC   
# MAGIC   -- Sensor readings
# MAGIC   temperature_celsius DOUBLE COMMENT 'Temperature in Celsius',
# MAGIC   humidity_percent DOUBLE COMMENT 'Relative humidity (0-100)',
# MAGIC   pressure_hpa DOUBLE COMMENT 'Atmospheric pressure in hPa',
# MAGIC   co2_ppm INT COMMENT 'CO2 concentration in parts per million',
# MAGIC   voc_ppb INT COMMENT 'Volatile Organic Compounds in parts per billion',
# MAGIC   
# MAGIC   -- Device health
# MAGIC   battery_percent DOUBLE COMMENT 'Battery level (0-100)',
# MAGIC   signal_strength_dbm INT COMMENT 'WiFi signal strength in dBm',
# MAGIC   firmware_version STRING COMMENT 'Device firmware version',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   device_status STRING COMMENT 'Device operational status'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (building_id, DATE(event_timestamp))
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC COMMENT 'Bronze: Raw sensor telemetry from smart buildings';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Zerobus Connection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Zerobus ingest connection
# MAGIC CREATE CONNECTION IF NOT EXISTS smart_building_ingest
# MAGIC TYPE ZEROBUS_INGEST
# MAGIC OPTIONS (
# MAGIC   target_table = 'zerobus_workshop.smart_building.sensor_telemetry_bronze'
# MAGIC )
# MAGIC COMMENT 'Zerobus connection for smart building sensor data';

# COMMAND ----------

# Get endpoint URL
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
endpoint_url = f"https://{workspace_url}/api/2.0/zerobus/v1/ingest/zerobus_workshop/smart_building/smart_building_ingest"

print("SMART BUILDING INGESTION ENDPOINT")
print("=" * 80)
print(f"Endpoint URL: {endpoint_url}")
print("=" * 80)
print("\n📋 Save this URL for your IoT devices/applications")

# Store in widget
dbutils.widgets.text("endpoint_url", endpoint_url, "Endpoint URL")
dbutils.widgets.text("auth_token", "", "Auth Token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate Simulated IoT Data

# COMMAND ----------

import random
import time
import requests
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

class SmartBuildingSensorSimulator:
    """
    Simulates IoT sensors in a smart building.
    """
    
    def __init__(self, buildings: List[str], sensors_per_building: int = 20):
        self.buildings = buildings
        self.sensors = self._initialize_sensors(sensors_per_building)
        
    def _initialize_sensors(self, count_per_building):
        """Initialize sensor configuration."""
        sensors = []
        sensor_types = ['temperature', 'humidity', 'air_quality', 'combined']
        zones = ['North Wing', 'South Wing', 'East Wing', 'West Wing', 'Central']
        
        for building in self.buildings:
            for i in range(count_per_building):
                sensor = {
                    'sensor_id': f"{building}-SENSOR-{i+1:03d}",
                    'building_id': building,
                    'floor': random.randint(1, 10),
                    'zone': random.choice(zones),
                    'sensor_type': random.choice(sensor_types),
                    'firmware_version': f"2.{random.randint(0, 5)}.{random.randint(0, 10)}",
                    # Baseline readings
                    'baseline_temp': random.uniform(20, 24),
                    'baseline_humidity': random.uniform(40, 60),
                    'baseline_co2': random.randint(400, 600),
                    'battery_drain_rate': random.uniform(0.01, 0.05),  # % per hour
                    'battery_level': random.uniform(60, 100)
                }
                sensors.append(sensor)
        
        return sensors
    
    def generate_reading(self, sensor: Dict, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a single sensor reading."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        # Add realistic variations
        temp_variation = random.gauss(0, 0.5)
        humidity_variation = random.gauss(0, 2)
        
        # Simulate time-of-day effects
        hour = timestamp.hour
        if 8 <= hour <= 18:  # Business hours - higher occupancy
            co2_factor = 1.5
            temp_factor = 1.2
        else:  # Off hours
            co2_factor = 0.8
            temp_factor = 0.95
        
        # Occasionally simulate anomalies
        if random.random() < 0.02:  # 2% chance of anomaly
            temp_anomaly = random.choice([5, -5])  # Sudden temp change
        else:
            temp_anomaly = 0
        
        reading = {
            'sensor_id': sensor['sensor_id'],
            'building_id': sensor['building_id'],
            'floor': sensor['floor'],
            'zone': sensor['zone'],
            'sensor_type': sensor['sensor_type'],
            'event_timestamp': timestamp.isoformat(),
            'ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
            'temperature_celsius': round(sensor['baseline_temp'] * temp_factor + temp_variation + temp_anomaly, 2),
            'humidity_percent': round(max(0, min(100, sensor['baseline_humidity'] + humidity_variation)), 2),
            'pressure_hpa': round(random.uniform(1008, 1018), 2),
            'co2_ppm': int(sensor['baseline_co2'] * co2_factor + random.gauss(0, 50)),
            'voc_ppb': random.randint(100, 500),
            'battery_percent': round(max(0, sensor['battery_level']), 2),
            'signal_strength_dbm': random.randint(-70, -30),
            'firmware_version': sensor['firmware_version'],
            'device_status': 'active' if sensor['battery_level'] > 10 else 'low_battery'
        }
        
        # Update sensor battery
        sensor['battery_level'] -= sensor['battery_drain_rate']
        if sensor['battery_level'] < 0:
            sensor['battery_level'] = 100  # Simulate battery replacement
        
        return reading
    
    def generate_batch(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Generate a batch of sensor readings."""
        readings = []
        for _ in range(batch_size):
            sensor = random.choice(self.sensors)
            reading = self.generate_reading(sensor)
            readings.append(reading)
        return readings

# Initialize simulator
buildings = ['BUILDING-A', 'BUILDING-B', 'BUILDING-C']
simulator = SmartBuildingSensorSimulator(buildings, sensors_per_building=15)

print(f"✅ Simulator initialized")
print(f"Buildings: {len(buildings)}")
print(f"Total sensors: {len(simulator.sensors)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Send Simulated Data to Zerobus

# COMMAND ----------

# Generate and send test data
AUTH_TOKEN = dbutils.widgets.get("auth_token")
ENDPOINT_URL = dbutils.widgets.get("endpoint_url")

if not AUTH_TOKEN:
    print("⚠️  Please enter your authentication token in the widget above!")
else:
    print("Generating and sending simulated sensor data...")
    print("-" * 80)
    
    # Send 5 batches of data
    for batch_num in range(1, 6):
        batch = simulator.generate_batch(batch_size=100)
        
        headers = {
            "Authorization": f"Bearer {AUTH_TOKEN}",
            "Content-Type": "application/json"
        }
        
        try:
            start = time.time()
            response = requests.post(ENDPOINT_URL, headers=headers, json=batch, timeout=30)
            end = time.time()
            
            if response.status_code == 200:
                print(f"✅ Batch {batch_num}: {len(batch)} events sent in {end-start:.2f}s")
            else:
                print(f"❌ Batch {batch_num} failed: {response.status_code} - {response.text}")
        
        except Exception as e:
            print(f"❌ Batch {batch_num} error: {str(e)}")
        
        time.sleep(1)  # Small delay between batches
    
    print("\n✅ Data generation complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Silver Layer with Data Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver layer: Validated and enriched sensor data
# MAGIC CREATE TABLE IF NOT EXISTS sensor_telemetry_silver (
# MAGIC   -- All fields from bronze
# MAGIC   sensor_id STRING,
# MAGIC   building_id STRING,
# MAGIC   floor INT,
# MAGIC   zone STRING,
# MAGIC   sensor_type STRING,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   ingestion_timestamp TIMESTAMP,
# MAGIC   temperature_celsius DOUBLE,
# MAGIC   humidity_percent DOUBLE,
# MAGIC   pressure_hpa DOUBLE,
# MAGIC   co2_ppm INT,
# MAGIC   voc_ppb INT,
# MAGIC   battery_percent DOUBLE,
# MAGIC   signal_strength_dbm INT,
# MAGIC   firmware_version STRING,
# MAGIC   device_status STRING,
# MAGIC   
# MAGIC   -- Enriched fields
# MAGIC   temperature_fahrenheit DOUBLE COMMENT 'Temperature in Fahrenheit',
# MAGIC   event_date DATE COMMENT 'Event date for partitioning',
# MAGIC   event_hour INT COMMENT 'Hour of day (0-23)',
# MAGIC   processing_timestamp TIMESTAMP COMMENT 'When record was processed to silver',
# MAGIC   
# MAGIC   -- Data quality flags
# MAGIC   is_valid BOOLEAN COMMENT 'Passes all quality checks',
# MAGIC   quality_issues STRING COMMENT 'Comma-separated list of quality issues',
# MAGIC   
# MAGIC   -- Alert flags
# MAGIC   needs_maintenance BOOLEAN COMMENT 'Device needs maintenance',
# MAGIC   temp_alert BOOLEAN COMMENT 'Temperature outside normal range',
# MAGIC   air_quality_alert BOOLEAN COMMENT 'Poor air quality detected',
# MAGIC   battery_alert BOOLEAN COMMENT 'Low battery warning'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (building_id, event_date)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC )
# MAGIC COMMENT 'Silver: Validated and enriched sensor telemetry';

# COMMAND ----------

# Transform bronze to silver
from pyspark.sql.functions import *
from pyspark.sql.types import *

def transform_to_silver():
    """Transform bronze data to silver with quality checks."""
    
    bronze_df = spark.table("sensor_telemetry_bronze")
    
    silver_df = (
        bronze_df
        # Add derived fields
        .withColumn("temperature_fahrenheit", col("temperature_celsius") * 9/5 + 32)
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("processing_timestamp", current_timestamp())
        
        # Data quality checks
        .withColumn("quality_issues", 
            concat_ws(",",
                when(col("temperature_celsius").isNull(), lit("missing_temperature")),
                when((col("temperature_celsius") < -10) | (col("temperature_celsius") > 50), lit("invalid_temperature")),
                when((col("humidity_percent") < 0) | (col("humidity_percent") > 100), lit("invalid_humidity")),
                when(col("co2_ppm") > 5000, lit("extreme_co2")),
                when(col("battery_percent") < 0, lit("invalid_battery"))
            )
        )
        .withColumn("is_valid", col("quality_issues") == "")
        
        # Alert flags
        .withColumn("battery_alert", col("battery_percent") < 20)
        .withColumn("temp_alert", (col("temperature_celsius") < 15) | (col("temperature_celsius") > 30))
        .withColumn("air_quality_alert", (col("co2_ppm") > 1000) | (col("voc_ppb") > 400))
        .withColumn("needs_maintenance", 
            col("battery_alert") | col("temp_alert") | (col("signal_strength_dbm") < -75)
        )
    )
    
    return silver_df

# Process to silver (display sample - in production use writeStream)
silver_sample = transform_to_silver().limit(100)
display(silver_sample.select(
    "sensor_id", "building_id", "floor", "zone",
    "temperature_celsius", "humidity_percent", "co2_ppm",
    "battery_percent", "is_valid", "needs_maintenance",
    "temp_alert", "air_quality_alert", "battery_alert"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Gold Layer Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold layer: Hourly aggregated metrics per building
# MAGIC CREATE TABLE IF NOT EXISTS building_metrics_gold (
# MAGIC   building_id STRING,
# MAGIC   event_date DATE,
# MAGIC   event_hour INT,
# MAGIC   
# MAGIC   -- Volume metrics
# MAGIC   sensor_count INT COMMENT 'Number of active sensors',
# MAGIC   reading_count BIGINT COMMENT 'Total readings in hour',
# MAGIC   
# MAGIC   -- Environmental averages
# MAGIC   avg_temperature DOUBLE,
# MAGIC   min_temperature DOUBLE,
# MAGIC   max_temperature DOUBLE,
# MAGIC   avg_humidity DOUBLE,
# MAGIC   avg_co2_ppm DOUBLE,
# MAGIC   max_co2_ppm INT,
# MAGIC   avg_voc_ppb DOUBLE,
# MAGIC   
# MAGIC   -- Health metrics
# MAGIC   avg_battery_percent DOUBLE,
# MAGIC   low_battery_count INT COMMENT 'Sensors with battery < 20%',
# MAGIC   offline_sensor_count INT COMMENT 'Sensors not reporting',
# MAGIC   
# MAGIC   -- Alert summary
# MAGIC   temp_alert_count INT,
# MAGIC   air_quality_alert_count INT,
# MAGIC   maintenance_needed_count INT,
# MAGIC   
# MAGIC   -- Aggregation metadata
# MAGIC   aggregation_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (building_id, event_date)
# MAGIC COMMENT 'Gold: Hourly building-level metrics';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Real-Time Analytics Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current building status (last 5 minutes)
# MAGIC SELECT 
# MAGIC   building_id,
# MAGIC   COUNT(DISTINCT sensor_id) as active_sensors,
# MAGIC   COUNT(*) as recent_readings,
# MAGIC   ROUND(AVG(temperature_celsius), 2) as avg_temp_c,
# MAGIC   ROUND(AVG(humidity_percent), 2) as avg_humidity,
# MAGIC   ROUND(AVG(co2_ppm), 0) as avg_co2,
# MAGIC   SUM(CASE WHEN battery_percent < 20 THEN 1 ELSE 0 END) as low_battery_sensors,
# MAGIC   MAX(event_timestamp) as last_reading
# MAGIC FROM sensor_telemetry_bronze
# MAGIC WHERE event_timestamp >= current_timestamp() - INTERVAL 5 MINUTES
# MAGIC GROUP BY building_id
# MAGIC ORDER BY building_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temperature distribution by floor (last hour)
# MAGIC SELECT 
# MAGIC   building_id,
# MAGIC   floor,
# MAGIC   COUNT(DISTINCT sensor_id) as sensors,
# MAGIC   ROUND(AVG(temperature_celsius), 2) as avg_temp,
# MAGIC   ROUND(MIN(temperature_celsius), 2) as min_temp,
# MAGIC   ROUND(MAX(temperature_celsius), 2) as max_temp,
# MAGIC   ROUND(STDDEV(temperature_celsius), 2) as temp_stddev
# MAGIC FROM sensor_telemetry_bronze
# MAGIC WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY building_id, floor
# MAGIC ORDER BY building_id, floor;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Air quality monitoring (CO2 levels)
# MAGIC SELECT 
# MAGIC   building_id,
# MAGIC   zone,
# MAGIC   COUNT(*) as readings,
# MAGIC   ROUND(AVG(co2_ppm), 0) as avg_co2,
# MAGIC   MAX(co2_ppm) as max_co2,
# MAGIC   SUM(CASE WHEN co2_ppm > 1000 THEN 1 ELSE 0 END) as high_co2_count,
# MAGIC   CASE 
# MAGIC     WHEN AVG(co2_ppm) < 800 THEN '🟢 Excellent'
# MAGIC     WHEN AVG(co2_ppm) < 1000 THEN '🟡 Good'
# MAGIC     WHEN AVG(co2_ppm) < 1500 THEN '🟠 Fair'
# MAGIC     ELSE '🔴 Poor'
# MAGIC   END as air_quality_status
# MAGIC FROM sensor_telemetry_bronze
# MAGIC WHERE event_timestamp >= current_timestamp() - INTERVAL 15 MINUTES
# MAGIC GROUP BY building_id, zone
# MAGIC ORDER BY avg_co2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sensors needing attention
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   building_id,
# MAGIC   floor,
# MAGIC   zone,
# MAGIC   MAX(event_timestamp) as last_reading,
# MAGIC   ROUND((UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(event_timestamp))) / 60, 2) as minutes_ago,
# MAGIC   ROUND(AVG(battery_percent), 2) as avg_battery,
# MAGIC   ROUND(AVG(temperature_celsius), 2) as avg_temp,
# MAGIC   ROUND(AVG(co2_ppm), 0) as avg_co2,
# MAGIC   CASE 
# MAGIC     WHEN MAX(event_timestamp) < current_timestamp() - INTERVAL 15 MINUTES THEN '🔴 OFFLINE'
# MAGIC     WHEN AVG(battery_percent) < 10 THEN '🔴 CRITICAL BATTERY'
# MAGIC     WHEN AVG(battery_percent) < 20 THEN '🟠 LOW BATTERY'
# MAGIC     WHEN AVG(co2_ppm) > 1500 THEN '🟠 POOR AIR QUALITY'
# MAGIC     ELSE '🟢 OK'
# MAGIC   END as status
# MAGIC FROM sensor_telemetry_bronze
# MAGIC WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY sensor_id, building_id, floor, zone
# MAGIC HAVING status != '🟢 OK'
# MAGIC ORDER BY 
# MAGIC   CASE status 
# MAGIC     WHEN '🔴 OFFLINE' THEN 1 
# MAGIC     WHEN '🔴 CRITICAL BATTERY' THEN 2 
# MAGIC     WHEN '🟠 LOW BATTERY' THEN 3 
# MAGIC     WHEN '🟠 POOR AIR QUALITY' THEN 4 
# MAGIC   END,
# MAGIC   minutes_ago DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Dashboard Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard view: Real-time building overview
# MAGIC CREATE OR REPLACE VIEW v_building_overview AS
# MAGIC SELECT 
# MAGIC   building_id,
# MAGIC   COUNT(DISTINCT sensor_id) as total_sensors,
# MAGIC   COUNT(*) as readings_last_hour,
# MAGIC   ROUND(AVG(temperature_celsius), 1) as avg_temperature,
# MAGIC   ROUND(AVG(humidity_percent), 1) as avg_humidity,
# MAGIC   ROUND(AVG(co2_ppm), 0) as avg_co2,
# MAGIC   MAX(event_timestamp) as last_update,
# MAGIC   SUM(CASE WHEN battery_percent < 20 THEN 1 ELSE 0 END) as sensors_low_battery,
# MAGIC   SUM(CASE WHEN co2_ppm > 1000 THEN 1 ELSE 0 END) as zones_poor_air_quality,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(DISTINCT sensor_id) < 10 THEN '🟠 Some sensors offline'
# MAGIC     WHEN AVG(co2_ppm) > 1200 THEN '🟠 Air quality needs attention'
# MAGIC     ELSE '🟢 All systems normal'
# MAGIC   END as building_status
# MAGIC FROM sensor_telemetry_bronze
# MAGIC WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY building_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard view: Environmental trends
# MAGIC CREATE OR REPLACE VIEW v_environmental_trends AS
# MAGIC SELECT 
# MAGIC   building_id,
# MAGIC   DATE_TRUNC('minute', event_timestamp) as minute,
# MAGIC   ROUND(AVG(temperature_celsius), 2) as avg_temp,
# MAGIC   ROUND(AVG(humidity_percent), 2) as avg_humidity,
# MAGIC   ROUND(AVG(co2_ppm), 0) as avg_co2,
# MAGIC   COUNT(DISTINCT sensor_id) as sensor_count
# MAGIC FROM sensor_telemetry_bronze
# MAGIC WHERE event_timestamp >= current_timestamp() - INTERVAL 2 HOURS
# MAGIC GROUP BY building_id, DATE_TRUNC('minute', event_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test dashboard views
# MAGIC SELECT * FROM v_building_overview;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Environmental trends (last 30 minutes)
# MAGIC SELECT 
# MAGIC   building_id,
# MAGIC   minute,
# MAGIC   avg_temp,
# MAGIC   avg_humidity,
# MAGIC   avg_co2
# MAGIC FROM v_environmental_trends
# MAGIC WHERE minute >= current_timestamp() - INTERVAL 30 MINUTES
# MAGIC ORDER BY building_id, minute DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Implement Automated Alerts

# COMMAND ----------

def check_alerts():
    """
    Check for alert conditions and return alerts.
    """
    from datetime import datetime
    
    # Critical: High CO2 levels
    high_co2 = spark.sql("""
        SELECT building_id, zone, AVG(co2_ppm) as avg_co2, COUNT(*) as reading_count
        FROM sensor_telemetry_bronze
        WHERE event_timestamp >= current_timestamp() - INTERVAL 10 MINUTES
        GROUP BY building_id, zone
        HAVING AVG(co2_ppm) > 1500
    """).collect()
    
    # Warning: Low battery devices
    low_battery = spark.sql("""
        SELECT sensor_id, building_id, AVG(battery_percent) as avg_battery
        FROM sensor_telemetry_bronze
        WHERE event_timestamp >= current_timestamp() - INTERVAL 30 MINUTES
        GROUP BY sensor_id, building_id
        HAVING AVG(battery_percent) < 15
    """).collect()
    
    # Warning: Offline sensors
    offline = spark.sql("""
        SELECT sensor_id, building_id, MAX(event_timestamp) as last_seen
        FROM sensor_telemetry_bronze
        WHERE sensor_id IN (
            SELECT DISTINCT sensor_id 
            FROM sensor_telemetry_bronze 
            WHERE event_timestamp >= current_timestamp() - INTERVAL 2 HOURS
        )
        GROUP BY sensor_id, building_id
        HAVING MAX(event_timestamp) < current_timestamp() - INTERVAL 20 MINUTES
    """).collect()
    
    # Compile alerts
    alerts = []
    
    for row in high_co2:
        alerts.append({
            "severity": "CRITICAL",
            "type": "AIR_QUALITY",
            "building": row['building_id'],
            "zone": row['zone'],
            "message": f"High CO2 levels detected: {row['avg_co2']:.0f} ppm",
            "value": row['avg_co2'],
            "threshold": 1500
        })
    
    for row in low_battery:
        alerts.append({
            "severity": "WARNING",
            "type": "BATTERY",
            "sensor": row['sensor_id'],
            "building": row['building_id'],
            "message": f"Low battery: {row['avg_battery']:.1f}%",
            "value": row['avg_battery'],
            "threshold": 15
        })
    
    for row in offline:
        minutes_offline = (datetime.now() - row['last_seen']).total_seconds() / 60
        alerts.append({
            "severity": "WARNING",
            "type": "OFFLINE",
            "sensor": row['sensor_id'],
            "building": row['building_id'],
            "message": f"Sensor offline for {minutes_offline:.0f} minutes",
            "last_seen": row['last_seen'].isoformat()
        })
    
    return {
        "timestamp": datetime.now().isoformat(),
        "total_alerts": len(alerts),
        "critical_count": len([a for a in alerts if a['severity'] == 'CRITICAL']),
        "warning_count": len([a for a in alerts if a['severity'] == 'WARNING']),
        "alerts": alerts
    }

# Run alert check
alert_report = check_alerts()

print("SMART BUILDING ALERT REPORT")
print("=" * 80)
print(f"Timestamp: {alert_report['timestamp']}")
print(f"Total Alerts: {alert_report['total_alerts']}")
print(f"  Critical: {alert_report['critical_count']}")
print(f"  Warning: {alert_report['warning_count']}")

if alert_report['alerts']:
    print("\n🚨 ACTIVE ALERTS:")
    for alert in alert_report['alerts']:
        print(f"\n[{alert['severity']}] {alert['type']}")
        print(f"  {alert['message']}")
        for key, value in alert.items():
            if key not in ['severity', 'type', 'message']:
                print(f"    {key}: {value}")
else:
    print("\n✅ No active alerts - All systems operating normally")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Create Databricks SQL Dashboard
# MAGIC
# MAGIC ### Dashboard Components
# MAGIC
# MAGIC **Create these visualizations in Databricks SQL:**
# MAGIC
# MAGIC 1. **Building Status Card**
# MAGIC    - Query: `SELECT * FROM v_building_overview`
# MAGIC    - Visualization: Counter (show total sensors, avg temp, status)
# MAGIC
# MAGIC 2. **Temperature Trend**
# MAGIC    - Query: `SELECT * FROM v_environmental_trends WHERE minute >= current_timestamp() - INTERVAL 1 HOUR`
# MAGIC    - Visualization: Line chart (time on X, temperature on Y, color by building)
# MAGIC
# MAGIC 3. **CO2 Levels by Zone**
# MAGIC    - Query from Step 8 (air quality monitoring)
# MAGIC    - Visualization: Bar chart or heat map
# MAGIC
# MAGIC 4. **Sensors Needing Attention**
# MAGIC    - Query from Step 8 (sensors needing attention)
# MAGIC    - Visualization: Table with status colors
# MAGIC
# MAGIC 5. **Alert Summary**
# MAGIC    - Custom Python query using `check_alerts()` function
# MAGIC    - Visualization: Table or counter
# MAGIC
# MAGIC ### Auto-Refresh
# MAGIC - Set dashboard to refresh every 30-60 seconds for near-real-time monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Production Considerations
# MAGIC
# MAGIC ### What We Built
# MAGIC
# MAGIC ✅ Complete smart building IoT monitoring system  
# MAGIC ✅ Real-time sensor data ingestion via Zerobus  
# MAGIC ✅ Multi-layer data architecture (Bronze → Silver → Gold)  
# MAGIC ✅ Data quality validation and enrichment  
# MAGIC ✅ Real-time analytics and aggregations  
# MAGIC ✅ Automated alert detection  
# MAGIC ✅ Interactive dashboard views  
# MAGIC
# MAGIC ### Production Deployment Checklist
# MAGIC
# MAGIC **Infrastructure:**
# MAGIC - [ ] Use service principals for authentication (not PAT tokens)
# MAGIC - [ ] Store credentials in secret management system
# MAGIC - [ ] Configure network security (VPN, PrivateLink)
# MAGIC - [ ] Set up monitoring and logging
# MAGIC - [ ] Implement backup and disaster recovery
# MAGIC
# MAGIC **Data Pipeline:**
# MAGIC - [ ] Create Delta Live Tables pipeline for bronze→silver→gold
# MAGIC - [ ] Set up scheduled table optimization (OPTIMIZE, VACUUM)
# MAGIC - [ ] Implement data retention policies
# MAGIC - [ ] Configure change data feed for downstream systems
# MAGIC - [ ] Set up data quality monitoring
# MAGIC
# MAGIC **Alerting:**
# MAGIC - [ ] Integrate with notification system (email, Slack, PagerDuty)
# MAGIC - [ ] Define escalation procedures
# MAGIC - [ ] Create runbook for common issues
# MAGIC - [ ] Set up on-call rotation
# MAGIC
# MAGIC **Dashboard:**
# MAGIC - [ ] Deploy Databricks SQL dashboard
# MAGIC - [ ] Configure appropriate refresh intervals
# MAGIC - [ ] Set up user access controls
# MAGIC - [ ] Create mobile-friendly views
# MAGIC - [ ] Export key metrics to external BI tools
# MAGIC
# MAGIC **Cost Optimization:**
# MAGIC - [ ] Use serverless SQL warehouses
# MAGIC - [ ] Implement auto-pause for unused resources
# MAGIC - [ ] Optimize batch sizes for ingestion
# MAGIC - [ ] Monitor and control data storage growth
# MAGIC - [ ] Review and optimize query patterns
# MAGIC
# MAGIC ### Key Metrics to Track
# MAGIC
# MAGIC **Operational:**
# MAGIC - Events ingested per minute/hour
# MAGIC - End-to-end latency (sensor → dashboard)
# MAGIC - Query performance (p95, p99 response times)
# MAGIC - System uptime and availability
# MAGIC
# MAGIC **Business:**
# MAGIC - Active sensors per building
# MAGIC - Environmental compliance (temp, humidity, CO2)
# MAGIC - Equipment failure rate
# MAGIC - Energy efficiency trends
# MAGIC - Maintenance cost reduction
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC **Extend the Solution:**
# MAGIC 1. Add predictive maintenance with ML models
# MAGIC 2. Integrate with building management systems
# MAGIC 3. Implement anomaly detection algorithms
# MAGIC 4. Create mobile app for facility managers
# MAGIC 5. Add energy consumption analytics
# MAGIC 6. Implement automated HVAC optimization
# MAGIC
# MAGIC **Scale the Platform:**
# MAGIC 1. Add more buildings and sensors
# MAGIC 2. Implement edge computing for preprocessing
# MAGIC 3. Add real-time streaming dashboards
# MAGIC 4. Create tenant-specific views (multi-tenancy)
# MAGIC 5. Add historical trend analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the Databricks Zerobus Streaming Ingestion Workshop!
# MAGIC
# MAGIC You now have the skills to:
# MAGIC - Build production-ready streaming ingestion pipelines
# MAGIC - Handle schema evolution and data quality
# MAGIC - Monitor and troubleshoot real-time systems
# MAGIC - Create end-to-end IoT applications
# MAGIC - Deploy scalable, cost-effective solutions
# MAGIC
# MAGIC **Ready to eliminate message broker complexity in your organization?** 🚀
