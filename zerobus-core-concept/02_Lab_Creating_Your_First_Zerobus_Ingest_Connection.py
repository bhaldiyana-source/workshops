# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Creating Your First Zerobus Ingest Connection
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll create your first Zerobus Ingest connection in Unity Catalog, configure a target Delta table for streaming ingestion, generate authentication credentials, and test your endpoint with sample data. By the end, you'll have a fully functional streaming ingestion pipeline ready to receive events.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Create a Zerobus Ingest connection in Unity Catalog with proper permissions
# MAGIC - Design and create a target Delta table schema for event ingestion
# MAGIC - Generate and securely manage authentication tokens (PAT or service principal)
# MAGIC - Obtain and construct the Zerobus HTTP endpoint URL
# MAGIC - Test the connection using cURL and Python with sample JSON payloads
# MAGIC - Validate successful ingestion with SQL queries
# MAGIC - Troubleshoot common connection and authentication issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with Lakeflow Connect and Zerobus Ingest enabled
# MAGIC - Serverless compute or all-purpose cluster (DBR 14.3+)
# MAGIC - CREATE CONNECTION and CREATE TABLE permissions in Unity Catalog
# MAGIC - Access to generate Personal Access Tokens
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Environment Setup
# MAGIC
# MAGIC First, let's set up our catalog and schema for this lab. We'll create a dedicated schema to organize our Zerobus tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog for the workshop (if it doesn't exist)
# MAGIC -- Replace 'your_catalog' with your actual catalog name
# MAGIC CREATE CATALOG IF NOT EXISTS zerobus_workshop;
# MAGIC
# MAGIC -- Use the catalog
# MAGIC USE CATALOG zerobus_workshop;
# MAGIC
# MAGIC -- Create a schema for Zerobus ingestion
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_ingestion
# MAGIC   COMMENT 'Schema for Zerobus streaming ingestion lab';
# MAGIC
# MAGIC -- Use the schema
# MAGIC USE SCHEMA streaming_ingestion;
# MAGIC
# MAGIC -- Verify setup
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Target Delta Table
# MAGIC
# MAGIC Before creating the Zerobus connection, we need to define where the streaming data will land. Let's create a target Delta table with a schema appropriate for IoT sensor data.
# MAGIC
# MAGIC ### Table Schema Design
# MAGIC
# MAGIC Our table will capture:
# MAGIC - **sensor_id**: Identifier for the device/sensor
# MAGIC - **timestamp**: Event timestamp (ISO 8601 format)
# MAGIC - **temperature**: Temperature reading in Celsius
# MAGIC - **humidity**: Humidity percentage
# MAGIC - **pressure**: Atmospheric pressure in hPa
# MAGIC - **location**: Physical location of the sensor
# MAGIC - **ingestion_time**: When data arrived in Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the target Delta table for sensor events
# MAGIC CREATE TABLE IF NOT EXISTS sensor_events (
# MAGIC   sensor_id STRING NOT NULL COMMENT 'Unique sensor identifier',
# MAGIC   timestamp TIMESTAMP NOT NULL COMMENT 'Event timestamp from sensor',
# MAGIC   temperature DOUBLE COMMENT 'Temperature in Celsius',
# MAGIC   humidity DOUBLE COMMENT 'Humidity percentage (0-100)',
# MAGIC   pressure DOUBLE COMMENT 'Atmospheric pressure in hPa',
# MAGIC   location STRING COMMENT 'Physical location of sensor',
# MAGIC   ingestion_time TIMESTAMP COMMENT 'Databricks ingestion timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(timestamp))
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC COMMENT 'IoT sensor events ingested via Zerobus';
# MAGIC
# MAGIC -- Describe the table
# MAGIC DESCRIBE EXTENDED sensor_events;

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Table Properties:**
# MAGIC
# MAGIC - **Partitioning by date**: Improves query performance for time-based filtering
# MAGIC - **Change Data Feed**: Enables downstream CDC tracking
# MAGIC - **Auto Optimize**: Automatic file compaction for better performance
# MAGIC - **NOT NULL constraints**: Ensures data quality at ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Zerobus Ingest Connection
# MAGIC
# MAGIC Now we'll create the Zerobus Ingest connection in Unity Catalog. This connection defines the ingestion endpoint and links it to our target table.
# MAGIC
# MAGIC ### Connection Types
# MAGIC
# MAGIC Databricks supports multiple connection types for Zerobus:
# MAGIC - **HTTP**: Standard REST API endpoint
# MAGIC - **HTTPS**: Encrypted connection (recommended for production)
# MAGIC
# MAGIC ### Important Notes
# MAGIC - The connection name must be unique within the catalog
# MAGIC - You need CREATE CONNECTION privilege
# MAGIC - The connection is governed by Unity Catalog permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Zerobus Ingest connection
# MAGIC CREATE CONNECTION IF NOT EXISTS sensor_ingest_connection
# MAGIC TYPE ZEROBUS_INGEST
# MAGIC OPTIONS (
# MAGIC   target_table = 'zerobus_workshop.streaming_ingestion.sensor_events'
# MAGIC )
# MAGIC COMMENT 'Zerobus connection for IoT sensor data ingestion';

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️ Note:** If you receive a permission error, ensure you have:
# MAGIC 1. CREATE CONNECTION privilege on the catalog
# MAGIC 2. INSERT privilege on the target table
# MAGIC 3. Zerobus Ingest feature enabled in your workspace

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the connection was created
# MAGIC SHOW CONNECTIONS IN CATALOG zerobus_workshop;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get detailed information about the connection
# MAGIC DESCRIBE CONNECTION sensor_ingest_connection;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Obtain Endpoint URL
# MAGIC
# MAGIC The Zerobus endpoint URL follows this pattern:
# MAGIC
# MAGIC ```
# MAGIC https://<workspace-url>/api/2.0/zerobus/v1/ingest/<catalog>/<schema>/<connection-name>
# MAGIC ```
# MAGIC
# MAGIC Let's construct the endpoint URL for our connection.

# COMMAND ----------

# Python code to construct endpoint URL
import json

# Get workspace URL from Spark context
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

# Connection details
catalog = "zerobus_workshop"
schema = "streaming_ingestion"
connection_name = "sensor_ingest_connection"

# Construct endpoint URL
endpoint_url = f"https://{workspace_url}/api/2.0/zerobus/v1/ingest/{catalog}/{schema}/{connection_name}"

print("=" * 80)
print("ZEROBUS ENDPOINT INFORMATION")
print("=" * 80)
print(f"Workspace URL: {workspace_url}")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Connection: {connection_name}")
print(f"\nEndpoint URL:")
print(endpoint_url)
print("=" * 80)

# Store for later use
dbutils.widgets.text("endpoint_url", endpoint_url, "Endpoint URL")

# COMMAND ----------

# MAGIC %md
# MAGIC **📝 Action Required:** Copy the endpoint URL above - you'll need it for testing!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Authentication Token
# MAGIC
# MAGIC To send data to the Zerobus endpoint, you need an authentication token. We'll use a Personal Access Token (PAT) for this lab.
# MAGIC
# MAGIC ### Generating a PAT Token
# MAGIC
# MAGIC **Manual Steps (perform in Databricks UI):**
# MAGIC
# MAGIC 1. Click on your profile icon in the top right
# MAGIC 2. Select **Settings**
# MAGIC 3. Navigate to **Developer** → **Access tokens**
# MAGIC 4. Click **Generate new token**
# MAGIC 5. Set token name: `Zerobus Lab Token`
# MAGIC 6. Set lifetime: 90 days (or as per your organization's policy)
# MAGIC 7. Click **Generate**
# MAGIC 8. **Copy the token immediately** (you won't be able to see it again!)
# MAGIC
# MAGIC ### Token Security Best Practices
# MAGIC
# MAGIC ✅ **DO:**
# MAGIC - Store tokens in secret management systems (Databricks Secrets, Azure Key Vault, AWS Secrets Manager)
# MAGIC - Use service principals for production applications
# MAGIC - Set appropriate token expiration periods
# MAGIC - Rotate tokens regularly
# MAGIC
# MAGIC ❌ **DON'T:**
# MAGIC - Hardcode tokens in source code
# MAGIC - Share tokens via email or chat
# MAGIC - Commit tokens to version control
# MAGIC - Use overly permissive tokens

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️ Important:** For this lab, we'll assume you have generated a PAT token. In production, use Databricks Secrets:
# MAGIC
# MAGIC ```python
# MAGIC # Production approach
# MAGIC token = dbutils.secrets.get(scope="my_scope", key="zerobus_token")
# MAGIC ```

# COMMAND ----------

# For lab purposes, create a text widget to input your token
# DO NOT use this approach in production!
dbutils.widgets.text("auth_token", "", "Your PAT Token (for lab only)")

print("⚠️  SECURITY WARNING ⚠️")
print("=" * 80)
print("For this lab, you can paste your PAT token in the widget above.")
print("In production environments, ALWAYS use Databricks Secrets!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test Connection with cURL
# MAGIC
# MAGIC Let's test our Zerobus endpoint using cURL commands. This validates that:
# MAGIC - The endpoint URL is correct
# MAGIC - Authentication is working
# MAGIC - The target table is accessible
# MAGIC - JSON payload is properly formatted

# COMMAND ----------

# Generate cURL command for testing
auth_token = dbutils.widgets.get("auth_token")
endpoint_url = dbutils.widgets.get("endpoint_url")

if not auth_token:
    print("⚠️  Please enter your PAT token in the 'Your PAT Token' widget above!")
else:
    # Sample event payload
    sample_event = {
        "sensor_id": "SENSOR-001",
        "timestamp": "2026-01-10T10:00:00.000Z",
        "temperature": 22.5,
        "humidity": 45.0,
        "pressure": 1013.25,
        "location": "Building A - Floor 1",
        "ingestion_time": "2026-01-10T10:00:01.000Z"
    }
    
    curl_command = f"""
curl -X POST '{endpoint_url}' \\
  -H 'Authorization: Bearer {auth_token[:10]}...[REDACTED]' \\
  -H 'Content-Type: application/json' \\
  -d '{json.dumps([sample_event], indent=2)}'
"""
    
    print("CURL COMMAND FOR TESTING")
    print("=" * 80)
    print(curl_command)
    print("=" * 80)
    print("\n📋 Copy this command and run it in your terminal to test the endpoint")
    print("   (Replace [REDACTED] with your actual token)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected Responses:**
# MAGIC
# MAGIC **✅ Success (200 OK):**
# MAGIC ```json
# MAGIC {
# MAGIC   "status": "success",
# MAGIC   "events_accepted": 1,
# MAGIC   "ingestion_id": "abc-123-xyz"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **❌ Authentication Error (401):**
# MAGIC ```json
# MAGIC {
# MAGIC   "error": "Unauthorized",
# MAGIC   "message": "Invalid or expired token"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **❌ Bad Request (400):**
# MAGIC ```json
# MAGIC {
# MAGIC   "error": "Bad Request",
# MAGIC   "message": "Invalid JSON payload or schema mismatch"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test Connection with Python
# MAGIC
# MAGIC Now let's send test events using Python's `requests` library. This is the most common approach for production applications.

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone

# Get credentials from widgets
auth_token = dbutils.widgets.get("auth_token")
endpoint_url = dbutils.widgets.get("endpoint_url")

if not auth_token:
    print("⚠️  Please enter your PAT token in the widget above!")
    dbutils.notebook.exit("Missing authentication token")

# Prepare headers
headers = {
    "Authorization": f"Bearer {auth_token}",
    "Content-Type": "application/json"
}

# Create sample events
test_events = [
    {
        "sensor_id": "SENSOR-001",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": 22.5,
        "humidity": 45.0,
        "pressure": 1013.25,
        "location": "Building A - Floor 1",
        "ingestion_time": datetime.now(timezone.utc).isoformat()
    },
    {
        "sensor_id": "SENSOR-002",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": 23.1,
        "humidity": 42.5,
        "pressure": 1012.80,
        "location": "Building A - Floor 2",
        "ingestion_time": datetime.now(timezone.utc).isoformat()
    },
    {
        "sensor_id": "SENSOR-003",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": 21.8,
        "humidity": 48.2,
        "pressure": 1013.50,
        "location": "Building B - Floor 1",
        "ingestion_time": datetime.now(timezone.utc).isoformat()
    }
]

print("Sending test events to Zerobus endpoint...")
print(f"Endpoint: {endpoint_url}")
print(f"Number of events: {len(test_events)}")
print("-" * 80)

try:
    # Send POST request
    response = requests.post(
        endpoint_url,
        headers=headers,
        json=test_events,
        timeout=30
    )
    
    # Check response
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("\n✅ SUCCESS! Events ingested successfully.")
    else:
        print(f"\n❌ ERROR: {response.status_code}")
        print(f"Message: {response.text}")
        
except requests.exceptions.RequestException as e:
    print(f"\n❌ Request failed: {str(e)}")
except Exception as e:
    print(f"\n❌ Unexpected error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Validate Data Ingestion
# MAGIC
# MAGIC Let's verify that our test events were successfully written to the Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the latest ingested events
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   timestamp,
# MAGIC   temperature,
# MAGIC   humidity,
# MAGIC   pressure,
# MAGIC   location,
# MAGIC   ingestion_time
# MAGIC FROM sensor_events
# MAGIC ORDER BY ingestion_time DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count total events by sensor
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   COUNT(*) as event_count,
# MAGIC   MIN(timestamp) as first_event,
# MAGIC   MAX(timestamp) as last_event
# MAGIC FROM sensor_events
# MAGIC GROUP BY sensor_id
# MAGIC ORDER BY sensor_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table statistics
# MAGIC SELECT
# MAGIC   COUNT(*) as total_events,
# MAGIC   COUNT(DISTINCT sensor_id) as unique_sensors,
# MAGIC   MIN(timestamp) as earliest_timestamp,
# MAGIC   MAX(timestamp) as latest_timestamp,
# MAGIC   AVG(temperature) as avg_temperature,
# MAGIC   AVG(humidity) as avg_humidity
# MAGIC FROM sensor_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Send Batch Events
# MAGIC
# MAGIC In production, you'll typically send events in batches for better performance. Let's simulate a batch ingestion.

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone
import time
import random

# Get credentials
auth_token = dbutils.widgets.get("auth_token")
endpoint_url = dbutils.widgets.get("endpoint_url")

if not auth_token:
    print("⚠️  Please enter your PAT token in the widget above!")
    dbutils.notebook.exit("Missing authentication token")

# Function to generate random sensor data
def generate_sensor_event(sensor_id, location):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(18.0, 28.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "pressure": round(random.uniform(1000.0, 1020.0), 2),
        "location": location,
        "ingestion_time": datetime.now(timezone.utc).isoformat()
    }

# Sensor configuration
sensors = [
    ("SENSOR-001", "Building A - Floor 1"),
    ("SENSOR-002", "Building A - Floor 2"),
    ("SENSOR-003", "Building B - Floor 1"),
    ("SENSOR-004", "Building B - Floor 2"),
    ("SENSOR-005", "Building C - Floor 1")
]

# Generate batch of 50 events
batch_size = 50
events_batch = []

for i in range(batch_size):
    sensor_id, location = random.choice(sensors)
    event = generate_sensor_event(sensor_id, location)
    events_batch.append(event)
    time.sleep(0.01)  # Small delay to vary timestamps

print(f"Generated {len(events_batch)} events")
print(f"Sending to: {endpoint_url}")
print("-" * 80)

# Send batch
try:
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    start_time = time.time()
    response = requests.post(
        endpoint_url,
        headers=headers,
        json=events_batch,
        timeout=30
    )
    end_time = time.time()
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print(f"Duration: {(end_time - start_time):.2f} seconds")
    print(f"Throughput: {len(events_batch) / (end_time - start_time):.0f} events/second")
    
    if response.status_code == 200:
        print("\n✅ Batch ingestion successful!")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Verify Batch Ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check recent ingestion statistics
# MAGIC SELECT 
# MAGIC   DATE(ingestion_time) as ingestion_date,
# MAGIC   HOUR(ingestion_time) as ingestion_hour,
# MAGIC   COUNT(*) as event_count,
# MAGIC   COUNT(DISTINCT sensor_id) as unique_sensors
# MAGIC FROM sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY DATE(ingestion_time), HOUR(ingestion_time)
# MAGIC ORDER BY ingestion_date DESC, ingestion_hour DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualize temperature distribution
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   AVG(temperature) as avg_temp,
# MAGIC   MIN(temperature) as min_temp,
# MAGIC   MAX(temperature) as max_temp,
# MAGIC   COUNT(*) as reading_count
# MAGIC FROM sensor_events
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY sensor_id
# MAGIC ORDER BY sensor_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Guide
# MAGIC
# MAGIC ### Common Issues and Solutions
# MAGIC
# MAGIC **1. 401 Unauthorized Error**
# MAGIC - **Cause**: Invalid or expired PAT token
# MAGIC - **Solution**: Generate a new token and update your configuration
# MAGIC - **Check**: Token has not expired and has proper permissions
# MAGIC
# MAGIC **2. 400 Bad Request - Schema Mismatch**
# MAGIC - **Cause**: JSON payload doesn't match target table schema
# MAGIC - **Solution**: Verify field names and data types match exactly
# MAGIC - **Check**: Required fields (NOT NULL) are present
# MAGIC
# MAGIC **3. 403 Forbidden**
# MAGIC - **Cause**: Token lacks INSERT permission on target table
# MAGIC - **Solution**: Grant INSERT privilege on the table
# MAGIC - **Check**: `GRANT INSERT ON TABLE sensor_events TO user`
# MAGIC
# MAGIC **4. 404 Not Found**
# MAGIC - **Cause**: Endpoint URL is incorrect
# MAGIC - **Solution**: Verify catalog/schema/connection names
# MAGIC - **Check**: Connection exists with `SHOW CONNECTIONS`
# MAGIC
# MAGIC **5. Network Timeout**
# MAGIC - **Cause**: Network connectivity issues or firewall blocking
# MAGIC - **Solution**: Check network configuration and firewall rules
# MAGIC - **Check**: Can reach workspace URL from your client
# MAGIC
# MAGIC **6. No Data Appearing in Table**
# MAGIC - **Cause**: Events sent but not visible in queries
# MAGIC - **Solution**: Wait 5-10 seconds for micro-batch processing
# MAGIC - **Check**: Query table again after brief delay

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if connection is properly configured
# MAGIC DESCRIBE CONNECTION sensor_ingest_connection;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify table properties
# MAGIC SHOW TBLPROPERTIES sensor_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table history (Delta time travel)
# MAGIC DESCRIBE HISTORY sensor_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ Created a Zerobus Ingest connection in Unity Catalog  
# MAGIC ✅ Designed and created a partitioned Delta table for sensor events  
# MAGIC ✅ Generated authentication credentials (PAT token)  
# MAGIC ✅ Obtained the Zerobus HTTP endpoint URL  
# MAGIC ✅ Tested the connection with cURL and Python  
# MAGIC ✅ Successfully ingested single and batch events  
# MAGIC ✅ Validated data arrival with SQL queries  
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Simple Setup**: Creating a Zerobus connection requires just a few SQL commands
# MAGIC 2. **Standard HTTP**: No special protocols or libraries needed
# MAGIC 3. **Immediate Access**: Data is queryable within seconds of ingestion
# MAGIC 4. **Unity Catalog Governance**: Full security and permission management
# MAGIC 5. **Batch Performance**: Batching events significantly improves throughput
# MAGIC
# MAGIC ### Configuration to Save
# MAGIC
# MAGIC Store these values for use in subsequent labs:
# MAGIC - **Endpoint URL**: `{endpoint_url}`
# MAGIC - **Catalog**: zerobus_workshop
# MAGIC - **Schema**: streaming_ingestion
# MAGIC - **Table**: sensor_events
# MAGIC - **Connection**: sensor_ingest_connection
# MAGIC
# MAGIC ### Next Lab
# MAGIC
# MAGIC Proceed to **Lab 3: Streaming Event Data with HTTP POST** to:
# MAGIC - Explore different event payload formats
# MAGIC - Implement production-grade error handling
# MAGIC - Optimize batch sizing and throughput
# MAGIC - Handle authentication and retries
# MAGIC - Build a complete event streaming application

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up (Optional)
# MAGIC
# MAGIC If you want to start fresh, uncomment and run the following commands:

# COMMAND ----------

# -- Uncomment to clean up
# DROP CONNECTION IF EXISTS sensor_ingest_connection;
# DROP TABLE IF EXISTS sensor_events;
# DROP SCHEMA IF EXISTS streaming_ingestion CASCADE;
# DROP CATALOG IF EXISTS zerobus_workshop CASCADE;
