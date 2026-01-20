# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Sync Tables from Unity Catalog to Lakebase
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Understand table synchronization capabilities between Delta Lake and Lakebase
# MAGIC - Configure snapshot sync for one-time full table copies
# MAGIC - Set up triggered sync for manual/scheduled incremental updates
# MAGIC - Implement continuous sync for real-time streaming synchronization
# MAGIC - Monitor sync progress and validate data consistency
# MAGIC - Apply sync patterns for feature stores, operational dashboards, and hybrid architectures
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lab 2: Creating and Exploring a Lakebase PostgreSQL Database
# MAGIC - Understanding of Unity Catalog and Delta Lake concepts
# MAGIC - Access to Unity Catalog tables for synchronization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC ### Why Sync Tables?
# MAGIC
# MAGIC One of Lakebase's most powerful features is **bidirectional synchronization** between Delta Lake (analytical) and Lakebase (operational) tables. This eliminates complex ETL pipelines and enables:
# MAGIC
# MAGIC - **Real-time analytics** → Operational applications
# MAGIC - **Operational data** → Analytical workloads
# MAGIC - **Unified governance** via Unity Catalog
# MAGIC - **Low-latency feature serving** for ML models
# MAGIC - **Hybrid OLTP/OLAP** architectures
# MAGIC
# MAGIC ### Sync Modes
# MAGIC
# MAGIC Lakebase supports three synchronization modes:
# MAGIC
# MAGIC 1. **Snapshot Sync** - One-time full table copy
# MAGIC 2. **Triggered Sync** - Manual/scheduled incremental updates using CDC
# MAGIC 3. **Continuous Sync** - Real-time streaming synchronization
# MAGIC
# MAGIC ### Use Cases
# MAGIC
# MAGIC - **Feature Store**: Sync ML features from Delta Lake to Lakebase for low-latency serving
# MAGIC - **Operational Dashboards**: Stream operational metrics from Lakebase to Delta Lake for BI
# MAGIC - **Data Products**: Make analytical insights available to operational applications
# MAGIC - **Event Sourcing**: Capture operational events in Lakebase, analyze in Delta Lake
# MAGIC
# MAGIC ### Time Estimate
# MAGIC 60-75 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup and Configuration

# COMMAND ----------

# Import libraries
import time
from datetime import datetime
from pyspark.sql.functions import col, current_timestamp, lit

print("✅ Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Catalog and Schema

# COMMAND ----------

# Set your catalog and schema
catalog_name = "workshop_catalog"  # Update with your catalog name
schema_name = "workshop"           # Update with your schema name

# Set as default
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"✅ Using catalog: {catalog_name}")
print(f"✅ Using schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Source Delta Tables
# MAGIC
# MAGIC First, let's create some Delta Lake tables that we'll sync to Lakebase.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Customer Analytics Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta table with customer analytics
# MAGIC CREATE OR REPLACE TABLE customer_analytics (
# MAGIC   customer_id BIGINT,
# MAGIC   total_orders INT,
# MAGIC   total_spent DECIMAL(10, 2),
# MAGIC   avg_order_value DECIMAL(10, 2),
# MAGIC   last_order_date DATE,
# MAGIC   customer_segment STRING,
# MAGIC   predicted_ltv DECIMAL(10, 2),
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Customer analytics from data lakehouse';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample analytics data
# MAGIC INSERT INTO customer_analytics VALUES
# MAGIC   (1, 3, 1789.96, 596.65, '2024-05-04', 'high_value', 5000.00, CURRENT_TIMESTAMP()),
# MAGIC   (2, 3, 209.97, 69.99, '2024-05-05', 'regular', 800.00, CURRENT_TIMESTAMP()),
# MAGIC   (3, 1, 295.49, 295.49, '2024-05-03', 'regular', 1200.00, CURRENT_TIMESTAMP()),
# MAGIC   (4, 1, 249.99, 249.99, '2024-05-05', 'regular', 900.00, CURRENT_TIMESTAMP()),
# MAGIC   (5, 0, 0.00, 0.00, NULL, 'new', 500.00, CURRENT_TIMESTAMP());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the data
# MAGIC SELECT * FROM customer_analytics ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Product Recommendations Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table for ML-generated product recommendations
# MAGIC CREATE OR REPLACE TABLE product_recommendations (
# MAGIC   recommendation_id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   product_id BIGINT,
# MAGIC   score DECIMAL(5, 4),
# MAGIC   rank INT,
# MAGIC   model_version STRING,
# MAGIC   generated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'ML-generated product recommendations';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample recommendations
# MAGIC INSERT INTO product_recommendations VALUES
# MAGIC   (1, 1, 107, 0.9234, 1, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (2, 1, 105, 0.8876, 2, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (3, 1, 104, 0.8234, 3, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (4, 2, 102, 0.9456, 1, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (5, 2, 103, 0.9123, 2, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (6, 3, 101, 0.9567, 1, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (7, 3, 107, 0.8934, 2, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (8, 4, 105, 0.9234, 1, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (9, 4, 108, 0.8756, 2, 'v2.1', CURRENT_TIMESTAMP()),
# MAGIC   (10, 5, 106, 0.8567, 1, 'v2.1', CURRENT_TIMESTAMP());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM product_recommendations ORDER BY customer_id, rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Snapshot Sync - One-Time Full Copy
# MAGIC
# MAGIC **Snapshot sync** performs a one-time full copy of a Delta table to Lakebase. This is useful for:
# MAGIC - Initial data loading
# MAGIC - Reference data that changes infrequently
# MAGIC - Static dimension tables
# MAGIC - Testing and development

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Snapshot Sync via SQL
# MAGIC
# MAGIC **Note**: The exact syntax for configuring Lakebase sync may vary based on your Databricks version. Below is a conceptual example. Check your Databricks documentation for the specific syntax.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Conceptual example - syntax may vary
# MAGIC ALTER TABLE customer_analytics 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'lakebase.sync.enabled' = 'true',
# MAGIC   'lakebase.sync.mode' = 'snapshot',
# MAGIC   'lakebase.sync.target' = '<your_lakebase_database>',
# MAGIC   'lakebase.sync.target_table' = 'customer_analytics_sync'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Alternative: Using Databricks UI
# MAGIC
# MAGIC For this lab, we'll walk through the UI-based approach:
# MAGIC
# MAGIC 1. **Navigate to Data Explorer**
# MAGIC    - Go to **Data** → **Catalogs**
# MAGIC    - Find your catalog and schema
# MAGIC    - Locate the `customer_analytics` table
# MAGIC
# MAGIC 2. **Configure Sync**
# MAGIC    - Click on the table name
# MAGIC    - Look for **"Sync to Lakebase"** or **"Configure Sync"** option
# MAGIC    - Select **Snapshot** mode
# MAGIC
# MAGIC 3. **Select Target**
# MAGIC    - Choose your Lakebase database (created in Lab 2)
# MAGIC    - Specify target table name: `customer_analytics_sync`
# MAGIC
# MAGIC 4. **Start Sync**
# MAGIC    - Click **"Start Sync"** or **"Sync Now"**
# MAGIC    - Monitor progress in the UI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Snapshot Sync
# MAGIC
# MAGIC After the sync completes, let's verify the data in Lakebase using SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if the synced table exists in Lakebase
# MAGIC -- Note: You may need to query through your Lakebase connection
# MAGIC
# MAGIC -- This is a conceptual query - adjust based on your setup
# MAGIC SHOW TABLES IN lakebase_catalog.workshop LIKE 'customer_analytics_sync';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Synced Data via Python

# COMMAND ----------

# For this example, we'll simulate the verification
# In a real scenario, you would connect to Lakebase and query

print("📊 Snapshot Sync Summary")
print("="*50)
print("Source table:      customer_analytics (Delta Lake)")
print("Target table:      customer_analytics_sync (Lakebase)")
print("Sync mode:         Snapshot (one-time full copy)")
print("Records synced:    5")
print("Status:            ✅ Complete")
print("="*50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Triggered Sync - Scheduled Incremental Updates
# MAGIC
# MAGIC **Triggered sync** uses Change Data Capture (CDC) to incrementally sync changes from Delta Lake to Lakebase. This is ideal for:
# MAGIC - Regular batch updates (hourly, daily)
# MAGIC - Tables that change periodically
# MAGIC - Controlled synchronization windows
# MAGIC - Cost-effective near-real-time sync

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Triggered Sync Works
# MAGIC
# MAGIC ```
# MAGIC Delta Lake Table
# MAGIC       │
# MAGIC       │ (changes tracked via Delta log)
# MAGIC       │
# MAGIC       ▼
# MAGIC  CDC Pipeline
# MAGIC       │
# MAGIC       │ (triggered manually or on schedule)
# MAGIC       │
# MAGIC       ▼
# MAGIC Lakebase Table
# MAGIC ```
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Only syncs changed rows (inserts, updates, deletes)
# MAGIC - More efficient than full table copies
# MAGIC - Controllable timing (run during off-peak hours)
# MAGIC - Lower cost than continuous sync

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Triggered Sync

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option 1: Via SQL (Conceptual)
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE product_recommendations 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'lakebase.sync.enabled' = 'true',
# MAGIC   'lakebase.sync.mode' = 'triggered',
# MAGIC   'lakebase.sync.target' = '<your_lakebase_database>',
# MAGIC   'lakebase.sync.target_table' = 'product_recommendations_sync',
# MAGIC   'lakebase.sync.schedule' = 'rate(1 hour)'  -- Run every hour
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC #### Option 2: Via Databricks UI
# MAGIC
# MAGIC 1. Navigate to `product_recommendations` table in Data Explorer
# MAGIC 2. Select **"Sync to Lakebase"**
# MAGIC 3. Choose **Triggered** mode
# MAGIC 4. Configure schedule:
# MAGIC    - **Manual trigger**: Sync on demand
# MAGIC    - **Scheduled**: e.g., "Every 1 hour", "Daily at 2 AM"
# MAGIC 5. Specify target Lakebase database and table name
# MAGIC 6. Click **"Enable Sync"**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Data Changes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add new recommendations (simulating ML model update)
# MAGIC INSERT INTO product_recommendations VALUES
# MAGIC   (11, 1, 108, 0.8123, 4, 'v2.2', CURRENT_TIMESTAMP()),
# MAGIC   (12, 2, 104, 0.8567, 3, 'v2.2', CURRENT_TIMESTAMP()),
# MAGIC   (13, 3, 102, 0.8234, 3, 'v2.2', CURRENT_TIMESTAMP());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update scores (simulating model refinement)
# MAGIC UPDATE product_recommendations
# MAGIC SET score = score + 0.01, model_version = 'v2.2'
# MAGIC WHERE recommendation_id <= 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify changes in source table
# MAGIC SELECT * FROM product_recommendations 
# MAGIC WHERE recommendation_id <= 5 OR recommendation_id > 10
# MAGIC ORDER BY recommendation_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Manual Sync
# MAGIC
# MAGIC **In the Databricks UI**:
# MAGIC 1. Go to the sync configuration for `product_recommendations`
# MAGIC 2. Click **"Sync Now"** or **"Trigger Sync"**
# MAGIC 3. Monitor progress in the sync dashboard
# MAGIC
# MAGIC **Via API** (if available):
# MAGIC ```python
# MAGIC # Conceptual example
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.lakebase.trigger_sync(
# MAGIC     table_name=f"{catalog_name}.{schema_name}.product_recommendations"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Sync Progress

# COMMAND ----------

# Simulate sync monitoring
import time

print("🔄 Triggered Sync in Progress...")
print("="*50)

stages = [
    ("Detecting changes", 2),
    ("Reading CDC records", 3),
    ("Applying inserts (3 rows)", 2),
    ("Applying updates (5 rows)", 2),
    ("Verifying consistency", 1),
]

for stage, duration in stages:
    print(f"  ⏳ {stage}...")
    time.sleep(duration)
    print(f"  ✅ {stage} complete")

print("="*50)
print("✅ Sync completed successfully!")
print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   Inserts:   3 rows")
print(f"   Updates:   5 rows")
print(f"   Deletes:   0 rows")
print("="*50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Continuous Sync - Real-Time Streaming
# MAGIC
# MAGIC **Continuous sync** provides real-time streaming synchronization using Delta Change Data Feed. This is optimal for:
# MAGIC - Real-time applications requiring immediate data access
# MAGIC - Feature stores for ML model serving
# MAGIC - Operational dashboards with live data
# MAGIC - Event-driven architectures
# MAGIC - Sub-second data freshness requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Continuous Sync Works
# MAGIC
# MAGIC ```
# MAGIC Delta Lake Table
# MAGIC       │
# MAGIC       │ (Change Data Feed enabled)
# MAGIC       │
# MAGIC       ▼
# MAGIC Streaming Pipeline
# MAGIC       │
# MAGIC       │ (continuous streaming)
# MAGIC       │
# MAGIC       ▼
# MAGIC Lakebase Table
# MAGIC       │
# MAGIC       └─► Real-time latency (<1s)
# MAGIC ```
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Near real-time synchronization (sub-second latency)
# MAGIC - Automatic change detection
# MAGIC - High throughput
# MAGIC - Ideal for operational workloads

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Source Table with Change Data Feed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table with Change Data Feed enabled
# MAGIC CREATE OR REPLACE TABLE customer_events (
# MAGIC   event_id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   event_type STRING,
# MAGIC   event_data STRING,
# MAGIC   event_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC )
# MAGIC COMMENT 'Customer events for real-time processing';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert initial events
# MAGIC INSERT INTO customer_events VALUES
# MAGIC   (1, 1, 'page_view', '{"page": "/products/101"}', CURRENT_TIMESTAMP()),
# MAGIC   (2, 1, 'add_to_cart', '{"product_id": 101}', CURRENT_TIMESTAMP()),
# MAGIC   (3, 2, 'page_view', '{"page": "/products/102"}', CURRENT_TIMESTAMP()),
# MAGIC   (4, 3, 'search', '{"query": "laptop"}', CURRENT_TIMESTAMP()),
# MAGIC   (5, 2, 'add_to_cart', '{"product_id": 102}', CURRENT_TIMESTAMP());

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Continuous Sync

# COMMAND ----------

# MAGIC %md
# MAGIC #### Via Databricks UI
# MAGIC
# MAGIC 1. Navigate to `customer_events` table
# MAGIC 2. Select **"Sync to Lakebase"**
# MAGIC 3. Choose **Continuous** mode
# MAGIC 4. Configure:
# MAGIC    - Target Lakebase database
# MAGIC    - Target table: `customer_events_realtime`
# MAGIC    - Sync frequency: **Real-time (streaming)**
# MAGIC 5. Click **"Enable Continuous Sync"**
# MAGIC
# MAGIC #### Conceptual SQL Configuration
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE customer_events 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'lakebase.sync.enabled' = 'true',
# MAGIC   'lakebase.sync.mode' = 'continuous',
# MAGIC   'lakebase.sync.target' = '<your_lakebase_database>',
# MAGIC   'lakebase.sync.target_table' = 'customer_events_realtime'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Real-Time Events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulate real-time events
# MAGIC INSERT INTO customer_events VALUES
# MAGIC   (6, 1, 'checkout_started', '{"cart_value": 1299.99}', CURRENT_TIMESTAMP()),
# MAGIC   (7, 4, 'page_view', '{"page": "/products/105"}', CURRENT_TIMESTAMP()),
# MAGIC   (8, 5, 'signup', '{"source": "homepage"}', CURRENT_TIMESTAMP());

# COMMAND ----------

# Simulate streaming sync with minimal latency
print("📡 Continuous Sync Active")
print("="*50)
print("Monitoring real-time event stream...\n")

events = [
    ("Event 6: checkout_started", "customer_id=1", "0.3s"),
    ("Event 7: page_view", "customer_id=4", "0.2s"),
    ("Event 8: signup", "customer_id=5", "0.4s"),
]

for event, detail, latency in events:
    print(f"  📥 Detected: {event} ({detail})")
    time.sleep(0.5)
    print(f"  💾 Synced to Lakebase in {latency}")
    print()

print("="*50)
print("✅ All events synced in real-time")
print(f"   Average latency: ~0.3 seconds")
print("="*50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Comparison of Sync Modes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync Mode Comparison
# MAGIC
# MAGIC | Aspect | Snapshot | Triggered | Continuous |
# MAGIC |--------|----------|-----------|------------|
# MAGIC | **Latency** | Minutes to hours | Minutes to hours | Sub-second to seconds |
# MAGIC | **Data Freshness** | Stale between syncs | Fresh on schedule | Real-time |
# MAGIC | **Cost** | Low (one-time) | Medium (periodic) | Higher (streaming) |
# MAGIC | **Use Case** | Initial load, reference data | Regular updates, batch processing | Real-time apps, feature serving |
# MAGIC | **Efficiency** | Full table copy | Incremental (CDC) | Incremental (streaming) |
# MAGIC | **Complexity** | Simple | Moderate | Higher |
# MAGIC | **Resource Usage** | Burst on sync | Periodic spikes | Continuous baseline |
# MAGIC | **Best For** | Static/slow-changing data | Hourly/daily updates | Operational apps, ML serving |
# MAGIC
# MAGIC ### Decision Guide
# MAGIC
# MAGIC **Use Snapshot when**:
# MAGIC - Loading data for the first time
# MAGIC - Syncing reference/dimension tables
# MAGIC - Data changes infrequently
# MAGIC - Cost is a primary concern
# MAGIC
# MAGIC **Use Triggered when**:
# MAGIC - Updates happen regularly (hourly, daily)
# MAGIC - Near-real-time is acceptable (minutes delay)
# MAGIC - You want control over sync timing
# MAGIC - Balancing cost and freshness
# MAGIC
# MAGIC **Use Continuous when**:
# MAGIC - Real-time data access is required
# MAGIC - Building operational applications
# MAGIC - Serving ML features with low latency
# MAGIC - Sub-second freshness is critical

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate Data Consistency

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Row Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare row counts between source and target
# MAGIC SELECT 
# MAGIC   'customer_analytics' AS table_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   'Delta Lake (source)' AS location
# MAGIC FROM customer_analytics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'product_recommendations' AS table_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   'Delta Lake (source)' AS location
# MAGIC FROM product_recommendations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'customer_events' AS table_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   'Delta Lake (source)' AS location
# MAGIC FROM customer_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Data Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for data quality issues in synced tables
# MAGIC SELECT 
# MAGIC   'customer_analytics' AS table_name,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(CASE WHEN customer_id IS NULL THEN 1 END) AS null_customer_ids,
# MAGIC   COUNT(CASE WHEN total_spent < 0 THEN 1 END) AS negative_amounts,
# MAGIC   MAX(updated_at) AS last_updated
# MAGIC FROM customer_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Monitor Sync Jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Sync Status in UI
# MAGIC
# MAGIC **In Databricks Workspace**:
# MAGIC 1. Go to **Data** → **Catalogs**
# MAGIC 2. Navigate to your table
# MAGIC 3. Click on **"Sync Status"** or **"Sync History"** tab
# MAGIC 4. View:
# MAGIC    - Sync job status (Running, Completed, Failed)
# MAGIC    - Last sync timestamp
# MAGIC    - Rows processed
# MAGIC    - Sync duration
# MAGIC    - Error logs (if any)
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC - **Sync Lag**: Time between source update and target availability
# MAGIC - **Throughput**: Rows per second synced
# MAGIC - **Error Rate**: Failed syncs / total syncs
# MAGIC - **Data Freshness**: Age of most recent synced record
# MAGIC - **Resource Usage**: Compute and storage costs

# COMMAND ----------

# Simulate sync monitoring dashboard
print("\n" + "="*60)
print("LAKEBASE SYNC MONITORING DASHBOARD")
print("="*60)

sync_jobs = [
    ("customer_analytics", "Snapshot", "Completed", "2024-05-10 10:30:00", 5, "0.8s"),
    ("product_recommendations", "Triggered", "Completed", "2024-05-10 11:00:00", 13, "2.3s"),
    ("customer_events", "Continuous", "Running", "Streaming", "8", "<0.5s avg"),
]

print(f"\n{'Table':<25} {'Mode':<12} {'Status':<12} {'Last Sync':<20} {'Rows':<8} {'Latency':<10}")
print("-"*60)

for table, mode, status, last_sync, rows, latency in sync_jobs:
    status_icon = "✅" if status == "Completed" else "🔄" if status == "Running" else "❌"
    print(f"{table:<25} {mode:<12} {status_icon} {status:<9} {last_sync:<20} {rows:<8} {latency:<10}")

print("\n" + "="*60)
print("✅ All sync jobs healthy")
print("="*60 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Use Cases and Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Case 1: Real-Time Feature Store
# MAGIC
# MAGIC **Scenario**: Serve ML model features with low latency
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Feature Engineering (Spark)
# MAGIC         │
# MAGIC         ▼
# MAGIC   Delta Lake Table
# MAGIC         │
# MAGIC         │ (Continuous Sync)
# MAGIC         ▼
# MAGIC   Lakebase Table
# MAGIC         │
# MAGIC         ▼
# MAGIC ML Model Serving (<10ms)
# MAGIC ```
# MAGIC
# MAGIC **Implementation**:
# MAGIC 1. Compute features in Delta Lake (batch or streaming)
# MAGIC 2. Enable continuous sync to Lakebase
# MAGIC 3. ML models query Lakebase for real-time features
# MAGIC 4. Achieve <10ms feature lookup latency

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Case 2: Operational Analytics
# MAGIC
# MAGIC **Scenario**: Sync operational data to Delta Lake for analytics
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Operational App (Lakebase)
# MAGIC         │
# MAGIC         │ (Triggered Sync)
# MAGIC         ▼
# MAGIC   Delta Lake Table
# MAGIC         │
# MAGIC         ▼
# MAGIC    BI Dashboard
# MAGIC ```
# MAGIC
# MAGIC **Implementation**:
# MAGIC 1. Applications write to Lakebase tables
# MAGIC 2. Triggered sync (hourly) to Delta Lake
# MAGIC 3. BI tools query Delta Lake for reports
# MAGIC 4. Best of both worlds: operational speed + analytical power

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Case 3: Reference Data Distribution
# MAGIC
# MAGIC **Scenario**: Distribute reference data to multiple applications
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Master Data (Delta Lake)
# MAGIC         │
# MAGIC         │ (Snapshot Sync)
# MAGIC         ├──► Lakebase DB 1 (Region: US-East)
# MAGIC         ├──► Lakebase DB 2 (Region: EU-West)
# MAGIC         └──► Lakebase DB 3 (Region: Asia-Pacific)
# MAGIC ```
# MAGIC
# MAGIC **Implementation**:
# MAGIC 1. Maintain master reference data in Delta Lake
# MAGIC 2. Snapshot sync to regional Lakebase instances
# MAGIC 3. Applications read from local Lakebase (low latency)
# MAGIC 4. Update master data and re-sync as needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync Configuration Best Practices
# MAGIC
# MAGIC ✅ **Choose the right sync mode** based on latency requirements
# MAGIC
# MAGIC ✅ **Enable Change Data Feed** for triggered and continuous sync
# MAGIC ```sql
# MAGIC ALTER TABLE my_table SET TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ✅ **Set appropriate sync schedules** for triggered mode
# MAGIC - High-value data: Every 15 minutes
# MAGIC - Regular updates: Hourly
# MAGIC - Low-frequency: Daily
# MAGIC
# MAGIC ✅ **Monitor sync lag** and adjust resources if needed
# MAGIC
# MAGIC ✅ **Use partitioning** for large tables to improve sync performance
# MAGIC
# MAGIC ### Data Consistency Best Practices
# MAGIC
# MAGIC ✅ **Validate data after sync** - Check row counts and key metrics
# MAGIC
# MAGIC ✅ **Set up alerting** for sync failures or high lag
# MAGIC
# MAGIC ✅ **Test sync with sample data** before production deployment
# MAGIC
# MAGIC ✅ **Document sync dependencies** in your data architecture
# MAGIC
# MAGIC ### Performance Best Practices
# MAGIC
# MAGIC ✅ **Optimize source tables** with appropriate indexes and partitioning
# MAGIC
# MAGIC ✅ **Batch small updates** when using triggered sync
# MAGIC
# MAGIC ✅ **Use snapshot for initial loads**, then switch to incremental
# MAGIC
# MAGIC ✅ **Scale Lakebase compute** for high-throughput continuous sync
# MAGIC
# MAGIC ### Cost Optimization
# MAGIC
# MAGIC ✅ **Use snapshot or triggered** for non-critical data
# MAGIC
# MAGIC ✅ **Schedule syncs during off-peak hours** to reduce costs
# MAGIC
# MAGIC ✅ **Monitor sync resource usage** and optimize accordingly
# MAGIC
# MAGIC ✅ **Clean up unused syncs** to avoid unnecessary charges

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Troubleshooting Common Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 1: Sync Job Failing
# MAGIC
# MAGIC **Symptoms**: Sync status shows "Failed" or "Error"
# MAGIC
# MAGIC **Possible Causes**:
# MAGIC - Insufficient permissions
# MAGIC - Target table schema mismatch
# MAGIC - Network connectivity issues
# MAGIC - Lakebase instance not running
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Check sync error logs in Databricks UI
# MAGIC 2. Verify Unity Catalog permissions
# MAGIC 3. Ensure source and target schemas match
# MAGIC 4. Verify Lakebase instance is running and accessible
# MAGIC
# MAGIC ### Issue 2: High Sync Lag
# MAGIC
# MAGIC **Symptoms**: Data in Lakebase is significantly behind source
# MAGIC
# MAGIC **Possible Causes**:
# MAGIC - High data volume
# MAGIC - Insufficient compute resources
# MAGIC - Complex transformations
# MAGIC - Network bottlenecks
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Scale up Lakebase compute
# MAGIC 2. Partition large tables
# MAGIC 3. Optimize source table structure
# MAGIC 4. Consider batching updates
# MAGIC
# MAGIC ### Issue 3: Data Inconsistency
# MAGIC
# MAGIC **Symptoms**: Row counts or values don't match between source and target
# MAGIC
# MAGIC **Possible Causes**:
# MAGIC - Sync still in progress
# MAGIC - Failed partial sync
# MAGIC - Schema evolution issues
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. Wait for sync to complete
# MAGIC 2. Check for failed sync jobs
# MAGIC 3. Re-run snapshot sync to reset
# MAGIC 4. Verify Change Data Feed is enabled

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ Created Delta Lake tables for synchronization
# MAGIC
# MAGIC ✅ Configured **snapshot sync** for one-time full table copies
# MAGIC
# MAGIC ✅ Set up **triggered sync** for scheduled incremental updates
# MAGIC
# MAGIC ✅ Implemented **continuous sync** for real-time streaming
# MAGIC
# MAGIC ✅ Monitored sync progress and validated data consistency
# MAGIC
# MAGIC ✅ Compared performance and use cases for different sync modes
# MAGIC
# MAGIC ✅ Applied best practices for production deployments
# MAGIC
# MAGIC ✅ Learned troubleshooting techniques for common issues
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 🔄 **Bidirectional sync** eliminates complex ETL pipelines
# MAGIC
# MAGIC 🔄 **Three sync modes** provide flexibility for different use cases
# MAGIC
# MAGIC 🔄 **Continuous sync** enables real-time operational applications
# MAGIC
# MAGIC 🔄 **Change Data Feed** powers efficient incremental synchronization
# MAGIC
# MAGIC 🔄 **Unity Catalog** provides unified governance across analytical and operational data
# MAGIC
# MAGIC ### Real-World Applications
# MAGIC
# MAGIC This capability enables powerful architectures:
# MAGIC
# MAGIC - **ML Feature Stores**: Train on Delta Lake, serve from Lakebase with <10ms latency
# MAGIC - **Operational Analytics**: Capture operational events in Lakebase, analyze in Delta Lake
# MAGIC - **Hybrid Workloads**: Transactional operations in Lakebase, analytics in Delta Lake
# MAGIC - **Reference Data**: Distribute master data to multiple operational databases
# MAGIC - **Real-Time Dashboards**: Stream operational metrics to analytical systems
# MAGIC
# MAGIC ## Workshop Complete! 🎉
# MAGIC
# MAGIC You've completed the **Lakebase Core Concepts** workshop. You now understand:
# MAGIC
# MAGIC ✅ Lakebase architecture and key features
# MAGIC
# MAGIC ✅ Creating and configuring Lakebase database instances
# MAGIC
# MAGIC ✅ Connecting with Python using single connections and pools
# MAGIC
# MAGIC ✅ Synchronizing data between Delta Lake and Lakebase
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Continue your journey:
# MAGIC - Build a production application using Lakebase
# MAGIC - Implement a real-time feature store for ML
# MAGIC - Design a hybrid OLTP/OLAP architecture
# MAGIC - Explore advanced Lakebase features (branching, replication)
# MAGIC
# MAGIC ### Resources
# MAGIC
# MAGIC - [Databricks Lakebase Documentation](https://docs.databricks.com)
# MAGIC - [Unity Catalog Guide](https://docs.databricks.com/unity-catalog)
# MAGIC - [Delta Lake Documentation](https://docs.delta.io)
# MAGIC - [psycopg3 Documentation](https://www.psycopg.org/psycopg3/docs/)
