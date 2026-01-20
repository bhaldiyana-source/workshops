# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will create and configure Lakeflow Connect components in Databricks to ingest data from PostgreSQL.
# MAGIC
# MAGIC **Duration:** 30 minutes
# MAGIC
# MAGIC **Objectives:**
# MAGIC - Create Unity Catalog catalog and schemas
# MAGIC - Create Unity Catalog volume for staging
# MAGIC - Create Unity Catalog connection to PostgreSQL
# MAGIC - Set up Lakeflow Connect ingestion gateway
# MAGIC - Create and configure ingestion pipeline
# MAGIC - Run initial snapshot load
# MAGIC - Verify data ingestion
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Completed Lab 2: PostgreSQL CDC configuration
# MAGIC - Databricks workspace with Unity Catalog and Lakeflow Connect enabled
# MAGIC - Permissions to create catalogs, schemas, connections, and pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create Unity Catalog Resources
# MAGIC
# MAGIC ### Step 1.1: Create Catalog
# MAGIC
# MAGIC Run these SQL commands to create the Unity Catalog structure:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog for retail analytics
# MAGIC CREATE CATALOG IF NOT EXISTS retail_analytics
# MAGIC COMMENT 'Retail CDC data from PostgreSQL';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify catalog creation
# MAGIC SHOW CATALOGS LIKE 'retail_analytics';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Create Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create landing schema for staging volumes
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_analytics.landing
# MAGIC COMMENT 'Staging area for CDC change files from Lakeflow Connect gateway';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create bronze schema for ingested tables
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_analytics.bronze
# MAGIC COMMENT 'Bronze layer - raw CDC data from PostgreSQL';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify schemas
# MAGIC SHOW SCHEMAS IN retail_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3: Create Unity Catalog Volume
# MAGIC
# MAGIC This volume will store staged CDC change files from the ingestion gateway.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create managed volume for staging
# MAGIC CREATE VOLUME IF NOT EXISTS retail_analytics.landing.cdc_staging_volume
# MAGIC COMMENT 'Staging volume for PostgreSQL CDC change files';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify volume creation
# MAGIC DESCRIBE VOLUME EXTENDED retail_analytics.landing.cdc_staging_volume;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Create Unity Catalog Connection to PostgreSQL
# MAGIC
# MAGIC ### Important Connection Details
# MAGIC
# MAGIC Before proceeding, gather your PostgreSQL connection information:
# MAGIC - **Host**: Your PostgreSQL hostname (e.g., `my-postgres.abc123.us-west-2.rds.amazonaws.com`)
# MAGIC - **Port**: Usually `5432`
# MAGIC - **Database**: `retaildb`
# MAGIC - **Username**: `lakeflow_replication`
# MAGIC - **Password**: Your replication user password
# MAGIC
# MAGIC ### Step 2.1: Create Connection via UI
# MAGIC
# MAGIC ⚠️ **Note:** Unity Catalog connections are currently created via UI or API, not SQL.
# MAGIC
# MAGIC **Follow these steps:**
# MAGIC
# MAGIC 1. In Databricks workspace, navigate to **Data** (left sidebar)
# MAGIC 2. Click on **Connections** tab
# MAGIC 3. Click **Create Connection** button
# MAGIC 4. Fill in the connection details:
# MAGIC
# MAGIC ```
# MAGIC Connection name: retail_postgres_connection
# MAGIC Connection type: PostgreSQL
# MAGIC Host: <your-postgres-host>
# MAGIC Port: 5432
# MAGIC Database: retaildb
# MAGIC Username: lakeflow_replication
# MAGIC Password: <your-secure-password>
# MAGIC ```
# MAGIC
# MAGIC 5. **Optional:** Enable SSL if your PostgreSQL requires it
# MAGIC 6. Click **Test Connection** to verify connectivity
# MAGIC 7. Click **Create** to save

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Verify Connection (via Python)

# COMMAND ----------

# List all connections
spark.sql("SHOW CONNECTIONS").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Test Connection

# COMMAND ----------

# Test reading from PostgreSQL via connection
# This confirms network connectivity and credentials

test_query = """
(SELECT COUNT(*) as customer_count FROM customers) AS customer_test
"""

try:
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://<your-host>:5432/retaildb") \
        .option("dbtable", test_query) \
        .option("user", "lakeflow_replication") \
        .option("password", "<your-password>") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df.show()
    print("✓ Connection test successful!")
except Exception as e:
    print(f"✗ Connection test failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Create Lakeflow Connect Ingestion Gateway
# MAGIC
# MAGIC ### What is an Ingestion Gateway?
# MAGIC
# MAGIC The ingestion gateway is a dedicated compute VM that:
# MAGIC - Connects to PostgreSQL replication slot
# MAGIC - Reads change events from WAL stream
# MAGIC - Stages changes in Unity Catalog volume
# MAGIC
# MAGIC ### Step 3.1: Create Gateway via UI
# MAGIC
# MAGIC **Follow these steps:**
# MAGIC
# MAGIC 1. Navigate to **Data Ingestion** or **Workflows** → **Lakeflow Connect**
# MAGIC 2. Click **Create Gateway** button
# MAGIC 3. Configure gateway settings:
# MAGIC
# MAGIC ```
# MAGIC Gateway name: retail_ingestion_gateway
# MAGIC Description: CDC gateway for retail PostgreSQL database
# MAGIC VM size: Medium (m5.xlarge equivalent)
# MAGIC Staging location: retail_analytics.landing.cdc_staging_volume
# MAGIC Connection: retail_postgres_connection
# MAGIC ```
# MAGIC
# MAGIC 4. **Network Configuration**:
# MAGIC    - If using VPN: Select VPN configuration
# MAGIC    - If using PrivateLink: Select PrivateLink endpoint
# MAGIC    - If public: Ensure security groups allow Databricks IPs
# MAGIC
# MAGIC 5. Click **Create and Start Gateway**
# MAGIC
# MAGIC ⏱️ **Wait Time:** Gateway provisioning takes 3-5 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Monitor Gateway Status
# MAGIC
# MAGIC While waiting, check gateway status:

# COMMAND ----------

# Check gateway status (this is a conceptual query - actual implementation may vary)
# %sql
# SELECT 
#   gateway_name,
#   status,
#   last_heartbeat,
#   created_at
# FROM system.lakeflow.gateways
# WHERE gateway_name = 'retail_ingestion_gateway';

print("Check gateway status in the Lakeflow Connect UI:")
print("- Status should show 'RUNNING' (green)")
print("- Last heartbeat should be within last 60 seconds")
print("- Configuration should show correct staging volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Create Lakeflow Connect Ingestion Pipeline
# MAGIC
# MAGIC ### What is an Ingestion Pipeline?
# MAGIC
# MAGIC The ingestion pipeline is a Delta Live Tables (DLT) job that:
# MAGIC - Reads staged change files from volume
# MAGIC - Applies CDC operations (MERGE, DELETE) to bronze tables
# MAGIC - Maintains checkpoints for exactly-once processing
# MAGIC
# MAGIC ### Step 4.1: Create Pipeline via UI
# MAGIC
# MAGIC **Follow these steps:**
# MAGIC
# MAGIC 1. Navigate to **Workflows** → **Delta Live Tables**
# MAGIC 2. Click **Create Pipeline** button
# MAGIC 3. Configure pipeline:
# MAGIC
# MAGIC ```
# MAGIC Pipeline name: retail_cdc_pipeline
# MAGIC Product edition: Advanced (required for CDC)
# MAGIC Pipeline mode: Triggered (we'll change to scheduled later)
# MAGIC Source: Lakeflow Connect Gateway
# MAGIC Gateway: retail_ingestion_gateway
# MAGIC ```
# MAGIC
# MAGIC 4. **Table Selection**:
# MAGIC    - Click **Add Tables**
# MAGIC    - Select publication: `lakeflow_publication`
# MAGIC    - Select tables:
# MAGIC      - ☑ customers
# MAGIC      - ☑ orders
# MAGIC      - ☑ products
# MAGIC
# MAGIC 5. **Destination Configuration**:
# MAGIC    - Target catalog: `retail_analytics`
# MAGIC    - Target schema: `bronze`
# MAGIC    - Table prefix: (leave empty)
# MAGIC
# MAGIC 6. **Ingestion Settings**:
# MAGIC    - Initial snapshot: ☑ Enabled (for first run)
# MAGIC    - Incremental sync: ☑ Enabled
# MAGIC    - Delete handling: Soft delete (recommended)
# MAGIC
# MAGIC 7. **Compute Configuration**:
# MAGIC    - Cluster mode: Fixed size
# MAGIC    - Workers: 2
# MAGIC    - Node type: i3.xlarge (I/O optimized)
# MAGIC
# MAGIC 8. Click **Create**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Alternative - Create Pipeline via API

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Example pipeline configuration (for reference)
# MAGIC pipeline_config = {
# MAGIC   "name": "retail_cdc_pipeline",
# MAGIC   "catalog": "retail_analytics",
# MAGIC   "target": "bronze",
# MAGIC   "continuous": False,
# MAGIC   "channel": "CURRENT",
# MAGIC   "edition": "ADVANCED",
# MAGIC   "configuration": {
# MAGIC     "lakeflow.gateway_name": "retail_ingestion_gateway",
# MAGIC     "lakeflow.publication": "lakeflow_publication",
# MAGIC     "lakeflow.initial_snapshot": "true",
# MAGIC     "lakeflow.delete_mode": "soft"
# MAGIC   },
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "num_workers": 2,
# MAGIC       "node_type_id": "i3.xlarge",
# MAGIC       "spark_conf": {
# MAGIC         "spark.databricks.delta.optimizeWrite.enabled": "true",
# MAGIC         "spark.databricks.delta.autoCompact.enabled": "true"
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC
# MAGIC # Create pipeline via API
# MAGIC # (Use Databricks CLI or REST API)
# MAGIC # databricks pipelines create --json-file pipeline_config.json
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Run Initial Snapshot Load
# MAGIC
# MAGIC ### Step 5.1: Start Pipeline
# MAGIC
# MAGIC 1. Navigate to your pipeline: **Workflows** → **Delta Live Tables** → `retail_cdc_pipeline`
# MAGIC 2. Click **Start** button
# MAGIC 3. Monitor execution in pipeline UI
# MAGIC
# MAGIC ### Expected Timeline:
# MAGIC - **Initialization**: 1-2 minutes (cluster startup)
# MAGIC - **Snapshot load**: 2-5 minutes (for small sample dataset)
# MAGIC - **Table creation**: 1-2 minutes
# MAGIC - **Total**: ~5-10 minutes for first run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.2: Monitor Pipeline Execution
# MAGIC
# MAGIC Watch for these stages in the pipeline UI:
# MAGIC
# MAGIC 1. **INITIALIZING** - Starting compute cluster
# MAGIC 2. **RUNNING** - Executing snapshot
# MAGIC    - Customers snapshot: Reading 5 records
# MAGIC    - Orders snapshot: Reading 5 records
# MAGIC    - Products snapshot: Reading 5 records
# MAGIC 3. **APPLYING CHANGES** - Writing to bronze tables
# MAGIC 4. **COMPLETED** - Pipeline finished successfully

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Verify Data Ingestion
# MAGIC
# MAGIC ### Step 6.1: Check Bronze Tables Exist

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in bronze schema
# MAGIC SHOW TABLES IN retail_analytics.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC You should see three tables:
# MAGIC - `customers`
# MAGIC - `orders`
# MAGIC - `products`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.2: Verify Record Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' as table_name, COUNT(*) as record_count 
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*) 
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*) 
# MAGIC FROM retail_analytics.bronze.products
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected Results:**
# MAGIC ```
# MAGIC table_name | record_count
# MAGIC -----------+-------------
# MAGIC customers  | 5
# MAGIC orders     | 5
# MAGIC products   | 5
# MAGIC ```
# MAGIC
# MAGIC If counts don't match, troubleshoot:
# MAGIC - Check pipeline logs for errors
# MAGIC - Verify gateway is running
# MAGIC - Check PostgreSQL permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.3: Inspect Data Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View sample customer records
# MAGIC SELECT * FROM retail_analytics.bronze.customers LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View sample order records
# MAGIC SELECT * FROM retail_analytics.bronze.orders LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View sample product records
# MAGIC SELECT * FROM retail_analytics.bronze.products LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.4: Check CDC Metadata Columns
# MAGIC
# MAGIC Lakeflow Connect adds metadata columns to track CDC operations:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected CDC Metadata Columns:**
# MAGIC - `_change_type`: Type of operation (INSERT, UPDATE, DELETE)
# MAGIC - `_commit_version`: Delta Lake version when change was committed
# MAGIC - `_commit_timestamp`: Timestamp when change was committed
# MAGIC - `_is_deleted`: Boolean flag for soft deletes
# MAGIC
# MAGIC *(Actual column names may vary based on Lakeflow Connect version)*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.5: Verify Initial Snapshot Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check change types (all should be initial snapshot)
# MAGIC SELECT 
# MAGIC #   _change_type,
# MAGIC #   COUNT(*) as count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC GROUP BY _change_type;

# COMMAND ----------

# MAGIC %md
# MAGIC For initial snapshot, you may see:
# MAGIC - All records marked as `INSERT` or `SNAPSHOT`
# MAGIC - `_commit_timestamp` should be recent (within last 10 minutes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Validate Staging Volume
# MAGIC
# MAGIC ### Step 7.1: List Staged Files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List files in staging volume
# MAGIC LIST '/Volumes/retail_analytics/landing/cdc_staging_volume/';

# COMMAND ----------

# MAGIC %md
# MAGIC You should see directory structure like:
# MAGIC ```
# MAGIC customers/
# MAGIC   snapshot_2024-01-15_10-30-45.parquet
# MAGIC orders/
# MAGIC   snapshot_2024-01-15_10-30-50.parquet
# MAGIC products/
# MAGIC   snapshot_2024-01-15_10-30-55.parquet
# MAGIC _checkpoints/
# MAGIC   ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7.2: Inspect Staged Change File

# COMMAND ----------

# Read one of the staged parquet files directly
staged_path = "/Volumes/retail_analytics/landing/cdc_staging_volume/customers/"

try:
    df_staged = spark.read.parquet(staged_path)
    print(f"✓ Successfully read staged files")
    print(f"  Schema: {len(df_staged.columns)} columns")
    print(f"  Record count: {df_staged.count()}")
    df_staged.printSchema()
    df_staged.show(5, truncate=False)
except Exception as e:
    print(f"Note: {str(e)}")
    print("(Files may be cleaned up after successful pipeline run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Configure Pipeline Scheduling
# MAGIC
# MAGIC ### Step 8.1: Set Up Scheduled Execution
# MAGIC
# MAGIC Now that initial snapshot is complete, configure ongoing incremental sync:
# MAGIC
# MAGIC 1. Navigate to pipeline: `retail_cdc_pipeline`
# MAGIC 2. Click **Settings**
# MAGIC 3. Change **Pipeline Mode**:
# MAGIC    - From: `Triggered`
# MAGIC    - To: `Scheduled`
# MAGIC 4. Set **Schedule**: 
# MAGIC    - Cron expression: `*/15 * * * *` (every 15 minutes)
# MAGIC    - Or use UI to select "Every 15 minutes"
# MAGIC 5. **Save** changes
# MAGIC
# MAGIC **Alternative Modes:**
# MAGIC - **Continuous**: Always running, sub-second latency (higher cost)
# MAGIC - **Triggered**: Manual execution only
# MAGIC - **Scheduled**: Periodic execution (balanced latency/cost)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8.2: Verify Schedule Configuration

# COMMAND ----------

# Check pipeline configuration (conceptual)
print("Verify in pipeline UI:")
print("- Mode: Scheduled")
print("- Frequency: Every 15 minutes")
print("- Next run: Should show upcoming execution time")
print("- Auto-restart on failure: Recommended to enable")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Monitor Pipeline Health
# MAGIC
# MAGIC ### Step 9.1: View Pipeline History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent pipeline executions
# MAGIC SELECT 
# MAGIC #   pipeline_name,
# MAGIC #   update_id,
# MAGIC #   state,
# MAGIC #   creation_time,
# MAGIC #   completion_time,
# MAGIC #   TIMESTAMPDIFF(MINUTE, creation_time, completion_time) as duration_minutes
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_cdc_pipeline'
# MAGIC ORDER BY creation_time DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC *(Note: System table availability depends on Databricks Runtime version)*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9.2: Check Gateway Health

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check gateway status
# MAGIC -- SELECT 
# MAGIC --   gateway_name,
# MAGIC --   status,
# MAGIC --   last_heartbeat,
# MAGIC --   TIMESTAMPDIFF(MINUTE, last_heartbeat, CURRENT_TIMESTAMP()) as minutes_since_heartbeat
# MAGIC -- FROM system.lakeflow.gateways
# MAGIC -- WHERE gateway_name = 'retail_ingestion_gateway';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Data Lineage and Governance
# MAGIC
# MAGIC ### Step 10.1: View Table Lineage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10.2: Check Table Details

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table metadata
# MAGIC DESCRIBE DETAIL retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC Key details to note:
# MAGIC - **format**: Should be `delta`
# MAGIC - **location**: Storage location in cloud storage
# MAGIC - **createdAt**: When table was created
# MAGIC - **properties**: May include CDC-related properties

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10.3: Add Table Comments and Tags

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add table comments
# MAGIC COMMENT ON TABLE retail_analytics.bronze.customers 
# MAGIC IS 'Customer data replicated from PostgreSQL via CDC';
# MAGIC
# MAGIC COMMENT ON TABLE retail_analytics.bronze.orders 
# MAGIC IS 'Order data replicated from PostgreSQL via CDC';
# MAGIC
# MAGIC COMMENT ON TABLE retail_analytics.bronze.products 
# MAGIC IS 'Product catalog replicated from PostgreSQL via CDC';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Completion Checklist
# MAGIC
# MAGIC Verify you've completed all steps:
# MAGIC
# MAGIC ### Unity Catalog Setup
# MAGIC - [ ] Created catalog `retail_analytics`
# MAGIC - [ ] Created schema `retail_analytics.landing`
# MAGIC - [ ] Created schema `retail_analytics.bronze`
# MAGIC - [ ] Created volume `retail_analytics.landing.cdc_staging_volume`
# MAGIC
# MAGIC ### Connection and Gateway
# MAGIC - [ ] Created Unity Catalog connection `retail_postgres_connection`
# MAGIC - [ ] Tested connection successfully
# MAGIC - [ ] Created ingestion gateway `retail_ingestion_gateway`
# MAGIC - [ ] Gateway status shows RUNNING
# MAGIC
# MAGIC ### Pipeline
# MAGIC - [ ] Created ingestion pipeline `retail_cdc_pipeline`
# MAGIC - [ ] Selected all three tables (customers, orders, products)
# MAGIC - [ ] Configured snapshot + incremental mode
# MAGIC - [ ] Set target to `retail_analytics.bronze`
# MAGIC - [ ] Ran initial snapshot successfully
# MAGIC
# MAGIC ### Validation
# MAGIC - [ ] Verified all three bronze tables exist
# MAGIC - [ ] Confirmed record counts match source (5 each)
# MAGIC - [ ] Inspected data quality in sample records
# MAGIC - [ ] Checked CDC metadata columns present
# MAGIC - [ ] Configured scheduled execution (every 15 minutes)
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC **Congratulations!** You've successfully:
# MAGIC - Set up Lakeflow Connect infrastructure
# MAGIC - Created end-to-end CDC pipeline from PostgreSQL to Delta Lake
# MAGIC - Loaded initial data snapshot
# MAGIC - Configured ongoing incremental synchronization
# MAGIC
# MAGIC **Data Flow:**
# MAGIC ```
# MAGIC PostgreSQL → Ingestion Gateway → Staging Volume → Ingestion Pipeline → Bronze Tables
# MAGIC ```
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lecture 4: Understanding Ingestion Modes - Snapshot vs. Incremental** to learn about:
# MAGIC - When to use snapshot vs. incremental mode
# MAGIC - Performance implications
# MAGIC - Transition strategies
# MAGIC - Optimization techniques
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Your CDC pipeline is live!** 🚀
