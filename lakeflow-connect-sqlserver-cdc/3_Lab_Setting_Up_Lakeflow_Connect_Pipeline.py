# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll create the Databricks components needed for CDC ingestion: Unity Catalog resources (catalog, schema, volume), a Lakeflow Connect ingestion gateway, a connection to your SQL Server database, and an ingestion pipeline that will sync data to bronze Delta tables.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Create Unity Catalog resources for CDC staging and storage
# MAGIC - Set up a Lakeflow Connect ingestion gateway
# MAGIC - Configure a Unity Catalog connection to SQL Server
# MAGIC - Create and configure an ingestion pipeline
# MAGIC - Run an initial snapshot load to establish baseline data
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2 (SQL Server configured with CDC enabled)
# MAGIC - SQL Server connection details (hostname, port, database name, credentials)
# MAGIC - Databricks workspace with Lakeflow Connect enabled
# MAGIC - Unity Catalog permissions to create catalogs, schemas, and volumes
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set Up Unity Catalog Resources
# MAGIC
# MAGIC First, we'll create the Unity Catalog structure for staging and storing CDC data.
# MAGIC
# MAGIC ### 1.1 Create Catalog
# MAGIC
# MAGIC We'll create a catalog to organize all retail analytics data.

# COMMAND ----------

# DBTITLE 1,Create Retail Analytics Catalog
# Set your catalog name (customize if needed)
catalog_name = "retail_analytics"

# Create catalog
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
COMMENT 'Retail analytics data with CDC from SQL Server'
""")

print(f"✓ Catalog '{catalog_name}' created successfully")

# Use the catalog
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create Landing Schema for Staging
# MAGIC
# MAGIC This schema will hold the Unity Catalog volume where Lakeflow Connect stages captured changes.

# COMMAND ----------

# DBTITLE 1,Create Landing Schema
landing_schema = "landing"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{landing_schema}
COMMENT 'Staging area for Lakeflow Connect CDC volumes'
""")

print(f"✓ Schema '{catalog_name}.{landing_schema}' created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Create Bronze Schema for Ingested Tables
# MAGIC
# MAGIC This schema will contain the bronze layer Delta tables that replicate source tables.

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema
bronze_schema = "bronze"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema}
COMMENT 'Bronze layer for CDC-ingested tables from SQL Server'
""")

print(f"✓ Schema '{catalog_name}.{bronze_schema}' created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Verify Catalog Structure
# MAGIC
# MAGIC Let's confirm our Unity Catalog resources were created.

# COMMAND ----------

# DBTITLE 1,List Created Schemas
# Show schemas in catalog
display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** You should see both `landing` and `bronze` schemas listed
# MAGIC
# MAGIC Expected output:
# MAGIC ```
# MAGIC namespace
# MAGIC ---------
# MAGIC landing
# MAGIC bronze
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog Connection to SQL Server
# MAGIC
# MAGIC Now we'll create a connection that stores SQL Server credentials securely in Unity Catalog.
# MAGIC
# MAGIC ### 2.1 Store SQL Server Credentials in Databricks Secrets (Optional but Recommended)
# MAGIC
# MAGIC **Best Practice:** Store credentials in Databricks secrets for security.
# MAGIC
# MAGIC **Using Databricks CLI:**
# MAGIC ```bash
# MAGIC # Create secret scope (one-time setup)
# MAGIC databricks secrets create-scope --scope sqlserver_credentials
# MAGIC
# MAGIC # Store credentials
# MAGIC databricks secrets put --scope sqlserver_credentials --key hostname
# MAGIC databricks secrets put --scope sqlserver_credentials --key username
# MAGIC databricks secrets put --scope sqlserver_credentials --key password
# MAGIC ```
# MAGIC
# MAGIC **Or use Databricks UI:**
# MAGIC 1. Navigate to **Settings** → **Secrets**
# MAGIC 2. Create scope: `sqlserver_credentials`
# MAGIC 3. Add secrets: `hostname`, `username`, `password`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Create Connection Using SQL
# MAGIC
# MAGIC **Option A: Using Secrets (Recommended)**

# COMMAND ----------

# DBTITLE 1,Create Connection with Secrets
# MAGIC %sql
# MAGIC -- Create connection using secrets
# MAGIC CREATE CONNECTION IF NOT EXISTS sqlserver_retail_connection
# MAGIC TYPE sqlserver
# MAGIC OPTIONS (
# MAGIC   host secret('sqlserver_credentials', 'hostname'),
# MAGIC   port '1433',
# MAGIC   user secret('sqlserver_credentials', 'username'),
# MAGIC   password secret('sqlserver_credentials', 'password')
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Option B: Using Direct Credentials (For Testing Only)**
# MAGIC
# MAGIC ⚠️ **Warning:** Only use this approach in non-production environments

# COMMAND ----------

# DBTITLE 1,Create Connection with Direct Credentials (Testing Only)
# MAGIC %sql
# MAGIC -- Replace with your actual SQL Server details
# MAGIC -- ⚠️ DO NOT USE IN PRODUCTION - Use secrets instead!
# MAGIC /*
# MAGIC CREATE CONNECTION IF NOT EXISTS sqlserver_retail_connection
# MAGIC TYPE sqlserver
# MAGIC OPTIONS (
# MAGIC   host 'your-sqlserver-host.database.windows.net',
# MAGIC   port '1433',
# MAGIC   user 'your_username',
# MAGIC   password 'your_password'
# MAGIC );
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Verify Connection

# COMMAND ----------

# DBTITLE 1,Test Connection
# MAGIC %sql
# MAGIC -- List connections
# MAGIC SHOW CONNECTIONS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Test Connection to SQL Server
# MAGIC
# MAGIC Let's verify we can reach the SQL Server database.

# COMMAND ----------

# DBTITLE 1,Query SQL Server Through Connection
# MAGIC %sql
# MAGIC -- Test querying SQL Server through the connection
# MAGIC SELECT * FROM sqlserver_retail_connection.RetailDB.dbo.customers LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** You should see customer records from SQL Server
# MAGIC
# MAGIC If you see an error:
# MAGIC - Verify hostname, port, username, and password are correct
# MAGIC - Check network connectivity (firewall, security groups)
# MAGIC - Ensure SQL Server allows remote connections
# MAGIC - Verify database name is 'RetailDB'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Lakeflow Connect Ingestion Gateway
# MAGIC
# MAGIC The ingestion gateway is a dedicated VM that reads SQL Server transaction logs.
# MAGIC
# MAGIC ### 3.1 Create Gateway Using UI
# MAGIC
# MAGIC **Follow these steps in the Databricks UI:**
# MAGIC
# MAGIC 1. **Navigate to Data Ingestion:**
# MAGIC    - Click on **Data** in left sidebar
# MAGIC    - Select **Ingestion** tab
# MAGIC    - Click **Create Ingestion**
# MAGIC
# MAGIC 2. **Select Source:**
# MAGIC    - Choose **SQL Server** as source type
# MAGIC    - Click **Next**
# MAGIC
# MAGIC 3. **Create Ingestion Gateway:**
# MAGIC    - **Gateway Name:** `retail_ingestion_gateway`
# MAGIC    - **Staging Location:** Select `retail_analytics.landing`
# MAGIC    - **VM Size:** Small (for this lab, scale up for production)
# MAGIC    - Click **Create Gateway**
# MAGIC
# MAGIC 4. **Wait for Provisioning:**
# MAGIC    - Gateway creation takes 3-5 minutes
# MAGIC    - Status will change from "Provisioning" to "Running"
# MAGIC    - ☕ Good time for a coffee break!
# MAGIC
# MAGIC 5. **Verify Gateway Status:**
# MAGIC    - Gateway should show status: **Running** (green indicator)
# MAGIC    - Note the gateway ID for reference

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Verify Gateway Using SQL
# MAGIC
# MAGIC Once created, you can check gateway status programmatically.

# COMMAND ----------

# DBTITLE 1,Check Gateway Status
# MAGIC %sql
# MAGIC -- Query gateway status from system tables
# MAGIC SELECT 
# MAGIC   gateway_name,
# MAGIC   gateway_id,
# MAGIC   status,
# MAGIC   staging_location,
# MAGIC   created_time,
# MAGIC   CASE 
# MAGIC     WHEN status = 'RUNNING' THEN '✓ Gateway is operational'
# MAGIC     WHEN status = 'PROVISIONING' THEN '⏳ Wait for provisioning to complete'
# MAGIC     ELSE '⚠ Check gateway status'
# MAGIC   END as status_message
# MAGIC FROM system.lakeflow.gateways
# MAGIC WHERE gateway_name = 'retail_ingestion_gateway';

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** Gateway status should be "RUNNING"
# MAGIC
# MAGIC **Troubleshooting:**
# MAGIC - If status is "PROVISIONING" for >10 minutes, contact Databricks support
# MAGIC - If status is "FAILED", check error message and retry creation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Ingestion Pipeline
# MAGIC
# MAGIC The ingestion pipeline is a Delta Live Tables job that applies changes to bronze tables.
# MAGIC
# MAGIC ### 4.1 Create Pipeline Using UI
# MAGIC
# MAGIC **Continue in the Databricks UI:**
# MAGIC
# MAGIC 1. **Configure Source Connection:**
# MAGIC    - **Connection:** Select `sqlserver_retail_connection` (created in Step 2)
# MAGIC    - **Database:** Enter `RetailDB`
# MAGIC    - **Schema:** Enter `dbo`
# MAGIC    - Click **Test Connection** to verify
# MAGIC    - Click **Next**
# MAGIC
# MAGIC 2. **Select Tables:**
# MAGIC    - Check the box next to:
# MAGIC      - ☑️ `customers`
# MAGIC      - ☑️ `orders`
# MAGIC      - ☑️ `products`
# MAGIC    - Click **Next**
# MAGIC
# MAGIC 3. **Configure Destination:**
# MAGIC    - **Target Catalog:** `retail_analytics`
# MAGIC    - **Target Schema:** `bronze`
# MAGIC    - **Table Naming:** Leave as default (source table names)
# MAGIC    - Click **Next**
# MAGIC
# MAGIC 4. **Pipeline Settings:**
# MAGIC    - **Pipeline Name:** `retail_ingestion_pipeline`
# MAGIC    - **Ingestion Mode:** Select **Snapshot** (for initial load)
# MAGIC    - **Schedule:** Leave as **Triggered** for now (we'll change later)
# MAGIC    - Click **Create**
# MAGIC
# MAGIC 5. **Review Configuration:**
# MAGIC    - Verify all settings
# MAGIC    - Click **Confirm** to create pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Alternative: Create Pipeline Using Python API (Advanced)
# MAGIC
# MAGIC **For automation, you can create pipelines programmatically:**

# COMMAND ----------

# DBTITLE 1,Create Pipeline Programmatically (Optional)
# Example configuration for automated deployment
pipeline_config = {
    "name": "retail_ingestion_pipeline",
    "gateway_name": "retail_ingestion_gateway",
    "connection_name": "sqlserver_retail_connection",
    "source": {
        "database": "RetailDB",
        "schema": "dbo",
        "tables": ["customers", "orders", "products"]
    },
    "destination": {
        "catalog": catalog_name,
        "schema": bronze_schema
    },
    "ingestion_mode": "snapshot",  # Initial load
    "schedule": "triggered"
}

print("Pipeline Configuration:")
print(f"  Name: {pipeline_config['name']}")
print(f"  Gateway: {pipeline_config['gateway_name']}")
print(f"  Connection: {pipeline_config['connection_name']}")
print(f"  Tables: {', '.join(pipeline_config['source']['tables'])}")
print(f"  Destination: {pipeline_config['destination']['catalog']}.{pipeline_config['destination']['schema']}")

# Note: Actual API call would use Databricks REST API or SDK
# This is for reference and documentation purposes

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** Pipeline created successfully with 3 tables configured

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Run Initial Snapshot Load
# MAGIC
# MAGIC Now let's run the pipeline for the first time to load baseline data.
# MAGIC
# MAGIC ### 5.1 Start Pipeline Execution
# MAGIC
# MAGIC **In the Databricks UI:**
# MAGIC
# MAGIC 1. Navigate to **Data** → **Ingestion**
# MAGIC 2. Find your pipeline: `retail_ingestion_pipeline`
# MAGIC 3. Click **Start** or **Run Now**
# MAGIC 4. Pipeline will execute in **Snapshot Mode** (full load)
# MAGIC
# MAGIC **Expected Timeline:**
# MAGIC - Small datasets (< 10K rows): 2-5 minutes
# MAGIC - Medium datasets (10K-100K rows): 5-15 minutes
# MAGIC - Large datasets (>100K rows): 15+ minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Monitor Pipeline Execution
# MAGIC
# MAGIC **Watch the pipeline progress:**
# MAGIC
# MAGIC 1. **Pipeline Status:**
# MAGIC    - QUEUED → Pipeline waiting to start
# MAGIC    - RUNNING → Actively loading data
# MAGIC    - SUCCEEDED → Completed successfully
# MAGIC    - FAILED → Error occurred (check logs)
# MAGIC
# MAGIC 2. **Table-Level Progress:**
# MAGIC    - Each table shows individual status
# MAGIC    - Monitor rows processed
# MAGIC    - Check for any errors
# MAGIC
# MAGIC 3. **View Logs:**
# MAGIC    - Click on **Logs** tab
# MAGIC    - Review execution details
# MAGIC    - Check for warnings or errors

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Query Pipeline Status

# COMMAND ----------

# DBTITLE 1,Check Pipeline Execution Status
# MAGIC %sql
# MAGIC -- Query pipeline execution history
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   execution_id,
# MAGIC   execution_status,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   DATEDIFF(minute, start_time, COALESCE(end_time, CURRENT_TIMESTAMP())) as duration_minutes,
# MAGIC   records_processed,
# MAGIC   CASE 
# MAGIC     WHEN execution_status = 'SUCCEEDED' THEN '✓ Pipeline completed successfully'
# MAGIC     WHEN execution_status = 'RUNNING' THEN '⏳ Pipeline is running'
# MAGIC     WHEN execution_status = 'FAILED' THEN '✗ Pipeline failed - check logs'
# MAGIC     ELSE execution_status
# MAGIC   END as status_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Data Ingestion
# MAGIC
# MAGIC Once the pipeline succeeds, let's verify data was loaded correctly.
# MAGIC
# MAGIC ### 6.1 Check Bronze Tables Created

# COMMAND ----------

# DBTITLE 1,List Bronze Tables
# MAGIC %sql
# MAGIC -- Show tables in bronze schema
# MAGIC SHOW TABLES IN retail_analytics.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** You should see three tables:
# MAGIC - `customers`
# MAGIC - `orders`
# MAGIC - `products`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Verify Record Counts

# COMMAND ----------

# DBTITLE 1,Compare Source vs Target Record Counts
# Query record counts from both SQL Server and bronze tables
print("Record Count Validation:\n")

# Customers
sql_customers = spark.read \
    .format("sqlserver") \
    .option("url", "jdbc:sqlserver://your-server:1433;databaseName=RetailDB") \
    .option("dbtable", "dbo.customers") \
    .load().count()

bronze_customers = spark.table(f"{catalog_name}.{bronze_schema}.customers").count()

print(f"Customers:")
print(f"  SQL Server: {sql_customers}")
print(f"  Bronze: {bronze_customers}")
print(f"  Match: {'✓ YES' if sql_customers == bronze_customers else '✗ NO'}\n")

# Orders
bronze_orders = spark.table(f"{catalog_name}.{bronze_schema}.orders").count()
print(f"Orders:")
print(f"  Bronze: {bronze_orders}\n")

# Products
bronze_products = spark.table(f"{catalog_name}.{bronze_schema}.products").count()
print(f"Products:")
print(f"  Bronze: {bronze_products}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Sample Data Verification

# COMMAND ----------

# DBTITLE 1,Query Bronze Customers Table
# MAGIC %sql
# MAGIC -- View sample records from bronze.customers
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state,
# MAGIC   created_date,
# MAGIC   _commit_timestamp  -- CDC metadata column added by Lakeflow Connect
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Query Bronze Orders Table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   order_status,
# MAGIC   total_amount,
# MAGIC   payment_method,
# MAGIC   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC ORDER BY order_id
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Query Bronze Products Table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   price,
# MAGIC   stock_quantity,
# MAGIC   supplier,
# MAGIC   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.products
# MAGIC ORDER BY product_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Check CDC Metadata Columns
# MAGIC
# MAGIC Lakeflow Connect adds metadata columns to track changes:

# COMMAND ----------

# DBTITLE 1,Describe Bronze Table Schema
# MAGIC %sql
# MAGIC -- View full schema including CDC metadata
# MAGIC DESCRIBE TABLE retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC **CDC Metadata Columns:**
# MAGIC - `_commit_timestamp`: When the change was committed to bronze table
# MAGIC - `_change_type`: Type of operation (INSERT, UPDATE, DELETE) - visible in incremental mode
# MAGIC - `_sequence_number`: LSN from SQL Server transaction log
# MAGIC - `_rescued_data`: Any columns that couldn't be mapped (schema evolution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Check Staging Volume
# MAGIC
# MAGIC Let's inspect the Unity Catalog volume where changes are staged.

# COMMAND ----------

# DBTITLE 1,List Files in Staging Volume
# List contents of staging volume
volume_path = f"/Volumes/{catalog_name}/{landing_schema}/ingestion_volume"

try:
    files = dbutils.fs.ls(volume_path)
    print(f"Staging Volume Contents ({volume_path}):\n")
    for file in files:
        print(f"  {file.name} - {file.size / 1024 / 1024:.2f} MB")
except Exception as e:
    print(f"Volume path: {volume_path}")
    print(f"Note: Volume is created automatically by Lakeflow Connect gateway")
    print(f"If empty, that's expected after snapshot mode completes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Configure Pipeline for Incremental Mode
# MAGIC
# MAGIC Now that baseline data is loaded, switch to incremental mode for ongoing CDC.
# MAGIC
# MAGIC ### 8.1 Update Pipeline Settings
# MAGIC
# MAGIC **In the Databricks UI:**
# MAGIC
# MAGIC 1. Navigate to **Data** → **Ingestion**
# MAGIC 2. Click on `retail_ingestion_pipeline`
# MAGIC 3. Click **Settings** or **Edit**
# MAGIC 4. **Change Ingestion Mode:**
# MAGIC    - From: **Snapshot**
# MAGIC    - To: **Incremental**
# MAGIC 5. **Set Schedule (Optional):**
# MAGIC    - **Continuous**: Always running (for real-time)
# MAGIC    - **Scheduled**: Every 15 minutes (for this lab)
# MAGIC    - **Triggered**: On-demand (keep for now)
# MAGIC 6. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Verify Pipeline Configuration

# COMMAND ----------

# DBTITLE 1,Check Pipeline Settings
# MAGIC %sql
# MAGIC -- Query pipeline configuration
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   ingestion_mode,
# MAGIC   schedule_type,
# MAGIC   status,
# MAGIC   last_updated_time
# MAGIC FROM system.lakeflow.pipelines
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline';

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** Pipeline configured for incremental mode

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue 1: Gateway Stuck in "Provisioning"
# MAGIC
# MAGIC **Symptoms:** Gateway shows "Provisioning" status for >10 minutes
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Check workspace has sufficient quota for VMs
# MAGIC - Verify region has available capacity
# MAGIC - Try deleting and recreating gateway
# MAGIC - Contact Databricks support if persists
# MAGIC
# MAGIC ### Issue 2: Connection Test Failed
# MAGIC
# MAGIC **Symptoms:** Cannot connect to SQL Server
# MAGIC
# MAGIC **Diagnostic Steps:**
# MAGIC ```sql
# MAGIC -- Test from notebook
# MAGIC SELECT * FROM sqlserver_retail_connection.RetailDB.dbo.customers LIMIT 1;
# MAGIC ```
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Verify hostname and port are correct
# MAGIC - Check firewall allows Databricks IP ranges
# MAGIC - Ensure SQL Server allows remote connections
# MAGIC - Test credentials with SQL client (SSMS)
# MAGIC - Check network connectivity (VPN/PrivateLink)
# MAGIC
# MAGIC ### Issue 3: Pipeline Fails with Schema Error
# MAGIC
# MAGIC **Error:** "Column mismatch" or "Schema evolution"
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```sql
# MAGIC -- Enable schema evolution on bronze tables
# MAGIC ALTER TABLE retail_analytics.bronze.customers 
# MAGIC SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');
# MAGIC ```
# MAGIC
# MAGIC ### Issue 4: No Data in Bronze Tables
# MAGIC
# MAGIC **Diagnostic:**
# MAGIC ```sql
# MAGIC -- Check pipeline events for errors
# MAGIC SELECT * 
# MAGIC FROM system.lakeflow.pipeline_events 
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline' 
# MAGIC   AND execution_status = 'FAILED'
# MAGIC ORDER BY start_time DESC;
# MAGIC ```
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Check pipeline logs for detailed error messages
# MAGIC - Verify source tables have data
# MAGIC - Ensure CDC enabled on SQL Server (from Lab 2)
# MAGIC - Re-run pipeline in snapshot mode

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ **Created Unity Catalog resources:**
# MAGIC - Catalog: `retail_analytics`
# MAGIC - Schemas: `landing` (staging), `bronze` (ingested data)
# MAGIC
# MAGIC ✅ **Configured SQL Server connection:**
# MAGIC - Stored credentials securely
# MAGIC - Tested connectivity
# MAGIC
# MAGIC ✅ **Set up Lakeflow Connect components:**
# MAGIC - Ingestion Gateway: `retail_ingestion_gateway` (running)
# MAGIC - Ingestion Pipeline: `retail_ingestion_pipeline`
# MAGIC
# MAGIC ✅ **Loaded baseline data:**
# MAGIC - Ran snapshot mode for initial load
# MAGIC - Verified all 3 tables synced correctly
# MAGIC - Switched to incremental mode for ongoing CDC
# MAGIC
# MAGIC ### Validation Checklist
# MAGIC
# MAGIC Run this final validation:

# COMMAND ----------

# DBTITLE 1,Final Validation Query
# MAGIC %sql
# MAGIC -- Comprehensive setup validation
# MAGIC WITH validation AS (
# MAGIC   SELECT 'Catalog Exists' as check_name, 
# MAGIC          COUNT(*) as result 
# MAGIC   FROM system.information_schema.catalogs 
# MAGIC   WHERE catalog_name = 'retail_analytics'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 'Schemas Created',
# MAGIC          COUNT(*)
# MAGIC   FROM system.information_schema.schemata
# MAGIC   WHERE catalog_name = 'retail_analytics' 
# MAGIC     AND schema_name IN ('landing', 'bronze')
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 'Bronze Tables Created',
# MAGIC          COUNT(*)
# MAGIC   FROM system.information_schema.tables
# MAGIC   WHERE table_catalog = 'retail_analytics'
# MAGIC     AND table_schema = 'bronze'
# MAGIC     AND table_name IN ('customers', 'orders', 'products')
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 'Pipeline Exists',
# MAGIC          COUNT(*)
# MAGIC   FROM system.lakeflow.pipelines
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC )
# MAGIC SELECT 
# MAGIC   check_name,
# MAGIC   result,
# MAGIC   CASE 
# MAGIC     WHEN check_name = 'Catalog Exists' AND result >= 1 THEN '✓ PASS'
# MAGIC     WHEN check_name = 'Schemas Created' AND result = 2 THEN '✓ PASS'
# MAGIC     WHEN check_name = 'Bronze Tables Created' AND result = 3 THEN '✓ PASS'
# MAGIC     WHEN check_name = 'Pipeline Exists' AND result = 1 THEN '✓ PASS'
# MAGIC     ELSE '✗ FAIL'
# MAGIC   END as status
# MAGIC FROM validation;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next Steps
# MAGIC
# MAGIC Now proceed to **Lab 5: Validating Incremental CDC with CRUD Operations** where you'll:
# MAGIC - Perform INSERT, UPDATE, DELETE operations on SQL Server
# MAGIC - Run the pipeline in incremental mode
# MAGIC - Verify changes propagate to bronze tables
# MAGIC - Observe CDC metadata and change tracking
# MAGIC
# MAGIC **Your pipeline is ready for incremental CDC! Let's test it in Lab 5!** 🚀
