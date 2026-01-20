# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will configure Databricks Lakeflow Connect to ingest data from your MySQL database. You'll create Unity Catalog resources, configure an Ingestion Gateway, create a connection to MySQL, build an Ingestion Pipeline, and execute the initial snapshot load.
# MAGIC
# MAGIC **Estimated Time**: 45-60 minutes (includes gateway provisioning time)
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Create Unity Catalog catalog, schemas, and volumes for CDC workflows
# MAGIC - Configure a Lakeflow Connect Ingestion Gateway
# MAGIC - Create a Unity Catalog connection to MySQL with credentials
# MAGIC - Build an Ingestion Pipeline to sync MySQL tables to Delta Lake
# MAGIC - Execute initial snapshot load for baseline data
# MAGIC - Validate ingested data in bronze layer tables
# MAGIC - Monitor pipeline execution and troubleshoot issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2 (MySQL CDC Configuration)
# MAGIC - MySQL database with binary logging enabled
# MAGIC - Network connectivity between Databricks and MySQL established
# MAGIC - Unity Catalog enabled in your Databricks workspace
# MAGIC - CREATE CATALOG permission in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create Unity Catalog Resources
# MAGIC
# MAGIC First, let's create the Unity Catalog structure for our CDC pipeline.
# MAGIC
# MAGIC ### Architecture Reminder
# MAGIC
# MAGIC ```
# MAGIC retail_analytics (Catalog)
# MAGIC ├── landing (Schema)
# MAGIC │   └── ingestion_volume (Volume) - Staging for CDC changes
# MAGIC └── bronze (Schema)
# MAGIC     ├── customers (Delta Table)
# MAGIC     ├── orders (Delta Table)
# MAGIC     └── products (Delta Table)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the main catalog for retail analytics
# MAGIC CREATE CATALOG IF NOT EXISTS retail_analytics
# MAGIC   COMMENT 'Catalog for retail data analytics with MySQL CDC';
# MAGIC
# MAGIC -- Verify catalog creation
# MAGIC SHOW CATALOGS LIKE 'retail_analytics';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create landing schema for staging CDC changes
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_analytics.landing
# MAGIC   COMMENT 'Staging area for Lakeflow Connect CDC volumes';
# MAGIC
# MAGIC -- Create bronze schema for ingested tables
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_analytics.bronze
# MAGIC   COMMENT 'Bronze layer - raw data from MySQL with CDC metadata';
# MAGIC
# MAGIC -- Verify schema creation
# MAGIC SHOW SCHEMAS IN retail_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create volume for Ingestion Gateway staging
# MAGIC CREATE VOLUME IF NOT EXISTS retail_analytics.landing.ingestion_volume
# MAGIC   COMMENT 'Staging volume for MySQL CDC changes from Lakeflow Connect Gateway';
# MAGIC
# MAGIC -- Verify volume creation
# MAGIC DESCRIBE VOLUME retail_analytics.landing.ingestion_volume;

# COMMAND ----------

# Document the Unity Catalog resources
print("Unity Catalog Resources Created:")
print("=" * 60)
print(f"Catalog:         retail_analytics")
print(f"Landing Schema:  retail_analytics.landing")
print(f"Staging Volume:  retail_analytics.landing.ingestion_volume")
print(f"Bronze Schema:   retail_analytics.bronze")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Create Unity Catalog Connection to MySQL
# MAGIC
# MAGIC We'll store MySQL credentials securely in Databricks Secrets and create a Unity Catalog connection.
# MAGIC
# MAGIC ### Step 2.1: Create Databricks Secret Scope (CLI or UI)
# MAGIC
# MAGIC **Option A: Using Databricks CLI**:
# MAGIC ```bash
# MAGIC # Install Databricks CLI if not already installed
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # Configure CLI with your workspace
# MAGIC databricks configure --token
# MAGIC # Enter workspace URL and personal access token
# MAGIC
# MAGIC # Create secret scope
# MAGIC databricks secrets create-scope --scope mysql_credentials
# MAGIC
# MAGIC # Store MySQL password
# MAGIC databricks secrets put --scope mysql_credentials --key cdc_password
# MAGIC # Opens editor to enter password
# MAGIC ```
# MAGIC
# MAGIC **Option B: Using Databricks UI**:
# MAGIC 1. Navigate to `https://<your-workspace>/#secrets/createScope`
# MAGIC 2. Scope Name: `mysql_credentials`
# MAGIC 3. Manage Principal: All Users (or restrict as needed)
# MAGIC 4. Create the scope
# MAGIC 5. Add secret: key = `cdc_password`, value = your MySQL CDC user password

# COMMAND ----------

# Verify secret scope exists (will show error if not created)
try:
    dbutils.secrets.get(scope="mysql_credentials", key="cdc_password")
    print("✅ Secret scope 'mysql_credentials' configured successfully")
except Exception as e:
    print("❌ Secret scope not found. Please create it using CLI or UI.")
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Create Unity Catalog Connection
# MAGIC
# MAGIC ⚠️ **Replace the placeholder values** with your actual MySQL connection details from Lab 2.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create connection to MySQL database
# MAGIC CREATE CONNECTION IF NOT EXISTS retail_mysql_connection
# MAGIC   TYPE mysql
# MAGIC   OPTIONS (
# MAGIC     host 'your-mysql-host.com',              -- Replace with your MySQL hostname
# MAGIC     port '3306',                               -- Default MySQL port
# MAGIC     user 'cdc_user',                          -- CDC user created in Lab 2
# MAGIC     password secret('mysql_credentials', 'cdc_password')  -- Reference to secret
# MAGIC   )
# MAGIC   COMMENT 'Connection to MySQL retail_db for CDC operations';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify connection was created
# MAGIC DESCRIBE CONNECTION retail_mysql_connection;
# MAGIC
# MAGIC -- Note: This shows connection metadata but not the password (secure)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Test Connection
# MAGIC
# MAGIC Let's verify we can connect to MySQL and query the source database.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test connection by listing databases
# MAGIC SHOW DATABASES 
# MAGIC USING CONNECTION retail_mysql_connection;
# MAGIC -- Should show 'retail_db' among other databases

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in retail_db
# MAGIC SHOW TABLES IN retail_db
# MAGIC USING CONNECTION retail_mysql_connection;
# MAGIC -- Should show: customers, orders, products

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test query to verify we can read data
# MAGIC SELECT COUNT(*) as customer_count
# MAGIC FROM retail_db.customers
# MAGIC USING CONNECTION retail_mysql_connection;
# MAGIC -- Should return the number of customers inserted in Lab 2 (at least 10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Configure Lakeflow Connect Ingestion Gateway
# MAGIC
# MAGIC The Ingestion Gateway is a managed VM that reads MySQL binary logs and stages changes in Unity Catalog volumes.
# MAGIC
# MAGIC ### Step 3.1: Navigate to Lakeflow Connect UI
# MAGIC
# MAGIC **In Databricks Workspace**:
# MAGIC 1. Click on **Data** in the left sidebar
# MAGIC 2. Select **Data Ingestion** (or **Lakeflow Connect** depending on UI version)
# MAGIC 3. Click **Create Ingestion Gateway**
# MAGIC
# MAGIC ### Step 3.2: Configure Gateway Settings
# MAGIC
# MAGIC **Gateway Configuration**:
# MAGIC - **Name**: `retail_ingestion_gateway`
# MAGIC - **Connection**: Select `retail_mysql_connection` (created above)
# MAGIC - **Staging Volume**: `retail_analytics.landing.ingestion_volume`
# MAGIC - **VM Size**: 
# MAGIC   - Small (2 cores, 8 GB) for < 10K changes/hour
# MAGIC   - Medium (4 cores, 16 GB) for 10K-100K changes/hour
# MAGIC   - **Recommended for workshop**: Small
# MAGIC - **Network**: 
# MAGIC   - If using VPC/VNet peering, select appropriate network configuration
# MAGIC   - If using public access, ensure MySQL firewall allows Databricks IPs
# MAGIC
# MAGIC ### Step 3.3: Create Gateway
# MAGIC
# MAGIC Click **Create Gateway** and wait for provisioning.
# MAGIC
# MAGIC **Provisioning Time**: 3-5 minutes
# MAGIC
# MAGIC **Expected Status Progression**:
# MAGIC - `PROVISIONING` → Gateway VM is being created
# MAGIC - `STARTING` → Gateway is initializing
# MAGIC - `RUNNING` → Gateway is connected to MySQL and ready
# MAGIC
# MAGIC ⚠️ **If gateway fails to connect**:
# MAGIC - Check network connectivity (firewall rules, security groups)
# MAGIC - Verify MySQL credentials in connection
# MAGIC - Ensure MySQL binary logging is enabled
# MAGIC - Review gateway logs in the UI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Create Lakeflow Connect Ingestion Pipeline
# MAGIC
# MAGIC Once the gateway is running, we'll create the ingestion pipeline to apply changes to bronze tables.
# MAGIC
# MAGIC ### Step 4.1: Navigate to Create Pipeline
# MAGIC
# MAGIC **In Databricks Workspace**:
# MAGIC 1. Go to **Data** → **Lakeflow Connect** → **Ingestion Pipelines**
# MAGIC 2. Click **Create Ingestion Pipeline**
# MAGIC
# MAGIC ### Step 4.2: Configure Pipeline Settings
# MAGIC
# MAGIC **Pipeline Configuration**:
# MAGIC - **Pipeline Name**: `retail_ingestion_pipeline`
# MAGIC - **Ingestion Gateway**: Select `retail_ingestion_gateway`
# MAGIC - **Source Database**: `retail_db`
# MAGIC - **Source Tables**: Select all three tables:
# MAGIC   - ☑️ `customers`
# MAGIC   - ☑️ `orders`
# MAGIC   - ☑️ `products`
# MAGIC - **Destination Catalog**: `retail_analytics`
# MAGIC - **Destination Schema**: `bronze`
# MAGIC - **Ingestion Mode**: `Snapshot` (for initial load)
# MAGIC   - After initial load, this will automatically switch to `Incremental`
# MAGIC - **Schedule**: 
# MAGIC   - For initial load: Leave as manual trigger
# MAGIC   - We'll configure scheduling in Lab 8
# MAGIC
# MAGIC ### Step 4.3: Configure Table-Specific Settings
# MAGIC
# MAGIC **For each table, configure**:
# MAGIC
# MAGIC **customers**:
# MAGIC - Primary Key: `customer_id`
# MAGIC - Target Table Name: `customers` (will create `bronze.customers`)
# MAGIC
# MAGIC **orders**:
# MAGIC - Primary Key: `order_id`
# MAGIC - Target Table Name: `orders` (will create `bronze.orders`)
# MAGIC - Dependency: `customers` (orders reference customers via FK)
# MAGIC
# MAGIC **products**:
# MAGIC - Primary Key: `product_id`
# MAGIC - Target Table Name: `products` (will create `bronze.products`)
# MAGIC
# MAGIC ### Step 4.4: Create and Start Pipeline
# MAGIC
# MAGIC 1. Click **Create Pipeline**
# MAGIC 2. Review configuration summary
# MAGIC 3. Click **Start** to begin initial snapshot load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Monitor Initial Snapshot Load
# MAGIC
# MAGIC While the pipeline executes the initial snapshot, let's monitor its progress.
# MAGIC
# MAGIC ### Step 5.1: Monitor Pipeline Execution
# MAGIC
# MAGIC **In Pipeline UI**:
# MAGIC - View **Pipeline Graph** showing data flow
# MAGIC - Check **Status** for each table (RUNNING → COMPLETED)
# MAGIC - Monitor **Progress**: rows ingested, execution time
# MAGIC - Review **Logs** if any issues occur
# MAGIC
# MAGIC **Expected Duration**:
# MAGIC - Small dataset (< 100 rows per table): 2-5 minutes
# MAGIC - Medium dataset (1K-10K rows): 5-15 minutes
# MAGIC - Large dataset (>10K rows): 15+ minutes

# COMMAND ----------

# Check if bronze tables are being created
# Note: These queries will fail initially until pipeline creates the tables

try:
    spark.sql("SHOW TABLES IN retail_analytics.bronze").display()
    print("✅ Bronze tables visible")
except Exception as e:
    print("⏳ Bronze tables not yet created. Wait for pipeline to complete initial snapshot.")
    print(f"Details: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Validate Initial Snapshot Load
# MAGIC
# MAGIC Once the pipeline completes, let's verify the data was ingested correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in bronze schema
# MAGIC SHOW TABLES IN retail_analytics.bronze;
# MAGIC -- Should show: customers, orders, products

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check record counts for all bronze tables
# MAGIC SELECT 'customers' as table_name, COUNT(*) as record_count 
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'orders', COUNT(*) 
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'products', COUNT(*) 
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify customer data
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     city,
# MAGIC     state,
# MAGIC     _commit_timestamp  -- CDC metadata added by Lakeflow Connect
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify orders with customer join
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     o.order_status,
# MAGIC     o.total_amount,
# MAGIC     o._commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC ORDER BY o.order_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify products by category
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) as product_count,
# MAGIC     AVG(price) as avg_price,
# MAGIC     SUM(stock_quantity) as total_stock
# MAGIC FROM retail_analytics.bronze.products
# MAGIC GROUP BY category
# MAGIC ORDER BY product_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7: Inspect CDC Metadata Fields
# MAGIC
# MAGIC Lakeflow Connect adds metadata columns to track CDC operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe bronze.customers table structure
# MAGIC DESCRIBE retail_analytics.bronze.customers;
# MAGIC
# MAGIC -- Look for CDC metadata columns:
# MAGIC -- _commit_timestamp: When change was committed to MySQL
# MAGIC -- _commit_version: Delta table version when applied
# MAGIC -- _rescued_data: Any fields that couldn't be parsed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check CDC metadata values
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     _commit_timestamp,
# MAGIC     _commit_version
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC ORDER BY _commit_timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8: Inspect Staging Volume
# MAGIC
# MAGIC Let's look at the staging volume where the gateway stores CDC changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List contents of staging volume
# MAGIC LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/';
# MAGIC
# MAGIC -- You should see directories for each table:
# MAGIC -- - customers/
# MAGIC -- - orders/
# MAGIC -- - products/
# MAGIC -- - _checkpoint/ (checkpoint tracking)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List files in customers staging directory
# MAGIC LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/customers/';
# MAGIC
# MAGIC -- You'll see Parquet files containing the snapshot data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9: View Pipeline Execution History
# MAGIC
# MAGIC Query the system tables to see pipeline execution logs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View pipeline execution events
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     update_id,
# MAGIC     timestamp,
# MAGIC     state,
# MAGIC     message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for any errors
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     timestamp,
# MAGIC     state,
# MAGIC     error_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND state = 'FAILED'
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 10: Verify Data Lineage in Unity Catalog
# MAGIC
# MAGIC Unity Catalog tracks the lineage of data from source to destination.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history for customers
# MAGIC DESCRIBE HISTORY retail_analytics.bronze.customers;
# MAGIC
# MAGIC -- Shows all versions of the table with timestamps and operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get detailed table metadata
# MAGIC DESCRIBE DETAIL retail_analytics.bronze.customers;
# MAGIC
# MAGIC -- Shows:
# MAGIC -- - Table location
# MAGIC -- - Format (Delta)
# MAGIC -- - Number of files
# MAGIC -- - Size in bytes
# MAGIC -- - Creation time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Validation Checklist
# MAGIC
# MAGIC Verify you've completed all tasks successfully:

# COMMAND ----------

# Run comprehensive validation
print("Lab 3 Validation Checklist")
print("=" * 60)

# Check Unity Catalog resources
try:
    spark.sql("DESCRIBE CATALOG retail_analytics")
    print("✅ Catalog 'retail_analytics' exists")
except:
    print("❌ Catalog 'retail_analytics' not found")

try:
    spark.sql("DESCRIBE SCHEMA retail_analytics.landing")
    print("✅ Schema 'retail_analytics.landing' exists")
except:
    print("❌ Schema 'retail_analytics.landing' not found")

try:
    spark.sql("DESCRIBE SCHEMA retail_analytics.bronze")
    print("✅ Schema 'retail_analytics.bronze' exists")
except:
    print("❌ Schema 'retail_analytics.bronze' not found")

try:
    spark.sql("DESCRIBE VOLUME retail_analytics.landing.ingestion_volume")
    print("✅ Volume 'ingestion_volume' exists")
except:
    print("❌ Volume 'ingestion_volume' not found")

# Check connection
try:
    spark.sql("DESCRIBE CONNECTION retail_mysql_connection")
    print("✅ Connection 'retail_mysql_connection' exists")
except:
    print("❌ Connection 'retail_mysql_connection' not found")

# Check bronze tables
tables_expected = ['customers', 'orders', 'products']
for table in tables_expected:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM retail_analytics.bronze.{table}").collect()[0]['cnt']
        if count > 0:
            print(f"✅ Table 'bronze.{table}' exists with {count} rows")
        else:
            print(f"⚠️  Table 'bronze.{table}' exists but has no rows")
    except:
        print(f"❌ Table 'bronze.{table}' not found")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ **Unity Catalog Setup**:
# MAGIC - Created `retail_analytics` catalog
# MAGIC - Created `landing` schema with staging volume
# MAGIC - Created `bronze` schema for ingested tables
# MAGIC
# MAGIC ✅ **Secure Connectivity**:
# MAGIC - Configured Databricks Secrets for MySQL password
# MAGIC - Created Unity Catalog connection to MySQL
# MAGIC - Tested connection and verified access to source tables
# MAGIC
# MAGIC ✅ **Lakeflow Connect Components**:
# MAGIC - Deployed Ingestion Gateway (managed VM reading binary logs)
# MAGIC - Created Ingestion Pipeline (DLT pipeline applying changes)
# MAGIC - Configured table mappings and dependencies
# MAGIC
# MAGIC ✅ **Initial Data Load**:
# MAGIC - Executed snapshot mode to load baseline data
# MAGIC - Validated all three tables ingested successfully
# MAGIC - Verified referential integrity (orders → customers)
# MAGIC
# MAGIC ✅ **Monitoring**:
# MAGIC - Inspected CDC metadata fields (_commit_timestamp, _commit_version)
# MAGIC - Reviewed staging volume contents
# MAGIC - Queried pipeline execution history
# MAGIC - Verified data lineage in Unity Catalog
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Unity Catalog provides governance** for the entire CDC pipeline with lineage, access control, and audit logging
# MAGIC
# MAGIC 2. **Secrets management is critical** - never hardcode passwords in connections or notebooks
# MAGIC
# MAGIC 3. **Ingestion Gateway is a managed service** - Databricks handles VM provisioning, scaling, and monitoring
# MAGIC
# MAGIC 4. **Snapshot mode is for initial load** - captures current state of tables without binary log history
# MAGIC
# MAGIC 5. **Delta Lake provides ACID guarantees** - failed ingestions can be retried without data corruption
# MAGIC
# MAGIC ### Architecture Achieved
# MAGIC
# MAGIC ```
# MAGIC MySQL (retail_db)
# MAGIC      │
# MAGIC      ├─ Binary Logs (ROW format, GTID enabled)
# MAGIC      │
# MAGIC      ▼
# MAGIC Ingestion Gateway (retail_ingestion_gateway)
# MAGIC      │
# MAGIC      ├─ Reads binary logs
# MAGIC      ├─ Stages changes to Volume
# MAGIC      │
# MAGIC      ▼
# MAGIC Unity Catalog Volume (retail_analytics.landing.ingestion_volume)
# MAGIC      │
# MAGIC      ├─ customers/ (Parquet files)
# MAGIC      ├─ orders/ (Parquet files)
# MAGIC      ├─ products/ (Parquet files)
# MAGIC      │
# MAGIC      ▼
# MAGIC Ingestion Pipeline (retail_ingestion_pipeline)
# MAGIC      │
# MAGIC      ├─ Applies INSERT/UPDATE/DELETE
# MAGIC      ├─ Maintains checkpoints
# MAGIC      │
# MAGIC      ▼
# MAGIC Bronze Delta Tables (retail_analytics.bronze)
# MAGIC      ├─ customers (with CDC metadata)
# MAGIC      ├─ orders (with CDC metadata)
# MAGIC      └─ products (with CDC metadata)
# MAGIC ```
# MAGIC
# MAGIC ## Troubleshooting Common Issues
# MAGIC
# MAGIC | Issue | Diagnosis | Solution |
# MAGIC |-------|-----------|----------|
# MAGIC | Gateway stuck in PROVISIONING | Check Databricks workspace quotas | Request quota increase or use smaller VM |
# MAGIC | Gateway shows DISCONNECTED | Network connectivity issue | Verify firewall rules, security groups |
# MAGIC | Connection test fails | Invalid credentials or host | Double-check MySQL connection details |
# MAGIC | Pipeline fails with "binlog not enabled" | MySQL configuration issue | Verify binary logging enabled from Lab 2 |
# MAGIC | Tables empty after snapshot | Pipeline still running | Wait for COMPLETED status in pipeline UI |
# MAGIC | Orders table missing records | Foreign key constraint or timing | Check customers loaded before orders |
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lecture 4: Understanding Ingestion Modes - Snapshot vs. Incremental** to learn:
# MAGIC - How snapshot mode works (what we just did)
# MAGIC - How incremental mode captures ongoing changes
# MAGIC - Checkpoint management and recovery
# MAGIC - Performance characteristics and optimization
# MAGIC
# MAGIC **Before moving on, ensure**:
# MAGIC - [ ] All three bronze tables exist and have data
# MAGIC - [ ] Record counts match MySQL source (within expected range)
# MAGIC - [ ] Ingestion Gateway status is RUNNING
# MAGIC - [ ] Pipeline execution completed successfully (status = COMPLETED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
# MAGIC - [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
# MAGIC - [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
# MAGIC - [Delta Live Tables Pipelines](https://docs.databricks.com/delta-live-tables/)
# MAGIC - [Databricks Secrets Management](https://docs.databricks.com/security/secrets/index.html)
