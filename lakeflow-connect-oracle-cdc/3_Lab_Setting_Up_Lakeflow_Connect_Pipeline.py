# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline
# MAGIC
# MAGIC ## Lab Objectives
# MAGIC By the end of this lab, you will:
# MAGIC - Create a Unity Catalog connection to your Oracle database
# MAGIC - Set up a Lakeflow Connect ingestion gateway
# MAGIC - Create a Unity Catalog volume for CDC staging
# MAGIC - Configure an ingestion pipeline using Delta Live Tables
# MAGIC - Perform the initial snapshot of CUSTOMERS, ORDERS, and PRODUCTS tables
# MAGIC - Validate data ingestion into bronze layer tables
# MAGIC - Monitor pipeline execution and data lineage
# MAGIC
# MAGIC **Estimated Duration:** 30 minutes  
# MAGIC **Prerequisites:** 
# MAGIC - Completion of Lab 2 (Oracle configured for CDC)
# MAGIC - Unity Catalog enabled in your Databricks workspace
# MAGIC - Lakeflow Connect feature available
# MAGIC - CREATE CATALOG and CREATE CONNECTION permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Architecture
# MAGIC
# MAGIC In this lab, you'll build the Databricks side of the CDC pipeline:
# MAGIC
# MAGIC ```
# MAGIC Oracle Database                    Databricks Lakehouse (YOU ARE HERE)
# MAGIC ━━━━━━━━━━━━━━━━━                  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC
# MAGIC RETAIL.CUSTOMERS                   Step 1: Create Unity Catalog Resources
# MAGIC RETAIL.ORDERS         ═══CDC═══►    └─► Catalog: retail_analytics
# MAGIC RETAIL.PRODUCTS                         ├─► Schema: landing (for volumes)
# MAGIC                                          └─► Schema: bronze (for tables)
# MAGIC
# MAGIC                                    Step 2: Create Unity Catalog Connection
# MAGIC                                      └─► Stores Oracle credentials securely
# MAGIC
# MAGIC                                    Step 3: Create Ingestion Gateway
# MAGIC                                      └─► Provisions VM to read Oracle logs
# MAGIC
# MAGIC                                    Step 4: Create Ingestion Pipeline
# MAGIC                                      └─► Delta Live Tables for CDC processing
# MAGIC
# MAGIC                                    Step 5: Run Initial Snapshot
# MAGIC                                      └─► Full load of all tables
# MAGIC
# MAGIC                                    Step 6: Validate Bronze Tables
# MAGIC                                      └─► Verify data and row counts
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Unity Catalog Resources
# MAGIC
# MAGIC ### Create Catalog and Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog for retail analytics
# MAGIC CREATE CATALOG IF NOT EXISTS retail_analytics
# MAGIC   COMMENT 'Retail data ingested from Oracle via Lakeflow Connect CDC';
# MAGIC
# MAGIC -- Use the catalog
# MAGIC USE CATALOG retail_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create landing schema for CDC volumes (staging area)
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_analytics.landing
# MAGIC   COMMENT 'Staging area for Lakeflow Connect CDC volumes';
# MAGIC
# MAGIC -- Create bronze schema for raw ingested tables
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_analytics.bronze
# MAGIC   COMMENT 'Bronze layer: Raw data ingested from Oracle CDC';
# MAGIC
# MAGIC -- Verify schemas
# MAGIC SHOW SCHEMAS IN retail_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Unity Catalog Volume for CDC Staging
# MAGIC
# MAGIC This volume will store intermediate CDC files written by the ingestion gateway.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create managed volume for CDC staging
# MAGIC CREATE VOLUME IF NOT EXISTS retail_analytics.landing.cdc_volume
# MAGIC   COMMENT 'Staging volume for Oracle CDC change data';
# MAGIC
# MAGIC -- Verify volume creation
# MAGIC DESCRIBE VOLUME retail_analytics.landing.cdc_volume;

# COMMAND ----------

# Get volume path for later use
volume_path = "/Volumes/retail_analytics/landing/cdc_volume"
print(f"CDC Staging Volume Path: {volume_path}")

# List initial contents (should be empty)
display(dbutils.fs.ls(volume_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog Connection to Oracle
# MAGIC
# MAGIC ### Store Oracle Credentials in Databricks Secrets (Recommended)
# MAGIC
# MAGIC **Before creating the connection, store credentials securely:**
# MAGIC
# MAGIC ```bash
# MAGIC # Run these commands in Databricks CLI or workspace settings:
# MAGIC
# MAGIC # Create secret scope
# MAGIC databricks secrets create-scope oracle
# MAGIC
# MAGIC # Add Oracle CDC username
# MAGIC databricks secrets put-secret oracle cdc_username
# MAGIC # Enter: CDC_USER
# MAGIC
# MAGIC # Add Oracle CDC password
# MAGIC databricks secrets put-secret oracle cdc_password
# MAGIC # Enter: Your secure password
# MAGIC ```
# MAGIC
# MAGIC ### Create Unity Catalog Connection

# COMMAND ----------

# IMPORTANT: Replace these values with your Oracle connection details from Lab 2

oracle_config = {
    "host": "oracle.example.com",  # Your Oracle hostname or IP
    "port": "1521",                # Oracle port (usually 1521)
    "service_name": "RETAILDB",    # Oracle service name or SID
    "username": dbutils.secrets.get("oracle", "cdc_username"),  # CDC_USER
    "password": dbutils.secrets.get("oracle", "cdc_password")   # Secure password
}

print("Oracle Connection Configuration:")
print(f"  Host: {oracle_config['host']}")
print(f"  Port: {oracle_config['port']}")
print(f"  Service Name: {oracle_config['service_name']}")
print(f"  Username: {oracle_config['username']}")
print(f"  Password: ********")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Unity Catalog connection for Oracle
# MAGIC -- Note: This SQL syntax may vary based on Databricks version
# MAGIC -- For UI-based creation, follow instructions in next cell
# MAGIC
# MAGIC CREATE CONNECTION IF NOT EXISTS oracle_retail_connection
# MAGIC   TYPE oracle
# MAGIC   OPTIONS (
# MAGIC     host 'oracle.example.com',
# MAGIC     port '1521',
# MAGIC     service_name 'RETAILDB',
# MAGIC     user 'CDC_USER',
# MAGIC     password secret('oracle', 'cdc_password')
# MAGIC   )
# MAGIC   COMMENT 'Oracle connection for CDC ingestion of retail database';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Create Connection via UI
# MAGIC
# MAGIC If SQL creation doesn't work in your Databricks version, follow these steps:
# MAGIC
# MAGIC 1. Navigate to **Data** → **Connections** in the left sidebar
# MAGIC 2. Click **Create Connection**
# MAGIC 3. Select **Oracle** as connection type
# MAGIC 4. Fill in connection details:
# MAGIC    - **Name:** `oracle_retail_connection`
# MAGIC    - **Host:** Your Oracle hostname (from Lab 2)
# MAGIC    - **Port:** `1521`
# MAGIC    - **Service Name:** `RETAILDB`
# MAGIC    - **Username:** `CDC_USER`
# MAGIC    - **Password:** Your CDC_USER password
# MAGIC 5. Click **Test Connection** to verify connectivity
# MAGIC 6. Click **Create** to save the connection

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Connection

# COMMAND ----------

# Test Oracle connection by querying current SCN
try:
    test_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:@{oracle_config['host']}:{oracle_config['port']}/{oracle_config['service_name']}") \
        .option("dbtable", "(SELECT CURRENT_SCN, SYSTIMESTAMP AS db_time FROM V$DATABASE)") \
        .option("user", oracle_config['username']) \
        .option("password", oracle_config['password']) \
        .option("driver", "oracle.jdbc.OracleDriver") \
        .load()
    
    result = test_df.first()
    print("✅ Connection successful!")
    print(f"   Oracle Current SCN: {result.CURRENT_SCN}")
    print(f"   Oracle Database Time: {result.DB_TIME}")
except Exception as e:
    print("❌ Connection failed!")
    print(f"   Error: {str(e)}")
    print("\n   Troubleshooting:")
    print("   - Verify network connectivity between Databricks and Oracle")
    print("   - Check Oracle hostname, port, and service name")
    print("   - Confirm CDC_USER credentials are correct")
    print("   - Ensure Oracle listener is running")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Lakeflow Connect Ingestion Gateway
# MAGIC
# MAGIC ### About Ingestion Gateways
# MAGIC
# MAGIC An **ingestion gateway** is a managed compute resource that:
# MAGIC - Connects to your Oracle database
# MAGIC - Reads redo logs via LogMiner
# MAGIC - Converts changes to Parquet format
# MAGIC - Writes staged data to Unity Catalog volumes
# MAGIC
# MAGIC **Provisioning time:** 3-5 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gateway via Databricks UI
# MAGIC
# MAGIC **Follow these steps in the Databricks UI:**
# MAGIC
# MAGIC 1. Navigate to **Data Engineering** → **Lakeflow** → **Connections** in the left sidebar
# MAGIC 2. Click **Create Ingestion Gateway**
# MAGIC 3. Configure gateway settings:
# MAGIC    - **Name:** `retail_ingestion_gateway`
# MAGIC    - **Connection:** Select `oracle_retail_connection`
# MAGIC    - **Staging Location:** `retail_analytics.landing.cdc_volume`
# MAGIC    - **Gateway Size:** Select `Small` (sufficient for workshop; scale up for production)
# MAGIC 4. Click **Create** and wait for provisioning (3-5 minutes)
# MAGIC 5. Gateway status should change to **Running**
# MAGIC
# MAGIC **Alternative: Create via SDK (if available):**

# COMMAND ----------

# Create ingestion gateway via Databricks SDK (if available in your version)
try:
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    
    gateway = w.lakeflow.gateways.create(
        name="retail_ingestion_gateway",
        connection_name="oracle_retail_connection",
        staging_location="/Volumes/retail_analytics/landing/cdc_volume",
        size="SMALL",
        comment="Gateway for Oracle retail CDC ingestion"
    )
    
    print(f"✅ Gateway created: {gateway.name}")
    print(f"   Status: {gateway.status}")
    print(f"   Gateway ID: {gateway.gateway_id}")
except Exception as e:
    print("⚠️ SDK creation not available. Please create gateway via UI as described above.")
    print(f"   Details: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Gateway Provisioning

# COMMAND ----------

# Check gateway status
import time

def check_gateway_status(gateway_name="retail_ingestion_gateway", max_wait=300):
    """Poll gateway status until running or timeout"""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            gateways = list(w.lakeflow.gateways.list())
            matching = [g for g in gateways if g.name == gateway_name]
            
            if matching:
                gateway = matching[0]
                print(f"Gateway Status: {gateway.status}")
                
                if gateway.status == "RUNNING":
                    print("✅ Gateway is running and ready!")
                    return True
                elif gateway.status == "FAILED":
                    print("❌ Gateway provisioning failed. Check gateway logs.")
                    return False
                else:
                    print(f"⏳ Gateway provisioning in progress... (status: {gateway.status})")
                    time.sleep(30)
            else:
                print(f"⚠️ Gateway '{gateway_name}' not found. Please create via UI.")
                return False
        
        print(f"⏱️ Timeout: Gateway did not reach RUNNING state in {max_wait}s")
        return False
    except Exception as e:
        print(f"⚠️ Unable to check gateway status via SDK: {str(e)}")
        print("   Please check gateway status manually in Databricks UI")
        return None

# Run status check
check_gateway_status()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Lakeflow Connect Ingestion Pipeline
# MAGIC
# MAGIC ### About Ingestion Pipelines
# MAGIC
# MAGIC An **ingestion pipeline** is a Delta Live Tables (DLT) workflow that:
# MAGIC - Reads staged CDC data from the volume
# MAGIC - Applies INSERT/UPDATE/DELETE operations
# MAGIC - Maintains bronze layer Delta tables
# MAGIC - Handles schema evolution and late-arriving data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Pipeline Configuration

# COMMAND ----------

# Pipeline configuration
pipeline_config = {
    "name": "retail_ingestion_pipeline",
    "storage": "/pipelines/retail_ingestion",
    "target_catalog": "retail_analytics",
    "target_schema": "bronze",
    "source_tables": [
        {
            "source_table": "RETAIL.CUSTOMERS",
            "target_table": "customers",
            "primary_keys": ["CUSTOMER_ID"]
        },
        {
            "source_table": "RETAIL.ORDERS",
            "target_table": "orders",
            "primary_keys": ["ORDER_ID"]
        },
        {
            "source_table": "RETAIL.PRODUCTS",
            "target_table": "products",
            "primary_keys": ["PRODUCT_ID"]
        }
    ]
}

print("Pipeline Configuration:")
print(f"  Name: {pipeline_config['name']}")
print(f"  Target: {pipeline_config['target_catalog']}.{pipeline_config['target_schema']}")
print(f"  Tables to ingest: {len(pipeline_config['source_tables'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Pipeline via Databricks UI
# MAGIC
# MAGIC **Follow these steps to create the pipeline:**
# MAGIC
# MAGIC 1. Navigate to **Data Engineering** → **Delta Live Tables** in the left sidebar
# MAGIC 2. Click **Create Pipeline**
# MAGIC 3. Configure pipeline:
# MAGIC    - **Pipeline Name:** `retail_ingestion_pipeline`
# MAGIC    - **Product Edition:** Select `Advanced` or `Core` (Advanced recommended for production)
# MAGIC    - **Pipeline Mode:** Select `Triggered` (or `Continuous` for real-time)
# MAGIC    - **Notebook Libraries:** Leave empty for now (Lakeflow Connect auto-generates)
# MAGIC    - **Target:** `retail_analytics.bronze`
# MAGIC    - **Storage Location:** `/pipelines/retail_ingestion`
# MAGIC    - **Cluster Mode:** `Fixed Size` with 1-2 workers
# MAGIC 4. Click **Create**
# MAGIC 5. Add **Configuration**:
# MAGIC    - Key: `lakeflow.connect.gateway`, Value: `retail_ingestion_gateway`
# MAGIC    - Key: `lakeflow.connect.source_tables`, Value: `RETAIL.CUSTOMERS,RETAIL.ORDERS,RETAIL.PRODUCTS`
# MAGIC 6. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Use Lakeflow Connect Wizard
# MAGIC
# MAGIC **Simpler UI-guided approach:**
# MAGIC
# MAGIC 1. Navigate to **Data Ingestion** → **Lakeflow Connect**
# MAGIC 2. Select your `retail_ingestion_gateway`
# MAGIC 3. Click **Add Tables**
# MAGIC 4. Select tables to ingest:
# MAGIC    - ☑️ RETAIL.CUSTOMERS
# MAGIC    - ☑️ RETAIL.ORDERS
# MAGIC    - ☑️ RETAIL.PRODUCTS
# MAGIC 5. Configure destination:
# MAGIC    - **Target Catalog:** `retail_analytics`
# MAGIC    - **Target Schema:** `bronze`
# MAGIC 6. Review and click **Start Ingestion**
# MAGIC 7. Pipeline will be auto-created and started

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Run Initial Snapshot
# MAGIC
# MAGIC ### About the Snapshot Phase
# MAGIC
# MAGIC The first pipeline run performs a **snapshot (full load)** of each table:
# MAGIC - Captures current state at specific SCN
# MAGIC - All rows treated as INSERT operations
# MAGIC - Establishes baseline for incremental CDC
# MAGIC - Duration depends on table size (expect 5-15 minutes for workshop data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Pipeline Run
# MAGIC
# MAGIC **Option 1: Via UI**
# MAGIC 1. Navigate to your pipeline in **Delta Live Tables**
# MAGIC 2. Click **Start** button
# MAGIC 3. Monitor progress in the pipeline graph view
# MAGIC
# MAGIC **Option 2: Via SDK**

# COMMAND ----------

# Trigger pipeline run via SDK
try:
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    
    # Find pipeline by name
    pipelines = list(w.pipelines.list_pipelines())
    retail_pipeline = [p for p in pipelines if p.name == "retail_ingestion_pipeline"]
    
    if retail_pipeline:
        pipeline_id = retail_pipeline[0].pipeline_id
        
        # Start pipeline
        update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
        
        print(f"✅ Pipeline started!")
        print(f"   Pipeline ID: {pipeline_id}")
        print(f"   Update ID: {update.update_id}")
        print(f"\n   Monitor progress at:")
        print(f"   https://<your-workspace>/#joblist/pipelines/{pipeline_id}")
    else:
        print("⚠️ Pipeline not found. Please create via UI first.")
except Exception as e:
    print(f"⚠️ Unable to start pipeline via SDK: {str(e)}")
    print("   Please start pipeline manually in Databricks UI")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Snapshot Progress

# COMMAND ----------

# Monitor pipeline execution
import time

def monitor_pipeline_run(pipeline_name="retail_ingestion_pipeline", check_interval=30):
    """Monitor pipeline until completion"""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        pipelines = list(w.pipelines.list_pipelines())
        retail_pipeline = [p for p in pipelines if p.name == pipeline_name]
        
        if not retail_pipeline:
            print(f"⚠️ Pipeline '{pipeline_name}' not found")
            return
        
        pipeline_id = retail_pipeline[0].pipeline_id
        
        print("Monitoring pipeline execution...")
        print("(This may take 5-15 minutes for initial snapshot)\n")
        
        while True:
            updates = w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=1)
            if updates:
                latest_update = list(updates)[0]
                state = latest_update.state.value if latest_update.state else "UNKNOWN"
                
                print(f"[{time.strftime('%H:%M:%S')}] Pipeline state: {state}")
                
                if state in ["COMPLETED", "FAILED", "CANCELED"]:
                    if state == "COMPLETED":
                        print("\n✅ Pipeline completed successfully!")
                    else:
                        print(f"\n❌ Pipeline ended with state: {state}")
                    break
                
                time.sleep(check_interval)
            else:
                print("No updates found. Pipeline may not have started yet.")
                time.sleep(check_interval)
    except Exception as e:
        print(f"⚠️ Unable to monitor pipeline: {str(e)}")
        print("   Please monitor progress manually in Databricks UI")

# Start monitoring (comment out if you prefer to monitor via UI)
# monitor_pipeline_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Validate Bronze Layer Tables
# MAGIC
# MAGIC ### Check Tables Were Created

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in bronze schema
# MAGIC SHOW TABLES IN retail_analytics.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Row Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare row counts between Oracle and bronze tables
# MAGIC SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*) FROM retail_analytics.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*) FROM retail_analytics.bronze.products;
# MAGIC
# MAGIC -- Expected results (from Lab 2):
# MAGIC -- customers: 101 (100 + 1 test insert)
# MAGIC -- orders: 200
# MAGIC -- products: 50

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Sample Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View first 10 customers
# MAGIC SELECT * FROM retail_analytics.bronze.customers LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View first 10 orders
# MAGIC SELECT * FROM retail_analytics.bronze.orders LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View first 10 products
# MAGIC SELECT * FROM retail_analytics.bronze.products LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check CDC Metadata Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect CDC metadata columns
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   FIRST_NAME,
# MAGIC   EMAIL,
# MAGIC   _commit_timestamp,  -- When change was committed in Oracle
# MAGIC   _rescued_data       -- Handles schema evolution
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC ORDER BY _commit_timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Table Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Delta table details
# MAGIC DESCRIBE DETAIL retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY retail_analytics.bronze.customers LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate Data Lineage

# COMMAND ----------

# Visualize data lineage in Unity Catalog
# Navigate to: Data > retail_analytics > bronze > customers
# Click on "Lineage" tab to see:
#   Oracle RETAIL.CUSTOMERS → Gateway → Volume → Pipeline → bronze.customers

print("To view data lineage:")
print("1. Navigate to Data Explorer in Databricks UI")
print("2. Browse to retail_analytics > bronze > customers")
print("3. Click 'Lineage' tab")
print("4. Observe connection from Oracle source to bronze table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Test Reconciliation Query

# COMMAND ----------

# Compare Oracle and Bronze row counts programmatically
def reconcile_table_counts(oracle_table, bronze_table):
    """Compare row counts between Oracle and Bronze"""
    
    # Query Oracle
    oracle_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:oracle:thin:@{oracle_config['host']}:{oracle_config['port']}/{oracle_config['service_name']}") \
        .option("dbtable", f"(SELECT COUNT(*) AS cnt FROM {oracle_table})") \
        .option("user", oracle_config['username']) \
        .option("password", oracle_config['password']) \
        .option("driver", "oracle.jdbc.OracleDriver") \
        .load()
    
    oracle_count = oracle_df.first().cnt
    
    # Query Bronze
    bronze_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {bronze_table}").first().cnt
    
    # Compare
    diff = abs(oracle_count - bronze_count)
    match = "✅ MATCH" if diff == 0 else f"⚠️ DIFF: {diff}"
    
    print(f"{oracle_table} → {bronze_table}")
    print(f"  Oracle: {oracle_count:,} rows")
    print(f"  Bronze: {bronze_count:,} rows")
    print(f"  Status: {match}\n")
    
    return diff == 0

# Run reconciliation for all tables
print("Row Count Reconciliation:")
print("=" * 50)
customers_ok = reconcile_table_counts("RETAIL.CUSTOMERS", "retail_analytics.bronze.customers")
orders_ok = reconcile_table_counts("RETAIL.ORDERS", "retail_analytics.bronze.orders")
products_ok = reconcile_table_counts("RETAIL.PRODUCTS", "retail_analytics.bronze.products")

if all([customers_ok, orders_ok, products_ok]):
    print("✅ All tables reconciled successfully!")
else:
    print("⚠️ Some tables have mismatches. Check pipeline logs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary and Next Steps
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ **Unity Catalog Resources Created:**
# MAGIC - Catalog: `retail_analytics`
# MAGIC - Schemas: `landing`, `bronze`
# MAGIC - Volume: `retail_analytics.landing.cdc_volume`
# MAGIC
# MAGIC ✅ **Lakeflow Connect Configured:**
# MAGIC - Unity Catalog connection to Oracle
# MAGIC - Ingestion gateway provisioned
# MAGIC - Ingestion pipeline created
# MAGIC
# MAGIC ✅ **Initial Snapshot Completed:**
# MAGIC - CUSTOMERS table: 101 rows
# MAGIC - ORDERS table: 200 rows
# MAGIC - PRODUCTS table: 50 rows
# MAGIC
# MAGIC ✅ **Bronze Tables Validated:**
# MAGIC - Row counts match Oracle source
# MAGIC - CDC metadata columns present
# MAGIC - Data lineage visible in Unity Catalog
# MAGIC
# MAGIC ### Pipeline Status Check

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Final validation query
# MAGIC SELECT 
# MAGIC   'Lakeflow Connect Pipeline' AS component,
# MAGIC   'Operational' AS status,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.bronze.customers) AS customers,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.bronze.orders) AS orders,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.bronze.products) AS products,
# MAGIC   CURRENT_TIMESTAMP() AS validated_at;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next Steps
# MAGIC
# MAGIC **Congratulations!** You've successfully set up a complete Lakeflow Connect CDC pipeline from Oracle to Databricks.
# MAGIC
# MAGIC **What's happening now:**
# MAGIC - Your ingestion gateway is monitoring Oracle redo logs
# MAGIC - Changes are being captured incrementally
# MAGIC - Bronze tables will automatically update when Oracle data changes
# MAGIC
# MAGIC **Proceed to Lab 5** (skip Lecture 4 if needed, or review it first) where you'll:
# MAGIC - Perform INSERT, UPDATE, DELETE operations in Oracle
# MAGIC - Validate incremental CDC captures all changes
# MAGIC - Monitor CDC latency
# MAGIC - Test exactly-once semantics
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC If you encounter issues, check:
# MAGIC 1. Gateway status is "RUNNING"
# MAGIC 2. Pipeline completed without errors
# MAGIC 3. Oracle supplemental logging is enabled (from Lab 2)
# MAGIC 4. Network connectivity between Databricks and Oracle
# MAGIC 5. CDC_USER has correct permissions

# COMMAND ----------

# MAGIC %md
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
