# Troubleshooting Guide - Lakeflow Connect SQL Server CDC Workshop

This comprehensive guide covers common issues you may encounter during the Lakeflow Connect SQL Server CDC workshop and their solutions.

---

## Table of Contents

1. [SQL Server CDC Configuration Issues](#sql-server-cdc-configuration-issues)
2. [Connectivity and Network Issues](#connectivity-and-network-issues)
3. [Lakeflow Connect Gateway Issues](#lakeflow-connect-gateway-issues)
4. [Pipeline Execution Issues](#pipeline-execution-issues)
5. [Data Synchronization Issues](#data-synchronization-issues)
6. [Schema Evolution Issues](#schema-evolution-issues)
7. [Performance and Resource Issues](#performance-and-resource-issues)
8. [Data Quality Issues](#data-quality-issues)

---

## SQL Server CDC Configuration Issues

### Issue 1.1: Permission Denied When Enabling CDC

**Symptoms:**
```
Msg 22902, Level 16, State 1
You do not have permission to run sp_cdc_enable_db
```

**Root Cause:** User lacks sysadmin or db_owner permissions.

**Solutions:**

**Option A: Check Current Permissions**
```sql
-- Check your permissions
SELECT 
    USER_NAME() AS current_user,
    IS_SRVROLEMEMBER('sysadmin') AS is_sysadmin,
    IS_MEMBER('db_owner') AS is_db_owner;
```

**Option B: Grant Required Permissions (have an admin run)**
```sql
-- Grant sysadmin role
ALTER SERVER ROLE sysadmin ADD MEMBER [YourUserName];

-- OR grant db_owner role
USE RetailDB;
ALTER ROLE db_owner ADD MEMBER [YourUserName];
```

---

### Issue 1.2: CDC Enable Fails - Recovery Mode Not FULL

**Symptoms:**
```
CDC cannot be enabled on database 'RetailDB' because it is not in FULL recovery mode
```

**Root Cause:** Database must be in FULL recovery mode for CDC.

**Solution:**
```sql
-- Change recovery mode to FULL
ALTER DATABASE RetailDB SET RECOVERY FULL;

-- Verify
SELECT name, recovery_model_desc 
FROM sys.databases 
WHERE name = 'RetailDB';
```

---

### Issue 1.3: SQL Server Agent Not Running

**Symptoms:**
- CDC tables created but no changes captured
- `sp_cdc_help_jobs` shows jobs but they never execute

**Root Cause:** SQL Server Agent service is stopped (required for CDC capture/cleanup jobs).

**Solutions:**

**On Windows:**
1. Open **SQL Server Configuration Manager**
2. Navigate to **SQL Server Services**
3. Right-click **SQL Server Agent** → **Start**

**Verify Agent Status:**
```sql
EXEC xp_servicecontrol 'QueryState', 'SQLServerAGENT';
-- Should return: Running.
```

**On Azure SQL Database:**
- Agent is managed automatically; no action needed

---

### Issue 1.4: Transaction Log Full

**Symptoms:**
```
The transaction log for database 'RetailDB' is full due to 'REPLICATION'
```

**Root Cause:** CDC prevents log truncation until changes are captured.

**Solutions:**

**Immediate Fix:**
```sql
-- Back up the log to allow truncation
BACKUP LOG RetailDB TO DISK = 'C:\Backup\RetailDB_log.trn';
```

**Long-term Solution:**
```sql
-- Increase log file size
ALTER DATABASE RetailDB
MODIFY FILE (
    NAME = RetailDB_log,
    SIZE = 2GB,
    MAXSIZE = 10GB,
    FILEGROWTH = 512MB
);

-- Monitor log size
SELECT 
    name,
    (size * 8.0 / 1024) AS size_mb,
    (FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024) AS used_mb
FROM sys.database_files
WHERE type_desc = 'LOG';
```

---

## Connectivity and Network Issues

### Issue 2.1: Cannot Connect to SQL Server from Databricks

**Symptoms:**
```
Connection timeout to SQL Server at [hostname]:1433
java.net.ConnectException: Connection refused
```

**Root Cause:** Network connectivity, firewall, or SQL Server configuration issues.

**Diagnostic Steps:**

**1. Test from Databricks Notebook:**
```python
# Test connectivity
import socket

hostname = "your-sqlserver-host.database.windows.net"
port = 1433

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex((hostname, port))
    if result == 0:
        print("✓ Port is reachable")
    else:
        print(f"✗ Port is not reachable (error code: {result})")
    sock.close()
except Exception as e:
    print(f"✗ Connection test failed: {e}")
```

**2. Verify SQL Server Allows Remote Connections:**
```sql
-- Run on SQL Server
EXEC sp_configure 'remote access';
-- Should show: run_value = 1

-- Enable if needed
EXEC sp_configure 'remote access', 1;
RECONFIGURE;
```

**3. Check SQL Server TCP/IP Enabled:**
- Open **SQL Server Configuration Manager**
- Navigate to **SQL Server Network Configuration** → **Protocols**
- Ensure **TCP/IP** is **Enabled**
- Restart SQL Server service

**4. Verify Firewall Rules:**

**Azure SQL Database:**
- Add Databricks workspace IP ranges to Azure SQL firewall
- Or enable "Allow Azure services" (temporary for testing)

**On-Premises/AWS RDS:**
- Configure security groups to allow port 1433 from Databricks IPs
- Check network ACLs and VPN/PrivateLink configuration

---

### Issue 2.2: Authentication Failed

**Symptoms:**
```
Login failed for user 'your_username'
```

**Root Cause:** Invalid credentials or authentication method mismatch.

**Solutions:**

**Option A: Verify Credentials in Databricks Connection:**
```sql
-- Test connection
SELECT * FROM sqlserver_retail_connection.RetailDB.dbo.customers LIMIT 1;
```

**Option B: Test Credentials Directly:**
```python
# Test with explicit credentials
from pyspark.sql import SparkSession

jdbc_url = "jdbc:sqlserver://your-host:1433;databaseName=RetailDB"
properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

try:
    df = spark.read.jdbc(url=jdbc_url, table="dbo.customers", properties=properties)
    print("✓ Authentication successful")
    df.show(5)
except Exception as e:
    print(f"✗ Authentication failed: {e}")
```

**Option C: Check SQL Server Authentication Mode:**
```sql
-- Ensure SQL Server authentication is enabled
SELECT SERVERPROPERTY('IsIntegratedSecurityOnly');
-- Should return 0 for mixed mode (SQL + Windows auth)
```

---

## Lakeflow Connect Gateway Issues

### Issue 3.1: Gateway Stuck in "Provisioning" Status

**Symptoms:**
- Gateway shows "Provisioning" for >10 minutes
- Never reaches "Running" state

**Root Cause:** Insufficient capacity, quota limits, or regional availability.

**Solutions:**

**1. Check Workspace Quotas:**
- Navigate to **Admin Console** → **Quotas**
- Verify you have available VM quota

**2. Retry Creation:**
```python
# Delete stuck gateway via UI or API
# Then recreate with different VM size

gateway_config = {
    "name": "retail_ingestion_gateway_v2",
    "vm_size": "small",  # Try different size
    "staging_location": "retail_analytics.landing"
}
```

**3. Check Region Availability:**
- Try creating gateway in different availability zone
- Contact Databricks support if issue persists

---

### Issue 3.2: Gateway Connection Failed

**Symptoms:**
```
Gateway status: CONNECTION_ERROR
Unable to connect to source database
```

**Root Cause:** Network, credentials, or source database issues.

**Diagnostic Steps:**

**1. Verify Gateway Status:**
```sql
SELECT 
  gateway_name,
  status,
  last_error_message,
  last_successful_connection
FROM system.lakeflow.gateways
WHERE gateway_name = 'retail_ingestion_gateway';
```

**2. Test Connection from Gateway:**
- Ensure Unity Catalog connection is valid
- Verify network connectivity (VPN/PrivateLink)
- Check SQL Server is accepting connections

**3. Review Gateway Logs:**
- Navigate to **Data** → **Ingestion** → **Gateways**
- Click on gateway name → **Logs**
- Look for detailed error messages

---

## Pipeline Execution Issues

### Issue 4.1: Pipeline Fails with "Schema Mismatch"

**Symptoms:**
```
Schema mismatch detected
Column 'loyalty_points' not found in target table
```

**Root Cause:** Source schema changed but bronze table schema not updated.

**Solutions:**

**Option A: Enable Auto-Merge (Recommended):**
```sql
-- Enable schema evolution
ALTER TABLE retail_analytics.bronze.customers 
SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');

ALTER TABLE retail_analytics.bronze.orders 
SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');

ALTER TABLE retail_analytics.bronze.products 
SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');
```

**Option B: Manual Schema Sync:**
```sql
-- Add missing column to bronze table
ALTER TABLE retail_analytics.bronze.customers 
ADD COLUMN loyalty_points INT;

-- Backfill with default value if needed
UPDATE retail_analytics.bronze.customers 
SET loyalty_points = 0 
WHERE loyalty_points IS NULL;
```

**Option C: Re-run Snapshot Mode:**
- Change pipeline to snapshot mode
- Re-run to rebuild tables with new schema
- Switch back to incremental mode

---

### Issue 4.2: Pipeline Fails - LSN Not Found

**Symptoms:**
```
Checkpoint LSN no longer available in transaction log
CDC checkpoint expired
```

**Root Cause:** Transaction log cleanup removed CDC data before pipeline could process it.

**Solutions:**

**Immediate:**
```sql
-- Re-run pipeline in SNAPSHOT mode to re-establish baseline
```

**Prevention:**

**1. Increase CDC Retention:**
```sql
-- On SQL Server
ALTER DATABASE RetailDB
SET CHANGE_TRACKING (CHANGE_RETENTION = 7 DAYS);
```

**2. More Frequent Backups:**
```sql
-- Schedule regular transaction log backups
BACKUP LOG RetailDB TO DISK = 'backup_path';
```

**3. Monitor Checkpoint Age:**
```sql
-- Databricks - alert if checkpoint >6 hours old
SELECT 
  table_name,
  last_checkpoint_time,
  TIMESTAMPDIFF(HOUR, last_checkpoint_time, CURRENT_TIMESTAMP()) as hours_old
FROM system.lakeflow.checkpoints
WHERE hours_old > 6;
```

---

### Issue 4.3: Pipeline Shows 0 Records Processed

**Symptoms:**
- Pipeline succeeds but processes 0 records
- No changes appearing in bronze tables despite source modifications

**Root Cause:** CDC not capturing changes or pipeline not reading correct checkpoint.

**Diagnostic Steps:**

**1. Verify CDC Enabled on SQL Server:**
```sql
-- Check CDC status
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'RetailDB';
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo');

-- Check CDC capture job
EXEC sys.sp_cdc_help_jobs;
```

**2. Verify Changes in CDC Tables:**
```sql
-- Check if changes are in CDC tables
SELECT COUNT(*) FROM cdc.dbo_customers_CT;
SELECT COUNT(*) FROM cdc.dbo_orders_CT;
SELECT COUNT(*) FROM cdc.dbo_products_CT;

-- View recent changes
SELECT TOP 10 * FROM cdc.dbo_customers_CT ORDER BY __$start_lsn DESC;
```

**3. Check Staging Volume:**
```python
# List files in staging volume
volume_path = "/Volumes/retail_analytics/landing/ingestion_volume"
files = dbutils.fs.ls(volume_path)
for file in files:
    print(f"{file.name}: {file.size / 1024:.2f} KB")
```

**Solutions:**
- Restart SQL Server Agent if CDC jobs stuck
- Re-run pipeline with force refresh
- Verify pipeline is in incremental mode (not snapshot)

---

## Data Synchronization Issues

### Issue 5.1: Changes Not Appearing in Bronze Tables

**Symptoms:**
- Made changes in SQL Server
- Pipeline runs successfully  
- Changes don't appear in Databricks

**Root Cause:** Timing, caching, or incomplete sync.

**Diagnostic Steps:**

**1. Check Pipeline Execution Time:**
```sql
-- Ensure pipeline ran AFTER you made changes
SELECT 
  execution_id,
  start_time,
  end_time,
  execution_status,
  records_processed
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY start_time DESC
LIMIT 5;
```

**2. Compare Timestamps:**
```sql
-- Check when data was last updated in bronze
SELECT 
  MAX(_commit_timestamp) as last_bronze_update,
  CURRENT_TIMESTAMP() as now,
  TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as minutes_ago
FROM retail_analytics.bronze.customers;
```

**3. Query with Cache Refresh:**
```sql
-- Refresh table cache
REFRESH TABLE retail_analytics.bronze.customers;

-- Query again
SELECT * FROM retail_analytics.bronze.customers
WHERE customer_id = 10001;
```

**Solutions:**
- Wait 5-10 minutes after making changes
- Manually trigger pipeline run
- Check CDC capture lag on SQL Server

---

### Issue 5.2: Delete Operations Not Reflected

**Symptoms:**
- Deleted records in SQL Server
- Records still exist in bronze tables

**Root Cause:** CDC delete not captured or merge logic issue.

**Diagnostic Steps:**

**1. Verify Delete in SQL Server:**
```sql
-- Confirm record deleted at source
SELECT * FROM dbo.customers WHERE customer_id = 10002;
-- Should return 0 rows
```

**2. Check CDC Table for Delete Operation:**
```sql
-- Look for __$operation = 1 (DELETE)
SELECT * 
FROM cdc.dbo_customers_CT 
WHERE customer_id = 10002 
  AND __$operation = 1;
```

**3. Check Bronze Table:**
```sql
-- Should also return 0 rows if delete propagated
SELECT * 
FROM retail_analytics.bronze.customers 
WHERE customer_id = 10002;
```

**Solutions:**

**If CDC captured but bronze not updated:**
```sql
-- Force full resync with snapshot mode
-- OR manually delete record
DELETE FROM retail_analytics.bronze.customers 
WHERE customer_id = 10002;
```

**If CDC didn't capture:**
- Verify CDC capture job running
- Check transaction log retention
- Manually trigger CDC cleanup job

---

## Schema Evolution Issues

### Issue 6.1: New Column Not Appearing in Bronze Table

**Symptoms:**
- Added column to SQL Server table
- Column doesn't appear in Databricks bronze table

**Root Cause:** Schema evolution not enabled or not triggered.

**Solutions:**

**1. Enable Schema Evolution:**
```sql
ALTER TABLE retail_analytics.bronze.customers 
SET TBLPROPERTIES (
  'delta.autoMerge.enabled' = 'true',
  'delta.columnMapping.mode' = 'name'
);
```

**2. Manually Add Column:**
```sql
-- If auto-merge doesn't work, add manually
ALTER TABLE retail_analytics.bronze.customers 
ADD COLUMN loyalty_points INT;
```

**3. Re-run Pipeline:**
- Trigger incremental run
- Verify new column appears

**4. Backfill Existing Rows:**
```sql
-- Set default value for existing rows
UPDATE retail_analytics.bronze.customers 
SET loyalty_points = 0 
WHERE loyalty_points IS NULL;
```

---

### Issue 6.2: Column Type Change Fails

**Symptoms:**
```
Cannot change column type from INT to BIGINT
Type mismatch error
```

**Root Cause:** Delta Lake doesn't support automatic type changes.

**Solutions:**

**Option A: Add New Column + Migrate:**
```sql
-- Add new column with new type
ALTER TABLE retail_analytics.bronze.customers 
ADD COLUMN customer_id_new BIGINT;

-- Copy data
UPDATE retail_analytics.bronze.customers 
SET customer_id_new = customer_id;

-- Drop old, rename new
ALTER TABLE retail_analytics.bronze.customers DROP COLUMN customer_id;
ALTER TABLE retail_analytics.bronze.customers RENAME COLUMN customer_id_new TO customer_id;
```

**Option B: Recreate Table:**
```python
# Backup existing data
spark.sql("""
CREATE TABLE retail_analytics.bronze.customers_backup 
AS SELECT * FROM retail_analytics.bronze.customers
""")

# Drop and recreate with new schema
spark.sql("DROP TABLE retail_analytics.bronze.customers")

# Re-run snapshot mode to rebuild with new schema
```

---

## Performance and Resource Issues

### Issue 7.1: Pipeline Runs Very Slowly

**Symptoms:**
- Pipeline duration >10x normal
- Snapshot mode taking hours

**Root Cause:** Resource constraints, large data volumes, or inefficient queries.

**Solutions:**

**1. Check Resource Utilization:**
```sql
-- View pipeline metrics
SELECT 
  execution_id,
  start_time,
  end_time,
  TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes,
  records_processed,
  records_processed / TIMESTAMPDIFF(MINUTE, start_time, end_time) as records_per_minute
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY start_time DESC
LIMIT 10;
```

**2. Optimize Bronze Tables:**
```sql
-- Run OPTIMIZE with Z-ordering
OPTIMIZE retail_analytics.bronze.customers ZORDER BY (customer_id, state);
OPTIMIZE retail_analytics.bronze.orders ZORDER BY (customer_id, order_date);
OPTIMIZE retail_analytics.bronze.products ZORDER BY (category, product_id);
```

**3. Scale Up Gateway:**
- Navigate to gateway settings
- Increase VM size (Small → Medium → Large)
- Save and restart gateway

**4. Tune SQL Server:**
```sql
-- Add indexes on CDC tables (if needed)
-- Monitor SQL Server query performance
-- Ensure adequate resources allocated
```

---

### Issue 7.2: Staging Volume Growing Too Large

**Symptoms:**
- Unity Catalog volume consuming excessive storage
- Cost increasing rapidly

**Root Cause:** Staging files not cleaned up after processing.

**Solutions:**

**1. Implement Cleanup Job:**
```python
# Schedule this as daily job
from datetime import datetime, timedelta

def cleanup_staging_volume():
    volume_path = "/Volumes/retail_analytics/landing/ingestion_volume/"
    cutoff_date = datetime.now() - timedelta(days=7)
    
    files = dbutils.fs.ls(volume_path)
    deleted_count = 0
    
    for file_info in files:
        if file_info.modificationTime < cutoff_date.timestamp() * 1000:
            try:
                dbutils.fs.rm(file_info.path, recurse=True)
                deleted_count += 1
            except Exception as e:
                print(f"Error deleting {file_info.path}: {e}")
    
    print(f"✓ Deleted {deleted_count} old staging files")

# Run cleanup
cleanup_staging_volume()
```

**2. Configure Retention Policy:**
```python
# Set lifecycle policy on volume
# Automatically delete files older than 7 days
```

---

## Data Quality Issues

### Issue 8.1: Duplicate Records in Bronze Tables

**Symptoms:**
```sql
-- Multiple records for same primary key
SELECT customer_id, COUNT(*) 
FROM retail_analytics.bronze.customers 
GROUP BY customer_id 
HAVING COUNT(*) > 1;
```

**Root Cause:** Pipeline ran multiple times without proper MERGE logic or PK not enforced.

**Solutions:**

**1. Deduplicate Manually:**
```sql
-- Create temp table with unique records
CREATE OR REPLACE TEMP VIEW customers_dedup AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _commit_timestamp DESC) as rn
  FROM retail_analytics.bronze.customers
)
WHERE rn = 1;

-- Recreate table from deduped view
CREATE OR REPLACE TABLE retail_analytics.bronze.customers 
AS SELECT * FROM customers_dedup;
```

**2. Verify Pipeline Merge Logic:**
- Ensure pipeline configured with proper primary key
- Check Lakeflow Connect settings
- May need to recreate pipeline

---

### Issue 8.2: NULL Values in Required Fields

**Symptoms:**
```sql
SELECT COUNT(*) 
FROM retail_analytics.bronze.customers 
WHERE email IS NULL;
```

**Root Cause:** Source data quality issue or schema mismatch.

**Solutions:**

**1. Quarantine Invalid Records:**
```sql
-- Create quarantine table
CREATE TABLE IF NOT EXISTS retail_analytics.bronze.customers_quarantine (
  customer_id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  quarantine_reason STRING,
  quarantine_timestamp TIMESTAMP
);

-- Move invalid records
INSERT INTO retail_analytics.bronze.customers_quarantine
SELECT 
  customer_id,
  first_name,
  last_name,
  email,
  'Missing required email field' as quarantine_reason,
  CURRENT_TIMESTAMP()
FROM retail_analytics.bronze.customers
WHERE email IS NULL;

-- Remove from main table
DELETE FROM retail_analytics.bronze.customers WHERE email IS NULL;
```

**2. Add Validation in Source:**
```sql
-- On SQL Server, add constraints
ALTER TABLE customers 
ALTER COLUMN email VARCHAR(100) NOT NULL;
```

---

## Getting Additional Help

If issues persist after trying these solutions:

1. **Check Databricks Documentation:**
   - [Lakeflow Connect Guide](https://docs.databricks.com/ingestion/lakeflow-connect/)
   - [Delta Live Tables Docs](https://docs.databricks.com/delta-live-tables/)

2. **Contact Support:**
   - Databricks Support Portal
   - Include: Pipeline logs, error messages, execution IDs

3. **Community Resources:**
   - [Databricks Community Forums](https://community.databricks.com/)
   - Stack Overflow (tag: databricks, delta-lake)

4. **Workshop Resources:**
   - Re-review lecture notebooks for concepts
   - Check validation_queries.sql for diagnostic queries
   - Reference README.md for architecture details

---

**Happy Troubleshooting!** 🔧✨
