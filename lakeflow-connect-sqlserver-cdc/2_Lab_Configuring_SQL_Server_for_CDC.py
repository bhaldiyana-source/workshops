# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Configuring SQL Server for CDC Operations
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll configure a SQL Server database to enable Change Data Capture (CDC) operations. You'll enable CDC at the database level, configure change tracking on individual tables, set up transaction log retention, and verify that CDC is working correctly.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Enable CDC at the SQL Server database level
# MAGIC - Configure change tracking on individual tables
# MAGIC - Set up appropriate transaction log retention policies
# MAGIC - Verify CDC configuration and test change capture
# MAGIC - Troubleshoot common CDC configuration issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Access to a SQL Server instance (on-premises, AWS RDS, or Azure SQL)
# MAGIC - **sysadmin** or **db_owner** permissions on the database
# MAGIC - SQL Server Management Studio (SSMS), Azure Data Studio, or similar SQL client
# MAGIC - Database in FULL recovery mode (required for CDC)
# MAGIC
# MAGIC ## Duration
# MAGIC 15-20 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Prerequisites
# MAGIC
# MAGIC Before enabling CDC, let's verify your SQL Server environment meets requirements.
# MAGIC
# MAGIC ### 1.1 Check SQL Server Version
# MAGIC
# MAGIC Connect to your SQL Server instance and run:
# MAGIC
# MAGIC ```sql
# MAGIC -- Check SQL Server version (CDC requires SQL Server 2008 or later)
# MAGIC SELECT @@VERSION AS sql_server_version;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Microsoft SQL Server 2019 (RTM) - 15.0.2000.5 (X64)
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** Version should be SQL Server 2008 or later
# MAGIC
# MAGIC ### 1.2 Verify Database Recovery Mode
# MAGIC
# MAGIC ```sql
# MAGIC -- Check recovery mode (must be FULL for CDC)
# MAGIC SELECT 
# MAGIC     name AS database_name,
# MAGIC     recovery_model_desc AS recovery_mode,
# MAGIC     CASE 
# MAGIC         WHEN recovery_model_desc = 'FULL' THEN '✓ Ready for CDC'
# MAGIC         ELSE '✗ Must change to FULL recovery mode'
# MAGIC     END AS cdc_ready
# MAGIC FROM sys.databases
# MAGIC WHERE name = 'RetailDB';  -- Replace with your database name
# MAGIC ```
# MAGIC
# MAGIC **If not in FULL mode, change it:**
# MAGIC ```sql
# MAGIC ALTER DATABASE RetailDB SET RECOVERY FULL;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** Database should be in FULL recovery mode
# MAGIC
# MAGIC ### 1.3 Check Your Permissions
# MAGIC
# MAGIC ```sql
# MAGIC -- Verify you have sysadmin or db_owner role
# MAGIC SELECT 
# MAGIC     USER_NAME() AS current_user,
# MAGIC     IS_SRVROLEMEMBER('sysadmin') AS is_sysadmin,
# MAGIC     IS_MEMBER('db_owner') AS is_db_owner;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC current_user  | is_sysadmin | is_db_owner
# MAGIC --------------|-------------|------------
# MAGIC sa            | 1           | 1
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** Either is_sysadmin or is_db_owner should be 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Sample Database and Tables
# MAGIC
# MAGIC If you don't already have a database, create the RetailDB sample database.
# MAGIC
# MAGIC ### 2.1 Create Database
# MAGIC
# MAGIC ```sql
# MAGIC -- Create the retail database
# MAGIC IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'RetailDB')
# MAGIC BEGIN
# MAGIC     CREATE DATABASE RetailDB;
# MAGIC     PRINT 'Database RetailDB created successfully';
# MAGIC END
# MAGIC ELSE
# MAGIC BEGIN
# MAGIC     PRINT 'Database RetailDB already exists';
# MAGIC END
# MAGIC
# MAGIC -- Switch to the database
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 2.2 Create Customers Table
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Create customers table
# MAGIC IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customers')
# MAGIC BEGIN
# MAGIC     CREATE TABLE customers (
# MAGIC         customer_id INT PRIMARY KEY,
# MAGIC         first_name VARCHAR(50) NOT NULL,
# MAGIC         last_name VARCHAR(50) NOT NULL,
# MAGIC         email VARCHAR(100) UNIQUE,
# MAGIC         phone VARCHAR(20),
# MAGIC         address VARCHAR(200),
# MAGIC         city VARCHAR(50),
# MAGIC         state VARCHAR(2),
# MAGIC         zip_code VARCHAR(10),
# MAGIC         created_date DATETIME DEFAULT GETDATE(),
# MAGIC         last_updated DATETIME DEFAULT GETDATE()
# MAGIC     );
# MAGIC     PRINT 'Table customers created successfully';
# MAGIC END
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 2.3 Create Orders Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Create orders table
# MAGIC IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders')
# MAGIC BEGIN
# MAGIC     CREATE TABLE orders (
# MAGIC         order_id INT PRIMARY KEY,
# MAGIC         customer_id INT,
# MAGIC         order_date DATETIME DEFAULT GETDATE(),
# MAGIC         order_status VARCHAR(20) DEFAULT 'PENDING',
# MAGIC         total_amount DECIMAL(10, 2),
# MAGIC         shipping_address VARCHAR(200),
# MAGIC         payment_method VARCHAR(50),
# MAGIC         created_date DATETIME DEFAULT GETDATE(),
# MAGIC         last_updated DATETIME DEFAULT GETDATE(),
# MAGIC         FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
# MAGIC     );
# MAGIC     PRINT 'Table orders created successfully';
# MAGIC END
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 2.4 Create Products Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Create products table
# MAGIC IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'products')
# MAGIC BEGIN
# MAGIC     CREATE TABLE products (
# MAGIC         product_id INT PRIMARY KEY,
# MAGIC         product_name VARCHAR(100) NOT NULL,
# MAGIC         category VARCHAR(50),
# MAGIC         price DECIMAL(10, 2),
# MAGIC         stock_quantity INT DEFAULT 0,
# MAGIC         supplier VARCHAR(100),
# MAGIC         description TEXT,
# MAGIC         created_date DATETIME DEFAULT GETDATE(),
# MAGIC         last_updated DATETIME DEFAULT GETDATE()
# MAGIC     );
# MAGIC     PRINT 'Table products created successfully';
# MAGIC END
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** All three tables (customers, orders, products) created successfully

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Insert Sample Data
# MAGIC
# MAGIC Let's add some sample records to test with.
# MAGIC
# MAGIC ### 3.1 Insert Sample Customers
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Insert sample customers
# MAGIC INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, zip_code)
# MAGIC VALUES
# MAGIC     (1001, 'John', 'Doe', 'john.doe@email.com', '555-0101', 'Seattle', 'WA', '98101'),
# MAGIC     (1002, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', 'Portland', 'OR', '97201'),
# MAGIC     (1003, 'Bob', 'Johnson', 'bob.j@email.com', '555-0103', 'San Francisco', 'CA', '94102'),
# MAGIC     (1004, 'Alice', 'Williams', 'alice.w@email.com', '555-0104', 'Los Angeles', 'CA', '90001'),
# MAGIC     (1005, 'Charlie', 'Brown', 'charlie.b@email.com', '555-0105', 'Denver', 'CO', '80201');
# MAGIC
# MAGIC PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' customers inserted';
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 3.2 Insert Sample Products
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert sample products
# MAGIC INSERT INTO products (product_id, product_name, category, price, stock_quantity, supplier)
# MAGIC VALUES
# MAGIC     (2001, 'Laptop Pro 15', 'Electronics', 1299.99, 50, 'Tech Supplies Inc'),
# MAGIC     (2002, 'Wireless Mouse', 'Electronics', 29.99, 200, 'Tech Supplies Inc'),
# MAGIC     (2003, 'Office Chair', 'Furniture', 249.99, 75, 'Office Depot'),
# MAGIC     (2004, 'Standing Desk', 'Furniture', 599.99, 30, 'Office Depot'),
# MAGIC     (2005, 'Monitor 27 inch', 'Electronics', 349.99, 100, 'Tech Supplies Inc');
# MAGIC
# MAGIC PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' products inserted';
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 3.3 Insert Sample Orders
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert sample orders
# MAGIC INSERT INTO orders (order_id, customer_id, order_status, total_amount, payment_method)
# MAGIC VALUES
# MAGIC     (5001, 1001, 'PENDING', 1329.98, 'Credit Card'),
# MAGIC     (5002, 1002, 'PROCESSING', 249.99, 'PayPal'),
# MAGIC     (5003, 1003, 'SHIPPED', 949.98, 'Credit Card'),
# MAGIC     (5004, 1001, 'DELIVERED', 29.99, 'Debit Card'),
# MAGIC     (5005, 1004, 'PENDING', 599.99, 'Credit Card');
# MAGIC
# MAGIC PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' orders inserted';
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 3.4 Verify Data
# MAGIC
# MAGIC ```sql
# MAGIC -- Verify record counts
# MAGIC SELECT 'customers' AS table_name, COUNT(*) AS record_count FROM customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*) FROM orders
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*) FROM products;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC table_name  | record_count
# MAGIC ------------|-------------
# MAGIC customers   | 5
# MAGIC orders      | 5
# MAGIC products    | 5
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** All sample data inserted successfully

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Enable CDC at Database Level
# MAGIC
# MAGIC Now we'll enable CDC on the database. This is the first required step.
# MAGIC
# MAGIC ### 4.1 Enable CDC on Database
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Enable CDC at database level
# MAGIC EXEC sys.sp_cdc_enable_db;
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Command(s) completed successfully.
# MAGIC ```
# MAGIC
# MAGIC ### 4.2 Verify CDC is Enabled
# MAGIC
# MAGIC ```sql
# MAGIC -- Check if CDC is enabled on the database
# MAGIC SELECT 
# MAGIC     name AS database_name,
# MAGIC     is_cdc_enabled,
# MAGIC     CASE 
# MAGIC         WHEN is_cdc_enabled = 1 THEN '✓ CDC Enabled'
# MAGIC         ELSE '✗ CDC Not Enabled'
# MAGIC     END AS status
# MAGIC FROM sys.databases
# MAGIC WHERE name = 'RetailDB';
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC database_name | is_cdc_enabled | status
# MAGIC --------------|----------------|----------------
# MAGIC RetailDB      | 1              | ✓ CDC Enabled
# MAGIC ```
# MAGIC
# MAGIC ### 4.3 Verify CDC System Objects Created
# MAGIC
# MAGIC ```sql
# MAGIC -- CDC creates a new schema called 'cdc'
# MAGIC SELECT name, type_desc 
# MAGIC FROM sys.schemas 
# MAGIC WHERE name = 'cdc';
# MAGIC
# MAGIC -- Check CDC system tables
# MAGIC SELECT name 
# MAGIC FROM sys.tables 
# MAGIC WHERE schema_id = SCHEMA_ID('cdc')
# MAGIC ORDER BY name;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Schema 'cdc' exists
# MAGIC
# MAGIC System tables:
# MAGIC - cdc.captured_columns
# MAGIC - cdc.change_tables
# MAGIC - cdc.ddl_history
# MAGIC - cdc.index_columns
# MAGIC - cdc.lsn_time_mapping
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** CDC enabled at database level, cdc schema created

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Enable CDC on Individual Tables
# MAGIC
# MAGIC Now we'll enable CDC tracking on each table we want to capture changes from.
# MAGIC
# MAGIC ### 5.1 Enable CDC on Customers Table
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Enable CDC on customers table
# MAGIC EXEC sys.sp_cdc_enable_table
# MAGIC     @source_schema = N'dbo',
# MAGIC     @source_name = N'customers',
# MAGIC     @role_name = NULL,
# MAGIC     @supports_net_changes = 1;
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC **Parameter Explanation:**
# MAGIC - `@source_schema`: Schema containing the table (usually 'dbo')
# MAGIC - `@source_name`: Table name to enable CDC on
# MAGIC - `@role_name`: Database role for controlling access (NULL = no restriction)
# MAGIC - `@supports_net_changes`: 1 = Track net changes (recommended)
# MAGIC
# MAGIC ### 5.2 Enable CDC on Orders Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable CDC on orders table
# MAGIC EXEC sys.sp_cdc_enable_table
# MAGIC     @source_schema = N'dbo',
# MAGIC     @source_name = N'orders',
# MAGIC     @role_name = NULL,
# MAGIC     @supports_net_changes = 1;
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 5.3 Enable CDC on Products Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable CDC on products table
# MAGIC EXEC sys.sp_cdc_enable_table
# MAGIC     @source_schema = N'dbo',
# MAGIC     @source_name = N'products',
# MAGIC     @role_name = NULL,
# MAGIC     @supports_net_changes = 1;
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 5.4 Verify CDC Enabled on All Tables
# MAGIC
# MAGIC ```sql
# MAGIC -- Check which tables have CDC enabled
# MAGIC SELECT 
# MAGIC     SCHEMA_NAME(t.schema_id) AS schema_name,
# MAGIC     t.name AS table_name,
# MAGIC     t.is_tracked_by_cdc,
# MAGIC     CASE 
# MAGIC         WHEN t.is_tracked_by_cdc = 1 THEN '✓ CDC Enabled'
# MAGIC         ELSE '✗ CDC Not Enabled'
# MAGIC     END AS cdc_status
# MAGIC FROM sys.tables t
# MAGIC WHERE t.name IN ('customers', 'orders', 'products')
# MAGIC ORDER BY t.name;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC schema_name | table_name | is_tracked_by_cdc | cdc_status
# MAGIC ------------|------------|-------------------|----------------
# MAGIC dbo         | customers  | 1                 | ✓ CDC Enabled
# MAGIC dbo         | orders     | 1                 | ✓ CDC Enabled
# MAGIC dbo         | products   | 1                 | ✓ CDC Enabled
# MAGIC ```
# MAGIC
# MAGIC ### 5.5 Check CDC Change Tables Created
# MAGIC
# MAGIC ```sql
# MAGIC -- CDC creates shadow tables to store changes
# MAGIC SELECT 
# MAGIC     source_object_id,
# MAGIC     OBJECT_NAME(source_object_id) AS source_table,
# MAGIC     capture_instance,
# MAGIC     start_lsn,
# MAGIC     create_date
# MAGIC FROM cdc.change_tables
# MAGIC ORDER BY create_date;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC source_table | capture_instance      | start_lsn         | create_date
# MAGIC -------------|----------------------|-------------------|------------------
# MAGIC customers    | dbo_customers        | 0x00000027...     | 2024-01-15 10:30:00
# MAGIC orders       | dbo_orders           | 0x00000027...     | 2024-01-15 10:30:15
# MAGIC products     | dbo_products         | 0x00000027...     | 2024-01-15 10:30:30
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** CDC enabled on all three tables, change tables created

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Configure Change Tracking Settings
# MAGIC
# MAGIC Configure change retention and cleanup policies.
# MAGIC
# MAGIC ### 6.1 Set Change Retention Period
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Enable change tracking with 7-day retention
# MAGIC -- This is separate from CDC and provides additional metadata
# MAGIC ALTER DATABASE RetailDB
# MAGIC SET CHANGE_TRACKING = ON
# MAGIC (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC **Parameter Explanation:**
# MAGIC - `CHANGE_RETENTION`: How long to keep change metadata (7 days recommended)
# MAGIC - `AUTO_CLEANUP`: Automatically clean up old change data
# MAGIC
# MAGIC ### 6.2 Enable Change Tracking on Tables
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable change tracking on customers table
# MAGIC ALTER TABLE customers
# MAGIC ENABLE CHANGE_TRACKING
# MAGIC WITH (TRACK_COLUMNS_UPDATED = ON);
# MAGIC GO
# MAGIC
# MAGIC -- Enable change tracking on orders table
# MAGIC ALTER TABLE orders
# MAGIC ENABLE CHANGE_TRACKING
# MAGIC WITH (TRACK_COLUMNS_UPDATED = ON);
# MAGIC GO
# MAGIC
# MAGIC -- Enable change tracking on products table
# MAGIC ALTER TABLE products
# MAGIC ENABLE CHANGE_TRACKING
# MAGIC WITH (TRACK_COLUMNS_UPDATED = ON);
# MAGIC GO
# MAGIC ```
# MAGIC
# MAGIC ### 6.3 Verify Change Tracking Configuration
# MAGIC
# MAGIC ```sql
# MAGIC -- Check database-level change tracking
# MAGIC SELECT 
# MAGIC     name AS database_name,
# MAGIC     is_change_tracking_enabled,
# MAGIC     CASE 
# MAGIC         WHEN is_change_tracking_enabled = 1 THEN '✓ Enabled'
# MAGIC         ELSE '✗ Disabled'
# MAGIC     END AS status
# MAGIC FROM sys.databases
# MAGIC WHERE name = 'RetailDB';
# MAGIC
# MAGIC -- Check table-level change tracking
# MAGIC SELECT 
# MAGIC     OBJECT_NAME(object_id) AS table_name,
# MAGIC     is_track_columns_updated_on,
# MAGIC     CASE 
# MAGIC         WHEN is_track_columns_updated_on = 1 THEN '✓ Tracking Column Updates'
# MAGIC         ELSE '✗ Not Tracking Columns'
# MAGIC     END AS column_tracking_status
# MAGIC FROM sys.change_tracking_tables
# MAGIC WHERE OBJECT_NAME(object_id) IN ('customers', 'orders', 'products')
# MAGIC ORDER BY table_name;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** Change tracking configured with 7-day retention

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Configure Transaction Log Settings
# MAGIC
# MAGIC Ensure transaction log is properly configured for CDC operations.
# MAGIC
# MAGIC ### 7.1 Check Current Log Configuration
# MAGIC
# MAGIC ```sql
# MAGIC -- Check transaction log size and usage
# MAGIC SELECT 
# MAGIC     name AS log_name,
# MAGIC     (size * 8.0 / 1024) AS size_mb,
# MAGIC     (FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024) AS used_mb,
# MAGIC     ((size - FILEPROPERTY(name, 'SpaceUsed')) * 8.0 / 1024) AS free_mb,
# MAGIC     (FILEPROPERTY(name, 'SpaceUsed') * 100.0 / size) AS percent_used
# MAGIC FROM sys.database_files
# MAGIC WHERE type_desc = 'LOG';
# MAGIC ```
# MAGIC
# MAGIC ### 7.2 Check Log Reuse Wait Description
# MAGIC
# MAGIC ```sql
# MAGIC -- Check what's preventing log truncation
# MAGIC SELECT 
# MAGIC     name AS database_name,
# MAGIC     log_reuse_wait_desc,
# MAGIC     CASE log_reuse_wait_desc
# MAGIC         WHEN 'NOTHING' THEN '✓ Log can be truncated'
# MAGIC         WHEN 'REPLICATION' THEN '! Waiting for replication'
# MAGIC         WHEN 'LOG_BACKUP' THEN '! Waiting for log backup'
# MAGIC         ELSE '! ' + log_reuse_wait_desc
# MAGIC     END AS status
# MAGIC FROM sys.databases
# MAGIC WHERE name = 'RetailDB';
# MAGIC ```
# MAGIC
# MAGIC ### 7.3 Adjust Log Size if Needed
# MAGIC
# MAGIC ```sql
# MAGIC -- If log is too small, increase it (optional)
# MAGIC -- Adjust size based on your change volume
# MAGIC ALTER DATABASE RetailDB
# MAGIC MODIFY FILE (
# MAGIC     NAME = RetailDB_log,  -- Use actual log file name
# MAGIC     SIZE = 1GB,           -- Initial size
# MAGIC     MAXSIZE = 10GB,       -- Maximum size
# MAGIC     FILEGROWTH = 512MB    -- Growth increment
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Note:** Adjust sizes based on your expected change volume
# MAGIC
# MAGIC ✅ **Checkpoint:** Transaction log properly configured for CDC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Test CDC Configuration
# MAGIC
# MAGIC Let's verify CDC is capturing changes correctly.
# MAGIC
# MAGIC ### 8.1 Make Test Changes
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- INSERT: Add a new customer
# MAGIC INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, zip_code)
# MAGIC VALUES (9999, 'Test', 'User', 'test.user@email.com', '555-9999', 'Test City', 'TX', '75001');
# MAGIC PRINT 'INSERT test completed';
# MAGIC
# MAGIC -- UPDATE: Modify existing customer
# MAGIC UPDATE customers
# MAGIC SET email = 'john.doe.updated@email.com', last_updated = GETDATE()
# MAGIC WHERE customer_id = 1001;
# MAGIC PRINT 'UPDATE test completed';
# MAGIC
# MAGIC -- DELETE: Remove test customer
# MAGIC DELETE FROM customers WHERE customer_id = 9999;
# MAGIC PRINT 'DELETE test completed';
# MAGIC ```
# MAGIC
# MAGIC ### 8.2 Query CDC Change Table
# MAGIC
# MAGIC ```sql
# MAGIC -- View captured changes in CDC table
# MAGIC SELECT TOP 10
# MAGIC     __$start_lsn AS lsn,
# MAGIC     __$operation AS operation,
# MAGIC     CASE __$operation
# MAGIC         WHEN 1 THEN 'DELETE'
# MAGIC         WHEN 2 THEN 'INSERT'
# MAGIC         WHEN 3 THEN 'UPDATE (before)'
# MAGIC         WHEN 4 THEN 'UPDATE (after)'
# MAGIC     END AS operation_type,
# MAGIC     __$update_mask,
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email
# MAGIC FROM cdc.dbo_customers_CT
# MAGIC ORDER BY __$start_lsn DESC;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC lsn              | operation | operation_type  | customer_id | email
# MAGIC -----------------|-----------|-----------------|-------------|-------------------------
# MAGIC 0x00000027...    | 1         | DELETE          | 9999        | test.user@email.com
# MAGIC 0x00000027...    | 4         | UPDATE (after)  | 1001        | john.doe.updated@email.com
# MAGIC 0x00000027...    | 3         | UPDATE (before) | 1001        | john.doe@email.com
# MAGIC 0x00000027...    | 2         | INSERT          | 9999        | test.user@email.com
# MAGIC ```
# MAGIC
# MAGIC ### 8.3 Use CDC Functions
# MAGIC
# MAGIC ```sql
# MAGIC -- Get all changes within a time range
# MAGIC DECLARE @from_lsn binary(10), @to_lsn binary(10);
# MAGIC
# MAGIC -- Get LSN range for last hour
# MAGIC SET @from_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', DATEADD(hour, -1, GETDATE()));
# MAGIC SET @to_lsn = sys.fn_cdc_get_max_lsn();
# MAGIC
# MAGIC -- Query changes using CDC function
# MAGIC SELECT *
# MAGIC FROM cdc.fn_cdc_get_all_changes_dbo_customers(@from_lsn, @to_lsn, 'all')
# MAGIC ORDER BY __$start_lsn;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** CDC successfully capturing INSERT, UPDATE, and DELETE operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Verify CDC Jobs
# MAGIC
# MAGIC SQL Server uses SQL Agent jobs to manage CDC operations.
# MAGIC
# MAGIC ### 9.1 Check CDC Jobs Status
# MAGIC
# MAGIC ```sql
# MAGIC -- Check CDC capture and cleanup jobs
# MAGIC EXEC sys.sp_cdc_help_jobs;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC job_type | job_id                               | maxtrans | maxscans | continuous | pollinginterval
# MAGIC ---------|--------------------------------------|----------|----------|------------|----------------
# MAGIC capture  | 3E7B0B62-...                         | 500      | 10       | 1          | 5
# MAGIC cleanup  | 8A4C9D51-...                         | NULL     | NULL     | NULL       | NULL
# MAGIC ```
# MAGIC
# MAGIC ### 9.2 Verify SQL Server Agent is Running
# MAGIC
# MAGIC ```sql
# MAGIC -- Check SQL Server Agent status
# MAGIC EXEC xp_servicecontrol 'QueryState', 'SQLServerAGENT';
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Running.
# MAGIC ```
# MAGIC
# MAGIC **If Agent is not running:**
# MAGIC - On Windows: Start SQL Server Agent service
# MAGIC - On Linux: SQL Server Agent is always running
# MAGIC - On Azure SQL: Managed automatically
# MAGIC
# MAGIC ✅ **Checkpoint:** CDC capture and cleanup jobs are configured and running

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue 1: Permission Denied
# MAGIC
# MAGIC **Error Message:**
# MAGIC ```
# MAGIC Msg 22902, Level 16, State 1
# MAGIC You do not have permission to run sp_cdc_enable_db
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC -- Verify you're a sysadmin or db_owner
# MAGIC SELECT IS_SRVROLEMEMBER('sysadmin'), IS_MEMBER('db_owner');
# MAGIC
# MAGIC -- If needed, have an admin grant permissions
# MAGIC -- ALTER SERVER ROLE sysadmin ADD MEMBER [YourUserName];
# MAGIC ```
# MAGIC
# MAGIC ### Issue 2: Database Not in FULL Recovery Mode
# MAGIC
# MAGIC **Error Message:**
# MAGIC ```
# MAGIC CDC cannot be enabled on database 'RetailDB' because it is not in FULL recovery mode
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC ALTER DATABASE RetailDB SET RECOVERY FULL;
# MAGIC ```
# MAGIC
# MAGIC ### Issue 3: CDC Jobs Not Running
# MAGIC
# MAGIC **Symptom:** Changes not appearing in CDC tables
# MAGIC
# MAGIC **Diagnostic:**
# MAGIC ```sql
# MAGIC -- Check job history
# MAGIC SELECT 
# MAGIC     j.name AS job_name,
# MAGIC     h.run_status,
# MAGIC     h.run_date,
# MAGIC     h.run_time,
# MAGIC     h.message
# MAGIC FROM msdb.dbo.sysjobs j
# MAGIC JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
# MAGIC WHERE j.name LIKE 'cdc.RetailDB%'
# MAGIC ORDER BY h.run_date DESC, h.run_time DESC;
# MAGIC ```
# MAGIC
# MAGIC **Solution:**
# MAGIC - Ensure SQL Server Agent is running
# MAGIC - Check for errors in job history
# MAGIC - Restart CDC capture job if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Validation
# MAGIC
# MAGIC ### Final Validation Checklist
# MAGIC
# MAGIC Run this comprehensive validation query:
# MAGIC
# MAGIC ```sql
# MAGIC -- Comprehensive CDC configuration check
# MAGIC SELECT 'Database CDC Status' AS check_type,
# MAGIC        CASE WHEN is_cdc_enabled = 1 THEN '✓ PASS' ELSE '✗ FAIL' END AS result
# MAGIC FROM sys.databases WHERE name = 'RetailDB'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Recovery Mode',
# MAGIC        CASE WHEN recovery_model_desc = 'FULL' THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC FROM sys.databases WHERE name = 'RetailDB'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Tables with CDC',
# MAGIC        CASE WHEN COUNT(*) = 3 THEN '✓ PASS (3 tables)' ELSE '✗ FAIL' END
# MAGIC FROM sys.tables WHERE is_tracked_by_cdc = 1 AND name IN ('customers', 'orders', 'products')
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Change Tracking',
# MAGIC        CASE WHEN is_change_tracking_enabled = 1 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC FROM sys.databases WHERE name = 'RetailDB'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'CDC Jobs',
# MAGIC        CASE WHEN COUNT(*) >= 2 THEN '✓ PASS (capture + cleanup)' ELSE '✗ FAIL' END
# MAGIC FROM msdb.dbo.sysjobs WHERE name LIKE 'cdc.RetailDB%';
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC check_type              | result
# MAGIC ------------------------|-------------------------
# MAGIC Database CDC Status     | ✓ PASS
# MAGIC Recovery Mode           | ✓ PASS
# MAGIC Tables with CDC         | ✓ PASS (3 tables)
# MAGIC Change Tracking         | ✓ PASS
# MAGIC CDC Jobs                | ✓ PASS (capture + cleanup)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You Accomplished
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ **Verified SQL Server prerequisites** (version, recovery mode, permissions)
# MAGIC
# MAGIC ✅ **Created retail database and tables** (customers, orders, products)
# MAGIC
# MAGIC ✅ **Enabled CDC at database level** using `sys.sp_cdc_enable_db`
# MAGIC
# MAGIC ✅ **Enabled CDC on individual tables** with `sys.sp_cdc_enable_table`
# MAGIC
# MAGIC ✅ **Configured change tracking** with 7-day retention period
# MAGIC
# MAGIC ✅ **Tested CDC operations** (INSERT, UPDATE, DELETE) and verified capture
# MAGIC
# MAGIC ✅ **Validated CDC jobs** are running correctly
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that SQL Server is configured for CDC, proceed to **Lab 3: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** where you'll:
# MAGIC - Create Unity Catalog resources (catalog, schema, volume)
# MAGIC - Set up a Lakeflow Connect ingestion gateway
# MAGIC - Create a connection to your SQL Server database
# MAGIC - Build an ingestion pipeline to sync data to Databricks
# MAGIC
# MAGIC **Important:** Keep your SQL Server connection details handy (hostname, port, database name, credentials) - you'll need them in the next lab!
# MAGIC
# MAGIC **Ready to build your CDC pipeline? Let's move to Lab 3!** 🚀
