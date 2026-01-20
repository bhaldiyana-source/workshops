# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2: Configuring Oracle for CDC Operations
# MAGIC
# MAGIC ## Lab Objectives
# MAGIC By the end of this lab, you will:
# MAGIC - Enable ARCHIVELOG mode on your Oracle database
# MAGIC - Configure supplemental logging at database and table levels
# MAGIC - Create a dedicated CDC user with appropriate LogMiner privileges
# MAGIC - Create sample retail database tables (CUSTOMERS, ORDERS, PRODUCTS)
# MAGIC - Load test data into source tables
# MAGIC - Verify Oracle CDC configuration is correct
# MAGIC
# MAGIC **Estimated Duration:** 45 minutes  
# MAGIC **Prerequisites:** 
# MAGIC - Oracle Enterprise Edition database
# MAGIC - DBA or SYSDBA privileges
# MAGIC - SQL*Plus, SQL Developer, or DBeaver installed
# MAGIC - Completion of Lecture 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Architecture
# MAGIC
# MAGIC In this lab, you'll configure the **Oracle Source** side of the CDC pipeline:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    ORACLE DATABASE (YOU ARE HERE)                │
# MAGIC │                                                                   │
# MAGIC │  Step 1: Enable ARCHIVELOG Mode                                 │
# MAGIC │    └─► Persist redo logs for CDC                                │
# MAGIC │                                                                   │
# MAGIC │  Step 2: Configure Supplemental Logging                         │
# MAGIC │    └─► Capture before/after values in logs                      │
# MAGIC │                                                                   │
# MAGIC │  Step 3: Create CDC User                                        │
# MAGIC │    └─► Grant LogMiner privileges                                │
# MAGIC │                                                                   │
# MAGIC │  Step 4: Create Sample Tables                                   │
# MAGIC │    ├─► CUSTOMERS                                                │
# MAGIC │    ├─► ORDERS                                                   │
# MAGIC │    └─► PRODUCTS                                                 │
# MAGIC │                                                                   │
# MAGIC │  Step 5: Load Test Data                                         │
# MAGIC │    └─► Insert 10,000+ sample records                            │
# MAGIC │                                                                   │
# MAGIC │  Step 6: Verify Configuration                                   │
# MAGIC │    └─► Run validation queries                                   │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC                               │
# MAGIC                               │ (Ready for Lakeflow Connect in Lab 3)
# MAGIC                               ▼
# MAGIC                    Databricks Lakehouse
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Lab: Connection Information
# MAGIC
# MAGIC Before starting, gather your Oracle connection details:
# MAGIC
# MAGIC ```python
# MAGIC # Fill in your Oracle connection information:
# MAGIC ORACLE_HOST = "oracle.example.com"  # Your Oracle hostname or IP
# MAGIC ORACLE_PORT = 1521                  # Default Oracle port
# MAGIC ORACLE_SERVICE_NAME = "RETAILDB"    # Service name or SID
# MAGIC ORACLE_DBA_USER = "system"          # DBA user (e.g., system, sys)
# MAGIC ORACLE_DBA_PASSWORD = "***"         # DBA password (keep secure!)
# MAGIC ```
# MAGIC
# MAGIC **Security Note:** For production, use Oracle Wallet or environment variables for passwords. Never commit credentials to version control!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Enable ARCHIVELOG Mode
# MAGIC
# MAGIC ### What is ARCHIVELOG Mode?
# MAGIC
# MAGIC Oracle databases can run in two modes:
# MAGIC - **NOARCHIVELOG** (default): Redo logs are overwritten, no CDC possible
# MAGIC - **ARCHIVELOG**: Redo logs are archived before overwriting, enabling CDC
# MAGIC
# MAGIC ### Check Current Mode
# MAGIC
# MAGIC Connect to your Oracle database using SQL*Plus or your preferred SQL client, then run:
# MAGIC
# MAGIC ```sql
# MAGIC -- Check if database is in ARCHIVELOG mode
# MAGIC SELECT LOG_MODE, FORCE_LOGGING FROM V$DATABASE;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC LOG_MODE      FORCE_LOGGING
# MAGIC ------------- -------------
# MAGIC NOARCHIVELOG  NO
# MAGIC ```
# MAGIC
# MAGIC If `LOG_MODE` is already `ARCHIVELOG`, you can skip to Step 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable ARCHIVELOG Mode
# MAGIC
# MAGIC ⚠️ **Warning:** Enabling ARCHIVELOG requires database restart. Plan for maintenance window.
# MAGIC
# MAGIC ⚠️ **Warning:** Archive logs consume significant disk space. Ensure adequate storage and implement cleanup policies.
# MAGIC
# MAGIC **Execute these steps as DBA user (e.g., system or sys):**
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1.1: Check current database status
# MAGIC SELECT INSTANCE_NAME, STATUS FROM V$INSTANCE;
# MAGIC -- Should show: STATUS = OPEN
# MAGIC
# MAGIC -- Step 1.2: Shutdown database
# MAGIC SHUTDOWN IMMEDIATE;
# MAGIC -- Wait for: "Database closed. Database dismounted. ORACLE instance shut down."
# MAGIC
# MAGIC -- Step 1.3: Startup in MOUNT mode
# MAGIC STARTUP MOUNT;
# MAGIC -- Wait for: "Database mounted."
# MAGIC
# MAGIC -- Step 1.4: Enable ARCHIVELOG mode
# MAGIC ALTER DATABASE ARCHIVELOG;
# MAGIC -- Should see: "Database altered."
# MAGIC
# MAGIC -- Step 1.5: Open database
# MAGIC ALTER DATABASE OPEN;
# MAGIC -- Should see: "Database altered."
# MAGIC
# MAGIC -- Step 1.6: Verify ARCHIVELOG is enabled
# MAGIC SELECT LOG_MODE FROM V$DATABASE;
# MAGIC -- Should show: LOG_MODE = ARCHIVELOG
# MAGIC
# MAGIC -- Step 1.7: Check archive log destination
# MAGIC SELECT NAME, SPACE_LIMIT/1024/1024/1024 AS SPACE_LIMIT_GB, 
# MAGIC        SPACE_USED/1024/1024/1024 AS SPACE_USED_GB
# MAGIC FROM V$RECOVERY_FILE_DEST;
# MAGIC ```
# MAGIC
# MAGIC **Success Criteria:**
# MAGIC - `LOG_MODE` shows `ARCHIVELOG`
# MAGIC - Archive destination has adequate space (recommended: 100+ GB)
# MAGIC - Database is OPEN and accepting connections

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Archive Log Retention
# MAGIC
# MAGIC Set up automatic archive log management to prevent disk space issues:
# MAGIC
# MAGIC ```sql
# MAGIC -- View current archive log location
# MAGIC SHOW PARAMETER DB_RECOVERY_FILE_DEST;
# MAGIC
# MAGIC -- Set recovery file destination size (adjust based on your needs)
# MAGIC ALTER SYSTEM SET DB_RECOVERY_FILE_DEST_SIZE = 100G SCOPE=BOTH;
# MAGIC
# MAGIC -- Enable automatic archive log deletion after backup
# MAGIC -- (Requires RMAN backup strategy - see oracle_scripts/rman_backup.sh)
# MAGIC
# MAGIC -- View current archive logs
# MAGIC SELECT 
# MAGIC   SEQUENCE#,
# MAGIC   FIRST_TIME,
# MAGIC   NEXT_TIME,
# MAGIC   BLOCKS * BLOCK_SIZE / 1024 / 1024 AS SIZE_MB,
# MAGIC   ARCHIVED,
# MAGIC   APPLIED,
# MAGIC   DELETED
# MAGIC FROM V$ARCHIVED_LOG
# MAGIC WHERE FIRST_TIME > SYSDATE - 1
# MAGIC ORDER BY SEQUENCE# DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Supplemental Logging
# MAGIC
# MAGIC ### What is Supplemental Logging?
# MAGIC
# MAGIC By default, Oracle redo logs contain minimal information for recovery. **Supplemental logging** adds extra data needed for CDC:
# MAGIC - Primary key values
# MAGIC - Before/after values for updated columns
# MAGIC - All column values for complete row reconstruction
# MAGIC
# MAGIC ### Enable Database-Level Supplemental Logging
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 2.1: Check current supplemental logging status
# MAGIC SELECT 
# MAGIC   SUPPLEMENTAL_LOG_DATA_MIN AS "Minimal",
# MAGIC   SUPPLEMENTAL_LOG_DATA_PK AS "Primary Key",
# MAGIC   SUPPLEMENTAL_LOG_DATA_UI AS "Unique Index",
# MAGIC   SUPPLEMENTAL_LOG_DATA_FK AS "Foreign Key",
# MAGIC   SUPPLEMENTAL_LOG_DATA_ALL AS "All Columns"
# MAGIC FROM V$DATABASE;
# MAGIC
# MAGIC -- Expected initially:
# MAGIC -- Minimal=NO, Primary Key=NO, Unique Index=NO, Foreign Key=NO, All Columns=NO
# MAGIC
# MAGIC -- Step 2.2: Enable minimal supplemental logging (required)
# MAGIC ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
# MAGIC -- Should see: "Database altered."
# MAGIC
# MAGIC -- Step 2.3: Enable primary key logging (recommended for CDC)
# MAGIC ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
# MAGIC -- Should see: "Database altered."
# MAGIC
# MAGIC -- Step 2.4: Verify supplemental logging is enabled
# MAGIC SELECT 
# MAGIC   SUPPLEMENTAL_LOG_DATA_MIN AS "Minimal",
# MAGIC   SUPPLEMENTAL_LOG_DATA_PK AS "Primary Key"
# MAGIC FROM V$DATABASE;
# MAGIC
# MAGIC -- Expected:
# MAGIC -- Minimal=YES, Primary Key=YES
# MAGIC ```
# MAGIC
# MAGIC **Performance Impact:** Enabling supplemental logging increases redo log generation by approximately 5-15%. Monitor your redo log size and switch frequency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Retail Database Schema
# MAGIC
# MAGIC ### Create Schema User
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 3.1: Create RETAIL schema user
# MAGIC CREATE USER RETAIL IDENTIFIED BY "StrongPassword123!"
# MAGIC   DEFAULT TABLESPACE USERS
# MAGIC   TEMPORARY TABLESPACE TEMP
# MAGIC   QUOTA UNLIMITED ON USERS;
# MAGIC
# MAGIC -- Step 3.2: Grant necessary privileges
# MAGIC GRANT CONNECT, RESOURCE TO RETAIL;
# MAGIC GRANT CREATE TABLE, CREATE VIEW, CREATE SEQUENCE TO RETAIL;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create CUSTOMERS Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Connect as RETAIL user or run as DBA with schema prefix
# MAGIC
# MAGIC CREATE TABLE RETAIL.CUSTOMERS (
# MAGIC     CUSTOMER_ID NUMBER(10) PRIMARY KEY,
# MAGIC     FIRST_NAME VARCHAR2(50) NOT NULL,
# MAGIC     LAST_NAME VARCHAR2(50) NOT NULL,
# MAGIC     EMAIL VARCHAR2(100) UNIQUE NOT NULL,
# MAGIC     PHONE VARCHAR2(20),
# MAGIC     ADDRESS VARCHAR2(200),
# MAGIC     CITY VARCHAR2(50),
# MAGIC     STATE VARCHAR2(2),
# MAGIC     ZIP_CODE VARCHAR2(10),
# MAGIC     CREATED_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
# MAGIC     LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Add comment for documentation
# MAGIC COMMENT ON TABLE RETAIL.CUSTOMERS IS 'Customer master data for retail operations';
# MAGIC
# MAGIC -- Create index for performance
# MAGIC CREATE INDEX RETAIL.IDX_CUSTOMERS_EMAIL ON RETAIL.CUSTOMERS(EMAIL);
# MAGIC CREATE INDEX RETAIL.IDX_CUSTOMERS_STATE ON RETAIL.CUSTOMERS(STATE);
# MAGIC
# MAGIC -- Enable table-level supplemental logging (ALL COLUMNS)
# MAGIC ALTER TABLE RETAIL.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create ORDERS Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE RETAIL.ORDERS (
# MAGIC     ORDER_ID NUMBER(10) PRIMARY KEY,
# MAGIC     CUSTOMER_ID NUMBER(10) NOT NULL,
# MAGIC     ORDER_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
# MAGIC     ORDER_STATUS VARCHAR2(20) DEFAULT 'PENDING',
# MAGIC     TOTAL_AMOUNT NUMBER(10, 2) NOT NULL,
# MAGIC     SHIPPING_ADDRESS VARCHAR2(200),
# MAGIC     PAYMENT_METHOD VARCHAR2(50),
# MAGIC     CREATED_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
# MAGIC     LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP,
# MAGIC     CONSTRAINT FK_CUSTOMER FOREIGN KEY (CUSTOMER_ID) 
# MAGIC         REFERENCES RETAIL.CUSTOMERS(CUSTOMER_ID) ON DELETE CASCADE
# MAGIC );
# MAGIC
# MAGIC COMMENT ON TABLE RETAIL.ORDERS IS 'Order transactions with customer references';
# MAGIC
# MAGIC -- Create indexes
# MAGIC CREATE INDEX RETAIL.IDX_ORDERS_CUSTOMER ON RETAIL.ORDERS(CUSTOMER_ID);
# MAGIC CREATE INDEX RETAIL.IDX_ORDERS_DATE ON RETAIL.ORDERS(ORDER_DATE);
# MAGIC CREATE INDEX RETAIL.IDX_ORDERS_STATUS ON RETAIL.ORDERS(ORDER_STATUS);
# MAGIC
# MAGIC -- Enable table-level supplemental logging
# MAGIC ALTER TABLE RETAIL.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create PRODUCTS Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE RETAIL.PRODUCTS (
# MAGIC     PRODUCT_ID NUMBER(10) PRIMARY KEY,
# MAGIC     PRODUCT_NAME VARCHAR2(100) NOT NULL,
# MAGIC     CATEGORY VARCHAR2(50),
# MAGIC     PRICE NUMBER(10, 2) NOT NULL,
# MAGIC     STOCK_QUANTITY NUMBER(10) DEFAULT 0,
# MAGIC     SUPPLIER VARCHAR2(100),
# MAGIC     DESCRIPTION CLOB,
# MAGIC     CREATED_DATE TIMESTAMP DEFAULT SYSTIMESTAMP,
# MAGIC     LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP
# MAGIC );
# MAGIC
# MAGIC COMMENT ON TABLE RETAIL.PRODUCTS IS 'Product catalog with inventory levels';
# MAGIC
# MAGIC -- Create indexes
# MAGIC CREATE INDEX RETAIL.IDX_PRODUCTS_CATEGORY ON RETAIL.PRODUCTS(CATEGORY);
# MAGIC CREATE INDEX RETAIL.IDX_PRODUCTS_NAME ON RETAIL.PRODUCTS(PRODUCT_NAME);
# MAGIC
# MAGIC -- Enable table-level supplemental logging
# MAGIC ALTER TABLE RETAIL.PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Table Creation and Supplemental Logging
# MAGIC
# MAGIC ```sql
# MAGIC -- Check tables were created
# MAGIC SELECT TABLE_NAME, NUM_ROWS, LAST_ANALYZED
# MAGIC FROM DBA_TABLES
# MAGIC WHERE OWNER = 'RETAIL'
# MAGIC ORDER BY TABLE_NAME;
# MAGIC
# MAGIC -- Verify supplemental logging on tables
# MAGIC SELECT 
# MAGIC     OWNER,
# MAGIC     TABLE_NAME,
# MAGIC     LOG_GROUP_NAME,
# MAGIC     LOG_GROUP_TYPE,
# MAGIC     ALWAYS
# MAGIC FROM DBA_LOG_GROUPS
# MAGIC WHERE OWNER = 'RETAIL'
# MAGIC ORDER BY TABLE_NAME;
# MAGIC
# MAGIC -- Expected: Each table should have LOG_GROUP_TYPE = 'ALL COLUMN LOGGING'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create CDC User with LogMiner Privileges
# MAGIC
# MAGIC ### Create Dedicated CDC User
# MAGIC
# MAGIC Best practice: Use a dedicated user for CDC operations with minimal privileges.
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 4.1: Create CDC user
# MAGIC CREATE USER CDC_USER IDENTIFIED BY "SecureCDCPassword123!"
# MAGIC   DEFAULT TABLESPACE USERS
# MAGIC   TEMPORARY TABLESPACE TEMP;
# MAGIC
# MAGIC -- Step 4.2: Grant connection privilege
# MAGIC GRANT CREATE SESSION TO CDC_USER;
# MAGIC
# MAGIC -- Step 4.3: Grant LogMiner required privileges
# MAGIC GRANT SELECT ANY TRANSACTION TO CDC_USER;
# MAGIC GRANT EXECUTE ON DBMS_LOGMNR TO CDC_USER;
# MAGIC GRANT EXECUTE ON DBMS_LOGMNR_D TO CDC_USER;
# MAGIC
# MAGIC -- Step 4.4: Grant read access to V$ views
# MAGIC GRANT SELECT ON V_$DATABASE TO CDC_USER;
# MAGIC GRANT SELECT ON V_$ARCHIVED_LOG TO CDC_USER;
# MAGIC GRANT SELECT ON V_$LOG TO CDC_USER;
# MAGIC GRANT SELECT ON V_$LOGFILE TO CDC_USER;
# MAGIC GRANT SELECT ON V_$LOGMNR_CONTENTS TO CDC_USER;
# MAGIC GRANT SELECT ON V_$LOGMNR_LOGS TO CDC_USER;
# MAGIC
# MAGIC -- Step 4.5: Grant SELECT on RETAIL tables
# MAGIC GRANT SELECT ON RETAIL.CUSTOMERS TO CDC_USER;
# MAGIC GRANT SELECT ON RETAIL.ORDERS TO CDC_USER;
# MAGIC GRANT SELECT ON RETAIL.PRODUCTS TO CDC_USER;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test CDC User Permissions
# MAGIC
# MAGIC ```sql
# MAGIC -- Connect as CDC_USER and test access:
# MAGIC
# MAGIC -- Test 1: Can query tables?
# MAGIC SELECT COUNT(*) FROM RETAIL.CUSTOMERS;
# MAGIC -- Expected: 0 (table is empty at this point)
# MAGIC
# MAGIC -- Test 2: Can access V$DATABASE?
# MAGIC SELECT CURRENT_SCN FROM V$DATABASE;
# MAGIC -- Expected: A number (e.g., 8745000)
# MAGIC
# MAGIC -- Test 3: Can access LogMiner views?
# MAGIC SELECT * FROM V$LOGMNR_LOGS WHERE ROWNUM <= 1;
# MAGIC -- Expected: Either empty result or log file info
# MAGIC
# MAGIC -- Test 4: Can execute DBMS_LOGMNR?
# MAGIC BEGIN
# MAGIC   DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);
# MAGIC   DBMS_LOGMNR.END_LOGMNR;
# MAGIC END;
# MAGIC /
# MAGIC -- Expected: "PL/SQL procedure successfully completed."
# MAGIC ```
# MAGIC
# MAGIC ✅ **Success Criteria:** All test queries execute without permission errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load Test Data
# MAGIC
# MAGIC ### Load Sample Customers
# MAGIC
# MAGIC ```sql
# MAGIC -- Connect as RETAIL user
# MAGIC
# MAGIC -- Insert 100 sample customers
# MAGIC BEGIN
# MAGIC   FOR i IN 1..100 LOOP
# MAGIC     INSERT INTO RETAIL.CUSTOMERS (
# MAGIC       CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, 
# MAGIC       ADDRESS, CITY, STATE, ZIP_CODE
# MAGIC     ) VALUES (
# MAGIC       i,
# MAGIC       'FirstName' || i,
# MAGIC       'LastName' || i,
# MAGIC       'customer' || i || '@email.com',
# MAGIC       '555-' || LPAD(i, 4, '0'),
# MAGIC       i || ' Main Street',
# MAGIC       CASE MOD(i, 5)
# MAGIC         WHEN 0 THEN 'Seattle'
# MAGIC         WHEN 1 THEN 'Portland'
# MAGIC         WHEN 2 THEN 'San Francisco'
# MAGIC         WHEN 3 THEN 'Los Angeles'
# MAGIC         WHEN 4 THEN 'San Diego'
# MAGIC       END,
# MAGIC       CASE MOD(i, 5)
# MAGIC         WHEN 0 THEN 'WA'
# MAGIC         WHEN 1 THEN 'OR'
# MAGIC         ELSE 'CA'
# MAGIC       END,
# MAGIC       LPAD(MOD(i * 100, 99999), 5, '0')
# MAGIC     );
# MAGIC   END LOOP;
# MAGIC   COMMIT;
# MAGIC END;
# MAGIC /
# MAGIC
# MAGIC -- Verify customer data
# MAGIC SELECT COUNT(*) AS customer_count FROM RETAIL.CUSTOMERS;
# MAGIC -- Expected: 100
# MAGIC
# MAGIC SELECT STATE, COUNT(*) AS count
# MAGIC FROM RETAIL.CUSTOMERS
# MAGIC GROUP BY STATE
# MAGIC ORDER BY STATE;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Sample Products
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert 50 sample products
# MAGIC BEGIN
# MAGIC   FOR i IN 1..50 LOOP
# MAGIC     INSERT INTO RETAIL.PRODUCTS (
# MAGIC       PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE, 
# MAGIC       STOCK_QUANTITY, SUPPLIER, DESCRIPTION
# MAGIC     ) VALUES (
# MAGIC       i,
# MAGIC       'Product ' || i,
# MAGIC       CASE MOD(i, 5)
# MAGIC         WHEN 0 THEN 'Electronics'
# MAGIC         WHEN 1 THEN 'Clothing'
# MAGIC         WHEN 2 THEN 'Books'
# MAGIC         WHEN 3 THEN 'Home & Garden'
# MAGIC         WHEN 4 THEN 'Sports'
# MAGIC       END,
# MAGIC       ROUND(DBMS_RANDOM.VALUE(10, 500), 2),
# MAGIC       ROUND(DBMS_RANDOM.VALUE(0, 1000)),
# MAGIC       'Supplier ' || MOD(i, 10),
# MAGIC       'Description for product ' || i
# MAGIC     );
# MAGIC   END LOOP;
# MAGIC   COMMIT;
# MAGIC END;
# MAGIC /
# MAGIC
# MAGIC -- Verify product data
# MAGIC SELECT COUNT(*) AS product_count FROM RETAIL.PRODUCTS;
# MAGIC -- Expected: 50
# MAGIC
# MAGIC SELECT CATEGORY, COUNT(*) AS count, AVG(PRICE) AS avg_price
# MAGIC FROM RETAIL.PRODUCTS
# MAGIC GROUP BY CATEGORY
# MAGIC ORDER BY CATEGORY;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Sample Orders
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert 200 sample orders
# MAGIC BEGIN
# MAGIC   FOR i IN 1..200 LOOP
# MAGIC     INSERT INTO RETAIL.ORDERS (
# MAGIC       ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS,
# MAGIC       TOTAL_AMOUNT, SHIPPING_ADDRESS, PAYMENT_METHOD
# MAGIC     ) VALUES (
# MAGIC       i,
# MAGIC       MOD(i, 100) + 1,  -- Reference existing customers
# MAGIC       SYSTIMESTAMP - DBMS_RANDOM.VALUE(0, 30),  -- Random date within last 30 days
# MAGIC       CASE MOD(i, 4)
# MAGIC         WHEN 0 THEN 'PENDING'
# MAGIC         WHEN 1 THEN 'PROCESSING'
# MAGIC         WHEN 2 THEN 'SHIPPED'
# MAGIC         WHEN 3 THEN 'DELIVERED'
# MAGIC       END,
# MAGIC       ROUND(DBMS_RANDOM.VALUE(25, 500), 2),
# MAGIC       (MOD(i, 100) + 1) || ' Shipping Address',
# MAGIC       CASE MOD(i, 3)
# MAGIC         WHEN 0 THEN 'Credit Card'
# MAGIC         WHEN 1 THEN 'PayPal'
# MAGIC         WHEN 2 THEN 'Bank Transfer'
# MAGIC       END
# MAGIC     );
# MAGIC   END LOOP;
# MAGIC   COMMIT;
# MAGIC END;
# MAGIC /
# MAGIC
# MAGIC -- Verify order data
# MAGIC SELECT COUNT(*) AS order_count FROM RETAIL.ORDERS;
# MAGIC -- Expected: 200
# MAGIC
# MAGIC SELECT ORDER_STATUS, COUNT(*) AS count, SUM(TOTAL_AMOUNT) AS total_revenue
# MAGIC FROM RETAIL.ORDERS
# MAGIC GROUP BY ORDER_STATUS
# MAGIC ORDER BY ORDER_STATUS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Complete Oracle CDC Configuration
# MAGIC
# MAGIC ### Comprehensive Validation Checklist
# MAGIC
# MAGIC ```sql
# MAGIC -- Validation 1: ARCHIVELOG Mode
# MAGIC SELECT 'ARCHIVELOG Status' AS check_name, LOG_MODE AS status
# MAGIC FROM V$DATABASE;
# MAGIC -- Expected: LOG_MODE = ARCHIVELOG
# MAGIC
# MAGIC -- Validation 2: Supplemental Logging (Database Level)
# MAGIC SELECT 'Supplemental Logging' AS check_name,
# MAGIC        SUPPLEMENTAL_LOG_DATA_MIN || ' / ' || SUPPLEMENTAL_LOG_DATA_PK AS status
# MAGIC FROM V$DATABASE;
# MAGIC -- Expected: YES / YES
# MAGIC
# MAGIC -- Validation 3: Table-Level Supplemental Logging
# MAGIC SELECT 
# MAGIC   'Table Logging: ' || TABLE_NAME AS check_name,
# MAGIC   LOG_GROUP_TYPE AS status
# MAGIC FROM DBA_LOG_GROUPS
# MAGIC WHERE OWNER = 'RETAIL'
# MAGIC ORDER BY TABLE_NAME;
# MAGIC -- Expected: ALL COLUMN LOGGING for each table
# MAGIC
# MAGIC -- Validation 4: CDC User Exists and Has Privileges
# MAGIC SELECT 'CDC User Exists' AS check_name,
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END AS status
# MAGIC FROM DBA_USERS
# MAGIC WHERE USERNAME = 'CDC_USER';
# MAGIC -- Expected: YES
# MAGIC
# MAGIC -- Validation 5: Table Row Counts
# MAGIC SELECT 'CUSTOMERS row count' AS check_name, TO_CHAR(COUNT(*)) AS status
# MAGIC FROM RETAIL.CUSTOMERS
# MAGIC UNION ALL
# MAGIC SELECT 'ORDERS row count', TO_CHAR(COUNT(*))
# MAGIC FROM RETAIL.ORDERS
# MAGIC UNION ALL
# MAGIC SELECT 'PRODUCTS row count', TO_CHAR(COUNT(*))
# MAGIC FROM RETAIL.PRODUCTS;
# MAGIC -- Expected: CUSTOMERS=100, ORDERS=200, PRODUCTS=50
# MAGIC
# MAGIC -- Validation 6: Current SCN (for reference)
# MAGIC SELECT 'Current SCN' AS check_name, TO_CHAR(CURRENT_SCN) AS status
# MAGIC FROM V$DATABASE;
# MAGIC -- Note this SCN - it's the starting point for CDC
# MAGIC
# MAGIC -- Validation 7: Archive Log Generation
# MAGIC SELECT 'Recent Archive Logs' AS check_name,
# MAGIC        TO_CHAR(COUNT(*)) AS status
# MAGIC FROM V$ARCHIVED_LOG
# MAGIC WHERE FIRST_TIME > SYSDATE - 1;
# MAGIC -- Expected: > 0 (archive logs are being generated)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Sample CDC Activity
# MAGIC
# MAGIC Let's create some changes to test CDC in the next lab:
# MAGIC
# MAGIC ```sql
# MAGIC -- Make some changes to generate redo log entries
# MAGIC
# MAGIC -- Update a customer
# MAGIC UPDATE RETAIL.CUSTOMERS 
# MAGIC SET EMAIL = 'updated_customer1@email.com', LAST_UPDATED = SYSTIMESTAMP
# MAGIC WHERE CUSTOMER_ID = 1;
# MAGIC
# MAGIC -- Insert a new customer
# MAGIC INSERT INTO RETAIL.CUSTOMERS (
# MAGIC   CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, CITY, STATE, ZIP_CODE
# MAGIC ) VALUES (
# MAGIC   101, 'TestFirstName', 'TestLastName', 'test101@email.com', 
# MAGIC   '555-0101', 'Boston', 'MA', '02101'
# MAGIC );
# MAGIC
# MAGIC -- Update an order status
# MAGIC UPDATE RETAIL.ORDERS
# MAGIC SET ORDER_STATUS = 'SHIPPED', LAST_UPDATED = SYSTIMESTAMP
# MAGIC WHERE ORDER_ID = 1;
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify changes
# MAGIC SELECT * FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID IN (1, 101);
# MAGIC SELECT * FROM RETAIL.ORDERS WHERE ORDER_ID = 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary and Validation
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ **Oracle Database Configured for CDC:**
# MAGIC - ARCHIVELOG mode enabled
# MAGIC - Supplemental logging configured (database and table level)
# MAGIC - Archive log retention configured
# MAGIC
# MAGIC ✅ **Schema and Data Created:**
# MAGIC - RETAIL schema with 3 tables (CUSTOMERS, ORDERS, PRODUCTS)
# MAGIC - 100 customers loaded
# MAGIC - 50 products loaded
# MAGIC - 200 orders loaded
# MAGIC
# MAGIC ✅ **CDC User Configured:**
# MAGIC - CDC_USER created with LogMiner privileges
# MAGIC - Read access granted to RETAIL tables
# MAGIC - Permissions tested and verified
# MAGIC
# MAGIC ✅ **Test Data Generated:**
# MAGIC - Initial dataset loaded
# MAGIC - Sample changes made to generate redo log entries
# MAGIC
# MAGIC ### Final Validation Query
# MAGIC
# MAGIC Run this comprehensive check to ensure everything is ready:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   'Oracle CDC Configuration' AS category,
# MAGIC   'Complete' AS status,
# MAGIC   SYSTIMESTAMP AS validated_at
# MAGIC FROM DUAL
# MAGIC WHERE 
# MAGIC   -- Check 1: ARCHIVELOG enabled
# MAGIC   (SELECT LOG_MODE FROM V$DATABASE) = 'ARCHIVELOG'
# MAGIC   -- Check 2: Supplemental logging enabled
# MAGIC   AND (SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE) = 'YES'
# MAGIC   -- Check 3: Tables exist
# MAGIC   AND (SELECT COUNT(*) FROM DBA_TABLES WHERE OWNER = 'RETAIL') = 3
# MAGIC   -- Check 4: Data loaded
# MAGIC   AND (SELECT COUNT(*) FROM RETAIL.CUSTOMERS) >= 100
# MAGIC   AND (SELECT COUNT(*) FROM RETAIL.ORDERS) >= 200
# MAGIC   AND (SELECT COUNT(*) FROM RETAIL.PRODUCTS) >= 50
# MAGIC   -- Check 5: CDC user exists
# MAGIC   AND (SELECT COUNT(*) FROM DBA_USERS WHERE USERNAME = 'CDC_USER') = 1;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC CATEGORY                       STATUS      VALIDATED_AT
# MAGIC ------------------------------ ----------- ---------------------------
# MAGIC Oracle CDC Configuration       Complete    10-JAN-26 08:00:00.000000
# MAGIC ```
# MAGIC
# MAGIC If you see this result, your Oracle database is fully configured for CDC! 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue 1: Insufficient Privileges
# MAGIC ```
# MAGIC Error: ORA-01031: insufficient privileges
# MAGIC Solution: Ensure you're connected as DBA user (system, sys with SYSDBA)
# MAGIC ```
# MAGIC
# MAGIC ### Issue 2: Archive Destination Full
# MAGIC ```
# MAGIC Error: ORA-00257: archiver error. Connect internal only, until freed.
# MAGIC Solution: Increase DB_RECOVERY_FILE_DEST_SIZE or clean up old archive logs
# MAGIC
# MAGIC -- Check space:
# MAGIC SELECT * FROM V$RECOVERY_FILE_DEST;
# MAGIC
# MAGIC -- Delete obsolete archive logs (via RMAN):
# MAGIC DELETE ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE-2';
# MAGIC ```
# MAGIC
# MAGIC ### Issue 3: Table Already Exists
# MAGIC ```
# MAGIC Error: ORA-00955: name is already used by an existing object
# MAGIC Solution: Drop existing table first (be careful in production!)
# MAGIC
# MAGIC DROP TABLE RETAIL.CUSTOMERS CASCADE CONSTRAINTS;
# MAGIC ```
# MAGIC
# MAGIC ### Issue 4: Cannot Enable ARCHIVELOG
# MAGIC ```
# MAGIC Error: ORA-01126: database must be mounted in this instance and not open in any instance
# MAGIC Solution: Ensure database is in MOUNT mode, not OPEN mode
# MAGIC
# MAGIC SELECT STATUS FROM V$INSTANCE;  -- Should show MOUNTED
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC **Congratulations!** Your Oracle database is now configured for CDC operations.
# MAGIC
# MAGIC ### Connection Details to Save
# MAGIC
# MAGIC Record these details for Lab 3:
# MAGIC
# MAGIC ```python
# MAGIC # Oracle Connection Information (needed for Lakeflow Connect)
# MAGIC ORACLE_HOST = "your_oracle_host"
# MAGIC ORACLE_PORT = 1521
# MAGIC ORACLE_SERVICE_NAME = "RETAILDB"
# MAGIC CDC_USERNAME = "CDC_USER"
# MAGIC CDC_PASSWORD = "SecureCDCPassword123!"  # Store in Databricks Secrets!
# MAGIC
# MAGIC # Schema Information
# MAGIC SOURCE_SCHEMA = "RETAIL"
# MAGIC TABLES_TO_INGEST = ["CUSTOMERS", "ORDERS", "PRODUCTS"]
# MAGIC
# MAGIC # Current SCN (baseline for CDC)
# MAGIC INITIAL_SCN = "8745000"  # Replace with your actual SCN from validation query
# MAGIC ```
# MAGIC
# MAGIC ### Proceed to Lab 3
# MAGIC
# MAGIC Now that Oracle is configured, move on to **Lab 3: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** where you'll:
# MAGIC - Create Unity Catalog connection with your CDC credentials
# MAGIC - Configure Lakeflow Connect ingestion gateway
# MAGIC - Build the ingestion pipeline
# MAGIC - Perform the initial snapshot
# MAGIC - Enable incremental CDC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Oracle ARCHIVELOG Mode Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/admin/managing-archived-redo-log-files.html)
# MAGIC - [Oracle Supplemental Logging](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/using-logminer.html#GUID-D857AF96-AC24-4CA1-B620-8EA3DF30D72E)
# MAGIC - [Oracle LogMiner Concepts](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
