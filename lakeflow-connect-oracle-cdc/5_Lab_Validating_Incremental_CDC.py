# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5: Validating Incremental CDC with CRUD Operations
# MAGIC
# MAGIC ## Lab Objectives
# MAGIC By the end of this lab, you will:
# MAGIC - Perform INSERT operations in Oracle and verify CDC capture
# MAGIC - Execute UPDATE operations and validate change propagation
# MAGIC - Test DELETE operations and confirm removal in bronze tables
# MAGIC - Measure end-to-end CDC latency
# MAGIC - Validate exactly-once semantics
# MAGIC - Monitor incremental mode operation
# MAGIC - Analyze CDC metadata and SCN progression
# MAGIC
# MAGIC **Estimated Duration:** 30 minutes  
# MAGIC **Prerequisites:** 
# MAGIC - Completion of Lab 3 (Pipeline operational with snapshot complete)
# MAGIC - Oracle database access for running DML operations
# MAGIC - Lakeflow Connect gateway and pipeline running

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Overview
# MAGIC
# MAGIC In this lab, you'll test the incremental CDC pipeline by making changes in Oracle and verifying they appear in Databricks:
# MAGIC
# MAGIC ```
# MAGIC Oracle Database                          Databricks Bronze Tables
# MAGIC ━━━━━━━━━━━━━━━━━                        ━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC
# MAGIC Test 1: INSERT                           Expected: New row appears
# MAGIC   INSERT INTO CUSTOMERS...     ═CDC═►    bronze.customers (+1 row)
# MAGIC
# MAGIC Test 2: UPDATE                           Expected: Existing row modified
# MAGIC   UPDATE CUSTOMERS SET...      ═CDC═►    bronze.customers (email changed)
# MAGIC
# MAGIC Test 3: DELETE                           Expected: Row removed
# MAGIC   DELETE FROM CUSTOMERS...     ═CDC═►    bronze.customers (-1 row)
# MAGIC
# MAGIC Test 4: Bulk Operations                  Expected: All changes captured
# MAGIC   Multiple INSERTs/UPDATEs     ═CDC═►    All operations applied correctly
# MAGIC
# MAGIC Test 5: Foreign Key Cascade              Expected: Order deletion cascades
# MAGIC   DELETE FROM CUSTOMERS...     ═CDC═►    Related orders also deleted
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Lab: Verify Pipeline Status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify bronze tables exist and have data
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   MAX(_commit_timestamp) AS latest_change
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'orders',
# MAGIC   COUNT(*),
# MAGIC   MAX(_commit_timestamp)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'products',
# MAGIC   COUNT(*),
# MAGIC   MAX(_commit_timestamp)
# MAGIC FROM retail_analytics.bronze.products;
# MAGIC
# MAGIC -- All tables should have data from snapshot

# COMMAND ----------

# Check gateway and pipeline status
def check_cdc_health():
    """Verify CDC components are operational"""
    issues = []
    
    # Check 1: Bronze tables exist
    try:
        tables = spark.sql("SHOW TABLES IN retail_analytics.bronze").collect()
        table_names = [row.tableName for row in tables]
        
        required_tables = ['customers', 'orders', 'products']
        missing = [t for t in required_tables if t not in table_names]
        
        if missing:
            issues.append(f"Missing tables: {missing}")
        else:
            print("✅ All bronze tables exist")
    except Exception as e:
        issues.append(f"Cannot access bronze schema: {e}")
    
    # Check 2: Recent data exists
    try:
        customer_count = spark.sql("SELECT COUNT(*) AS cnt FROM retail_analytics.bronze.customers").first().cnt
        if customer_count == 0:
            issues.append("Customers table is empty")
        else:
            print(f"✅ Customers table has {customer_count} rows")
    except Exception as e:
        issues.append(f"Cannot query customers: {e}")
    
    if issues:
        print("\n⚠️ Issues detected:")
        for issue in issues:
            print(f"   - {issue}")
        print("\n   Please complete Lab 3 before proceeding.")
        return False
    else:
        print("\n✅ CDC pipeline is healthy and ready for testing!")
        return True

check_cdc_health()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: INSERT Operation
# MAGIC
# MAGIC ### Capture Baseline State

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Record current state before INSERT
# MAGIC SELECT 
# MAGIC   COUNT(*) AS customer_count_before,
# MAGIC   MAX(CUSTOMER_ID) AS max_customer_id,
# MAGIC   MAX(_commit_timestamp) AS latest_change_before
# MAGIC FROM retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute INSERT in Oracle
# MAGIC
# MAGIC **Switch to your Oracle SQL client and execute:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Connect to Oracle as RETAIL user
# MAGIC -- Insert a new customer
# MAGIC INSERT INTO RETAIL.CUSTOMERS (
# MAGIC   CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, 
# MAGIC   ADDRESS, CITY, STATE, ZIP_CODE, CREATED_DATE, LAST_UPDATED
# MAGIC ) VALUES (
# MAGIC   200, 
# MAGIC   'CDC_Test', 
# MAGIC   'User', 
# MAGIC   'cdc.test@email.com', 
# MAGIC   '555-0200',
# MAGIC   '200 Test Street',
# MAGIC   'Seattle',
# MAGIC   'WA',
# MAGIC   '98101',
# MAGIC   SYSTIMESTAMP,
# MAGIC   SYSTIMESTAMP
# MAGIC );
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify in Oracle
# MAGIC SELECT * FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 200;
# MAGIC
# MAGIC -- Note the current time for latency measurement
# MAGIC SELECT TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS insert_time FROM DUAL;
# MAGIC ```
# MAGIC
# MAGIC **Record Oracle insert time here:**

# COMMAND ----------

# Record Oracle insert time
import datetime
oracle_insert_time = datetime.datetime.now()
print(f"Oracle INSERT executed at: {oracle_insert_time.strftime('%H:%M:%S')}")
print("Waiting for CDC to capture change...")
print("(Typical latency: 10-60 seconds)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for CDC Propagation

# COMMAND ----------

import time

def wait_for_change(table_name, expected_count, max_wait=120, check_interval=10):
    """Poll bronze table until expected row count appears"""
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        current_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM retail_analytics.bronze.{table_name}").first().cnt
        
        if current_count >= expected_count:
            elapsed = time.time() - start_time
            print(f"✅ Change detected in bronze.{table_name}!")
            print(f"   CDC Latency: {elapsed:.1f} seconds")
            return True
        
        print(f"[{int(time.time() - start_time)}s] Waiting... (current: {current_count}, expected: {expected_count})")
        time.sleep(check_interval)
    
    print(f"⏱️ Timeout after {max_wait}s. Change may not have propagated yet.")
    print("   - Check gateway status")
    print("   - Verify pipeline is running")
    print("   - Check Oracle COMMIT was executed")
    return False

# Wait for INSERT to propagate (expect 101 → 102 rows for customers)
# Note: Adjust expected count based on your actual baseline
wait_for_change("customers", expected_count=102)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify INSERT in Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify new customer appears in bronze table
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   FIRST_NAME,
# MAGIC   LAST_NAME,
# MAGIC   EMAIL,
# MAGIC   CITY,
# MAGIC   STATE,
# MAGIC   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID = 200;
# MAGIC
# MAGIC -- Expected: 1 row with customer_id=200

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare counts before and after
# MAGIC SELECT 
# MAGIC   COUNT(*) AS customer_count_after,
# MAGIC   MAX(_commit_timestamp) AS latest_change_after
# MAGIC FROM retail_analytics.bronze.customers;
# MAGIC
# MAGIC -- Expected: Count increased by 1, latest_change_after is recent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: UPDATE Operation
# MAGIC
# MAGIC ### Execute UPDATE in Oracle
# MAGIC
# MAGIC **In your Oracle SQL client:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Update the customer we just inserted
# MAGIC UPDATE RETAIL.CUSTOMERS
# MAGIC SET EMAIL = 'cdc.test.updated@email.com',
# MAGIC     PHONE = '555-9999',
# MAGIC     LAST_UPDATED = SYSTIMESTAMP
# MAGIC WHERE CUSTOMER_ID = 200;
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify in Oracle
# MAGIC SELECT CUSTOMER_ID, EMAIL, PHONE, LAST_UPDATED 
# MAGIC FROM RETAIL.CUSTOMERS 
# MAGIC WHERE CUSTOMER_ID = 200;
# MAGIC ```

# COMMAND ----------

# Wait for UPDATE to propagate
print("Waiting for UPDATE to propagate...")
time.sleep(30)  # Wait 30 seconds for CDC to process

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify UPDATE in Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check updated values appear
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   EMAIL,
# MAGIC   PHONE,
# MAGIC   LAST_UPDATED,
# MAGIC   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID = 200
# MAGIC ORDER BY _commit_timestamp DESC;
# MAGIC
# MAGIC -- Expected: Updated email and phone values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View change history using Delta time travel
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   EMAIL,
# MAGIC   PHONE,
# MAGIC   _commit_timestamp,
# MAGIC   _commit_version
# MAGIC FROM retail_analytics.bronze.customers VERSION AS OF 1  -- Initial snapshot
# MAGIC WHERE CUSTOMER_ID = 200
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   EMAIL,
# MAGIC   PHONE,
# MAGIC   _commit_timestamp,
# MAGIC   _commit_version
# MAGIC FROM retail_analytics.bronze.customers  -- Current version
# MAGIC WHERE CUSTOMER_ID = 200;
# MAGIC
# MAGIC -- Shows before and after values

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: DELETE Operation
# MAGIC
# MAGIC ### Execute DELETE in Oracle
# MAGIC
# MAGIC **In your Oracle SQL client:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Delete the test customer
# MAGIC DELETE FROM RETAIL.CUSTOMERS
# MAGIC WHERE CUSTOMER_ID = 200;
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify deletion in Oracle
# MAGIC SELECT COUNT(*) FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 200;
# MAGIC -- Expected: 0
# MAGIC ```

# COMMAND ----------

# Wait for DELETE to propagate
print("Waiting for DELETE to propagate...")
time.sleep(30)  # Wait 30 seconds for CDC to process

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify DELETE in Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify customer no longer exists
# MAGIC SELECT * FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID = 200;
# MAGIC
# MAGIC -- Expected: 0 rows (record deleted)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify row count decreased
# MAGIC SELECT COUNT(*) AS customer_count FROM retail_analytics.bronze.customers;
# MAGIC
# MAGIC -- Expected: Back to original count (or 101 if customer 200 was the only addition)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Bulk Operations
# MAGIC
# MAGIC ### Execute Multiple Changes in Oracle
# MAGIC
# MAGIC **In your Oracle SQL client:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Bulk INSERT: Add 10 new customers
# MAGIC BEGIN
# MAGIC   FOR i IN 201..210 LOOP
# MAGIC     INSERT INTO RETAIL.CUSTOMERS (
# MAGIC       CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, 
# MAGIC       CITY, STATE, ZIP_CODE, CREATED_DATE, LAST_UPDATED
# MAGIC     ) VALUES (
# MAGIC       i,
# MAGIC       'BulkTest' || i,
# MAGIC       'User',
# MAGIC       'bulk' || i || '@email.com',
# MAGIC       '555-' || LPAD(i, 4, '0'),
# MAGIC       'Portland',
# MAGIC       'OR',
# MAGIC       '97201',
# MAGIC       SYSTIMESTAMP,
# MAGIC       SYSTIMESTAMP
# MAGIC     );
# MAGIC   END LOOP;
# MAGIC   COMMIT;
# MAGIC END;
# MAGIC /
# MAGIC
# MAGIC -- Bulk UPDATE: Change state for existing customers
# MAGIC UPDATE RETAIL.CUSTOMERS
# MAGIC SET STATE = 'TX', 
# MAGIC     CITY = 'Austin',
# MAGIC     LAST_UPDATED = SYSTIMESTAMP
# MAGIC WHERE CUSTOMER_ID BETWEEN 1 AND 5;
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify in Oracle
# MAGIC SELECT COUNT(*) FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID BETWEEN 201 AND 210;
# MAGIC -- Expected: 10
# MAGIC
# MAGIC SELECT CUSTOMER_ID, CITY, STATE FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID BETWEEN 1 AND 5;
# MAGIC -- Expected: All should show Austin, TX
# MAGIC ```

# COMMAND ----------

# Wait for bulk operations to propagate
print("Waiting for bulk operations to propagate...")
print("(This may take 30-60 seconds)")
time.sleep(60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Bulk Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify 10 new customers were added
# MAGIC SELECT COUNT(*) AS bulk_insert_count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID BETWEEN 201 AND 210;
# MAGIC
# MAGIC -- Expected: 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify bulk UPDATE propagated
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   FIRST_NAME,
# MAGIC   CITY,
# MAGIC   STATE,
# MAGIC   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID BETWEEN 1 AND 5
# MAGIC ORDER BY CUSTOMER_ID;
# MAGIC
# MAGIC -- Expected: All show Austin, TX

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check overall row count increased
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_customers,
# MAGIC   COUNT(CASE WHEN CUSTOMER_ID BETWEEN 201 AND 210 THEN 1 END) AS bulk_insert_customers,
# MAGIC   COUNT(CASE WHEN STATE = 'TX' THEN 1 END) AS texas_customers
# MAGIC FROM retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Foreign Key Cascade Delete
# MAGIC
# MAGIC ### Test Referential Integrity Handling
# MAGIC
# MAGIC **In your Oracle SQL client:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Check how many orders exist for customer 201
# MAGIC SELECT COUNT(*) AS order_count
# MAGIC FROM RETAIL.ORDERS
# MAGIC WHERE CUSTOMER_ID = 201;
# MAGIC
# MAGIC -- Create an order for customer 201 if none exist
# MAGIC INSERT INTO RETAIL.ORDERS (
# MAGIC   ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, 
# MAGIC   TOTAL_AMOUNT, PAYMENT_METHOD
# MAGIC ) VALUES (
# MAGIC   9001,
# MAGIC   201,
# MAGIC   SYSTIMESTAMP,
# MAGIC   'PENDING',
# MAGIC   99.99,
# MAGIC   'Credit Card'
# MAGIC );
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Delete customer 201 (CASCADE will delete order too)
# MAGIC DELETE FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 201;
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify both customer and order are gone
# MAGIC SELECT COUNT(*) FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 201;
# MAGIC -- Expected: 0
# MAGIC
# MAGIC SELECT COUNT(*) FROM RETAIL.ORDERS WHERE ORDER_ID = 9001;
# MAGIC -- Expected: 0 (cascaded delete)
# MAGIC ```

# COMMAND ----------

# Wait for cascade delete to propagate
print("Waiting for cascade delete to propagate...")
time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Cascade Delete in Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify customer 201 is deleted
# MAGIC SELECT * FROM retail_analytics.bronze.customers WHERE CUSTOMER_ID = 201;
# MAGIC -- Expected: 0 rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify related order is also deleted
# MAGIC SELECT * FROM retail_analytics.bronze.orders WHERE ORDER_ID = 9001;
# MAGIC -- Expected: 0 rows (cascade delete captured by CDC)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: CDC Latency Measurement

# COMMAND ----------

# Measure end-to-end CDC latency
def measure_cdc_latency(num_samples=5):
    """Measure CDC latency over multiple operations"""
    
    print(f"Measuring CDC latency over {num_samples} operations...\n")
    
    latencies = []
    
    for i in range(num_samples):
        # Record time before Oracle operation
        oracle_time = datetime.datetime.now()
        
        # Note: In real test, you would execute Oracle INSERT here
        print(f"Sample {i+1}: Simulating Oracle INSERT at {oracle_time.strftime('%H:%M:%S')}")
        
        # Wait and check when change appears
        max_wait = 120
        check_interval = 5
        detected = False
        
        for elapsed in range(0, max_wait, check_interval):
            time.sleep(check_interval)
            
            # Check latest timestamp in bronze table
            latest = spark.sql("""
                SELECT MAX(_commit_timestamp) AS latest 
                FROM retail_analytics.bronze.customers
            """).first().latest
            
            if latest and latest > oracle_time:
                latency = (datetime.datetime.now() - oracle_time).total_seconds()
                latencies.append(latency)
                print(f"   ✅ Detected after {latency:.1f}s\n")
                detected = True
                break
        
        if not detected:
            print(f"   ⏱️ Not detected within {max_wait}s\n")
    
    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        print("CDC Latency Summary:")
        print(f"  Average: {avg_latency:.1f}s")
        print(f"  Min: {min_latency:.1f}s")
        print(f"  Max: {max_latency:.1f}s")
    else:
        print("⚠️ No latency measurements collected")

# Note: Uncomment to run actual latency test
# measure_cdc_latency(num_samples=3)

print("Latency measurement framework ready.")
print("Execute Oracle INSERTs manually and observe bronze table updates.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Exactly-Once Semantics Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for duplicate records (should be none)
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   COUNT(*) AS occurrence_count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC GROUP BY CUSTOMER_ID
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC -- Expected: 0 rows (no duplicates, exactly-once maintained)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify primary key constraint is maintained
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT CUSTOMER_ID) AS unique_keys,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = COUNT(DISTINCT CUSTOMER_ID) THEN '✅ Valid'
# MAGIC     ELSE '❌ Duplicates detected'
# MAGIC   END AS status
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'orders',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT ORDER_ID),
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = COUNT(DISTINCT ORDER_ID) THEN '✅ Valid'
# MAGIC     ELSE '❌ Duplicates detected'
# MAGIC   END
# MAGIC FROM retail_analytics.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary and Analysis
# MAGIC
# MAGIC ### Comprehensive Validation Report

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate comprehensive CDC validation report
# MAGIC SELECT 
# MAGIC   'Total Customers' AS metric,
# MAGIC   CAST(COUNT(*) AS STRING) AS value
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Bulk Insert Customers (201-210)',
# MAGIC   CAST(COUNT(*) AS STRING)
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID BETWEEN 201 AND 210
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Texas Customers (Updated)',
# MAGIC   CAST(COUNT(*) AS STRING)
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE STATE = 'TX'
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Total Orders',
# MAGIC   CAST(COUNT(*) AS STRING)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Latest CDC Timestamp',
# MAGIC   CAST(MAX(_commit_timestamp) AS STRING)
# MAGIC FROM retail_analytics.bronze.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You Validated
# MAGIC
# MAGIC ✅ **INSERT Operations:**
# MAGIC - Single row inserts captured correctly
# MAGIC - Bulk inserts (10 rows) propagated successfully
# MAGIC - CDC latency measured (typically 10-60 seconds)
# MAGIC
# MAGIC ✅ **UPDATE Operations:**
# MAGIC - Field updates captured with new values
# MAGIC - Multiple column updates in single transaction
# MAGIC - Bulk updates across multiple rows
# MAGIC
# MAGIC ✅ **DELETE Operations:**
# MAGIC - Single row deletes processed correctly
# MAGIC - Cascade deletes captured (foreign key relationships)
# MAGIC - Row counts reconcile with Oracle
# MAGIC
# MAGIC ✅ **Data Quality:**
# MAGIC - No duplicate records (exactly-once semantics)
# MAGIC - Primary key uniqueness maintained
# MAGIC - All changes from Oracle present in bronze tables
# MAGIC
# MAGIC ✅ **CDC Metadata:**
# MAGIC - `_commit_timestamp` tracks when changes occurred
# MAGIC - `_commit_version` shows Delta table evolution
# MAGIC - SCN progression indicates incremental mode

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Guide
# MAGIC
# MAGIC ### Issue: Changes not appearing in bronze tables
# MAGIC
# MAGIC **Possible causes:**
# MAGIC 1. Oracle COMMIT not executed
# MAGIC    - Solution: Always COMMIT after DML in Oracle
# MAGIC 2. Gateway not running
# MAGIC    - Solution: Check gateway status in Lakeflow UI
# MAGIC 3. Pipeline not running
# MAGIC    - Solution: Start pipeline or check for errors
# MAGIC 4. Archive logs deleted
# MAGIC    - Solution: Verify archive log retention
# MAGIC
# MAGIC ### Issue: High CDC latency (> 2 minutes)
# MAGIC
# MAGIC **Possible causes:**
# MAGIC 1. Gateway undersized for change volume
# MAGIC    - Solution: Upgrade gateway VM size
# MAGIC 2. Pipeline cluster too small
# MAGIC    - Solution: Increase worker count or cluster size
# MAGIC 3. Network latency between Databricks and Oracle
# MAGIC    - Solution: Check network performance, consider private link
# MAGIC
# MAGIC ### Issue: Duplicate records appearing
# MAGIC
# MAGIC **Possible causes:**
# MAGIC 1. Pipeline restarted during processing
# MAGIC    - Solution: Normal - deduplication should resolve on next run
# MAGIC 2. Checkpoint corruption
# MAGIC    - Solution: Reset checkpoint or full refresh pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC **Congratulations!** You've successfully validated that incremental CDC is working correctly.
# MAGIC
# MAGIC **Key Achievements:**
# MAGIC - Verified all CRUD operations are captured
# MAGIC - Measured CDC latency
# MAGIC - Confirmed exactly-once semantics
# MAGIC - Tested referential integrity handling
# MAGIC
# MAGIC **Proceed to Lab 6** where you'll:
# MAGIC - Configure multi-table CDC with dependencies
# MAGIC - Handle complex foreign key relationships
# MAGIC - Implement parallel ingestion patterns
# MAGIC - Optimize for tables with different change velocities

# COMMAND ----------

# MAGIC %md
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
