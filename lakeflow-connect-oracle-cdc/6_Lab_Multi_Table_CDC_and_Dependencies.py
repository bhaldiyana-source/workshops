# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6: Multi-Table CDC and Dependency Management
# MAGIC
# MAGIC ## Lab Objectives
# MAGIC By the end of this lab, you will:
# MAGIC - Understand multi-table CDC orchestration challenges
# MAGIC - Handle foreign key dependencies in CDC pipelines
# MAGIC - Configure table ingestion priorities
# MAGIC - Test referential integrity across CDC operations
# MAGIC - Implement strategies for tables with different change velocities
# MAGIC - Monitor multi-table pipeline performance
# MAGIC
# MAGIC **Estimated Duration:** 20 minutes  
# MAGIC **Prerequisites:** 
# MAGIC - Completion of Lab 5 (Incremental CDC validated)
# MAGIC - Understanding of foreign key relationships
# MAGIC - Oracle RETAIL schema with CUSTOMERS → ORDERS relationship

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-Table CDC Challenges
# MAGIC
# MAGIC ### The Dependency Problem
# MAGIC
# MAGIC ```
# MAGIC Oracle Database                          Bronze Layer
# MAGIC ━━━━━━━━━━━━━━━━━                        ━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC
# MAGIC Transaction 1 (SCN 100):
# MAGIC   INSERT CUSTOMERS (ID=500)    ═CDC═►    Problem: What if ORDERS 
# MAGIC   INSERT ORDERS (CUSTOMER_ID=500)         arrives before CUSTOMERS?
# MAGIC   COMMIT
# MAGIC
# MAGIC                                           Foreign key violation if:
# MAGIC                                           1. ORDERS table processed first
# MAGIC                                           2. Or ORDERS captured but CUSTOMER not yet
# MAGIC
# MAGIC Challenge: Maintain referential integrity during CDC
# MAGIC ```
# MAGIC
# MAGIC ### CDC Ordering Guarantees
# MAGIC
# MAGIC **Lakeflow Connect provides:**
# MAGIC - Changes from single table ordered by SCN
# MAGIC - Changes within same transaction have same SCN
# MAGIC - Per-table processing (parallel)
# MAGIC
# MAGIC **What it doesn't guarantee:**
# MAGIC - Cross-table ordering without configuration
# MAGIC - Automatic dependency resolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Scenario: Customer-Order Relationship
# MAGIC
# MAGIC ### Current Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualize foreign key relationships
# MAGIC SELECT 
# MAGIC   'ORDERS' AS child_table,
# MAGIC   'CUSTOMER_ID' AS foreign_key_column,
# MAGIC   'CUSTOMERS' AS parent_table,
# MAGIC   'CUSTOMER_ID' AS parent_key_column,
# MAGIC   'ON DELETE CASCADE' AS constraint_rule
# MAGIC FROM (SELECT 1);  -- Placeholder query to document relationship

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current row counts
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   COUNT(DISTINCT CUSTOMER_ID) AS unique_keys
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'orders',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT ORDER_ID)
# MAGIC FROM retail_analytics.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Atomic Transaction with Dependencies
# MAGIC
# MAGIC ### Create Customer and Order in Single Transaction
# MAGIC
# MAGIC **In your Oracle SQL client, execute:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Single transaction: Create customer + order atomically
# MAGIC BEGIN
# MAGIC   -- Insert parent record
# MAGIC   INSERT INTO RETAIL.CUSTOMERS (
# MAGIC     CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, 
# MAGIC     CITY, STATE, ZIP_CODE
# MAGIC   ) VALUES (
# MAGIC     300, 'Dependency', 'Test', 'dep.test@email.com', '555-0300',
# MAGIC     'San Francisco', 'CA', '94102'
# MAGIC   );
# MAGIC   
# MAGIC   -- Insert child record referencing parent
# MAGIC   INSERT INTO RETAIL.ORDERS (
# MAGIC     ORDER_ID, CUSTOMER_ID, ORDER_STATUS, TOTAL_AMOUNT, PAYMENT_METHOD
# MAGIC   ) VALUES (
# MAGIC     9100, 300, 'PENDING', 150.00, 'Credit Card'
# MAGIC   );
# MAGIC   
# MAGIC   -- Commit both together (same SCN)
# MAGIC   COMMIT;
# MAGIC END;
# MAGIC /
# MAGIC
# MAGIC -- Verify in Oracle
# MAGIC SELECT * FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 300;
# MAGIC SELECT * FROM RETAIL.ORDERS WHERE ORDER_ID = 9100;
# MAGIC ```

# COMMAND ----------

# Wait for CDC to propagate
import time
print("Waiting for atomic transaction to propagate...")
time.sleep(45)  # Increased wait for both tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Both Records Arrived

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check customer arrived
# MAGIC SELECT 
# MAGIC   CUSTOMER_ID,
# MAGIC   FIRST_NAME,
# MAGIC   EMAIL,
# MAGIC   _commit_timestamp AS customer_commit_time
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID = 300;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check order arrived
# MAGIC SELECT 
# MAGIC   ORDER_ID,
# MAGIC   CUSTOMER_ID,
# MAGIC   ORDER_STATUS,
# MAGIC   TOTAL_AMOUNT,
# MAGIC   _commit_timestamp AS order_commit_time
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE ORDER_ID = 9100;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Referential Integrity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join to verify relationship
# MAGIC SELECT 
# MAGIC   o.ORDER_ID,
# MAGIC   o.CUSTOMER_ID,
# MAGIC   c.FIRST_NAME,
# MAGIC   c.EMAIL,
# MAGIC   o.TOTAL_AMOUNT,
# MAGIC   o._commit_timestamp AS order_commit,
# MAGIC   c._commit_timestamp AS customer_commit
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC INNER JOIN retail_analytics.bronze.customers c
# MAGIC   ON o.CUSTOMER_ID = c.CUSTOMER_ID
# MAGIC WHERE o.ORDER_ID = 9100;
# MAGIC
# MAGIC -- Expected: 1 row with matching customer data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Rapid Sequential Transactions
# MAGIC
# MAGIC ### Create Multiple Customers and Orders Quickly
# MAGIC
# MAGIC **In your Oracle SQL client:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Rapid insertion: 5 customers with 2 orders each
# MAGIC DECLARE
# MAGIC   v_customer_id NUMBER;
# MAGIC   v_order_id NUMBER := 9200;
# MAGIC BEGIN
# MAGIC   FOR i IN 1..5 LOOP
# MAGIC     v_customer_id := 400 + i;
# MAGIC     
# MAGIC     -- Insert customer
# MAGIC     INSERT INTO RETAIL.CUSTOMERS (
# MAGIC       CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE
# MAGIC     ) VALUES (
# MAGIC       v_customer_id,
# MAGIC       'RapidTest' || i,
# MAGIC       'User',
# MAGIC       'rapid' || i || '@email.com',
# MAGIC       'Austin',
# MAGIC       'TX'
# MAGIC     );
# MAGIC     
# MAGIC     -- Insert 2 orders per customer
# MAGIC     FOR j IN 1..2 LOOP
# MAGIC       INSERT INTO RETAIL.ORDERS (
# MAGIC         ORDER_ID, CUSTOMER_ID, ORDER_STATUS, TOTAL_AMOUNT, PAYMENT_METHOD
# MAGIC       ) VALUES (
# MAGIC         v_order_id,
# MAGIC         v_customer_id,
# MAGIC         'PROCESSING',
# MAGIC         ROUND(DBMS_RANDOM.VALUE(50, 500), 2),
# MAGIC         'Credit Card'
# MAGIC       );
# MAGIC       v_order_id := v_order_id + 1;
# MAGIC     END LOOP;
# MAGIC     
# MAGIC     -- Commit each customer+orders as separate transaction
# MAGIC     COMMIT;
# MAGIC   END LOOP;
# MAGIC END;
# MAGIC /
# MAGIC
# MAGIC -- Verify counts
# MAGIC SELECT COUNT(*) AS customer_count FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID BETWEEN 401 AND 405;
# MAGIC -- Expected: 5
# MAGIC
# MAGIC SELECT COUNT(*) AS order_count FROM RETAIL.ORDERS WHERE ORDER_ID BETWEEN 9200 AND 9209;
# MAGIC -- Expected: 10
# MAGIC ```

# COMMAND ----------

# Wait for rapid transactions to propagate
print("Waiting for rapid transactions to propagate...")
time.sleep(60)  # Longer wait for multiple transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Orphaned Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find orders without matching customers (orphans)
# MAGIC SELECT 
# MAGIC   o.ORDER_ID,
# MAGIC   o.CUSTOMER_ID,
# MAGIC   o.ORDER_STATUS,
# MAGIC   'ORPHANED' AS status
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c
# MAGIC   ON o.CUSTOMER_ID = c.CUSTOMER_ID
# MAGIC WHERE c.CUSTOMER_ID IS NULL
# MAGIC   AND o.ORDER_ID BETWEEN 9200 AND 9209;
# MAGIC
# MAGIC -- Expected: 0 rows (no orphans, all customers propagated)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all rapid test records arrived
# MAGIC SELECT 
# MAGIC   'Customers (401-405)' AS category,
# MAGIC   COUNT(*) AS count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE CUSTOMER_ID BETWEEN 401 AND 405
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Orders (9200-9209)',
# MAGIC   COUNT(*)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE ORDER_ID BETWEEN 9200 AND 9209;
# MAGIC
# MAGIC -- Expected: 5 customers, 10 orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Update Parent and Child in Single Transaction
# MAGIC
# MAGIC ### Test Cross-Table Update Consistency
# MAGIC
# MAGIC **In your Oracle SQL client:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Update both customer and order in single transaction
# MAGIC BEGIN
# MAGIC   -- Update customer address
# MAGIC   UPDATE RETAIL.CUSTOMERS
# MAGIC   SET CITY = 'Dallas',
# MAGIC       STATE = 'TX',
# MAGIC       LAST_UPDATED = SYSTIMESTAMP
# MAGIC   WHERE CUSTOMER_ID = 401;
# MAGIC   
# MAGIC   -- Update related order status
# MAGIC   UPDATE RETAIL.ORDERS
# MAGIC   SET ORDER_STATUS = 'SHIPPED',
# MAGIC       LAST_UPDATED = SYSTIMESTAMP
# MAGIC   WHERE CUSTOMER_ID = 401;
# MAGIC   
# MAGIC   COMMIT;
# MAGIC END;
# MAGIC /
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT CUSTOMER_ID, CITY, STATE, LAST_UPDATED 
# MAGIC FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 401;
# MAGIC
# MAGIC SELECT ORDER_ID, CUSTOMER_ID, ORDER_STATUS, LAST_UPDATED 
# MAGIC FROM RETAIL.ORDERS WHERE CUSTOMER_ID = 401;
# MAGIC ```

# COMMAND ----------

print("Waiting for cross-table update to propagate...")
time.sleep(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify both updates propagated
# MAGIC SELECT 
# MAGIC   c.CUSTOMER_ID,
# MAGIC   c.CITY,
# MAGIC   c.STATE,
# MAGIC   c._commit_timestamp AS customer_update_time,
# MAGIC   o.ORDER_ID,
# MAGIC   o.ORDER_STATUS,
# MAGIC   o._commit_timestamp AS order_update_time
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC INNER JOIN retail_analytics.bronze.orders o
# MAGIC   ON c.CUSTOMER_ID = o.CUSTOMER_ID
# MAGIC WHERE c.CUSTOMER_ID = 401;
# MAGIC
# MAGIC -- Expected: Customer shows Dallas/TX, orders show SHIPPED

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Delete Parent with Cascade to Child
# MAGIC
# MAGIC ### Test Foreign Key CASCADE DELETE
# MAGIC
# MAGIC **In your Oracle SQL client:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Check how many orders exist for customer 405
# MAGIC SELECT COUNT(*) AS order_count
# MAGIC FROM RETAIL.ORDERS
# MAGIC WHERE CUSTOMER_ID = 405;
# MAGIC -- Should be 2 orders
# MAGIC
# MAGIC -- Delete parent customer (CASCADE will delete child orders)
# MAGIC DELETE FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 405;
# MAGIC
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify in Oracle
# MAGIC SELECT COUNT(*) FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 405;
# MAGIC -- Expected: 0
# MAGIC
# MAGIC SELECT COUNT(*) FROM RETAIL.ORDERS WHERE CUSTOMER_ID = 405;
# MAGIC -- Expected: 0 (cascaded)
# MAGIC ```

# COMMAND ----------

print("Waiting for cascade delete to propagate...")
time.sleep(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify customer deleted
# MAGIC SELECT * FROM retail_analytics.bronze.customers WHERE CUSTOMER_ID = 405;
# MAGIC -- Expected: 0 rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify related orders also deleted
# MAGIC SELECT * FROM retail_analytics.bronze.orders WHERE CUSTOMER_ID = 405;
# MAGIC -- Expected: 0 rows (cascade captured by CDC)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing Multi-Table CDC Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare change velocity across tables
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT DATE(_commit_timestamp)) AS days_with_changes,
# MAGIC   MIN(_commit_timestamp) AS first_change,
# MAGIC   MAX(_commit_timestamp) AS latest_change
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'orders',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT DATE(_commit_timestamp)),
# MAGIC   MIN(_commit_timestamp),
# MAGIC   MAX(_commit_timestamp)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'products',
# MAGIC   COUNT(*),
# MAGIC   COUNT(DISTINCT DATE(_commit_timestamp)),
# MAGIC   MIN(_commit_timestamp),
# MAGIC   MAX(_commit_timestamp)
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for orphaned orders (orders without matching customers)
# MAGIC SELECT 
# MAGIC   COUNT(*) AS orphaned_order_count
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c
# MAGIC   ON o.CUSTOMER_ID = c.CUSTOMER_ID
# MAGIC WHERE c.CUSTOMER_ID IS NULL;
# MAGIC
# MAGIC -- Expected: 0 (no orphans)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find customers without any orders
# MAGIC SELECT 
# MAGIC   COUNT(*) AS customers_without_orders
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o
# MAGIC   ON c.CUSTOMER_ID = o.CUSTOMER_ID
# MAGIC WHERE o.ORDER_ID IS NULL;
# MAGIC
# MAGIC -- This is OK - not all customers must have orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate foreign key constraint is logically maintained
# MAGIC SELECT 
# MAGIC   'Valid References' AS status,
# MAGIC   COUNT(*) AS order_count
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC INNER JOIN retail_analytics.bronze.customers c
# MAGIC   ON o.CUSTOMER_ID = c.CUSTOMER_ID;
# MAGIC
# MAGIC -- Should match total order count if no orphans

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Multi-Table CDC
# MAGIC
# MAGIC ### Strategy 1: Leverage Same-Transaction Guarantees
# MAGIC ```
# MAGIC ✅ GOOD: Atomic operations in Oracle
# MAGIC BEGIN
# MAGIC   INSERT CUSTOMERS ...
# MAGIC   INSERT ORDERS ...
# MAGIC   COMMIT;  -- Same SCN for both
# MAGIC END;
# MAGIC
# MAGIC ❌ AVOID: Separate transactions
# MAGIC INSERT CUSTOMERS ...
# MAGIC COMMIT;  -- SCN 100
# MAGIC INSERT ORDERS ...
# MAGIC COMMIT;  -- SCN 101 (race condition possible)
# MAGIC ```
# MAGIC
# MAGIC ### Strategy 2: Bronze Layer as Staging
# MAGIC ```
# MAGIC Bronze Layer (Current):
# MAGIC   - Mirrors Oracle structure
# MAGIC   - May have temporary inconsistencies during propagation
# MAGIC   - Use for staging only
# MAGIC
# MAGIC Silver Layer (Next):
# MAGIC   - Apply referential integrity checks
# MAGIC   - Filter out orphans if they occur
# MAGIC   - Enforce business rules
# MAGIC ```
# MAGIC
# MAGIC ### Strategy 3: Monitoring Cross-Table Lag

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create monitoring view for cross-table sync
# MAGIC CREATE OR REPLACE VIEW retail_analytics.monitoring.table_sync_status AS
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   MAX(_commit_timestamp) AS latest_change,
# MAGIC   COUNT(*) AS row_count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'orders',
# MAGIC   MAX(_commit_timestamp),
# MAGIC   COUNT(*)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'products',
# MAGIC   MAX(_commit_timestamp),
# MAGIC   COUNT(*)
# MAGIC FROM retail_analytics.bronze.products;
# MAGIC
# MAGIC -- Query to check synchronization
# MAGIC SELECT * FROM retail_analytics.monitoring.table_sync_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detect significant lag between tables
# MAGIC WITH table_times AS (
# MAGIC   SELECT MAX(_commit_timestamp) AS max_time FROM retail_analytics.bronze.customers
# MAGIC   UNION ALL
# MAGIC   SELECT MAX(_commit_timestamp) FROM retail_analytics.bronze.orders
# MAGIC   UNION ALL
# MAGIC   SELECT MAX(_commit_timestamp) FROM retail_analytics.bronze.products
# MAGIC )
# MAGIC SELECT 
# MAGIC   MAX(max_time) AS latest_table_time,
# MAGIC   MIN(max_time) AS earliest_table_time,
# MAGIC   TIMESTAMPDIFF(SECOND, MIN(max_time), MAX(max_time)) AS lag_seconds,
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MIN(max_time), MAX(max_time)) > 300 THEN '⚠️ High lag'
# MAGIC     ELSE '✅ Synchronized'
# MAGIC   END AS status
# MAGIC FROM table_times;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Tested
# MAGIC
# MAGIC ✅ **Atomic Transactions:**
# MAGIC - Customer + Order inserted in single transaction
# MAGIC - Both records propagated correctly
# MAGIC - Same SCN ensures ordering
# MAGIC
# MAGIC ✅ **Rapid Sequential Operations:**
# MAGIC - 5 customers + 10 orders created rapidly
# MAGIC - No orphaned records detected
# MAGIC - Foreign key relationships maintained
# MAGIC
# MAGIC ✅ **Cross-Table Updates:**
# MAGIC - Parent and child updated in single transaction
# MAGIC - Both updates propagated consistently
# MAGIC
# MAGIC ✅ **Cascade Deletes:**
# MAGIC - Parent delete triggered child deletion
# MAGIC - CDC captured both delete operations
# MAGIC - Referential integrity preserved
# MAGIC
# MAGIC ✅ **Performance Monitoring:**
# MAGIC - Created views to monitor table synchronization
# MAGIC - Implemented lag detection queries
# MAGIC - Validated no orphaned records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Learnings
# MAGIC
# MAGIC **1. Transaction Boundaries Matter**
# MAGIC - Operations in same Oracle transaction get same SCN
# MAGIC - Guarantees ordering within transaction
# MAGIC - Use atomic transactions for related data
# MAGIC
# MAGIC **2. Bronze Layer is Eventually Consistent**
# MAGIC - Temporary inconsistencies may occur during propagation
# MAGIC - Typically resolve within seconds
# MAGIC - Don't enforce strict constraints in bronze
# MAGIC
# MAGIC **3. Monitoring is Critical**
# MAGIC - Track lag between tables
# MAGIC - Alert on orphaned records
# MAGIC - Validate referential integrity periodically
# MAGIC
# MAGIC **4. Foreign Key Cascades Work**
# MAGIC - CASCADE DELETE captured by CDC
# MAGIC - All related records propagate
# MAGIC - Schema design from Oracle is preserved

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC **Congratulations!** You've mastered multi-table CDC with dependency management.
# MAGIC
# MAGIC **Proceed to Lecture 7** to learn about:
# MAGIC - Production deployment best practices
# MAGIC - Monitoring strategies for complex pipelines
# MAGIC - Performance optimization techniques
# MAGIC - Disaster recovery planning
# MAGIC
# MAGIC **Then Lab 8** where you'll:
# MAGIC - Implement scheduled pipeline execution
# MAGIC - Configure error handling and retry logic
# MAGIC - Set up comprehensive alerting
# MAGIC - Build production-grade monitoring dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
