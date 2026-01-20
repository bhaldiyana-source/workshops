# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Validating Incremental CDC with CRUD Operations
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll test incremental CDC by performing INSERT, UPDATE, and DELETE operations on your SQL Server database, then running your Lakeflow Connect pipeline to verify that changes are captured and applied correctly to your bronze Delta tables.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Perform CRUD operations on SQL Server and track them through CDC
# MAGIC - Run Lakeflow Connect pipeline in incremental mode
# MAGIC - Verify INSERT operations appear in bronze tables
# MAGIC - Validate UPDATE operations with before/after values
# MAGIC - Confirm DELETE operations remove records appropriately
# MAGIC - Analyze CDC metadata and change tracking information
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2 (SQL Server CDC configured)
# MAGIC - Completed Lab 3 (Lakeflow Connect pipeline created and initial snapshot loaded)
# MAGIC - Pipeline configured for incremental mode
# MAGIC - SQL Server client (SSMS, Azure Data Studio, etc.)
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Baseline State
# MAGIC
# MAGIC Before making changes, let's capture the current state of our data.

# COMMAND ----------

# DBTITLE 1,Set Configuration Variables
# Catalog and schema names
catalog_name = "retail_analytics"
bronze_schema = "bronze"

# Use catalog
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {bronze_schema}")

print(f"✓ Using {catalog_name}.{bronze_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Record Current Row Counts

# COMMAND ----------

# DBTITLE 1,Baseline Record Counts
# MAGIC %sql
# MAGIC -- Capture baseline counts before making changes
# MAGIC SELECT 'customers' as table_name, COUNT(*) as row_count FROM customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*) FROM orders
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*) FROM products
# MAGIC ORDER BY table_name;

# COMMAND ----------

# Store baseline counts for comparison
baseline_counts = {
    'customers': spark.table("customers").count(),
    'orders': spark.table("orders").count(),
    'products': spark.table("products").count()
}

print("Baseline Record Counts:")
for table, count in baseline_counts.items():
    print(f"  {table}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Identify Test Records
# MAGIC
# MAGIC Let's note specific records we'll modify for testing.

# COMMAND ----------

# DBTITLE 1,Select Records for Testing
# MAGIC %sql
# MAGIC -- Find existing records we'll update
# MAGIC SELECT 
# MAGIC   customer_id, 
# MAGIC   first_name, 
# MAGIC   last_name, 
# MAGIC   email,
# MAGIC   city,
# MAGIC   state
# MAGIC FROM customers
# MAGIC WHERE customer_id IN (1001, 1002)
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** Note the current values for customers 1001 and 1002

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Perform INSERT Operations on SQL Server
# MAGIC
# MAGIC Now let's add new records to SQL Server.
# MAGIC
# MAGIC ### 2.1 Execute INSERT Statements
# MAGIC
# MAGIC **Connect to your SQL Server and run these INSERT statements:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- INSERT Test 1: Add new customer
# MAGIC INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, zip_code, created_date, last_updated)
# MAGIC VALUES (10001, 'Sarah', 'Connor', 'sarah.connor@email.com', '555-1234', 'Austin', 'TX', '78701', GETDATE(), GETDATE());
# MAGIC
# MAGIC -- INSERT Test 2: Add another customer
# MAGIC INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, zip_code, created_date, last_updated)
# MAGIC VALUES (10002, 'Kyle', 'Reese', 'kyle.reese@email.com', '555-5678', 'Houston', 'TX', '77001', GETDATE(), GETDATE());
# MAGIC
# MAGIC -- INSERT Test 3: Add new product
# MAGIC INSERT INTO products (product_id, product_name, category, price, stock_quantity, supplier, created_date, last_updated)
# MAGIC VALUES (3001, 'USB-C Cable', 'Electronics', 19.99, 500, 'Tech Supplies Inc', GETDATE(), GETDATE());
# MAGIC
# MAGIC -- INSERT Test 4: Add new order
# MAGIC INSERT INTO orders (order_id, customer_id, order_date, order_status, total_amount, payment_method, created_date, last_updated)
# MAGIC VALUES (6001, 10001, GETDATE(), 'PENDING', 19.99, 'Credit Card', GETDATE(), GETDATE());
# MAGIC
# MAGIC PRINT '4 INSERT operations completed';
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Verify Inserts in SQL Server
# MAGIC
# MAGIC **Run this query in SQL Server to confirm:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Verify new records exist in SQL Server
# MAGIC SELECT customer_id, first_name, last_name, email 
# MAGIC FROM customers 
# MAGIC WHERE customer_id IN (10001, 10002);
# MAGIC
# MAGIC SELECT product_id, product_name, price 
# MAGIC FROM products 
# MAGIC WHERE product_id = 3001;
# MAGIC
# MAGIC SELECT order_id, customer_id, total_amount 
# MAGIC FROM orders 
# MAGIC WHERE order_id = 6001;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** You should see 2 new customers, 1 new product, 1 new order in SQL Server

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Perform UPDATE Operations on SQL Server
# MAGIC
# MAGIC Let's modify existing records to test UPDATE capture.
# MAGIC
# MAGIC ### 3.1 Execute UPDATE Statements
# MAGIC
# MAGIC **In SQL Server, run:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- UPDATE Test 1: Change customer email
# MAGIC UPDATE customers
# MAGIC SET email = 'john.doe.new@email.com', last_updated = GETDATE()
# MAGIC WHERE customer_id = 1001;
# MAGIC
# MAGIC -- UPDATE Test 2: Change customer city and state
# MAGIC UPDATE customers
# MAGIC SET city = 'Miami', state = 'FL', last_updated = GETDATE()
# MAGIC WHERE customer_id = 1002;
# MAGIC
# MAGIC -- UPDATE Test 3: Change order status
# MAGIC UPDATE orders
# MAGIC SET order_status = 'SHIPPED', last_updated = GETDATE()
# MAGIC WHERE order_id = 5001;
# MAGIC
# MAGIC -- UPDATE Test 4: Change product price
# MAGIC UPDATE products
# MAGIC SET price = 24.99, last_updated = GETDATE()
# MAGIC WHERE product_id = 2002;
# MAGIC
# MAGIC PRINT '4 UPDATE operations completed';
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Verify Updates in SQL Server
# MAGIC
# MAGIC **Confirm changes in SQL Server:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Verify updates
# MAGIC SELECT customer_id, email, city, state, last_updated 
# MAGIC FROM customers 
# MAGIC WHERE customer_id IN (1001, 1002);
# MAGIC
# MAGIC SELECT order_id, order_status, last_updated 
# MAGIC FROM orders 
# MAGIC WHERE order_id = 5001;
# MAGIC
# MAGIC SELECT product_id, product_name, price, last_updated 
# MAGIC FROM products 
# MAGIC WHERE product_id = 2002;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** Customer 1001's email updated, customer 1002's location changed, order 5001 status = SHIPPED, product 2002 price = $24.99

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Perform DELETE Operations on SQL Server
# MAGIC
# MAGIC Let's test DELETE capture by removing records.
# MAGIC
# MAGIC ### 4.1 Execute DELETE Statements
# MAGIC
# MAGIC **In SQL Server, run:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- DELETE Test 1: Remove an order (must delete before customer due to FK)
# MAGIC DELETE FROM orders WHERE order_id = 6001;
# MAGIC
# MAGIC -- DELETE Test 2: Remove new customer
# MAGIC DELETE FROM customers WHERE customer_id = 10002;
# MAGIC
# MAGIC -- DELETE Test 3: Remove a product
# MAGIC DELETE FROM products WHERE product_id = 3001;
# MAGIC
# MAGIC PRINT '3 DELETE operations completed';
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Verify Deletes in SQL Server
# MAGIC
# MAGIC **Confirm records removed:**
# MAGIC
# MAGIC ```sql
# MAGIC -- These should return 0 rows
# MAGIC SELECT * FROM customers WHERE customer_id = 10002;
# MAGIC SELECT * FROM products WHERE product_id = 3001;
# MAGIC SELECT * FROM orders WHERE order_id = 6001;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Checkpoint:** Customer 10002, product 3001, and order 6001 deleted from SQL Server

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Run Incremental Pipeline
# MAGIC
# MAGIC Now let's run our Lakeflow Connect pipeline to capture and apply these changes.
# MAGIC
# MAGIC ### 5.1 Trigger Pipeline Execution
# MAGIC
# MAGIC **In Databricks UI:**
# MAGIC
# MAGIC 1. Navigate to **Data** → **Ingestion**
# MAGIC 2. Find `retail_ingestion_pipeline`
# MAGIC 3. Click **Start** or **Run Now**
# MAGIC 4. **Verify Mode:** Confirm it's running in **Incremental** mode (not Snapshot)
# MAGIC 5. **Wait for Completion:** Usually 1-3 minutes for small change volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Monitor Pipeline Progress

# COMMAND ----------

# DBTITLE 1,Check Pipeline Status
# MAGIC %sql
# MAGIC -- Monitor pipeline execution
# MAGIC SELECT 
# MAGIC   execution_id,
# MAGIC   execution_status,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   TIMESTAMPDIFF(SECOND, start_time, COALESCE(end_time, CURRENT_TIMESTAMP())) as duration_seconds,
# MAGIC   records_processed,
# MAGIC   CASE 
# MAGIC     WHEN execution_status = 'SUCCEEDED' THEN '✓ Ready to validate'
# MAGIC     WHEN execution_status = 'RUNNING' THEN '⏳ Wait for completion'
# MAGIC     WHEN execution_status = 'FAILED' THEN '✗ Check logs for errors'
# MAGIC     ELSE execution_status
# MAGIC   END as status_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ **Wait for pipeline status to show "SUCCEEDED" before proceeding**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Validate INSERT Operations
# MAGIC
# MAGIC Let's verify that inserted records now appear in bronze tables.
# MAGIC
# MAGIC ### 6.1 Check New Customers

# COMMAND ----------

# DBTITLE 1,Verify Inserted Customer (10001)
# MAGIC %sql
# MAGIC -- Should show Sarah Connor
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state,
# MAGIC   _commit_timestamp as ingested_at
# MAGIC FROM customers
# MAGIC WHERE customer_id = 10001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** One row for Sarah Connor (customer 10001) with recent `_commit_timestamp`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Check New Product

# COMMAND ----------

# DBTITLE 1,Verify Inserted Product
# MAGIC %sql
# MAGIC -- Note: Product 3001 was INSERTED then DELETED
# MAGIC -- Check if it exists (should be DELETED in Step 4)
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   price,
# MAGIC   _commit_timestamp
# MAGIC FROM products
# MAGIC WHERE product_id = 3001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Zero rows (product 3001 was deleted after insert)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Verify Record Count Changes

# COMMAND ----------

# DBTITLE 1,Compare Record Counts
# Get current counts
current_counts = {
    'customers': spark.table("customers").count(),
    'orders': spark.table("orders").count(),
    'products': spark.table("products").count()
}

print("Record Count Comparison:\n")
print(f"{'Table':<15} {'Baseline':<10} {'Current':<10} {'Change':<10} {'Status'}")
print("-" * 55)

for table in ['customers', 'orders', 'products']:
    baseline = baseline_counts[table]
    current = current_counts[table]
    change = current - baseline
    status = "✓" if change >= 0 else "✗"
    print(f"{table:<15} {baseline:<10} {current:<10} {change:+<10} {status}")

print("\nExpected Changes:")
print("  customers: +1 (10001 added, 10002 deleted)")
print("  orders: 0 (6001 added and deleted)")
print("  products: 0 (3001 added and deleted)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate UPDATE Operations
# MAGIC
# MAGIC Verify that updated records show new values.
# MAGIC
# MAGIC ### 7.1 Check Updated Customer Email

# COMMAND ----------

# DBTITLE 1,Verify Customer 1001 Email Update
# MAGIC %sql
# MAGIC -- Should show updated email
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   last_updated,
# MAGIC   _commit_timestamp
# MAGIC FROM customers
# MAGIC WHERE customer_id = 1001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Email = `john.doe.new@email.com`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Check Updated Customer Location

# COMMAND ----------

# DBTITLE 1,Verify Customer 1002 Location Update
# MAGIC %sql
# MAGIC -- Should show updated city and state
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   city,
# MAGIC   state,
# MAGIC   last_updated,
# MAGIC   _commit_timestamp
# MAGIC FROM customers
# MAGIC WHERE customer_id = 1002;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** City = `Miami`, State = `FL`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Check Updated Order Status

# COMMAND ----------

# DBTITLE 1,Verify Order Status Update
# MAGIC %sql
# MAGIC -- Should show SHIPPED status
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_status,
# MAGIC   total_amount,
# MAGIC   last_updated,
# MAGIC   _commit_timestamp
# MAGIC FROM orders
# MAGIC WHERE order_id = 5001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** order_status = `SHIPPED`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Check Updated Product Price

# COMMAND ----------

# DBTITLE 1,Verify Product Price Update
# MAGIC %sql
# MAGIC -- Should show new price
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   price,
# MAGIC   last_updated,
# MAGIC   _commit_timestamp
# MAGIC FROM products
# MAGIC WHERE product_id = 2002;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** price = `24.99` (updated from original price)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Validate DELETE Operations
# MAGIC
# MAGIC Confirm that deleted records are removed from bronze tables.
# MAGIC
# MAGIC ### 8.1 Verify Deleted Customer

# COMMAND ----------

# DBTITLE 1,Check for Deleted Customer 10002
# MAGIC %sql
# MAGIC -- Should return 0 rows
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email
# MAGIC FROM customers
# MAGIC WHERE customer_id = 10002;

# COMMAND ----------

# Record count check
deleted_customer_count = spark.table("customers").filter("customer_id = 10002").count()
print(f"Customer 10002 count: {deleted_customer_count}")
print("✓ DELETE validated" if deleted_customer_count == 0 else "✗ Record still exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Zero rows (customer 10002 was deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Verify Deleted Product

# COMMAND ----------

# DBTITLE 1,Check for Deleted Product 3001
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   price
# MAGIC FROM products
# MAGIC WHERE product_id = 3001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Zero rows (product 3001 was deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Verify Deleted Order

# COMMAND ----------

# DBTITLE 1,Check for Deleted Order 6001
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   total_amount
# MAGIC FROM orders
# MAGIC WHERE order_id = 6001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Zero rows (order 6001 was deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Analyze CDC Metadata
# MAGIC
# MAGIC Let's examine the CDC metadata columns added by Lakeflow Connect.
# MAGIC
# MAGIC ### 9.1 Check Commit Timestamps

# COMMAND ----------

# DBTITLE 1,View Recent Changes with Timestamps
# MAGIC %sql
# MAGIC -- Show recent changes ordered by commit timestamp
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   _commit_timestamp,
# MAGIC   TIMESTAMPDIFF(MINUTE, _commit_timestamp, CURRENT_TIMESTAMP()) as minutes_ago
# MAGIC FROM customers
# MAGIC WHERE _commit_timestamp > CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC ORDER BY _commit_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **Observations:**
# MAGIC - `_commit_timestamp`: When Lakeflow Connect applied the change to bronze table
# MAGIC - Recent timestamps indicate changes from this lab
# MAGIC - Can use for time-based queries and auditing

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 View Table History (Delta Lake Time Travel)

# COMMAND ----------

# DBTITLE 1,View Delta History for Customers Table
# MAGIC %sql
# MAGIC -- See all versions of the table
# MAGIC DESCRIBE HISTORY customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC **Delta Lake Versioning:**
# MAGIC - Each pipeline run creates a new version
# MAGIC - `version`: Sequential version number
# MAGIC - `timestamp`: When version was created
# MAGIC - `operation`: Type of operation (MERGE, WRITE, DELETE)
# MAGIC - `operationMetrics`: Rows added/updated/deleted

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 Time Travel to Previous Version (Optional)

# COMMAND ----------

# DBTITLE 1,Query Previous Version
# MAGIC %sql
# MAGIC -- Query table as it was before recent changes (adjust version number)
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state
# MAGIC FROM customers VERSION AS OF 0  -- Use version before changes
# MAGIC WHERE customer_id IN (1001, 1002);

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Adjust `VERSION AS OF 0` to the version number before your changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Performance Comparison
# MAGIC
# MAGIC Let's compare snapshot vs. incremental mode performance.

# COMMAND ----------

# DBTITLE 1,Compare Snapshot vs Incremental Performance
# MAGIC %sql
# MAGIC -- Compare execution times
# MAGIC SELECT 
# MAGIC   execution_id,
# MAGIC   execution_mode,
# MAGIC   records_processed,
# MAGIC   TIMESTAMPDIFF(SECOND, start_time, end_time) as duration_seconds,
# MAGIC   ROUND(records_processed / TIMESTAMPDIFF(SECOND, start_time, end_time), 2) as records_per_second
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND execution_status = 'SUCCEEDED'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected Observations:**
# MAGIC - **Snapshot mode**: Longer duration, more records (full table scan)
# MAGIC - **Incremental mode**: Shorter duration, fewer records (only changes)
# MAGIC - Incremental typically 10-100x faster for ongoing sync

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC ### Issue 1: Changes Not Appearing in Bronze
# MAGIC
# MAGIC **Diagnostic Steps:**

# COMMAND ----------

# DBTITLE 1,Check Pipeline Events for Errors
# MAGIC %sql
# MAGIC -- Look for recent errors
# MAGIC SELECT 
# MAGIC   execution_id,
# MAGIC   execution_status,
# MAGIC   start_time,
# MAGIC   error_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND execution_status IN ('FAILED', 'RUNNING')
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Solutions:**
# MAGIC - Verify pipeline ran in **Incremental** mode (not Snapshot)
# MAGIC - Check CDC still enabled on SQL Server
# MAGIC - Ensure sufficient time passed between making changes and running pipeline
# MAGIC - Check SQL Server Agent jobs are running

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 2: Deletes Not Reflected
# MAGIC
# MAGIC **Diagnostic:**

# COMMAND ----------

# DBTITLE 1,Check for "Zombie" Records
# MAGIC %sql
# MAGIC -- Check if deleted records still exist
# MAGIC SELECT 'customer_10002' as test_case, COUNT(*) as count FROM customers WHERE customer_id = 10002
# MAGIC UNION ALL
# MAGIC SELECT 'product_3001', COUNT(*) FROM products WHERE product_id = 3001
# MAGIC UNION ALL
# MAGIC SELECT 'order_6001', COUNT(*) FROM orders WHERE order_id = 6001;

# COMMAND ----------

# MAGIC %md
# MAGIC **If counts > 0:**
# MAGIC - Verify deletes executed in SQL Server
# MAGIC - Check CDC capture job in SQL Server is running
# MAGIC - Re-run pipeline
# MAGIC - Consider running snapshot mode to reset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 3: Updated Values Not Showing
# MAGIC
# MAGIC **Verify in SQL Server:**
# MAGIC ```sql
# MAGIC -- Run in SQL Server
# MAGIC SELECT customer_id, email FROM customers WHERE customer_id = 1001;
# MAGIC ```
# MAGIC
# MAGIC **If correct in SQL Server but not bronze:**
# MAGIC - Check pipeline completion time vs. update time
# MAGIC - Verify no errors in pipeline logs
# MAGIC - Re-run incremental sync

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Validation
# MAGIC
# MAGIC ### Final Validation Checklist

# COMMAND ----------

# DBTITLE 1,Comprehensive Validation Query
# MAGIC %sql
# MAGIC WITH validation_checks AS (
# MAGIC   -- Check INSERT: Customer 10001 exists
# MAGIC   SELECT 'INSERT - Customer 10001' as test,
# MAGIC          CASE WHEN COUNT(*) = 1 THEN '✓ PASS' ELSE '✗ FAIL' END as result
# MAGIC   FROM customers WHERE customer_id = 10001
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check UPDATE: Customer 1001 email changed
# MAGIC   SELECT 'UPDATE - Customer 1001 Email',
# MAGIC          CASE WHEN MAX(email) = 'john.doe.new@email.com' THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC   FROM customers WHERE customer_id = 1001
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check UPDATE: Customer 1002 location changed
# MAGIC   SELECT 'UPDATE - Customer 1002 Location',
# MAGIC          CASE WHEN MAX(city) = 'Miami' AND MAX(state) = 'FL' THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC   FROM customers WHERE customer_id = 1002
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check UPDATE: Order 5001 status
# MAGIC   SELECT 'UPDATE - Order 5001 Status',
# MAGIC          CASE WHEN MAX(order_status) = 'SHIPPED' THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC   FROM orders WHERE order_id = 5001
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check UPDATE: Product 2002 price
# MAGIC   SELECT 'UPDATE - Product 2002 Price',
# MAGIC          CASE WHEN MAX(price) = 24.99 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC   FROM products WHERE product_id = 2002
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check DELETE: Customer 10002 removed
# MAGIC   SELECT 'DELETE - Customer 10002',
# MAGIC          CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC   FROM customers WHERE customer_id = 10002
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check DELETE: Product 3001 removed
# MAGIC   SELECT 'DELETE - Product 3001',
# MAGIC          CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC   FROM products WHERE product_id = 3001
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check DELETE: Order 6001 removed
# MAGIC   SELECT 'DELETE - Order 6001',
# MAGIC          CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC   FROM orders WHERE order_id = 6001
# MAGIC )
# MAGIC SELECT * FROM validation_checks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ **Tested INSERT operations:**
# MAGIC - Added new customers, products, and orders in SQL Server
# MAGIC - Verified insertions appeared in bronze tables
# MAGIC
# MAGIC ✅ **Validated UPDATE operations:**
# MAGIC - Modified customer email and location
# MAGIC - Changed order status and product price
# MAGIC - Confirmed updates reflected in bronze tables
# MAGIC
# MAGIC ✅ **Confirmed DELETE operations:**
# MAGIC - Removed records from SQL Server
# MAGIC - Verified deletions propagated to bronze tables
# MAGIC
# MAGIC ✅ **Analyzed CDC metadata:**
# MAGIC - Examined commit timestamps
# MAGIC - Explored Delta Lake versioning
# MAGIC - Compared snapshot vs. incremental performance
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 6: Multi-Table CDC and Dependency Management** where you'll:
# MAGIC - Handle foreign key relationships
# MAGIC - Manage ingestion order for dependent tables
# MAGIC - Test referential integrity scenarios
# MAGIC - Implement error handling for constraint violations
# MAGIC
# MAGIC **Your CDC pipeline is working! Let's tackle multi-table scenarios next!** 🚀
