# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5: Validating Incremental CDC with CRUD Operations
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will test incremental Change Data Capture by performing INSERT, UPDATE, and DELETE operations on MySQL and validating that changes propagate to bronze Delta tables. You'll learn how to trigger incremental pipeline runs, inspect CDC metadata, and verify data consistency.
# MAGIC
# MAGIC **Estimated Time**: 30-40 minutes
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Switch from snapshot mode to incremental mode for ongoing CDC
# MAGIC - Perform INSERT operations on MySQL and validate propagation
# MAGIC - Perform UPDATE operations and verify before/after state changes
# MAGIC - Perform DELETE operations and confirm removal in bronze tables
# MAGIC - Inspect _commit_timestamp and other CDC metadata fields
# MAGIC - Query Delta table history to audit all changes
# MAGIC - Measure end-to-end latency from MySQL to Databricks
# MAGIC - Troubleshoot incremental ingestion issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2 (MySQL CDC Configuration)
# MAGIC - Completed Lab 3 (Lakeflow Connect Setup)
# MAGIC - Completed Lecture 4 (Ingestion Modes)
# MAGIC - Initial snapshot load completed successfully
# MAGIC - Ingestion Gateway status = RUNNING

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Verify Current State
# MAGIC
# MAGIC Before making changes, let's capture the baseline state of our bronze tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Capture current record counts
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
# MAGIC -- Check latest commit timestamps
# MAGIC SELECT 
# MAGIC     'customers' as table_name,
# MAGIC     MAX(_commit_timestamp) as latest_change,
# MAGIC     COUNT(*) as total_records
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'orders', MAX(_commit_timestamp), COUNT(*)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'products', MAX(_commit_timestamp), COUNT(*)
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# Store baseline counts for comparison
baseline_counts = spark.sql("""
    SELECT 'customers' as table_name, COUNT(*) as record_count 
    FROM retail_analytics.bronze.customers
    UNION ALL
    SELECT 'orders', COUNT(*) FROM retail_analytics.bronze.orders
    UNION ALL
    SELECT 'products', COUNT(*) FROM retail_analytics.bronze.products
""").collect()

print("Baseline Record Counts:")
print("=" * 50)
for row in baseline_counts:
    print(f"{row['table_name']:15s}: {row['record_count']:>6,} rows")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Perform INSERT Operations on MySQL
# MAGIC
# MAGIC Connect to your MySQL database and insert new records.
# MAGIC
# MAGIC ### Step 2.1: Insert New Customers
# MAGIC
# MAGIC Run these commands in your MySQL client:
# MAGIC
# MAGIC ```sql
# MAGIC USE retail_db;
# MAGIC
# MAGIC -- Insert new customers
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, city, state, zip_code)
# MAGIC VALUES 
# MAGIC     ('Jane', 'Smith', 'jane.smith@email.com', '555-0123', 'Seattle', 'WA', '98101'),
# MAGIC     ('Carlos', 'Rodriguez', 'carlos.r@email.com', '555-0124', 'Miami', 'FL', '33101'),
# MAGIC     ('Aisha', 'Patel', 'aisha.p@email.com', '555-0125', 'Austin', 'TX', '78701');
# MAGIC
# MAGIC -- Verify inserts
# MAGIC SELECT * FROM customers 
# MAGIC WHERE email IN ('jane.smith@email.com', 'carlos.r@email.com', 'aisha.p@email.com');
# MAGIC
# MAGIC -- Note the customer_id values for next operations
# MAGIC ```
# MAGIC
# MAGIC ### Step 2.2: Insert New Products
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert new products
# MAGIC INSERT INTO products (product_name, category, price, stock_quantity, supplier, description)
# MAGIC VALUES
# MAGIC     ('4K Webcam Pro', 'Electronics', 149.99, 45, 'Tech Supply Co', 'Professional 4K webcam with AI features'),
# MAGIC     ('Ergonomic Mouse Pad', 'Office Supplies', 34.99, 120, 'Office Depot', 'Gel-filled wrist support mouse pad'),
# MAGIC     ('Desk Cable Organizer', 'Office Supplies', 22.99, 180, 'Office Depot', 'Premium cable management system');
# MAGIC
# MAGIC -- Verify inserts
# MAGIC SELECT * FROM products 
# MAGIC WHERE product_name LIKE '%Pro%' OR product_name LIKE '%Cable%';
# MAGIC ```
# MAGIC
# MAGIC ### Step 2.3: Insert New Orders
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert orders for newly created customers
# MAGIC -- Replace customer_id values with actual IDs from your insert
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
# MAGIC VALUES
# MAGIC     (11, 'PENDING', 149.99, '123 Pike St, Seattle, WA 98101', 'Credit Card'),
# MAGIC     (12, 'PROCESSING', 57.98, '456 Ocean Dr, Miami, FL 33101', 'PayPal'),
# MAGIC     (13, 'PENDING', 22.99, '789 Congress Ave, Austin, TX 78701', 'Credit Card');
# MAGIC
# MAGIC -- Verify inserts with customer join
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     o.order_status,
# MAGIC     o.total_amount
# MAGIC FROM orders o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id >= 11;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Trigger Incremental Pipeline Run
# MAGIC
# MAGIC Now let's run the ingestion pipeline in incremental mode to capture the changes.
# MAGIC
# MAGIC ### Step 3.1: Run Pipeline from UI
# MAGIC
# MAGIC 1. Navigate to **Data** → **Lakeflow Connect** → **Ingestion Pipelines**
# MAGIC 2. Select `retail_ingestion_pipeline`
# MAGIC 3. Click **Run Now** (or **Refresh** if it shows that button)
# MAGIC 4. Wait for pipeline to complete (typically 30-60 seconds for small changes)
# MAGIC 5. Status should change from RUNNING → COMPLETED
# MAGIC
# MAGIC ### Step 3.2: Monitor Pipeline Progress

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent pipeline executions
# MAGIC SELECT 
# MAGIC     update_id,
# MAGIC     timestamp,
# MAGIC     state,
# MAGIC     message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ **Wait for pipeline to complete** before proceeding to validation.
# MAGIC
# MAGIC Expected: State = 'COMPLETED' in the query above

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Validate INSERT Operations
# MAGIC
# MAGIC Verify that the newly inserted records appear in bronze tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for newly inserted customers
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     city,
# MAGIC     state,
# MAGIC     _commit_timestamp,
# MAGIC     _commit_version
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email IN ('jane.smith@email.com', 'carlos.r@email.com', 'aisha.p@email.com')
# MAGIC ORDER BY _commit_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate new record count
# MAGIC SELECT 
# MAGIC     COUNT(*) as current_count,
# MAGIC     COUNT(*) - 10 as new_records  -- Assuming baseline was 10
# MAGIC FROM retail_analytics.bronze.customers;
# MAGIC
# MAGIC -- Expected: new_records = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for newly inserted products
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     price,
# MAGIC     stock_quantity,
# MAGIC     _commit_timestamp
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE product_name IN ('4K Webcam Pro', 'Ergonomic Mouse Pad', 'Desk Cable Organizer')
# MAGIC ORDER BY _commit_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify new orders with customer details
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.email,
# MAGIC     o.order_status,
# MAGIC     o.total_amount,
# MAGIC     o._commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.email IN ('jane.smith@email.com', 'carlos.r@email.com', 'aisha.p@email.com')
# MAGIC ORDER BY o._commit_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Perform UPDATE Operations on MySQL
# MAGIC
# MAGIC Now let's test UPDATE operations by modifying existing records.
# MAGIC
# MAGIC ### Step 5.1: Update Customer Information
# MAGIC
# MAGIC Run in MySQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- Update customer address
# MAGIC UPDATE customers
# MAGIC SET city = 'Bellevue', 
# MAGIC     zip_code = '98004',
# MAGIC     address = '999 Tech Blvd'
# MAGIC WHERE email = 'jane.smith@email.com';
# MAGIC
# MAGIC -- Verify update
# MAGIC SELECT customer_id, first_name, last_name, city, zip_code, address, last_updated
# MAGIC FROM customers
# MAGIC WHERE email = 'jane.smith@email.com';
# MAGIC ```
# MAGIC
# MAGIC ### Step 5.2: Update Order Status
# MAGIC
# MAGIC ```sql
# MAGIC -- Update order from PENDING to SHIPPED
# MAGIC UPDATE orders
# MAGIC SET order_status = 'SHIPPED'
# MAGIC WHERE order_id IN (
# MAGIC     SELECT order_id FROM orders 
# MAGIC     WHERE order_status = 'PENDING' 
# MAGIC     LIMIT 2
# MAGIC );
# MAGIC
# MAGIC -- Verify updates
# MAGIC SELECT order_id, customer_id, order_status, total_amount, last_updated
# MAGIC FROM orders
# MAGIC WHERE order_status = 'SHIPPED'
# MAGIC ORDER BY last_updated DESC
# MAGIC LIMIT 5;
# MAGIC ```
# MAGIC
# MAGIC ### Step 5.3: Update Product Prices
# MAGIC
# MAGIC ```sql
# MAGIC -- Apply 10% discount to Electronics category
# MAGIC UPDATE products
# MAGIC SET price = ROUND(price * 0.90, 2)
# MAGIC WHERE category = 'Electronics'
# MAGIC   AND product_name LIKE '%Mouse%';
# MAGIC
# MAGIC -- Verify price changes
# MAGIC SELECT product_id, product_name, category, price, last_updated
# MAGIC FROM products
# MAGIC WHERE product_name LIKE '%Mouse%'
# MAGIC ORDER BY last_updated DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Trigger Pipeline and Validate UPDATES
# MAGIC
# MAGIC Run the pipeline again to capture UPDATE operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.1: Run Pipeline
# MAGIC
# MAGIC 1. Go to pipeline UI and click **Run Now**
# MAGIC 2. Wait for completion (30-60 seconds)
# MAGIC 3. Verify status = COMPLETED

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check latest pipeline execution
# MAGIC SELECT 
# MAGIC     update_id,
# MAGIC     timestamp,
# MAGIC     state,
# MAGIC     message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.2: Validate Customer UPDATE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify customer address update
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     city,
# MAGIC     zip_code,
# MAGIC     address,
# MAGIC     _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'jane.smith@email.com';
# MAGIC
# MAGIC -- Expected: city = 'Bellevue', zip_code = '98004'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View change history for this customer using Delta table history
# MAGIC SELECT 
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationParameters
# MAGIC FROM (DESCRIBE HISTORY retail_analytics.bronze.customers)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.3: Validate Order Status UPDATES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check order status changes
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     total_amount,
# MAGIC     _commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE order_status = 'SHIPPED'
# MAGIC ORDER BY _commit_timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.4: Validate Product Price UPDATES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check updated product prices
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     price,
# MAGIC     _commit_timestamp
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE product_name LIKE '%Mouse%'
# MAGIC ORDER BY _commit_timestamp DESC;
# MAGIC
# MAGIC -- Verify prices reflect 10% discount

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7: Perform DELETE Operations on MySQL
# MAGIC
# MAGIC Test CDC capture of DELETE operations.
# MAGIC
# MAGIC ⚠️ **Note**: Due to foreign key constraints, we need to delete orders before customers.
# MAGIC
# MAGIC ### Step 7.1: Delete Test Records
# MAGIC
# MAGIC Run in MySQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a test customer and order for deletion
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, city, state)
# MAGIC VALUES ('Test', 'DeleteMe', 'test.delete@email.com', '555-9999', 'Test City', 'TX');
# MAGIC
# MAGIC -- Get the customer_id
# MAGIC SELECT customer_id FROM customers WHERE email = 'test.delete@email.com';
# MAGIC -- Note the customer_id (e.g., 14)
# MAGIC
# MAGIC -- Insert an order for this customer
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, payment_method)
# MAGIC VALUES (14, 'CANCELLED', 0.01, 'Test');  -- Replace 14 with actual customer_id
# MAGIC
# MAGIC -- Now delete the order first (to satisfy FK constraint)
# MAGIC DELETE FROM orders 
# MAGIC WHERE customer_id = (SELECT customer_id FROM customers WHERE email = 'test.delete@email.com');
# MAGIC
# MAGIC -- Then delete the customer
# MAGIC DELETE FROM customers 
# MAGIC WHERE email = 'test.delete@email.com';
# MAGIC
# MAGIC -- Verify deletion
# MAGIC SELECT * FROM customers WHERE email = 'test.delete@email.com';
# MAGIC -- Should return empty result
# MAGIC ```
# MAGIC
# MAGIC ### Step 7.2: Delete a Product
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert and delete a test product
# MAGIC INSERT INTO products (product_name, category, price, stock_quantity)
# MAGIC VALUES ('Test Product DELETE', 'Test Category', 0.01, 0);
# MAGIC
# MAGIC -- Get product_id
# MAGIC SELECT product_id FROM products WHERE product_name = 'Test Product DELETE';
# MAGIC -- Note the product_id
# MAGIC
# MAGIC -- Delete the product
# MAGIC DELETE FROM products 
# MAGIC WHERE product_name = 'Test Product DELETE';
# MAGIC
# MAGIC -- Verify deletion
# MAGIC SELECT * FROM products WHERE product_name = 'Test Product DELETE';
# MAGIC -- Should return empty
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8: Trigger Pipeline and Validate DELETES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8.1: Run Pipeline
# MAGIC
# MAGIC Run the pipeline once more to capture DELETE operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor pipeline execution
# MAGIC SELECT 
# MAGIC     update_id,
# MAGIC     timestamp,
# MAGIC     state,
# MAGIC     message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8.2: Validate DELETE Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify test customer is deleted from bronze
# MAGIC SELECT COUNT(*) as count_should_be_zero
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'test.delete@email.com';
# MAGIC
# MAGIC -- Expected: 0 rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify test product is deleted
# MAGIC SELECT COUNT(*) as count_should_be_zero
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE product_name = 'Test Product DELETE';
# MAGIC
# MAGIC -- Expected: 0 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9: Measure End-to-End Latency
# MAGIC
# MAGIC Let's measure how long it takes for a change in MySQL to appear in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9.1: Insert a Timestamped Record
# MAGIC
# MAGIC Run in MySQL and note the exact time:
# MAGIC
# MAGIC ```sql
# MAGIC -- Note current time
# MAGIC SELECT NOW() as insert_time;
# MAGIC
# MAGIC -- Insert a latency test record
# MAGIC INSERT INTO customers (first_name, last_name, email, city, state)
# MAGIC VALUES ('Latency', 'Test', 'latency.test@email.com', 'Latency City', 'TX');
# MAGIC
# MAGIC -- Verify and note created_date
# MAGIC SELECT customer_id, first_name, last_name, email, created_date
# MAGIC FROM customers
# MAGIC WHERE email = 'latency.test@email.com';
# MAGIC ```
# MAGIC
# MAGIC ### Step 9.2: Run Pipeline and Measure

# COMMAND ----------

import time
from datetime import datetime

# Record pipeline start time
pipeline_start = datetime.now()
print(f"Pipeline trigger time: {pipeline_start}")
print("Run pipeline now and wait for completion...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9.3: Check When Record Appears

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if latency test record exists
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     created_date as mysql_insert_time,
# MAGIC     _commit_timestamp as databricks_commit_time,
# MAGIC     TIMESTAMPDIFF(SECOND, created_date, _commit_timestamp) as latency_seconds
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'latency.test@email.com';

# COMMAND ----------

# Calculate total latency
query_time = datetime.now()
print(f"Query time: {query_time}")

# Note: Actual latency = (databricks_commit_time - mysql_insert_time)
# This measures pure CDC pipeline latency
# Typical values: 30-120 seconds for manual trigger, 1-5 minutes for scheduled

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 10: Analyze Change Patterns
# MAGIC
# MAGIC Let's analyze the types and volume of changes processed.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze changes by timestamp (last hour)
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('MINUTE', _commit_timestamp) as minute,
# MAGIC     COUNT(*) as change_count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC GROUP BY DATE_TRUNC('MINUTE', _commit_timestamp)
# MAGIC ORDER BY minute DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare record counts: before vs after
# MAGIC SELECT 
# MAGIC     'customers' as table_name,
# MAGIC     COUNT(*) as current_count,
# MAGIC     10 as baseline_count,  -- Update with your baseline
# MAGIC     COUNT(*) - 10 as net_change
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'orders', COUNT(*), 10, COUNT(*) - 10
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'products', COUNT(*), 15, COUNT(*) - 15
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all changes in last hour with metadata
# MAGIC SELECT 
# MAGIC     'customers' as table_name,
# MAGIC     customer_id as record_id,
# MAGIC     CONCAT(first_name, ' ', last_name) as record_description,
# MAGIC     _commit_timestamp,
# MAGIC     _commit_version
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'orders',
# MAGIC     order_id,
# MAGIC     CONCAT('Order #', order_id, ' - ', order_status),
# MAGIC     _commit_timestamp,
# MAGIC     _commit_version
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'products',
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     _commit_timestamp,
# MAGIC     _commit_version
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC
# MAGIC ORDER BY _commit_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ **Incremental CDC Testing**:
# MAGIC - Performed INSERT operations and validated propagation
# MAGIC - Performed UPDATE operations and verified changes
# MAGIC - Performed DELETE operations and confirmed removal
# MAGIC - All CRUD operations successfully captured by CDC
# MAGIC
# MAGIC ✅ **Pipeline Operations**:
# MAGIC - Triggered multiple incremental pipeline runs
# MAGIC - Monitored execution status and logs
# MAGIC - Verified COMPLETED status for all runs
# MAGIC
# MAGIC ✅ **Data Validation**:
# MAGIC - Inspected CDC metadata (_commit_timestamp, _commit_version)
# MAGIC - Verified referential integrity (orders → customers)
# MAGIC - Confirmed record counts match expectations
# MAGIC - Validated before/after states for UPDATES
# MAGIC
# MAGIC ✅ **Performance Analysis**:
# MAGIC - Measured end-to-end CDC latency
# MAGIC - Analyzed change patterns over time
# MAGIC - Reviewed Delta table history and versioning
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Incremental mode only processes changes** - much more efficient than snapshot for ongoing operations
# MAGIC
# MAGIC 2. **CDC metadata provides audit trail** - _commit_timestamp shows exactly when each change occurred
# MAGIC
# MAGIC 3. **Delta table history enables time travel** - can query data as of any previous version
# MAGIC
# MAGIC 4. **Foreign key dependencies matter** - must delete in correct order (child before parent)
# MAGIC
# MAGIC 5. **Typical CDC latency is 1-5 minutes** - acceptable for most near-real-time analytics use cases
# MAGIC
# MAGIC ### CDC Operations Validated
# MAGIC
# MAGIC | Operation | Source (MySQL) | Validated in Bronze | Latency |
# MAGIC |-----------|----------------|---------------------|---------|
# MAGIC | INSERT customers | ✅ 3 records | ✅ Appeared | ~30-60 sec |
# MAGIC | INSERT products | ✅ 3 records | ✅ Appeared | ~30-60 sec |
# MAGIC | INSERT orders | ✅ 3 records | ✅ Appeared | ~30-60 sec |
# MAGIC | UPDATE customers | ✅ Address change | ✅ Updated | ~30-60 sec |
# MAGIC | UPDATE orders | ✅ Status change | ✅ Updated | ~30-60 sec |
# MAGIC | UPDATE products | ✅ Price change | ✅ Updated | ~30-60 sec |
# MAGIC | DELETE customers | ✅ Test record | ✅ Removed | ~30-60 sec |
# MAGIC | DELETE products | ✅ Test record | ✅ Removed | ~30-60 sec |
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 6: Multi-Table CDC and Dependency Management** where you'll:
# MAGIC - Handle complex foreign key relationships
# MAGIC - Configure pipeline dependencies for correct execution order
# MAGIC - Test referential integrity scenarios
# MAGIC - Manage orphaned records and data quality issues
# MAGIC
# MAGIC **Before moving on, ensure**:
# MAGIC - [ ] All CRUD operations successfully propagated to bronze tables
# MAGIC - [ ] Pipeline executions completed without errors
# MAGIC - [ ] CDC metadata fields (_commit_timestamp) are populated correctly
# MAGIC - [ ] Referential integrity maintained (no orphaned orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Delta Lake Time Travel](https://docs.databricks.com/delta/history.html)
# MAGIC - [Delta Table History](https://docs.databricks.com/delta/history.html#query-an-older-snapshot-of-a-table-time-travel)
# MAGIC - [Lakeflow Connect Monitoring](https://docs.databricks.com/ingestion/lakeflow-connect/monitor.html)
# MAGIC - [MySQL Binary Log Events](https://dev.mysql.com/doc/refman/8.0/en/binary-log-event-types.html)
