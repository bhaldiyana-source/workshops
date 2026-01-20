# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5: Validating Incremental CDC with CRUD Operations
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will test incremental CDC by performing INSERT, UPDATE, and DELETE operations in PostgreSQL and validating that changes propagate to your Delta Lake bronze tables.
# MAGIC
# MAGIC **Duration:** 30 minutes
# MAGIC
# MAGIC **Objectives:**
# MAGIC - Capture current baseline state of bronze tables
# MAGIC - Perform INSERT operations in PostgreSQL and verify in Databricks
# MAGIC - Perform UPDATE operations and track changes
# MAGIC - Perform DELETE operations and validate handling
# MAGIC - Measure end-to-end latency
# MAGIC - Analyze CDC metadata
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Completed Lab 3: Lakeflow Connect setup
# MAGIC - Pipeline running with scheduled execution
# MAGIC - Access to PostgreSQL database

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Baseline Snapshot
# MAGIC
# MAGIC ### Step 1.1: Record Current State
# MAGIC
# MAGIC Before making changes, capture the current state of all tables:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create temporary view of current state
# MAGIC CREATE OR REPLACE TEMP VIEW baseline_counts AS
# MAGIC SELECT 'customers' as table_name, COUNT(*) as count, CURRENT_TIMESTAMP() as snapshot_time 
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*), CURRENT_TIMESTAMP()
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*), CURRENT_TIMESTAMP()
# MAGIC FROM retail_analytics.bronze.products;
# MAGIC
# MAGIC SELECT * FROM baseline_counts;

# COMMAND ----------

# Save baseline for comparison
baseline_df = spark.sql("SELECT * FROM baseline_counts")
baseline_df.createOrReplaceTempView("baseline_state")

print("✓ Baseline captured")
baseline_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Capture Latest Commit Timestamps

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Record latest commit timestamp for each table
# MAGIC SELECT 
# MAGIC #   'customers' as table_name,
# MAGIC #   MAX(_commit_timestamp) as latest_commit,
# MAGIC #   CURRENT_TIMESTAMP() as check_time
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', MAX(_commit_timestamp), CURRENT_TIMESTAMP()
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'products', MAX(_commit_timestamp), CURRENT_TIMESTAMP()
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Test INSERT Operations
# MAGIC
# MAGIC ### Step 2.1: Insert New Customer in PostgreSQL
# MAGIC
# MAGIC **In your PostgreSQL client, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert new customer
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, created_date)
# MAGIC VALUES (
# MAGIC #   'Emily',
# MAGIC #   'Chen',
# MAGIC #   'emily.chen@email.com',
# MAGIC #   '555-0201',
# MAGIC #   '789 Market St',
# MAGIC #   'San Francisco',
# MAGIC #   'CA',
# MAGIC #   '94103',
# MAGIC #   CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Verify insertion and note the customer_id
# MAGIC SELECT customer_id, first_name, last_name, email, created_date
# MAGIC FROM customers
# MAGIC WHERE email = 'emily.chen@email.com';
# MAGIC ```
# MAGIC
# MAGIC **Record the customer_id for later use**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Insert New Product in PostgreSQL
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert new product
# MAGIC INSERT INTO products (product_name, category, price, stock_quantity, supplier, description, created_date)
# MAGIC VALUES (
# MAGIC #   'Mechanical Keyboard',
# MAGIC #   'Electronics',
# MAGIC #   89.99,
# MAGIC #   150,
# MAGIC #   'TechSupply Inc',
# MAGIC #   'RGB backlit mechanical gaming keyboard',
# MAGIC #   CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Verify insertion
# MAGIC SELECT product_id, product_name, category, price, created_date
# MAGIC FROM products
# MAGIC WHERE product_name = 'Mechanical Keyboard';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Insert New Order in PostgreSQL
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert new order (use an existing customer_id, e.g., 1)
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method, created_date)
# MAGIC VALUES (
# MAGIC #   1,
# MAGIC #   'PENDING',
# MAGIC #   119.98,
# MAGIC #   '123 Main St, Seattle, WA 98101',
# MAGIC #   'Credit Card',
# MAGIC #   CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Verify insertion
# MAGIC SELECT order_id, customer_id, order_status, total_amount, created_date
# MAGIC FROM orders
# MAGIC ORDER BY created_date DESC
# MAGIC LIMIT 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.4: Wait for Pipeline Execution
# MAGIC
# MAGIC ⏱️ **Wait for next scheduled pipeline run** (up to 15 minutes if schedule is */15)
# MAGIC
# MAGIC **Monitor pipeline execution:**
# MAGIC 1. Navigate to **Workflows** → **Delta Live Tables** → `retail_cdc_pipeline`
# MAGIC 2. Watch for next execution to complete
# MAGIC 3. Check execution logs for "Records processed"

# COMMAND ----------

import time

print("Waiting for pipeline to process changes...")
print("⏱️ Pipeline schedule: Every 15 minutes")
print("💡 Tip: You can manually trigger the pipeline to speed up testing")
print("")
print("To manually trigger:")
print("1. Go to pipeline UI")
print("2. Click 'Run Now' button")
print("")
print("Once pipeline completes, continue to next cell...")

# Optional: Add a sleep timer if you want to automate waiting
# time.sleep(60)  # Wait 1 minute before checking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.5: Verify INSERT in Customers Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for new customer record
# MAGIC SELECT 
# MAGIC #   customer_id,
# MAGIC #   first_name,
# MAGIC #   last_name,
# MAGIC #   email,
# MAGIC #   city,
# MAGIC #   state,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'emily.chen@email.com';

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected Result:**
# MAGIC - 1 record returned
# MAGIC - `first_name`: Emily
# MAGIC - `last_name`: Chen
# MAGIC - `_change_type`: INSERT
# MAGIC - `_commit_timestamp`: Recent (within last 15 minutes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.6: Verify INSERT in Products Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for new product record
# MAGIC SELECT 
# MAGIC #   product_id,
# MAGIC #   product_name,
# MAGIC #   category,
# MAGIC #   price,
# MAGIC #   stock_quantity,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE product_name = 'Mechanical Keyboard';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.7: Verify INSERT in Orders Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for new order record
# MAGIC SELECT 
# MAGIC #   order_id,
# MAGIC #   customer_id,
# MAGIC #   order_status,
# MAGIC #   total_amount,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC ORDER BY _commit_timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.8: Verify Total Counts Increased

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare current counts to baseline
# MAGIC WITH current_counts AS (
# MAGIC #   SELECT 'customers' as table_name, COUNT(*) as current_count 
# MAGIC #   FROM retail_analytics.bronze.customers
# MAGIC #   UNION ALL
# MAGIC #   SELECT 'orders', COUNT(*) FROM retail_analytics.bronze.orders
# MAGIC #   UNION ALL
# MAGIC #   SELECT 'products', COUNT(*) FROM retail_analytics.bronze.products
# MAGIC # )
# MAGIC SELECT 
# MAGIC #   b.table_name,
# MAGIC #   b.count as baseline_count,
# MAGIC #   c.current_count,
# MAGIC #   (c.current_count - b.count) as new_records
# MAGIC FROM baseline_state b
# MAGIC JOIN current_counts c ON b.table_name = c.table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected Result:**
# MAGIC - customers: +1 new record
# MAGIC - orders: +1 new record
# MAGIC - products: +1 new record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Test UPDATE Operations
# MAGIC
# MAGIC ### Step 3.1: Update Customer Address in PostgreSQL
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Update customer address
# MAGIC UPDATE customers
# MAGIC SET 
# MAGIC #   address = '456 New Address Blvd',
# MAGIC #   city = 'Oakland',
# MAGIC #   state = 'CA',
# MAGIC #   zip_code = '94607',
# MAGIC #   last_updated = CURRENT_TIMESTAMP
# MAGIC WHERE email = 'john.doe@email.com';
# MAGIC
# MAGIC -- Verify update
# MAGIC SELECT customer_id, first_name, last_name, address, city, state, last_updated
# MAGIC FROM customers
# MAGIC WHERE email = 'john.doe@email.com';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Update Order Status in PostgreSQL
# MAGIC
# MAGIC ```sql
# MAGIC -- Find a pending order
# MAGIC SELECT order_id, customer_id, order_status, total_amount
# MAGIC FROM orders
# MAGIC WHERE order_status = 'PENDING'
# MAGIC LIMIT 1;
# MAGIC
# MAGIC -- Update order status (replace order_id with actual value)
# MAGIC UPDATE orders
# MAGIC SET 
# MAGIC #   order_status = 'SHIPPED',
# MAGIC #   last_updated = CURRENT_TIMESTAMP
# MAGIC WHERE order_id = <your_order_id>;
# MAGIC
# MAGIC -- Verify update
# MAGIC SELECT order_id, order_status, last_updated
# MAGIC FROM orders
# MAGIC WHERE order_id = <your_order_id>;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.3: Update Product Price in PostgreSQL
# MAGIC
# MAGIC ```sql
# MAGIC -- Update product price (discount!)
# MAGIC UPDATE products
# MAGIC SET 
# MAGIC #   price = 79.99,
# MAGIC #   last_updated = CURRENT_TIMESTAMP
# MAGIC WHERE product_name = 'Mechanical Keyboard';
# MAGIC
# MAGIC -- Verify update
# MAGIC SELECT product_id, product_name, price, last_updated
# MAGIC FROM products
# MAGIC WHERE product_name = 'Mechanical Keyboard';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.4: Wait for Pipeline and Verify Customer Update

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Wait for pipeline, then check updated customer
# MAGIC SELECT 
# MAGIC #   customer_id,
# MAGIC #   first_name,
# MAGIC #   last_name,
# MAGIC #   email,
# MAGIC #   address,
# MAGIC #   city,
# MAGIC #   state,
# MAGIC #   zip_code,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'john.doe@email.com'
# MAGIC ORDER BY _commit_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected Result:**
# MAGIC - `address`: 456 New Address Blvd
# MAGIC - `city`: Oakland
# MAGIC - `state`: CA
# MAGIC - `_change_type`: UPDATE
# MAGIC - `_commit_timestamp`: Recent

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.5: Verify Order Status Update

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for order status update
# MAGIC SELECT 
# MAGIC #   order_id,
# MAGIC #   customer_id,
# MAGIC #   order_status,
# MAGIC #   total_amount,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE order_status = 'SHIPPED'
# MAGIC ORDER BY _commit_timestamp DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.6: Verify Product Price Update

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for product price update
# MAGIC SELECT 
# MAGIC #   product_id,
# MAGIC #   product_name,
# MAGIC #   price,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE product_name = 'Mechanical Keyboard';

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected:** price = 79.99, _change_type = UPDATE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.7: View Change History with Time Travel
# MAGIC
# MAGIC Delta Lake time travel allows you to see previous versions:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View history of changes to customers table
# MAGIC DESCRIBE HISTORY retail_analytics.bronze.customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query previous version (before UPDATE)
# MAGIC -- Replace VERSION AS OF with actual version number from history
# MAGIC SELECT customer_id, first_name, last_name, address, city
# MAGIC FROM retail_analytics.bronze.customers VERSION AS OF 0
# MAGIC WHERE email = 'john.doe@email.com';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare with current version (after UPDATE)
# MAGIC SELECT customer_id, first_name, last_name, address, city
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'john.doe@email.com';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Test DELETE Operations
# MAGIC
# MAGIC ### Step 4.1: Create Test Record to Delete
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert test customer for deletion
# MAGIC INSERT INTO customers (first_name, last_name, email, city, state, created_date)
# MAGIC VALUES (
# MAGIC #   'Test',
# MAGIC #   'DeleteMe',
# MAGIC #   'test.delete@email.com',
# MAGIC #   'TestCity',
# MAGIC #   'CA',
# MAGIC #   CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Get the customer_id
# MAGIC SELECT customer_id, first_name, last_name, email
# MAGIC FROM customers
# MAGIC WHERE email = 'test.delete@email.com';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Wait for INSERT to Propagate
# MAGIC
# MAGIC Wait for pipeline to run, then verify:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify test record exists in bronze
# MAGIC SELECT 
# MAGIC #   customer_id,
# MAGIC #   first_name,
# MAGIC #   last_name,
# MAGIC #   email,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'test.delete@email.com';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.3: Delete Record in PostgreSQL
# MAGIC
# MAGIC ```sql
# MAGIC -- Delete the test customer
# MAGIC DELETE FROM customers
# MAGIC WHERE email = 'test.delete@email.com';
# MAGIC
# MAGIC -- Verify deletion in PostgreSQL
# MAGIC SELECT COUNT(*) FROM customers WHERE email = 'test.delete@email.com';
# MAGIC -- Should return 0
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.4: Wait for Pipeline and Verify DELETE Handling
# MAGIC
# MAGIC **Note:** Delete handling depends on pipeline configuration:
# MAGIC - **Soft delete**: Record marked as deleted (_is_deleted = TRUE)
# MAGIC - **Hard delete**: Record physically removed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for deleted record (soft delete)
# MAGIC SELECT 
# MAGIC #   customer_id,
# MAGIC #   first_name,
# MAGIC #   last_name,
# MAGIC #   email,
# MAGIC #   _change_type,
# MAGIC #   _is_deleted,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'test.delete@email.com';

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected Result (Soft Delete):**
# MAGIC - Record still present
# MAGIC - `_is_deleted`: TRUE
# MAGIC - `_change_type`: DELETE
# MAGIC
# MAGIC **✓ Expected Result (Hard Delete):**
# MAGIC - No records returned (physically deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.5: Query Active Records Only
# MAGIC
# MAGIC For soft deletes, filter out deleted records:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query only active (non-deleted) customers
# MAGIC SELECT 
# MAGIC #   customer_id,
# MAGIC #   first_name,
# MAGIC #   last_name,
# MAGIC #   email,
# MAGIC #   city,
# MAGIC #   state
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _is_deleted IS NULL OR _is_deleted = FALSE
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Measure End-to-End Latency
# MAGIC
# MAGIC ### Step 5.1: Perform Timed INSERT Test

# COMMAND ----------

from datetime import datetime

# Record PostgreSQL insert time
postgres_insert_time = datetime.now()
print(f"PostgreSQL INSERT timestamp: {postgres_insert_time}")
print("")
print("Now go to PostgreSQL and run:")
print("""
INSERT INTO products (product_name, category, price, stock_quantity, created_date)
VALUES ('Latency Test Product', 'Test', 9.99, 1, CURRENT_TIMESTAMP);
""")
print("")
print("After running the INSERT, wait for pipeline and check next cell")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.2: Check When Change Appears in Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for latency test product
# MAGIC SELECT 
# MAGIC #   product_name,
# MAGIC #   price,
# MAGIC #   _commit_timestamp,
# MAGIC #   CURRENT_TIMESTAMP() as check_time,
# MAGIC #   TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP()) as seconds_old
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE product_name = 'Latency Test Product';

# COMMAND ----------

# Calculate approximate latency
databricks_commit_time_str = "2024-01-15 10:35:00"  # Replace with actual _commit_timestamp
print(f"Approximate end-to-end latency:")
print(f"- PostgreSQL INSERT: {postgres_insert_time}")
print(f"- Databricks commit: {databricks_commit_time_str}")
print(f"- Latency: Check seconds_old from query above")
print(f"")
print(f"💡 Latency factors:")
print(f"  - Pipeline schedule (15 min in our case)")
print(f"  - WAL consumption lag (typically < 30 sec)")
print(f"  - Pipeline execution time (1-3 min)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Analyze CDC Metadata
# MAGIC
# MAGIC ### Step 6.1: Change Type Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze change types across all tables
# MAGIC SELECT 
# MAGIC #   'customers' as table_name,
# MAGIC #   _change_type,
# MAGIC #   COUNT(*) as operation_count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC GROUP BY _change_type
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'orders', _change_type, COUNT(*)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC GROUP BY _change_type
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'products', _change_type, COUNT(*)
# MAGIC FROM retail_analytics.bronze.products
# MAGIC GROUP BY _change_type
# MAGIC
# MAGIC ORDER BY table_name, _change_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.2: Recent Changes Timeline

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent changes across all tables
# MAGIC SELECT 
# MAGIC #   'customers' as table_name,
# MAGIC #   customer_id as record_id,
# MAGIC #   CONCAT(first_name, ' ', last_name) as record_description,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'orders',
# MAGIC #   order_id,
# MAGIC #   CONCAT('Order ', CAST(order_id AS STRING), ' - ', order_status),
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'products',
# MAGIC #   product_id,
# MAGIC #   product_name,
# MAGIC #   _change_type,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC
# MAGIC ORDER BY _commit_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.3: Data Quality Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for any quality issues
# MAGIC SELECT 
# MAGIC #   'customers' as table_name,
# MAGIC #   COUNT(*) as total_records,
# MAGIC #   SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END) as deleted_count,
# MAGIC #   SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as missing_email,
# MAGIC #   MIN(_commit_timestamp) as oldest_change,
# MAGIC #   MAX(_commit_timestamp) as newest_change
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'orders',
# MAGIC #   COUNT(*),
# MAGIC #   SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END),
# MAGIC #   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
# MAGIC #   MIN(_commit_timestamp),
# MAGIC #   MAX(_commit_timestamp)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'products',
# MAGIC #   COUNT(*),
# MAGIC #   SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END),
# MAGIC #   SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END),
# MAGIC #   MIN(_commit_timestamp),
# MAGIC #   MAX(_commit_timestamp)
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Completion Checklist
# MAGIC
# MAGIC Verify you've successfully tested all CDC operations:
# MAGIC
# MAGIC ### INSERT Operations
# MAGIC - [ ] Inserted new customer in PostgreSQL
# MAGIC - [ ] Verified customer appeared in bronze.customers
# MAGIC - [ ] Inserted new product in PostgreSQL
# MAGIC - [ ] Verified product appeared in bronze.products
# MAGIC - [ ] Inserted new order in PostgreSQL
# MAGIC - [ ] Verified order appeared in bronze.orders
# MAGIC - [ ] Confirmed _change_type = INSERT for all
# MAGIC
# MAGIC ### UPDATE Operations
# MAGIC - [ ] Updated customer address in PostgreSQL
# MAGIC - [ ] Verified address change in bronze.customers
# MAGIC - [ ] Updated order status in PostgreSQL
# MAGIC - [ ] Verified status change in bronze.orders
# MAGIC - [ ] Updated product price in PostgreSQL
# MAGIC - [ ] Verified price change in bronze.products
# MAGIC - [ ] Confirmed _change_type = UPDATE for all
# MAGIC - [ ] Used Delta time travel to view previous versions
# MAGIC
# MAGIC ### DELETE Operations
# MAGIC - [ ] Created test record for deletion
# MAGIC - [ ] Verified test record in bronze table
# MAGIC - [ ] Deleted record in PostgreSQL
# MAGIC - [ ] Verified delete handling (soft or hard)
# MAGIC - [ ] Queried active records only (excluding deleted)
# MAGIC
# MAGIC ### Performance and Metadata
# MAGIC - [ ] Measured end-to-end latency
# MAGIC - [ ] Analyzed change type distribution
# MAGIC - [ ] Reviewed recent changes timeline
# MAGIC - [ ] Checked data quality metrics
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC **Excellent work!** You've successfully:
# MAGIC - Tested all CRUD operations (CREATE, READ, UPDATE, DELETE)
# MAGIC - Validated bidirectional data flow from PostgreSQL to Databricks
# MAGIC - Measured CDC latency and performance
# MAGIC - Analyzed CDC metadata and change tracking
# MAGIC - Verified data quality and consistency
# MAGIC
# MAGIC **Key Findings:**
# MAGIC - ✅ INSERT operations captured correctly
# MAGIC - ✅ UPDATE operations tracked with change metadata
# MAGIC - ✅ DELETE operations handled (soft or hard delete)
# MAGIC - ✅ End-to-end latency: ~15 minutes (based on schedule)
# MAGIC - ✅ Data consistency maintained across all tables
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 6: Multi-Table CDC and Dependency Management** where you'll:
# MAGIC - Handle foreign key relationships in CDC
# MAGIC - Manage table dependencies and load order
# MAGIC - Implement referential integrity checks
# MAGIC - Handle cascading updates and deletes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Your CDC pipeline is validated and working!** ✅
