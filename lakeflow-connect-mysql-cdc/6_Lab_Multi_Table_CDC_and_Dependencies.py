# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6: Multi-Table CDC and Dependency Management
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will learn how to handle complex CDC scenarios involving multiple tables with foreign key relationships. You'll configure pipeline dependencies, test referential integrity scenarios, and manage edge cases like orphaned records and cascading deletes.
# MAGIC
# MAGIC **Estimated Time**: 30-40 minutes
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Understand how foreign key relationships affect CDC ingestion order
# MAGIC - Configure pipeline dependencies to ensure parent tables load before child tables
# MAGIC - Test referential integrity scenarios (orphaned records, cascading updates)
# MAGIC - Handle data quality issues in multi-table CDC
# MAGIC - Implement strategies for managing FK constraint violations
# MAGIC - Query across related tables to validate data consistency
# MAGIC - Design CDC pipelines for complex database schemas
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2-5 (MySQL configuration through incremental CDC)
# MAGIC - Understanding of foreign key relationships and referential integrity
# MAGIC - Bronze tables (customers, orders, products) populated with data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Review Current Table Relationships
# MAGIC
# MAGIC Let's examine the foreign key relationships in our retail schema.
# MAGIC
# MAGIC ### Schema Relationships:
# MAGIC ```
# MAGIC customers (Parent)
# MAGIC     │
# MAGIC     │ customer_id (PK)
# MAGIC     │
# MAGIC     └──► orders (Child)
# MAGIC             │ customer_id (FK)
# MAGIC             │ order_id (PK)
# MAGIC
# MAGIC products (Independent)
# MAGIC     │ product_id (PK)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify current record counts
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
# MAGIC -- Check referential integrity: all orders should have valid customer references
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT o.customer_id) as orders_distinct_customers,
# MAGIC     COUNT(DISTINCT c.customer_id) as total_customers_with_orders
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify any orphaned orders (orders without matching customers)
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     o.customer_id as orphaned_customer_id,
# MAGIC     o.order_status,
# MAGIC     o.total_amount
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;
# MAGIC
# MAGIC -- Expected: 0 rows (no orphaned orders initially)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Test Scenario 1 - Correct Order (Parent then Child)
# MAGIC
# MAGIC Insert a new customer followed by their orders (correct dependency order).
# MAGIC
# MAGIC ### Step 2.1: Insert Parent Record First (Customer)
# MAGIC
# MAGIC Run in MySQL:
# MAGIC
# MAGIC ```sql
# MAGIC USE retail_db;
# MAGIC
# MAGIC -- Insert new customer (parent)
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, city, state)
# MAGIC VALUES ('Maria', 'Garcia', 'maria.garcia@email.com', '555-2001', 'Denver', 'CO');
# MAGIC
# MAGIC -- Get the customer_id
# MAGIC SELECT customer_id, first_name, last_name, email 
# MAGIC FROM customers 
# MAGIC WHERE email = 'maria.garcia@email.com';
# MAGIC -- Note the customer_id (e.g., 15)
# MAGIC ```
# MAGIC
# MAGIC ### Step 2.2: Insert Child Records (Orders)
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert orders for the new customer (child records)
# MAGIC -- Replace 15 with your actual customer_id
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
# MAGIC VALUES
# MAGIC     (15, 'PENDING', 299.99, '123 Denver St, Denver, CO 80201', 'Credit Card'),
# MAGIC     (15, 'PROCESSING', 149.99, '123 Denver St, Denver, CO 80201', 'PayPal');
# MAGIC
# MAGIC -- Verify inserts with join
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     o.order_status,
# MAGIC     o.total_amount
# MAGIC FROM orders o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.email = 'maria.garcia@email.com';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Run Pipeline and Validate

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ **Run the ingestion pipeline** from the UI and wait for completion.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check pipeline status
# MAGIC SELECT 
# MAGIC     update_id,
# MAGIC     timestamp,
# MAGIC     state,
# MAGIC     message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate: customer and orders should both appear
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.email,
# MAGIC     COUNT(o.order_id) as order_count,
# MAGIC     SUM(o.total_amount) as total_order_value
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'maria.garcia@email.com'
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name, c.email;
# MAGIC
# MAGIC -- Expected: 1 customer with 2 orders totaling $449.98

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify no orphaned records
# MAGIC SELECT COUNT(*) as orphaned_order_count
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;
# MAGIC
# MAGIC -- Expected: 0

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Result**: When parent (customer) is inserted before child (orders), referential integrity is maintained automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Test Scenario 2 - Simultaneous Inserts
# MAGIC
# MAGIC What happens when parent and child records are inserted in the same MySQL transaction?
# MAGIC
# MAGIC ### Step 3.1: Insert Parent and Child in Single Transaction
# MAGIC
# MAGIC Run in MySQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- Begin transaction
# MAGIC START TRANSACTION;
# MAGIC
# MAGIC -- Insert customer
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, city, state)
# MAGIC VALUES ('James', 'Wilson', 'james.wilson@email.com', '555-3001', 'Portland', 'OR');
# MAGIC
# MAGIC -- Get the customer_id (use LAST_INSERT_ID())
# MAGIC SET @new_customer_id = LAST_INSERT_ID();
# MAGIC
# MAGIC -- Insert orders immediately in same transaction
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, payment_method)
# MAGIC VALUES
# MAGIC     (@new_customer_id, 'PENDING', 599.99, 'Credit Card'),
# MAGIC     (@new_customer_id, 'PENDING', 299.99, 'PayPal'),
# MAGIC     (@new_customer_id, 'PROCESSING', 149.99, 'Credit Card');
# MAGIC
# MAGIC -- Commit transaction (both customer and orders committed together)
# MAGIC COMMIT;
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     COUNT(o.order_id) as order_count
# MAGIC FROM customers c
# MAGIC LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'james.wilson@email.com'
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Run Pipeline and Validate

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ **Run the ingestion pipeline** and wait for completion.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate both customer and orders appear
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     COUNT(o.order_id) as order_count,
# MAGIC     SUM(o.total_amount) as total_value
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'james.wilson@email.com'
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name;
# MAGIC
# MAGIC -- Expected: 1 customer with 3 orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if there are any referential integrity issues
# MAGIC SELECT COUNT(*) as orphaned_count
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;
# MAGIC
# MAGIC -- Expected: 0 (Lakeflow Connect handles transactional consistency)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Result**: MySQL transactions ensure both parent and child are committed together. Binary log captures them in the same GTID, maintaining consistency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Test Scenario 3 - Cascading Updates
# MAGIC
# MAGIC Test how updates to parent records affect related child records.
# MAGIC
# MAGIC ### Step 4.1: Update Customer Information
# MAGIC
# MAGIC Run in MySQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- Update customer information
# MAGIC UPDATE customers
# MAGIC SET city = 'Beaverton',
# MAGIC     state = 'OR',
# MAGIC     zip_code = '97005'
# MAGIC WHERE email = 'james.wilson@email.com';
# MAGIC
# MAGIC -- Verify update
# MAGIC SELECT customer_id, first_name, last_name, city, state, zip_code
# MAGIC FROM customers
# MAGIC WHERE email = 'james.wilson@email.com';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Run Pipeline and Validate

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ **Run the ingestion pipeline** and wait for completion.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify customer update propagated
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zip_code,
# MAGIC     _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'james.wilson@email.com';
# MAGIC
# MAGIC -- Expected: city = 'Beaverton', state = 'OR'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check that orders still reference the correct customer
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city,
# MAGIC     c.state,
# MAGIC     o.order_status,
# MAGIC     o.total_amount
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.email = 'james.wilson@email.com';
# MAGIC
# MAGIC -- Expected: All 3 orders still properly joined with updated customer info

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Result**: Updates to parent records don't break FK relationships. Child records maintain their references.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Test Scenario 4 - Handling Deletes with FK Constraints
# MAGIC
# MAGIC Test deletion scenarios respecting foreign key constraints.
# MAGIC
# MAGIC ### Step 5.1: Create Test Data for Deletion
# MAGIC
# MAGIC Run in MySQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert test customer
# MAGIC INSERT INTO customers (first_name, last_name, email, city, state)
# MAGIC VALUES ('Delete', 'TestUser', 'delete.test@email.com', 'Test City', 'CA');
# MAGIC
# MAGIC -- Get customer_id
# MAGIC SET @delete_customer_id = LAST_INSERT_ID();
# MAGIC
# MAGIC -- Insert test orders
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, payment_method)
# MAGIC VALUES
# MAGIC     (@delete_customer_id, 'CANCELLED', 50.00, 'Test'),
# MAGIC     (@delete_customer_id, 'CANCELLED', 75.00, 'Test');
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT c.customer_id, c.first_name, COUNT(o.order_id) as order_count
# MAGIC FROM customers c
# MAGIC LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'delete.test@email.com'
# MAGIC GROUP BY c.customer_id, c.first_name;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ **Run pipeline** to ingest test data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify test data exists in bronze
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     COUNT(o.order_id) as order_count
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'delete.test@email.com'
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name;
# MAGIC
# MAGIC -- Expected: 1 customer with 2 orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.2: Delete in Correct Order (Child then Parent)
# MAGIC
# MAGIC Run in MySQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- First, delete child records (orders)
# MAGIC DELETE FROM orders
# MAGIC WHERE customer_id = (
# MAGIC     SELECT customer_id FROM customers WHERE email = 'delete.test@email.com'
# MAGIC );
# MAGIC
# MAGIC -- Then, delete parent record (customer)
# MAGIC DELETE FROM customers
# MAGIC WHERE email = 'delete.test@email.com';
# MAGIC
# MAGIC -- Verify both deleted
# MAGIC SELECT COUNT(*) as count_should_be_zero
# MAGIC FROM customers
# MAGIC WHERE email = 'delete.test@email.com';
# MAGIC
# MAGIC SELECT COUNT(*) as orders_count_should_be_zero
# MAGIC FROM orders
# MAGIC WHERE customer_id NOT IN (SELECT customer_id FROM customers);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.3: Run Pipeline and Validate Deletes

# COMMAND ----------

# MAGIC %md
# MAGIC ⏳ **Run pipeline** to capture delete operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify customer deleted from bronze
# MAGIC SELECT COUNT(*) as count_should_be_zero
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'delete.test@email.com';
# MAGIC
# MAGIC -- Expected: 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify orders also deleted
# MAGIC SELECT COUNT(*) as orphaned_orders
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;
# MAGIC
# MAGIC -- Expected: 0 (all orders should have valid customer references)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Result**: DELETE operations captured correctly when performed in proper order (child before parent).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Data Quality Monitoring for Multi-Table CDC
# MAGIC
# MAGIC Implement queries to monitor referential integrity and data quality.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create data quality monitoring view
# MAGIC CREATE OR REPLACE VIEW retail_analytics.bronze.vw_data_quality_checks AS
# MAGIC
# MAGIC -- Check 1: Orphaned orders (orders without customers)
# MAGIC SELECT 
# MAGIC     'Orphaned Orders' as check_name,
# MAGIC     COUNT(*) as issue_count,
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END as status
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 2: Null customer IDs in orders
# MAGIC SELECT 
# MAGIC     'Null Customer IDs in Orders',
# MAGIC     COUNT(*),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE customer_id IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 3: Customers without any orders (informational, not necessarily an error)
# MAGIC SELECT 
# MAGIC     'Customers Without Orders',
# MAGIC     COUNT(*),
# MAGIC     'INFO' as status
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE o.order_id IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 4: Duplicate customer emails
# MAGIC SELECT 
# MAGIC     'Duplicate Customer Emails',
# MAGIC     COUNT(*),
# MAGIC     CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END
# MAGIC FROM (
# MAGIC     SELECT email, COUNT(*) as count
# MAGIC     FROM retail_analytics.bronze.customers
# MAGIC     GROUP BY email
# MAGIC     HAVING COUNT(*) > 1
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run data quality checks
# MAGIC SELECT * FROM retail_analytics.bronze.vw_data_quality_checks
# MAGIC ORDER BY 
# MAGIC     CASE status 
# MAGIC         WHEN 'FAIL' THEN 1 
# MAGIC         WHEN 'WARN' THEN 2 
# MAGIC         WHEN 'INFO' THEN 3 
# MAGIC         ELSE 4 
# MAGIC     END,
# MAGIC     check_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7: Advanced Multi-Table Query Patterns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer order summary with CDC metadata
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.city,
# MAGIC     c.state,
# MAGIC     COUNT(o.order_id) as total_orders,
# MAGIC     SUM(o.total_amount) as lifetime_value,
# MAGIC     AVG(o.total_amount) as avg_order_value,
# MAGIC     MAX(o.order_date) as last_order_date,
# MAGIC     MAX(c._commit_timestamp) as customer_last_updated,
# MAGIC     MAX(o._commit_timestamp) as orders_last_updated
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.state
# MAGIC ORDER BY lifetime_value DESC NULLS LAST
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify customers with recent changes and their orders
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c._commit_timestamp as customer_updated,
# MAGIC     o.order_id,
# MAGIC     o.order_status,
# MAGIC     o.total_amount,
# MAGIC     o._commit_timestamp as order_updated
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c._commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC    OR o._commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC ORDER BY GREATEST(c._commit_timestamp, COALESCE(o._commit_timestamp, c._commit_timestamp)) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track referential integrity over time
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('HOUR', _commit_timestamp) as hour,
# MAGIC     COUNT(*) as order_changes
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 6 HOURS
# MAGIC GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC ORDER BY hour DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ **Foreign Key Relationship Testing**:
# MAGIC - Tested correct order (parent then child) insertions
# MAGIC - Validated transactional consistency with simultaneous inserts
# MAGIC - Confirmed cascading updates don't break relationships
# MAGIC - Verified delete operations maintain referential integrity
# MAGIC
# MAGIC ✅ **Data Quality Monitoring**:
# MAGIC - Created data quality check views
# MAGIC - Implemented orphaned record detection
# MAGIC - Built null value and duplicate checks
# MAGIC - Established monitoring patterns for production
# MAGIC
# MAGIC ✅ **Multi-Table Queries**:
# MAGIC - Joined customers and orders with CDC metadata
# MAGIC - Analyzed customer lifetime value with change tracking
# MAGIC - Identified recent changes across related tables
# MAGIC - Built referential integrity monitoring queries
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **MySQL transactions preserve consistency** - parent and child records committed together stay consistent through CDC
# MAGIC
# MAGIC 2. **Binary log order matters** - Lakeflow Connect processes changes in the order they were committed
# MAGIC
# MAGIC 3. **FK constraints must be respected in deletes** - always delete child records before parent records
# MAGIC
# MAGIC 4. **GTID ensures transactional integrity** - all changes in a MySQL transaction get the same GTID
# MAGIC
# MAGIC 5. **Data quality monitoring is essential** - regularly check for orphaned records and constraint violations
# MAGIC
# MAGIC 6. **CDC metadata enables change tracking** - _commit_timestamp helps correlate changes across related tables
# MAGIC
# MAGIC ### Best Practices for Multi-Table CDC
# MAGIC
# MAGIC | Scenario | Best Practice | Why |
# MAGIC |----------|---------------|-----|
# MAGIC | Insert parent+child | Use MySQL transactions | Ensures atomic commit |
# MAGIC | Delete parent+child | Delete child first, then parent | Respects FK constraints |
# MAGIC | Update parent | No special handling needed | FK references remain valid |
# MAGIC | Data quality | Monitor for orphaned records | Catch integrity issues early |
# MAGIC | Pipeline order | Configure dependencies in Lakeflow | Ensures correct processing order |
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lecture 7: Production Best Practices and Monitoring Strategies** to learn:
# MAGIC - Binary log retention and management
# MAGIC - Comprehensive monitoring and alerting
# MAGIC - Performance optimization techniques
# MAGIC - Cost optimization strategies
# MAGIC - Disaster recovery planning
# MAGIC
# MAGIC **Before moving on, ensure**:
# MAGIC - [ ] All multi-table scenarios tested successfully
# MAGIC - [ ] No orphaned records detected in data quality checks
# MAGIC - [ ] Referential integrity maintained across all operations
# MAGIC - [ ] Data quality monitoring view created and validated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [MySQL Foreign Key Constraints](https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html)
# MAGIC - [MySQL Transactions and GTID](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html)
# MAGIC - [Delta Lake MERGE Operations](https://docs.databricks.com/delta/merge.html)
# MAGIC - [Data Quality Monitoring Best Practices](https://docs.databricks.com/lakehouse/data-quality.html)
