# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6: Multi-Table CDC and Dependency Management
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this lab, you will learn how to handle foreign key relationships and table dependencies in CDC pipelines, ensuring referential integrity is maintained.
# MAGIC
# MAGIC **Duration:** 20 minutes
# MAGIC
# MAGIC **Objectives:**
# MAGIC - Understand foreign key relationships in CDC context
# MAGIC - Handle parent-child table dependencies
# MAGIC - Test cascading updates and deletes
# MAGIC - Implement referential integrity checks
# MAGIC - Design silver layer transformations with joins
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Completed Lab 5: CDC validation
# MAGIC - Understanding of database foreign keys
# MAGIC - Basic SQL join knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Review Table Relationships
# MAGIC
# MAGIC ### Current Schema Relationships
# MAGIC
# MAGIC ```
# MAGIC customers (parent)
# MAGIC     ↓ (1:many)
# MAGIC orders (child)
# MAGIC     - foreign key: customer_id → customers.customer_id
# MAGIC ```
# MAGIC
# MAGIC ### Step 1.1: Verify Foreign Keys in PostgreSQL
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- View foreign key constraints
# MAGIC SELECT
# MAGIC     tc.table_name,
# MAGIC     kcu.column_name,
# MAGIC     ccu.table_name AS foreign_table_name,
# MAGIC     ccu.column_name AS foreign_column_name
# MAGIC FROM information_schema.table_constraints AS tc
# MAGIC JOIN information_schema.key_column_usage AS kcu
# MAGIC   ON tc.constraint_name = kcu.constraint_name
# MAGIC JOIN information_schema.constraint_column_usage AS ccu
# MAGIC   ON ccu.constraint_name = tc.constraint_name
# MAGIC WHERE tc.constraint_type = 'FOREIGN KEY'
# MAGIC   AND tc.table_schema = 'public'
# MAGIC   AND tc.table_name IN ('orders', 'customers', 'products');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Visualize Current Relationships

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show customer-order relationships
# MAGIC SELECT 
# MAGIC #   c.customer_id,
# MAGIC #   c.first_name,
# MAGIC #   c.last_name,
# MAGIC #   COUNT(o.order_id) as order_count,
# MAGIC #   SUM(o.total_amount) as total_spent
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o 
# MAGIC #   ON c.customer_id = o.customer_id
# MAGIC WHERE (c._is_deleted IS NULL OR c._is_deleted = FALSE)
# MAGIC #   AND (o._is_deleted IS NULL OR o._is_deleted = FALSE)
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name
# MAGIC ORDER BY order_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Test Parent Record INSERT → Child Record INSERT
# MAGIC
# MAGIC ### Scenario: New customer places an order
# MAGIC
# MAGIC ### Step 2.1: Insert Parent Record (Customer)
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert new customer
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, city, state, created_date)
# MAGIC VALUES (
# MAGIC #   'Michael',
# MAGIC #   'Rodriguez',
# MAGIC #   'michael.rodriguez@email.com',
# MAGIC #   '555-0301',
# MAGIC #   'Austin',
# MAGIC #   'TX',
# MAGIC #   CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Get the new customer_id
# MAGIC SELECT customer_id, first_name, last_name, email
# MAGIC FROM customers
# MAGIC WHERE email = 'michael.rodriguez@email.com';
# MAGIC
# MAGIC -- Record the customer_id for next step
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Wait for Customer to Propagate
# MAGIC
# MAGIC ⏱️ Wait for pipeline to run, then verify:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify new customer in bronze
# MAGIC SELECT customer_id, first_name, last_name, email, _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'michael.rodriguez@email.com';

# COMMAND ----------

# Note the customer_id for the child record
customer_id_for_order = None  # Replace with actual customer_id from query above

print(f"Customer ID for next step: {customer_id_for_order}")
print("Replace 'customer_id_for_order' variable above with actual value")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Insert Child Record (Order)
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert order for the new customer (replace customer_id)
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method, created_date)
# MAGIC VALUES (
# MAGIC #   <customer_id_from_previous_step>,
# MAGIC #   'PENDING',
# MAGIC #   149.99,
# MAGIC #   '123 Austin St, Austin, TX 78701',
# MAGIC #   'Credit Card',
# MAGIC #   CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Verify order
# MAGIC SELECT order_id, customer_id, order_status, total_amount, created_date
# MAGIC FROM orders
# MAGIC WHERE customer_id = <customer_id_from_previous_step>;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.4: Verify Parent-Child Relationship

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After pipeline runs, verify joined data
# MAGIC SELECT 
# MAGIC #   c.customer_id,
# MAGIC #   c.first_name,
# MAGIC #   c.last_name,
# MAGIC #   o.order_id,
# MAGIC #   o.order_status,
# MAGIC #   o.total_amount,
# MAGIC #   o._commit_timestamp as order_commit_time
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'michael.rodriguez@email.com';

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected Result:**
# MAGIC - JOIN successful
# MAGIC - Customer and order data both present
# MAGIC - Foreign key relationship maintained

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Test Parent UPDATE → Child Consistency
# MAGIC
# MAGIC ### Scenario: Update customer information, verify orders still linked
# MAGIC
# MAGIC ### Step 3.1: Update Parent Record
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Update customer information
# MAGIC UPDATE customers
# MAGIC SET 
# MAGIC #   city = 'Dallas',
# MAGIC #   state = 'TX',
# MAGIC #   last_updated = CURRENT_TIMESTAMP
# MAGIC WHERE email = 'michael.rodriguez@email.com';
# MAGIC
# MAGIC -- Verify update
# MAGIC SELECT customer_id, first_name, last_name, city, state, last_updated
# MAGIC FROM customers
# MAGIC WHERE email = 'michael.rodriguez@email.com';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Verify Relationship Preserved After Update

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After pipeline runs, verify relationship still intact
# MAGIC SELECT 
# MAGIC #   c.customer_id,
# MAGIC #   c.first_name,
# MAGIC #   c.last_name,
# MAGIC #   c.city,
# MAGIC #   c.state,
# MAGIC #   COUNT(o.order_id) as order_count,
# MAGIC #   c._commit_timestamp as customer_last_change
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'michael.rodriguez@email.com'
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.state, c._commit_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected Result:**
# MAGIC - Customer city updated to 'Dallas'
# MAGIC - Order count still accurate
# MAGIC - No orphaned orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Handle Orphaned Child Records
# MAGIC
# MAGIC ### Scenario: What happens if parent deleted but child remains?
# MAGIC
# MAGIC ### Step 4.1: Identify Potential Orphans
# MAGIC
# MAGIC In real-world scenarios, orphaned records can occur due to:
# MAGIC - Deletion timing (child arrives before parent delete)
# MAGIC - Foreign key constraints not enforced in source
# MAGIC - Data quality issues

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find orders without valid customers (orphans)
# MAGIC SELECT 
# MAGIC #   o.order_id,
# MAGIC #   o.customer_id,
# MAGIC #   o.order_status,
# MAGIC #   o.total_amount,
# MAGIC #   c.customer_id as customer_exists
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c 
# MAGIC #   ON o.customer_id = c.customer_id 
# MAGIC #   AND (c._is_deleted IS NULL OR c._is_deleted = FALSE)
# MAGIC WHERE c.customer_id IS NULL
# MAGIC #   AND (o._is_deleted IS NULL OR o._is_deleted = FALSE);

# COMMAND ----------

# MAGIC %md
# MAGIC **✓ Expected Result:** 
# MAGIC - Should return 0 rows (no orphans)
# MAGIC - If orphans found, indicates referential integrity issue

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Create Referential Integrity Check

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create view for data quality monitoring
# MAGIC CREATE OR REPLACE VIEW retail_analytics.bronze.orphaned_orders_check AS
# MAGIC SELECT 
# MAGIC #   o.order_id,
# MAGIC #   o.customer_id,
# MAGIC #   o.order_status,
# MAGIC #   o.total_amount,
# MAGIC #   o._commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c 
# MAGIC #   ON o.customer_id = c.customer_id 
# MAGIC #   AND (c._is_deleted IS NULL OR c._is_deleted = FALSE)
# MAGIC WHERE c.customer_id IS NULL
# MAGIC #   AND (o._is_deleted IS NULL OR o._is_deleted = FALSE);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the check view
# MAGIC SELECT * FROM retail_analytics.bronze.orphaned_orders_check;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Test Parent DELETE with CASCADE Logic
# MAGIC
# MAGIC ### Step 5.1: Create Test Customer with Orders
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Create test customer
# MAGIC INSERT INTO customers (first_name, last_name, email, city, state, created_date)
# MAGIC VALUES ('Delete', 'TestUser', 'delete.test@email.com', 'TestCity', 'CA', CURRENT_TIMESTAMP);
# MAGIC
# MAGIC -- Get customer_id
# MAGIC SELECT customer_id FROM customers WHERE email = 'delete.test@email.com';
# MAGIC
# MAGIC -- Create orders for test customer (replace customer_id)
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, payment_method, created_date)
# MAGIC VALUES 
# MAGIC   (<test_customer_id>, 'PENDING', 50.00, 'Credit Card', CURRENT_TIMESTAMP),
# MAGIC   (<test_customer_id>, 'SHIPPED', 75.00, 'PayPal', CURRENT_TIMESTAMP);
# MAGIC
# MAGIC -- Verify setup
# MAGIC SELECT o.order_id, o.customer_id, o.order_status, c.first_name, c.last_name
# MAGIC FROM orders o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.email = 'delete.test@email.com';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.2: Wait and Verify Test Data in Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify test customer and orders in bronze
# MAGIC SELECT 
# MAGIC #   c.customer_id,
# MAGIC #   c.first_name,
# MAGIC #   c.last_name,
# MAGIC #   COUNT(o.order_id) as order_count
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.email = 'delete.test@email.com'
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.3: Delete Parent in PostgreSQL
# MAGIC
# MAGIC **In PostgreSQL, run:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Check if foreign key has CASCADE option
# MAGIC SELECT 
# MAGIC #   tc.constraint_name,
# MAGIC #   rc.delete_rule
# MAGIC FROM information_schema.table_constraints tc
# MAGIC JOIN information_schema.referential_constraints rc 
# MAGIC #   ON tc.constraint_name = rc.constraint_name
# MAGIC WHERE tc.table_name = 'orders' AND tc.constraint_type = 'FOREIGN KEY';
# MAGIC
# MAGIC -- If CASCADE is NOT set, you'll need to delete orders first:
# MAGIC DELETE FROM orders WHERE customer_id = <test_customer_id>;
# MAGIC
# MAGIC -- Then delete customer:
# MAGIC DELETE FROM customers WHERE email = 'delete.test@email.com';
# MAGIC
# MAGIC -- Verify deletion
# MAGIC SELECT COUNT(*) FROM customers WHERE email = 'delete.test@email.com';
# MAGIC SELECT COUNT(*) FROM orders WHERE customer_id = <test_customer_id>;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.4: Verify CASCADE in Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After pipeline runs, check if customer is marked deleted
# MAGIC SELECT 
# MAGIC #   customer_id,
# MAGIC #   first_name,
# MAGIC #   last_name,
# MAGIC #   email,
# MAGIC #   _is_deleted,
# MAGIC #   _commit_timestamp
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE email = 'delete.test@email.com';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if orders are also marked deleted
# MAGIC SELECT 
# MAGIC #   o.order_id,
# MAGIC #   o.customer_id,
# MAGIC #   o.order_status,
# MAGIC #   o._is_deleted,
# MAGIC #   o._commit_timestamp
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC WHERE o.customer_id IN (
# MAGIC #   SELECT customer_id 
# MAGIC #   FROM retail_analytics.bronze.customers 
# MAGIC #   WHERE email = 'delete.test@email.com'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** CDC captures deletions as they occur in PostgreSQL. If PostgreSQL has CASCADE delete, both parent and child deletes are captured separately.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Create Silver Layer with Joined Data
# MAGIC
# MAGIC ### Step 6.1: Create Customer Orders Silver View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create silver layer view with denormalized data
# MAGIC CREATE OR REPLACE VIEW retail_analytics.silver.customer_orders AS
# MAGIC SELECT 
# MAGIC #   -- Customer dimensions
# MAGIC #   c.customer_id,
# MAGIC #   c.first_name,
# MAGIC #   c.last_name,
# MAGIC #   c.email,
# MAGIC #   c.city,
# MAGIC #   c.state,
# MAGIC #   
# MAGIC #   -- Order facts
# MAGIC #   o.order_id,
# MAGIC #   o.order_date,
# MAGIC #   o.order_status,
# MAGIC #   o.total_amount,
# MAGIC #   o.payment_method,
# MAGIC #   
# MAGIC #   -- Metadata
# MAGIC #   c._commit_timestamp as customer_last_updated,
# MAGIC #   o._commit_timestamp as order_last_updated
# MAGIC #   
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC INNER JOIN retail_analytics.bronze.orders o 
# MAGIC #   ON c.customer_id = o.customer_id
# MAGIC WHERE (c._is_deleted IS NULL OR c._is_deleted = FALSE)
# MAGIC #   AND (o._is_deleted IS NULL OR o._is_deleted = FALSE);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.2: Query Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query denormalized customer-order data
# MAGIC SELECT * FROM retail_analytics.bronze.customer_orders
# MAGIC ORDER BY order_last_updated DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.3: Create Aggregated Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create aggregated customer metrics table
# MAGIC CREATE OR REPLACE TABLE retail_analytics.silver.customer_metrics AS
# MAGIC SELECT 
# MAGIC #   c.customer_id,
# MAGIC #   c.first_name,
# MAGIC #   c.last_name,
# MAGIC #   c.email,
# MAGIC #   c.city,
# MAGIC #   c.state,
# MAGIC #   
# MAGIC #   -- Order metrics
# MAGIC #   COUNT(o.order_id) as total_orders,
# MAGIC #   COALESCE(SUM(o.total_amount), 0) as lifetime_value,
# MAGIC #   COALESCE(AVG(o.total_amount), 0) as avg_order_value,
# MAGIC #   MAX(o.order_date) as last_order_date,
# MAGIC #   
# MAGIC #   -- Status breakdown
# MAGIC #   SUM(CASE WHEN o.order_status = 'PENDING' THEN 1 ELSE 0 END) as pending_orders,
# MAGIC #   SUM(CASE WHEN o.order_status = 'SHIPPED' THEN 1 ELSE 0 END) as shipped_orders,
# MAGIC #   SUM(CASE WHEN o.order_status = 'DELIVERED' THEN 1 ELSE 0 END) as delivered_orders,
# MAGIC #   
# MAGIC #   -- Metadata
# MAGIC #   CURRENT_TIMESTAMP() as metrics_calculated_at
# MAGIC #   
# MAGIC FROM retail_analytics.bronze.customers c
# MAGIC LEFT JOIN retail_analytics.bronze.orders o 
# MAGIC #   ON c.customer_id = o.customer_id
# MAGIC #   AND (o._is_deleted IS NULL OR o._is_deleted = FALSE)
# MAGIC WHERE (c._is_deleted IS NULL OR c._is_deleted = FALSE)
# MAGIC GROUP BY 
# MAGIC #   c.customer_id, c.first_name, c.last_name, 
# MAGIC #   c.email, c.city, c.state;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View customer metrics
# MAGIC SELECT * FROM retail_analytics.silver.customer_metrics
# MAGIC ORDER BY lifetime_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Dependency Order Best Practices
# MAGIC
# MAGIC ### Recommended CDC Pipeline Order
# MAGIC
# MAGIC For tables with foreign keys, ensure parent tables are processed before child tables:
# MAGIC
# MAGIC **Tier 1 (No dependencies):**
# MAGIC - customers
# MAGIC - products
# MAGIC
# MAGIC **Tier 2 (Depends on Tier 1):**
# MAGIC - orders (depends on customers)
# MAGIC
# MAGIC **Tier 3 (Depends on Tier 2):**
# MAGIC - order_items (if you had this table, depends on orders and products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implementation in Lakeflow Connect
# MAGIC
# MAGIC Lakeflow Connect handles dependencies automatically, but you can optimize by:
# MAGIC
# MAGIC 1. **Configure table priorities** (if supported)
# MAGIC 2. **Use multiple pipelines** with dependencies:
# MAGIC    - Pipeline 1: customers, products
# MAGIC    - Pipeline 2: orders (depends on Pipeline 1)
# MAGIC 3. **Schedule with offsets**:
# MAGIC    - Pipeline 1: :00 minutes
# MAGIC    - Pipeline 2: :05 minutes (5-minute offset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Completion Checklist
# MAGIC
# MAGIC Verify you've completed all multi-table scenarios:
# MAGIC
# MAGIC ### Parent-Child Relationships
# MAGIC - [ ] Verified foreign key relationships in schema
# MAGIC - [ ] Tested parent INSERT followed by child INSERT
# MAGIC - [ ] Verified relationship preserved after parent UPDATE
# MAGIC - [ ] Tested parent DELETE with CASCADE logic
# MAGIC
# MAGIC ### Data Quality
# MAGIC - [ ] Created orphaned records check
# MAGIC - [ ] Verified no referential integrity violations
# MAGIC - [ ] Implemented data quality monitoring view
# MAGIC
# MAGIC ### Silver Layer
# MAGIC - [ ] Created denormalized customer-orders view
# MAGIC - [ ] Built aggregated customer metrics table
# MAGIC - [ ] Queried joined data successfully
# MAGIC
# MAGIC ### Best Practices
# MAGIC - [ ] Documented table dependency tiers
# MAGIC - [ ] Understood processing order implications
# MAGIC - [ ] Planned for multi-pipeline architecture (if needed)
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC **Great job!** You've learned how to:
# MAGIC - Handle foreign key relationships in CDC pipelines
# MAGIC - Maintain referential integrity across parent-child tables
# MAGIC - Detect and prevent orphaned records
# MAGIC - Build silver layer transformations with joins
# MAGIC - Design multi-table CDC architectures
# MAGIC
# MAGIC **Key Insights:**
# MAGIC - CDC captures changes at table level, not relationship level
# MAGIC - Parent tables should be processed before child tables
# MAGIC - Soft deletes help maintain historical relationships
# MAGIC - Silver layer is ideal for denormalization and aggregation
# MAGIC - Data quality checks are essential for multi-table CDC
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lecture 7: Production Best Practices and Monitoring Strategies** where you'll learn:
# MAGIC - Comprehensive monitoring and alerting
# MAGIC - Performance optimization techniques
# MAGIC - Cost management strategies
# MAGIC - Disaster recovery planning
# MAGIC - Production deployment checklist
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Multi-table CDC mastered!** 🎯
