# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Multi-Table CDC and Dependency Management
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll learn how to handle CDC for multiple tables with foreign key relationships. You'll manage ingestion order, deal with referential integrity constraints, and implement strategies for handling dependent table scenarios.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Understand foreign key dependencies in CDC pipelines
# MAGIC - Configure ingestion order for parent-child table relationships
# MAGIC - Handle referential integrity violations during CDC
# MAGIC - Test scenarios with cascading changes
# MAGIC - Implement error handling strategies for constraint violations
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2 (SQL Server CDC configured)
# MAGIC - Completed Lab 3 (Lakeflow Connect pipeline created)
# MAGIC - Completed Lab 5 (Tested basic CRUD operations)
# MAGIC - Understanding of foreign key relationships
# MAGIC
# MAGIC ## Duration
# MAGIC 15-20 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Understand Table Dependencies
# MAGIC
# MAGIC Let's map out the foreign key relationships in our retail database.
# MAGIC
# MAGIC ### 1.1 Visualize Table Relationships
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐
# MAGIC │     customers       │  ← Parent Table
# MAGIC │  (customer_id PK)   │
# MAGIC └─────────────────────┘
# MAGIC            ↓
# MAGIC         (FK: customer_id)
# MAGIC            ↓
# MAGIC ┌─────────────────────┐
# MAGIC │      orders         │  ← Child Table
# MAGIC │  (order_id PK)      │
# MAGIC │  (customer_id FK)   │
# MAGIC └─────────────────────┘
# MAGIC
# MAGIC ┌─────────────────────┐
# MAGIC │     products        │  ← Independent Table
# MAGIC │  (product_id PK)    │
# MAGIC └─────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Set Configuration
catalog_name = "retail_analytics"
bronze_schema = "bronze"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {bronze_schema}")

print(f"✓ Using {catalog_name}.{bronze_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Query Foreign Key Relationships in SQL Server
# MAGIC
# MAGIC **Run this in SQL Server to see FK constraints:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- View foreign key relationships
# MAGIC SELECT 
# MAGIC     fk.name AS foreign_key_name,
# MAGIC     OBJECT_NAME(fk.parent_object_id) AS child_table,
# MAGIC     COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS child_column,
# MAGIC     OBJECT_NAME(fk.referenced_object_id) AS parent_table,
# MAGIC     COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS parent_column
# MAGIC FROM sys.foreign_keys AS fk
# MAGIC INNER JOIN sys.foreign_key_columns AS fkc 
# MAGIC     ON fk.object_id = fkc.constraint_object_id
# MAGIC WHERE OBJECT_NAME(fk.parent_object_id) IN ('customers', 'orders', 'products')
# MAGIC ORDER BY child_table;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC foreign_key_name    | child_table | child_column | parent_table | parent_column
# MAGIC --------------------|-------------|--------------|--------------|---------------
# MAGIC FK_orders_customers | orders      | customer_id  | customers    | customer_id
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Check Bronze Tables for Relationships

# COMMAND ----------

# DBTITLE 1,Verify Customer-Order Relationships
# MAGIC %sql
# MAGIC -- Check how many orders per customer
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.first_name,
# MAGIC   c.last_name,
# MAGIC   COUNT(o.order_id) as order_count
# MAGIC FROM customers c
# MAGIC LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name
# MAGIC HAVING COUNT(o.order_id) > 0
# MAGIC ORDER BY order_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Checkpoint:** Understanding that orders depend on customers existing first

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Test Proper Ingestion Order
# MAGIC
# MAGIC Let's test the happy path where parent records are inserted before child records.
# MAGIC
# MAGIC ### 2.1 Insert Parent Record First (Customer)
# MAGIC
# MAGIC **In SQL Server:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Test 1: Add parent record first
# MAGIC INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, zip_code, created_date, last_updated)
# MAGIC VALUES (20001, 'Emma', 'Watson', 'emma.watson@email.com', '555-2001', 'Boston', 'MA', '02101', GETDATE(), GETDATE());
# MAGIC
# MAGIC PRINT 'Parent record (customer 20001) inserted';
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Insert Child Record (Order for that Customer)
# MAGIC
# MAGIC **In SQL Server:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Test 2: Add child record referencing parent
# MAGIC INSERT INTO orders (order_id, customer_id, order_date, order_status, total_amount, payment_method, created_date, last_updated)
# MAGIC VALUES (7001, 20001, GETDATE(), 'PENDING', 599.99, 'Credit Card', GETDATE(), GETDATE());
# MAGIC
# MAGIC PRINT 'Child record (order 7001 for customer 20001) inserted';
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Run Pipeline and Verify
# MAGIC
# MAGIC **In Databricks UI:**
# MAGIC 1. Navigate to **Data** → **Ingestion**
# MAGIC 2. Run `retail_ingestion_pipeline` (incremental mode)
# MAGIC 3. Wait for completion

# COMMAND ----------

# DBTITLE 1,Verify Parent-Child Ingestion
# MAGIC %sql
# MAGIC -- Check customer exists
# MAGIC SELECT 'Customer' as record_type, customer_id, first_name, last_name 
# MAGIC FROM customers 
# MAGIC WHERE customer_id = 20001
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check order exists
# MAGIC SELECT 'Order' as record_type, order_id, customer_id, order_status 
# MAGIC FROM orders 
# MAGIC WHERE order_id = 7001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Both customer 20001 and order 7001 appear correctly

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Verify Referential Integrity

# COMMAND ----------

# DBTITLE 1,Join Customer with Orders
# MAGIC %sql
# MAGIC -- Verify FK relationship works
# MAGIC SELECT 
# MAGIC   o.order_id,
# MAGIC   o.customer_id,
# MAGIC   c.first_name,
# MAGIC   c.last_name,
# MAGIC   o.total_amount,
# MAGIC   o.order_status
# MAGIC FROM orders o
# MAGIC INNER JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_id = 7001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Order 7001 successfully joins with customer Emma Watson

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test Cascading Updates
# MAGIC
# MAGIC Let's test updating a parent record and see impact on child records.
# MAGIC
# MAGIC ### 3.1 Update Parent Record
# MAGIC
# MAGIC **In SQL Server:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Update customer information
# MAGIC UPDATE customers
# MAGIC SET 
# MAGIC     email = 'emma.w.updated@email.com',
# MAGIC     city = 'Cambridge',
# MAGIC     last_updated = GETDATE()
# MAGIC WHERE customer_id = 20001;
# MAGIC
# MAGIC PRINT 'Customer 20001 updated';
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Update Child Record
# MAGIC
# MAGIC **In SQL Server:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Update related order
# MAGIC UPDATE orders
# MAGIC SET 
# MAGIC     order_status = 'PROCESSING',
# MAGIC     last_updated = GETDATE()
# MAGIC WHERE order_id = 7001;
# MAGIC
# MAGIC PRINT 'Order 7001 status updated';
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Run Pipeline and Verify Updates
# MAGIC
# MAGIC **Run pipeline again in incremental mode**

# COMMAND ----------

# DBTITLE 1,Verify Cascading Updates
# MAGIC %sql
# MAGIC -- Check both parent and child updates applied
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.first_name,
# MAGIC   c.email,
# MAGIC   c.city,
# MAGIC   o.order_id,
# MAGIC   o.order_status,
# MAGIC   c.last_updated as customer_updated,
# MAGIC   o.last_updated as order_updated
# MAGIC FROM customers c
# MAGIC INNER JOIN orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.customer_id = 20001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Customer email = `emma.w.updated@email.com`, city = `Cambridge`, order status = `PROCESSING`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test Delete with Foreign Key Constraints
# MAGIC
# MAGIC Let's test deletion scenarios with parent-child relationships.
# MAGIC
# MAGIC ### 4.1 Attempt to Delete Parent with Existing Children (Will Fail in SQL Server)
# MAGIC
# MAGIC **In SQL Server:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- This will FAIL due to FK constraint
# MAGIC -- Uncomment to test:
# MAGIC /*
# MAGIC DELETE FROM customers WHERE customer_id = 20001;
# MAGIC */
# MAGIC
# MAGIC -- Expected Error:
# MAGIC -- The DELETE statement conflicted with the REFERENCE constraint "FK_orders_customers"
# MAGIC -- Cannot delete parent row while child rows exist
# MAGIC ```
# MAGIC
# MAGIC **This demonstrates SQL Server enforces referential integrity**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Proper Delete Order: Child First, Then Parent
# MAGIC
# MAGIC **In SQL Server:**
# MAGIC
# MAGIC ```sql
# MAGIC USE RetailDB;
# MAGIC GO
# MAGIC
# MAGIC -- Step 1: Delete child record(s) first
# MAGIC DELETE FROM orders WHERE order_id = 7001;
# MAGIC PRINT 'Child record (order 7001) deleted';
# MAGIC
# MAGIC -- Step 2: Now delete parent record
# MAGIC DELETE FROM customers WHERE customer_id = 20001;
# MAGIC PRINT 'Parent record (customer 20001) deleted';
# MAGIC
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Run Pipeline and Verify Deletions

# COMMAND ----------

# DBTITLE 1,Verify Cascading Deletes
# MAGIC %sql
# MAGIC -- Both records should be deleted
# MAGIC SELECT 'Customer 20001' as record_check, COUNT(*) as count 
# MAGIC FROM customers 
# MAGIC WHERE customer_id = 20001
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Order 7001', COUNT(*) 
# MAGIC FROM orders 
# MAGIC WHERE order_id = 7001;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Expected:** Both counts = 0 (deleted successfully)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Handle Orphaned Records Scenario
# MAGIC
# MAGIC What if bronze tables don't enforce FK constraints? Let's simulate and detect orphaned records.
# MAGIC
# MAGIC ### 5.1 Check for Orphaned Orders
# MAGIC
# MAGIC Orders that reference non-existent customers:

# COMMAND ----------

# DBTITLE 1,Find Orphaned Orders
# MAGIC %sql
# MAGIC -- Find orders with no matching customer
# MAGIC SELECT 
# MAGIC   o.order_id,
# MAGIC   o.customer_id as missing_customer_id,
# MAGIC   o.order_status,
# MAGIC   o.total_amount,
# MAGIC   '⚠ Orphaned Record' as warning
# MAGIC FROM orders o
# MAGIC LEFT JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **If orphaned records exist:**
# MAGIC - This indicates orders ingested before their customers
# MAGIC - Or customers deleted while orders still exist
# MAGIC - Need to implement handling strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Create Orphan Detection Query

# COMMAND ----------

# DBTITLE 1,Comprehensive Referential Integrity Check
# MAGIC %sql
# MAGIC WITH integrity_checks AS (
# MAGIC   -- Total orders
# MAGIC   SELECT COUNT(*) as total_orders FROM orders
# MAGIC ),
# MAGIC valid_orders AS (
# MAGIC   -- Orders with valid customer references
# MAGIC   SELECT COUNT(*) as valid_count
# MAGIC   FROM orders o
# MAGIC   INNER JOIN customers c ON o.customer_id = c.customer_id
# MAGIC ),
# MAGIC orphaned_orders AS (
# MAGIC   -- Orders with invalid customer references
# MAGIC   SELECT COUNT(*) as orphaned_count
# MAGIC   FROM orders o
# MAGIC   LEFT JOIN customers c ON o.customer_id = c.customer_id
# MAGIC   WHERE c.customer_id IS NULL
# MAGIC )
# MAGIC SELECT 
# MAGIC   ic.total_orders,
# MAGIC   vo.valid_count,
# MAGIC   oo.orphaned_count,
# MAGIC   ROUND(vo.valid_count * 100.0 / ic.total_orders, 2) as integrity_percentage,
# MAGIC   CASE 
# MAGIC     WHEN oo.orphaned_count = 0 THEN '✓ All references valid'
# MAGIC     ELSE CONCAT('⚠ ', oo.orphaned_count, ' orphaned records found')
# MAGIC   END as status
# MAGIC FROM integrity_checks ic, valid_orders vo, orphaned_orders oo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Strategies for Handling Dependencies
# MAGIC
# MAGIC ### Strategy 1: Rely on SQL Server Constraints (Recommended)
# MAGIC
# MAGIC **Approach:**
# MAGIC - Keep FK constraints enabled in SQL Server
# MAGIC - SQL Server enforces proper INSERT/DELETE order
# MAGIC - CDC captures operations in correct sequence
# MAGIC - Bronze tables receive data in valid order
# MAGIC
# MAGIC **Pros:**
# MAGIC - ✅ Simplest approach
# MAGIC - ✅ Guaranteed referential integrity at source
# MAGIC - ✅ No additional logic needed in pipeline
# MAGIC
# MAGIC **Cons:**
# MAGIC - ❌ Requires source database control
# MAGIC - ❌ May block some application operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 2: Add FK Constraints to Bronze Tables
# MAGIC
# MAGIC **Approach:**
# MAGIC - Define FK constraints on bronze Delta tables
# MAGIC - Databricks enforces at write time
# MAGIC
# MAGIC **Example:**

# COMMAND ----------

# DBTITLE 1,Add Foreign Key Constraint (Delta Lake)
# MAGIC %sql
# MAGIC -- Note: Delta Lake FK constraints are for metadata/documentation
# MAGIC -- They don't enforce at runtime like traditional databases
# MAGIC
# MAGIC ALTER TABLE orders 
# MAGIC ADD CONSTRAINT fk_orders_customers 
# MAGIC FOREIGN KEY (customer_id) 
# MAGIC REFERENCES customers(customer_id);

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Delta Lake constraints are informational. For enforcement, use CHECK constraints or application logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 3: Implement Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Create Data Quality Validation Table
# MAGIC %sql
# MAGIC -- Create table to log referential integrity violations
# MAGIC CREATE TABLE IF NOT EXISTS bronze.data_quality_issues (
# MAGIC   issue_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   check_timestamp TIMESTAMP,
# MAGIC   issue_type STRING,
# MAGIC   table_name STRING,
# MAGIC   record_id STRING,
# MAGIC   issue_description STRING,
# MAGIC   severity STRING
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Log Orphaned Records
# MAGIC %sql
# MAGIC -- Insert orphaned orders into issues table
# MAGIC INSERT INTO bronze.data_quality_issues (
# MAGIC   check_timestamp,
# MAGIC   issue_type,
# MAGIC   table_name,
# MAGIC   record_id,
# MAGIC   issue_description,
# MAGIC   severity
# MAGIC )
# MAGIC SELECT 
# MAGIC   CURRENT_TIMESTAMP() as check_timestamp,
# MAGIC   'ORPHANED_RECORD' as issue_type,
# MAGIC   'orders' as table_name,
# MAGIC   CAST(o.order_id AS STRING) as record_id,
# MAGIC   CONCAT('Order references non-existent customer_id: ', o.customer_id) as issue_description,
# MAGIC   'WARNING' as severity
# MAGIC FROM orders o
# MAGIC LEFT JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;

# COMMAND ----------

# DBTITLE 1,View Data Quality Issues
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   check_timestamp,
# MAGIC   issue_type,
# MAGIC   table_name,
# MAGIC   record_id,
# MAGIC   issue_description,
# MAGIC   severity
# MAGIC FROM bronze.data_quality_issues
# MAGIC ORDER BY check_timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 4: Create Staging Layer for Validation
# MAGIC
# MAGIC **Approach:**
# MAGIC - Land CDC data in staging tables (no constraints)
# MAGIC - Validate referential integrity
# MAGIC - Promote only valid records to final bronze tables
# MAGIC - Quarantine invalid records for review

# COMMAND ----------

# DBTITLE 1,Create Quarantine Table
# MAGIC %sql
# MAGIC -- Table to hold orders with integrity issues
# MAGIC CREATE TABLE IF NOT EXISTS bronze.orders_quarantine (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   order_status STRING,
# MAGIC   total_amount DECIMAL(10, 2),
# MAGIC   payment_method STRING,
# MAGIC   quarantine_reason STRING,
# MAGIC   quarantine_timestamp TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Best Practices for Multi-Table CDC
# MAGIC
# MAGIC ### 7.1 Ingestion Order Best Practices
# MAGIC
# MAGIC **Recommendation:**
# MAGIC ```
# MAGIC Priority 1 (Parent Tables):
# MAGIC   - customers
# MAGIC   - products
# MAGIC   
# MAGIC Priority 2 (Child Tables):
# MAGIC   - orders (depends on customers)
# MAGIC   
# MAGIC Priority 3 (Fact/Transaction Tables):
# MAGIC   - order_items (depends on orders and products)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Monitor Referential Integrity
# MAGIC
# MAGIC **Create scheduled job to check integrity:**

# COMMAND ----------

# DBTITLE 1,Referential Integrity Monitoring Query
# MAGIC %sql
# MAGIC -- Run this daily to monitor data quality
# MAGIC SELECT 
# MAGIC   CURRENT_DATE() as check_date,
# MAGIC   'orders_to_customers' as relationship,
# MAGIC   COUNT(DISTINCT o.customer_id) as distinct_customer_refs,
# MAGIC   COUNT(DISTINCT c.customer_id) as valid_customer_refs,
# MAGIC   COUNT(DISTINCT o.customer_id) - COUNT(DISTINCT c.customer_id) as invalid_refs,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(DISTINCT o.customer_id) = COUNT(DISTINCT c.customer_id) THEN '✓ Integrity OK'
# MAGIC     ELSE '⚠ Integrity Issues Detected'
# MAGIC   END as status
# MAGIC FROM orders o
# MAGIC LEFT JOIN customers c ON o.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Handle Schema Evolution
# MAGIC
# MAGIC **If FK columns change (rare but possible):**
# MAGIC
# MAGIC ```sql
# MAGIC -- Handle customer_id type change from INT to BIGINT
# MAGIC ALTER TABLE orders ALTER COLUMN customer_id TYPE BIGINT;
# MAGIC ALTER TABLE customers ALTER COLUMN customer_id TYPE BIGINT;
# MAGIC ```
# MAGIC
# MAGIC Lakeflow Connect handles schema evolution, but ensure both tables evolve consistently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Validation
# MAGIC
# MAGIC ### Final Validation

# COMMAND ----------

# DBTITLE 1,Multi-Table CDC Validation
# MAGIC %sql
# MAGIC WITH validation AS (
# MAGIC   -- Check referential integrity
# MAGIC   SELECT 'Referential Integrity' as test,
# MAGIC          CASE 
# MAGIC            WHEN COUNT(*) = 0 THEN '✓ PASS'
# MAGIC            ELSE '✗ FAIL - Orphaned records exist'
# MAGIC          END as result
# MAGIC   FROM orders o
# MAGIC   LEFT JOIN customers c ON o.customer_id = c.customer_id
# MAGIC   WHERE c.customer_id IS NULL
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Verify test customer exists
# MAGIC   SELECT 'Test Data Present',
# MAGIC          CASE 
# MAGIC            WHEN COUNT(*) >= 5 THEN '✓ PASS'
# MAGIC            ELSE '✗ FAIL - Insufficient test data'
# MAGIC          END
# MAGIC   FROM customers
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Verify orders link to customers
# MAGIC   SELECT 'Orders Join Customers',
# MAGIC          CASE 
# MAGIC            WHEN COUNT(*) > 0 THEN '✓ PASS'
# MAGIC            ELSE '✗ FAIL - No valid joins'
# MAGIC          END
# MAGIC   FROM orders o
# MAGIC   INNER JOIN customers c ON o.customer_id = c.customer_id
# MAGIC )
# MAGIC SELECT * FROM validation;

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ **Understood table dependencies:**
# MAGIC - Mapped foreign key relationships (customers → orders)
# MAGIC - Identified parent-child table hierarchies
# MAGIC
# MAGIC ✅ **Tested proper ingestion order:**
# MAGIC - Inserted parent records before children
# MAGIC - Verified referential integrity maintained
# MAGIC
# MAGIC ✅ **Handled cascading operations:**
# MAGIC - Updated parent and child records
# MAGIC - Deleted in proper order (child first, then parent)
# MAGIC
# MAGIC ✅ **Detected orphaned records:**
# MAGIC - Created queries to find integrity violations
# MAGIC - Implemented data quality monitoring
# MAGIC
# MAGIC ✅ **Learned handling strategies:**
# MAGIC - Rely on source DB constraints
# MAGIC - Add validation checks
# MAGIC - Implement quarantine tables
# MAGIC - Monitor referential integrity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 8: Implementing Scheduled Pipelines and Error Handling** where you'll:
# MAGIC - Configure pipeline scheduling (continuous, batch, triggered)
# MAGIC - Implement comprehensive monitoring queries
# MAGIC - Build alerting for pipeline failures
# MAGIC - Create operational dashboards
# MAGIC - Test error recovery scenarios
# MAGIC
# MAGIC **Your multi-table CDC is working! Let's operationalize it next!** 🚀
