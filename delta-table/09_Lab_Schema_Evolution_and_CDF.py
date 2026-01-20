# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Schema Evolution and Change Data Feed
# MAGIC
# MAGIC ## Overview
# MAGIC This lab teaches you how to safely evolve table schemas and track row-level changes with Change Data Feed (CDF). You'll learn to add, rename, and drop columns, handle schema changes gracefully, enable CDF for CDC patterns, and implement comprehensive change tracking for auditing and incremental processing.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Add, rename, and drop columns from Delta tables
# MAGIC - Handle schema evolution during writes
# MAGIC - Understand schema enforcement vs. evolution
# MAGIC - Enable and configure Change Data Feed (CDF)
# MAGIC - Query CDF to track inserts, updates, and deletes
# MAGIC - Implement CDC (Change Data Capture) patterns
# MAGIC - Process incremental changes efficiently
# MAGIC - Track row-level changes for auditing and compliance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Labs 2-3 (Table creation and CRUD operations)
# MAGIC - Understanding of Delta Lake versioning
# MAGIC - Basic knowledge of data pipelines
# MAGIC
# MAGIC ## Duration
# MAGIC 45 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Prepare Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lab_db;
# MAGIC USE delta_lab_db;
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit, expr
from pyspark.sql.types import *

print("✅ Environment ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Create Base Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create initial employees table
# MAGIC CREATE OR REPLACE TABLE employees_schema (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     salary DECIMAL(10,2),
# MAGIC     department STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert initial data
# MAGIC INSERT INTO employees_schema VALUES
# MAGIC     (1, 'Alice Johnson', 95000.00, 'Engineering'),
# MAGIC     (2, 'Bob Smith', 87000.00, 'Marketing'),
# MAGIC     (3, 'Charlie Brown', 105000.00, 'Engineering');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View initial schema
# MAGIC DESCRIBE employees_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Add Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL ALTER TABLE - Add Single Column

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add hire_date column
# MAGIC ALTER TABLE employees_schema
# MAGIC ADD COLUMN hire_date DATE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View updated schema
# MAGIC DESCRIBE employees_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Existing rows have NULL for new column
# MAGIC SELECT * FROM employees_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Multiple Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add multiple columns at once
# MAGIC ALTER TABLE employees_schema
# MAGIC ADD COLUMNS (
# MAGIC     email STRING COMMENT 'Employee email address',
# MAGIC     manager_id INT COMMENT 'ID of direct manager'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE employees_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution During Write

# COMMAND ----------

# Create DataFrame with new columns
new_employees = spark.createDataFrame([
    (4, 'Diana Prince', 92000.00, 'Sales', '2022-05-01', 'diana@company.com', 1),
    (5, 'Eve Wilson', 78000.00, 'Marketing', '2023-01-12', 'eve@company.com', 2)
], ["id", "name", "salary", "department", "hire_date", "email", "manager_id"])

# Try to write without schema evolution (will fail if schema doesn't match exactly)
try:
    new_employees.write.format("delta").mode("append").saveAsTable("employees_schema")
    print("✅ Write succeeded")
except Exception as e:
    print(f"❌ Write failed: {type(e).__name__}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees_schema ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable Schema Evolution for Writes

# COMMAND ----------

# Create DataFrame with an additional new column
evolving_data = spark.createDataFrame([
    (6, 'Frank Castle', 115000.00, 'Engineering', '2021-09-15', 'frank@company.com', 1, 'Senior')
], ["id", "name", "salary", "department", "hire_date", "email", "manager_id", "level"])

# Write with schema evolution enabled
evolving_data.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("employees_schema")

print("✅ Schema evolution applied - new column added automatically")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View evolved schema
# MAGIC DESCRIBE employees_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Old rows have NULL for new column
# MAGIC SELECT * FROM employees_schema ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Rename Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rename column
# MAGIC ALTER TABLE employees_schema
# MAGIC RENAME COLUMN manager_id TO supervisor_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE employees_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, name, supervisor_id FROM employees_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Drop Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop column
# MAGIC ALTER TABLE employees_schema
# MAGIC DROP COLUMN level;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE employees_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Change Data Types

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widen Data Type (Safe)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change salary precision (safe widening)
# MAGIC ALTER TABLE employees_schema
# MAGIC ALTER COLUMN salary TYPE DECIMAL(12,2);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE employees_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Enforcement

# COMMAND ----------

# Try to write incompatible data
bad_schema_df = spark.createDataFrame([
    (7, 'George Wilson', 'invalid_salary', 'IT', '2024-01-01', 'george@company.com', 1)
], ["id", "name", "salary", "department", "hire_date", "email", "supervisor_id"])

try:
    bad_schema_df.write.format("delta").mode("append").saveAsTable("employees_schema")
    print("Write succeeded")
except Exception as e:
    print(f"✅ Schema enforcement prevented bad data: {type(e).__name__}")
    print("   Delta Lake protects data quality!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Change Data Feed (CDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Enable Change Data Feed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table with CDF Enabled

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create new table with CDF enabled
# MAGIC CREATE OR REPLACE TABLE customer_orders (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id INT,
# MAGIC     order_date DATE,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     status STRING
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert initial data
# MAGIC INSERT INTO customer_orders VALUES
# MAGIC     (1001, 101, '2024-01-15', 150.00, 'pending'),
# MAGIC     (1002, 102, '2024-01-16', 275.50, 'pending'),
# MAGIC     (1003, 101, '2024-01-17', 89.99, 'pending'),
# MAGIC     (1004, 103, '2024-01-18', 450.00, 'pending'),
# MAGIC     (1005, 102, '2024-01-19', 125.00, 'pending');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_orders ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable CDF on Existing Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable CDF on existing table
# MAGIC ALTER TABLE employees_schema
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Track Changes with CDF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Make Changes to Track

# COMMAND ----------

# Get current version before changes
current_version = spark.sql("DESCRIBE HISTORY customer_orders LIMIT 1") \
    .select("version").collect()[0][0]

print(f"Current version before changes: {current_version}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE: Ship some orders
# MAGIC UPDATE customer_orders
# MAGIC SET status = 'shipped'
# MAGIC WHERE order_id IN (1001, 1003);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT: New orders
# MAGIC INSERT INTO customer_orders VALUES
# MAGIC     (1006, 104, '2024-01-20', 320.00, 'pending'),
# MAGIC     (1007, 101, '2024-01-21', 199.99, 'pending');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE: Cancel an order
# MAGIC DELETE FROM customer_orders
# MAGIC WHERE order_id = 1005;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current state
# MAGIC SELECT * FROM customer_orders ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Change Data Feed

# COMMAND ----------

# Read changes using CDF
changes_df = spark.read.format("delta") \
    .option("readChangeData", "true") \
    .option("startingVersion", current_version + 1) \
    .table("customer_orders")

print(f"📊 Changes from version {current_version + 1} onwards:")
display(changes_df.select(
    "order_id", 
    "customer_id", 
    "amount", 
    "status",
    "_change_type",
    "_commit_version",
    "_commit_timestamp"
).orderBy("_commit_version", "order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Change Types

# COMMAND ----------

# MAGIC %md
# MAGIC **CDF Change Types:**
# MAGIC
# MAGIC | Change Type | Description |
# MAGIC |-------------|-------------|
# MAGIC | `insert` | New row added |
# MAGIC | `update_preimage` | Row before UPDATE |
# MAGIC | `update_postimage` | Row after UPDATE |
# MAGIC | `delete` | Row removed |

# COMMAND ----------

# Count changes by type
changes_summary = changes_df.groupBy("_change_type").count().orderBy("_change_type")

print("📊 Changes Summary:")
display(changes_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Incremental Processing with CDF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process Only Inserts

# COMMAND ----------

# Get only inserted rows
inserts = changes_df.filter(col("_change_type") == "insert")

print("📝 New Orders (Inserts):")
display(inserts.select("order_id", "customer_id", "order_date", "amount", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process Only Updates

# COMMAND ----------

# Get update post-images (current values after update)
updates = changes_df.filter(col("_change_type") == "update_postimage")

print("🔄 Updated Orders:")
display(updates.select("order_id", "customer_id", "status", "_commit_version"))

# COMMAND ----------

# Compare before and after for updates
update_comparison = spark.sql("""
    SELECT 
        pre.order_id,
        pre.status as old_status,
        post.status as new_status,
        post._commit_version,
        post._commit_timestamp
    FROM (
        SELECT * FROM customer_orders
        WHERE _change_type = 'update_preimage'
    ) pre
    JOIN (
        SELECT * FROM customer_orders  
        WHERE _change_type = 'update_postimage'
    ) post ON pre.order_id = post.order_id
""")

# Note: Above query won't work directly, need to read with CDF option
# Using DataFrame API instead:

pre_updates = changes_df.filter(col("_change_type") == "update_preimage") \
    .select("order_id", col("status").alias("old_status"), "_commit_version")

post_updates = changes_df.filter(col("_change_type") == "update_postimage") \
    .select("order_id", col("status").alias("new_status"), "_commit_version")

update_comparison = pre_updates.join(post_updates, ["order_id", "_commit_version"])

print("🔄 Status Changes:")
display(update_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process Only Deletes

# COMMAND ----------

# Get deleted rows
deletes = changes_df.filter(col("_change_type") == "delete")

print("🗑️ Deleted Orders:")
display(deletes.select("order_id", "customer_id", "amount", "_commit_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: CDC Pattern Implementation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: Sync to Downstream System

# COMMAND ----------

class CDCProcessor:
    """Process CDC events from Delta table"""
    
    def __init__(self, table_name):
        self.table_name = table_name
        self.last_processed_version = 0
    
    def get_changes_since_last_run(self):
        """Get all changes since last processed version"""
        changes = spark.read.format("delta") \
            .option("readChangeData", "true") \
            .option("startingVersion", self.last_processed_version + 1) \
            .table(self.table_name)
        
        return changes
    
    def process_changes(self):
        """Process incremental changes"""
        changes = self.get_changes_since_last_run()
        
        if changes.count() == 0:
            print("No new changes to process")
            return
        
        # Separate by change type
        inserts = changes.filter(col("_change_type") == "insert")
        updates = changes.filter(col("_change_type") == "update_postimage")
        deletes = changes.filter(col("_change_type") == "delete")
        
        insert_count = inserts.count()
        update_count = updates.count()
        delete_count = deletes.count()
        
        print(f"📊 Processing changes for {self.table_name}:")
        print(f"   Inserts: {insert_count}")
        print(f"   Updates: {update_count}")
        print(f"   Deletes: {delete_count}")
        
        # Process each type (implement your sync logic here)
        if insert_count > 0:
            self._process_inserts(inserts)
        
        if update_count > 0:
            self._process_updates(updates)
        
        if delete_count > 0:
            self._process_deletes(deletes)
        
        # Update last processed version
        latest_version = changes.agg({"_commit_version": "max"}).collect()[0][0]
        self.last_processed_version = latest_version
        
        print(f"✅ Processed up to version {latest_version}")
    
    def _process_inserts(self, inserts_df):
        """Handle insert operations"""
        print(f"   → Processing {inserts_df.count()} inserts")
        # Implement: Write to downstream system
    
    def _process_updates(self, updates_df):
        """Handle update operations"""
        print(f"   → Processing {updates_df.count()} updates")
        # Implement: Update downstream system
    
    def _process_deletes(self, deletes_df):
        """Handle delete operations"""
        print(f"   → Processing {deletes_df.count()} deletes")
        # Implement: Delete from downstream system

# COMMAND ----------

# Use CDC processor
processor = CDCProcessor("customer_orders")

# Get starting version
start_version = spark.sql("DESCRIBE HISTORY customer_orders") \
    .agg({"version": "min"}).collect()[0][0]
processor.last_processed_version = start_version

# Process changes
processor.process_changes()

# COMMAND ----------

# Make more changes
spark.sql("""
    INSERT INTO customer_orders VALUES
        (1008, 105, '2024-01-22', 599.99, 'pending')
""")

spark.sql("""
    UPDATE customer_orders
    SET status = 'completed'
    WHERE status = 'shipped'
""")

# COMMAND ----------

# Process new changes
processor.process_changes()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 10: Query CDF by Time Range

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Between Versions

# COMMAND ----------

# Get changes between specific versions
changes_range = spark.read.format("delta") \
    .option("readChangeData", "true") \
    .option("startingVersion", 1) \
    .option("endingVersion", 3) \
    .table("customer_orders")

print("📊 Changes between versions 1-3:")
display(changes_range.select(
    "order_id",
    "_change_type",
    "_commit_version"
).orderBy("_commit_version", "order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query by Timestamp

# COMMAND ----------

# Get a timestamp from history
sample_timestamp = spark.sql("""
    SELECT timestamp FROM (DESCRIBE HISTORY customer_orders)
    WHERE version = 2
""").collect()[0][0]

print(f"Querying changes since: {sample_timestamp}")

# Query changes since that timestamp
changes_since_time = spark.read.format("delta") \
    .option("readChangeData", "true") \
    .option("startingTimestamp", str(sample_timestamp)) \
    .table("customer_orders")

display(changes_since_time.select("order_id", "_change_type", "_commit_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 11: Audit Trail with CDF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Comprehensive Audit Log

# COMMAND ----------

def create_audit_trail(table_name, start_version=0):
    """Create detailed audit trail from CDF"""
    
    changes = spark.read.format("delta") \
        .option("readChangeData", "true") \
        .option("startingVersion", start_version) \
        .table(table_name)
    
    # Create audit report
    audit_trail = changes.select(
        col("order_id").alias("entity_id"),
        col("_change_type").alias("operation"),
        col("_commit_version").alias("version"),
        col("_commit_timestamp").alias("timestamp"),
        col("status"),
        col("amount")
    ).orderBy("timestamp", "entity_id")
    
    print(f"📋 Audit Trail for {table_name}:")
    print("=" * 80)
    display(audit_trail)
    
    return audit_trail

# COMMAND ----------

# Generate audit trail
audit = create_audit_trail("customer_orders", start_version=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Track Specific Entity Changes

# COMMAND ----------

def track_entity_history(table_name, entity_id_col, entity_id_value):
    """Track all changes for a specific entity"""
    
    changes = spark.read.format("delta") \
        .option("readChangeData", "true") \
        .option("startingVersion", 0) \
        .table(table_name)
    
    entity_changes = changes.filter(col(entity_id_col) == entity_id_value) \
        .select(
            entity_id_col,
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
            "*"
        ).orderBy("_commit_version")
    
    print(f"📜 Change History for {entity_id_col} = {entity_id_value}:")
    display(entity_changes)
    
    return entity_changes

# COMMAND ----------

# Track changes for specific order
track_entity_history("customer_orders", "order_id", 1001)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 12: Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution Best Practices
# MAGIC
# MAGIC ✅ **Planning:**
# MAGIC - Document schema changes
# MAGIC - Consider backward compatibility
# MAGIC - Test schema changes in development first
# MAGIC
# MAGIC ✅ **Safe Operations:**
# MAGIC - Add columns with default values
# MAGIC - Widen data types (e.g., INT → BIGINT)
# MAGIC - Rename columns (doesn't break existing code using column positions)
# MAGIC
# MAGIC ⚠️ **Risky Operations:**
# MAGIC - Dropping columns (may break downstream consumers)
# MAGIC - Narrowing data types (may lose data)
# MAGIC - Changing column semantics
# MAGIC
# MAGIC ✅ **Migration Strategy:**
# MAGIC - Use schema evolution for additive changes
# MAGIC - Create new tables for major restructuring
# MAGIC - Maintain compatibility periods for consumers

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF Best Practices
# MAGIC
# MAGIC ✅ **When to Enable CDF:**
# MAGIC - Need CDC patterns
# MAGIC - Incremental processing required
# MAGIC - Audit trail requirements
# MAGIC - Downstream system synchronization
# MAGIC
# MAGIC ✅ **Performance Considerations:**
# MAGIC - CDF adds ~20% storage overhead
# MAGIC - Minimal write performance impact
# MAGIC - Enables efficient incremental reads
# MAGIC - Worth the cost for CDC use cases
# MAGIC
# MAGIC ✅ **Processing Patterns:**
# MAGIC - Track last processed version/timestamp
# MAGIC - Handle all change types (insert/update/delete)
# MAGIC - Implement idempotent processing
# MAGIC - Monitor lag between source and target

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Schema Evolution**
# MAGIC - ALTER TABLE: Add, rename, drop columns
# MAGIC - mergeSchema option: Automatic schema evolution during writes
# MAGIC - Schema enforcement: Protects data quality
# MAGIC - Safe operations: Add columns, widen types
# MAGIC
# MAGIC ✅ **Change Data Feed**
# MAGIC - Enable with delta.enableChangeDataFeed = true
# MAGIC - Tracks inserts, updates, deletes
# MAGIC - Provides before/after values for updates
# MAGIC - Essential for CDC patterns
# MAGIC
# MAGIC ✅ **CDF Query Options**
# MAGIC - readChangeData = true: Enable CDF reading
# MAGIC - startingVersion/endingVersion: Version range
# MAGIC - startingTimestamp/endingTimestamp: Time range
# MAGIC
# MAGIC ✅ **Change Types**
# MAGIC - insert: New rows
# MAGIC - update_preimage: Before update
# MAGIC - update_postimage: After update
# MAGIC - delete: Removed rows
# MAGIC
# MAGIC ✅ **Use Cases**
# MAGIC - Incremental ETL processing
# MAGIC - Downstream system synchronization
# MAGIC - Audit trails and compliance
# MAGIC - Change data capture (CDC)
# MAGIC - Event sourcing patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Implement complete schema evolution and CDC pipeline.
# MAGIC
# MAGIC ### Scenario:
# MAGIC - Product catalog table needs new columns for promotions
# MAGIC - Must track all price changes for audit
# MAGIC - Sync changes to recommendation engine
# MAGIC - Support rollback if issues occur
# MAGIC
# MAGIC ### Tasks:
# MAGIC 1. Create product table with initial schema
# MAGIC 2. Enable CDF from the start
# MAGIC 3. Evolve schema to add promotion fields
# MAGIC 4. Make various changes (inserts, updates, deletes)
# MAGIC 5. Implement CDC processor for the table
# MAGIC 6. Create audit report showing all price changes
# MAGIC 7. Query changes by time range

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Space

# COMMAND ----------

# TODO: Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to cleanup
# spark.sql("DROP TABLE IF EXISTS employees_schema")
# spark.sql("DROP TABLE IF EXISTS customer_orders")
# print("✅ Tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Excellent work mastering schema evolution and CDF! 🎉
# MAGIC
# MAGIC **Continue to Lab 10: Production-Ready Delta Lake Pipeline (Demo)**
# MAGIC - Complete end-to-end pipeline
# MAGIC - Medallion architecture (Bronze-Silver-Gold)
# MAGIC - Data quality checks
# MAGIC - Error handling and monitoring
# MAGIC - Production best practices
# MAGIC
# MAGIC ---
# MAGIC **Further Reading:**
# MAGIC - [Schema Evolution](https://docs.delta.io/latest/delta-update.html#automatic-schema-evolution)
# MAGIC - [Change Data Feed](https://docs.delta.io/latest/delta-change-data-feed.html)
# MAGIC - [Table Properties](https://docs.delta.io/latest/table-properties.html)
