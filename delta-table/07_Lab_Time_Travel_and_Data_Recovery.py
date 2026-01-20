# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Time Travel, Versioning, and Data Recovery
# MAGIC
# MAGIC ## Overview
# MAGIC This lab teaches you to use Delta Lake's time travel capabilities for auditing, historical analysis, and data recovery. You'll learn to query previous versions of tables, restore data after mistakes, compare versions for change tracking, and implement robust data recovery strategies.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Query historical versions of tables by version number and timestamp
# MAGIC - Use time travel for auditing and compliance
# MAGIC - Restore tables to previous versions after mistakes
# MAGIC - Compare versions to track changes over time
# MAGIC - Configure retention policies for time travel
# MAGIC - Clone tables for testing and development
# MAGIC - Implement data recovery workflows
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Labs 2-3 (Table creation and CRUD operations)
# MAGIC - Understanding of Delta Lake versioning
# MAGIC - Basic knowledge of data governance
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
from pyspark.sql.functions import col, current_timestamp, lit
import time

print("✅ Environment ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Delta Lake Versioning
# MAGIC
# MAGIC Every operation on a Delta table creates a new version:
# MAGIC - Version 0: Table creation
# MAGIC - Version 1: First write/insert
# MAGIC - Version 2: Update operation
# MAGIC - Version 3: Delete operation
# MAGIC - ... and so on
# MAGIC
# MAGIC Each version is immutable and can be queried independently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Create Table with Version History

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Initial Table (Version 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create employees table
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     salary DECIMAL(10,2),
# MAGIC     department STRING,
# MAGIC     hire_date DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert initial data (Version 1)
# MAGIC INSERT INTO employees VALUES
# MAGIC     (1, 'Alice Johnson', 95000.00, 'Engineering', '2022-01-15'),
# MAGIC     (2, 'Bob Smith', 87000.00, 'Marketing', '2022-03-20'),
# MAGIC     (3, 'Charlie Brown', 105000.00, 'Engineering', '2021-06-10'),
# MAGIC     (4, 'Diana Prince', 92000.00, 'Sales', '2022-05-01'),
# MAGIC     (5, 'Eve Wilson', 78000.00, 'Marketing', '2023-01-12');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View current data
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Multiple Versions

# COMMAND ----------

# Wait a moment to see timestamp differences
time.sleep(1)

%sql
#-- Version 2: Add new employees
INSERT INTO employees VALUES
    (6, 'Frank Castle', 115000.00, 'Engineering', '2021-09-15'),
    (7, 'Grace Hopper', 98000.00, 'Engineering', '2022-07-01');

# COMMAND ----------

time.sleep(1)

%sql
-- Version 3: Give raises
UPDATE employees 
SET salary = salary * 1.10 
WHERE department = 'Engineering';

# COMMAND ----------

time.sleep(1)

%sql
-- Version 4: Delete employee
DELETE FROM employees WHERE id = 2;

# COMMAND ----------

time.sleep(1)

%sql
-- Version 5: Add more employees
INSERT INTO employees VALUES
    (8, 'Henry Ford', 88000.00, 'Sales', '2023-02-15'),
    (9, 'Ivy Chen', 102000.00, 'Engineering', '2021-11-20');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current state
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: View Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View complete history
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding History Columns
# MAGIC
# MAGIC - **version**: Sequential version number
# MAGIC - **timestamp**: When operation occurred
# MAGIC - **operation**: Type of operation (CREATE, WRITE, UPDATE, DELETE, MERGE)
# MAGIC - **operationParameters**: Details about the operation
# MAGIC - **operationMetrics**: Performance metrics (rows changed, files added/removed)
# MAGIC - **userIdentity**: Who made the change
# MAGIC - **notebook/job**: Context of the operation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View history with specific columns
# MAGIC SELECT 
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationParameters,
# MAGIC     operationMetrics
# MAGIC FROM (DESCRIBE HISTORY employees)
# MAGIC ORDER BY version DESC;

# COMMAND ----------

# Python API to view history
history_df = spark.sql("DESCRIBE HISTORY employees")
display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Time Travel by Version

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Specific Version (SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query version 1 (original data)
# MAGIC SELECT * FROM employees VERSION AS OF 1
# MAGIC ORDER BY id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query version 3 (after raises, before deletion)
# MAGIC SELECT * FROM employees VERSION AS OF 3
# MAGIC ORDER BY id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare: current vs version 1
# MAGIC SELECT 'Version 1' as version, COUNT(*) as employee_count, AVG(salary) as avg_salary
# MAGIC FROM employees VERSION AS OF 1
# MAGIC UNION ALL
# MAGIC SELECT 'Current' as version, COUNT(*) as employee_count, AVG(salary) as avg_salary
# MAGIC FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Specific Version (Python)

# COMMAND ----------

# Read version 1
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("employees")
print("Version 1 - Original data:")
display(df_v1)

# COMMAND ----------

# Read version 3
df_v3 = spark.read.format("delta").option("versionAsOf", 3).table("employees")
print("Version 3 - After raises:")
display(df_v3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Time Travel by Timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Timestamps from History

# COMMAND ----------

# Get timestamps for each version
timestamps = spark.sql("""
    SELECT version, timestamp 
    FROM (DESCRIBE HISTORY employees)
    ORDER BY version
""")

display(timestamps)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query by Timestamp (SQL)

# COMMAND ----------

# Get a specific timestamp (use one from history)
sample_timestamp = spark.sql("""
    SELECT timestamp FROM (DESCRIBE HISTORY employees)
    WHERE version = 2
""").collect()[0][0]

print(f"Querying as of: {sample_timestamp}")

# COMMAND ----------

# Query using timestamp
query_by_time = f"""
    SELECT * FROM employees 
    TIMESTAMP AS OF '{sample_timestamp}'
    ORDER BY id
"""

spark.sql(query_by_time).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query by Timestamp (Python)

# COMMAND ----------

# Read by timestamp
timestamp_str = str(sample_timestamp)
df_timestamp = spark.read.format("delta") \
    .option("timestampAsOf", timestamp_str) \
    .table("employees")

print(f"Data as of {timestamp_str}:")
display(df_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Compare Versions for Auditing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find Changed Rows Between Versions

# COMMAND ----------

# Get data from two versions
current_df = spark.read.format("delta").table("employees")
previous_df = spark.read.format("delta").option("versionAsOf", 1).table("employees")

# Find rows in current but not in previous (additions)
new_rows = current_df.subtract(previous_df)
print("New/Modified rows since version 1:")
display(new_rows)

# COMMAND ----------

# Find rows in previous but not in current (deletions)
deleted_rows = previous_df.subtract(current_df)
print("Deleted rows since version 1:")
display(deleted_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detailed Change Tracking

# COMMAND ----------

# Compare specific columns to track salary changes
from pyspark.sql.functions import col as column

salary_comparison = spark.sql("""
    SELECT 
        v1.id,
        v1.name,
        v1.salary as salary_v1,
        current.salary as salary_current,
        current.salary - v1.salary as salary_change,
        ROUND((current.salary - v1.salary) / v1.salary * 100, 2) as pct_change
    FROM employees VERSION AS OF 1 v1
    JOIN employees current ON v1.id = current.id
    WHERE v1.salary != current.salary
    ORDER BY salary_change DESC
""")

print("Salary changes from version 1 to current:")
display(salary_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Restore Table (RESTORE)
# MAGIC
# MAGIC One of the most powerful features: undo mistakes!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1: Accidental Deletion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current count
# MAGIC SELECT COUNT(*) as current_count FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Oops! Accidentally deleted all Engineering employees
# MAGIC DELETE FROM employees WHERE department = 'Engineering';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Oh no! Check the damage
# MAGIC SELECT COUNT(*) as count_after_delete FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore to Previous Version

# COMMAND ----------

# Get the version before deletion
last_good_version = spark.sql("""
    SELECT MAX(version) as version
    FROM (DESCRIBE HISTORY employees)
    WHERE operation != 'DELETE'
""").collect()[0][0]

print(f"Restoring to version: {last_good_version}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore the table
# MAGIC RESTORE TABLE employees TO VERSION AS OF 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify restoration
# MAGIC SELECT COUNT(*) as count_after_restore FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2: Bad Update

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Accidentally set all salaries to 0
# MAGIC UPDATE employees SET salary = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the mistake
# MAGIC SELECT * FROM employees ORDER BY id LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore to version before the bad update
# MAGIC RESTORE TABLE employees TO VERSION AS OF 7;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify salaries are back
# MAGIC SELECT * FROM employees ORDER BY id LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore by Timestamp

# COMMAND ----------

# Get timestamp from 5 minutes ago (example)
# In practice, use actual timestamp from DESCRIBE HISTORY

%sql
-- Restore to a specific timestamp
-- RESTORE TABLE employees TO TIMESTAMP AS OF '2024-01-15 10:30:00';

-- Note: Commented out - use actual timestamp from your history

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Table Cloning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shallow Clone (Metadata Only)
# MAGIC
# MAGIC Creates a copy with references to original data files (fast, no data copy).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create shallow clone for testing
# MAGIC CREATE TABLE IF NOT EXISTS employees_test_shallow
# MAGIC SHALLOW CLONE employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify clone
# MAGIC SELECT * FROM employees_test_shallow ORDER BY id LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clone from specific version
# MAGIC CREATE OR REPLACE TABLE employees_v1_clone
# MAGIC SHALLOW CLONE employees VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees_v1_clone ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deep Clone (Complete Copy)
# MAGIC
# MAGIC Creates independent copy with all data files (slower, but fully independent).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create deep clone for backup
# MAGIC CREATE OR REPLACE TABLE employees_backup
# MAGIC DEEP CLONE employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify deep clone
# MAGIC SELECT COUNT(*) as count FROM employees_backup;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Cases for Cloning
# MAGIC
# MAGIC **Shallow Clone:**
# MAGIC - ✅ Quick testing/development environments
# MAGIC - ✅ Creating views of historical data
# MAGIC - ✅ A/B testing scenarios
# MAGIC - ⚠️ Dependent on original table's retention
# MAGIC
# MAGIC **Deep Clone:**
# MAGIC - ✅ Long-term backups
# MAGIC - ✅ Archiving before major changes
# MAGIC - ✅ Data migration
# MAGIC - ✅ Independent test environments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Retention Policies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Retention
# MAGIC
# MAGIC Two key retention settings:
# MAGIC 1. **Log Retention**: How long to keep transaction log history
# MAGIC 2. **Deleted File Retention**: How long to keep old data files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current retention settings
# MAGIC SHOW TBLPROPERTIES employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Retention

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set retention for 90 days (for compliance/auditing)
# MAGIC ALTER TABLE employees
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.logRetentionDuration' = '90 days',
# MAGIC     'delta.deletedFileRetentionDuration' = '90 days'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify settings
# MAGIC SHOW TBLPROPERTIES employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retention Guidelines
# MAGIC
# MAGIC **Production Tables:**
# MAGIC - Log retention: 30-90 days (compliance dependent)
# MAGIC - Deleted file retention: 7-30 days (recovery window)
# MAGIC
# MAGIC **Compliance Requirements:**
# MAGIC - Financial data: 7+ years
# MAGIC - Healthcare (HIPAA): 6+ years
# MAGIC - General: 30-90 days
# MAGIC
# MAGIC **Development/Test:**
# MAGIC - Log retention: 7 days
# MAGIC - Deleted file retention: 1-2 days

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: Production Recovery Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recovery Workflow 1: Scheduled Backups

# COMMAND ----------

def create_backup(table_name, backup_suffix="backup"):
    """Create timestamped backup of table"""
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{table_name}_{backup_suffix}_{timestamp}"
    
    spark.sql(f"""
        CREATE TABLE {backup_name}
        DEEP CLONE {table_name}
    """)
    
    print(f"✅ Backup created: {backup_name}")
    return backup_name

# Example usage
# backup_name = create_backup("employees", "daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recovery Workflow 2: Pre-Change Snapshots

# COMMAND ----------

def safe_operation_with_snapshot(table_name, operation_func):
    """Execute operation with automatic snapshot for recovery"""
    
    # Get current version
    current_version = spark.sql(f"""
        SELECT MAX(version) as version 
        FROM (DESCRIBE HISTORY {table_name})
    """).collect()[0][0]
    
    print(f"📸 Snapshot: Current version is {current_version}")
    
    try:
        # Execute operation
        operation_func()
        print("✅ Operation completed successfully")
    except Exception as e:
        # Rollback on error
        print(f"❌ Operation failed: {e}")
        print(f"🔄 Rolling back to version {current_version}")
        spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {current_version}")
        print("✅ Rollback complete")
        raise

# Example usage:
# safe_operation_with_snapshot("employees", 
#     lambda: spark.sql("UPDATE employees SET salary = salary * 2"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recovery Workflow 3: Audit Trail

# COMMAND ----------

def audit_changes(table_name, start_version=None, end_version=None):
    """Generate audit report of changes between versions"""
    
    history = spark.sql(f"DESCRIBE HISTORY {table_name}")
    
    if start_version:
        history = history.filter(col("version") >= start_version)
    if end_version:
        history = history.filter(col("version") <= end_version)
    
    print(f"📊 Audit Trail: {table_name}")
    print("=" * 80)
    
    audit_df = history.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics.numOutputRows",
        "operationMetrics.numAddedFiles",
        "operationMetrics.numRemovedFiles"
    ).orderBy("version")
    
    display(audit_df)
    
    return audit_df

# Generate audit trail
audit_changes("employees", start_version=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recovery Workflow 4: Change Detection

# COMMAND ----------

def detect_anomalies(table_name, threshold_pct=50):
    """Detect anomalous changes in table (large deletes/updates)"""
    
    history = spark.sql(f"DESCRIBE HISTORY {table_name}")
    
    latest_version = history.select(col("version")).first()[0]
    
    # Check last few operations
    recent = history.filter(col("version") >= latest_version - 5)
    
    print(f"🔍 Checking for anomalies in {table_name}")
    print(f"Threshold: {threshold_pct}% change")
    print("=" * 80)
    
    for row in recent.collect():
        version = row["version"]
        operation = row["operation"]
        
        # Check for large deletions
        if operation in ["DELETE", "MERGE", "UPDATE"]:
            metrics = row["operationMetrics"]
            
            if metrics and "numRemovedFiles" in metrics:
                removed = int(metrics.get("numRemovedFiles", 0))
                
                if removed > 0:
                    current_files = spark.sql(f"DESCRIBE DETAIL {table_name}") \
                        .select("numFiles").first()[0]
                    
                    pct_change = (removed / current_files) * 100
                    
                    if pct_change > threshold_pct:
                        print(f"⚠️ ANOMALY DETECTED at version {version}")
                        print(f"   Operation: {operation}")
                        print(f"   Files removed: {removed} ({pct_change:.1f}%)")
                        print()

detect_anomalies("employees", threshold_pct=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 10: Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel Best Practices
# MAGIC
# MAGIC ✅ **Regular Backups**
# MAGIC - Create deep clones for critical tables
# MAGIC - Schedule daily/weekly backups
# MAGIC - Store backups separately
# MAGIC
# MAGIC ✅ **Retention Configuration**
# MAGIC - Set appropriate retention based on compliance
# MAGIC - Balance recovery needs vs. storage costs
# MAGIC - Document retention policies
# MAGIC
# MAGIC ✅ **Testing & Validation**
# MAGIC - Use shallow clones for testing changes
# MAGIC - Validate operations before production
# MAGIC - Test recovery procedures regularly
# MAGIC
# MAGIC ✅ **Monitoring & Alerts**
# MAGIC - Monitor version history growth
# MAGIC - Alert on anomalous changes
# MAGIC - Track storage costs from old versions
# MAGIC
# MAGIC ✅ **Documentation**
# MAGIC - Document major version changes
# MAGIC - Maintain change log
# MAGIC - Record recovery procedures

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Time Travel Queries**
# MAGIC - VERSION AS OF: Query by version number
# MAGIC - TIMESTAMP AS OF: Query by timestamp
# MAGIC - Both SQL and DataFrame API supported
# MAGIC
# MAGIC ✅ **Data Recovery**
# MAGIC - RESTORE TABLE: Rollback to previous version
# MAGIC - Fast recovery from mistakes
# MAGIC - Can restore by version or timestamp
# MAGIC
# MAGIC ✅ **Table Cloning**
# MAGIC - Shallow Clone: Fast, metadata-only references
# MAGIC - Deep Clone: Complete independent copy
# MAGIC - Use for testing, backups, migrations
# MAGIC
# MAGIC ✅ **Retention Management**
# MAGIC - Configure logRetentionDuration
# MAGIC - Configure deletedFileRetentionDuration
# MAGIC - Balance recovery needs vs. costs
# MAGIC
# MAGIC ✅ **Production Patterns**
# MAGIC - Regular backups with deep clones
# MAGIC - Pre-change snapshots for safety
# MAGIC - Audit trails for compliance
# MAGIC - Anomaly detection for protection

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Implement a complete data recovery strategy.
# MAGIC
# MAGIC ### Scenario:
# MAGIC - Critical sales table with daily updates
# MAGIC - Must comply with financial regulations (7-year retention)
# MAGIC - Need to recover from accidental deletes within 24 hours
# MAGIC - Want to test schema changes safely
# MAGIC
# MAGIC ### Tasks:
# MAGIC 1. Create a sample sales table with multiple versions
# MAGIC 2. Configure appropriate retention policies
# MAGIC 3. Create a backup strategy (schedule and storage)
# MAGIC 4. Simulate a mistake and recover
# MAGIC 5. Implement change auditing
# MAGIC 6. Test a schema change with shallow clone

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
# spark.sql("DROP TABLE IF EXISTS employees")
# spark.sql("DROP TABLE IF EXISTS employees_test_shallow")
# spark.sql("DROP TABLE IF EXISTS employees_v1_clone")
# spark.sql("DROP TABLE IF EXISTS employees_backup")
# print("✅ Tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Excellent work mastering time travel and recovery! 🎉
# MAGIC
# MAGIC **Continue to Lab 8: Table Maintenance**
# MAGIC - Understand and fix small file problems
# MAGIC - Run OPTIMIZE for compaction
# MAGIC - Use VACUUM to remove old files
# MAGIC - Configure auto-optimize
# MAGIC - Monitor table health
# MAGIC
# MAGIC ---
# MAGIC **Further Reading:**
# MAGIC - [Delta Lake Time Travel](https://docs.delta.io/latest/delta-batch.html#deltatimetravel)
# MAGIC - [Table Restore](https://docs.delta.io/latest/delta-utility.html#restore-a-delta-table)
# MAGIC - [Table Cloning](https://docs.delta.io/latest/delta-utility.html#clone-a-delta-table)
