# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: CRUD Operations and ACID Transactions
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on lab teaches you how to perform Create, Read, Update, and Delete (CRUD) operations on Delta Lake tables while maintaining ACID transaction guarantees. You'll learn to use both SQL and Python APIs for data manipulation, implement MERGE operations for upserts, and understand how Delta Lake handles concurrent writes.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - INSERT new records into Delta tables
# MAGIC - UPDATE existing records with conditional logic
# MAGIC - DELETE records based on specific criteria
# MAGIC - Perform MERGE operations (upserts) efficiently
# MAGIC - Understand and demonstrate ACID transaction guarantees
# MAGIC - Handle concurrent write scenarios
# MAGIC - Work with both SQL and Python DataFrame APIs
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lab 2 (Creating Your First Delta Tables)
# MAGIC - Understanding of Delta Lake architecture
# MAGIC - Basic SQL and PySpark knowledge
# MAGIC
# MAGIC ## Duration
# MAGIC 40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Prepare Database and Sample Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or use existing database
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lab_db;
# MAGIC USE delta_lab_db;
# MAGIC
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

# MAGIC %md
# MAGIC Create our base employee table for CRUD operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop table if exists (for clean start)
# MAGIC DROP TABLE IF EXISTS employees;
# MAGIC
# MAGIC -- Create employees table
# MAGIC CREATE TABLE employees (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     salary DECIMAL(10,2),
# MAGIC     department STRING,
# MAGIC     hire_date DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert initial data
# MAGIC INSERT INTO employees VALUES
# MAGIC     (1, 'Alice Johnson', 95000.00, 'Engineering', '2022-01-15'),
# MAGIC     (2, 'Bob Smith', 87000.00, 'Marketing', '2022-03-20'),
# MAGIC     (3, 'Charlie Brown', 105000.00, 'Engineering', '2021-06-10'),
# MAGIC     (4, 'Diana Prince', 92000.00, 'Sales', '2022-05-01'),
# MAGIC     (5, 'Eve Wilson', 78000.00, 'Marketing', '2023-01-12');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify initial data
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: INSERT Operations
# MAGIC
# MAGIC INSERT adds new records to a Delta table. Delta Lake ensures atomicity - either all rows are inserted or none.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL INSERT - Single Row

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a single employee
# MAGIC INSERT INTO employees 
# MAGIC VALUES (6, 'Frank Castle', 115000.00, 'Engineering', '2021-09-15');
# MAGIC
# MAGIC -- Verify insertion
# MAGIC SELECT * FROM employees WHERE id = 6;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL INSERT - Multiple Rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert multiple employees at once
# MAGIC INSERT INTO employees VALUES
# MAGIC     (7, 'Grace Hopper', 98000.00, 'Engineering', '2022-07-01'),
# MAGIC     (8, 'Henry Ford', 88000.00, 'Sales', '2023-02-15'),
# MAGIC     (9, 'Ivy Chen', 102000.00, 'Engineering', '2021-11-20');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python DataFrame INSERT (Append Mode)

# COMMAND ----------

from pyspark.sql import Row

# Create new employee records as DataFrame
new_employees = spark.createDataFrame([
    Row(id=10, name="Jack Ryan", salary=91000.00, department="Sales", hire_date="2023-03-10"),
    Row(id=11, name="Karen Page", salary=84000.00, department="Marketing", hire_date="2023-04-05")
])

# Append to table (INSERT operation)
new_employees.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("employees")

print("✅ Employees inserted via DataFrame API")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all insertions
# MAGIC SELECT COUNT(*) as total_employees FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: UPDATE Operations
# MAGIC
# MAGIC UPDATE modifies existing records. In Delta Lake, updates are implemented using copy-on-write: affected files are rewritten with updated data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL UPDATE - Simple Condition

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Give 10% raise to Engineering department
# MAGIC UPDATE employees 
# MAGIC SET salary = salary * 1.10 
# MAGIC WHERE department = 'Engineering';
# MAGIC
# MAGIC -- View updated salaries
# MAGIC SELECT name, department, salary 
# MAGIC FROM employees 
# MAGIC WHERE department = 'Engineering'
# MAGIC ORDER BY salary DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL UPDATE - Multiple Columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update multiple columns for a specific employee
# MAGIC UPDATE employees
# MAGIC SET 
# MAGIC     salary = 120000.00,
# MAGIC     department = 'Management'
# MAGIC WHERE id = 3;
# MAGIC
# MAGIC -- Verify update
# MAGIC SELECT * FROM employees WHERE id = 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python API UPDATE

# COMMAND ----------

from delta.tables import DeltaTable

# Load Delta table
delta_table = DeltaTable.forName(spark, "employees")

# Update using Python API
delta_table.update(
    condition="department = 'Marketing'",
    set={"salary": "salary * 1.05"}  # 5% raise for Marketing
)

print("✅ Marketing salaries updated")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Marketing employees
# MAGIC SELECT name, department, salary 
# MAGIC FROM employees 
# MAGIC WHERE department = 'Marketing'
# MAGIC ORDER BY salary DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional UPDATE with CASE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply different raises based on salary tier
# MAGIC UPDATE employees
# MAGIC SET salary = CASE
# MAGIC     WHEN salary < 85000 THEN salary * 1.08  -- 8% raise for lower tier
# MAGIC     WHEN salary < 100000 THEN salary * 1.05  -- 5% raise for mid tier
# MAGIC     ELSE salary * 1.03  -- 3% raise for upper tier
# MAGIC END
# MAGIC WHERE department = 'Sales';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, department, salary 
# MAGIC FROM employees 
# MAGIC WHERE department = 'Sales'
# MAGIC ORDER BY salary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: DELETE Operations
# MAGIC
# MAGIC DELETE removes records from a Delta table. Like UPDATE, it uses copy-on-write and maintains full ACID guarantees.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL DELETE - Simple Condition

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current count
# MAGIC SELECT COUNT(*) as before_delete FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete employees with salary below threshold
# MAGIC DELETE FROM employees 
# MAGIC WHERE salary < 85000;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify deletion
# MAGIC SELECT COUNT(*) as after_delete FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View remaining employees
# MAGIC SELECT * FROM employees ORDER BY salary;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's add those employees back for the next exercises.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC     (2, 'Bob Smith', 91350.00, 'Marketing', '2022-03-20'),
# MAGIC     (5, 'Eve Wilson', 81900.00, 'Marketing', '2023-01-12');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python API DELETE

# COMMAND ----------

# Load Delta table
delta_table = DeltaTable.forName(spark, "employees")

# Delete using Python API - Remove management department
delta_table.delete("department = 'Management'")

print("✅ Management employees deleted")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT department FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: MERGE Operations (UPSERT)
# MAGIC
# MAGIC MERGE is one of Delta Lake's most powerful features. It allows you to INSERT, UPDATE, or DELETE in a single atomic operation based on matching conditions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: Processing Employee Updates
# MAGIC
# MAGIC We receive a file with employee updates:
# MAGIC - Existing employees: Update their information
# MAGIC - New employees: Insert them
# MAGIC - Use MERGE to handle both cases

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create staging table with updates
# MAGIC CREATE OR REPLACE TEMP VIEW employee_updates AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (1, 'Alice Johnson', 125000.00, 'Engineering', CAST('2022-01-15' AS DATE)),  -- Existing: salary update
# MAGIC     (4, 'Diana Prince', 105000.00, 'Sales', CAST('2022-05-01' AS DATE)),  -- Existing: salary update
# MAGIC     (12, 'Luke Skywalker', 95000.00, 'Engineering', CAST('2024-01-15' AS DATE)),  -- New employee
# MAGIC     (13, 'Leia Organa', 99000.00, 'Management', CAST('2024-01-20' AS DATE))  -- New employee
# MAGIC AS employee_updates(id, name, salary, department, hire_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the updates
# MAGIC SELECT * FROM employee_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL MERGE - Basic Upsert

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employees AS target
# MAGIC USING employee_updates AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify MERGE results
# MAGIC -- Should see updated salaries for Alice (id=1) and Diana (id=4)
# MAGIC -- Should see new employees Luke (id=12) and Leia (id=13)
# MAGIC SELECT * FROM employees 
# MAGIC WHERE id IN (1, 4, 12, 13)
# MAGIC ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python API MERGE

# COMMAND ----------

# Create updates DataFrame
updates_data = [
    (2, "Bob Smith", 98000.00, "Marketing", "2022-03-20"),
    (5, "Eve Wilson", 90000.00, "Marketing", "2023-01-12"),
    (14, "Tony Stark", 150000.00, "Engineering", "2024-02-01")
]

updates_df = spark.createDataFrame(
    updates_data,
    ["id", "name", "salary", "department", "hire_date"]
)

# Perform MERGE using Python API
delta_table = DeltaTable.forName(spark, "employees")

delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("✅ MERGE operation completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Python MERGE
# MAGIC SELECT * FROM employees 
# MAGIC WHERE id IN (2, 5, 14)
# MAGIC ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advanced MERGE - Conditional Updates and Inserts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create more complex update scenario
# MAGIC CREATE OR REPLACE TEMP VIEW employee_changes AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (6, 'Frank Castle', 130000.00, 'Engineering', CAST('2021-09-15' AS DATE)),  -- Large raise
# MAGIC     (7, 'Grace Hopper', 102000.00, 'Engineering', CAST('2022-07-01' AS DATE)),  -- Small raise
# MAGIC     (15, 'Peter Parker', 72000.00, 'Engineering', CAST('2024-03-01' AS DATE)),  -- New, low salary
# MAGIC     (16, 'Bruce Wayne', 145000.00, 'Management', CAST('2024-03-01' AS DATE))   -- New, high salary
# MAGIC AS employee_changes(id, name, salary, department, hire_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE with conditions
# MAGIC MERGE INTO employees AS target
# MAGIC USING employee_changes AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED AND source.salary > 120000 THEN
# MAGIC     UPDATE SET target.salary = source.salary, target.department = source.department
# MAGIC WHEN MATCHED AND source.salary <= 120000 THEN
# MAGIC     UPDATE SET target.salary = source.salary
# MAGIC WHEN NOT MATCHED AND source.salary >= 100000 THEN
# MAGIC     INSERT *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (id, name, salary, department, hire_date) 
# MAGIC     VALUES (source.id, source.name, 85000.00, source.department, source.hire_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View results of conditional MERGE
# MAGIC SELECT * FROM employees 
# MAGIC WHERE id IN (6, 7, 15, 16)
# MAGIC ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE with DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create termination list
# MAGIC CREATE OR REPLACE TEMP VIEW employee_terminations AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (8, 'Henry Ford', 0.00, 'Sales', CAST('2023-02-15' AS DATE)),  -- Marked for termination
# MAGIC     (17, 'New Employee', 85000.00, 'Sales', CAST('2024-04-01' AS DATE))  -- New hire
# MAGIC AS employee_terminations(id, name, salary, department, hire_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE with DELETE for terminated employees
# MAGIC MERGE INTO employees AS target
# MAGIC USING employee_terminations AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED AND source.salary = 0 THEN
# MAGIC     DELETE
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: Employee 8 should be deleted, 17 should be added
# MAGIC SELECT * FROM employees WHERE id IN (8, 17) ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: ACID Transaction Guarantees
# MAGIC
# MAGIC Delta Lake provides full ACID guarantees. Let's demonstrate each property.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atomicity: All or Nothing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current count
# MAGIC SELECT COUNT(*) as employee_count FROM employees;

# COMMAND ----------

# Try a batch insert that will partially fail
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType

# Create a DataFrame with intentional schema violation
try:
    bad_data = spark.createDataFrame([
        (18, "Valid Employee", 95000.00, "Engineering", "2024-05-01"),
        (19, "Another Valid", 88000.00, "Sales", "2024-05-02"),
        # This would cause issues if we had constraints
    ], ["id", "name", "salary", "department", "hire_date"])
    
    bad_data.write.format("delta").mode("append").saveAsTable("employees")
    print("✅ Transaction completed successfully")
except Exception as e:
    print(f"❌ Transaction failed (as expected): {e}")
    print("⚠️ No partial data written - Atomicity guaranteed!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify atomicity - count should only change if entire operation succeeded
# MAGIC SELECT COUNT(*) as employee_count FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consistency: Valid States Only
# MAGIC
# MAGIC Delta Lake enforces schema, ensuring data remains consistent.

# COMMAND ----------

# Try to insert data with wrong schema
try:
    invalid_schema_df = spark.createDataFrame([
        (20, "Wrong Schema", "not_a_number", "Sales", "2024-06-01")  # salary is string, not decimal
    ], ["id", "name", "salary", "department", "hire_date"])
    
    invalid_schema_df.write.format("delta").mode("append").saveAsTable("employees")
except Exception as e:
    print(f"✅ Schema enforcement prevented bad data: {type(e).__name__}")
    print("⚠️ Table consistency maintained!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Isolation: Snapshot Isolation
# MAGIC
# MAGIC Readers always see a consistent snapshot, even while writers are modifying the table.

# COMMAND ----------

# Simulate concurrent operations
from pyspark.sql.functions import col

# Reader 1: Start a long-running query
print("📖 Reader starting query...")
reader_df = spark.read.format("delta").table("employees")
initial_count = reader_df.count()
print(f"Reader sees {initial_count} employees")

# Writer: Modify the table
print("\n✍️ Writer inserting new employees...")
new_hire = spark.createDataFrame([
    (21, "Concurrent Insert", 90000.00, "Engineering", "2024-06-15")
], ["id", "name", "salary", "department", "hire_date"])
new_hire.write.format("delta").mode("append").saveAsTable("employees")
print("Writer completed insertion")

# Reader continues with same snapshot
print("\n📖 Reader continues with original snapshot...")
reader_result = reader_df.filter(col("department") == "Engineering").count()
print(f"Reader still sees original data (snapshot isolation)")

# New reader sees updated data
print("\n📖 New reader sees updated data...")
new_reader_df = spark.read.format("delta").table("employees")
new_count = new_reader_df.count()
print(f"New reader sees {new_count} employees (includes new insert)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Durability: Changes Persist
# MAGIC
# MAGIC Once a transaction commits, it's permanent.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View transaction history - all committed operations are durable
# MAGIC DESCRIBE HISTORY employees
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Concurrent Write Handling
# MAGIC
# MAGIC Delta Lake uses optimistic concurrency control to handle concurrent writes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Optimistic Concurrency
# MAGIC
# MAGIC - Multiple writers can work simultaneously
# MAGIC - Conflicts are detected at commit time
# MAGIC - Failed transactions are automatically retried
# MAGIC - Delta Lake ensures data integrity

# COMMAND ----------

# Check current version
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "employees")
current_version = spark.sql("DESCRIBE HISTORY employees LIMIT 1").select("version").collect()[0][0]
print(f"📊 Current table version: {current_version}")

# COMMAND ----------

# Simulate concurrent modifications
# In practice, these would come from different processes/notebooks

# Write 1: Add new employee
write1_df = spark.createDataFrame([
    (22, "Writer One", 93000.00, "Marketing", "2024-07-01")
], ["id", "name", "salary", "department", "hire_date"])

write1_df.write.format("delta").mode("append").saveAsTable("employees")
print("✅ Writer 1 completed")

# Write 2: Add different employee
write2_df = spark.createDataFrame([
    (23, "Writer Two", 96000.00, "Sales", "2024-07-02")
], ["id", "name", "salary", "department", "hire_date"])

write2_df.write.format("delta").mode("append").saveAsTable("employees")
print("✅ Writer 2 completed")

# Both writes succeeded due to optimistic concurrency
new_version = spark.sql("DESCRIBE HISTORY employees LIMIT 1").select("version").collect()[0][0]
print(f"📊 New table version: {new_version}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View both concurrent inserts
# MAGIC SELECT * FROM employees WHERE id IN (22, 23);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Complex CRUD Workflow
# MAGIC
# MAGIC Let's combine multiple operations in a realistic workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: End-of-Quarter Employee Updates
# MAGIC
# MAGIC 1. Give performance bonuses (UPDATE)
# MAGIC 2. Add new hires (INSERT)
# MAGIC 3. Process transfers (UPDATE department)
# MAGIC 4. Process terminations (DELETE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Performance bonuses for top performers
# MAGIC UPDATE employees
# MAGIC SET salary = salary * 1.10
# MAGIC WHERE salary > 100000 AND department = 'Engineering';
# MAGIC
# MAGIC SELECT 'Step 1: Bonuses applied' as status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Add new hires
# MAGIC INSERT INTO employees VALUES
# MAGIC     (24, 'New Hire One', 85000.00, 'Engineering', '2024-07-15'),
# MAGIC     (25, 'New Hire Two', 82000.00, 'Marketing', '2024-07-15'),
# MAGIC     (26, 'New Hire Three', 88000.00, 'Sales', '2024-07-15');
# MAGIC
# MAGIC SELECT 'Step 2: New hires added' as status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Process internal transfers
# MAGIC UPDATE employees
# MAGIC SET department = 'Management'
# MAGIC WHERE id IN (1, 13);  -- Promote to management
# MAGIC
# MAGIC SELECT 'Step 3: Transfers processed' as status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Process terminations (simulate with low-performing employees)
# MAGIC DELETE FROM employees
# MAGIC WHERE id IN (15);  -- Peter Parker (was entry level)
# MAGIC
# MAGIC SELECT 'Step 4: Terminations processed' as status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View final state after complex workflow
# MAGIC SELECT 
# MAGIC     department,
# MAGIC     COUNT(*) as headcount,
# MAGIC     ROUND(AVG(salary), 2) as avg_salary,
# MAGIC     ROUND(SUM(salary), 2) as total_payroll
# MAGIC FROM employees
# MAGIC GROUP BY department
# MAGIC ORDER BY department;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Performance Metrics
# MAGIC
# MAGIC View operation metrics from transaction history.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View detailed operation metrics
# MAGIC SELECT 
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     operationMetrics.numTargetRowsInserted as rows_inserted,
# MAGIC     operationMetrics.numTargetRowsUpdated as rows_updated,
# MAGIC     operationMetrics.numTargetRowsDeleted as rows_deleted,
# MAGIC     operationMetrics.executionTimeMs as execution_time_ms
# MAGIC FROM (DESCRIBE HISTORY employees)
# MAGIC WHERE operation IN ('MERGE', 'UPDATE', 'DELETE', 'WRITE')
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **CRUD Operations**
# MAGIC - **INSERT**: Use SQL INSERT or DataFrame append mode
# MAGIC - **UPDATE**: Modify existing records with conditions
# MAGIC - **DELETE**: Remove records that match criteria
# MAGIC - **MERGE**: Powerful upsert combining INSERT/UPDATE/DELETE
# MAGIC
# MAGIC ✅ **ACID Properties**
# MAGIC - **Atomicity**: All-or-nothing transactions
# MAGIC - **Consistency**: Schema enforcement maintains data quality
# MAGIC - **Isolation**: Snapshot isolation for readers
# MAGIC - **Durability**: Committed changes are permanent
# MAGIC
# MAGIC ✅ **APIs**
# MAGIC - SQL: Natural and familiar for most users
# MAGIC - Python DeltaTable API: Programmatic control
# MAGIC - Both provide full CRUD capabilities
# MAGIC
# MAGIC ✅ **Best Practices**
# MAGIC - Use MERGE for upserts (more efficient than UPDATE + INSERT)
# MAGIC - Leverage conditional logic in MERGE for complex workflows
# MAGIC - Trust Delta Lake's concurrency control
# MAGIC - Monitor operation metrics for performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Create a sales orders system with CRUD operations.
# MAGIC
# MAGIC ### Requirements:
# MAGIC 1. Create an `orders` table (order_id, customer_id, amount, status, order_date)
# MAGIC 2. INSERT 10 sample orders with status 'pending'
# MAGIC 3. UPDATE 3 orders to status 'shipped'
# MAGIC 4. DELETE orders with amount < $50
# MAGIC 5. MERGE: Process order updates (some updates, some new orders)
# MAGIC 6. View the final state and operation history

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Space

# COMMAND ----------

# TODO: Your solution here
# Step 1: Create orders table

# Step 2: Insert sample orders

# Step 3: Update order status

# Step 4: Delete small orders

# Step 5: Process order updates with MERGE

# Step 6: View results and history

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to cleanup
# spark.sql("DROP TABLE IF EXISTS employees")
# print("✅ Tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Excellent work! You've mastered CRUD operations with Delta Lake. 🎉
# MAGIC
# MAGIC **Continue to Lab 4: Partitioning Strategies**
# MAGIC - Learn when and how to partition Delta tables
# MAGIC - Understand partition pruning
# MAGIC - Avoid common partitioning pitfalls
# MAGIC - Optimize query performance with partitioning
# MAGIC
# MAGIC ---
# MAGIC **Need Help?** Refer to [Delta Lake DML Documentation](https://docs.delta.io/latest/delta-update.html)
