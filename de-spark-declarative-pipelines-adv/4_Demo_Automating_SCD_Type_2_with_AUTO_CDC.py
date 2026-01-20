# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Automating SCD Type 2 with AUTO CDC in Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demonstration shows how to implement SCD (Slowly Changing Dimension) Type 2 using the AUTO CDC INTO syntax in Lakeflow Spark Declarative Pipelines. You'll learn how to automatically track historical changes, manage effective dates, and query point-in-time data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Implement AUTO CDC INTO for SCD Type 2 patterns
# MAGIC - Handle INSERT, UPDATE, and DELETE operations automatically
# MAGIC - Use TRACK HISTORY ON to select which columns trigger new versions
# MAGIC - Query historical data using system-generated columns
# MAGIC - Combine SCD Type 1 and Type 2 in a single pipeline
# MAGIC - Apply best practices for production CDC pipelines
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with Serverless V4 compute enabled
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Completion of the CDC Review lecture
# MAGIC
# MAGIC ## Duration
# MAGIC 35-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# DBTITLE 1,Configure Catalog and Schema
# Set your catalog name here
catalog_name = "YOUR_CATALOG"
schema_name = "cdc_demo"

# Create the schema if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Using catalog: {catalog_name}")
print(f"Using schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario: Employee Management System
# MAGIC
# MAGIC We're building an employee data pipeline that needs to track:
# MAGIC - **Historical changes**: Promotions, department transfers, salary adjustments
# MAGIC - **Current state**: Latest employee information
# MAGIC - **Audit trail**: Who changed what and when
# MAGIC
# MAGIC ### Tracked Attributes (SCD Type 2):
# MAGIC - Job Title
# MAGIC - Department
# MAGIC - Salary
# MAGIC - Manager
# MAGIC
# MAGIC ### Non-Tracked Attributes (SCD Type 1):
# MAGIC - Email (corrections only)
# MAGIC - Phone (corrections only)
# MAGIC - Office Location (current only)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Initial Employee Data

# COMMAND ----------

# DBTITLE 1,Create Initial Employee Records
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from datetime import datetime, timedelta
import random

# Define schema
employee_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("job_title", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", DecimalType(10, 2), False),
    StructField("manager_id", IntegerType(), True),
    StructField("office_location", StringType(), True),
    StructField("hire_date", StringType(), False),
    StructField("operation", StringType(), False),  # I, U, D
    StructField("sequence_num", IntegerType(), False),
    StructField("change_timestamp", TimestampType(), False)
])

# Create initial employee data (all inserts)
initial_employees = [
    (1001, "Alice", "Johnson", "alice.johnson@company.com", "555-0101", "Senior Engineer", "Engineering", 120000.00, 1005, "Building A", "2020-01-15", "I", 1, datetime(2020, 1, 15, 9, 0, 0)),
    (1002, "Bob", "Smith", "bob.smith@company.com", "555-0102", "Data Analyst", "Analytics", 85000.00, 1006, "Building A", "2021-03-20", "I", 2, datetime(2021, 3, 20, 9, 0, 0)),
    (1003, "Carol", "Williams", "carol.williams@company.com", "555-0103", "Product Manager", "Product", 110000.00, 1007, "Building B", "2019-06-10", "I", 3, datetime(2019, 6, 10, 9, 0, 0)),
    (1004, "David", "Brown", "david.brown@company.com", "555-0104", "Marketing Specialist", "Marketing", 75000.00, 1008, "Building B", "2022-01-05", "I", 4, datetime(2022, 1, 5, 9, 0, 0)),
    (1005, "Eva", "Davis", "eva.davis@company.com", "555-0105", "Engineering Manager", "Engineering", 150000.00, None, "Building A", "2018-04-12", "I", 5, datetime(2018, 4, 12, 9, 0, 0)),
    (1006, "Frank", "Miller", "frank.miller@company.com", "555-0106", "Analytics Manager", "Analytics", 140000.00, None, "Building A", "2019-08-22", "I", 6, datetime(2019, 8, 22, 9, 0, 0)),
    (1007, "Grace", "Wilson", "grace.wilson@company.com", "555-0107", "Product Director", "Product", 180000.00, None, "Building B", "2017-11-30", "I", 7, datetime(2017, 11, 30, 9, 0, 0)),
    (1008, "Henry", "Moore", "henry.moore@company.com", "555-0108", "Marketing Director", "Marketing", 175000.00, None, "Building B", "2018-02-14", "I", 8, datetime(2018, 2, 14, 9, 0, 0)),
]

initial_df = spark.createDataFrame(initial_employees, schema=employee_schema)
initial_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.employee_changes")

print(f"Created initial employee data with {initial_df.count()} records")
initial_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the SCD Type 2 Target Table
# MAGIC
# MAGIC We'll create a streaming table that will serve as our target for AUTO CDC.

# COMMAND ----------

# DBTITLE 1,Create Target Table for SCD Type 2
# This would normally be done in the Lakeflow pipeline, but we'll create it here for demo purposes
create_target_sql = f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.employee_history (
  employee_id INT NOT NULL,
  first_name STRING NOT NULL,
  last_name STRING NOT NULL,
  email STRING NOT NULL,
  phone STRING,
  job_title STRING NOT NULL,
  department STRING NOT NULL,
  salary DECIMAL(10,2) NOT NULL,
  manager_id INT,
  office_location STRING,
  hire_date STRING NOT NULL
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
"""

spark.sql(create_target_sql)
print("Created employee_history target table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Understanding the AUTO CDC INTO Syntax
# MAGIC
# MAGIC Here's the Lakeflow Declarative Pipeline code you would use in the multi-file editor:
# MAGIC
# MAGIC ```sql
# MAGIC -- employee_history.sql
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE employee_history;
# MAGIC
# MAGIC AUTO CDC INTO
# MAGIC   LIVE.employee_history
# MAGIC FROM
# MAGIC   STREAM(LIVE.employee_changes)
# MAGIC KEYS
# MAGIC   (employee_id)
# MAGIC SEQUENCE BY
# MAGIC   sequence_num
# MAGIC TRACK HISTORY ON
# MAGIC   (job_title, department, salary, manager_id)
# MAGIC STORED AS
# MAGIC   SCD TYPE 2;
# MAGIC ```
# MAGIC
# MAGIC ### What This Does:
# MAGIC
# MAGIC 1. **KEYS (employee_id)**: Uses employee_id as the business key to match records
# MAGIC 2. **SEQUENCE BY sequence_num**: Orders changes by sequence number
# MAGIC 3. **TRACK HISTORY ON**: Only these columns trigger new versions:
# MAGIC    - Changes to job_title, department, salary, or manager_id → new row
# MAGIC    - Changes to email, phone, office_location → update in place (Type 1)
# MAGIC 4. **STORED AS SCD TYPE 2**: Enables automatic history management
# MAGIC
# MAGIC ### System-Generated Columns:
# MAGIC - `__start_at`: When this version became effective
# MAGIC - `__end_at`: When this version was superseded (NULL for current)
# MAGIC - `__is_current`: Boolean flag for current version
# MAGIC - `__sequence_num`: The sequence value from source
# MAGIC - `__deleted`: Boolean flag for deleted records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Simulate the AUTO CDC Process
# MAGIC
# MAGIC Since we can't run an actual Lakeflow pipeline in this notebook, we'll simulate what AUTO CDC does behind the scenes. This helps you understand the mechanism.

# COMMAND ----------

# DBTITLE 1,Simulate AUTO CDC Logic (Initial Load)
from pyspark.sql.functions import col, lit, current_timestamp, when

# Read initial changes
changes_df = spark.table(f"{catalog_name}.{schema_name}.employee_changes")

# Simulate AUTO CDC processing for initial load (all inserts)
processed_df = changes_df.select(
    col("employee_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("phone"),
    col("job_title"),
    col("department"),
    col("salary"),
    col("manager_id"),
    col("office_location"),
    col("hire_date"),
    col("change_timestamp").alias("__start_at"),
    lit(None).cast("timestamp").alias("__end_at"),
    lit(True).alias("__is_current"),
    col("sequence_num").alias("__sequence_num"),
    lit(False).alias("__deleted")
)

# Write to target table
processed_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.employee_history")

print("Initial load complete")
spark.table(f"{catalog_name}.{schema_name}.employee_history").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Change Events
# MAGIC
# MAGIC Now let's simulate various types of changes:
# MAGIC 1. **Promotion**: Alice gets promoted to Lead Engineer with salary increase
# MAGIC 2. **Department Transfer**: Bob moves to Engineering department
# MAGIC 3. **Email Update**: Carol's email changes (typo correction)
# MAGIC 4. **Manager Change**: David gets a new manager
# MAGIC 5. **Office Move**: Eva moves to Building B
# MAGIC 6. **Employee Departure**: Frank leaves the company

# COMMAND ----------

# DBTITLE 1,Create Change Events
# Change events (sequence continues from 9)
change_events = [
    # Event 9: Alice gets promoted (TRACKED - creates new version)
    (1001, "Alice", "Johnson", "alice.johnson@company.com", "555-0101", "Lead Engineer", "Engineering", 135000.00, 1005, "Building A", "2020-01-15", "U", 9, datetime(2024, 1, 15, 10, 30, 0)),
    
    # Event 10: Bob transfers departments (TRACKED - creates new version)
    (1002, "Bob", "Smith", "bob.smith@company.com", "555-0102", "Data Engineer", "Engineering", 95000.00, 1005, "Building A", "2021-03-20", "U", 10, datetime(2024, 2, 1, 14, 0, 0)),
    
    # Event 11: Carol's email correction (NOT TRACKED - updates in place)
    (1003, "Carol", "Williams", "carol.w@company.com", "555-0103", "Product Manager", "Product", 110000.00, 1007, "Building B", "2019-06-10", "U", 11, datetime(2024, 2, 15, 9, 15, 0)),
    
    # Event 12: David gets new manager (TRACKED - creates new version)
    (1004, "David", "Brown", "david.brown@company.com", "555-0104", "Marketing Specialist", "Marketing", 75000.00, 1007, "Building B", "2022-01-05", "U", 12, datetime(2024, 3, 1, 11, 0, 0)),
    
    # Event 13: Eva moves office (NOT TRACKED - updates in place)
    (1005, "Eva", "Davis", "eva.davis@company.com", "555-0105", "Engineering Manager", "Engineering", 150000.00, None, "Building B", "2018-04-12", "U", 13, datetime(2024, 3, 10, 16, 30, 0)),
    
    # Event 14: Frank leaves company (DELETE)
    (1006, "Frank", "Miller", "frank.miller@company.com", "555-0106", "Analytics Manager", "Analytics", 140000.00, None, "Building A", "2019-08-22", "D", 14, datetime(2024, 3, 20, 17, 0, 0)),
    
    # Event 15: Alice gets another promotion (TRACKED - creates another new version)
    (1001, "Alice", "Johnson", "alice.johnson@company.com", "555-0101", "Principal Engineer", "Engineering", 155000.00, 1005, "Building A", "2020-01-15", "U", 15, datetime(2024, 6, 1, 10, 0, 0)),
]

changes_df = spark.createDataFrame(change_events, schema=employee_schema)
changes_df.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.{schema_name}.employee_changes")

print(f"Added {changes_df.count()} change events")
changes_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Process Changes with AUTO CDC Logic

# COMMAND ----------

# DBTITLE 1,Simulate AUTO CDC Processing of Changes
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, max as spark_max, lead

# Columns that trigger new versions (TRACK HISTORY ON)
tracked_columns = ['job_title', 'department', 'salary', 'manager_id']

# Read current state
current_state = spark.table(f"{catalog_name}.{schema_name}.employee_history")

# Read new changes
all_changes = spark.table(f"{catalog_name}.{schema_name}.employee_changes")

# For this demo, we'll manually process each change type
# In a real Lakeflow pipeline, AUTO CDC does this automatically

# Process each change event
for change_row in change_events:
    employee_id = change_row[0]
    operation = change_row[11]
    sequence_num = change_row[12]
    change_ts = change_row[13]
    
    if operation == "U":  # UPDATE
        # Check if any tracked columns changed
        tracked_changed = False
        current_record = current_state.filter(
            (col("employee_id") == employee_id) & (col("__is_current") == True)
        ).first()
        
        if current_record:
            # Check each tracked column
            for i, col_name in enumerate(['job_title', 'department', 'salary', 'manager_id']):
                old_val = current_record[col_name]
                new_val = change_row[5 + i] if col_name == 'job_title' else \
                         change_row[6] if col_name == 'department' else \
                         change_row[7] if col_name == 'salary' else \
                         change_row[8]
                if old_val != new_val:
                    tracked_changed = True
                    break
            
            if tracked_changed:
                # Close current record
                current_state = current_state.withColumn(
                    "__end_at",
                    when((col("employee_id") == employee_id) & (col("__is_current") == True), lit(change_ts))
                    .otherwise(col("__end_at"))
                ).withColumn(
                    "__is_current",
                    when((col("employee_id") == employee_id) & (col("__is_current") == True), lit(False))
                    .otherwise(col("__is_current"))
                )
                
                # Insert new version
                new_version = spark.createDataFrame([change_row], schema=employee_schema).select(
                    col("employee_id"),
                    col("first_name"),
                    col("last_name"),
                    col("email"),
                    col("phone"),
                    col("job_title"),
                    col("department"),
                    col("salary"),
                    col("manager_id"),
                    col("office_location"),
                    col("hire_date"),
                    col("change_timestamp").alias("__start_at"),
                    lit(None).cast("timestamp").alias("__end_at"),
                    lit(True).alias("__is_current"),
                    col("sequence_num").alias("__sequence_num"),
                    lit(False).alias("__deleted")
                )
                current_state = current_state.union(new_version)
            else:
                # Update in place (Type 1)
                current_state = current_state.withColumn(
                    "email",
                    when((col("employee_id") == employee_id) & (col("__is_current") == True), lit(change_row[3]))
                    .otherwise(col("email"))
                ).withColumn(
                    "office_location",
                    when((col("employee_id") == employee_id) & (col("__is_current") == True), lit(change_row[9]))
                    .otherwise(col("office_location"))
                )
    
    elif operation == "D":  # DELETE
        # Soft delete: close record and mark as deleted
        current_state = current_state.withColumn(
            "__end_at",
            when((col("employee_id") == employee_id) & (col("__is_current") == True), lit(change_ts))
            .otherwise(col("__end_at"))
        ).withColumn(
            "__is_current",
            when((col("employee_id") == employee_id) & (col("__is_current") == True), lit(False))
            .otherwise(col("__is_current"))
        ).withColumn(
            "__deleted",
            when((col("employee_id") == employee_id) & (col("__is_current") == False) & (col("__end_at") == change_ts), lit(True))
            .otherwise(col("__deleted"))
        )

# Save updated state
current_state.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.employee_history")

print("CDC processing complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Query Current State
# MAGIC
# MAGIC Let's query the current active employees.

# COMMAND ----------

# DBTITLE 1,View Current Employee Records
current_employees = spark.sql(f"""
SELECT 
  employee_id,
  first_name,
  last_name,
  email,
  job_title,
  department,
  salary,
  manager_id,
  office_location,
  __start_at as effective_from,
  __is_current as is_current,
  __deleted as is_deleted
FROM {catalog_name}.{schema_name}.employee_history
WHERE __is_current = true AND __deleted = false
ORDER BY employee_id
""")

print("Current Active Employees:")
current_employees.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Query Historical Data
# MAGIC
# MAGIC Now let's explore the historical changes.

# COMMAND ----------

# DBTITLE 1,View Complete History for Alice (Employee 1001)
alice_history = spark.sql(f"""
SELECT 
  employee_id,
  first_name,
  last_name,
  job_title,
  department,
  salary,
  __start_at as effective_from,
  __end_at as effective_to,
  __is_current as is_current,
  __sequence_num as sequence
FROM {catalog_name}.{schema_name}.employee_history
WHERE employee_id = 1001
ORDER BY __start_at
""")

print("Alice's Employment History (showing promotions):")
alice_history.display()

# COMMAND ----------

# DBTITLE 1,Track Salary Changes Over Time
salary_history = spark.sql(f"""
SELECT 
  employee_id,
  first_name,
  last_name,
  job_title,
  salary,
  __start_at as effective_from,
  LAG(salary) OVER (PARTITION BY employee_id ORDER BY __start_at) as previous_salary,
  salary - LAG(salary) OVER (PARTITION BY employee_id ORDER BY __start_at) as salary_increase
FROM {catalog_name}.{schema_name}.employee_history
WHERE employee_id IN (1001, 1002)
ORDER BY employee_id, __start_at
""")

print("Salary Change History:")
salary_history.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Point-in-Time Queries
# MAGIC
# MAGIC Query the state of employees at a specific point in time.

# COMMAND ----------

# DBTITLE 1,Employee State on March 1, 2024
point_in_time = spark.sql(f"""
SELECT 
  employee_id,
  first_name,
  last_name,
  job_title,
  department,
  salary,
  __start_at as effective_from,
  __end_at as effective_to
FROM {catalog_name}.{schema_name}.employee_history
WHERE __start_at <= '2024-03-01'
  AND (__end_at > '2024-03-01' OR __end_at IS NULL)
  AND __deleted = false
ORDER BY employee_id
""")

print("Employee State as of March 1, 2024:")
point_in_time.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Analyze Change Patterns

# COMMAND ----------

# DBTITLE 1,Count Changes by Employee
change_counts = spark.sql(f"""
SELECT 
  employee_id,
  first_name,
  last_name,
  COUNT(*) - 1 as number_of_changes,
  MIN(__start_at) as first_record,
  MAX(__start_at) as latest_change
FROM {catalog_name}.{schema_name}.employee_history
GROUP BY employee_id, first_name, last_name
HAVING COUNT(*) > 1
ORDER BY number_of_changes DESC
""")

print("Employees with Multiple Versions (Change History):")
change_counts.display()

# COMMAND ----------

# DBTITLE 1,Identify Department Transfers
transfers = spark.sql(f"""
SELECT 
  employee_id,
  first_name,
  last_name,
  LAG(department) OVER (PARTITION BY employee_id ORDER BY __start_at) as from_department,
  department as to_department,
  __start_at as transfer_date
FROM {catalog_name}.{schema_name}.employee_history
WHERE employee_id IN (
  SELECT employee_id 
  FROM {catalog_name}.{schema_name}.employee_history 
  GROUP BY employee_id 
  HAVING COUNT(DISTINCT department) > 1
)
ORDER BY employee_id, __start_at
""")

print("Department Transfer History:")
transfers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Handle Deleted Records

# COMMAND ----------

# DBTITLE 1,View Deleted Employees
deleted_employees = spark.sql(f"""
SELECT 
  employee_id,
  first_name,
  last_name,
  job_title,
  department,
  salary,
  __start_at as employed_from,
  __end_at as departed_on,
  __deleted as is_deleted
FROM {catalog_name}.{schema_name}.employee_history
WHERE __deleted = true
ORDER BY __end_at DESC
""")

print("Departed Employees:")
deleted_employees.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Create Useful Views

# COMMAND ----------

# DBTITLE 1,Create Current State View
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.v_current_employees AS
SELECT 
  employee_id,
  first_name,
  last_name,
  email,
  phone,
  job_title,
  department,
  salary,
  manager_id,
  office_location,
  hire_date,
  __start_at as current_since
FROM {catalog_name}.{schema_name}.employee_history
WHERE __is_current = true AND __deleted = false
""")

print("Created view: v_current_employees")

# COMMAND ----------

# DBTITLE 1,Create Audit Trail View
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.v_employee_audit_trail AS
SELECT 
  employee_id,
  first_name,
  last_name,
  job_title,
  department,
  salary,
  manager_id,
  __start_at as effective_from,
  __end_at as effective_to,
  __sequence_num as change_sequence,
  CASE 
    WHEN __deleted THEN 'DEPARTED'
    WHEN __is_current THEN 'CURRENT'
    ELSE 'HISTORICAL'
  END as record_status,
  DATEDIFF(__end_at, __start_at) as days_in_version
FROM {catalog_name}.{schema_name}.employee_history
ORDER BY employee_id, __start_at
""")

print("Created view: v_employee_audit_trail")

# Query the audit trail
spark.table(f"{catalog_name}.{schema_name}.v_employee_audit_trail").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Demonstrated
# MAGIC
# MAGIC ### 1. Selective History Tracking
# MAGIC - Used `TRACK HISTORY ON` for job_title, department, salary, manager_id
# MAGIC - Email and office updates don't create new versions (Type 1 behavior)
# MAGIC - Reduces storage and improves query performance
# MAGIC
# MAGIC ### 2. Proper Sequencing
# MAGIC - Used monotonically increasing sequence numbers
# MAGIC - Ensures changes are applied in correct order
# MAGIC - Critical for handling late-arriving data
# MAGIC
# MAGIC ### 3. Soft Deletes
# MAGIC - Deleted records marked with `__deleted = true`
# MAGIC - Historical data preserved for audit
# MAGIC - Can restore or analyze departed employees
# MAGIC
# MAGIC ### 4. Useful Views
# MAGIC - Current state view for operational queries
# MAGIC - Audit trail view for compliance reporting
# MAGIC - Simplified access to complex historical data
# MAGIC
# MAGIC ### 5. Point-in-Time Queries
# MAGIC - Reconstruct state at any past date
# MAGIC - Essential for auditing and analysis
# MAGIC - Uses effective date ranges

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Query Patterns

# COMMAND ----------

# DBTITLE 1,Query Pattern Examples
query_patterns = """
-- Pattern 1: Get current active employees
SELECT * FROM employee_history 
WHERE __is_current = true AND __deleted = false;

-- Pattern 2: Get all versions of a specific employee
SELECT * FROM employee_history 
WHERE employee_id = 1001 
ORDER BY __start_at;

-- Pattern 3: Point-in-time query (state at specific date)
SELECT * FROM employee_history
WHERE __start_at <= '2024-03-01'
  AND (__end_at > '2024-03-01' OR __end_at IS NULL)
  AND __deleted = false;

-- Pattern 4: Find employees who changed departments
SELECT DISTINCT employee_id, first_name, last_name
FROM employee_history
GROUP BY employee_id, first_name, last_name
HAVING COUNT(DISTINCT department) > 1;

-- Pattern 5: Calculate tenure in each role
SELECT 
  employee_id,
  job_title,
  department,
  __start_at,
  COALESCE(__end_at, CURRENT_TIMESTAMP) as end_time,
  DATEDIFF(COALESCE(__end_at, CURRENT_DATE), __start_at) as days_in_role
FROM employee_history
ORDER BY employee_id, __start_at;

-- Pattern 6: Identify salary increases over time
SELECT 
  employee_id,
  first_name,
  last_name,
  salary,
  LAG(salary) OVER (PARTITION BY employee_id ORDER BY __start_at) as previous_salary,
  __start_at as effective_date
FROM employee_history
WHERE LAG(salary) OVER (PARTITION BY employee_id ORDER BY __start_at) IS NOT NULL
  AND salary > LAG(salary) OVER (PARTITION BY employee_id ORDER BY __start_at);
"""

print("Common Query Patterns for SCD Type 2 Tables:")
print(query_patterns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Lakeflow Pipeline Example
# MAGIC
# MAGIC Here's the complete pipeline you would create in the Lakeflow multi-file editor:
# MAGIC
# MAGIC ### File 1: employee_changes.sql (Source)
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE employee_changes
# MAGIC AS SELECT *
# MAGIC FROM cloud_files(
# MAGIC   '/path/to/employee/changes/',
# MAGIC   'json',
# MAGIC   map(
# MAGIC     'cloudFiles.schemaLocation', '${schema_location}/employee_changes',
# MAGIC     'cloudFiles.inferColumnTypes', 'true'
# MAGIC   )
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### File 2: employee_history.sql (SCD Type 2)
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE employee_history;
# MAGIC
# MAGIC AUTO CDC INTO
# MAGIC   LIVE.employee_history
# MAGIC FROM
# MAGIC   STREAM(LIVE.employee_changes)
# MAGIC KEYS
# MAGIC   (employee_id)
# MAGIC SEQUENCE BY
# MAGIC   sequence_num
# MAGIC TRACK HISTORY ON
# MAGIC   (job_title, department, salary, manager_id)
# MAGIC STORED AS
# MAGIC   SCD TYPE 2;
# MAGIC ```
# MAGIC
# MAGIC ### File 3: v_current_employees.sql (View)
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW v_current_employees
# MAGIC AS SELECT 
# MAGIC   employee_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   phone,
# MAGIC   job_title,
# MAGIC   department,
# MAGIC   salary,
# MAGIC   manager_id,
# MAGIC   office_location,
# MAGIC   hire_date
# MAGIC FROM LIVE.employee_history
# MAGIC WHERE __is_current = true AND __deleted = false;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **AUTO CDC INTO** automates SCD Type 2 implementation
# MAGIC 2. **TRACK HISTORY ON** provides fine-grained control over which columns create versions
# MAGIC 3. System-generated columns (**__start_at**, **__end_at**, **__is_current**) simplify queries
# MAGIC 4. Soft deletes preserve audit trail with **__deleted** flag
# MAGIC 5. Combine Type 1 and Type 2 in the same table for flexibility
# MAGIC 6. Point-in-time queries enable historical analysis
# MAGIC 7. Create views to simplify access to current and historical data
# MAGIC
# MAGIC ## Production Considerations
# MAGIC
# MAGIC - **Performance**: Use Liquid Clustering on frequently queried columns
# MAGIC - **Storage**: Archive old versions based on retention policies
# MAGIC - **Monitoring**: Track pipeline metrics and data quality
# MAGIC - **Testing**: Validate all CDC operations (INSERT, UPDATE, DELETE)
# MAGIC - **Documentation**: Document which columns are tracked and why
# MAGIC - **Access Control**: Apply appropriate Unity Catalog permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# DBTITLE 1,Cleanup Demo Resources
cleanup_mode = False  # Set to True to run cleanup

if cleanup_mode:
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.employee_changes")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.employee_history")
    spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.{schema_name}.v_current_employees")
    spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.{schema_name}.v_employee_audit_trail")
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")
    print("Cleanup complete!")
else:
    print("Cleanup mode is disabled. Set cleanup_mode = True to run cleanup.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've completed the Advanced Techniques for Lakeflow Declarative Pipelines workshop series. You now know how to:
# MAGIC
# MAGIC - Build multi-flow data pipelines with diverse sources
# MAGIC - Implement data quality expectations with multiple violation actions
# MAGIC - Use Liquid Clustering for query optimization
# MAGIC - Understand and implement CDC patterns
# MAGIC - Automate SCD Type 2 with AUTO CDC INTO
# MAGIC - Query and analyze historical data
# MAGIC - Apply best practices for production pipelines
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - Build your own Lakeflow pipelines with these patterns
# MAGIC - Explore advanced data quality rules
# MAGIC - Implement monitoring and alerting
# MAGIC - Check out the [Databricks Academy course](https://home.databricks.com/fieldeng/fe-technical-enablement/field-engineering-hands-on-labs/)
