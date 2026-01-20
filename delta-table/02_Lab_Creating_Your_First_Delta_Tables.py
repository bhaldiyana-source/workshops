# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Creating Your First Delta Tables (Small Datasets)
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll create your first Delta Lake tables using small datasets (1-10 GB). You'll learn different write modes, explore managed vs. external tables, and master querying Delta tables using both SQL and DataFrame APIs.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Create Delta tables from DataFrames and SQL
# MAGIC - Understand and apply different write modes (append, overwrite, merge)
# MAGIC - Differentiate between managed and external Delta tables
# MAGIC - Configure table properties for optimal performance
# MAGIC - Query Delta tables using SQL and DataFrame API
# MAGIC - View and interpret table metadata and history
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Module 1 (Delta Lake Architecture and Fundamentals)
# MAGIC - Basic SQL and PySpark knowledge
# MAGIC - Access to Databricks workspace with Unity Catalog
# MAGIC
# MAGIC ## Duration
# MAGIC 35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Database
# MAGIC
# MAGIC We'll create a dedicated database for this lab to keep our tables organized.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create database for this lab
# MAGIC CREATE DATABASE IF NOT EXISTS delta_lab_db
# MAGIC COMMENT 'Database for Delta Lake labs';
# MAGIC
# MAGIC -- Set as default
# MAGIC USE delta_lab_db;
# MAGIC
# MAGIC -- Verify we're in the right database
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Create Delta Table from DataFrame
# MAGIC
# MAGIC Let's start with the most common pattern: creating a Delta table from a DataFrame.
# MAGIC
# MAGIC ### Scenario: Employee Database
# MAGIC We're building an employee management system and need to store employee records.

# COMMAND ----------

# Create sample employee data
from pyspark.sql import Row

employees_data = [
    (1, "Alice Johnson", 95000, "Engineering", "alice@company.com"),
    (2, "Bob Smith", 87000, "Marketing", "bob@company.com"),
    (3, "Charlie Brown", 105000, "Engineering", "charlie@company.com"),
    (4, "Diana Prince", 92000, "Sales", "diana@company.com"),
    (5, "Eve Wilson", 78000, "Marketing", "eve@company.com"),
    (6, "Frank Castle", 115000, "Engineering", "frank@company.com"),
    (7, "Grace Hopper", 98000, "Engineering", "grace@company.com"),
    (8, "Henry Ford", 88000, "Sales", "henry@company.com")
]

# Create DataFrame with schema
df_employees = spark.createDataFrame(
    employees_data,
    ["id", "name", "salary", "department", "email"]
)

# Display the DataFrame
display(df_employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write DataFrame as Delta Table (Mode: Overwrite)
# MAGIC
# MAGIC The `overwrite` mode replaces any existing data in the table.

# COMMAND ----------

# Write DataFrame as managed Delta table
df_employees.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("employees")

print("✅ Delta table 'employees' created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the Delta Table
# MAGIC
# MAGIC Let's verify our table was created and query it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query all employees
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query with filter
# MAGIC SELECT name, salary, department
# MAGIC FROM employees
# MAGIC WHERE department = 'Engineering'
# MAGIC ORDER BY salary DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Different Write Modes
# MAGIC
# MAGIC Delta Lake supports several write modes. Let's explore each one.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 1: Append
# MAGIC
# MAGIC Add new records without affecting existing data.

# COMMAND ----------

# Create new employee records
new_employees = [
    (9, "Ivy Chen", 102000, "Engineering", "ivy@company.com"),
    (10, "Jack Ryan", 91000, "Sales", "jack@company.com")
]

df_new = spark.createDataFrame(
    new_employees,
    ["id", "name", "salary", "department", "email"]
)

# Append to existing table
df_new.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("employees")

print("✅ New employees appended!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify append - should now have 10 employees
# MAGIC SELECT COUNT(*) as total_employees FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the new employees
# MAGIC SELECT * FROM employees WHERE id IN (9, 10);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mode 2: Overwrite
# MAGIC
# MAGIC Replace all existing data with new data.

# COMMAND ----------

# Create a smaller dataset
small_dataset = [
    (1, "Alice Johnson", 98000, "Engineering", "alice@company.com"),
    (2, "Bob Smith", 90000, "Marketing", "bob@company.com")
]

df_small = spark.createDataFrame(
    small_dataset,
    ["id", "name", "salary", "department", "email"]
)

# Overwrite the table
df_small.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("employees_temp")

print("✅ Table overwritten with smaller dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify - should only have 2 employees
# MAGIC SELECT * FROM employees_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's restore our original employees table for the remaining exercises.

# COMMAND ----------

# Restore original data
df_employees.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("employees")

df_new.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("employees")

print("✅ Employees table restored")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Create Delta Table with SQL
# MAGIC
# MAGIC You can also create Delta tables using pure SQL syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create products table with SQL
# MAGIC CREATE TABLE IF NOT EXISTS products (
# MAGIC     product_id INT,
# MAGIC     product_name STRING,
# MAGIC     price DECIMAL(10,2),
# MAGIC     category STRING,
# MAGIC     created_date DATE,
# MAGIC     in_stock BOOLEAN
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Product catalog table';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data into products table
# MAGIC INSERT INTO products VALUES
# MAGIC     (101, 'Laptop Pro 15', 1299.99, 'Electronics', '2024-01-15', true),
# MAGIC     (102, 'Wireless Mouse', 29.99, 'Electronics', '2024-01-16', true),
# MAGIC     (103, 'Office Chair', 249.99, 'Furniture', '2024-01-17', true),
# MAGIC     (104, 'Standing Desk', 499.99, 'Furniture', '2024-01-18', false),
# MAGIC     (105, 'USB-C Cable', 12.99, 'Electronics', '2024-01-19', true),
# MAGIC     (106, 'Monitor 27"', 349.99, 'Electronics', '2024-01-20', true),
# MAGIC     (107, 'Keyboard Mechanical', 89.99, 'Electronics', '2024-01-21', true),
# MAGIC     (108, 'Desk Lamp', 39.99, 'Furniture', '2024-01-22', true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query products
# MAGIC SELECT * FROM products ORDER BY product_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analytics query
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) as product_count,
# MAGIC     AVG(price) as avg_price,
# MAGIC     SUM(CASE WHEN in_stock THEN 1 ELSE 0 END) as in_stock_count
# MAGIC FROM products
# MAGIC GROUP BY category
# MAGIC ORDER BY category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Managed vs. External Tables
# MAGIC
# MAGIC Understanding the difference between managed and external tables is crucial.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Tables
# MAGIC
# MAGIC - **Databricks manages both data and metadata**
# MAGIC - Dropping the table deletes both data and metadata
# MAGIC - Stored in default location (Unity Catalog managed storage)
# MAGIC - Recommended for most use cases

# COMMAND ----------

# Create managed table (default behavior)
df_employees.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("employees_managed")

print("✅ Managed table created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table details
# MAGIC DESCRIBE EXTENDED employees_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ### External Tables
# MAGIC
# MAGIC - **You manage data location**
# MAGIC - Dropping the table only removes metadata, data remains
# MAGIC - Useful for sharing data across workspaces
# MAGIC - Required when data location must be specific

# COMMAND ----------

# Create external table with specific location
# Note: Replace with your desired path
external_path = "/tmp/delta_lab/employees_external"

df_employees.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", external_path) \
    .saveAsTable("employees_external")

print(f"✅ External table created at: {external_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View external table details - note the 'Location' field
# MAGIC DESCRIBE EXTENDED employees_external;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Table Properties and Configuration
# MAGIC
# MAGIC Delta tables support various properties for optimization and behavior control.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with specific properties
# MAGIC CREATE TABLE IF NOT EXISTS orders (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id INT,
# MAGIC     order_date DATE,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     status STRING
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.logRetentionDuration' = '30 days',
# MAGIC     'delta.deletedFileRetentionDuration' = '7 days',
# MAGIC     'delta.enableChangeDataFeed' = 'false',
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Table Properties
# MAGIC
# MAGIC | Property | Description | Default |
# MAGIC |----------|-------------|---------|
# MAGIC | `delta.logRetentionDuration` | How long to keep transaction log history | 30 days |
# MAGIC | `delta.deletedFileRetentionDuration` | How long to keep deleted data files | 7 days |
# MAGIC | `delta.enableChangeDataFeed` | Enable Change Data Feed for CDC | false |
# MAGIC | `delta.autoOptimize.optimizeWrite` | Optimize file sizes during writes | false |
# MAGIC | `delta.autoOptimize.autoCompact` | Auto-compact small files | false |

# COMMAND ----------

# Insert sample orders
spark.sql("""
    INSERT INTO orders VALUES
        (1001, 1, '2024-01-15', 150.00, 'completed'),
        (1002, 2, '2024-01-16', 275.50, 'completed'),
        (1003, 1, '2024-01-17', 89.99, 'pending'),
        (1004, 3, '2024-01-18', 450.00, 'completed'),
        (1005, 2, '2024-01-19', 125.00, 'shipped')
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Query Delta Tables with DataFrame API
# MAGIC
# MAGIC In addition to SQL, you can use PySpark DataFrame API.

# COMMAND ----------

# Read Delta table into DataFrame
df = spark.read.format("delta").table("employees")

# Display DataFrame
display(df)

# COMMAND ----------

# DataFrame transformations
from pyspark.sql.functions import col, avg, count, max, min

# Filter and aggregate
engineering_stats = df.filter(col("department") == "Engineering") \
    .agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        min("salary").alias("min_salary"),
        max("salary").alias("max_salary")
    )

display(engineering_stats)

# COMMAND ----------

# Complex query with multiple operations
result = df.filter(col("salary") > 90000) \
    .select("name", "department", "salary") \
    .orderBy(col("salary").desc())

display(result)

# COMMAND ----------

# Group by and aggregate
dept_summary = df.groupBy("department") \
    .agg(
        count("*").alias("headcount"),
        avg("salary").alias("avg_salary"),
        sum("salary").alias("total_salary")
    ) \
    .orderBy(col("avg_salary").desc())

display(dept_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: View Table Metadata and History
# MAGIC
# MAGIC Delta Lake automatically tracks all changes to tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table metadata
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Metadata Fields
# MAGIC
# MAGIC - **format**: Always "delta" for Delta tables
# MAGIC - **location**: Physical storage location
# MAGIC - **numFiles**: Number of data files in the table
# MAGIC - **sizeInBytes**: Total table size
# MAGIC - **properties**: Table properties/configurations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history (all operations)
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding History Output
# MAGIC
# MAGIC - **version**: Sequential version number (starts at 0)
# MAGIC - **timestamp**: When operation occurred
# MAGIC - **operation**: Type of operation (CREATE, WRITE, MERGE, etc.)
# MAGIC - **operationParameters**: Details about the operation
# MAGIC - **operationMetrics**: Performance metrics (rows written, files added, etc.)

# COMMAND ----------

# View history with Python API
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "employees")
history_df = delta_table.history()

display(history_df.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View history for specific number of versions
# MAGIC DESCRIBE HISTORY employees LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Read Delta Table by Path
# MAGIC
# MAGIC You can read Delta tables directly by their storage path.

# COMMAND ----------

# Get table location
table_location = spark.sql("DESCRIBE DETAIL employees").select("location").collect()[0][0]
print(f"Table location: {table_location}")

# Read by path
df_by_path = spark.read.format("delta").load(table_location)
display(df_by_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: Create Larger Sample Dataset
# MAGIC
# MAGIC Let's create a more realistic dataset (simulating ~1GB of data).

# COMMAND ----------

from pyspark.sql.functions import rand, expr, monotonically_increasing_id
from datetime import datetime, timedelta

# Generate larger dataset (100,000 rows)
large_df = spark.range(0, 100000) \
    .withColumn("customer_id", (rand() * 10000).cast("int")) \
    .withColumn("order_date", expr("date_add('2023-01-01', cast(rand() * 365 as int))")) \
    .withColumn("amount", (rand() * 1000).cast("decimal(10,2)")) \
    .withColumn("product_category", expr("""
        CASE 
            WHEN rand() < 0.3 THEN 'Electronics'
            WHEN rand() < 0.6 THEN 'Furniture'
            WHEN rand() < 0.8 THEN 'Clothing'
            ELSE 'Books'
        END
    """)) \
    .withColumn("status", expr("""
        CASE 
            WHEN rand() < 0.7 THEN 'completed'
            WHEN rand() < 0.9 THEN 'shipped'
            ELSE 'pending'
        END
    """))

# Write to Delta table
large_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_transactions")

print("✅ Large dataset created with 100,000 rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the table
# MAGIC SELECT COUNT(*) as total_rows FROM sales_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample query on large dataset
# MAGIC SELECT 
# MAGIC     product_category,
# MAGIC     status,
# MAGIC     COUNT(*) as transaction_count,
# MAGIC     SUM(amount) as total_amount,
# MAGIC     AVG(amount) as avg_amount
# MAGIC FROM sales_transactions
# MAGIC GROUP BY product_category, status
# MAGIC ORDER BY product_category, status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table details for the large dataset
# MAGIC DESCRIBE DETAIL sales_transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 10: Practice Exercise
# MAGIC
# MAGIC **Your Turn!** Create a Delta table for a customer database.
# MAGIC
# MAGIC ### Requirements:
# MAGIC 1. Create a table named `customers` with columns:
# MAGIC    - customer_id (INT)
# MAGIC    - first_name (STRING)
# MAGIC    - last_name (STRING)
# MAGIC    - email (STRING)
# MAGIC    - signup_date (DATE)
# MAGIC    - loyalty_tier (STRING): 'bronze', 'silver', or 'gold'
# MAGIC    - total_spent (DECIMAL)
# MAGIC
# MAGIC 2. Insert at least 10 customer records
# MAGIC 3. Query to find all 'gold' tier customers
# MAGIC 4. Calculate average total_spent by loyalty_tier
# MAGIC 5. View the table history

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Space (Try it yourself first!)

# COMMAND ----------

# TODO: Your solution here
# Step 1: Create the customers table

# Step 2: Insert customer data

# Step 3: Query gold tier customers

# Step 4: Calculate averages by tier

# Step 5: View history

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Creating Delta Tables**
# MAGIC - Use `.format("delta")` with DataFrame API
# MAGIC - Use `USING DELTA` with SQL
# MAGIC - Both methods create production-ready tables
# MAGIC
# MAGIC ✅ **Write Modes**
# MAGIC - `append`: Add new data without touching existing
# MAGIC - `overwrite`: Replace all existing data
# MAGIC - `merge`: Upsert (covered in Module 3)
# MAGIC
# MAGIC ✅ **Table Types**
# MAGIC - **Managed**: Databricks controls data and metadata
# MAGIC - **External**: You control data location
# MAGIC
# MAGIC ✅ **Querying**
# MAGIC - SQL: Natural for analysts
# MAGIC - DataFrame API: Powerful for data engineers
# MAGIC - Both work seamlessly with Delta
# MAGIC
# MAGIC ✅ **Metadata**
# MAGIC - `DESCRIBE DETAIL`: Table information
# MAGIC - `DESCRIBE HISTORY`: Version history and operations
# MAGIC - Automatic tracking of all changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Run this cell if you want to remove all tables created in this lab.

# COMMAND ----------

# Uncomment to cleanup
# spark.sql("DROP TABLE IF EXISTS employees")
# spark.sql("DROP TABLE IF EXISTS employees_temp")
# spark.sql("DROP TABLE IF EXISTS employees_managed")
# spark.sql("DROP TABLE IF EXISTS employees_external")
# spark.sql("DROP TABLE IF EXISTS products")
# spark.sql("DROP TABLE IF EXISTS orders")
# spark.sql("DROP TABLE IF EXISTS sales_transactions")
# spark.sql("DROP TABLE IF EXISTS customers")
# print("✅ All tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Congratulations! You've successfully created your first Delta Lake tables. 🎉
# MAGIC
# MAGIC **Continue to Lab 3: CRUD Operations and ACID Transactions**
# MAGIC - Learn INSERT, UPDATE, DELETE operations
# MAGIC - Master MERGE (upsert) patterns
# MAGIC - Understand ACID transaction guarantees
# MAGIC - Handle concurrent writes
# MAGIC
# MAGIC ---
# MAGIC **Questions?** Review Module 1 (Delta Lake Architecture) or consult the [Delta Lake documentation](https://docs.delta.io/).
