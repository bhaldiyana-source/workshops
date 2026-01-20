# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Multi-Database Federation with Complex Joins
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Create and configure foreign catalogs for multiple PostgreSQL databases
# MAGIC - Establish connections with proper credential management
# MAGIC - Execute complex cross-database queries with joins
# MAGIC - Optimize federated queries for performance
# MAGIC - Analyze query execution plans to identify bottlenecks
# MAGIC - Implement caching strategies for frequently accessed data
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lecture 1: Advanced Lakehouse Federation Architecture
# MAGIC - Completion of Lakebase Core Concepts workshop
# MAGIC - Access to Unity Catalog with federation enabled
# MAGIC - Permissions to create catalogs and connections
# MAGIC - At least one Lakebase database created (from core concepts workshop)
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Set up multiple foreign catalog connections
# MAGIC 2. Create sample databases and tables
# MAGIC 3. Execute cross-database queries
# MAGIC 4. Optimize query performance
# MAGIC 5. Analyze execution plans
# MAGIC
# MAGIC ### Time Estimate
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Environment Setup
# MAGIC
# MAGIC First, let's configure our environment and import necessary libraries.

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import time

# Get current user for unique naming
current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_name = current_user.split("@")[0].replace(".", "_")

# Configure catalog and schema names
MAIN_CATALOG = "main"
WORKSHOP_SCHEMA = f"federation_lab_{user_name}"
FOREIGN_CATALOG_1 = f"federated_orders_{user_name}"
FOREIGN_CATALOG_2 = f"federated_inventory_{user_name}"

print(f"✅ Environment configured for user: {current_user}")
print(f"   Workshop schema: {WORKSHOP_SCHEMA}")
print(f"   Foreign catalog 1: {FOREIGN_CATALOG_1}")
print(f"   Foreign catalog 2: {FOREIGN_CATALOG_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Database Connections
# MAGIC
# MAGIC ### Important Note About Connections
# MAGIC
# MAGIC In this lab, we'll create connections to PostgreSQL databases. You have two options:
# MAGIC
# MAGIC **Option A**: Use your existing Lakebase database from the Core Concepts workshop  
# MAGIC **Option B**: Connect to external PostgreSQL databases
# MAGIC
# MAGIC For this lab, we'll demonstrate with Lakebase databases for consistency.
# MAGIC
# MAGIC ### Security Best Practice
# MAGIC
# MAGIC Always use Databricks Secrets for credential management. Never hardcode passwords!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Connection for Orders Database
# MAGIC
# MAGIC **⚠️ Important**: Replace the connection details below with your actual Lakebase or PostgreSQL connection information.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a connection to the first database (Orders)
# MAGIC -- Note: Replace these values with your actual connection details
# MAGIC
# MAGIC CREATE CONNECTION IF NOT EXISTS orders_connection
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host '<your-lakebase-host>',
# MAGIC   port '5432',
# MAGIC   user '<your-username>',
# MAGIC   password secret('<scope>', '<key>')
# MAGIC );
# MAGIC
# MAGIC -- Verify connection
# MAGIC DESCRIBE CONNECTION orders_connection;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Connection for Inventory Database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a connection to the second database (Inventory)
# MAGIC -- For this lab, we can use the same physical database
# MAGIC -- but different schemas to simulate separate systems
# MAGIC
# MAGIC CREATE CONNECTION IF NOT EXISTS inventory_connection
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host '<your-lakebase-host>',
# MAGIC   port '5432',
# MAGIC   user '<your-username>',
# MAGIC   password secret('<scope>', '<key>')
# MAGIC );
# MAGIC
# MAGIC -- Verify connection
# MAGIC DESCRIBE CONNECTION inventory_connection;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Foreign Catalogs
# MAGIC
# MAGIC Now we'll create foreign catalogs that use these connections.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create foreign catalog for Orders database
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS ${FOREIGN_CATALOG_1}
# MAGIC USING CONNECTION orders_connection
# MAGIC OPTIONS (
# MAGIC   database 'lakebase_workshop'
# MAGIC );
# MAGIC
# MAGIC -- Show the catalog
# MAGIC SHOW SCHEMAS IN ${FOREIGN_CATALOG_1};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create foreign catalog for Inventory database
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS ${FOREIGN_CATALOG_2}
# MAGIC USING CONNECTION inventory_connection
# MAGIC OPTIONS (
# MAGIC   database 'lakebase_workshop'
# MAGIC );
# MAGIC
# MAGIC -- Show the catalog
# MAGIC SHOW SCHEMAS IN ${FOREIGN_CATALOG_2};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Sample Data in Foreign Catalogs
# MAGIC
# MAGIC Let's create realistic sample data across multiple databases to practice federation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Orders Schema and Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema in orders database
# MAGIC USE CATALOG ${FOREIGN_CATALOG_1};
# MAGIC CREATE SCHEMA IF NOT EXISTS orders_db
# MAGIC COMMENT 'Order management system';
# MAGIC
# MAGIC USE SCHEMA orders_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customers table
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC   customer_id BIGINT PRIMARY KEY,
# MAGIC   customer_name VARCHAR(200) NOT NULL,
# MAGIC   email VARCHAR(200),
# MAGIC   region VARCHAR(50),
# MAGIC   account_status VARCHAR(20),
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   lifetime_value DECIMAL(12,2)
# MAGIC );
# MAGIC
# MAGIC -- Create orders table
# MAGIC CREATE TABLE IF NOT EXISTS orders (
# MAGIC   order_id BIGINT PRIMARY KEY,
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   order_status VARCHAR(20),
# MAGIC   order_total DECIMAL(12,2),
# MAGIC   shipping_address VARCHAR(500),
# MAGIC   payment_method VARCHAR(50)
# MAGIC );
# MAGIC
# MAGIC -- Create order_items table
# MAGIC CREATE TABLE IF NOT EXISTS order_items (
# MAGIC   item_id BIGINT PRIMARY KEY,
# MAGIC   order_id BIGINT NOT NULL,
# MAGIC   product_id BIGINT NOT NULL,
# MAGIC   quantity INT NOT NULL,
# MAGIC   unit_price DECIMAL(10,2),
# MAGIC   discount_percent DECIMAL(5,2) DEFAULT 0
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Inventory Schema and Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema in inventory database
# MAGIC USE CATALOG ${FOREIGN_CATALOG_2};
# MAGIC CREATE SCHEMA IF NOT EXISTS inventory_db
# MAGIC COMMENT 'Inventory management system';
# MAGIC
# MAGIC USE SCHEMA inventory_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create products table
# MAGIC CREATE TABLE IF NOT EXISTS products (
# MAGIC   product_id BIGINT PRIMARY KEY,
# MAGIC   product_name VARCHAR(200) NOT NULL,
# MAGIC   category VARCHAR(100),
# MAGIC   unit_cost DECIMAL(10,2),
# MAGIC   unit_price DECIMAL(10,2),
# MAGIC   supplier_id BIGINT,
# MAGIC   reorder_level INT
# MAGIC );
# MAGIC
# MAGIC -- Create inventory_levels table
# MAGIC CREATE TABLE IF NOT EXISTS inventory_levels (
# MAGIC   inventory_id BIGINT PRIMARY KEY,
# MAGIC   product_id BIGINT NOT NULL,
# MAGIC   warehouse_location VARCHAR(100),
# MAGIC   quantity_available INT,
# MAGIC   quantity_reserved INT,
# MAGIC   last_restock_date TIMESTAMP,
# MAGIC   next_restock_date TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Create suppliers table
# MAGIC CREATE TABLE IF NOT EXISTS suppliers (
# MAGIC   supplier_id BIGINT PRIMARY KEY,
# MAGIC   supplier_name VARCHAR(200),
# MAGIC   contact_email VARCHAR(200),
# MAGIC   country VARCHAR(100),
# MAGIC   lead_time_days INT,
# MAGIC   reliability_score DECIMAL(3,2)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Sample Data

# COMMAND ----------

# Generate sample data using PySpark
from pyspark.sql.types import *
from random import randint, choice, uniform
from datetime import datetime, timedelta

# Generate customers data
customers_data = [
    (i, 
     f"Customer_{i}", 
     f"customer{i}@example.com", 
     choice(['North', 'South', 'East', 'West', 'Central']),
     choice(['Active', 'Inactive', 'Premium']),
     datetime.now() - timedelta(days=randint(1, 1000)),
     round(uniform(1000, 50000), 2))
    for i in range(1, 101)
]

customers_df = spark.createDataFrame(
    customers_data,
    ["customer_id", "customer_name", "email", "region", "account_status", "created_at", "lifetime_value"]
)

# Write to federated catalog
customers_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://<host>:5432/<database>") \
    .option("dbtable", f"{FOREIGN_CATALOG_1}.orders_db.customers") \
    .option("user", "<username>") \
    .option("password", "<password>") \
    .mode("append") \
    .save()

print("✅ Sample customer data generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Insert Data Using SQL
# MAGIC
# MAGIC For simplicity in this lab, let's use SQL INSERT statements:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample customers
# MAGIC USE CATALOG ${FOREIGN_CATALOG_1};
# MAGIC USE SCHEMA orders_db;
# MAGIC
# MAGIC INSERT INTO customers VALUES
# MAGIC (1, 'Acme Corporation', 'contact@acme.com', 'North', 'Premium', CURRENT_TIMESTAMP, 125000.00),
# MAGIC (2, 'Global Industries', 'info@global.com', 'South', 'Active', CURRENT_TIMESTAMP, 87500.00),
# MAGIC (3, 'Tech Solutions Ltd', 'sales@techsol.com', 'East', 'Active', CURRENT_TIMESTAMP, 45000.00),
# MAGIC (4, 'Manufacturing Co', 'orders@mfg.com', 'West', 'Premium', CURRENT_TIMESTAMP, 210000.00),
# MAGIC (5, 'Retail Partners', 'info@retail.com', 'Central', 'Active', CURRENT_TIMESTAMP, 62000.00);
# MAGIC
# MAGIC -- Insert sample orders
# MAGIC INSERT INTO orders VALUES
# MAGIC (101, 1, CURRENT_TIMESTAMP - INTERVAL 5 DAYS, 'Completed', 15000.00, '123 Main St, NY', 'Credit Card'),
# MAGIC (102, 1, CURRENT_TIMESTAMP - INTERVAL 3 DAYS, 'Shipped', 8500.00, '123 Main St, NY', 'Credit Card'),
# MAGIC (103, 2, CURRENT_TIMESTAMP - INTERVAL 4 DAYS, 'Completed', 22000.00, '456 Oak Ave, TX', 'Wire Transfer'),
# MAGIC (104, 3, CURRENT_TIMESTAMP - INTERVAL 2 DAYS, 'Processing', 5500.00, '789 Pine Rd, CA', 'PayPal'),
# MAGIC (105, 4, CURRENT_TIMESTAMP - INTERVAL 1 DAYS, 'Pending', 45000.00, '321 Elm St, WA', 'Purchase Order'),
# MAGIC (106, 2, CURRENT_TIMESTAMP, 'Processing', 12000.00, '456 Oak Ave, TX', 'Wire Transfer'),
# MAGIC (107, 5, CURRENT_TIMESTAMP - INTERVAL 6 DAYS, 'Completed', 7800.00, '654 Maple Dr, FL', 'Credit Card');
# MAGIC
# MAGIC -- Insert sample order items
# MAGIC INSERT INTO order_items VALUES
# MAGIC (1001, 101, 201, 10, 150.00, 5.0),
# MAGIC (1002, 101, 202, 5, 200.00, 0.0),
# MAGIC (1003, 102, 203, 20, 85.00, 10.0),
# MAGIC (1004, 103, 201, 25, 150.00, 8.0),
# MAGIC (1005, 103, 204, 15, 300.00, 5.0),
# MAGIC (1006, 104, 205, 8, 125.00, 0.0),
# MAGIC (1007, 105, 206, 50, 450.00, 15.0),
# MAGIC (1008, 106, 202, 12, 200.00, 5.0),
# MAGIC (1009, 107, 203, 30, 85.00, 12.0);
# MAGIC
# MAGIC SELECT '✅ Sample orders data created' as status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample products
# MAGIC USE CATALOG ${FOREIGN_CATALOG_2};
# MAGIC USE SCHEMA inventory_db;
# MAGIC
# MAGIC INSERT INTO products VALUES
# MAGIC (201, 'Industrial Widget A', 'Widgets', 100.00, 150.00, 301, 50),
# MAGIC (202, 'Premium Gadget B', 'Gadgets', 150.00, 200.00, 302, 30),
# MAGIC (203, 'Standard Tool C', 'Tools', 60.00, 85.00, 303, 100),
# MAGIC (204, 'Deluxe Component D', 'Components', 200.00, 300.00, 301, 25),
# MAGIC (205, 'Basic Accessory E', 'Accessories', 80.00, 125.00, 304, 75),
# MAGIC (206, 'Professional Kit F', 'Kits', 350.00, 450.00, 302, 20);
# MAGIC
# MAGIC -- Insert sample inventory levels
# MAGIC INSERT INTO inventory_levels VALUES
# MAGIC (5001, 201, 'Warehouse-North', 500, 50, CURRENT_TIMESTAMP - INTERVAL 10 DAYS, CURRENT_TIMESTAMP + INTERVAL 5 DAYS),
# MAGIC (5002, 202, 'Warehouse-South', 300, 30, CURRENT_TIMESTAMP - INTERVAL 8 DAYS, CURRENT_TIMESTAMP + INTERVAL 7 DAYS),
# MAGIC (5003, 203, 'Warehouse-East', 800, 100, CURRENT_TIMESTAMP - INTERVAL 5 DAYS, CURRENT_TIMESTAMP + INTERVAL 10 DAYS),
# MAGIC (5004, 204, 'Warehouse-West', 200, 25, CURRENT_TIMESTAMP - INTERVAL 12 DAYS, CURRENT_TIMESTAMP + INTERVAL 3 DAYS),
# MAGIC (5005, 205, 'Warehouse-Central', 600, 75, CURRENT_TIMESTAMP - INTERVAL 7 DAYS, CURRENT_TIMESTAMP + INTERVAL 8 DAYS),
# MAGIC (5006, 206, 'Warehouse-North', 150, 20, CURRENT_TIMESTAMP - INTERVAL 15 DAYS, CURRENT_TIMESTAMP + INTERVAL 2 DAYS);
# MAGIC
# MAGIC -- Insert sample suppliers
# MAGIC INSERT INTO suppliers VALUES
# MAGIC (301, 'Global Suppliers Inc', 'contact@globalsuppliers.com', 'USA', 7, 0.95),
# MAGIC (302, 'International Parts Co', 'sales@intlparts.com', 'Germany', 14, 0.92),
# MAGIC (303, 'Asian Manufacturing', 'orders@asianmfg.com', 'China', 21, 0.88),
# MAGIC (304, 'Local Distributors', 'info@localdist.com', 'USA', 3, 0.98);
# MAGIC
# MAGIC SELECT '✅ Sample inventory data created' as status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Basic Cross-Database Queries
# MAGIC
# MAGIC Now that we have data in multiple foreign catalogs, let's start with simple federated queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: List All Customers from Orders Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   region,
# MAGIC   account_status,
# MAGIC   lifetime_value
# MAGIC FROM ${FOREIGN_CATALOG_1}.orders_db.customers
# MAGIC ORDER BY lifetime_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: List All Products from Inventory Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   unit_price,
# MAGIC   unit_cost,
# MAGIC   (unit_price - unit_cost) as margin
# MAGIC FROM ${FOREIGN_CATALOG_2}.inventory_db.products
# MAGIC ORDER BY margin DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Complex Cross-Database Joins
# MAGIC
# MAGIC Now let's perform more complex queries that join data across both databases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Customer Orders with Product Details
# MAGIC
# MAGIC This query joins three tables across two foreign catalogs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join orders with customers and products across databases
# MAGIC SELECT 
# MAGIC   c.customer_name,
# MAGIC   c.region,
# MAGIC   o.order_id,
# MAGIC   o.order_date,
# MAGIC   o.order_status,
# MAGIC   p.product_name,
# MAGIC   oi.quantity,
# MAGIC   oi.unit_price,
# MAGIC   (oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) as line_total
# MAGIC FROM ${FOREIGN_CATALOG_1}.orders_db.customers c
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_1}.orders_db.orders o 
# MAGIC     ON c.customer_id = o.customer_id
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_1}.orders_db.order_items oi 
# MAGIC     ON o.order_id = oi.order_id
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_2}.inventory_db.products p 
# MAGIC     ON oi.product_id = p.product_id
# MAGIC WHERE o.order_status IN ('Completed', 'Shipped')
# MAGIC ORDER BY o.order_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: Inventory Status with Order Demand

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze inventory vs. demand
# MAGIC SELECT 
# MAGIC   p.product_name,
# MAGIC   p.category,
# MAGIC   il.warehouse_location,
# MAGIC   il.quantity_available,
# MAGIC   il.quantity_reserved,
# MAGIC   (il.quantity_available - il.quantity_reserved) as available_to_promise,
# MAGIC   COALESCE(SUM(oi.quantity), 0) as recent_demand,
# MAGIC   s.supplier_name,
# MAGIC   s.lead_time_days,
# MAGIC   il.next_restock_date
# MAGIC FROM ${FOREIGN_CATALOG_2}.inventory_db.products p
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_2}.inventory_db.inventory_levels il
# MAGIC     ON p.product_id = il.product_id
# MAGIC   LEFT JOIN ${FOREIGN_CATALOG_2}.inventory_db.suppliers s
# MAGIC     ON p.supplier_id = s.supplier_id
# MAGIC   LEFT JOIN ${FOREIGN_CATALOG_1}.orders_db.order_items oi
# MAGIC     ON p.product_id = oi.product_id
# MAGIC   LEFT JOIN ${FOREIGN_CATALOG_1}.orders_db.orders o
# MAGIC     ON oi.order_id = o.order_id
# MAGIC     AND o.order_date >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
# MAGIC GROUP BY 
# MAGIC   p.product_name, p.category, il.warehouse_location, 
# MAGIC   il.quantity_available, il.quantity_reserved,
# MAGIC   s.supplier_name, s.lead_time_days, il.next_restock_date
# MAGIC HAVING available_to_promise < recent_demand * 2  -- Low stock alert
# MAGIC ORDER BY available_to_promise ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 5: Customer Analytics with Multi-Database Aggregation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complex analytics across databases
# MAGIC SELECT 
# MAGIC   c.customer_name,
# MAGIC   c.region,
# MAGIC   c.account_status,
# MAGIC   COUNT(DISTINCT o.order_id) as total_orders,
# MAGIC   SUM(o.order_total) as total_spent,
# MAGIC   AVG(o.order_total) as avg_order_value,
# MAGIC   COUNT(DISTINCT p.category) as distinct_categories_purchased,
# MAGIC   MAX(o.order_date) as last_order_date,
# MAGIC   DATEDIFF(CURRENT_DATE, MAX(o.order_date)) as days_since_last_order
# MAGIC FROM ${FOREIGN_CATALOG_1}.orders_db.customers c
# MAGIC   LEFT JOIN ${FOREIGN_CATALOG_1}.orders_db.orders o
# MAGIC     ON c.customer_id = o.customer_id
# MAGIC   LEFT JOIN ${FOREIGN_CATALOG_1}.orders_db.order_items oi
# MAGIC     ON o.order_id = oi.order_id
# MAGIC   LEFT JOIN ${FOREIGN_CATALOG_2}.inventory_db.products p
# MAGIC     ON oi.product_id = p.product_id
# MAGIC GROUP BY c.customer_name, c.region, c.account_status
# MAGIC HAVING total_orders > 0
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Query Performance Analysis
# MAGIC
# MAGIC Let's analyze how these federated queries perform and identify optimization opportunities.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Query Execution Plan

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use EXPLAIN to see query execution plan
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT 
# MAGIC   c.customer_name,
# MAGIC   o.order_id,
# MAGIC   p.product_name,
# MAGIC   oi.quantity
# MAGIC FROM ${FOREIGN_CATALOG_1}.orders_db.customers c
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_1}.orders_db.orders o 
# MAGIC     ON c.customer_id = o.customer_id
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_1}.orders_db.order_items oi 
# MAGIC     ON o.order_id = oi.order_id
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_2}.inventory_db.products p 
# MAGIC     ON oi.product_id = p.product_id
# MAGIC WHERE o.order_date >= CURRENT_DATE - INTERVAL 7 DAYS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Timing

# COMMAND ----------

import time

# Time the federated query
start_time = time.time()

result = spark.sql(f"""
SELECT 
  c.customer_name,
  COUNT(*) as order_count,
  SUM(o.order_total) as total_spent
FROM {FOREIGN_CATALOG_1}.orders_db.customers c
  INNER JOIN {FOREIGN_CATALOG_1}.orders_db.orders o 
    ON c.customer_id = o.customer_id
GROUP BY c.customer_name
""")

result.show()
end_time = time.time()

print(f"⏱️  Query execution time: {end_time - start_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Query Optimization Techniques
# MAGIC
# MAGIC Let's implement optimization strategies to improve performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization 1: Predicate Pushdown
# MAGIC
# MAGIC Add filters early in the query to reduce data transfer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimized query with predicate pushdown
# MAGIC SELECT 
# MAGIC   c.customer_name,
# MAGIC   o.order_id,
# MAGIC   o.order_total
# MAGIC FROM ${FOREIGN_CATALOG_1}.orders_db.customers c
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_1}.orders_db.orders o 
# MAGIC     ON c.customer_id = o.customer_id
# MAGIC WHERE 
# MAGIC   c.region = 'North'                              -- Filter pushed to customers table
# MAGIC   AND o.order_date >= CURRENT_DATE - INTERVAL 7 DAYS  -- Filter pushed to orders table
# MAGIC   AND o.order_status = 'Completed';               -- Filter pushed to orders table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization 2: Column Pruning
# MAGIC
# MAGIC Select only necessary columns to reduce data transfer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bad: SELECT *
# MAGIC -- Good: Select specific columns
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   o.order_id,
# MAGIC   o.order_total
# MAGIC FROM ${FOREIGN_CATALOG_1}.orders_db.customers c
# MAGIC   INNER JOIN ${FOREIGN_CATALOG_1}.orders_db.orders o 
# MAGIC     ON c.customer_id = o.customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization 3: Caching Frequently Accessed Data
# MAGIC
# MAGIC For data that doesn't change frequently, cache it in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta table cache of product catalog
# MAGIC USE CATALOG main;
# MAGIC CREATE SCHEMA IF NOT EXISTS ${WORKSHOP_SCHEMA};
# MAGIC USE SCHEMA ${WORKSHOP_SCHEMA};
# MAGIC
# MAGIC CREATE OR REPLACE TABLE products_cache
# MAGIC AS
# MAGIC SELECT * FROM ${FOREIGN_CATALOG_2}.inventory_db.products;
# MAGIC
# MAGIC -- Now query the cache instead of federated table
# MAGIC SELECT COUNT(*) as cached_products FROM products_cache;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Performance: Federated vs Cached

# COMMAND ----------

import time

# Query federated table
start_federated = time.time()
federated_result = spark.sql(f"""
  SELECT product_name, category, unit_price 
  FROM {FOREIGN_CATALOG_2}.inventory_db.products 
  WHERE category = 'Widgets'
""")
federated_count = federated_result.count()
time_federated = time.time() - start_federated

# Query cached table
start_cached = time.time()
cached_result = spark.sql(f"""
  SELECT product_name, category, unit_price 
  FROM main.{WORKSHOP_SCHEMA}.products_cache 
  WHERE category = 'Widgets'
""")
cached_count = cached_result.count()
time_cached = time.time() - start_cached

print(f"📊 Performance Comparison:")
print(f"   Federated query: {time_federated:.3f} seconds")
print(f"   Cached query:    {time_cached:.3f} seconds")
print(f"   Speedup:         {time_federated/time_cached:.2f}x faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Advanced Federation Patterns
# MAGIC
# MAGIC Let's implement some advanced patterns for production use.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern: Incremental Cache Refresh

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a cache with timestamp tracking
# MAGIC CREATE OR REPLACE TABLE main.${WORKSHOP_SCHEMA}.orders_cache
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   CURRENT_TIMESTAMP as cached_at
# MAGIC FROM ${FOREIGN_CATALOG_1}.orders_db.orders;
# MAGIC
# MAGIC -- Later, refresh only new/updated records
# MAGIC MERGE INTO main.${WORKSHOP_SCHEMA}.orders_cache target
# MAGIC USING (
# MAGIC   SELECT *, CURRENT_TIMESTAMP as cached_at
# MAGIC   FROM ${FOREIGN_CATALOG_1}.orders_db.orders
# MAGIC   WHERE order_date >= (SELECT MAX(order_date) FROM main.${WORKSHOP_SCHEMA}.orders_cache)
# MAGIC ) source
# MAGIC ON target.order_id = source.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern: Hybrid Query (Hot + Cold Data)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query combines recent live data with historical cached data
# MAGIC WITH recent_orders AS (
# MAGIC   -- Live data from last 7 days
# MAGIC   SELECT * FROM ${FOREIGN_CATALOG_1}.orders_db.orders
# MAGIC   WHERE order_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC ),
# MAGIC historical_orders AS (
# MAGIC   -- Historical data from cache
# MAGIC   SELECT * FROM main.${WORKSHOP_SCHEMA}.orders_cache
# MAGIC   WHERE order_date < CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC )
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('day', order_date) as order_day,
# MAGIC   COUNT(*) as orders,
# MAGIC   SUM(order_total) as revenue
# MAGIC FROM (
# MAGIC   SELECT * FROM recent_orders
# MAGIC   UNION ALL
# MAGIC   SELECT order_id, customer_id, order_date, order_status, order_total, 
# MAGIC          shipping_address, payment_method
# MAGIC   FROM historical_orders
# MAGIC )
# MAGIC GROUP BY DATE_TRUNC('day', order_date)
# MAGIC ORDER BY order_day DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Monitoring and Troubleshooting

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Foreign Catalog Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View catalog information
# MAGIC DESCRIBE CATALOG EXTENDED ${FOREIGN_CATALOG_1};
# MAGIC
# MAGIC -- View connection details
# MAGIC DESCRIBE CONNECTION orders_connection;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Table Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect statistics for better query planning
# MAGIC ANALYZE TABLE ${FOREIGN_CATALOG_1}.orders_db.orders COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE ${FOREIGN_CATALOG_2}.inventory_db.products COMPUTE STATISTICS;
# MAGIC
# MAGIC -- View statistics
# MAGIC DESCRIBE EXTENDED ${FOREIGN_CATALOG_1}.orders_db.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query History and Performance Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent queries (available in SQL Warehouse)
# MAGIC -- This query works in Databricks SQL
# MAGIC SELECT 
# MAGIC   query_id,
# MAGIC   query_text,
# MAGIC   execution_time_ms,
# MAGIC   rows_produced
# MAGIC FROM system.query.history
# MAGIC WHERE query_start_time >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
# MAGIC ORDER BY execution_time_ms DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Best Practices Summary
# MAGIC
# MAGIC Based on this lab, here are key best practices:

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ DO:
# MAGIC
# MAGIC 1. **Use predicate pushdown**: Add WHERE clauses to filter data at the source
# MAGIC 2. **Select specific columns**: Avoid SELECT * in federated queries
# MAGIC 3. **Cache reference data**: Store slowly-changing data in Delta Lake
# MAGIC 4. **Use incremental refresh**: Update caches with only new/changed data
# MAGIC 5. **Analyze execution plans**: Use EXPLAIN to understand query behavior
# MAGIC 6. **Collect statistics**: Run ANALYZE TABLE for better query optimization
# MAGIC 7. **Implement connection pooling**: Configure appropriate pool sizes
# MAGIC 8. **Monitor performance**: Track query execution times and optimize slow queries
# MAGIC
# MAGIC ### ❌ DON'T:
# MAGIC
# MAGIC 1. **Don't perform large cross-system joins**: Materialize to Delta first
# MAGIC 2. **Don't use UDFs on federated tables**: They prevent pushdown optimization
# MAGIC 3. **Don't query very large tables**: Use incremental processing or sync to Delta
# MAGIC 4. **Don't ignore network latency**: Consider data locality in your architecture
# MAGIC 5. **Don't hardcode credentials**: Always use Databricks Secrets
# MAGIC 6. **Don't skip error handling**: Implement retry logic for transient failures

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Cleanup (Optional)
# MAGIC
# MAGIC Run these commands if you want to clean up resources created in this lab.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop cached tables
# MAGIC -- DROP TABLE IF EXISTS main.${WORKSHOP_SCHEMA}.products_cache;
# MAGIC -- DROP TABLE IF EXISTS main.${WORKSHOP_SCHEMA}.orders_cache;
# MAGIC
# MAGIC -- Drop foreign catalogs
# MAGIC -- DROP CATALOG IF EXISTS ${FOREIGN_CATALOG_1};
# MAGIC -- DROP CATALOG IF EXISTS ${FOREIGN_CATALOG_2};
# MAGIC
# MAGIC -- Drop connections
# MAGIC -- DROP CONNECTION IF EXISTS orders_connection;
# MAGIC -- DROP CONNECTION IF EXISTS inventory_connection;
# MAGIC
# MAGIC SELECT 'Cleanup commands available above (commented out)' as note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC In this lab, you learned:
# MAGIC
# MAGIC 1. ✅ How to create and configure foreign catalogs for multiple databases
# MAGIC 2. ✅ Executing complex cross-database queries with joins
# MAGIC 3. ✅ Analyzing query execution plans to identify bottlenecks
# MAGIC 4. ✅ Implementing optimization techniques (pushdown, caching, column pruning)
# MAGIC 5. ✅ Using hybrid query patterns for hot/cold data
# MAGIC 6. ✅ Monitoring and troubleshooting federated queries
# MAGIC 7. ✅ Best practices for production federation architectures
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Lecture 3**: Change Data Capture and Real-Time Sync
# MAGIC - Learn how to implement CDC pipelines for real-time data synchronization
# MAGIC - Explore Debezium and Delta Live Tables integration
