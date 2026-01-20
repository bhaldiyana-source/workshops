# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Creating and Exploring a Lakebase PostgreSQL Database
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Create a Lakebase database instance using the Databricks UI
# MAGIC - Register the database in Unity Catalog for governed access
# MAGIC - Configure instance settings including compute sizing and scaling
# MAGIC - Connect to Lakebase via SQL Warehouse
# MAGIC - Create tables and schemas for operational workloads
# MAGIC - Perform basic CRUD operations using SQL
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lecture 1: Lakebase Core Concepts and Architecture
# MAGIC - Access to a Databricks workspace with Lakebase enabled
# MAGIC - Permissions to create catalogs in Unity Catalog
# MAGIC - Access to a SQL Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Create your first Lakebase database instance
# MAGIC 2. Register it in Unity Catalog
# MAGIC 3. Connect and verify connectivity
# MAGIC 4. Create a sample schema and tables
# MAGIC 5. Perform basic SQL operations
# MAGIC
# MAGIC **Important**: This lab is **required** before proceeding to subsequent labs, as later notebooks will use the database and tables created here.
# MAGIC
# MAGIC ### Time Estimate
# MAGIC 30-45 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Lakebase Database Instance
# MAGIC
# MAGIC ### Using the Databricks UI
# MAGIC
# MAGIC Follow these steps to create your Lakebase database:
# MAGIC
# MAGIC 1. **Navigate to the Data Explorer**
# MAGIC    - In your Databricks workspace, click on **Data** in the left sidebar
# MAGIC    - Or access the **Catalog** icon
# MAGIC
# MAGIC 2. **Create a New Database**
# MAGIC    - Click the **Create** button
# MAGIC    - Select **Lakebase Database** from the dropdown
# MAGIC
# MAGIC 3. **Configure Your Database**
# MAGIC    - **Database Name**: `lakebase_workshop_<your_name>` (use your name or initials to make it unique)
# MAGIC    - **Catalog**: Select an existing catalog or create a new one (e.g., `workshop_catalog`)
# MAGIC    - **Schema**: `workshop` (this will be created automatically)
# MAGIC
# MAGIC 4. **Compute Configuration**
# MAGIC    - **Compute Size**: Start with **Small** (suitable for development and testing)
# MAGIC    - **Auto-scaling**: Enable (allows automatic scaling based on workload)
# MAGIC    - **Min Compute Units**: 1
# MAGIC    - **Max Compute Units**: 4
# MAGIC    - **Scale to Zero**: Enable (reduces costs when idle)
# MAGIC
# MAGIC 5. **Network and Security** (Optional - usually defaults are fine for testing)
# MAGIC    - Leave default settings for now
# MAGIC    - In production, you would configure:
# MAGIC      - Private Link (for network isolation)
# MAGIC      - IP Access Lists (for security)
# MAGIC
# MAGIC 6. **Click Create**
# MAGIC    - The database will take 1-2 minutes to provision
# MAGIC    - You'll see a status indicator showing "Creating..."
# MAGIC    - Wait until status changes to "Running"
# MAGIC
# MAGIC ### What Just Happened?
# MAGIC
# MAGIC Databricks has:
# MAGIC - Created a PostgreSQL-compatible database instance
# MAGIC - Registered it in Unity Catalog
# MAGIC - Provisioned serverless compute resources
# MAGIC - Set up automated backups and security

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Locate Your Database Connection Information
# MAGIC
# MAGIC Once your database is created and running:
# MAGIC
# MAGIC 1. **Navigate to Your Database**
# MAGIC    - Go to **Data Explorer** → **Catalogs**
# MAGIC    - Find your catalog (e.g., `workshop_catalog`)
# MAGIC    - Locate your Lakebase database
# MAGIC
# MAGIC 2. **View Database Details**
# MAGIC    - Click on your database name
# MAGIC    - You should see database metadata and connection information
# MAGIC
# MAGIC 3. **Note Key Information**
# MAGIC    - **Host**: The database endpoint (e.g., `xxxxx.cloud.databricks.com`)
# MAGIC    - **Port**: Usually `5432` (standard PostgreSQL port)
# MAGIC    - **Database Name**: Your database identifier
# MAGIC    - **Connection String**: Available for programmatic access
# MAGIC
# MAGIC 4. **Generate Credentials** (We'll use these in later labs)
# MAGIC    - Click **Generate credentials** or **Connection details**
# MAGIC    - Save the username and password securely
# MAGIC    - Store these in Databricks Secrets for production use
# MAGIC
# MAGIC **Security Note**: Never hardcode credentials in notebooks. We'll demonstrate proper credential management in later labs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Connect to Your Database via SQL
# MAGIC
# MAGIC Now let's verify connectivity using SQL through your SQL Warehouse.
# MAGIC
# MAGIC ### Option A: Using Databricks SQL Warehouse (Recommended for this lab)
# MAGIC
# MAGIC We can query Lakebase through Unity Catalog using standard SQL:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Up Your Catalog and Schema
# MAGIC
# MAGIC First, let's set the context to work with your database.
# MAGIC
# MAGIC **⚠️ Important**: Replace the catalog and schema names below with your actual values.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set your catalog and schema
# MAGIC -- Replace 'workshop_catalog' with your actual catalog name
# MAGIC USE CATALOG workshop_catalog;
# MAGIC
# MAGIC -- If needed, create a schema for our workshop tables
# MAGIC CREATE SCHEMA IF NOT EXISTS workshop
# MAGIC COMMENT 'Workshop schema for Lakebase hands-on labs';
# MAGIC
# MAGIC -- Set the schema as default
# MAGIC USE SCHEMA workshop;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify connection by listing schemas in your catalog
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Sample Tables
# MAGIC
# MAGIC Let's create tables for a simple e-commerce scenario. These tables will be used in subsequent labs.
# MAGIC
# MAGIC ### Schema Design
# MAGIC
# MAGIC We'll create three tables:
# MAGIC 1. **customers** - Customer information
# MAGIC 2. **products** - Product catalog
# MAGIC 3. **orders** - Customer orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customers table
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC   customer_id BIGINT PRIMARY KEY,
# MAGIC   email VARCHAR(255) NOT NULL UNIQUE,
# MAGIC   first_name VARCHAR(100),
# MAGIC   last_name VARCHAR(100),
# MAGIC   country VARCHAR(2),
# MAGIC   signup_date DATE,
# MAGIC   loyalty_points INT DEFAULT 0,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Customer master data';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create products table
# MAGIC CREATE TABLE IF NOT EXISTS products (
# MAGIC   product_id BIGINT PRIMARY KEY,
# MAGIC   product_name VARCHAR(255) NOT NULL,
# MAGIC   category VARCHAR(100),
# MAGIC   price DECIMAL(10, 2) NOT NULL,
# MAGIC   stock_quantity INT DEFAULT 0,
# MAGIC   is_active BOOLEAN DEFAULT TRUE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Product catalog';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create orders table
# MAGIC CREATE TABLE IF NOT EXISTS orders (
# MAGIC   order_id BIGINT PRIMARY KEY,
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   total_amount DECIMAL(10, 2) NOT NULL,
# MAGIC   order_status VARCHAR(50) DEFAULT 'pending',
# MAGIC   shipping_address TEXT,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
# MAGIC )
# MAGIC COMMENT 'Customer orders';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify tables were created
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe the structure of the customers table
# MAGIC DESCRIBE TABLE customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Insert Sample Data
# MAGIC
# MAGIC Let's populate our tables with sample data to work with in subsequent labs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample customers
# MAGIC INSERT INTO customers (customer_id, email, first_name, last_name, country, signup_date, loyalty_points)
# MAGIC VALUES
# MAGIC   (1, 'alice.johnson@email.com', 'Alice', 'Johnson', 'US', '2024-01-15', 1200),
# MAGIC   (2, 'bob.smith@email.com', 'Bob', 'Smith', 'CA', '2024-02-20', 850),
# MAGIC   (3, 'carol.williams@email.com', 'Carol', 'Williams', 'UK', '2024-03-10', 2100),
# MAGIC   (4, 'david.brown@email.com', 'David', 'Brown', 'AU', '2024-03-25', 450),
# MAGIC   (5, 'emma.davis@email.com', 'Emma', 'Davis', 'US', '2024-04-05', 1650);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample products
# MAGIC INSERT INTO products (product_id, product_name, category, price, stock_quantity, is_active)
# MAGIC VALUES
# MAGIC   (101, 'Laptop Pro 15', 'Electronics', 1299.99, 50, TRUE),
# MAGIC   (102, 'Wireless Mouse', 'Electronics', 29.99, 200, TRUE),
# MAGIC   (103, 'USB-C Cable', 'Accessories', 19.99, 500, TRUE),
# MAGIC   (104, 'Desk Lamp', 'Home', 45.50, 75, TRUE),
# MAGIC   (105, 'Office Chair', 'Furniture', 249.99, 30, TRUE),
# MAGIC   (106, 'Notebook Set', 'Stationery', 12.99, 150, TRUE),
# MAGIC   (107, 'Mechanical Keyboard', 'Electronics', 89.99, 80, TRUE),
# MAGIC   (108, 'Monitor Stand', 'Accessories', 35.00, 100, TRUE);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample orders
# MAGIC INSERT INTO orders (order_id, customer_id, order_date, total_amount, order_status, shipping_address)
# MAGIC VALUES
# MAGIC   (1001, 1, '2024-05-01 10:30:00', 1349.98, 'delivered', '123 Main St, San Francisco, CA 94102'),
# MAGIC   (1002, 2, '2024-05-02 14:15:00', 59.98, 'shipped', '456 Oak Ave, Toronto, ON M5V 2T6'),
# MAGIC   (1003, 3, '2024-05-03 09:45:00', 295.49, 'delivered', '789 High St, London W1A 1AA'),
# MAGIC   (1004, 1, '2024-05-04 16:20:00', 89.99, 'pending', '123 Main St, San Francisco, CA 94102'),
# MAGIC   (1005, 4, '2024-05-05 11:00:00', 249.99, 'processing', '321 Beach Rd, Sydney NSW 2000');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Perform Basic CRUD Operations
# MAGIC
# MAGIC Now let's practice Create, Read, Update, and Delete operations.
# MAGIC
# MAGIC ### READ Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query all customers
# MAGIC SELECT * FROM customers
# MAGIC ORDER BY signup_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query products with stock below 100
# MAGIC SELECT product_name, category, price, stock_quantity
# MAGIC FROM products
# MAGIC WHERE stock_quantity < 100
# MAGIC ORDER BY stock_quantity;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query orders with customer details (JOIN)
# MAGIC SELECT 
# MAGIC   o.order_id,
# MAGIC   c.first_name || ' ' || c.last_name AS customer_name,
# MAGIC   c.email,
# MAGIC   o.order_date,
# MAGIC   o.total_amount,
# MAGIC   o.order_status
# MAGIC FROM orders o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC ORDER BY o.order_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregate query: Total revenue by order status
# MAGIC SELECT 
# MAGIC   order_status,
# MAGIC   COUNT(*) AS num_orders,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   AVG(total_amount) AS avg_order_value
# MAGIC FROM orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPDATE Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update: Mark an order as delivered
# MAGIC UPDATE orders
# MAGIC SET 
# MAGIC   order_status = 'delivered',
# MAGIC   updated_at = CURRENT_TIMESTAMP
# MAGIC WHERE order_id = 1004;
# MAGIC
# MAGIC -- Verify the update
# MAGIC SELECT order_id, order_status, updated_at
# MAGIC FROM orders
# MAGIC WHERE order_id = 1004;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update: Reduce product stock after a sale
# MAGIC UPDATE products
# MAGIC SET 
# MAGIC   stock_quantity = stock_quantity - 10,
# MAGIC   updated_at = CURRENT_TIMESTAMP
# MAGIC WHERE product_id = 102;
# MAGIC
# MAGIC -- Verify the update
# MAGIC SELECT product_id, product_name, stock_quantity, updated_at
# MAGIC FROM products
# MAGIC WHERE product_id = 102;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update: Award loyalty points to a customer
# MAGIC UPDATE customers
# MAGIC SET 
# MAGIC   loyalty_points = loyalty_points + 100,
# MAGIC   updated_at = CURRENT_TIMESTAMP
# MAGIC WHERE customer_id = 1;
# MAGIC
# MAGIC -- Verify the update
# MAGIC SELECT customer_id, first_name, last_name, loyalty_points, updated_at
# MAGIC FROM customers
# MAGIC WHERE customer_id = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELETE Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a test record first
# MAGIC INSERT INTO customers (customer_id, email, first_name, last_name, country, signup_date)
# MAGIC VALUES (999, 'test.user@email.com', 'Test', 'User', 'US', '2024-05-10');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the test record
# MAGIC DELETE FROM customers
# MAGIC WHERE customer_id = 999;
# MAGIC
# MAGIC -- Verify deletion
# MAGIC SELECT COUNT(*) AS remaining_customers FROM customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction Example
# MAGIC
# MAGIC Lakebase supports full ACID transactions. Let's demonstrate with a multi-step operation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Process an order (this would typically be in a transaction block)
# MAGIC -- In Python/psycopg3, we'll use explicit transactions in Lab 4
# MAGIC
# MAGIC -- For now, let's see individual operations that would be wrapped in a transaction:
# MAGIC
# MAGIC -- 1. Create the order
# MAGIC INSERT INTO orders (order_id, customer_id, order_date, total_amount, order_status)
# MAGIC VALUES (1006, 2, CURRENT_TIMESTAMP, 1299.99, 'processing');
# MAGIC
# MAGIC -- 2. Reduce product stock
# MAGIC UPDATE products
# MAGIC SET stock_quantity = stock_quantity - 1
# MAGIC WHERE product_id = 101;
# MAGIC
# MAGIC -- 3. Award loyalty points
# MAGIC UPDATE customers
# MAGIC SET loyalty_points = loyalty_points + 130
# MAGIC WHERE customer_id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all changes were applied
# MAGIC SELECT 'Order Created' AS action, COUNT(*) AS result FROM orders WHERE order_id = 1006
# MAGIC UNION ALL
# MAGIC SELECT 'Stock Updated' AS action, stock_quantity FROM products WHERE product_id = 101
# MAGIC UNION ALL
# MAGIC SELECT 'Points Awarded' AS action, loyalty_points FROM customers WHERE customer_id = 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Explore Database Performance
# MAGIC
# MAGIC Let's examine query performance and database metrics.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an index on a frequently queried column
# MAGIC CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
# MAGIC CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);
# MAGIC CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to test index usage
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.first_name,
# MAGIC   c.last_name,
# MAGIC   COUNT(o.order_id) AS total_orders,
# MAGIC   SUM(o.total_amount) AS total_spent
# MAGIC FROM customers c
# MAGIC LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: View Database Metadata
# MAGIC
# MAGIC Let's explore metadata about our database and tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in the current schema
# MAGIC SHOW TABLES IN workshop;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get detailed information about the customers table
# MAGIC DESCRIBE EXTENDED customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table statistics
# MAGIC SELECT 
# MAGIC   'customers' AS table_name, COUNT(*) AS row_count FROM customers
# MAGIC UNION ALL
# MAGIC SELECT 'products' AS table_name, COUNT(*) AS row_count FROM products
# MAGIC UNION ALL
# MAGIC SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Observability
# MAGIC
# MAGIC ### View Database Metrics in the UI
# MAGIC
# MAGIC To monitor your Lakebase database:
# MAGIC
# MAGIC 1. **Navigate to Data Explorer** → Your Catalog → Your Database
# MAGIC 2. **Click on the Monitoring tab** (if available)
# MAGIC 3. **Observe key metrics**:
# MAGIC    - Transactions per second (TPS)
# MAGIC    - Query latency
# MAGIC    - Active connections
# MAGIC    - Compute utilization
# MAGIC    - Storage usage
# MAGIC
# MAGIC ### Key Metrics to Watch
# MAGIC
# MAGIC - **Query Latency**: Should be <10ms for point queries
# MAGIC - **Connection Count**: Monitor for connection leaks
# MAGIC - **Compute Scaling**: Verify auto-scaling behavior
# MAGIC - **Error Rate**: Track failed queries
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC ✅ **Enable auto-scaling** to handle variable workloads
# MAGIC
# MAGIC ✅ **Use scale-to-zero** for development databases
# MAGIC
# MAGIC ✅ **Create indexes** on frequently queried columns
# MAGIC
# MAGIC ✅ **Monitor connection pools** to prevent exhaustion
# MAGIC
# MAGIC ✅ **Set up alerts** for high latency or errors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ Created a Lakebase PostgreSQL database instance
# MAGIC
# MAGIC ✅ Registered it in Unity Catalog for governed access
# MAGIC
# MAGIC ✅ Configured compute sizing and auto-scaling settings
# MAGIC
# MAGIC ✅ Connected to the database via SQL Warehouse
# MAGIC
# MAGIC ✅ Created a sample schema with customers, products, and orders tables
# MAGIC
# MAGIC ✅ Performed CRUD operations (Create, Read, Update, Delete)
# MAGIC
# MAGIC ✅ Created indexes for query optimization
# MAGIC
# MAGIC ✅ Explored database metadata and monitoring
# MAGIC
# MAGIC ### What You've Learned
# MAGIC
# MAGIC - How to provision operational database infrastructure in minutes
# MAGIC - How Unity Catalog provides unified governance
# MAGIC - Basic PostgreSQL operations on Lakebase
# MAGIC - Transaction support and ACID guarantees
# MAGIC - Performance optimization with indexes
# MAGIC
# MAGIC ### Important Notes for Next Labs
# MAGIC
# MAGIC **Save your connection details:**
# MAGIC - Database name
# MAGIC - Host endpoint
# MAGIC - Credentials (username/password)
# MAGIC
# MAGIC You'll use these in the upcoming labs on Python connectivity.
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to:
# MAGIC - **Lecture 3: Ways to Access Your Database** - Learn about different connection methods
# MAGIC - **Lab 4: Single Connection Python Access** - Connect using Python and psycopg3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Cleanup (Only if starting over)
# MAGIC
# MAGIC **⚠️ WARNING**: Only run this section if you want to delete all data and start fresh.
# MAGIC
# MAGIC DO NOT run this now - you'll need these tables for subsequent labs!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ONLY RUN IF YOU WANT TO DELETE EVERYTHING
# MAGIC -- DROP TABLE IF EXISTS orders;
# MAGIC -- DROP TABLE IF EXISTS products;
# MAGIC -- DROP TABLE IF EXISTS customers;
# MAGIC -- DROP SCHEMA IF EXISTS workshop CASCADE;
