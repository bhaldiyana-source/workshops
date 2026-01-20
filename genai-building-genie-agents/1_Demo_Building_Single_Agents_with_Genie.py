# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building Single Agents with Genie
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo walks through the process of creating Genie-compatible Unity Catalog functions and configuring a Genie space for natural language data querying. You'll learn the key differences between AI Playground and Genie, how to create table-valued functions, and how to test your Genie agent.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Understand the differences between Genie and AI Playground
# MAGIC - Convert scalar functions to table-valued functions for Genie compatibility
# MAGIC - Create Unity Catalog functions that work with Genie
# MAGIC - Configure a Genie space with custom functions
# MAGIC - Query Genie spaces using natural language
# MAGIC - Observe and interpret tool-calling behavior in Genie
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - SQL warehouse (required for Genie)
# MAGIC - Basic SQL knowledge
# MAGIC
# MAGIC ## Duration
# MAGIC 10-15 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Genie?
# MAGIC
# MAGIC **Genie** is Databricks' conversational AI interface for data exploration and analysis. It allows users to ask questions about their data in natural language and receive insights, visualizations, and detailed results.
# MAGIC
# MAGIC ### Key Differences: Genie vs AI Playground
# MAGIC
# MAGIC | Feature | AI Playground | Genie |
# MAGIC |---------|--------------|-------|
# MAGIC | **Primary Use Case** | General-purpose agent development | Data exploration & BI queries |
# MAGIC | **Function Types** | Scalar and table-valued | **Table-valued only** |
# MAGIC | **Configuration** | Attach functions per session | Pre-configured Genie space |
# MAGIC | **User Interface** | Developer-focused testing | Business user-friendly |
# MAGIC | **Output** | Text responses | Tables, charts, and insights |
# MAGIC | **Tool Visibility** | Shows tool calls explicitly | Integrated seamlessly |
# MAGIC
# MAGIC ### Why Table-Valued Functions?
# MAGIC
# MAGIC Genie is designed for **data exploration**, so it needs functions that return **tabular data** that can be:
# MAGIC - Displayed as tables
# MAGIC - Visualized in charts
# MAGIC - Further analyzed with follow-up questions
# MAGIC - Filtered and sorted by users
# MAGIC
# MAGIC **Scalar functions** (returning single values) don't provide the rich, explorable data that Genie's interface is built for.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Catalog and Schema
# MAGIC
# MAGIC First, we'll set up our workspace with a catalog and schema for our functions.
# MAGIC
# MAGIC **Note:** Replace `main` with your catalog name if using a different catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use your catalog (replace 'main' if needed)
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC -- Create a schema for Genie tools
# MAGIC CREATE SCHEMA IF NOT EXISTS genie_tools;
# MAGIC
# MAGIC -- Use the schema
# MAGIC USE SCHEMA genie_tools;
# MAGIC
# MAGIC -- Verify current location
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample Data
# MAGIC
# MAGIC Let's create sample tables representing an e-commerce system. This data will be queried through Genie.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customers table
# MAGIC CREATE OR REPLACE TABLE customers (
# MAGIC   customer_id STRING,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   tier STRING,
# MAGIC   join_date DATE,
# MAGIC   region STRING
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO customers VALUES
# MAGIC   ('C001', 'Alice Anderson', 'alice@example.com', 'Gold', '2024-01-15', 'West'),
# MAGIC   ('C002', 'Bob Brown', 'bob@example.com', 'Silver', '2024-03-20', 'East'),
# MAGIC   ('C003', 'Carol Chen', 'carol@example.com', 'Gold', '2024-02-10', 'West'),
# MAGIC   ('C004', 'David Davis', 'david@example.com', 'Bronze', '2024-06-05', 'Central'),
# MAGIC   ('C005', 'Emma Evans', 'emma@example.com', 'Silver', '2024-04-12', 'East'),
# MAGIC   ('C006', 'Frank Foster', 'frank@example.com', 'Gold', '2024-01-20', 'West'),
# MAGIC   ('C007', 'Grace Garcia', 'grace@example.com', 'Bronze', '2024-07-15', 'Central');
# MAGIC
# MAGIC SELECT * FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create orders table
# MAGIC CREATE OR REPLACE TABLE orders (
# MAGIC   order_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   order_date DATE,
# MAGIC   total_amount DOUBLE,
# MAGIC   status STRING,
# MAGIC   product_category STRING
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO orders VALUES
# MAGIC   ('O001', 'C001', '2024-12-01', 150.00, 'completed', 'Electronics'),
# MAGIC   ('O002', 'C001', '2024-12-15', 200.00, 'completed', 'Electronics'),
# MAGIC   ('O003', 'C001', '2025-01-05', 175.00, 'shipped', 'Furniture'),
# MAGIC   ('O004', 'C002', '2024-12-10', 89.99, 'completed', 'Electronics'),
# MAGIC   ('O005', 'C002', '2025-01-03', 125.50, 'processing', 'Accessories'),
# MAGIC   ('O006', 'C003', '2024-11-20', 300.00, 'completed', 'Furniture'),
# MAGIC   ('O007', 'C003', '2024-12-28', 450.00, 'completed', 'Electronics'),
# MAGIC   ('O008', 'C004', '2025-01-08', 75.00, 'shipped', 'Accessories'),
# MAGIC   ('O009', 'C005', '2024-12-05', 220.00, 'completed', 'Furniture'),
# MAGIC   ('O010', 'C006', '2024-11-15', 380.00, 'completed', 'Electronics'),
# MAGIC   ('O011', 'C006', '2024-12-20', 295.00, 'completed', 'Furniture'),
# MAGIC   ('O012', 'C007', '2025-01-02', 45.00, 'processing', 'Accessories');
# MAGIC
# MAGIC SELECT * FROM orders ORDER BY order_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create products table
# MAGIC CREATE OR REPLACE TABLE products (
# MAGIC   product_id STRING,
# MAGIC   name STRING,
# MAGIC   category STRING,
# MAGIC   price DOUBLE,
# MAGIC   stock_quantity INT,
# MAGIC   reorder_level INT
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO products VALUES
# MAGIC   ('P001', 'Laptop Pro 15', 'Electronics', 1299.99, 25, 10),
# MAGIC   ('P002', 'Wireless Mouse', 'Electronics', 29.99, 150, 50),
# MAGIC   ('P003', 'Office Chair', 'Furniture', 249.99, 40, 15),
# MAGIC   ('P004', 'Desk Lamp', 'Furniture', 45.99, 8, 20),
# MAGIC   ('P005', 'USB-C Cable', 'Accessories', 12.99, 200, 100),
# MAGIC   ('P006', 'Monitor 27inch', 'Electronics', 399.99, 30, 10),
# MAGIC   ('P007', 'Keyboard Mechanical', 'Electronics', 129.99, 60, 25),
# MAGIC   ('P008', 'Standing Desk', 'Furniture', 499.99, 5, 10),
# MAGIC   ('P009', 'Webcam HD', 'Electronics', 89.99, 45, 20),
# MAGIC   ('P010', 'Phone Stand', 'Accessories', 19.99, 3, 25);
# MAGIC
# MAGIC SELECT * FROM products ORDER BY category, name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Scalar vs Table-Valued Functions
# MAGIC
# MAGIC Before we create Genie-compatible functions, let's understand why scalar functions don't work with Genie.
# MAGIC
# MAGIC ### Example: Scalar Function (Won't Work with Genie)
# MAGIC
# MAGIC A scalar function returns a **single value** (INT, STRING, DOUBLE, etc.)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This is a SCALAR function - returns a single number
# MAGIC CREATE OR REPLACE FUNCTION get_order_count_scalar(customer_id STRING)
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns the total number of orders for a customer (SCALAR - NOT compatible with Genie)'
# MAGIC RETURN (
# MAGIC   SELECT COUNT(*)
# MAGIC   FROM orders
# MAGIC   WHERE customer_id = get_order_count_scalar.customer_id
# MAGIC );
# MAGIC
# MAGIC -- Test it
# MAGIC SELECT get_order_count_scalar('C001') as order_count;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Scalar Functions Don't Work with Genie
# MAGIC
# MAGIC **Problem:** Genie's interface is designed to display and visualize tabular data. A single number doesn't provide:
# MAGIC - Column headers for context
# MAGIC - Multiple rows to explore
# MAGIC - Data that can be charted or filtered
# MAGIC - Follow-up query opportunities
# MAGIC
# MAGIC **Solution:** Convert to a table-valued function that returns structured data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Table-Valued Functions for Genie
# MAGIC
# MAGIC Table-valued functions use `RETURNS TABLE(...)` syntax to define the structure of returned data.
# MAGIC
# MAGIC ### Example 1: Customer Orders (Table Function)
# MAGIC
# MAGIC Let's convert our scalar function to a table-valued function.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_customer_orders_table(customer_id STRING)
# MAGIC RETURNS TABLE(
# MAGIC   order_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   order_date DATE,
# MAGIC   total_amount DOUBLE,
# MAGIC   status STRING,
# MAGIC   product_category STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Retrieves detailed order information for a specific customer, including order ID, date, amount, status, and product category. Returns up to 50 most recent orders. Use this when users ask about a customer''s orders, purchase history, or recent transactions.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     o.order_id,
# MAGIC     c.name as customer_name,
# MAGIC     o.order_date,
# MAGIC     o.total_amount,
# MAGIC     o.status,
# MAGIC     o.product_category
# MAGIC   FROM orders o
# MAGIC   JOIN customers c ON o.customer_id = c.customer_id
# MAGIC   WHERE o.customer_id = get_customer_orders_table.customer_id
# MAGIC   ORDER BY o.order_date DESC
# MAGIC   LIMIT 50
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with customer C001
# MAGIC SELECT * FROM get_customer_orders_table('C001');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with customer C003
# MAGIC SELECT * FROM get_customer_orders_table('C003');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Product Inventory Status
# MAGIC
# MAGIC A function that shows which products need reordering.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_product_inventory_table()
# MAGIC RETURNS TABLE(
# MAGIC   product_id STRING,
# MAGIC   product_name STRING,
# MAGIC   category STRING,
# MAGIC   current_stock INT,
# MAGIC   reorder_level INT,
# MAGIC   stock_status STRING,
# MAGIC   needs_reorder BOOLEAN
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns current inventory status for all products, including stock levels, reorder thresholds, and whether reordering is needed. Use this when users ask about inventory, stock levels, products running low, or what needs to be reordered.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     product_id,
# MAGIC     name as product_name,
# MAGIC     category,
# MAGIC     stock_quantity as current_stock,
# MAGIC     reorder_level,
# MAGIC     CASE 
# MAGIC       WHEN stock_quantity = 0 THEN 'Out of Stock'
# MAGIC       WHEN stock_quantity <= reorder_level THEN 'Low Stock'
# MAGIC       ELSE 'In Stock'
# MAGIC     END as stock_status,
# MAGIC     CASE 
# MAGIC       WHEN stock_quantity <= reorder_level THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END as needs_reorder
# MAGIC   FROM products
# MAGIC   ORDER BY 
# MAGIC     CASE 
# MAGIC       WHEN stock_quantity = 0 THEN 1
# MAGIC       WHEN stock_quantity <= reorder_level THEN 2
# MAGIC       ELSE 3
# MAGIC     END,
# MAGIC     stock_quantity ASC
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM get_product_inventory_table();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See only items that need reordering
# MAGIC SELECT * FROM get_product_inventory_table()
# MAGIC WHERE needs_reorder = TRUE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Sales Summary by Period
# MAGIC
# MAGIC A function that aggregates sales data over a date range.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_sales_by_period_table(
# MAGIC   start_date DATE COMMENT 'Start of the date range (inclusive)',
# MAGIC   end_date DATE COMMENT 'End of the date range (inclusive)'
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   product_category STRING,
# MAGIC   total_orders BIGINT,
# MAGIC   total_revenue DOUBLE,
# MAGIC   avg_order_value DOUBLE,
# MAGIC   unique_customers BIGINT
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Provides sales analytics grouped by product category for a specific date range. Shows total orders, revenue, average order value, and unique customer count per category. Use this when users ask about sales performance, revenue by category, or category comparisons over time.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     product_category,
# MAGIC     COUNT(*) as total_orders,
# MAGIC     ROUND(SUM(total_amount), 2) as total_revenue,
# MAGIC     ROUND(AVG(total_amount), 2) as avg_order_value,
# MAGIC     COUNT(DISTINCT customer_id) as unique_customers
# MAGIC   FROM orders
# MAGIC   WHERE order_date BETWEEN get_sales_by_period_table.start_date 
# MAGIC                        AND get_sales_by_period_table.end_date
# MAGIC   GROUP BY product_category
# MAGIC   ORDER BY total_revenue DESC
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- December 2024 sales
# MAGIC SELECT * FROM get_sales_by_period_table('2024-12-01', '2024-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All 2024 sales
# MAGIC SELECT * FROM get_sales_by_period_table('2024-01-01', '2024-12-31');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4: Customer Tier Analysis
# MAGIC
# MAGIC A function that analyzes customers by tier with no parameters.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_customer_tier_analysis_table()
# MAGIC RETURNS TABLE(
# MAGIC   tier STRING,
# MAGIC   customer_count BIGINT,
# MAGIC   total_orders BIGINT,
# MAGIC   total_revenue DOUBLE,
# MAGIC   avg_customer_value DOUBLE,
# MAGIC   avg_orders_per_customer DOUBLE
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Analyzes customer metrics grouped by tier (Gold, Silver, Bronze). Shows customer count, order volume, revenue, and averages per tier. Use this when users ask about customer segments, tier performance, or comparing customer groups.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     c.tier,
# MAGIC     COUNT(DISTINCT c.customer_id) as customer_count,
# MAGIC     COUNT(o.order_id) as total_orders,
# MAGIC     ROUND(COALESCE(SUM(o.total_amount), 0), 2) as total_revenue,
# MAGIC     ROUND(COALESCE(SUM(o.total_amount), 0) / COUNT(DISTINCT c.customer_id), 2) as avg_customer_value,
# MAGIC     ROUND(CAST(COUNT(o.order_id) AS DOUBLE) / COUNT(DISTINCT c.customer_id), 2) as avg_orders_per_customer
# MAGIC   FROM customers c
# MAGIC   LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC   GROUP BY c.tier
# MAGIC   ORDER BY total_revenue DESC
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM get_customer_tier_analysis_table();

# COMMAND ----------

# MAGIC %md
# MAGIC ## View All Functions
# MAGIC
# MAGIC Let's see all the functions we've created for Genie.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all functions in the genie_tools schema
# MAGIC SHOW FUNCTIONS IN genie_tools LIKE '*_table';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get details about a specific function
# MAGIC DESCRIBE FUNCTION EXTENDED get_customer_orders_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuring Your Genie Space
# MAGIC
# MAGIC Now that we have table-valued functions, let's configure a Genie space to use them.
# MAGIC
# MAGIC ### Step-by-Step Instructions
# MAGIC
# MAGIC #### Step 1: Navigate to Genie
# MAGIC 1. In your Databricks workspace, click on **"Genie"** in the left sidebar
# MAGIC 2. If you don't see Genie, it may be under **"AI"** or **"Machine Learning"** → **"Genie"**
# MAGIC
# MAGIC #### Step 2: Create a New Genie Space
# MAGIC 1. Click **"Create Genie Space"** or **"New Space"**
# MAGIC 2. Give it a name: **"E-commerce Analytics"**
# MAGIC 3. Add a description: **"Analyze customer orders, product inventory, and sales data"**
# MAGIC
# MAGIC #### Step 3: Configure Data Sources
# MAGIC 1. In the Genie space configuration:
# MAGIC    - Click **"Add Data Sources"**
# MAGIC    - Navigate to your catalog: **`main.genie_tools`**
# MAGIC    - Select the following tables:
# MAGIC      - ✅ `customers`
# MAGIC      - ✅ `orders`
# MAGIC      - ✅ `products`
# MAGIC
# MAGIC #### Step 4: Attach Unity Catalog Functions
# MAGIC 1. In the same configuration screen:
# MAGIC    - Click **"Add Tools"** or **"Attach Functions"**
# MAGIC    - Navigate to **`main.genie_tools`**
# MAGIC    - Select your table-valued functions:
# MAGIC      - ✅ `get_customer_orders_table`
# MAGIC      - ✅ `get_product_inventory_table`
# MAGIC      - ✅ `get_sales_by_period_table`
# MAGIC      - ✅ `get_customer_tier_analysis_table`
# MAGIC
# MAGIC #### Step 5: Configure SQL Warehouse
# MAGIC 1. Select a **SQL Warehouse** for query execution
# MAGIC 2. Choose a serverless warehouse for best performance (if available)
# MAGIC 3. Or select an existing SQL warehouse
# MAGIC
# MAGIC #### Step 6: Set Permissions (Optional)
# MAGIC 1. Define who can access this Genie space
# MAGIC 2. Add users or groups
# MAGIC 3. Set permissions (view, edit, manage)
# MAGIC
# MAGIC #### Step 7: Save and Launch
# MAGIC 1. Click **"Save"** or **"Create"**
# MAGIC 2. Your Genie space is now ready!
# MAGIC 3. Click **"Open Genie Space"** to start querying

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing Your Genie Space
# MAGIC
# MAGIC Once your Genie space is configured, you can test it with natural language queries.
# MAGIC
# MAGIC ### Example Queries to Try
# MAGIC
# MAGIC #### Customer Analysis Queries
# MAGIC - **"Show me all orders for customer C001"**
# MAGIC   - *Expected: Genie calls `get_customer_orders_table('C001')`*
# MAGIC   
# MAGIC - **"What orders did Alice Anderson place?"**
# MAGIC   - *Expected: Genie looks up Alice's customer_id, then calls the function*
# MAGIC
# MAGIC - **"Show me the purchase history for customer C003"**
# MAGIC   - *Expected: Returns Carol Chen's orders*
# MAGIC
# MAGIC #### Inventory Queries
# MAGIC - **"What products are low in stock?"**
# MAGIC   - *Expected: Genie calls `get_product_inventory_table()` and filters for low stock*
# MAGIC
# MAGIC - **"Which items need to be reordered?"**
# MAGIC   - *Expected: Shows products where `needs_reorder = TRUE`*
# MAGIC
# MAGIC - **"Show me current inventory status"**
# MAGIC   - *Expected: Returns full inventory table*
# MAGIC
# MAGIC #### Sales Analysis Queries
# MAGIC - **"Summarize sales for December 2024"**
# MAGIC   - *Expected: Genie calls `get_sales_by_period_table('2024-12-01', '2024-12-31')`*
# MAGIC
# MAGIC - **"What were the sales by category in January 2025?"**
# MAGIC   - *Expected: Returns sales breakdown for January*
# MAGIC
# MAGIC - **"Show me revenue by product category for Q4 2024"**
# MAGIC   - *Expected: Aggregates October-December 2024 data*
# MAGIC
# MAGIC #### Customer Tier Queries
# MAGIC - **"Compare customer tiers"**
# MAGIC   - *Expected: Genie calls `get_customer_tier_analysis_table()`*
# MAGIC
# MAGIC - **"Which customer tier is most valuable?"**
# MAGIC   - *Expected: Shows tier analysis with Gold at top*
# MAGIC
# MAGIC - **"Show me customer segments by tier"**
# MAGIC   - *Expected: Returns tier analysis table*
# MAGIC
# MAGIC ### Complex Follow-Up Queries
# MAGIC
# MAGIC One of Genie's strengths is handling follow-up questions:
# MAGIC
# MAGIC 1. **First query:** "Show me all orders for customer C001"
# MAGIC 2. **Follow-up:** "What was their total spending?"
# MAGIC 3. **Follow-up:** "Which categories did they buy from?"
# MAGIC
# MAGIC Genie maintains context and can query the previous results or call additional functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observing Tool-Calling Behavior
# MAGIC
# MAGIC Genie integrates tool calling seamlessly, but you can still observe when functions are used.
# MAGIC
# MAGIC ### How to See Tool Calls in Genie
# MAGIC
# MAGIC 1. **Query Explanation Panel**
# MAGIC    - After Genie responds, look for an **"Explain"** or **"Show Query"** button
# MAGIC    - This reveals the SQL or function that Genie executed
# MAGIC    - You'll see statements like: `SELECT * FROM get_customer_orders_table('C001')`
# MAGIC
# MAGIC 2. **Query History**
# MAGIC    - Access the query history in your Genie space
# MAGIC    - Shows all SQL queries and function calls
# MAGIC    - Useful for debugging and understanding behavior
# MAGIC
# MAGIC 3. **Result Metadata**
# MAGIC    - Some Genie interfaces show metadata about the query
# MAGIC    - Look for indicators like "Using function: get_customer_orders_table"
# MAGIC
# MAGIC ### What to Look For
# MAGIC
# MAGIC **Function is Called Correctly:**
# MAGIC - ✅ Genie identifies the right function based on your question
# MAGIC - ✅ Parameters are passed correctly (e.g., correct customer_id, dates)
# MAGIC - ✅ Results are displayed in a table format
# MAGIC
# MAGIC **Function is NOT Called:**
# MAGIC - ❌ Genie queries base tables directly instead of using your function
# MAGIC - ❌ Response says "I don't have information about that"
# MAGIC - ❌ Genie gives a generic answer without data
# MAGIC
# MAGIC **Why this happens:**
# MAGIC - Function description may be too vague
# MAGIC - Parameter names are unclear
# MAGIC - Question doesn't match function purpose
# MAGIC
# MAGIC **Fix:** Improve function COMMENT to be more specific about when to use it

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue 1: Genie Doesn't Use My Function
# MAGIC
# MAGIC **Problem:** You created a function but Genie queries base tables instead.
# MAGIC
# MAGIC **Possible Causes:**
# MAGIC 1. **Function not attached to Genie space**
# MAGIC    - Solution: Check Genie space configuration, re-attach the function
# MAGIC
# MAGIC 2. **Function description is too vague**
# MAGIC    - Bad: `COMMENT 'Gets orders'`
# MAGIC    - Good: `COMMENT 'Retrieves detailed order information for a specific customer... Use this when users ask about a customer''s orders or purchase history.'`
# MAGIC
# MAGIC 3. **Function is scalar, not table-valued**
# MAGIC    - Solution: Convert to `RETURNS TABLE(...)` format
# MAGIC
# MAGIC ### Issue 2: "Function Not Found" Error
# MAGIC
# MAGIC **Problem:** Genie says the function doesn't exist.
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Verify function is created: `SHOW FUNCTIONS IN genie_tools;`
# MAGIC - Check function name matches exactly (case-sensitive in some contexts)
# MAGIC - Ensure function is in the catalog/schema attached to Genie
# MAGIC - Grant EXECUTE permission: `GRANT EXECUTE ON FUNCTION main.genie_tools.function_name TO users;`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify functions exist
# MAGIC SHOW FUNCTIONS IN genie_tools LIKE '*_table';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 3: Function Returns Wrong Data
# MAGIC
# MAGIC **Problem:** Function is called but results don't match expectations.
# MAGIC
# MAGIC **Debugging Steps:**
# MAGIC 1. Test function directly in SQL to verify logic
# MAGIC 2. Check parameter handling (NULL values, edge cases)
# MAGIC 3. Verify JOIN conditions if using multiple tables
# MAGIC 4. Check LIMIT clauses aren't too restrictive

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test functions directly with various inputs
# MAGIC SELECT * FROM get_customer_orders_table('C001');
# MAGIC SELECT * FROM get_customer_orders_table('C999');  -- Non-existent customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 4: Permission Denied
# MAGIC
# MAGIC **Problem:** Genie can't execute the function or access underlying tables.
# MAGIC
# MAGIC **Solutions:**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant execute permission on functions (run as admin)
# MAGIC -- GRANT EXECUTE ON FUNCTION get_customer_orders_table TO `your_users`;
# MAGIC -- GRANT EXECUTE ON FUNCTION get_product_inventory_table TO `your_users`;
# MAGIC -- GRANT EXECUTE ON FUNCTION get_sales_by_period_table TO `your_users`;
# MAGIC -- GRANT EXECUTE ON FUNCTION get_customer_tier_analysis_table TO `your_users`;
# MAGIC
# MAGIC -- Grant select permission on underlying tables
# MAGIC -- GRANT SELECT ON TABLE customers TO `your_users`;
# MAGIC -- GRANT SELECT ON TABLE orders TO `your_users`;
# MAGIC -- GRANT SELECT ON TABLE products TO `your_users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Genie Functions
# MAGIC
# MAGIC ### ✅ Function Design
# MAGIC 1. **Always use table-valued returns** - `RETURNS TABLE(...)`
# MAGIC 2. **Include meaningful column names** - Users will see these in results
# MAGIC 3. **Limit result sizes** - Use LIMIT to prevent huge datasets (50-100 rows ideal)
# MAGIC 4. **Order results logically** - Most recent first, highest value first, etc.
# MAGIC
# MAGIC ### ✅ Documentation
# MAGIC 5. **Write detailed COMMENT** - Explain what, when, and why
# MAGIC 6. **Mention use cases explicitly** - "Use this when users ask about..."
# MAGIC 7. **Document parameters clearly** - Include COMMENT for each parameter
# MAGIC 8. **Describe returned columns** - What does each column represent?
# MAGIC
# MAGIC ### ✅ Data Quality
# MAGIC 9. **Handle NULL values** - Use COALESCE or CASE statements
# MAGIC 10. **Validate inputs** - Check for invalid date ranges, negative values, etc.
# MAGIC 11. **Provide default behaviors** - What happens with no parameters?
# MAGIC 12. **Include calculated fields** - Add status flags, categories, derived metrics
# MAGIC
# MAGIC ### ✅ Performance
# MAGIC 13. **Filter early** - Apply WHERE clauses before JOINs when possible
# MAGIC 14. **Use appropriate indexes** - Ensure underlying tables are optimized
# MAGIC 15. **Test with realistic data** - Verify performance with production-like volumes
# MAGIC 16. **Monitor execution time** - Target < 10 seconds for most queries
# MAGIC
# MAGIC ### ✅ Testing
# MAGIC 17. **Test directly in SQL first** - Verify logic before adding to Genie
# MAGIC 18. **Test edge cases** - NULL parameters, empty results, invalid inputs
# MAGIC 19. **Test in Genie with natural language** - Try multiple phrasings
# MAGIC 20. **Verify tool calling** - Check that Genie uses your function appropriately

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist: Before Adding Functions to Genie
# MAGIC
# MAGIC Use this checklist before attaching functions to your Genie space:
# MAGIC
# MAGIC ### Function Structure
# MAGIC - [ ] Function uses `RETURNS TABLE(...)` (not scalar)
# MAGIC - [ ] Column names are clear and business-friendly
# MAGIC - [ ] Result set includes useful columns (IDs, names, dates, amounts)
# MAGIC - [ ] LIMIT clause prevents huge result sets
# MAGIC
# MAGIC ### Documentation
# MAGIC - [ ] Function has a detailed COMMENT
# MAGIC - [ ] COMMENT explains when to use the function
# MAGIC - [ ] Parameters have COMMENT descriptions
# MAGIC - [ ] COMMENT includes example use cases
# MAGIC
# MAGIC ### Testing
# MAGIC - [ ] Function tested directly with SQL
# MAGIC - [ ] Edge cases tested (NULL, empty, invalid)
# MAGIC - [ ] Returns expected results
# MAGIC - [ ] Executes in reasonable time (< 10 seconds)
# MAGIC
# MAGIC ### Genie Configuration
# MAGIC - [ ] Function created in correct catalog/schema
# MAGIC - [ ] Permissions granted (EXECUTE on function, SELECT on tables)
# MAGIC - [ ] Function attached to Genie space
# MAGIC - [ ] Underlying tables also added to Genie space

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Scalar vs Table-Valued Functions
# MAGIC
# MAGIC Here's a side-by-side comparison to reinforce the key concepts:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCALAR FUNCTION (❌ Doesn't work with Genie)
# MAGIC CREATE OR REPLACE FUNCTION example_scalar()
# MAGIC RETURNS INT
# MAGIC COMMENT 'Returns a single count - NOT Genie compatible'
# MAGIC RETURN (SELECT COUNT(*) FROM orders);
# MAGIC
# MAGIC -- Call it
# MAGIC SELECT example_scalar() as total_orders;
# MAGIC -- Returns: Just a number (e.g., 12)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TABLE-VALUED FUNCTION (✅ Works with Genie)
# MAGIC CREATE OR REPLACE FUNCTION example_table_valued()
# MAGIC RETURNS TABLE(
# MAGIC   metric_name STRING,
# MAGIC   metric_value BIGINT
# MAGIC )
# MAGIC COMMENT 'Returns order metrics in table format - Genie compatible'
# MAGIC RETURN (
# MAGIC   SELECT 'Total Orders' as metric_name, COUNT(*) as metric_value FROM orders
# MAGIC   UNION ALL
# MAGIC   SELECT 'Completed Orders' as metric_name, COUNT(*) as metric_value 
# MAGIC   FROM orders WHERE status = 'completed'
# MAGIC   UNION ALL
# MAGIC   SELECT 'Processing Orders' as metric_name, COUNT(*) as metric_value 
# MAGIC   FROM orders WHERE status = 'processing'
# MAGIC );
# MAGIC
# MAGIC -- Call it
# MAGIC SELECT * FROM example_table_valued();
# MAGIC -- Returns: A table with columns and multiple rows

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Difference:**
# MAGIC - Scalar: Returns `12` → Hard to display, visualize, or explore
# MAGIC - Table: Returns a structured table → Can be displayed, charted, filtered, and analyzed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced: Parameterized Genie Functions
# MAGIC
# MAGIC Genie is smart enough to extract parameters from natural language and pass them to your functions.
# MAGIC
# MAGIC ### Example: How Genie Maps Language to Parameters
# MAGIC
# MAGIC **User asks:** *"Show me sales from December 2024"*
# MAGIC
# MAGIC **Genie interprets:**
# MAGIC - Function to call: `get_sales_by_period_table`
# MAGIC - Parameter 1: `start_date = '2024-12-01'`
# MAGIC - Parameter 2: `end_date = '2024-12-31'`
# MAGIC
# MAGIC **Genie executes:**
# MAGIC ```sql
# MAGIC SELECT * FROM get_sales_by_period_table('2024-12-01', '2024-12-31')
# MAGIC ```
# MAGIC
# MAGIC ### Tips for Parameter-Friendly Functions
# MAGIC
# MAGIC 1. **Use intuitive parameter names**
# MAGIC    - ✅ `customer_id`, `start_date`, `end_date`
# MAGIC    - ❌ `id`, `date1`, `date2`
# MAGIC
# MAGIC 2. **Add parameter comments**
# MAGIC    - Helps Genie understand expected formats and values
# MAGIC
# MAGIC 3. **Keep parameter count low**
# MAGIC    - 0-2 parameters: Ideal
# MAGIC    - 3-4 parameters: Acceptable
# MAGIC    - 5+ parameters: Too complex for natural language
# MAGIC
# MAGIC 4. **Provide defaults when possible**
# MAGIC    - Use CURRENT_DATE() for date parameters
# MAGIC    - Use common filters as defaults

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC If you want to remove the demo resources, run these commands:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to drop functions
# MAGIC -- DROP FUNCTION IF EXISTS get_customer_orders_table;
# MAGIC -- DROP FUNCTION IF EXISTS get_product_inventory_table;
# MAGIC -- DROP FUNCTION IF EXISTS get_sales_by_period_table;
# MAGIC -- DROP FUNCTION IF EXISTS get_customer_tier_analysis_table;
# MAGIC -- DROP FUNCTION IF EXISTS get_order_count_scalar;
# MAGIC -- DROP FUNCTION IF EXISTS example_scalar;
# MAGIC -- DROP FUNCTION IF EXISTS example_table_valued;
# MAGIC
# MAGIC -- Uncomment to drop tables
# MAGIC -- DROP TABLE IF EXISTS customers;
# MAGIC -- DROP TABLE IF EXISTS orders;
# MAGIC -- DROP TABLE IF EXISTS products;
# MAGIC
# MAGIC -- Uncomment to drop schema
# MAGIC -- DROP SCHEMA IF EXISTS genie_tools CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Genie requires table-valued functions** - Scalar functions (returning single values) won't work
# MAGIC
# MAGIC 2. **Table-valued functions use `RETURNS TABLE(...)`** - Define column names and types
# MAGIC
# MAGIC 3. **Detailed descriptions are critical** - Genie uses COMMENT to understand when to call functions
# MAGIC
# MAGIC 4. **Genie spaces are pre-configured** - Unlike AI Playground, attach functions once in space configuration
# MAGIC
# MAGIC 5. **Natural language is powerful** - Genie interprets questions and maps them to function parameters
# MAGIC
# MAGIC 6. **Tool calling is seamless** - Genie integrates function results naturally into responses
# MAGIC
# MAGIC 7. **Testing matters** - Always test functions directly in SQL before adding to Genie
# MAGIC
# MAGIC 8. **Follow-up questions work** - Genie maintains context across multiple queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC ### Expand Your Genie Space
# MAGIC
# MAGIC Consider adding more functions for:
# MAGIC - **Customer segmentation** - RFM analysis, cohort analysis
# MAGIC - **Product recommendations** - Frequently bought together, trending items
# MAGIC - **Sales forecasting** - Historical trends, seasonal patterns
# MAGIC - **Operational metrics** - Shipping times, return rates, fulfillment status
# MAGIC
# MAGIC ### Learn More
# MAGIC
# MAGIC - **AI Playground** - For more general-purpose agent development
# MAGIC - **Python Functions** - Build functions with complex logic using DatabricksFunctionClient
# MAGIC - **Mosaic AI** - Deploy production agents with Mosaic AI Agent Framework
# MAGIC
# MAGIC ### Related Labs
# MAGIC
# MAGIC - **Building Agent Tools** (`genai-building-agent-tools`) - Deep dive into AI Playground
# MAGIC - **Using Single Agents with MLflow** - Deploy and manage agents in production

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You've learned how to build Genie-compatible agent tools on Databricks.
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Open your Genie space
# MAGIC 2. Try the example queries
# MAGIC 3. Observe which functions are called
# MAGIC 4. Experiment with your own questions
# MAGIC 5. Create additional functions for your use cases
