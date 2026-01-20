# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building SQL Functions as Agent Tools with AI Playground
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo walks through the process of creating SQL-based Unity Catalog functions that serve as tools for AI agents. You'll learn how to write, register, test, and troubleshoot SQL functions, then see them in action in the AI Playground.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Create SQL UC functions using CREATE FUNCTION syntax
# MAGIC - Write effective function descriptions for agents
# MAGIC - Test functions both directly and in AI Playground
# MAGIC - Identify and fix common issues with SQL functions
# MAGIC - Apply best practices for SQL-based agent tools
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Serverless or classic compute cluster
# MAGIC - Basic SQL knowledge
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Catalog and Schema
# MAGIC
# MAGIC First, we'll set up our workspace with a catalog and schema for our functions.
# MAGIC
# MAGIC **Note:** Replace `main` with your catalog name if using a different catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog (skip if using existing catalog like 'main')
# MAGIC -- CREATE CATALOG IF NOT EXISTS agent_tools_demo;
# MAGIC
# MAGIC -- Use your catalog
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC -- Create a schema for our agent tools
# MAGIC CREATE SCHEMA IF NOT EXISTS agent_tools;
# MAGIC
# MAGIC -- Use the schema
# MAGIC USE SCHEMA agent_tools;
# MAGIC
# MAGIC -- Verify current location
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample Data
# MAGIC
# MAGIC Let's create some sample tables to work with. These represent a simple e-commerce system.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customers table
# MAGIC CREATE OR REPLACE TABLE customers (
# MAGIC   customer_id STRING,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   tier STRING,
# MAGIC   join_date DATE
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO customers VALUES
# MAGIC   ('C001', 'Alice Anderson', 'alice@example.com', 'Gold', '2024-01-15'),
# MAGIC   ('C002', 'Bob Brown', 'bob@example.com', 'Silver', '2024-03-20'),
# MAGIC   ('C003', 'Carol Chen', 'carol@example.com', 'Gold', '2024-02-10'),
# MAGIC   ('C004', 'David Davis', 'david@example.com', 'Bronze', '2024-06-05'),
# MAGIC   ('C005', 'Emma Evans', 'emma@example.com', 'Silver', '2024-04-12');
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
# MAGIC   status STRING
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO orders VALUES
# MAGIC   ('O001', 'C001', '2024-12-01', 150.00, 'completed'),
# MAGIC   ('O002', 'C001', '2024-12-15', 200.00, 'completed'),
# MAGIC   ('O003', 'C001', '2025-01-05', 175.00, 'shipped'),
# MAGIC   ('O004', 'C002', '2024-12-10', 89.99, 'completed'),
# MAGIC   ('O005', 'C002', '2025-01-03', 125.50, 'processing'),
# MAGIC   ('O006', 'C003', '2024-11-20', 300.00, 'completed'),
# MAGIC   ('O007', 'C003', '2024-12-28', 450.00, 'completed'),
# MAGIC   ('O008', 'C004', '2025-01-08', 75.00, 'shipped'),
# MAGIC   ('O009', 'C005', '2024-12-05', 220.00, 'completed');
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
# MAGIC   stock_quantity INT
# MAGIC );
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO products VALUES
# MAGIC   ('P001', 'Laptop Pro 15', 'Electronics', 1299.99, 25),
# MAGIC   ('P002', 'Wireless Mouse', 'Electronics', 29.99, 150),
# MAGIC   ('P003', 'Office Chair', 'Furniture', 249.99, 40),
# MAGIC   ('P004', 'Desk Lamp', 'Furniture', 45.99, 75),
# MAGIC   ('P005', 'USB-C Cable', 'Accessories', 12.99, 200),
# MAGIC   ('P006', 'Monitor 27inch', 'Electronics', 399.99, 30),
# MAGIC   ('P007', 'Keyboard Mechanical', 'Electronics', 129.99, 60),
# MAGIC   ('P008', 'Standing Desk', 'Furniture', 499.99, 15);
# MAGIC
# MAGIC SELECT * FROM products ORDER BY category, name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Simple Count Function
# MAGIC
# MAGIC Let's start with the simplest type of function: returning a count.
# MAGIC
# MAGIC ### Business Need
# MAGIC Users often ask "How many orders does customer X have?" - we need a function that answers this.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_customer_order_count(customer_id STRING)
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns the total number of orders placed by a specific customer. Use this when users ask about how many orders a customer has made.'
# MAGIC RETURN (
# MAGIC   SELECT COUNT(*)
# MAGIC   FROM orders
# MAGIC   WHERE customer_id = get_customer_order_count.customer_id
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function Directly
# MAGIC
# MAGIC Before using in AI Playground, always test functions directly to ensure they work correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with valid customer
# MAGIC SELECT get_customer_order_count('C001') as order_count;
# MAGIC
# MAGIC -- Expected: 3 orders for C001

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with multiple customers
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   get_customer_order_count(customer_id) as total_orders
# MAGIC FROM customers
# MAGIC ORDER BY total_orders DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with non-existent customer
# MAGIC SELECT get_customer_order_count('C999') as order_count;
# MAGIC
# MAGIC -- Expected: 0 (no orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Now test in AI Playground:**
# MAGIC
# MAGIC 1. Open AI Playground in Databricks
# MAGIC 2. Attach the function `main.agent_tools.get_customer_order_count`
# MAGIC 3. Try these queries:
# MAGIC    - "How many orders does customer C001 have?"
# MAGIC    - "Which customer has placed the most orders?"
# MAGIC    - "Tell me the order count for Alice Anderson"
# MAGIC
# MAGIC **Observe:**
# MAGIC - When does the agent call the function?
# MAGIC - What parameters does it pass?
# MAGIC - How does it use the results?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Customer Lifetime Value Calculation
# MAGIC
# MAGIC A more complex aggregation that sums amounts across orders.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION calculate_customer_lifetime_value(customer_id STRING)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Calculates the total revenue (sum of all order amounts) from a specific customer across all their orders. Returns 0.0 if customer has no orders. Use this when asked about customer value, total spending, or revenue from a customer.'
# MAGIC RETURN (
# MAGIC   SELECT COALESCE(SUM(total_amount), 0.0)
# MAGIC   FROM orders
# MAGIC   WHERE customer_id = calculate_customer_lifetime_value.customer_id
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with customer C001
# MAGIC SELECT calculate_customer_lifetime_value('C001') as lifetime_value;
# MAGIC
# MAGIC -- Expected: 525.00 (150 + 200 + 175)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare all customers
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.name,
# MAGIC   c.tier,
# MAGIC   calculate_customer_lifetime_value(c.customer_id) as lifetime_value
# MAGIC FROM customers c
# MAGIC ORDER BY lifetime_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test queries:**
# MAGIC - "What is the lifetime value of customer C003?"
# MAGIC - "Which customer has spent the most money?"
# MAGIC - "Show me the total revenue from Carol Chen"
# MAGIC - "Compare the lifetime value of Gold tier customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Returning Multiple Rows (Table Function)
# MAGIC
# MAGIC Functions can return multiple rows, which is useful for listing items.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_customer_recent_orders(customer_id STRING)
# MAGIC RETURNS TABLE(
# MAGIC   order_id STRING,
# MAGIC   order_date DATE,
# MAGIC   total_amount DOUBLE,
# MAGIC   status STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Retrieves the most recent orders (up to 10) for a specific customer, including order ID, date, amount, and status. Orders are sorted by date (newest first). Use this when users ask about recent purchases, order history, or want to see what a customer has ordered.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     order_id,
# MAGIC     order_date,
# MAGIC     total_amount,
# MAGIC     status
# MAGIC   FROM orders
# MAGIC   WHERE customer_id = get_customer_recent_orders.customer_id
# MAGIC   ORDER BY order_date DESC
# MAGIC   LIMIT 10
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with customer C001
# MAGIC SELECT * FROM get_customer_recent_orders('C001');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with customer C003
# MAGIC SELECT * FROM get_customer_recent_orders('C003');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test queries:**
# MAGIC - "Show me the recent orders for customer C001"
# MAGIC - "What has Alice Anderson ordered recently?"
# MAGIC - "List the order history for customer C002"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Boolean Check Function
# MAGIC
# MAGIC Functions that return TRUE/FALSE are useful for validation and checks.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION is_product_in_stock(product_id STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Checks whether a product is currently in stock (has quantity greater than 0). Returns TRUE if in stock, FALSE if out of stock or product does not exist. Use this when users ask about product availability, stock status, or if they can order an item.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     CASE 
# MAGIC       WHEN COUNT(*) = 0 THEN FALSE
# MAGIC       WHEN SUM(stock_quantity) > 0 THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END
# MAGIC   FROM products
# MAGIC   WHERE product_id = is_product_in_stock.product_id
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check specific products
# MAGIC SELECT 
# MAGIC   'P001' as product_id,
# MAGIC   is_product_in_stock('P001') as in_stock
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'P999' as product_id,
# MAGIC   is_product_in_stock('P999') as in_stock;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check all products
# MAGIC SELECT 
# MAGIC   p.product_id,
# MAGIC   p.name,
# MAGIC   p.stock_quantity,
# MAGIC   is_product_in_stock(p.product_id) as in_stock
# MAGIC FROM products p
# MAGIC ORDER BY p.product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test queries:**
# MAGIC - "Is product P001 in stock?"
# MAGIC - "Can I order the Laptop Pro 15?"
# MAGIC - "Check if the Standing Desk is available"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Search/Filter Function
# MAGIC
# MAGIC Functions that search and filter data based on criteria.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION find_products_by_category(category_name STRING)
# MAGIC RETURNS TABLE(
# MAGIC   product_id STRING,
# MAGIC   name STRING,
# MAGIC   price DOUBLE,
# MAGIC   stock_quantity INT
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Finds all products in a specific category (e.g., Electronics, Furniture, Accessories). Returns up to 20 products sorted by name. Use this when users want to browse products by category, see what is available in a category, or compare items within a category.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     product_id,
# MAGIC     name,
# MAGIC     price,
# MAGIC     stock_quantity
# MAGIC   FROM products
# MAGIC   WHERE category = find_products_by_category.category_name
# MAGIC   ORDER BY name
# MAGIC   LIMIT 20
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find electronics
# MAGIC SELECT * FROM find_products_by_category('Electronics');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find furniture
# MAGIC SELECT * FROM find_products_by_category('Furniture');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Try non-existent category
# MAGIC SELECT * FROM find_products_by_category('Clothing');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test queries:**
# MAGIC - "Show me all electronics products"
# MAGIC - "What furniture items do you have?"
# MAGIC - "List products in the Accessories category"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Date Range Query
# MAGIC
# MAGIC Functions with multiple parameters for filtering by date ranges.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_orders_in_date_range(
# MAGIC   start_date DATE COMMENT 'Start of date range (inclusive)',
# MAGIC   end_date DATE COMMENT 'End of date range (inclusive)'
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   order_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   order_date DATE,
# MAGIC   total_amount DOUBLE,
# MAGIC   status STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Retrieves all orders placed within a specific date range (inclusive of both start and end dates). Returns up to 100 orders sorted by date (newest first). Use this when users ask about orders in a time period, sales during specific dates, or recent order activity.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_date,
# MAGIC     total_amount,
# MAGIC     status
# MAGIC   FROM orders
# MAGIC   WHERE order_date BETWEEN get_orders_in_date_range.start_date 
# MAGIC                        AND get_orders_in_date_range.end_date
# MAGIC   ORDER BY order_date DESC
# MAGIC   LIMIT 100
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get December 2024 orders
# MAGIC SELECT * FROM get_orders_in_date_range('2024-12-01', '2024-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get January 2025 orders
# MAGIC SELECT * FROM get_orders_in_date_range('2025-01-01', '2025-01-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get orders from last 7 days (using date functions)
# MAGIC SELECT * FROM get_orders_in_date_range(
# MAGIC   CURRENT_DATE() - INTERVAL 7 DAYS,
# MAGIC   CURRENT_DATE()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test queries:**
# MAGIC - "Show me orders from December 2024"
# MAGIC - "What orders were placed in January 2025?"
# MAGIC - "List orders between December 1st and December 15th, 2024"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Complex Aggregation with Grouping
# MAGIC
# MAGIC More sophisticated analysis combining multiple tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_sales_summary_by_customer_tier()
# MAGIC RETURNS TABLE(
# MAGIC   tier STRING,
# MAGIC   customer_count BIGINT,
# MAGIC   total_orders BIGINT,
# MAGIC   total_revenue DOUBLE,
# MAGIC   avg_order_value DOUBLE
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Provides a summary of sales metrics grouped by customer tier (Gold, Silver, Bronze). Shows customer count, total orders, total revenue, and average order value for each tier. Use this when analyzing customer segments, comparing tier performance, or understanding revenue by customer group.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     c.tier,
# MAGIC     COUNT(DISTINCT c.customer_id) as customer_count,
# MAGIC     COUNT(o.order_id) as total_orders,
# MAGIC     COALESCE(SUM(o.total_amount), 0.0) as total_revenue,
# MAGIC     COALESCE(AVG(o.total_amount), 0.0) as avg_order_value
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
# MAGIC SELECT * FROM get_sales_summary_by_customer_tier();

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test queries:**
# MAGIC - "Show me sales summary by customer tier"
# MAGIC - "Which customer tier generates the most revenue?"
# MAGIC - "Compare Gold and Silver customers"
# MAGIC - "What's the average order value for each tier?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Issues and Troubleshooting
# MAGIC
# MAGIC ### Issue 1: Agent Doesn't Call the Function
# MAGIC
# MAGIC **Problem:** The function exists but the agent doesn't use it.
# MAGIC
# MAGIC **Example of a poor function:**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BAD: Vague description
# MAGIC CREATE OR REPLACE FUNCTION bad_get_orders(id STRING)
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Gets orders'
# MAGIC RETURN (SELECT COUNT(*) FROM orders WHERE customer_id = id);

# COMMAND ----------

# MAGIC %md
# MAGIC **Why it's bad:**
# MAGIC - Function name is vague ("bad_get_orders")
# MAGIC - Parameter name is ambiguous ("id")
# MAGIC - Description is too brief ("Gets orders")
# MAGIC - Agent won't understand when to use it
# MAGIC
# MAGIC **Solution: Be specific and descriptive** (see get_customer_order_count above)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 2: Function Returns Wrong Results
# MAGIC
# MAGIC **Problem:** Function logic has bugs or edge cases not handled.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BAD: Doesn't handle NULL or missing data
# MAGIC CREATE OR REPLACE FUNCTION bad_calculate_discount(price DOUBLE, discount DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN (price * (1 - discount / 100));
# MAGIC
# MAGIC -- Test with NULL values
# MAGIC SELECT bad_calculate_discount(NULL, 10) as result;
# MAGIC -- Returns NULL instead of handling gracefully

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOOD: Handles edge cases
# MAGIC CREATE OR REPLACE FUNCTION good_calculate_discount(price DOUBLE, discount DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Calculates the discounted price given an original price and discount percentage (0-100). Returns NULL if inputs are invalid.'
# MAGIC RETURN (
# MAGIC   CASE 
# MAGIC     WHEN price IS NULL OR discount IS NULL THEN NULL
# MAGIC     WHEN price < 0 OR discount < 0 OR discount > 100 THEN NULL
# MAGIC     ELSE price * (1 - discount / 100)
# MAGIC   END
# MAGIC );
# MAGIC
# MAGIC -- Test edge cases
# MAGIC SELECT 
# MAGIC   good_calculate_discount(100, 10) as normal,
# MAGIC   good_calculate_discount(NULL, 10) as null_price,
# MAGIC   good_calculate_discount(100, NULL) as null_discount,
# MAGIC   good_calculate_discount(100, -5) as negative_discount,
# MAGIC   good_calculate_discount(100, 150) as over_100_discount;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 3: Function is Too Slow
# MAGIC
# MAGIC **Problem:** Function takes too long to execute, causing timeouts.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BAD: No LIMIT, could return millions of rows
# MAGIC CREATE OR REPLACE FUNCTION bad_get_all_orders()
# MAGIC RETURNS TABLE(order_id STRING, customer_id STRING, total_amount DOUBLE)
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   SELECT order_id, customer_id, total_amount
# MAGIC   FROM orders
# MAGIC   -- No LIMIT! Could be huge!
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOOD: Limits results and filters appropriately
# MAGIC CREATE OR REPLACE FUNCTION good_get_recent_orders()
# MAGIC RETURNS TABLE(order_id STRING, customer_id STRING, total_amount DOUBLE, order_date DATE)
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns the 50 most recent orders from the last 90 days.'
# MAGIC RETURN (
# MAGIC   SELECT order_id, customer_id, total_amount, order_date
# MAGIC   FROM orders
# MAGIC   WHERE order_date >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC   ORDER BY order_date DESC
# MAGIC   LIMIT 50
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 4: Ambiguous Parameter Names
# MAGIC
# MAGIC **Problem:** Parameter names could mean multiple things.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BAD: Ambiguous parameters
# MAGIC CREATE OR REPLACE FUNCTION bad_get_data(id STRING, type STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC RETURN 'placeholder';
# MAGIC -- What kind of ID? What type? Too vague!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOOD: Clear, specific parameters
# MAGIC CREATE OR REPLACE FUNCTION good_get_customer_name(customer_id STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Retrieves the full name of a customer given their customer ID.'
# MAGIC RETURN (
# MAGIC   SELECT name FROM customers WHERE customer_id = good_get_customer_name.customer_id
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Checklist
# MAGIC
# MAGIC Use this checklist when creating SQL functions for agents:
# MAGIC
# MAGIC ### ✅ Naming
# MAGIC - [ ] Use verb-based function names (get_, calculate_, check_, find_)
# MAGIC - [ ] Names are descriptive and self-explanatory
# MAGIC - [ ] Parameter names are clear and specific
# MAGIC
# MAGIC ### ✅ Documentation
# MAGIC - [ ] Function has a detailed COMMENT describing what it does
# MAGIC - [ ] Description explains WHEN to use the function
# MAGIC - [ ] Parameters have comments (for multi-parameter functions)
# MAGIC - [ ] Description mentions what is returned
# MAGIC
# MAGIC ### ✅ Implementation
# MAGIC - [ ] Handles NULL values appropriately
# MAGIC - [ ] Validates input parameters
# MAGIC - [ ] Uses LIMIT to cap result sizes
# MAGIC - [ ] Filters data early in the query
# MAGIC - [ ] Returns appropriate data types
# MAGIC
# MAGIC ### ✅ Testing
# MAGIC - [ ] Tested with valid inputs
# MAGIC - [ ] Tested with edge cases (NULL, empty, invalid)
# MAGIC - [ ] Tested in AI Playground with natural language
# MAGIC - [ ] Verified agent calls it at appropriate times
# MAGIC
# MAGIC ### ✅ Performance
# MAGIC - [ ] Executes in reasonable time (< 30 seconds)
# MAGIC - [ ] Result set is limited (< 1000 rows ideally)
# MAGIC - [ ] Uses indexed columns in WHERE clauses

# COMMAND ----------

# MAGIC %md
# MAGIC ## View All Functions in Schema
# MAGIC
# MAGIC You can list all functions you've created to manage them.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all functions in current schema
# MAGIC SHOW FUNCTIONS IN agent_tools;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get details about a specific function
# MAGIC DESCRIBE FUNCTION EXTENDED get_customer_order_count;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete AI Playground Workflow
# MAGIC
# MAGIC ### Step-by-Step Guide
# MAGIC
# MAGIC 1. **Navigate to AI Playground**
# MAGIC    - Click on "Machine Learning" or "AI" in the left sidebar
# MAGIC    - Select "AI Playground"
# MAGIC
# MAGIC 2. **Select a Model**
# MAGIC    - Choose a foundation model (e.g., DBRX, Llama 3, GPT-4)
# MAGIC    - Configure model parameters if needed
# MAGIC
# MAGIC 3. **Attach Tools**
# MAGIC    - Click "Add Tools" or "Attach Functions"
# MAGIC    - Browse to `main.agent_tools`
# MAGIC    - Select the functions you created:
# MAGIC      - `get_customer_order_count`
# MAGIC      - `calculate_customer_lifetime_value`
# MAGIC      - `get_customer_recent_orders`
# MAGIC      - `is_product_in_stock`
# MAGIC      - `find_products_by_category`
# MAGIC      - `get_orders_in_date_range`
# MAGIC      - `get_sales_summary_by_customer_tier`
# MAGIC
# MAGIC 4. **Test with Queries**
# MAGIC    - Start with simple queries
# MAGIC    - Observe which tools are called
# MAGIC    - Check the parameters and results
# MAGIC
# MAGIC 5. **Iterate and Improve**
# MAGIC    - If agent doesn't use a function, improve its description
# MAGIC    - If parameters are wrong, clarify parameter comments
# MAGIC    - If results are unexpected, fix the logic and re-test
# MAGIC
# MAGIC ### Example Test Queries
# MAGIC
# MAGIC **Customer Analysis:**
# MAGIC - "Who is our most valuable customer?"
# MAGIC - "Show me order history for Alice Anderson"
# MAGIC - "How many orders has customer C002 placed?"
# MAGIC
# MAGIC **Product Queries:**
# MAGIC - "Is the Laptop Pro 15 in stock?"
# MAGIC - "Show me all furniture products"
# MAGIC - "What electronics are available?"
# MAGIC
# MAGIC **Sales Analysis:**
# MAGIC - "What were the sales in December 2024?"
# MAGIC - "Compare revenue by customer tier"
# MAGIC - "Show me recent orders from the last week"
# MAGIC
# MAGIC **Complex Queries:**
# MAGIC - "Which Gold tier customer has the highest lifetime value?"
# MAGIC - "Are there any out-of-stock electronics?"
# MAGIC - "Show me orders over $200 from January 2025"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC If you want to remove the demo resources, run these commands:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to drop functions
# MAGIC -- DROP FUNCTION IF EXISTS get_customer_order_count;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_customer_lifetime_value;
# MAGIC -- DROP FUNCTION IF EXISTS get_customer_recent_orders;
# MAGIC -- DROP FUNCTION IF EXISTS is_product_in_stock;
# MAGIC -- DROP FUNCTION IF EXISTS find_products_by_category;
# MAGIC -- DROP FUNCTION IF EXISTS get_orders_in_date_range;
# MAGIC -- DROP FUNCTION IF EXISTS get_sales_summary_by_customer_tier;
# MAGIC
# MAGIC -- Uncomment to drop tables
# MAGIC -- DROP TABLE IF EXISTS customers;
# MAGIC -- DROP TABLE IF EXISTS orders;
# MAGIC -- DROP TABLE IF EXISTS products;
# MAGIC
# MAGIC -- Uncomment to drop schema
# MAGIC -- DROP SCHEMA IF EXISTS agent_tools CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **SQL functions** are perfect for data retrieval, aggregations, and simple transformations
# MAGIC
# MAGIC 2. **Detailed descriptions** are critical - the agent uses them to decide when to call functions
# MAGIC
# MAGIC 3. **Function names** should be verb-based and descriptive (get_, calculate_, check_, find_)
# MAGIC
# MAGIC 4. **Always use LIMIT** to prevent returning huge result sets
# MAGIC
# MAGIC 5. **Handle edge cases** like NULL values, empty results, and invalid inputs
# MAGIC
# MAGIC 6. **Test thoroughly** - both directly with SQL and in AI Playground
# MAGIC
# MAGIC 7. **Parameter comments** help agents understand what to pass in
# MAGIC
# MAGIC 8. **Iterate based on behavior** - if agent doesn't use a function correctly, refine the description

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Continue to the next notebook: **3 Demo - Building Python Functions as Agent Tools with AI Playground**
# MAGIC
# MAGIC You'll learn how to:
# MAGIC - Create Python-based UC functions
# MAGIC - Use DatabricksFunctionClient() for registration
# MAGIC - Implement complex logic not possible in SQL
# MAGIC - Integrate Python libraries and custom code

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You've learned how to build SQL-based agent tools on Databricks.
# MAGIC
