# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Unity Catalog Functions as Agent Tools
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture provides a deep dive into Unity Catalog (UC) Functions and how they serve as tools for AI agents on Databricks. We'll explore the differences between SQL and Python functions, learn about registration methods, and understand best practices for creating effective agent tools.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand Unity Catalog Functions and their role in agent systems
# MAGIC - Differentiate between SQL and Python UC functions
# MAGIC - Recognize function metadata and schema requirements
# MAGIC - Apply security and governance principles to agent tools
# MAGIC - Follow best practices for function design
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are Unity Catalog Functions?
# MAGIC
# MAGIC ### Definition
# MAGIC **Unity Catalog Functions** are user-defined functions (UDFs) that are:
# MAGIC - **Registered** in Unity Catalog's three-level namespace (`catalog.schema.function`)
# MAGIC - **Persisted** with metadata including parameters, return types, and descriptions
# MAGIC - **Governed** by Unity Catalog's access control and audit mechanisms
# MAGIC - **Discoverable** through catalog exploration and metadata queries
# MAGIC
# MAGIC ### Why Use UC Functions for Agents?
# MAGIC
# MAGIC 1. **Centralized Registry**
# MAGIC    - All tools in one place
# MAGIC    - Easy to discover and manage
# MAGIC    - Consistent namespace across workspace
# MAGIC
# MAGIC 2. **Metadata-Rich**
# MAGIC    - Function descriptions guide agent behavior
# MAGIC    - Parameter schemas enable validation
# MAGIC    - Return type information aids reasoning
# MAGIC
# MAGIC 3. **Governed Access**
# MAGIC    - Grant/revoke permissions per function
# MAGIC    - Audit who uses which tools
# MAGIC    - Enforce data access policies
# MAGIC
# MAGIC 4. **Reusability**
# MAGIC    - Share functions across multiple agents
# MAGIC    - Version control through updates
# MAGIC    - Consistent behavior across applications

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL vs. Python Functions
# MAGIC
# MAGIC Unity Catalog supports two types of functions for agents:
# MAGIC
# MAGIC ### SQL Functions
# MAGIC
# MAGIC **What they are:**
# MAGIC - Functions defined using SQL syntax
# MAGIC - Execute SQL queries and transformations
# MAGIC - Work directly with tables and views
# MAGIC
# MAGIC **Best for:**
# MAGIC - Data retrieval from tables
# MAGIC - Aggregations and analytics
# MAGIC - Simple transformations
# MAGIC - Joining and filtering data
# MAGIC
# MAGIC **Example Use Cases:**
# MAGIC - Get customer order history
# MAGIC - Calculate sales metrics
# MAGIC - Check inventory levels
# MAGIC - Find recent transactions
# MAGIC
# MAGIC **Limitations:**
# MAGIC - Limited to SQL operations
# MAGIC - No external API calls
# MAGIC - Restricted to data in Unity Catalog
# MAGIC
# MAGIC ### Python Functions
# MAGIC
# MAGIC **What they are:**
# MAGIC - Functions written in Python
# MAGIC - Can include complex logic and computations
# MAGIC - Support external library imports (with restrictions)
# MAGIC
# MAGIC **Best for:**
# MAGIC - Complex business logic
# MAGIC - Mathematical computations
# MAGIC - Data validation and parsing
# MAGIC - Multi-step processing
# MAGIC
# MAGIC **Example Use Cases:**
# MAGIC - Calculate compound metrics
# MAGIC - Validate and format data
# MAGIC - Apply business rules
# MAGIC - Parse and transform strings
# MAGIC
# MAGIC **Limitations:**
# MAGIC - Cannot make external API calls directly
# MAGIC - Limited to available libraries
# MAGIC - No file system access

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Registration Methods
# MAGIC
# MAGIC There are two primary methods for registering UC functions:
# MAGIC
# MAGIC ### Method 1: SQL DDL Syntax
# MAGIC
# MAGIC Use `CREATE FUNCTION` statements:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION catalog.schema.get_order_count(customer_id STRING)
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns the total number of orders for a given customer'
# MAGIC RETURN (
# MAGIC   SELECT COUNT(*) 
# MAGIC   FROM catalog.schema.orders 
# MAGIC   WHERE customer_id = customer_id
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Best for:**
# MAGIC - SQL functions
# MAGIC - Simple, declarative definitions
# MAGIC - Quick prototyping
# MAGIC
# MAGIC ### Method 2: DatabricksFunctionClient (Python)
# MAGIC
# MAGIC Use the Python SDK to register functions programmatically:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.catalog import *
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC w.functions.create(
# MAGIC     name="catalog.schema.calculate_discount",
# MAGIC     input_params=[
# MAGIC         FunctionParameterInfo(
# MAGIC             name="price",
# MAGIC             type_text="DOUBLE",
# MAGIC             comment="Original price"
# MAGIC         ),
# MAGIC         FunctionParameterInfo(
# MAGIC             name="discount_pct",
# MAGIC             type_text="DOUBLE",
# MAGIC             comment="Discount percentage"
# MAGIC         )
# MAGIC     ],
# MAGIC     data_type="DOUBLE",
# MAGIC     full_data_type="DOUBLE",
# MAGIC     routine_body="EXTERNAL",
# MAGIC     routine_definition="return price * (1 - discount_pct / 100)",
# MAGIC     language="PYTHON",
# MAGIC     comment="Calculates discounted price"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Best for:**
# MAGIC - Python functions
# MAGIC - Programmatic registration
# MAGIC - Batch creation of multiple functions
# MAGIC - CI/CD pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Metadata and Schema
# MAGIC
# MAGIC ### Required Components
# MAGIC
# MAGIC Every UC function must have:
# MAGIC
# MAGIC 1. **Name** (fully qualified)
# MAGIC    - Format: `catalog.schema.function_name`
# MAGIC    - Must be unique within the schema
# MAGIC    - Use descriptive, verb-based names
# MAGIC
# MAGIC 2. **Parameters**
# MAGIC    - Name and type for each parameter
# MAGIC    - Optional: parameter comments/descriptions
# MAGIC    - Support for multiple parameters
# MAGIC
# MAGIC 3. **Return Type**
# MAGIC    - Data type of the return value
# MAGIC    - Can be scalar or complex types
# MAGIC    - Must match actual return value
# MAGIC
# MAGIC 4. **Language**
# MAGIC    - SQL or PYTHON
# MAGIC    - Determines execution environment
# MAGIC
# MAGIC 5. **Definition/Body**
# MAGIC    - The actual function logic
# MAGIC    - SQL query or Python code
# MAGIC
# MAGIC ### Optional but Recommended
# MAGIC
# MAGIC 6. **Comment/Description**
# MAGIC    - Critical for agents to understand usage
# MAGIC    - Should explain when and why to use the function
# MAGIC    - Include any important constraints or behaviors
# MAGIC
# MAGIC 7. **Parameter Comments**
# MAGIC    - Help agents provide correct inputs
# MAGIC    - Explain expected formats or ranges

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Naming Conventions
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC **Use verb-based names:**
# MAGIC - ✅ `get_customer_orders`
# MAGIC - ✅ `calculate_revenue`
# MAGIC - ✅ `check_inventory`
# MAGIC - ❌ `customer_orders` (not clear it's a function)
# MAGIC - ❌ `revenue` (ambiguous)
# MAGIC
# MAGIC **Be specific and descriptive:**
# MAGIC - ✅ `get_orders_by_date_range`
# MAGIC - ❌ `get_data` (too vague)
# MAGIC - ✅ `calculate_total_with_tax`
# MAGIC - ❌ `calculate` (incomplete)
# MAGIC
# MAGIC **Use consistent prefixes:**
# MAGIC - `get_*` for retrieval operations
# MAGIC - `calculate_*` for computations
# MAGIC - `check_*` for validations
# MAGIC - `find_*` for search operations
# MAGIC - `validate_*` for validation logic
# MAGIC
# MAGIC **Avoid abbreviations:**
# MAGIC - ✅ `get_customer_information`
# MAGIC - ❌ `get_cust_info`
# MAGIC - ✅ `calculate_annual_revenue`
# MAGIC - ❌ `calc_ann_rev`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Effective Function Descriptions
# MAGIC
# MAGIC The function description is crucial for agent behavior. The agent uses it to decide when to call the function.
# MAGIC
# MAGIC ### Description Template
# MAGIC
# MAGIC ```
# MAGIC [Action Verb] [What] [Optional: When/Where] [Optional: Returns]
# MAGIC ```
# MAGIC
# MAGIC ### Examples
# MAGIC
# MAGIC **Poor Description:**
# MAGIC ```
# MAGIC "Gets orders"
# MAGIC ```
# MAGIC - Too vague
# MAGIC - Doesn't explain what orders or when to use
# MAGIC
# MAGIC **Better Description:**
# MAGIC ```
# MAGIC "Retrieves all orders for a customer from the orders table"
# MAGIC ```
# MAGIC - Clear action
# MAGIC - Specifies scope (customer's orders)
# MAGIC - Mentions data source
# MAGIC
# MAGIC **Best Description:**
# MAGIC ```
# MAGIC "Retrieves all orders placed by a specific customer in the last 90 days, 
# MAGIC including order ID, date, total amount, and status. Use this when a user 
# MAGIC asks about their order history or recent purchases."
# MAGIC ```
# MAGIC - Very specific about what's returned
# MAGIC - Includes time constraint
# MAGIC - Explicitly states when to use it
# MAGIC - Lists returned fields
# MAGIC
# MAGIC ### Guidelines
# MAGIC
# MAGIC 1. **Be Specific**: Include details about what data is returned
# MAGIC 2. **Set Expectations**: Mention any limitations or filters
# MAGIC 3. **Provide Context**: Explain when the tool should be used
# MAGIC 4. **Use Examples**: Reference common use cases
# MAGIC 5. **Mention Side Effects**: Note if function modifies data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Design Best Practices
# MAGIC
# MAGIC ### 1. Keep It Simple
# MAGIC
# MAGIC **Minimize the number of parameters:**
# MAGIC - ✅ 1-3 parameters (ideal)
# MAGIC - ⚠️ 4-5 parameters (acceptable)
# MAGIC - ❌ 6+ parameters (too complex)
# MAGIC
# MAGIC If you need many parameters, consider:
# MAGIC - Breaking into multiple functions
# MAGIC - Using default values
# MAGIC - Creating a more specific function
# MAGIC
# MAGIC ### 2. Use Appropriate Types
# MAGIC
# MAGIC **Match types to use cases:**
# MAGIC - Use `STRING` for IDs, names, free text
# MAGIC - Use `INT` or `BIGINT` for counts, quantities
# MAGIC - Use `DOUBLE` or `DECIMAL` for monetary values
# MAGIC - Use `DATE` or `TIMESTAMP` for temporal data
# MAGIC - Use `BOOLEAN` for flags and toggles
# MAGIC
# MAGIC ### 3. Make Parameters Intuitive
# MAGIC
# MAGIC **Clear parameter names:**
# MAGIC - ✅ `customer_id` (clear)
# MAGIC - ❌ `id` (ambiguous)
# MAGIC - ✅ `start_date` (descriptive)
# MAGIC - ❌ `date` (which date?)
# MAGIC
# MAGIC ### 4. Provide Parameter Comments
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION get_sales_by_region(
# MAGIC   region_code STRING COMMENT 'Two-letter region code (e.g., US, EU, AP)',
# MAGIC   start_date DATE COMMENT 'Start of date range (inclusive)',
# MAGIC   end_date DATE COMMENT 'End of date range (inclusive)'
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return Type Considerations
# MAGIC
# MAGIC ### Scalar Returns
# MAGIC
# MAGIC **Single values** are easiest for agents to work with:
# MAGIC - `INT`, `BIGINT` - counts, IDs
# MAGIC - `DOUBLE`, `DECIMAL` - metrics, amounts
# MAGIC - `STRING` - names, descriptions, formatted output
# MAGIC - `BOOLEAN` - yes/no, true/false answers
# MAGIC - `DATE`, `TIMESTAMP` - temporal values
# MAGIC
# MAGIC **Example:**
# MAGIC ```sql
# MAGIC -- Returns a single number
# MAGIC CREATE FUNCTION get_order_count(customer_id STRING)
# MAGIC RETURNS INT
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC ### Table Returns
# MAGIC
# MAGIC **Multiple rows** for complex queries:
# MAGIC - Use `RETURNS TABLE(...)` syntax
# MAGIC - Define column names and types
# MAGIC - Good for list-based results
# MAGIC
# MAGIC **Example:**
# MAGIC ```sql
# MAGIC -- Returns multiple rows
# MAGIC CREATE FUNCTION get_recent_orders(customer_id STRING)
# MAGIC RETURNS TABLE(
# MAGIC   order_id STRING,
# MAGIC   order_date DATE,
# MAGIC   total_amount DOUBLE,
# MAGIC   status STRING
# MAGIC )
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC ### Choosing Return Types
# MAGIC
# MAGIC **Use scalar returns when:**
# MAGIC - Answering yes/no questions
# MAGIC - Returning single metrics
# MAGIC - Providing counts or sums
# MAGIC
# MAGIC **Use table returns when:**
# MAGIC - Listing multiple items
# MAGIC - Providing detailed records
# MAGIC - Supporting further analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security and Governance
# MAGIC
# MAGIC ### Access Control
# MAGIC
# MAGIC UC Functions inherit Unity Catalog's permission model:
# MAGIC
# MAGIC **Function-Level Permissions:**
# MAGIC ```sql
# MAGIC -- Grant execute permission
# MAGIC GRANT EXECUTE ON FUNCTION catalog.schema.get_customer_orders 
# MAGIC TO `data-analysts`;
# MAGIC
# MAGIC -- Revoke permission
# MAGIC REVOKE EXECUTE ON FUNCTION catalog.schema.get_customer_orders 
# MAGIC FROM `data-analysts`;
# MAGIC ```
# MAGIC
# MAGIC **Considerations:**
# MAGIC - Agents need EXECUTE permission on functions
# MAGIC - Functions inherit user's permissions on underlying data
# MAGIC - Use schema-level grants for groups of functions
# MAGIC
# MAGIC ### Data Access Patterns
# MAGIC
# MAGIC **Functions execute with caller's permissions:**
# MAGIC - If user can't access `orders` table, function will fail
# MAGIC - This provides security: functions don't bypass permissions
# MAGIC - Use row-level and column-level security for fine-grained control
# MAGIC
# MAGIC ### Audit and Lineage
# MAGIC
# MAGIC **Unity Catalog tracks:**
# MAGIC - Who created the function
# MAGIC - When it was last modified
# MAGIC - Who has executed it
# MAGIC - What data sources it accesses
# MAGIC
# MAGIC **View audit logs:**
# MAGIC ```sql
# MAGIC -- See function metadata
# MAGIC DESCRIBE FUNCTION EXTENDED catalog.schema.function_name;
# MAGIC
# MAGIC -- View lineage
# MAGIC -- Use Databricks Unity Catalog UI to see data lineage graphs
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling in Functions
# MAGIC
# MAGIC ### SQL Functions
# MAGIC
# MAGIC **Handle NULL values:**
# MAGIC ```sql
# MAGIC CREATE FUNCTION calculate_discount(price DOUBLE, discount_pct DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC RETURN (
# MAGIC   CASE 
# MAGIC     WHEN price IS NULL OR discount_pct IS NULL THEN NULL
# MAGIC     WHEN discount_pct < 0 OR discount_pct > 100 THEN NULL
# MAGIC     ELSE price * (1 - discount_pct / 100)
# MAGIC   END
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Validate inputs:**
# MAGIC ```sql
# MAGIC CREATE FUNCTION get_orders_in_range(start_date DATE, end_date DATE)
# MAGIC RETURNS TABLE(order_id STRING, order_date DATE, amount DOUBLE)
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     order_id,
# MAGIC     order_date,
# MAGIC     amount
# MAGIC   FROM orders
# MAGIC   WHERE 
# MAGIC     CASE 
# MAGIC       WHEN start_date > end_date THEN FALSE
# MAGIC       ELSE order_date BETWEEN start_date AND end_date
# MAGIC     END
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Python Functions
# MAGIC
# MAGIC **Use try-except blocks:**
# MAGIC ```python
# MAGIC def calculate_percentage_change(old_value: float, new_value: float) -> float:
# MAGIC     """Calculates percentage change between two values"""
# MAGIC     try:
# MAGIC         if old_value == 0:
# MAGIC             return None  # Avoid division by zero
# MAGIC         return ((new_value - old_value) / old_value) * 100
# MAGIC     except (TypeError, ValueError):
# MAGIC         return None  # Handle invalid inputs
# MAGIC ```
# MAGIC
# MAGIC **Return meaningful defaults:**
# MAGIC - Use `None` for missing/invalid data
# MAGIC - Return empty strings or zero when appropriate
# MAGIC - Document error behavior in function description

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC ### SQL Function Optimization
# MAGIC
# MAGIC **1. Use Appropriate Filters**
# MAGIC ```sql
# MAGIC -- Good: Filter early
# MAGIC CREATE FUNCTION get_recent_orders(customer_id STRING)
# MAGIC RETURNS TABLE(...)
# MAGIC RETURN (
# MAGIC   SELECT * 
# MAGIC   FROM orders 
# MAGIC   WHERE customer_id = customer_id
# MAGIC     AND order_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC   LIMIT 100  -- Prevent huge result sets
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **2. Leverage Indexes**
# MAGIC - Ensure tables have appropriate indexes
# MAGIC - Filter on indexed columns when possible
# MAGIC - Use partition columns in filters
# MAGIC
# MAGIC **3. Limit Result Sizes**
# MAGIC ```sql
# MAGIC -- Always use LIMIT for potentially large results
# MAGIC RETURN (
# MAGIC   SELECT * FROM large_table
# MAGIC   WHERE condition
# MAGIC   ORDER BY relevance DESC
# MAGIC   LIMIT 50  -- Return top 50 results
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Python Function Optimization
# MAGIC
# MAGIC **1. Minimize Computation**
# MAGIC ```python
# MAGIC # Good: Simple, fast computation
# MAGIC def calculate_total_with_tax(subtotal: float, tax_rate: float) -> float:
# MAGIC     return subtotal * (1 + tax_rate)
# MAGIC ```
# MAGIC
# MAGIC **2. Avoid Complex Logic**
# MAGIC - Keep functions focused and simple
# MAGIC - If logic is complex, consider breaking into multiple functions
# MAGIC - Avoid nested loops and expensive operations
# MAGIC
# MAGIC ### General Guidelines
# MAGIC
# MAGIC - **Target execution time**: < 5 seconds (ideal), < 30 seconds (maximum)
# MAGIC - **Result set size**: < 1000 rows (ideal), < 10,000 rows (maximum)
# MAGIC - **Memory usage**: Keep working set small
# MAGIC - **Test with realistic data**: Performance test before deployment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Versioning and Updates
# MAGIC
# MAGIC ### Updating Functions
# MAGIC
# MAGIC **Use CREATE OR REPLACE:**
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION catalog.schema.get_order_count(customer_id STRING)
# MAGIC RETURNS INT
# MAGIC COMMENT 'Updated version: now includes cancelled orders in count'
# MAGIC RETURN (
# MAGIC   SELECT COUNT(*) 
# MAGIC   FROM orders 
# MAGIC   WHERE customer_id = customer_id
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Version Control Best Practices
# MAGIC
# MAGIC **1. Document changes in comments:**
# MAGIC ```sql
# MAGIC COMMENT 'v2: Added date range filtering. Updated 2025-01-09'
# MAGIC ```
# MAGIC
# MAGIC **2. Maintain backward compatibility:**
# MAGIC - Avoid changing parameter names or types
# MAGIC - Add new parameters as optional (with defaults)
# MAGIC - Don't change return type structures
# MAGIC
# MAGIC **3. Test before updating:**
# MAGIC - Test new version in AI Playground
# MAGIC - Verify existing agents still work
# MAGIC - Update documentation
# MAGIC
# MAGIC **4. Communicate changes:**
# MAGIC - Notify agent developers of updates
# MAGIC - Document breaking changes
# MAGIC - Provide migration guidance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing Functions
# MAGIC
# MAGIC ### Method 1: Direct SQL Testing
# MAGIC
# MAGIC ```sql
# MAGIC -- Test a function directly
# MAGIC SELECT catalog.schema.get_order_count('customer_123');
# MAGIC
# MAGIC -- Test with various inputs
# MAGIC SELECT 
# MAGIC   'customer_123' as customer_id,
# MAGIC   catalog.schema.get_order_count('customer_123') as order_count
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'customer_456' as customer_id,
# MAGIC   catalog.schema.get_order_count('customer_456') as order_count;
# MAGIC ```
# MAGIC
# MAGIC ### Method 2: Python Testing
# MAGIC
# MAGIC ```python
# MAGIC # Test Python function
# MAGIC result = spark.sql("""
# MAGIC   SELECT catalog.schema.calculate_discount(100.0, 10.0) as discounted_price
# MAGIC """).collect()[0]['discounted_price']
# MAGIC
# MAGIC print(f"Discounted price: {result}")
# MAGIC
# MAGIC # Test multiple cases
# MAGIC test_cases = [
# MAGIC     (100.0, 10.0, 90.0),
# MAGIC     (50.0, 20.0, 40.0),
# MAGIC     (75.0, 0.0, 75.0)
# MAGIC ]
# MAGIC
# MAGIC for price, discount, expected in test_cases:
# MAGIC     result = spark.sql(f"""
# MAGIC       SELECT catalog.schema.calculate_discount({price}, {discount})
# MAGIC     """).collect()[0][0]
# MAGIC     assert result == expected, f"Failed for {price}, {discount}"
# MAGIC ```
# MAGIC
# MAGIC ### Method 3: AI Playground Testing
# MAGIC
# MAGIC **Most important for agent tools:**
# MAGIC 1. Open AI Playground in Databricks
# MAGIC 2. Attach your function as a tool
# MAGIC 3. Send test queries that should trigger the function
# MAGIC 4. Verify:
# MAGIC    - Function is called at appropriate times
# MAGIC    - Parameters are passed correctly
# MAGIC    - Results are used properly by agent
# MAGIC    - Error cases are handled gracefully

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Patterns for Agent Tools
# MAGIC
# MAGIC ### Pattern 1: Data Retrieval
# MAGIC
# MAGIC **Purpose**: Fetch specific data from tables
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION get_customer_info(customer_id STRING)
# MAGIC RETURNS TABLE(
# MAGIC   customer_id STRING,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   tier STRING
# MAGIC )
# MAGIC COMMENT 'Retrieves basic customer information including tier status'
# MAGIC RETURN (
# MAGIC   SELECT customer_id, name, email, tier
# MAGIC   FROM customers
# MAGIC   WHERE customer_id = customer_id
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Aggregation/Calculation
# MAGIC
# MAGIC **Purpose**: Compute metrics or statistics
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION calculate_customer_lifetime_value(customer_id STRING)
# MAGIC RETURNS DOUBLE
# MAGIC COMMENT 'Calculates total revenue from a customer across all orders'
# MAGIC RETURN (
# MAGIC   SELECT COALESCE(SUM(total_amount), 0.0)
# MAGIC   FROM orders
# MAGIC   WHERE customer_id = customer_id
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Validation/Check
# MAGIC
# MAGIC **Purpose**: Verify conditions or validate data
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION is_product_in_stock(product_id STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Checks if a product is currently in stock (quantity > 0)'
# MAGIC RETURN (
# MAGIC   SELECT CASE 
# MAGIC     WHEN COUNT(*) > 0 AND SUM(quantity) > 0 THEN TRUE 
# MAGIC     ELSE FALSE 
# MAGIC   END
# MAGIC   FROM inventory
# MAGIC   WHERE product_id = product_id
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 4: Search/Filter
# MAGIC
# MAGIC **Purpose**: Find items matching criteria
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION search_products_by_category(category STRING)
# MAGIC RETURNS TABLE(
# MAGIC   product_id STRING,
# MAGIC   name STRING,
# MAGIC   price DOUBLE
# MAGIC )
# MAGIC COMMENT 'Finds all products in a given category, ordered by popularity'
# MAGIC RETURN (
# MAGIC   SELECT product_id, name, price
# MAGIC   FROM products
# MAGIC   WHERE category = category
# MAGIC     AND active = TRUE
# MAGIC   ORDER BY sales_count DESC
# MAGIC   LIMIT 20
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with AI Playground
# MAGIC
# MAGIC ### Attaching Functions to Agents
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Navigate to AI Playground in Databricks
# MAGIC 2. Select a foundation model (e.g., DBRX, Llama 3, GPT-4)
# MAGIC 3. Click "Add Tools" or "Attach Functions"
# MAGIC 4. Browse Unity Catalog to find your functions
# MAGIC 5. Select functions to attach
# MAGIC 6. Test with natural language queries
# MAGIC
# MAGIC ### Observing Tool Usage
# MAGIC
# MAGIC **The Playground shows:**
# MAGIC - When a tool is called
# MAGIC - What parameters were passed
# MAGIC - What the tool returned
# MAGIC - How the agent used the results
# MAGIC
# MAGIC **Example interaction:**
# MAGIC ```
# MAGIC User: "How many orders does customer C001 have?"
# MAGIC
# MAGIC Agent thinking: I should use the get_order_count function
# MAGIC
# MAGIC Tool call: get_order_count(customer_id="C001")
# MAGIC
# MAGIC Tool result: 15
# MAGIC
# MAGIC Agent response: "Customer C001 has 15 orders."
# MAGIC ```
# MAGIC
# MAGIC ### Debugging in Playground
# MAGIC
# MAGIC **Common issues:**
# MAGIC - Agent doesn't call the function → Improve description
# MAGIC - Agent calls with wrong parameters → Add parameter comments
# MAGIC - Function errors → Check input validation
# MAGIC - Results not used well → Simplify return structure

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Function Design
# MAGIC 1. ✅ **Single Responsibility**: Each function does one thing well
# MAGIC 2. ✅ **Clear Naming**: Verb-based, descriptive names
# MAGIC 3. ✅ **Detailed Descriptions**: Explain what, when, and why
# MAGIC 4. ✅ **Simple Parameters**: Minimize count, use clear types
# MAGIC 5. ✅ **Appropriate Returns**: Match return type to use case
# MAGIC
# MAGIC ### Performance
# MAGIC 6. ✅ **Fast Execution**: Target < 5 seconds
# MAGIC 7. ✅ **Limited Results**: Use LIMIT to cap result sizes
# MAGIC 8. ✅ **Efficient Queries**: Filter early, use indexes
# MAGIC
# MAGIC ### Quality
# MAGIC 9. ✅ **Error Handling**: Graceful handling of edge cases
# MAGIC 10. ✅ **Input Validation**: Check parameters before use
# MAGIC 11. ✅ **Testing**: Test directly AND in AI Playground
# MAGIC
# MAGIC ### Governance
# MAGIC 12. ✅ **Access Control**: Grant appropriate permissions
# MAGIC 13. ✅ **Documentation**: Maintain comments and metadata
# MAGIC 14. ✅ **Versioning**: Track changes, maintain compatibility

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Now that you understand Unity Catalog Functions, let's see them in action:
# MAGIC
# MAGIC ### Demo 2: Building SQL Functions as Agent Tools
# MAGIC - Create SQL-based UC functions
# MAGIC - Register them using SQL DDL
# MAGIC - Test in AI Playground
# MAGIC - Common patterns and examples
# MAGIC
# MAGIC ### Demo 3: Building Python Functions as Agent Tools
# MAGIC - Create Python-based UC functions
# MAGIC - Use DatabricksFunctionClient()
# MAGIC - Advanced patterns
# MAGIC - Testing and debugging
# MAGIC
# MAGIC ### Lab 4: Hands-On Exercises
# MAGIC - Build your own agent tools
# MAGIC - Troubleshoot common issues
# MAGIC - Test with real scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Unity Catalog Functions** provide a governed, discoverable registry for agent tools
# MAGIC
# MAGIC 2. **SQL functions** are best for data retrieval; **Python functions** for complex logic
# MAGIC
# MAGIC 3. **Function descriptions** are critical - the agent uses them to decide when to call functions
# MAGIC
# MAGIC 4. **Parameters** should be simple, well-typed, and clearly documented
# MAGIC
# MAGIC 5. **Performance matters** - keep functions fast and limit result sizes
# MAGIC
# MAGIC 6. **Test in AI Playground** to see how agents actually use your functions
# MAGIC
# MAGIC 7. **Security** is built-in - functions inherit Unity Catalog's access controls

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Ready to continue?** Open the next notebook: **2 Demo - Building SQL Functions as Agent Tools with AI Playground**
# MAGIC
