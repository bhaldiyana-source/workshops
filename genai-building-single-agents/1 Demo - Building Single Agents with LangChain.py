# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building Single Agents with LangChain
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo demonstrates how to build intelligent single agents using **LangChain** on Databricks. You'll learn to integrate **Unity Catalog functions** as tools, configure agents with **Mosaic AI Model Serving** endpoints, implement the **ReAct pattern**, and analyze agent behavior using **MLflow tracing**.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Set up LangChain with Databricks Model Serving
# MAGIC - Create and register Unity Catalog functions as agent tools
# MAGIC - Use the `UCFunctionToolkit` to integrate UC functions with LangChain
# MAGIC - Build and configure a ReAct agent for tool calling
# MAGIC - Execute agent queries and interpret results
# MAGIC - Analyze agent execution using MLflow traces
# MAGIC - Debug common agent issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Access to Databricks Model Serving
# MAGIC - Basic understanding of LangChain concepts
# MAGIC
# MAGIC ## Duration
# MAGIC 35-45 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install Required Packages
# MAGIC
# MAGIC First, let's install the necessary packages for building LangChain agents on Databricks.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-agents langchain langchain-community mlflow databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration: Set Up Workspace Parameters
# MAGIC
# MAGIC Configure your workspace-specific parameters.

# COMMAND ----------

import os
from pyspark.sql import SparkSession

# Get current user and workspace info
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0].replace(".", "_")

# Configure catalog and schema
CATALOG_NAME = "main"  # Change if using a different catalog
SCHEMA_NAME = f"{username}_agent_demo"

# Configure warehouse (get from your workspace)
# Navigate to SQL Warehouses and copy your warehouse ID
WAREHOUSE_ID = "YOUR_WAREHOUSE_ID"  # Update this!

print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")
print(f"Warehouse: {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Schema and Sample Data
# MAGIC
# MAGIC Let's create a sample e-commerce dataset to work with.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for our demo
# MAGIC CREATE SCHEMA IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME};
# MAGIC USE ${CATALOG_NAME}.${SCHEMA_NAME};

# COMMAND ----------

# Create customers table
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.customers (
  customer_id STRING,
  name STRING,
  email STRING,
  tier STRING,
  join_date DATE,
  total_lifetime_value DOUBLE
)
""")

# Insert sample data
customers_data = [
    ("C001", "Alice Anderson", "alice@example.com", "Gold", "2024-01-15", 1250.00),
    ("C002", "Bob Brown", "bob@example.com", "Silver", "2024-03-20", 450.00),
    ("C003", "Carol Chen", "carol@example.com", "Gold", "2024-02-10", 2100.00),
    ("C004", "David Davis", "david@example.com", "Bronze", "2024-06-05", 125.00),
    ("C005", "Emma Evans", "emma@example.com", "Silver", "2024-04-12", 780.00),
]

spark.createDataFrame(
    customers_data,
    ["customer_id", "name", "email", "tier", "join_date", "total_lifetime_value"]
).write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.customers")

print("✓ Customers table created")

# COMMAND ----------

# Create orders table
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.orders (
  order_id STRING,
  customer_id STRING,
  order_date DATE,
  total_amount DOUBLE,
  status STRING,
  product_category STRING
)
""")

# Insert sample data
orders_data = [
    ("O001", "C001", "2024-12-01", 150.00, "completed", "Electronics"),
    ("O002", "C001", "2024-12-15", 200.00, "completed", "Home"),
    ("O003", "C001", "2025-01-05", 175.00, "shipped", "Electronics"),
    ("O004", "C002", "2024-12-10", 89.99, "completed", "Books"),
    ("O005", "C002", "2025-01-03", 125.50, "processing", "Electronics"),
    ("O006", "C003", "2024-11-20", 300.00, "completed", "Home"),
    ("O007", "C003", "2024-12-28", 450.00, "completed", "Electronics"),
    ("O008", "C003", "2025-01-08", 275.00, "shipped", "Fashion"),
    ("O009", "C004", "2024-12-30", 45.00, "completed", "Books"),
    ("O010", "C005", "2025-01-02", 199.99, "processing", "Electronics"),
]

spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "order_date", "total_amount", "status", "product_category"]
).write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.orders")

print("✓ Orders table created")

# COMMAND ----------

# Create inventory table
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.inventory (
  product_id STRING,
  product_name STRING,
  category STRING,
  stock_quantity INT,
  price DOUBLE,
  reorder_level INT
)
""")

# Insert sample data
inventory_data = [
    ("P001", "Wireless Headphones", "Electronics", 45, 79.99, 20),
    ("P002", "Smart Watch", "Electronics", 12, 299.99, 10),
    ("P003", "Coffee Maker", "Home", 30, 89.99, 15),
    ("P004", "Python Programming Book", "Books", 5, 49.99, 10),
    ("P005", "Winter Jacket", "Fashion", 8, 149.99, 5),
    ("P006", "Desk Lamp", "Home", 50, 34.99, 20),
]

spark.createDataFrame(
    inventory_data,
    ["product_id", "product_name", "category", "stock_quantity", "price", "reorder_level"]
).write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.inventory")

print("✓ Inventory table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog Functions as Tools
# MAGIC
# MAGIC Now let's create SQL functions that our agent can use as tools. These functions will be discoverable and callable by the agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 1: Get Customer Information

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.get_customer_info(customer_id STRING)
RETURNS STRUCT<customer_id: STRING, name: STRING, email: STRING, tier: STRING, join_date: DATE, total_lifetime_value: DOUBLE>
LANGUAGE SQL
COMMENT 'Retrieves complete information about a customer by their customer ID. Use this when you need to look up customer details, tier status, or lifetime value. Input: customer_id (e.g., "C001"). Returns: customer information including name, email, tier, join date, and total lifetime value.'
RETURN (
  SELECT STRUCT(customer_id, name, email, tier, join_date, total_lifetime_value)
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.customers
  WHERE customer_id = get_customer_info.customer_id
  LIMIT 1
)
""")

print("✓ Function created: get_customer_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 2: Get Customer Orders

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.get_customer_orders(customer_id STRING)
RETURNS TABLE(order_id STRING, order_date DATE, total_amount DOUBLE, status STRING, product_category STRING)
LANGUAGE SQL
COMMENT 'Retrieves all orders for a specific customer. Use this to find order history, check order statuses, or calculate total spending. Input: customer_id (e.g., "C001"). Returns: table of orders with order_id, order_date, total_amount, status, and product_category.'
RETURN (
  SELECT order_id, order_date, total_amount, status, product_category
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.orders
  WHERE customer_id = get_customer_orders.customer_id
  ORDER BY order_date DESC
)
""")

print("✓ Function created: get_customer_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 3: Check Product Inventory

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.check_inventory(product_id STRING)
RETURNS STRUCT<product_id: STRING, product_name: STRING, category: STRING, stock_quantity: INT, price: DOUBLE, needs_reorder: BOOLEAN>
LANGUAGE SQL
COMMENT 'Checks inventory levels for a specific product. Use this to verify product availability, check stock levels, or determine if reordering is needed. Input: product_id (e.g., "P001"). Returns: product details including stock quantity and whether it needs reordering.'
RETURN (
  SELECT STRUCT(
    product_id, 
    product_name, 
    category, 
    stock_quantity, 
    price,
    stock_quantity <= reorder_level AS needs_reorder
  )
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.inventory
  WHERE product_id = check_inventory.product_id
  LIMIT 1
)
""")

print("✓ Function created: check_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 4: Get Low Stock Products

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.get_low_stock_products()
RETURNS TABLE(product_id STRING, product_name STRING, category STRING, stock_quantity INT, reorder_level INT)
LANGUAGE SQL
COMMENT 'Retrieves all products that are at or below their reorder level. Use this to identify which products need to be restocked. No input required. Returns: table of products with low inventory.'
RETURN (
  SELECT product_id, product_name, category, stock_quantity, reorder_level
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.inventory
  WHERE stock_quantity <= reorder_level
  ORDER BY stock_quantity ASC
)
""")

print("✓ Function created: get_low_stock_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Functions
# MAGIC
# MAGIC Let's verify our functions work correctly before using them with the agent.

# COMMAND ----------

# Test get_customer_info
result = spark.sql(f"SELECT {CATALOG_NAME}.{SCHEMA_NAME}.get_customer_info('C001') AS customer_info")
display(result)

# COMMAND ----------

# Test get_customer_orders
result = spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.get_customer_orders('C001')")
display(result)

# COMMAND ----------

# Test check_inventory
result = spark.sql(f"SELECT {CATALOG_NAME}.{SCHEMA_NAME}.check_inventory('P001') AS inventory_info")
display(result)

# COMMAND ----------

# Test get_low_stock_products
result = spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.get_low_stock_products()")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Set Up LangChain with Model Serving
# MAGIC
# MAGIC Configure LangChain to use Databricks Foundation Model APIs.

# COMMAND ----------

from langchain_community.chat_models import ChatDatabricks
from databricks.agents import UCFunctionToolkit
import mlflow

# Enable MLflow autologging for LangChain
mlflow.langchain.autolog()

# Configure the LLM using Databricks Foundation Models
# Using Meta Llama 3.1 70B Instruct - good balance of capability and speed
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    temperature=0.1,  # Lower temperature for more consistent tool calling
    max_tokens=2000
)

print("✓ LLM configured with Databricks Model Serving")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Unity Catalog Functions as Tools
# MAGIC
# MAGIC Use the `UCFunctionToolkit` to automatically convert our UC functions into LangChain tools.

# COMMAND ----------

# Create toolkit with our Unity Catalog functions
toolkit = UCFunctionToolkit(
    warehouse_id=WAREHOUSE_ID,
    function_names=[
        f"{CATALOG_NAME}.{SCHEMA_NAME}.get_customer_info",
        f"{CATALOG_NAME}.{SCHEMA_NAME}.get_customer_orders",
        f"{CATALOG_NAME}.{SCHEMA_NAME}.check_inventory",
        f"{CATALOG_NAME}.{SCHEMA_NAME}.get_low_stock_products",
    ]
)

# Get the tools
tools = toolkit.tools

print(f"✓ Loaded {len(tools)} tools from Unity Catalog:")
for tool in tools:
    print(f"  - {tool.name}: {tool.description[:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create the ReAct Agent
# MAGIC
# MAGIC Build a ReAct (Reasoning + Acting) agent that can use our tools.

# COMMAND ----------

from langchain.agents import AgentExecutor, create_react_agent
from langchain.prompts import PromptTemplate

# Define the ReAct prompt template
react_prompt = PromptTemplate.from_template("""
You are a helpful AI assistant for an e-commerce company. You have access to tools that can query customer information, orders, and inventory.

Your goal is to answer user questions accurately using the available tools. Always use tools to get factual information rather than making assumptions.

TOOLS:
------
You have access to the following tools:

{tools}

To use a tool, use the following format:

Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action

When you have enough information to answer the question, use this format:

Thought: Do I need to use a tool? No
Final Answer: [your answer here]

Begin!

Question: {input}
Thought: {agent_scratchpad}
""")

# Create the agent
agent = create_react_agent(
    llm=llm,
    tools=tools,
    prompt=react_prompt
)

# Create the agent executor
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    max_iterations=5,
    handle_parsing_errors=True,
    return_intermediate_steps=True
)

print("✓ ReAct agent created and ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test the Agent
# MAGIC
# MAGIC Let's test our agent with various queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: Simple Customer Lookup

# COMMAND ----------

# Start MLflow run to track this execution
with mlflow.start_run(run_name="simple_customer_lookup") as run:
    query = "What tier is customer C001?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")
    
    # Log the query and response
    mlflow.log_param("query", query)
    mlflow.log_text(response['output'], "response.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Multi-Step Reasoning

# COMMAND ----------

with mlflow.start_run(run_name="multi_step_orders") as run:
    query = "How many orders has customer C001 placed, and what's the total value?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: Inventory Check

# COMMAND ----------

with mlflow.start_run(run_name="inventory_check") as run:
    query = "Check the inventory status for product P004. Do we need to reorder?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 4: Complex Multi-Tool Query

# COMMAND ----------

with mlflow.start_run(run_name="complex_query") as run:
    query = "Which products are low on stock? And has customer C003 ordered any electronics recently?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Analyze MLflow Traces
# MAGIC
# MAGIC Let's examine the agent's reasoning process using MLflow traces.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Trace
# MAGIC
# MAGIC To view the MLflow trace:
# MAGIC
# MAGIC 1. Click on the "Experiment" icon in the right sidebar
# MAGIC 2. Find your most recent run
# MAGIC 3. Click on the run name
# MAGIC 4. Navigate to the "Traces" tab
# MAGIC 5. Explore the tree view showing:
# MAGIC    - **Input**: The user's question
# MAGIC    - **Agent Steps**: Each reasoning and action cycle
# MAGIC    - **Tool Calls**: Function names and parameters
# MAGIC    - **Tool Results**: What the tools returned
# MAGIC    - **Output**: The final answer
# MAGIC
# MAGIC ### Key Insights from Traces
# MAGIC
# MAGIC **What to Look For:**
# MAGIC - **Number of iterations**: How many steps did the agent take?
# MAGIC - **Tool selection**: Did it choose the right tools?
# MAGIC - **Reasoning quality**: Are the thoughts logical?
# MAGIC - **Error handling**: How did it handle any issues?
# MAGIC - **Token usage**: How efficient was the execution?

# COMMAND ----------

# Get the last run for detailed analysis
experiment_id = mlflow.get_experiment_by_name("/Users/" + current_user + "/langchain-agent-demo").experiment_id if mlflow.get_experiment_by_name("/Users/" + current_user + "/langchain-agent-demo") else None

if experiment_id:
    runs = mlflow.search_runs(experiment_ids=[experiment_id], order_by=["start_time DESC"], max_results=5)
    display(runs[["run_id", "start_time", "params.query", "status"]])
else:
    print("No experiment found yet. Run some agent queries first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Advanced Agent Configuration
# MAGIC
# MAGIC Let's explore different configuration options to optimize agent behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration 1: Higher Temperature for Creative Responses

# COMMAND ----------

# Create a more creative agent (higher temperature)
creative_llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    temperature=0.7,  # Higher for more varied responses
    max_tokens=2000
)

creative_agent = create_react_agent(creative_llm, tools, react_prompt)
creative_executor = AgentExecutor(
    agent=creative_agent,
    tools=tools,
    verbose=True,
    max_iterations=5,
    handle_parsing_errors=True
)

# Test with a creative query
query = "Give me an interesting insight about our customer C003 based on their order history."
response = creative_executor.invoke({"input": query})
print(f"\nCreative Response: {response['output']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration 2: Strict Mode (Lower Temperature)

# COMMAND ----------

# Create a very deterministic agent (very low temperature)
strict_llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    temperature=0.0,  # Most deterministic
    max_tokens=2000
)

strict_agent = create_react_agent(strict_llm, tools, react_prompt)
strict_executor = AgentExecutor(
    agent=strict_agent,
    tools=tools,
    verbose=True,
    max_iterations=5,
    handle_parsing_errors=True
)

# Test with same query twice to see consistency
query = "What is the tier of customer C001?"
response1 = strict_executor.invoke({"input": query})
response2 = strict_executor.invoke({"input": query})

print(f"\nResponse 1: {response1['output']}")
print(f"Response 2: {response2['output']}")
print(f"Identical: {response1['output'] == response2['output']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Error Handling and Edge Cases
# MAGIC
# MAGIC Let's see how the agent handles various edge cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test: Non-existent Customer

# COMMAND ----------

query = "What orders does customer C999 have?"
response = agent_executor.invoke({"input": query})
print(f"\nResponse: {response['output']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test: Ambiguous Query

# COMMAND ----------

query = "What's the status?"
response = agent_executor.invoke({"input": query})
print(f"\nResponse: {response['output']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test: Out of Scope Query

# COMMAND ----------

query = "What's the weather today?"
response = agent_executor.invoke({"input": query})
print(f"\nResponse: {response['output']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Best Practices Demonstrated
# MAGIC
# MAGIC Let's review the best practices we've applied in this demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Best Practices Applied
# MAGIC
# MAGIC **1. Clear Function Descriptions**
# MAGIC - Each UC function has a detailed COMMENT explaining its purpose
# MAGIC - Includes when to use it and what it returns
# MAGIC - Specifies expected input format with examples
# MAGIC
# MAGIC **2. Focused Functions**
# MAGIC - Each function does one thing well
# MAGIC - `get_customer_info` only gets customer data
# MAGIC - `get_customer_orders` only gets orders
# MAGIC - No multi-purpose "do everything" functions
# MAGIC
# MAGIC **3. Appropriate Model Selection**
# MAGIC - Using Llama 3.1 70B for good reasoning capability
# MAGIC - Temperature tuned based on use case
# MAGIC - Max tokens set to reasonable limit
# MAGIC
# MAGIC **4. MLflow Tracing Enabled**
# MAGIC - `mlflow.langchain.autolog()` captures all executions
# MAGIC - Each test run is tracked separately
# MAGIC - Traces available for debugging and analysis
# MAGIC
# MAGIC **5. Error Handling**
# MAGIC - `handle_parsing_errors=True` in AgentExecutor
# MAGIC - Max iterations set to prevent runaway execution
# MAGIC - Verbose mode for development/debugging
# MAGIC
# MAGIC **6. Structured Testing**
# MAGIC - Test cases progress from simple to complex
# MAGIC - Edge cases explicitly tested
# MAGIC - Results logged and traceable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Issues and Debugging
# MAGIC
# MAGIC ### Issue 1: Agent Doesn't Call Tools
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Agent tries to answer without using tools
# MAGIC - Makes up information
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```python
# MAGIC # Add explicit instruction in system prompt
# MAGIC prompt = "You MUST use the available tools to get factual information. Never make assumptions about data."
# MAGIC
# MAGIC # Try a more capable model
# MAGIC llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-405b-instruct")
# MAGIC ```
# MAGIC
# MAGIC ### Issue 2: Parsing Errors
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Agent output doesn't match expected format
# MAGIC - "Could not parse LLM output" errors
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```python
# MAGIC # Already enabled in our agent:
# MAGIC agent_executor = AgentExecutor(
# MAGIC     agent=agent,
# MAGIC     tools=tools,
# MAGIC     handle_parsing_errors=True  # This helps!
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Issue 3: Tool Execution Failures
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - SQL errors from UC functions
# MAGIC - Permission denied errors
# MAGIC
# MAGIC **Solutions:**
# MAGIC ```sql
# MAGIC -- Verify function exists
# MAGIC SHOW FUNCTIONS IN catalog.schema;
# MAGIC
# MAGIC -- Check permissions
# MAGIC SHOW GRANTS ON FUNCTION catalog.schema.function_name;
# MAGIC
# MAGIC -- Test function directly
# MAGIC SELECT catalog.schema.function_name('test_input');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Built
# MAGIC
# MAGIC 1. ✅ Created Unity Catalog functions as reusable tools
# MAGIC 2. ✅ Configured LangChain with Databricks Model Serving
# MAGIC 3. ✅ Built a ReAct agent with tool calling capabilities
# MAGIC 4. ✅ Tested with various query complexities
# MAGIC 5. ✅ Analyzed execution using MLflow traces
# MAGIC 6. ✅ Explored different configurations and edge cases
# MAGIC
# MAGIC ### Key Learnings
# MAGIC
# MAGIC **UCFunctionToolkit**
# MAGIC - Seamlessly integrates UC functions with LangChain
# MAGIC - Automatically converts function metadata to tool schemas
# MAGIC - Handles execution and result formatting
# MAGIC
# MAGIC **ReAct Pattern**
# MAGIC - Alternates between reasoning and acting
# MAGIC - Effective for multi-step problems
# MAGIC - Transparent decision-making process
# MAGIC
# MAGIC **MLflow Tracing**
# MAGIC - Essential for understanding agent behavior
# MAGIC - Helps debug tool selection issues
# MAGIC - Tracks performance metrics
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Continue to **Demo 2: Building Single Agents with DSPy** to learn about the declarative approach to agent development and compare it with LangChain.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment and run to remove demo resources.

# COMMAND ----------

# # Drop the schema and all its contents
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME} CASCADE")
# print(f"✓ Cleaned up schema: {CATALOG_NAME}.{SCHEMA_NAME}")
