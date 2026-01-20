# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Unity Catalog Functions as Agent Tools
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture explores how Unity Catalog functions serve as powerful tools for AI agents. You'll learn why UC functions are ideal for agent systems, how to create SQL and Python functions with proper metadata, and best practices for governance and security.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand the benefits of using Unity Catalog functions as agent tools
# MAGIC - Create SQL functions with comprehensive metadata for agents
# MAGIC - Build Python UC functions with proper documentation
# MAGIC - Apply best practices for tool design, security, and governance
# MAGIC - Test functions in AI Playground
# MAGIC
# MAGIC ## Duration
# MAGIC 60 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed "00_Lecture_Foundations_of_Agentic_AI"
# MAGIC - Access to Unity Catalog enabled workspace
# MAGIC - Permission to CREATE FUNCTION in a catalog/schema
# MAGIC - Basic SQL and Python knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Why Unity Catalog Functions for Agents?
# MAGIC
# MAGIC Unity Catalog functions provide the ideal infrastructure for agent tools on Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Key Benefits
# MAGIC
# MAGIC #### ✅ Centralized Tool Management
# MAGIC - **Single Source of Truth**: All agent capabilities defined in one place
# MAGIC - **Versioned**: Track changes and roll back if needed
# MAGIC - **Discoverable**: Agents can find and understand available tools
# MAGIC - **Reusable**: Same functions work across multiple agents
# MAGIC
# MAGIC #### ✅ Security & Governance
# MAGIC - **Fine-Grained Access Control**: Grant EXECUTE permissions per function
# MAGIC - **Audit Logging**: Track who called which functions when
# MAGIC - **Data Lineage**: See how data flows through agent operations
# MAGIC - **Compliance**: Meet regulatory requirements with governed tools
# MAGIC
# MAGIC #### ✅ Multi-Language Support
# MAGIC - **SQL Functions**: Optimized for data queries and transformations
# MAGIC - **Python Functions**: Handle complex logic and integrations
# MAGIC - **Seamless Integration**: Mix and match in same agent
# MAGIC
# MAGIC #### ✅ Built for Databricks
# MAGIC - **Native AI Playground Integration**: Test tools immediately
# MAGIC - **Model Serving Ready**: Deploy agents with their tools
# MAGIC - **Access to Delta Tables**: Query data with governance
# MAGIC - **Vector Search Integration**: Enable RAG capabilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 UC Functions vs Other Approaches
# MAGIC
# MAGIC | Approach | UC Functions | Custom Code in Agent | External APIs |
# MAGIC |----------|--------------|---------------------|---------------|
# MAGIC | **Governance** | ✅ Full UC governance | ❌ No governance | ⚠️ Limited control |
# MAGIC | **Discovery** | ✅ Automatic via UC | ❌ Manual registration | ❌ Manual registration |
# MAGIC | **Versioning** | ✅ Built-in | ❌ Manual tracking | ⚠️ External versioning |
# MAGIC | **Access Control** | ✅ Fine-grained ACLs | ❌ All or nothing | ⚠️ External auth |
# MAGIC | **Audit Trail** | ✅ Automatic logging | ❌ Custom logging | ⚠️ External logs |
# MAGIC | **Performance** | ✅ Optimized | ⚠️ Depends on code | ⚠️ Network latency |
# MAGIC | **Data Access** | ✅ Native Delta access | ✅ Via Spark | ❌ Requires export |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating SQL Functions as Tools
# MAGIC
# MAGIC SQL functions are ideal for data retrieval and transformation tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Simple Query Function
# MAGIC
# MAGIC A basic function to retrieve customer orders:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION main.agents.get_customer_orders(
# MAGIC   customer_id BIGINT 
# MAGIC   COMMENT 'The unique ID of the customer'
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   order_id BIGINT COMMENT 'Order identifier',
# MAGIC   order_date DATE COMMENT 'Date order was placed',
# MAGIC   total_amount DECIMAL(10,2) COMMENT 'Total order value in USD',
# MAGIC   status STRING COMMENT 'Order status: pending, shipped, delivered'
# MAGIC )
# MAGIC COMMENT 'Retrieves all orders for a specific customer, ordered by most recent first. Use this tool when you need to look up a customer order history.'
# MAGIC RETURN 
# MAGIC   SELECT 
# MAGIC     order_id,
# MAGIC     order_date,
# MAGIC     total_amount,
# MAGIC     status
# MAGIC   FROM main.sales.orders
# MAGIC   WHERE customer_id = customer_id
# MAGIC   ORDER BY order_date DESC;
# MAGIC ```
# MAGIC
# MAGIC **Key Elements:**
# MAGIC - ✅ Parameter COMMENT explains what input is needed
# MAGIC - ✅ Function COMMENT describes purpose and when to use
# MAGIC - ✅ RETURNS TABLE structure is clear and documented
# MAGIC - ✅ Query is focused and performant

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Aggregation Function
# MAGIC
# MAGIC Functions can perform calculations and aggregations:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION main.agents.calculate_revenue_by_region(
# MAGIC   start_date DATE COMMENT 'Start of date range (inclusive)',
# MAGIC   end_date DATE COMMENT 'End of date range (inclusive)'
# MAGIC )
# MAGIC RETURNS TABLE(
# MAGIC   region STRING COMMENT 'Geographic region',
# MAGIC   total_revenue DECIMAL(12,2) COMMENT 'Total revenue in USD',
# MAGIC   order_count BIGINT COMMENT 'Number of orders',
# MAGIC   avg_order_value DECIMAL(10,2) COMMENT 'Average order value'
# MAGIC )
# MAGIC COMMENT 'Calculates revenue metrics by region for a specified date range. Use this tool to analyze regional sales performance or compare regions.'
# MAGIC RETURN
# MAGIC   SELECT 
# MAGIC     c.region,
# MAGIC     SUM(o.total_amount) as total_revenue,
# MAGIC     COUNT(DISTINCT o.order_id) as order_count,
# MAGIC     AVG(o.total_amount) as avg_order_value
# MAGIC   FROM main.sales.orders o
# MAGIC   JOIN main.sales.customers c ON o.customer_id = c.customer_id
# MAGIC   WHERE o.order_date BETWEEN start_date AND end_date
# MAGIC   GROUP BY c.region
# MAGIC   ORDER BY total_revenue DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 SQL Function Best Practices
# MAGIC
# MAGIC Follow these patterns for agent-friendly SQL functions:
# MAGIC
# MAGIC #### Naming Convention
# MAGIC - Use **verb_noun** pattern: `get_customer_orders`, `calculate_revenue`, `find_top_products`
# MAGIC - Be descriptive and specific
# MAGIC - Avoid abbreviations
# MAGIC
# MAGIC #### Documentation
# MAGIC - **Function COMMENT**: Explain what it does and when to use it
# MAGIC - **Parameter COMMENTS**: Describe each input's purpose and format
# MAGIC - **Return COMMENTS**: Document each output column
# MAGIC
# MAGIC #### Design
# MAGIC - **Single Responsibility**: Each function does one thing well
# MAGIC - **Performance**: Use proper indexes and avoid expensive operations
# MAGIC - **Parameterization**: Never use string concatenation (SQL injection risk)
# MAGIC - **Return Structure**: Use TABLE returns for structured data
# MAGIC
# MAGIC #### Error Handling
# MAGIC ```sql
# MAGIC -- Example: Handle edge cases gracefully
# MAGIC CREATE OR REPLACE FUNCTION main.agents.get_customer_ltv(
# MAGIC   customer_id BIGINT COMMENT 'Customer ID'
# MAGIC )
# MAGIC RETURNS DECIMAL(12,2)
# MAGIC COMMENT 'Calculates customer lifetime value. Returns 0 if customer has no orders.'
# MAGIC RETURN 
# MAGIC   COALESCE(
# MAGIC     (SELECT SUM(total_amount) 
# MAGIC      FROM main.sales.orders 
# MAGIC      WHERE customer_id = customer_id),
# MAGIC     0.0
# MAGIC   );
# MAGIC ```

# COMMAND ----------

# Example: Creating SQL functions (demonstration code)

# Note: This is demonstration code showing the pattern
# In practice, you would execute these in a SQL cell

sql_function_examples = """
-- Example 1: Simple data retrieval
CREATE OR REPLACE FUNCTION main.agents.get_recent_orders(
  days_back INT COMMENT 'Number of days to look back'
)
RETURNS TABLE(order_id BIGINT, customer_name STRING, total DECIMAL(10,2))
COMMENT 'Get orders from the last N days'
RETURN 
  SELECT o.order_id, c.name, o.total_amount
  FROM main.sales.orders o
  JOIN main.sales.customers c ON o.customer_id = c.customer_id
  WHERE o.order_date >= CURRENT_DATE - INTERVAL days_back DAYS
  ORDER BY o.order_date DESC;

-- Example 2: Analytical function
CREATE OR REPLACE FUNCTION main.agents.analyze_product_performance(
  product_category STRING COMMENT 'Product category to analyze',
  quarter STRING COMMENT 'Quarter in format YYYY-Q# (e.g., 2024-Q4)'
)
RETURNS TABLE(
  product_name STRING,
  units_sold BIGINT,
  revenue DECIMAL(12,2),
  growth_pct DECIMAL(5,2)
)
COMMENT 'Analyzes product performance for a category in a specific quarter'
RETURN
  WITH current_quarter AS (
    SELECT 
      p.product_name,
      COUNT(*) as units_sold,
      SUM(o.total_amount) as revenue
    FROM main.sales.orders o
    JOIN main.sales.products p ON o.product_id = p.product_id
    WHERE p.category = product_category
      AND DATE_FORMAT(o.order_date, 'yyyy-Q') = quarter
    GROUP BY p.product_name
  )
  SELECT 
    product_name,
    units_sold,
    revenue,
    0.0 as growth_pct  -- Simplified for example
  FROM current_quarter
  ORDER BY revenue DESC;

-- Example 3: Search function
CREATE OR REPLACE FUNCTION main.agents.search_customers(
  search_term STRING COMMENT 'Name or email to search for'
)
RETURNS TABLE(
  customer_id BIGINT,
  name STRING,
  email STRING,
  total_orders BIGINT,
  lifetime_value DECIMAL(12,2)
)
COMMENT 'Search for customers by name or email (partial match supported)'
RETURN
  SELECT 
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as lifetime_value
  FROM main.sales.customers c
  LEFT JOIN main.sales.orders o ON c.customer_id = o.customer_id
  WHERE LOWER(c.name) LIKE LOWER(CONCAT('%', search_term, '%'))
     OR LOWER(c.email) LIKE LOWER(CONCAT('%', search_term, '%'))
  GROUP BY c.customer_id, c.name, c.email
  ORDER BY lifetime_value DESC;
"""

print("SQL Function Examples:")
print("=" * 60)
print(sql_function_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating Python Functions as Tools
# MAGIC
# MAGIC Python functions enable complex logic and external integrations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Simple Python Function
# MAGIC
# MAGIC Basic pattern for Python UC functions:

# COMMAND ----------

# Example: Simple Python UC function
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col

def calculate_customer_lifetime_value(customer_id: int) -> float:
    """
    Calculates the lifetime value of a customer by summing all their orders.
    
    This function queries the orders table and returns the total amount spent
    by the specified customer across all time.
    
    Args:
        customer_id (int): The unique identifier of the customer
        
    Returns:
        float: The total lifetime value in USD. Returns 0.0 if customer has no orders.
        
    Example:
        >>> calculate_customer_lifetime_value(12345)
        15420.50
    """
    try:
        spark = SparkSession.builder.getOrCreate()
        
        # Query orders for this customer
        result = spark.sql(f"""
            SELECT COALESCE(SUM(total_amount), 0.0) as ltv
            FROM main.sales.orders
            WHERE customer_id = {customer_id}
        """).collect()
        
        ltv = float(result[0]['ltv']) if result else 0.0
        return ltv
        
    except Exception as e:
        print(f"Error calculating LTV for customer {customer_id}: {str(e)}")
        return 0.0

# Test the function
print("Example: Calculate Customer Lifetime Value")
print("=" * 60)
print("Function: calculate_customer_lifetime_value(customer_id: int) -> float")
print("\nDocstring:")
print(calculate_customer_lifetime_value.__doc__)
print("\nThis function would be registered as a UC function using:")
print("CREATE FUNCTION main.agents.calculate_customer_ltv ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Using Databricks SDK for Function Creation
# MAGIC
# MAGIC Create UC functions programmatically:

# COMMAND ----------

# Example: Creating UC function with Databricks SDK
from databricks.sdk import WorkspaceClient

def register_python_function_example():
    """
    Demonstrates how to register a Python function as a UC function
    using the Databricks SDK
    """
    
    # This is example code showing the pattern
    example_code = """
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    FunctionInfo, 
    FunctionParameterInfo,
    ColumnInfo
)

w = WorkspaceClient()

# Define function metadata
function_info = FunctionInfo(
    name="main.agents.calculate_customer_ltv",
    catalog_name="main",
    schema_name="agents",
    
    # Input parameters
    input_params=FunctionParameterInfoList([
        FunctionParameterInfo(
            name="customer_id",
            type_text="INT",
            type_name="INT",
            position=0,
            comment="The unique identifier of the customer"
        )
    ]),
    
    # Return type
    data_type="DECIMAL(12,2)",
    full_data_type="DECIMAL(12,2)",
    return_params=ColumnInfo(
        name="lifetime_value",
        type_text="DECIMAL(12,2)",
        type_name="DECIMAL",
        comment="Total customer lifetime value in USD"
    ),
    
    # Function details
    comment="Calculates customer lifetime value by summing all order amounts. Returns 0.0 if customer has no orders.",
    routine_body="EXTERNAL",
    routine_definition="return calculate_customer_lifetime_value(customer_id)",
    external_language="PYTHON",
    is_deterministic=True,
    sql_data_access="READS_SQL_DATA"
)

# Create the function
w.functions.create(function_info=function_info)
print("Function created successfully!")
"""
    
    return example_code

print("Creating UC Functions with Databricks SDK:")
print("=" * 60)
print(register_python_function_example())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Python Function with External API
# MAGIC
# MAGIC Integrate with external services:

# COMMAND ----------

# Example: Python function with external API integration
import requests
from typing import Dict, Any, Optional

def enrich_company_data(company_name: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Enriches company data using an external API service.
    
    This function calls a third-party API to gather additional information
    about a company, such as industry, size, and revenue data.
    
    Args:
        company_name (str): Name of the company to look up
        api_key (str, optional): API key for authentication. 
                                If None, retrieves from Databricks secrets.
    
    Returns:
        dict: Dictionary containing company information with keys:
            - name (str): Company name
            - industry (str): Industry classification
            - employee_count (int): Number of employees
            - revenue_usd (float): Annual revenue in USD
            - founded_year (int): Year company was founded
            - error (str, optional): Error message if lookup failed
    
    Example:
        >>> enrich_company_data("Acme Corporation")
        {
            "name": "Acme Corporation",
            "industry": "Technology",
            "employee_count": 5000,
            "revenue_usd": 500000000.0,
            "founded_year": 1995
        }
    """
    try:
        # Get API key from secrets if not provided
        if api_key is None:
            # In production: api_key = dbutils.secrets.get("api_keys", "company_data_api")
            api_key = "demo_key_12345"
        
        # Call external API (example - would use real API)
        # response = requests.get(
        #     f"https://api.companydata.com/v1/companies/{company_name}",
        #     headers={"Authorization": f"Bearer {api_key}"},
        #     timeout=10
        # )
        
        # Simulated response for demonstration
        if company_name.lower() == "acme corporation":
            company_data = {
                "name": "Acme Corporation",
                "industry": "Technology",
                "employee_count": 5000,
                "revenue_usd": 500000000.0,
                "founded_year": 1995,
                "headquarters": "San Francisco, CA"
            }
        else:
            company_data = {
                "name": company_name,
                "error": "Company not found in database"
            }
        
        return company_data
        
    except requests.exceptions.Timeout:
        return {
            "name": company_name,
            "error": "API request timed out"
        }
    except requests.exceptions.RequestException as e:
        return {
            "name": company_name,
            "error": f"API request failed: {str(e)}"
        }
    except Exception as e:
        return {
            "name": company_name,
            "error": f"Unexpected error: {str(e)}"
        }

# Test the function
print("Example: Company Data Enrichment")
print("=" * 60)

test_company = "Acme Corporation"
result = enrich_company_data(test_company)

print(f"Company: {test_company}")
print(f"Result:")
for key, value in result.items():
    print(f"  {key}: {value}")

print("\nKey Features:")
print("  ✓ External API integration")
print("  ✓ Secrets management for credentials")
print("  ✓ Error handling and timeouts")
print("  ✓ Structured return format")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Python Function Best Practices
# MAGIC
# MAGIC #### Type Hints and Documentation
# MAGIC - Use type hints for all parameters and returns
# MAGIC - Write comprehensive docstrings (Google or NumPy style)
# MAGIC - Include examples in docstrings
# MAGIC
# MAGIC #### Error Handling
# MAGIC ```python
# MAGIC def safe_function(param: str) -> Dict[str, Any]:
# MAGIC     """Always handle errors gracefully"""
# MAGIC     try:
# MAGIC         # Main logic
# MAGIC         result = process(param)
# MAGIC         return {"success": True, "data": result}
# MAGIC     except ValueError as e:
# MAGIC         return {"success": False, "error": f"Invalid input: {str(e)}"}
# MAGIC     except Exception as e:
# MAGIC         return {"success": False, "error": f"Unexpected error: {str(e)}"}
# MAGIC ```
# MAGIC
# MAGIC #### Security
# MAGIC - **Never hardcode credentials** - use dbutils.secrets
# MAGIC - **Validate inputs** before using them
# MAGIC - **Sanitize outputs** to prevent data leakage
# MAGIC - **Use timeouts** for external API calls
# MAGIC
# MAGIC #### Performance
# MAGIC - **Cache expensive operations** when appropriate
# MAGIC - **Use connection pooling** for databases
# MAGIC - **Limit result sizes** to prevent memory issues
# MAGIC - **Add timeouts** to prevent hanging

# COMMAND ----------

# Example: Comprehensive Python function with all best practices
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

class FunctionResult:
    """Standard result format for UC functions"""
    def __init__(self, success: bool, data: Any = None, error: str = None):
        self.success = success
        self.data = data
        self.error = error
    
    def to_dict(self) -> Dict:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "timestamp": datetime.now().isoformat()
        }

def analyze_customer_segment(
    segment_name: str,
    date_range_days: int = 90,
    min_order_value: float = 0.0
) -> Dict[str, Any]:
    """
    Analyzes customer behavior and metrics for a specific segment.
    
    This function performs comprehensive analysis on a customer segment,
    including purchase patterns, lifetime value, and engagement metrics.
    
    Args:
        segment_name (str): Name of customer segment to analyze 
                           (e.g., 'enterprise', 'smb', 'consumer')
        date_range_days (int, optional): Number of days to analyze. 
                                        Default is 90. Max is 365.
        min_order_value (float, optional): Minimum order value to include.
                                          Default is 0.0.
    
    Returns:
        dict: Analysis results with keys:
            - success (bool): Whether analysis completed successfully
            - data (dict): Analysis metrics including:
                - segment (str): Segment name
                - customer_count (int): Number of customers
                - total_revenue (float): Total revenue in USD
                - avg_order_value (float): Average order value
                - repeat_rate (float): Percentage of repeat customers
                - churn_risk_count (int): Customers at risk of churning
            - error (str, optional): Error message if analysis failed
            - timestamp (str): ISO format timestamp of analysis
    
    Example:
        >>> analyze_customer_segment("enterprise", date_range_days=30)
        {
            "success": True,
            "data": {
                "segment": "enterprise",
                "customer_count": 150,
                "total_revenue": 2500000.0,
                "avg_order_value": 16666.67,
                "repeat_rate": 85.5,
                "churn_risk_count": 12
            },
            "timestamp": "2024-01-15T10:30:00"
        }
    
    Raises:
        ValueError: If segment_name is empty or date_range_days is invalid
    """
    try:
        # Input validation
        if not segment_name or not segment_name.strip():
            raise ValueError("segment_name cannot be empty")
        
        if date_range_days < 1 or date_range_days > 365:
            raise ValueError("date_range_days must be between 1 and 365")
        
        if min_order_value < 0:
            raise ValueError("min_order_value cannot be negative")
        
        # Normalize input
        segment_name = segment_name.strip().lower()
        
        # Simulate analysis (in production, would query actual data)
        analysis_data = {
            "segment": segment_name,
            "customer_count": 150,
            "total_revenue": 2500000.0,
            "avg_order_value": 16666.67,
            "repeat_rate": 85.5,
            "churn_risk_count": 12,
            "analysis_period_days": date_range_days,
            "min_order_value_filter": min_order_value
        }
        
        result = FunctionResult(success=True, data=analysis_data)
        return result.to_dict()
        
    except ValueError as e:
        result = FunctionResult(success=False, error=f"Validation error: {str(e)}")
        return result.to_dict()
    
    except Exception as e:
        result = FunctionResult(success=False, error=f"Analysis failed: {str(e)}")
        return result.to_dict()

# Test the function
print("Example: Comprehensive Python Function")
print("=" * 60)
print("\nFunction Signature:")
print("analyze_customer_segment(segment_name, date_range_days=90, min_order_value=0.0)")

print("\n\nTest 1: Valid input")
result1 = analyze_customer_segment("enterprise", date_range_days=30)
print(json.dumps(result1, indent=2))

print("\n\nTest 2: Invalid input (validation)")
result2 = analyze_customer_segment("", date_range_days=30)
print(json.dumps(result2, indent=2))

print("\n\nBest Practices Demonstrated:")
print("  ✓ Type hints for all parameters")
print("  ✓ Comprehensive docstring with examples")
print("  ✓ Input validation")
print("  ✓ Error handling")
print("  ✓ Structured return format")
print("  ✓ Timestamp for auditing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Testing Functions in AI Playground
# MAGIC
# MAGIC AI Playground provides an interactive environment to test UC functions with LLMs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 AI Playground Setup
# MAGIC
# MAGIC **Steps to test your functions:**
# MAGIC
# MAGIC 1. **Navigate to AI Playground**
# MAGIC    - Click "Machine Learning" in sidebar
# MAGIC    - Select "AI Playground"
# MAGIC
# MAGIC 2. **Select a Model**
# MAGIC    - Choose from available models (DBRX, Llama 3, etc.)
# MAGIC    - Recommended: Start with `databricks-meta-llama-3-70b-instruct`
# MAGIC
# MAGIC 3. **Add Tools (UC Functions)**
# MAGIC    - Click "Add Tools" button
# MAGIC    - Select "Unity Catalog Functions"
# MAGIC    - Choose your catalog and schema
# MAGIC    - Select functions to enable
# MAGIC
# MAGIC 4. **Test with Natural Language**
# MAGIC    - Type questions that would trigger your functions
# MAGIC    - Observe which functions the agent calls
# MAGIC    - Review function outputs
# MAGIC
# MAGIC 5. **Iterate and Improve**
# MAGIC    - Refine function descriptions based on testing
# MAGIC    - Adjust parameter documentation
# MAGIC    - Add examples to help the agent understand when to use each tool

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Example Test Prompts
# MAGIC
# MAGIC Good prompts to test your functions:
# MAGIC
# MAGIC **For `get_customer_orders`:**
# MAGIC - "Show me all orders for customer ID 12345"
# MAGIC - "What is the order history for customer 67890?"
# MAGIC - "List the recent orders for customer 24680"
# MAGIC
# MAGIC **For `calculate_revenue_by_region`:**
# MAGIC - "What was our revenue by region in Q4 2024?"
# MAGIC - "Compare regional sales for January 2024"
# MAGIC - "Show me revenue breakdown by region for last month"
# MAGIC
# MAGIC **For `calculate_customer_ltv`:**
# MAGIC - "What is the lifetime value of customer 12345?"
# MAGIC - "Calculate total spend for customer 67890"
# MAGIC - "How much has customer 24680 spent with us?"
# MAGIC
# MAGIC **For `search_customers`:**
# MAGIC - "Find customers named John Smith"
# MAGIC - "Search for customers with email containing '@acme.com'"
# MAGIC - "Who are our customers in the enterprise segment?"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Function Discovery
# MAGIC
# MAGIC Agents can programmatically discover available tools:

# COMMAND ----------

# Example: Function discovery
from databricks.sdk import WorkspaceClient

def discover_agent_functions(catalog: str, schema: str):
    """
    Discover all UC functions available in a catalog/schema.
    This simulates what an agent would see.
    """
    
    # In production with actual workspace:
    # w = WorkspaceClient()
    # functions = w.functions.list(catalog_name=catalog, schema_name=schema)
    
    # Simulated function metadata
    functions = [
        {
            "name": "get_customer_orders",
            "full_name": f"{catalog}.{schema}.get_customer_orders",
            "comment": "Retrieves all orders for a specific customer",
            "input_params": [
                {"name": "customer_id", "type": "BIGINT", "comment": "The unique ID of the customer"}
            ],
            "return_type": "TABLE",
            "data_type": "RETURNS TABLE(order_id BIGINT, order_date DATE, total_amount DECIMAL, status STRING)"
        },
        {
            "name": "calculate_revenue_by_region",
            "full_name": f"{catalog}.{schema}.calculate_revenue_by_region",
            "comment": "Calculates revenue metrics by region for a specified date range",
            "input_params": [
                {"name": "start_date", "type": "DATE", "comment": "Start of date range"},
                {"name": "end_date", "type": "DATE", "comment": "End of date range"}
            ],
            "return_type": "TABLE",
            "data_type": "RETURNS TABLE(region STRING, total_revenue DECIMAL, order_count BIGINT)"
        },
        {
            "name": "calculate_customer_ltv",
            "full_name": f"{catalog}.{schema}.calculate_customer_ltv",
            "comment": "Calculates customer lifetime value by summing all orders",
            "input_params": [
                {"name": "customer_id", "type": "BIGINT", "comment": "Customer ID"}
            ],
            "return_type": "DECIMAL(12,2)",
            "data_type": "DECIMAL(12,2)"
        }
    ]
    
    return functions

# Discover functions
catalog = "main"
schema = "agents"

print(f"Agent Functions Available in {catalog}.{schema}")
print("=" * 60)

functions = discover_agent_functions(catalog, schema)

for i, func in enumerate(functions, 1):
    print(f"\n{i}. {func['name']}")
    print(f"   Full Name: {func['full_name']}")
    print(f"   Description: {func['comment']}")
    print(f"   Return Type: {func['return_type']}")
    print(f"   Parameters:")
    for param in func['input_params']:
        print(f"     - {param['name']} ({param['type']}): {param['comment']}")

print("\n" + "=" * 60)
print(f"\n✓ Agent can discover {len(functions)} tools in this schema")
print("✓ Each tool has clear documentation for the LLM")
print("✓ Function metadata helps agent choose appropriate tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Governance and Security
# MAGIC
# MAGIC Proper governance ensures safe and compliant agent operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Access Control
# MAGIC
# MAGIC Grant granular permissions on UC functions:
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant EXECUTE permission to a user
# MAGIC GRANT EXECUTE ON FUNCTION main.agents.get_customer_orders 
# MAGIC TO `user@company.com`;
# MAGIC
# MAGIC -- Grant EXECUTE permission to a group
# MAGIC GRANT EXECUTE ON FUNCTION main.agents.calculate_revenue_by_region 
# MAGIC TO `data-analysts`;
# MAGIC
# MAGIC -- Grant EXECUTE on all functions in schema
# MAGIC GRANT EXECUTE ON SCHEMA main.agents 
# MAGIC TO `agent-service-principal`;
# MAGIC
# MAGIC -- Revoke permissions
# MAGIC REVOKE EXECUTE ON FUNCTION main.agents.delete_customer_data 
# MAGIC FROM `data-analysts`;
# MAGIC ```

# COMMAND ----------

# Example: Programmatic permission management
def manage_function_permissions_example():
    """
    Example of managing UC function permissions programmatically
    """
    
    example_code = """
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PrivilegeAssignment, Privilege

w = WorkspaceClient()

# Grant EXECUTE permission to service principal
w.grants.update(
    full_name="main.agents.get_customer_orders",
    updates=[
        PrivilegeAssignment(
            principal="agent-service-principal",
            privileges=[Privilege.EXECUTE]
        )
    ]
)

# Grant to multiple users/groups
principals = [
    "data-analyst-group",
    "sales-team-group",
    "agent-service-principal"
]

for principal in principals:
    w.grants.update(
        full_name="main.agents.calculate_revenue_by_region",
        updates=[
            PrivilegeAssignment(
                principal=principal,
                privileges=[Privilege.EXECUTE]
            )
        ]
    )

print("Permissions granted successfully")
"""
    
    return example_code

print("Managing UC Function Permissions:")
print("=" * 60)
print(manage_function_permissions_example())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Audit Logging
# MAGIC
# MAGIC Track function usage for compliance and debugging:
# MAGIC
# MAGIC ```sql
# MAGIC -- Query audit logs for function calls
# MAGIC SELECT 
# MAGIC   event_time,
# MAGIC   user_identity.email as user,
# MAGIC   request_params.function_name as function,
# MAGIC   request_params.parameters as params,
# MAGIC   response.status_code as status
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name = 'executeFunction'
# MAGIC   AND request_params.function_name LIKE 'main.agents.%'
# MAGIC   AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Function Versioning
# MAGIC
# MAGIC Manage function changes safely:
# MAGIC
# MAGIC **Strategy 1: Version Suffixes**
# MAGIC ```sql
# MAGIC -- Create new version with suffix
# MAGIC CREATE FUNCTION main.agents.calculate_revenue_v2(...) ...
# MAGIC
# MAGIC -- Test new version
# MAGIC -- Once validated, drop old and rename new
# MAGIC DROP FUNCTION main.agents.calculate_revenue;
# MAGIC ALTER FUNCTION main.agents.calculate_revenue_v2 
# MAGIC RENAME TO main.agents.calculate_revenue;
# MAGIC ```
# MAGIC
# MAGIC **Strategy 2: Separate Schemas**
# MAGIC ```sql
# MAGIC -- Development functions
# MAGIC CREATE FUNCTION main.agents_dev.calculate_revenue(...) ...
# MAGIC
# MAGIC -- Production functions
# MAGIC CREATE FUNCTION main.agents_prod.calculate_revenue(...) ...
# MAGIC
# MAGIC -- Agents use different schemas based on environment
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Security Best Practices
# MAGIC
# MAGIC ✅ **Principle of Least Privilege**
# MAGIC - Grant only necessary EXECUTE permissions
# MAGIC - Don't give agents ALL PRIVILEGES
# MAGIC - Regularly audit permissions
# MAGIC
# MAGIC ✅ **Input Validation**
# MAGIC - Validate all parameters
# MAGIC - Sanitize string inputs
# MAGIC - Check ranges for numeric inputs
# MAGIC - Prevent SQL injection
# MAGIC
# MAGIC ✅ **Output Filtering**
# MAGIC - Don't return sensitive data (PII, credentials)
# MAGIC - Limit result set sizes
# MAGIC - Redact when necessary
# MAGIC
# MAGIC ✅ **Secrets Management**
# MAGIC - Store API keys in Databricks Secrets
# MAGIC - Never hardcode credentials
# MAGIC - Rotate secrets regularly
# MAGIC
# MAGIC ✅ **Monitoring**
# MAGIC - Track function usage
# MAGIC - Alert on suspicious patterns
# MAGIC - Monitor costs
# MAGIC - Log errors

# COMMAND ----------

# Example: Secure function pattern
def secure_function_pattern():
    """
    Template for a secure UC function
    """
    
    template = '''
from typing import Dict, Any
import re

def secure_query_function(
    user_input: str,
    user_id: str,
    max_results: int = 100
) -> Dict[str, Any]:
    """
    Secure function pattern with input validation and output filtering
    """
    
    # 1. INPUT VALIDATION
    if not user_input or len(user_input) > 1000:
        return {"error": "Invalid input length"}
    
    # Sanitize input
    sanitized_input = re.sub(r'[^a-zA-Z0-9\s-]', '', user_input)
    
    # Validate user_id format
    if not re.match(r'^[a-zA-Z0-9_-]+@[a-zA-Z0-9.-]+$', user_id):
        return {"error": "Invalid user_id format"}
    
    # Limit result size
    max_results = min(max_results, 1000)
    
    # 2. AUTHORIZATION CHECK
    # Verify user has access to requested data
    if not has_permission(user_id, "read_data"):
        return {"error": "Permission denied"}
    
    # 3. EXECUTE QUERY SAFELY
    try:
        # Use parameterized query (prevents SQL injection)
        result = execute_parameterized_query(
            query="SELECT * FROM table WHERE field = ?",
            params=[sanitized_input],
            limit=max_results
        )
        
        # 4. OUTPUT FILTERING
        # Remove sensitive fields
        filtered_result = [
            {k: v for k, v in row.items() 
             if k not in ['ssn', 'credit_card', 'password']}
            for row in result
        ]
        
        # 5. AUDIT LOGGING
        log_function_call(
            function_name="secure_query_function",
            user_id=user_id,
            input_hash=hash(user_input),
            result_count=len(filtered_result)
        )
        
        return {
            "success": True,
            "data": filtered_result,
            "count": len(filtered_result)
        }
        
    except Exception as e:
        # Log error without exposing details
        log_error(function_name="secure_query_function", error=str(e))
        return {"error": "Query failed"}
'''
    
    return template

print("Secure Function Pattern:")
print("=" * 60)
print(secure_function_pattern())
print("\nKey Security Features:")
print("  ✓ Input validation and sanitization")
print("  ✓ Authorization checks")
print("  ✓ Parameterized queries (SQL injection prevention)")
print("  ✓ Output filtering (sensitive data removal)")
print("  ✓ Audit logging")
print("  ✓ Error handling without information leakage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC ✅ **Unity Catalog functions provide ideal infrastructure for agent tools**
# MAGIC - Centralized management and versioning
# MAGIC - Built-in security and governance
# MAGIC - Native Databricks integration
# MAGIC
# MAGIC ✅ **SQL functions are perfect for data operations**
# MAGIC - Simple query and aggregation patterns
# MAGIC - Comprehensive documentation with COMMENT
# MAGIC - TABLE returns for structured data
# MAGIC
# MAGIC ✅ **Python functions enable complex logic**
# MAGIC - External API integrations
# MAGIC - Advanced processing and transformations
# MAGIC - Rich error handling
# MAGIC
# MAGIC ✅ **Testing in AI Playground validates tool design**
# MAGIC - Interactive testing with LLMs
# MAGIC - Immediate feedback on function usability
# MAGIC - Iteration and refinement
# MAGIC
# MAGIC ✅ **Security and governance are critical**
# MAGIC - Fine-grained access control
# MAGIC - Audit logging for compliance
# MAGIC - Input validation and output filtering

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Next Steps
# MAGIC
# MAGIC Now you're ready to create tools for your agents!
# MAGIC
# MAGIC **Continue to:**
# MAGIC - **02_Lecture_Model_Context_Protocol**: Learn about MCP servers for extensible tool ecosystems
# MAGIC - **08_Demo_Building_SQL_Functions**: See SQL functions in action
# MAGIC - **09_Demo_Building_Python_Functions**: Build Python UC functions
# MAGIC
# MAGIC **Practice by:**
# MAGIC 1. Creating 3-5 UC functions for a specific domain (sales, support, analytics)
# MAGIC 2. Testing them in AI Playground
# MAGIC 3. Iterating based on agent behavior
# MAGIC
# MAGIC **Additional Resources:**
# MAGIC - [Unity Catalog Functions Documentation](https://docs.databricks.com/sql/language-manual/sql-ref-functions-udf.html)
# MAGIC - [Databricks SDK for Python](https://databricks-sdk-py.readthedocs.io/)
# MAGIC - [AI Playground Guide](https://docs.databricks.com/ai-playground/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this lecture, we covered:
# MAGIC
# MAGIC - ✅ Why Unity Catalog functions are ideal for agent tools
# MAGIC - ✅ Creating SQL functions with proper metadata
# MAGIC - ✅ Building Python UC functions with SDK
# MAGIC - ✅ External API integration patterns
# MAGIC - ✅ Testing functions in AI Playground
# MAGIC - ✅ Security, governance, and best practices
# MAGIC
# MAGIC You now understand how to create production-ready tools for AI agents on Databricks!
