# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building Python Functions as Agent Tools with AI Playground
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo walks through creating Python-based Unity Catalog functions that serve as tools for AI agents. You'll learn how to write, register using both SQL and DatabricksFunctionClient(), test, and troubleshoot Python functions for agent use.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Create Python UC functions with complex logic
# MAGIC - Use DatabricksFunctionClient() for programmatic registration
# MAGIC - Implement business rules and calculations in Python
# MAGIC - Handle errors gracefully in Python functions
# MAGIC - Test Python functions in AI Playground
# MAGIC - Choose between SQL and Python for different use cases
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Serverless or classic compute cluster
# MAGIC - Python knowledge
# MAGIC - Completion of SQL functions demo (recommended)
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Libraries and Configure Workspace

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
import json

# Initialize Workspace Client
w = WorkspaceClient()

# Display current user info
current_user = w.current_user.me()
print(f"Logged in as: {current_user.user_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use your catalog
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC -- Create schema for Python functions (or reuse from SQL demo)
# MAGIC CREATE SCHEMA IF NOT EXISTS agent_tools_python;
# MAGIC
# MAGIC -- Use the schema
# MAGIC USE SCHEMA agent_tools_python;
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample data for Python functions demo
# MAGIC CREATE OR REPLACE TABLE transactions (
# MAGIC   transaction_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   amount DOUBLE,
# MAGIC   transaction_date TIMESTAMP,
# MAGIC   category STRING,
# MAGIC   status STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO transactions VALUES
# MAGIC   ('T001', 'C001', 150.00, '2024-12-01 10:30:00', 'retail', 'completed'),
# MAGIC   ('T002', 'C001', -20.00, '2024-12-02 14:15:00', 'refund', 'completed'),
# MAGIC   ('T003', 'C002', 300.00, '2024-12-05 09:45:00', 'retail', 'completed'),
# MAGIC   ('T004', 'C003', 500.00, '2024-12-10 16:20:00', 'wholesale', 'completed'),
# MAGIC   ('T005', 'C001', 75.50, '2024-12-15 11:00:00', 'retail', 'completed'),
# MAGIC   ('T006', 'C002', 450.00, '2024-12-20 13:30:00', 'wholesale', 'pending'),
# MAGIC   ('T007', 'C004', 125.00, '2024-12-28 15:45:00', 'retail', 'completed'),
# MAGIC   ('T008', 'C003', -50.00, '2025-01-03 10:00:00', 'refund', 'completed'),
# MAGIC   ('T009', 'C001', 200.00, '2025-01-05 12:30:00', 'retail', 'completed'),
# MAGIC   ('T010', 'C005', 350.00, '2025-01-08 14:00:00', 'wholesale', 'completed');
# MAGIC
# MAGIC SELECT * FROM transactions ORDER BY transaction_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1: Creating Python Functions with SQL Syntax
# MAGIC
# MAGIC The simplest way to create Python functions is using SQL CREATE FUNCTION syntax.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Simple Calculation Function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION calculate_tax(amount DOUBLE, tax_rate DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculates the tax amount given a base amount and tax rate (as a decimal, e.g., 0.08 for 8%). Returns the tax amount only, not the total. Use this when calculating sales tax or VAT.'
# MAGIC AS $$
# MAGIC   if amount is None or tax_rate is None:
# MAGIC       return None
# MAGIC   if amount < 0 or tax_rate < 0:
# MAGIC       return None
# MAGIC   return amount * tax_rate
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test basic calculation
# MAGIC SELECT calculate_tax(100.0, 0.08) as tax_amount;
# MAGIC -- Expected: 8.0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test multiple scenarios
# MAGIC SELECT 
# MAGIC   100.0 as amount,
# MAGIC   0.08 as tax_rate,
# MAGIC   calculate_tax(100.0, 0.08) as tax,
# MAGIC   100.0 + calculate_tax(100.0, 0.08) as total
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   250.0 as amount,
# MAGIC   0.10 as tax_rate,
# MAGIC   calculate_tax(250.0, 0.10) as tax,
# MAGIC   250.0 + calculate_tax(250.0, 0.10) as total;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: String Manipulation Function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION format_customer_id(raw_id STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Formats a customer ID to standard format: uppercase, trimmed, with C prefix if missing. For example, converts "  c123  " to "C123" or "456" to "C456". Use this when normalizing customer IDs from user input.'
# MAGIC AS $$
# MAGIC   if raw_id is None:
# MAGIC       return None
# MAGIC   
# MAGIC   # Clean and uppercase
# MAGIC   clean_id = raw_id.strip().upper()
# MAGIC   
# MAGIC   # Add C prefix if missing
# MAGIC   if not clean_id.startswith('C'):
# MAGIC       clean_id = 'C' + clean_id
# MAGIC   
# MAGIC   return clean_id
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test String Function

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   '  c123  ' as input,
# MAGIC   format_customer_id('  c123  ') as output
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '456' as input,
# MAGIC   format_customer_id('456') as output
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'C789' as input,
# MAGIC   format_customer_id('C789') as output;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Business Logic Function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION calculate_loyalty_points(amount DOUBLE, customer_tier STRING)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculates loyalty points earned based on purchase amount and customer tier. Bronze earns 1 point per dollar, Silver earns 1.5x, Gold earns 2x. Returns rounded integer points. Use this when calculating rewards or showing potential points for a purchase.'
# MAGIC AS $$
# MAGIC   if amount is None or customer_tier is None:
# MAGIC       return 0
# MAGIC   
# MAGIC   if amount < 0:
# MAGIC       return 0
# MAGIC   
# MAGIC   # Define multipliers by tier
# MAGIC   multipliers = {
# MAGIC       'BRONZE': 1.0,
# MAGIC       'SILVER': 1.5,
# MAGIC       'GOLD': 2.0
# MAGIC   }
# MAGIC   
# MAGIC   # Get multiplier (default to 1.0 if tier not found)
# MAGIC   tier_upper = customer_tier.upper()
# MAGIC   multiplier = multipliers.get(tier_upper, 1.0)
# MAGIC   
# MAGIC   # Calculate points
# MAGIC   points = amount * multiplier
# MAGIC   
# MAGIC   return int(round(points))
# MAGIC $$;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Business Logic

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   100.0 as amount,
# MAGIC   'Bronze' as tier,
# MAGIC   calculate_loyalty_points(100.0, 'Bronze') as points
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   100.0 as amount,
# MAGIC   'Silver' as tier,
# MAGIC   calculate_loyalty_points(100.0, 'Silver') as points
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   100.0 as amount,
# MAGIC   'Gold' as tier,
# MAGIC   calculate_loyalty_points(100.0, 'Gold') as points;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test these functions with:**
# MAGIC - "Calculate the tax on $150 at 7% rate"
# MAGIC - "How many loyalty points would a Gold customer get for a $250 purchase?"
# MAGIC - "Format this customer ID: c456"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: Using DatabricksFunctionClient() (Programmatic)
# MAGIC
# MAGIC For more complex scenarios, use the Python SDK to register functions programmatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4: Creating Function with SDK

# COMMAND ----------

# Define the Python function first
def calculate_discount_amount(price: float, discount_percentage: float) -> float:
    """
    Calculates the discount amount (not the final price).
    
    Args:
        price: Original price
        discount_percentage: Discount as percentage (e.g., 20 for 20%)
    
    Returns:
        The discount amount in dollars
    """
    if price is None or discount_percentage is None:
        return None
    
    if price < 0 or discount_percentage < 0 or discount_percentage > 100:
        return None
    
    return price * (discount_percentage / 100)

# Test locally first
print(f"Test: 100 at 20% off = ${calculate_discount_amount(100, 20)}")
print(f"Test: 50 at 15% off = ${calculate_discount_amount(50, 15)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register the Function using SDK

# COMMAND ----------

# Note: The SDK method for registering Python UDFs is different
# We'll use SQL CREATE FUNCTION for simplicity, but show the structure
# In practice, you would use CREATE FUNCTION AS shown above

%sql
CREATE OR REPLACE FUNCTION calculate_discount_amount(price DOUBLE, discount_percentage DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'Calculates the discount amount (not final price) given original price and discount percentage (0-100). For example, $100 at 20% returns $20. Use this when showing savings or discount amounts to customers.'
AS $$
  if price is None or discount_percentage is None:
      return None
  
  if price < 0 or discount_percentage < 0 or discount_percentage > 100:
      return None
  
  return price * (discount_percentage / 100)
$$;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 5: Date/Time Processing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION is_business_hours(hour_of_day INT)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Checks if a given hour (0-23) falls within business hours (9 AM to 5 PM, hours 9-16 inclusive). Returns TRUE if within business hours, FALSE otherwise. Use this for determining if operations should be allowed or if business support is available.'
# MAGIC AS $$
# MAGIC   if hour_of_day is None:
# MAGIC       return False
# MAGIC   
# MAGIC   # Business hours: 9 AM to 5 PM (9-16 inclusive)
# MAGIC   return 9 <= hour_of_day <= 16
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test business hours
# MAGIC SELECT 
# MAGIC   8 as hour,
# MAGIC   is_business_hours(8) as is_business_hours
# MAGIC UNION ALL
# MAGIC SELECT 10 as hour, is_business_hours(10) as is_business_hours
# MAGIC UNION ALL
# MAGIC SELECT 17 as hour, is_business_hours(17) as is_business_hours
# MAGIC UNION ALL
# MAGIC SELECT 16 as hour, is_business_hours(16) as is_business_hours;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 6: Complex Mathematical Function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION calculate_compound_interest(
# MAGIC   principal DOUBLE,
# MAGIC   annual_rate DOUBLE,
# MAGIC   years INT
# MAGIC )
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculates compound interest using the formula A = P(1 + r)^t, where P is principal, r is annual rate (as decimal like 0.05 for 5%), and t is years. Returns the final amount (principal + interest). Use for financial projections and investment calculations.'
# MAGIC AS $$
# MAGIC   if principal is None or annual_rate is None or years is None:
# MAGIC       return None
# MAGIC   
# MAGIC   if principal < 0 or annual_rate < 0 or years < 0:
# MAGIC       return None
# MAGIC   
# MAGIC   # Calculate compound interest: A = P(1 + r)^t
# MAGIC   final_amount = principal * ((1 + annual_rate) ** years)
# MAGIC   
# MAGIC   return round(final_amount, 2)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test compound interest
# MAGIC SELECT 
# MAGIC   1000 as principal,
# MAGIC   0.05 as rate,
# MAGIC   5 as years,
# MAGIC   calculate_compound_interest(1000, 0.05, 5) as final_amount,
# MAGIC   calculate_compound_interest(1000, 0.05, 5) - 1000 as interest_earned;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 7: Validation Function with Multiple Rules

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION validate_email(email STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Validates if a string is a properly formatted email address. Checks for @ symbol, domain, and basic format. Returns TRUE if valid, FALSE otherwise. Use this when validating user input for email addresses.'
# MAGIC AS $$
# MAGIC   if email is None:
# MAGIC       return False
# MAGIC   
# MAGIC   email = email.strip()
# MAGIC   
# MAGIC   # Basic validation rules
# MAGIC   if len(email) < 5:  # Minimum: a@b.c
# MAGIC       return False
# MAGIC   
# MAGIC   if '@' not in email:
# MAGIC       return False
# MAGIC   
# MAGIC   # Split on @
# MAGIC   parts = email.split('@')
# MAGIC   if len(parts) != 2:
# MAGIC       return False
# MAGIC   
# MAGIC   local, domain = parts
# MAGIC   
# MAGIC   # Check local part
# MAGIC   if len(local) == 0:
# MAGIC       return False
# MAGIC   
# MAGIC   # Check domain part
# MAGIC   if len(domain) == 0 or '.' not in domain:
# MAGIC       return False
# MAGIC   
# MAGIC   return True
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test email validation
# MAGIC SELECT 
# MAGIC   'user@example.com' as email,
# MAGIC   validate_email('user@example.com') as is_valid
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'invalid.email' as email,
# MAGIC   validate_email('invalid.email') as is_valid
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'no@domain' as email,
# MAGIC   validate_email('no@domain') as is_valid
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '@example.com' as email,
# MAGIC   validate_email('@example.com') as is_valid
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'good.email@company.co.uk' as email,
# MAGIC   validate_email('good.email@company.co.uk') as is_valid;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 8: Data Categorization Function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION categorize_transaction_size(amount DOUBLE)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Categorizes a transaction amount into size buckets: Small (<100), Medium (100-500), Large (500-1000), or Very Large (>1000). Returns the category name. Use this for transaction analysis and reporting.'
# MAGIC AS $$
# MAGIC   if amount is None:
# MAGIC       return 'Unknown'
# MAGIC   
# MAGIC   if amount < 0:
# MAGIC       return 'Refund'
# MAGIC   elif amount < 100:
# MAGIC       return 'Small'
# MAGIC   elif amount < 500:
# MAGIC       return 'Medium'
# MAGIC   elif amount < 1000:
# MAGIC       return 'Large'
# MAGIC   else:
# MAGIC       return 'Very Large'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test categorization
# MAGIC SELECT 
# MAGIC   transaction_id,
# MAGIC   amount,
# MAGIC   categorize_transaction_size(amount) as size_category
# MAGIC FROM transactions
# MAGIC ORDER BY amount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 AI Playground Testing
# MAGIC
# MAGIC **Test with these queries:**
# MAGIC - "What would $10,000 grow to in 10 years at 6% annual interest?"
# MAGIC - "Is user@company.com a valid email?"
# MAGIC - "Categorize a transaction of $750"
# MAGIC - "Is 2 PM during business hours?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 9: Advanced - Working with Arrays/Lists

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION calculate_average_from_array(values ARRAY<DOUBLE>)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculates the average (mean) of an array of numbers. Ignores NULL values. Returns NULL if array is empty or all values are NULL. Use this for computing averages from lists of values.'
# MAGIC AS $$
# MAGIC   if values is None or len(values) == 0:
# MAGIC       return None
# MAGIC   
# MAGIC   # Filter out None values
# MAGIC   valid_values = [v for v in values if v is not None]
# MAGIC   
# MAGIC   if len(valid_values) == 0:
# MAGIC       return None
# MAGIC   
# MAGIC   return sum(valid_values) / len(valid_values)
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with arrays
# MAGIC SELECT calculate_average_from_array(ARRAY(10.0, 20.0, 30.0, 40.0)) as average;
# MAGIC -- Expected: 25.0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT calculate_average_from_array(ARRAY(100.0, 200.0, 150.0)) as average;
# MAGIC -- Expected: 150.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 10: Text Analysis Function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION extract_domain_from_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Extracts the domain name from an email address. For example, "user@company.com" returns "company.com". Returns NULL if email is invalid. Use this for grouping users by email domain or company analysis.'
# MAGIC AS $$
# MAGIC   if email is None:
# MAGIC       return None
# MAGIC   
# MAGIC   email = email.strip()
# MAGIC   
# MAGIC   if '@' not in email:
# MAGIC       return None
# MAGIC   
# MAGIC   parts = email.split('@')
# MAGIC   if len(parts) != 2:
# MAGIC       return None
# MAGIC   
# MAGIC   domain = parts[1]
# MAGIC   
# MAGIC   if len(domain) == 0:
# MAGIC       return None
# MAGIC   
# MAGIC   return domain.lower()
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   'alice@example.com' as email,
# MAGIC   extract_domain_from_email('alice@example.com') as domain
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'BOB@COMPANY.COM' as email,
# MAGIC   extract_domain_from_email('BOB@COMPANY.COM') as domain
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'invalid' as email,
# MAGIC   extract_domain_from_email('invalid') as domain;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Comprehensive Error Handling

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION safe_divide(numerator DOUBLE, denominator DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Safely divides two numbers with comprehensive error handling. Returns NULL for division by zero, invalid inputs, or NULL inputs. Use this whenever you need to perform division that might encounter edge cases.'
# MAGIC AS $$
# MAGIC   # Handle NULL inputs
# MAGIC   if numerator is None or denominator is None:
# MAGIC       return None
# MAGIC   
# MAGIC   # Handle division by zero
# MAGIC   if denominator == 0:
# MAGIC       return None
# MAGIC   
# MAGIC   # Handle invalid numbers (infinity, NaN)
# MAGIC   try:
# MAGIC       result = numerator / denominator
# MAGIC       
# MAGIC       # Check if result is valid
# MAGIC       if str(result) in ['inf', '-inf', 'nan']:
# MAGIC           return None
# MAGIC       
# MAGIC       return result
# MAGIC   except:
# MAGIC       return None
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test error handling
# MAGIC SELECT 
# MAGIC   'Normal' as test_case,
# MAGIC   safe_divide(10.0, 2.0) as result
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Division by zero' as test_case,
# MAGIC   safe_divide(10.0, 0.0) as result
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'NULL numerator' as test_case,
# MAGIC   safe_divide(NULL, 2.0) as result
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'NULL denominator' as test_case,
# MAGIC   safe_divide(10.0, NULL) as result;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL vs Python: When to Use Which?
# MAGIC
# MAGIC ### Use SQL Functions When:
# MAGIC - ✅ Querying tables and views
# MAGIC - ✅ Simple aggregations (COUNT, SUM, AVG)
# MAGIC - ✅ Filtering and joining data
# MAGIC - ✅ Date/time operations with SQL functions
# MAGIC - ✅ Simple transformations
# MAGIC
# MAGIC ### Use Python Functions When:
# MAGIC - ✅ Complex business logic
# MAGIC - ✅ Mathematical calculations
# MAGIC - ✅ String manipulation and parsing
# MAGIC - ✅ Validation and data quality checks
# MAGIC - ✅ Categorization and bucketing
# MAGIC - ✅ Conditional logic with many branches
# MAGIC - ✅ Working with arrays/collections
# MAGIC
# MAGIC ### Example Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL: Simple aggregation (GOOD use of SQL)
# MAGIC CREATE OR REPLACE FUNCTION sql_get_total_sales()
# MAGIC RETURNS DOUBLE
# MAGIC RETURN (SELECT SUM(amount) FROM transactions WHERE amount > 0);
# MAGIC
# MAGIC SELECT sql_get_total_sales();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Python: Complex calculation (GOOD use of Python)
# MAGIC CREATE OR REPLACE FUNCTION py_calculate_risk_score(
# MAGIC   transaction_amount DOUBLE,
# MAGIC   customer_history_months INT,
# MAGIC   is_international BOOLEAN
# MAGIC )
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculates a risk score (Low, Medium, High) for a transaction based on amount, customer history, and location. Complex business logic best suited for Python.'
# MAGIC AS $$
# MAGIC   if transaction_amount is None or customer_history_months is None:
# MAGIC       return 'Unknown'
# MAGIC   
# MAGIC   score = 0
# MAGIC   
# MAGIC   # Amount factor
# MAGIC   if transaction_amount > 1000:
# MAGIC       score += 2
# MAGIC   elif transaction_amount > 500:
# MAGIC       score += 1
# MAGIC   
# MAGIC   # History factor
# MAGIC   if customer_history_months < 3:
# MAGIC       score += 2
# MAGIC   elif customer_history_months < 12:
# MAGIC       score += 1
# MAGIC   
# MAGIC   # International factor
# MAGIC   if is_international:
# MAGIC       score += 1
# MAGIC   
# MAGIC   # Determine risk level
# MAGIC   if score >= 4:
# MAGIC       return 'High'
# MAGIC   elif score >= 2:
# MAGIC       return 'Medium'
# MAGIC   else:
# MAGIC       return 'Low'
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test risk scoring
# MAGIC SELECT 
# MAGIC   py_calculate_risk_score(1500.0, 2, TRUE) as risk_high,
# MAGIC   py_calculate_risk_score(200.0, 24, FALSE) as risk_low,
# MAGIC   py_calculate_risk_score(600.0, 6, TRUE) as risk_medium;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Issues and Troubleshooting

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 1: Function Returns Wrong Type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BAD: Returns float but declared as INT
# MAGIC -- This will cause errors
# MAGIC -- CREATE FUNCTION bad_function(x DOUBLE)
# MAGIC -- RETURNS INT
# MAGIC -- LANGUAGE PYTHON
# MAGIC -- AS $$ return x / 2.0 $$;  -- Returns float!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOOD: Type matches return value
# MAGIC CREATE OR REPLACE FUNCTION good_half(x DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$ 
# MAGIC   if x is None:
# MAGIC       return None
# MAGIC   return x / 2.0 
# MAGIC $$;
# MAGIC
# MAGIC SELECT good_half(100.0);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 2: Not Handling NULL Values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BAD: Will crash on NULL input
# MAGIC -- CREATE FUNCTION bad_length(s STRING)
# MAGIC -- RETURNS INT
# MAGIC -- LANGUAGE PYTHON
# MAGIC -- AS $$ return len(s) $$;  -- Crashes if s is None!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOOD: Handles NULL
# MAGIC CREATE OR REPLACE FUNCTION good_length(s STRING)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$ 
# MAGIC   if s is None:
# MAGIC       return 0
# MAGIC   return len(s)
# MAGIC $$;
# MAGIC
# MAGIC SELECT 
# MAGIC   good_length('hello') as normal,
# MAGIC   good_length(NULL) as null_input;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 3: Complex Functions Too Slow

# COMMAND ----------

# MAGIC %md
# MAGIC **Tip:** Keep Python functions simple and fast. If you need complex operations:
# MAGIC - Break into multiple simpler functions
# MAGIC - Do heavy lifting in SQL queries first
# MAGIC - Avoid loops when possible
# MAGIC - Use built-in Python functions (they're optimized)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing Workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Unit Test with Sample Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with known inputs and expected outputs
# MAGIC SELECT 
# MAGIC   'Test 1' as test_name,
# MAGIC   calculate_tax(100.0, 0.08) as result,
# MAGIC   8.0 as expected,
# MAGIC   CASE WHEN calculate_tax(100.0, 0.08) = 8.0 THEN 'PASS' ELSE 'FAIL' END as status
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Test 2' as test_name,
# MAGIC   calculate_loyalty_points(100.0, 'Gold') as result,
# MAGIC   200 as expected,
# MAGIC   CASE WHEN calculate_loyalty_points(100.0, 'Gold') = 200 THEN 'PASS' ELSE 'FAIL' END as status;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Test Edge Cases

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test edge cases and error conditions
# MAGIC SELECT 
# MAGIC   'NULL input' as test_case,
# MAGIC   calculate_tax(NULL, 0.08) as result,
# MAGIC   'Should be NULL' as expected
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Negative amount' as test_case,
# MAGIC   calculate_tax(-100.0, 0.08) as result,
# MAGIC   'Should be NULL' as expected
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Zero amount' as test_case,
# MAGIC   calculate_tax(0.0, 0.08) as result,
# MAGIC   'Should be 0' as expected;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Test in AI Playground
# MAGIC
# MAGIC **Navigate to AI Playground and attach your Python functions:**
# MAGIC - `calculate_tax`
# MAGIC - `calculate_loyalty_points`
# MAGIC - `format_customer_id`
# MAGIC - `validate_email`
# MAGIC - `categorize_transaction_size`
# MAGIC - `calculate_compound_interest`
# MAGIC - `extract_domain_from_email`
# MAGIC
# MAGIC **Test with natural language queries:**
# MAGIC - "Calculate tax on $500 at 7.5% rate"
# MAGIC - "How many points would a Silver customer earn on $300?"
# MAGIC - "Is test@example.com a valid email?"
# MAGIC - "What size category is a $450 transaction?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## View All Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all functions in schema
# MAGIC SHOW FUNCTIONS IN agent_tools_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get detailed info about a function
# MAGIC DESCRIBE FUNCTION EXTENDED calculate_loyalty_points;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### ✅ Design
# MAGIC 1. Keep functions focused on single tasks
# MAGIC 2. Use clear, descriptive names
# MAGIC 3. Write detailed comments explaining usage
# MAGIC 4. Document parameters and return values
# MAGIC
# MAGIC ### ✅ Implementation
# MAGIC 5. Always handle NULL inputs
# MAGIC 6. Validate input parameters
# MAGIC 7. Return appropriate types
# MAGIC 8. Use try-except for risky operations
# MAGIC 9. Keep logic simple and fast
# MAGIC
# MAGIC ### ✅ Testing
# MAGIC 10. Test with valid inputs
# MAGIC 11. Test edge cases (NULL, zero, negative)
# MAGIC 12. Test invalid inputs
# MAGIC 13. Verify in AI Playground
# MAGIC
# MAGIC ### ✅ Performance
# MAGIC 14. Avoid complex loops
# MAGIC 15. Use built-in functions
# MAGIC 16. Keep execution time under 5 seconds

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to drop functions
# MAGIC -- DROP FUNCTION IF EXISTS calculate_tax;
# MAGIC -- DROP FUNCTION IF EXISTS format_customer_id;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_loyalty_points;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_discount_amount;
# MAGIC -- DROP FUNCTION IF EXISTS is_business_hours;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_compound_interest;
# MAGIC -- DROP FUNCTION IF EXISTS validate_email;
# MAGIC -- DROP FUNCTION IF EXISTS categorize_transaction_size;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_average_from_array;
# MAGIC -- DROP FUNCTION IF EXISTS extract_domain_from_email;
# MAGIC -- DROP FUNCTION IF EXISTS safe_divide;
# MAGIC -- DROP FUNCTION IF EXISTS py_calculate_risk_score;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS transactions;
# MAGIC -- DROP SCHEMA IF EXISTS agent_tools_python CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Python functions** excel at complex logic, calculations, and validations
# MAGIC
# MAGIC 2. **Use SQL syntax** for simple Python functions; it's straightforward and easy
# MAGIC
# MAGIC 3. **Always handle NULL** - Python functions should never crash on None inputs
# MAGIC
# MAGIC 4. **Type safety matters** - ensure return type matches declaration
# MAGIC
# MAGIC 5. **Keep it simple** - complex functions are slow and hard to debug
# MAGIC
# MAGIC 6. **Test thoroughly** - unit tests AND AI Playground testing
# MAGIC
# MAGIC 7. **Choose wisely** - use SQL for data access, Python for business logic
# MAGIC
# MAGIC 8. **Document well** - detailed comments help agents use functions correctly

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Continue to: **4 Lab - Building AI Agent Tools with Unity Catalog Functions**
# MAGIC
# MAGIC In the lab, you'll:
# MAGIC - Build your own agent tools from scratch
# MAGIC - Solve real-world scenarios
# MAGIC - Troubleshoot broken functions
# MAGIC - Test tools with AI Playground
# MAGIC - Apply everything you've learned

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You now know how to build Python-based agent tools on Databricks.
# MAGIC
