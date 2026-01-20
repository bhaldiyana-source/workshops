# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building SQL Functions as Agent Tools
# MAGIC
# MAGIC ## Overview
# MAGIC Hands-on demonstration of creating SQL Unity Catalog functions for use as agent tools. You'll create, test, and validate SQL functions in AI Playground.
# MAGIC
# MAGIC ## What You'll Build
# MAGIC - Customer query function
# MAGIC - Revenue calculation function
# MAGIC - Product search function
# MAGIC - Test in AI Playground
# MAGIC
# MAGIC ## Duration
# MAGIC 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample Data Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample customers table
# MAGIC CREATE OR REPLACE TABLE main.default.demo_customers (
# MAGIC   customer_id BIGINT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   state STRING,
# MAGIC   lifetime_value DECIMAL(12,2)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO main.default.demo_customers VALUES
# MAGIC   (1, 'John Smith', 'john@example.com', 'CA', 25000.00),
# MAGIC   (2, 'Jane Doe', 'jane@example.com', 'NY', 35000.00),
# MAGIC   (3, 'Bob Johnson', 'bob@example.com', 'TX', 15000.00);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create SQL Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Function 1: Get customer by ID
# MAGIC CREATE OR REPLACE FUNCTION main.default.get_customer(
# MAGIC   customer_id BIGINT COMMENT 'ID of customer to retrieve'
# MAGIC )
# MAGIC RETURNS TABLE(customer_id BIGINT, name STRING, email STRING, state STRING, lifetime_value DECIMAL(12,2))
# MAGIC COMMENT 'Retrieves customer details by ID. Use when you need customer information.'
# MAGIC RETURN
# MAGIC   SELECT * FROM main.default.demo_customers WHERE customer_id = customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test the Function

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.get_customer(1);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test in AI Playground
# MAGIC
# MAGIC 1. Go to AI Playground
# MAGIC 2. Select a model
# MAGIC 3. Add Tools → Unity Catalog Functions
# MAGIC 4. Select `main.default.get_customer`
# MAGIC 5. Try prompt: "Show me details for customer 1"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Created SQL UC functions
# MAGIC ✅ Tested functions directly
# MAGIC ✅ Ready for AI Playground testing
