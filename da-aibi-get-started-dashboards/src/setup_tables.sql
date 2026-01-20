-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup Tables for AI/BI Dashboard Workshop
-- MAGIC
-- MAGIC This notebook contains the DDL statements to create the sample tables used in the workshop.
-- MAGIC Run this after you've created your catalog and schema.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sales Transactions Table
-- MAGIC This table contains sample sales transaction data including product, customer, location, and revenue information.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_transactions (
  transaction_id STRING,
  transaction_date DATE,
  customer_id STRING,
  product_id STRING,
  product_name STRING,
  product_category STRING,
  quantity INT,
  unit_price DECIMAL(10,2),
  total_amount DECIMAL(10,2),
  payment_method STRING,
  store_id STRING,
  store_name STRING,
  zip_code STRING,
  city STRING,
  state STRING,
  region STRING,
  sales_channel STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customers Table
-- MAGIC This table contains customer demographic and segment information.

-- COMMAND ----------

CREATE OR REPLACE TABLE customers (
  customer_id STRING,
  customer_name STRING,
  email STRING,
  phone STRING,
  customer_segment STRING,
  loyalty_tier STRING,
  signup_date DATE,
  lifetime_value DECIMAL(10,2),
  zip_code STRING,
  city STRING,
  state STRING,
  region STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Products Table
-- MAGIC This table contains product catalog information.

-- COMMAND ----------

CREATE OR REPLACE TABLE products (
  product_id STRING,
  product_name STRING,
  product_category STRING,
  product_subcategory STRING,
  brand STRING,
  unit_price DECIMAL(10,2),
  cost DECIMAL(10,2),
  margin DECIMAL(5,2),
  stock_quantity INT,
  supplier STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Store Locations Table
-- MAGIC This table contains information about store locations.

-- COMMAND ----------

CREATE OR REPLACE TABLE store_locations (
  store_id STRING,
  store_name STRING,
  store_type STRING,
  address STRING,
  city STRING,
  state STRING,
  zip_code STRING,
  region STRING,
  latitude DECIMAL(10,6),
  longitude DECIMAL(10,6),
  opening_date DATE,
  square_footage INT,
  manager_name STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Geography Table
-- MAGIC This table contains zip code geographic information for mapping.

-- COMMAND ----------

CREATE OR REPLACE TABLE geography (
  zip_code STRING,
  city STRING,
  state STRING,
  latitude DECIMAL(10,6),
  longitude DECIMAL(10,6),
  region STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Customer Journey Table
-- MAGIC This table tracks customer interactions across different touchpoints.

-- COMMAND ----------

CREATE OR REPLACE TABLE customer_journey (
  journey_id STRING,
  customer_id STRING,
  interaction_date DATE,
  touchpoint STRING,
  channel STRING,
  outcome STRING,
  value DECIMAL(10,2)
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tables Created Successfully
-- MAGIC All tables have been created. You can now run the data loading scripts.
-- MAGIC
