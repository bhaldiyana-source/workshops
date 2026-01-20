# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Multi Flow Data Pipeline with Liquid Clustering and Data Quality
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demonstration shows how to build a multi-flow data pipeline that consolidates data from multiple sources with different formats (CSV and JSON) into unified streaming tables. We'll implement Liquid Clustering for optimal query performance and comprehensive data quality controls with expectations.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Design multi-flow architectures that consolidate data from diverse sources
# MAGIC - Resolve schema conflicts between different data formats
# MAGIC - Implement Liquid Clustering to optimize query performance
# MAGIC - Apply data quality expectations with WARN, DROP, and FAIL actions
# MAGIC - Use conditional logic for complex business validation rules
# MAGIC - Apply Unity Catalog tags for data governance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with Serverless V4 compute enabled
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Basic familiarity with Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration
# MAGIC
# MAGIC First, let's set up our catalog and schema. Replace `YOUR_CATALOG` with your actual catalog name.

# COMMAND ----------

# DBTITLE 1,Configure Catalog and Schema
# Set your catalog name here
catalog_name = "YOUR_CATALOG"
schema_name = "pipeline_demo"

# Create the schema if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Using catalog: {catalog_name}")
print(f"Using schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample Source Data
# MAGIC
# MAGIC We'll create sample data in two formats:
# MAGIC - **CSV format**: Customer transactions from Store A
# MAGIC - **JSON format**: Customer transactions from Store B
# MAGIC
# MAGIC These sources have slightly different schemas that we'll need to reconcile.

# COMMAND ----------

# DBTITLE 1,Create Sample CSV Data (Store A)
import pandas as pd
from datetime import datetime, timedelta
import random

# Generate sample CSV data
csv_data = []
base_date = datetime(2024, 1, 1)

for i in range(100):
    csv_data.append({
        'transaction_id': f'STORE_A_{i:05d}',
        'customer_id': f'CUST_{random.randint(1000, 9999)}',
        'product_id': f'PROD_{random.randint(100, 999)}',
        'amount': round(random.uniform(10, 500), 2),
        'quantity': random.randint(1, 10),
        'transaction_date': (base_date + timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d'),
        'store_location': 'Store_A',
        'payment_method': random.choice(['Credit', 'Debit', 'Cash'])
    })

# Create DataFrame and write to volume or table
csv_df = spark.createDataFrame(pd.DataFrame(csv_data))
csv_df.write.format("csv").mode("overwrite").option("header", "true").saveAsTable(f"{catalog_name}.{schema_name}.source_store_a_csv")

print(f"Created CSV source table with {csv_df.count()} records")
csv_df.display()

# COMMAND ----------

# DBTITLE 1,Create Sample JSON Data (Store B)
# Generate sample JSON data with slightly different schema
json_data = []

for i in range(100):
    json_data.append({
        'txn_id': f'STORE_B_{i:05d}',  # Different column name
        'customer_id': f'CUST_{random.randint(1000, 9999)}',
        'product_id': f'PROD_{random.randint(100, 999)}',
        'total_amount': round(random.uniform(10, 500), 2),  # Different column name
        'qty': random.randint(1, 10),  # Different column name
        'txn_timestamp': (base_date + timedelta(days=random.randint(0, 90))).isoformat(),  # Different format
        'location': 'Store_B',  # Different column name
        'payment_type': random.choice(['CreditCard', 'DebitCard', 'Cash', 'MobilePay']),  # Different values
        'discount_applied': random.choice([True, False])  # Additional field
    })

# Create DataFrame and write
json_df = spark.createDataFrame(pd.DataFrame(json_data))
json_df.write.format("json").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.source_store_b_json")

print(f"Created JSON source table with {json_df.count()} records")
json_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Understanding Multi-Flow Architecture
# MAGIC
# MAGIC In a multi-flow pipeline, we need to:
# MAGIC 1. **Ingest** data from multiple sources
# MAGIC 2. **Standardize** schemas to a common format
# MAGIC 3. **Validate** data quality across all flows
# MAGIC 4. **Consolidate** into a unified target table
# MAGIC
# MAGIC ### Schema Mapping Strategy
# MAGIC
# MAGIC | Common Field | Store A (CSV) | Store B (JSON) |
# MAGIC |-------------|---------------|----------------|
# MAGIC | transaction_id | transaction_id | txn_id |
# MAGIC | customer_id | customer_id | customer_id |
# MAGIC | product_id | product_id | product_id |
# MAGIC | amount | amount | total_amount |
# MAGIC | quantity | quantity | qty |
# MAGIC | transaction_date | transaction_date | txn_timestamp |
# MAGIC | store_location | store_location | location |
# MAGIC | payment_method | payment_method | payment_type |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the Multi-Flow Pipeline
# MAGIC
# MAGIC Now we'll create a Lakeflow Declarative Pipeline using the **multi-file editor**. 
# MAGIC
# MAGIC ### Pipeline Structure:
# MAGIC ```
# MAGIC 1. bronze_store_a (streaming table) <- CSV source
# MAGIC 2. bronze_store_b (streaming table) <- JSON source
# MAGIC 3. silver_transactions (streaming table with UNION) <- Both bronze tables
# MAGIC 4. gold_daily_sales (materialized view) <- Aggregated metrics
# MAGIC ```
# MAGIC
# MAGIC Let's create this pipeline step by step.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline File 1: Bronze Layer - Store A
# MAGIC
# MAGIC ```sql
# MAGIC -- bronze_store_a.sql
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_store_a
# MAGIC AS SELECT
# MAGIC   transaction_id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   amount,
# MAGIC   quantity,
# MAGIC   to_date(transaction_date) as transaction_date,
# MAGIC   store_location,
# MAGIC   payment_method,
# MAGIC   current_timestamp() as ingestion_timestamp,
# MAGIC   'store_a' as source_system
# MAGIC FROM STREAM read_files(
# MAGIC   'spark.sql(${catalog_name}.${schema_name}.source_store_a_csv)',
# MAGIC   format => 'cloudFiles',
# MAGIC   cloudFiles.format => 'csv',
# MAGIC   cloudFiles.schemaLocation => '${schema_location}/store_a',
# MAGIC   header => 'true'
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline File 2: Bronze Layer - Store B
# MAGIC
# MAGIC ```sql
# MAGIC -- bronze_store_b.sql
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_store_b
# MAGIC AS SELECT
# MAGIC   txn_id as transaction_id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   total_amount as amount,
# MAGIC   qty as quantity,
# MAGIC   to_date(txn_timestamp) as transaction_date,
# MAGIC   location as store_location,
# MAGIC   CASE 
# MAGIC     WHEN payment_type = 'CreditCard' THEN 'Credit'
# MAGIC     WHEN payment_type = 'DebitCard' THEN 'Debit'
# MAGIC     WHEN payment_type = 'MobilePay' THEN 'Mobile'
# MAGIC     ELSE payment_type
# MAGIC   END as payment_method,
# MAGIC   discount_applied,
# MAGIC   current_timestamp() as ingestion_timestamp,
# MAGIC   'store_b' as source_system
# MAGIC FROM STREAM read_files(
# MAGIC   'spark.sql(${catalog_name}.${schema_name}.source_store_b_json)',
# MAGIC   format => 'cloudFiles',
# MAGIC   cloudFiles.format => 'json',
# MAGIC   cloudFiles.schemaLocation => '${schema_location}/store_b'
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Implement Data Quality Expectations
# MAGIC
# MAGIC Data quality expectations define rules that your data must follow. We'll implement three types of actions:
# MAGIC - **WARN**: Log violations but allow records through
# MAGIC - **DROP**: Remove violating records
# MAGIC - **FAIL**: Stop the pipeline if violations occur

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline File 3: Silver Layer with Data Quality and Liquid Clustering
# MAGIC
# MAGIC ```sql
# MAGIC -- silver_transactions.sql
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_transactions (
# MAGIC   CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP,
# MAGIC   CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP,
# MAGIC   CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL,
# MAGIC   CONSTRAINT valid_transaction_date EXPECT (transaction_date <= current_date()) ON VIOLATION WARN,
# MAGIC   CONSTRAINT reasonable_amount EXPECT (amount <= 10000) ON VIOLATION WARN,
# MAGIC   CONSTRAINT valid_payment EXPECT (payment_method IN ('Credit', 'Debit', 'Cash', 'Mobile')) ON VIOLATION DROP
# MAGIC )
# MAGIC CLUSTER BY (transaction_date, store_location)
# MAGIC COMMENT "Consolidated transactions from all stores with data quality controls"
# MAGIC TBLPROPERTIES (
# MAGIC   'quality.level' = 'silver',
# MAGIC   'data.domain' = 'sales',
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC )
# MAGIC AS SELECT * FROM (
# MAGIC   SELECT 
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     amount,
# MAGIC     quantity,
# MAGIC     transaction_date,
# MAGIC     store_location,
# MAGIC     payment_method,
# MAGIC     cast(null as boolean) as discount_applied,
# MAGIC     ingestion_timestamp,
# MAGIC     source_system
# MAGIC   FROM STREAM(LIVE.bronze_store_a)
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     amount,
# MAGIC     quantity,
# MAGIC     transaction_date,
# MAGIC     store_location,
# MAGIC     payment_method,
# MAGIC     discount_applied,
# MAGIC     ingestion_timestamp,
# MAGIC     source_system
# MAGIC   FROM STREAM(LIVE.bronze_store_b)
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Understanding Liquid Clustering
# MAGIC
# MAGIC **Liquid Clustering** is a new Delta Lake feature that replaces traditional partitioning and Z-ordering with a more flexible approach:
# MAGIC
# MAGIC ### Benefits:
# MAGIC - **Automatic optimization**: No manual OPTIMIZE commands needed
# MAGIC - **Flexible clustering**: Can cluster by multiple columns
# MAGIC - **Better performance**: Optimized for common query patterns
# MAGIC - **Adaptive**: Adjusts to changing data patterns
# MAGIC
# MAGIC ### Syntax:
# MAGIC ```sql
# MAGIC CLUSTER BY (column1, column2, ...)
# MAGIC ```
# MAGIC
# MAGIC In our example, we cluster by `transaction_date` and `store_location` because these are common filter columns in queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Gold Layer with Aggregations
# MAGIC
# MAGIC ```sql
# MAGIC -- gold_daily_sales.sql
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
# MAGIC COMMENT "Daily sales metrics by store and payment method"
# MAGIC TBLPROPERTIES (
# MAGIC   'quality.level' = 'gold',
# MAGIC   'data.domain' = 'sales_analytics'
# MAGIC )
# MAGIC AS SELECT
# MAGIC   transaction_date,
# MAGIC   store_location,
# MAGIC   payment_method,
# MAGIC   COUNT(*) as transaction_count,
# MAGIC   SUM(amount) as total_sales,
# MAGIC   AVG(amount) as avg_transaction_value,
# MAGIC   SUM(quantity) as total_items_sold,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers,
# MAGIC   MAX(amount) as max_transaction,
# MAGIC   MIN(amount) as min_transaction
# MAGIC FROM LIVE.silver_transactions
# MAGIC GROUP BY transaction_date, store_location, payment_method
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Apply Unity Catalog Tags
# MAGIC
# MAGIC Tags help with data governance, discovery, and compliance. Let's apply tags to our tables.

# COMMAND ----------

# DBTITLE 1,Apply Unity Catalog Tags
# Note: In a real pipeline, these would be applied after the pipeline creates the tables
# This is示例 code to show the syntax

sql_commands = f"""
-- Create tags if they don't exist
CREATE TAG IF NOT EXISTS {catalog_name}.{schema_name}.pii_level;
CREATE TAG IF NOT EXISTS {catalog_name}.{schema_name}.data_classification;
CREATE TAG IF NOT EXISTS {catalog_name}.{schema_name}.retention_days;

-- Apply tags to silver table
ALTER TABLE {catalog_name}.{schema_name}.silver_transactions 
  SET TAGS ('pii_level' = 'medium', 'data_classification' = 'internal', 'retention_days' = '365');

-- Apply tags to gold view
ALTER TABLE {catalog_name}.{schema_name}.gold_daily_sales
  SET TAGS ('pii_level' = 'low', 'data_classification' = 'internal', 'retention_days' = '730');

-- Apply column-level tags
ALTER TABLE {catalog_name}.{schema_name}.silver_transactions 
  ALTER COLUMN customer_id SET TAGS ('pii_level' = 'high');
"""

print("Tag application commands:")
print(sql_commands)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Monitor Data Quality Metrics
# MAGIC
# MAGIC After running the pipeline, you can monitor data quality using event logs:

# COMMAND ----------

# DBTITLE 1,Query Data Quality Metrics
# This query would work after the pipeline runs
query = f"""
SELECT
  timestamp,
  dataset as table_name,
  flow_name,
  expectation_name,
  expectation,
  passed_records,
  failed_records
FROM event_log('{catalog_name}.{schema_name}')
WHERE event_type = 'flow_progress'
  AND expectation_name IS NOT NULL
ORDER BY timestamp DESC
"""

print("Data quality monitoring query:")
print(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Query the Results
# MAGIC
# MAGIC Once the pipeline runs, you can query the results:

# COMMAND ----------

# DBTITLE 1,Sample Queries
sample_queries = f"""
-- Query 1: View consolidated transactions
SELECT * FROM {catalog_name}.{schema_name}.silver_transactions LIMIT 10;

-- Query 2: Check data quality by source
SELECT 
  source_system,
  COUNT(*) as record_count,
  SUM(amount) as total_amount,
  AVG(amount) as avg_amount
FROM {catalog_name}.{schema_name}.silver_transactions
GROUP BY source_system;

-- Query 3: Daily sales summary
SELECT * FROM {catalog_name}.{schema_name}.gold_daily_sales
ORDER BY transaction_date DESC, total_sales DESC;

-- Query 4: Payment method analysis
SELECT 
  payment_method,
  COUNT(*) as transaction_count,
  SUM(total_sales) as total_sales,
  AVG(avg_transaction_value) as avg_value
FROM {catalog_name}.{schema_name}.gold_daily_sales
GROUP BY payment_method
ORDER BY total_sales DESC;

-- Query 5: Check Liquid Clustering effectiveness
DESCRIBE DETAIL {catalog_name}.{schema_name}.silver_transactions;
"""

print("Sample queries to run after pipeline execution:")
print(sample_queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Multi-Flow Architecture**: Consolidate data from multiple sources using UNION ALL in streaming tables
# MAGIC 2. **Schema Reconciliation**: Use SELECT with column aliasing and CASE statements to standardize schemas
# MAGIC 3. **Data Quality**: Implement expectations with different violation actions (WARN, DROP, FAIL)
# MAGIC 4. **Liquid Clustering**: Use CLUSTER BY for automatic optimization instead of manual partitioning
# MAGIC 5. **Governance**: Apply Unity Catalog tags at table and column level for compliance
# MAGIC 6. **Monitoring**: Use event_log to track data quality metrics
# MAGIC
# MAGIC ## Best Practices
# MAGIC
# MAGIC - Start with WARN actions to understand data quality issues before using DROP or FAIL
# MAGIC - Choose clustering columns based on common filter patterns in queries
# MAGIC - Apply tags early for better data governance
# MAGIC - Monitor expectation violations regularly
# MAGIC - Use meaningful comments and table properties for documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Run this section to clean up the demo resources:

# COMMAND ----------

# DBTITLE 1,Cleanup Demo Resources
cleanup_mode = False  # Set to True to run cleanup

if cleanup_mode:
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.source_store_a_csv")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.source_store_b_json")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.bronze_store_a")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.bronze_store_b")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.silver_transactions")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.gold_daily_sales")
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")
    print("Cleanup complete!")
else:
    print("Cleanup mode is disabled. Set cleanup_mode = True to run cleanup.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to the next lecture: **3_Lecture_Change_Data_Capture_CDC_Review** to learn about CDC patterns and SCD Type 2 automation.
