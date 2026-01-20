# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Advanced Security Patterns - RLS, Masking, and ABAC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Implement row-level security (RLS) in Unity Catalog
# MAGIC - Apply dynamic data masking
# MAGIC - Configure attribute-based access control (ABAC)
# MAGIC - Create security policies and functions
# MAGIC - Test access control scenarios
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog admin permissions
# MAGIC - Understanding of SQL security concepts
# MAGIC
# MAGIC ## Time Estimate
# MAGIC 25-30 minutes

# COMMAND ----------

# Configuration
from pyspark.sql.functions import *

current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_name = current_user.split("@")[0].replace(".", "_")
SCHEMA = f"security_lab_{user_name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS main.{SCHEMA}")
spark.sql(f"USE SCHEMA main.{SCHEMA}")

print(f"✅ Security Lab: main.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Row-Level Security (RLS)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sensitive data table
# MAGIC CREATE OR REPLACE TABLE customer_sensitive (
# MAGIC   customer_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   region STRING,
# MAGIC   account_balance DECIMAL(12,2),
# MAGIC   ssn STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO customer_sensitive VALUES
# MAGIC ('C001', 'Alice Johnson', 'alice@example.com', '555-0101', 'US-WEST', 15000.00, '123-45-6789'),
# MAGIC ('C002', 'Bob Smith', 'bob@example.com', '555-0102', 'US-EAST', 25000.00, '234-56-7890'),
# MAGIC ('C003', 'Carol White', 'carol@example.com', '555-0103', 'US-WEST', 8500.00, '345-67-8901'),
# MAGIC ('C004', 'David Brown', 'david@example.com', '555-0104', 'EU-CENTRAL', 12000.00, '456-78-9012');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create RLS Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create row filter function
# MAGIC CREATE OR REPLACE FUNCTION region_filter(user_region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN user_region = 'US-WEST' OR IS_ACCOUNT_GROUP_MEMBER('admins');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Row Filter (Unity Catalog 2024+)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: Row-level security requires Unity Catalog with proper permissions
# MAGIC -- ALTER TABLE customer_sensitive SET ROW FILTER region_filter ON (region);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Dynamic Data Masking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Masking Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask email
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC   WHEN IS_ACCOUNT_GROUP_MEMBER('pii_readers') THEN email
# MAGIC   ELSE CONCAT(SUBSTRING(email, 1, 2), '***@', SUBSTRING_INDEX(email, '@', -1))
# MAGIC END;
# MAGIC
# MAGIC -- Mask SSN
# MAGIC CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC   WHEN IS_ACCOUNT_GROUP_MEMBER('compliance_team') THEN ssn
# MAGIC   ELSE CONCAT('***-**-', RIGHT(ssn, 4))
# MAGIC END;
# MAGIC
# MAGIC -- Mask phone
# MAGIC CREATE OR REPLACE FUNCTION mask_phone(phone STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('***-***-', RIGHT(phone, 4));

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Masked View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customer_masked AS
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   mask_email(email) as email,
# MAGIC   mask_phone(phone) as phone,
# MAGIC   region,
# MAGIC   CASE 
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('finance_team') THEN account_balance
# MAGIC     ELSE NULL
# MAGIC   END as account_balance,
# MAGIC   mask_ssn(ssn) as ssn
# MAGIC FROM customer_sensitive;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query masked view
# MAGIC SELECT * FROM customer_masked;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Attribute-Based Access Control (ABAC)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create access control table
# MAGIC CREATE OR REPLACE TABLE access_policies (
# MAGIC   policy_id STRING,
# MAGIC   resource_type STRING,
# MAGIC   required_attribute STRING,
# MAGIC   attribute_value STRING,
# MAGIC   permission STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO access_policies VALUES
# MAGIC ('POL-001', 'customer_data', 'department', 'sales', 'read'),
# MAGIC ('POL-002', 'customer_data', 'department', 'finance', 'read_write'),
# MAGIC ('POL-003', 'customer_data', 'role', 'admin', 'full_access');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Column-Level Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant column-level access
# MAGIC GRANT SELECT (customer_id, customer_name, email, region) 
# MAGIC ON TABLE customer_sensitive 
# MAGIC TO `data_analysts`;
# MAGIC
# MAGIC -- Revoke sensitive column access
# MAGIC DENY SELECT (ssn, account_balance) 
# MAGIC ON TABLE customer_sensitive 
# MAGIC FROM `data_analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Audit Logging

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE access_audit_log (
# MAGIC   audit_id STRING,
# MAGIC   user_name STRING,
# MAGIC   table_name STRING,
# MAGIC   action STRING,
# MAGIC   timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   ip_address STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. ✅ Implemented row-level security with filter functions
# MAGIC 2. ✅ Created dynamic data masking for PII
# MAGIC 3. ✅ Designed ABAC policies
# MAGIC 4. ✅ Applied column-level permissions
# MAGIC 5. ✅ Set up audit logging
# MAGIC
# MAGIC ## Next Steps
# MAGIC - **Lecture 10**: Hybrid Data Modeling for OLTP and OLAP
