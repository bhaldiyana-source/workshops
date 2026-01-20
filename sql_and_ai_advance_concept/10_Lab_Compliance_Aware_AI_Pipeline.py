# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 10: Compliance-Aware AI Pipeline
# MAGIC
# MAGIC ## Lab Overview
# MAGIC Build PII detection and masking functions, implement audit logging, create approval workflows, and generate compliance reports.
# MAGIC
# MAGIC ## Duration: 45-60 minutes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC CREATE SCHEMA IF NOT EXISTS lab10_compliance;
# MAGIC USE SCHEMA lab10_compliance;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: PII Detection System

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create PII detection table
# MAGIC CREATE TABLE IF NOT EXISTS pii_scan_results (
# MAGIC   scan_id STRING DEFAULT uuid(),
# MAGIC   record_id STRING,
# MAGIC   field_name STRING,
# MAGIC   pii_detected BOOLEAN,
# MAGIC   pii_types ARRAY<STRING>,
# MAGIC   confidence_score DOUBLE,
# MAGIC   scan_timestamp TIMESTAMP,
# MAGIC   compliance_tags ARRAY<STRING>
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample data with PII
# MAGIC CREATE OR REPLACE TABLE customer_data_raw AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('CUST001', 'John Smith', 'john.smith@email.com', '123-45-6789', '555-0123'),
# MAGIC   ('CUST002', 'Jane Doe', 'jane@company.com', '987-65-4321', '555-4567'),
# MAGIC   ('CUST003', 'Bob Johnson', 'bob.j@test.com', NULL, '555-8901')
# MAGIC AS t(customer_id, name, email, ssn, phone);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detect PII
# MAGIC INSERT INTO pii_scan_results
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   customer_id,
# MAGIC   'email',
# MAGIC   email IS NOT NULL,
# MAGIC   CASE WHEN email IS NOT NULL THEN ARRAY('EMAIL') ELSE ARRAY() END,
# MAGIC   1.0,
# MAGIC   current_timestamp(),
# MAGIC   ARRAY('GDPR', 'CCPA')
# MAGIC FROM customer_data_raw
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   customer_id,
# MAGIC   'ssn',
# MAGIC   ssn IS NOT NULL,
# MAGIC   CASE WHEN ssn IS NOT NULL THEN ARRAY('SSN') ELSE ARRAY() END,
# MAGIC   1.0,
# MAGIC   current_timestamp(),
# MAGIC   ARRAY('HIPAA', 'PCI-DSS')
# MAGIC FROM customer_data_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Data Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create masked data
# MAGIC CREATE OR REPLACE TABLE customer_data_masked AS
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   CONCAT(SUBSTRING(SPLIT(email, '@')[0], 1, 2), '***@', SPLIT(email, '@')[1]) as email_masked,
# MAGIC   CASE WHEN ssn IS NOT NULL THEN 'XXX-XX-XXXX' ELSE NULL END as ssn_masked,
# MAGIC   CONCAT('XXX-', SUBSTRING(phone, -4)) as phone_masked
# MAGIC FROM customer_data_raw;
# MAGIC
# MAGIC SELECT * FROM customer_data_masked;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Audit Trail

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create audit log
# MAGIC CREATE TABLE IF NOT EXISTS compliance_audit_log (
# MAGIC   audit_id STRING DEFAULT uuid(),
# MAGIC   action_type STRING,
# MAGIC   record_id STRING,
# MAGIC   user_name STRING,
# MAGIC   action_timestamp TIMESTAMP,
# MAGIC   pii_accessed BOOLEAN,
# MAGIC   justification STRING,
# MAGIC   compliance_approved BOOLEAN
# MAGIC );
# MAGIC
# MAGIC -- Log access
# MAGIC INSERT INTO compliance_audit_log
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   'DATA_ACCESS',
# MAGIC   customer_id,
# MAGIC   current_user(),
# MAGIC   current_timestamp(),
# MAGIC   TRUE,
# MAGIC   'Required for support case #12345',
# MAGIC   TRUE
# MAGIC FROM customer_data_raw
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Compliance Reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compliance dashboard
# MAGIC CREATE OR REPLACE VIEW compliance_dashboard AS
# MAGIC SELECT 
# MAGIC   'PII Records Scanned' as metric,
# MAGIC   CAST(COUNT(DISTINCT record_id) AS STRING) as value
# MAGIC FROM pii_scan_results
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'PII Types Detected',
# MAGIC   CAST(COUNT(DISTINCT EXPLODE(pii_types)) AS STRING)
# MAGIC FROM pii_scan_results
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Audit Log Entries',
# MAGIC   CAST(COUNT(*) AS STRING)
# MAGIC FROM compliance_audit_log
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Data Access Events',
# MAGIC   CAST(SUM(CASE WHEN pii_accessed THEN 1 ELSE 0 END) AS STRING)
# MAGIC FROM compliance_audit_log;
# MAGIC
# MAGIC SELECT * FROM compliance_dashboard;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   'Compliance System' as component,
# MAGIC   CASE WHEN 
# MAGIC     (SELECT COUNT(*) FROM pii_scan_results) > 0 AND
# MAGIC     (SELECT COUNT(*) FROM customer_data_masked) > 0 AND
# MAGIC     (SELECT COUNT(*) FROM compliance_audit_log) > 0
# MAGIC   THEN 'COMPLETE' ELSE 'INCOMPLETE' END as status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC Continue to **Challenge Lab 11: Multi-Tenant RAG System**
