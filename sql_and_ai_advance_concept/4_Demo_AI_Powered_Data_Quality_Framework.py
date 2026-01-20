# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: AI-Powered Data Quality Framework
# MAGIC
# MAGIC ## Overview
# MAGIC This demonstration shows how to build an AI-powered data quality framework using Databricks SQL AI Functions. You'll learn how to use AI for schema inference and validation, detect anomalies and outliers, identify and redact PII, and create comprehensive data profiling and quality scoring systems.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Use AI Functions for intelligent schema inference and validation
# MAGIC - Build anomaly detection systems with AI-powered pattern recognition
# MAGIC - Implement PII detection and redaction pipelines
# MAGIC - Create data profiling frameworks with quality scores
# MAGIC - Design automated data quality checks and alerts
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1-3
# MAGIC - Understanding of data quality concepts
# MAGIC - Familiarity with AI Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for data quality demo
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS data_quality;
# MAGIC USE SCHEMA data_quality;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Schema Inference and Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sample Data with Quality Issues

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample customer data with various quality issues
# MAGIC CREATE OR REPLACE TABLE raw_customer_data (
# MAGIC   record_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   registration_date STRING, -- Inconsistent format
# MAGIC   account_balance STRING, -- Should be numeric
# MAGIC   status STRING,
# MAGIC   notes STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO raw_customer_data VALUES
# MAGIC   ('REC001', 'John Smith', 'john.smith@email.com', '555-0123', '123 Main St, Boston, MA', '2024-01-15', '1250.00', 'active', 'VIP customer'),
# MAGIC   ('REC002', 'Jane Doe', 'jane.doe@email.com', '(555) 456-7890', '456 Oak Ave, New York, NY', '01/20/2024', '$2,500', 'active', 'Premium member'),
# MAGIC   ('REC003', NULL, 'invalidemail', '555.789.0123', 'Incomplete address', '2024-02-30', 'N/A', 'inactive', NULL), -- Multiple issues
# MAGIC   ('REC004', 'Robert Johnson', 'rob@company.com', '5551234567', '789 Pine Rd, Seattle, WA', '2024-01-10', '500', 'active', 'Standard account'),
# MAGIC   ('REC005', 'Sarah Williams SSN:123-45-6789', 'sarah@email.com', '555-2468', '321 Elm St, Austin, TX', 'January 25, 2024', '3500.50', 'active', 'Contains PII'),
# MAGIC   ('REC006', 'Mike Brown', NULL, '555-1357', '654 Maple Dr, Denver, CO', '2024-01-18', '-100.00', 'suspended', 'Negative balance'),
# MAGIC   ('REC007', 'test@test.com', 'lisa@test.com', '000-0000', 'N/A', '2024-13-45', '999999', 'active', 'Test record'),
# MAGIC   ('REC008', 'David Lee', 'david.lee@email.com', '555-8642', '987 Birch Ln, Miami, FL', '2024-01-22', '750.25', 'active', 'Credit card: 4532-1234-5678-9010');
# MAGIC
# MAGIC SELECT COUNT(*) as total_records FROM raw_customer_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI-Powered Schema Inference

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use AI to infer expected data types and formats
# MAGIC CREATE OR REPLACE TEMP VIEW schema_inference AS
# MAGIC SELECT 
# MAGIC   'customer_name' as column_name,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     'What data type should this be: customer name field containing "John Smith", "Jane Doe", etc.',
# MAGIC     ARRAY('text/string', 'numeric', 'date', 'email', 'phone', 'currency')
# MAGIC   ) as inferred_type,
# MAGIC   'Names should be non-empty strings with reasonable length' as validation_rule
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'email',
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     'What data type: email addresses like john@email.com',
# MAGIC     ARRAY('text/string', 'numeric', 'date', 'email', 'phone', 'currency')
# MAGIC   ),
# MAGIC   'Must be valid email format'
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'phone',
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     'What data type: phone numbers like 555-0123, (555) 456-7890',
# MAGIC     ARRAY('text/string', 'numeric', 'date', 'email', 'phone', 'currency')
# MAGIC   ),
# MAGIC   'Should follow standard phone format'
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'account_balance',
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     'What data type: account balance like 1250.00, $2,500, N/A',
# MAGIC     ARRAY('text/string', 'numeric', 'date', 'email', 'phone', 'currency')
# MAGIC   ),
# MAGIC   'Should be numeric currency value'
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'registration_date',
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     'What data type: dates like 2024-01-15, 01/20/2024, January 25, 2024',
# MAGIC     ARRAY('text/string', 'numeric', 'date', 'email', 'phone', 'currency')
# MAGIC   ),
# MAGIC   'Should be valid date format';
# MAGIC
# MAGIC SELECT * FROM schema_inference;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Data Against Inferred Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create validation results table
# MAGIC CREATE OR REPLACE TABLE data_validation_results AS
# MAGIC SELECT 
# MAGIC   record_id,
# MAGIC   -- Name validation
# MAGIC   CASE 
# MAGIC     WHEN customer_name IS NULL THEN FALSE
# MAGIC     WHEN LENGTH(TRIM(customer_name)) < 2 THEN FALSE
# MAGIC     WHEN customer_name RLIKE '^[A-Za-z\\s\\.\\-]+$' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END as name_valid,
# MAGIC   -- Email validation
# MAGIC   CASE 
# MAGIC     WHEN email IS NULL THEN FALSE
# MAGIC     WHEN email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END as email_valid,
# MAGIC   -- Phone validation (basic)
# MAGIC   CASE 
# MAGIC     WHEN phone IS NULL THEN FALSE
# MAGIC     WHEN REGEXP_REPLACE(phone, '[^0-9]', '') RLIKE '^[0-9]{10}$' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END as phone_valid,
# MAGIC   -- Date validation
# MAGIC   CASE 
# MAGIC     WHEN TRY_CAST(registration_date AS DATE) IS NOT NULL THEN TRUE
# MAGIC     WHEN registration_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END as date_valid,
# MAGIC   -- Balance validation
# MAGIC   CASE 
# MAGIC     WHEN TRY_CAST(REGEXP_REPLACE(REGEXP_REPLACE(account_balance, '[$,]', ''), '[^0-9.-]', '') AS DECIMAL(10,2)) IS NOT NULL THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END as balance_valid,
# MAGIC   -- Calculate overall quality score
# MAGIC   (
# MAGIC     CAST((customer_name IS NOT NULL AND LENGTH(TRIM(customer_name)) >= 2) AS INT) +
# MAGIC     CAST((email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$') AS INT) +
# MAGIC     CAST((REGEXP_REPLACE(phone, '[^0-9]', '') RLIKE '^[0-9]{10}$') AS INT) +
# MAGIC     CAST((TRY_CAST(registration_date AS DATE) IS NOT NULL) AS INT) +
# MAGIC     CAST((TRY_CAST(REGEXP_REPLACE(REGEXP_REPLACE(account_balance, '[$,]', ''), '[^0-9.-]', '') AS DECIMAL(10,2)) IS NOT NULL) AS INT)
# MAGIC   ) / 5.0 as quality_score
# MAGIC FROM raw_customer_data;
# MAGIC
# MAGIC -- View validation summary
# MAGIC SELECT 
# MAGIC   ROUND(AVG(quality_score) * 100, 2) as avg_quality_pct,
# MAGIC   SUM(CASE WHEN quality_score = 1.0 THEN 1 ELSE 0 END) as perfect_records,
# MAGIC   SUM(CASE WHEN quality_score >= 0.8 THEN 1 ELSE 0 END) as good_quality_records,
# MAGIC   SUM(CASE WHEN quality_score < 0.5 THEN 1 ELSE 0 END) as poor_quality_records,
# MAGIC   COUNT(*) as total_records
# MAGIC FROM data_validation_results;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: AI-Powered Anomaly Detection

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect Anomalies Using AI

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use AI to detect anomalous patterns
# MAGIC CREATE OR REPLACE TABLE anomaly_detection AS
# MAGIC SELECT 
# MAGIC   rcd.record_id,
# MAGIC   rcd.customer_name,
# MAGIC   rcd.email,
# MAGIC   rcd.account_balance,
# MAGIC   rcd.status,
# MAGIC   -- AI-powered anomaly detection
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT(
# MAGIC       'Is this customer record anomalous or suspicious? ',
# MAGIC       'Name: ', COALESCE(rcd.customer_name, 'NULL'), ', ',
# MAGIC       'Email: ', COALESCE(rcd.email, 'NULL'), ', ',
# MAGIC       'Balance: ', COALESCE(rcd.account_balance, 'NULL'), ', ',
# MAGIC       'Status: ', COALESCE(rcd.status, 'NULL')
# MAGIC     ),
# MAGIC     ARRAY('normal', 'suspicious', 'anomalous')
# MAGIC   ) as anomaly_classification,
# MAGIC   -- Specific anomaly checks
# MAGIC   ARRAY(
# MAGIC     CASE WHEN customer_name IS NULL THEN 'missing_name' END,
# MAGIC     CASE WHEN email IS NULL OR NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$' THEN 'invalid_email' END,
# MAGIC     CASE WHEN account_balance LIKE '%N/A%' OR account_balance LIKE '%null%' THEN 'missing_balance' END,
# MAGIC     CASE WHEN TRY_CAST(REGEXP_REPLACE(REGEXP_REPLACE(account_balance, '[$,]', ''), '[^0-9.-]', '') AS DECIMAL(10,2)) < 0 THEN 'negative_balance' END,
# MAGIC     CASE WHEN customer_name LIKE '%test%' OR email LIKE '%test%' THEN 'test_data' END,
# MAGIC     CASE WHEN customer_name LIKE '%SSN:%' OR notes LIKE '%SSN:%' THEN 'contains_ssn' END,
# MAGIC     CASE WHEN notes LIKE '%credit card:%' OR notes LIKE '%[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]%' THEN 'contains_credit_card' END
# MAGIC   ) as detected_anomalies
# MAGIC FROM raw_customer_data rcd;
# MAGIC
# MAGIC -- View anomaly summary
# MAGIC SELECT 
# MAGIC   anomaly_classification,
# MAGIC   COUNT(*) as record_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM anomaly_detection
# MAGIC GROUP BY anomaly_classification;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detailed anomaly breakdown
# MAGIC SELECT 
# MAGIC   record_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   anomaly_classification,
# MAGIC   FILTER(detected_anomalies, x -> x IS NOT NULL) as specific_issues,
# MAGIC   SIZE(FILTER(detected_anomalies, x -> x IS NOT NULL)) as issue_count
# MAGIC FROM anomaly_detection
# MAGIC WHERE anomaly_classification IN ('suspicious', 'anomalous')
# MAGIC   OR SIZE(FILTER(detected_anomalies, x -> x IS NOT NULL)) > 0
# MAGIC ORDER BY issue_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: PII Detection and Redaction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect PII Using AI Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create PII detection table
# MAGIC CREATE OR REPLACE TABLE pii_detection AS
# MAGIC SELECT 
# MAGIC   record_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   phone,
# MAGIC   address,
# MAGIC   notes,
# MAGIC   -- Detect PII in customer name field
# MAGIC   CASE 
# MAGIC     WHEN customer_name LIKE '%SSN:%' OR customer_name RLIKE '[0-9]{3}-[0-9]{2}-[0-9]{4}' THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END as name_contains_pii,
# MAGIC   -- Detect PII in notes field
# MAGIC   CASE 
# MAGIC     WHEN notes IS NULL THEN FALSE
# MAGIC     WHEN notes RLIKE '[0-9]{3}-[0-9]{2}-[0-9]{4}' THEN TRUE -- SSN pattern
# MAGIC     WHEN notes RLIKE '[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}' THEN TRUE -- Credit card pattern
# MAGIC     ELSE FALSE
# MAGIC   END as notes_contain_pii,
# MAGIC   -- Extract PII types using pattern matching
# MAGIC   ARRAY(
# MAGIC     CASE WHEN customer_name RLIKE '[0-9]{3}-[0-9]{2}-[0-9]{4}' OR notes RLIKE '[0-9]{3}-[0-9]{2}-[0-9]{4}' THEN 'SSN' END,
# MAGIC     CASE WHEN notes RLIKE '[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}' THEN 'CREDIT_CARD' END,
# MAGIC     CASE WHEN email IS NOT NULL THEN 'EMAIL' END,
# MAGIC     CASE WHEN phone IS NOT NULL THEN 'PHONE' END,
# MAGIC     CASE WHEN address IS NOT NULL AND address != 'N/A' THEN 'ADDRESS' END
# MAGIC   ) as pii_types_detected
# MAGIC FROM raw_customer_data;
# MAGIC
# MAGIC -- View PII detection summary
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN name_contains_pii THEN 1 ELSE 0 END) as records_with_pii_in_name,
# MAGIC   SUM(CASE WHEN notes_contain_pii THEN 1 ELSE 0 END) as records_with_pii_in_notes,
# MAGIC   SUM(CASE WHEN name_contains_pii OR notes_contain_pii THEN 1 ELSE 0 END) as records_needing_redaction
# MAGIC FROM pii_detection;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Redact PII from Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create redacted data table
# MAGIC CREATE OR REPLACE TABLE customer_data_redacted AS
# MAGIC SELECT 
# MAGIC   record_id,
# MAGIC   -- Redact PII from customer name
# MAGIC   REGEXP_REPLACE(
# MAGIC     customer_name, 
# MAGIC     'SSN:[0-9]{3}-[0-9]{2}-[0-9]{4}', 
# MAGIC     'SSN:[REDACTED]'
# MAGIC   ) as customer_name,
# MAGIC   -- Mask email (keep domain for analysis)
# MAGIC   CONCAT(
# MAGIC     SUBSTRING(SPLIT(email, '@')[0], 1, 2),
# MAGIC     '***@',
# MAGIC     SPLIT(email, '@')[1]
# MAGIC   ) as email_masked,
# MAGIC   -- Mask phone (keep last 4 digits)
# MAGIC   CONCAT('XXX-XXX-', SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), -4)) as phone_masked,
# MAGIC   -- Keep address as is (could add street number masking)
# MAGIC   address,
# MAGIC   -- Redact PII from notes
# MAGIC   REGEXP_REPLACE(
# MAGIC     REGEXP_REPLACE(
# MAGIC       COALESCE(notes, ''),
# MAGIC       '[0-9]{3}-[0-9]{2}-[0-9]{4}',
# MAGIC       'XXX-XX-XXXX'
# MAGIC     ),
# MAGIC     '[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}',
# MAGIC     'XXXX-XXXX-XXXX-XXXX'
# MAGIC   ) as notes_redacted,
# MAGIC   -- Original fields for comparison
# MAGIC   email as email_original,
# MAGIC   phone as phone_original,
# MAGIC   notes as notes_original
# MAGIC FROM raw_customer_data;
# MAGIC
# MAGIC -- View redacted data
# MAGIC SELECT 
# MAGIC   record_id,
# MAGIC   customer_name,
# MAGIC   email_original,
# MAGIC   email_masked,
# MAGIC   phone_original,
# MAGIC   phone_masked,
# MAGIC   notes_original,
# MAGIC   notes_redacted
# MAGIC FROM customer_data_redacted
# MAGIC WHERE notes_original IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create PII Audit Log

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create PII audit log
# MAGIC CREATE TABLE IF NOT EXISTS pii_audit_log (
# MAGIC   audit_id STRING DEFAULT uuid(),
# MAGIC   record_id STRING,
# MAGIC   pii_detected BOOLEAN,
# MAGIC   pii_types ARRAY<STRING>,
# MAGIC   redaction_performed BOOLEAN,
# MAGIC   fields_redacted ARRAY<STRING>,
# MAGIC   detected_by STRING,
# MAGIC   detection_method STRING,
# MAGIC   detection_timestamp TIMESTAMP,
# MAGIC   compliance_tags ARRAY<STRING>
# MAGIC );
# MAGIC
# MAGIC -- Log PII detection and redaction events
# MAGIC INSERT INTO pii_audit_log
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   record_id,
# MAGIC   name_contains_pii OR notes_contain_pii as pii_detected,
# MAGIC   FILTER(pii_types_detected, x -> x IS NOT NULL) as pii_types,
# MAGIC   name_contains_pii OR notes_contain_pii as redaction_performed,
# MAGIC   ARRAY(
# MAGIC     CASE WHEN name_contains_pii THEN 'customer_name' END,
# MAGIC     CASE WHEN notes_contain_pii THEN 'notes' END,
# MAGIC     'email',
# MAGIC     'phone'
# MAGIC   ) as fields_redacted,
# MAGIC   current_user() as detected_by,
# MAGIC   'pattern_matching_and_ai' as detection_method,
# MAGIC   current_timestamp() as detection_timestamp,
# MAGIC   ARRAY('GDPR', 'CCPA', 'HIPAA') as compliance_tags
# MAGIC FROM pii_detection
# MAGIC WHERE name_contains_pii OR notes_contain_pii;
# MAGIC
# MAGIC -- View audit log
# MAGIC SELECT 
# MAGIC   pii_detected,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT FLATTEN(pii_types)) as unique_pii_types_found
# MAGIC FROM pii_audit_log
# MAGIC GROUP BY pii_detected;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Data Profiling and Quality Scoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprehensive Data Profiling

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create data profile table
# MAGIC CREATE OR REPLACE TABLE data_profile AS
# MAGIC SELECT 
# MAGIC   'customer_name' as column_name,
# MAGIC   'STRING' as data_type,
# MAGIC   COUNT(*) as total_rows,
# MAGIC   COUNT(customer_name) as non_null_count,
# MAGIC   COUNT(*) - COUNT(customer_name) as null_count,
# MAGIC   ROUND(100.0 * COUNT(customer_name) / COUNT(*), 2) as completeness_pct,
# MAGIC   COUNT(DISTINCT customer_name) as distinct_count,
# MAGIC   ROUND(100.0 * COUNT(DISTINCT customer_name) / COUNT(customer_name), 2) as uniqueness_pct,
# MAGIC   MIN(LENGTH(customer_name)) as min_length,
# MAGIC   MAX(LENGTH(customer_name)) as max_length,
# MAGIC   AVG(LENGTH(customer_name)) as avg_length
# MAGIC FROM raw_customer_data
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'email',
# MAGIC   'STRING',
# MAGIC   COUNT(*),
# MAGIC   COUNT(email),
# MAGIC   COUNT(*) - COUNT(email),
# MAGIC   ROUND(100.0 * COUNT(email) / COUNT(*), 2),
# MAGIC   COUNT(DISTINCT email),
# MAGIC   ROUND(100.0 * COUNT(DISTINCT email) / NULLIF(COUNT(email), 0), 2),
# MAGIC   MIN(LENGTH(email)),
# MAGIC   MAX(LENGTH(email)),
# MAGIC   AVG(LENGTH(email))
# MAGIC FROM raw_customer_data
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'phone',
# MAGIC   'STRING',
# MAGIC   COUNT(*),
# MAGIC   COUNT(phone),
# MAGIC   COUNT(*) - COUNT(phone),
# MAGIC   ROUND(100.0 * COUNT(phone) / COUNT(*), 2),
# MAGIC   COUNT(DISTINCT phone),
# MAGIC   ROUND(100.0 * COUNT(DISTINCT phone) / NULLIF(COUNT(phone), 0), 2),
# MAGIC   MIN(LENGTH(phone)),
# MAGIC   MAX(LENGTH(phone)),
# MAGIC   AVG(LENGTH(phone))
# MAGIC FROM raw_customer_data;
# MAGIC
# MAGIC SELECT * FROM data_profile ORDER BY column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Quality Scores

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create comprehensive quality score table
# MAGIC CREATE OR REPLACE TABLE data_quality_scores AS
# MAGIC SELECT 
# MAGIC   record_id,
# MAGIC   -- Individual dimension scores
# MAGIC   dvr.quality_score as validity_score,
# MAGIC   CASE 
# MAGIC     WHEN customer_name IS NULL THEN 0.0
# MAGIC     WHEN email IS NULL THEN 0.2
# MAGIC     WHEN phone IS NULL THEN 0.4
# MAGIC     WHEN address IS NULL OR address = 'N/A' THEN 0.6
# MAGIC     WHEN notes IS NULL THEN 0.8
# MAGIC     ELSE 1.0
# MAGIC   END as completeness_score,
# MAGIC   CASE 
# MAGIC     WHEN ad.anomaly_classification = 'normal' THEN 1.0
# MAGIC     WHEN ad.anomaly_classification = 'suspicious' THEN 0.5
# MAGIC     WHEN ad.anomaly_classification = 'anomalous' THEN 0.0
# MAGIC     ELSE 0.7
# MAGIC   END as anomaly_score,
# MAGIC   CASE 
# MAGIC     WHEN pd.name_contains_pii OR pd.notes_contain_pii THEN 0.0
# MAGIC     ELSE 1.0
# MAGIC   END as privacy_score,
# MAGIC   -- Overall quality score (weighted average)
# MAGIC   ROUND(
# MAGIC     (dvr.quality_score * 0.4 +
# MAGIC      (CASE 
# MAGIC        WHEN customer_name IS NULL THEN 0.0
# MAGIC        WHEN email IS NULL THEN 0.2
# MAGIC        WHEN phone IS NULL THEN 0.4
# MAGIC        WHEN address IS NULL OR address = 'N/A' THEN 0.6
# MAGIC        WHEN notes IS NULL THEN 0.8
# MAGIC        ELSE 1.0
# MAGIC      END) * 0.3 +
# MAGIC      (CASE 
# MAGIC        WHEN ad.anomaly_classification = 'normal' THEN 1.0
# MAGIC        WHEN ad.anomaly_classification = 'suspicious' THEN 0.5
# MAGIC        ELSE 0.0
# MAGIC      END) * 0.2 +
# MAGIC      (CASE WHEN pd.name_contains_pii OR pd.notes_contain_pii THEN 0.0 ELSE 1.0 END) * 0.1
# MAGIC     ), 3
# MAGIC   ) as overall_quality_score,
# MAGIC   -- Quality tier
# MAGIC   CASE 
# MAGIC     WHEN ROUND((dvr.quality_score * 0.4 + 
# MAGIC               (CASE WHEN customer_name IS NULL THEN 0.0 WHEN email IS NULL THEN 0.2 
# MAGIC                WHEN phone IS NULL THEN 0.4 WHEN address IS NULL OR address = 'N/A' THEN 0.6 
# MAGIC                WHEN notes IS NULL THEN 0.8 ELSE 1.0 END) * 0.3 +
# MAGIC               (CASE WHEN ad.anomaly_classification = 'normal' THEN 1.0 
# MAGIC                WHEN ad.anomaly_classification = 'suspicious' THEN 0.5 ELSE 0.0 END) * 0.2 +
# MAGIC               (CASE WHEN pd.name_contains_pii OR pd.notes_contain_pii THEN 0.0 ELSE 1.0 END) * 0.1
# MAGIC               ), 3) >= 0.9 THEN 'EXCELLENT'
# MAGIC     WHEN ROUND((dvr.quality_score * 0.4 + 
# MAGIC               (CASE WHEN customer_name IS NULL THEN 0.0 WHEN email IS NULL THEN 0.2 
# MAGIC                WHEN phone IS NULL THEN 0.4 WHEN address IS NULL OR address = 'N/A' THEN 0.6 
# MAGIC                WHEN notes IS NULL THEN 0.8 ELSE 1.0 END) * 0.3 +
# MAGIC               (CASE WHEN ad.anomaly_classification = 'normal' THEN 1.0 
# MAGIC                WHEN ad.anomaly_classification = 'suspicious' THEN 0.5 ELSE 0.0 END) * 0.2 +
# MAGIC               (CASE WHEN pd.name_contains_pii OR pd.notes_contain_pii THEN 0.0 ELSE 1.0 END) * 0.1
# MAGIC               ), 3) >= 0.7 THEN 'GOOD'
# MAGIC     WHEN ROUND((dvr.quality_score * 0.4 + 
# MAGIC               (CASE WHEN customer_name IS NULL THEN 0.0 WHEN email IS NULL THEN 0.2 
# MAGIC                WHEN phone IS NULL THEN 0.4 WHEN address IS NULL OR address = 'N/A' THEN 0.6 
# MAGIC                WHEN notes IS NULL THEN 0.8 ELSE 1.0 END) * 0.3 +
# MAGIC               (CASE WHEN ad.anomaly_classification = 'normal' THEN 1.0 
# MAGIC                WHEN ad.anomaly_classification = 'suspicious' THEN 0.5 ELSE 0.0 END) * 0.2 +
# MAGIC               (CASE WHEN pd.name_contains_pii OR pd.notes_contain_pii THEN 0.0 ELSE 1.0 END) * 0.1
# MAGIC               ), 3) >= 0.5 THEN 'FAIR'
# MAGIC     ELSE 'POOR'
# MAGIC   END as quality_tier
# MAGIC FROM raw_customer_data rcd
# MAGIC JOIN data_validation_results dvr ON rcd.record_id = dvr.record_id
# MAGIC JOIN anomaly_detection ad ON rcd.record_id = ad.record_id
# MAGIC JOIN pii_detection pd ON rcd.record_id = pd.record_id;
# MAGIC
# MAGIC -- View quality score distribution
# MAGIC SELECT 
# MAGIC   quality_tier,
# MAGIC   COUNT(*) as record_count,
# MAGIC   ROUND(AVG(overall_quality_score), 3) as avg_quality_score,
# MAGIC   ROUND(AVG(validity_score), 3) as avg_validity,
# MAGIC   ROUND(AVG(completeness_score), 3) as avg_completeness,
# MAGIC   ROUND(AVG(anomaly_score), 3) as avg_anomaly_score
# MAGIC FROM data_quality_scores
# MAGIC GROUP BY quality_tier
# MAGIC ORDER BY 
# MAGIC   CASE quality_tier
# MAGIC     WHEN 'EXCELLENT' THEN 1
# MAGIC     WHEN 'GOOD' THEN 2
# MAGIC     WHEN 'FAIR' THEN 3
# MAGIC     WHEN 'POOR' THEN 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Automated Quality Checks and Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create quality check definitions
# MAGIC CREATE TABLE IF NOT EXISTS quality_check_definitions (
# MAGIC   check_id STRING DEFAULT uuid(),
# MAGIC   check_name STRING,
# MAGIC   check_type STRING, -- 'completeness', 'validity', 'anomaly', 'privacy'
# MAGIC   column_name STRING,
# MAGIC   threshold_value DOUBLE,
# MAGIC   severity STRING, -- 'critical', 'warning', 'info'
# MAGIC   is_active BOOLEAN
# MAGIC );
# MAGIC
# MAGIC -- Define quality checks
# MAGIC INSERT INTO quality_check_definitions (check_name, check_type, column_name, threshold_value, severity, is_active)
# MAGIC VALUES
# MAGIC   ('Completeness Check - Names', 'completeness', 'customer_name', 0.95, 'critical', TRUE),
# MAGIC   ('Completeness Check - Emails', 'completeness', 'email', 0.90, 'warning', TRUE),
# MAGIC   ('Validity Check - Email Format', 'validity', 'email', 0.95, 'critical', TRUE),
# MAGIC   ('Anomaly Rate Check', 'anomaly', 'all', 0.10, 'warning', TRUE),
# MAGIC   ('PII Detection Check', 'privacy', 'all', 0.0, 'critical', TRUE),
# MAGIC   ('Overall Quality Check', 'quality_score', 'all', 0.80, 'warning', TRUE);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Execute quality checks
# MAGIC CREATE OR REPLACE TABLE quality_check_results AS
# MAGIC -- Check 1: Completeness
# MAGIC SELECT 
# MAGIC   current_timestamp() as check_timestamp,
# MAGIC   'Completeness Check - Names' as check_name,
# MAGIC   'completeness' as check_type,
# MAGIC   ROUND(100.0 * COUNT(customer_name) / COUNT(*), 2) as actual_value,
# MAGIC   95.0 as threshold_value,
# MAGIC   CASE WHEN 100.0 * COUNT(customer_name) / COUNT(*) >= 95.0 THEN 'PASS' ELSE 'FAIL' END as check_status,
# MAGIC   'critical' as severity
# MAGIC FROM raw_customer_data
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 2: Email validity
# MAGIC SELECT 
# MAGIC   current_timestamp(),
# MAGIC   'Validity Check - Email Format',
# MAGIC   'validity',
# MAGIC   ROUND(100.0 * SUM(CASE WHEN email_valid THEN 1 ELSE 0 END) / COUNT(*), 2),
# MAGIC   95.0,
# MAGIC   CASE WHEN 100.0 * SUM(CASE WHEN email_valid THEN 1 ELSE 0 END) / COUNT(*) >= 95.0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   'critical'
# MAGIC FROM data_validation_results
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 3: Anomaly rate
# MAGIC SELECT 
# MAGIC   current_timestamp(),
# MAGIC   'Anomaly Rate Check',
# MAGIC   'anomaly',
# MAGIC   ROUND(100.0 * SUM(CASE WHEN anomaly_classification IN ('suspicious', 'anomalous') THEN 1 ELSE 0 END) / COUNT(*), 2),
# MAGIC   10.0,
# MAGIC   CASE WHEN 100.0 * SUM(CASE WHEN anomaly_classification IN ('suspicious', 'anomalous') THEN 1 ELSE 0 END) / COUNT(*) <= 10.0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   'warning'
# MAGIC FROM anomaly_detection
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 4: PII detection
# MAGIC SELECT 
# MAGIC   current_timestamp(),
# MAGIC   'PII Detection Check',
# MAGIC   'privacy',
# MAGIC   ROUND(100.0 * SUM(CASE WHEN name_contains_pii OR notes_contain_pii THEN 1 ELSE 0 END) / COUNT(*), 2),
# MAGIC   0.0,
# MAGIC   CASE WHEN SUM(CASE WHEN name_contains_pii OR notes_contain_pii THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   'critical'
# MAGIC FROM pii_detection
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Check 5: Overall quality score
# MAGIC SELECT 
# MAGIC   current_timestamp(),
# MAGIC   'Overall Quality Check',
# MAGIC   'quality_score',
# MAGIC   ROUND(AVG(overall_quality_score) * 100, 2),
# MAGIC   80.0,
# MAGIC   CASE WHEN AVG(overall_quality_score) >= 0.80 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   'warning'
# MAGIC FROM data_quality_scores;
# MAGIC
# MAGIC -- View quality check results
# MAGIC SELECT 
# MAGIC   check_name,
# MAGIC   check_type,
# MAGIC   actual_value,
# MAGIC   threshold_value,
# MAGIC   check_status,
# MAGIC   severity,
# MAGIC   CASE 
# MAGIC     WHEN check_status = 'FAIL' AND severity = 'critical' THEN 'IMMEDIATE ACTION REQUIRED'
# MAGIC     WHEN check_status = 'FAIL' AND severity = 'warning' THEN 'Review and address soon'
# MAGIC     ELSE 'No action needed'
# MAGIC   END as recommendation
# MAGIC FROM quality_check_results
# MAGIC ORDER BY 
# MAGIC   CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
# MAGIC   check_status DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Dashboard Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comprehensive quality dashboard
# MAGIC SELECT 
# MAGIC   'Data Quality Summary' as report_section,
# MAGIC   COUNT(*) as total_records,
# MAGIC   ROUND(AVG(overall_quality_score) * 100, 2) as avg_quality_score_pct,
# MAGIC   SUM(CASE WHEN quality_tier = 'EXCELLENT' THEN 1 ELSE 0 END) as excellent_records,
# MAGIC   SUM(CASE WHEN quality_tier = 'GOOD' THEN 1 ELSE 0 END) as good_records,
# MAGIC   SUM(CASE WHEN quality_tier IN ('FAIR', 'POOR') THEN 1 ELSE 0 END) as needs_improvement,
# MAGIC   (SELECT COUNT(*) FROM quality_check_results WHERE check_status = 'FAIL' AND severity = 'critical') as critical_failures,
# MAGIC   (SELECT COUNT(*) FROM quality_check_results WHERE check_status = 'FAIL' AND severity = 'warning') as warning_failures,
# MAGIC   (SELECT COUNT(*) FROM pii_audit_log WHERE pii_detected = TRUE) as records_with_pii
# MAGIC FROM data_quality_scores;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### AI-Powered Data Quality Benefits
# MAGIC 1. **Intelligent Schema Inference**: AI can understand data types and patterns
# MAGIC 2. **Context-Aware Validation**: Goes beyond rules to understand semantics
# MAGIC 3. **Anomaly Detection**: Identifies suspicious patterns humans might miss
# MAGIC 4. **PII Detection**: Automatically finds sensitive data across free text
# MAGIC 5. **Comprehensive Profiling**: Multi-dimensional quality assessment
# MAGIC
# MAGIC ### Quality Framework Components
# MAGIC - ✓ Schema inference and validation
# MAGIC - ✓ Anomaly and outlier detection
# MAGIC - ✓ PII detection and redaction
# MAGIC - ✓ Data profiling with statistics
# MAGIC - ✓ Quality scoring across dimensions
# MAGIC - ✓ Automated quality checks
# MAGIC - ✓ Audit logging for compliance
# MAGIC
# MAGIC ### Production Considerations
# MAGIC - **Performance**: Cache AI results for repeated checks
# MAGIC - **Cost**: Balance AI usage with rule-based validation
# MAGIC - **Accuracy**: Combine AI with deterministic rules
# MAGIC - **Compliance**: Maintain audit trails for all PII handling
# MAGIC - **Monitoring**: Track quality trends over time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Demo 5: Incremental Processing and Optimization** to learn:
# MAGIC - Change Data Capture (CDC) with AI Functions
# MAGIC - Checkpoint management for large-scale processing
# MAGIC - Adaptive batching strategies
# MAGIC - Cost-aware processing patterns
