# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Apply Tags on Unity Catalog Objects
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on lab provides guided exercises to reinforce your learning about Unity Catalog tagging. You'll apply tags to schemas and tables, query and audit tags using information schema, create stored procedures for automation, and implement governance tagging strategies. Each exercise includes clear instructions, verification steps, and complete solutions.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Apply user-defined tags to Unity Catalog schemas and tables independently
# MAGIC - Query information schema views to verify and audit applied tags
# MAGIC - Write SQL queries to find objects based on tag criteria
# MAGIC - Create stored procedures to automate tagging across multiple objects
# MAGIC - Design and implement a comprehensive tagging strategy for governance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of **1 Demo - Unity Catalog Tagging Overview**
# MAGIC - A Databricks workspace with Unity Catalog enabled
# MAGIC - A SQL Warehouse or compute cluster
# MAGIC - Permissions to create schemas and tables in a catalog you own
# MAGIC - Basic familiarity with SQL commands
# MAGIC
# MAGIC ## Lab Structure
# MAGIC - **Setup**: Create lab environment with sample data
# MAGIC - **Exercises 1-5**: Progressively challenging tagging tasks
# MAGIC - **Challenge Exercise**: Comprehensive scenario combining all skills
# MAGIC - **Solutions**: Provided after each exercise
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Setup
# MAGIC
# MAGIC First, let's create a dedicated schema for this lab and populate it with sample data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Setup catalog and schema
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS tagging_lab
# MAGIC COMMENT 'Lab environment for practicing Unity Catalog tagging';
# MAGIC
# MAGIC USE SCHEMA tagging_lab;
# MAGIC
# MAGIC -- Verify current location
# MAGIC SELECT current_catalog() as catalog, current_schema() as schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sample Data
# MAGIC
# MAGIC We'll create tables representing a healthcare system with different data sensitivity levels.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create patients table (highly sensitive PII and PHI)
# MAGIC CREATE OR REPLACE TABLE patients (
# MAGIC   patient_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   date_of_birth DATE,
# MAGIC   ssn STRING,
# MAGIC   phone STRING,
# MAGIC   email STRING,
# MAGIC   address STRING,
# MAGIC   insurance_id STRING
# MAGIC )
# MAGIC COMMENT 'Patient master data - contains PII and PHI';
# MAGIC
# MAGIC INSERT INTO patients VALUES
# MAGIC   ('P001', 'John', 'Smith', '1980-05-15', '123-45-6789', '555-1001', 'john.smith@email.com', '100 Main St, Boston, MA', 'INS001'),
# MAGIC   ('P002', 'Sarah', 'Johnson', '1975-08-22', '234-56-7890', '555-1002', 'sarah.j@email.com', '200 Oak Ave, Boston, MA', 'INS002'),
# MAGIC   ('P003', 'Michael', 'Williams', '1992-03-10', '345-67-8901', '555-1003', 'mwilliams@email.com', '300 Pine St, Boston, MA', 'INS003'),
# MAGIC   ('P004', 'Emily', 'Brown', '1988-11-30', '456-78-9012', '555-1004', 'ebrown@email.com', '400 Elm Rd, Boston, MA', 'INS004'),
# MAGIC   ('P005', 'David', 'Davis', '1965-07-18', '567-89-0123', '555-1005', 'ddavis@email.com', '500 Maple Dr, Boston, MA', 'INS005');
# MAGIC
# MAGIC SELECT * FROM patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create medical_records table (protected health information)
# MAGIC CREATE OR REPLACE TABLE medical_records (
# MAGIC   record_id STRING,
# MAGIC   patient_id STRING,
# MAGIC   visit_date DATE,
# MAGIC   diagnosis_code STRING,
# MAGIC   diagnosis_description STRING,
# MAGIC   treatment STRING,
# MAGIC   provider_id STRING
# MAGIC )
# MAGIC COMMENT 'Medical records - contains protected health information';
# MAGIC
# MAGIC INSERT INTO medical_records VALUES
# MAGIC   ('R001', 'P001', '2024-12-01', 'J00', 'Acute nasopharyngitis (common cold)', 'Rest and fluids', 'DR001'),
# MAGIC   ('R002', 'P001', '2025-01-05', 'E11', 'Type 2 diabetes mellitus', 'Metformin 500mg', 'DR002'),
# MAGIC   ('R003', 'P002', '2024-11-15', 'I10', 'Essential hypertension', 'Lisinopril 10mg', 'DR001'),
# MAGIC   ('R004', 'P003', '2024-12-20', 'M54.5', 'Low back pain', 'Physical therapy', 'DR003'),
# MAGIC   ('R005', 'P004', '2025-01-08', 'J45.0', 'Asthma', 'Albuterol inhaler', 'DR002');
# MAGIC
# MAGIC SELECT * FROM medical_records;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create appointments table (operational data)
# MAGIC CREATE OR REPLACE TABLE appointments (
# MAGIC   appointment_id STRING,
# MAGIC   patient_id STRING,
# MAGIC   provider_id STRING,
# MAGIC   appointment_date TIMESTAMP,
# MAGIC   status STRING,
# MAGIC   appointment_type STRING
# MAGIC )
# MAGIC COMMENT 'Appointment scheduling data';
# MAGIC
# MAGIC INSERT INTO appointments VALUES
# MAGIC   ('A001', 'P001', 'DR001', '2025-01-15 09:00:00', 'scheduled', 'follow_up'),
# MAGIC   ('A002', 'P002', 'DR001', '2025-01-15 10:30:00', 'scheduled', 'routine_checkup'),
# MAGIC   ('A003', 'P003', 'DR003', '2025-01-16 14:00:00', 'scheduled', 'physical_therapy'),
# MAGIC   ('A004', 'P004', 'DR002', '2025-01-17 11:00:00', 'scheduled', 'follow_up'),
# MAGIC   ('A005', 'P005', 'DR001', '2025-01-20 15:30:00', 'scheduled', 'routine_checkup'),
# MAGIC   ('A006', 'P001', 'DR002', '2024-12-20 09:00:00', 'completed', 'initial_visit');
# MAGIC
# MAGIC SELECT * FROM appointments;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create providers table (reference data)
# MAGIC CREATE OR REPLACE TABLE providers (
# MAGIC   provider_id STRING,
# MAGIC   name STRING,
# MAGIC   specialty STRING,
# MAGIC   license_number STRING,
# MAGIC   phone STRING,
# MAGIC   email STRING
# MAGIC )
# MAGIC COMMENT 'Healthcare provider directory';
# MAGIC
# MAGIC INSERT INTO providers VALUES
# MAGIC   ('DR001', 'Dr. Amanda Wilson', 'Internal Medicine', 'MD-12345', '555-2001', 'awilson@hospital.org'),
# MAGIC   ('DR002', 'Dr. Robert Chen', 'Endocrinology', 'MD-23456', '555-2002', 'rchen@hospital.org'),
# MAGIC   ('DR003', 'Dr. Lisa Martinez', 'Physical Medicine', 'MD-34567', '555-2003', 'lmartinez@hospital.org');
# MAGIC
# MAGIC SELECT * FROM providers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create billing_summary table (aggregated financial data)
# MAGIC CREATE OR REPLACE TABLE billing_summary (
# MAGIC   summary_id STRING,
# MAGIC   patient_id STRING,
# MAGIC   month DATE,
# MAGIC   total_charges DECIMAL(10,2),
# MAGIC   insurance_paid DECIMAL(10,2),
# MAGIC   patient_paid DECIMAL(10,2),
# MAGIC   outstanding_balance DECIMAL(10,2)
# MAGIC )
# MAGIC COMMENT 'Monthly billing summary by patient';
# MAGIC
# MAGIC INSERT INTO billing_summary VALUES
# MAGIC   ('B001', 'P001', '2024-12-01', 450.00, 360.00, 90.00, 0.00),
# MAGIC   ('B002', 'P002', '2024-11-01', 250.00, 225.00, 25.00, 0.00),
# MAGIC   ('B003', 'P003', '2024-12-01', 800.00, 640.00, 100.00, 60.00),
# MAGIC   ('B004', 'P004', '2025-01-01', 180.00, 144.00, 36.00, 0.00);
# MAGIC
# MAGIC SELECT * FROM billing_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create department_metrics table (analytics)
# MAGIC CREATE OR REPLACE TABLE department_metrics (
# MAGIC   metric_id STRING,
# MAGIC   department STRING,
# MAGIC   month DATE,
# MAGIC   patient_count INT,
# MAGIC   appointment_count INT,
# MAGIC   avg_wait_time_minutes INT,
# MAGIC   satisfaction_score DECIMAL(3,2)
# MAGIC )
# MAGIC COMMENT 'Departmental performance metrics';
# MAGIC
# MAGIC INSERT INTO department_metrics VALUES
# MAGIC   ('M001', 'Internal Medicine', '2024-12-01', 45, 120, 15, 4.5),
# MAGIC   ('M002', 'Endocrinology', '2024-12-01', 30, 85, 20, 4.7),
# MAGIC   ('M003', 'Physical Medicine', '2024-12-01', 35, 95, 12, 4.6);
# MAGIC
# MAGIC SELECT * FROM department_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Apply Tags to a Schema
# MAGIC
# MAGIC **Objective**: Practice tagging a schema with organizational metadata.
# MAGIC
# MAGIC ### Task
# MAGIC
# MAGIC Apply the following tags to the `tagging_lab` schema:
# MAGIC - `owner` = 'healthcare_it_team'
# MAGIC - `environment` = 'training'
# MAGIC - `department` = 'healthcare'
# MAGIC - `cost_center` = 'HC-LAB-001'
# MAGIC
# MAGIC ### Instructions
# MAGIC
# MAGIC 1. Write an `ALTER SCHEMA` statement with `SET TAGS` clause
# MAGIC 2. Execute the statement
# MAGIC 3. Verify the tags by querying `system.information_schema.schema_tags`
# MAGIC
# MAGIC ### Your Solution
# MAGIC
# MAGIC Write your code in the cell below:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Apply tags to the tagging_lab schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify your tags (run this after applying tags)
# MAGIC SELECT 
# MAGIC   schema_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.schema_tags
# MAGIC WHERE schema_name = 'tagging_lab'
# MAGIC ORDER BY tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1: Solution
# MAGIC
# MAGIC Click "Show" below to reveal the solution after attempting the exercise.
# MAGIC
# MAGIC <details>
# MAGIC <summary><b>Show Solution</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC -- Apply tags to the schema
# MAGIC ALTER SCHEMA tagging_lab
# MAGIC SET TAGS (
# MAGIC   'owner' = 'healthcare_it_team',
# MAGIC   'environment' = 'training',
# MAGIC   'department' = 'healthcare',
# MAGIC   'cost_center' = 'HC-LAB-001'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# Uncomment and run this cell to see the solution in action
# %sql
# ALTER SCHEMA tagging_lab
# SET TAGS (
#   'owner' = 'healthcare_it_team',
#   'environment' = 'training',
#   'department' = 'healthcare',
#   'cost_center' = 'HC-LAB-001'
# );

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Apply Multiple Tags to Tables
# MAGIC
# MAGIC **Objective**: Practice tagging tables with data classification and governance metadata.
# MAGIC
# MAGIC ### Task
# MAGIC
# MAGIC Apply appropriate tags to the following tables based on their data sensitivity:
# MAGIC
# MAGIC #### Table 1: `patients`
# MAGIC - `data_classification` = 'restricted'
# MAGIC - `contains_pii` = 'true'
# MAGIC - `contains_phi` = 'true'
# MAGIC - `pii_types` = 'name,ssn,dob,address,email,phone'
# MAGIC - `compliance_requirement` = 'HIPAA'
# MAGIC - `retention_years` = '7'
# MAGIC - `data_owner` = 'patient_services'
# MAGIC
# MAGIC #### Table 2: `medical_records`
# MAGIC - `data_classification` = 'restricted'
# MAGIC - `contains_pii` = 'false'
# MAGIC - `contains_phi` = 'true'
# MAGIC - `compliance_requirement` = 'HIPAA'
# MAGIC - `retention_years` = '10'
# MAGIC - `data_owner` = 'medical_records_dept'
# MAGIC
# MAGIC ### Instructions
# MAGIC
# MAGIC 1. Write `ALTER TABLE` statements for both tables
# MAGIC 2. Execute the statements
# MAGIC 3. Verify the tags were applied correctly
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Tag the patients table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Tag the medical_records table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify your tags (run this after applying tags)
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_lab'
# MAGIC   AND table_name IN ('patients', 'medical_records')
# MAGIC ORDER BY table_name, tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Solution
# MAGIC
# MAGIC <details>
# MAGIC <summary><b>Show Solution</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC -- Tag the patients table
# MAGIC ALTER TABLE patients
# MAGIC SET TAGS (
# MAGIC   'data_classification' = 'restricted',
# MAGIC   'contains_pii' = 'true',
# MAGIC   'contains_phi' = 'true',
# MAGIC   'pii_types' = 'name,ssn,dob,address,email,phone',
# MAGIC   'compliance_requirement' = 'HIPAA',
# MAGIC   'retention_years' = '7',
# MAGIC   'data_owner' = 'patient_services'
# MAGIC );
# MAGIC
# MAGIC -- Tag the medical_records table
# MAGIC ALTER TABLE medical_records
# MAGIC SET TAGS (
# MAGIC   'data_classification' = 'restricted',
# MAGIC   'contains_pii' = 'false',
# MAGIC   'contains_phi' = 'true',
# MAGIC   'compliance_requirement' = 'HIPAA',
# MAGIC   'retention_years' = '10',
# MAGIC   'data_owner' = 'medical_records_dept'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# Uncomment and run these cells to see the solution in action
# %sql
# ALTER TABLE patients
# SET TAGS (
#   'data_classification' = 'restricted',
#   'contains_pii' = 'true',
#   'contains_phi' = 'true',
#   'pii_types' = 'name,ssn,dob,address,email,phone',
#   'compliance_requirement' = 'HIPAA',
#   'retention_years' = '7',
#   'data_owner' = 'patient_services'
# );

# COMMAND ----------

# %sql
# ALTER TABLE medical_records
# SET TAGS (
#   'data_classification' = 'restricted',
#   'contains_pii' = 'false',
#   'contains_phi' = 'true',
#   'compliance_requirement' = 'HIPAA',
#   'retention_years' = '10',
#   'data_owner' = 'medical_records_dept'
# );

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: Query and Audit Tags
# MAGIC
# MAGIC **Objective**: Practice writing queries to find and audit tagged objects.
# MAGIC
# MAGIC ### Task
# MAGIC
# MAGIC Write SQL queries to answer the following questions:
# MAGIC
# MAGIC #### Question 1
# MAGIC Find all tables in the `tagging_lab` schema that contain Protected Health Information (PHI).
# MAGIC
# MAGIC #### Question 2
# MAGIC Find all tables with a `data_classification` of 'restricted', and show their owners and retention periods.
# MAGIC
# MAGIC #### Question 3
# MAGIC Create a summary view showing each table with its classification, whether it contains PII, whether it contains PHI, and its owner (pivot the tags into columns).
# MAGIC
# MAGIC ### Instructions
# MAGIC
# MAGIC Write your queries in the cells below. Use the `system.information_schema.table_tags` view.
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Question 1: Find all tables containing PHI
# MAGIC -- YOUR CODE HERE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Question 2: Find restricted tables with owners and retention
# MAGIC -- YOUR CODE HERE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Question 3: Create a pivoted summary view
# MAGIC -- YOUR CODE HERE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3: Solution
# MAGIC
# MAGIC <details>
# MAGIC <summary><b>Show Solution</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC -- Question 1: Find all tables containing PHI
# MAGIC SELECT DISTINCT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   tag_value as contains_phi
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_lab'
# MAGIC   AND tag_name = 'contains_phi'
# MAGIC   AND tag_value = 'true'
# MAGIC ORDER BY table_name;
# MAGIC
# MAGIC -- Question 2: Find restricted tables with owners and retention
# MAGIC SELECT DISTINCT
# MAGIC   t1.table_name,
# MAGIC   t1.tag_value as classification,
# MAGIC   MAX(CASE WHEN t2.tag_name = 'data_owner' THEN t2.tag_value END) as owner,
# MAGIC   MAX(CASE WHEN t2.tag_name = 'retention_years' THEN t2.tag_value END) as retention_years
# MAGIC FROM system.information_schema.table_tags t1
# MAGIC LEFT JOIN system.information_schema.table_tags t2
# MAGIC   ON t1.catalog_name = t2.catalog_name
# MAGIC   AND t1.schema_name = t2.schema_name
# MAGIC   AND t1.table_name = t2.table_name
# MAGIC WHERE t1.schema_name = 'tagging_lab'
# MAGIC   AND t1.tag_name = 'data_classification'
# MAGIC   AND t1.tag_value = 'restricted'
# MAGIC GROUP BY t1.table_name, t1.tag_value
# MAGIC ORDER BY t1.table_name;
# MAGIC
# MAGIC -- Question 3: Create a pivoted summary view
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   MAX(CASE WHEN tag_name = 'data_classification' THEN tag_value END) as classification,
# MAGIC   MAX(CASE WHEN tag_name = 'contains_pii' THEN tag_value END) as contains_pii,
# MAGIC   MAX(CASE WHEN tag_name = 'contains_phi' THEN tag_value END) as contains_phi,
# MAGIC   MAX(CASE WHEN tag_name = 'data_owner' THEN tag_value END) as owner,
# MAGIC   MAX(CASE WHEN tag_name = 'retention_years' THEN tag_value END) as retention_years
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_lab'
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name;
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# Uncomment to run solution for Question 1
# %sql
# SELECT DISTINCT
#   catalog_name,
#   schema_name,
#   table_name,
#   tag_value as contains_phi
# FROM system.information_schema.table_tags
# WHERE schema_name = 'tagging_lab'
#   AND tag_name = 'contains_phi'
#   AND tag_value = 'true'
# ORDER BY table_name;

# COMMAND ----------

# Uncomment to run solution for Question 2
# %sql
# SELECT DISTINCT
#   t1.table_name,
#   t1.tag_value as classification,
#   MAX(CASE WHEN t2.tag_name = 'data_owner' THEN t2.tag_value END) as owner,
#   MAX(CASE WHEN t2.tag_name = 'retention_years' THEN t2.tag_value END) as retention_years
# FROM system.information_schema.table_tags t1
# LEFT JOIN system.information_schema.table_tags t2
#   ON t1.catalog_name = t2.catalog_name
#   AND t1.schema_name = t2.schema_name
#   AND t1.table_name = t2.table_name
# WHERE t1.schema_name = 'tagging_lab'
#   AND t1.tag_name = 'data_classification'
#   AND t1.tag_value = 'restricted'
# GROUP BY t1.table_name, t1.tag_value
# ORDER BY t1.table_name;

# COMMAND ----------

# Uncomment to run solution for Question 3
# %sql
# SELECT 
#   table_name,
#   MAX(CASE WHEN tag_name = 'data_classification' THEN tag_value END) as classification,
#   MAX(CASE WHEN tag_name = 'contains_pii' THEN tag_value END) as contains_pii,
#   MAX(CASE WHEN tag_name = 'contains_phi' THEN tag_value END) as contains_phi,
#   MAX(CASE WHEN tag_name = 'data_owner' THEN tag_value END) as owner,
#   MAX(CASE WHEN tag_name = 'retention_years' THEN tag_value END) as retention_years
# FROM system.information_schema.table_tags
# WHERE schema_name = 'tagging_lab'
# GROUP BY table_name
# ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Create a Stored Procedure to Automate Tagging
# MAGIC
# MAGIC **Objective**: Build a reusable stored procedure to apply standard tags to multiple tables.
# MAGIC
# MAGIC ### Task
# MAGIC
# MAGIC Create a stored procedure named `apply_standard_governance_tags` that:
# MAGIC - Accepts parameters: `schema_name`, `table_name`, `classification`, `owner`, `retention_years`
# MAGIC - Applies these tags to the specified table
# MAGIC - Returns a confirmation message
# MAGIC
# MAGIC Then, use the procedure to tag the remaining tables:
# MAGIC
# MAGIC #### Table: `appointments`
# MAGIC - classification: 'internal'
# MAGIC - owner: 'scheduling_team'
# MAGIC - retention_years: 3
# MAGIC
# MAGIC #### Table: `providers`
# MAGIC - classification: 'internal'
# MAGIC - owner: 'hr_department'
# MAGIC - retention_years: 10
# MAGIC
# MAGIC #### Table: `billing_summary`
# MAGIC - classification: 'confidential'
# MAGIC - owner: 'finance_team'
# MAGIC - retention_years: 7
# MAGIC
# MAGIC ### Instructions
# MAGIC
# MAGIC 1. Create the stored procedure
# MAGIC 2. Call the procedure for each table
# MAGIC 3. Verify the tags were applied
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Create the stored procedure

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Call the procedure for the appointments table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Call the procedure for the providers table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Call the procedure for the billing_summary table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the tags were applied
# MAGIC SELECT 
# MAGIC #   table_name,
# MAGIC #   MAX(CASE WHEN tag_name = 'data_classification' THEN tag_value END) as classification,
# MAGIC #   MAX(CASE WHEN tag_name = 'data_owner' THEN tag_value END) as owner,
# MAGIC #   MAX(CASE WHEN tag_name = 'retention_years' THEN tag_value END) as retention_years
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_lab'
# MAGIC   AND table_name IN ('appointments', 'providers', 'billing_summary')
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4: Solution
# MAGIC
# MAGIC <details>
# MAGIC <summary><b>Show Solution</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC -- Create the stored procedure
# MAGIC CREATE OR REPLACE PROCEDURE apply_standard_governance_tags(
# MAGIC   schema_name STRING,
# MAGIC   table_name STRING,
# MAGIC   classification STRING,
# MAGIC   owner STRING,
# MAGIC   retention_years INT
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Apply standard governance tags to a table'
# MAGIC AS
# MAGIC BEGIN
# MAGIC   DECLARE sql_stmt STRING;
# MAGIC   
# MAGIC   -- Build ALTER TABLE statement
# MAGIC   SET VARIABLE sql_stmt = CONCAT(
# MAGIC     'ALTER TABLE ', schema_name, '.', table_name,
# MAGIC     ' SET TAGS (',
# MAGIC     '''data_classification'' = ''', classification, ''', ',
# MAGIC     '''data_owner'' = ''', owner, ''', ',
# MAGIC     '''retention_years'' = ''', CAST(retention_years AS STRING), '''',
# MAGIC     ')'
# MAGIC   );
# MAGIC   
# MAGIC   -- Execute the statement
# MAGIC   EXECUTE IMMEDIATE sql_stmt;
# MAGIC   
# MAGIC   -- Return confirmation
# MAGIC   SELECT CONCAT('Successfully tagged table: ', schema_name, '.', table_name) as result;
# MAGIC END;
# MAGIC
# MAGIC -- Call the procedure for each table
# MAGIC CALL apply_standard_governance_tags('tagging_lab', 'appointments', 'internal', 'scheduling_team', 3);
# MAGIC CALL apply_standard_governance_tags('tagging_lab', 'providers', 'internal', 'hr_department', 10);
# MAGIC CALL apply_standard_governance_tags('tagging_lab', 'billing_summary', 'confidential', 'finance_team', 7);
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# Uncomment to create the stored procedure
# %sql
# CREATE OR REPLACE PROCEDURE apply_standard_governance_tags(
#   schema_name STRING,
#   table_name STRING,
#   classification STRING,
#   owner STRING,
#   retention_years INT
# )
# LANGUAGE SQL
# COMMENT 'Apply standard governance tags to a table'
# AS
# BEGIN
#   DECLARE sql_stmt STRING;
#   
#   SET VARIABLE sql_stmt = CONCAT(
#     'ALTER TABLE ', schema_name, '.', table_name,
#     ' SET TAGS (',
#     '''data_classification'' = ''', classification, ''', ',
#     '''data_owner'' = ''', owner, ''', ',
#     '''retention_years'' = ''', CAST(retention_years AS STRING), '''',
#     ')'
#   );
#   
#   EXECUTE IMMEDIATE sql_stmt;
#   
#   SELECT CONCAT('Successfully tagged table: ', schema_name, '.', table_name) as result;
# END;

# COMMAND ----------

# Uncomment to call the procedure
# %sql
# CALL apply_standard_governance_tags('tagging_lab', 'appointments', 'internal', 'scheduling_team', 3);

# COMMAND ----------

# %sql
# CALL apply_standard_governance_tags('tagging_lab', 'providers', 'internal', 'hr_department', 10);

# COMMAND ----------

# %sql
# CALL apply_standard_governance_tags('tagging_lab', 'billing_summary', 'confidential', 'finance_team', 7);

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: Implement a Governance Tagging Strategy
# MAGIC
# MAGIC **Objective**: Design and implement a comprehensive tagging strategy for data governance.
# MAGIC
# MAGIC ### Task
# MAGIC
# MAGIC You need to implement additional governance tags for the healthcare system. Add the following tags to appropriate tables:
# MAGIC
# MAGIC #### New Tag Categories:
# MAGIC
# MAGIC 1. **Data Quality Tags**:
# MAGIC    - `quality_tier`: 'gold', 'silver', or 'bronze'
# MAGIC    - `last_validated`: Date when data quality was last checked
# MAGIC
# MAGIC 2. **Access Control Tags**:
# MAGIC    - `access_level`: 'restricted', 'limited', or 'general'
# MAGIC    - `requires_audit_log`: 'true' or 'false'
# MAGIC
# MAGIC 3. **Operational Tags**:
# MAGIC    - `update_frequency`: 'real_time', 'daily', 'weekly', 'monthly'
# MAGIC    - `source_system`: Name of the source system
# MAGIC
# MAGIC ### Your Task:
# MAGIC
# MAGIC 1. Decide which tables should have which tags based on their characteristics
# MAGIC 2. Apply the tags to all tables in the schema
# MAGIC 3. Create a comprehensive audit query that shows all tags for all tables
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Apply additional tags to the tables
# MAGIC -- Consider the nature of each table when deciding tag values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Create a comprehensive audit query showing all tags

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5: Solution
# MAGIC
# MAGIC <details>
# MAGIC <summary><b>Show Solution</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC -- Apply additional tags to patients table
# MAGIC ALTER TABLE patients
# MAGIC SET TAGS (
# MAGIC   'quality_tier' = 'gold',
# MAGIC   'last_validated' = '2025-01-09',
# MAGIC   'access_level' = 'restricted',
# MAGIC   'requires_audit_log' = 'true',
# MAGIC   'update_frequency' = 'real_time',
# MAGIC   'source_system' = 'PatientManagementSystem'
# MAGIC );
# MAGIC
# MAGIC -- Apply additional tags to medical_records table
# MAGIC ALTER TABLE medical_records
# MAGIC SET TAGS (
# MAGIC   'quality_tier' = 'gold',
# MAGIC   'last_validated' = '2025-01-09',
# MAGIC   'access_level' = 'restricted',
# MAGIC   'requires_audit_log' = 'true',
# MAGIC   'update_frequency' = 'real_time',
# MAGIC   'source_system' = 'ElectronicHealthRecords'
# MAGIC );
# MAGIC
# MAGIC -- Apply additional tags to appointments table
# MAGIC ALTER TABLE appointments
# MAGIC SET TAGS (
# MAGIC   'quality_tier' = 'silver',
# MAGIC   'last_validated' = '2025-01-08',
# MAGIC   'access_level' = 'limited',
# MAGIC   'requires_audit_log' = 'false',
# MAGIC   'update_frequency' = 'real_time',
# MAGIC   'source_system' = 'SchedulingSystem'
# MAGIC );
# MAGIC
# MAGIC -- Apply additional tags to providers table
# MAGIC ALTER TABLE providers
# MAGIC SET TAGS (
# MAGIC   'quality_tier' = 'gold',
# MAGIC   'last_validated' = '2025-01-05',
# MAGIC   'access_level' = 'limited',
# MAGIC   'requires_audit_log' = 'false',
# MAGIC   'update_frequency' = 'weekly',
# MAGIC   'source_system' = 'HRSystem'
# MAGIC );
# MAGIC
# MAGIC -- Apply additional tags to billing_summary table
# MAGIC ALTER TABLE billing_summary
# MAGIC SET TAGS (
# MAGIC   'quality_tier' = 'silver',
# MAGIC   'last_validated' = '2025-01-09',
# MAGIC   'access_level' = 'limited',
# MAGIC   'requires_audit_log' = 'true',
# MAGIC   'update_frequency' = 'daily',
# MAGIC   'source_system' = 'BillingSystem'
# MAGIC );
# MAGIC
# MAGIC -- Apply additional tags to department_metrics table
# MAGIC ALTER TABLE department_metrics
# MAGIC SET TAGS (
# MAGIC   'data_classification' = 'internal',
# MAGIC   'data_owner' = 'analytics_team',
# MAGIC   'retention_years' = '3',
# MAGIC   'quality_tier' = 'bronze',
# MAGIC   'last_validated' = '2025-01-09',
# MAGIC   'access_level' = 'general',
# MAGIC   'requires_audit_log' = 'false',
# MAGIC   'update_frequency' = 'monthly',
# MAGIC   'source_system' = 'AnalyticsPlatform'
# MAGIC );
# MAGIC
# MAGIC -- Comprehensive audit query
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   MAX(CASE WHEN tag_name = 'data_classification' THEN tag_value END) as classification,
# MAGIC   MAX(CASE WHEN tag_name = 'data_owner' THEN tag_value END) as owner,
# MAGIC   MAX(CASE WHEN tag_name = 'retention_years' THEN tag_value END) as retention,
# MAGIC   MAX(CASE WHEN tag_name = 'quality_tier' THEN tag_value END) as quality,
# MAGIC   MAX(CASE WHEN tag_name = 'access_level' THEN tag_value END) as access,
# MAGIC   MAX(CASE WHEN tag_name = 'update_frequency' THEN tag_value END) as frequency,
# MAGIC   MAX(CASE WHEN tag_name = 'source_system' THEN tag_value END) as source,
# MAGIC   MAX(CASE WHEN tag_name = 'requires_audit_log' THEN tag_value END) as audit_log,
# MAGIC   MAX(CASE WHEN tag_name = 'contains_pii' THEN tag_value END) as pii,
# MAGIC   MAX(CASE WHEN tag_name = 'contains_phi' THEN tag_value END) as phi
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_lab'
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name;
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# Uncomment to apply solution tags
# %sql
# ALTER TABLE patients
# SET TAGS (
#   'quality_tier' = 'gold',
#   'last_validated' = '2025-01-09',
#   'access_level' = 'restricted',
#   'requires_audit_log' = 'true',
#   'update_frequency' = 'real_time',
#   'source_system' = 'PatientManagementSystem'
# );

# COMMAND ----------

# %sql
# ALTER TABLE medical_records
# SET TAGS (
#   'quality_tier' = 'gold',
#   'last_validated' = '2025-01-09',
#   'access_level' = 'restricted',
#   'requires_audit_log' = 'true',
#   'update_frequency' = 'real_time',
#   'source_system' = 'ElectronicHealthRecords'
# );

# COMMAND ----------

# %sql
# ALTER TABLE appointments
# SET TAGS (
#   'quality_tier' = 'silver',
#   'last_validated' = '2025-01-08',
#   'access_level' = 'limited',
#   'requires_audit_log' = 'false',
#   'update_frequency' = 'real_time',
#   'source_system' = 'SchedulingSystem'
# );

# COMMAND ----------

# %sql
# ALTER TABLE providers
# SET TAGS (
#   'quality_tier' = 'gold',
#   'last_validated' = '2025-01-05',
#   'access_level' = 'limited',
#   'requires_audit_log' = 'false',
#   'update_frequency' = 'weekly',
#   'source_system' = 'HRSystem'
# );

# COMMAND ----------

# %sql
# ALTER TABLE billing_summary
# SET TAGS (
#   'quality_tier' = 'silver',
#   'last_validated' = '2025-01-09',
#   'access_level' = 'limited',
#   'requires_audit_log' = 'true',
#   'update_frequency' = 'daily',
#   'source_system' = 'BillingSystem'
# );

# COMMAND ----------

# %sql
# ALTER TABLE department_metrics
# SET TAGS (
#   'data_classification' = 'internal',
#   'data_owner' = 'analytics_team',
#   'retention_years' = '3',
#   'quality_tier' = 'bronze',
#   'last_validated' = '2025-01-09',
#   'access_level' = 'general',
#   'requires_audit_log' = 'false',
#   'update_frequency' = 'monthly',
#   'source_system' = 'AnalyticsPlatform'
# );

# COMMAND ----------

# Uncomment to run comprehensive audit query
# %sql
# SELECT 
#   table_name,
#   MAX(CASE WHEN tag_name = 'data_classification' THEN tag_value END) as classification,
#   MAX(CASE WHEN tag_name = 'data_owner' THEN tag_value END) as owner,
#   MAX(CASE WHEN tag_name = 'retention_years' THEN tag_value END) as retention,
#   MAX(CASE WHEN tag_name = 'quality_tier' THEN tag_value END) as quality,
#   MAX(CASE WHEN tag_name = 'access_level' THEN tag_value END) as access,
#   MAX(CASE WHEN tag_name = 'update_frequency' THEN tag_value END) as frequency,
#   MAX(CASE WHEN tag_name = 'source_system' THEN tag_value END) as source,
#   MAX(CASE WHEN tag_name = 'requires_audit_log' THEN tag_value END) as audit_log,
#   MAX(CASE WHEN tag_name = 'contains_pii' THEN tag_value END) as pii,
#   MAX(CASE WHEN tag_name = 'contains_phi' THEN tag_value END) as phi
# FROM system.information_schema.table_tags
# WHERE schema_name = 'tagging_lab'
# GROUP BY table_name
# ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge Exercise: Complete Tag Management Workflow
# MAGIC
# MAGIC **Objective**: Build a comprehensive tag management system combining all the skills you've learned.
# MAGIC
# MAGIC ### Scenario
# MAGIC
# MAGIC Your organization needs a complete tag management workflow that includes:
# MAGIC 1. A stored procedure to validate tag compliance
# MAGIC 2. A view that identifies untagged or incompletely tagged tables
# MAGIC 3. A report showing tag coverage metrics
# MAGIC 4. Automated remediation for tables missing required tags
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC #### Part 1: Tag Validation Procedure
# MAGIC Create a stored procedure `validate_tag_compliance` that:
# MAGIC - Checks if a table has all required tags: `data_classification`, `data_owner`, `retention_years`
# MAGIC - Returns 'COMPLIANT' or 'NON_COMPLIANT' with details of missing tags
# MAGIC
# MAGIC #### Part 2: Compliance View
# MAGIC Create a view `tag_compliance_report` that shows:
# MAGIC - All tables in the schema
# MAGIC - Which required tags they have
# MAGIC - A compliance status
# MAGIC
# MAGIC #### Part 3: Coverage Metrics
# MAGIC Write a query that calculates:
# MAGIC - Total number of tables
# MAGIC - Number of tables with each required tag
# MAGIC - Percentage coverage for each tag
# MAGIC - Overall compliance percentage
# MAGIC
# MAGIC #### Part 4: Bulk Remediation
# MAGIC Create a procedure `apply_default_tags` that:
# MAGIC - Finds tables missing required tags
# MAGIC - Applies default values to missing tags
# MAGIC - Returns a summary of actions taken
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Part 1: Create tag validation procedure

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Part 2: Create compliance view

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Part 3: Write coverage metrics query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR CODE HERE
# MAGIC -- Part 4: Create bulk remediation procedure

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge Exercise: Solution
# MAGIC
# MAGIC <details>
# MAGIC <summary><b>Show Solution</b></summary>
# MAGIC
# MAGIC ```sql
# MAGIC -- Part 1: Tag Validation Procedure
# MAGIC CREATE OR REPLACE PROCEDURE validate_tag_compliance(
# MAGIC   schema_name STRING,
# MAGIC   table_name STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Validate if a table has all required governance tags'
# MAGIC AS
# MAGIC BEGIN
# MAGIC   DECLARE has_classification BOOLEAN DEFAULT FALSE;
# MAGIC   DECLARE has_owner BOOLEAN DEFAULT FALSE;
# MAGIC   DECLARE has_retention BOOLEAN DEFAULT FALSE;
# MAGIC   DECLARE missing_tags STRING DEFAULT '';
# MAGIC   DECLARE compliance_status STRING;
# MAGIC   
# MAGIC   -- Check for data_classification tag
# MAGIC   SET VARIABLE has_classification = (
# MAGIC     SELECT COUNT(*) > 0
# MAGIC     FROM system.information_schema.table_tags
# MAGIC     WHERE table_schema = schema_name
# MAGIC       AND table_name = validate_tag_compliance.table_name
# MAGIC       AND tag_name = 'data_classification'
# MAGIC   );
# MAGIC   
# MAGIC   -- Check for data_owner tag
# MAGIC   SET VARIABLE has_owner = (
# MAGIC     SELECT COUNT(*) > 0
# MAGIC     FROM system.information_schema.table_tags
# MAGIC     WHERE table_schema = schema_name
# MAGIC       AND table_name = validate_tag_compliance.table_name
# MAGIC       AND tag_name = 'data_owner'
# MAGIC   );
# MAGIC   
# MAGIC   -- Check for retention_years tag
# MAGIC   SET VARIABLE has_retention = (
# MAGIC     SELECT COUNT(*) > 0
# MAGIC     FROM system.information_schema.table_tags
# MAGIC     WHERE table_schema = schema_name
# MAGIC       AND table_name = validate_tag_compliance.table_name
# MAGIC       AND tag_name = 'retention_years'
# MAGIC   );
# MAGIC   
# MAGIC   -- Build list of missing tags
# MAGIC   IF NOT has_classification THEN
# MAGIC     SET VARIABLE missing_tags = CONCAT(missing_tags, 'data_classification, ');
# MAGIC   END IF;
# MAGIC   
# MAGIC   IF NOT has_owner THEN
# MAGIC     SET VARIABLE missing_tags = CONCAT(missing_tags, 'data_owner, ');
# MAGIC   END IF;
# MAGIC   
# MAGIC   IF NOT has_retention THEN
# MAGIC     SET VARIABLE missing_tags = CONCAT(missing_tags, 'retention_years, ');
# MAGIC   END IF;
# MAGIC   
# MAGIC   -- Determine compliance status
# MAGIC   IF has_classification AND has_owner AND has_retention THEN
# MAGIC     SET VARIABLE compliance_status = 'COMPLIANT';
# MAGIC     SET VARIABLE missing_tags = 'None';
# MAGIC   ELSE
# MAGIC     SET VARIABLE compliance_status = 'NON_COMPLIANT';
# MAGIC     SET VARIABLE missing_tags = LEFT(missing_tags, LENGTH(missing_tags) - 2);
# MAGIC   END IF;
# MAGIC   
# MAGIC   -- Return results
# MAGIC   SELECT 
# MAGIC     schema_name,
# MAGIC     table_name,
# MAGIC     compliance_status,
# MAGIC     missing_tags;
# MAGIC END;
# MAGIC
# MAGIC -- Part 2: Compliance View
# MAGIC CREATE OR REPLACE VIEW tag_compliance_report AS
# MAGIC SELECT 
# MAGIC   t.table_name,
# MAGIC   MAX(CASE WHEN tags.tag_name = 'data_classification' THEN 'YES' ELSE NULL END) as has_classification,
# MAGIC   MAX(CASE WHEN tags.tag_name = 'data_owner' THEN 'YES' ELSE NULL END) as has_owner,
# MAGIC   MAX(CASE WHEN tags.tag_name = 'retention_years' THEN 'YES' ELSE NULL END) as has_retention,
# MAGIC   CASE 
# MAGIC     WHEN 
# MAGIC       MAX(CASE WHEN tags.tag_name = 'data_classification' THEN 1 ELSE 0 END) = 1 AND
# MAGIC       MAX(CASE WHEN tags.tag_name = 'data_owner' THEN 1 ELSE 0 END) = 1 AND
# MAGIC       MAX(CASE WHEN tags.tag_name = 'retention_years' THEN 1 ELSE 0 END) = 1
# MAGIC     THEN 'COMPLIANT'
# MAGIC     ELSE 'NON_COMPLIANT'
# MAGIC   END as compliance_status
# MAGIC FROM information_schema.tables t
# MAGIC LEFT JOIN system.information_schema.table_tags tags
# MAGIC   ON t.table_schema = tags.schema_name
# MAGIC   AND t.table_name = tags.table_name
# MAGIC   AND tags.tag_name IN ('data_classification', 'data_owner', 'retention_years')
# MAGIC WHERE t.table_schema = 'tagging_lab'
# MAGIC   AND t.table_type = 'MANAGED'
# MAGIC GROUP BY t.table_name
# MAGIC ORDER BY compliance_status, t.table_name;
# MAGIC
# MAGIC -- Test the view
# MAGIC SELECT * FROM tag_compliance_report;
# MAGIC
# MAGIC -- Part 3: Coverage Metrics Query
# MAGIC WITH table_count AS (
# MAGIC   SELECT COUNT(DISTINCT table_name) as total_tables
# MAGIC   FROM information_schema.tables
# MAGIC   WHERE table_schema = 'tagging_lab'
# MAGIC     AND table_type = 'MANAGED'
# MAGIC ),
# MAGIC tag_counts AS (
# MAGIC   SELECT 
# MAGIC     tag_name,
# MAGIC     COUNT(DISTINCT table_name) as tagged_tables
# MAGIC   FROM system.information_schema.table_tags
# MAGIC   WHERE schema_name = 'tagging_lab'
# MAGIC     AND tag_name IN ('data_classification', 'data_owner', 'retention_years')
# MAGIC   GROUP BY tag_name
# MAGIC ),
# MAGIC compliance_count AS (
# MAGIC   SELECT COUNT(*) as compliant_tables
# MAGIC   FROM tag_compliance_report
# MAGIC   WHERE compliance_status = 'COMPLIANT'
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Overall Metrics' as metric_type,
# MAGIC   tc.total_tables,
# MAGIC   cc.compliant_tables,
# MAGIC   ROUND(cc.compliant_tables * 100.0 / tc.total_tables, 2) as compliance_percentage
# MAGIC FROM table_count tc, compliance_count cc
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   CONCAT('Tag: ', tag_name) as metric_type,
# MAGIC   tc.total_tables,
# MAGIC   COALESCE(tg.tagged_tables, 0) as tagged_tables,
# MAGIC   ROUND(COALESCE(tg.tagged_tables, 0) * 100.0 / tc.total_tables, 2) as coverage_percentage
# MAGIC FROM table_count tc
# MAGIC LEFT JOIN tag_counts tg ON 1=1
# MAGIC ORDER BY metric_type;
# MAGIC
# MAGIC -- Part 4: Bulk Remediation Procedure
# MAGIC CREATE OR REPLACE PROCEDURE apply_default_tags(
# MAGIC   schema_name STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Apply default tags to tables missing required tags'
# MAGIC AS
# MAGIC BEGIN
# MAGIC   DECLARE table_list ARRAY<STRING>;
# MAGIC   DECLARE table_name STRING;
# MAGIC   DECLARE i INT DEFAULT 0;
# MAGIC   DECLARE tables_updated INT DEFAULT 0;
# MAGIC   DECLARE sql_stmt STRING;
# MAGIC   DECLARE needs_classification BOOLEAN;
# MAGIC   DECLARE needs_owner BOOLEAN;
# MAGIC   DECLARE needs_retention BOOLEAN;
# MAGIC   DECLARE tag_clause STRING;
# MAGIC   
# MAGIC   -- Get list of non-compliant tables
# MAGIC   SET VARIABLE table_list = (
# MAGIC     SELECT COLLECT_LIST(table_name)
# MAGIC     FROM tag_compliance_report
# MAGIC     WHERE compliance_status = 'NON_COMPLIANT'
# MAGIC   );
# MAGIC   
# MAGIC   -- Loop through tables
# MAGIC   WHILE i < CARDINALITY(table_list) DO
# MAGIC     SET VARIABLE table_name = table_list[i];
# MAGIC     SET VARIABLE tag_clause = '';
# MAGIC     
# MAGIC     -- Check which tags are missing
# MAGIC     SET VARIABLE needs_classification = (
# MAGIC       SELECT COUNT(*) = 0
# MAGIC       FROM system.information_schema.table_tags
# MAGIC       WHERE table_schema = schema_name
# MAGIC         AND table_tags.table_name = apply_default_tags.table_name
# MAGIC         AND tag_name = 'data_classification'
# MAGIC     );
# MAGIC     
# MAGIC     SET VARIABLE needs_owner = (
# MAGIC       SELECT COUNT(*) = 0
# MAGIC       FROM system.information_schema.table_tags
# MAGIC       WHERE table_schema = schema_name
# MAGIC         AND table_tags.table_name = apply_default_tags.table_name
# MAGIC         AND tag_name = 'data_owner'
# MAGIC     );
# MAGIC     
# MAGIC     SET VARIABLE needs_retention = (
# MAGIC       SELECT COUNT(*) = 0
# MAGIC       FROM system.information_schema.table_tags
# MAGIC       WHERE table_schema = schema_name
# MAGIC         AND table_tags.table_name = apply_default_tags.table_name
# MAGIC         AND tag_name = 'retention_years'
# MAGIC     );
# MAGIC     
# MAGIC     -- Build tag clause for missing tags only
# MAGIC     IF needs_classification THEN
# MAGIC       SET VARIABLE tag_clause = CONCAT(tag_clause, '''data_classification'' = ''internal'', ');
# MAGIC     END IF;
# MAGIC     
# MAGIC     IF needs_owner THEN
# MAGIC       SET VARIABLE tag_clause = CONCAT(tag_clause, '''data_owner'' = ''unassigned'', ');
# MAGIC     END IF;
# MAGIC     
# MAGIC     IF needs_retention THEN
# MAGIC       SET VARIABLE tag_clause = CONCAT(tag_clause, '''retention_years'' = ''5'', ');
# MAGIC     END IF;
# MAGIC     
# MAGIC     -- Remove trailing comma and space
# MAGIC     IF LENGTH(tag_clause) > 0 THEN
# MAGIC       SET VARIABLE tag_clause = LEFT(tag_clause, LENGTH(tag_clause) - 2);
# MAGIC       
# MAGIC       -- Build and execute ALTER TABLE statement
# MAGIC       SET VARIABLE sql_stmt = CONCAT(
# MAGIC         'ALTER TABLE ', schema_name, '.', table_name,
# MAGIC         ' SET TAGS (', tag_clause, ')'
# MAGIC       );
# MAGIC       
# MAGIC       EXECUTE IMMEDIATE sql_stmt;
# MAGIC       SET VARIABLE tables_updated = tables_updated + 1;
# MAGIC     END IF;
# MAGIC     
# MAGIC     SET VARIABLE i = i + 1;
# MAGIC   END WHILE;
# MAGIC   
# MAGIC   -- Return summary
# MAGIC   SELECT 
# MAGIC     CONCAT('Applied default tags to ', CAST(tables_updated AS STRING), ' tables') as summary,
# MAGIC     tables_updated,
# MAGIC     CARDINALITY(table_list) as total_non_compliant_tables;
# MAGIC END;
# MAGIC
# MAGIC -- Test the remediation procedure
# MAGIC CALL apply_default_tags('tagging_lab');
# MAGIC
# MAGIC -- Verify the results
# MAGIC SELECT * FROM tag_compliance_report;
# MAGIC ```
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC Congratulations! You've completed the Unity Catalog tagging lab. Here's what you've accomplished:
# MAGIC
# MAGIC ### Skills Mastered
# MAGIC
# MAGIC ✅ **Exercise 1**: Applied tags to schemas for organizational metadata  
# MAGIC ✅ **Exercise 2**: Tagged tables with multiple governance attributes  
# MAGIC ✅ **Exercise 3**: Queried and audited tags using information schema  
# MAGIC ✅ **Exercise 4**: Created stored procedures to automate tagging  
# MAGIC ✅ **Exercise 5**: Implemented a comprehensive tagging strategy  
# MAGIC ✅ **Challenge**: Built a complete tag management workflow  
# MAGIC
# MAGIC ### Key Concepts Reinforced
# MAGIC
# MAGIC - **Tag Application**: Using `ALTER` statements with `SET TAGS` clause
# MAGIC - **Tag Querying**: Leveraging information schema views for auditing
# MAGIC - **Automation**: Creating reusable stored procedures
# MAGIC - **Governance**: Implementing compliance and quality controls
# MAGIC - **Validation**: Building workflows to ensure tag completeness
# MAGIC
# MAGIC ### Real-World Applications
# MAGIC
# MAGIC The skills you've learned apply directly to:
# MAGIC - **Regulatory Compliance**: GDPR, HIPAA, CCPA data identification
# MAGIC - **Data Discovery**: Making data assets searchable and discoverable
# MAGIC - **Cost Management**: Tracking data ownership and usage
# MAGIC - **Quality Assurance**: Monitoring data quality and freshness
# MAGIC - **Access Control**: Supporting fine-grained permission strategies
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Apply to your data**: Implement tagging in your organization's Unity Catalog
# MAGIC 2. **Design taxonomy**: Create a standardized tag schema for your use cases
# MAGIC 3. **Build automation**: Develop workflows to maintain tag consistency
# MAGIC 4. **Monitor compliance**: Create dashboards to track tag coverage
# MAGIC 5. **Consider governed tags**: Explore metastore-level governed tags for critical classifications
# MAGIC
# MAGIC ### Additional Resources
# MAGIC
# MAGIC - [Unity Catalog Tags Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html)
# MAGIC - [Information Schema Reference](https://docs.databricks.com/en/sql/language-manual/information-schema.html)
# MAGIC - [Data Governance Best Practices](https://docs.databricks.com/en/data-governance/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Cleanup (Optional)
# MAGIC
# MAGIC When you're ready to clean up the lab environment, uncomment and run the cell below.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to clean up lab objects
# MAGIC -- DROP SCHEMA IF EXISTS tagging_lab CASCADE;
# MAGIC -- DROP PROCEDURE IF EXISTS apply_standard_governance_tags;
# MAGIC -- DROP PROCEDURE IF EXISTS validate_tag_compliance;
# MAGIC -- DROP PROCEDURE IF EXISTS apply_default_tags;
# MAGIC -- DROP VIEW IF EXISTS tag_compliance_report;
