# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Unity Catalog Tagging Overview
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on demo introduces the fundamentals of tagging in Databricks Unity Catalog. You'll learn how to apply user-defined tags to schemas and tables, verify and audit them through information schema queries, and use stored procedures to automate tagging across multiple objects. The demo also covers governed and system tags, highlighting their role in consistency, compliance, and enterprise-wide governance.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Understand what Unity Catalog tags are and their governance benefits
# MAGIC - Apply user-defined tags to schemas and tables using SQL commands
# MAGIC - Query information schema views to verify and audit applied tags
# MAGIC - Automate the application of standard tags across multiple tables using stored procedures
# MAGIC - Differentiate between user-defined, governed, and system tags and explain when to use each
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - A Databricks workspace with Unity Catalog enabled
# MAGIC - A SQL Warehouse or compute cluster to execute SQL statements
# MAGIC - Permissions to create a schema and tables in a catalog you own
# MAGIC - Basic familiarity with SQL commands
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Fundamentals of Tagging in Unity Catalog
# MAGIC
# MAGIC ### What Are Tags?
# MAGIC
# MAGIC **Tags** are key-value pairs that you can attach to Unity Catalog objects (catalogs, schemas, tables, volumes, etc.) to:
# MAGIC - **Organize and categorize** data assets
# MAGIC - **Enable discovery** through search and filtering
# MAGIC - **Support governance** and compliance requirements
# MAGIC - **Document metadata** about data ownership, sensitivity, and purpose
# MAGIC
# MAGIC ### Why Use Tags?
# MAGIC
# MAGIC Tags provide several critical benefits:
# MAGIC
# MAGIC 1. **Data Discovery**: Users can find relevant datasets by searching for specific tags
# MAGIC 2. **Compliance**: Tag data by sensitivity level (PII, confidential, public) for regulatory compliance
# MAGIC 3. **Data Quality**: Track data quality metrics, freshness, and validation status
# MAGIC 4. **Cost Management**: Track which team or project owns datasets for chargeback
# MAGIC 5. **Lifecycle Management**: Mark datasets for retention, archival, or deletion policies
# MAGIC 6. **Documentation**: Provide context about data sources, update frequency, and business purpose
# MAGIC
# MAGIC ### Tag Types in Unity Catalog
# MAGIC
# MAGIC Unity Catalog supports three types of tags:
# MAGIC
# MAGIC | Tag Type | Description | Who Can Apply | Use Cases |
# MAGIC |----------|-------------|---------------|-----------|
# MAGIC | **User-Defined** | Arbitrary key-value pairs created by users | Object owners and users with APPLY TAG privilege | Flexible categorization, team-specific metadata |
# MAGIC | **Governed** | Centrally managed tags with controlled vocabularies | Only users with specific governance privileges | Standardized compliance tags, official classifications |
# MAGIC | **System** | Automatically applied by Databricks | System (read-only) | Lineage metadata, system-generated properties |
# MAGIC
# MAGIC In this demo, we'll focus primarily on **user-defined tags**, which are the most commonly used.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Setup - Create Demo Environment
# MAGIC
# MAGIC Let's create a sample catalog structure with schemas and tables that we can tag throughout this demo.
# MAGIC
# MAGIC **Note**: Replace `main` with your catalog name if you prefer to use a different catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use an existing catalog (replace 'main' with your catalog name)
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC -- Create a schema for our tagging demo
# MAGIC CREATE SCHEMA IF NOT EXISTS tagging_demo
# MAGIC COMMENT 'Schema for demonstrating Unity Catalog tagging features';
# MAGIC
# MAGIC -- Use the schema
# MAGIC USE SCHEMA tagging_demo;
# MAGIC
# MAGIC -- Verify current location
# MAGIC SELECT current_catalog() as catalog, current_schema() as schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Sample Tables
# MAGIC
# MAGIC We'll create several tables representing different types of data in a fictional e-commerce system. These tables will have different sensitivity levels and business purposes, making them good candidates for tagging.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a customers table (contains PII)
# MAGIC CREATE OR REPLACE TABLE customers (
# MAGIC   customer_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   zip_code STRING,
# MAGIC   created_date TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Customer master data - contains personally identifiable information';
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO customers VALUES
# MAGIC   ('C001', 'Alice', 'Anderson', 'alice.anderson@email.com', '555-0101', '123 Oak St', 'Seattle', 'WA', '98101', '2024-01-15T10:30:00'),
# MAGIC   ('C002', 'Bob', 'Brown', 'bob.brown@email.com', '555-0102', '456 Pine Ave', 'Portland', 'OR', '97201', '2024-02-20T14:15:00'),
# MAGIC   ('C003', 'Carol', 'Chen', 'carol.chen@email.com', '555-0103', '789 Elm Blvd', 'San Francisco', 'CA', '94102', '2024-03-10T09:45:00'),
# MAGIC   ('C004', 'David', 'Davis', 'david.davis@email.com', '555-0104', '321 Maple Dr', 'Los Angeles', 'CA', '90001', '2024-04-05T16:20:00'),
# MAGIC   ('C005', 'Emma', 'Evans', 'emma.evans@email.com', '555-0105', '654 Birch Ln', 'San Diego', 'CA', '92101', '2024-05-12T11:00:00');
# MAGIC
# MAGIC SELECT * FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an orders table (transactional data)
# MAGIC CREATE OR REPLACE TABLE orders (
# MAGIC   order_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   order_date DATE,
# MAGIC   total_amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   payment_method STRING
# MAGIC )
# MAGIC COMMENT 'Order transaction records';
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO orders VALUES
# MAGIC   ('O001', 'C001', '2024-12-01', 150.00, 'completed', 'credit_card'),
# MAGIC   ('O002', 'C001', '2024-12-15', 200.00, 'completed', 'credit_card'),
# MAGIC   ('O003', 'C001', '2025-01-05', 175.00, 'shipped', 'paypal'),
# MAGIC   ('O004', 'C002', '2024-12-10', 89.99, 'completed', 'debit_card'),
# MAGIC   ('O005', 'C002', '2025-01-03', 125.50, 'processing', 'credit_card'),
# MAGIC   ('O006', 'C003', '2024-11-20', 300.00, 'completed', 'credit_card'),
# MAGIC   ('O007', 'C003', '2024-12-28', 450.00, 'completed', 'paypal'),
# MAGIC   ('O008', 'C004', '2024-12-05', 75.25, 'cancelled', 'credit_card'),
# MAGIC   ('O009', 'C004', '2025-01-08', 220.00, 'processing', 'debit_card'),
# MAGIC   ('O010', 'C005', '2024-12-22', 99.99, 'completed', 'paypal');
# MAGIC
# MAGIC SELECT * FROM orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a products table (reference data)
# MAGIC CREATE OR REPLACE TABLE products (
# MAGIC   product_id STRING,
# MAGIC   product_name STRING,
# MAGIC   category STRING,
# MAGIC   price DECIMAL(10,2),
# MAGIC   inventory_count INT
# MAGIC )
# MAGIC COMMENT 'Product catalog and inventory data';
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO products VALUES
# MAGIC   ('P001', 'Laptop', 'Electronics', 999.99, 50),
# MAGIC   ('P002', 'Mouse', 'Electronics', 29.99, 200),
# MAGIC   ('P003', 'Keyboard', 'Electronics', 79.99, 150),
# MAGIC   ('P004', 'Monitor', 'Electronics', 299.99, 75),
# MAGIC   ('P005', 'Desk Chair', 'Furniture', 199.99, 40),
# MAGIC   ('P006', 'Desk', 'Furniture', 349.99, 30),
# MAGIC   ('P007', 'Notebook', 'Office Supplies', 4.99, 500),
# MAGIC   ('P008', 'Pen Set', 'Office Supplies', 12.99, 300);
# MAGIC
# MAGIC SELECT * FROM products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an analytics aggregated table (derived data)
# MAGIC CREATE OR REPLACE TABLE customer_order_summary (
# MAGIC   customer_id STRING,
# MAGIC   total_orders INT,
# MAGIC   total_spent DECIMAL(10,2),
# MAGIC   avg_order_value DECIMAL(10,2),
# MAGIC   last_order_date DATE
# MAGIC )
# MAGIC COMMENT 'Aggregated customer order metrics for analytics';
# MAGIC
# MAGIC -- Insert sample aggregated data
# MAGIC INSERT INTO customer_order_summary
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   COUNT(*) as total_orders,
# MAGIC   SUM(total_amount) as total_spent,
# MAGIC   AVG(total_amount) as avg_order_value,
# MAGIC   MAX(order_date) as last_order_date
# MAGIC FROM orders
# MAGIC GROUP BY customer_id;
# MAGIC
# MAGIC SELECT * FROM customer_order_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Applying User-Defined Tags
# MAGIC
# MAGIC Now that we have our demo environment set up, let's learn how to apply tags to various Unity Catalog objects.
# MAGIC
# MAGIC ### Tag Syntax
# MAGIC
# MAGIC Tags are applied using the `ALTER` statement with the `SET TAGS` clause:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER {SCHEMA | TABLE | VOLUME | etc.} object_name
# MAGIC SET TAGS ('key1' = 'value1', 'key2' = 'value2', ...);
# MAGIC ```
# MAGIC
# MAGIC ### Tagging Best Practices
# MAGIC
# MAGIC 1. **Use consistent naming**: Adopt a standard naming convention (e.g., `snake_case` or `camelCase`)
# MAGIC 2. **Be descriptive**: Use clear, meaningful keys and values
# MAGIC 3. **Plan your taxonomy**: Define your tag schema before widespread adoption
# MAGIC 4. **Document tag meanings**: Maintain documentation of what each tag represents
# MAGIC 5. **Avoid sensitive data in tags**: Don't put actual PII or confidential data in tag values

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tagging a Schema
# MAGIC
# MAGIC Let's start by tagging our schema to indicate its purpose and ownership.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply tags to the schema
# MAGIC ALTER SCHEMA tagging_demo
# MAGIC SET TAGS (
# MAGIC   'owner' = 'data_engineering_team',
# MAGIC   'purpose' = 'demonstration',
# MAGIC   'environment' = 'development',
# MAGIC   'cost_center' = 'engineering'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tagging Tables
# MAGIC
# MAGIC Now let's apply tags to our tables based on their characteristics.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag the customers table (contains PII)
# MAGIC ALTER TABLE customers
# MAGIC SET TAGS (
# MAGIC   'data_classification' = 'confidential',
# MAGIC   'contains_pii' = 'true',
# MAGIC   'pii_types' = 'name,email,phone,address',
# MAGIC   'retention_years' = '7',
# MAGIC   'compliance_requirement' = 'GDPR,CCPA',
# MAGIC   'data_owner' = 'customer_success_team',
# MAGIC   'update_frequency' = 'real_time'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag the orders table (transactional data)
# MAGIC ALTER TABLE orders
# MAGIC SET TAGS (
# MAGIC   'data_classification' = 'internal',
# MAGIC   'contains_pii' = 'false',
# MAGIC   'data_type' = 'transactional',
# MAGIC   'retention_years' = '10',
# MAGIC   'data_owner' = 'sales_operations_team',
# MAGIC   'update_frequency' = 'real_time',
# MAGIC   'quality_score' = '95'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag the products table (reference data)
# MAGIC ALTER TABLE products
# MAGIC SET TAGS (
# MAGIC   'data_classification' = 'public',
# MAGIC   'contains_pii' = 'false',
# MAGIC   'data_type' = 'reference',
# MAGIC   'retention_years' = '5',
# MAGIC   'data_owner' = 'product_team',
# MAGIC   'update_frequency' = 'daily',
# MAGIC   'quality_score' = '99'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag the analytics table (derived data)
# MAGIC ALTER TABLE customer_order_summary
# MAGIC SET TAGS (
# MAGIC   'data_classification' = 'internal',
# MAGIC   'contains_pii' = 'false',
# MAGIC   'data_type' = 'analytical',
# MAGIC   'source_table' = 'orders',
# MAGIC   'retention_years' = '3',
# MAGIC   'data_owner' = 'analytics_team',
# MAGIC   'update_frequency' = 'daily',
# MAGIC   'refresh_time' = 'midnight'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Querying Tags via Information Schema
# MAGIC
# MAGIC Unity Catalog provides system views in the `information_schema` that allow you to query and audit tags across your data assets.
# MAGIC
# MAGIC ### Available Tag Views
# MAGIC
# MAGIC - `system.information_schema.catalog_tags` - Tags on catalogs
# MAGIC - `system.information_schema.schema_tags` - Tags on schemas
# MAGIC - `system.information_schema.table_tags` - Tags on tables
# MAGIC - `system.information_schema.volume_tags` - Tags on volumes
# MAGIC - `system.information_schema.column_tags` - Tags on columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Schema Tags

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all tags on our schema
# MAGIC SELECT 
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.schema_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC ORDER BY tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Table Tags

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all tags on all tables in our schema
# MAGIC SELECT 
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC ORDER BY table_name, tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering and Auditing Tags
# MAGIC
# MAGIC Let's explore some common audit queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all tables that contain PII
# MAGIC SELECT DISTINCT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   tag_value as contains_pii
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name = 'contains_pii'
# MAGIC   AND tag_value = 'true'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all tables classified as 'confidential'
# MAGIC SELECT DISTINCT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   tag_value as classification
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name = 'data_classification'
# MAGIC   AND tag_value = 'confidential'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find tables owned by a specific team
# MAGIC SELECT DISTINCT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   tag_value as owner
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name = 'data_owner'
# MAGIC   AND tag_value LIKE '%analytics%'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audit: Find tables with retention policies
# MAGIC SELECT 
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   tag_value as retention_years
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name = 'retention_years'
# MAGIC ORDER BY CAST(tag_value AS INT) DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot Tags for Better Readability
# MAGIC
# MAGIC Often it's useful to see all tags for each table in a single row.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a comprehensive view of all table tags
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   MAX(CASE WHEN tag_name = 'data_classification' THEN tag_value END) as classification,
# MAGIC   MAX(CASE WHEN tag_name = 'contains_pii' THEN tag_value END) as contains_pii,
# MAGIC   MAX(CASE WHEN tag_name = 'data_owner' THEN tag_value END) as owner,
# MAGIC   MAX(CASE WHEN tag_name = 'data_type' THEN tag_value END) as data_type,
# MAGIC   MAX(CASE WHEN tag_name = 'update_frequency' THEN tag_value END) as update_frequency,
# MAGIC   MAX(CASE WHEN tag_name = 'retention_years' THEN tag_value END) as retention_years
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC GROUP BY table_name
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Automating Tags with Stored Procedures
# MAGIC
# MAGIC Manually tagging tables one by one can be tedious, especially when you have many tables. Let's create SQL stored procedures to automate the tagging process.
# MAGIC
# MAGIC ### Why Automate Tagging?
# MAGIC
# MAGIC - **Consistency**: Ensure standard tags are applied uniformly
# MAGIC - **Efficiency**: Tag multiple tables with a single command
# MAGIC - **Maintenance**: Easily update tags across many tables
# MAGIC - **Governance**: Enforce tagging policies programmatically

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Stored Procedure to Tag Multiple Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a stored procedure to apply standard governance tags
# MAGIC CREATE OR REPLACE PROCEDURE apply_governance_tags(
# MAGIC   schema_name STRING,
# MAGIC   table_pattern STRING,
# MAGIC   classification STRING,
# MAGIC   owner STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Apply standard governance tags to tables matching a pattern'
# MAGIC AS
# MAGIC BEGIN
# MAGIC   DECLARE table_list ARRAY<STRING>;
# MAGIC   DECLARE table_name STRING;
# MAGIC   DECLARE i INT DEFAULT 0;
# MAGIC   DECLARE sql_stmt STRING;
# MAGIC   
# MAGIC   -- Get list of tables matching the pattern
# MAGIC   SET VARIABLE table_list = (
# MAGIC     SELECT COLLECT_LIST(table_name)
# MAGIC     FROM information_schema.tables
# MAGIC     WHERE table_schema = schema_name
# MAGIC       AND table_name LIKE table_pattern
# MAGIC   );
# MAGIC   
# MAGIC   -- Loop through tables and apply tags
# MAGIC   WHILE i < CARDINALITY(table_list) DO
# MAGIC     SET VARIABLE table_name = table_list[i];
# MAGIC     
# MAGIC     -- Build and execute ALTER TABLE statement
# MAGIC     SET VARIABLE sql_stmt = CONCAT(
# MAGIC       'ALTER TABLE ', schema_name, '.', table_name,
# MAGIC       ' SET TAGS (''data_classification'' = ''', classification, ''',',
# MAGIC       ' ''data_owner'' = ''', owner, ''')'
# MAGIC     );
# MAGIC     
# MAGIC     EXECUTE IMMEDIATE sql_stmt;
# MAGIC     
# MAGIC     SET VARIABLE i = i + 1;
# MAGIC   END WHILE;
# MAGIC   
# MAGIC   -- Return summary
# MAGIC   SELECT CONCAT('Tagged ', CAST(CARDINALITY(table_list) AS STRING), ' tables') as result;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Stored Procedure
# MAGIC
# MAGIC Let's create some additional test tables and use our procedure to tag them.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a few more tables for testing automation
# MAGIC CREATE OR REPLACE TABLE test_table_1 (id INT, name STRING);
# MAGIC CREATE OR REPLACE TABLE test_table_2 (id INT, value DOUBLE);
# MAGIC CREATE OR REPLACE TABLE test_table_3 (id INT, status STRING);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the stored procedure to tag all test tables at once
# MAGIC CALL apply_governance_tags(
# MAGIC   'tagging_demo',
# MAGIC   'test_table_%',
# MAGIC   'internal',
# MAGIC   'test_team'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the tags were applied
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND table_name LIKE 'test_table_%'
# MAGIC ORDER BY table_name, tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a More Advanced Tagging Procedure
# MAGIC
# MAGIC Let's create a procedure that applies multiple standard tags at once.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a comprehensive tagging procedure
# MAGIC CREATE OR REPLACE PROCEDURE apply_standard_tags(
# MAGIC   schema_name STRING,
# MAGIC   table_name STRING,
# MAGIC   classification STRING,
# MAGIC   owner STRING,
# MAGIC   contains_pii BOOLEAN,
# MAGIC   retention_years INT
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Apply a standard set of governance tags to a table'
# MAGIC AS
# MAGIC BEGIN
# MAGIC   DECLARE sql_stmt STRING;
# MAGIC   DECLARE pii_value STRING;
# MAGIC   
# MAGIC   -- Convert boolean to string
# MAGIC   IF contains_pii THEN
# MAGIC     SET VARIABLE pii_value = 'true';
# MAGIC   ELSE
# MAGIC     SET VARIABLE pii_value = 'false';
# MAGIC   END IF;
# MAGIC   
# MAGIC   -- Build ALTER TABLE statement with multiple tags
# MAGIC   SET VARIABLE sql_stmt = CONCAT(
# MAGIC     'ALTER TABLE ', schema_name, '.', table_name,
# MAGIC     ' SET TAGS (',
# MAGIC     '''data_classification'' = ''', classification, ''', ',
# MAGIC     '''data_owner'' = ''', owner, ''', ',
# MAGIC     '''contains_pii'' = ''', pii_value, ''', ',
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

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the comprehensive tagging procedure
# MAGIC CALL apply_standard_tags(
# MAGIC   'tagging_demo',
# MAGIC   'test_table_1',
# MAGIC   'confidential',
# MAGIC   'data_governance_team',
# MAGIC   true,
# MAGIC   5
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the comprehensive tags
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND table_name = 'test_table_1'
# MAGIC ORDER BY tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Understanding Governed and System Tags
# MAGIC
# MAGIC ### Governed Tags
# MAGIC
# MAGIC **Governed tags** are centrally managed tags that provide stricter control over tag values and who can apply them.
# MAGIC
# MAGIC #### Key Characteristics:
# MAGIC - **Controlled vocabulary**: Only predefined values are allowed
# MAGIC - **Privileged application**: Requires specific governance privileges to apply
# MAGIC - **Consistent enforcement**: Ensures organization-wide consistency
# MAGIC - **Audit trail**: Enhanced tracking of who applied which tags
# MAGIC
# MAGIC #### Use Cases:
# MAGIC - Official data classifications (Top Secret, Secret, Public)
# MAGIC - Regulatory compliance tags (HIPAA Protected, PCI Data, etc.)
# MAGIC - Data quality certifications
# MAGIC - Official data ownership designations
# MAGIC
# MAGIC #### Creating Governed Tags:
# MAGIC
# MAGIC Governed tags are created at the metastore level by users with `CREATE GOVERNED TAG` privilege:
# MAGIC
# MAGIC ```sql
# MAGIC -- Example (requires metastore admin privileges)
# MAGIC CREATE GOVERNED TAG IF NOT EXISTS official_classification
# MAGIC   ALLOWED_VALUES ('public', 'internal', 'confidential', 'restricted');
# MAGIC
# MAGIC -- Grant privilege to apply the governed tag
# MAGIC GRANT APPLY TAG ON GOVERNED TAG official_classification TO `governance_team`;
# MAGIC ```
# MAGIC
# MAGIC **Note**: This demo uses user-defined tags since governed tags require metastore admin privileges.

# COMMAND ----------

# MAGIC %md
# MAGIC ### System Tags
# MAGIC
# MAGIC **System tags** are automatically applied by Databricks and provide system-generated metadata.
# MAGIC
# MAGIC #### Key Characteristics:
# MAGIC - **Read-only**: Users cannot modify or delete system tags
# MAGIC - **Automatically applied**: Set by Databricks based on system events
# MAGIC - **System metadata**: Contains information about lineage, creation, modifications
# MAGIC
# MAGIC #### Examples:
# MAGIC - `__databricks_lineage` - Data lineage information
# MAGIC - `__databricks_creator` - User who created the object
# MAGIC - `__databricks_created_at` - Creation timestamp
# MAGIC
# MAGIC #### Querying System Tags:
# MAGIC
# MAGIC System tags appear in the information schema views with a `__databricks_` prefix:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name LIKE '__databricks_%'
# MAGIC ORDER BY table_name, tag_name;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparison of Tag Types
# MAGIC
# MAGIC | Feature | User-Defined Tags | Governed Tags | System Tags |
# MAGIC |---------|------------------|---------------|-------------|
# MAGIC | **Who creates** | Any user with privileges | Metastore admins | Databricks system |
# MAGIC | **Who applies** | Object owners | Users with APPLY TAG privilege | System automatically |
# MAGIC | **Value constraints** | None | Controlled vocabulary | System-determined |
# MAGIC | **Use case** | Flexible categorization | Official governance | System metadata |
# MAGIC | **Can be modified** | Yes | Yes (by authorized users) | No |
# MAGIC | **Prefix** | None | None | `__databricks_` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Tag Management and Best Practices
# MAGIC
# MAGIC ### Updating Tags
# MAGIC
# MAGIC You can update existing tags by using `SET TAGS` again. New tags are added, and existing tags are updated.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update a tag value
# MAGIC ALTER TABLE products
# MAGIC SET TAGS ('quality_score' = '100');
# MAGIC
# MAGIC -- Verify the update
# MAGIC SELECT tag_name, tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND table_name = 'products'
# MAGIC   AND tag_name = 'quality_score';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Tags
# MAGIC
# MAGIC Use `UNSET TAGS` to remove specific tags from an object.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove a specific tag
# MAGIC ALTER TABLE test_table_3
# MAGIC UNSET TAGS ('data_owner');
# MAGIC
# MAGIC -- Verify the tag was removed
# MAGIC SELECT tag_name, tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND table_name = 'test_table_3'
# MAGIC ORDER BY tag_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tag Naming Conventions
# MAGIC
# MAGIC Establish and follow consistent naming conventions:
# MAGIC
# MAGIC #### Recommended Conventions:
# MAGIC
# MAGIC 1. **Use lowercase with underscores**: `data_classification`, `contains_pii`
# MAGIC 2. **Be descriptive**: `retention_years` instead of just `retention`
# MAGIC 3. **Namespace when needed**: `compliance_gdpr`, `compliance_hipaa`
# MAGIC 4. **Avoid special characters**: Stick to alphanumeric and underscores
# MAGIC 5. **Use standard values**: Define and document acceptable tag values
# MAGIC
# MAGIC #### Example Tag Taxonomy:
# MAGIC
# MAGIC ```
# MAGIC Governance Tags:
# MAGIC   - data_classification: [public, internal, confidential, restricted]
# MAGIC   - contains_pii: [true, false]
# MAGIC   - data_owner: [team_name]
# MAGIC   - retention_years: [number]
# MAGIC
# MAGIC Compliance Tags:
# MAGIC   - compliance_gdpr: [yes, no, partial]
# MAGIC   - compliance_hipaa: [yes, no]
# MAGIC   - compliance_ccpa: [yes, no]
# MAGIC
# MAGIC Operational Tags:
# MAGIC   - update_frequency: [real_time, hourly, daily, weekly, monthly]
# MAGIC   - data_type: [transactional, reference, analytical, derived]
# MAGIC   - quality_score: [0-100]
# MAGIC   - cost_center: [department_code]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Best Practices Summary
# MAGIC
# MAGIC #### 1. Plan Your Tag Strategy
# MAGIC - Define a clear tag taxonomy before wide adoption
# MAGIC - Document what each tag means and acceptable values
# MAGIC - Get buy-in from stakeholders and teams
# MAGIC
# MAGIC #### 2. Implement Governance
# MAGIC - Use governed tags for official classifications
# MAGIC - Restrict who can apply sensitive tags
# MAGIC - Regularly audit tag compliance
# MAGIC
# MAGIC #### 3. Automate When Possible
# MAGIC - Create stored procedures for common tagging patterns
# MAGIC - Integrate tagging into data pipeline workflows
# MAGIC - Use notebooks or scripts to bulk-apply tags
# MAGIC
# MAGIC #### 4. Make Tags Discoverable
# MAGIC - Create views that surface tagged data
# MAGIC - Build dashboards showing tag usage
# MAGIC - Enable search by tags in your data catalog
# MAGIC
# MAGIC #### 5. Monitor and Maintain
# MAGIC - Regularly review and update tags
# MAGIC - Archive or remove obsolete tags
# MAGIC - Track tag coverage across your data estate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Real-World Tagging Scenarios
# MAGIC
# MAGIC Let's explore some practical scenarios where tagging provides significant value.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1: GDPR Compliance Audit
# MAGIC
# MAGIC Find all tables containing EU customer PII for GDPR compliance review.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all tables with GDPR requirements
# MAGIC SELECT DISTINCT
# MAGIC   t1.catalog_name,
# MAGIC   t1.schema_name,
# MAGIC   t1.table_name,
# MAGIC   MAX(CASE WHEN t1.tag_name = 'contains_pii' THEN t1.tag_value END) as has_pii,
# MAGIC   MAX(CASE WHEN t1.tag_name = 'pii_types' THEN t1.tag_value END) as pii_types,
# MAGIC   MAX(CASE WHEN t1.tag_name = 'compliance_requirement' THEN t1.tag_value END) as compliance
# MAGIC FROM system.information_schema.table_tags t1
# MAGIC WHERE t1.schema_name = 'tagging_demo'
# MAGIC   AND EXISTS (
# MAGIC     SELECT 1 FROM system.information_schema.table_tags t2
# MAGIC     WHERE t2.catalog_name = t1.catalog_name
# MAGIC       AND t2.schema_name = t1.schema_name
# MAGIC       AND t2.table_name = t1.table_name
# MAGIC       AND t2.tag_name = 'compliance_requirement'
# MAGIC       AND t2.tag_value LIKE '%GDPR%'
# MAGIC   )
# MAGIC GROUP BY t1.catalog_name, t1.schema_name, t1.table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2: Data Retention Policy Enforcement
# MAGIC
# MAGIC Identify tables that should be archived based on retention policies.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find tables with short retention periods that may need attention
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   tag_value as retention_years,
# MAGIC   CASE 
# MAGIC     WHEN CAST(tag_value AS INT) <= 3 THEN 'Review Soon'
# MAGIC     WHEN CAST(tag_value AS INT) <= 5 THEN 'Monitor'
# MAGIC     ELSE 'Long-term'
# MAGIC   END as retention_status
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name = 'retention_years'
# MAGIC ORDER BY CAST(tag_value AS INT);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3: Cost Allocation Report
# MAGIC
# MAGIC Generate a report of data assets by owning team for cost allocation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary of tables by owner
# MAGIC SELECT 
# MAGIC   tag_value as owning_team,
# MAGIC   COUNT(DISTINCT table_name) as table_count,
# MAGIC   COLLECT_LIST(table_name) as tables
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name = 'data_owner'
# MAGIC GROUP BY tag_value
# MAGIC ORDER BY table_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 4: Data Quality Dashboard
# MAGIC
# MAGIC Identify tables with quality issues that need attention.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find tables with quality scores below threshold
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   CAST(tag_value AS INT) as quality_score,
# MAGIC   CASE 
# MAGIC     WHEN CAST(tag_value AS INT) >= 95 THEN 'Excellent'
# MAGIC     WHEN CAST(tag_value AS INT) >= 85 THEN 'Good'
# MAGIC     WHEN CAST(tag_value AS INT) >= 70 THEN 'Needs Improvement'
# MAGIC     ELSE 'Critical'
# MAGIC   END as quality_status
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'tagging_demo'
# MAGIC   AND tag_name = 'quality_score'
# MAGIC ORDER BY CAST(tag_value AS INT);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue 1: Tags Not Appearing in Queries
# MAGIC
# MAGIC **Problem**: Tags are applied but don't show up in information schema queries.
# MAGIC
# MAGIC **Solutions**:
# MAGIC - Check you're querying the correct information schema view
# MAGIC - Verify you have SELECT privilege on the information schema
# MAGIC - Ensure you're filtering by the correct catalog/schema names
# MAGIC - Wait a few seconds for the information schema to refresh
# MAGIC
# MAGIC ### Issue 2: Cannot Apply Tags
# MAGIC
# MAGIC **Problem**: Getting permission denied when trying to apply tags.
# MAGIC
# MAGIC **Solutions**:
# MAGIC - Verify you have MODIFY privilege on the object
# MAGIC - For governed tags, check you have APPLY TAG privilege
# MAGIC - Ensure the object exists and you have access to it
# MAGIC
# MAGIC ### Issue 3: Tag Values Not Standardized
# MAGIC
# MAGIC **Problem**: Different variations of tag values causing confusion (e.g., "true" vs "True" vs "yes").
# MAGIC
# MAGIC **Solutions**:
# MAGIC - Use governed tags with controlled vocabularies
# MAGIC - Document standard tag values in your governance documentation
# MAGIC - Create validation scripts to check tag compliance
# MAGIC - Use stored procedures that enforce standard values

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What We Covered
# MAGIC
# MAGIC In this demo, you learned:
# MAGIC
# MAGIC ✅ **Fundamentals**: What Unity Catalog tags are and why they matter  
# MAGIC ✅ **Application**: How to apply tags to schemas and tables using `ALTER` statements  
# MAGIC ✅ **Querying**: How to query and audit tags using information schema views  
# MAGIC ✅ **Automation**: How to create stored procedures to automate tagging  
# MAGIC ✅ **Tag Types**: Differences between user-defined, governed, and system tags  
# MAGIC ✅ **Best Practices**: Naming conventions, governance strategies, and real-world scenarios  
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Practice**: Complete the hands-on lab (Notebook 2) to reinforce these concepts
# MAGIC 2. **Plan**: Design a tag taxonomy for your organization's data
# MAGIC 3. **Implement**: Start tagging your existing Unity Catalog objects
# MAGIC 4. **Govern**: Consider implementing governed tags for critical classifications
# MAGIC 5. **Automate**: Build stored procedures and workflows to maintain tag consistency
# MAGIC
# MAGIC ### Additional Resources
# MAGIC
# MAGIC - [Unity Catalog Tags Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/tags.html)
# MAGIC - [Information Schema Reference](https://docs.databricks.com/en/sql/language-manual/information-schema.html)
# MAGIC - [SQL Stored Procedures](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-procedure.html)
# MAGIC - [Data Governance Best Practices](https://docs.databricks.com/en/data-governance/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC If you want to remove the demo objects, uncomment and run the cell below.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to clean up demo objects
# MAGIC -- DROP SCHEMA IF EXISTS tagging_demo CASCADE;
