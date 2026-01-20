# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Change Data Capture (CDC) Review
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture provides a comprehensive review of Change Data Capture (CDC) concepts and their implementation in Lakeflow Spark Declarative Pipelines. We'll explore SCD (Slowly Changing Dimension) patterns, the AUTO CDC INTO syntax, and best practices for maintaining historical data.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain Change Data Capture and its business value
# MAGIC - Differentiate between SCD Type 1 and Type 2 patterns
# MAGIC - Understand the AUTO CDC INTO syntax and capabilities
# MAGIC - Design pipelines that track historical changes effectively
# MAGIC - Apply best practices for CDC operations in production
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Change Data Capture (CDC)?
# MAGIC
# MAGIC **Change Data Capture (CDC)** is a design pattern that identifies and captures changes made to data in a source system and makes those changes available for downstream processing.
# MAGIC
# MAGIC ### Why CDC Matters
# MAGIC
# MAGIC 1. **Incremental Processing**: Process only changed data instead of full reloads
# MAGIC 2. **Historical Tracking**: Maintain a complete audit trail of all changes
# MAGIC 3. **Reduced Load**: Minimize impact on source systems
# MAGIC 4. **Real-time Analytics**: Enable near real-time data synchronization
# MAGIC 5. **Compliance**: Meet regulatory requirements for data lineage
# MAGIC
# MAGIC ### Common CDC Use Cases
# MAGIC
# MAGIC - **Customer Data**: Track changes to customer profiles, addresses, preferences
# MAGIC - **Product Catalog**: Monitor pricing changes, product descriptions, availability
# MAGIC - **Financial Records**: Maintain audit trails for transactions and balances
# MAGIC - **Employee Data**: Track promotions, department changes, salary adjustments
# MAGIC - **Inventory**: Monitor stock levels, locations, and movements

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Operations: Insert, Update, Delete
# MAGIC
# MAGIC CDC systems track three types of changes:
# MAGIC
# MAGIC | Operation | Description | Example |
# MAGIC |-----------|-------------|---------|
# MAGIC | **INSERT** | New record added | New customer registration |
# MAGIC | **UPDATE** | Existing record modified | Customer address change |
# MAGIC | **DELETE** | Record removed | Customer account closed |
# MAGIC
# MAGIC ### CDC Data Format
# MAGIC
# MAGIC CDC feeds typically include:
# MAGIC - **Change Type**: Operation indicator (I/U/D or INSERT/UPDATE/DELETE)
# MAGIC - **Sequence Number**: Order of changes
# MAGIC - **Timestamp**: When the change occurred
# MAGIC - **Changed Data**: The actual data values (before/after)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slowly Changing Dimensions (SCD) Overview
# MAGIC
# MAGIC **Slowly Changing Dimensions** are dimensional data that changes slowly over time, rather than changing on a regular schedule. Different SCD types determine how we handle these changes.
# MAGIC
# MAGIC ### SCD Type 0: Fixed Dimension
# MAGIC - No changes allowed
# MAGIC - Original values are preserved
# MAGIC - Used for immutable data (e.g., birth date)
# MAGIC
# MAGIC ### SCD Type 1: Overwrite
# MAGIC - New data overwrites old data
# MAGIC - No history is kept
# MAGIC - Simple and requires less storage
# MAGIC - Used when history is not important
# MAGIC
# MAGIC ### SCD Type 2: Add New Row
# MAGIC - Maintains full history
# MAGIC - Each change creates a new row
# MAGIC - Uses effective dates or version numbers
# MAGIC - Most common for audit and compliance
# MAGIC
# MAGIC ### SCD Type 3: Add New Columns
# MAGIC - Limited history (usually previous value only)
# MAGIC - Adds columns like "previous_value" and "effective_date"
# MAGIC - Less common in modern architectures

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep Dive: SCD Type 1
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC When a record changes:
# MAGIC 1. Find the existing record using the key
# MAGIC 2. Replace all changed values
# MAGIC 3. Keep only the most recent state
# MAGIC
# MAGIC ### Example: Customer Phone Number Update
# MAGIC
# MAGIC **Before Update:**
# MAGIC | customer_id | name | phone | email |
# MAGIC |-------------|------|-------|-------|
# MAGIC | 1001 | John Smith | 555-1234 | john@email.com |
# MAGIC
# MAGIC **After Update (SCD Type 1):**
# MAGIC | customer_id | name | phone | email |
# MAGIC |-------------|------|-------|-------|
# MAGIC | 1001 | John Smith | 555-5678 | john@email.com |
# MAGIC
# MAGIC ⚠️ **Note**: Old phone number (555-1234) is lost forever!
# MAGIC
# MAGIC ### When to Use SCD Type 1
# MAGIC
# MAGIC ✅ **Good for:**
# MAGIC - Correcting data errors
# MAGIC - Updating non-critical attributes
# MAGIC - When storage is limited
# MAGIC - When historical tracking is not required
# MAGIC
# MAGIC ❌ **Avoid for:**
# MAGIC - Audit requirements
# MAGIC - Compliance tracking
# MAGIC - Historical analysis
# MAGIC - Reversible changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep Dive: SCD Type 2
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC When a record changes:
# MAGIC 1. Close the current active record (set end date)
# MAGIC 2. Insert a new record with the updated values
# MAGIC 3. Each record has a validity period
# MAGIC
# MAGIC ### Key Columns for SCD Type 2
# MAGIC
# MAGIC | Column | Purpose | Example Values |
# MAGIC |--------|---------|----------------|
# MAGIC | **Primary Key** | Unique record identifier | customer_id |
# MAGIC | **Surrogate Key** | Unique row identifier | customer_sk (auto-increment) |
# MAGIC | **Effective Start Date** | When record became valid | 2024-01-01 |
# MAGIC | **Effective End Date** | When record was superseded | 2024-06-15 or NULL |
# MAGIC | **Current Flag** | Is this the active record? | true/false or 1/0 |
# MAGIC | **Version Number** | Record version | 1, 2, 3... |
# MAGIC
# MAGIC ### Example: Customer Phone Number Update
# MAGIC
# MAGIC **Initial State:**
# MAGIC | customer_sk | customer_id | name | phone | effective_start | effective_end | is_current |
# MAGIC |-------------|-------------|------|-------|-----------------|---------------|------------|
# MAGIC | 1 | 1001 | John Smith | 555-1234 | 2024-01-01 | NULL | true |
# MAGIC
# MAGIC **After Update (SCD Type 2):**
# MAGIC | customer_sk | customer_id | name | phone | effective_start | effective_end | is_current |
# MAGIC |-------------|-------------|------|-------|-----------------|---------------|------------|
# MAGIC | 1 | 1001 | John Smith | 555-1234 | 2024-01-01 | 2024-06-15 | false |
# MAGIC | 2 | 1001 | John Smith | 555-5678 | 2024-06-15 | NULL | true |
# MAGIC
# MAGIC ✅ **Note**: Both records preserved! Complete history available.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying SCD Type 2 Data
# MAGIC
# MAGIC ### Get Current State (Most Recent Values)
# MAGIC ```sql
# MAGIC SELECT customer_id, name, phone, email
# MAGIC FROM customers
# MAGIC WHERE is_current = true
# MAGIC -- OR WHERE effective_end IS NULL
# MAGIC ```
# MAGIC
# MAGIC ### Get Historical State (Point-in-Time Query)
# MAGIC ```sql
# MAGIC SELECT customer_id, name, phone, email
# MAGIC FROM customers
# MAGIC WHERE effective_start <= '2024-03-01'
# MAGIC   AND (effective_end > '2024-03-01' OR effective_end IS NULL)
# MAGIC ```
# MAGIC
# MAGIC ### Get Change History for a Customer
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   phone,
# MAGIC   effective_start,
# MAGIC   effective_end,
# MAGIC   is_current
# MAGIC FROM customers
# MAGIC WHERE customer_id = 1001
# MAGIC ORDER BY effective_start
# MAGIC ```
# MAGIC
# MAGIC ### Track What Changed
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   effective_start as change_date,
# MAGIC   LAG(phone) OVER (PARTITION BY customer_id ORDER BY effective_start) as old_phone,
# MAGIC   phone as new_phone
# MAGIC FROM customers
# MAGIC WHERE customer_id = 1001
# MAGIC ORDER BY effective_start
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## APPLY CHANGES INTO: SCD Type 1
# MAGIC
# MAGIC Lakeflow Declarative Pipelines provides `APPLY CHANGES INTO` for SCD Type 1 updates.
# MAGIC
# MAGIC ### Syntax
# MAGIC ```sql
# MAGIC APPLY CHANGES INTO
# MAGIC   LIVE.target_table
# MAGIC FROM
# MAGIC   STREAM(LIVE.source_changes)
# MAGIC KEYS
# MAGIC   (key_column)
# MAGIC SEQUENCE BY
# MAGIC   sequence_column
# MAGIC STORED AS
# MAGIC   SCD TYPE 1
# MAGIC ```
# MAGIC
# MAGIC ### Example: Customer Updates (SCD Type 1)
# MAGIC ```sql
# MAGIC APPLY CHANGES INTO
# MAGIC   LIVE.customers
# MAGIC FROM
# MAGIC   STREAM(LIVE.customer_changes)
# MAGIC KEYS
# MAGIC   (customer_id)
# MAGIC SEQUENCE BY
# MAGIC   update_timestamp
# MAGIC STORED AS
# MAGIC   SCD TYPE 1
# MAGIC ```
# MAGIC
# MAGIC ### Key Components
# MAGIC
# MAGIC - **KEYS**: Columns that uniquely identify records (primary key)
# MAGIC - **SEQUENCE BY**: Column to order changes (timestamp, sequence number)
# MAGIC - **STORED AS SCD TYPE 1**: Overwrites existing records

# COMMAND ----------

# MAGIC %md
# MAGIC ## AUTO CDC INTO: SCD Type 2
# MAGIC
# MAGIC The `AUTO CDC INTO` syntax simplifies SCD Type 2 implementation by automatically:
# MAGIC - Creating surrogate keys
# MAGIC - Managing effective dates
# MAGIC - Maintaining current flags
# MAGIC - Handling inserts, updates, and deletes
# MAGIC
# MAGIC ### Syntax
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE target_table;
# MAGIC
# MAGIC AUTO CDC INTO
# MAGIC   LIVE.target_table
# MAGIC FROM
# MAGIC   STREAM(LIVE.source_changes)
# MAGIC KEYS
# MAGIC   (key_column)
# MAGIC SEQUENCE BY
# MAGIC   sequence_column
# MAGIC TRACK HISTORY ON
# MAGIC   (columns_to_track)
# MAGIC STORED AS
# MAGIC   SCD TYPE 2
# MAGIC ```
# MAGIC
# MAGIC ### Example: Customer Updates (SCD Type 2)
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customers_history;
# MAGIC
# MAGIC AUTO CDC INTO
# MAGIC   LIVE.customers_history
# MAGIC FROM
# MAGIC   STREAM(LIVE.customer_changes)
# MAGIC KEYS
# MAGIC   (customer_id)
# MAGIC SEQUENCE BY
# MAGIC   update_timestamp
# MAGIC TRACK HISTORY ON
# MAGIC   (name, phone, email, address)
# MAGIC STORED AS
# MAGIC   SCD TYPE 2
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## AUTO CDC INTO: Key Components
# MAGIC
# MAGIC ### KEYS Clause
# MAGIC - Defines the business key (natural key)
# MAGIC - Can be a single column or composite key
# MAGIC - Used to match source and target records
# MAGIC
# MAGIC ```sql
# MAGIC -- Single key
# MAGIC KEYS (customer_id)
# MAGIC
# MAGIC -- Composite key
# MAGIC KEYS (customer_id, account_id)
# MAGIC ```
# MAGIC
# MAGIC ### SEQUENCE BY Clause
# MAGIC - Determines the order of changes
# MAGIC - Must be monotonically increasing
# MAGIC - Typically a timestamp or sequence number
# MAGIC
# MAGIC ```sql
# MAGIC SEQUENCE BY update_timestamp
# MAGIC -- or
# MAGIC SEQUENCE BY sequence_number
# MAGIC ```
# MAGIC
# MAGIC ### TRACK HISTORY ON Clause
# MAGIC - Specifies which columns trigger new versions
# MAGIC - Changes to these columns create new rows
# MAGIC - Other columns are updated in place
# MAGIC
# MAGIC ```sql
# MAGIC TRACK HISTORY ON (name, phone, email)
# MAGIC -- Changes to name, phone, or email create new versions
# MAGIC -- Changes to other columns just update the current row
# MAGIC ```
# MAGIC
# MAGIC ### STORED AS SCD TYPE 2
# MAGIC - Enables automatic historical tracking
# MAGIC - Creates system columns: __start_at, __end_at, __is_current
# MAGIC - Manages version lifecycle automatically

# COMMAND ----------

# MAGIC %md
# MAGIC ## AUTO CDC Generated Columns
# MAGIC
# MAGIC When using `AUTO CDC INTO` with SCD Type 2, Databricks automatically adds metadata columns:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | **__start_at** | TIMESTAMP | When this version became effective |
# MAGIC | **__end_at** | TIMESTAMP | When this version was superseded (NULL for current) |
# MAGIC | **__is_current** | BOOLEAN | True for the most recent version |
# MAGIC | **__sequence_num** | LONG | The sequence value from SEQUENCE BY clause |
# MAGIC | **__deleted** | BOOLEAN | True if the record was deleted |
# MAGIC
# MAGIC ### Example Query Using Generated Columns
# MAGIC ```sql
# MAGIC -- Get current active records only
# MAGIC SELECT *
# MAGIC FROM customers_history
# MAGIC WHERE __is_current = true
# MAGIC   AND __deleted = false
# MAGIC
# MAGIC -- Get all versions of a customer
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   phone,
# MAGIC   __start_at,
# MAGIC   __end_at,
# MAGIC   __is_current
# MAGIC FROM customers_history
# MAGIC WHERE customer_id = 1001
# MAGIC ORDER BY __start_at
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Deletes in SCD Type 2
# MAGIC
# MAGIC There are two common approaches for handling deletions:
# MAGIC
# MAGIC ### 1. Soft Delete (Recommended)
# MAGIC - Mark records as deleted with `__deleted = true`
# MAGIC - Keep the historical record
# MAGIC - Set `__end_at` timestamp
# MAGIC - Useful for audit and recovery
# MAGIC
# MAGIC ```sql
# MAGIC -- Soft delete is automatic with AUTO CDC INTO
# MAGIC -- Records with operation = 'D' get __deleted = true
# MAGIC ```
# MAGIC
# MAGIC ### 2. Hard Delete
# MAGIC - Physically remove the record
# MAGIC - Cannot be recovered
# MAGIC - Use APPLY CHANGES with DELETE option
# MAGIC
# MAGIC ```sql
# MAGIC APPLY CHANGES INTO LIVE.customers
# MAGIC FROM STREAM(LIVE.customer_changes)
# MAGIC KEYS (customer_id)
# MAGIC APPLY AS DELETE WHEN operation = 'D'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining SCD Type 1 and Type 2
# MAGIC
# MAGIC In real-world scenarios, you often need both:
# MAGIC - **Type 2** for columns that require history (compliance, audit)
# MAGIC - **Type 1** for columns where only current value matters (corrections)
# MAGIC
# MAGIC ### Strategy: Two-Table Approach
# MAGIC
# MAGIC ```sql
# MAGIC -- Table 1: SCD Type 2 for historical attributes
# MAGIC CREATE OR REFRESH STREAMING TABLE customer_history;
# MAGIC AUTO CDC INTO LIVE.customer_history
# MAGIC FROM STREAM(LIVE.customer_changes)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY update_timestamp
# MAGIC TRACK HISTORY ON (address, phone, email)
# MAGIC STORED AS SCD TYPE 2;
# MAGIC
# MAGIC -- Table 2: SCD Type 1 for current-only attributes
# MAGIC APPLY CHANGES INTO LIVE.customer_current
# MAGIC FROM STREAM(LIVE.customer_changes)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY update_timestamp
# MAGIC STORED AS SCD TYPE 1;
# MAGIC ```
# MAGIC
# MAGIC ### Strategy: Selective Column Tracking
# MAGIC
# MAGIC ```sql
# MAGIC -- Only track history on specific columns
# MAGIC AUTO CDC INTO LIVE.customers
# MAGIC FROM STREAM(LIVE.customer_changes)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY update_timestamp
# MAGIC TRACK HISTORY ON (salary, title, department)  -- Track these
# MAGIC STORED AS SCD TYPE 2;
# MAGIC -- Other columns (like email_typo_fix) update in place
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Best Practices
# MAGIC
# MAGIC ### 1. Choose the Right SCD Type
# MAGIC - Use **Type 1** for corrections and non-critical changes
# MAGIC - Use **Type 2** for compliance, audit, and historical analysis
# MAGIC - Combine both when needed
# MAGIC
# MAGIC ### 2. Design Proper Keys
# MAGIC - Use stable business keys (customer_id, not email)
# MAGIC - Avoid keys that might change over time
# MAGIC - Use composite keys when necessary
# MAGIC
# MAGIC ### 3. Sequence Management
# MAGIC - Ensure monotonically increasing sequence values
# MAGIC - Use timestamps with sufficient precision
# MAGIC - Consider using sequence numbers for ordered batch loads
# MAGIC
# MAGIC ### 4. Column Selection
# MAGIC - In `TRACK HISTORY ON`, include only columns that need history
# MAGIC - Reduces storage and processing overhead
# MAGIC - Group logically related columns together
# MAGIC
# MAGIC ### 5. Performance Optimization
# MAGIC - Partition by date columns (__start_at)
# MAGIC - Create indexes on frequently queried keys
# MAGIC - Use Liquid Clustering for flexible access patterns
# MAGIC - Archive old versions if retention policies allow
# MAGIC
# MAGIC ### 6. Query Patterns
# MAGIC - Always filter on __is_current for current state queries
# MAGIC - Use point-in-time queries for historical analysis
# MAGIC - Cache frequently accessed historical views
# MAGIC
# MAGIC ### 7. Testing
# MAGIC - Test all CDC operations: INSERT, UPDATE, DELETE
# MAGIC - Verify sequence ordering handles out-of-order events
# MAGIC - Test duplicate detection and idempotency
# MAGIC - Validate historical queries return correct results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common CDC Pitfalls to Avoid
# MAGIC
# MAGIC ### 1. Out-of-Order Data
# MAGIC **Problem**: Changes arrive out of sequence order
# MAGIC
# MAGIC **Solution**: 
# MAGIC - Use proper SEQUENCE BY column
# MAGIC - Consider using watermarking for streaming sources
# MAGIC - Implement late-arriving data handling
# MAGIC
# MAGIC ### 2. Incorrect Key Selection
# MAGIC **Problem**: Using mutable fields as keys (like email)
# MAGIC
# MAGIC **Solution**: 
# MAGIC - Use immutable business identifiers
# MAGIC - Generate surrogate keys if needed
# MAGIC
# MAGIC ### 3. Missing Sequence Values
# MAGIC **Problem**: NULL or duplicate sequence values
# MAGIC
# MAGIC **Solution**: 
# MAGIC - Add NOT NULL constraints
# MAGIC - Generate monotonic sequences at source
# MAGIC - Use UUID or timestamp with sufficient precision
# MAGIC
# MAGIC ### 4. Over-Tracking History
# MAGIC **Problem**: Tracking too many columns creates excessive versions
# MAGIC
# MAGIC **Solution**: 
# MAGIC - Only track business-critical columns
# MAGIC - Use Type 1 for metadata and technical fields
# MAGIC - Review and optimize column selection periodically
# MAGIC
# MAGIC ### 5. Not Handling Deletes
# MAGIC **Problem**: Deleted records not propagated
# MAGIC
# MAGIC **Solution**: 
# MAGIC - Ensure source CDC includes delete operations
# MAGIC - Use soft deletes for audit requirements
# MAGIC - Document delete handling strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: SCD Type 1 vs Type 2
# MAGIC
# MAGIC | Aspect | SCD Type 1 | SCD Type 2 |
# MAGIC |--------|------------|------------|
# MAGIC | **History** | Not preserved | Full history maintained |
# MAGIC | **Storage** | Minimal | Higher (one row per change) |
# MAGIC | **Complexity** | Simple | More complex queries |
# MAGIC | **Performance** | Faster updates | Slower (inserts + updates) |
# MAGIC | **Use Cases** | Corrections, non-critical | Audit, compliance, analysis |
# MAGIC | **Auditability** | No audit trail | Complete audit trail |
# MAGIC | **Rollback** | Not possible | Can reconstruct history |
# MAGIC | **Syntax** | APPLY CHANGES INTO | AUTO CDC INTO |
# MAGIC | **System Columns** | None | __start_at, __end_at, etc. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-World Example: E-Commerce Customer
# MAGIC
# MAGIC ### Scenario
# MAGIC An e-commerce company tracks customer data. Different attributes have different requirements:
# MAGIC
# MAGIC | Attribute | History Required? | SCD Type |
# MAGIC |-----------|-------------------|----------|
# MAGIC | Customer ID | Immutable | - |
# MAGIC | Name | Corrections only | Type 1 |
# MAGIC | Email | Corrections only | Type 1 |
# MAGIC | Shipping Address | Yes (for analytics) | Type 2 |
# MAGIC | Billing Address | Yes (for compliance) | Type 2 |
# MAGIC | Loyalty Tier | Yes (for campaigns) | Type 2 |
# MAGIC | Last Login | No | Type 1 |
# MAGIC
# MAGIC ### Implementation
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customer_history;
# MAGIC
# MAGIC AUTO CDC INTO LIVE.customer_history
# MAGIC FROM STREAM(LIVE.customer_changes)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY update_timestamp
# MAGIC TRACK HISTORY ON (
# MAGIC   shipping_address,
# MAGIC   shipping_city,
# MAGIC   shipping_state,
# MAGIC   shipping_zip,
# MAGIC   billing_address,
# MAGIC   billing_city,
# MAGIC   billing_state,
# MAGIC   billing_zip,
# MAGIC   loyalty_tier
# MAGIC )
# MAGIC STORED AS SCD TYPE 2;
# MAGIC
# MAGIC -- Name, email, last_login update in place (Type 1 behavior)
# MAGIC -- Tracked columns create new versions (Type 2 behavior)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC 1. **CDC** captures and processes data changes incrementally
# MAGIC 2. **SCD Type 1** overwrites data, no history preserved
# MAGIC 3. **SCD Type 2** maintains full history with versioning
# MAGIC 4. **APPLY CHANGES INTO** implements Type 1 updates
# MAGIC 5. **AUTO CDC INTO** automates Type 2 with system-managed columns
# MAGIC 6. **TRACK HISTORY ON** specifies which columns create new versions
# MAGIC 7. Combine Type 1 and Type 2 in the same pipeline for flexibility
# MAGIC
# MAGIC ### When to Use Each Pattern
# MAGIC
# MAGIC | Use SCD Type 1 When: | Use SCD Type 2 When: |
# MAGIC |---------------------|----------------------|
# MAGIC | Data corrections | Compliance requirements |
# MAGIC | Storage is limited | Audit trail needed |
# MAGIC | History not required | Historical analysis needed |
# MAGIC | Simple reporting | Point-in-time queries needed |
# MAGIC | Current state only matters | Change tracking required |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand CDC concepts and SCD patterns, continue to the next demo:
# MAGIC
# MAGIC **4_Demo_Automating_SCD_Type_2_with_AUTO_CDC**
# MAGIC
# MAGIC In the next demo, you'll:
# MAGIC - Build a complete SCD Type 2 pipeline using AUTO CDC INTO
# MAGIC - Handle inserts, updates, and deletes
# MAGIC - Query historical data with window functions
# MAGIC - Implement best practices for production CDC pipelines
