# Databricks notebook source
# MAGIC %md
# MAGIC # Hybrid Data Modeling for OLTP and OLAP
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand differences between OLTP and OLAP data models
# MAGIC - Design schemas optimized for both transaction and analytics
# MAGIC - Implement Slowly Changing Dimensions (SCD) patterns
# MAGIC - Apply temporal table patterns
# MAGIC - Understand event sourcing principles
# MAGIC - Design CQRS (Command Query Responsibility Segregation) architecture
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Understanding of database normalization
# MAGIC - Knowledge of star/snowflake schemas
# MAGIC - Familiarity with temporal data concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## OLTP vs OLAP Data Models
# MAGIC
# MAGIC ### Characteristics Comparison
# MAGIC
# MAGIC | Aspect | OLTP | OLAP |
# MAGIC |--------|------|------|
# MAGIC | **Purpose** | Transactional processing | Analytical queries |
# MAGIC | **Schema** | Normalized (3NF) | Denormalized (star/snowflake) |
# MAGIC | **Operations** | INSERT, UPDATE, DELETE | SELECT, aggregations |
# MAGIC | **Query Pattern** | Point queries, small results | Scans, large aggregations |
# MAGIC | **Optimization** | Row-based, indexes | Column-based, partitions |
# MAGIC | **Consistency** | ACID, immediate | Eventual, batch updates |
# MAGIC | **Updates** | Frequent | Infrequent |
# MAGIC
# MAGIC ### Schema Evolution
# MAGIC
# MAGIC ```
# MAGIC OLTP (Normalized):
# MAGIC ┌──────────────┐
# MAGIC │   Orders     │
# MAGIC ├──────────────┤
# MAGIC │ order_id     │◄──┐
# MAGIC │ customer_id  │   │
# MAGIC │ order_date   │   │
# MAGIC └──────────────┘   │
# MAGIC                    │
# MAGIC ┌──────────────────┴──┐
# MAGIC │   Order_Items       │
# MAGIC ├─────────────────────┤
# MAGIC │ item_id             │
# MAGIC │ order_id (FK)       │
# MAGIC │ product_id          │
# MAGIC │ quantity            │
# MAGIC └─────────────────────┘
# MAGIC
# MAGIC OLAP (Denormalized):
# MAGIC ┌────────────────────────────────┐
# MAGIC │     Order_Facts                │
# MAGIC ├────────────────────────────────┤
# MAGIC │ order_id                       │
# MAGIC │ customer_name                  │
# MAGIC │ product_name                   │
# MAGIC │ quantity                       │
# MAGIC │ total                          │
# MAGIC │ order_date                     │
# MAGIC │ customer_region                │
# MAGIC │ product_category               │
# MAGIC └────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slowly Changing Dimensions (SCD)
# MAGIC
# MAGIC ### SCD Type 1: Overwrite
# MAGIC
# MAGIC ```sql
# MAGIC -- No history tracking, simply update
# MAGIC UPDATE customers
# MAGIC SET tier = 'platinum', updated_at = CURRENT_TIMESTAMP
# MAGIC WHERE customer_id = 'C001';
# MAGIC ```
# MAGIC
# MAGIC **Use when**: Historical values not needed
# MAGIC
# MAGIC ### SCD Type 2: Add New Row
# MAGIC
# MAGIC ```sql
# MAGIC -- Keep full history with validity periods
# MAGIC CREATE TABLE customers_scd2 (
# MAGIC   customer_key BIGINT,
# MAGIC   customer_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   tier STRING,
# MAGIC   valid_from TIMESTAMP,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC );
# MAGIC
# MAGIC -- Insert new version
# MAGIC UPDATE customers_scd2 SET 
# MAGIC   valid_to = CURRENT_TIMESTAMP,
# MAGIC   is_current = FALSE
# MAGIC WHERE customer_id = 'C001' AND is_current = TRUE;
# MAGIC
# MAGIC INSERT INTO customers_scd2 VALUES (
# MAGIC   2, 'C001', 'Alice', 'platinum', 
# MAGIC   CURRENT_TIMESTAMP, NULL, TRUE
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Use when**: Need complete history for analysis
# MAGIC
# MAGIC ### SCD Type 3: Add New Column
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE customers 
# MAGIC ADD COLUMN previous_tier STRING;
# MAGIC
# MAGIC UPDATE customers
# MAGIC SET previous_tier = tier,
# MAGIC     tier = 'platinum'
# MAGIC WHERE customer_id = 'C001';
# MAGIC ```
# MAGIC
# MAGIC **Use when**: Only need previous value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Tables
# MAGIC
# MAGIC ### System-Versioned Temporal Tables
# MAGIC
# MAGIC ```sql
# MAGIC -- Main table
# MAGIC CREATE TABLE orders (
# MAGIC   order_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   order_total DECIMAL(12,2),
# MAGIC   status STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- History table (automatic with Delta)
# MAGIC SELECT * FROM orders VERSION AS OF 5;
# MAGIC SELECT * FROM orders TIMESTAMP AS OF '2024-01-15T10:00:00';
# MAGIC ```
# MAGIC
# MAGIC ### Application-Time Temporal Tables
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE insurance_policies (
# MAGIC   policy_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   coverage_amount DECIMAL(12,2),
# MAGIC   effective_from DATE,
# MAGIC   effective_to DATE
# MAGIC );
# MAGIC
# MAGIC -- Query policies active on specific date
# MAGIC SELECT * FROM insurance_policies
# MAGIC WHERE '2024-01-15' BETWEEN effective_from AND effective_to;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Sourcing Pattern
# MAGIC
# MAGIC ### Concept
# MAGIC
# MAGIC Store all changes as immutable events, reconstruct current state by replaying events.
# MAGIC
# MAGIC ```
# MAGIC Event Stream:
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │ Event 1: AccountCreated                 │
# MAGIC │   account_id: A001                      │
# MAGIC │   initial_balance: 1000                 │
# MAGIC ├─────────────────────────────────────────┤
# MAGIC │ Event 2: MoneyDeposited                 │
# MAGIC │   account_id: A001                      │
# MAGIC │   amount: 500                           │
# MAGIC ├─────────────────────────────────────────┤
# MAGIC │ Event 3: MoneyWithdrawn                 │
# MAGIC │   account_id: A001                      │
# MAGIC │   amount: 200                           │
# MAGIC └─────────────────────────────────────────┘
# MAGIC
# MAGIC Current State (Projection):
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │ Account A001                            │
# MAGIC │ balance: 1300 (1000 + 500 - 200)        │
# MAGIC └─────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Event Store Schema
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE event_store (
# MAGIC   event_id STRING,
# MAGIC   aggregate_id STRING,
# MAGIC   aggregate_type STRING,
# MAGIC   event_type STRING,
# MAGIC   event_data STRING,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   sequence_number BIGINT
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (aggregate_type);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## CQRS Pattern
# MAGIC
# MAGIC ### Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────┐
# MAGIC │                Application                   │
# MAGIC └──────────────┬───────────────────┬───────────┘
# MAGIC                │                   │
# MAGIC                ▼                   ▼
# MAGIC         ┌─────────────┐    ┌──────────────┐
# MAGIC         │  Commands   │    │   Queries    │
# MAGIC         │  (Writes)   │    │   (Reads)    │
# MAGIC         └──────┬──────┘    └──────┬───────┘
# MAGIC                │                   │
# MAGIC                ▼                   ▼
# MAGIC         ┌─────────────┐    ┌──────────────┐
# MAGIC         │   OLTP DB   │    │   OLAP DB    │
# MAGIC         │(Normalized) │────►│(Denormalized)│
# MAGIC         │             │sync │              │
# MAGIC         └─────────────┘    └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC - **Separate optimization**: OLTP optimized for writes, OLAP for reads
# MAGIC - **Independent scaling**: Scale read/write workloads independently
# MAGIC - **Security**: Different permissions for commands vs queries
# MAGIC - **Performance**: No contention between transactional and analytical queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hybrid Schema Design Patterns
# MAGIC
# MAGIC ### Pattern 1: Medallion with Operational Layer
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │           Operational Layer (OLTP)          │
# MAGIC │  - Normalized schema                        │
# MAGIC │  - Row-level updates                        │
# MAGIC │  - ACID transactions                        │
# MAGIC └────────────────┬────────────────────────────┘
# MAGIC                  │ CDC
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │         Bronze Layer (Raw Events)           │
# MAGIC └────────────────┬────────────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │       Silver Layer (Cleaned, SCD Type 2)    │
# MAGIC └────────────────┬────────────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │     Gold Layer (Aggregated, Denormalized)   │
# MAGIC └─────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Lambda Architecture
# MAGIC
# MAGIC ```
# MAGIC                 ┌──────────────┐
# MAGIC                 │   Ingestion  │
# MAGIC                 └───────┬──────┘
# MAGIC                         │
# MAGIC             ┌───────────┴───────────┐
# MAGIC             ▼                       ▼
# MAGIC      ┌─────────────┐        ┌─────────────┐
# MAGIC      │ Batch Layer │        │Stream Layer │
# MAGIC      │  (Complete) │        │(Real-time)  │
# MAGIC      └──────┬──────┘        └──────┬──────┘
# MAGIC             │                      │
# MAGIC             └──────────┬───────────┘
# MAGIC                        ▼
# MAGIC                 ┌─────────────┐
# MAGIC                 │Serving Layer│
# MAGIC                 │(Unified View│
# MAGIC                 └─────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **OLTP** prioritizes transactional integrity, **OLAP** prioritizes query performance
# MAGIC 2. **SCD patterns** track dimensional changes over time
# MAGIC 3. **Temporal tables** provide time-based querying
# MAGIC 4. **Event sourcing** stores all state changes as events
# MAGIC 5. **CQRS** separates read/write workloads
# MAGIC 6. **Hybrid models** combine benefits of both approaches
# MAGIC
# MAGIC ## Next Steps
# MAGIC - **Lab 11**: Building Production-Grade Event Sourcing Systems
