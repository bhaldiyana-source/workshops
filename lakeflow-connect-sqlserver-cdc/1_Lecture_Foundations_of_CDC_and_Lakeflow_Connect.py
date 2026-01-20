# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Foundations of Change Data Capture and Lakeflow Connect
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the fundamental concepts of Change Data Capture (CDC) and how Databricks Lakeflow Connect enables real-time data synchronization from SQL Server databases to the Lakehouse. You'll learn about transaction logs, CDC mechanisms, and the architecture of Lakeflow Connect components.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain what Change Data Capture (CDC) is and why it's important for modern data architectures
# MAGIC - Understand how SQL Server transaction logs enable CDC operations
# MAGIC - Identify the three core components of Lakeflow Connect (Gateway, Volume, Pipeline)
# MAGIC - Recognize when to use CDC vs. full-load ETL patterns
# MAGIC - Describe common use cases for real-time data synchronization
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Change Data Capture (CDC)?
# MAGIC
# MAGIC ### Traditional ETL: Full Load Pattern
# MAGIC
# MAGIC **Traditional Approach:**
# MAGIC - Extract entire table from source database
# MAGIC - Load complete dataset into target system
# MAGIC - Replace or truncate existing data
# MAGIC - Runs on scheduled intervals (daily, hourly)
# MAGIC
# MAGIC **Limitations:**
# MAGIC - **High Resource Usage**: Reading millions of rows repeatedly
# MAGIC - **Slow Processing**: Complete table scans every time
# MAGIC - **Network Overhead**: Transferring duplicate data
# MAGIC - **Limited Freshness**: Data only as fresh as last full load
# MAGIC - **No Delete Tracking**: Cannot identify removed records
# MAGIC
# MAGIC ### Modern Approach: Change Data Capture
# MAGIC
# MAGIC **CDC captures only the changes:**
# MAGIC - Identifies **INSERT** operations (new records)
# MAGIC - Tracks **UPDATE** operations (modified records with before/after values)
# MAGIC - Captures **DELETE** operations (removed records)
# MAGIC - Processes only changed data since last sync
# MAGIC
# MAGIC **Benefits:**
# MAGIC - ✅ **Efficient**: Process only changed records (1000s vs. millions)
# MAGIC - ✅ **Fast**: Near real-time synchronization (seconds to minutes)
# MAGIC - ✅ **Low Impact**: Minimal load on source systems
# MAGIC - ✅ **Complete Picture**: Captures all CRUD operations including deletes
# MAGIC - ✅ **Scalable**: Handles high-volume operational databases

# COMMAND ----------

# MAGIC %md
# MAGIC ## How SQL Server CDC Works
# MAGIC
# MAGIC ### Transaction Log Architecture
# MAGIC
# MAGIC SQL Server maintains a **transaction log** that records every change:
# MAGIC
# MAGIC ```
# MAGIC Application → SQL Server Database → Transaction Log
# MAGIC                                         ↓
# MAGIC                                    Log Records:
# MAGIC                                    - INSERT into customers
# MAGIC                                    - UPDATE orders SET status
# MAGIC                                    - DELETE from products
# MAGIC ```
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Log Sequence Number (LSN)**: Unique identifier for each transaction
# MAGIC - **Write-Ahead Logging**: Changes written to log before data files
# MAGIC - **ACID Compliance**: Ensures transaction consistency
# MAGIC - **Recovery Capability**: Used for backup/restore operations
# MAGIC
# MAGIC ### Enabling CDC on SQL Server
# MAGIC
# MAGIC **Database-Level Configuration:**
# MAGIC ```sql
# MAGIC -- Enable CDC at database level
# MAGIC EXEC sys.sp_cdc_enable_db;
# MAGIC ```
# MAGIC
# MAGIC **Table-Level Configuration:**
# MAGIC ```sql
# MAGIC -- Enable CDC for specific table
# MAGIC EXEC sys.sp_cdc_enable_table
# MAGIC     @source_schema = N'dbo',
# MAGIC     @source_name = N'customers',
# MAGIC     @role_name = NULL;
# MAGIC ```
# MAGIC
# MAGIC **What Happens:**
# MAGIC - Creates shadow tables (e.g., `cdc.dbo_customers_CT`)
# MAGIC - Captures INSERT, UPDATE, DELETE operations
# MAGIC - Stores before/after values for updates
# MAGIC - Maintains operation type metadata (`__$operation`: 1=delete, 2=insert, 3=update before, 4=update after)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Lakeflow Connect Architecture
# MAGIC
# MAGIC Lakeflow Connect is Databricks' native CDC solution that eliminates the need for third-party tools like Debezium, Kafka, or custom scripts.
# MAGIC
# MAGIC ### Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              SQL Server (Source Database)                │
# MAGIC │  ┌────────────────────────────────────────────────┐     │
# MAGIC │  │  RetailDB                                       │     │
# MAGIC │  │  ├─ customers Table  (CDC Enabled)             │     │
# MAGIC │  │  ├─ orders Table     (CDC Enabled)             │     │
# MAGIC │  │  └─ products Table   (CDC Enabled)             │     │
# MAGIC │  │                                                  │     │
# MAGIC │  │  Transaction Log → Change Tracking Tables      │     │
# MAGIC │  └────────────────────────────────────────────────┘     │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          │ ① Read CDC Changes
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              Databricks Lakehouse Platform              │
# MAGIC │                                                           │
# MAGIC │  ┌──────────────────────────────────────────────────┐  │
# MAGIC │  │  Component 1: Ingestion Gateway                  │  │
# MAGIC │  │  - Dedicated VM that reads transaction logs      │  │
# MAGIC │  │  - Authenticates to SQL Server                   │  │
# MAGIC │  │  - Tracks checkpoint/LSN position                │  │
# MAGIC │  └──────────────────────────────────────────────────┘  │
# MAGIC │                          │                               │
# MAGIC │                          │ ② Stage Changes               │
# MAGIC │                          ▼                               │
# MAGIC │  ┌──────────────────────────────────────────────────┐  │
# MAGIC │  │  Component 2: Unity Catalog Volume (Staging)     │  │
# MAGIC │  │  - Stores captured changes in Parquet format     │  │
# MAGIC │  │  - Organized by table and timestamp              │  │
# MAGIC │  │  - Retains metadata (operation type, LSN)        │  │
# MAGIC │  └──────────────────────────────────────────────────┘  │
# MAGIC │                          │                               │
# MAGIC │                          │ ③ Apply Changes               │
# MAGIC │                          ▼                               │
# MAGIC │  ┌──────────────────────────────────────────────────┐  │
# MAGIC │  │  Component 3: Ingestion Pipeline (DLT)           │  │
# MAGIC │  │  - Delta Live Tables streaming job               │  │
# MAGIC │  │  - Applies UPSERT/DELETE operations              │  │
# MAGIC │  │  - Handles schema evolution                      │  │
# MAGIC │  └──────────────────────────────────────────────────┘  │
# MAGIC │                          │                               │
# MAGIC │                          │ ④ Write to Bronze             │
# MAGIC │                          ▼                               │
# MAGIC │  ┌──────────────────────────────────────────────────┐  │
# MAGIC │  │  Bronze Layer (Delta Tables)                     │  │
# MAGIC │  │  ├─ bronze.customers  (Streaming Table)          │  │
# MAGIC │  │  ├─ bronze.orders     (Streaming Table)          │  │
# MAGIC │  │  └─ bronze.products   (Streaming Table)          │  │
# MAGIC │  └──────────────────────────────────────────────────┘  │
# MAGIC │                                                           │
# MAGIC │  Unity Catalog: Governance, Lineage, Access Control     │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Component 1: Ingestion Gateway
# MAGIC
# MAGIC ### What is the Ingestion Gateway?
# MAGIC
# MAGIC The **Ingestion Gateway** is a dedicated compute VM managed by Databricks that:
# MAGIC - Connects to your SQL Server database
# MAGIC - Reads transaction log records continuously
# MAGIC - Extracts change data (inserts, updates, deletes)
# MAGIC - Writes captured changes to Unity Catalog volumes
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC **Connectivity:**
# MAGIC - Supports SQL Server on-premises, AWS RDS, Azure SQL
# MAGIC - Works with VPN, PrivateLink, or public endpoints
# MAGIC - Handles authentication (SQL auth, Windows auth, Azure AD)
# MAGIC
# MAGIC **Change Tracking:**
# MAGIC - Maintains checkpoint position (LSN) in transaction log
# MAGIC - Resumes from last position after restarts
# MAGIC - Handles log rotation and cleanup gracefully
# MAGIC
# MAGIC **Performance:**
# MAGIC - Minimal impact on source database (read-only)
# MAGIC - Configurable batch sizes for change capture
# MAGIC - Automatic backpressure handling
# MAGIC
# MAGIC ### Gateway Configuration
# MAGIC
# MAGIC When creating a gateway, you specify:
# MAGIC - **Name**: Identifier for the gateway (e.g., `retail_ingestion_gateway`)
# MAGIC - **Staging Location**: Unity Catalog schema for volumes (e.g., `retail_analytics.landing`)
# MAGIC - **VM Size**: Compute capacity based on change volume
# MAGIC - **Connection**: Unity Catalog connection with SQL Server credentials

# COMMAND ----------

# MAGIC %md
# MAGIC ## Component 2: Unity Catalog Volume (Staging)
# MAGIC
# MAGIC ### What is a Unity Catalog Volume?
# MAGIC
# MAGIC A **Volume** is a managed storage location in Unity Catalog that stores files:
# MAGIC - Similar to a cloud storage bucket but governed by Unity Catalog
# MAGIC - Supports structured files (Parquet, Delta) and unstructured files
# MAGIC - Provides fine-grained access control
# MAGIC
# MAGIC ### CDC Staging Volume
# MAGIC
# MAGIC Lakeflow Connect creates a volume to stage captured changes:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/retail_analytics/landing/ingestion_volume/
# MAGIC   ├─ customers/
# MAGIC   │   ├─ changes_2024-01-15_10-30-00.parquet
# MAGIC   │   ├─ changes_2024-01-15_10-35-00.parquet
# MAGIC   │   └─ ...
# MAGIC   ├─ orders/
# MAGIC   │   ├─ changes_2024-01-15_10-30-00.parquet
# MAGIC   │   └─ ...
# MAGIC   └─ products/
# MAGIC       └─ changes_2024-01-15_10-30-00.parquet
# MAGIC ```
# MAGIC
# MAGIC ### Staged Change Format
# MAGIC
# MAGIC Each Parquet file contains CDC records with metadata:
# MAGIC
# MAGIC | Column | Description | Example |
# MAGIC |--------|-------------|---------|
# MAGIC | `_change_type` | Operation type | INSERT, UPDATE, DELETE |
# MAGIC | `_commit_timestamp` | When change occurred | 2024-01-15 10:32:15 |
# MAGIC | `_sequence_number` | LSN from transaction log | 00000027:000003e8:0001 |
# MAGIC | `customer_id` | Business columns... | 10001 |
# MAGIC | `first_name` | Business columns... | Jane |
# MAGIC | ... | ... | ... |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Component 3: Ingestion Pipeline (Delta Live Tables)
# MAGIC
# MAGIC ### What is the Ingestion Pipeline?
# MAGIC
# MAGIC The **Ingestion Pipeline** is a Delta Live Tables (DLT) job that:
# MAGIC - Reads staged changes from Unity Catalog volumes
# MAGIC - Applies UPSERT and DELETE operations to bronze tables
# MAGIC - Handles schema evolution automatically
# MAGIC - Maintains data quality and consistency
# MAGIC
# MAGIC ### Pipeline Operations
# MAGIC
# MAGIC **INSERT Processing:**
# MAGIC ```python
# MAGIC # New customer record from staging volume
# MAGIC INSERT INTO bronze.customers (customer_id, first_name, last_name, ...)
# MAGIC VALUES (10001, 'Jane', 'Smith', ...);
# MAGIC ```
# MAGIC
# MAGIC **UPDATE Processing (UPSERT):**
# MAGIC ```python
# MAGIC # Merge operation - update if exists, insert if new
# MAGIC MERGE INTO bronze.customers AS target
# MAGIC USING staged_changes AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC
# MAGIC **DELETE Processing:**
# MAGIC ```python
# MAGIC # Remove deleted records
# MAGIC DELETE FROM bronze.customers
# MAGIC WHERE customer_id IN (SELECT customer_id FROM staged_deletes);
# MAGIC ```
# MAGIC
# MAGIC ### Pipeline Scheduling
# MAGIC
# MAGIC **Triggered Mode:** Run on-demand for testing
# MAGIC **Continuous Mode:** Always running, processing changes as they arrive
# MAGIC **Scheduled Mode:** Run at regular intervals (e.g., every 5 minutes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC vs. Full Load: When to Use Each
# MAGIC
# MAGIC ### Use CDC When:
# MAGIC
# MAGIC ✅ **High Change Volume**: Millions of records, thousands of changes daily
# MAGIC - Example: E-commerce orders, financial transactions, IoT sensor data
# MAGIC
# MAGIC ✅ **Real-Time Requirements**: Need data freshness in seconds/minutes
# MAGIC - Example: Fraud detection, inventory alerts, real-time dashboards
# MAGIC
# MAGIC ✅ **Large Tables**: Multi-GB tables where full scans are expensive
# MAGIC - Example: Customer database with 50M+ records, 100K daily changes
# MAGIC
# MAGIC ✅ **Delete Tracking**: Must know when records are removed
# MAGIC - Example: Customer opt-outs, product discontinuations, order cancellations
# MAGIC
# MAGIC ✅ **Network Constraints**: Limited bandwidth between systems
# MAGIC - Example: On-premises to cloud with 100 Mbps connection
# MAGIC
# MAGIC ### Use Full Load When:
# MAGIC
# MAGIC ✅ **Small Tables**: <10K rows, full scan completes in seconds
# MAGIC - Example: Reference tables (countries, categories, product types)
# MAGIC
# MAGIC ✅ **Infrequent Updates**: Changes happen rarely (weekly/monthly)
# MAGIC - Example: Organizational hierarchy, configuration settings
# MAGIC
# MAGIC ✅ **Simple Requirements**: Don't need real-time or delete tracking
# MAGIC - Example: Daily batch reporting, monthly aggregations
# MAGIC
# MAGIC ✅ **CDC Not Supported**: Source system doesn't provide CDC capability
# MAGIC - Example: Legacy systems, flat files, third-party APIs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Use Cases for CDC with Lakeflow Connect
# MAGIC
# MAGIC ### 1. Real-Time Customer 360
# MAGIC
# MAGIC **Scenario:** Marketing team needs up-to-date customer profiles
# MAGIC
# MAGIC **Implementation:**
# MAGIC - CDC from CRM database (customers, interactions, preferences)
# MAGIC - Near real-time sync to Databricks (5-minute latency)
# MAGIC - Power personalization engine and dashboards
# MAGIC
# MAGIC **Benefit:** Customer support sees recent purchases and interactions immediately
# MAGIC
# MAGIC ### 2. Order Fulfillment Analytics
# MAGIC
# MAGIC **Scenario:** Operations dashboard showing order pipeline status
# MAGIC
# MAGIC **Implementation:**
# MAGIC - CDC captures order status changes (placed → processing → shipped → delivered)
# MAGIC - Track fulfillment SLAs in real-time
# MAGIC - Alert on bottlenecks or delays
# MAGIC
# MAGIC **Benefit:** Proactive issue resolution, improved customer satisfaction
# MAGIC
# MAGIC ### 3. Inventory Management
# MAGIC
# MAGIC **Scenario:** E-commerce needs accurate stock levels across warehouses
# MAGIC
# MAGIC **Implementation:**
# MAGIC - CDC from inventory system (products, stock_quantity, locations)
# MAGIC - Real-time updates to data warehouse
# MAGIC - Trigger alerts when stock below threshold
# MAGIC
# MAGIC **Benefit:** Prevent overselling, optimize reordering
# MAGIC
# MAGIC ### 4. Fraud Detection
# MAGIC
# MAGIC **Scenario:** Monitor suspicious transaction patterns
# MAGIC
# MAGIC **Implementation:**
# MAGIC - CDC from transaction database (orders, payments, customer changes)
# MAGIC - Stream to fraud detection ML model
# MAGIC - Flag anomalies within seconds
# MAGIC
# MAGIC **Benefit:** Catch fraudulent activity before completion
# MAGIC
# MAGIC ### 5. Compliance and Audit Trails
# MAGIC
# MAGIC **Scenario:** Regulatory requirement to track all data changes
# MAGIC
# MAGIC **Implementation:**
# MAGIC - CDC captures every INSERT/UPDATE/DELETE with timestamp
# MAGIC - Immutable audit log in Delta Lake
# MAGIC - Query historical changes for compliance reports
# MAGIC
# MAGIC **Benefit:** Meet GDPR, HIPAA, SOX requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benefits of Lakeflow Connect vs. Traditional CDC Tools
# MAGIC
# MAGIC ### Traditional CDC Stack
# MAGIC
# MAGIC ```
# MAGIC SQL Server → Debezium → Kafka → Kafka Connect → S3 → Databricks
# MAGIC ```
# MAGIC
# MAGIC **Challenges:**
# MAGIC - ❌ **Complexity**: 5+ components to configure and manage
# MAGIC - ❌ **Operational Overhead**: Kafka clusters, schema registry, connector monitoring
# MAGIC - ❌ **Cost**: Additional infrastructure for message queues
# MAGIC - ❌ **Expertise Required**: Need Kafka/Debezium specialists
# MAGIC - ❌ **Latency**: Multiple hops increase end-to-end time
# MAGIC
# MAGIC ### Lakeflow Connect Approach
# MAGIC
# MAGIC ```
# MAGIC SQL Server → Lakeflow Connect → Databricks Delta Tables
# MAGIC ```
# MAGIC
# MAGIC **Advantages:**
# MAGIC - ✅ **Simplicity**: Native Databricks integration, 3 components
# MAGIC - ✅ **Managed Service**: Databricks handles infrastructure
# MAGIC - ✅ **Cost-Effective**: No separate Kafka clusters
# MAGIC - ✅ **Unity Catalog Integration**: Governance from source to target
# MAGIC - ✅ **Low Latency**: Direct path reduces overhead
# MAGIC - ✅ **Schema Evolution**: Automatic handling of DDL changes
# MAGIC - ✅ **Monitoring**: Built-in observability and lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Terminology Reference
# MAGIC
# MAGIC ### CDC Terms
# MAGIC
# MAGIC | Term | Definition |
# MAGIC |------|------------|
# MAGIC | **Change Data Capture (CDC)** | Method of identifying and capturing changes in a database |
# MAGIC | **Transaction Log** | Sequential record of all database transactions |
# MAGIC | **Log Sequence Number (LSN)** | Unique identifier for position in transaction log |
# MAGIC | **Checkpoint** | Saved position in transaction log for resuming CDC |
# MAGIC | **Operation Type** | Type of change: INSERT (1), UPDATE (2-4), DELETE (1) |
# MAGIC
# MAGIC ### Lakeflow Connect Terms
# MAGIC
# MAGIC | Term | Definition |
# MAGIC |------|------------|
# MAGIC | **Ingestion Gateway** | VM that reads source database transaction logs |
# MAGIC | **Unity Catalog Volume** | Staging storage for captured changes |
# MAGIC | **Ingestion Pipeline** | Delta Live Tables job that applies changes |
# MAGIC | **Bronze Layer** | Landing zone for ingested data in Delta format |
# MAGIC | **Snapshot Mode** | Initial full load of table data |
# MAGIC | **Incremental Mode** | Ongoing capture of only changed records |
# MAGIC
# MAGIC ### SQL Server CDC Terms
# MAGIC
# MAGIC | Term | Definition |
# MAGIC |------|------------|
# MAGIC | **Change Tracking** | Lightweight CDC mechanism (tracks that changes occurred) |
# MAGIC | **CDC Tables** | Shadow tables storing before/after values (e.g., cdc.dbo_customers_CT) |
# MAGIC | **sys.sp_cdc_enable_db** | Stored procedure to enable CDC at database level |
# MAGIC | **sys.sp_cdc_enable_table** | Stored procedure to enable CDC on specific table |

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You Learned
# MAGIC
# MAGIC In this lecture, you learned:
# MAGIC
# MAGIC ✅ **CDC Fundamentals**: How CDC captures only changed records (INSERT/UPDATE/DELETE) instead of full table scans
# MAGIC
# MAGIC ✅ **SQL Server CDC**: How transaction logs enable change tracking and CDC configuration
# MAGIC
# MAGIC ✅ **Lakeflow Connect Architecture**: Three core components:
# MAGIC   - Ingestion Gateway (reads transaction logs)
# MAGIC   - Unity Catalog Volume (stages changes)
# MAGIC   - Ingestion Pipeline (applies changes to Delta tables)
# MAGIC
# MAGIC ✅ **Use Cases**: Real-time customer 360, order analytics, inventory management, fraud detection
# MAGIC
# MAGIC ✅ **Benefits**: Simplicity, managed service, Unity Catalog integration, low latency
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC In **Lab 2: Configuring SQL Server for CDC Operations**, you'll:
# MAGIC - Enable CDC on a SQL Server database
# MAGIC - Configure change tracking on tables
# MAGIC - Set up transaction log retention
# MAGIC - Verify CDC is working correctly
# MAGIC
# MAGIC **Ready to get hands-on? Let's configure SQL Server for CDC!** 🚀
