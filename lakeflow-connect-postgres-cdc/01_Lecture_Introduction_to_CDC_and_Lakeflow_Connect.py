# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Introduction to Change Data Capture and Lakeflow Connect Architecture
# MAGIC
# MAGIC ## Module Overview
# MAGIC This lecture introduces the fundamentals of Change Data Capture (CDC) and the Databricks Lakeflow Connect architecture for real-time data ingestion from PostgreSQL databases.
# MAGIC
# MAGIC **Duration:** 20 minutes
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Understand the limitations of traditional full-load ETL patterns
# MAGIC - Learn Change Data Capture concepts and benefits
# MAGIC - Explore PostgreSQL Write-Ahead Logging (WAL) and logical replication
# MAGIC - Understand Lakeflow Connect architecture and components
# MAGIC - Learn how CDC operations (INSERT/UPDATE/DELETE) flow from source to Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Traditional ETL vs. CDC
# MAGIC
# MAGIC ### Traditional Full-Load ETL Pattern
# MAGIC
# MAGIC **How it works:**
# MAGIC - Periodically (daily, hourly) read entire source tables
# MAGIC - Compare with previous snapshot to identify changes
# MAGIC - Write all data to target, replacing previous version
# MAGIC
# MAGIC **Problems:**
# MAGIC - **High latency**: Changes only visible after next scheduled load
# MAGIC - **Resource intensive**: Reads entire table even if only 1% changed
# MAGIC - **No delete detection**: Can't easily identify deleted records
# MAGIC - **Network overhead**: Transfers all data over network repeatedly
# MAGIC - **Source system impact**: Full table scans create database load
# MAGIC
# MAGIC ### Change Data Capture (CDC) Pattern
# MAGIC
# MAGIC **How it works:**
# MAGIC - Continuously monitors database transaction logs
# MAGIC - Captures only the changes (inserts, updates, deletes)
# MAGIC - Streams changes to target in near-real-time
# MAGIC
# MAGIC **Benefits:**
# MAGIC - **Low latency**: Changes visible within seconds
# MAGIC - **Efficient**: Only changed records are transferred
# MAGIC - **Complete accuracy**: Captures all operations including deletes
# MAGIC - **Minimal impact**: Reads log files, not production tables
# MAGIC - **Scalable**: Performance independent of table size

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: PostgreSQL Write-Ahead Logging (WAL)
# MAGIC
# MAGIC ### What is WAL?
# MAGIC
# MAGIC PostgreSQL uses **Write-Ahead Logging** for crash recovery and replication:
# MAGIC
# MAGIC 1. **Before** modifying data files, PostgreSQL writes changes to WAL
# MAGIC 2. WAL is an append-only log stored on disk (pg_wal directory)
# MAGIC 3. Every INSERT, UPDATE, DELETE is recorded in WAL
# MAGIC 4. WAL ensures durability and enables point-in-time recovery
# MAGIC
# MAGIC ### WAL Levels
# MAGIC
# MAGIC PostgreSQL supports three WAL levels:
# MAGIC
# MAGIC | WAL Level | Purpose | Use Case |
# MAGIC |-----------|---------|----------|
# MAGIC | **minimal** | Basic crash recovery only | Standalone databases |
# MAGIC | **replica** | Physical replication (byte-level copy) | Hot standby servers |
# MAGIC | **logical** | Logical replication (row-level changes) | CDC, selective replication |
# MAGIC
# MAGIC **For CDC, we need `wal_level = logical`**
# MAGIC
# MAGIC ### Logical Replication Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │         PostgreSQL Database                 │
# MAGIC │                                             │
# MAGIC │  Application → Transaction → WAL Buffer    │
# MAGIC │                                ↓            │
# MAGIC │                          WAL Files          │
# MAGIC │                          (pg_wal/)          │
# MAGIC │                                ↓            │
# MAGIC │                    Logical Decoding         │
# MAGIC │                    (pgoutput plugin)        │
# MAGIC │                                ↓            │
# MAGIC │                    Replication Slot         │
# MAGIC │                    (tracks position)        │
# MAGIC └─────────────────────────────────────────────┘
# MAGIC                      ↓
# MAGIC          Replication Protocol (TCP)
# MAGIC                      ↓
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │        CDC Consumer (Lakeflow Connect)      │
# MAGIC └─────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC **Publication**: Defines which tables to replicate
# MAGIC ```sql
# MAGIC CREATE PUBLICATION my_publication FOR TABLE customers, orders;
# MAGIC ```
# MAGIC
# MAGIC **Replication Slot**: Tracks consumer position in WAL
# MAGIC ```sql
# MAGIC SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
# MAGIC ```
# MAGIC
# MAGIC **Replica Identity**: Determines what data is logged for updates/deletes
# MAGIC - DEFAULT: Only primary key
# MAGIC - FULL: All columns (required for CDC)
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE customers REPLICA IDENTITY FULL;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Lakeflow Connect Architecture
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Databricks **Lakeflow Connect** is a managed CDC ingestion service that eliminates the need for external tools like Debezium, Kafka, or custom scripts.
# MAGIC
# MAGIC ### Three Core Components
# MAGIC
# MAGIC #### 1. Ingestion Gateway
# MAGIC - **What**: Dedicated compute VM managed by Databricks
# MAGIC - **Purpose**: Connects to PostgreSQL replication slot and reads WAL changes
# MAGIC - **How it works**:
# MAGIC   - Establishes replication connection to PostgreSQL
# MAGIC   - Subscribes to publication via replication slot
# MAGIC   - Continuously reads change events from WAL stream
# MAGIC   - Converts binary WAL format to structured format
# MAGIC   - Writes staged changes to Unity Catalog Volume
# MAGIC - **Configuration**: VM size, security settings, retry policies
# MAGIC
# MAGIC #### 2. Unity Catalog Volume (Staging Layer)
# MAGIC - **What**: Managed object storage location in Unity Catalog
# MAGIC - **Purpose**: Temporary staging area for captured changes
# MAGIC - **How it works**:
# MAGIC   - Stores change events as Parquet files
# MAGIC   - Organizes by table, operation type, timestamp
# MAGIC   - Provides checkpoint mechanism for exactly-once processing
# MAGIC   - Enables replay and auditing of changes
# MAGIC - **Lifecycle**: Files cleaned up after successful pipeline application
# MAGIC
# MAGIC #### 3. Ingestion Pipeline (Delta Live Tables)
# MAGIC - **What**: Delta Live Tables (DLT) pipeline that processes staged changes
# MAGIC - **Purpose**: Applies CDC operations to target Delta tables
# MAGIC - **How it works**:
# MAGIC   - Reads staged change files from volume
# MAGIC   - Applies MERGE operations for INSERT/UPDATE
# MAGIC   - Applies DELETE operations
# MAGIC   - Maintains data quality and consistency
# MAGIC   - Tracks lineage and metadata
# MAGIC - **Execution**: Scheduled, triggered, or continuous

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: End-to-End Data Flow
# MAGIC
# MAGIC ### Step-by-Step Flow for an UPDATE Operation
# MAGIC
# MAGIC **Example: Customer changes their address**
# MAGIC
# MAGIC ```sql
# MAGIC -- In PostgreSQL
# MAGIC UPDATE customers 
# MAGIC SET address = '456 New St', city = 'Portland' 
# MAGIC WHERE customer_id = 100;
# MAGIC ```
# MAGIC
# MAGIC #### Step 1: Transaction Execution (PostgreSQL)
# MAGIC - Application executes UPDATE statement
# MAGIC - PostgreSQL writes change to WAL before modifying data file
# MAGIC - Transaction commits
# MAGIC - WAL record includes:
# MAGIC   - Table OID (object identifier)
# MAGIC   - Operation type (UPDATE)
# MAGIC   - Old values (before update) - because REPLICA IDENTITY FULL
# MAGIC   - New values (after update)
# MAGIC   - Transaction ID and LSN (Log Sequence Number)
# MAGIC
# MAGIC #### Step 2: Change Capture (Ingestion Gateway)
# MAGIC - Gateway reads new WAL records via replication protocol
# MAGIC - Logical decoding plugin (pgoutput) converts binary WAL to logical change
# MAGIC - Gateway deserializes change event
# MAGIC - Gateway writes to staging volume:
# MAGIC   ```
# MAGIC   /Volumes/retail_analytics/landing/ingestion_volume/
# MAGIC     customers/
# MAGIC       2024-01-15-10-30-45.parquet
# MAGIC   ```
# MAGIC - Parquet file contains:
# MAGIC   - `_change_type`: "UPDATE"
# MAGIC   - `_commit_timestamp`: Transaction commit time
# MAGIC   - All customer columns with new values
# MAGIC
# MAGIC #### Step 3: Change Application (Ingestion Pipeline)
# MAGIC - DLT pipeline triggered (scheduled or continuous)
# MAGIC - Pipeline reads staged Parquet file
# MAGIC - Executes MERGE operation on bronze.customers:
# MAGIC   ```sql
# MAGIC   MERGE INTO bronze.customers target
# MAGIC   USING staged_changes source
# MAGIC   ON target.customer_id = source.customer_id
# MAGIC   WHEN MATCHED THEN UPDATE SET *
# MAGIC   ```
# MAGIC - Delta Lake records:
# MAGIC   - New version with updated values
# MAGIC   - Transaction log entry
# MAGIC   - Lineage metadata
# MAGIC
# MAGIC #### Step 4: Checkpoint and Cleanup
# MAGIC - Pipeline updates checkpoint (records LSN position)
# MAGIC - Gateway can safely advance replication slot
# MAGIC - Staged Parquet file can be deleted (after retention period)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: CDC Operations Deep Dive
# MAGIC
# MAGIC ### INSERT Operations
# MAGIC
# MAGIC **PostgreSQL:**
# MAGIC ```sql
# MAGIC INSERT INTO customers (first_name, last_name, email, city, state)
# MAGIC VALUES ('Jane', 'Smith', 'jane@email.com', 'Seattle', 'WA');
# MAGIC ```
# MAGIC
# MAGIC **Captured in WAL:**
# MAGIC - Operation: INSERT
# MAGIC - New row values (no old values needed)
# MAGIC - Generated primary key value
# MAGIC
# MAGIC **Applied in Delta:**
# MAGIC ```python
# MAGIC # DLT pipeline appends new record
# MAGIC new_record = {
# MAGIC     "customer_id": 1001,
# MAGIC     "first_name": "Jane",
# MAGIC     "last_name": "Smith",
# MAGIC     "_change_type": "INSERT",
# MAGIC     "_commit_timestamp": "2024-01-15 10:30:45"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### UPDATE Operations
# MAGIC
# MAGIC **PostgreSQL:**
# MAGIC ```sql
# MAGIC UPDATE orders 
# MAGIC SET order_status = 'SHIPPED', last_updated = NOW()
# MAGIC WHERE order_id = 5001;
# MAGIC ```
# MAGIC
# MAGIC **Captured in WAL (with REPLICA IDENTITY FULL):**
# MAGIC - Operation: UPDATE
# MAGIC - Old values: `{order_status: 'PROCESSING', last_updated: '2024-01-10'}`
# MAGIC - New values: `{order_status: 'SHIPPED', last_updated: '2024-01-15'}`
# MAGIC
# MAGIC **Applied in Delta:**
# MAGIC ```python
# MAGIC # MERGE operation updates existing record
# MAGIC # Only new values written, old values used for matching
# MAGIC ```
# MAGIC
# MAGIC ### DELETE Operations
# MAGIC
# MAGIC **PostgreSQL:**
# MAGIC ```sql
# MAGIC DELETE FROM customers WHERE customer_id = 999;
# MAGIC ```
# MAGIC
# MAGIC **Captured in WAL:**
# MAGIC - Operation: DELETE
# MAGIC - Old values (all columns) - because REPLICA IDENTITY FULL
# MAGIC - No new values
# MAGIC
# MAGIC **Applied in Delta:**
# MAGIC Two patterns supported:
# MAGIC
# MAGIC 1. **Hard Delete** (default):
# MAGIC    ```python
# MAGIC    # Record physically removed from table
# MAGIC    DELETE FROM bronze.customers WHERE customer_id = 999
# MAGIC    ```
# MAGIC
# MAGIC 2. **Soft Delete** (recommended for audit):
# MAGIC    ```python
# MAGIC    # Record marked as deleted, retained for history
# MAGIC    UPDATE bronze.customers 
# MAGIC    SET _is_deleted = TRUE, _delete_timestamp = NOW()
# MAGIC    WHERE customer_id = 999
# MAGIC    ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Key Advantages of Lakeflow Connect
# MAGIC
# MAGIC ### 1. Fully Managed Service
# MAGIC - No need to deploy/manage Debezium, Kafka, or custom scripts
# MAGIC - Databricks handles gateway provisioning, scaling, monitoring
# MAGIC - Automatic retry and error handling
# MAGIC - Built-in checkpoint management
# MAGIC
# MAGIC ### 2. Native Unity Catalog Integration
# MAGIC - Credentials securely stored in Unity Catalog connections
# MAGIC - Changes staged in governed Unity Catalog volumes
# MAGIC - Target tables automatically cataloged with lineage
# MAGIC - Access controls enforced throughout pipeline
# MAGIC
# MAGIC ### 3. Delta Lake Optimizations
# MAGIC - Efficient MERGE operations using Delta Lake
# MAGIC - Z-ordering and liquid clustering support
# MAGIC - Time travel for CDC replay
# MAGIC - VACUUM and optimization handled automatically
# MAGIC
# MAGIC ### 4. Operational Excellence
# MAGIC - Monitoring dashboards for pipeline health
# MAGIC - Alerting on ingestion failures or lag
# MAGIC - Lineage tracking from source to bronze
# MAGIC - Audit logs for compliance
# MAGIC
# MAGIC ### 5. Performance and Scale
# MAGIC - Parallel ingestion for multiple tables
# MAGIC - Configurable batch sizes and refresh intervals
# MAGIC - Support for high-volume transaction workloads
# MAGIC - Minimal impact on source PostgreSQL database

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Prerequisites and Requirements
# MAGIC
# MAGIC ### PostgreSQL Requirements
# MAGIC
# MAGIC **Version**: PostgreSQL 10 or later (10, 11, 12, 13, 14, 15, 16)
# MAGIC
# MAGIC **Configuration**:
# MAGIC - `wal_level = logical` (requires restart)
# MAGIC - `max_replication_slots >= 10` 
# MAGIC - `max_wal_senders >= 10`
# MAGIC - Sufficient WAL retention (`wal_keep_size` or `wal_keep_segments`)
# MAGIC
# MAGIC **Permissions**:
# MAGIC - REPLICATION privilege
# MAGIC - SELECT on all tables to replicate
# MAGIC - Ability to create replication slots and publications
# MAGIC
# MAGIC **Network**:
# MAGIC - Databricks must reach PostgreSQL (VPN, PrivateLink, public with security groups)
# MAGIC - Port 5432 (default) accessible
# MAGIC - SSL/TLS optional but recommended
# MAGIC
# MAGIC ### Databricks Requirements
# MAGIC
# MAGIC **Features**:
# MAGIC - Unity Catalog enabled
# MAGIC - Lakeflow Connect feature enabled (contact Databricks if not available)
# MAGIC
# MAGIC **Compute**:
# MAGIC - All-purpose cluster (DBR 14.3 LTS or later) for notebooks
# MAGIC - SQL Warehouse (2X-Small minimum) for queries
# MAGIC - Ingestion gateway VM (managed by Databricks)
# MAGIC
# MAGIC **Permissions**:
# MAGIC - Create catalogs, schemas, tables in Unity Catalog
# MAGIC - Create Unity Catalog connections
# MAGIC - Create Unity Catalog volumes
# MAGIC - Create and run Delta Live Tables pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Workshop Roadmap
# MAGIC
# MAGIC ### Phase 1: Foundation (60 minutes)
# MAGIC - **Lecture 1** (current): CDC concepts and architecture ✓
# MAGIC - **Lab 2**: Configure PostgreSQL for CDC operations
# MAGIC   - Enable logical replication
# MAGIC   - Set replica identity
# MAGIC   - Create publications and replication slots
# MAGIC
# MAGIC ### Phase 2: Pipeline Setup (45 minutes)
# MAGIC - **Lab 3**: Set up Lakeflow Connect
# MAGIC   - Create ingestion gateway
# MAGIC   - Configure Unity Catalog connection
# MAGIC   - Create ingestion pipeline
# MAGIC - **Lecture 4**: Snapshot vs. incremental modes
# MAGIC
# MAGIC ### Phase 3: Validation (30 minutes)
# MAGIC - **Lab 5**: Test CDC with CRUD operations
# MAGIC   - Perform INSERT, UPDATE, DELETE in PostgreSQL
# MAGIC   - Validate propagation to Delta tables
# MAGIC   - Verify latency and correctness
# MAGIC
# MAGIC ### Phase 4: Production Patterns (15 minutes)
# MAGIC - **Lab 6**: Multi-table CDC with dependencies
# MAGIC - **Lecture 7**: Production best practices
# MAGIC - **Lab 8**: Scheduling and error handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **CDC captures only changes** from transaction logs, not full table scans
# MAGIC 2. **PostgreSQL WAL** with logical replication provides change stream
# MAGIC 3. **Lakeflow Connect** has three components: Gateway, Volume, Pipeline
# MAGIC 4. **All CDC operations** (INSERT/UPDATE/DELETE) are captured and applied
# MAGIC 5. **Fully managed** by Databricks with Unity Catalog integration
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Proceed to **Lab 2: Configuring PostgreSQL for CDC Operations** where you'll:
# MAGIC - Connect to your PostgreSQL database
# MAGIC - Enable logical replication
# MAGIC - Create publications and replication slots
# MAGIC - Grant necessary permissions
# MAGIC - Verify CDC readiness
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Questions?** Review the troubleshooting guide in the workshop README or post in Databricks Community Forums.
