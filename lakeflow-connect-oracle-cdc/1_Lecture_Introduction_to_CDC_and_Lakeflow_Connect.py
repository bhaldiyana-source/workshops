# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture 1: Introduction to Change Data Capture and Lakeflow Connect Architecture
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will understand:
# MAGIC - What Change Data Capture (CDC) is and why it's critical for modern data architectures
# MAGIC - Traditional CDC approaches and their limitations
# MAGIC - Databricks Lakeflow Connect architecture and components
# MAGIC - How Oracle LogMiner enables CDC operations
# MAGIC - Key concepts: SCN (System Change Number), redo logs, supplemental logging
# MAGIC - When to use CDC vs. full-load patterns
# MAGIC
# MAGIC **Duration:** 30 minutes  
# MAGIC **Prerequisites:** Basic understanding of databases, ETL concepts, and data warehousing

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Change Data Capture (CDC)?
# MAGIC
# MAGIC **Change Data Capture (CDC)** is a design pattern that identifies and captures changes made to data in a database and delivers them to downstream systems in near real-time.
# MAGIC
# MAGIC ### Traditional ETL vs. CDC
# MAGIC
# MAGIC **Traditional Full-Load ETL:**
# MAGIC ```
# MAGIC Time: 00:00 → Extract all 10 million customer records
# MAGIC Time: 06:00 → Extract all 10 million customer records (again)
# MAGIC Time: 12:00 → Extract all 10 million customer records (again)
# MAGIC ```
# MAGIC - High resource consumption
# MAGIC - Increased network traffic
# MAGIC - Long processing windows
# MAGIC - Cannot detect deletes easily
# MAGIC - Stale data between refresh cycles
# MAGIC
# MAGIC **CDC Incremental Pattern:**
# MAGIC ```
# MAGIC Time: 00:00 → Extract all 10 million customer records (initial load)
# MAGIC Time: 06:00 → Extract only 150 changed records (CDC)
# MAGIC Time: 12:00 → Extract only 200 changed records (CDC)
# MAGIC ```
# MAGIC - Minimal resource consumption
# MAGIC - Reduced network traffic
# MAGIC - Near real-time updates
# MAGIC - Captures deletes
# MAGIC - Continuous data freshness

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why CDC Matters for Your Organization
# MAGIC
# MAGIC ### Business Drivers
# MAGIC
# MAGIC | Use Case | Without CDC | With CDC |
# MAGIC |----------|-------------|----------|
# MAGIC | **Customer 360 View** | Updated daily at 2 AM | Updated within seconds of change |
# MAGIC | **Fraud Detection** | Batch analysis of yesterday's transactions | Real-time anomaly detection |
# MAGIC | **Inventory Management** | Stock levels refreshed hourly | Live stock synchronization |
# MAGIC | **Order Analytics** | Static reports from last night | Live order tracking dashboards |
# MAGIC | **Compliance Auditing** | Reconstruct history from snapshots | Complete audit trail of all changes |
# MAGIC
# MAGIC ### Technical Benefits
# MAGIC
# MAGIC 1. **Reduced Load on Source Systems**
# MAGIC    - No need to scan entire tables repeatedly
# MAGIC    - Leverages database transaction logs
# MAGIC    - Minimal impact on operational workloads
# MAGIC
# MAGIC 2. **Lower Data Transfer Costs**
# MAGIC    - Transfer only changed records
# MAGIC    - 100:1 or better reduction in data volume
# MAGIC    - Critical for cloud data transfer pricing
# MAGIC
# MAGIC 3. **Enable Real-Time Analytics**
# MAGIC    - Sub-minute data latency possible
# MAGIC    - Support streaming dashboards
# MAGIC    - Power operational intelligence
# MAGIC
# MAGIC 4. **Capture Complete History**
# MAGIC    - Track all inserts, updates, deletes
# MAGIC    - Enable slowly changing dimensions (SCD Type 2)
# MAGIC    - Maintain full audit trails

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Implementation Approaches
# MAGIC
# MAGIC ### Approach 1: Timestamp-Based CDC (Pseudo-CDC)
# MAGIC ```sql
# MAGIC SELECT * FROM CUSTOMERS 
# MAGIC WHERE LAST_UPDATED > :last_checkpoint
# MAGIC ```
# MAGIC
# MAGIC **Pros:**
# MAGIC - Simple to implement
# MAGIC - No database configuration required
# MAGIC - Works with any database
# MAGIC
# MAGIC **Cons:**
# MAGIC - ❌ Cannot detect deletes
# MAGIC - ❌ Requires LAST_UPDATED column on every table
# MAGIC - ❌ Not reliable if timestamps are backdated
# MAGIC - ❌ Misses changes if multiple updates occur between polls
# MAGIC
# MAGIC ### Approach 2: Trigger-Based CDC
# MAGIC ```sql
# MAGIC CREATE TRIGGER customer_audit_trigger
# MAGIC AFTER INSERT OR UPDATE OR DELETE ON CUSTOMERS
# MAGIC FOR EACH ROW
# MAGIC BEGIN
# MAGIC   INSERT INTO CUSTOMERS_AUDIT ...
# MAGIC END;
# MAGIC ```
# MAGIC
# MAGIC **Pros:**
# MAGIC - Captures all operations (INSERT/UPDATE/DELETE)
# MAGIC - Application-transparent
# MAGIC - Can add custom logic
# MAGIC
# MAGIC **Cons:**
# MAGIC - ❌ High overhead on source database
# MAGIC - ❌ Requires triggers on every table
# MAGIC - ❌ Additional storage for audit tables
# MAGIC - ❌ Can slow down transactions
# MAGIC
# MAGIC ### Approach 3: Log-Based CDC (Gold Standard) ✅
# MAGIC
# MAGIC **How it works:**
# MAGIC - Read database transaction logs (redo logs in Oracle)
# MAGIC - Parse change events from log files
# MAGIC - Deliver changes to target systems
# MAGIC
# MAGIC **Pros:**
# MAGIC - ✅ Zero impact on source database performance
# MAGIC - ✅ Captures all operations including deletes
# MAGIC - ✅ No schema changes required
# MAGIC - ✅ Provides complete audit trail
# MAGIC - ✅ Enables real-time replication
# MAGIC - ✅ Minimal latency (seconds)
# MAGIC
# MAGIC **Cons:**
# MAGIC - Requires database configuration (supplemental logging)
# MAGIC - More complex setup initially
# MAGIC - **Solved by Databricks Lakeflow Connect!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Oracle CDC with LogMiner
# MAGIC
# MAGIC ### What is LogMiner?
# MAGIC
# MAGIC **LogMiner** is Oracle's built-in utility that allows you to query and analyze transaction logs (redo logs and archive logs). It's the foundation for log-based CDC from Oracle databases.
# MAGIC
# MAGIC ### How Oracle Captures Changes
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────┐
# MAGIC │              Oracle Database Instance                 │
# MAGIC │                                                        │
# MAGIC │  Application executes:                                │
# MAGIC │  UPDATE CUSTOMERS SET EMAIL='new@email.com'          │
# MAGIC │  WHERE CUSTOMER_ID = 12345;                          │
# MAGIC │  COMMIT;                                              │
# MAGIC │                                                        │
# MAGIC │  ┌────────────────────────────────────────┐          │
# MAGIC │  │        Redo Log Buffer                  │          │
# MAGIC │  │  Stores: Before/After values, SCN,     │          │
# MAGIC │  │  Operation type, Transaction ID         │          │
# MAGIC │  └────────────────────────────────────────┘          │
# MAGIC │                    ↓                                   │
# MAGIC │  ┌────────────────────────────────────────┐          │
# MAGIC │  │     Online Redo Logs (Circular)        │          │
# MAGIC │  │  Group 1: redo01a.log, redo01b.log     │          │
# MAGIC │  │  Group 2: redo02a.log, redo02b.log     │          │
# MAGIC │  │  Group 3: redo03a.log, redo03b.log     │          │
# MAGIC │  └────────────────────────────────────────┘          │
# MAGIC │                    ↓                                   │
# MAGIC │  ┌────────────────────────────────────────┐          │
# MAGIC │  │     Archive Logs (Persistent)          │          │
# MAGIC │  │  /archive/arch_20260110_001.log        │          │
# MAGIC │  │  /archive/arch_20260110_002.log        │          │
# MAGIC │  │  /archive/arch_20260110_003.log        │          │
# MAGIC │  └────────────────────────────────────────┘          │
# MAGIC │                                                        │
# MAGIC │  ┌────────────────────────────────────────┐          │
# MAGIC │  │          LogMiner Process               │          │
# MAGIC │  │  Parses logs and extracts:              │          │
# MAGIC │  │  - Operation: UPDATE                    │          │
# MAGIC │  │  - Table: CUSTOMERS                     │          │
# MAGIC │  │  - Old: email='old@email.com'          │          │
# MAGIC │  │  - New: email='new@email.com'          │          │
# MAGIC │  │  - SCN: 8745932                         │          │
# MAGIC │  └────────────────────────────────────────┘          │
# MAGIC └──────────────────────────────────────────────────────┘
# MAGIC                         ↓
# MAGIC              Lakeflow Connect reads
# MAGIC              parsed change events
# MAGIC ```
# MAGIC
# MAGIC ### System Change Number (SCN)
# MAGIC
# MAGIC **SCN** is Oracle's internal timestamp that orders all transactions. Each committed transaction gets a unique SCN.
# MAGIC
# MAGIC ```sql
# MAGIC -- Get current SCN
# MAGIC SELECT CURRENT_SCN FROM V$DATABASE;
# MAGIC -- Example output: 8745932
# MAGIC
# MAGIC -- SCN enables incremental CDC:
# MAGIC -- Read all changes WHERE SCN > last_processed_scn
# MAGIC ```
# MAGIC
# MAGIC ### Supplemental Logging
# MAGIC
# MAGIC By default, Oracle redo logs contain minimal information to recover the database. For CDC, we need **supplemental logging** to capture complete before/after values.
# MAGIC
# MAGIC **Without Supplemental Logging:**
# MAGIC ```
# MAGIC UPDATE CUSTOMERS SET EMAIL='new@email.com' WHERE CUSTOMER_ID=12345;
# MAGIC
# MAGIC Redo Log Entry:
# MAGIC - ROWID: AAASs7AAEAAAAAfAAB
# MAGIC - New EMAIL value: 'new@email.com'
# MAGIC (Missing: Old email value, other column values)
# MAGIC ```
# MAGIC
# MAGIC **With Supplemental Logging:**
# MAGIC ```
# MAGIC UPDATE CUSTOMERS SET EMAIL='new@email.com' WHERE CUSTOMER_ID=12345;
# MAGIC
# MAGIC Redo Log Entry:
# MAGIC - CUSTOMER_ID: 12345 (primary key)
# MAGIC - Old EMAIL: 'old@email.com'
# MAGIC - New EMAIL: 'new@email.com'
# MAGIC - FIRST_NAME: 'John' (unchanged)
# MAGIC - LAST_NAME: 'Doe' (unchanged)
# MAGIC - All other columns...
# MAGIC ```
# MAGIC
# MAGIC This complete information allows CDC systems to reconstruct the full row state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Lakeflow Connect Architecture
# MAGIC
# MAGIC ### What is Lakeflow Connect?
# MAGIC
# MAGIC **Databricks Lakeflow Connect** is a fully managed service for ingesting data from external databases into Unity Catalog. It eliminates the need to manage separate CDC tools like Debezium, Kafka, or custom ETL scripts.
# MAGIC
# MAGIC ### Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    ORACLE SOURCE DATABASE                        │
# MAGIC │  ┌──────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Retail Database Tables:                                  │  │
# MAGIC │  │  - CUSTOMERS  (10M rows, ~500 changes/hour)              │  │
# MAGIC │  │  - ORDERS     (50M rows, ~2000 changes/hour)             │  │
# MAGIC │  │  - PRODUCTS   (100K rows, ~50 changes/hour)              │  │
# MAGIC │  │                                                            │  │
# MAGIC │  │  Redo Logs → Archive Logs → LogMiner                     │  │
# MAGIC │  └──────────────────────────────────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC                               ⇅ JDBC Connection
# MAGIC                               ⇅ Reads redo log changes
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │              DATABRICKS LAKEHOUSE PLATFORM                       │
# MAGIC │                                                                   │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  COMPONENT 1: LAKEFLOW CONNECT INGESTION GATEWAY        │   │
# MAGIC │  │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │   │
# MAGIC │  │  - Dedicated VM managed by Databricks                    │   │
# MAGIC │  │  - Connects to Oracle via JDBC                           │   │
# MAGIC │  │  - Invokes LogMiner to read redo logs                    │   │
# MAGIC │  │  - Parses change events (INSERT/UPDATE/DELETE)           │   │
# MAGIC │  │  - Tracks SCN checkpoint for incremental processing      │   │
# MAGIC │  │  - Converts changes to Parquet format                    │   │
# MAGIC │  │  - Writes to Unity Catalog volume                        │   │
# MAGIC │  │                                                            │   │
# MAGIC │  │  Gateway Stages:                                          │   │
# MAGIC │  │  1. Initial Snapshot → Full table extract via SELECT     │   │
# MAGIC │  │  2. Incremental CDC → Read changes WHERE SCN > checkpoint│   │
# MAGIC │  └─────────────────────────────────────────────────────────┘   │
# MAGIC │                               ↓                                  │
# MAGIC │                    Writes Parquet files                         │
# MAGIC │                               ↓                                  │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  COMPONENT 2: UNITY CATALOG VOLUME (STAGING)            │   │
# MAGIC │  │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │   │
# MAGIC │  │  Path: /Volumes/retail_analytics/landing/cdc_volume/    │   │
# MAGIC │  │                                                            │   │
# MAGIC │  │  Structure:                                               │   │
# MAGIC │  │  customers/                                               │   │
# MAGIC │  │    snapshot_20260110_080000.parquet                      │   │
# MAGIC │  │    change_20260110_080500.parquet (100 INSERT ops)       │   │
# MAGIC │  │    change_20260110_081000.parquet (50 UPDATE ops)        │   │
# MAGIC │  │  orders/                                                  │   │
# MAGIC │  │    snapshot_20260110_080000.parquet                      │   │
# MAGIC │  │    change_20260110_080500.parquet (500 INSERT ops)       │   │
# MAGIC │  │  products/                                                │   │
# MAGIC │  │    snapshot_20260110_080000.parquet                      │   │
# MAGIC │  │    change_20260110_080500.parquet (10 UPDATE ops)        │   │
# MAGIC │  └─────────────────────────────────────────────────────────┘   │
# MAGIC │                               ↓                                  │
# MAGIC │                    Reads staged changes                         │
# MAGIC │                               ↓                                  │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  COMPONENT 3: LAKEFLOW CONNECT INGESTION PIPELINE       │   │
# MAGIC │  │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │   │
# MAGIC │  │  - Powered by Delta Live Tables (DLT)                    │   │
# MAGIC │  │  - Reads Parquet from volume (streaming source)          │   │
# MAGIC │  │  - Applies CDC logic:                                     │   │
# MAGIC │  │    * INSERT → Add new rows                               │   │
# MAGIC │  │    * UPDATE → Merge changes using primary key            │   │
# MAGIC │  │    * DELETE → Remove rows or add tombstone               │   │
# MAGIC │  │  - Maintains Delta table consistency                     │   │
# MAGIC │  │  - Handles late-arriving data                            │   │
# MAGIC │  │  - Auto-recovers from failures                           │   │
# MAGIC │  └─────────────────────────────────────────────────────────┘   │
# MAGIC │                               ↓                                  │
# MAGIC │                    Writes to Delta tables                       │
# MAGIC │                               ↓                                  │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  COMPONENT 4: BRONZE LAYER DELTA TABLES                 │   │
# MAGIC │  │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │   │
# MAGIC │  │  Catalog: retail_analytics                               │   │
# MAGIC │  │  Schema: bronze                                           │   │
# MAGIC │  │                                                            │   │
# MAGIC │  │  Tables:                                                  │   │
# MAGIC │  │  - bronze.customers  (Streaming Table, ACID compliant)   │   │
# MAGIC │  │  - bronze.orders     (Streaming Table, ACID compliant)   │   │
# MAGIC │  │  - bronze.products   (Streaming Table, ACID compliant)   │   │
# MAGIC │  │                                                            │   │
# MAGIC │  │  Metadata columns added:                                  │   │
# MAGIC │  │  - _commit_timestamp: When change was committed          │   │
# MAGIC │  │  - _rescued_data: Handles schema evolution               │   │
# MAGIC │  └─────────────────────────────────────────────────────────┘   │
# MAGIC │                                                                   │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐   │
# MAGIC │  │  COMPONENT 5: UNITY CATALOG CONNECTION                  │   │
# MAGIC │  │  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │   │
# MAGIC │  │  - Stores Oracle credentials securely                    │   │
# MAGIC │  │  - Configuration:                                         │   │
# MAGIC │  │    * Host: oracle.example.com                            │   │
# MAGIC │  │    * Port: 1521                                           │   │
# MAGIC │  │    * Service Name: RETAILDB                              │   │
# MAGIC │  │    * Username: cdc_user (with LogMiner grants)           │   │
# MAGIC │  │    * Password: (encrypted in Unity Catalog)              │   │
# MAGIC │  │  - Governs access via Unity Catalog permissions          │   │
# MAGIC │  └─────────────────────────────────────────────────────────┘   │
# MAGIC │                                                                   │
# MAGIC │  Unity Catalog: Lineage, Governance, Access Control             │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Component Deep Dive
# MAGIC
# MAGIC ### 1. Ingestion Gateway
# MAGIC
# MAGIC **Purpose:** Connect to source databases and extract change data
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Fully managed by Databricks (no VM maintenance)
# MAGIC - Scales automatically based on change volume
# MAGIC - Supports multiple source connections
# MAGIC - Handles network failures and reconnections
# MAGIC - Tracks checkpoint (SCN) for exactly-once semantics
# MAGIC
# MAGIC **Configuration:**
# MAGIC ```python
# MAGIC # Gateway creation (via UI or API)
# MAGIC gateway_name = "retail_ingestion_gateway"
# MAGIC connection_name = "oracle_retail_connection"  # Unity Catalog connection
# MAGIC staging_location = "/Volumes/retail_analytics/landing/cdc_volume"
# MAGIC ```
# MAGIC
# MAGIC **What happens when gateway starts:**
# MAGIC 1. Provisions dedicated compute VM
# MAGIC 2. Connects to Oracle using Unity Catalog connection
# MAGIC 3. Performs initial snapshot OR resumes from last checkpoint
# MAGIC 4. Begins continuous log reading
# MAGIC 5. Converts changes to Parquet and writes to volume
# MAGIC
# MAGIC ### 2. Unity Catalog Volume (Staging)
# MAGIC
# MAGIC **Purpose:** Temporary staging for change data before applying to Delta tables
# MAGIC
# MAGIC **Why use a volume?**
# MAGIC - Decouples ingestion from processing
# MAGIC - Allows reprocessing if pipeline fails
# MAGIC - Provides audit trail of raw changes
# MAGIC - Enables multiple pipelines to read same source
# MAGIC
# MAGIC **Volume Structure:**
# MAGIC ```
# MAGIC /Volumes/retail_analytics/landing/cdc_volume/
# MAGIC   customers/
# MAGIC     _metadata/
# MAGIC       checkpoint.json  # Tracks last processed SCN
# MAGIC     snapshot/
# MAGIC       part-00000.parquet  # Initial full load
# MAGIC       part-00001.parquet
# MAGIC     incremental/
# MAGIC       20260110-080000/
# MAGIC         part-00000.parquet  # Changes from this batch
# MAGIC       20260110-081000/
# MAGIC         part-00000.parquet
# MAGIC ```
# MAGIC
# MAGIC **Change record format:**
# MAGIC ```json
# MAGIC {
# MAGIC   "_change_type": "UPDATE",
# MAGIC   "_commit_scn": 8745932,
# MAGIC   "_commit_timestamp": "2026-01-10T08:05:23Z",
# MAGIC   "CUSTOMER_ID": 12345,
# MAGIC   "FIRST_NAME": "John",
# MAGIC   "LAST_NAME": "Doe",
# MAGIC   "EMAIL": "newemail@example.com",
# MAGIC   "_before_EMAIL": "oldemail@example.com"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 3. Ingestion Pipeline (Delta Live Tables)
# MAGIC
# MAGIC **Purpose:** Apply staged changes to Delta tables with ACID guarantees
# MAGIC
# MAGIC **CDC Logic:**
# MAGIC ```python
# MAGIC # Simplified CDC application logic
# MAGIC @dlt.table(name="customers")
# MAGIC def customers_bronze():
# MAGIC     return (
# MAGIC         dlt.read_stream("/Volumes/retail_analytics/landing/cdc_volume/customers")
# MAGIC         .apply_changes(
# MAGIC             keys=["CUSTOMER_ID"],
# MAGIC             sequence_by="_commit_scn",
# MAGIC             ignore_null_updates=False,
# MAGIC             apply_as_deletes=expr("_change_type = 'DELETE'"),
# MAGIC             except_column_list=["_change_type", "_commit_scn"]
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **How it handles operations:**
# MAGIC - **INSERT**: Add new row to Delta table
# MAGIC - **UPDATE**: Merge based on primary key, keep latest by SCN
# MAGIC - **DELETE**: Remove row or mark as deleted
# MAGIC - **Late arrivals**: SCN ordering ensures correct final state
# MAGIC
# MAGIC ### 4. Unity Catalog Integration
# MAGIC
# MAGIC **Connections:**
# MAGIC - Store credentials securely
# MAGIC - Support rotation without pipeline changes
# MAGIC - Control access via GRANT statements
# MAGIC
# MAGIC **Lineage:**
# MAGIC ```
# MAGIC Oracle CUSTOMERS table
# MAGIC    ↓
# MAGIC Ingestion Gateway (oracle_retail_connection)
# MAGIC    ↓
# MAGIC Volume: /Volumes/retail_analytics/landing/cdc_volume/customers
# MAGIC    ↓
# MAGIC Pipeline: retail_ingestion_pipeline
# MAGIC    ↓
# MAGIC Table: retail_analytics.bronze.customers
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Data Flow Example
# MAGIC
# MAGIC Let's trace a single customer update through the entire system:
# MAGIC
# MAGIC ### Step 1: Transaction in Oracle
# MAGIC ```sql
# MAGIC -- Time: 08:05:23 AM, SCN: 8745932
# MAGIC UPDATE CUSTOMERS 
# MAGIC SET EMAIL = 'john.doe.new@email.com',
# MAGIC     LAST_UPDATED = SYSTIMESTAMP
# MAGIC WHERE CUSTOMER_ID = 12345;
# MAGIC COMMIT;
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Oracle Writes to Redo Log
# MAGIC ```
# MAGIC Redo Log Entry:
# MAGIC ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC SCN:              8745932
# MAGIC Timestamp:        2026-01-10 08:05:23.456789
# MAGIC Operation:        UPDATE
# MAGIC Schema:           RETAIL
# MAGIC Table:            CUSTOMERS
# MAGIC Transaction ID:   0x000700.015.00000ab3
# MAGIC
# MAGIC Columns Changed:
# MAGIC - CUSTOMER_ID:    12345 (key column)
# MAGIC - EMAIL:          'john.doe.new@email.com' (NEW)
# MAGIC                   'john.doe@email.com' (OLD)
# MAGIC - LAST_UPDATED:   2026-01-10 08:05:23.456789 (NEW)
# MAGIC                   2026-01-09 14:23:11.123456 (OLD)
# MAGIC ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC ```
# MAGIC
# MAGIC ### Step 3: Ingestion Gateway Reads via LogMiner
# MAGIC ```
# MAGIC [08:05:25] Gateway: Querying LogMiner for changes since SCN 8745900
# MAGIC [08:05:25] Gateway: Found 1 change event (SCN 8745932)
# MAGIC [08:05:25] Gateway: Parsing UPDATE operation on CUSTOMERS
# MAGIC [08:05:25] Gateway: Converting to Parquet format
# MAGIC ```
# MAGIC
# MAGIC ### Step 4: Gateway Writes to Volume
# MAGIC ```
# MAGIC File: /Volumes/retail_analytics/landing/cdc_volume/customers/
# MAGIC       incremental/20260110-080500/part-00000.parquet
# MAGIC
# MAGIC Record:
# MAGIC {
# MAGIC   "_change_type": "UPDATE",
# MAGIC   "_commit_scn": 8745932,
# MAGIC   "_commit_timestamp": "2026-01-10T08:05:23.456789Z",
# MAGIC   "_transaction_id": "0x000700.015.00000ab3",
# MAGIC   "CUSTOMER_ID": 12345,
# MAGIC   "FIRST_NAME": "John",
# MAGIC   "LAST_NAME": "Doe",
# MAGIC   "EMAIL": "john.doe.new@email.com",
# MAGIC   "PHONE": "555-0123",
# MAGIC   "CITY": "Seattle",
# MAGIC   "STATE": "WA",
# MAGIC   "CREATED_DATE": "2024-03-15T10:30:00Z",
# MAGIC   "LAST_UPDATED": "2026-01-10T08:05:23.456789Z"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Step 5: Ingestion Pipeline Processes Change
# MAGIC ```
# MAGIC [08:05:27] Pipeline: Detected new file in volume
# MAGIC [08:05:27] Pipeline: Reading incremental batch (1 record)
# MAGIC [08:05:27] Pipeline: Applying UPDATE to bronze.customers
# MAGIC [08:05:27] Pipeline: Merging based on CUSTOMER_ID=12345
# MAGIC [08:05:27] Pipeline: Writing to Delta table
# MAGIC [08:05:28] Pipeline: Commit successful, version 247
# MAGIC ```
# MAGIC
# MAGIC ### Step 6: Updated Data in Bronze Table
# MAGIC ```sql
# MAGIC SELECT * FROM retail_analytics.bronze.customers 
# MAGIC WHERE CUSTOMER_ID = 12345;
# MAGIC
# MAGIC Result:
# MAGIC ┌─────────────┬────────────┬───────────┬─────────────────────────┬───────────┐
# MAGIC │ CUSTOMER_ID │ FIRST_NAME │ LAST_NAME │ EMAIL                   │ STATE     │
# MAGIC ├─────────────┼────────────┼───────────┼─────────────────────────┼───────────┤
# MAGIC │ 12345       │ John       │ Doe       │ john.doe.new@email.com  │ WA        │
# MAGIC └─────────────┴────────────┴───────────┴─────────────────────────┴───────────┘
# MAGIC ```
# MAGIC
# MAGIC **Total Latency: ~5 seconds** from commit in Oracle to availability in Databricks!

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Use CDC vs. Full Load
# MAGIC
# MAGIC ### Use CDC When:
# MAGIC ✅ **High Change Velocity**
# MAGIC - Tables with frequent updates (orders, inventory, transactions)
# MAGIC - Thousands of changes per hour
# MAGIC
# MAGIC ✅ **Large Tables**
# MAGIC - Multi-million or billion-row tables
# MAGIC - Where full scans are expensive
# MAGIC
# MAGIC ✅ **Low Latency Required**
# MAGIC - Real-time dashboards
# MAGIC - Operational intelligence
# MAGIC - Fraud detection
# MAGIC
# MAGIC ✅ **Delete Tracking Needed**
# MAGIC - Audit requirements
# MAGIC - Compliance needs
# MAGIC - Customer data removal (GDPR)
# MAGIC
# MAGIC ✅ **Historical Analysis**
# MAGIC - Track how data changes over time
# MAGIC - Slowly Changing Dimensions (SCD Type 2)
# MAGIC
# MAGIC ### Use Full Load When:
# MAGIC ⚠️ **Small Reference Tables**
# MAGIC - Lookup tables with < 10,000 rows
# MAGIC - Country codes, product categories
# MAGIC
# MAGIC ⚠️ **Rarely Changing Data**
# MAGIC - Monthly dimension updates
# MAGIC - Static configuration tables
# MAGIC
# MAGIC ⚠️ **No CDC Support**
# MAGIC - Source system doesn't support log-based CDC
# MAGIC - Permissions not available for LogMiner
# MAGIC
# MAGIC ⚠️ **Batch Processing Acceptable**
# MAGIC - Daily reporting where next-day data is sufficient
# MAGIC - No real-time requirements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Concepts Summary
# MAGIC
# MAGIC ### System Change Number (SCN)
# MAGIC - Oracle's internal transaction ordering mechanism
# MAGIC - Monotonically increasing number
# MAGIC - Used for checkpoint and recovery
# MAGIC - Query current SCN: `SELECT CURRENT_SCN FROM V$DATABASE;`
# MAGIC
# MAGIC ### Redo Logs
# MAGIC - Circular buffer of transaction records
# MAGIC - Typically 3-5 log groups with 2 members each
# MAGIC - Overwritten when group switches
# MAGIC - Essential for database recovery
# MAGIC
# MAGIC ### Archive Logs
# MAGIC - Persisted copies of redo logs
# MAGIC - Created when database is in ARCHIVELOG mode
# MAGIC - Required for CDC to access historical changes
# MAGIC - Need retention policy to manage disk space
# MAGIC
# MAGIC ### Supplemental Logging
# MAGIC - Adds extra information to redo logs
# MAGIC - Captures before/after values for all columns
# MAGIC - Required for accurate CDC
# MAGIC - Minimal performance overhead (~5-10%)
# MAGIC
# MAGIC ### LogMiner
# MAGIC - Built-in Oracle utility
# MAGIC - Queries and analyzes redo/archive logs
# MAGIC - Converts binary log format to SQL
# MAGIC - Foundation for CDC implementations
# MAGIC
# MAGIC ### Change Types
# MAGIC - **INSERT**: New record added
# MAGIC - **UPDATE**: Existing record modified
# MAGIC - **DELETE**: Record removed
# MAGIC - **DDL**: Schema changes (optional)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benefits of Databricks Lakeflow Connect
# MAGIC
# MAGIC ### Compared to Traditional CDC Tools
# MAGIC
# MAGIC | Feature | Traditional (Debezium + Kafka) | Lakeflow Connect |
# MAGIC |---------|-------------------------------|------------------|
# MAGIC | **Setup Complexity** | High - Deploy Kafka, Zookeeper, Debezium, Kafka Connect | Low - UI-driven configuration |
# MAGIC | **Infrastructure Management** | Self-managed VMs, clusters, storage | Fully managed by Databricks |
# MAGIC | **Scaling** | Manual cluster resizing | Auto-scaling |
# MAGIC | **Cost** | 24/7 Kafka cluster costs | Pay only for ingestion time |
# MAGIC | **Integration** | Custom code to read from Kafka | Native Unity Catalog integration |
# MAGIC | **Governance** | Separate system | Built-in with Unity Catalog |
# MAGIC | **Monitoring** | Multiple tools (Kafka, Debezium) | Unified Databricks monitoring |
# MAGIC | **Security** | Manage separately | Unity Catalog RBAC |
# MAGIC | **Time to Value** | Weeks of setup | Hours |
# MAGIC
# MAGIC ### Operational Benefits
# MAGIC
# MAGIC 1. **Zero Infrastructure Management**
# MAGIC    - No Kafka clusters to maintain
# MAGIC    - No connector deployments
# MAGIC    - Automatic VM provisioning and scaling
# MAGIC
# MAGIC 2. **Built-in Reliability**
# MAGIC    - Automatic retries on transient failures
# MAGIC    - Exactly-once semantics
# MAGIC    - Checkpoint management handled automatically
# MAGIC
# MAGIC 3. **Native Unity Catalog Integration**
# MAGIC    - Connections, volumes, and tables in one place
# MAGIC    - End-to-end lineage tracking
# MAGIC    - Centralized access control
# MAGIC    - Audit logging
# MAGIC
# MAGIC 4. **Cost Optimization**
# MAGIC    - No always-on infrastructure
# MAGIC    - Efficient storage in Delta format
# MAGIC    - Automatic cleanup of staging data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workshop Roadmap
# MAGIC
# MAGIC ### What We'll Build
# MAGIC
# MAGIC ```
# MAGIC Oracle Retail Database                    Databricks Lakehouse
# MAGIC ━━━━━━━━━━━━━━━━━━━━━━━━                ━━━━━━━━━━━━━━━━━━━━━━
# MAGIC
# MAGIC CUSTOMERS (10,000 rows)      ═══CDC═══►  bronze.customers
# MAGIC ORDERS (50,000 rows)         ═══CDC═══►  bronze.orders
# MAGIC PRODUCTS (1,000 rows)        ═══CDC═══►  bronze.products
# MAGIC
# MAGIC Changes we'll capture:
# MAGIC • New customer registration
# MAGIC • Customer address updates
# MAGIC • Order status transitions
# MAGIC • Product price changes
# MAGIC • Customer account deletions
# MAGIC ```
# MAGIC
# MAGIC ### Lab Sequence
# MAGIC
# MAGIC **Lab 2: Oracle Configuration** (45 min)
# MAGIC - Enable ARCHIVELOG mode
# MAGIC - Configure supplemental logging
# MAGIC - Set up LogMiner access
# MAGIC - Create sample tables and data
# MAGIC
# MAGIC **Lab 3: Lakeflow Connect Setup** (30 min)
# MAGIC - Create Unity Catalog connection
# MAGIC - Configure ingestion gateway
# MAGIC - Build ingestion pipeline
# MAGIC - Perform initial snapshot
# MAGIC
# MAGIC **Lecture 4: Ingestion Modes** (15 min)
# MAGIC - Understand snapshot vs. incremental
# MAGIC - Learn SCN-based checkpointing
# MAGIC
# MAGIC **Lab 5: CDC Validation** (30 min)
# MAGIC - Execute INSERT operations
# MAGIC - Test UPDATE operations
# MAGIC - Verify DELETE handling
# MAGIC - Check data consistency
# MAGIC
# MAGIC **Lab 6: Multi-Table CDC** (20 min)
# MAGIC - Handle foreign key relationships
# MAGIC - Configure dependencies
# MAGIC - Parallel ingestion patterns
# MAGIC
# MAGIC **Lecture 7: Production Practices** (15 min)
# MAGIC - Monitoring strategies
# MAGIC - Performance optimization
# MAGIC - Disaster recovery
# MAGIC
# MAGIC **Lab 8: Automation** (15 min)
# MAGIC - Schedule pipeline runs
# MAGIC - Configure alerting
# MAGIC - Error handling patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Workshop Checklist
# MAGIC
# MAGIC Before proceeding to Lab 2, ensure you have:
# MAGIC
# MAGIC ### Oracle Access
# MAGIC - [ ] Oracle database hostname/IP address
# MAGIC - [ ] Port number (default: 1521)
# MAGIC - [ ] Service name or SID
# MAGIC - [ ] DBA or SYSDBA credentials
# MAGIC - [ ] Oracle Enterprise Edition (Standard Edition lacks supplemental logging)
# MAGIC - [ ] SQL client installed (SQL*Plus, SQL Developer, DBeaver, etc.)
# MAGIC
# MAGIC ### Network Connectivity
# MAGIC - [ ] Databricks workspace can reach Oracle (firewall rules configured)
# MAGIC - [ ] Oracle listener accepting remote connections
# MAGIC - [ ] Test connection manually before starting workshop
# MAGIC
# MAGIC ### Databricks Resources
# MAGIC - [ ] Unity Catalog enabled in workspace
# MAGIC - [ ] Lakeflow Connect feature available
# MAGIC - [ ] CREATE CATALOG permission
# MAGIC - [ ] All-purpose cluster or SQL Warehouse available
# MAGIC
# MAGIC ### Knowledge Prerequisites
# MAGIC - [ ] Comfortable with SQL (SELECT, INSERT, UPDATE, DELETE)
# MAGIC - [ ] Basic Oracle administration (connecting, running scripts)
# MAGIC - [ ] Familiar with Databricks UI
# MAGIC - [ ] Understanding of data pipeline concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC
# MAGIC Test your understanding before moving to the labs:
# MAGIC
# MAGIC 1. **What is the main advantage of log-based CDC over timestamp-based approaches?**
# MAGIC    - A) Easier to implement
# MAGIC    - B) Can capture deletes and has zero performance impact on source
# MAGIC    - C) Doesn't require any database configuration
# MAGIC    - D) Works without network connectivity
# MAGIC
# MAGIC 2. **What is Oracle's SCN used for in CDC?**
# MAGIC    - A) Encrypting data
# MAGIC    - B) Ordering transactions and tracking checkpoint position
# MAGIC    - C) Compressing log files
# MAGIC    - D) User authentication
# MAGIC
# MAGIC 3. **Why is supplemental logging required for CDC?**
# MAGIC    - A) To improve query performance
# MAGIC    - B) To reduce storage costs
# MAGIC    - C) To capture complete before/after values in redo logs
# MAGIC    - D) To enable database backups
# MAGIC
# MAGIC 4. **What are the three main Lakeflow Connect components?**
# MAGIC    - A) Gateway, Volume, Pipeline
# MAGIC    - B) Producer, Consumer, Broker
# MAGIC    - C) Extract, Transform, Load
# MAGIC    - D) Source, Sink, Stream
# MAGIC
# MAGIC 5. **When should you NOT use CDC?**
# MAGIC    - A) For large tables with millions of rows
# MAGIC    - B) For small reference tables that rarely change
# MAGIC    - C) When delete tracking is required
# MAGIC    - D) For real-time dashboards
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to see answers</summary>
# MAGIC
# MAGIC 1. **B** - Log-based CDC can capture deletes and has minimal impact on source databases
# MAGIC 2. **B** - SCN orders transactions and tracks where CDC processing left off
# MAGIC 3. **C** - Supplemental logging captures complete row data for CDC reconstruction
# MAGIC 4. **A** - Gateway (reads logs), Volume (stages changes), Pipeline (applies to Delta)
# MAGIC 5. **B** - Full load is more appropriate for small, rarely-changing tables
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
# MAGIC - [Oracle LogMiner Concepts](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
# MAGIC - [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
# MAGIC - [Delta Live Tables Guide](https://docs.databricks.com/delta-live-tables/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC **Ready to get hands-on?** 
# MAGIC
# MAGIC Proceed to **Lab 2: Configuring Oracle for CDC Operations** where you'll enable ARCHIVELOG mode, configure supplemental logging, and prepare your Oracle database for CDC.

# COMMAND ----------

# MAGIC %md
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
