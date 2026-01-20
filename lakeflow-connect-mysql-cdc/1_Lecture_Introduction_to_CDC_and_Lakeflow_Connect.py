# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture 1: Introduction to Change Data Capture and Lakeflow Connect Architecture
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will understand:
# MAGIC - What Change Data Capture (CDC) is and why it's critical for modern data architectures
# MAGIC - The difference between full-load ETL and incremental CDC patterns
# MAGIC - How traditional CDC tools (Debezium, Kafka) compare to Databricks Lakeflow Connect
# MAGIC - The architecture and components of Lakeflow Connect for MySQL
# MAGIC - MySQL binary log concepts: binlog, GTID, and row-based replication
# MAGIC - The end-to-end flow from source database changes to Delta Lake tables
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Intermediate SQL knowledge
# MAGIC - Basic understanding of relational databases
# MAGIC - Familiarity with data pipeline concepts (ETL/ELT)
# MAGIC - Access to Databricks workspace with Unity Catalog enabled

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Change Data Capture (CDC)?
# MAGIC
# MAGIC **Change Data Capture (CDC)** is a design pattern that tracks and captures changes made to data in a source system (inserts, updates, deletes) and propagates those changes to downstream systems in near-real-time.
# MAGIC
# MAGIC ### Traditional ETL vs. CDC
# MAGIC
# MAGIC | Aspect | Traditional Full-Load ETL | Change Data Capture (CDC) |
# MAGIC |--------|---------------------------|---------------------------|
# MAGIC | **Data Transfer** | Complete table snapshot every run | Only changed records since last sync |
# MAGIC | **Frequency** | Batch (hourly, daily, weekly) | Near real-time (seconds to minutes) |
# MAGIC | **Resource Usage** | High (reads entire table) | Low (reads only changes) |
# MAGIC | **Latency** | Hours to days | Seconds to minutes |
# MAGIC | **Delete Detection** | Difficult (requires full comparison) | Native (captured from transaction log) |
# MAGIC | **Use Case** | Historical reporting, low-frequency analytics | Real-time dashboards, operational analytics |
# MAGIC
# MAGIC ### Why CDC Matters
# MAGIC
# MAGIC 1. **Real-Time Analytics**: Enable dashboards and applications that reflect current operational state
# MAGIC 2. **Resource Efficiency**: Reduce load on source systems by reading only changes, not full tables
# MAGIC 3. **Complete History**: Capture every change, including deletes that full-load patterns miss
# MAGIC 4. **Data Freshness**: Minutes vs. hours/days latency for business-critical decisions
# MAGIC 5. **Scalability**: Handle high-volume transactional systems without overwhelming source databases

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common CDC Use Cases
# MAGIC
# MAGIC ### 1. Real-Time Customer 360
# MAGIC ```
# MAGIC MySQL CRM Database → CDC → Databricks → Customer Analytics Dashboard
# MAGIC   - Customer profile updates
# MAGIC   - Purchase history
# MAGIC   - Support ticket status
# MAGIC   → Live personalization engine
# MAGIC ```
# MAGIC
# MAGIC ### 2. Operational Reporting
# MAGIC ```
# MAGIC MySQL Order Management → CDC → Databricks → Real-Time KPI Dashboard
# MAGIC   - Order placed → Processing → Shipped → Delivered
# MAGIC   → Operations team sees current fulfillment status
# MAGIC ```
# MAGIC
# MAGIC ### 3. Fraud Detection
# MAGIC ```
# MAGIC MySQL Transaction Database → CDC → Databricks → ML Fraud Detection
# MAGIC   - Account changes
# MAGIC   - Transaction patterns
# MAGIC   → Real-time fraud alerts
# MAGIC ```
# MAGIC
# MAGIC ### 4. Data Synchronization
# MAGIC ```
# MAGIC MySQL (OLTP) → CDC → Databricks (OLAP) → BI Tools
# MAGIC   - Offload analytics workloads
# MAGIC   - Preserve operational database performance
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traditional CDC Architecture: Debezium + Kafka
# MAGIC
# MAGIC Before Lakeflow Connect, implementing CDC typically required:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐      ┌──────────────┐      ┌───────────────┐      ┌─────────────────┐
# MAGIC │   MySQL     │      │   Debezium   │      │  Apache Kafka │      │   Spark/Flink   │
# MAGIC │  (Source)   │─────▶│  Connector   │─────▶│   (Streaming) │─────▶│   Consumer      │
# MAGIC │             │      │  (Java VM)   │      │               │      │                 │
# MAGIC └─────────────┘      └──────────────┘      └───────────────┘      └─────────────────┘
# MAGIC                                                                              │
# MAGIC                                                                              ▼
# MAGIC                                                                    ┌─────────────────┐
# MAGIC                                                                    │   Delta Lake    │
# MAGIC                                                                    │  (Databricks)   │
# MAGIC                                                                    └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Challenges with Traditional CDC:
# MAGIC
# MAGIC 1. **Complexity**: Multiple components to deploy, configure, and manage
# MAGIC    - Kafka cluster provisioning and management
# MAGIC    - Debezium connector configuration and monitoring
# MAGIC    - Schema registry for message formats
# MAGIC    - Zookeeper coordination (for older Kafka versions)
# MAGIC
# MAGIC 2. **Operational Overhead**: 
# MAGIC    - Separate infrastructure for Kafka brokers
# MAGIC    - Connector failure handling and restart logic
# MAGIC    - Topic partition management
# MAGIC    - Consumer lag monitoring
# MAGIC
# MAGIC 3. **Cost**: 
# MAGIC    - Always-on Kafka cluster costs
# MAGIC    - Additional compute for connectors
# MAGIC    - Storage for Kafka message retention
# MAGIC
# MAGIC 4. **Expertise Required**:
# MAGIC    - Kafka administration knowledge
# MAGIC    - Debezium configuration expertise
# MAGIC    - JVM tuning and troubleshooting
# MAGIC
# MAGIC 5. **Integration Complexity**:
# MAGIC    - Custom code to consume Kafka messages
# MAGIC    - Schema evolution handling
# MAGIC    - Exactly-once processing guarantees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Lakeflow Connect: Simplified CDC
# MAGIC
# MAGIC **Lakeflow Connect** is Databricks' native CDC solution that eliminates the need for external tools like Debezium and Kafka.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐           ┌──────────────────────────────────────────────┐
# MAGIC │      MySQL          │           │         Databricks Lakehouse Platform        │
# MAGIC │   (Source DB)       │           │                                              │
# MAGIC │                     │           │  ┌────────────────────────────────────┐     │
# MAGIC │  Binary Logs        │◀─ Read ───┤  │   Ingestion Gateway (Managed VM)   │     │
# MAGIC │  (Row-based)        │           │  └────────────────────────────────────┘     │
# MAGIC └─────────────────────┘           │                    │                         │
# MAGIC                                    │                    ▼                         │
# MAGIC                                    │  ┌────────────────────────────────────┐     │
# MAGIC                                    │  │  Unity Catalog Volume (Staging)    │     │
# MAGIC                                    │  │  - Parquet files with CDC events   │     │
# MAGIC                                    │  └────────────────────────────────────┘     │
# MAGIC                                    │                    │                         │
# MAGIC                                    │                    ▼                         │
# MAGIC                                    │  ┌────────────────────────────────────┐     │
# MAGIC                                    │  │  Ingestion Pipeline (DLT)          │     │
# MAGIC                                    │  │  - Applies INSERT/UPDATE/DELETE    │     │
# MAGIC                                    │  └────────────────────────────────────┘     │
# MAGIC                                    │                    │                         │
# MAGIC                                    │                    ▼                         │
# MAGIC                                    │  ┌────────────────────────────────────┐     │
# MAGIC                                    │  │  Bronze Layer (Delta Tables)       │     │
# MAGIC                                    │  │  - customers, orders, products     │     │
# MAGIC                                    │  └────────────────────────────────────┘     │
# MAGIC                                    │                                              │
# MAGIC                                    │  Unity Catalog: Governance & Lineage        │
# MAGIC                                    └──────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Benefits:
# MAGIC
# MAGIC 1. **Simplified Architecture**: No Kafka, no Debezium - just Databricks
# MAGIC 2. **Fully Managed**: Databricks handles infrastructure, scaling, and monitoring
# MAGIC 3. **Native Integration**: Built-in Unity Catalog governance and Delta Lake ACID transactions
# MAGIC 4. **Cost Effective**: Pay only when ingestion runs, no always-on infrastructure
# MAGIC 5. **Easy Configuration**: Point-and-click UI or simple SQL/Python APIs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Connect Components Deep Dive
# MAGIC
# MAGIC ### 1. Ingestion Gateway
# MAGIC
# MAGIC **Purpose**: Dedicated compute VM that reads MySQL binary logs and stages changes
# MAGIC
# MAGIC **How it Works**:
# MAGIC - Establishes connection to MySQL using credentials in Unity Catalog Connection
# MAGIC - Subscribes to MySQL binary log stream (like a replication slave)
# MAGIC - Reads row-based replication events (INSERT, UPDATE, DELETE, DDL)
# MAGIC - Converts binary log events to structured format (Parquet)
# MAGIC - Writes staged changes to Unity Catalog Volume
# MAGIC - Maintains checkpoint/offset tracking for exactly-once processing
# MAGIC
# MAGIC **Configuration**:
# MAGIC - VM size (based on change volume)
# MAGIC - Network connectivity (VPC peering, PrivateLink)
# MAGIC - Staging volume location
# MAGIC
# MAGIC ### 2. Unity Catalog Volume (Staging Layer)
# MAGIC
# MAGIC **Purpose**: Temporary storage for CDC events before applying to target tables
# MAGIC
# MAGIC **Contents**:
# MAGIC - Parquet files organized by table and timestamp
# MAGIC - Metadata: operation type (I/U/D), before/after values, transaction timestamp
# MAGIC - Checkpoint markers for incremental processing
# MAGIC
# MAGIC **Example Structure**:
# MAGIC ```
# MAGIC /Volumes/retail_analytics/landing/ingestion_volume/
# MAGIC   ├── customers/
# MAGIC   │   ├── 2024-01-10-10-30-00.parquet  (INSERT records)
# MAGIC   │   ├── 2024-01-10-10-35-00.parquet  (UPDATE records)
# MAGIC   │   └── _checkpoint/
# MAGIC   ├── orders/
# MAGIC   └── products/
# MAGIC ```
# MAGIC
# MAGIC ### 3. Ingestion Pipeline (Delta Live Tables)
# MAGIC
# MAGIC **Purpose**: Applies staged CDC changes to target Delta tables
# MAGIC
# MAGIC **How it Works**:
# MAGIC - Reads CDC events from staging volume
# MAGIC - Applies changes to bronze Delta tables using MERGE operations
# MAGIC - Handles UPDATE: replaces existing rows based on primary key
# MAGIC - Handles INSERT: adds new rows
# MAGIC - Handles DELETE: removes rows (or marks as deleted if soft-delete pattern)
# MAGIC - Maintains data lineage and audit trail in Unity Catalog
# MAGIC
# MAGIC **Pipeline Modes**:
# MAGIC - **Snapshot**: Initial full load of source table
# MAGIC - **Incremental**: Processes only new changes from staging volume
# MAGIC - **Scheduled**: Runs at defined intervals (e.g., every 5 minutes)
# MAGIC
# MAGIC ### 4. Unity Catalog Connection
# MAGIC
# MAGIC **Purpose**: Stores credentials and connection metadata for MySQL
# MAGIC
# MAGIC **Configuration**:
# MAGIC ```python
# MAGIC connection_properties = {
# MAGIC     "host": "mysql-prod.company.com",
# MAGIC     "port": "3306",
# MAGIC     "database": "retail_db",
# MAGIC     "user": "cdc_user",
# MAGIC     "password": "{{secrets/mysql_creds/password}}"  # From Databricks Secrets
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Security**:
# MAGIC - Passwords stored in Databricks Secrets (encrypted)
# MAGIC - Fine-grained access control via Unity Catalog
# MAGIC - Audit logging for connection usage

# COMMAND ----------

# MAGIC %md
# MAGIC ## MySQL Binary Log Concepts
# MAGIC
# MAGIC Understanding MySQL binary logs is essential for CDC operations.
# MAGIC
# MAGIC ### What is the Binary Log (binlog)?
# MAGIC
# MAGIC The **binary log** is a set of files that MySQL uses to record all changes to data:
# MAGIC - Data modifications (INSERT, UPDATE, DELETE)
# MAGIC - Schema changes (CREATE, ALTER, DROP)
# MAGIC - Transaction metadata (commit, rollback)
# MAGIC
# MAGIC **Primary Uses**:
# MAGIC 1. **Replication**: Master-slave database replication
# MAGIC 2. **Point-in-time Recovery**: Restore database to specific timestamp
# MAGIC 3. **Change Data Capture**: Downstream analytics systems (our use case!)
# MAGIC
# MAGIC ### Binary Log Formats
# MAGIC
# MAGIC | Format | Description | CDC Suitable? |
# MAGIC |--------|-------------|---------------|
# MAGIC | **STATEMENT** | Logs SQL statements (e.g., `UPDATE orders SET status='SHIPPED'`) | ❌ No - loses row-level detail |
# MAGIC | **ROW** | Logs actual row changes (before/after values) | ✅ Yes - required for CDC |
# MAGIC | **MIXED** | Uses STATEMENT when safe, ROW otherwise | ⚠️ Partial - inconsistent for CDC |
# MAGIC
# MAGIC **For CDC, you MUST use ROW format**: `binlog_format = ROW`
# MAGIC
# MAGIC ### GTID (Global Transaction Identifiers)
# MAGIC
# MAGIC **GTID** provides a unique identifier for each transaction across all MySQL servers in a replication topology.
# MAGIC
# MAGIC Format: `server_uuid:transaction_id`
# MAGIC Example: `3e11fa47-71ca-11e1-9e33-c80aa9429562:23`
# MAGIC
# MAGIC **Benefits for CDC**:
# MAGIC - Consistent replication tracking across failovers
# MAGIC - Simplified checkpoint management
# MAGIC - Prevents duplicate change processing
# MAGIC
# MAGIC **Configuration**: `gtid_mode = ON`
# MAGIC
# MAGIC ### Row-Based Replication Events
# MAGIC
# MAGIC When `binlog_format = ROW`, MySQL logs detailed before/after values:
# MAGIC
# MAGIC **INSERT Event**:
# MAGIC ```
# MAGIC Event Type: WRITE_ROWS
# MAGIC Table: retail_db.customers
# MAGIC After Image: {customer_id: 101, first_name: 'Jane', last_name: 'Smith', email: 'jane@example.com', ...}
# MAGIC ```
# MAGIC
# MAGIC **UPDATE Event**:
# MAGIC ```
# MAGIC Event Type: UPDATE_ROWS
# MAGIC Table: retail_db.orders
# MAGIC Before Image: {order_id: 5, order_status: 'PENDING', ...}
# MAGIC After Image:  {order_id: 5, order_status: 'SHIPPED', ...}
# MAGIC ```
# MAGIC
# MAGIC **DELETE Event**:
# MAGIC ```
# MAGIC Event Type: DELETE_ROWS
# MAGIC Table: retail_db.customers
# MAGIC Before Image: {customer_id: 99, first_name: 'John', ...}
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-End CDC Flow Example
# MAGIC
# MAGIC Let's trace a single UPDATE operation through the entire CDC pipeline:
# MAGIC
# MAGIC ### Step 1: Change Occurs in MySQL
# MAGIC ```sql
# MAGIC -- Application updates order status
# MAGIC UPDATE orders 
# MAGIC SET order_status = 'SHIPPED', last_updated = NOW()
# MAGIC WHERE order_id = 1001;
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: MySQL Writes to Binary Log
# MAGIC ```
# MAGIC # at 12345
# MAGIC #240110 10:30:15 server id 1  end_log_pos 12456 CRC32 0x1234abcd
# MAGIC ### UPDATE_ROWS_EVENT
# MAGIC ### TABLE retail_db.orders
# MAGIC ### WHERE
# MAGIC ###   @1=1001 /* INT (order_id) */
# MAGIC ###   @4='PENDING' /* VARCHAR (order_status) */
# MAGIC ###   @9='2024-01-10 09:15:00' /* DATETIME (last_updated) */
# MAGIC ### SET
# MAGIC ###   @1=1001 /* INT (order_id) */
# MAGIC ###   @4='SHIPPED' /* VARCHAR (order_status) */
# MAGIC ###   @9='2024-01-10 10:30:15' /* DATETIME (last_updated) */
# MAGIC ```
# MAGIC
# MAGIC ### Step 3: Ingestion Gateway Reads Binary Log
# MAGIC - Gateway polls MySQL binary log stream
# MAGIC - Detects UPDATE_ROWS_EVENT for `orders` table
# MAGIC - Extracts before/after values
# MAGIC - Converts to structured format
# MAGIC
# MAGIC ### Step 4: Staged to Unity Catalog Volume
# MAGIC ```
# MAGIC File: /Volumes/retail_analytics/landing/ingestion_volume/orders/2024-01-10-10-30-15.parquet
# MAGIC
# MAGIC | _change_type | order_id | customer_id | order_status | total_amount | last_updated          | _commit_timestamp        |
# MAGIC |--------------|----------|-------------|--------------|--------------|----------------------|--------------------------|
# MAGIC | UPDATE       | 1001     | 55          | SHIPPED      | 299.99       | 2024-01-10 10:30:15  | 2024-01-10 10:30:15.123  |
# MAGIC ```
# MAGIC
# MAGIC ### Step 5: Ingestion Pipeline Applies Change
# MAGIC ```sql
# MAGIC -- DLT pipeline executes MERGE operation
# MAGIC MERGE INTO bronze.orders AS target
# MAGIC USING staged_changes AS source
# MAGIC ON target.order_id = source.order_id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     order_status = source.order_status,
# MAGIC     last_updated = source.last_updated,
# MAGIC     _commit_timestamp = source._commit_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC ```
# MAGIC
# MAGIC ### Step 6: Available for Analytics
# MAGIC ```sql
# MAGIC -- Analysts query updated data
# MAGIC SELECT order_id, order_status, last_updated
# MAGIC FROM bronze.orders
# MAGIC WHERE order_id = 1001;
# MAGIC
# MAGIC -- Result: order_status = 'SHIPPED' (updated value)
# MAGIC ```
# MAGIC
# MAGIC **Total Latency**: Typically 1-5 minutes from MySQL commit to queryable in Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Metadata Fields
# MAGIC
# MAGIC Lakeflow Connect automatically adds metadata columns to help track changes:
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `_change_type` | STRING | Operation type: 'INSERT', 'UPDATE', 'DELETE' |
# MAGIC | `_commit_version` | LONG | Delta table version when change was applied |
# MAGIC | `_commit_timestamp` | TIMESTAMP | When change was committed to MySQL |
# MAGIC | `_rescued_data` | STRING | Any fields that couldn't be parsed (schema evolution safety) |
# MAGIC
# MAGIC **Usage Examples**:
# MAGIC
# MAGIC ```sql
# MAGIC -- Find all recent updates
# MAGIC SELECT * FROM bronze.customers
# MAGIC WHERE _change_type = 'UPDATE'
# MAGIC   AND _commit_timestamp >= current_timestamp() - INTERVAL 1 HOUR;
# MAGIC
# MAGIC -- Audit trail for specific customer
# MAGIC SELECT customer_id, first_name, last_name, email, _change_type, _commit_timestamp
# MAGIC FROM bronze.customers
# MAGIC WHERE customer_id = 55
# MAGIC ORDER BY _commit_timestamp DESC;
# MAGIC
# MAGIC -- Count operations by type
# MAGIC SELECT _change_type, COUNT(*) as operation_count
# MAGIC FROM bronze.orders
# MAGIC GROUP BY _change_type;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workshop Overview: What You'll Build
# MAGIC
# MAGIC Across the remaining labs and lectures, you will:
# MAGIC
# MAGIC ### Lab 2: MySQL CDC Configuration
# MAGIC - Enable binary logging on MySQL server
# MAGIC - Configure row-based replication format
# MAGIC - Enable GTID for transaction tracking
# MAGIC - Create CDC user with replication privileges
# MAGIC - Set up retail database with customers, orders, products tables
# MAGIC
# MAGIC ### Lab 3: Lakeflow Connect Setup
# MAGIC - Create Unity Catalog resources (catalog, schemas, volumes)
# MAGIC - Deploy Ingestion Gateway through Databricks UI
# MAGIC - Configure Unity Catalog connection to MySQL
# MAGIC - Build Ingestion Pipeline with table selection
# MAGIC - Execute initial snapshot load
# MAGIC
# MAGIC ### Lecture 4: Ingestion Modes
# MAGIC - Understand snapshot vs. incremental processing
# MAGIC - Learn checkpoint management strategies
# MAGIC - Compare performance characteristics
# MAGIC
# MAGIC ### Lab 5: Incremental CDC Validation
# MAGIC - Perform INSERT operations and validate propagation
# MAGIC - Perform UPDATE operations and verify changes
# MAGIC - Perform DELETE operations and confirm removal
# MAGIC - Inspect CDC metadata and timestamps
# MAGIC
# MAGIC ### Lab 6: Multi-Table CDC
# MAGIC - Handle foreign key relationships
# MAGIC - Configure pipeline dependencies
# MAGIC - Test referential integrity scenarios
# MAGIC
# MAGIC ### Lecture 7: Production Best Practices
# MAGIC - Binary log retention policies
# MAGIC - Monitoring and alerting strategies
# MAGIC - Performance optimization techniques
# MAGIC - Cost optimization patterns
# MAGIC
# MAGIC ### Lab 8: Production Implementation
# MAGIC - Configure scheduled pipeline execution
# MAGIC - Implement monitoring queries
# MAGIC - Set up error handling and alerting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **CDC enables real-time analytics** by capturing only changed data, not full table snapshots
# MAGIC
# MAGIC 2. **Lakeflow Connect simplifies CDC** by eliminating the need for Kafka, Debezium, and custom integration code
# MAGIC
# MAGIC 3. **MySQL binary logs** are the foundation of CDC - they must be enabled with ROW format and GTID
# MAGIC
# MAGIC 4. **Three core components**:
# MAGIC    - Ingestion Gateway (reads binary logs)
# MAGIC    - Unity Catalog Volume (stages changes)
# MAGIC    - Ingestion Pipeline (applies changes to Delta tables)
# MAGIC
# MAGIC 5. **Built-in governance** through Unity Catalog provides lineage, access control, and audit logging
# MAGIC
# MAGIC 6. **Metadata fields** like `_change_type` and `_commit_timestamp` enable change tracking and auditing
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 2: Configuring MySQL for CDC Operations** where you'll enable binary logging, create the CDC user, and set up the retail database schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional References
# MAGIC
# MAGIC - [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
# MAGIC - [MySQL Binary Log Documentation](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
# MAGIC - [MySQL Replication and GTID](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html)
# MAGIC - [Delta Live Tables Guide](https://docs.databricks.com/delta-live-tables/)
# MAGIC - [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
