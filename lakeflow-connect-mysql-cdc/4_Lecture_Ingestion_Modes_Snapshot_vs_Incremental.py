# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture 4: Understanding Ingestion Modes - Snapshot vs. Incremental
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will understand:
# MAGIC - The difference between snapshot and incremental ingestion modes
# MAGIC - When to use each mode in CDC pipelines
# MAGIC - How checkpoint management works in Lakeflow Connect
# MAGIC - Performance characteristics and trade-offs of each mode
# MAGIC - Recovery and failover strategies
# MAGIC - Best practices for production CDC deployments
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2 (MySQL CDC Configuration)
# MAGIC - Completed Lab 3 (Lakeflow Connect Setup)
# MAGIC - Understanding of MySQL binary logs from Lecture 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Mode Overview
# MAGIC
# MAGIC Lakeflow Connect supports two primary ingestion modes:
# MAGIC
# MAGIC | Mode | Description | Use Case | Data Source |
# MAGIC |------|-------------|----------|-------------|
# MAGIC | **Snapshot** | Full table scan and load | Initial load, backfill, recovery | Current table state |
# MAGIC | **Incremental** | Only changed rows since last checkpoint | Ongoing CDC operations | MySQL binary log |
# MAGIC
# MAGIC Both modes can coexist in the same pipeline lifecycle:
# MAGIC 1. **Initial Load**: Snapshot mode to populate bronze tables
# MAGIC 2. **Continuous Sync**: Incremental mode to capture ongoing changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot Mode Deep Dive
# MAGIC
# MAGIC ### What is Snapshot Mode?
# MAGIC
# MAGIC Snapshot mode performs a **point-in-time full table read** from MySQL and loads all rows into the target Delta table.
# MAGIC
# MAGIC **Process Flow**:
# MAGIC ```
# MAGIC ┌─────────────────────┐
# MAGIC │   MySQL Table       │
# MAGIC │   customers         │
# MAGIC │   (1,000,000 rows)  │
# MAGIC └─────────────────────┘
# MAGIC           │
# MAGIC           │ SELECT * FROM customers
# MAGIC           │ (Full table scan)
# MAGIC           ▼
# MAGIC ┌─────────────────────┐
# MAGIC │ Ingestion Gateway   │
# MAGIC │ - Reads all rows    │
# MAGIC │ - Batches into      │
# MAGIC │   Parquet files     │
# MAGIC └─────────────────────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC ┌─────────────────────┐
# MAGIC │ Unity Catalog Vol   │
# MAGIC │ (Staging)           │
# MAGIC └─────────────────────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC ┌─────────────────────┐
# MAGIC │ Ingestion Pipeline  │
# MAGIC │ - INSERT all rows   │
# MAGIC └─────────────────────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC ┌─────────────────────┐
# MAGIC │ bronze.customers    │
# MAGIC │ (1,000,000 rows)    │
# MAGIC └─────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### When to Use Snapshot Mode
# MAGIC
# MAGIC ✅ **Appropriate Use Cases**:
# MAGIC 1. **Initial Pipeline Setup**: First-time load to populate bronze tables
# MAGIC 2. **Backfill After Schema Changes**: Reload table after major DDL operations
# MAGIC 3. **Recovery from Binary Log Expiration**: If binary logs aged out before ingestion
# MAGIC 4. **Data Validation**: Periodic full reconciliation with source
# MAGIC 5. **New Table Addition**: Adding a table to existing CDC pipeline
# MAGIC
# MAGIC ❌ **Avoid Snapshot Mode When**:
# MAGIC - Source table is very large (millions+ rows) and changes frequently
# MAGIC - You need near-real-time latency (snapshot adds significant delay)
# MAGIC - Source database is under heavy load (full scans impact performance)
# MAGIC
# MAGIC ### Snapshot Mode Performance Characteristics
# MAGIC
# MAGIC | Aspect | Details |
# MAGIC |--------|---------|
# MAGIC | **Read Pattern** | Full table scan (sequential read) |
# MAGIC | **Source Load** | High - reads every row |
# MAGIC | **Network Transfer** | High - entire table transferred |
# MAGIC | **Ingestion Time** | O(n) - linear with table size |
# MAGIC | **Example Timing** | 1M rows ~ 5-10 minutes, 10M rows ~ 30-60 minutes |
# MAGIC | **Impact on MySQL** | Can slow down operational queries during scan |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Mode Deep Dive
# MAGIC
# MAGIC ### What is Incremental Mode?
# MAGIC
# MAGIC Incremental mode reads **only changes** from MySQL binary logs since the last checkpoint, capturing INSERT, UPDATE, and DELETE operations.
# MAGIC
# MAGIC **Process Flow**:
# MAGIC ```
# MAGIC ┌─────────────────────────────┐
# MAGIC │   MySQL Binary Log          │
# MAGIC │   (binlog position tracking)│
# MAGIC │                             │
# MAGIC │   Event 1: INSERT customer  │
# MAGIC │   Event 2: UPDATE order     │
# MAGIC │   Event 3: DELETE product   │
# MAGIC └─────────────────────────────┘
# MAGIC           │
# MAGIC           │ Read only new events
# MAGIC           │ since last checkpoint
# MAGIC           ▼
# MAGIC ┌─────────────────────────────┐
# MAGIC │ Ingestion Gateway           │
# MAGIC │ - Maintains binlog position │
# MAGIC │ - Reads incremental changes │
# MAGIC │ - Extracts before/after     │
# MAGIC └─────────────────────────────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC ┌─────────────────────────────┐
# MAGIC │ Unity Catalog Volume        │
# MAGIC │ (Only changed rows)         │
# MAGIC └─────────────────────────────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC ┌─────────────────────────────┐
# MAGIC │ Ingestion Pipeline          │
# MAGIC │ - MERGE changes into target │
# MAGIC │ - UPDATE existing rows      │
# MAGIC │ - INSERT new rows           │
# MAGIC │ - DELETE removed rows       │
# MAGIC └─────────────────────────────┘
# MAGIC           │
# MAGIC           ▼
# MAGIC ┌─────────────────────────────┐
# MAGIC │ bronze.customers/orders/... │
# MAGIC │ (Only modified rows updated)│
# MAGIC └─────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### When to Use Incremental Mode
# MAGIC
# MAGIC ✅ **Ideal Use Cases**:
# MAGIC 1. **Continuous CDC Operations**: Ongoing synchronization after initial load
# MAGIC 2. **High-Volume Tables**: Tables with millions of rows but few changes per interval
# MAGIC 3. **Real-Time Analytics**: Low-latency requirements (seconds to minutes)
# MAGIC 4. **Operational Efficiency**: Minimize impact on source database
# MAGIC 5. **Change Tracking**: Need to capture all operations including deletes
# MAGIC
# MAGIC ### Incremental Mode Performance Characteristics
# MAGIC
# MAGIC | Aspect | Details |
# MAGIC |--------|---------|
# MAGIC | **Read Pattern** | Binary log sequential scan (append-only log) |
# MAGIC | **Source Load** | Very low - no table queries, only log reading |
# MAGIC | **Network Transfer** | Low - only changed rows |
# MAGIC | **Ingestion Time** | O(m) where m = number of changes (not table size) |
# MAGIC | **Example Timing** | 1K changes ~ 10-30 seconds, 100K changes ~ 2-5 minutes |
# MAGIC | **Impact on MySQL** | Minimal - binary log reading doesn't affect queries |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Management
# MAGIC
# MAGIC Checkpoints are critical for exactly-once processing and recovery in CDC pipelines.
# MAGIC
# MAGIC ### What is a Checkpoint?
# MAGIC
# MAGIC A **checkpoint** records the last successfully processed position in the MySQL binary log, enabling the ingestion gateway to resume from the correct location after restarts or failures.
# MAGIC
# MAGIC **Checkpoint Components**:
# MAGIC ```python
# MAGIC checkpoint = {
# MAGIC     "binlog_file": "mysql-bin.000042",      # Binary log file name
# MAGIC     "binlog_position": 194857,              # Byte offset in file
# MAGIC     "gtid_set": "uuid:1-1523",              # GTID position (if enabled)
# MAGIC     "table": "retail_db.customers",         # Table being tracked
# MAGIC     "last_processed_timestamp": "2024-01-10T10:30:15Z"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Checkpoint Storage
# MAGIC
# MAGIC Lakeflow Connect stores checkpoints in:
# MAGIC 1. **Unity Catalog Volume**: Persistent storage for gateway state
# MAGIC 2. **Delta Table Metadata**: DLT pipeline tracks applied changes
# MAGIC
# MAGIC **Location Example**:
# MAGIC ```
# MAGIC /Volumes/retail_analytics/landing/ingestion_volume/_checkpoint/
# MAGIC   ├── customers_checkpoint.json
# MAGIC   ├── orders_checkpoint.json
# MAGIC   └── products_checkpoint.json
# MAGIC ```
# MAGIC
# MAGIC ### Checkpoint Update Frequency
# MAGIC
# MAGIC Checkpoints are updated:
# MAGIC - **After each batch**: When gateway writes staged changes to volume
# MAGIC - **After pipeline completion**: When DLT applies changes to bronze tables
# MAGIC - **Configurable interval**: Can be set to every N events or N seconds
# MAGIC
# MAGIC ### Exactly-Once Processing Guarantee
# MAGIC
# MAGIC Lakeflow Connect ensures each change is processed exactly once:
# MAGIC
# MAGIC 1. **Gateway reads events** from binlog position stored in checkpoint
# MAGIC 2. **Gateway stages changes** to Unity Catalog Volume
# MAGIC 3. **Gateway updates checkpoint** only after successful write
# MAGIC 4. **Pipeline reads staged changes** from volume
# MAGIC 5. **Pipeline applies changes** to Delta table (ACID transaction)
# MAGIC 6. **Pipeline updates checkpoint** only after successful commit
# MAGIC
# MAGIC **Failure Scenarios**:
# MAGIC - Gateway crashes before checkpoint update → Re-reads same events (idempotent)
# MAGIC - Pipeline fails mid-execution → Restarts from last checkpoint (no duplicates)
# MAGIC - Network interruption → Resumes from last committed position

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparing Snapshot vs. Incremental
# MAGIC
# MAGIC ### Performance Comparison
# MAGIC
# MAGIC **Scenario**: 10 million row `customers` table with 1,000 changes per hour
# MAGIC
# MAGIC | Metric | Snapshot Mode | Incremental Mode |
# MAGIC |--------|---------------|------------------|
# MAGIC | **Rows Read** | 10,000,000 | 1,000 |
# MAGIC | **Execution Time** | 30-60 minutes | 10-30 seconds |
# MAGIC | **Network Transfer** | ~5 GB | ~500 KB |
# MAGIC | **MySQL CPU Impact** | High (table scan) | Low (log reading) |
# MAGIC | **Frequency** | Every 6-24 hours | Every 1-5 minutes |
# MAGIC | **Latency** | Hours | Seconds to minutes |
# MAGIC | **Cost** | High (compute hours) | Low (short runs) |
# MAGIC
# MAGIC ### Resource Utilization
# MAGIC
# MAGIC **Snapshot Mode**:
# MAGIC ```
# MAGIC MySQL CPU:  ████████████████████ (High - full table scan)
# MAGIC Network:    ████████████████████ (High - full data transfer)
# MAGIC Gateway:    ████████████████     (Medium - data processing)
# MAGIC Storage:    ████████████████████ (High - complete dataset staged)
# MAGIC Duration:   ████████████████████ (Long - 30-60 min for 10M rows)
# MAGIC ```
# MAGIC
# MAGIC **Incremental Mode**:
# MAGIC ```
# MAGIC MySQL CPU:  ██                   (Low - log reading only)
# MAGIC Network:    ██                   (Low - only changes)
# MAGIC Gateway:    ███                  (Low - minimal processing)
# MAGIC Storage:    ██                   (Low - only deltas)
# MAGIC Duration:   ██                   (Short - 10-30 sec for 1K changes)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hybrid Approach: Snapshot + Incremental
# MAGIC
# MAGIC The optimal CDC strategy uses **both modes** at different stages:
# MAGIC
# MAGIC ### Pattern 1: Initial Load + Continuous Sync
# MAGIC
# MAGIC ```
# MAGIC Day 1: Snapshot Mode
# MAGIC ├─ 00:00 - Start pipeline
# MAGIC ├─ 00:30 - Load customers (10M rows)
# MAGIC ├─ 01:00 - Load orders (50M rows)
# MAGIC ├─ 02:00 - Load products (100K rows)
# MAGIC └─ 02:10 - Switch to incremental mode
# MAGIC
# MAGIC Day 1+: Incremental Mode (scheduled every 5 minutes)
# MAGIC ├─ 02:15 - Process 500 changes
# MAGIC ├─ 02:20 - Process 300 changes
# MAGIC ├─ 02:25 - Process 450 changes
# MAGIC └─ ... (ongoing)
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Periodic Full Refresh
# MAGIC
# MAGIC For critical tables requiring data validation:
# MAGIC
# MAGIC ```
# MAGIC Weekly Schedule:
# MAGIC ├─ Mon-Sat: Incremental mode every 5 minutes
# MAGIC └─ Sunday 2 AM: Snapshot mode (full reconciliation)
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Hybrid Table Strategy
# MAGIC
# MAGIC Different tables use different modes based on characteristics:
# MAGIC
# MAGIC | Table | Rows | Change Rate | Mode | Frequency |
# MAGIC |-------|------|-------------|------|-----------|
# MAGIC | `customers` | 10M | 0.1% daily | Incremental | Every 5 min |
# MAGIC | `orders` | 50M | 10% daily | Incremental | Every 1 min |
# MAGIC | `products` | 100K | 1% daily | Snapshot | Every 6 hours |
# MAGIC | `dim_countries` | 200 | 0.01% monthly | Snapshot | Daily |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recovery and Failover Strategies
# MAGIC
# MAGIC ### Binary Log Expiration Recovery
# MAGIC
# MAGIC **Problem**: Binary logs are purged before ingestion catches up
# MAGIC
# MAGIC **Symptoms**:
# MAGIC ```
# MAGIC Error: Binary log 'mysql-bin.000038' not found
# MAGIC Last checkpoint position no longer available
# MAGIC ```
# MAGIC
# MAGIC **Solution**:
# MAGIC 1. Perform **snapshot load** to reset baseline
# MAGIC 2. Resume **incremental mode** from new binary log position
# MAGIC 3. Adjust `binlog_expire_logs_seconds` to increase retention
# MAGIC
# MAGIC **Prevention**:
# MAGIC - Monitor binary log retention vs. ingestion lag
# MAGIC - Set retention ≥ 2x maximum expected pipeline downtime
# MAGIC - Configure alerts for lag exceeding 50% of retention period
# MAGIC
# MAGIC ### Gateway Failure Recovery
# MAGIC
# MAGIC **Scenario**: Ingestion gateway VM crashes or restarts
# MAGIC
# MAGIC **Recovery Process**:
# MAGIC 1. Gateway reads last checkpoint from Unity Catalog Volume
# MAGIC 2. Reconnects to MySQL and subscribes to binary log at checkpoint position
# MAGIC 3. Resumes reading events (may re-process small number of events)
# MAGIC 4. MERGE operations in pipeline are idempotent, preventing duplicates
# MAGIC
# MAGIC **Automatic Recovery**: Databricks automatically restarts gateway and resumes from checkpoint
# MAGIC
# MAGIC ### Pipeline Failure Recovery
# MAGIC
# MAGIC **Scenario**: DLT pipeline fails during execution
# MAGIC
# MAGIC **Recovery Process**:
# MAGIC 1. Staged changes remain in Unity Catalog Volume
# MAGIC 2. Pipeline checkpoint not updated (stays at last successful commit)
# MAGIC 3. Restart pipeline → reads from last checkpoint
# MAGIC 4. Re-applies changes (Delta Lake transactions ensure consistency)
# MAGIC
# MAGIC ### MySQL Failover Handling (with GTID)
# MAGIC
# MAGIC **Scenario**: MySQL master fails, replica promoted to master
# MAGIC
# MAGIC **With GTID Enabled**:
# MAGIC ```
# MAGIC Old Master: server-uuid-1:1-1000
# MAGIC New Master: server-uuid-2:1001-1050 (continues from last GTID)
# MAGIC ```
# MAGIC
# MAGIC **Recovery**:
# MAGIC 1. Update Unity Catalog connection to point to new master
# MAGIC 2. Gateway uses GTID to find correct position in new master's binary log
# MAGIC 3. Seamless continuation without data loss
# MAGIC
# MAGIC **Without GTID**: Manual intervention required to find equivalent binlog position

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Production
# MAGIC
# MAGIC ### 1. Choose Appropriate Ingestion Frequency
# MAGIC
# MAGIC ```python
# MAGIC # High-value, fast-changing tables
# MAGIC orders_pipeline_schedule = "*/1 * * * *"  # Every 1 minute
# MAGIC
# MAGIC # Standard tables
# MAGIC customers_pipeline_schedule = "*/5 * * * *"  # Every 5 minutes
# MAGIC
# MAGIC # Slow-changing dimension tables
# MAGIC products_pipeline_schedule = "0 */6 * * *"  # Every 6 hours
# MAGIC ```
# MAGIC
# MAGIC **Considerations**:
# MAGIC - Business latency requirements (real-time vs. near-real-time)
# MAGIC - Change volume (high volume → more frequent)
# MAGIC - Cost vs. value trade-off
# MAGIC - Source database load tolerance
# MAGIC
# MAGIC ### 2. Monitor Binary Log Lag
# MAGIC
# MAGIC ```sql
# MAGIC -- Check time difference between current time and last processed event
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   last_processed_position,
# MAGIC   last_processed_timestamp,
# MAGIC   TIMESTAMPDIFF(MINUTE, last_processed_timestamp, NOW()) as lag_minutes
# MAGIC FROM system.lakeflow.ingestion_metrics
# MAGIC WHERE lag_minutes > 30;  -- Alert if lag exceeds 30 minutes
# MAGIC ```
# MAGIC
# MAGIC ### 3. Set Appropriate Binary Log Retention
# MAGIC
# MAGIC ```sql
# MAGIC -- MySQL configuration
# MAGIC SET GLOBAL binlog_expire_logs_seconds = 259200;  -- 3 days (72 hours)
# MAGIC
# MAGIC -- Rule of thumb: retention = 2x * (max expected downtime + pipeline execution time)
# MAGIC -- Example: 8 hours downtime + 4 hours pipeline = 24 hours * 2 = 48 hours minimum
# MAGIC ```
# MAGIC
# MAGIC ### 4. Implement Monitoring and Alerts
# MAGIC
# MAGIC **Key Metrics to Track**:
# MAGIC - Pipeline execution time (watch for increases)
# MAGIC - Number of changes processed per run
# MAGIC - Binary log lag (time behind current)
# MAGIC - Gateway health status
# MAGIC - Pipeline failure rate
# MAGIC - Staging volume size
# MAGIC
# MAGIC **Alert Thresholds**:
# MAGIC ```python
# MAGIC alerts = {
# MAGIC     "pipeline_failure": "immediate",
# MAGIC     "binlog_lag_minutes > 60": "warning",
# MAGIC     "binlog_lag_minutes > 180": "critical",
# MAGIC     "staging_volume_size_gb > 100": "warning",
# MAGIC     "execution_time_minutes > 30": "investigate"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 5. Test Failover Scenarios
# MAGIC
# MAGIC Regularly validate recovery procedures:
# MAGIC - Simulate gateway failure and verify automatic restart
# MAGIC - Test pipeline failure recovery
# MAGIC - Practice MySQL failover with GTID
# MAGIC - Verify binary log expiration recovery process

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Techniques
# MAGIC
# MAGIC ### For Snapshot Mode
# MAGIC
# MAGIC 1. **Partition Large Tables**: Break into smaller chunks
# MAGIC ```sql
# MAGIC -- Process by date range
# MAGIC WHERE created_date >= '2024-01-01' AND created_date < '2024-02-01'
# MAGIC ```
# MAGIC
# MAGIC 2. **Off-Peak Scheduling**: Run during low-traffic periods
# MAGIC ```
# MAGIC Snapshot Schedule: Daily at 2 AM (lowest DB load)
# MAGIC ```
# MAGIC
# MAGIC 3. **Parallel Table Loading**: Process multiple tables simultaneously
# MAGIC ```
# MAGIC Pipeline 1: Load customers + orders
# MAGIC Pipeline 2: Load products + suppliers (parallel execution)
# MAGIC ```
# MAGIC
# MAGIC ### For Incremental Mode
# MAGIC
# MAGIC 1. **Optimize Binary Log Size**: Use minimal row image
# MAGIC ```sql
# MAGIC SET GLOBAL binlog_row_image = 'MINIMAL';  -- Only changed columns
# MAGIC ```
# MAGIC
# MAGIC 2. **Batch Changes Appropriately**: Balance latency vs. efficiency
# MAGIC ```
# MAGIC Small batches (1-5 min): Lower latency, more frequent execution
# MAGIC Large batches (30-60 min): Higher throughput, fewer pipeline runs
# MAGIC ```
# MAGIC
# MAGIC 3. **Right-Size Gateway VM**: Match to change volume
# MAGIC ```
# MAGIC Low volume (<10K changes/hour): Small VM
# MAGIC Medium volume (10K-100K): Medium VM
# MAGIC High volume (>100K): Large VM or multiple gateways
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Snapshot mode** is for initial loads and full refreshes; **incremental mode** is for ongoing CDC operations
# MAGIC
# MAGIC 2. **Incremental mode is dramatically more efficient** - processes only changes, not entire tables
# MAGIC
# MAGIC 3. **Checkpoints enable exactly-once processing** and automatic recovery from failures
# MAGIC
# MAGIC 4. **GTID makes MySQL failover seamless** by providing consistent transaction identifiers across replicas
# MAGIC
# MAGIC 5. **Production deployments should combine both modes** - snapshot for initial load, incremental for continuous sync
# MAGIC
# MAGIC 6. **Monitor binary log lag** to prevent checkpoint expiration and ensure timely ingestion
# MAGIC
# MAGIC 7. **Test recovery scenarios** before production deployment to validate failover procedures
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 5: Validating Incremental CDC with CRUD Operations** where you'll:
# MAGIC - Execute INSERT, UPDATE, DELETE operations on MySQL
# MAGIC - Observe incremental ingestion in action
# MAGIC - Inspect checkpoints and CDC metadata
# MAGIC - Measure incremental vs. snapshot performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [MySQL Binary Log Formats](https://dev.mysql.com/doc/refman/8.0/en/binary-log-formats.html)
# MAGIC - [MySQL GTID Concepts](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html)
# MAGIC - [Delta Live Tables Streaming](https://docs.databricks.com/delta-live-tables/streaming.html)
# MAGIC - [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
