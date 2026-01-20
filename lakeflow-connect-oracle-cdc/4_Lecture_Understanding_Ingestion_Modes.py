# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture 4: Understanding Ingestion Modes - Snapshot vs. Incremental
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will understand:
# MAGIC - The difference between snapshot and incremental ingestion modes
# MAGIC - How Oracle SCN (System Change Number) enables incremental CDC
# MAGIC - When to use snapshot mode vs. incremental mode
# MAGIC - Checkpoint management and exactly-once semantics
# MAGIC - How Lakeflow Connect handles mode transitions
# MAGIC - Best practices for initial loads and ongoing synchronization
# MAGIC
# MAGIC **Duration:** 15 minutes  
# MAGIC **Prerequisites:** Completion of Lecture 1 and Lab 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Two Ingestion Modes
# MAGIC
# MAGIC Lakeflow Connect supports two distinct modes for reading data from Oracle:
# MAGIC
# MAGIC ### 1. Snapshot Mode (Initial Load)
# MAGIC
# MAGIC **Purpose:** Capture the current state of a table at a point in time
# MAGIC
# MAGIC **How it works:**
# MAGIC ```sql
# MAGIC -- Lakeflow Connect executes (simplified):
# MAGIC SELECT * FROM CUSTOMERS AS OF SCN 8745000;
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Full table scan of source table
# MAGIC - Captures data at a specific SCN
# MAGIC - No change metadata (_change_type)
# MAGIC - All records treated as INSERT
# MAGIC - One-time operation per table
# MAGIC
# MAGIC **When used:**
# MAGIC - First time ingesting a table
# MAGIC - Rebuilding after pipeline reset
# MAGIC - Backfilling historical data
# MAGIC
# MAGIC ### 2. Incremental Mode (CDC)
# MAGIC
# MAGIC **Purpose:** Capture only changes since last checkpoint
# MAGIC
# MAGIC **How it works:**
# MAGIC ```sql
# MAGIC -- Lakeflow Connect queries LogMiner (simplified):
# MAGIC SELECT * FROM V$LOGMNR_CONTENTS
# MAGIC WHERE SCN > 8745000  -- Last checkpoint
# MAGIC   AND SCN <= 8750000  -- Current SCN
# MAGIC   AND TABLE_NAME = 'CUSTOMERS';
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Reads only changed records from redo/archive logs
# MAGIC - Includes change metadata (INSERT/UPDATE/DELETE)
# MAGIC - Continuous operation
# MAGIC - Efficient for large tables
# MAGIC - Captures deletes
# MAGIC
# MAGIC **When used:**
# MAGIC - After initial snapshot completes
# MAGIC - Ongoing synchronization
# MAGIC - Real-time data pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mode Lifecycle: From Snapshot to Incremental
# MAGIC
# MAGIC ### Phase 1: Initial Snapshot
# MAGIC
# MAGIC ```
# MAGIC Time: T0 (Pipeline First Start)
# MAGIC ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC
# MAGIC Oracle Source:
# MAGIC   CUSTOMERS table: 10,000 rows
# MAGIC   Current SCN: 8745000
# MAGIC
# MAGIC Lakeflow Connect Action:
# MAGIC   1. Create checkpoint at SCN 8745000
# MAGIC   2. Execute: SELECT * FROM CUSTOMERS AS OF SCN 8745000
# MAGIC   3. Write 10,000 rows to staging volume
# MAGIC   4. Mark snapshot complete
# MAGIC
# MAGIC Staging Volume:
# MAGIC   /customers/snapshot/part-00000.parquet (2,500 rows)
# MAGIC   /customers/snapshot/part-00001.parquet (2,500 rows)
# MAGIC   /customers/snapshot/part-00002.parquet (2,500 rows)
# MAGIC   /customers/snapshot/part-00003.parquet (2,500 rows)
# MAGIC   /customers/_metadata/checkpoint.json:
# MAGIC     {
# MAGIC       "mode": "snapshot",
# MAGIC       "snapshot_scn": 8745000,
# MAGIC       "rows_captured": 10000,
# MAGIC       "completed_at": "2026-01-10T08:00:00Z"
# MAGIC     }
# MAGIC
# MAGIC Bronze Table (bronze.customers):
# MAGIC   10,000 rows inserted
# MAGIC   Delta version: 1
# MAGIC ```
# MAGIC
# MAGIC ### Phase 2: Transition to Incremental
# MAGIC
# MAGIC ```
# MAGIC Time: T1 (5 minutes later)
# MAGIC ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC
# MAGIC Oracle Source:
# MAGIC   Current SCN: 8746500
# MAGIC   Changes since 8745000:
# MAGIC     - 100 INSERTs (new customers)
# MAGIC     - 50 UPDATEs (address changes)
# MAGIC     - 5 DELETEs (account closures)
# MAGIC
# MAGIC Lakeflow Connect Action:
# MAGIC   1. Switch to incremental mode
# MAGIC   2. Start LogMiner session
# MAGIC   3. Query: WHERE SCN > 8745000 AND SCN <= 8746500
# MAGIC   4. Parse 155 change events
# MAGIC   5. Write to staging volume
# MAGIC   6. Update checkpoint to 8746500
# MAGIC
# MAGIC Staging Volume:
# MAGIC   /customers/incremental/20260110-080500/part-00000.parquet
# MAGIC     Contains 155 records with:
# MAGIC       - _change_type: INSERT, UPDATE, or DELETE
# MAGIC       - _commit_scn: 8745001 to 8746500
# MAGIC   
# MAGIC   /customers/_metadata/checkpoint.json:
# MAGIC     {
# MAGIC       "mode": "incremental",
# MAGIC       "last_scn": 8746500,
# MAGIC       "changes_captured": 155,
# MAGIC       "completed_at": "2026-01-10T08:05:00Z"
# MAGIC     }
# MAGIC
# MAGIC Bronze Table (bronze.customers):
# MAGIC   100 new rows inserted
# MAGIC   50 existing rows updated
# MAGIC   5 rows deleted
# MAGIC   Total rows: 10,095
# MAGIC   Delta version: 2
# MAGIC ```
# MAGIC
# MAGIC ### Phase 3: Continuous Incremental
# MAGIC
# MAGIC ```
# MAGIC Time: T2, T3, T4... (Every 5 minutes)
# MAGIC ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC
# MAGIC Each cycle:
# MAGIC   1. Read checkpoint SCN from previous run
# MAGIC   2. Query LogMiner for changes since checkpoint
# MAGIC   3. Write changes to staging volume
# MAGIC   4. Update checkpoint to latest processed SCN
# MAGIC   5. Pipeline applies changes to bronze table
# MAGIC
# MAGIC Example metrics:
# MAGIC   T2 (08:10): 75 changes, SCN 8746500 → 8747200
# MAGIC   T3 (08:15): 120 changes, SCN 8747200 → 8748100
# MAGIC   T4 (08:20): 200 changes, SCN 8748100 → 8749300
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## System Change Number (SCN) Deep Dive
# MAGIC
# MAGIC ### What is SCN?
# MAGIC
# MAGIC **SCN (System Change Number)** is Oracle's internal mechanism for maintaining database consistency and transaction ordering.
# MAGIC
# MAGIC **Key Properties:**
# MAGIC - Monotonically increasing integer
# MAGIC - Assigned to every committed transaction
# MAGIC - Globally unique across database
# MAGIC - Never resets (except in extreme cases)
# MAGIC - Used for read consistency, recovery, and CDC
# MAGIC
# MAGIC ### SCN Assignment Example
# MAGIC
# MAGIC ```sql
# MAGIC -- Time: 08:00:00.000
# MAGIC INSERT INTO CUSTOMERS VALUES (10001, 'Alice', 'Smith', ...);
# MAGIC COMMIT;
# MAGIC -- Oracle assigns SCN: 8745001
# MAGIC
# MAGIC -- Time: 08:00:00.123
# MAGIC UPDATE ORDERS SET STATUS='SHIPPED' WHERE ORDER_ID=5001;
# MAGIC COMMIT;
# MAGIC -- Oracle assigns SCN: 8745002
# MAGIC
# MAGIC -- Time: 08:00:00.456
# MAGIC DELETE FROM PRODUCTS WHERE PRODUCT_ID=2001;
# MAGIC COMMIT;
# MAGIC -- Oracle assigns SCN: 8745003
# MAGIC
# MAGIC -- Time: 08:00:01.000
# MAGIC -- Multiple statements in single transaction
# MAGIC BEGIN
# MAGIC   UPDATE CUSTOMERS SET EMAIL='...' WHERE CUSTOMER_ID=10001;
# MAGIC   INSERT INTO ORDERS VALUES (...);
# MAGIC   UPDATE PRODUCTS SET STOCK=STOCK-1 WHERE PRODUCT_ID=3001;
# MAGIC   COMMIT;
# MAGIC END;
# MAGIC -- Oracle assigns single SCN to entire transaction: 8745004
# MAGIC ```
# MAGIC
# MAGIC ### Querying Current SCN
# MAGIC
# MAGIC ```sql
# MAGIC -- Get current database SCN
# MAGIC SELECT CURRENT_SCN FROM V$DATABASE;
# MAGIC -- Output: 8745932
# MAGIC
# MAGIC -- Get SCN at specific time (Flashback)
# MAGIC SELECT TIMESTAMP_TO_SCN(TO_TIMESTAMP('2026-01-10 08:00:00', 'YYYY-MM-DD HH24:MI:SS'))
# MAGIC FROM DUAL;
# MAGIC -- Output: 8745000
# MAGIC
# MAGIC -- Query data as of specific SCN
# MAGIC SELECT COUNT(*) FROM CUSTOMERS AS OF SCN 8745000;
# MAGIC -- Returns count at that point in time
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Management
# MAGIC
# MAGIC ### What is a Checkpoint?
# MAGIC
# MAGIC A **checkpoint** is a marker that tracks the last successfully processed SCN. It ensures:
# MAGIC - No duplicate processing
# MAGIC - No missed changes
# MAGIC - Exactly-once semantics
# MAGIC - Recovery from failures
# MAGIC
# MAGIC ### Checkpoint Storage
# MAGIC
# MAGIC Lakeflow Connect stores checkpoints in the Unity Catalog volume:
# MAGIC
# MAGIC ```json
# MAGIC // File: /Volumes/retail_analytics/landing/cdc_volume/customers/_metadata/checkpoint.json
# MAGIC {
# MAGIC   "table_name": "CUSTOMERS",
# MAGIC   "schema_name": "RETAIL",
# MAGIC   "mode": "incremental",
# MAGIC   "snapshot_scn": 8745000,
# MAGIC   "last_processed_scn": 8746500,
# MAGIC   "last_commit_timestamp": "2026-01-10T08:05:23.456789Z",
# MAGIC   "total_changes_captured": 155,
# MAGIC   "last_update": "2026-01-10T08:05:25Z",
# MAGIC   "gateway_id": "retail_ingestion_gateway",
# MAGIC   "status": "active"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Checkpoint Update Flow
# MAGIC
# MAGIC ```
# MAGIC Step 1: Gateway reads checkpoint
# MAGIC   ↓
# MAGIC   Current checkpoint SCN: 8746500
# MAGIC
# MAGIC Step 2: Gateway queries LogMiner
# MAGIC   ↓
# MAGIC   WHERE SCN > 8746500 AND SCN <= 8748000
# MAGIC   Found 200 change events
# MAGIC
# MAGIC Step 3: Gateway writes changes to volume
# MAGIC   ↓
# MAGIC   File: incremental/20260110-081000/part-00000.parquet
# MAGIC   Status: Write successful
# MAGIC
# MAGIC Step 4: Gateway updates checkpoint
# MAGIC   ↓
# MAGIC   New checkpoint SCN: 8748000
# MAGIC   Checkpoint saved atomically
# MAGIC
# MAGIC Step 5: Pipeline processes changes
# MAGIC   ↓
# MAGIC   Reads from volume, applies to Delta table
# MAGIC   If failure occurs, replay from checkpoint 8746500
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exactly-Once Semantics
# MAGIC
# MAGIC ### The Challenge
# MAGIC
# MAGIC Distributed systems face challenges ensuring each change is processed exactly once:
# MAGIC - **At-most-once:** May lose changes during failures ❌
# MAGIC - **At-least-once:** May process duplicates ⚠️
# MAGIC - **Exactly-once:** Each change processed exactly one time ✅
# MAGIC
# MAGIC ### How Lakeflow Connect Achieves Exactly-Once
# MAGIC
# MAGIC **1. Atomic Checkpoint Updates**
# MAGIC ```
# MAGIC Transaction boundary:
# MAGIC   BEGIN
# MAGIC     Write changes to staging volume
# MAGIC     Update checkpoint to new SCN
# MAGIC   COMMIT
# MAGIC
# MAGIC If any step fails, entire transaction rolls back
# MAGIC ```
# MAGIC
# MAGIC **2. SCN-Based Deduplication**
# MAGIC ```python
# MAGIC # Pipeline applies changes in SCN order
# MAGIC df = spark.readStream.parquet(staging_volume)
# MAGIC
# MAGIC # Later changes override earlier ones for same key
# MAGIC apply_changes(
# MAGIC     keys=["CUSTOMER_ID"],
# MAGIC     sequence_by="_commit_scn",  # Use SCN for ordering
# MAGIC     stored_as_scd_type=1  # Keep only latest version
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **3. Failure Recovery**
# MAGIC ```
# MAGIC Scenario: Gateway fails after writing changes but before updating checkpoint
# MAGIC
# MAGIC On restart:
# MAGIC   1. Read last checkpoint: SCN 8746500
# MAGIC   2. Query LogMiner: WHERE SCN > 8746500
# MAGIC   3. May get some duplicate events from uncommitted batch
# MAGIC   4. Pipeline deduplicates based on SCN and primary key
# MAGIC   5. Final state is correct (exactly-once)
# MAGIC ```
# MAGIC
# MAGIC ### Example: Handling Duplicate Changes
# MAGIC
# MAGIC ```
# MAGIC Checkpoint before failure: SCN 8746500
# MAGIC
# MAGIC Batch 1 (written but checkpoint not updated due to failure):
# MAGIC   - UPDATE customer_id=100, email='new@email.com', SCN=8746501
# MAGIC   - INSERT customer_id=101, name='Alice', SCN=8746502
# MAGIC
# MAGIC Gateway restarts, re-reads from 8746500:
# MAGIC   - UPDATE customer_id=100, email='new@email.com', SCN=8746501 (duplicate)
# MAGIC   - INSERT customer_id=101, name='Alice', SCN=8746502 (duplicate)
# MAGIC   - UPDATE customer_id=100, email='newer@email.com', SCN=8746503 (new)
# MAGIC
# MAGIC Pipeline MERGE logic:
# MAGIC   - customer_id=100: Sees SCN 8746501 and 8746503, keeps 8746503 (latest)
# MAGIC   - customer_id=101: Sees SCN 8746502, inserts once
# MAGIC
# MAGIC Result: Exactly-once semantics maintained ✅
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot Mode Considerations
# MAGIC
# MAGIC ### Performance Factors
# MAGIC
# MAGIC | Table Size | Estimated Duration | Best Practices |
# MAGIC |------------|-------------------|----------------|
# MAGIC | < 1 million rows | 5-10 minutes | Default settings sufficient |
# MAGIC | 1-10 million rows | 15-60 minutes | Consider parallel workers |
# MAGIC | 10-100 million rows | 1-5 hours | Enable partitioned reads, larger gateway VM |
# MAGIC | > 100 million rows | 5+ hours | Partition by date range, use multiple pipelines |
# MAGIC
# MAGIC ### Optimizing Large Snapshots
# MAGIC
# MAGIC **1. Partition by Column**
# MAGIC ```python
# MAGIC # Configure gateway to read table in chunks
# MAGIC partition_column = "CREATED_DATE"
# MAGIC partition_boundaries = "2020-01-01, 2021-01-01, 2022-01-01, ..."
# MAGIC
# MAGIC # Gateway executes multiple parallel queries:
# MAGIC # SELECT * FROM CUSTOMERS WHERE CREATED_DATE >= '2020-01-01' AND CREATED_DATE < '2021-01-01'
# MAGIC # SELECT * FROM CUSTOMERS WHERE CREATED_DATE >= '2021-01-01' AND CREATED_DATE < '2022-01-01'
# MAGIC # ...
# MAGIC ```
# MAGIC
# MAGIC **2. Filter Unwanted Data**
# MAGIC ```sql
# MAGIC -- If you only need active records:
# MAGIC WHERE STATUS = 'ACTIVE'
# MAGIC
# MAGIC -- If you only need recent history:
# MAGIC WHERE CREATED_DATE >= ADD_MONTHS(SYSDATE, -24)
# MAGIC ```
# MAGIC
# MAGIC **3. Incremental Snapshot (Custom)**
# MAGIC ```python
# MAGIC # For extremely large tables, stage initial load over multiple days
# MAGIC # Day 1: Load 2020-2023 data
# MAGIC # Day 2: Load 2024 data
# MAGIC # Day 3: Enable CDC for ongoing changes
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Mode Considerations
# MAGIC
# MAGIC ### Latency Factors
# MAGIC
# MAGIC **End-to-end CDC latency = Gateway polling + LogMiner parsing + Volume write + Pipeline processing**
# MAGIC
# MAGIC Typical latencies:
# MAGIC - **Gateway polling interval:** 5-60 seconds (configurable)
# MAGIC - **LogMiner parsing:** 1-5 seconds (depends on change volume)
# MAGIC - **Volume write:** 1-2 seconds
# MAGIC - **Pipeline processing:** 2-10 seconds (depends on cluster)
# MAGIC - **Total:** 10-80 seconds typical
# MAGIC
# MAGIC ### Change Volume Impact
# MAGIC
# MAGIC ```
# MAGIC Low Volume (< 100 changes/minute):
# MAGIC   - Poll interval: 60 seconds
# MAGIC   - Gateway size: Small
# MAGIC   - Cost: Minimal
# MAGIC
# MAGIC Medium Volume (100-1000 changes/minute):
# MAGIC   - Poll interval: 15-30 seconds
# MAGIC   - Gateway size: Medium
# MAGIC   - Cost: Moderate
# MAGIC
# MAGIC High Volume (> 1000 changes/minute):
# MAGIC   - Poll interval: 5-10 seconds
# MAGIC   - Gateway size: Large
# MAGIC   - Consider batch processing
# MAGIC   - Cost: Higher but still efficient vs full loads
# MAGIC ```
# MAGIC
# MAGIC ### Archive Log Retention
# MAGIC
# MAGIC **Critical:** CDC requires access to archive logs for the time range being captured.
# MAGIC
# MAGIC ```
# MAGIC Scenario: Gateway offline for 6 hours
# MAGIC
# MAGIC On restart:
# MAGIC   - Last checkpoint: SCN 8746500 (from 6 hours ago)
# MAGIC   - Current SCN: 8950000
# MAGIC   - LogMiner needs archive logs covering SCN 8746500 → 8950000
# MAGIC
# MAGIC If archive logs deleted:
# MAGIC   ❌ CDC cannot resume incrementally
# MAGIC   ⚠️ Must perform new snapshot (full reload)
# MAGIC
# MAGIC Best Practice:
# MAGIC   - Retain archive logs for 24-48 hours minimum
# MAGIC   - Implement RMAN backup with archive log preservation
# MAGIC   - Monitor archive log space usage
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Check archive log retention
# MAGIC SELECT NAME, FIRST_TIME, NEXT_TIME, BLOCKS * BLOCK_SIZE / 1024 / 1024 AS SIZE_MB
# MAGIC FROM V$ARCHIVED_LOG
# MAGIC WHERE FIRST_TIME > SYSDATE - 7
# MAGIC ORDER BY FIRST_TIME DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mode Transition Decision Matrix
# MAGIC
# MAGIC ### When Gateway Chooses Snapshot Mode
# MAGIC
# MAGIC | Condition | Reason | Solution |
# MAGIC |-----------|--------|----------|
# MAGIC | First-time table ingestion | No previous checkpoint exists | Normal - proceed with snapshot |
# MAGIC | Archive log gap detected | Required logs have been deleted | Investigate log retention policy |
# MAGIC | Manual pipeline reset | User triggered full reload | Intentional - let snapshot complete |
# MAGIC | Checkpoint corruption | Metadata file damaged | Restore from backup or accept snapshot |
# MAGIC | SCN too old | Checkpoint beyond archive log retention | Increase retention or reduce downtime |
# MAGIC
# MAGIC ### When Gateway Chooses Incremental Mode
# MAGIC
# MAGIC | Condition | Reason |
# MAGIC |-----------|--------|
# MAGIC | Valid checkpoint exists | Normal operation |
# MAGIC | Archive logs available | Can read changes from redo logs |
# MAGIC | Snapshot previously completed | Ready for CDC |
# MAGIC | SCN within retention window | Logs covering checkpoint still available |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Mode Status
# MAGIC
# MAGIC ### Check Current Mode
# MAGIC
# MAGIC ```sql
# MAGIC -- Query pipeline execution details
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   event_type,
# MAGIC   details:mode::string AS ingestion_mode,
# MAGIC   details:last_scn::bigint AS checkpoint_scn,
# MAGIC   details:changes_captured::int AS change_count,
# MAGIC   timestamp
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND event_type = 'INGESTION_BATCH'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC ┌───────────────────────────┬─────────────────┬────────────────┬────────────────┬──────────────┬─────────────────────┐
# MAGIC │ pipeline_name             │ event_type      │ ingestion_mode │ checkpoint_scn │ change_count │ timestamp           │
# MAGIC ├───────────────────────────┼─────────────────┼────────────────┼────────────────┼──────────────┼─────────────────────┤
# MAGIC │ retail_ingestion_pipeline │ INGESTION_BATCH │ incremental    │ 8750000        │ 120          │ 2026-01-10 08:15:00 │
# MAGIC │ retail_ingestion_pipeline │ INGESTION_BATCH │ incremental    │ 8748000        │ 200          │ 2026-01-10 08:10:00 │
# MAGIC │ retail_ingestion_pipeline │ INGESTION_BATCH │ incremental    │ 8746500        │ 155          │ 2026-01-10 08:05:00 │
# MAGIC │ retail_ingestion_pipeline │ INGESTION_BATCH │ snapshot       │ 8745000        │ 10000        │ 2026-01-10 08:00:00 │
# MAGIC └───────────────────────────┴─────────────────┴────────────────┴────────────────┴──────────────┴─────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Check Checkpoint File
# MAGIC
# MAGIC ```python
# MAGIC # Read checkpoint metadata from volume
# MAGIC checkpoint_path = "/Volumes/retail_analytics/landing/cdc_volume/customers/_metadata/checkpoint.json"
# MAGIC checkpoint_df = spark.read.json(checkpoint_path)
# MAGIC display(checkpoint_df)
# MAGIC ```
# MAGIC
# MAGIC ### Monitor SCN Lag
# MAGIC
# MAGIC ```python
# MAGIC # Compare Oracle current SCN with last processed SCN
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # Get current Oracle SCN (from gateway metrics)
# MAGIC current_scn = 8750000
# MAGIC
# MAGIC # Get checkpoint SCN
# MAGIC checkpoint_scn = spark.read.json(checkpoint_path).select("last_processed_scn").first()[0]
# MAGIC
# MAGIC scn_lag = current_scn - checkpoint_scn
# MAGIC print(f"SCN Lag: {scn_lag}")
# MAGIC print(f"Estimated time lag: {scn_lag / 1000} minutes (approximate)")
# MAGIC
# MAGIC # Alert if lag is too high
# MAGIC if scn_lag > 10000:
# MAGIC     print("⚠️ WARNING: CDC lag is high. Check gateway and pipeline status.")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### Initial Setup
# MAGIC
# MAGIC 1. **Start with Snapshot During Off-Peak Hours**
# MAGIC    - Large snapshots consume database resources
# MAGIC    - Schedule initial load during maintenance windows
# MAGIC    - Monitor Oracle CPU and I/O during snapshot
# MAGIC
# MAGIC 2. **Validate Snapshot Completeness**
# MAGIC    ```sql
# MAGIC    -- Compare row counts
# MAGIC    SELECT COUNT(*) FROM oracle_connection.CUSTOMERS;  -- Source count
# MAGIC    SELECT COUNT(*) FROM bronze.customers;             -- Target count
# MAGIC    -- Counts should match exactly
# MAGIC    ```
# MAGIC
# MAGIC 3. **Test Incremental Before Production**
# MAGIC    - Perform test INSERT/UPDATE/DELETE in Oracle
# MAGIC    - Verify changes appear in bronze table
# MAGIC    - Check latency is acceptable
# MAGIC    - Validate data accuracy
# MAGIC
# MAGIC ### Ongoing Operations
# MAGIC
# MAGIC 4. **Monitor Archive Log Space**
# MAGIC    - Set up alerts for archive log destination > 80% full
# MAGIC    - Implement automatic cleanup via RMAN
# MAGIC    - Ensure retention covers expected downtime + buffer
# MAGIC
# MAGIC 5. **Track CDC Latency**
# MAGIC    ```sql
# MAGIC    -- Create monitoring query
# MAGIC    SELECT 
# MAGIC      table_name,
# MAGIC      TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP()) AS latency_seconds
# MAGIC    FROM bronze.customers
# MAGIC    ORDER BY _commit_timestamp DESC
# MAGIC    LIMIT 1;
# MAGIC    ```
# MAGIC
# MAGIC 6. **Plan for Extended Downtime**
# MAGIC    - If gateway will be offline > archive log retention, perform snapshot on restart
# MAGIC    - Alternative: Temporarily extend archive log retention
# MAGIC
# MAGIC 7. **Regular Checkpoint Validation**
# MAGIC    - Periodically verify checkpoint files are updating
# MAGIC    - Backup checkpoint metadata for disaster recovery
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC 8. **Unexpected Snapshot Mode**
# MAGIC    - Check: Archive logs available?
# MAGIC    - Check: Checkpoint file corrupted?
# MAGIC    - Check: SCN too old (beyond flashback retention)?
# MAGIC
# MAGIC 9. **High Incremental Latency**
# MAGIC    - Increase gateway VM size
# MAGIC    - Reduce polling interval
# MAGIC    - Check Oracle redo log contention
# MAGIC    - Verify network bandwidth

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison with Other CDC Systems
# MAGIC
# MAGIC ### Lakeflow Connect vs. Debezium
# MAGIC
# MAGIC | Aspect | Debezium (Manual) | Lakeflow Connect |
# MAGIC |--------|------------------|------------------|
# MAGIC | **Mode Management** | Manual configuration in JSON files | Automatic mode selection |
# MAGIC | **Checkpoint Storage** | Kafka topics or custom | Unity Catalog volumes |
# MAGIC | **Snapshot Strategy** | Must configure snapshot.mode | Automatic initial snapshot |
# MAGIC | **Recovery** | Manual Kafka offset management | Automatic SCN-based recovery |
# MAGIC | **Monitoring** | Kafka metrics + Debezium metrics | Unified Databricks monitoring |
# MAGIC
# MAGIC ### Lakeflow Connect vs. GoldenGate
# MAGIC
# MAGIC | Aspect | Oracle GoldenGate | Lakeflow Connect |
# MAGIC |--------|------------------|------------------|
# MAGIC | **Licensing** | Expensive Oracle license | Included with Databricks |
# MAGIC | **Initial Load** | Separate tool (Data Pump) | Built-in snapshot mode |
# MAGIC | **Mode Transition** | Manual coordination | Seamless automatic transition |
# MAGIC | **Deployment** | Complex multi-component setup | Single UI-driven configuration |
# MAGIC | **Target Format** | Various (requires mapping) | Direct to Delta Lake |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Snapshot Mode
# MAGIC - ✅ Used for initial full table load
# MAGIC - ✅ Captures data at specific SCN point-in-time
# MAGIC - ✅ One-time operation per table
# MAGIC - ⚠️ Resource-intensive for large tables
# MAGIC - ⚠️ Consider partitioning for tables > 10M rows
# MAGIC
# MAGIC ### Incremental Mode
# MAGIC - ✅ Ongoing CDC after snapshot completes
# MAGIC - ✅ Reads only changes from redo/archive logs
# MAGIC - ✅ Efficient for any table size
# MAGIC - ✅ Captures deletes
# MAGIC - ⚠️ Requires archive log retention
# MAGIC - ⚠️ Depends on supplemental logging
# MAGIC
# MAGIC ### SCN Management
# MAGIC - ✅ Monotonically increasing transaction identifier
# MAGIC - ✅ Enables precise checkpoint tracking
# MAGIC - ✅ Provides exactly-once semantics
# MAGIC - ✅ Supports failure recovery
# MAGIC
# MAGIC ### Mode Transition
# MAGIC - ✅ Automatic by Lakeflow Connect
# MAGIC - ✅ Seamless snapshot → incremental
# MAGIC - ✅ Falls back to snapshot if logs unavailable
# MAGIC - ⚠️ Monitor for unexpected snapshots

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC
# MAGIC 1. **What determines when a pipeline uses snapshot vs. incremental mode?**
# MAGIC    - A) User must manually switch modes
# MAGIC    - B) Lakeflow Connect automatically chooses based on checkpoint and log availability
# MAGIC    - C) Mode is fixed at pipeline creation
# MAGIC    - D) Oracle database administrator controls the mode
# MAGIC
# MAGIC 2. **What information does an SCN represent?**
# MAGIC    - A) The number of rows in a table
# MAGIC    - B) A unique identifier for a committed transaction
# MAGIC    - C) The size of a redo log file
# MAGIC    - D) The number of active database sessions
# MAGIC
# MAGIC 3. **What happens if archive logs are deleted before CDC can process them?**
# MAGIC    - A) Pipeline continues with incremental mode using redo logs only
# MAGIC    - B) Gateway automatically downloads logs from backup
# MAGIC    - C) Pipeline must fall back to snapshot mode (full reload)
# MAGIC    - D) Pipeline waits indefinitely for logs to be restored
# MAGIC
# MAGIC 4. **How does Lakeflow Connect achieve exactly-once semantics?**
# MAGIC    - A) By processing changes in random order
# MAGIC    - B) Using SCN-based ordering and atomic checkpoint updates
# MAGIC    - C) By requiring manual deduplication queries
# MAGIC    - D) By reading from database twice to verify
# MAGIC
# MAGIC 5. **For a 50 million row table, what's the best practice for initial snapshot?**
# MAGIC    - A) Use default settings, no optimization needed
# MAGIC    - B) Enable partitioned reads and larger gateway VM
# MAGIC    - C) Skip snapshot and start with incremental mode
# MAGIC    - D) Use multiple separate databases instead
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to see answers</summary>
# MAGIC
# MAGIC 1. **B** - Lakeflow Connect automatically manages mode transitions based on state
# MAGIC 2. **B** - SCN is Oracle's unique identifier for committed transactions
# MAGIC 3. **C** - Without required archive logs, CDC must perform full table snapshot
# MAGIC 4. **B** - SCN ordering plus atomic checkpoints prevent duplicates and data loss
# MAGIC 5. **B** - Large tables benefit from partitioned parallel reads and larger compute
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Oracle SCN Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/cncpt/oracle-database-instance.html#GUID-6C5E9E2F-1E0F-4F5F-9E3F-0F9F3E9F9F9F)
# MAGIC - [Oracle LogMiner Guide](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
# MAGIC - [Databricks Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
# MAGIC - [Lakeflow Connect Best Practices](https://docs.databricks.com/ingestion/lakeflow-connect/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand snapshot and incremental modes, proceed to **Lab 5: Validating Incremental CDC with CRUD Operations** where you'll test both modes and verify exactly-once processing.

# COMMAND ----------

# MAGIC %md
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
