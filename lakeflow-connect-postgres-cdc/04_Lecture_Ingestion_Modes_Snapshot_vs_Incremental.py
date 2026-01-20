# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Understanding Ingestion Modes - Snapshot vs. Incremental
# MAGIC
# MAGIC ## Module Overview
# MAGIC This lecture explains the two primary ingestion modes used in Lakeflow Connect CDC pipelines: initial snapshot loading and incremental change capture.
# MAGIC
# MAGIC **Duration:** 15 minutes
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Understand when and why to use snapshot mode vs. incremental mode
# MAGIC - Learn the technical implementation of each mode
# MAGIC - Explore transition strategies from snapshot to incremental
# MAGIC - Identify best practices for each mode
# MAGIC - Understand performance implications and optimization strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Snapshot Mode (Initial Load)
# MAGIC
# MAGIC ### What is Snapshot Mode?
# MAGIC
# MAGIC **Snapshot mode** performs a complete one-time copy of the source table to the target Delta table. This establishes a baseline before CDC begins.
# MAGIC
# MAGIC ### When to Use Snapshot Mode
# MAGIC
# MAGIC ✅ **Use snapshot mode for:**
# MAGIC - **Initial pipeline setup**: First time ingesting a table
# MAGIC - **Historical data backfill**: Populating bronze layer with existing records
# MAGIC - **Pipeline resets**: After schema changes or pipeline failures
# MAGIC - **New table additions**: Adding tables to existing CDC pipeline
# MAGIC
# MAGIC ❌ **Don't use snapshot mode for:**
# MAGIC - Regular ongoing synchronization (use incremental)
# MAGIC - Large tables with frequent changes (too expensive)
# MAGIC - Tables already being tracked via CDC
# MAGIC
# MAGIC ### How Snapshot Mode Works
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         PostgreSQL Source Table            │
# MAGIC │         customers (1M records)             │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC          SELECT * FROM customers
# MAGIC          (Full table scan in batches)
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         Ingestion Gateway                  │
# MAGIC │  - Reads in configurable batch sizes       │
# MAGIC │  - Converts to Parquet format              │
# MAGIC │  - Marks as _change_type = "INSERT"        │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │      Unity Catalog Volume (Staging)        │
# MAGIC │  customers_snapshot_2024-01-15.parquet     │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         Ingestion Pipeline (DLT)           │
# MAGIC │  - Reads staged snapshot files             │
# MAGIC │  - Creates or replaces target table        │
# MAGIC │  - No MERGE needed (initial load)          │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │      bronze.customers (1M records)         │
# MAGIC │      Delta Table Version 0                 │
# MAGIC └────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Snapshot Mode Characteristics
# MAGIC
# MAGIC | Aspect | Details |
# MAGIC |--------|---------|
# MAGIC | **Speed** | Dependent on table size and network bandwidth |
# MAGIC | **Resource Usage** | High - reads entire table |
# MAGIC | **Source Impact** | Full table scan can impact PostgreSQL performance |
# MAGIC | **Target Operation** | INSERT or CREATE TABLE |
# MAGIC | **WAL Requirement** | Not required for snapshot |
# MAGIC | **Consistency** | Snapshot reflects a point-in-time state |
# MAGIC | **Retry Strategy** | Can restart from beginning if failure occurs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Incremental Mode (CDC)
# MAGIC
# MAGIC ### What is Incremental Mode?
# MAGIC
# MAGIC **Incremental mode** captures only the changes (INSERT, UPDATE, DELETE) from the PostgreSQL WAL since the last checkpoint.
# MAGIC
# MAGIC ### When to Use Incremental Mode
# MAGIC
# MAGIC ✅ **Use incremental mode for:**
# MAGIC - **Ongoing synchronization**: After initial snapshot is complete
# MAGIC - **Real-time analytics**: Low latency requirements
# MAGIC - **Large tables**: Where full scans are impractical
# MAGIC - **High-frequency updates**: Tables with constant changes
# MAGIC
# MAGIC ✅ **Benefits:**
# MAGIC - Minimal source database impact
# MAGIC - Low network bandwidth usage
# MAGIC - Captures all operation types (including deletes)
# MAGIC - Enables near-real-time data availability
# MAGIC
# MAGIC ### How Incremental Mode Works
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         PostgreSQL Source                  │
# MAGIC │                                            │
# MAGIC │  App: UPDATE customers SET ...             │
# MAGIC │           ↓                                │
# MAGIC │       WAL Log                              │
# MAGIC │  LSN: 0/1A2B3C4 - UPDATE customers...     │
# MAGIC │           ↓                                │
# MAGIC │  Replication Slot (tracks position)       │
# MAGIC │  Last consumed LSN: 0/1A2B3C0             │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC        Replication Protocol Stream
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         Ingestion Gateway                  │
# MAGIC │  - Subscribes to replication slot          │
# MAGIC │  - Receives only changes (4 records)       │
# MAGIC │  - Decodes logical replication format      │
# MAGIC │  - Tags with _change_type and _commit_ts   │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │      Unity Catalog Volume (Staging)        │
# MAGIC │  customers_2024-01-15-10-30-45.parquet     │
# MAGIC │  - 2 INSERTs                               │
# MAGIC │  - 1 UPDATE                                │
# MAGIC │  - 1 DELETE                                │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │         Ingestion Pipeline (DLT)           │
# MAGIC │  - Reads only new change files             │
# MAGIC │  - MERGE for INSERT/UPDATE                 │
# MAGIC │  - DELETE for DELETE operations            │
# MAGIC │  - Updates checkpoint (new LSN)            │
# MAGIC └────────────────────────────────────────────┘
# MAGIC                    ↓
# MAGIC ┌────────────────────────────────────────────┐
# MAGIC │      bronze.customers (1M + 4 records)     │
# MAGIC │      Delta Table Version 1, 2, 3...        │
# MAGIC └────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Incremental Mode Characteristics
# MAGIC
# MAGIC | Aspect | Details |
# MAGIC |--------|---------|
# MAGIC | **Speed** | Fast - only processes changes |
# MAGIC | **Resource Usage** | Low - minimal data transfer |
# MAGIC | **Source Impact** | Minimal - reads WAL, not tables |
# MAGIC | **Target Operation** | MERGE and DELETE |
# MAGIC | **WAL Requirement** | Requires logical replication enabled |
# MAGIC | **Consistency** | Eventually consistent with source |
# MAGIC | **Retry Strategy** | Can resume from last checkpoint (LSN) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Snapshot to Incremental Transition
# MAGIC
# MAGIC ### Automatic Transition Pattern
# MAGIC
# MAGIC Lakeflow Connect automatically handles the transition:
# MAGIC
# MAGIC **Stage 1: Initial Setup**
# MAGIC ```python
# MAGIC # When creating pipeline, you specify both modes:
# MAGIC # - Initial snapshot: True
# MAGIC # - Incremental sync: True
# MAGIC ```
# MAGIC
# MAGIC **Stage 2: Snapshot Execution**
# MAGIC 1. Gateway performs full table snapshot
# MAGIC 2. Records current WAL LSN position (e.g., `0/1A2B3C4`)
# MAGIC 3. Writes snapshot to staging volume
# MAGIC 4. Pipeline loads snapshot to bronze table
# MAGIC 5. Creates checkpoint with snapshot LSN
# MAGIC
# MAGIC **Stage 3: Transition Point**
# MAGIC ```
# MAGIC Snapshot LSN: 0/1A2B3C4
# MAGIC       ↓
# MAGIC   Checkpoint created
# MAGIC       ↓
# MAGIC Incremental mode starts reading from 0/1A2B3C5
# MAGIC ```
# MAGIC
# MAGIC **Stage 4: Incremental Sync**
# MAGIC - Gateway switches to replication protocol
# MAGIC - Only new changes captured going forward
# MAGIC - Continuous synchronization begins
# MAGIC
# MAGIC ### Handling Transition Gap
# MAGIC
# MAGIC **Potential Issue**: Changes that occur DURING snapshot
# MAGIC
# MAGIC **Solution**: Lakeflow Connect uses consistent snapshot technique
# MAGIC ```
# MAGIC Timeline:
# MAGIC T0: Snapshot starts, LSN = 0/1000
# MAGIC T1: (during snapshot) Customer 100 updated
# MAGIC T2: (during snapshot) Customer 200 inserted  
# MAGIC T3: Snapshot completes, marks LSN = 0/1000
# MAGIC T4: Incremental starts from LSN = 0/1000
# MAGIC     → Captures updates at T1, T2 via CDC
# MAGIC     → Applied after snapshot loads
# MAGIC ```
# MAGIC
# MAGIC **Result**: No data loss, eventual consistency guaranteed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Performance Considerations
# MAGIC
# MAGIC ### Snapshot Mode Optimization
# MAGIC
# MAGIC **Source Database (PostgreSQL)**
# MAGIC ```sql
# MAGIC -- Index key columns for faster scans
# MAGIC CREATE INDEX idx_customers_id ON customers(customer_id);
# MAGIC
# MAGIC -- Use read replicas if available
# MAGIC -- Configure gateway to point to replica
# MAGIC
# MAGIC -- Schedule snapshots during low-traffic periods
# MAGIC -- Off-hours: 2 AM - 5 AM
# MAGIC ```
# MAGIC
# MAGIC **Ingestion Gateway**
# MAGIC - Increase batch size for large tables
# MAGIC - Use larger VM size for faster processing
# MAGIC - Configure parallel reads if supported
# MAGIC
# MAGIC **Network**
# MAGIC - Use private connectivity (VPN/PrivateLink) for faster throughput
# MAGIC - Consider data transfer costs for cloud deployments
# MAGIC - Monitor bandwidth utilization
# MAGIC
# MAGIC ### Incremental Mode Optimization
# MAGIC
# MAGIC **WAL Management (PostgreSQL)**
# MAGIC ```sql
# MAGIC -- Monitor replication slot lag
# MAGIC SELECT 
# MAGIC     slot_name,
# MAGIC     pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size
# MAGIC FROM pg_replication_slots;
# MAGIC
# MAGIC -- Configure adequate WAL retention
# MAGIC -- PostgreSQL 13+
# MAGIC ALTER SYSTEM SET wal_keep_size = '2GB';
# MAGIC
# MAGIC -- PostgreSQL 12 and earlier
# MAGIC ALTER SYSTEM SET wal_keep_segments = 128;
# MAGIC ```
# MAGIC
# MAGIC **Pipeline Scheduling**
# MAGIC - High-frequency tables: Every 5-15 minutes
# MAGIC - Medium-frequency: Every 30-60 minutes
# MAGIC - Low-frequency: Hourly or daily
# MAGIC - Balance between latency and cost
# MAGIC
# MAGIC **Delta Table Optimization**
# MAGIC ```sql
# MAGIC -- Enable Auto Optimize for frequent updates
# MAGIC ALTER TABLE bronze.customers
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Use Z-ordering for query performance
# MAGIC OPTIMIZE bronze.customers ZORDER BY (customer_id, state);
# MAGIC
# MAGIC -- Vacuum old versions periodically
# MAGIC VACUUM bronze.customers RETAIN 168 HOURS; -- 7 days
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Choosing the Right Mode
# MAGIC
# MAGIC ### Decision Matrix
# MAGIC
# MAGIC | Scenario | Recommended Mode | Rationale |
# MAGIC |----------|------------------|-----------|
# MAGIC | **New table, no history needed** | Incremental only | Skip unnecessary full scan |
# MAGIC | **New table, need historical data** | Snapshot → Incremental | Get complete baseline first |
# MAGIC | **Table already in sync** | Incremental only | Continue from checkpoint |
# MAGIC | **Pipeline failed, data intact** | Incremental only | Resume from last checkpoint |
# MAGIC | **Pipeline failed, data corrupted** | Snapshot → Incremental | Rebuild from source |
# MAGIC | **Schema changed significantly** | Snapshot → Incremental | Re-baseline after DDL |
# MAGIC | **Large backlog in WAL** | Snapshot (if WAL lost) | If replication slot invalidated |
# MAGIC | **Testing/development** | Snapshot | Quick refresh of test data |
# MAGIC
# MAGIC ### Example Configurations
# MAGIC
# MAGIC **Small, High-Frequency Table** (e.g., order_status changes)
# MAGIC ```python
# MAGIC {
# MAGIC   "table": "order_status_log",
# MAGIC   "initial_snapshot": False,  # Skip if empty or don't need history
# MAGIC   "incremental": True,
# MAGIC   "schedule": "*/5 * * * *"  # Every 5 minutes
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Large, Historical Table** (e.g., customers with 10M records)
# MAGIC ```python
# MAGIC {
# MAGIC   "table": "customers",
# MAGIC   "initial_snapshot": True,   # Load all historical records
# MAGIC   "incremental": True,
# MAGIC   "snapshot_parallelism": 8,  # Faster initial load
# MAGIC   "schedule": "*/15 * * * *"  # Every 15 minutes after snapshot
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Reference Data Table** (e.g., product catalog, updated weekly)
# MAGIC ```python
# MAGIC {
# MAGIC   "table": "products",
# MAGIC   "initial_snapshot": True,
# MAGIC   "incremental": True,
# MAGIC   "schedule": "0 2 * * *"  # Daily at 2 AM (low frequency ok)
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Monitoring and Troubleshooting
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC **Snapshot Mode Metrics**
# MAGIC ```sql
# MAGIC -- Check snapshot progress
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   records_read,
# MAGIC   estimated_total_records,
# MAGIC   (records_read / estimated_total_records * 100) as percent_complete,
# MAGIC   start_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, start_time, CURRENT_TIMESTAMP) as elapsed_minutes
# MAGIC FROM system.lakeflow.snapshot_progress
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline';
# MAGIC ```
# MAGIC
# MAGIC **Incremental Mode Metrics**
# MAGIC ```sql
# MAGIC -- Check replication lag
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   last_checkpoint_lsn,
# MAGIC   current_source_lsn,
# MAGIC   records_processed_last_run,
# MAGIC   last_run_timestamp,
# MAGIC   TIMESTAMPDIFF(MINUTE, last_run_timestamp, CURRENT_TIMESTAMP) as minutes_since_last_run
# MAGIC FROM system.lakeflow.ingestion_status
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline';
# MAGIC ```
# MAGIC
# MAGIC ### Common Issues and Solutions
# MAGIC
# MAGIC **Issue: Snapshot Taking Too Long**
# MAGIC - ✅ Increase gateway VM size
# MAGIC - ✅ Use PostgreSQL read replica
# MAGIC - ✅ Increase batch size
# MAGIC - ✅ Schedule during off-peak hours
# MAGIC - ✅ Consider partitioning large tables
# MAGIC
# MAGIC **Issue: Incremental Lag Growing**
# MAGIC - ✅ Increase pipeline frequency
# MAGIC - ✅ Check WAL retention (may need larger `wal_keep_size`)
# MAGIC - ✅ Verify gateway is running
# MAGIC - ✅ Check for long-running transactions blocking replication
# MAGIC - ✅ Optimize Delta table (OPTIMIZE, VACUUM)
# MAGIC
# MAGIC **Issue: Snapshot Fails Midway**
# MAGIC - ✅ Check network connectivity
# MAGIC - ✅ Verify PostgreSQL permissions
# MAGIC - ✅ Increase timeout settings
# MAGIC - ✅ Check disk space on staging volume
# MAGIC - ✅ Review gateway logs for errors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Best Practices
# MAGIC
# MAGIC ### Snapshot Mode Best Practices
# MAGIC
# MAGIC 1. **Use read replicas** for snapshots to avoid production impact
# MAGIC 2. **Schedule during off-peak hours** to minimize resource contention
# MAGIC 3. **Test snapshot time** on subset before full table
# MAGIC 4. **Monitor source database** CPU, I/O during snapshot
# MAGIC 5. **Enable compression** on staging volume to save storage costs
# MAGIC 6. **Partition large tables** if snapshot times are excessive
# MAGIC 7. **Document snapshot windows** for operational runbooks
# MAGIC
# MAGIC ### Incremental Mode Best Practices
# MAGIC
# MAGIC 1. **Monitor replication lag** proactively
# MAGIC 2. **Set up alerts** for lag exceeding threshold (e.g., > 1 hour)
# MAGIC 3. **Regularly check WAL disk space** on PostgreSQL
# MAGIC 4. **Clean up old replication slots** if pipelines are decommissioned
# MAGIC 5. **Optimize Delta tables** regularly (OPTIMIZE, ZORDER)
# MAGIC 6. **Test pipeline recovery** by simulating failures
# MAGIC 7. **Document checkpoint LSN** for disaster recovery scenarios
# MAGIC
# MAGIC ### General Best Practices
# MAGIC
# MAGIC 1. **Start with snapshot + incremental** for new pipelines
# MAGIC 2. **Test in dev environment** before production rollout
# MAGIC 3. **Validate data counts** after snapshot completes
# MAGIC 4. **Monitor costs** for data transfer and compute
# MAGIC 5. **Implement data quality checks** in DLT pipelines
# MAGIC 6. **Use Unity Catalog tags** for governance and discovery
# MAGIC 7. **Maintain runbook** with troubleshooting procedures

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Snapshot mode** loads complete table, used for initial baseline
# MAGIC 2. **Incremental mode** captures only changes via WAL, used for ongoing sync
# MAGIC 3. **Transition is automatic** from snapshot to incremental with consistent checkpoint
# MAGIC 4. **Performance optimization** differs between modes (batch size vs. frequency)
# MAGIC 5. **Monitoring is critical** to ensure pipelines stay healthy
# MAGIC
# MAGIC ### Quick Reference
# MAGIC
# MAGIC | Aspect | Snapshot | Incremental |
# MAGIC |--------|----------|-------------|
# MAGIC | Data volume | Full table | Only changes |
# MAGIC | Speed | Hours for large tables | Seconds to minutes |
# MAGIC | Frequency | Once or rarely | Continuous |
# MAGIC | Source impact | High | Low |
# MAGIC | WAL required | No | Yes |
# MAGIC | Best for | Initial load | Ongoing sync |
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC You've completed the foundational lectures. Now proceed to:
# MAGIC - **Lab 5**: Validate incremental CDC with CRUD operations
# MAGIC   - Test INSERT, UPDATE, DELETE
# MAGIC   - Measure latency
# MAGIC   - Verify data consistency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready to test your CDC pipeline!** 🚀
