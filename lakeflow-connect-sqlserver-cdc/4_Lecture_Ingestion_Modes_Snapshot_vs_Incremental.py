# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Understanding Ingestion Modes - Snapshot vs. Incremental
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture explains the two primary modes of data ingestion with Lakeflow Connect: Snapshot mode for initial full loads and Incremental mode for ongoing change capture. You'll understand when to use each mode, how checkpoint management works, and best practices for refresh patterns.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain the difference between snapshot and incremental ingestion modes
# MAGIC - Understand checkpoint management and Log Sequence Number (LSN) tracking
# MAGIC - Identify when to use snapshot vs. incremental mode
# MAGIC - Recognize refresh patterns and scheduling strategies
# MAGIC - Troubleshoot common checkpoint and sync issues
# MAGIC
# MAGIC ## Duration
# MAGIC 15-20 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Modes Overview
# MAGIC
# MAGIC Lakeflow Connect supports two ingestion modes that work together to provide complete data synchronization:
# MAGIC
# MAGIC ### Mode 1: Snapshot (Initial Load)
# MAGIC
# MAGIC **Purpose:** Capture the current state of all data in a table
# MAGIC
# MAGIC **How it works:**
# MAGIC ```
# MAGIC SQL Server Table (customers)
# MAGIC ┌─────────────┬────────────┬───────────┐
# MAGIC │ customer_id │ first_name │ last_name │
# MAGIC ├─────────────┼────────────┼───────────┤
# MAGIC │ 1001        │ John       │ Doe       │
# MAGIC │ 1002        │ Jane       │ Smith     │
# MAGIC │ 1003        │ Bob        │ Johnson   │
# MAGIC │ ...         │ ...        │ ...       │
# MAGIC │ 50000       │ Alice      │ Williams  │
# MAGIC └─────────────┴────────────┴───────────┘
# MAGIC          ↓ (SELECT * FROM customers)
# MAGIC    Full table scan - reads ALL rows
# MAGIC          ↓
# MAGIC Bronze Table (bronze.customers)
# MAGIC - All 50,000 rows loaded
# MAGIC - Creates baseline for incremental sync
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Reads entire table from source
# MAGIC - First step in CDC pipeline setup
# MAGIC - Establishes checkpoint position in transaction log
# MAGIC - Can take minutes to hours for large tables
# MAGIC
# MAGIC ### Mode 2: Incremental (Change Capture)
# MAGIC
# MAGIC **Purpose:** Capture only changes since last sync
# MAGIC
# MAGIC **How it works:**
# MAGIC ```
# MAGIC Transaction Log
# MAGIC ┌─────────────┬────────────┬──────────────┬───────────┐
# MAGIC │ LSN         │ Operation  │ Table        │ Data      │
# MAGIC ├─────────────┼────────────┼──────────────┼───────────┤
# MAGIC │ 00000:0100  │ INSERT     │ customers    │ {id:50001}│
# MAGIC │ 00000:0101  │ UPDATE     │ orders       │ {id:2001} │
# MAGIC │ 00000:0102  │ DELETE     │ customers    │ {id:1050} │
# MAGIC │ 00000:0103  │ INSERT     │ products     │ {id:301}  │
# MAGIC └─────────────┴────────────┴──────────────┴───────────┘
# MAGIC          ↓ (Read from last checkpoint)
# MAGIC    Only changed records (4 operations)
# MAGIC          ↓
# MAGIC Bronze Tables
# MAGIC - Apply 1 INSERT to customers
# MAGIC - Apply 1 UPDATE to orders
# MAGIC - Apply 1 DELETE to customers
# MAGIC - Apply 1 INSERT to products
# MAGIC ```
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Reads only transaction log entries
# MAGIC - Processes changed records (could be 1000s vs. millions)
# MAGIC - Updates checkpoint after successful processing
# MAGIC - Runs continuously or on schedule

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot Mode Deep Dive
# MAGIC
# MAGIC ### When to Use Snapshot Mode
# MAGIC
# MAGIC ✅ **Initial Pipeline Setup**: First run of Lakeflow Connect ingestion
# MAGIC - Example: Setting up CDC for existing 10-year-old customer database
# MAGIC
# MAGIC ✅ **Re-Sync After Extended Downtime**: Transaction logs rotated/purged
# MAGIC - Example: Pipeline down for 2 weeks, logs only retain 7 days
# MAGIC
# MAGIC ✅ **New Table Addition**: Adding another table to existing pipeline
# MAGIC - Example: Started with customers/orders, now adding products table
# MAGIC
# MAGIC ✅ **Data Validation**: Periodic full refresh to verify consistency
# MAGIC - Example: Monthly validation comparing source vs. target counts
# MAGIC
# MAGIC ✅ **Checkpoint Lost/Corrupted**: Recovery from checkpoint issues
# MAGIC - Example: Metadata corruption requires re-establishing baseline
# MAGIC
# MAGIC ### Snapshot Process Flow
# MAGIC
# MAGIC **Step 1: Initiation**
# MAGIC - User triggers snapshot mode in Lakeflow Connect UI
# MAGIC - Ingestion gateway connects to SQL Server
# MAGIC - Queries current LSN from transaction log
# MAGIC
# MAGIC **Step 2: Data Extraction**
# MAGIC ```sql
# MAGIC -- SQL Server executes full table scan
# MAGIC SELECT * FROM dbo.customers;
# MAGIC ```
# MAGIC - Reads all rows in batches (typically 10K-100K rows per batch)
# MAGIC - Respects source database read capacity limits
# MAGIC - Uses snapshot isolation to avoid locking
# MAGIC
# MAGIC **Step 3: Staging**
# MAGIC - Writes batches to Unity Catalog volume as Parquet files
# MAGIC - Each file tagged with snapshot metadata
# MAGIC - Progress tracked for resumability
# MAGIC
# MAGIC **Step 4: Loading**
# MAGIC - Ingestion pipeline reads staged files
# MAGIC - Creates or replaces bronze table
# MAGIC - Establishes checkpoint LSN for future incremental runs
# MAGIC
# MAGIC **Step 5: Completion**
# MAGIC - Records snapshot completion timestamp
# MAGIC - Switches to incremental mode automatically
# MAGIC - Begins processing changes from established checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Mode Deep Dive
# MAGIC
# MAGIC ### When to Use Incremental Mode
# MAGIC
# MAGIC ✅ **Ongoing Synchronization**: Normal operation after initial snapshot
# MAGIC - Example: Hourly sync capturing customer updates throughout day
# MAGIC
# MAGIC ✅ **Real-Time Requirements**: Near real-time data freshness
# MAGIC - Example: Order status updates visible within 5 minutes
# MAGIC
# MAGIC ✅ **Large Tables with Frequent Changes**: Efficient processing
# MAGIC - Example: 50M row table, 100K daily changes (0.2% of table)
# MAGIC
# MAGIC ✅ **Delete Tracking**: Must capture record removals
# MAGIC - Example: Customer deletions for GDPR compliance
# MAGIC
# MAGIC ### Incremental Process Flow
# MAGIC
# MAGIC **Step 1: Checkpoint Read**
# MAGIC - Load last successful checkpoint LSN
# MAGIC - Example: Last LSN was `00000027:000003e8:0001`
# MAGIC
# MAGIC **Step 2: Change Identification**
# MAGIC ```sql
# MAGIC -- Query CDC tables for changes since checkpoint
# MAGIC SELECT * FROM cdc.dbo_customers_CT
# MAGIC WHERE __$start_lsn > @last_checkpoint_lsn
# MAGIC ORDER BY __$start_lsn;
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Operation Type Classification**
# MAGIC
# MAGIC SQL Server CDC operation codes:
# MAGIC - `__$operation = 1`: DELETE
# MAGIC - `__$operation = 2`: INSERT  
# MAGIC - `__$operation = 3`: UPDATE (before image)
# MAGIC - `__$operation = 4`: UPDATE (after image)
# MAGIC
# MAGIC Example changes captured:
# MAGIC ```
# MAGIC LSN: 00000027:000003e9:0001, Operation: 2 (INSERT)
# MAGIC   → customer_id: 50001, first_name: "Jane", last_name: "Smith"
# MAGIC
# MAGIC LSN: 00000027:000003ea:0001, Operation: 4 (UPDATE after)
# MAGIC   → customer_id: 1001, email: "john.doe@newemail.com"
# MAGIC
# MAGIC LSN: 00000027:000003eb:0001, Operation: 1 (DELETE)
# MAGIC   → customer_id: 9999
# MAGIC ```
# MAGIC
# MAGIC **Step 4: Change Application**
# MAGIC
# MAGIC Pipeline applies changes to bronze tables:
# MAGIC
# MAGIC **INSERT handling:**
# MAGIC ```sql
# MAGIC INSERT INTO bronze.customers (customer_id, first_name, last_name, ...)
# MAGIC VALUES (50001, 'Jane', 'Smith', ...);
# MAGIC ```
# MAGIC
# MAGIC **UPDATE handling (MERGE):**
# MAGIC ```sql
# MAGIC MERGE INTO bronze.customers AS target
# MAGIC USING staged_updates AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET email = source.email, last_updated = source.last_updated;
# MAGIC ```
# MAGIC
# MAGIC **DELETE handling:**
# MAGIC ```sql
# MAGIC DELETE FROM bronze.customers WHERE customer_id = 9999;
# MAGIC ```
# MAGIC
# MAGIC **Step 5: Checkpoint Update**
# MAGIC - Save new checkpoint LSN: `00000027:000003eb:0001`
# MAGIC - Next run will start from this position

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Checkpoints and LSN
# MAGIC
# MAGIC ### What is a Checkpoint?
# MAGIC
# MAGIC A **checkpoint** is a saved position in the SQL Server transaction log that marks:
# MAGIC - Last successfully processed change
# MAGIC - Resume point for next incremental run
# MAGIC - Recovery point in case of failures
# MAGIC
# MAGIC ### Log Sequence Number (LSN) Structure
# MAGIC
# MAGIC **Format:** `Virtual Log File : Log Block : Log Record`
# MAGIC
# MAGIC **Example:** `00000027:000003e8:0001`
# MAGIC - `00000027`: Virtual log file number (hexadecimal)
# MAGIC - `000003e8`: Log block within that file (hexadecimal = 1000 decimal)
# MAGIC - `0001`: Log record within that block
# MAGIC
# MAGIC **Properties:**
# MAGIC - Sequential and monotonically increasing
# MAGIC - Unique identifier for every transaction
# MAGIC - Used for point-in-time recovery
# MAGIC - Never reused (until log wraps in circular logging)
# MAGIC
# MAGIC ### Checkpoint Management
# MAGIC
# MAGIC **Checkpoint Storage:**
# MAGIC - Persisted in Unity Catalog metadata
# MAGIC - Stored per table being ingested
# MAGIC - Includes timestamp of last successful sync
# MAGIC
# MAGIC **Checkpoint Update Frequency:**
# MAGIC - After each successful batch of changes
# MAGIC - Typically every few seconds to minutes
# MAGIC - Configurable based on pipeline settings
# MAGIC
# MAGIC **Checkpoint Validation:**
# MAGIC ```sql
# MAGIC -- Check if checkpoint LSN still valid in transaction log
# MAGIC SELECT MIN(__$start_lsn) as oldest_lsn
# MAGIC FROM cdc.dbo_customers_CT;
# MAGIC
# MAGIC -- If checkpoint < oldest_lsn, need snapshot mode
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Patterns and Scheduling
# MAGIC
# MAGIC ### Pattern 1: Continuous Streaming
# MAGIC
# MAGIC **Configuration:**
# MAGIC - Pipeline runs continuously (24/7)
# MAGIC - Processes changes as soon as they're available
# MAGIC - Minimal latency (seconds to low minutes)
# MAGIC
# MAGIC **Best For:**
# MAGIC - Real-time dashboards
# MAGIC - Fraud detection systems
# MAGIC - Operational analytics
# MAGIC
# MAGIC **Considerations:**
# MAGIC - Higher compute costs (always-on cluster)
# MAGIC - Best for high-volume, time-sensitive data
# MAGIC
# MAGIC **Example Use Case:**
# MAGIC ```
# MAGIC E-commerce Order Processing:
# MAGIC - Orders placed in SQL Server
# MAGIC - CDC captures within 30 seconds
# MAGIC - Order fulfillment dashboard updates in real-time
# MAGIC - Customer service sees live order status
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Scheduled Batch
# MAGIC
# MAGIC **Configuration:**
# MAGIC - Pipeline runs on fixed schedule (e.g., every 15 minutes)
# MAGIC - Processes accumulated changes in batches
# MAGIC - Predictable latency based on schedule
# MAGIC
# MAGIC **Best For:**
# MAGIC - Regular reporting needs
# MAGIC - Cost-sensitive environments
# MAGIC - Moderate freshness requirements (minutes to hours)
# MAGIC
# MAGIC **Considerations:**
# MAGIC - Cluster starts/stops add overhead (~2-3 minutes)
# MAGIC - Balance frequency vs. cost
# MAGIC
# MAGIC **Schedule Examples:**
# MAGIC ```
# MAGIC Every 5 minutes:  Max latency = 5 min, High freshness, Higher cost
# MAGIC Every 15 minutes: Max latency = 15 min, Good for most use cases
# MAGIC Every hour:       Max latency = 60 min, Lower cost, Batch reporting
# MAGIC Once daily:       Max latency = 24 hr, Lowest cost, Daily aggregations
# MAGIC ```
# MAGIC
# MAGIC **Example Use Case:**
# MAGIC ```
# MAGIC Customer Analytics:
# MAGIC - Run every 15 minutes during business hours (6 AM - 10 PM)
# MAGIC - Run every hour during off-hours (10 PM - 6 AM)
# MAGIC - Balances freshness with cost
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Triggered/On-Demand
# MAGIC
# MAGIC **Configuration:**
# MAGIC - Pipeline runs when manually triggered
# MAGIC - Used for testing and development
# MAGIC - No automated schedule
# MAGIC
# MAGIC **Best For:**
# MAGIC - Development and testing environments
# MAGIC - Ad-hoc data loads
# MAGIC - Validation after source changes
# MAGIC
# MAGIC **Example Use Case:**
# MAGIC ```
# MAGIC Development Testing:
# MAGIC - Developer modifies SQL Server schema
# MAGIC - Manually triggers pipeline to test change propagation
# MAGIC - Validates data in bronze tables before promoting to prod
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 4: Hybrid Approach
# MAGIC
# MAGIC **Configuration:**
# MAGIC - Continuous for critical tables (orders, transactions)
# MAGIC - Scheduled for reference tables (products, customers)
# MAGIC - Different SLAs per table
# MAGIC
# MAGIC **Example Architecture:**
# MAGIC ```
# MAGIC Pipeline 1 (Continuous):
# MAGIC   - orders table → Real-time, always running
# MAGIC   - payments table → Real-time, always running
# MAGIC
# MAGIC Pipeline 2 (Every 15 min):
# MAGIC   - customers table → Near real-time, acceptable lag
# MAGIC   - addresses table → Near real-time, acceptable lag
# MAGIC
# MAGIC Pipeline 3 (Hourly):
# MAGIC   - products table → Changes infrequent
# MAGIC   - categories table → Mostly static reference data
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot vs. Incremental: Performance Comparison
# MAGIC
# MAGIC ### Example: 10 Million Row Customer Table
# MAGIC
# MAGIC **Scenario Details:**
# MAGIC - Table: 10,000,000 customers
# MAGIC - Average row size: 500 bytes
# MAGIC - Total table size: ~5 GB
# MAGIC - Daily changes: 50,000 rows (0.5% of table)
# MAGIC
# MAGIC **Snapshot Mode (Full Load):**
# MAGIC ```
# MAGIC Data Transferred: 5 GB
# MAGIC Processing Time: 15-20 minutes
# MAGIC Network Usage: High
# MAGIC Source Impact: Moderate (full table scan)
# MAGIC Use Case: Initial load, weekly validation
# MAGIC ```
# MAGIC
# MAGIC **Incremental Mode (Daily Changes):**
# MAGIC ```
# MAGIC Data Transferred: ~25 MB (50K rows × 500 bytes)
# MAGIC Processing Time: 30-60 seconds
# MAGIC Network Usage: Minimal
# MAGIC Source Impact: Very low (log read only)
# MAGIC Use Case: Hourly/continuous sync
# MAGIC ```
# MAGIC
# MAGIC **Efficiency Gain:**
# MAGIC - Data volume: **200x reduction** (5 GB → 25 MB)
# MAGIC - Time: **20x faster** (20 min → 1 min)
# MAGIC - Cost: **~90% reduction** in compute and network
# MAGIC
# MAGIC ### Real-World Performance Benchmarks
# MAGIC
# MAGIC | Table Size | Daily Changes | Snapshot Time | Incremental Time | Efficiency Gain |
# MAGIC |------------|---------------|---------------|------------------|-----------------|
# MAGIC | 1M rows    | 10K (1%)      | 2-3 min       | 10-15 sec        | 12x faster      |
# MAGIC | 10M rows   | 50K (0.5%)    | 15-20 min     | 30-60 sec        | 20x faster      |
# MAGIC | 50M rows   | 200K (0.4%)   | 60-90 min     | 2-3 min          | 30x faster      |
# MAGIC | 100M rows  | 500K (0.5%)   | 2-3 hours     | 5-8 min          | 25x faster      |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Issues and Troubleshooting
# MAGIC
# MAGIC ### Issue 1: Checkpoint Lost or Expired
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Pipeline fails with "LSN not found in transaction log"
# MAGIC - Error message: "Checkpoint position no longer available"
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Transaction log cleanup job purged old log entries
# MAGIC - Checkpoint older than log retention period
# MAGIC - Typically happens after extended pipeline downtime
# MAGIC
# MAGIC **Solution:**
# MAGIC ```
# MAGIC 1. Check log retention settings:
# MAGIC    SELECT name, log_reuse_wait_desc 
# MAGIC    FROM sys.databases 
# MAGIC    WHERE name = 'RetailDB';
# MAGIC
# MAGIC 2. Increase retention if needed:
# MAGIC    ALTER DATABASE RetailDB
# MAGIC    SET CHANGE_TRACKING (CHANGE_RETENTION = 7 DAYS);
# MAGIC
# MAGIC 3. Re-run in snapshot mode to re-establish baseline
# MAGIC ```
# MAGIC
# MAGIC ### Issue 2: Snapshot Mode Takes Too Long
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Initial snapshot running for hours
# MAGIC - Timeout errors during full load
# MAGIC
# MAGIC **Solutions:**
# MAGIC - **Increase gateway VM size** for more bandwidth
# MAGIC - **Partition large tables** by date or ID ranges
# MAGIC - **Run during off-peak hours** to reduce source load
# MAGIC - **Filter unnecessary historical data** if not all rows needed
# MAGIC
# MAGIC ### Issue 3: Incremental Mode Missing Changes
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Changes made in SQL Server not appearing in bronze tables
# MAGIC - Record counts don't match between source and target
# MAGIC
# MAGIC **Diagnostic Queries:**
# MAGIC ```sql
# MAGIC -- Check if CDC still enabled
# MAGIC SELECT name, is_cdc_enabled FROM sys.databases;
# MAGIC SELECT name, is_tracked_by_cdc FROM sys.tables;
# MAGIC
# MAGIC -- Check CDC capture job status
# MAGIC EXEC sys.sp_cdc_help_jobs;
# MAGIC
# MAGIC -- Verify changes in CDC tables
# MAGIC SELECT COUNT(*) FROM cdc.dbo_customers_CT;
# MAGIC ```
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Verify CDC not accidentally disabled
# MAGIC - Check CDC capture job is running
# MAGIC - Restart SQL Server Agent if jobs stuck
# MAGIC
# MAGIC ### Issue 4: High Incremental Latency
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Changes taking longer than expected to appear
# MAGIC - Growing backlog in staging volume
# MAGIC
# MAGIC **Solutions:**
# MAGIC - **Increase pipeline frequency** (e.g., 15 min → 5 min)
# MAGIC - **Scale up gateway VM** for higher throughput
# MAGIC - **Check network bandwidth** between systems
# MAGIC - **Optimize bronze tables** (Z-order, liquid clustering)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### Checkpoint Management
# MAGIC
# MAGIC ✅ **Set Appropriate Log Retention:**
# MAGIC ```sql
# MAGIC -- Retain at least 2x your max expected downtime
# MAGIC ALTER DATABASE RetailDB
# MAGIC SET CHANGE_TRACKING (CHANGE_RETENTION = 7 DAYS);
# MAGIC ```
# MAGIC
# MAGIC ✅ **Monitor Checkpoint Age:**
# MAGIC ```sql
# MAGIC -- Alert if checkpoint older than 6 hours
# MAGIC SELECT pipeline_name, 
# MAGIC        last_checkpoint_time,
# MAGIC        DATEDIFF(hour, last_checkpoint_time, GETDATE()) as hours_old
# MAGIC FROM system.lakeflow.checkpoints
# MAGIC WHERE hours_old > 6;
# MAGIC ```
# MAGIC
# MAGIC ✅ **Regular Validation:**
# MAGIC - Compare source vs. target row counts weekly
# MAGIC - Run periodic snapshots for critical tables (monthly)
# MAGIC
# MAGIC ### Scheduling Strategy
# MAGIC
# MAGIC ✅ **Match Frequency to Business Needs:**
# MAGIC - Real-time critical: Continuous or every 5 minutes
# MAGIC - Operational reporting: Every 15-30 minutes
# MAGIC - Daily analytics: Hourly or daily batches
# MAGIC
# MAGIC ✅ **Consider Time Zones:**
# MAGIC ```python
# MAGIC # Schedule for business hours in user's timezone
# MAGIC schedule = {
# MAGIC     "weekday_business_hours": "*/15 * * * *",  # Every 15 min
# MAGIC     "weekday_off_hours": "0 * * * *",         # Every hour
# MAGIC     "weekend": "0 */4 * * *"                   # Every 4 hours
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC
# MAGIC ✅ **Optimize Initial Snapshot:**
# MAGIC - Run during maintenance windows
# MAGIC - Use larger gateway VM temporarily
# MAGIC - Consider filtering old historical data
# MAGIC
# MAGIC ✅ **Optimize Incremental Processing:**
# MAGIC - Z-order bronze tables on join keys
# MAGIC - Enable auto-optimize on Delta tables
# MAGIC - Monitor and tune batch sizes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You Learned
# MAGIC
# MAGIC In this lecture, you learned:
# MAGIC
# MAGIC ✅ **Snapshot Mode**: Full table load for initial baseline
# MAGIC   - When: First run, re-sync, new tables
# MAGIC   - Performance: Minutes to hours for large tables
# MAGIC   - Use: Establishes checkpoint for incremental sync
# MAGIC
# MAGIC ✅ **Incremental Mode**: Captures only changed records
# MAGIC   - When: Ongoing sync after snapshot
# MAGIC   - Performance: Seconds to minutes (10-100x faster)
# MAGIC   - Use: Normal operation for real-time/near real-time sync
# MAGIC
# MAGIC ✅ **Checkpoint Management**: LSN tracking for resume capability
# MAGIC   - Structure: Virtual log file : log block : log record
# MAGIC   - Storage: Unity Catalog metadata per table
# MAGIC   - Retention: Must exceed max expected downtime
# MAGIC
# MAGIC ✅ **Refresh Patterns**: Continuous, scheduled, triggered, hybrid
# MAGIC   - Match pattern to business SLAs and cost constraints
# MAGIC   - Different strategies per table based on criticality
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC In **Lab 5: Validating Incremental CDC with CRUD Operations**, you'll:
# MAGIC - Perform INSERT, UPDATE, DELETE operations on SQL Server
# MAGIC - Observe changes captured in staging volume
# MAGIC - Validate changes applied to bronze Delta tables
# MAGIC - Compare snapshot vs. incremental performance
# MAGIC
# MAGIC **Ready to test CDC in action? Let's validate incremental sync!** 🚀
