# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Production Best Practices and Monitoring Strategies
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture covers essential best practices for deploying Lakeflow Connect CDC pipelines in production environments. You'll learn monitoring strategies, performance optimization techniques, error handling patterns, and disaster recovery approaches to ensure reliable, production-grade data synchronization.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Implement comprehensive monitoring and alerting for CDC pipelines
# MAGIC - Apply performance optimization techniques for high-volume scenarios
# MAGIC - Design error handling and retry strategies for production resilience
# MAGIC - Plan disaster recovery and business continuity procedures
# MAGIC - Establish operational runbooks and troubleshooting workflows
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Readiness Checklist
# MAGIC
# MAGIC Before deploying CDC pipelines to production, ensure you have addressed:
# MAGIC
# MAGIC ### Infrastructure & Security
# MAGIC - ☑️ **Network Connectivity**: Private connectivity (VPN, PrivateLink) configured
# MAGIC - ☑️ **Authentication**: Service accounts with least-privilege access
# MAGIC - ☑️ **Encryption**: TLS/SSL for data in transit, encryption at rest
# MAGIC - ☑️ **Unity Catalog Permissions**: Fine-grained access controls on bronze tables
# MAGIC - ☑️ **Secrets Management**: Database credentials stored in Databricks secrets
# MAGIC
# MAGIC ### Configuration & Capacity
# MAGIC - ☑️ **Gateway Sizing**: VM sized for peak change volume
# MAGIC - ☑️ **Log Retention**: SQL Server CDC retention >= 2x max downtime
# MAGIC - ☑️ **Storage Capacity**: Volume storage sized with growth projection
# MAGIC - ☑️ **Pipeline Schedule**: Frequency matches business SLAs
# MAGIC - ☑️ **Cluster Configuration**: Auto-scaling enabled for variable workloads
# MAGIC
# MAGIC ### Monitoring & Operations
# MAGIC - ☑️ **Health Checks**: Automated monitoring of pipeline status
# MAGIC - ☑️ **Alerting**: Notifications for failures, lag, and anomalies
# MAGIC - ☑️ **Logging**: Centralized logs with retention policy
# MAGIC - ☑️ **Metrics Dashboard**: Real-time visibility into CDC operations
# MAGIC - ☑️ **Runbook Documentation**: Operational procedures for common issues
# MAGIC
# MAGIC ### Data Quality & Validation
# MAGIC - ☑️ **Row Count Validation**: Automated source vs. target comparison
# MAGIC - ☑️ **Schema Drift Detection**: Alerts on unexpected schema changes
# MAGIC - ☑️ **Data Lineage**: Complete tracking from source to bronze
# MAGIC - ☑️ **Checksum Validation**: Periodic data integrity checks
# MAGIC - ☑️ **Reconciliation Jobs**: Daily validation reports
# MAGIC
# MAGIC ### Disaster Recovery
# MAGIC - ☑️ **Backup Strategy**: Regular snapshots of bronze tables
# MAGIC - ☑️ **Checkpoint Backup**: Metadata backup for recovery
# MAGIC - ☑️ **Recovery Procedures**: Documented steps for restoration
# MAGIC - ☑️ **RTO/RPO Defined**: Clear recovery objectives (e.g., RTO 4hr, RPO 1hr)
# MAGIC - ☑️ **DR Testing**: Quarterly recovery drills

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Strategy
# MAGIC
# MAGIC ### Key Metrics to Monitor
# MAGIC
# MAGIC #### 1. Pipeline Health Metrics
# MAGIC
# MAGIC **Pipeline Status:**
# MAGIC ```sql
# MAGIC -- Monitor pipeline execution status
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   execution_status,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   DATEDIFF(minute, start_time, end_time) as duration_minutes
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND event_date >= CURRENT_DATE - 7
# MAGIC ORDER BY start_time DESC;
# MAGIC ```
# MAGIC
# MAGIC **Alert Thresholds:**
# MAGIC - 🔴 **Critical**: Pipeline failed 2+ consecutive runs
# MAGIC - 🟠 **Warning**: Pipeline duration >2x normal average
# MAGIC - 🟢 **Info**: Pipeline completed successfully
# MAGIC
# MAGIC #### 2. Data Freshness Metrics
# MAGIC
# MAGIC **Checkpoint Age:**
# MAGIC ```sql
# MAGIC -- Monitor checkpoint lag
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   last_checkpoint_time,
# MAGIC   CURRENT_TIMESTAMP() - last_checkpoint_time as checkpoint_age,
# MAGIC   CASE 
# MAGIC     WHEN CURRENT_TIMESTAMP() - last_checkpoint_time > INTERVAL 2 HOURS THEN 'CRITICAL'
# MAGIC     WHEN CURRENT_TIMESTAMP() - last_checkpoint_time > INTERVAL 1 HOUR THEN 'WARNING'
# MAGIC     ELSE 'OK'
# MAGIC   END as status
# MAGIC FROM system.lakeflow.checkpoints
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline';
# MAGIC ```
# MAGIC
# MAGIC **Alert Thresholds:**
# MAGIC - 🔴 **Critical**: Checkpoint age > 2 hours
# MAGIC - 🟠 **Warning**: Checkpoint age > 1 hour
# MAGIC - 🟢 **OK**: Checkpoint age < 30 minutes
# MAGIC
# MAGIC #### 3. Volume Metrics
# MAGIC
# MAGIC **Change Volume Tracking:**
# MAGIC ```sql
# MAGIC -- Track changes processed per run
# MAGIC SELECT 
# MAGIC   DATE(timestamp) as date,
# MAGIC   table_name,
# MAGIC   SUM(records_inserted) as total_inserts,
# MAGIC   SUM(records_updated) as total_updates,
# MAGIC   SUM(records_deleted) as total_deletes,
# MAGIC   SUM(records_inserted + records_updated + records_deleted) as total_changes
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND timestamp >= CURRENT_DATE - 30
# MAGIC GROUP BY DATE(timestamp), table_name
# MAGIC ORDER BY date DESC, table_name;
# MAGIC ```
# MAGIC
# MAGIC **Alert Thresholds:**
# MAGIC - 🔴 **Critical**: Change volume >5x daily average (potential issue)
# MAGIC - 🟠 **Warning**: Change volume <10% daily average (source problem?)
# MAGIC - 🟢 **OK**: Within 2x standard deviation of average
# MAGIC
# MAGIC #### 4. Error Rate Metrics
# MAGIC
# MAGIC **Error Tracking:**
# MAGIC ```sql
# MAGIC -- Monitor error patterns
# MAGIC SELECT 
# MAGIC   DATE(timestamp) as date,
# MAGIC   error_type,
# MAGIC   COUNT(*) as error_count,
# MAGIC   COUNT(DISTINCT execution_id) as affected_runs,
# MAGIC   MAX(error_message) as sample_error
# MAGIC FROM system.lakeflow.pipeline_errors
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND timestamp >= CURRENT_DATE - 7
# MAGIC GROUP BY DATE(timestamp), error_type
# MAGIC ORDER BY date DESC, error_count DESC;
# MAGIC ```
# MAGIC
# MAGIC **Alert Thresholds:**
# MAGIC - 🔴 **Critical**: Error rate >5% of transactions
# MAGIC - 🟠 **Warning**: Error rate 1-5%
# MAGIC - 🟢 **OK**: Error rate <1%

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alerting Architecture
# MAGIC
# MAGIC ### Multi-Tier Alerting Strategy
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                   Monitoring Layer                       │
# MAGIC │  - Pipeline events                                      │
# MAGIC │  - Checkpoint tracking                                  │
# MAGIC │  - Error logs                                           │
# MAGIC │  - Performance metrics                                  │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                        ↓
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              Alert Evaluation Engine                     │
# MAGIC │  - Check metrics against thresholds                     │
# MAGIC │  - Correlate related failures                           │
# MAGIC │  - Suppress duplicate alerts                            │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                        ↓
# MAGIC ┌──────────────┬──────────────┬──────────────────────────┐
# MAGIC │   Critical   │   Warning    │      Informational       │
# MAGIC │  Immediate   │  30min delay │      Daily summary       │
# MAGIC │  PagerDuty   │    Slack     │        Email             │
# MAGIC └──────────────┴──────────────┴──────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Critical Alerts (Immediate Response Required)
# MAGIC
# MAGIC **Trigger Conditions:**
# MAGIC - Pipeline failed 2+ consecutive runs
# MAGIC - Checkpoint lag >2 hours (data staleness)
# MAGIC - Gateway disconnected from SQL Server
# MAGIC - Bronze table write failures
# MAGIC - Storage quota exceeded (volume full)
# MAGIC
# MAGIC **Notification Channels:**
# MAGIC - PagerDuty for on-call engineer
# MAGIC - Slack #data-alerts-critical
# MAGIC - Email to data-ops team
# MAGIC
# MAGIC **Example Alert:**
# MAGIC ```
# MAGIC 🔴 CRITICAL: retail_ingestion_pipeline FAILED
# MAGIC Pipeline: retail_ingestion_pipeline
# MAGIC Status: FAILED (2 consecutive failures)
# MAGIC Last Success: 2024-01-15 08:45:00 (3 hours ago)
# MAGIC Error: "Connection timeout to SQL Server"
# MAGIC Checkpoint Age: 2.5 hours (exceeds 2hr SLA)
# MAGIC Impact: Order data stale, affecting real-time dashboard
# MAGIC Runbook: https://wiki/cdc-troubleshooting#connection-timeout
# MAGIC ```
# MAGIC
# MAGIC ### Warning Alerts (Monitor and Investigate)
# MAGIC
# MAGIC **Trigger Conditions:**
# MAGIC - Pipeline duration >2x average
# MAGIC - Change volume anomaly (>3x or <10% of normal)
# MAGIC - Error rate 1-5%
# MAGIC - Staging volume >80% capacity
# MAGIC
# MAGIC **Notification Channels:**
# MAGIC - Slack #data-alerts-warning
# MAGIC - Email to data engineering team
# MAGIC
# MAGIC ### Informational Alerts (Daily Summary)
# MAGIC
# MAGIC **Content:**
# MAGIC - Daily change volume summary
# MAGIC - Pipeline performance trends
# MAGIC - Row count validation results
# MAGIC - Upcoming maintenance recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC ### SQL Server Optimization
# MAGIC
# MAGIC #### 1. Transaction Log Management
# MAGIC
# MAGIC **Best Practices:**
# MAGIC ```sql
# MAGIC -- Set appropriate retention period
# MAGIC ALTER DATABASE RetailDB
# MAGIC SET CHANGE_TRACKING (
# MAGIC     CHANGE_RETENTION = 7 DAYS,
# MAGIC     AUTO_CLEANUP = ON
# MAGIC );
# MAGIC
# MAGIC -- Monitor log size growth
# MAGIC SELECT 
# MAGIC     name,
# MAGIC     (size * 8.0 / 1024) as size_mb,
# MAGIC     (FILEPROPERTY(name, 'SpaceUsed') * 8.0 / 1024) as used_mb
# MAGIC FROM sys.database_files
# MAGIC WHERE type_desc = 'LOG';
# MAGIC
# MAGIC -- Regular log backups to manage size
# MAGIC BACKUP LOG RetailDB TO DISK = 'backup_path';
# MAGIC ```
# MAGIC
# MAGIC **Avoid:**
# MAGIC - ❌ Auto-growth in small increments (causes fragmentation)
# MAGIC - ❌ Excessive log retention (wastes space)
# MAGIC - ❌ Infrequent log backups (log can't truncate)
# MAGIC
# MAGIC #### 2. Index Optimization for CDC Tables
# MAGIC
# MAGIC ```sql
# MAGIC -- Ensure CDC capture tables have appropriate indexes
# MAGIC -- SQL Server creates these automatically, but verify:
# MAGIC SELECT 
# MAGIC     t.name as table_name,
# MAGIC     i.name as index_name,
# MAGIC     i.type_desc
# MAGIC FROM cdc.change_tables ct
# MAGIC JOIN sys.tables t ON ct.object_id = t.object_id
# MAGIC JOIN sys.indexes i ON t.object_id = i.object_id;
# MAGIC ```
# MAGIC
# MAGIC ### Databricks Optimization
# MAGIC
# MAGIC #### 1. Bronze Table Optimization
# MAGIC
# MAGIC **Z-Ordering for Query Performance:**
# MAGIC ```sql
# MAGIC -- Optimize tables based on common query patterns
# MAGIC OPTIMIZE bronze.customers ZORDER BY (customer_id, state);
# MAGIC OPTIMIZE bronze.orders ZORDER BY (customer_id, order_date);
# MAGIC OPTIMIZE bronze.products ZORDER BY (category, product_id);
# MAGIC ```
# MAGIC
# MAGIC **Liquid Clustering (Alternative to Z-Order):**
# MAGIC ```sql
# MAGIC -- For tables with evolving query patterns
# MAGIC CREATE TABLE bronze.orders_v2 (...)
# MAGIC USING DELTA
# MAGIC CLUSTER BY (customer_id, order_date);
# MAGIC ```
# MAGIC
# MAGIC **Auto-Optimize Settings:**
# MAGIC ```sql
# MAGIC -- Enable auto-optimization for bronze tables
# MAGIC ALTER TABLE bronze.customers 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC #### 2. Staging Volume Cleanup
# MAGIC
# MAGIC **Implement Lifecycle Policy:**
# MAGIC ```python
# MAGIC # Clean up processed staging files older than 7 days
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC def cleanup_staging_volume():
# MAGIC     cutoff_date = datetime.now() - timedelta(days=7)
# MAGIC     
# MAGIC     # List files in staging volume
# MAGIC     files = dbutils.fs.ls("/Volumes/retail_analytics/landing/ingestion_volume/")
# MAGIC     
# MAGIC     for file in files:
# MAGIC         if file.modificationTime < cutoff_date:
# MAGIC             dbutils.fs.rm(file.path, recurse=True)
# MAGIC             print(f"Deleted: {file.path}")
# MAGIC
# MAGIC # Schedule as daily job
# MAGIC ```
# MAGIC
# MAGIC #### 3. Pipeline Compute Configuration
# MAGIC
# MAGIC **Right-Size Gateway VM:**
# MAGIC ```
# MAGIC Change Volume         Recommended Gateway Size
# MAGIC <10K rows/hour        Small (1-2 cores, 4-8 GB RAM)
# MAGIC 10K-100K rows/hour    Medium (2-4 cores, 8-16 GB RAM)
# MAGIC 100K-1M rows/hour     Large (4-8 cores, 16-32 GB RAM)
# MAGIC >1M rows/hour         X-Large (8+ cores, 32+ GB RAM)
# MAGIC ```
# MAGIC
# MAGIC **DLT Pipeline Configuration:**
# MAGIC ```json
# MAGIC {
# MAGIC   "clusters": [{
# MAGIC     "node_type_id": "i3.xlarge",
# MAGIC     "autoscale": {
# MAGIC       "min_workers": 1,
# MAGIC       "max_workers": 5,
# MAGIC       "mode": "ENHANCED"
# MAGIC     }
# MAGIC   }],
# MAGIC   "configuration": {
# MAGIC     "spark.databricks.delta.optimizeWrite.enabled": "true",
# MAGIC     "spark.databricks.delta.autoCompact.enabled": "true"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling and Recovery
# MAGIC
# MAGIC ### Common Error Scenarios
# MAGIC
# MAGIC #### 1. Transient Network Failures
# MAGIC
# MAGIC **Error Example:**
# MAGIC ```
# MAGIC Connection timeout to SQL Server at 10.0.1.50:1433
# MAGIC ```
# MAGIC
# MAGIC **Handling Strategy:**
# MAGIC - **Automatic Retry**: Lakeflow Connect retries 3x with exponential backoff
# MAGIC - **Checkpoint Preservation**: No data loss, resumes from last position
# MAGIC - **Alert Threshold**: Notify if fails after all retries
# MAGIC
# MAGIC **Manual Recovery:**
# MAGIC ```python
# MAGIC # Check network connectivity
# MAGIC %sh
# MAGIC nc -zv sql-server-host 1433
# MAGIC
# MAGIC # Verify gateway status
# MAGIC %sql
# MAGIC SELECT * FROM system.lakeflow.gateway_status 
# MAGIC WHERE gateway_name = 'retail_ingestion_gateway';
# MAGIC
# MAGIC # Restart pipeline after connectivity restored
# MAGIC ```
# MAGIC
# MAGIC #### 2. Schema Evolution Conflicts
# MAGIC
# MAGIC **Error Example:**
# MAGIC ```
# MAGIC Schema mismatch: Column 'email_verified' not found in target table
# MAGIC ```
# MAGIC
# MAGIC **Handling Strategy:**
# MAGIC - **Schema Auto-Merge**: Enable schema evolution in DLT pipeline
# MAGIC ```sql
# MAGIC ALTER TABLE bronze.customers 
# MAGIC SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');
# MAGIC ```
# MAGIC
# MAGIC - **Manual Schema Sync**:
# MAGIC ```sql
# MAGIC -- Add missing column to bronze table
# MAGIC ALTER TABLE bronze.customers 
# MAGIC ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;
# MAGIC
# MAGIC -- Re-run pipeline
# MAGIC ```
# MAGIC
# MAGIC #### 3. Data Quality Issues
# MAGIC
# MAGIC **Error Example:**
# MAGIC ```
# MAGIC Constraint violation: NULL value in NOT NULL column 'customer_id'
# MAGIC ```
# MAGIC
# MAGIC **Handling Strategy:**
# MAGIC - **Quarantine Bad Records**:
# MAGIC ```python
# MAGIC # Create quarantine table for invalid records
# MAGIC CREATE TABLE bronze.customers_quarantine (
# MAGIC   original_record STRING,
# MAGIC   error_message STRING,
# MAGIC   quarantine_timestamp TIMESTAMP,
# MAGIC   validation_failure_reason STRING
# MAGIC );
# MAGIC
# MAGIC # DLT pipeline with error handling
# MAGIC @dlt.table
# MAGIC def customers_validated():
# MAGIC     return (
# MAGIC         dlt.read_stream("customers_raw")
# MAGIC         .withColumn("is_valid", 
# MAGIC                     col("customer_id").isNotNull() & 
# MAGIC                     col("email").rlike("^[\\w.-]+@[\\w.-]+\\.\\w+$"))
# MAGIC     )
# MAGIC
# MAGIC # Route invalid records to quarantine
# MAGIC @dlt.table
# MAGIC def customers_quarantine():
# MAGIC     return dlt.read_stream("customers_validated").filter(col("is_valid") == False)
# MAGIC
# MAGIC # Load only valid records to bronze
# MAGIC @dlt.table
# MAGIC def customers_bronze():
# MAGIC     return dlt.read_stream("customers_validated").filter(col("is_valid") == True)
# MAGIC ```
# MAGIC
# MAGIC #### 4. Checkpoint Corruption
# MAGIC
# MAGIC **Error Example:**
# MAGIC ```
# MAGIC Unable to read checkpoint metadata
# MAGIC ```
# MAGIC
# MAGIC **Recovery Procedure:**
# MAGIC ```
# MAGIC 1. Stop the pipeline
# MAGIC 2. Backup current bronze tables (just in case)
# MAGIC 3. Reset checkpoint to last known good LSN
# MAGIC 4. OR re-run in snapshot mode if LSN unknown
# MAGIC 5. Validate data completeness
# MAGIC 6. Resume normal operations
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disaster Recovery Planning
# MAGIC
# MAGIC ### Recovery Objectives
# MAGIC
# MAGIC **Define Clear Targets:**
# MAGIC - **RTO (Recovery Time Objective)**: How quickly to restore service
# MAGIC   - Example: Restore CDC pipeline within 4 hours of failure
# MAGIC - **RPO (Recovery Point Objective)**: Maximum acceptable data loss
# MAGIC   - Example: Lose at most 1 hour of transaction data
# MAGIC
# MAGIC ### Backup Strategy
# MAGIC
# MAGIC #### 1. Bronze Table Backups
# MAGIC
# MAGIC **Daily Snapshots:**
# MAGIC ```sql
# MAGIC -- Create daily backup using CLONE
# MAGIC CREATE TABLE bronze.customers_backup_20240115
# MAGIC DEEP CLONE bronze.customers;
# MAGIC
# MAGIC -- Automated backup job (run daily at 2 AM)
# MAGIC CREATE OR REPLACE FUNCTION backup_bronze_tables()
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC AS $$
# MAGIC   DECLARE
# MAGIC     backup_suffix STRING := DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd');
# MAGIC   BEGIN
# MAGIC     EXECUTE IMMEDIATE 'CREATE TABLE bronze.customers_backup_' || backup_suffix || 
# MAGIC                      ' DEEP CLONE bronze.customers';
# MAGIC     EXECUTE IMMEDIATE 'CREATE TABLE bronze.orders_backup_' || backup_suffix || 
# MAGIC                      ' DEEP CLONE bronze.orders';
# MAGIC     RETURN 'Backup completed: ' || backup_suffix;
# MAGIC   END;
# MAGIC $$;
# MAGIC ```
# MAGIC
# MAGIC **Retention Policy:**
# MAGIC ```sql
# MAGIC -- Keep daily backups for 30 days
# MAGIC -- Keep weekly backups for 90 days
# MAGIC -- Keep monthly backups for 1 year
# MAGIC
# MAGIC DROP TABLE IF EXISTS bronze.customers_backup_20231215;  -- Older than 30 days
# MAGIC ```
# MAGIC
# MAGIC #### 2. Checkpoint Metadata Backup
# MAGIC
# MAGIC **Export Checkpoints:**
# MAGIC ```sql
# MAGIC -- Daily export of checkpoint metadata
# MAGIC CREATE TABLE system.lakeflow.checkpoint_backups AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   CURRENT_TIMESTAMP() as backup_timestamp
# MAGIC FROM system.lakeflow.checkpoints;
# MAGIC ```
# MAGIC
# MAGIC #### 3. Configuration Backup
# MAGIC
# MAGIC **Document as Code:**
# MAGIC ```python
# MAGIC # Export pipeline configurations to version control
# MAGIC pipeline_config = {
# MAGIC     "gateway_name": "retail_ingestion_gateway",
# MAGIC     "connection_name": "sqlserver_retail_connection",
# MAGIC     "source_schema": "dbo",
# MAGIC     "target_catalog": "retail_analytics",
# MAGIC     "target_schema": "bronze",
# MAGIC     "tables": ["customers", "orders", "products"],
# MAGIC     "schedule": "0 */15 * * *"  # Every 15 minutes
# MAGIC }
# MAGIC
# MAGIC # Store in Git repository for disaster recovery
# MAGIC ```
# MAGIC
# MAGIC ### Recovery Procedures
# MAGIC
# MAGIC #### Scenario 1: Bronze Table Corruption
# MAGIC
# MAGIC **Recovery Steps:**
# MAGIC ```sql
# MAGIC -- 1. Identify corruption timestamp
# MAGIC DESCRIBE HISTORY bronze.customers;
# MAGIC
# MAGIC -- 2. Restore from most recent backup before corruption
# MAGIC DROP TABLE IF EXISTS bronze.customers;
# MAGIC CREATE TABLE bronze.customers 
# MAGIC SHALLOW CLONE bronze.customers_backup_20240115;
# MAGIC
# MAGIC -- 3. OR time travel to before corruption
# MAGIC CREATE OR REPLACE TABLE bronze.customers AS
# MAGIC SELECT * FROM bronze.customers VERSION AS OF 12345;
# MAGIC
# MAGIC -- 4. Reset checkpoint to backup restore point
# MAGIC -- 5. Resume pipeline
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 2: Complete Pipeline Failure
# MAGIC
# MAGIC **Recovery Steps:**
# MAGIC ```
# MAGIC 1. Check SQL Server health (CDC still enabled?)
# MAGIC 2. Verify network connectivity
# MAGIC 3. Recreate ingestion gateway if necessary
# MAGIC 4. Restore checkpoint from backup
# MAGIC 5. Run in incremental mode if checkpoint valid
# MAGIC 6. OR run snapshot mode if checkpoint too old
# MAGIC 7. Validate data completeness
# MAGIC 8. Resume scheduled operations
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 3: SQL Server Database Restore
# MAGIC
# MAGIC **Impact:** SQL Server restored from backup, CDC metadata lost
# MAGIC
# MAGIC **Recovery Steps:**
# MAGIC ```sql
# MAGIC -- 1. Re-enable CDC on SQL Server
# MAGIC EXEC sys.sp_cdc_enable_db;
# MAGIC EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'customers';
# MAGIC
# MAGIC -- 2. Run full snapshot in Lakeflow Connect
# MAGIC -- This will re-baseline all data
# MAGIC
# MAGIC -- 3. Compare record counts
# MAGIC SELECT COUNT(*) FROM dbo.customers;  -- SQL Server
# MAGIC SELECT COUNT(*) FROM bronze.customers;  -- Databricks
# MAGIC
# MAGIC -- 4. Resume incremental mode
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operational Runbook
# MAGIC
# MAGIC ### Daily Operations
# MAGIC
# MAGIC **Morning Health Check (10 minutes):**
# MAGIC ```
# MAGIC 1. Review overnight pipeline runs
# MAGIC    - Check Slack #data-alerts for failures
# MAGIC    - Review daily summary email
# MAGIC
# MAGIC 2. Validate data freshness
# MAGIC    - Checkpoint age < 30 minutes
# MAGIC    - Row counts increasing normally
# MAGIC
# MAGIC 3. Check resource utilization
# MAGIC    - Staging volume < 80% capacity
# MAGIC    - Gateway CPU/memory normal
# MAGIC
# MAGIC 4. Review anomaly reports
# MAGIC    - Change volume within expected range
# MAGIC    - No schema drift detected
# MAGIC ```
# MAGIC
# MAGIC ### Weekly Operations
# MAGIC
# MAGIC **Performance Review (30 minutes):**
# MAGIC ```
# MAGIC 1. Analyze pipeline performance trends
# MAGIC    - Average duration per run
# MAGIC    - Change volume patterns
# MAGIC    - Error rate trends
# MAGIC
# MAGIC 2. Review and optimize slow tables
# MAGIC    - Identify tables with growing latency
# MAGIC    - Run OPTIMIZE with Z-ORDER
# MAGIC
# MAGIC 3. Validate data quality
# MAGIC    - Row count reconciliation report
# MAGIC    - Quarantine table review
# MAGIC
# MAGIC 4. Check upcoming capacity needs
# MAGIC    - Storage growth projection
# MAGIC    - Plan for scaling if needed
# MAGIC ```
# MAGIC
# MAGIC ### Monthly Operations
# MAGIC
# MAGIC **Comprehensive Review (2 hours):**
# MAGIC ```
# MAGIC 1. Full snapshot validation
# MAGIC    - Run snapshot mode on one table
# MAGIC    - Compare with incremental results
# MAGIC
# MAGIC 2. Disaster recovery test
# MAGIC    - Restore from backup in non-prod
# MAGIC    - Verify recovery procedures
# MAGIC
# MAGIC 3. Performance optimization
# MAGIC    - Review and tune gateway sizing
# MAGIC    - Adjust pipeline schedules
# MAGIC
# MAGIC 4. Documentation update
# MAGIC    - Update runbook with new learnings
# MAGIC    - Refresh architecture diagrams
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Deployment Example
# MAGIC
# MAGIC ### Deployment Workflow
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │  Step 1: Development Environment                        │
# MAGIC │  - Build and test with sample data                      │
# MAGIC │  - Validate logic and transformations                   │
# MAGIC │  - Document configuration                               │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                        ↓
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │  Step 2: Staging Environment                            │
# MAGIC │  - Connect to production SQL Server replica             │
# MAGIC │  - Run snapshot mode for full validation                │
# MAGIC │  - Performance testing with real data volumes           │
# MAGIC │  - Stakeholder review and approval                      │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                        ↓
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │  Step 3: Production Deployment                          │
# MAGIC │  - Deploy during maintenance window                     │
# MAGIC │  - Run initial snapshot                                 │
# MAGIC │  - Switch to incremental mode                           │
# MAGIC │  - Monitor closely for 24 hours                         │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                        ↓
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │  Step 4: Post-Deployment                                │
# MAGIC │  - Enable alerting                                      │
# MAGIC │  - Schedule daily validation jobs                       │
# MAGIC │  - Train operations team on runbook                     │
# MAGIC │  - Document lessons learned                             │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You Learned
# MAGIC
# MAGIC In this lecture, you learned:
# MAGIC
# MAGIC ✅ **Production Readiness**: Comprehensive checklist covering infrastructure, monitoring, and disaster recovery
# MAGIC
# MAGIC ✅ **Monitoring Strategy**: Key metrics to track
# MAGIC   - Pipeline health (status, duration)
# MAGIC   - Data freshness (checkpoint age)
# MAGIC   - Volume trends (change patterns)
# MAGIC   - Error rates (quality issues)
# MAGIC
# MAGIC ✅ **Alerting Architecture**: Multi-tier approach
# MAGIC   - Critical alerts for immediate response
# MAGIC   - Warning alerts for investigation
# MAGIC   - Informational daily summaries
# MAGIC
# MAGIC ✅ **Performance Optimization**: SQL Server and Databricks tuning
# MAGIC   - Transaction log management
# MAGIC   - Z-ordering and liquid clustering
# MAGIC   - Auto-optimize settings
# MAGIC   - Resource right-sizing
# MAGIC
# MAGIC ✅ **Error Handling**: Recovery procedures for common failures
# MAGIC   - Network issues
# MAGIC   - Schema evolution
# MAGIC   - Data quality problems
# MAGIC   - Checkpoint corruption
# MAGIC
# MAGIC ✅ **Disaster Recovery**: Backup and restore strategies
# MAGIC   - Bronze table backups
# MAGIC   - Checkpoint metadata preservation
# MAGIC   - Recovery procedures
# MAGIC   - RTO/RPO planning
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC In **Lab 8: Implementing Scheduled Pipelines and Error Handling**, you'll:
# MAGIC - Configure pipeline scheduling for production
# MAGIC - Implement monitoring queries and alerts
# MAGIC - Test error handling and recovery scenarios
# MAGIC - Build operational dashboards
# MAGIC
# MAGIC **Ready to operationalize your CDC pipeline? Let's implement production patterns!** 🚀
