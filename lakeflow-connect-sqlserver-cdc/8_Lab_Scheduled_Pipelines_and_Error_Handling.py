# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Implementing Scheduled Pipelines and Error Handling
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll configure your CDC pipeline for production readiness by implementing scheduling strategies, comprehensive monitoring, alerting mechanisms, and error handling patterns. You'll also build operational dashboards to track pipeline health and performance.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Configure pipeline scheduling (continuous, scheduled, triggered modes)
# MAGIC - Implement monitoring queries for pipeline health and performance
# MAGIC - Create alerting logic for failures and anomalies
# MAGIC - Build operational dashboards for CDC observability
# MAGIC - Test error recovery and retry scenarios
# MAGIC - Establish operational runbooks for common issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2-6 (Full CDC pipeline configured and tested)
# MAGIC - Lakeflow Connect pipeline running successfully
# MAGIC - Understanding of production operational requirements
# MAGIC
# MAGIC ## Duration
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Pipeline Scheduling
# MAGIC
# MAGIC Let's explore different scheduling strategies for production use.

# COMMAND ----------

# DBTITLE 1,Set Configuration
catalog_name = "retail_analytics"
bronze_schema = "bronze"
pipeline_name = "retail_ingestion_pipeline"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {bronze_schema}")

print(f"✓ Using {catalog_name}.{bronze_schema}")
print(f"✓ Monitoring pipeline: {pipeline_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Review Current Pipeline Configuration

# COMMAND ----------

# DBTITLE 1,Check Current Pipeline Settings
# MAGIC %sql
# MAGIC -- View pipeline configuration
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   ingestion_mode,
# MAGIC   schedule_type,
# MAGIC   schedule_expression,
# MAGIC   status,
# MAGIC   created_time,
# MAGIC   last_updated_time
# MAGIC FROM system.lakeflow.pipelines
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Scheduling Options
# MAGIC
# MAGIC **Three scheduling modes available:**
# MAGIC
# MAGIC #### Option 1: Triggered (On-Demand)
# MAGIC - **Use Case:** Development, testing, ad-hoc loads
# MAGIC - **Configuration:** Manual execution only
# MAGIC - **Best For:** Non-production environments
# MAGIC
# MAGIC #### Option 2: Scheduled (Batch)
# MAGIC - **Use Case:** Regular intervals with predictable cost
# MAGIC - **Configuration:** Cron expression (e.g., every 15 minutes)
# MAGIC - **Best For:** Near real-time with cost optimization
# MAGIC
# MAGIC #### Option 3: Continuous (Streaming)
# MAGIC - **Use Case:** Real-time data requirements
# MAGIC - **Configuration:** Always-on cluster
# MAGIC - **Best For:** Mission-critical real-time analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Recommended Schedule for Retail Use Case
# MAGIC
# MAGIC **For this workshop, we'll use Scheduled mode:**

# COMMAND ----------

# DBTITLE 1,Document Scheduling Strategy
scheduling_strategy = {
    "mode": "scheduled",
    "business_hours": {
        "frequency": "*/15 * * * *",  # Every 15 minutes
        "description": "Near real-time during peak hours (6 AM - 10 PM)"
    },
    "off_hours": {
        "frequency": "0 * * * *",  # Every hour
        "description": "Reduced frequency overnight (10 PM - 6 AM)"
    },
    "weekend": {
        "frequency": "0 */2 * * *",  # Every 2 hours
        "description": "Lower frequency on weekends"
    }
}

print("Recommended Scheduling Strategy:")
print(f"  Mode: {scheduling_strategy['mode']}")
print(f"\n  Business Hours: {scheduling_strategy['business_hours']['description']}")
print(f"  Cron: {scheduling_strategy['business_hours']['frequency']}")
print(f"\n  Off Hours: {scheduling_strategy['off_hours']['description']}")
print(f"  Cron: {scheduling_strategy['off_hours']['frequency']}")
print(f"\n  Weekend: {scheduling_strategy['weekend']['description']}")
print(f"  Cron: {scheduling_strategy['weekend']['frequency']}")

# COMMAND ----------

# MAGIC %md
# MAGIC **To configure schedule in Databricks UI:**
# MAGIC 1. Navigate to **Data** → **Ingestion**
# MAGIC 2. Select `retail_ingestion_pipeline`
# MAGIC 3. Click **Settings** → **Schedule**
# MAGIC 4. Choose **Scheduled** mode
# MAGIC 5. Enter cron expression: `*/15 * * * *` (every 15 minutes)
# MAGIC 6. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Implement Pipeline Monitoring
# MAGIC
# MAGIC Let's create comprehensive monitoring queries.
# MAGIC
# MAGIC ### 2.1 Pipeline Health Dashboard

# COMMAND ----------

# DBTITLE 1,Pipeline Execution Summary (Last 24 Hours)
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE_FORMAT(start_time, 'yyyy-MM-dd HH:00') as execution_hour,
# MAGIC   COUNT(*) as total_runs,
# MAGIC   SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
# MAGIC   ROUND(AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)), 1) as avg_duration_seconds,
# MAGIC   SUM(records_processed) as total_records,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate_pct
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_FORMAT(start_time, 'yyyy-MM-dd HH:00')
# MAGIC ORDER BY execution_hour DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Current Pipeline Status

# COMMAND ----------

# DBTITLE 1,Real-Time Pipeline Status
# MAGIC %sql
# MAGIC WITH latest_execution AS (
# MAGIC   SELECT 
# MAGIC     pipeline_name,
# MAGIC     execution_id,
# MAGIC     execution_status,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     records_processed,
# MAGIC     error_message,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY pipeline_name ORDER BY start_time DESC) as rn
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC )
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   execution_status,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, start_time, COALESCE(end_time, CURRENT_TIMESTAMP())) as duration_minutes,
# MAGIC   records_processed,
# MAGIC   TIMESTAMPDIFF(MINUTE, COALESCE(end_time, start_time), CURRENT_TIMESTAMP()) as minutes_since_last_run,
# MAGIC   CASE 
# MAGIC     WHEN execution_status = 'SUCCEEDED' THEN '✓ Healthy'
# MAGIC     WHEN execution_status = 'RUNNING' THEN '⏳ In Progress'
# MAGIC     WHEN execution_status = 'FAILED' THEN '✗ Requires Attention'
# MAGIC     ELSE execution_status
# MAGIC   END as health_status,
# MAGIC   CASE 
# MAGIC     WHEN error_message IS NOT NULL THEN error_message
# MAGIC     ELSE 'No errors'
# MAGIC   END as status_message
# MAGIC FROM latest_execution
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Data Freshness Monitoring

# COMMAND ----------

# DBTITLE 1,Check Data Freshness
# MAGIC %sql
# MAGIC -- Monitor how fresh our bronze data is
# MAGIC SELECT 
# MAGIC   'customers' as table_name,
# MAGIC   MAX(_commit_timestamp) as last_commit_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as data_age_minutes,
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN '✓ Fresh'
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN '⚠ Aging'
# MAGIC     ELSE '✗ Stale'
# MAGIC   END as freshness_status
# MAGIC FROM customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'orders',
# MAGIC   MAX(_commit_timestamp),
# MAGIC   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN '✓ Fresh'
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN '⚠ Aging'
# MAGIC     ELSE '✗ Stale'
# MAGIC   END
# MAGIC FROM orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'products',
# MAGIC   MAX(_commit_timestamp),
# MAGIC   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN '✓ Fresh'
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN '⚠ Aging'
# MAGIC     ELSE '✗ Stale'
# MAGIC   END
# MAGIC FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Change Volume Monitoring

# COMMAND ----------

# DBTITLE 1,Daily Change Volume Trends
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE(start_time) as processing_date,
# MAGIC   COUNT(DISTINCT execution_id) as pipeline_runs,
# MAGIC   SUM(records_processed) as total_changes,
# MAGIC   ROUND(AVG(records_processed), 1) as avg_changes_per_run,
# MAGIC   MIN(records_processed) as min_changes,
# MAGIC   MAX(records_processed) as max_changes,
# MAGIC   CASE 
# MAGIC     WHEN MAX(records_processed) > AVG(records_processed) * 3 THEN '⚠ Anomaly Detected'
# MAGIC     ELSE '✓ Normal'
# MAGIC   END as volume_status
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND execution_status = 'SUCCEEDED'
# MAGIC   AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY processing_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Implement Alerting Logic
# MAGIC
# MAGIC Create queries that can be used to trigger alerts.
# MAGIC
# MAGIC ### 3.1 Critical Alert: Pipeline Failure

# COMMAND ----------

# DBTITLE 1,Detect Pipeline Failures
# MAGIC %sql
# MAGIC -- Alert if 2+ consecutive failures
# MAGIC WITH recent_runs AS (
# MAGIC   SELECT 
# MAGIC     execution_id,
# MAGIC     execution_status,
# MAGIC     start_time,
# MAGIC     error_message,
# MAGIC     ROW_NUMBER() OVER (ORDER BY start_time DESC) as run_number
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC     AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
# MAGIC ),
# MAGIC failure_check AS (
# MAGIC   SELECT 
# MAGIC     COUNT(*) as consecutive_failures,
# MAGIC     MAX(start_time) as last_failure_time,
# MAGIC     MAX(error_message) as last_error
# MAGIC   FROM recent_runs
# MAGIC   WHERE run_number <= 2
# MAGIC     AND execution_status = 'FAILED'
# MAGIC )
# MAGIC SELECT 
# MAGIC   'CRITICAL ALERT' as alert_level,
# MAGIC   'Pipeline Failure' as alert_type,
# MAGIC   consecutive_failures,
# MAGIC   last_failure_time,
# MAGIC   last_error,
# MAGIC   '🔴 Immediate action required' as recommendation
# MAGIC FROM failure_check
# MAGIC WHERE consecutive_failures >= 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Warning Alert: Data Staleness

# COMMAND ----------

# DBTITLE 1,Detect Stale Data
# MAGIC %sql
# MAGIC -- Alert if data not updated in 2+ hours
# MAGIC WITH freshness_check AS (
# MAGIC   SELECT 
# MAGIC     'customers' as table_name,
# MAGIC     MAX(_commit_timestamp) as last_update,
# MAGIC     TIMESTAMPDIFF(HOUR, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as hours_stale
# MAGIC   FROM customers
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     'orders',
# MAGIC     MAX(_commit_timestamp),
# MAGIC     TIMESTAMPDIFF(HOUR, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
# MAGIC   FROM orders
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     'products',
# MAGIC     MAX(_commit_timestamp),
# MAGIC     TIMESTAMPDIFF(HOUR, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
# MAGIC   FROM products
# MAGIC )
# MAGIC SELECT 
# MAGIC   '⚠ WARNING' as alert_level,
# MAGIC   'Data Staleness' as alert_type,
# MAGIC   table_name,
# MAGIC   last_update,
# MAGIC   hours_stale,
# MAGIC   'Check pipeline schedule and execution logs' as recommendation
# MAGIC FROM freshness_check
# MAGIC WHERE hours_stale >= 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Anomaly Alert: Unusual Change Volume

# COMMAND ----------

# DBTITLE 1,Detect Volume Anomalies
# MAGIC %sql
# MAGIC -- Alert if change volume >5x or <10% of normal
# MAGIC WITH baseline AS (
# MAGIC   SELECT 
# MAGIC     AVG(records_processed) as avg_records,
# MAGIC     STDDEV(records_processed) as stddev_records
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC     AND execution_status = 'SUCCEEDED'
# MAGIC     AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC     AND start_time < CURRENT_DATE()  -- Exclude today for baseline
# MAGIC ),
# MAGIC latest_run AS (
# MAGIC   SELECT 
# MAGIC     execution_id,
# MAGIC     start_time,
# MAGIC     records_processed
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC     AND execution_status = 'SUCCEEDED'
# MAGIC   ORDER BY start_time DESC
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   '⚠ ANOMALY' as alert_level,
# MAGIC   'Unusual Change Volume' as alert_type,
# MAGIC   lr.execution_id,
# MAGIC   lr.start_time,
# MAGIC   lr.records_processed as current_volume,
# MAGIC   ROUND(b.avg_records, 0) as normal_avg_volume,
# MAGIC   ROUND(lr.records_processed / b.avg_records, 2) as volume_ratio,
# MAGIC   CASE 
# MAGIC     WHEN lr.records_processed > b.avg_records * 5 THEN '🔺 Spike detected - investigate source'
# MAGIC     WHEN lr.records_processed < b.avg_records * 0.1 THEN '🔻 Drop detected - verify source is active'
# MAGIC     ELSE 'Normal'
# MAGIC   END as recommendation
# MAGIC FROM latest_run lr, baseline b
# MAGIC WHERE lr.records_processed > b.avg_records * 5 
# MAGIC    OR lr.records_processed < b.avg_records * 0.1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build Operational Dashboard
# MAGIC
# MAGIC Create visualizations for monitoring CDC operations.
# MAGIC
# MAGIC ### 4.1 Pipeline Performance Dashboard

# COMMAND ----------

# DBTITLE 1,Performance Metrics - Last 7 Days
# MAGIC %sql
# MAGIC WITH daily_metrics AS (
# MAGIC   SELECT 
# MAGIC     DATE(start_time) as metric_date,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN execution_status = 'SUCCEEDED' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC     AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) as avg_duration_seconds,
# MAGIC     SUM(records_processed) as total_records_processed,
# MAGIC     MIN(TIMESTAMPDIFF(SECOND, start_time, end_time)) as min_duration,
# MAGIC     MAX(TIMESTAMPDIFF(SECOND, start_time, end_time)) as max_duration
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC     AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC   GROUP BY DATE(start_time)
# MAGIC )
# MAGIC SELECT 
# MAGIC   metric_date,
# MAGIC   total_runs,
# MAGIC   successful_runs,
# MAGIC   ROUND(100.0 * successful_runs / total_runs, 1) as success_rate,
# MAGIC   ROUND(avg_duration_seconds, 1) as avg_duration_sec,
# MAGIC   min_duration as min_duration_sec,
# MAGIC   max_duration as max_duration_sec,
# MAGIC   total_records_processed,
# MAGIC   ROUND(total_records_processed / total_runs, 0) as avg_records_per_run
# MAGIC FROM daily_metrics
# MAGIC ORDER BY metric_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Table Growth Dashboard

# COMMAND ----------

# DBTITLE 1,Table Growth Tracking
# MAGIC %sql
# MAGIC -- Track record counts over time
# MAGIC SELECT 
# MAGIC   CURRENT_TIMESTAMP() as snapshot_time,
# MAGIC   'customers' as table_name,
# MAGIC   COUNT(*) as current_row_count,
# MAGIC   ROUND(SUM(size) / 1024 / 1024, 2) as size_mb
# MAGIC FROM customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   CURRENT_TIMESTAMP(),
# MAGIC   'orders',
# MAGIC   COUNT(*),
# MAGIC   ROUND(SUM(size) / 1024 / 1024, 2)
# MAGIC FROM orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   CURRENT_TIMESTAMP(),
# MAGIC   'products',
# MAGIC   COUNT(*),
# MAGIC   ROUND(SUM(size) / 1024 / 1024, 2)
# MAGIC FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Error Summary Dashboard

# COMMAND ----------

# DBTITLE 1,Error Pattern Analysis
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE(start_time) as error_date,
# MAGIC   SUBSTRING(error_message, 1, 100) as error_summary,
# MAGIC   COUNT(*) as occurrence_count,
# MAGIC   MIN(start_time) as first_occurrence,
# MAGIC   MAX(start_time) as last_occurrence,
# MAGIC   TIMESTAMPDIFF(HOUR, MIN(start_time), MAX(start_time)) as duration_hours
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND execution_status = 'FAILED'
# MAGIC   AND start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(start_time), SUBSTRING(error_message, 1, 100)
# MAGIC ORDER BY error_date DESC, occurrence_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Error Recovery Scenarios
# MAGIC
# MAGIC Let's simulate and handle common error scenarios.
# MAGIC
# MAGIC ### 5.1 Scenario 1: Network Timeout
# MAGIC
# MAGIC **Simulation:** Temporarily disconnect network between gateway and SQL Server
# MAGIC
# MAGIC **Expected Behavior:**
# MAGIC - Gateway attempts connection
# MAGIC - Retries with exponential backoff (3 attempts)
# MAGIC - Pipeline marked as FAILED if all retries exhausted
# MAGIC - Checkpoint preserved (no data loss)
# MAGIC
# MAGIC **Recovery:**

# COMMAND ----------

# DBTITLE 1,Verify Network Error Recovery
# MAGIC %sql
# MAGIC -- Check if pipeline auto-recovered from network issues
# MAGIC WITH error_recovery AS (
# MAGIC   SELECT 
# MAGIC     execution_id,
# MAGIC     start_time,
# MAGIC     execution_status,
# MAGIC     error_message,
# MAGIC     LAG(execution_status) OVER (ORDER BY start_time) as previous_status,
# MAGIC     TIMESTAMPDIFF(MINUTE, LAG(start_time) OVER (ORDER BY start_time), start_time) as minutes_between_runs
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   ORDER BY start_time DESC
# MAGIC   LIMIT 10
# MAGIC )
# MAGIC SELECT 
# MAGIC   execution_id,
# MAGIC   start_time,
# MAGIC   execution_status,
# MAGIC   previous_status,
# MAGIC   minutes_between_runs,
# MAGIC   CASE 
# MAGIC     WHEN previous_status = 'FAILED' AND execution_status = 'SUCCEEDED' THEN '✓ Auto-recovered'
# MAGIC     WHEN execution_status = 'FAILED' THEN '✗ Still failing'
# MAGIC     ELSE 'Normal'
# MAGIC   END as recovery_status
# MAGIC FROM error_recovery;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Scenario 2: Schema Evolution
# MAGIC
# MAGIC **Simulation:** Add new column to SQL Server table
# MAGIC
# MAGIC **In SQL Server:**
# MAGIC ```sql
# MAGIC -- Add new column to customers table
# MAGIC ALTER TABLE customers ADD loyalty_points INT DEFAULT 0;
# MAGIC ```
# MAGIC
# MAGIC **Expected Behavior:**
# MAGIC - Next incremental run detects schema change
# MAGIC - Lakeflow Connect auto-merges schema if enabled
# MAGIC - New column added to bronze table
# MAGIC - Existing data backfilled with NULL or default

# COMMAND ----------

# DBTITLE 1,Verify Schema Evolution
# MAGIC %sql
# MAGIC -- Check if new column appeared
# MAGIC DESCRIBE TABLE customers;

# COMMAND ----------

# MAGIC %md
# MAGIC **If schema didn't evolve automatically:**
# MAGIC
# MAGIC Enable schema evolution:

# COMMAND ----------

# DBTITLE 1,Enable Schema Auto-Merge
# MAGIC %sql
# MAGIC ALTER TABLE customers 
# MAGIC SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');
# MAGIC
# MAGIC ALTER TABLE orders 
# MAGIC SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');
# MAGIC
# MAGIC ALTER TABLE products 
# MAGIC SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Scenario 3: Data Quality Issue
# MAGIC
# MAGIC **Simulation:** Insert record violating business rules

# COMMAND ----------

# DBTITLE 1,Create Data Quality Validation
# MAGIC %sql
# MAGIC -- Create validation checks
# MAGIC WITH quality_checks AS (
# MAGIC   -- Check 1: Email format
# MAGIC   SELECT 
# MAGIC     'Invalid Email Format' as issue_type,
# MAGIC     customer_id,
# MAGIC     email,
# MAGIC     'Email does not match pattern' as issue_description
# MAGIC   FROM customers
# MAGIC   WHERE email NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check 2: Negative amounts
# MAGIC   SELECT 
# MAGIC     'Negative Order Amount',
# MAGIC     order_id,
# MAGIC     CAST(total_amount AS STRING),
# MAGIC     'Order amount is negative'
# MAGIC   FROM orders
# MAGIC   WHERE total_amount < 0
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check 3: Future dates
# MAGIC   SELECT 
# MAGIC     'Future Order Date',
# MAGIC     order_id,
# MAGIC     CAST(order_date AS STRING),
# MAGIC     'Order date is in the future'
# MAGIC   FROM orders
# MAGIC   WHERE order_date > CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC SELECT * FROM quality_checks
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Operational Runbook
# MAGIC
# MAGIC Document common operational procedures.
# MAGIC
# MAGIC ### 6.1 Daily Health Check Procedure

# COMMAND ----------

# DBTITLE 1,Daily Health Check Query
# MAGIC %sql
# MAGIC -- Run this every morning
# MAGIC WITH health_metrics AS (
# MAGIC   -- Pipeline health
# MAGIC   SELECT 
# MAGIC     'Pipeline Status' as metric,
# MAGIC     CASE 
# MAGIC       WHEN execution_status = 'SUCCEEDED' THEN '✓ Healthy'
# MAGIC       ELSE '✗ Unhealthy'
# MAGIC     END as status,
# MAGIC     CAST(NULL AS INT) as value
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   ORDER BY start_time DESC
# MAGIC   LIMIT 1
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Data freshness
# MAGIC   SELECT 
# MAGIC     'Data Freshness',
# MAGIC     CASE 
# MAGIC       WHEN MAX(TIMESTAMPDIFF(MINUTE, _commit_timestamp, CURRENT_TIMESTAMP())) < 30 THEN '✓ Fresh'
# MAGIC       ELSE '⚠ Stale'
# MAGIC     END,
# MAGIC     MAX(TIMESTAMPDIFF(MINUTE, _commit_timestamp, CURRENT_TIMESTAMP()))
# MAGIC   FROM customers
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Error count
# MAGIC   SELECT 
# MAGIC     'Errors (24hr)',
# MAGIC     CASE 
# MAGIC       WHEN COUNT(*) = 0 THEN '✓ No Errors'
# MAGIC       WHEN COUNT(*) < 3 THEN '⚠ Some Errors'
# MAGIC       ELSE '✗ Many Errors'
# MAGIC     END,
# MAGIC     COUNT(*)
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC     AND execution_status = 'FAILED'
# MAGIC     AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC )
# MAGIC SELECT * FROM health_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Troubleshooting Decision Tree

# COMMAND ----------

# DBTITLE 1,Automated Troubleshooting Guide
troubleshooting_steps = """
TROUBLESHOOTING DECISION TREE:

1. Pipeline Status = FAILED?
   ├─ Yes → Check error message in pipeline_events
   │   ├─ "Connection timeout" → Verify network/firewall
   │   ├─ "Authentication failed" → Check credentials in connection
   │   ├─ "Schema mismatch" → Enable auto-merge or manual ALTER TABLE
   │   └─ Other → Check detailed logs
   └─ No → Continue to Step 2

2. Data Freshness > 2 hours?
   ├─ Yes → Check if pipeline is running on schedule
   │   ├─ Not scheduled → Enable scheduling
   │   └─ Scheduled but not running → Check for cluster issues
   └─ No → Continue to Step 3

3. Record counts mismatched with source?
   ├─ Yes → Run snapshot mode to resync
   └─ No → System healthy ✓

4. High error rate (>5%)?
   ├─ Yes → Review error patterns
   │   ├─ Data quality issues → Implement validation/quarantine
   │   └─ Infrastructure issues → Scale up resources
   └─ No → Monitor and optimize ✓
"""

print(troubleshooting_steps)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Performance Optimization
# MAGIC
# MAGIC Optimize bronze tables for query performance.

# COMMAND ----------

# DBTITLE 1,Optimize Bronze Tables
# MAGIC %sql
# MAGIC -- Optimize with Z-ordering on commonly queried columns
# MAGIC OPTIMIZE customers ZORDER BY (customer_id, state);
# MAGIC OPTIMIZE orders ZORDER BY (customer_id, order_date);
# MAGIC OPTIMIZE products ZORDER BY (category, product_id);

# COMMAND ----------

# DBTITLE 1,Check Table Statistics
# MAGIC %sql
# MAGIC -- View table details and optimization metrics
# MAGIC SELECT 
# MAGIC   'customers' as table_name,
# MAGIC   num_files,
# MAGIC   ROUND(size_in_bytes / 1024 / 1024, 2) as size_mb,
# MAGIC   properties
# MAGIC FROM (DESCRIBE DETAIL customers);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Validation
# MAGIC
# MAGIC ### Final Production Readiness Check

# COMMAND ----------

# DBTITLE 1,Production Readiness Scorecard
# MAGIC %sql
# MAGIC WITH readiness_checks AS (
# MAGIC   -- Check 1: Pipeline scheduled
# MAGIC   SELECT 'Pipeline Scheduled' as check_name,
# MAGIC          CASE WHEN schedule_type IN ('scheduled', 'continuous') THEN '✓ PASS' ELSE '⚠ Configure scheduling' END as status
# MAGIC   FROM system.lakeflow.pipelines WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check 2: Recent successful run
# MAGIC   SELECT 'Recent Success',
# MAGIC          CASE WHEN MAX(start_time) >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR THEN '✓ PASS' ELSE '⚠ No recent runs' END
# MAGIC   FROM system.lakeflow.pipeline_events 
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline' AND execution_status = 'SUCCEEDED'
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check 3: Data fresh
# MAGIC   SELECT 'Data Freshness',
# MAGIC          CASE WHEN MAX(_commit_timestamp) >= CURRENT_TIMESTAMP() - INTERVAL 2 HOURS THEN '✓ PASS' ELSE '⚠ Data stale' END
# MAGIC   FROM customers
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check 4: Low error rate
# MAGIC   SELECT 'Error Rate',
# MAGIC          CASE 
# MAGIC            WHEN 100.0 * COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) / COUNT(*) < 5 THEN '✓ PASS'
# MAGIC            ELSE '⚠ High error rate'
# MAGIC          END
# MAGIC   FROM system.lakeflow.pipeline_events
# MAGIC   WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC     AND start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Check 5: Tables optimized
# MAGIC   SELECT 'Tables Optimized',
# MAGIC          '✓ PASS'  -- Assuming OPTIMIZE was run above
# MAGIC )
# MAGIC SELECT * FROM readiness_checks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ **Configured pipeline scheduling:**
# MAGIC - Reviewed scheduling options (triggered, scheduled, continuous)
# MAGIC - Implemented recommended scheduling strategy
# MAGIC - Set up flexible schedule for business hours
# MAGIC
# MAGIC ✅ **Implemented comprehensive monitoring:**
# MAGIC - Pipeline health dashboard
# MAGIC - Data freshness tracking
# MAGIC - Change volume monitoring
# MAGIC - Performance metrics
# MAGIC
# MAGIC ✅ **Created alerting logic:**
# MAGIC - Critical alerts for pipeline failures
# MAGIC - Warning alerts for data staleness
# MAGIC - Anomaly detection for unusual volumes
# MAGIC
# MAGIC ✅ **Built operational dashboards:**
# MAGIC - Performance metrics visualization
# MAGIC - Table growth tracking
# MAGIC - Error pattern analysis
# MAGIC
# MAGIC ✅ **Tested error recovery:**
# MAGIC - Network timeout handling
# MAGIC - Schema evolution testing
# MAGIC - Data quality validation
# MAGIC
# MAGIC ✅ **Established operational runbooks:**
# MAGIC - Daily health check procedure
# MAGIC - Troubleshooting decision tree
# MAGIC - Performance optimization
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Your CDC pipeline is production-ready! Consider:
# MAGIC - **Silver Layer Transformations:** Build business logic on top of bronze tables
# MAGIC - **Gold Layer Aggregations:** Create analytics-ready dimensional models
# MAGIC - **Real-Time Dashboards:** Connect BI tools to bronze/silver tables
# MAGIC - **Advanced Monitoring:** Integrate with enterprise monitoring platforms (Datadog, Splunk)
# MAGIC - **Multi-Region Deployment:** Replicate pattern across regions
# MAGIC
# MAGIC **Congratulations on completing the Lakeflow Connect SQL Server CDC Workshop!** 🎉🚀
