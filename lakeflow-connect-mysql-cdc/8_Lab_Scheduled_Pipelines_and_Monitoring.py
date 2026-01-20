# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 8: Implementing Scheduled Pipelines and Error Handling
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this final hands-on lab, you will configure production-grade scheduling for your CDC pipelines, implement comprehensive monitoring queries, set up alerting rules, and test error handling and recovery procedures. This lab prepares your CDC pipeline for production deployment.
# MAGIC
# MAGIC **Estimated Time**: 40-50 minutes
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Configure scheduled pipeline execution for continuous synchronization
# MAGIC - Implement monitoring queries for pipeline health and performance
# MAGIC - Set up alerting rules for critical metrics
# MAGIC - Test error handling and recovery scenarios
# MAGIC - Monitor binary log lag and ingestion latency
# MAGIC - Review system catalog tables for audit and debugging
# MAGIC - Create operational dashboards for CDC pipeline monitoring
# MAGIC - Document runbook procedures for production operations
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Labs 2-7 (entire CDC workshop through multi-table scenarios)
# MAGIC - Completed Lecture 7 (Production Best Practices)
# MAGIC - Bronze tables populated and pipeline operational

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Configure Pipeline Scheduling
# MAGIC
# MAGIC Set up automatic pipeline execution for continuous CDC synchronization.
# MAGIC
# MAGIC ### Step 1.1: Access Pipeline Schedule Settings
# MAGIC
# MAGIC **In Databricks UI**:
# MAGIC 1. Navigate to **Data** → **Lakeflow Connect** → **Ingestion Pipelines**
# MAGIC 2. Select `retail_ingestion_pipeline`
# MAGIC 3. Click **Schedule** or **Settings** → **Schedule**
# MAGIC
# MAGIC ### Step 1.2: Configure Schedule Based on Latency Requirements
# MAGIC
# MAGIC **Schedule Options**:
# MAGIC
# MAGIC | Use Case | Schedule | Cron Expression | Latency |
# MAGIC |----------|----------|-----------------|---------|
# MAGIC | **Real-time critical** | Every 1 minute | `*/1 * * * *` | 1-2 min |
# MAGIC | **Near real-time** | Every 5 minutes | `*/5 * * * *` | 5-10 min |
# MAGIC | **Standard batch** | Every 15 minutes | `*/15 * * * *` | 15-30 min |
# MAGIC | **Hourly refresh** | Every hour | `0 * * * *` | 1-2 hours |
# MAGIC | **Daily batch** | Once per day | `0 2 * * *` | 24 hours |
# MAGIC
# MAGIC **For this workshop, configure**:
# MAGIC - Schedule Type: **Scheduled**
# MAGIC - Frequency: **Every 5 minutes** (`*/5 * * * *`)
# MAGIC - Timezone: Your local timezone
# MAGIC - Start Time: Immediate
# MAGIC
# MAGIC ### Step 1.3: Save and Activate Schedule

# COMMAND ----------

# Verify scheduled pipeline configuration
print("Pipeline Schedule Configuration:")
print("=" * 60)
print(f"Pipeline Name: retail_ingestion_pipeline")
print(f"Schedule: Every 5 minutes (*/5 * * * *)")
print(f"Mode: Incremental (continuous CDC)")
print(f"Expected Latency: 5-10 minutes")
print(f"Status: Active")
print("=" * 60)
print("\n✅ Schedule configured. Pipeline will run automatically every 5 minutes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Implement Pipeline Health Monitoring
# MAGIC
# MAGIC Create queries to monitor pipeline execution and health.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create monitoring view for pipeline execution history
# MAGIC CREATE OR REPLACE VIEW retail_analytics.bronze.vw_pipeline_health AS
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     update_id,
# MAGIC     timestamp as execution_time,
# MAGIC     state,
# MAGIC     message,
# MAGIC     error_message,
# MAGIC     TIMESTAMPDIFF(SECOND, LAG(timestamp) OVER (PARTITION BY pipeline_name ORDER BY timestamp), timestamp) as seconds_since_last_run
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent pipeline executions
# MAGIC SELECT 
# MAGIC     execution_time,
# MAGIC     state,
# MAGIC     message,
# MAGIC     seconds_since_last_run,
# MAGIC     error_message
# MAGIC FROM retail_analytics.bronze.vw_pipeline_health
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate pipeline success rate (last 24 hours)
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC     SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC GROUP BY pipeline_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor pipeline execution duration trend
# MAGIC WITH execution_durations AS (
# MAGIC     SELECT 
# MAGIC         update_id,
# MAGIC         MIN(timestamp) as start_time,
# MAGIC         MAX(timestamp) as end_time,
# MAGIC         TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp)) as duration_seconds
# MAGIC     FROM system.lakeflow.pipeline_events
# MAGIC     WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC       AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC     GROUP BY update_id
# MAGIC )
# MAGIC SELECT 
# MAGIC     update_id,
# MAGIC     start_time,
# MAGIC     duration_seconds,
# MAGIC     CASE 
# MAGIC         WHEN duration_seconds > 300 THEN 'SLOW (>5 min)'
# MAGIC         WHEN duration_seconds > 120 THEN 'NORMAL (2-5 min)'
# MAGIC         ELSE 'FAST (<2 min)'
# MAGIC     END as performance_status
# MAGIC FROM execution_durations
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Monitor Ingestion Lag

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create ingestion lag monitoring view
# MAGIC CREATE OR REPLACE VIEW retail_analytics.bronze.vw_ingestion_lag AS
# MAGIC SELECT 
# MAGIC     'customers' as table_name,
# MAGIC     MAX(_commit_timestamp) as last_ingested_change,
# MAGIC     CURRENT_TIMESTAMP() as current_time,
# MAGIC     TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as lag_minutes,
# MAGIC     COUNT(*) as total_records,
# MAGIC     CASE 
# MAGIC         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 60 THEN 'CRITICAL'
# MAGIC         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 30 THEN 'WARNING'
# MAGIC         ELSE 'OK'
# MAGIC     END as lag_status
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'orders',
# MAGIC     MAX(_commit_timestamp),
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC     COUNT(*),
# MAGIC     CASE 
# MAGIC         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 60 THEN 'CRITICAL'
# MAGIC         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 30 THEN 'WARNING'
# MAGIC         ELSE 'OK'
# MAGIC     END
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'products',
# MAGIC     MAX(_commit_timestamp),
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC     COUNT(*),
# MAGIC     CASE 
# MAGIC         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 60 THEN 'CRITICAL'
# MAGIC         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 30 THEN 'WARNING'
# MAGIC         ELSE 'OK'
# MAGIC     END
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current ingestion lag
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     last_ingested_change,
# MAGIC     lag_minutes,
# MAGIC     lag_status,
# MAGIC     total_records
# MAGIC FROM retail_analytics.bronze.vw_ingestion_lag
# MAGIC ORDER BY lag_minutes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Monitor Change Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track change volume over time (last 24 hours, by hour)
# MAGIC WITH hourly_changes AS (
# MAGIC     SELECT 
# MAGIC         'customers' as table_name,
# MAGIC         DATE_TRUNC('HOUR', _commit_timestamp) as hour,
# MAGIC         COUNT(*) as change_count
# MAGIC     FROM retail_analytics.bronze.customers
# MAGIC     WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC     GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC         'orders',
# MAGIC         DATE_TRUNC('HOUR', _commit_timestamp),
# MAGIC         COUNT(*)
# MAGIC     FROM retail_analytics.bronze.orders
# MAGIC     WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC     GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC         'products',
# MAGIC         DATE_TRUNC('HOUR', _commit_timestamp),
# MAGIC         COUNT(*)
# MAGIC     FROM retail_analytics.bronze.products
# MAGIC     WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC     GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC )
# MAGIC SELECT 
# MAGIC     hour,
# MAGIC     table_name,
# MAGIC     change_count,
# MAGIC     SUM(change_count) OVER (PARTITION BY table_name ORDER BY hour) as cumulative_changes
# MAGIC FROM hourly_changes
# MAGIC ORDER BY hour DESC, table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detect anomalous change volume (statistical approach)
# MAGIC WITH hourly_stats AS (
# MAGIC     SELECT 
# MAGIC         DATE_TRUNC('HOUR', _commit_timestamp) as hour,
# MAGIC         COUNT(*) as change_count
# MAGIC     FROM retail_analytics.bronze.orders
# MAGIC     WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC     GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC ),
# MAGIC stats AS (
# MAGIC     SELECT 
# MAGIC         AVG(change_count) as avg_changes,
# MAGIC         STDDEV(change_count) as stddev_changes
# MAGIC     FROM hourly_stats
# MAGIC )
# MAGIC SELECT 
# MAGIC     h.hour,
# MAGIC     h.change_count,
# MAGIC     ROUND(s.avg_changes, 2) as avg_baseline,
# MAGIC     ROUND(s.stddev_changes, 2) as stddev,
# MAGIC     CASE 
# MAGIC         WHEN h.change_count > s.avg_changes + 2 * s.stddev_changes THEN 'ANOMALY (High)'
# MAGIC         WHEN h.change_count < s.avg_changes - 2 * s.stddev_changes THEN 'ANOMALY (Low)'
# MAGIC         ELSE 'NORMAL'
# MAGIC     END as status
# MAGIC FROM hourly_stats h
# MAGIC CROSS JOIN stats s
# MAGIC WHERE h.hour >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC ORDER BY h.hour DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Implement Alerting Rules
# MAGIC
# MAGIC Create views that can be used to trigger alerts.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create comprehensive alert view
# MAGIC CREATE OR REPLACE VIEW retail_analytics.bronze.vw_cdc_alerts AS
# MAGIC
# MAGIC -- Alert 1: Pipeline failures
# MAGIC SELECT 
# MAGIC     'Pipeline Failure' as alert_type,
# MAGIC     'CRITICAL' as severity,
# MAGIC     CONCAT('Pipeline failed: ', message) as alert_message,
# MAGIC     timestamp as alert_time,
# MAGIC     pipeline_name as context
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE state = 'FAILED'
# MAGIC   AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Alert 2: High ingestion lag
# MAGIC SELECT 
# MAGIC     'High Ingestion Lag',
# MAGIC     CASE 
# MAGIC         WHEN lag_minutes > 60 THEN 'CRITICAL'
# MAGIC         ELSE 'WARNING'
# MAGIC     END,
# MAGIC     CONCAT('Table ', table_name, ' has ', lag_minutes, ' minutes lag'),
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     table_name
# MAGIC FROM retail_analytics.bronze.vw_ingestion_lag
# MAGIC WHERE lag_status IN ('WARNING', 'CRITICAL')
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Alert 3: Data quality issues
# MAGIC SELECT 
# MAGIC     'Data Quality Issue',
# MAGIC     'WARNING',
# MAGIC     CONCAT(check_name, ': ', issue_count, ' issues found'),
# MAGIC     CURRENT_TIMESTAMP(),
# MAGIC     check_name
# MAGIC FROM retail_analytics.bronze.vw_data_quality_checks
# MAGIC WHERE status = 'FAIL'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Alert 4: Slow pipeline execution
# MAGIC SELECT 
# MAGIC     'Slow Pipeline Execution',
# MAGIC     'WARNING',
# MAGIC     CONCAT('Pipeline took ', seconds_since_last_run, ' seconds'),
# MAGIC     execution_time,
# MAGIC     pipeline_name
# MAGIC FROM retail_analytics.bronze.vw_pipeline_health
# MAGIC WHERE seconds_since_last_run > 600  -- More than 10 minutes between runs
# MAGIC   AND execution_time >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View active alerts
# MAGIC SELECT 
# MAGIC     alert_type,
# MAGIC     severity,
# MAGIC     alert_message,
# MAGIC     alert_time,
# MAGIC     context
# MAGIC FROM retail_analytics.bronze.vw_cdc_alerts
# MAGIC ORDER BY 
# MAGIC     CASE severity 
# MAGIC         WHEN 'CRITICAL' THEN 1 
# MAGIC         WHEN 'WARNING' THEN 2 
# MAGIC         ELSE 3 
# MAGIC     END,
# MAGIC     alert_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Test Error Handling and Recovery
# MAGIC
# MAGIC Simulate error scenarios and verify recovery procedures.
# MAGIC
# MAGIC ### Scenario 1: Simulate Network Interruption
# MAGIC
# MAGIC While this is difficult to test directly, we can verify checkpoint recovery by reviewing the checkpoint mechanism.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View staging volume checkpoint files
# MAGIC LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/_checkpoint/';
# MAGIC
# MAGIC -- These checkpoint files track the binary log position
# MAGIC -- If gateway restarts, it resumes from last checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2: Pipeline Failure Recovery
# MAGIC
# MAGIC Review how the system handles failed pipeline runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for any historical failures and subsequent recoveries
# MAGIC WITH failures AS (
# MAGIC     SELECT 
# MAGIC         update_id,
# MAGIC         timestamp as failure_time,
# MAGIC         error_message
# MAGIC     FROM system.lakeflow.pipeline_events
# MAGIC     WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC       AND state = 'FAILED'
# MAGIC ),
# MAGIC recoveries AS (
# MAGIC     SELECT 
# MAGIC         update_id,
# MAGIC         timestamp as recovery_time
# MAGIC     FROM system.lakeflow.pipeline_events
# MAGIC     WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC       AND state = 'COMPLETED'
# MAGIC )
# MAGIC SELECT 
# MAGIC     f.update_id as failed_update_id,
# MAGIC     f.failure_time,
# MAGIC     f.error_message,
# MAGIC     r.update_id as recovered_update_id,
# MAGIC     r.recovery_time,
# MAGIC     TIMESTAMPDIFF(MINUTE, f.failure_time, r.recovery_time) as recovery_time_minutes
# MAGIC FROM failures f
# MAGIC LEFT JOIN recoveries r ON r.recovery_time > f.failure_time
# MAGIC WHERE r.recovery_time = (
# MAGIC     SELECT MIN(recovery_time) 
# MAGIC     FROM recoveries 
# MAGIC     WHERE recovery_time > f.failure_time
# MAGIC )
# MAGIC ORDER BY f.failure_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7: Create Operational Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard Query 1: Current System Status
# MAGIC SELECT 
# MAGIC     'Pipeline Health' as metric_category,
# MAGIC     state as status,
# MAGIC     MAX(timestamp) as last_execution,
# MAGIC     TIMESTAMPDIFF(MINUTE, MAX(timestamp), CURRENT_TIMESTAMP()) as minutes_since_last_run
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC GROUP BY state
# MAGIC ORDER BY last_execution DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard Query 2: Table Statistics Summary
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     total_records,
# MAGIC     lag_minutes,
# MAGIC     lag_status,
# MAGIC     last_ingested_change
# MAGIC FROM retail_analytics.bronze.vw_ingestion_lag;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard Query 3: Recent Activity (Last 4 Hours)
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('HOUR', _commit_timestamp) as hour,
# MAGIC     'customers' as table_name,
# MAGIC     COUNT(*) as changes
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 4 HOURS
# MAGIC GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('HOUR', _commit_timestamp),
# MAGIC     'orders',
# MAGIC     COUNT(*)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 4 HOURS
# MAGIC GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('HOUR', _commit_timestamp),
# MAGIC     'products',
# MAGIC     COUNT(*)
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 4 HOURS
# MAGIC GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC
# MAGIC ORDER BY hour DESC, table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard Query 4: Pipeline Performance Trend
# MAGIC SELECT 
# MAGIC     DATE(timestamp) as date,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC     SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(timestamp)
# MAGIC ORDER BY date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8: Document Production Runbook

# COMMAND ----------

# Production Runbook Documentation
runbook = {
    "Pipeline Information": {
        "Pipeline Name": "retail_ingestion_pipeline",
        "Gateway Name": "retail_ingestion_gateway",
        "Schedule": "Every 5 minutes (*/5 * * * *)",
        "Source": "MySQL retail_db",
        "Destination": "retail_analytics.bronze",
        "Tables": ["customers", "orders", "products"]
    },
    
    "Key Monitoring Queries": {
        "Pipeline Health": "SELECT * FROM retail_analytics.bronze.vw_pipeline_health",
        "Ingestion Lag": "SELECT * FROM retail_analytics.bronze.vw_ingestion_lag",
        "Active Alerts": "SELECT * FROM retail_analytics.bronze.vw_cdc_alerts",
        "Data Quality": "SELECT * FROM retail_analytics.bronze.vw_data_quality_checks"
    },
    
    "Common Issues and Resolutions": {
        "Pipeline Failure": {
            "Diagnosis": "Check system.lakeflow.pipeline_events for error_message",
            "Resolution": "Review error, fix root cause (network, credentials, schema), restart pipeline",
            "Prevention": "Implement retry logic, pre-deployment testing"
        },
        "High Ingestion Lag": {
            "Diagnosis": "Query vw_ingestion_lag, check lag_minutes",
            "Resolution": "Increase gateway VM size, check MySQL binlog retention, optimize schedule",
            "Prevention": "Monitor lag alerts, capacity planning"
        },
        "Gateway Disconnected": {
            "Diagnosis": "Check gateway status in UI, test network connectivity",
            "Resolution": "Restart gateway, verify MySQL credentials, check firewall rules",
            "Prevention": "Network redundancy, health checks"
        },
        "Orphaned Records": {
            "Diagnosis": "Run data quality checks view",
            "Resolution": "Fix referential integrity, configure table dependencies",
            "Prevention": "Enforce FK constraints, dependency mapping"
        }
    },
    
    "Emergency Contacts": {
        "On-Call Engineer": "team@company.com",
        "DBA Team": "dba@company.com",
        "Network Team": "network@company.com",
        "Databricks Support": "support@databricks.com"
    },
    
    "SLA Targets": {
        "Pipeline Success Rate": ">= 99%",
        "Ingestion Lag": "<= 10 minutes",
        "Recovery Time (RTO)": "<= 30 minutes",
        "Data Loss Tolerance (RPO)": "<= 5 minutes"
    }
}

# Display runbook
print("=" * 80)
print("CDC PIPELINE PRODUCTION RUNBOOK")
print("=" * 80)
for section, content in runbook.items():
    print(f"\n{section}:")
    print("-" * 80)
    if isinstance(content, dict):
        for key, value in content.items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for subkey, subvalue in value.items():
                    print(f"    - {subkey}: {subvalue}")
            else:
                print(f"  {key}: {value}")
    else:
        print(f"  {content}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9: Validation Checklist for Production Readiness

# COMMAND ----------

# Production readiness validation
print("Production Readiness Checklist")
print("=" * 80)

validation_checks = [
    ("Pipeline scheduled and active", "Verify in Lakeflow Connect UI"),
    ("Success rate >= 99%", "Query pipeline_events for success rate"),
    ("Ingestion lag < 10 minutes", "Query vw_ingestion_lag"),
    ("No active CRITICAL alerts", "Query vw_cdc_alerts"),
    ("Data quality checks passing", "Query vw_data_quality_checks"),
    ("Monitoring views created", "Verify views exist in bronze schema"),
    ("Runbook documented", "Review runbook content above"),
    ("Alert thresholds configured", "Verify alert rules in vw_cdc_alerts"),
    ("Error recovery tested", "Review failure/recovery scenarios"),
    ("Dashboard queries validated", "Run all dashboard queries successfully")
]

for i, (check, validation) in enumerate(validation_checks, 1):
    print(f"{i:2d}. [ ] {check:40s} - {validation}")

print("=" * 80)
print("\n✅ Complete all checklist items before production deployment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ **Pipeline Scheduling**:
# MAGIC - Configured automatic pipeline execution (every 5 minutes)
# MAGIC - Set up continuous CDC synchronization
# MAGIC - Aligned schedule with latency requirements
# MAGIC
# MAGIC ✅ **Monitoring Implementation**:
# MAGIC - Created pipeline health monitoring views
# MAGIC - Implemented ingestion lag tracking
# MAGIC - Built change volume analysis queries
# MAGIC - Configured anomaly detection for unusual patterns
# MAGIC
# MAGIC ✅ **Alerting System**:
# MAGIC - Created comprehensive alert view (vw_cdc_alerts)
# MAGIC - Configured critical, warning, and info alert levels
# MAGIC - Set up alert rules for failures, lag, and data quality
# MAGIC
# MAGIC ✅ **Error Handling**:
# MAGIC - Reviewed checkpoint recovery mechanism
# MAGIC - Tested failure and recovery scenarios
# MAGIC - Verified retry and recovery procedures
# MAGIC
# MAGIC ✅ **Operational Readiness**:
# MAGIC - Created dashboard queries for operations team
# MAGIC - Documented production runbook
# MAGIC - Established SLA targets and monitoring
# MAGIC - Validated production readiness checklist
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Scheduling enables continuous CDC** - automatic execution eliminates manual intervention
# MAGIC
# MAGIC 2. **Proactive monitoring prevents issues** - tracking lag, performance, and data quality catches problems early
# MAGIC
# MAGIC 3. **Layered alerting is essential** - critical alerts for immediate action, warnings for trend monitoring
# MAGIC
# MAGIC 4. **Checkpoint mechanism ensures reliability** - automatic recovery from failures without data loss
# MAGIC
# MAGIC 5. **Operational documentation is critical** - runbooks and dashboards enable quick troubleshooting
# MAGIC
# MAGIC ### Production Monitoring Stack
# MAGIC
# MAGIC | Layer | Component | Purpose | Update Frequency |
# MAGIC |-------|-----------|---------|------------------|
# MAGIC | **Real-time** | vw_cdc_alerts | Critical alerts | Continuous |
# MAGIC | **Operational** | vw_pipeline_health | Execution monitoring | Per run (5 min) |
# MAGIC | **Performance** | vw_ingestion_lag | Latency tracking | Per run (5 min) |
# MAGIC | **Quality** | vw_data_quality_checks | Data integrity | Hourly |
# MAGIC | **Analytics** | Dashboard queries | Trend analysis | Daily |
# MAGIC
# MAGIC ## Workshop Completion
# MAGIC
# MAGIC ### 🎉 Congratulations! You've Completed the MySQL CDC Workshop
# MAGIC
# MAGIC You have successfully:
# MAGIC
# MAGIC ✅ **Lab 2**: Configured MySQL for CDC with binary logging, GTID, and replication user
# MAGIC
# MAGIC ✅ **Lab 3**: Set up Lakeflow Connect Ingestion Gateway and Pipeline
# MAGIC
# MAGIC ✅ **Lab 5**: Validated incremental CDC with INSERT, UPDATE, DELETE operations
# MAGIC
# MAGIC ✅ **Lab 6**: Handled multi-table CDC and foreign key dependencies
# MAGIC
# MAGIC ✅ **Lab 8**: Implemented production scheduling, monitoring, and error handling
# MAGIC
# MAGIC ### What You've Built
# MAGIC
# MAGIC A **production-grade CDC pipeline** that:
# MAGIC - Continuously syncs MySQL changes to Databricks Delta Lake
# MAGIC - Maintains referential integrity across related tables
# MAGIC - Monitors health and performance with comprehensive alerting
# MAGIC - Handles failures gracefully with automatic recovery
# MAGIC - Provides complete data lineage and governance through Unity Catalog
# MAGIC
# MAGIC ### Next Steps for Production Deployment
# MAGIC
# MAGIC 1. **Security Hardening**:
# MAGIC    - Review network security (PrivateLink, VPN)
# MAGIC    - Rotate credentials and store in secrets
# MAGIC    - Configure fine-grained Unity Catalog permissions
# MAGIC
# MAGIC 2. **Performance Optimization**:
# MAGIC    - Implement Z-ordering on bronze tables
# MAGIC    - Configure Auto Optimize
# MAGIC    - Review partition strategy for large tables
# MAGIC
# MAGIC 3. **Expand CDC Pipeline**:
# MAGIC    - Add more tables from retail_db
# MAGIC    - Integrate additional MySQL databases
# MAGIC    - Build silver and gold layer transformations
# MAGIC
# MAGIC 4. **Advanced Monitoring**:
# MAGIC    - Create Databricks SQL dashboards
# MAGIC    - Integrate with external alerting (PagerDuty, Slack)
# MAGIC    - Set up custom metrics and KPIs
# MAGIC
# MAGIC 5. **Disaster Recovery**:
# MAGIC    - Document and test MySQL failover procedures
# MAGIC    - Implement binary log backup strategy
# MAGIC    - Create recovery runbooks for all scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Databricks Documentation
# MAGIC - [Lakeflow Connect Production Guide](https://docs.databricks.com/ingestion/lakeflow-connect/production.html)
# MAGIC - [Delta Live Tables Monitoring](https://docs.databricks.com/delta-live-tables/observability.html)
# MAGIC - [Unity Catalog Audit Logs](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html)
# MAGIC - [Databricks SQL Dashboards](https://docs.databricks.com/sql/user/dashboards/index.html)
# MAGIC
# MAGIC ### MySQL Documentation
# MAGIC - [MySQL Binary Log Monitoring](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
# MAGIC - [MySQL High Availability](https://dev.mysql.com/doc/refman/8.0/en/replication.html)
# MAGIC - [MySQL Performance Tuning](https://dev.mysql.com/doc/refman/8.0/en/optimization.html)
# MAGIC
# MAGIC ### Best Practices
# MAGIC - [CDC Best Practices White Paper](https://www.databricks.com/resources/webinar/change-data-capture-best-practices)
# MAGIC - [Data Engineering on Databricks](https://www.databricks.com/learn/training/data-engineering-databricks)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Thank you for completing this workshop! You're now ready to implement production CDC pipelines on Databricks.** 🚀
