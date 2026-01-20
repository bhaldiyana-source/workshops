# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 8: Implementing Scheduled Pipelines and Error Handling
# MAGIC
# MAGIC ## Lab Objectives
# MAGIC By the end of this lab, you will:
# MAGIC - Configure pipeline scheduling for automated CDC execution
# MAGIC - Implement error handling and retry strategies
# MAGIC - Set up comprehensive monitoring and alerting
# MAGIC - Create data quality checks for CDC pipelines
# MAGIC - Build operational dashboards for CDC health
# MAGIC - Document runbook procedures for common issues
# MAGIC
# MAGIC **Estimated Duration:** 30 minutes  
# MAGIC **Prerequisites:** 
# MAGIC - Completion of Labs 2-6
# MAGIC - Access to Databricks Jobs and Workflows
# MAGIC - Understanding of production operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Architecture: Production Operations
# MAGIC
# MAGIC ```
# MAGIC                   ┌──────────────────────────────────┐
# MAGIC                   │   Oracle Source Database         │
# MAGIC                   │   (Continuous transactions)      │
# MAGIC                   └──────────────────────────────────┘
# MAGIC                                  │
# MAGIC                                  │ CDC
# MAGIC                                  ▼
# MAGIC                   ┌──────────────────────────────────┐
# MAGIC                   │   Lakeflow Connect Gateway       │
# MAGIC                   │   - Continuous monitoring        │
# MAGIC                   │   - Auto-retry on failure        │
# MAGIC                   │   - Health checks                │
# MAGIC                   └──────────────────────────────────┘
# MAGIC                                  │
# MAGIC                                  ▼
# MAGIC                   ┌──────────────────────────────────┐
# MAGIC                   │   Unity Catalog Volume           │
# MAGIC                   │   (Staged CDC data)              │
# MAGIC                   └──────────────────────────────────┘
# MAGIC                                  │
# MAGIC                                  ▼
# MAGIC     ┌────────────────────────────────────────────────────────┐
# MAGIC     │        Ingestion Pipeline (This Lab)                   │
# MAGIC     │  ┌──────────────────────────────────────────────────┐  │
# MAGIC     │  │  Scheduled Execution                             │  │
# MAGIC     │  │  - Triggered mode: Every 15 minutes              │  │
# MAGIC     │  │  - OR Continuous mode: Always running            │  │
# MAGIC     │  └──────────────────────────────────────────────────┘  │
# MAGIC     │  ┌──────────────────────────────────────────────────┐  │
# MAGIC     │  │  Error Handling                                  │  │
# MAGIC     │  │  - Auto-retry transient failures (3 attempts)    │  │
# MAGIC     │  │  - Alert on persistent failures                  │  │
# MAGIC     │  │  - Log all errors for debugging                  │  │
# MAGIC     │  └──────────────────────────────────────────────────┘  │
# MAGIC     │  ┌──────────────────────────────────────────────────┐  │
# MAGIC     │  │  Data Quality Checks                             │  │
# MAGIC     │  │  - Row count reconciliation                      │  │
# MAGIC     │  │  - Referential integrity validation              │  │
# MAGIC     │  │  - Null value monitoring                         │  │
# MAGIC     │  │  - Duplicate detection                           │  │
# MAGIC     │  └──────────────────────────────────────────────────┘  │
# MAGIC     │  ┌──────────────────────────────────────────────────┐  │
# MAGIC     │  │  Monitoring & Alerting                           │  │
# MAGIC     │  │  - Real-time metrics dashboard                   │  │
# MAGIC     │  │  - Slack/Email notifications                     │  │
# MAGIC     │  │  - SLA tracking (latency < 60s)                  │  │
# MAGIC     │  └──────────────────────────────────────────────────┘  │
# MAGIC     └────────────────────────────────────────────────────────┘
# MAGIC                                  │
# MAGIC                                  ▼
# MAGIC                   ┌──────────────────────────────────┐
# MAGIC                   │   Bronze Layer Delta Tables      │
# MAGIC                   │   (Production-ready data)        │
# MAGIC                   └──────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Pipeline Scheduling
# MAGIC
# MAGIC ### Scheduling Modes Comparison
# MAGIC
# MAGIC | Mode | When to Use | Pros | Cons | Cost |
# MAGIC |------|-------------|------|------|------|
# MAGIC | **Continuous** | Real-time requirements (< 1 min latency) | Low latency, always current | Cluster always running | $$$ |
# MAGIC | **Triggered - Frequent** (5-15 min) | Near real-time (< 15 min latency) | Balance of freshness and cost | Slight delay | $$ |
# MAGIC | **Triggered - Hourly** | Hourly reporting | Cost effective | Delayed data | $ |
# MAGIC | **Triggered - Daily** | Batch processing | Very cost effective | High latency | $ |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Continuous Mode (Real-Time)
# MAGIC
# MAGIC **For always-on, real-time CDC:**

# COMMAND ----------

# Configuration for continuous mode pipeline
continuous_config = {
    "name": "retail_ingestion_pipeline",
    "continuous": True,  # Keep pipeline always running
    "clusters": [{
        "label": "default",
        "autoscale": {
            "min_workers": 1,
            "max_workers": 3,
            "mode": "ENHANCED"
        }
    }],
    "channel": "CURRENT",  # Use latest features
    "development": False  # Production mode
}

print("Continuous Mode Configuration:")
print(f"  Mode: Always-on streaming")
print(f"  Expected Latency: 10-60 seconds")
print(f"  Use Case: Real-time dashboards, operational intelligence")
print(f"  Cost: Cluster runs 24/7")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Triggered Mode (Scheduled)
# MAGIC
# MAGIC **For cost-optimized periodic updates:**

# COMMAND ----------

# Configuration for triggered mode pipeline
triggered_config = {
    "name": "retail_ingestion_pipeline",
    "continuous": False,  # Triggered mode
    "trigger": {
        "schedule": {
            # Run every 15 minutes
            "quartz_cron_expression": "0 */15 * * * ?",
            "timezone_id": "America/Los_Angeles"
        }
    },
    "clusters": [{
        "label": "default",
        "num_workers": 2  # Fixed size for predictable cost
    }],
    "max_workers": 2,
    "channel": "CURRENT",
    "development": False
}

print("Triggered Mode Configuration:")
print(f"  Schedule: Every 15 minutes")
print(f"  Expected Latency: Up to 15 minutes + processing time")
print(f"  Use Case: Regular reporting, near real-time acceptable")
print(f"  Cost: Cluster spins up only for processing (~5 min per run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Pipeline Configuration via UI
# MAGIC
# MAGIC **To change pipeline scheduling:**
# MAGIC
# MAGIC 1. Navigate to **Delta Live Tables** → Your pipeline
# MAGIC 2. Click **Settings**
# MAGIC 3. Under **Pipeline Mode**:
# MAGIC    - Select **Triggered** for scheduled execution
# MAGIC    - Or select **Continuous** for always-on
# MAGIC 4. If Triggered, click **Add Trigger**:
# MAGIC    - **Type:** Scheduled
# MAGIC    - **Cron expression:** `0 */15 * * * ?` (every 15 minutes)
# MAGIC    - **Timezone:** Your timezone
# MAGIC 5. Click **Save**
# MAGIC
# MAGIC **Common Cron Expressions:**
# MAGIC ```
# MAGIC 0 */5 * * * ?     Every 5 minutes
# MAGIC 0 */15 * * * ?    Every 15 minutes
# MAGIC 0 */30 * * * ?    Every 30 minutes
# MAGIC 0 0 * * * ?       Every hour
# MAGIC 0 0 */4 * * ?     Every 4 hours
# MAGIC 0 0 0 * * ?       Daily at midnight
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Implement Error Handling
# MAGIC
# MAGIC ### Built-in Error Handling Features

# COMMAND ----------

# Error handling configuration
error_handling_config = {
    # Automatic retries for transient failures
    "max_auto_retry_attempts": 3,
    "auto_retry_interval_seconds": 300,  # Wait 5 minutes between retries
    
    # Email notifications on failure
    "email_notifications": {
        "on_failure": ["data-eng-team@company.com", "oncall@company.com"],
        "on_success": [],  # Optional: notify on successful recovery
        "no_alert_for_skipped_runs": True
    },
    
    # Webhook notifications (e.g., Slack)
    "webhook_notifications": {
        "on_failure": [{
            "id": "slack_cdc_alerts",
            "url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
        }]
    }
}

print("Error Handling Configuration:")
print(f"  Max Retries: {error_handling_config['max_auto_retry_attempts']}")
print(f"  Retry Interval: {error_handling_config['auto_retry_interval_seconds']}s")
print(f"  Alerts: Email + Slack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Custom Error Monitoring

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create view to track pipeline errors
# MAGIC CREATE OR REPLACE VIEW retail_analytics.monitoring.pipeline_errors AS
# MAGIC SELECT 
# MAGIC   event_time,
# MAGIC   pipeline_name,
# MAGIC   update_id,
# MAGIC   level,
# MAGIC   message,
# MAGIC   details
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND level IN ('ERROR', 'WARN')
# MAGIC   AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC;
# MAGIC
# MAGIC -- Query recent errors
# MAGIC SELECT * FROM retail_analytics.monitoring.pipeline_errors LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track error frequency over time
# MAGIC SELECT 
# MAGIC   DATE(event_time) AS error_date,
# MAGIC   level,
# MAGIC   COUNT(*) AS error_count,
# MAGIC   COUNT(DISTINCT update_id) AS failed_runs
# MAGIC FROM retail_analytics.monitoring.pipeline_errors
# MAGIC GROUP BY DATE(event_time), level
# MAGIC ORDER BY error_date DESC, level;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Implement Data Quality Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality Check 1: Row Count Reconciliation

# COMMAND ----------

# Automated row count reconciliation
def reconcile_all_tables():
    """Compare Oracle and Bronze row counts"""
    
    tables = [
        ("RETAIL.CUSTOMERS", "retail_analytics.bronze.customers"),
        ("RETAIL.ORDERS", "retail_analytics.bronze.orders"),
        ("RETAIL.PRODUCTS", "retail_analytics.bronze.products")
    ]
    
    results = []
    
    for oracle_table, bronze_table in tables:
        # Get bronze count
        bronze_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {bronze_table}").first().cnt
        
        # Note: In production, also query Oracle for comparison
        # oracle_count = <query Oracle via JDBC>
        
        results.append({
            "table": bronze_table,
            "bronze_count": bronze_count,
            "status": "✅ Active"
        })
    
    # Display results
    from pyspark.sql import Row
    results_df = spark.createDataFrame([Row(**r) for r in results])
    return results_df

display(reconcile_all_tables())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality Check 2: Referential Integrity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create data quality monitoring view
# MAGIC CREATE OR REPLACE VIEW retail_analytics.monitoring.data_quality AS
# MAGIC SELECT 
# MAGIC   'Orphaned Orders' AS check_name,
# MAGIC   COUNT(*) AS issue_count,
# MAGIC   CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status,
# MAGIC   'Orders without matching customer' AS description
# MAGIC FROM retail_analytics.bronze.orders o
# MAGIC LEFT JOIN retail_analytics.bronze.customers c
# MAGIC   ON o.CUSTOMER_ID = c.CUSTOMER_ID
# MAGIC WHERE c.CUSTOMER_ID IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Duplicate Customer IDs',
# MAGIC   COUNT(*),
# MAGIC   CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END,
# MAGIC   'Customer IDs appearing more than once'
# MAGIC FROM (
# MAGIC   SELECT CUSTOMER_ID, COUNT(*) AS cnt
# MAGIC   FROM retail_analytics.bronze.customers
# MAGIC   GROUP BY CUSTOMER_ID
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Duplicate Order IDs',
# MAGIC   COUNT(*),
# MAGIC   CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END,
# MAGIC   'Order IDs appearing more than once'
# MAGIC FROM (
# MAGIC   SELECT ORDER_ID, COUNT(*) AS cnt
# MAGIC   FROM retail_analytics.bronze.orders
# MAGIC   GROUP BY ORDER_ID
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Null Email Addresses',
# MAGIC   COUNT(*),
# MAGIC   CASE WHEN COUNT(*) < 10 THEN '✅ PASS' ELSE '⚠️ WARN' END,
# MAGIC   'Customers with NULL email (should be rare)'
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE EMAIL IS NULL;
# MAGIC
# MAGIC -- Run quality checks
# MAGIC SELECT * FROM retail_analytics.monitoring.data_quality
# MAGIC ORDER BY status DESC, check_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality Check 3: Data Freshness

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create freshness monitoring view
# MAGIC CREATE OR REPLACE VIEW retail_analytics.monitoring.data_freshness AS
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   MAX(_commit_timestamp) AS latest_change,
# MAGIC   TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS seconds_old,
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
# MAGIC     ELSE '❌ STALE'
# MAGIC   END AS freshness_status
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'orders',
# MAGIC   COUNT(*),
# MAGIC   MAX(_commit_timestamp),
# MAGIC   TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
# MAGIC     ELSE '❌ STALE'
# MAGIC   END
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'products',
# MAGIC   COUNT(*),
# MAGIC   MAX(_commit_timestamp),
# MAGIC   TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 300 THEN '✅ FRESH'
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 900 THEN '⚠️ AGING'
# MAGIC     ELSE '❌ STALE'
# MAGIC   END
# MAGIC FROM retail_analytics.bronze.products;
# MAGIC
# MAGIC -- Check freshness
# MAGIC SELECT * FROM retail_analytics.monitoring.data_freshness
# MAGIC ORDER BY freshness_status DESC, seconds_old DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Automated Alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alert 1: Stale Data Detection (Databricks SQL Alert)
# MAGIC
# MAGIC **Create in Databricks SQL:**
# MAGIC
# MAGIC 1. Navigate to **SQL Editor**
# MAGIC 2. Create query:
# MAGIC ```sql
# MAGIC SELECT COUNT(*) AS stale_table_count
# MAGIC FROM retail_analytics.monitoring.data_freshness
# MAGIC WHERE freshness_status = '❌ STALE';
# MAGIC ```
# MAGIC 3. Click **Schedule** → **Add Alert**
# MAGIC 4. Configure:
# MAGIC    - **Alert Name:** "CDC Data Freshness Alert"
# MAGIC    - **Trigger:** Value > 0
# MAGIC    - **Check frequency:** Every 15 minutes
# MAGIC    - **Notification:** Email to data-eng-team@company.com
# MAGIC 5. Save alert

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alert 2: Data Quality Failures

# COMMAND ----------

# Python-based alerting logic
import requests
import json
from datetime import datetime

def check_data_quality_and_alert():
    """Run quality checks and send alerts if issues found"""
    
    # Run quality checks
    issues_df = spark.sql("""
        SELECT * 
        FROM retail_analytics.monitoring.data_quality
        WHERE status IN ('❌ FAIL', '⚠️ WARN')
    """)
    
    issue_count = issues_df.count()
    
    if issue_count > 0:
        print(f"⚠️ Found {issue_count} data quality issue(s)")
        display(issues_df)
        
        # Prepare alert message
        issues_list = [row.check_name for row in issues_df.collect()]
        message = f"🚨 CDC Data Quality Alert\\n\\nIssues detected:\\n" + "\\n".join([f"- {issue}" for issue in issues_list])
        
        # Send to Slack (example)
        # slack_webhook = dbutils.secrets.get("monitoring", "slack_webhook")
        # requests.post(slack_webhook, json={"text": message})
        
        print(f"\\nAlert message:\\n{message}")
    else:
        print("✅ All data quality checks passed")
    
    return issue_count == 0

# Run quality checks
check_data_quality_and_alert()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Monitoring Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 1: CDC Health Overview

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall CDC pipeline health
# MAGIC SELECT 
# MAGIC   'CDC Pipeline Health' AS dashboard,
# MAGIC   CURRENT_TIMESTAMP() AS report_time,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.monitoring.data_freshness WHERE freshness_status = '✅ FRESH') AS fresh_tables,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.monitoring.data_freshness WHERE freshness_status = '❌ STALE') AS stale_tables,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.monitoring.data_quality WHERE status = '✅ PASS') AS quality_checks_passed,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.monitoring.data_quality WHERE status = '❌ FAIL') AS quality_checks_failed,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.bronze.customers) AS total_customers,
# MAGIC   (SELECT COUNT(*) FROM retail_analytics.bronze.orders) AS total_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 2: CDC Latency Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Track CDC latency over time (last 24 hours)
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('hour', _commit_timestamp) AS hour,
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS changes_captured,
# MAGIC   AVG(TIMESTAMPDIFF(SECOND, _commit_timestamp, CURRENT_TIMESTAMP())) AS avg_age_seconds
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_TRUNC('hour', _commit_timestamp)
# MAGIC ORDER BY hour DESC
# MAGIC LIMIT 24;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Query 3: Change Volume by Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change velocity over last 7 days
# MAGIC SELECT 
# MAGIC   DATE(_commit_timestamp) AS change_date,
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS change_count
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(_commit_timestamp)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   DATE(_commit_timestamp),
# MAGIC   'orders',
# MAGIC   COUNT(*)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE _commit_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY DATE(_commit_timestamp)
# MAGIC
# MAGIC ORDER BY change_date DESC, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Databricks SQL Dashboard
# MAGIC
# MAGIC **Steps to create production dashboard:**
# MAGIC
# MAGIC 1. Navigate to **SQL Editor** → **Dashboards**
# MAGIC 2. Click **Create Dashboard**
# MAGIC 3. **Dashboard Name:** "Oracle CDC Monitoring"
# MAGIC 4. Add visualizations:
# MAGIC    - **Counter:** Fresh vs Stale Tables (Query 1)
# MAGIC    - **Counter:** Quality Checks Passed vs Failed (Query 1)
# MAGIC    - **Line Chart:** CDC Latency Trend (Query 2)
# MAGIC    - **Bar Chart:** Change Volume by Table (Query 3)
# MAGIC    - **Table:** Recent Errors (from pipeline_errors view)
# MAGIC 5. Set **Auto-refresh:** Every 5 minutes
# MAGIC 6. **Share** with team

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Operational Runbook

# COMMAND ----------

# Runbook: Common CDC Issues and Resolution

runbook = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                  CDC OPERATIONS RUNBOOK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ISSUE 1: Data is stale (no updates in > 15 minutes)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Symptoms:
  - freshness_status shows '❌ STALE'
  - No recent changes in bronze tables

Diagnosis Steps:
  1. Check gateway status: Is it running?
  2. Check pipeline status: Is it running?
  3. Check Oracle: Are transactions being committed?
  4. Check network: Can Databricks reach Oracle?

Resolution:
  - If gateway stopped: Restart gateway via Lakeflow UI
  - If pipeline stopped: Start pipeline in DLT UI
  - If network issue: Check firewall rules, VPN status
  - If Oracle issue: Contact DBA team

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ISSUE 2: Pipeline failures
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Symptoms:
  - Pipeline shows FAILED status
  - Error alerts received

Diagnosis Steps:
  1. Review pipeline event logs
  2. Check error message in system.lakeflow.pipeline_events
  3. Look for schema evolution issues

Common Errors and Fixes:
  - "Schema mismatch" → Run pipeline with allow_schema_evolution=true
  - "Connection timeout" → Check Oracle connectivity
  - "Out of memory" → Increase cluster size

Resolution:
  - Transient errors: Pipeline will auto-retry (3 attempts)
  - Persistent errors: Fix root cause and manually restart

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ISSUE 3: High CDC latency (> 5 minutes)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Symptoms:
  - seconds_old > 300 in data_freshness view
  - Changes taking long time to appear

Diagnosis Steps:
  1. Check change volume: Is Oracle generating many changes?
  2. Check gateway size: Is it undersized?
  3. Check pipeline cluster: Is it undersized?
  4. Check network latency

Resolution:
  - High volume: Upgrade gateway VM size
  - Slow processing: Increase pipeline workers
  - Network: Investigate network performance

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ISSUE 4: Orphaned records detected
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Symptoms:
  - data_quality check shows orphaned orders
  - Orders exist without matching customers

Diagnosis:
  - Check if temporary (changes still propagating)
  - Check if persistent (>5 minutes)

Resolution:
  - Wait 2-3 minutes and re-check (may resolve automatically)
  - If persistent: Check if parent record was deleted in Oracle
  - Implement cleanup in silver layer if intentional

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ISSUE 5: Duplicate records
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Symptoms:
  - Same primary key appears multiple times

Diagnosis:
  - Check Delta table history
  - Look for pipeline restart during processing

Resolution:
  - Run OPTIMIZE on affected table
  - SCN-based merge should handle automatically
  - If persists: Contact Databricks support

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ESCALATION CONTACTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Data Engineering Team: data-eng@company.com
  On-Call Engineer: oncall@company.com (Slack: #data-oncall)
  Oracle DBA Team: oracle-dba@company.com
  Databricks Support: support.databricks.com
"""

print(runbook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Implemented
# MAGIC
# MAGIC ✅ **Pipeline Scheduling:**
# MAGIC - Configured continuous mode for real-time CDC
# MAGIC - Configured triggered mode for cost-optimized execution
# MAGIC - Set up cron schedules for periodic runs
# MAGIC
# MAGIC ✅ **Error Handling:**
# MAGIC - Enabled automatic retries (3 attempts)
# MAGIC - Configured email notifications
# MAGIC - Set up Slack webhooks for alerts
# MAGIC - Created error tracking views
# MAGIC
# MAGIC ✅ **Data Quality Checks:**
# MAGIC - Row count reconciliation
# MAGIC - Referential integrity validation
# MAGIC - Duplicate detection
# MAGIC - Null value monitoring
# MAGIC - Data freshness tracking
# MAGIC
# MAGIC ✅ **Monitoring & Alerting:**
# MAGIC - Created comprehensive monitoring views
# MAGIC - Set up automated alerts for stale data
# MAGIC - Built quality check alerts
# MAGIC - Designed operational dashboard queries
# MAGIC
# MAGIC ✅ **Operational Excellence:**
# MAGIC - Documented runbook procedures
# MAGIC - Defined escalation paths
# MAGIC - Created diagnostic queries
# MAGIC - Established SLA metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production Readiness Checklist

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Final production readiness check
# MAGIC SELECT 
# MAGIC   'Pipeline Configured' AS check_item,
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM system.lakeflow.pipelines WHERE name = 'retail_ingestion_pipeline') > 0 
# MAGIC        THEN '✅ PASS' ELSE '❌ FAIL' END AS status
# MAGIC
# MAGIC UNION ALL SELECT 'Monitoring Views Created',
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM information_schema.views 
# MAGIC              WHERE table_schema = 'monitoring' 
# MAGIC              AND table_name IN ('data_freshness', 'data_quality', 'pipeline_errors')) = 3
# MAGIC        THEN '✅ PASS' ELSE '⚠️ INCOMPLETE' END
# MAGIC
# MAGIC UNION ALL SELECT 'Bronze Tables Operational',
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM retail_analytics.bronze.customers) > 0
# MAGIC        THEN '✅ PASS' ELSE '❌ FAIL' END
# MAGIC
# MAGIC UNION ALL SELECT 'Data Quality Checks Passing',
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM retail_analytics.monitoring.data_quality WHERE status = '❌ FAIL') = 0
# MAGIC        THEN '✅ PASS' ELSE '⚠️ ISSUES DETECTED' END
# MAGIC
# MAGIC UNION ALL SELECT 'Data Freshness Acceptable',
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM retail_analytics.monitoring.data_freshness WHERE freshness_status = '❌ STALE') = 0
# MAGIC        THEN '✅ PASS' ELSE '⚠️ STALE DATA' END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC **You've completed the Oracle CDC workshop!**
# MAGIC
# MAGIC ### Workshop Achievements
# MAGIC
# MAGIC **Infrastructure:**
# MAGIC - ✅ Configured Oracle with ARCHIVELOG and supplemental logging
# MAGIC - ✅ Created Unity Catalog resources (catalog, schemas, volumes)
# MAGIC - ✅ Deployed Lakeflow Connect gateway and pipeline
# MAGIC
# MAGIC **CDC Operations:**
# MAGIC - ✅ Performed initial snapshot (full load)
# MAGIC - ✅ Validated incremental CDC (INSERT/UPDATE/DELETE)
# MAGIC - ✅ Tested multi-table dependencies
# MAGIC - ✅ Measured end-to-end latency
# MAGIC
# MAGIC **Production Readiness:**
# MAGIC - ✅ Implemented scheduling strategies
# MAGIC - ✅ Built error handling and retry logic
# MAGIC - ✅ Created data quality checks
# MAGIC - ✅ Set up monitoring and alerting
# MAGIC - ✅ Documented operational runbook
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC **Expand Your Pipeline:**
# MAGIC 1. Add silver layer transformations (data cleansing, enrichment)
# MAGIC 2. Build gold layer aggregations (business metrics)
# MAGIC 3. Create real-time dashboards using bronze/silver data
# MAGIC 4. Integrate with BI tools (Tableau, Power BI, Looker)
# MAGIC
# MAGIC **Advanced Topics:**
# MAGIC - Implement SCD Type 2 for historical tracking
# MAGIC - Add stream processing for real-time aggregations
# MAGIC - Configure cross-region disaster recovery
# MAGIC - Optimize for high-volume tables (partitioning, Z-ordering)
# MAGIC
# MAGIC **Share Your Knowledge:**
# MAGIC - Document your CDC implementation
# MAGIC - Train your team on operations
# MAGIC - Share lessons learned with the community

# COMMAND ----------

# MAGIC %md
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
