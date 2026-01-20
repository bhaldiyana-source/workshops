# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 8: Implementing Scheduled Pipelines and Error Handling
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this final lab, you will implement production-ready pipeline scheduling, monitoring, and error handling strategies.
# MAGIC
# MAGIC **Duration:** 25 minutes
# MAGIC
# MAGIC **Objectives:**
# MAGIC - Configure production pipeline schedules
# MAGIC - Implement monitoring queries and dashboards
# MAGIC - Create alerting rules (conceptual)
# MAGIC - Test error scenarios and recovery
# MAGIC - Document operational procedures
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Completed all previous labs
# MAGIC - Completed Lecture 7: Production Best Practices
# MAGIC - Understanding of monitoring concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Configure Pipeline Schedules
# MAGIC
# MAGIC ### Step 1.1: Review Current Schedule
# MAGIC
# MAGIC Navigate to your pipeline and check current configuration:
# MAGIC - **Workflows** → **Delta Live Tables** → `retail_cdc_pipeline`
# MAGIC - Review "Schedule" section

# COMMAND ----------

# Display current pipeline configuration info
print("Current Pipeline Schedule Configuration:")
print("=" * 50)
print("")
print("Pipeline Name: retail_cdc_pipeline")
print("Current Mode: Scheduled")
print("Current Frequency: Every 15 minutes (*/15 * * * *)")
print("")
print("Recommendation based on table characteristics:")
print("- High-frequency tables (orders): Every 5-15 minutes")
print("- Medium-frequency (customers): Every 15-30 minutes")
print("- Low-frequency (products): Hourly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Schedule Optimization Strategy

# COMMAND ----------

# Calculate optimal schedule based on business requirements
import json

schedule_recommendations = {
    "real_time_requirements": {
        "description": "Sub-minute latency needed",
        "mode": "Continuous",
        "schedule": "N/A (always running)",
        "cost": "High",
        "use_case": "Fraud detection, real-time dashboards"
    },
    "near_real_time": {
        "description": "5-15 minute latency acceptable",
        "mode": "Scheduled",
        "schedule": "*/5 * * * * or */15 * * * *",
        "cost": "Medium",
        "use_case": "Order processing, inventory updates"
    },
    "hourly_sync": {
        "description": "Hourly updates sufficient",
        "mode": "Scheduled",
        "schedule": "0 * * * *",
        "cost": "Low",
        "use_case": "Reference data, product catalog"
    },
    "daily_batch": {
        "description": "Daily refresh adequate",
        "mode": "Scheduled",
        "schedule": "0 2 * * *",
        "cost": "Very Low",
        "use_case": "Historical analysis, archival data"
    }
}

print(json.dumps(schedule_recommendations, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Implement Monitoring Queries
# MAGIC
# MAGIC ### Step 2.1: Gateway Health Monitor

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor ingestion gateway health
# MAGIC -- Note: Actual system table may vary by Databricks version
# MAGIC /*
# MAGIC SELECT 
# MAGIC #   gateway_name,
# MAGIC #   status,
# MAGIC #   last_heartbeat,
# MAGIC #   TIMESTAMPDIFF(MINUTE, last_heartbeat, CURRENT_TIMESTAMP()) as minutes_since_heartbeat,
# MAGIC #   CASE 
# MAGIC #     WHEN status != 'RUNNING' THEN '🔴 CRITICAL'
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, last_heartbeat, CURRENT_TIMESTAMP()) > 5 THEN '🟡 WARNING'
# MAGIC #     ELSE '🟢 HEALTHY'
# MAGIC #   END as health_status
# MAGIC FROM system.lakeflow.gateways
# MAGIC WHERE gateway_name = 'retail_ingestion_gateway';
# MAGIC */
# MAGIC
# MAGIC SELECT 'Gateway Health Check' as check_type, 
# MAGIC        'Verify in Lakeflow Connect UI' as status;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Pipeline Execution Monitor

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor recent pipeline executions
# MAGIC CREATE OR REPLACE TEMP VIEW pipeline_execution_summary AS
# MAGIC SELECT 
# MAGIC #   'retail_cdc_pipeline' as pipeline_name,
# MAGIC #   COUNT(*) as total_runs,
# MAGIC #   SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC #   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
# MAGIC #   ROUND(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
# MAGIC FROM (
# MAGIC #   -- Simulated pipeline execution data
# MAGIC #   -- Replace with actual system table query
# MAGIC #   SELECT 'SUCCEEDED' as status UNION ALL
# MAGIC #   SELECT 'SUCCEEDED' UNION ALL
# MAGIC #   SELECT 'SUCCEEDED' UNION ALL
# MAGIC #   SELECT 'SUCCEEDED' UNION ALL
# MAGIC #   SELECT 'SUCCEEDED'
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM pipeline_execution_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Data Latency Monitor

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor data freshness (latency)
# MAGIC SELECT 
# MAGIC #   'customers' as table_name,
# MAGIC #   MAX(_commit_timestamp) as latest_commit,
# MAGIC #   CURRENT_TIMESTAMP() as check_time,
# MAGIC #   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as latency_minutes,
# MAGIC #   CASE 
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 30 THEN '🔴 HIGH LATENCY'
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 20 THEN '🟡 MODERATE'
# MAGIC #     ELSE '🟢 NORMAL'
# MAGIC #   END as latency_status
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'orders',
# MAGIC #   MAX(_commit_timestamp),
# MAGIC #   CURRENT_TIMESTAMP(),
# MAGIC #   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC #   CASE 
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 30 THEN '🔴 HIGH LATENCY'
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 20 THEN '🟡 MODERATE'
# MAGIC #     ELSE '🟢 NORMAL'
# MAGIC #   END
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'products',
# MAGIC #   MAX(_commit_timestamp),
# MAGIC #   CURRENT_TIMESTAMP(),
# MAGIC #   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
# MAGIC #   CASE 
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 30 THEN '🔴 HIGH LATENCY'
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 20 THEN '🟡 MODERATE'
# MAGIC #     ELSE '🟢 NORMAL'
# MAGIC #   END
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.4: Data Quality Monitor

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor data quality metrics
# MAGIC SELECT 
# MAGIC #   'customers' as table_name,
# MAGIC #   COUNT(*) as total_records,
# MAGIC #   SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END) as deleted_records,
# MAGIC #   SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails,
# MAGIC #   SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) as null_states,
# MAGIC #   ROUND(SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as email_completeness_pct
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'orders',
# MAGIC #   COUNT(*),
# MAGIC #   SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END),
# MAGIC #   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
# MAGIC #   SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END),
# MAGIC #   ROUND(SUM(CASE WHEN customer_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'products',
# MAGIC #   COUNT(*),
# MAGIC #   SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END),
# MAGIC #   SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END),
# MAGIC #   SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END),
# MAGIC #   ROUND(SUM(CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
# MAGIC FROM retail_analytics.bronze.products;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.5: Record Count Reconciliation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare record counts (PostgreSQL vs. Databricks)
# MAGIC -- This helps detect data loss or sync issues
# MAGIC CREATE OR REPLACE TEMP VIEW record_count_check AS
# MAGIC SELECT 
# MAGIC #   'customers' as table_name,
# MAGIC #   COUNT(*) as databricks_count,
# MAGIC #   0 as postgres_count,  -- Replace with actual PostgreSQL count via JDBC
# MAGIC #   0 as difference
# MAGIC FROM retail_analytics.bronze.customers
# MAGIC WHERE _is_deleted IS NULL OR _is_deleted = FALSE
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'orders',
# MAGIC #   COUNT(*),
# MAGIC #   0,
# MAGIC #   0
# MAGIC FROM retail_analytics.bronze.orders
# MAGIC WHERE _is_deleted IS NULL OR _is_deleted = FALSE
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC #   'products',
# MAGIC #   COUNT(*),
# MAGIC #   0,
# MAGIC #   0
# MAGIC FROM retail_analytics.bronze.products
# MAGIC WHERE _is_deleted IS NULL OR _is_deleted = FALSE;
# MAGIC
# MAGIC SELECT * FROM record_count_check;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Create Monitoring Dashboard
# MAGIC
# MAGIC ### Step 3.1: Comprehensive Health Dashboard Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Single query for operational dashboard
# MAGIC WITH 
# MAGIC -- Table statistics
# MAGIC table_stats AS (
# MAGIC #   SELECT 'customers' as table_name, COUNT(*) as total, MAX(_commit_timestamp) as latest_change
# MAGIC #   FROM retail_analytics.bronze.customers
# MAGIC #   UNION ALL
# MAGIC #   SELECT 'orders', COUNT(*), MAX(_commit_timestamp)
# MAGIC #   FROM retail_analytics.bronze.orders
# MAGIC #   UNION ALL
# MAGIC #   SELECT 'products', COUNT(*), MAX(_commit_timestamp)
# MAGIC #   FROM retail_analytics.bronze.products
# MAGIC ),
# MAGIC -- Change activity last hour
# MAGIC recent_activity AS (
# MAGIC #   SELECT 'customers' as table_name, COUNT(*) as changes_last_hour
# MAGIC #   FROM retail_analytics.bronze.customers
# MAGIC #   WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC #   UNION ALL
# MAGIC #   SELECT 'orders', COUNT(*)
# MAGIC #   FROM retail_analytics.bronze.orders
# MAGIC #   WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC #   UNION ALL
# MAGIC #   SELECT 'products', COUNT(*)
# MAGIC #   FROM retail_analytics.bronze.products
# MAGIC #   WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC )
# MAGIC SELECT 
# MAGIC #   ts.table_name,
# MAGIC #   ts.total as total_records,
# MAGIC #   ts.latest_change,
# MAGIC #   TIMESTAMPDIFF(MINUTE, ts.latest_change, CURRENT_TIMESTAMP()) as minutes_since_last_change,
# MAGIC #   ra.changes_last_hour,
# MAGIC #   CASE 
# MAGIC #     WHEN TIMESTAMPDIFF(MINUTE, ts.latest_change, CURRENT_TIMESTAMP()) > 30 THEN '🔴 STALE'
# MAGIC #     WHEN ra.changes_last_hour = 0 THEN '🟡 NO ACTIVITY'
# MAGIC #     ELSE '🟢 ACTIVE'
# MAGIC #   END as health_status
# MAGIC FROM table_stats ts
# MAGIC JOIN recent_activity ra ON ts.table_name = ra.table_name
# MAGIC ORDER BY ts.table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Save Dashboard as SQL Query
# MAGIC
# MAGIC To create a Databricks SQL Dashboard:
# MAGIC
# MAGIC 1. Navigate to **SQL** → **Queries**
# MAGIC 2. Click **Create Query**
# MAGIC 3. Name: "CDC Pipeline Health Dashboard"
# MAGIC 4. Paste the query from previous cell
# MAGIC 5. **Save** and **Run**
# MAGIC 6. Click **Add to Dashboard** to create visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Implement Alerting Rules
# MAGIC
# MAGIC ### Step 4.1: Define Alert Thresholds

# COMMAND ----------

# Define alerting thresholds as configuration
alert_config = {
    "gateway_heartbeat": {
        "threshold_minutes": 5,
        "severity": "CRITICAL",
        "action": "Page on-call engineer"
    },
    "pipeline_failures": {
        "threshold_count": 3,
        "severity": "CRITICAL",
        "action": "Page on-call engineer"
    },
    "data_latency": {
        "warning_minutes": 20,
        "critical_minutes": 30,
        "severity": "HIGH",
        "action": "Email team"
    },
    "record_count_diff": {
        "threshold_percent": 1.0,
        "severity": "HIGH",
        "action": "Email team"
    },
    "no_activity": {
        "threshold_hours": 2,
        "severity": "MEDIUM",
        "action": "Slack notification"
    }
}

# Display alert configuration
import json
print("Alert Configuration:")
print("=" * 50)
print(json.dumps(alert_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Create Alert Detection Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to detect alert conditions
# MAGIC WITH alert_checks AS (
# MAGIC #   -- Check 1: Data latency
# MAGIC #   SELECT 
# MAGIC #     'DATA_LATENCY' as alert_type,
# MAGIC #     table_name,
# MAGIC #     CONCAT('No data received for ', 
# MAGIC #            CAST(TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS STRING), 
# MAGIC #            ' minutes') as alert_message,
# MAGIC #     'HIGH' as severity
# MAGIC #   FROM (
# MAGIC #     SELECT 'customers' as table_name, _commit_timestamp FROM retail_analytics.bronze.customers
# MAGIC #     UNION ALL
# MAGIC #     SELECT 'orders', _commit_timestamp FROM retail_analytics.bronze.orders
# MAGIC #     UNION ALL
# MAGIC #     SELECT 'products', _commit_timestamp FROM retail_analytics.bronze.products
# MAGIC #   )
# MAGIC #   GROUP BY table_name
# MAGIC #   HAVING TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 30
# MAGIC #   
# MAGIC #   UNION ALL
# MAGIC #   
# MAGIC #   -- Check 2: No recent activity
# MAGIC #   SELECT 
# MAGIC #     'NO_ACTIVITY',
# MAGIC #     table_name,
# MAGIC #     'No changes in last 2 hours',
# MAGIC #     'MEDIUM'
# MAGIC #   FROM (
# MAGIC #     SELECT 'customers' as table_name, COUNT(*) as change_count
# MAGIC #     FROM retail_analytics.bronze.customers
# MAGIC #     WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR
# MAGIC #     UNION ALL
# MAGIC #     SELECT 'orders', COUNT(*)
# MAGIC #     FROM retail_analytics.bronze.orders
# MAGIC #     WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR
# MAGIC #     UNION ALL
# MAGIC #     SELECT 'products', COUNT(*)
# MAGIC #     FROM retail_analytics.bronze.products
# MAGIC #     WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR
# MAGIC #   )
# MAGIC #   WHERE change_count = 0
# MAGIC )
# MAGIC SELECT 
# MAGIC #   alert_type,
# MAGIC #   table_name,
# MAGIC #   alert_message,
# MAGIC #   severity,
# MAGIC #   CURRENT_TIMESTAMP() as detected_at
# MAGIC FROM alert_checks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.3: Alert Notification Function (Conceptual)

# COMMAND ----------

def send_alert(alert_type, message, severity):
    """
    Send alert notification via multiple channels
    
    In production, integrate with:
    - PagerDuty for CRITICAL alerts
    - Email for HIGH alerts
    - Slack for MEDIUM alerts
    """
    print(f"🚨 ALERT TRIGGERED")
    print(f"Type: {alert_type}")
    print(f"Severity: {severity}")
    print(f"Message: {message}")
    print(f"Timestamp: {datetime.now()}")
    print("")
    
    if severity == "CRITICAL":
        print("Action: Paging on-call engineer...")
        # pagerduty.trigger_incident(message)
    elif severity == "HIGH":
        print("Action: Sending email to team...")
        # email.send(to="data-team@company.com", subject=f"CDC Alert: {alert_type}", body=message)
    elif severity == "MEDIUM":
        print("Action: Posting to Slack...")
        # slack.post_message(channel="#data-alerts", message=message)
    
    print("✓ Alert sent")

# Example usage
send_alert(
    alert_type="DATA_LATENCY",
    message="No data received from customers table for 35 minutes",
    severity="HIGH"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Test Error Scenarios
# MAGIC
# MAGIC ### Scenario 1: Simulated Network Outage
# MAGIC
# MAGIC **What happens:** Gateway loses connection to PostgreSQL
# MAGIC
# MAGIC **Expected behavior:**
# MAGIC - Gateway retries with exponential backoff
# MAGIC - Maintains checkpoint position
# MAGIC - Resumes from last LSN when connection restored
# MAGIC
# MAGIC **To test:**
# MAGIC 1. Temporarily block network (modify security group)
# MAGIC 2. Monitor gateway status
# MAGIC 3. Restore network
# MAGIC 4. Verify pipeline resumes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2: Invalid Credentials
# MAGIC
# MAGIC **What happens:** PostgreSQL password changed
# MAGIC
# MAGIC **Expected behavior:**
# MAGIC - Pipeline fails with authentication error
# MAGIC - Alert triggered
# MAGIC
# MAGIC **Recovery steps:**

# COMMAND ----------

print("Recovery Steps for Invalid Credentials:")
print("=" * 50)
print("")
print("1. Verify credentials in PostgreSQL")
print("   psql -h <host> -U lakeflow_replication -d retaildb")
print("")
print("2. Update Unity Catalog connection")
print("   - Navigate to Data → Connections")
print("   - Edit retail_postgres_connection")
print("   - Update password")
print("   - Test connection")
print("")
print("3. Restart gateway (if needed)")
print("   - Navigate to Lakeflow Connect UI")
print("   - Restart retail_ingestion_gateway")
print("")
print("4. Manually trigger pipeline")
print("   - Navigate to pipeline UI")
print("   - Click 'Run Now'")
print("")
print("5. Verify recovery")
print("   - Check pipeline logs")
print("   - Verify data is flowing")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3: Replication Slot Dropped
# MAGIC
# MAGIC **What happens:** Replication slot deleted or invalidated
# MAGIC
# MAGIC **Expected behavior:**
# MAGIC - Gateway cannot find slot
# MAGIC - Pipeline fails
# MAGIC
# MAGIC **Recovery steps:**

# COMMAND ----------

print("Recovery Steps for Lost Replication Slot:")
print("=" * 50)
print("")
print("In PostgreSQL:")
print("")
print("1. Check if slot exists:")
print("   SELECT * FROM pg_replication_slots WHERE slot_name = 'lakeflow_slot';")
print("")
print("2. If missing, recreate:")
print("   SELECT pg_create_logical_replication_slot('lakeflow_slot', 'pgoutput');")
print("")
print("3. If data gap is large, consider re-snapshot:")
print("   - Edit pipeline")
print("   - Enable 'Initial snapshot'")
print("   - Run pipeline")
print("")
print("4. Monitor for successful recovery")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Document Operational Procedures
# MAGIC
# MAGIC ### Step 6.1: Create Runbook

# COMMAND ----------

runbook = """
# CDC Pipeline Operational Runbook

## Daily Operations

### Morning Checks (9 AM)
1. Review overnight pipeline executions
2. Check data latency dashboard
3. Verify record counts match expected ranges
4. Review any alerts from previous 24 hours

### Afternoon Checks (3 PM)
1. Spot check recent data changes
2. Monitor gateway health
3. Review disk usage (WAL on PostgreSQL)

## Weekly Operations

### Monday
- Review weekly success rate metrics
- Check replication slot lag
- Verify backup procedures completed

### Wednesday
- Review performance trends
- Check for schema changes in source
- Test alert notifications

### Friday
- OPTIMIZE bronze tables
- Review storage costs
- Update documentation if needed

## Monthly Operations

- Full pipeline test (insert/update/delete)
- Disaster recovery drill
- Review and update alert thresholds
- Capacity planning review

## Incident Response

### Pipeline Failure
1. Check pipeline logs for error message
2. Verify gateway is running
3. Test PostgreSQL connectivity
4. Check replication slot status
5. Review recent schema changes
6. Escalate if unresolved in 30 minutes

### High Latency
1. Check pipeline schedule
2. Verify gateway performance
3. Check PostgreSQL WAL generation rate
4. Review network connectivity
5. Manually trigger pipeline if needed

### Data Quality Issues
1. Query affected table directly
2. Compare with PostgreSQL source
3. Check for schema mismatches
4. Review pipeline logs
5. Consider re-snapshot if corruption detected

## Contact Information

- On-call Engineer: [PagerDuty rotation]
- Data Team Email: data-team@company.com
- Slack Channel: #data-alerts
- Escalation: VP of Data
"""

print(runbook)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.2: Save Runbook Documentation

# COMMAND ----------

# In production, save runbook to shared location
# Example: Confluence, GitHub Wiki, or internal docs system

print("Save runbook to:")
print("- Confluence: Data Engineering → CDC Pipelines → PostgreSQL Runbook")
print("- GitHub: docs/operational-runbooks/postgresql-cdc.md")
print("- Google Docs: Shared Drive → Data Team → Runbooks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Completion Checklist
# MAGIC
# MAGIC Verify you've completed all production readiness steps:
# MAGIC
# MAGIC ### Scheduling
# MAGIC - [ ] Reviewed current pipeline schedule
# MAGIC - [ ] Documented schedule optimization strategy
# MAGIC - [ ] Balanced latency requirements with cost
# MAGIC
# MAGIC ### Monitoring
# MAGIC - [ ] Created gateway health monitor
# MAGIC - [ ] Created pipeline execution monitor
# MAGIC - [ ] Created data latency monitor
# MAGIC - [ ] Created data quality monitor
# MAGIC - [ ] Created record count reconciliation check
# MAGIC - [ ] Built comprehensive health dashboard
# MAGIC
# MAGIC ### Alerting
# MAGIC - [ ] Defined alert thresholds
# MAGIC - [ ] Created alert detection queries
# MAGIC - [ ] Documented alert notification procedures
# MAGIC - [ ] Tested alert detection logic
# MAGIC
# MAGIC ### Error Handling
# MAGIC - [ ] Documented network outage recovery
# MAGIC - [ ] Documented credential update procedure
# MAGIC - [ ] Documented replication slot recovery
# MAGIC - [ ] Tested error scenarios (if possible)
# MAGIC
# MAGIC ### Documentation
# MAGIC - [ ] Created operational runbook
# MAGIC - [ ] Documented daily operations
# MAGIC - [ ] Documented weekly operations
# MAGIC - [ ] Documented incident response procedures
# MAGIC - [ ] Saved documentation to shared location
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC **Congratulations!** You've completed the CDC workshop and built a production-ready pipeline!
# MAGIC
# MAGIC **What you've accomplished:**
# MAGIC - ✅ Configured PostgreSQL for CDC with logical replication
# MAGIC - ✅ Built Lakeflow Connect ingestion gateway and pipeline
# MAGIC - ✅ Validated all CDC operations (INSERT, UPDATE, DELETE)
# MAGIC - ✅ Handled multi-table dependencies and foreign keys
# MAGIC - ✅ Implemented comprehensive monitoring and alerting
# MAGIC - ✅ Created operational procedures and runbooks
# MAGIC
# MAGIC **Your CDC pipeline is:**
# MAGIC - 📊 Monitored with health dashboards
# MAGIC - 🚨 Protected with alerting rules
# MAGIC - 📋 Documented with operational runbooks
# MAGIC - 🔄 Scheduled for optimal performance
# MAGIC - 🛡️ Resilient with error recovery procedures
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC ### Immediate Actions
# MAGIC 1. Share dashboards with team
# MAGIC 2. Set up actual alert integrations (PagerDuty, Slack, Email)
# MAGIC 3. Schedule runbook review with operations team
# MAGIC 4. Conduct disaster recovery drill
# MAGIC
# MAGIC ### Future Enhancements
# MAGIC 1. **Silver Layer**: Build business logic transformations
# MAGIC 2. **Gold Layer**: Create analytics-ready dimensional models
# MAGIC 3. **Data Quality**: Implement Great Expectations
# MAGIC 4. **Performance**: Optimize with liquid clustering
# MAGIC 5. **Governance**: Add Unity Catalog tags and lineage
# MAGIC 6. **Multi-Source**: Integrate additional databases (MySQL, SQL Server, Oracle)
# MAGIC
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
# MAGIC - [PostgreSQL Logical Replication Guide](https://www.postgresql.org/docs/current/logical-replication.html)
# MAGIC - [Delta Live Tables Best Practices](https://docs.databricks.com/delta-live-tables/best-practices.html)
# MAGIC - [Unity Catalog Governance](https://docs.databricks.com/data-governance/unity-catalog/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Thank you for completing the PostgreSQL CDC workshop!** 🎉
# MAGIC
# MAGIC **You're now ready to build production CDC pipelines!** 🚀
