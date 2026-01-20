# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture 7: Production Best Practices and Monitoring Strategies
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will understand:
# MAGIC - Production deployment checklist for Oracle CDC pipelines
# MAGIC - Monitoring strategies for ingestion gateways and pipelines
# MAGIC - Performance optimization techniques for both Oracle and Databricks
# MAGIC - Error handling and recovery procedures
# MAGIC - Cost optimization strategies
# MAGIC - Security best practices
# MAGIC - Disaster recovery and business continuity planning
# MAGIC
# MAGIC **Duration:** 20 minutes  
# MAGIC **Prerequisites:** Completion of Labs 2-6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Deployment Checklist
# MAGIC
# MAGIC ### Phase 1: Oracle Configuration (Database Team)
# MAGIC
# MAGIC #### Archive Log Management ✅
# MAGIC ```sql
# MAGIC -- Verify ARCHIVELOG mode is enabled
# MAGIC SELECT LOG_MODE FROM V$DATABASE;
# MAGIC -- Must return: ARCHIVELOG
# MAGIC
# MAGIC -- Check archive log destination has sufficient space
# MAGIC SELECT NAME, SPACE_LIMIT/1024/1024/1024 AS SPACE_LIMIT_GB,
# MAGIC        SPACE_USED/1024/1024/1024 AS SPACE_USED_GB,
# MAGIC        SPACE_RECLAIMABLE/1024/1024/1024 AS SPACE_RECLAIMABLE_GB,
# MAGIC        NUMBER_OF_FILES
# MAGIC FROM V$RECOVERY_FILE_DEST;
# MAGIC
# MAGIC -- Set appropriate retention (minimum 48 hours recommended)
# MAGIC -- Configure via RMAN:
# MAGIC CONFIGURE ARCHIVELOG DELETION POLICY TO BACKED UP 2 TIMES TO DISK;
# MAGIC CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 7 DAYS;
# MAGIC ```
# MAGIC
# MAGIC #### Supplemental Logging Validation ✅
# MAGIC ```sql
# MAGIC -- Verify database-level supplemental logging
# MAGIC SELECT SUPPLEMENTAL_LOG_DATA_MIN, 
# MAGIC        SUPPLEMENTAL_LOG_DATA_PK,
# MAGIC        SUPPLEMENTAL_LOG_DATA_UI
# MAGIC FROM V$DATABASE;
# MAGIC -- Expected: YES, YES, NO (minimum)
# MAGIC
# MAGIC -- Verify table-level supplemental logging
# MAGIC SELECT OWNER, TABLE_NAME, LOG_GROUP_NAME, LOG_GROUP_TYPE
# MAGIC FROM DBA_LOG_GROUPS
# MAGIC WHERE OWNER = 'RETAIL'
# MAGIC   AND TABLE_NAME IN ('CUSTOMERS', 'ORDERS', 'PRODUCTS');
# MAGIC -- Should return entries for ALL COLUMN LOGGING
# MAGIC
# MAGIC -- Check supplemental logging overhead
# MAGIC SELECT NAME, VALUE FROM V$SYSSTAT
# MAGIC WHERE NAME LIKE '%redo size%';
# MAGIC -- Monitor before/after enabling to quantify impact (typically 5-15% increase)
# MAGIC ```
# MAGIC
# MAGIC #### Database Performance Baseline ✅
# MAGIC ```sql
# MAGIC -- Establish baseline metrics before enabling CDC
# MAGIC SELECT 
# MAGIC   METRIC_NAME,
# MAGIC   VALUE,
# MAGIC   METRIC_UNIT
# MAGIC FROM V$SYSMETRIC
# MAGIC WHERE METRIC_NAME IN (
# MAGIC   'Database CPU Time Ratio',
# MAGIC   'Database Wait Time Ratio',
# MAGIC   'Redo Generated Per Sec',
# MAGIC   'Redo Generated Per Txn',
# MAGIC   'Logical Reads Per Sec',
# MAGIC   'Physical Reads Per Sec'
# MAGIC )
# MAGIC ORDER BY METRIC_NAME;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Phase 2: Network & Security Configuration
# MAGIC
# MAGIC #### Network Connectivity ✅
# MAGIC ```python
# MAGIC # Test connectivity from Databricks to Oracle
# MAGIC # Run this from a Databricks notebook:
# MAGIC
# MAGIC import socket
# MAGIC
# MAGIC oracle_host = "oracle.example.com"
# MAGIC oracle_port = 1521
# MAGIC
# MAGIC try:
# MAGIC     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# MAGIC     sock.settimeout(5)
# MAGIC     result = sock.connect_ex((oracle_host, oracle_port))
# MAGIC     if result == 0:
# MAGIC         print(f"✅ Port {oracle_port} on {oracle_host} is OPEN")
# MAGIC     else:
# MAGIC         print(f"❌ Port {oracle_port} on {oracle_host} is CLOSED")
# MAGIC     sock.close()
# MAGIC except Exception as e:
# MAGIC     print(f"❌ Connection test failed: {e}")
# MAGIC
# MAGIC # Test JDBC connection
# MAGIC jdbc_url = f"jdbc:oracle:thin:@{oracle_host}:{oracle_port}/RETAILDB"
# MAGIC connection_props = {
# MAGIC     "user": "cdc_user",
# MAGIC     "password": dbutils.secrets.get("oracle", "cdc_password"),
# MAGIC     "driver": "oracle.jdbc.OracleDriver"
# MAGIC }
# MAGIC
# MAGIC try:
# MAGIC     test_df = spark.read.jdbc(
# MAGIC         url=jdbc_url,
# MAGIC         table="(SELECT 'Connection Successful' AS status FROM DUAL)",
# MAGIC         properties=connection_props
# MAGIC     )
# MAGIC     print(f"✅ JDBC connection successful: {test_df.first().status}")
# MAGIC except Exception as e:
# MAGIC     print(f"❌ JDBC connection failed: {e}")
# MAGIC ```
# MAGIC
# MAGIC #### Firewall Rules ✅
# MAGIC
# MAGIC **Required Rules:**
# MAGIC ```
# MAGIC Source: Databricks Control Plane NAT IPs
# MAGIC Destination: Oracle Database IP
# MAGIC Port: 1521 (or custom port)
# MAGIC Protocol: TCP
# MAGIC Direction: Outbound from Databricks
# MAGIC
# MAGIC Recommended: Use AWS PrivateLink or Azure Private Link instead of public internet
# MAGIC ```
# MAGIC
# MAGIC #### Security Groups / Network ACLs ✅
# MAGIC
# MAGIC **AWS Example:**
# MAGIC ```
# MAGIC Security Group for Oracle RDS:
# MAGIC   Inbound Rule:
# MAGIC     - Type: Oracle-RDS
# MAGIC     - Protocol: TCP
# MAGIC     - Port: 1521
# MAGIC     - Source: Databricks workspace NAT gateway IPs
# MAGIC   
# MAGIC   Outbound Rule:
# MAGIC     - Type: All traffic
# MAGIC     - Destination: 0.0.0.0/0
# MAGIC ```
# MAGIC
# MAGIC #### Credentials Management ✅
# MAGIC ```python
# MAGIC # Store Oracle credentials in Databricks Secrets
# MAGIC # Never hardcode credentials in notebooks or code!
# MAGIC
# MAGIC # Create secret scope (run once):
# MAGIC # databricks secrets create-scope --scope oracle
# MAGIC
# MAGIC # Add secrets (run once):
# MAGIC # databricks secrets put --scope oracle --key cdc_username
# MAGIC # databricks secrets put --scope oracle --key cdc_password
# MAGIC
# MAGIC # Use in code:
# MAGIC username = dbutils.secrets.get("oracle", "cdc_username")
# MAGIC password = dbutils.secrets.get("oracle", "cdc_password")
# MAGIC
# MAGIC # Unity Catalog Connection stores credentials securely
# MAGIC # Grant access via Unity Catalog permissions:
# MAGIC # GRANT USE CONNECTION oracle_retail_connection TO `data_engineers`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Phase 3: Unity Catalog Setup
# MAGIC
# MAGIC #### Catalog and Schema Organization ✅
# MAGIC ```sql
# MAGIC -- Create production catalogs
# MAGIC CREATE CATALOG IF NOT EXISTS prod_retail
# MAGIC   COMMENT 'Production retail analytics catalog';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS prod_retail.landing
# MAGIC   COMMENT 'CDC staging volumes';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS prod_retail.bronze
# MAGIC   COMMENT 'Raw ingested tables';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS prod_retail.silver
# MAGIC   COMMENT 'Cleansed and conformed tables';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS prod_retail.gold
# MAGIC   COMMENT 'Business-level aggregations';
# MAGIC
# MAGIC -- Set owners
# MAGIC ALTER CATALOG prod_retail SET OWNER TO `data_platform_admins`;
# MAGIC ALTER SCHEMA prod_retail.landing SET OWNER TO `data_engineers`;
# MAGIC ALTER SCHEMA prod_retail.bronze SET OWNER TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC #### Access Control ✅
# MAGIC ```sql
# MAGIC -- Grant permissions following principle of least privilege
# MAGIC
# MAGIC -- Data Engineers: Full access to landing and bronze
# MAGIC GRANT USE CATALOG ON CATALOG prod_retail TO `data_engineers`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod_retail.landing TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA prod_retail.landing TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA prod_retail.bronze TO `data_engineers`;
# MAGIC
# MAGIC -- Data Analysts: Read-only access to bronze and silver
# MAGIC GRANT USE CATALOG ON CATALOG prod_retail TO `data_analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod_retail.bronze TO `data_analysts`;
# MAGIC GRANT SELECT ON SCHEMA prod_retail.bronze TO `data_analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod_retail.silver TO `data_analysts`;
# MAGIC GRANT SELECT ON SCHEMA prod_retail.silver TO `data_analysts`;
# MAGIC
# MAGIC -- Connection usage (for gateway)
# MAGIC GRANT USE CONNECTION ON CONNECTION oracle_retail_connection TO `data_engineers`;
# MAGIC
# MAGIC -- Volume access
# MAGIC GRANT READ VOLUME, WRITE VOLUME ON VOLUME prod_retail.landing.cdc_volume TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC #### Audit Logging ✅
# MAGIC ```sql
# MAGIC -- Enable audit logging for compliance
# MAGIC -- Unity Catalog automatically logs all access
# MAGIC
# MAGIC -- Query audit logs
# MAGIC SELECT 
# MAGIC   event_time,
# MAGIC   user_identity.email,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg AS resource,
# MAGIC   response.status_code
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name LIKE '%TABLE%'
# MAGIC   AND request_params.full_name_arg LIKE 'prod_retail.bronze.%'
# MAGIC   AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Strategies
# MAGIC
# MAGIC ### 1. Ingestion Gateway Monitoring
# MAGIC
# MAGIC #### Gateway Health Check
# MAGIC ```python
# MAGIC # Check gateway status
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # List all gateways
# MAGIC gateways = w.lakeflow.gateways.list()
# MAGIC for gateway in gateways:
# MAGIC     print(f"Gateway: {gateway.name}")
# MAGIC     print(f"  Status: {gateway.status}")
# MAGIC     print(f"  Created: {gateway.created_at}")
# MAGIC     print(f"  Connection: {gateway.connection_name}")
# MAGIC     if gateway.status != "RUNNING":
# MAGIC         print(f"  ⚠️ WARNING: Gateway is not running!")
# MAGIC ```
# MAGIC
# MAGIC #### Monitor SCN Lag
# MAGIC ```sql
# MAGIC -- Create monitoring view
# MAGIC CREATE OR REPLACE VIEW prod_retail.monitoring.cdc_lag AS
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   checkpoint_scn,
# MAGIC   last_update_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, last_update_time, CURRENT_TIMESTAMP()) AS minutes_since_update,
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, last_update_time, CURRENT_TIMESTAMP()) > 15 THEN 'CRITICAL'
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, last_update_time, CURRENT_TIMESTAMP()) > 10 THEN 'WARNING'
# MAGIC     ELSE 'OK'
# MAGIC   END AS status
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     'customers' AS table_name,
# MAGIC     MAX(_commit_scn) AS checkpoint_scn,
# MAGIC     MAX(_commit_timestamp) AS last_update_time
# MAGIC   FROM prod_retail.bronze.customers
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'orders' AS table_name,
# MAGIC     MAX(_commit_scn) AS checkpoint_scn,
# MAGIC     MAX(_commit_timestamp) AS last_update_time
# MAGIC   FROM prod_retail.bronze.orders
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'products' AS table_name,
# MAGIC     MAX(_commit_scn) AS checkpoint_scn,
# MAGIC     MAX(_commit_timestamp) AS last_update_time
# MAGIC   FROM prod_retail.bronze.products
# MAGIC );
# MAGIC
# MAGIC -- Query lag status
# MAGIC SELECT * FROM prod_retail.monitoring.cdc_lag 
# MAGIC WHERE status != 'OK';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Pipeline Monitoring
# MAGIC
# MAGIC #### Pipeline Execution History
# MAGIC ```sql
# MAGIC -- Monitor pipeline runs
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   update_id,
# MAGIC   state,
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, start_time, COALESCE(end_time, CURRENT_TIMESTAMP())) AS duration_minutes,
# MAGIC   details:records_processed::int AS records_processed,
# MAGIC   details:errors::array AS errors
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND event_type = 'PIPELINE_UPDATE'
# MAGIC   AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC ORDER BY start_time DESC;
# MAGIC ```
# MAGIC
# MAGIC #### Track Data Freshness
# MAGIC ```sql
# MAGIC -- Create freshness monitoring query
# MAGIC CREATE OR REPLACE VIEW prod_retail.monitoring.data_freshness AS
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   MAX(_commit_timestamp) AS latest_change,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS seconds_since_update,
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 300 THEN '❌ STALE'
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 120 THEN '⚠️ WARNING'
# MAGIC     ELSE '✅ FRESH'
# MAGIC   END AS freshness_status
# MAGIC FROM prod_retail.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'orders' AS table_name,
# MAGIC   MAX(_commit_timestamp) AS latest_change,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS seconds_since_update,
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 300 THEN '❌ STALE'
# MAGIC     WHEN TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) > 120 THEN '⚠️ WARNING'
# MAGIC     ELSE '✅ FRESH'
# MAGIC   END AS freshness_status
# MAGIC FROM prod_retail.bronze.orders;
# MAGIC
# MAGIC -- Query freshness
# MAGIC SELECT * FROM prod_retail.monitoring.data_freshness;
# MAGIC ```
# MAGIC
# MAGIC #### Error Tracking
# MAGIC ```sql
# MAGIC -- Monitor for errors and warnings
# MAGIC SELECT 
# MAGIC   event_time,
# MAGIC   pipeline_name,
# MAGIC   level,
# MAGIC   message,
# MAGIC   details
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND level IN ('ERROR', 'WARN')
# MAGIC   AND event_date >= CURRENT_DATE() - INTERVAL 1 DAY
# MAGIC ORDER BY event_time DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Data Quality Monitoring
# MAGIC
# MAGIC #### Row Count Reconciliation
# MAGIC ```python
# MAGIC # Compare source and target row counts
# MAGIC def reconcile_counts(table_name):
# MAGIC     # Source count from Oracle
# MAGIC     oracle_count = spark.read.jdbc(
# MAGIC         url=jdbc_url,
# MAGIC         table=f"(SELECT COUNT(*) AS cnt FROM {table_name})",
# MAGIC         properties=connection_props
# MAGIC     ).first().cnt
# MAGIC     
# MAGIC     # Target count from Bronze
# MAGIC     bronze_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM prod_retail.bronze.{table_name.lower()}").first().cnt
# MAGIC     
# MAGIC     diff = abs(oracle_count - bronze_count)
# MAGIC     diff_pct = (diff / oracle_count * 100) if oracle_count > 0 else 0
# MAGIC     
# MAGIC     print(f"Table: {table_name}")
# MAGIC     print(f"  Oracle count: {oracle_count:,}")
# MAGIC     print(f"  Bronze count: {bronze_count:,}")
# MAGIC     print(f"  Difference: {diff:,} ({diff_pct:.2f}%)")
# MAGIC     
# MAGIC     if diff_pct > 0.1:  # Alert if difference > 0.1%
# MAGIC         print(f"  ⚠️ WARNING: Count mismatch exceeds threshold!")
# MAGIC     else:
# MAGIC         print(f"  ✅ Counts match within tolerance")
# MAGIC     print()
# MAGIC
# MAGIC # Run reconciliation for all tables
# MAGIC for table in ['CUSTOMERS', 'ORDERS', 'PRODUCTS']:
# MAGIC     reconcile_counts(table)
# MAGIC ```
# MAGIC
# MAGIC #### Schema Drift Detection
# MAGIC ```sql
# MAGIC -- Monitor for schema changes
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   column_name,
# MAGIC   data_type,
# MAGIC   version AS delta_version
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     'customers' AS table_name,
# MAGIC     *
# MAGIC   FROM (DESCRIBE HISTORY prod_retail.bronze.customers)
# MAGIC   WHERE operation = 'ADD COLUMNS' OR operation = 'CHANGE COLUMN'
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     'orders' AS table_name,
# MAGIC     *
# MAGIC   FROM (DESCRIBE HISTORY prod_retail.bronze.orders)
# MAGIC   WHERE operation = 'ADD COLUMNS' OR operation = 'CHANGE COLUMN'
# MAGIC )
# MAGIC ORDER BY timestamp DESC;
# MAGIC ```
# MAGIC
# MAGIC #### Null Value Tracking
# MAGIC ```sql
# MAGIC -- Monitor critical columns for unexpected nulls
# MAGIC SELECT 
# MAGIC   'customers' AS table_name,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_emails,
# MAGIC   SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) AS null_phones,
# MAGIC   SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_email_pct
# MAGIC FROM prod_retail.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'orders' AS table_name,
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
# MAGIC   SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
# MAGIC   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_customer_id_pct
# MAGIC FROM prod_retail.bronze.orders;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Alerting Setup
# MAGIC
# MAGIC #### Databricks SQL Alerts
# MAGIC ```sql
# MAGIC -- Create alert for stale data (run in Databricks SQL)
# MAGIC -- Alert Name: CDC Data Freshness
# MAGIC -- Query:
# MAGIC SELECT 
# MAGIC   COUNT(*) AS stale_tables
# MAGIC FROM prod_retail.monitoring.data_freshness
# MAGIC WHERE freshness_status IN ('❌ STALE', '⚠️ WARNING');
# MAGIC
# MAGIC -- Alert Condition: Value > 0
# MAGIC -- Notification: Email data-platform-team@company.com
# MAGIC -- Schedule: Every 15 minutes
# MAGIC ```
# MAGIC
# MAGIC #### Pipeline Failure Alert
# MAGIC ```python
# MAGIC # Set up webhook for pipeline failures
# MAGIC # Configure in Databricks Pipeline settings:
# MAGIC
# MAGIC webhook_config = {
# MAGIC     "on_failure": [
# MAGIC         {
# MAGIC             "email_notifications": {
# MAGIC                 "recipients": ["data-eng@company.com"]
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "webhook_notifications": {
# MAGIC                 "url": "https://company.slack.com/webhooks/...",
# MAGIC                 "headers": {"Content-Type": "application/json"}
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "on_success": [],  # Optional: Alert on success
# MAGIC     "no_alert_for_skipped_runs": True
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### Custom Python Alert Script
# MAGIC ```python
# MAGIC # Run this periodically (e.g., via Databricks Job)
# MAGIC import requests
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC def check_cdc_health():
# MAGIC     alerts = []
# MAGIC     
# MAGIC     # Check 1: Data freshness
# MAGIC     freshness_df = spark.sql("SELECT * FROM prod_retail.monitoring.data_freshness WHERE freshness_status != '✅ FRESH'")
# MAGIC     if freshness_df.count() > 0:
# MAGIC         alerts.append(f"⚠️ {freshness_df.count()} table(s) have stale data")
# MAGIC     
# MAGIC     # Check 2: Pipeline failures
# MAGIC     failures_df = spark.sql("""
# MAGIC         SELECT COUNT(*) AS failure_count
# MAGIC         FROM system.lakeflow.pipeline_events
# MAGIC         WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC           AND state = 'FAILED'
# MAGIC           AND event_date >= CURRENT_DATE()
# MAGIC     """)
# MAGIC     failure_count = failures_df.first().failure_count
# MAGIC     if failure_count > 0:
# MAGIC         alerts.append(f"❌ {failure_count} pipeline failure(s) today")
# MAGIC     
# MAGIC     # Check 3: Gateway status
# MAGIC     lag_df = spark.sql("SELECT * FROM prod_retail.monitoring.cdc_lag WHERE status = 'CRITICAL'")
# MAGIC     if lag_df.count() > 0:
# MAGIC         alerts.append(f"🔴 {lag_df.count()} table(s) have critical SCN lag")
# MAGIC     
# MAGIC     # Send alerts if any issues found
# MAGIC     if alerts:
# MAGIC         slack_webhook = dbutils.secrets.get("monitoring", "slack_webhook")
# MAGIC         message = {
# MAGIC             "text": "CDC Health Check Alert",
# MAGIC             "blocks": [
# MAGIC                 {
# MAGIC                     "type": "section",
# MAGIC                     "text": {"type": "mrkdwn", "text": "\n".join(alerts)}
# MAGIC                 }
# MAGIC             ]
# MAGIC         }
# MAGIC         requests.post(slack_webhook, json=message)
# MAGIC         print("🚨 Alerts sent")
# MAGIC     else:
# MAGIC         print("✅ All CDC systems healthy")
# MAGIC
# MAGIC check_cdc_health()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC ### Oracle Optimization
# MAGIC
# MAGIC #### 1. Redo Log Sizing
# MAGIC ```sql
# MAGIC -- Check current redo log configuration
# MAGIC SELECT 
# MAGIC   l.group#,
# MAGIC   l.thread#,
# MAGIC   l.bytes/1024/1024 AS size_mb,
# MAGIC   l.status,
# MAGIC   f.member AS filename
# MAGIC FROM v$log l
# MAGIC JOIN v$logfile f ON l.group# = f.group#
# MAGIC ORDER BY l.group#;
# MAGIC
# MAGIC -- Recommendation: 512MB - 1GB per log file for CDC workloads
# MAGIC -- Add larger redo logs if needed (requires downtime):
# MAGIC -- ALTER DATABASE ADD LOGFILE GROUP 4 (
# MAGIC --   '/u01/oradata/RETAILDB/redo04a.log',
# MAGIC --   '/u02/oradata/RETAILDB/redo04b.log'
# MAGIC -- ) SIZE 1024M;
# MAGIC ```
# MAGIC
# MAGIC #### 2. Monitor Redo Generation Rate
# MAGIC ```sql
# MAGIC -- Track redo generation over time
# MAGIC SELECT 
# MAGIC   TRUNC(first_time) AS log_date,
# MAGIC   COUNT(*) AS log_switches,
# MAGIC   ROUND(SUM(blocks * block_size) / 1024 / 1024 / 1024, 2) AS redo_gb_generated
# MAGIC FROM v$archived_log
# MAGIC WHERE first_time > SYSDATE - 7
# MAGIC GROUP BY TRUNC(first_time)
# MAGIC ORDER BY log_date DESC;
# MAGIC
# MAGIC -- Ideal: < 24 log switches per hour
# MAGIC -- If more frequent, consider increasing redo log size
# MAGIC ```
# MAGIC
# MAGIC #### 3. Archive Log Cleanup Automation
# MAGIC ```sql
# MAGIC -- Configure RMAN for automatic archive log management
# MAGIC -- Run in RMAN:
# MAGIC
# MAGIC CONFIGURE ARCHIVELOG DELETION POLICY TO BACKED UP 1 TIMES TO DISK;
# MAGIC CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 7 DAYS;
# MAGIC
# MAGIC -- Create automated backup script (run daily):
# MAGIC RUN {
# MAGIC   BACKUP ARCHIVELOG ALL DELETE INPUT;
# MAGIC   DELETE NOPROMPT OBSOLETE;
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks Optimization
# MAGIC
# MAGIC #### 1. Delta Table Optimization
# MAGIC ```sql
# MAGIC -- Run OPTIMIZE regularly to compact small files
# MAGIC OPTIMIZE prod_retail.bronze.customers;
# MAGIC OPTIMIZE prod_retail.bronze.orders ZORDER BY (customer_id, order_date);
# MAGIC OPTIMIZE prod_retail.bronze.products;
# MAGIC
# MAGIC -- Enable Auto Optimize for future writes
# MAGIC ALTER TABLE prod_retail.bronze.customers 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Set data retention for VACUUM
# MAGIC ALTER TABLE prod_retail.bronze.customers 
# MAGIC SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days');
# MAGIC
# MAGIC -- Run VACUUM to clean up old files (run weekly)
# MAGIC VACUUM prod_retail.bronze.customers RETAIN 168 HOURS; -- 7 days
# MAGIC ```
# MAGIC
# MAGIC #### 2. Ingestion Gateway Sizing
# MAGIC ```
# MAGIC Change Volume          Gateway Size    Monthly Cost (Estimate)
# MAGIC ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAGIC < 1K rows/min         Small           $300 - $500
# MAGIC 1K - 10K rows/min     Medium          $800 - $1,200
# MAGIC 10K - 100K rows/min   Large           $2,000 - $3,000
# MAGIC > 100K rows/min       X-Large         $5,000+
# MAGIC
# MAGIC Right-sizing tips:
# MAGIC - Start with Small, monitor CPU and memory usage
# MAGIC - Upgrade if gateway CPU > 80% consistently
# MAGIC - Downgrade if CPU < 30% for extended periods
# MAGIC - Monitor via Databricks system metrics
# MAGIC ```
# MAGIC
# MAGIC #### 3. Pipeline Cluster Configuration
# MAGIC ```python
# MAGIC # Recommended DLT pipeline cluster configuration
# MAGIC pipeline_config = {
# MAGIC     "name": "retail_ingestion_pipeline",
# MAGIC     "storage": "/mnt/prod_retail/pipelines/retail_ingestion",
# MAGIC     "configuration": {
# MAGIC         "spark.databricks.delta.optimizeWrite.enabled": "true",
# MAGIC         "spark.databricks.delta.autoCompact.enabled": "true"
# MAGIC     },
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5,  # Scale based on change volume
# MAGIC                 "mode": "ENHANCED"
# MAGIC             },
# MAGIC             "instance_pool_id": "pool-...",  # Use instance pools for cost savings
# MAGIC             "spark_conf": {
# MAGIC                 "spark.sql.shuffle.partitions": "auto"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "continuous": True,  # For real-time CDC
# MAGIC     "development": False  # Set to True for dev/test environments
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Optimization
# MAGIC
# MAGIC ### 1. Gateway Scheduling
# MAGIC ```python
# MAGIC # For non-24/7 requirements, schedule gateway start/stop
# MAGIC # Example: Run only during business hours
# MAGIC
# MAGIC # Morning startup job (runs at 7 AM)
# MAGIC # Task: Start ingestion gateway
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.lakeflow.gateways.start(gateway_id="retail_ingestion_gateway")
# MAGIC print("✅ Gateway started for business day")
# MAGIC
# MAGIC # Evening shutdown job (runs at 7 PM)
# MAGIC # Task: Stop ingestion gateway
# MAGIC w.lakeflow.gateways.stop(gateway_id="retail_ingestion_gateway")
# MAGIC print("✅ Gateway stopped after business hours")
# MAGIC
# MAGIC # Cost savings: 12 hours/day offline = ~50% reduction in gateway costs
# MAGIC ```
# MAGIC
# MAGIC ### 2. Pipeline Scheduling
# MAGIC ```python
# MAGIC # For non-real-time requirements, use triggered mode instead of continuous
# MAGIC pipeline_config = {
# MAGIC     "continuous": False,  # Disable continuous mode
# MAGIC     "trigger": {
# MAGIC         "schedule": {
# MAGIC             "quartz_cron_expression": "0 */15 * * * ?",  # Every 15 minutes
# MAGIC             "timezone_id": "America/Los_Angeles"
# MAGIC         }
# MAGIC     }
# MAGIC }
# MAGIC
# MAGIC # Cost benefit: Cluster only runs when processing changes
# MAGIC # Example: 15-min runs every hour = 75% idle time = significant savings
# MAGIC ```
# MAGIC
# MAGIC ### 3. Volume Lifecycle Management
# MAGIC ```sql
# MAGIC -- Clean up old staging files periodically
# MAGIC -- Create job that runs weekly:
# MAGIC
# MAGIC -- List files older than 7 days
# MAGIC SELECT * FROM dbutils.fs.ls('/Volumes/prod_retail/landing/cdc_volume/customers/incremental/')
# MAGIC WHERE modification_time < CURRENT_TIMESTAMP() - INTERVAL 7 DAYS;
# MAGIC
# MAGIC -- Delete old files (implement carefully with proper testing!)
# MAGIC -- dbutils.fs.rm('/Volumes/prod_retail/landing/cdc_volume/customers/incremental/20260103-*', recurse=True)
# MAGIC ```
# MAGIC
# MAGIC ### 4. Spot/Preemptible Instances
# MAGIC ```python
# MAGIC # Use spot instances for non-critical workloads
# MAGIC cluster_config = {
# MAGIC     "aws_attributes": {
# MAGIC         "availability": "SPOT_WITH_FALLBACK",
# MAGIC         "spot_bid_price_percent": 100,
# MAGIC         "fallback_to_ondemand": True
# MAGIC     }
# MAGIC }
# MAGIC # Potential savings: 50-70% on compute costs
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disaster Recovery & Business Continuity
# MAGIC
# MAGIC ### Backup Strategy
# MAGIC
# MAGIC #### 1. Oracle Backup (DBA Responsibility)
# MAGIC ```sql
# MAGIC -- Ensure regular Oracle backups with RMAN
# MAGIC -- Backup includes: datafiles, control files, archive logs
# MAGIC -- Test restore procedure quarterly
# MAGIC
# MAGIC -- Example RMAN backup script:
# MAGIC BACKUP DATABASE PLUS ARCHIVELOG;
# MAGIC BACKUP CURRENT CONTROLFILE;
# MAGIC ```
# MAGIC
# MAGIC #### 2. Unity Catalog Metadata Backup
# MAGIC ```python
# MAGIC # Export Unity Catalog metadata for disaster recovery
# MAGIC # Run weekly and store in separate cloud storage
# MAGIC
# MAGIC import json
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Backup connection definitions
# MAGIC connections = list(w.connections.list())
# MAGIC connection_backup = [
# MAGIC     {
# MAGIC         "name": conn.name,
# MAGIC         "connection_type": conn.connection_type,
# MAGIC         "options": conn.options,
# MAGIC         "comment": conn.comment
# MAGIC     }
# MAGIC     for conn in connections
# MAGIC ]
# MAGIC
# MAGIC # Save to external location
# MAGIC backup_path = "s3://company-backups/databricks/connections/backup_20260110.json"
# MAGIC dbutils.fs.put(backup_path, json.dumps(connection_backup), overwrite=True)
# MAGIC print(f"✅ Backed up {len(connection_backup)} connections to {backup_path}")
# MAGIC ```
# MAGIC
# MAGIC #### 3. Delta Table DEEP CLONE for Recovery
# MAGIC ```sql
# MAGIC -- Create periodic snapshots for rollback capability
# MAGIC -- Run daily during low-activity period:
# MAGIC
# MAGIC CREATE OR REPLACE TABLE prod_retail.backup.customers_snapshot_20260110
# MAGIC DEEP CLONE prod_retail.bronze.customers;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE prod_retail.backup.orders_snapshot_20260110
# MAGIC DEEP CLONE prod_retail.bronze.orders;
# MAGIC
# MAGIC -- Restore from snapshot if needed:
# MAGIC -- CREATE OR REPLACE TABLE prod_retail.bronze.customers
# MAGIC -- DEEP CLONE prod_retail.backup.customers_snapshot_20260110;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recovery Procedures
# MAGIC
# MAGIC #### Scenario 1: Gateway Failure
# MAGIC ```
# MAGIC Symptom: Gateway status shows "FAILED" or "TERMINATED"
# MAGIC Impact: CDC ingestion stops, data becomes stale
# MAGIC
# MAGIC Recovery Steps:
# MAGIC 1. Check gateway logs for error details
# MAGIC 2. Verify Oracle database is accessible
# MAGIC 3. Verify archive logs are available
# MAGIC 4. Restart gateway:
# MAGIC    w.lakeflow.gateways.restart(gateway_id="retail_ingestion_gateway")
# MAGIC 5. Monitor checkpoint - should resume from last SCN
# MAGIC 6. Validate data freshness after restart
# MAGIC
# MAGIC Expected Recovery Time: 5-10 minutes
# MAGIC Data Loss Risk: None (SCN checkpoint preserved)
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 2: Pipeline Failure
# MAGIC ```
# MAGIC Symptom: Pipeline status shows "FAILED"
# MAGIC Impact: Staged changes not applied to bronze tables
# MAGIC
# MAGIC Recovery Steps:
# MAGIC 1. Review pipeline error logs
# MAGIC 2. Check for schema mismatches or data quality issues
# MAGIC 3. If transient error, restart pipeline (auto-retry enabled)
# MAGIC 4. If persistent error:
# MAGIC    a. Fix root cause (e.g., add missing column)
# MAGIC    b. Reset pipeline to last good checkpoint
# MAGIC    c. Resume pipeline
# MAGIC 5. Validate data consistency
# MAGIC
# MAGIC Expected Recovery Time: 10-30 minutes
# MAGIC Data Loss Risk: None (staged data preserved in volume)
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 3: Archive Log Gap
# MAGIC ```
# MAGIC Symptom: Gateway switches to snapshot mode unexpectedly
# MAGIC Root Cause: Required archive logs were deleted
# MAGIC Impact: Full table reload required
# MAGIC
# MAGIC Recovery Steps:
# MAGIC 1. Identify missing archive log range from gateway logs
# MAGIC 2. Check if logs can be restored from backup
# MAGIC 3. If logs unrecoverable, allow snapshot to complete:
# MAGIC    - Large tables may take hours
# MAGIC    - Monitor progress via gateway metrics
# MAGIC 4. After snapshot completes, incremental CDC resumes
# MAGIC 5. Implement better archive log retention to prevent recurrence
# MAGIC
# MAGIC Expected Recovery Time: Hours to days (depends on table size)
# MAGIC Data Loss Risk: None (snapshot captures current state)
# MAGIC Data Lag: Potential gap during snapshot execution
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security Best Practices
# MAGIC
# MAGIC ### 1. Least Privilege Access
# MAGIC ```sql
# MAGIC -- Oracle CDC user should have minimal permissions
# MAGIC -- Required grants only:
# MAGIC GRANT SELECT ANY TRANSACTION TO cdc_user;
# MAGIC GRANT SELECT ON V_$LOGMNR_CONTENTS TO cdc_user;
# MAGIC GRANT EXECUTE ON DBMS_LOGMNR TO cdc_user;
# MAGIC GRANT EXECUTE ON DBMS_LOGMNR_D TO cdc_user;
# MAGIC
# MAGIC -- For specific tables only (preferred):
# MAGIC GRANT SELECT ON RETAIL.CUSTOMERS TO cdc_user;
# MAGIC GRANT SELECT ON RETAIL.ORDERS TO cdc_user;
# MAGIC GRANT SELECT ON RETAIL.PRODUCTS TO cdc_user;
# MAGIC
# MAGIC -- NO write permissions needed!
# MAGIC -- NO DBA privileges needed after initial setup!
# MAGIC ```
# MAGIC
# MAGIC ### 2. Network Security
# MAGIC ```
# MAGIC ✅ Best: Private connectivity (AWS PrivateLink, Azure Private Link)
# MAGIC ✅ Good: VPN tunnel with encryption
# MAGIC ⚠️ Acceptable: IP allowlist with TLS encryption
# MAGIC ❌ Never: Public internet without encryption
# MAGIC
# MAGIC Configure Oracle for encrypted connections:
# MAGIC - Enable SSL/TLS in listener.ora
# MAGIC - Use Oracle Wallet for certificate management
# MAGIC - Update JDBC URL: jdbc:oracle:thin:@(DESCRIPTION=...SECURITY=(SSL_SERVER_CERT_DN=...))
# MAGIC ```
# MAGIC
# MAGIC ### 3. Data Masking for Sensitive Fields
# MAGIC ```sql
# MAGIC -- Apply masking in silver layer for PII fields
# MAGIC CREATE OR REPLACE VIEW prod_retail.silver.customers_masked AS
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   CONCAT('***@', SPLIT(email, '@')[1]) AS email_masked,  -- Mask email local part
# MAGIC   CONCAT(SUBSTRING(phone, 1, 3), '-***-****') AS phone_masked,  -- Mask phone
# MAGIC   city,
# MAGIC   state,
# MAGIC   SUBSTRING(zip_code, 1, 3) || '**' AS zip_masked,  -- Mask ZIP+4
# MAGIC   created_date
# MAGIC FROM prod_retail.bronze.customers;
# MAGIC
# MAGIC -- Grant analysts access to masked view only
# MAGIC GRANT SELECT ON VIEW prod_retail.silver.customers_masked TO `data_analysts`;
# MAGIC REVOKE SELECT ON TABLE prod_retail.bronze.customers FROM `data_analysts`;
# MAGIC ```
# MAGIC
# MAGIC ### 4. Audit Compliance
# MAGIC ```sql
# MAGIC -- Track all access to sensitive tables
# MAGIC SELECT 
# MAGIC   user_identity.email,
# MAGIC   action_name,
# MAGIC   request_params.full_name_arg AS table_accessed,
# MAGIC   COUNT(*) AS access_count,
# MAGIC   MIN(event_time) AS first_access,
# MAGIC   MAX(event_time) AS last_access
# MAGIC FROM system.access.audit
# MAGIC WHERE request_params.full_name_arg LIKE '%customers%'
# MAGIC   AND event_date >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY access_count DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Deployment Summary
# MAGIC
# MAGIC ### Pre-Launch Checklist
# MAGIC
# MAGIC **Oracle Database:**
# MAGIC - [ ] ARCHIVELOG mode enabled and tested
# MAGIC - [ ] Supplemental logging configured on all CDC tables
# MAGIC - [ ] Archive log retention policy set (48+ hours recommended)
# MAGIC - [ ] RMAN backup schedule implemented
# MAGIC - [ ] Redo log sizing optimized (512MB-1GB recommended)
# MAGIC - [ ] CDC user created with minimal privileges
# MAGIC - [ ] Baseline performance metrics captured
# MAGIC
# MAGIC **Network & Security:**
# MAGIC - [ ] Connectivity tested from Databricks to Oracle
# MAGIC - [ ] Firewall rules configured
# MAGIC - [ ] Private link or VPN established (recommended)
# MAGIC - [ ] TLS encryption enabled for JDBC connection
# MAGIC - [ ] Credentials stored in Databricks Secrets
# MAGIC
# MAGIC **Databricks Configuration:**
# MAGIC - [ ] Unity Catalog catalogs and schemas created
# MAGIC - [ ] Access control policies applied
# MAGIC - [ ] Unity Catalog connection configured
# MAGIC - [ ] Ingestion gateway provisioned and tested
# MAGIC - [ ] Ingestion pipeline created and validated
# MAGIC - [ ] Initial snapshot completed successfully
# MAGIC - [ ] Incremental CDC tested with sample changes
# MAGIC
# MAGIC **Monitoring & Alerting:**
# MAGIC - [ ] Gateway health monitoring configured
# MAGIC - [ ] SCN lag alerts set up
# MAGIC - [ ] Data freshness monitoring implemented
# MAGIC - [ ] Pipeline failure alerts configured
# MAGIC - [ ] Row count reconciliation scheduled
# MAGIC - [ ] Slack/email notification channels tested
# MAGIC
# MAGIC **Documentation:**
# MAGIC - [ ] Architecture diagram created
# MAGIC - [ ] Runbook documented
# MAGIC - [ ] Escalation procedures defined
# MAGIC - [ ] Disaster recovery plan documented
# MAGIC - [ ] Training completed for operations team

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Critical Success Factors
# MAGIC 1. **Archive Log Management** - Most common production issue; plan retention carefully
# MAGIC 2. **Monitoring** - Implement comprehensive monitoring before go-live
# MAGIC 3. **Testing** - Validate recovery procedures before production deployment
# MAGIC 4. **Documentation** - Maintain clear runbooks for operations team
# MAGIC 5. **Security** - Follow least privilege and encryption best practices
# MAGIC
# MAGIC ### Common Pitfalls to Avoid
# MAGIC - ❌ Insufficient archive log retention
# MAGIC - ❌ No monitoring until issues arise
# MAGIC - ❌ Overly permissive Oracle user privileges
# MAGIC - ❌ No testing of failure scenarios
# MAGIC - ❌ Hardcoded credentials in code
# MAGIC - ❌ No documented recovery procedures
# MAGIC - ❌ Ignoring cost optimization opportunities
# MAGIC
# MAGIC ### Production Maturity Path
# MAGIC ```
# MAGIC Phase 1: Basic Production (Week 1-2)
# MAGIC   ✓ Core CDC pipeline operational
# MAGIC   ✓ Basic monitoring in place
# MAGIC   ✓ Manual intervention for issues
# MAGIC
# MAGIC Phase 2: Mature Production (Week 3-4)
# MAGIC   ✓ Automated alerting configured
# MAGIC   ✓ Performance optimized
# MAGIC   ✓ Cost controls implemented
# MAGIC   ✓ Documented runbooks
# MAGIC
# MAGIC Phase 3: Advanced Production (Month 2+)
# MAGIC   ✓ Predictive monitoring
# MAGIC   ✓ Auto-remediation for common issues
# MAGIC   ✓ Advanced cost optimization
# MAGIC   ✓ Disaster recovery tested quarterly
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 8: Implementing Scheduled Pipelines and Error Handling** where you'll configure production-grade automation, implement error handling patterns, and set up comprehensive monitoring for your CDC pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC © 2026 Databricks, Inc. All rights reserved. This content is for workshop educational purposes only.
