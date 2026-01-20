# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture 7: Production Best Practices and Monitoring Strategies
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will understand:
# MAGIC - Production-grade configuration for MySQL CDC pipelines
# MAGIC - Binary log retention and management strategies
# MAGIC - Comprehensive monitoring and alerting approaches
# MAGIC - Error handling patterns and recovery procedures
# MAGIC - Performance optimization techniques (Z-ordering, partitioning, compaction)
# MAGIC - Cost optimization strategies for Lakeflow Connect
# MAGIC - Security best practices and compliance considerations
# MAGIC - Disaster recovery and business continuity planning
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Labs 2-6 (MySQL configuration through multi-table CDC)
# MAGIC - Completed Lecture 4 (Ingestion modes and checkpoints)
# MAGIC - Operational experience with test CDC pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Readiness Checklist
# MAGIC
# MAGIC Before deploying CDC pipelines to production:
# MAGIC
# MAGIC ### MySQL Configuration
# MAGIC - ✅ Binary logging enabled with `binlog_format = ROW`
# MAGIC - ✅ GTID enabled for failover resilience (`gtid_mode = ON`)
# MAGIC - ✅ Binary log retention ≥ 2x expected downtime (`binlog_expire_logs_seconds`)
# MAGIC - ✅ Row image optimized for CDC (`binlog_row_image = MINIMAL`)
# MAGIC - ✅ CDC user has minimal required privileges (REPLICATION SLAVE, REPLICATION CLIENT, SELECT)
# MAGIC - ✅ Binary log disk space monitored (alerts at 70% capacity)
# MAGIC
# MAGIC ### Network and Security
# MAGIC - ✅ Private connectivity established (VPN, PrivateLink, VPC peering)
# MAGIC - ✅ Firewall rules configured for Databricks IP ranges
# MAGIC - ✅ SSL/TLS enabled for MySQL connections (if required)
# MAGIC - ✅ Secrets management configured (Databricks Secrets for credentials)
# MAGIC - ✅ Unity Catalog permissions properly restricted
# MAGIC - ✅ Network bandwidth adequate for peak change volumes
# MAGIC
# MAGIC ### Pipeline Configuration
# MAGIC - ✅ Ingestion gateway VM sized appropriately for workload
# MAGIC - ✅ Staging volume location and quota configured
# MAGIC - ✅ Pipeline schedule aligned with latency requirements
# MAGIC - ✅ Error handling and retry logic implemented
# MAGIC - ✅ Table dependencies mapped and configured
# MAGIC - ✅ Initial snapshot completed successfully
# MAGIC
# MAGIC ### Monitoring and Alerting
# MAGIC - ✅ Pipeline health monitoring dashboard created
# MAGIC - ✅ Binary log lag alerts configured
# MAGIC - ✅ Pipeline failure notifications set up
# MAGIC - ✅ Data quality checks implemented
# MAGIC - ✅ Lineage and audit logging enabled
# MAGIC - ✅ Performance baseline established
# MAGIC
# MAGIC ### Documentation and Runbooks
# MAGIC - ✅ Architecture diagram documented
# MAGIC - ✅ Runbook for common issues created
# MAGIC - ✅ Recovery procedures tested and documented
# MAGIC - ✅ On-call escalation path defined
# MAGIC - ✅ Capacity planning estimates documented

# COMMAND ----------

# MAGIC %md
# MAGIC ## Binary Log Management
# MAGIC
# MAGIC ### Retention Policy Strategy
# MAGIC
# MAGIC **Formula for Retention Calculation**:
# MAGIC ```
# MAGIC retention_seconds = 2 × (max_expected_downtime + max_pipeline_duration + buffer)
# MAGIC
# MAGIC Example:
# MAGIC - Max expected downtime: 8 hours (weekend maintenance window)
# MAGIC - Max pipeline duration: 4 hours (snapshot load of largest table)
# MAGIC - Safety buffer: 12 hours
# MAGIC - Total: 2 × (8 + 4 + 12) = 48 hours
# MAGIC
# MAGIC Recommended: 172800 seconds (48 hours) to 259200 seconds (72 hours)
# MAGIC ```
# MAGIC
# MAGIC **Configure on MySQL**:
# MAGIC ```sql
# MAGIC -- Set 3-day retention
# MAGIC SET GLOBAL binlog_expire_logs_seconds = 259200;
# MAGIC
# MAGIC -- Verify configuration
# MAGIC SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
# MAGIC
# MAGIC -- Check current binary logs
# MAGIC SHOW BINARY LOGS;
# MAGIC ```
# MAGIC
# MAGIC ### Binary Log Disk Space Management
# MAGIC
# MAGIC **Monitoring Query**:
# MAGIC ```sql
# MAGIC -- Check total binary log disk usage
# MAGIC SELECT 
# MAGIC     CONCAT(ROUND(SUM(file_size)/1024/1024/1024, 2), ' GB') as total_binlog_size
# MAGIC FROM information_schema.BINARY_LOGS;
# MAGIC
# MAGIC -- List individual binary log files
# MAGIC SELECT 
# MAGIC     log_name,
# MAGIC     CONCAT(ROUND(file_size/1024/1024, 2), ' MB') as size_mb,
# MAGIC     encrypted
# MAGIC FROM information_schema.BINARY_LOGS
# MAGIC ORDER BY log_name DESC;
# MAGIC ```
# MAGIC
# MAGIC **Disk Space Alerts**:
# MAGIC ```python
# MAGIC # Set up alerts in your monitoring system
# MAGIC alerts = {
# MAGIC     "binlog_disk_usage_70_percent": "warning - review retention policy",
# MAGIC     "binlog_disk_usage_85_percent": "critical - immediate action required",
# MAGIC     "binlog_disk_usage_95_percent": "emergency - MySQL may stop accepting writes"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Optimal Binary Log Configuration
# MAGIC
# MAGIC **my.cnf / my.ini Settings**:
# MAGIC ```ini
# MAGIC [mysqld]
# MAGIC # Binary logging
# MAGIC log_bin = mysql-bin
# MAGIC binlog_format = ROW                    # Required for CDC
# MAGIC binlog_row_image = MINIMAL             # Reduce log size (only changed columns)
# MAGIC max_binlog_size = 1073741824          # 1GB per file (default)
# MAGIC
# MAGIC # GTID configuration
# MAGIC gtid_mode = ON                         # Required for failover
# MAGIC enforce_gtid_consistency = ON
# MAGIC
# MAGIC # Retention and expiration
# MAGIC binlog_expire_logs_seconds = 259200   # 3 days
# MAGIC
# MAGIC # Performance tuning
# MAGIC sync_binlog = 1                        # Durability (flush to disk per transaction)
# MAGIC binlog_cache_size = 32768             # Per-connection cache (adjust based on transaction size)
# MAGIC max_binlog_cache_size = 4294967296   # Max cache size (4GB)
# MAGIC
# MAGIC # Server identification
# MAGIC server_id = 1                          # Unique ID in replication topology
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Observability
# MAGIC
# MAGIC ### Key Metrics to Track
# MAGIC
# MAGIC #### 1. Pipeline Health Metrics
# MAGIC
# MAGIC ```sql
# MAGIC -- Pipeline execution history
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     update_id,
# MAGIC     timestamp,
# MAGIC     state,  -- 'RUNNING', 'COMPLETED', 'FAILED'
# MAGIC     error_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC   AND timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC ORDER BY timestamp DESC;
# MAGIC
# MAGIC -- Calculate success rate
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     COUNT(*) as total_runs,
# MAGIC     SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY pipeline_name;
# MAGIC ```
# MAGIC
# MAGIC #### 2. Ingestion Lag Metrics
# MAGIC
# MAGIC ```sql
# MAGIC -- Check time lag between source changes and bronze tables
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     MAX(_commit_timestamp) as last_ingested_change,
# MAGIC     CURRENT_TIMESTAMP() as current_time,
# MAGIC     TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as lag_minutes
# MAGIC FROM bronze.customers
# MAGIC GROUP BY table_name
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'orders', MAX(_commit_timestamp), CURRENT_TIMESTAMP(), 
# MAGIC        TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
# MAGIC FROM bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'products', MAX(_commit_timestamp), CURRENT_TIMESTAMP(),
# MAGIC        TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
# MAGIC FROM bronze.products;
# MAGIC ```
# MAGIC
# MAGIC #### 3. Change Volume Metrics
# MAGIC
# MAGIC ```sql
# MAGIC -- Track changes processed per hour
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('HOUR', _commit_timestamp) as hour,
# MAGIC     _change_type,
# MAGIC     COUNT(*) as change_count
# MAGIC FROM bronze.orders
# MAGIC WHERE _commit_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_TRUNC('HOUR', _commit_timestamp), _change_type
# MAGIC ORDER BY hour DESC, change_count DESC;
# MAGIC
# MAGIC -- Identify spike in changes (anomaly detection)
# MAGIC WITH hourly_changes AS (
# MAGIC     SELECT 
# MAGIC         DATE_TRUNC('HOUR', _commit_timestamp) as hour,
# MAGIC         COUNT(*) as change_count
# MAGIC     FROM bronze.orders
# MAGIC     WHERE _commit_timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC     GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
# MAGIC )
# MAGIC SELECT 
# MAGIC     hour,
# MAGIC     change_count,
# MAGIC     AVG(change_count) OVER () as avg_changes,
# MAGIC     STDDEV(change_count) OVER () as stddev_changes,
# MAGIC     CASE 
# MAGIC         WHEN change_count > AVG(change_count) OVER () + 2 * STDDEV(change_count) OVER () 
# MAGIC         THEN 'ANOMALY'
# MAGIC         ELSE 'NORMAL'
# MAGIC     END as status
# MAGIC FROM hourly_changes
# MAGIC ORDER BY hour DESC;
# MAGIC ```
# MAGIC
# MAGIC #### 4. Data Quality Metrics
# MAGIC
# MAGIC ```sql
# MAGIC -- Check for null values in critical fields
# MAGIC SELECT 
# MAGIC     'customers' as table_name,
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
# MAGIC     SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails
# MAGIC FROM bronze.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'orders',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)
# MAGIC FROM bronze.orders;
# MAGIC
# MAGIC -- Validate referential integrity
# MAGIC SELECT 
# MAGIC     COUNT(*) as orphaned_orders
# MAGIC FROM bronze.orders o
# MAGIC LEFT JOIN bronze.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alerting Strategy
# MAGIC
# MAGIC ### Critical Alerts (Immediate Action)
# MAGIC
# MAGIC ```python
# MAGIC critical_alerts = {
# MAGIC     "pipeline_failure": {
# MAGIC         "condition": "pipeline state = 'FAILED'",
# MAGIC         "action": "Page on-call engineer immediately",
# MAGIC         "escalation": "15 minutes"
# MAGIC     },
# MAGIC     "gateway_disconnected": {
# MAGIC         "condition": "gateway status = 'DISCONNECTED' for > 5 minutes",
# MAGIC         "action": "Check network connectivity, restart gateway",
# MAGIC         "escalation": "10 minutes"
# MAGIC     },
# MAGIC     "binlog_lag_critical": {
# MAGIC         "condition": "lag > 75% of retention period",
# MAGIC         "action": "Risk of checkpoint expiration - investigate immediately",
# MAGIC         "escalation": "30 minutes"
# MAGIC     },
# MAGIC     "mysql_connection_failure": {
# MAGIC         "condition": "Unable to connect to MySQL for > 5 minutes",
# MAGIC         "action": "Check MySQL availability, credentials, network",
# MAGIC         "escalation": "15 minutes"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Warning Alerts (Monitor and Plan)
# MAGIC
# MAGIC ```python
# MAGIC warning_alerts = {
# MAGIC     "increased_execution_time": {
# MAGIC         "condition": "pipeline duration > 2x baseline",
# MAGIC         "action": "Review change volume, consider optimization",
# MAGIC         "review": "daily"
# MAGIC     },
# MAGIC     "binlog_disk_70_percent": {
# MAGIC         "condition": "binary log disk usage > 70%",
# MAGIC         "action": "Review retention policy, plan capacity increase",
# MAGIC         "review": "weekly"
# MAGIC     },
# MAGIC     "staging_volume_growth": {
# MAGIC         "condition": "staging volume size > threshold",
# MAGIC         "action": "Check for pipeline backlog, implement cleanup",
# MAGIC         "review": "daily"
# MAGIC     },
# MAGIC     "data_quality_issues": {
# MAGIC         "condition": "null values or referential integrity violations > 1%",
# MAGIC         "action": "Investigate source data quality issues",
# MAGIC         "review": "daily"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Alert Implementation Example
# MAGIC
# MAGIC ```sql
# MAGIC -- Create alert view for monitoring dashboard
# MAGIC CREATE OR REPLACE VIEW monitoring.cdc_alerts AS
# MAGIC SELECT 
# MAGIC     'Pipeline Failure' as alert_type,
# MAGIC     'CRITICAL' as severity,
# MAGIC     pipeline_name,
# MAGIC     error_message,
# MAGIC     timestamp
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE state = 'FAILED'
# MAGIC   AND timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'High Ingestion Lag',
# MAGIC     CASE 
# MAGIC         WHEN lag_minutes > 180 THEN 'CRITICAL'
# MAGIC         WHEN lag_minutes > 60 THEN 'WARNING'
# MAGIC         ELSE 'INFO'
# MAGIC     END,
# MAGIC     table_name,
# MAGIC     CONCAT('Lag: ', lag_minutes, ' minutes'),
# MAGIC     MAX(_commit_timestamp)
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         'customers' as table_name,
# MAGIC         MAX(_commit_timestamp) as _commit_timestamp,
# MAGIC         TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as lag_minutes
# MAGIC     FROM bronze.customers
# MAGIC ) WHERE lag_minutes > 60;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling Patterns
# MAGIC
# MAGIC ### Transient vs. Permanent Failures
# MAGIC
# MAGIC **Transient Failures** (Retry Automatically):
# MAGIC - Network timeouts
# MAGIC - MySQL connection drops
# MAGIC - Temporary resource unavailability
# MAGIC - Databricks cluster cold start
# MAGIC
# MAGIC **Permanent Failures** (Require Intervention):
# MAGIC - Schema incompatibility
# MAGIC - Invalid credentials
# MAGIC - Binary log expired/not found
# MAGIC - Table dropped on source
# MAGIC
# MAGIC ### Retry Configuration
# MAGIC
# MAGIC ```python
# MAGIC # Configure in pipeline settings
# MAGIC retry_policy = {
# MAGIC     "max_retries": 3,
# MAGIC     "initial_delay_seconds": 60,
# MAGIC     "max_delay_seconds": 600,
# MAGIC     "backoff_multiplier": 2,  # Exponential backoff
# MAGIC     "retryable_errors": [
# MAGIC         "ConnectionTimeout",
# MAGIC         "NetworkError",
# MAGIC         "TemporaryResourceUnavailable"
# MAGIC     ]
# MAGIC }
# MAGIC
# MAGIC # Example retry sequence:
# MAGIC # Attempt 1: Immediate
# MAGIC # Attempt 2: Wait 60 seconds
# MAGIC # Attempt 3: Wait 120 seconds
# MAGIC # Attempt 4: Wait 240 seconds
# MAGIC # After 4 attempts: Mark as failed, alert
# MAGIC ```
# MAGIC
# MAGIC ### Circuit Breaker Pattern
# MAGIC
# MAGIC Prevent cascading failures by temporarily disabling pipeline after repeated failures:
# MAGIC
# MAGIC ```python
# MAGIC circuit_breaker = {
# MAGIC     "failure_threshold": 5,           # Open circuit after 5 failures
# MAGIC     "timeout_seconds": 300,            # Keep circuit open for 5 minutes
# MAGIC     "success_threshold": 2             # Close circuit after 2 successful runs
# MAGIC }
# MAGIC
# MAGIC # State transitions:
# MAGIC # CLOSED (normal) → failures > 5 → OPEN (stopped)
# MAGIC # OPEN → wait 5 min → HALF_OPEN (test)
# MAGIC # HALF_OPEN → 2 successes → CLOSED
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC ### Delta Table Optimization
# MAGIC
# MAGIC #### 1. Z-Ordering for Query Performance
# MAGIC
# MAGIC ```sql
# MAGIC -- Optimize bronze tables for common query patterns
# MAGIC OPTIMIZE bronze.customers
# MAGIC ZORDER BY (customer_id, state, city);
# MAGIC
# MAGIC OPTIMIZE bronze.orders
# MAGIC ZORDER BY (customer_id, order_date, order_status);
# MAGIC
# MAGIC OPTIMIZE bronze.products
# MAGIC ZORDER BY (category, supplier, product_id);
# MAGIC
# MAGIC -- Schedule regular optimization (weekly)
# MAGIC -- Run during off-peak hours
# MAGIC ```
# MAGIC
# MAGIC #### 2. Auto Optimize and Auto Compaction
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable Auto Optimize on bronze tables
# MAGIC ALTER TABLE bronze.customers
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE bronze.orders
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC #### 3. Partitioning Strategy
# MAGIC
# MAGIC ```sql
# MAGIC -- For large tables (>1B rows), consider partitioning
# MAGIC CREATE TABLE bronze.orders_partitioned (
# MAGIC     order_id INT,
# MAGIC     customer_id INT,
# MAGIC     order_date DATE,
# MAGIC     order_status STRING,
# MAGIC     total_amount DECIMAL(10,2),
# MAGIC     _commit_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE_TRUNC('MONTH', order_date))
# MAGIC LOCATION '/mnt/retail_analytics/bronze/orders_partitioned';
# MAGIC
# MAGIC -- Partition pruning dramatically improves query performance
# MAGIC SELECT * FROM bronze.orders_partitioned
# MAGIC WHERE order_date >= '2024-01-01'  -- Only scans relevant partitions
# MAGIC   AND order_date < '2024-02-01';
# MAGIC ```
# MAGIC
# MAGIC #### 4. Vacuum Old Files
# MAGIC
# MAGIC ```sql
# MAGIC -- Remove old file versions (after retention period)
# MAGIC VACUUM bronze.customers RETAIN 168 HOURS;  -- 7 days
# MAGIC VACUUM bronze.orders RETAIN 168 HOURS;
# MAGIC VACUUM bronze.products RETAIN 168 HOURS;
# MAGIC
# MAGIC -- Schedule monthly vacuum job
# MAGIC -- Balance: shorter retention = less storage cost, longer retention = more time-travel capability
# MAGIC ```
# MAGIC
# MAGIC ### MySQL Optimization
# MAGIC
# MAGIC ```sql
# MAGIC -- Ensure source tables have proper indexes
# MAGIC CREATE INDEX idx_customers_state ON customers(state);
# MAGIC CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);
# MAGIC CREATE INDEX idx_orders_status ON orders(order_status);
# MAGIC
# MAGIC -- Analyze tables for query optimizer
# MAGIC ANALYZE TABLE customers;
# MAGIC ANALYZE TABLE orders;
# MAGIC ANALYZE TABLE products;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Optimization
# MAGIC
# MAGIC ### 1. Right-Size Ingestion Gateway
# MAGIC
# MAGIC | Change Volume | Gateway VM Size | Monthly Cost Estimate |
# MAGIC |---------------|-----------------|----------------------|
# MAGIC | < 10K changes/hour | Small (2 cores, 8 GB) | $150-200 |
# MAGIC | 10K-100K changes/hour | Medium (4 cores, 16 GB) | $300-400 |
# MAGIC | > 100K changes/hour | Large (8 cores, 32 GB) | $600-800 |
# MAGIC
# MAGIC ### 2. Optimize Pipeline Schedule
# MAGIC
# MAGIC ```python
# MAGIC # Balance latency requirements vs. cost
# MAGIC
# MAGIC # High-frequency (expensive but low latency)
# MAGIC critical_tables = ["orders", "transactions"]
# MAGIC schedule = "*/1 * * * *"  # Every 1 minute
# MAGIC
# MAGIC # Standard frequency (balanced)
# MAGIC standard_tables = ["customers", "products"]
# MAGIC schedule = "*/15 * * * *"  # Every 15 minutes
# MAGIC
# MAGIC # Low-frequency (cost-optimized)
# MAGIC slow_changing_tables = ["dim_countries", "dim_categories"]
# MAGIC schedule = "0 */6 * * *"  # Every 6 hours
# MAGIC ```
# MAGIC
# MAGIC ### 3. Staging Volume Cleanup
# MAGIC
# MAGIC ```sql
# MAGIC -- Implement lifecycle policy to delete old staged files
# MAGIC -- After pipeline successfully applies changes, staged files can be removed
# MAGIC
# MAGIC -- Example: Delete files older than 7 days
# MAGIC REMOVE 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/'
# MAGIC WHERE file_modification_time < current_timestamp() - INTERVAL 7 DAYS;
# MAGIC ```
# MAGIC
# MAGIC ### 4. Cost Monitoring Query
# MAGIC
# MAGIC ```sql
# MAGIC -- Estimate monthly cost based on usage
# MAGIC SELECT 
# MAGIC     pipeline_name,
# MAGIC     COUNT(*) * 30 as monthly_runs,  -- Extrapolate from daily runs
# MAGIC     AVG(execution_time_minutes) as avg_runtime_min,
# MAGIC     COUNT(*) * 30 * AVG(execution_time_minutes) / 60 as total_compute_hours_monthly,
# MAGIC     COUNT(*) * 30 * AVG(execution_time_minutes) / 60 * 0.15 as estimated_cost_usd  -- $0.15/DBU-hour
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         pipeline_name,
# MAGIC         update_id,
# MAGIC         TIMESTAMPDIFF(MINUTE, MIN(timestamp), MAX(timestamp)) as execution_time_minutes
# MAGIC     FROM system.lakeflow.pipeline_events
# MAGIC     WHERE timestamp >= current_timestamp() - INTERVAL 1 DAY
# MAGIC     GROUP BY pipeline_name, update_id
# MAGIC )
# MAGIC GROUP BY pipeline_name;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security Best Practices
# MAGIC
# MAGIC ### 1. Credential Management
# MAGIC
# MAGIC ```python
# MAGIC # Store credentials in Databricks Secrets (never in code or notebooks)
# MAGIC
# MAGIC # Create secret scope (CLI or UI)
# MAGIC databricks secrets create-scope --scope mysql_credentials
# MAGIC
# MAGIC # Store MySQL password
# MAGIC databricks secrets put --scope mysql_credentials --key cdc_password
# MAGIC
# MAGIC # Reference in Unity Catalog connection
# MAGIC connection_properties = {
# MAGIC     "host": "mysql-prod.company.com",
# MAGIC     "port": "3306",
# MAGIC     "user": "cdc_user",
# MAGIC     "password": "{{secrets/mysql_credentials/cdc_password}}"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### 2. Principle of Least Privilege
# MAGIC
# MAGIC ```sql
# MAGIC -- MySQL CDC user should have ONLY required permissions
# MAGIC CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'SecurePassword123!';
# MAGIC
# MAGIC -- Replication privileges (required for binlog reading)
# MAGIC GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
# MAGIC
# MAGIC -- Read access ONLY to specific database
# MAGIC GRANT SELECT ON retail_db.* TO 'cdc_user'@'%';
# MAGIC
# MAGIC -- NO write permissions
# MAGIC -- NO admin privileges
# MAGIC -- NO access to other databases
# MAGIC
# MAGIC FLUSH PRIVILEGES;
# MAGIC ```
# MAGIC
# MAGIC ### 3. Unity Catalog Access Control
# MAGIC
# MAGIC ```sql
# MAGIC -- Restrict bronze table access to data engineers
# MAGIC GRANT SELECT, MODIFY ON SCHEMA bronze TO `data_engineers`;
# MAGIC
# MAGIC -- Analysts only access silver/gold layers
# MAGIC GRANT SELECT ON SCHEMA silver TO `analysts`;
# MAGIC GRANT SELECT ON SCHEMA gold TO `analysts`;
# MAGIC
# MAGIC -- Audit connection usage
# MAGIC DESCRIBE CONNECTION retail_mysql_connection;
# MAGIC ```
# MAGIC
# MAGIC ### 4. Network Security
# MAGIC
# MAGIC ```python
# MAGIC # Recommended network architecture
# MAGIC network_security = {
# MAGIC     "connectivity": "AWS PrivateLink or Azure Private Link",
# MAGIC     "encryption_in_transit": "TLS 1.2+",
# MAGIC     "firewall_rules": "Restrict to Databricks workspace CIDR",
# MAGIC     "mysql_ssl": "require_secure_transport = ON",
# MAGIC     "audit_logging": "Enable CloudTrail/Activity Logs"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disaster Recovery Planning
# MAGIC
# MAGIC ### Backup Strategies
# MAGIC
# MAGIC #### 1. MySQL Binary Log Backup
# MAGIC
# MAGIC ```bash
# MAGIC # Archive binary logs to durable storage before expiration
# MAGIC #!/bin/bash
# MAGIC
# MAGIC # Copy binary logs to S3/ADLS
# MAGIC mysqlbinlog --read-from-remote-server \
# MAGIC              --host=mysql-prod.company.com \
# MAGIC              --user=backup_user \
# MAGIC              mysql-bin.000042 | \
# MAGIC              aws s3 cp - s3://mysql-binlog-backup/2024-01-10/mysql-bin.000042
# MAGIC
# MAGIC # Schedule: Daily, retain for 30 days
# MAGIC ```
# MAGIC
# MAGIC #### 2. Delta Table Time Travel
# MAGIC
# MAGIC ```sql
# MAGIC -- Delta tables maintain version history automatically
# MAGIC -- Restore to previous version if needed
# MAGIC
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY bronze.customers;
# MAGIC
# MAGIC -- Query as of specific timestamp
# MAGIC SELECT * FROM bronze.customers
# MAGIC TIMESTAMP AS OF '2024-01-10 10:00:00';
# MAGIC
# MAGIC -- Restore table to previous version
# MAGIC RESTORE TABLE bronze.customers TO VERSION AS OF 42;
# MAGIC ```
# MAGIC
# MAGIC ### Recovery Scenarios
# MAGIC
# MAGIC #### Scenario 1: Binary Log Expiration
# MAGIC
# MAGIC ```python
# MAGIC recovery_steps = [
# MAGIC     "1. Pause incremental pipeline",
# MAGIC     "2. Run snapshot mode to reload affected tables",
# MAGIC     "3. Verify row counts match source",
# MAGIC     "4. Resume incremental mode from new binlog position",
# MAGIC     "5. Monitor for 24 hours to confirm stability"
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 2: MySQL Failover
# MAGIC
# MAGIC ```python
# MAGIC failover_steps = [
# MAGIC     "1. Identify new MySQL master hostname",
# MAGIC     "2. Update Unity Catalog connection with new host",
# MAGIC     "3. Verify GTID continuity (no gaps)",
# MAGIC     "4. Resume pipeline - gateway uses GTID to find position",
# MAGIC     "5. Validate: check for missing/duplicate records"
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 3: Accidental Table Drop on Bronze
# MAGIC
# MAGIC ```sql
# MAGIC -- Restore from Delta table history
# MAGIC RESTORE TABLE bronze.customers TO TIMESTAMP AS OF '2024-01-10 09:00:00';
# MAGIC
# MAGIC -- If beyond retention period, reload from MySQL
# MAGIC -- 1. Run snapshot mode
# MAGIC -- 2. Resume incremental from latest checkpoint
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Runbook Template
# MAGIC
# MAGIC ### Common Issues and Resolutions
# MAGIC
# MAGIC | Issue | Diagnosis | Resolution | Prevention |
# MAGIC |-------|-----------|------------|------------|
# MAGIC | Pipeline failure | Check `system.lakeflow.pipeline_events` for error_message | Review error, fix root cause, restart pipeline | Implement retry logic, pre-deployment validation |
# MAGIC | High ingestion lag | Query lag metric, check MySQL binlog position | Increase gateway VM size, optimize pipeline schedule | Monitor lag alerts, capacity planning |
# MAGIC | Gateway disconnected | Check network connectivity, MySQL availability | Restart gateway, verify credentials | Network redundancy, health checks |
# MAGIC | Binlog expired | Check `SHOW BINARY LOGS` on MySQL | Run snapshot mode to reset | Increase retention, monitor lag vs retention |
# MAGIC | Schema mismatch | Compare source schema to bronze table schema | Enable schema evolution or manually sync | Test schema changes in dev first |
# MAGIC | Orphaned records | Query for FK violations | Fix referential integrity, configure dependencies | Enforce FK constraints, dependency mapping |
# MAGIC
# MAGIC ### Emergency Contact List
# MAGIC
# MAGIC ```python
# MAGIC contacts = {
# MAGIC     "on_call_engineer": "team@company.com",
# MAGIC     "dba_team": "dba@company.com",
# MAGIC     "network_team": "network@company.com",
# MAGIC     "databricks_support": "support@databricks.com"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Production readiness requires comprehensive planning** across MySQL configuration, network security, monitoring, and documentation
# MAGIC
# MAGIC 2. **Binary log retention is critical** - set to at least 2x expected downtime plus buffer to prevent checkpoint expiration
# MAGIC
# MAGIC 3. **Implement layered monitoring** with critical alerts for failures and warning alerts for degradation trends
# MAGIC
# MAGIC 4. **Error handling should distinguish transient vs. permanent failures** with appropriate retry logic
# MAGIC
# MAGIC 5. **Optimize performance through Z-ordering, auto-compaction, and appropriate partitioning** of bronze tables
# MAGIC
# MAGIC 6. **Cost optimization requires balancing latency requirements against compute costs** through smart scheduling
# MAGIC
# MAGIC 7. **Security best practices include secrets management, least privilege, and private network connectivity**
# MAGIC
# MAGIC 8. **Disaster recovery planning should cover binlog expiration, MySQL failover, and accidental data loss scenarios**
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 8: Implementing Scheduled Pipelines and Error Handling** where you'll:
# MAGIC - Configure production-grade pipeline schedules
# MAGIC - Implement monitoring queries and dashboards
# MAGIC - Test error handling and recovery procedures
# MAGIC - Set up alerting rules for critical metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [MySQL High Availability Solutions](https://dev.mysql.com/doc/refman/8.0/en/replication.html)
# MAGIC - [Delta Lake Performance Tuning](https://docs.databricks.com/delta/optimizations/index.html)
# MAGIC - [Unity Catalog Security Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
# MAGIC - [Databricks Monitoring and Logging](https://docs.databricks.com/administration-guide/account-settings/audit-logs.html)
