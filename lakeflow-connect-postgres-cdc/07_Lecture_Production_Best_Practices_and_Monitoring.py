# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Production Best Practices and Monitoring Strategies
# MAGIC
# MAGIC ## Module Overview
# MAGIC This lecture covers essential best practices for deploying and maintaining Lakeflow Connect CDC pipelines in production environments.
# MAGIC
# MAGIC **Duration:** 15 minutes
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Implement comprehensive monitoring and alerting strategies
# MAGIC - Design robust error handling and recovery procedures
# MAGIC - Optimize performance and cost efficiency
# MAGIC - Ensure data quality and governance
# MAGIC - Plan for disaster recovery and business continuity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Production Monitoring Strategy
# MAGIC
# MAGIC ### Critical Metrics to Monitor
# MAGIC
# MAGIC #### 1. Pipeline Health Metrics
# MAGIC
# MAGIC **Ingestion Gateway Status**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   gateway_name,
# MAGIC   status,  -- RUNNING, STOPPED, ERROR
# MAGIC   last_heartbeat,
# MAGIC   TIMESTAMPDIFF(MINUTE, last_heartbeat, CURRENT_TIMESTAMP) as minutes_since_heartbeat
# MAGIC FROM system.lakeflow.gateway_status
# MAGIC WHERE gateway_name = 'retail_ingestion_gateway';
# MAGIC
# MAGIC -- Alert if: status != 'RUNNING' OR minutes_since_heartbeat > 5
# MAGIC ```
# MAGIC
# MAGIC **Pipeline Execution Status**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   pipeline_name,
# MAGIC   execution_id,
# MAGIC   status,  -- RUNNING, SUCCEEDED, FAILED
# MAGIC   start_time,
# MAGIC   end_time,
# MAGIC   records_processed,
# MAGIC   error_message
# MAGIC FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_name = 'retail_ingestion_pipeline'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Alert if: Last 3 executions FAILED
# MAGIC ```
# MAGIC
# MAGIC #### 2. Data Latency Metrics
# MAGIC
# MAGIC **Replication Lag**
# MAGIC ```sql
# MAGIC -- In PostgreSQL: Check replication slot lag
# MAGIC SELECT 
# MAGIC   slot_name,
# MAGIC   active,
# MAGIC   pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size,
# MAGIC   restart_lsn,
# MAGIC   confirmed_flush_lsn
# MAGIC FROM pg_replication_slots
# MAGIC WHERE slot_name = 'lakeflow_slot';
# MAGIC
# MAGIC -- Alert if: lag_size > 1GB
# MAGIC ```
# MAGIC
# MAGIC **End-to-End Latency**
# MAGIC ```sql
# MAGIC -- In Databricks: Compare source last_updated with bronze _commit_timestamp
# MAGIC SELECT 
# MAGIC   'customers' as table_name,
# MAGIC   MAX(_commit_timestamp) as latest_bronze_commit,
# MAGIC   CURRENT_TIMESTAMP() as current_time,
# MAGIC   TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as lag_minutes
# MAGIC FROM bronze.customers;
# MAGIC
# MAGIC -- Alert if: lag_minutes > 30 (for 15-minute scheduled pipelines)
# MAGIC ```
# MAGIC
# MAGIC #### 3. Data Quality Metrics
# MAGIC
# MAGIC **Record Count Reconciliation**
# MAGIC ```sql
# MAGIC -- Compare source vs. target counts
# MAGIC WITH source_counts AS (
# MAGIC   -- Run in PostgreSQL
# MAGIC   SELECT COUNT(*) as pg_count FROM customers
# MAGIC ),
# MAGIC target_counts AS (
# MAGIC   SELECT COUNT(*) as delta_count 
# MAGIC   FROM bronze.customers 
# MAGIC   WHERE _is_deleted IS NULL OR _is_deleted = FALSE
# MAGIC )
# MAGIC SELECT 
# MAGIC   pg_count,
# MAGIC   delta_count,
# MAGIC   (delta_count - pg_count) as difference,
# MAGIC   ROUND((delta_count / pg_count * 100), 2) as match_percentage
# MAGIC FROM source_counts, target_counts;
# MAGIC
# MAGIC -- Alert if: ABS(difference) > 100 OR match_percentage < 99.5
# MAGIC ```
# MAGIC
# MAGIC **Data Freshness Check**
# MAGIC ```sql
# MAGIC -- Verify recent data is being captured
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   COUNT(*) as records_last_hour,
# MAGIC   MIN(_commit_timestamp) as oldest_commit,
# MAGIC   MAX(_commit_timestamp) as newest_commit
# MAGIC FROM (
# MAGIC   SELECT 'customers' as table_name, _commit_timestamp FROM bronze.customers
# MAGIC   WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC   UNION ALL
# MAGIC   SELECT 'orders', _commit_timestamp FROM bronze.orders
# MAGIC   WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC   UNION ALL
# MAGIC   SELECT 'products', _commit_timestamp FROM bronze.products
# MAGIC   WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC )
# MAGIC GROUP BY table_name;
# MAGIC
# MAGIC -- Alert if: records_last_hour = 0 for tables with expected activity
# MAGIC ```
# MAGIC
# MAGIC #### 4. Resource Utilization Metrics
# MAGIC
# MAGIC **PostgreSQL WAL Disk Usage**
# MAGIC ```sql
# MAGIC -- Monitor WAL directory size
# MAGIC SELECT 
# MAGIC   pg_size_pretty(SUM(size)) as total_wal_size
# MAGIC FROM pg_ls_waldir();
# MAGIC
# MAGIC -- Alert if: total_wal_size > 10GB (adjust based on your setup)
# MAGIC ```
# MAGIC
# MAGIC **Staging Volume Usage**
# MAGIC ```sql
# MAGIC -- Check Unity Catalog volume size
# MAGIC DESCRIBE VOLUME EXTENDED retail_analytics.landing.ingestion_volume;
# MAGIC
# MAGIC -- Alert if: size > expected threshold
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Alerting Strategy
# MAGIC
# MAGIC ### Alert Severity Levels
# MAGIC
# MAGIC #### Critical (P1) - Immediate Response Required
# MAGIC - Ingestion gateway down for > 5 minutes
# MAGIC - Pipeline failing for 3+ consecutive executions
# MAGIC - Replication slot invalidated (WAL files removed)
# MAGIC - Data count discrepancy > 1%
# MAGIC - End-to-end latency > 2 hours
# MAGIC
# MAGIC #### High (P2) - Response Within 1 Hour
# MAGIC - Replication lag > 1GB
# MAGIC - Pipeline success rate < 95% over 24 hours
# MAGIC - No data ingested in last hour (for active tables)
# MAGIC - WAL disk usage > 80%
# MAGIC
# MAGIC #### Medium (P3) - Response Within 4 Hours
# MAGIC - Pipeline duration increasing (trend analysis)
# MAGIC - Staging volume size growing unexpectedly
# MAGIC - Minor data quality issues detected
# MAGIC
# MAGIC #### Low (P4) - Informational
# MAGIC - Pipeline completed successfully
# MAGIC - Replication lag reduced after spike
# MAGIC - Maintenance window scheduled
# MAGIC
# MAGIC ### Sample Alert Configurations
# MAGIC
# MAGIC **Alert 1: Gateway Heartbeat Failure**
# MAGIC ```sql
# MAGIC -- Schedule: Every 5 minutes
# MAGIC -- Severity: P1
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN status != 'RUNNING' THEN 'Gateway not running'
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, last_heartbeat, CURRENT_TIMESTAMP()) > 5 
# MAGIC       THEN 'Gateway heartbeat lost'
# MAGIC     ELSE 'OK'
# MAGIC   END as alert_status,
# MAGIC   gateway_name,
# MAGIC   status,
# MAGIC   last_heartbeat
# MAGIC FROM system.lakeflow.gateway_status
# MAGIC WHERE gateway_name = 'retail_ingestion_gateway'
# MAGIC   AND (status != 'RUNNING' 
# MAGIC        OR TIMESTAMPDIFF(MINUTE, last_heartbeat, CURRENT_TIMESTAMP()) > 5);
# MAGIC ```
# MAGIC
# MAGIC **Alert 2: Replication Lag Threshold**
# MAGIC ```python
# MAGIC # Run in PostgreSQL monitoring script
# MAGIC import psycopg2
# MAGIC
# MAGIC def check_replication_lag():
# MAGIC     conn = psycopg2.connect(...)
# MAGIC     cursor = conn.cursor()
# MAGIC     
# MAGIC     cursor.execute("""
# MAGIC         SELECT 
# MAGIC             pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 as lag_mb
# MAGIC         FROM pg_replication_slots
# MAGIC         WHERE slot_name = 'lakeflow_slot'
# MAGIC     """)
# MAGIC     
# MAGIC     lag_mb = cursor.fetchone()[0]
# MAGIC     
# MAGIC     if lag_mb > 1024:  # 1GB
# MAGIC         send_alert(severity='P2', message=f'Replication lag: {lag_mb}MB')
# MAGIC     
# MAGIC     return lag_mb
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Error Handling and Recovery
# MAGIC
# MAGIC ### Common Error Scenarios and Solutions
# MAGIC
# MAGIC #### Scenario 1: Network Connectivity Loss
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Gateway shows connection errors
# MAGIC - Pipeline fails with "Cannot connect to source" error
# MAGIC
# MAGIC **Automatic Recovery:**
# MAGIC - Gateway retries connection with exponential backoff
# MAGIC - Maintains checkpoint, no data loss
# MAGIC - Resumes from last LSN when connectivity restored
# MAGIC
# MAGIC **Manual Intervention Required If:**
# MAGIC - Connectivity down > 4 hours
# MAGIC - WAL files rotated out during outage
# MAGIC
# MAGIC **Recovery Steps:**
# MAGIC ```sql
# MAGIC -- 1. Check replication slot status
# MAGIC SELECT * FROM pg_replication_slots WHERE slot_name = 'lakeflow_slot';
# MAGIC
# MAGIC -- 2. If restart_lsn is NULL or slot inactive:
# MAGIC --    Option A: Re-create slot (loses position, may need re-snapshot)
# MAGIC SELECT pg_drop_replication_slot('lakeflow_slot');
# MAGIC SELECT pg_create_logical_replication_slot('lakeflow_slot', 'pgoutput');
# MAGIC
# MAGIC --    Option B: Increase wal_keep_size to prevent future issues
# MAGIC ALTER SYSTEM SET wal_keep_size = '4GB';
# MAGIC SELECT pg_reload_conf();
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 2: Schema Evolution
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Pipeline fails with "Column mismatch" error
# MAGIC - New columns added to source table
# MAGIC
# MAGIC **Automatic Handling:**
# MAGIC - Lakeflow Connect supports additive schema changes
# MAGIC - New columns automatically added to target table
# MAGIC
# MAGIC **Manual Steps for Breaking Changes:**
# MAGIC ```sql
# MAGIC -- If column dropped or type changed:
# MAGIC
# MAGIC -- 1. Pause pipeline
# MAGIC ALTER PIPELINE retail_ingestion_pipeline PAUSE;
# MAGIC
# MAGIC -- 2. Update publication in PostgreSQL
# MAGIC DROP PUBLICATION dbz_publication;
# MAGIC CREATE PUBLICATION dbz_publication FOR TABLE customers, orders, products;
# MAGIC
# MAGIC -- 3. Optionally recreate target table
# MAGIC DROP TABLE bronze.customers;
# MAGIC
# MAGIC -- 4. Re-enable snapshot mode in pipeline
# MAGIC -- (via UI: Edit pipeline → Enable initial snapshot)
# MAGIC
# MAGIC -- 5. Resume pipeline
# MAGIC ALTER PIPELINE retail_ingestion_pipeline RESUME;
# MAGIC ```
# MAGIC
# MAGIC #### Scenario 3: Data Quality Failures
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - DLT expectations failing
# MAGIC - Invalid data detected in source
# MAGIC
# MAGIC **Implementation:**
# MAGIC ```python
# MAGIC # In DLT pipeline
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(
# MAGIC   name="bronze_customers",
# MAGIC   table_properties={
# MAGIC     "quality": "bronze"
# MAGIC   }
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
# MAGIC @dlt.expect_or_quarantine("valid_state", "state IN ('CA', 'NY', 'TX', ...)")
# MAGIC def customers_bronze():
# MAGIC     return spark.readStream.format("delta").table("staged_customers")
# MAGIC ```
# MAGIC
# MAGIC **Monitoring Quarantined Records:**
# MAGIC ```sql
# MAGIC SELECT * FROM bronze_customers_quarantine
# MAGIC WHERE _quarantine_timestamp >= CURRENT_DATE();
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Performance Optimization
# MAGIC
# MAGIC ### PostgreSQL-Side Optimizations
# MAGIC
# MAGIC #### 1. WAL Configuration Tuning
# MAGIC ```sql
# MAGIC -- Balance between disk usage and replication safety
# MAGIC ALTER SYSTEM SET wal_keep_size = '2GB';  -- PostgreSQL 13+
# MAGIC ALTER SYSTEM SET max_wal_size = '2GB';   -- Checkpoint tuning
# MAGIC ALTER SYSTEM SET wal_compression = 'on';  -- Reduce WAL size
# MAGIC
# MAGIC -- For high-volume systems
# MAGIC ALTER SYSTEM SET checkpoint_timeout = '15min';
# MAGIC ALTER SYSTEM SET checkpoint_completion_target = 0.9;
# MAGIC
# MAGIC SELECT pg_reload_conf();
# MAGIC ```
# MAGIC
# MAGIC #### 2. Replication Connection Optimization
# MAGIC ```sql
# MAGIC -- Increase connection limits for replication
# MAGIC ALTER SYSTEM SET max_wal_senders = 20;
# MAGIC ALTER SYSTEM SET max_replication_slots = 20;
# MAGIC
# MAGIC -- Adjust timeout for slow networks
# MAGIC ALTER SYSTEM SET wal_sender_timeout = '30s';
# MAGIC ```
# MAGIC
# MAGIC #### 3. Table-Level Optimizations
# MAGIC ```sql
# MAGIC -- Create indexes on frequently queried columns
# MAGIC CREATE INDEX idx_customers_email ON customers(email);
# MAGIC CREATE INDEX idx_orders_customer ON orders(customer_id);
# MAGIC
# MAGIC -- Update statistics for query planner
# MAGIC ANALYZE customers;
# MAGIC ANALYZE orders;
# MAGIC ANALYZE products;
# MAGIC
# MAGIC -- Consider partitioning very large tables
# MAGIC CREATE TABLE orders_partitioned (
# MAGIC     LIKE orders INCLUDING ALL
# MAGIC ) PARTITION BY RANGE (order_date);
# MAGIC ```
# MAGIC
# MAGIC ### Databricks-Side Optimizations
# MAGIC
# MAGIC #### 1. Delta Table Optimization
# MAGIC ```sql
# MAGIC -- Enable Auto Optimize for tables with frequent writes
# MAGIC ALTER TABLE bronze.customers SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Use Liquid Clustering for high-update tables (DBR 13.3+)
# MAGIC ALTER TABLE bronze.orders 
# MAGIC CLUSTER BY (customer_id, order_date);
# MAGIC
# MAGIC -- Traditional Z-ordering (if not using liquid clustering)
# MAGIC OPTIMIZE bronze.customers ZORDER BY (customer_id, state);
# MAGIC OPTIMIZE bronze.orders ZORDER BY (customer_id, order_date);
# MAGIC
# MAGIC -- Vacuum old versions (balance between time travel and storage cost)
# MAGIC VACUUM bronze.customers RETAIN 168 HOURS;  -- 7 days
# MAGIC ```
# MAGIC
# MAGIC #### 2. Pipeline Configuration
# MAGIC ```python
# MAGIC # DLT pipeline settings (cluster configuration)
# MAGIC {
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "num_workers": 2,  # Start small, scale based on load
# MAGIC       "node_type_id": "i3.xlarge",  # I/O optimized for CDC workloads
# MAGIC       "autoscale": {
# MAGIC         "min_workers": 2,
# MAGIC         "max_workers": 8  # Allow scaling for catch-up scenarios
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "configuration": {
# MAGIC     "pipelines.applyChangesPreviewEnabled": "true",
# MAGIC     "spark.databricks.delta.optimizeWrite.enabled": "true",
# MAGIC     "spark.databricks.delta.autoCompact.enabled": "true"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### 3. Scheduling Strategy
# MAGIC ```python
# MAGIC # High-frequency tables (order events, status changes)
# MAGIC schedule_high_freq = "*/5 * * * *"  # Every 5 minutes
# MAGIC
# MAGIC # Medium-frequency tables (customer updates, inventory)
# MAGIC schedule_medium_freq = "*/15 * * * *"  # Every 15 minutes
# MAGIC
# MAGIC # Low-frequency tables (reference data, product catalog)
# MAGIC schedule_low_freq = "0 * * * *"  # Hourly
# MAGIC
# MAGIC # Balance between:
# MAGIC # - Latency requirements
# MAGIC # - Pipeline execution cost
# MAGIC # - Resource contention
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Cost Optimization
# MAGIC
# MAGIC ### Cost Components
# MAGIC
# MAGIC #### 1. Compute Costs
# MAGIC - **Ingestion Gateway**: Dedicated VM (always running)
# MAGIC - **DLT Pipeline**: Cluster runs during execution
# MAGIC - **SQL Warehouse**: Used for monitoring queries
# MAGIC
# MAGIC **Optimization Strategies:**
# MAGIC ```python
# MAGIC # Right-size gateway based on change volume
# MAGIC gateway_size = {
# MAGIC   "light": "m5.large",      # < 1000 changes/minute
# MAGIC   "medium": "m5.xlarge",    # 1K-5K changes/minute
# MAGIC   "heavy": "m5.2xlarge"     # > 5K changes/minute
# MAGIC }
# MAGIC
# MAGIC # Use spot instances for non-critical pipelines
# MAGIC cluster_config = {
# MAGIC   "aws_attributes": {
# MAGIC     "availability": "SPOT_WITH_FALLBACK",
# MAGIC     "spot_bid_price_percent": 100
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC # Schedule pipelines to minimize runtime
# MAGIC # - Reduce frequency if latency requirements allow
# MAGIC # - Use continuous mode only for real-time requirements
# MAGIC ```
# MAGIC
# MAGIC #### 2. Storage Costs
# MAGIC - **Staging Volume**: Temporary storage for change files
# MAGIC - **Delta Tables**: Bronze layer storage
# MAGIC - **WAL Files**: PostgreSQL disk usage
# MAGIC
# MAGIC **Optimization Strategies:**
# MAGIC ```sql
# MAGIC -- Aggressive VACUUM for bronze tables
# MAGIC VACUUM bronze.customers RETAIN 24 HOURS;  -- If no time travel needed
# MAGIC
# MAGIC -- Compress older data
# MAGIC OPTIMIZE bronze.orders 
# MAGIC WHERE order_date < CURRENT_DATE() - INTERVAL 90 DAYS;
# MAGIC
# MAGIC -- Archive to cold storage
# MAGIC CREATE TABLE bronze.customers_archive
# MAGIC LOCATION 's3://archive-bucket/customers/'
# MAGIC AS SELECT * FROM bronze.customers 
# MAGIC WHERE last_updated < CURRENT_DATE() - INTERVAL 1 YEAR;
# MAGIC
# MAGIC DELETE FROM bronze.customers 
# MAGIC WHERE last_updated < CURRENT_DATE() - INTERVAL 1 YEAR;
# MAGIC ```
# MAGIC
# MAGIC #### 3. Data Transfer Costs
# MAGIC - **Cross-region transfer**: PostgreSQL to Databricks
# MAGIC - **Volume writes**: Gateway to staging
# MAGIC
# MAGIC **Optimization Strategies:**
# MAGIC - Deploy Databricks workspace in same region as PostgreSQL
# MAGIC - Use AWS PrivateLink or Azure Private Link to avoid egress charges
# MAGIC - Enable compression on replication stream if supported

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Disaster Recovery and Business Continuity
# MAGIC
# MAGIC ### Backup Strategy
# MAGIC
# MAGIC #### 1. PostgreSQL Backups
# MAGIC ```bash
# MAGIC # Regular PostgreSQL backups (base backups + WAL archiving)
# MAGIC pg_basebackup -D /backup/postgres -Ft -z -P
# MAGIC
# MAGIC # WAL archiving for point-in-time recovery
# MAGIC archive_command = 'cp %p /archive/wal/%f'
# MAGIC ```
# MAGIC
# MAGIC #### 2. Delta Table Backups
# MAGIC ```sql
# MAGIC -- Deep clone for backup (metadata + data)
# MAGIC CREATE TABLE bronze.customers_backup
# MAGIC DEEP CLONE bronze.customers
# MAGIC LOCATION 's3://backup-bucket/customers/';
# MAGIC
# MAGIC -- Shallow clone for testing (metadata only)
# MAGIC CREATE TABLE bronze.customers_test
# MAGIC SHALLOW CLONE bronze.customers;
# MAGIC ```
# MAGIC
# MAGIC #### 3. Configuration Backups
# MAGIC ```bash
# MAGIC # Export pipeline configuration
# MAGIC databricks pipelines get --pipeline-id <id> > pipeline_config.json
# MAGIC
# MAGIC # Export Unity Catalog connection
# MAGIC databricks connections get retail_postgres_conn > connection_config.json
# MAGIC
# MAGIC # Store in version control (Git)
# MAGIC git add pipeline_config.json connection_config.json
# MAGIC git commit -m "Backup CDC pipeline configuration"
# MAGIC ```
# MAGIC
# MAGIC ### Disaster Recovery Scenarios
# MAGIC
# MAGIC #### Scenario 1: PostgreSQL Database Failure
# MAGIC **Recovery Steps:**
# MAGIC 1. Restore PostgreSQL from backup
# MAGIC 2. Verify replication slot exists
# MAGIC 3. If slot lost, determine last checkpoint LSN from Databricks
# MAGIC 4. Re-create slot at checkpoint LSN or perform new snapshot
# MAGIC 5. Resume pipeline
# MAGIC
# MAGIC #### Scenario 2: Databricks Workspace Failure
# MAGIC **Recovery Steps:**
# MAGIC 1. Deploy new workspace or use DR workspace
# MAGIC 2. Restore Unity Catalog metastore
# MAGIC 3. Re-create ingestion gateway from config
# MAGIC 4. Re-create pipeline from config
# MAGIC 5. Pipeline resumes from last checkpoint (stored in Delta log)
# MAGIC
# MAGIC #### Scenario 3: Data Corruption
# MAGIC **Recovery Steps:**
# MAGIC ```sql
# MAGIC -- Use Delta Lake time travel
# MAGIC -- View history
# MAGIC DESCRIBE HISTORY bronze.customers;
# MAGIC
# MAGIC -- Restore to version before corruption
# MAGIC RESTORE TABLE bronze.customers TO VERSION AS OF 42;
# MAGIC
# MAGIC -- Or restore to specific timestamp
# MAGIC RESTORE TABLE bronze.customers TO TIMESTAMP AS OF '2024-01-15 10:00:00';
# MAGIC
# MAGIC -- Resume pipeline
# MAGIC ALTER PIPELINE retail_ingestion_pipeline RESUME;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Production Deployment Checklist
# MAGIC
# MAGIC ### Pre-Deployment
# MAGIC - [ ] Test full pipeline in dev/staging environment
# MAGIC - [ ] Validate record counts match source
# MAGIC - [ ] Measure baseline performance metrics
# MAGIC - [ ] Document configuration settings
# MAGIC - [ ] Create runbook for operations team
# MAGIC - [ ] Set up monitoring dashboards
# MAGIC - [ ] Configure alerting rules
# MAGIC - [ ] Establish on-call rotation
# MAGIC
# MAGIC ### PostgreSQL Production Readiness
# MAGIC - [ ] Logical replication enabled (`wal_level = logical`)
# MAGIC - [ ] Replication user created with minimal permissions
# MAGIC - [ ] WAL retention configured (`wal_keep_size`)
# MAGIC - [ ] Replication slots created
# MAGIC - [ ] Publications created for all tables
# MAGIC - [ ] Replica identity set to FULL on all tables
# MAGIC - [ ] Network connectivity verified (VPN/PrivateLink)
# MAGIC - [ ] SSL/TLS configured (if required)
# MAGIC - [ ] Backup and recovery tested
# MAGIC
# MAGIC ### Databricks Production Readiness
# MAGIC - [ ] Unity Catalog connection created and tested
# MAGIC - [ ] Staging volume created with appropriate permissions
# MAGIC - [ ] Ingestion gateway deployed and running
# MAGIC - [ ] Ingestion pipeline created and tested
# MAGIC - [ ] Bronze tables created in Unity Catalog
# MAGIC - [ ] Data quality expectations defined
# MAGIC - [ ] Pipeline scheduling configured
# MAGIC - [ ] Auto Optimize enabled on tables
# MAGIC - [ ] Tags applied for governance
# MAGIC - [ ] Access controls configured
# MAGIC
# MAGIC ### Monitoring and Alerting
# MAGIC - [ ] Gateway health monitoring enabled
# MAGIC - [ ] Pipeline execution monitoring enabled
# MAGIC - [ ] Replication lag alerts configured
# MAGIC - [ ] Data quality alerts configured
# MAGIC - [ ] Cost monitoring enabled
# MAGIC - [ ] Dashboards created for stakeholders
# MAGIC - [ ] Alert notification channels configured
# MAGIC - [ ] Escalation procedures documented
# MAGIC
# MAGIC ### Documentation
# MAGIC - [ ] Architecture diagram created
# MAGIC - [ ] Data flow documented
# MAGIC - [ ] Configuration settings documented
# MAGIC - [ ] Troubleshooting guide created
# MAGIC - [ ] Contact list maintained
# MAGIC - [ ] Change management process defined
# MAGIC - [ ] Disaster recovery plan documented
# MAGIC - [ ] SLA/SLO defined and communicated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Comprehensive monitoring** is essential for production CDC pipelines
# MAGIC 2. **Proactive alerting** prevents small issues from becoming outages
# MAGIC 3. **Error handling** should be automated with manual escalation paths
# MAGIC 4. **Performance optimization** is ongoing, not one-time
# MAGIC 5. **Cost management** requires balancing latency, frequency, and resources
# MAGIC 6. **Disaster recovery** planning is critical for business continuity
# MAGIC 7. **Documentation** enables operational excellence
# MAGIC
# MAGIC ### Critical Metrics Summary
# MAGIC
# MAGIC | Metric | Threshold | Action |
# MAGIC |--------|-----------|--------|
# MAGIC | Gateway heartbeat | > 5 min | P1 Alert |
# MAGIC | Replication lag | > 1GB | P2 Alert |
# MAGIC | Pipeline failures | 3 consecutive | P1 Alert |
# MAGIC | End-to-end latency | > 2x schedule | P2 Alert |
# MAGIC | Record count diff | > 1% | P1 Alert |
# MAGIC | WAL disk usage | > 80% | P2 Alert |
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Proceed to **Lab 8: Implementing Scheduled Pipelines and Error Handling** where you'll:
# MAGIC - Configure production-ready pipeline scheduling
# MAGIC - Implement monitoring queries
# MAGIC - Set up alerting (manual simulation)
# MAGIC - Test error recovery scenarios
# MAGIC - Create operational dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready to productionize your CDC pipeline!** 🎯
