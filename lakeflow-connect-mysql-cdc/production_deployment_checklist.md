# Production Deployment Checklist for MySQL CDC with Lakeflow Connect

## Overview

This checklist ensures your MySQL Change Data Capture (CDC) pipeline is production-ready before deployment. Review and complete all items systematically. Items marked with ⚠️ are critical for production operation.

**Document Version:** 1.0  
**Last Updated:** 2026-01-10  
**Workshop:** Lakeflow Connect MySQL CDC

---

## Pre-Deployment Phase

### 1. MySQL Configuration ⚠️

- [ ] **Binary Logging Enabled**
  ```sql
  SHOW VARIABLES LIKE 'log_bin';
  -- Must return: ON
  ```

- [ ] **Binary Log Format is ROW** ⚠️
  ```sql
  SHOW VARIABLES LIKE 'binlog_format';
  -- Must return: ROW
  ```

- [ ] **GTID Enabled (Recommended)**
  ```sql
  SHOW VARIABLES LIKE 'gtid_mode';
  -- Recommended: ON
  ```

- [ ] **Binary Log Retention Configured**
  ```sql
  SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
  -- Recommended: 172800 (48 hours) to 259200 (72 hours)
  -- Formula: 2x (max expected downtime + max pipeline duration + buffer)
  ```

- [ ] **Binary Log Disk Space Monitored**
  ```sql
  SELECT 
      CONCAT(ROUND(SUM(file_size)/1024/1024/1024, 2), ' GB') as total_binlog_size
  FROM information_schema.BINARY_LOGS;
  -- Ensure sufficient disk space (alert at 70% capacity)
  ```

- [ ] **Row Image Optimized (Optional)**
  ```sql
  SHOW VARIABLES LIKE 'binlog_row_image';
  -- Optional: MINIMAL (reduces binlog size)
  ```

- [ ] **Server ID Set and Unique**
  ```sql
  SHOW VARIABLES LIKE 'server_id';
  -- Must be non-zero and unique in replication topology
  ```

### 2. MySQL Security and Access Control ⚠️

- [ ] **CDC User Created with Minimal Privileges**
  ```sql
  SHOW GRANTS FOR 'cdc_user'@'%';
  -- Must have: REPLICATION SLAVE, REPLICATION CLIENT
  -- Must have: SELECT ON retail_db.*
  -- Must NOT have: INSERT, UPDATE, DELETE, SUPER
  ```

- [ ] **CDC User Host Restricted**
  - Development: `'cdc_user'@'%'` (acceptable)
  - **Production: `'cdc_user'@'<specific-IP-or-subnet>'`** ⚠️

- [ ] **Password Stored Securely**
  - Credentials stored in Databricks Secrets
  - Never hardcoded in notebooks or connection strings
  - Password rotation schedule documented

- [ ] **SSL/TLS Configured (if required)**
  ```sql
  SHOW VARIABLES LIKE '%ssl%';
  -- Verify SSL is enabled if security policy requires
  ```

### 3. Network Configuration ⚠️

- [ ] **Network Connectivity Established**
  - Type: ☐ VPN  ☐ PrivateLink  ☐ VPC Peering  ☐ Public Internet

- [ ] **Firewall Rules Configured**
  - MySQL port 3306 accessible from Databricks
  - Inbound rules allow Databricks workspace CIDR
  - Documented in network diagram

- [ ] **Databricks IP Ranges Whitelisted**
  - AWS: Security group rules configured
  - Azure: Network security group rules configured
  - On-premise: Firewall rules documented

- [ ] **Latency Acceptable**
  ```bash
  # Test latency from Databricks to MySQL
  ping your-mysql-host.com
  # Recommended: < 50ms for production
  ```

- [ ] **Bandwidth Adequate**
  - Estimated change volume: _____ GB/day
  - Network bandwidth: _____ Gbps
  - Peak hour capacity verified

### 4. MySQL Performance Baseline

- [ ] **Indexes Optimized**
  ```sql
  -- Verify indexes on source tables
  SHOW INDEX FROM customers;
  SHOW INDEX FROM orders;
  SHOW INDEX FROM products;
  ```

- [ ] **Table Statistics Current**
  ```sql
  ANALYZE TABLE customers;
  ANALYZE TABLE orders;
  ANALYZE TABLE products;
  ```

- [ ] **Baseline Performance Metrics Recorded**
  - Current QPS (Queries Per Second): _____
  - Average query latency: _____
  - Binary log generation rate: _____ MB/hour

- [ ] **MySQL Resource Utilization Monitored**
  - CPU usage: _____ % average
  - Memory usage: _____ % average
  - Disk I/O: _____ IOPS

---

## Databricks Configuration

### 5. Unity Catalog Setup ⚠️

- [ ] **Catalog Created**
  ```sql
  CREATE CATALOG IF NOT EXISTS retail_analytics;
  ```

- [ ] **Landing Schema Created**
  ```sql
  CREATE SCHEMA IF NOT EXISTS retail_analytics.landing
    COMMENT 'Staging area for CDC volumes';
  ```

- [ ] **Bronze Schema Created**
  ```sql
  CREATE SCHEMA IF NOT EXISTS retail_analytics.bronze
    COMMENT 'Bronze layer - raw data from MySQL';
  ```

- [ ] **Staging Volume Created**
  ```sql
  CREATE VOLUME IF NOT EXISTS retail_analytics.landing.ingestion_volume;
  ```

- [ ] **Volume Quota Configured**
  - Estimated volume size: _____ GB
  - Quota set with buffer: _____ GB

### 6. Unity Catalog Security ⚠️

- [ ] **Access Control Configured**
  ```sql
  -- Bronze tables: Data Engineers only
  GRANT SELECT, MODIFY ON SCHEMA bronze TO `data_engineers`;
  
  -- Silver/Gold: Analysts can read
  GRANT SELECT ON SCHEMA silver TO `analysts`;
  ```

- [ ] **Connection Permissions Restricted**
  - Only authorized users can modify connection
  - Credentials access audited

- [ ] **Audit Logging Enabled**
  - Unity Catalog audit logs active
  - Log retention policy configured

### 7. Databricks Secrets Management ⚠️

- [ ] **Secret Scope Created**
  ```bash
  databricks secrets create-scope --scope mysql_credentials
  ```

- [ ] **MySQL Password Stored**
  ```bash
  databricks secrets put --scope mysql_credentials --key cdc_password
  ```

- [ ] **Secret Access Restricted**
  - Only pipeline and authorized users have access
  - Access control list documented

---

## Lakeflow Connect Pipeline Configuration

### 8. Ingestion Gateway Setup ⚠️

- [ ] **Gateway Name Documented**
  - Gateway name: `retail_ingestion_gateway`
  - Environment: ☐ Development  ☐ Staging  ☐ Production

- [ ] **VM Size Appropriate**
  - Change volume: _____ changes/hour
  - VM size: ☐ Small (< 10K changes/hour)  ☐ Medium (10K-100K)  ☐ Large (> 100K)

- [ ] **Gateway Status: RUNNING** ⚠️
  - Verify in Lakeflow Connect UI
  - Connection to MySQL successful

- [ ] **Staging Location Configured**
  - Volume: `retail_analytics.landing.ingestion_volume`
  - Sufficient storage allocated

### 9. Unity Catalog Connection ⚠️

- [ ] **Connection Created**
  ```sql
  DESCRIBE CONNECTION retail_mysql_connection;
  ```

- [ ] **Connection Details Verified**
  - Host: `_____`
  - Port: `3306`
  - Database: `retail_db`
  - User: `cdc_user`
  - Password: `{{secrets/mysql_credentials/cdc_password}}`

- [ ] **Connection Test Successful**
  ```sql
  SHOW TABLES IN retail_db USING CONNECTION retail_mysql_connection;
  ```

### 10. Ingestion Pipeline Configuration ⚠️

- [ ] **Pipeline Name Documented**
  - Pipeline name: `retail_ingestion_pipeline`

- [ ] **Tables Configured**
  - ☐ customers (Primary Key: customer_id)
  - ☐ orders (Primary Key: order_id, Depends on: customers)
  - ☐ products (Primary Key: product_id)

- [ ] **Destination Schema Set**
  - Destination: `retail_analytics.bronze`

- [ ] **Initial Snapshot Completed** ⚠️
  ```sql
  SELECT 
      'customers' as table_name, COUNT(*) as record_count 
  FROM retail_analytics.bronze.customers
  UNION ALL
  SELECT 'orders', COUNT(*) FROM retail_analytics.bronze.orders
  UNION ALL
  SELECT 'products', COUNT(*) FROM retail_analytics.bronze.products;
  -- Verify counts match MySQL source
  ```

- [ ] **Incremental Mode Tested**
  - INSERT operations captured: ☐ Yes
  - UPDATE operations captured: ☐ Yes
  - DELETE operations captured: ☐ Yes

### 11. Pipeline Scheduling ⚠️

- [ ] **Schedule Configured**
  - Schedule type: ☐ Scheduled  ☐ Manual
  - Frequency: Every _____ minutes
  - Cron expression: `_____`

- [ ] **Schedule Aligned with Requirements**
  - Business SLA: Data freshness within _____ minutes
  - Pipeline frequency: Every _____ minutes
  - Buffer: _____ minutes

- [ ] **Schedule Active**
  - Pipeline running automatically
  - Last successful run: _____

---

## Data Quality and Validation

### 12. Data Quality Checks ⚠️

- [ ] **Referential Integrity Verified**
  ```sql
  -- No orphaned orders
  SELECT COUNT(*) FROM retail_analytics.bronze.orders o
  LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
  WHERE c.customer_id IS NULL;
  -- Must return: 0
  ```

- [ ] **NULL Value Checks Passing**
  ```sql
  -- Critical fields should not be NULL
  SELECT 
      SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
      SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails
  FROM retail_analytics.bronze.customers;
  -- Must return: 0, 0
  ```

- [ ] **Duplicate Checks Passing**
  ```sql
  -- No duplicate customer emails
  SELECT email, COUNT(*) FROM retail_analytics.bronze.customers
  GROUP BY email HAVING COUNT(*) > 1;
  -- Must return: empty
  ```

- [ ] **CDC Metadata Populated**
  ```sql
  -- _commit_timestamp must be present
  SELECT COUNT(*) FROM retail_analytics.bronze.customers
  WHERE _commit_timestamp IS NULL;
  -- Must return: 0
  ```

### 13. Data Validation Views Created

- [ ] **Data Quality View Created**
  ```sql
  SELECT * FROM retail_analytics.bronze.vw_data_quality_checks;
  -- All checks should show PASS or INFO
  ```

- [ ] **Validation Summary View Created**
  ```sql
  SELECT * FROM retail_analytics.bronze.vw_validation_summary;
  ```

---

## Monitoring and Observability

### 14. Pipeline Health Monitoring ⚠️

- [ ] **Pipeline Health View Created**
  ```sql
  SELECT * FROM retail_analytics.bronze.vw_pipeline_health
  ORDER BY execution_time DESC
  LIMIT 10;
  ```

- [ ] **Success Rate >= 99%** ⚠️
  ```sql
  -- Check last 7 days
  SELECT 
      ROUND(100.0 * SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
  FROM system.lakeflow.pipeline_events
  WHERE pipeline_name = 'retail_ingestion_pipeline'
    AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS;
  -- Must be >= 99.0
  ```

- [ ] **Ingestion Lag View Created**
  ```sql
  SELECT * FROM retail_analytics.bronze.vw_ingestion_lag;
  ```

- [ ] **Lag Within Acceptable Limits** ⚠️
  - All tables: Lag < _____ minutes (per SLA)
  - Alert threshold: _____ minutes

### 15. Alerting Configuration ⚠️

- [ ] **Alert View Created**
  ```sql
  SELECT * FROM retail_analytics.bronze.vw_cdc_alerts
  WHERE alert_level IN ('CRITICAL', 'WARNING');
  ```

- [ ] **Critical Alerts Configured**
  - ☐ Pipeline failure (immediate notification)
  - ☐ Gateway disconnected (> 5 minutes)
  - ☐ Ingestion lag > 60 minutes
  - ☐ Success rate < 95%

- [ ] **Warning Alerts Configured**
  - ☐ Execution time > 2x baseline
  - ☐ Ingestion lag 30-60 minutes
  - ☐ Data quality issues detected
  - ☐ Binary log disk usage > 70%

- [ ] **Alert Destinations Configured**
  - Email: `_____`
  - Slack channel: `_____`
  - PagerDuty (if used): `_____`

### 16. Dashboard and Reporting

- [ ] **Monitoring Dashboard Created**
  - Dashboard location: `_____`
  - Includes: Pipeline health, lag metrics, data quality

- [ ] **Key Metrics Tracked**
  - ☐ Pipeline success rate (last 24h, 7d, 30d)
  - ☐ Ingestion lag by table
  - ☐ Change volume trends
  - ☐ Execution duration trends
  - ☐ Data quality score

- [ ] **Daily Report Scheduled**
  - Report frequency: Daily at _____ AM/PM
  - Recipients: `_____`

---

## Performance Optimization

### 17. Delta Table Optimization

- [ ] **Auto Optimize Enabled**
  ```sql
  ALTER TABLE retail_analytics.bronze.customers
  SET TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
  );
  -- Repeat for orders and products
  ```

- [ ] **Z-Ordering Configured**
  ```sql
  OPTIMIZE retail_analytics.bronze.customers ZORDER BY (customer_id, state);
  OPTIMIZE retail_analytics.bronze.orders ZORDER BY (customer_id, order_date);
  OPTIMIZE retail_analytics.bronze.products ZORDER BY (category, product_id);
  ```

- [ ] **Vacuum Schedule Configured**
  - Frequency: Weekly
  - Retention: 168 hours (7 days)

### 18. Performance Baseline

- [ ] **Pipeline Execution Baseline Recorded**
  - Average duration: _____ minutes
  - 95th percentile: _____ minutes
  - Peak change volume: _____ changes/hour

- [ ] **Query Performance Tested**
  ```sql
  -- Example query performance test
  SELECT COUNT(*) FROM retail_analytics.bronze.orders
  WHERE order_date >= CURRENT_DATE() - INTERVAL 30 DAY;
  -- Execution time: _____ seconds
  ```

---

## Documentation and Runbooks

### 19. Documentation Complete ⚠️

- [ ] **Architecture Diagram Created**
  - Shows: MySQL → Gateway → Volume → Pipeline → Bronze
  - Includes: Network topology, security boundaries

- [ ] **Runbook Documented**
  - Common issues and resolutions
  - Emergency contacts
  - Escalation procedures
  - Recovery procedures

- [ ] **Connection Details Documented**
  - MySQL host, port, database
  - Databricks workspace URL
  - Unity Catalog paths
  - Secret scope names

- [ ] **SLA Targets Documented** ⚠️
  - Data freshness SLA: Within _____ minutes
  - Pipeline success rate: >= _____  %
  - Recovery Time Objective (RTO): _____ minutes
  - Recovery Point Objective (RPO): _____ minutes

### 20. Operational Procedures

- [ ] **Pipeline Restart Procedure Documented**
  - Steps to restart failed pipeline
  - Checkpoint recovery process

- [ ] **Binlog Expiration Recovery Procedure**
  - Steps to run snapshot mode
  - Data validation checklist

- [ ] **Failover Procedure (MySQL)**
  - Steps to update connection to new master
  - GTID verification process

- [ ] **Credential Rotation Procedure**
  - Schedule: Every _____ days
  - Steps documented
  - Testing procedure included

---

## Disaster Recovery and Business Continuity

### 21. Backup Strategy

- [ ] **MySQL Binary Log Backup**
  - Backup frequency: Daily
  - Retention period: _____ days
  - Storage location: `_____`

- [ ] **Delta Table Backup**
  - Time travel retention: _____ days
  - Backup location (if applicable): `_____`

- [ ] **Configuration Backup**
  - Pipeline configuration exported
  - Stored in version control: ☐ Yes  ☐ No

### 22. Disaster Recovery Testing

- [ ] **DR Plan Documented**
  - RTO: _____ hours
  - RPO: _____ hours

- [ ] **Recovery Procedures Tested**
  - ☐ Gateway failure recovery
  - ☐ Pipeline failure recovery
  - ☐ MySQL failover
  - ☐ Binary log expiration recovery
  - ☐ Full restore from backup

- [ ] **DR Test Schedule**
  - Frequency: Quarterly
  - Last test date: _____
  - Next test date: _____

---

## Security and Compliance

### 23. Security Review ⚠️

- [ ] **Access Control Reviewed**
  - Unity Catalog permissions appropriate
  - CDC user has minimal privileges
  - Secrets properly secured

- [ ] **Network Security Validated**
  - Private connectivity preferred
  - Public internet only if necessary and secured
  - SSL/TLS enforced (if required by policy)

- [ ] **Data Classification Reviewed**
  - PII handling documented
  - Compliance requirements met (GDPR, HIPAA, etc.)

- [ ] **Audit Logging Enabled**
  - Unity Catalog audit logs active
  - MySQL audit logs enabled (if required)
  - Log retention meets compliance requirements

### 24. Compliance Validation

- [ ] **Data Retention Policy Documented**
  - Bronze layer retention: _____ days
  - Archive policy: `_____`

- [ ] **Data Privacy Controls Implemented**
  - ☐ PII identified and documented
  - ☐ Encryption at rest (Delta Lake)
  - ☐ Encryption in transit (SSL/TLS)
  - ☐ Access logs reviewed regularly

- [ ] **Regulatory Requirements Met**
  - ☐ GDPR (if applicable)
  - ☐ HIPAA (if applicable)
  - ☐ SOC 2 (if applicable)
  - ☐ Other: `_____`

---

## Final Pre-Production Steps

### 25. Pre-Production Validation ⚠️

- [ ] **Full End-to-End Test Completed**
  - INSERT → Pipeline → Validate
  - UPDATE → Pipeline → Validate
  - DELETE → Pipeline → Validate

- [ ] **Load Testing Performed**
  - Peak change volume tested: _____ changes/minute
  - Pipeline handled load successfully: ☐ Yes

- [ ] **Failure Scenarios Tested**
  - Pipeline failure and recovery
  - Gateway restart
  - Network interruption

- [ ] **Monitoring Validated**
  - All alerts triggering correctly
  - Dashboard showing accurate metrics

### 26. Go-Live Approval ⚠️

- [ ] **Technical Review Complete**
  - Reviewer: `_____`
  - Date: `_____`
  - Approved: ☐ Yes  ☐ No

- [ ] **Security Review Complete**
  - Reviewer: `_____`
  - Date: `_____`
  - Approved: ☐ Yes  ☐ No

- [ ] **Business Stakeholder Approval**
  - Stakeholder: `_____`
  - Date: `_____`
  - Approved: ☐ Yes  ☐ No

- [ ] **Production Cutover Plan Documented**
  - Cutover date: `_____`
  - Cutover time: `_____`
  - Rollback criteria: `_____`

---

## Post-Deployment

### 27. Post-Deployment Monitoring (First 48 Hours) ⚠️

- [ ] **Hour 1: Pipeline Running Successfully**
  - Pipeline status: RUNNING
  - No critical alerts

- [ ] **Hour 6: Data Quality Validated**
  - All quality checks passing
  - Referential integrity maintained

- [ ] **Hour 24: Performance Within Baseline**
  - Execution times normal
  - Lag within SLA

- [ ] **Hour 48: Full Operations Validated**
  - All scheduled runs successful
  - Monitoring and alerts working
  - Team confident in production operations

### 28. Documentation Update

- [ ] **Production Configuration Documented**
  - Final pipeline settings
  - Actual performance metrics
  - Lessons learned

- [ ] **Knowledge Transfer Complete**
  - Operations team trained
  - Runbook reviewed with team
  - 24/7 support coverage arranged

---

## Sign-Off

### Deployment Team

| Role | Name | Signature | Date |
|------|------|-----------|------|
| **Data Engineer** | __________ | __________ | __________ |
| **DBA / MySQL Admin** | __________ | __________ | __________ |
| **DevOps / Infrastructure** | __________ | __________ | __________ |
| **Security Engineer** | __________ | __________ | __________ |
| **Business Stakeholder** | __________ | __________ | __________ |

### Production Go-Live

**Date:** __________  
**Time:** __________  
**Status:** ☐ Successful  ☐ Rollback Required

**Notes:**
___________________________________________________________________________
___________________________________________________________________________
___________________________________________________________________________

---

## Additional Resources

- [Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [MySQL Binary Log Best Practices](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
- [Unity Catalog Security Guide](https://docs.databricks.com/data-governance/unity-catalog/)
- [Delta Lake Performance Tuning](https://docs.databricks.com/delta/optimizations/)
- Workshop README: `lakeflow-connect-mysql-cdc/README.md`
- Troubleshooting Guide: `lakeflow-connect-mysql-cdc/troubleshooting_guide.md`
