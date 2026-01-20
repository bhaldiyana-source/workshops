# Lakeflow Connect MySQL CDC - Troubleshooting Guide

## Overview

This guide provides detailed troubleshooting steps for common issues encountered when implementing Change Data Capture (CDC) from MySQL to Databricks using Lakeflow Connect. Each issue includes symptoms, root causes, diagnostic steps, and resolution procedures.

---

## Table of Contents

1. [Binary Logging Issues](#binary-logging-issues)
2. [Network Connectivity Problems](#network-connectivity-problems)
3. [Ingestion Gateway Issues](#ingestion-gateway-issues)
4. [Pipeline Execution Failures](#pipeline-execution-failures)
5. [CDC Not Capturing Changes](#cdc-not-capturing-changes)
6. [Schema Evolution Issues](#schema-evolution-issues)
7. [High Ingestion Lag](#high-ingestion-lag)
8. [Referential Integrity Problems](#referential-integrity-problems)
9. [Binary Log Expiration](#binary-log-expiration)
10. [Performance Issues](#performance-issues)

---

## Binary Logging Issues

### Issue 1: Binary Logging Not Enabled

**Symptoms:**
- Cannot create CDC pipeline
- Error message: "Binary log is not enabled"
- Lakeflow Connect setup wizard reports missing binary log

**Root Causes:**
- MySQL `log_bin` parameter not set
- MySQL server not restarted after configuration change
- Using MySQL version that doesn't support binary logging

**Diagnostic Steps:**

```sql
-- Check if binary logging is enabled
SHOW VARIABLES LIKE 'log_bin';
-- Expected: ON

-- Check MySQL version
SELECT VERSION();
-- Required: MySQL 5.7+ or 8.0+

-- Check for binary log files
SHOW BINARY LOGS;
-- Should list at least one file
```

**Resolution:**

1. **Edit MySQL Configuration File:**
   - Linux: `/etc/my.cnf` or `/etc/mysql/my.cnf`
   - Windows: `C:\ProgramData\MySQL\MySQL Server 8.0\my.ini`
   - macOS: `/usr/local/mysql/my.cnf`

2. **Add Binary Logging Settings:**
   ```ini
   [mysqld]
   log_bin = mysql-bin
   binlog_format = ROW
   server_id = 1
   ```

3. **Restart MySQL Service:**
   ```bash
   # Linux (systemd)
   sudo systemctl restart mysql
   
   # Windows (as Administrator)
   net stop MySQL80 && net start MySQL80
   ```

4. **Verify Configuration:**
   ```sql
   SHOW VARIABLES LIKE 'log_bin';
   SHOW BINARY LOGS;
   ```

**AWS RDS MySQL:**
- Modify parameter group: `log_bin_trust_function_creators = 1`
- Reboot RDS instance from AWS Console
- Binary logging is enabled by default in RDS

**Azure Database for MySQL:**
- Navigate to Server Parameters in Azure Portal
- Enable `binlog_format = ROW`
- Automatic restart will be triggered

---

### Issue 2: Binary Log Format is Not ROW

**Symptoms:**
- CDC pipeline fails to capture changes
- Missing before/after values in change events
- Error: "Binary log format not suitable for CDC"

**Root Causes:**
- `binlog_format` set to STATEMENT or MIXED
- Configuration not applied after restart

**Diagnostic Steps:**

```sql
-- Check current binary log format
SHOW VARIABLES LIKE 'binlog_format';
-- Required: ROW

-- Check if dynamic change is allowed
SET GLOBAL binlog_format = 'ROW';
-- Note: Only works if no active connections are using binary log
```

**Resolution:**

1. **Update Configuration File:**
   ```ini
   [mysqld]
   binlog_format = ROW
   ```

2. **Restart MySQL:**
   ```bash
   sudo systemctl restart mysql
   ```

3. **Verify:**
   ```sql
   SHOW VARIABLES LIKE 'binlog_format';
   ```

4. **For Running Systems (Temporary):**
   ```sql
   -- Stop all replication slaves first
   SET GLOBAL binlog_format = 'ROW';
   ```

---

### Issue 3: GTID Not Enabled

**Symptoms:**
- Failover scenarios don't work correctly
- Checkpoints inconsistent after MySQL failover
- Warning during Lakeflow Connect setup

**Root Causes:**
- `gtid_mode` not enabled in MySQL configuration
- Incompatible GTID settings

**Diagnostic Steps:**

```sql
-- Check GTID status
SHOW VARIABLES LIKE 'gtid_mode';
-- Recommended: ON

SHOW VARIABLES LIKE 'enforce_gtid_consistency';
-- Should be: ON
```

**Resolution:**

1. **Enable GTID (MySQL 8.0+ with live traffic):**
   ```sql
   -- Step 1: Set GTID mode to OFF_PERMISSIVE
   SET GLOBAL enforce_gtid_consistency = WARN;
   -- Monitor logs for any violations
   
   SET GLOBAL enforce_gtid_consistency = ON;
   SET GLOBAL gtid_mode = OFF_PERMISSIVE;
   SET GLOBAL gtid_mode = ON_PERMISSIVE;
   SET GLOBAL gtid_mode = ON;
   ```

2. **Enable GTID (Fresh Installation or Maintenance Window):**
   ```ini
   [mysqld]
   gtid_mode = ON
   enforce_gtid_consistency = ON
   ```
   
   Then restart MySQL.

3. **Verify:**
   ```sql
   SHOW VARIABLES LIKE 'gtid_mode';
   SHOW MASTER STATUS;
   -- Check Executed_Gtid_Set is populated
   ```

---

## Network Connectivity Problems

### Issue 4: Ingestion Gateway Cannot Connect to MySQL

**Symptoms:**
- Gateway status shows "DISCONNECTED"
- Error: "Connection refused"
- Error: "Network unreachable"
- Timeout errors in gateway logs

**Root Causes:**
- Firewall blocking connections
- Security group rules not configured
- Incorrect hostname/IP address
- MySQL not listening on expected interface

**Diagnostic Steps:**

1. **Test Connectivity from Databricks:**
   ```python
   # In Databricks notebook
   import socket
   
   mysql_host = "your-mysql-host.com"
   mysql_port = 3306
   
   try:
       sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       sock.settimeout(5)
       result = sock.connect_ex((mysql_host, mysql_port))
       if result == 0:
           print("Port is open")
       else:
           print(f"Port is closed or unreachable (error {result})")
       sock.close()
   except Exception as e:
       print(f"Connection test failed: {e}")
   ```

2. **Check MySQL is Listening:**
   ```bash
   # On MySQL server
   netstat -tulpn | grep 3306
   # Should show MySQL listening on 0.0.0.0:3306 or specific IP
   ```

3. **Test MySQL Login:**
   ```bash
   # From Databricks or intermediate host
   mysql -h your-mysql-host.com -u cdc_user -p
   ```

**Resolution:**

1. **AWS Security Groups:**
   ```
   Inbound Rule:
   - Type: MySQL/Aurora
   - Protocol: TCP
   - Port: 3306
   - Source: Databricks workspace CIDR or security group
   ```

2. **Azure Network Security Group:**
   ```
   Inbound Security Rule:
   - Source: Databricks VNet or Service Tag
   - Destination Port: 3306
   - Protocol: TCP
   - Action: Allow
   ```

3. **On-Premise Firewall:**
   ```bash
   # Allow Databricks IP ranges
   # Example using iptables
   sudo iptables -A INPUT -p tcp --dport 3306 -s <databricks-cidr> -j ACCEPT
   ```

4. **MySQL Configuration (bind-address):**
   ```ini
   [mysqld]
   bind-address = 0.0.0.0  # Listen on all interfaces
   # Or specify specific IP
   # bind-address = 10.0.1.10
   ```

5. **VPC Peering / PrivateLink:**
   - Configure VPC peering between Databricks and MySQL VPC
   - Set up AWS PrivateLink or Azure Private Endpoint
   - Update route tables to allow traffic

---

### Issue 5: SSL/TLS Connection Errors

**Symptoms:**
- Error: "SSL connection error"
- Error: "Certificate verification failed"
- Connection works without SSL but fails with SSL

**Root Causes:**
- SSL not properly configured on MySQL
- Certificate issues
- Databricks cannot validate MySQL certificate

**Diagnostic Steps:**

```sql
-- Check SSL status on MySQL
SHOW VARIABLES LIKE '%ssl%';

-- Check if user requires SSL
SHOW GRANTS FOR 'cdc_user'@'%';
-- Look for REQUIRE SSL clause
```

**Resolution:**

1. **Disable SSL Requirement (Development Only):**
   ```sql
   ALTER USER 'cdc_user'@'%' REQUIRE NONE;
   FLUSH PRIVILEGES;
   ```

2. **Enable SSL on MySQL:**
   ```ini
   [mysqld]
   ssl-ca=/path/to/ca-cert.pem
   ssl-cert=/path/to/server-cert.pem
   ssl-key=/path/to/server-key.pem
   ```

3. **Configure Databricks Connection with SSL:**
   - Update Unity Catalog connection to include SSL parameters
   - Provide CA certificate if using custom CA

---

## Ingestion Gateway Issues

### Issue 6: Gateway Stuck in PROVISIONING Status

**Symptoms:**
- Gateway shows "PROVISIONING" for > 10 minutes
- Never transitions to "RUNNING"
- No error messages displayed

**Root Causes:**
- Insufficient workspace quota for VM resources
- Network configuration issues preventing VM startup
- Internal Databricks service issue

**Diagnostic Steps:**

1. **Check Workspace Quotas:**
   - Navigate to Databricks Admin Console
   - Review compute quotas and limits

2. **Review Gateway Logs:**
   - In Lakeflow Connect UI, view gateway logs
   - Look for initialization errors

**Resolution:**

1. **Increase Workspace Quota:**
   - Contact Databricks support to request quota increase
   - Or delete unused gateways/clusters

2. **Try Smaller VM Size:**
   - Delete failed gateway
   - Create new gateway with smaller VM size

3. **Check Network Configuration:**
   - Ensure VPC/VNet has available IP addresses
   - Verify subnet routing tables

4. **Contact Databricks Support:**
   - Provide workspace ID and gateway name
   - Include timestamp of provisioning attempt

---

### Issue 7: Gateway Repeatedly Disconnecting

**Symptoms:**
- Gateway alternates between "RUNNING" and "DISCONNECTED"
- Intermittent connection failures
- Changes not captured consistently

**Root Causes:**
- Network instability
- MySQL connection pool exhaustion
- MySQL server restarts or failovers

**Diagnostic Steps:**

```sql
-- Check MySQL process list for CDC user connections
SELECT * FROM information_schema.PROCESSLIST
WHERE USER = 'cdc_user';

-- Check MySQL uptime
SHOW STATUS LIKE 'Uptime';

-- Check for connection errors in MySQL error log
-- tail -f /var/log/mysql/error.log
```

**Resolution:**

1. **Increase MySQL max_connections:**
   ```sql
   SET GLOBAL max_connections = 500;
   ```
   
   Or in my.cnf:
   ```ini
   [mysqld]
   max_connections = 500
   ```

2. **Configure Connection Timeout:**
   ```sql
   SET GLOBAL wait_timeout = 28800;  # 8 hours
   SET GLOBAL interactive_timeout = 28800;
   ```

3. **Implement Network Redundancy:**
   - Use MySQL read replicas with load balancer
   - Configure automatic failover

4. **Monitor Network Latency:**
   - Check latency between Databricks and MySQL
   - Consider moving MySQL closer to Databricks region

---

## Pipeline Execution Failures

### Issue 8: Pipeline Fails with "Table Not Found" Error

**Symptoms:**
- Pipeline state: FAILED
- Error: "Table 'retail_db.customers' doesn't exist"
- Source tables exist in MySQL

**Root Causes:**
- CDC user lacks SELECT permission
- Table dropped after pipeline configuration
- Schema name mismatch

**Diagnostic Steps:**

```sql
-- Verify table exists
SHOW TABLES IN retail_db;

-- Check CDC user privileges
SHOW GRANTS FOR 'cdc_user'@'%';

-- Test SELECT as CDC user
-- Connect as CDC user, then:
SELECT COUNT(*) FROM retail_db.customers;
```

**Resolution:**

1. **Grant SELECT Privilege:**
   ```sql
   GRANT SELECT ON retail_db.* TO 'cdc_user'@'%';
   FLUSH PRIVILEGES;
   ```

2. **Verify Table Name:**
   - Check for case sensitivity issues
   - Ensure schema name matches

3. **Re-create Missing Tables:**
   - Restore from backup
   - Or remove table from pipeline configuration

---

### Issue 9: Schema Mismatch Errors

**Symptoms:**
- Error: "Schema mismatch between source and target"
- Pipeline fails after DDL changes on MySQL
- Columns missing or type mismatches

**Root Causes:**
- ALTER TABLE executed on MySQL without schema evolution enabled
- Column type changes incompatible with Delta Lake
- Column dropped or renamed

**Diagnostic Steps:**

```sql
-- Compare source and target schemas
-- MySQL:
DESCRIBE retail_db.customers;

-- Databricks:
DESCRIBE retail_analytics.bronze.customers;
```

**Resolution:**

1. **Enable Schema Evolution:**
   - In pipeline settings, enable "Schema Evolution"
   - Allows automatic schema updates

2. **Manual Schema Sync:**
   ```sql
   -- Databricks
   ALTER TABLE retail_analytics.bronze.customers
   ADD COLUMN new_column STRING;
   ```

3. **Re-snapshot Table:**
   - Switch pipeline to snapshot mode temporarily
   - Reload entire table with new schema
   - Switch back to incremental mode

4. **For Breaking Changes:**
   - Create new table version
   - Migrate data with transformation
   - Update downstream dependencies

---

### Issue 10: Checkpoint Corruption

**Symptoms:**
- Error: "Checkpoint file corrupted"
- Pipeline cannot resume from last position
- Repeated processing of same events

**Root Causes:**
- Storage issues with Unity Catalog Volume
- Concurrent writes to checkpoint
- Gateway crash during checkpoint write

**Diagnostic Steps:**

```sql
-- Check staging volume
LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/_checkpoint/';

-- Review pipeline logs for checkpoint errors
SELECT * FROM system.lakeflow.pipeline_events
WHERE error_message LIKE '%checkpoint%'
ORDER BY timestamp DESC;
```

**Resolution:**

1. **Clear Corrupted Checkpoint:**
   ```sql
   -- WARNING: This will re-process recent changes
   REMOVE 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/_checkpoint/customers_checkpoint.json';
   ```

2. **Run Snapshot Mode:**
   - Temporarily switch to snapshot to reset baseline
   - Then resume incremental mode

3. **Ensure Single Gateway:**
   - Verify only one gateway is processing each table
   - Multiple gateways cause checkpoint conflicts

---

## CDC Not Capturing Changes

### Issue 11: Changes Not Appearing in Bronze Tables

**Symptoms:**
- UPDATE/INSERT/DELETE in MySQL but not in Databricks
- Pipeline shows COMPLETED but no changes
- Ingestion lag increasing

**Root Causes:**
- Binary log format is STATEMENT instead of ROW
- Changes outside retention window
- Binary log position advanced past changes
- Pipeline not running on schedule

**Diagnostic Steps:**

```sql
-- Verify binlog format
SHOW VARIABLES LIKE 'binlog_format';

-- Check recent changes in MySQL
SELECT * FROM retail_db.customers
ORDER BY last_updated DESC
LIMIT 5;

-- Check recent changes in Databricks
SELECT * FROM retail_analytics.bronze.customers
ORDER BY _commit_timestamp DESC
LIMIT 5;

-- Check pipeline schedule
SELECT * FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY timestamp DESC
LIMIT 10;
```

**Resolution:**

1. **Verify Binary Log Format:**
   ```sql
   -- Must be ROW
   SET GLOBAL binlog_format = 'ROW';
   ```

2. **Check Pipeline Schedule:**
   - Ensure pipeline is scheduled (not manual only)
   - Verify schedule is active

3. **Manually Trigger Pipeline:**
   - In UI, click "Run Now"
   - Wait for completion

4. **Check Ingestion Lag View:**
   ```sql
   SELECT * FROM retail_analytics.bronze.vw_ingestion_lag;
   ```

---

## Binary Log Expiration

### Issue 12: Binary Log Position No Longer Available

**Symptoms:**
- Error: "Binary log 'mysql-bin.000042' not found"
- "Could not find first log file name in binary log index file"
- Pipeline cannot resume incremental mode

**Root Causes:**
- Binary log retention too short
- Pipeline downtime exceeded retention period
- Binary logs manually purged

**Diagnostic Steps:**

```sql
-- Check current binary logs
SHOW BINARY LOGS;

-- Check retention setting
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
-- Or for older versions:
SHOW VARIABLES LIKE 'expire_logs_days';

-- Check current binlog position needed
SHOW MASTER STATUS;
```

**Resolution:**

1. **Immediate Recovery - Run Snapshot Mode:**
   - Switch pipeline to snapshot mode
   - Reload all tables from current state
   - Resume incremental mode from new position

2. **Increase Retention Period:**
   ```sql
   -- Set to 3 days (259200 seconds)
   SET GLOBAL binlog_expire_logs_seconds = 259200;
   ```
   
   Or in my.cnf:
   ```ini
   [mysqld]
   binlog_expire_logs_seconds = 259200
   ```

3. **Monitor Binlog Lag:**
   - Set up alerts when lag > 50% of retention
   - Example: Alert if lag > 36 hours when retention is 72 hours

4. **Binary Log Backup (AWS RDS):**
   - Enable automated backups
   - Binary logs retained with automated backups

---

## Performance Issues

### Issue 13: Slow Pipeline Execution

**Symptoms:**
- Pipeline takes > 30 minutes to complete
- Execution time increasing over time
- High resource utilization on gateway

**Root Causes:**
- Large change volume
- Undersized gateway VM
- Network bandwidth limitations
- Inefficient indexing on MySQL

**Diagnostic Steps:**

```sql
-- Check recent execution durations
WITH execution_durations AS (
    SELECT 
        update_id,
        MIN(timestamp) as start_time,
        MAX(timestamp) as end_time,
        TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp)) as duration_seconds
    FROM system.lakeflow.pipeline_events
    WHERE pipeline_name = 'retail_ingestion_pipeline'
      AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY update_id
)
SELECT 
    start_time,
    duration_seconds,
    duration_seconds / 60.0 as duration_minutes
FROM execution_durations
ORDER BY start_time DESC
LIMIT 20;

-- Check change volume
SELECT 
    DATE_TRUNC('HOUR', _commit_timestamp) as hour,
    COUNT(*) as change_count
FROM retail_analytics.bronze.orders
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
ORDER BY hour DESC;
```

**Resolution:**

1. **Increase Gateway VM Size:**
   - Upgrade to larger VM (more CPU/memory)
   - Monitor resource utilization after upgrade

2. **Optimize MySQL Indexes:**
   ```sql
   -- Add indexes on columns used by CDC
   CREATE INDEX idx_last_updated ON customers(last_updated);
   CREATE INDEX idx_last_updated ON orders(last_updated);
   
   -- Analyze tables
   ANALYZE TABLE customers;
   ANALYZE TABLE orders;
   ```

3. **Batch Size Tuning:**
   - Adjust pipeline batch size in configuration
   - Balance between throughput and latency

4. **Optimize Binary Log Size:**
   ```sql
   SET GLOBAL binlog_row_image = 'MINIMAL';
   -- Only logs changed columns, not entire row
   ```

5. **Network Optimization:**
   - Use AWS PrivateLink or Azure Private Endpoint
   - Reduce inter-region data transfer
   - Increase network bandwidth

6. **Optimize Delta Tables:**
   ```sql
   -- Run Z-ordering
   OPTIMIZE retail_analytics.bronze.orders
   ZORDER BY (customer_id, order_date);
   
   -- Enable Auto Optimize
   ALTER TABLE retail_analytics.bronze.orders
   SET TBLPROPERTIES (
       'delta.autoOptimize.optimizeWrite' = 'true',
       'delta.autoOptimize.autoCompact' = 'true'
   );
   ```

---

## Contact and Escalation

### When to Escalate

Escalate to Databricks Support when:
- Issues persist after following troubleshooting steps
- Suspected platform bug or service outage
- Performance degradation without clear cause
- Data loss or corruption concerns

### Support Information

- **Databricks Support Portal**: https://help.databricks.com
- **Emergency Support**: Contact your Databricks account team
- **Community Forums**: https://community.databricks.com

### Information to Provide

When contacting support, include:
1. Workspace ID
2. Pipeline name and update_id
3. Timestamp of issue occurrence
4. Error messages from pipeline logs
5. MySQL version and configuration
6. Network architecture diagram
7. Steps already attempted

---

## Additional Resources

- [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [MySQL Binary Log Troubleshooting](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
- [Delta Lake Performance Tuning](https://docs.databricks.com/delta/optimizations/index.html)
