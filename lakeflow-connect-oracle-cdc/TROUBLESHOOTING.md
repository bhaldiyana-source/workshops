# Oracle CDC Troubleshooting Guide

This guide provides solutions to common issues encountered when running Oracle CDC with Databricks Lakeflow Connect.

## Table of Contents
- [CDC Not Capturing Changes](#cdc-not-capturing-changes)
- [Ingestion Gateway Issues](#ingestion-gateway-issues)
- [Pipeline Failures](#pipeline-failures)
- [High CDC Latency](#high-cdc-latency)
- [Data Quality Issues](#data-quality-issues)
- [Oracle Configuration Problems](#oracle-configuration-problems)
- [Performance Degradation](#performance-degradation)
- [Network and Connectivity](#network-and-connectivity)

---

## CDC Not Capturing Changes

### Symptom
- No new data appearing in bronze tables
- `_commit_timestamp` not updating
- Freshness monitoring shows "STALE" status

### Diagnosis

**Step 1: Check Pipeline Status**
```sql
-- In Databricks SQL
SELECT * FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY event_time DESC LIMIT 5;
```

**Step 2: Check Gateway Status**
- Navigate to **Data Engineering** → **Lakeflow** → **Connections**
- Verify gateway status is "RUNNING"

**Step 3: Verify Oracle Transactions**
```sql
-- In Oracle
SELECT COUNT(*) FROM V$TRANSACTION;
-- If 0, no transactions are active
```

### Solutions

#### Solution 1: Gateway Not Running
```
Cause: Gateway stopped or failed
Fix:
1. Restart gateway via Databricks UI
2. Check gateway logs for errors
3. Verify Oracle connectivity
```

#### Solution 2: Oracle COMMIT Not Executed
```sql
-- All Oracle DML must be committed
UPDATE RETAIL.CUSTOMERS SET EMAIL='...' WHERE CUSTOMER_ID=1;
COMMIT;  -- Required!
```

#### Solution 3: Archive Logs Deleted
```sql
-- Check archive log availability
SELECT SEQUENCE#, FIRST_TIME, APPLIED, DELETED
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 1
ORDER BY SEQUENCE# DESC;

-- If DELETED='YES' for recent logs:
-- Increase archive log retention
-- Implement RMAN backup with deletion policy
```

#### Solution 4: Supplemental Logging Disabled
```sql
-- Verify supplemental logging
SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE;
-- Must return 'YES'

-- Re-enable if needed
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
```

---

## Ingestion Gateway Issues

### Issue: Connection Error

**Symptoms:**
- Gateway status shows "CONNECTION_ERROR"
- Unable to reach Oracle database

**Diagnosis:**
```python
# Test connectivity from Databricks
import socket

oracle_host = "oracle.example.com"
oracle_port = 1521

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex((oracle_host, oracle_port))
    if result == 0:
        print("✅ Port is OPEN")
    else:
        print("❌ Port is CLOSED")
    sock.close()
except Exception as e:
    print(f"❌ Error: {e}")
```

**Solutions:**

| Cause | Solution |
|-------|----------|
| **Firewall blocking** | Open port 1521 (or custom port) in security groups |
| **VPN disconnected** | Verify VPN connection is active |
| **Wrong hostname/port** | Check Unity Catalog connection configuration |
| **Oracle listener down** | Restart Oracle listener: `lsnrctl start` |
| **Network route missing** | Configure PrivateLink or VPN routing |

### Issue: Authentication Failed

**Symptoms:**
- Error: "ORA-01017: invalid username/password"
- Gateway cannot authenticate to Oracle

**Solutions:**
```sql
-- Verify CDC_USER exists and is unlocked
SELECT USERNAME, ACCOUNT_STATUS FROM DBA_USERS WHERE USERNAME = 'CDC_USER';
-- Must show: ACCOUNT_STATUS = 'OPEN'

-- Unlock if needed
ALTER USER CDC_USER ACCOUNT UNLOCK;

-- Reset password if forgotten
ALTER USER CDC_USER IDENTIFIED BY "NewPassword123!";
-- Then update Unity Catalog connection in Databricks
```

### Issue: Insufficient Privileges

**Symptoms:**
- Error: "ORA-01031: insufficient privileges"
- LogMiner operations fail

**Solutions:**
```sql
-- Re-grant required privileges
GRANT SELECT ANY TRANSACTION TO CDC_USER;
GRANT EXECUTE ON DBMS_LOGMNR TO CDC_USER;
GRANT EXECUTE ON DBMS_LOGMNR_D TO CDC_USER;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO CDC_USER;

-- Verify grants
SELECT PRIVILEGE FROM DBA_SYS_PRIVS WHERE GRANTEE = 'CDC_USER';
```

---

## Pipeline Failures

### Issue: Schema Mismatch Error

**Symptoms:**
- Error: "Schema mismatch between source and target"
- Pipeline fails after Oracle ALTER TABLE

**Diagnosis:**
```sql
-- Check Delta table schema
DESCRIBE retail_analytics.bronze.customers;

-- Check Oracle table schema
-- (Run in Oracle)
DESCRIBE RETAIL.CUSTOMERS;
```

**Solutions:**

**Option 1: Allow Schema Evolution**
```python
# Enable in pipeline configuration
{
    "configuration": {
        "spark.databricks.delta.schema.autoMerge.enabled": "true"
    }
}
```

**Option 2: Manual Schema Update**
```sql
-- Add missing column to Delta table
ALTER TABLE retail_analytics.bronze.customers 
ADD COLUMN new_column STRING;
```

**Option 3: Full Refresh**
```python
# Restart pipeline with full refresh
# This will re-snapshot all tables
# Use with caution - may take time for large tables
```

### Issue: Out of Memory Error

**Symptoms:**
- Error: "java.lang.OutOfMemoryError"
- Pipeline fails during large batch processing

**Solutions:**
1. **Increase cluster size:**
   - Navigate to pipeline settings
   - Increase worker count or VM size
   - Recommended: 2-4 workers for production

2. **Reduce batch size:**
   ```python
   # In pipeline configuration
   {
       "configuration": {
           "spark.sql.streaming.maxBatchSize": "1000000"  # Reduce from default
       }
   }
   ```

3. **Enable auto-scaling:**
   ```python
   {
       "clusters": [{
           "autoscale": {
               "min_workers": 2,
               "max_workers": 8
           }
       }]
   }
   ```

### Issue: Checkpoint Corruption

**Symptoms:**
- Error: "Unable to read checkpoint"
- Pipeline cannot resume from last position

**Solutions:**
```python
# Clear checkpoint and restart (will re-process from beginning)
# Navigate to: Pipeline Settings → Advanced → Clear Checkpoint
# OR run full refresh

# Note: This may cause duplicate processing temporarily
# Exactly-once semantics will resolve on completion
```

---

## High CDC Latency

### Issue: Changes Taking > 5 Minutes to Appear

**Diagnosis:**
```sql
-- Check current latency
SELECT 
    table_name,
    TIMESTAMPDIFF(SECOND, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) AS latency_seconds
FROM (
    SELECT 'customers' AS table_name, MAX(_commit_timestamp) AS max_commit
    FROM retail_analytics.bronze.customers
    UNION ALL
    SELECT 'orders', MAX(_commit_timestamp)
    FROM retail_analytics.bronze.orders
) t;
```

**Solutions:**

| Cause | Solution | Expected Improvement |
|-------|----------|----------------------|
| **Gateway undersized** | Upgrade gateway VM size | 30-50% faster |
| **Pipeline undersized** | Increase worker count | 40-60% faster |
| **High change volume** | Scale both gateway & pipeline | 50-70% faster |
| **Network latency** | Implement PrivateLink/VPN optimization | 10-20% faster |
| **LogMiner slow** | Add indexes on source tables | 20-30% faster |

**Optimization Steps:**
```python
# 1. Upgrade gateway (via UI)
# Small → Medium → Large

# 2. Increase pipeline workers
{
    "clusters": [{
        "num_workers": 4  # Increase from 2
    }]
}

# 3. Reduce polling interval (if using triggered mode)
{
    "trigger": {
        "schedule": {
            "quartz_cron_expression": "0 */5 * * * ?"  # Every 5 min instead of 15
        }
    }
}
```

---

## Data Quality Issues

### Issue: Orphaned Records

**Symptoms:**
- Orders exist without matching customers
- Foreign key relationships broken

**Diagnosis:**
```sql
-- Find orphaned orders
SELECT 
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.ORDER_DATE,
    'ORPHANED' AS status
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL
LIMIT 10;
```

**Solutions:**

**Temporary Orphans (Normal):**
- Wait 2-3 minutes - parent record may still be propagating
- Check if same transaction had multiple operations
- SCN ordering will resolve automatically

**Persistent Orphans (Issue):**
```sql
-- Check if parent was deleted in Oracle
-- (Run in Oracle)
SELECT COUNT(*) FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = <orphaned_customer_id>;

-- Options:
-- 1. If parent exists in Oracle: Wait for next CDC sync
-- 2. If parent deleted: This is expected (cascaded delete may be delayed)
-- 3. If data inconsistency: Full refresh required
```

### Issue: Duplicate Primary Keys

**Symptoms:**
- Same ID appears multiple times in bronze table

**Diagnosis:**
```sql
-- Find duplicates
SELECT 
    CUSTOMER_ID,
    COUNT(*) AS occurrence_count
FROM retail_analytics.bronze.customers
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1;
```

**Solutions:**
```sql
-- Run OPTIMIZE to compact files and resolve duplicates
OPTIMIZE retail_analytics.bronze.customers;

-- Enable merge on read
ALTER TABLE retail_analytics.bronze.customers 
SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- If persists: Check pipeline configuration for merge logic
```

---

## Oracle Configuration Problems

### Issue: Archive Log Space Full

**Symptoms:**
- Error: "ORA-00257: archiver error"
- Oracle database becomes unresponsive

**Diagnosis:**
```sql
-- Check space usage
SELECT 
    SPACE_LIMIT/1024/1024/1024 AS limit_gb,
    SPACE_USED/1024/1024/1024 AS used_gb,
    (SPACE_USED/SPACE_LIMIT)*100 AS used_percent
FROM V$RECOVERY_FILE_DEST;
```

**Solutions:**

**Immediate (Emergency):**
```sql
-- Delete old archive logs (CAUTION!)
-- This will prevent CDC from accessing deleted logs
-- Use only if database is stuck

-- Via RMAN:
RMAN> DELETE ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE-1';

-- Increase space limit
ALTER SYSTEM SET DB_RECOVERY_FILE_DEST_SIZE = 200G SCOPE=BOTH;
```

**Long-term (Recommended):**
```bash
# Implement RMAN backup strategy
rman target /
CONFIGURE ARCHIVELOG DELETION POLICY TO BACKED UP 1 TIMES TO DISK;
BACKUP ARCHIVELOG ALL DELETE INPUT;
```

### Issue: Redo Log Switches Too Frequent

**Symptoms:**
- > 24 log switches per hour
- Archive logs generating rapidly

**Diagnosis:**
```sql
-- Check log switch frequency
SELECT 
    TRUNC(FIRST_TIME) AS day,
    COUNT(*) AS log_switches,
    ROUND(COUNT(*) / 24, 2) AS switches_per_hour
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 1
GROUP BY TRUNC(FIRST_TIME);
```

**Solutions:**
```sql
-- Increase redo log size (requires downtime)
-- Current size
SELECT 
    GROUP#,
    BYTES/1024/1024 AS size_mb
FROM V$LOG;

-- Recommended: 512MB - 1GB per log file for CDC workloads

-- To change (execute when database is quiet):
ALTER DATABASE ADD LOGFILE GROUP 4 
    ('/path/redo04a.log', '/path/redo04b.log') SIZE 1024M;
-- Repeat for all groups, then drop old smaller groups
```

---

## Performance Degradation

### Issue: Slow Query Performance

**Symptoms:**
- Queries taking longer than before
- Dashboard refresh times increased

**Diagnosis:**
```sql
-- Check file count
DESCRIBE DETAIL retail_analytics.bronze.customers;
-- If num_files > 1000: Small files problem

-- Check for table scans
-- Review query plans for full table scans
```

**Solutions:**
```sql
-- 1. OPTIMIZE to compact files
OPTIMIZE retail_analytics.bronze.customers;
OPTIMIZE retail_analytics.bronze.orders;

-- 2. Add Z-ORDERING on frequently queried columns
OPTIMIZE retail_analytics.bronze.orders 
ZORDER BY (CUSTOMER_ID, ORDER_DATE);

-- 3. Enable Auto-Optimize
ALTER TABLE retail_analytics.bronze.customers 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 4. VACUUM old versions (frees space)
VACUUM retail_analytics.bronze.customers RETAIN 168 HOURS;
```

### Issue: Pipeline Processing Slower

**Symptoms:**
- Pipeline duration increasing over time
- Records per minute decreasing

**Diagnosis:**
```sql
-- Track processing time trend
SELECT 
    DATE(start_time) AS date,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) AS avg_duration_minutes
FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
    AND state = 'COMPLETED'
GROUP BY DATE(start_time)
ORDER BY date DESC
LIMIT 30;
```

**Solutions:**
1. **Scale up cluster** (more workers or larger VMs)
2. **Optimize source tables** in Oracle (add indexes)
3. **Partition large tables** in silver layer
4. **Review and optimize transformations** in pipeline

---

## Network and Connectivity

### Issue: Intermittent Connection Drops

**Symptoms:**
- Gateway sporadically shows "CONNECTION_ERROR"
- Pipeline has periodic failures

**Diagnosis:**
```bash
# Test connection stability
# Run from Databricks notebook:
import time
for i in range(10):
    # Test connection
    try:
        test_df = spark.read.jdbc(
            url=jdbc_url, table="(SELECT 1 FROM DUAL)", properties=connection_props
        )
        print(f"Attempt {i+1}: ✅ Success")
    except Exception as e:
        print(f"Attempt {i+1}: ❌ Failed - {str(e)}")
    time.sleep(60)
```

**Solutions:**
1. **Implement connection pooling:**
   ```python
   # Add to Unity Catalog connection properties
   {
       "maxPoolSize": "10",
       "connectionTimeout": "30000"
   }
   ```

2. **Use PrivateLink instead of public internet:**
   - Provides stable, low-latency connection
   - Reduces intermittent failures

3. **Configure connection retries:**
   ```python
   {
       "max_auto_retry_attempts": 5,
       "auto_retry_interval_seconds": 120
   }
   ```

### Issue: High Network Latency

**Symptoms:**
- Gateway to Oracle roundtrip > 100ms
- Data transfer rate is slow

**Diagnosis:**
```python
# Measure network latency
import time
import socket

def measure_latency(host, port, num_tests=10):
    latencies = []
    for _ in range(num_tests):
        start = time.time()
        sock = socket.socket()
        sock.settimeout(5)
        sock.connect((host, port))
        sock.close()
        latencies.append((time.time() - start) * 1000)
    
    print(f"Avg latency: {sum(latencies)/len(latencies):.2f}ms")
    print(f"Max latency: {max(latencies):.2f}ms")

measure_latency("oracle.example.com", 1521)
```

**Solutions:**
- **< 10ms:** Excellent, no action needed
- **10-50ms:** Good, acceptable for most workloads
- **50-100ms:** Consider optimization (PrivateLink, VPN upgrade)
- **> 100ms:** Requires attention (co-location, network path optimization)

---

## Emergency Procedures

### Critical: Database Unresponsive

1. **Check Oracle alert log:**
   ```bash
   tail -f $ORACLE_BASE/diag/rdbms/<sid>/<sid>/trace/alert_<sid>.log
   ```

2. **Stop Lakeflow Connect gateway:**
   - Temporarily stop gateway via Databricks UI
   - This stops LogMiner queries on Oracle

3. **Check Oracle sessions:**
   ```sql
   -- Kill stuck LogMiner sessions if needed
   SELECT sid, serial#, username, program, status
   FROM V$SESSION
   WHERE program LIKE '%LogMiner%' OR username = 'CDC_USER';
   
   -- If stuck:
   ALTER SYSTEM KILL SESSION 'sid,serial#';
   ```

4. **Restart gateway once Oracle is healthy**

### Data Inconsistency Detected

1. **Stop pipeline immediately**
2. **Compare Oracle vs Databricks row counts:**
   ```sql
   -- Oracle
   SELECT COUNT(*) FROM RETAIL.CUSTOMERS;
   
   -- Databricks
   SELECT COUNT(*) FROM retail_analytics.bronze.customers;
   ```

3. **If mismatch:**
   - Review recent CDC operations
   - Check for failed transactions
   - Consider full refresh if > 1% discrepancy

4. **Document and report to Databricks support**

---

## Escalation Matrix

| Severity | Response Time | Escalation Path |
|----------|---------------|-----------------|
| **P1 - Critical** (Production down) | 15 minutes | 1. On-call engineer<br>2. Data platform lead<br>3. Databricks support |
| **P2 - High** (Degraded service) | 1 hour | 1. Data engineering team<br>2. On-call engineer |
| **P3 - Medium** (Non-critical issue) | 4 hours | 1. Data engineering team |
| **P4 - Low** (Question/guidance) | 24 hours | 1. Data engineering team |

### Contact Information
- **Data Engineering Team:** data-eng@company.com
- **On-Call Engineer:** oncall@company.com (Slack: #data-oncall)
- **Oracle DBA Team:** oracle-dba@company.com
- **Databricks Support:** support.databricks.com

---

## Additional Resources

- [Oracle LogMiner Troubleshooting](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/using-logminer.html)
- [Databricks Delta Live Tables Debugging](https://docs.databricks.com/delta-live-tables/debugging.html)
- [Unity Catalog Troubleshooting](https://docs.databricks.com/data-governance/unity-catalog/troubleshoot.html)
- Workshop README: `README.md` in this directory
