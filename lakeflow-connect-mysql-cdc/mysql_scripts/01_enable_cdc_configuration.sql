-- ============================================================================
-- MySQL CDC Configuration Script
-- Purpose: Enable and verify binary logging configuration for CDC operations
-- Prerequisites: SUPER privilege or equivalent
-- Important: Requires MySQL restart after configuration changes
-- ============================================================================

-- ============================================================================
-- STEP 1: Verify Current Configuration
-- ============================================================================

-- Check if binary logging is enabled
SHOW VARIABLES LIKE 'log_bin';
-- Expected: ON (if enabled), OFF (if disabled - needs configuration)

-- Check binary log format
SHOW VARIABLES LIKE 'binlog_format';
-- Required for CDC: ROW
-- Not suitable: STATEMENT or MIXED

-- Check GTID status
SHOW VARIABLES LIKE 'gtid_mode';
-- Recommended: ON (for failover resilience)

-- Check GTID consistency enforcement
SHOW VARIABLES LIKE 'enforce_gtid_consistency';
-- Should be: ON (when GTID is enabled)

-- Check server ID
SHOW VARIABLES LIKE 'server_id';
-- Must be unique in replication topology (non-zero)

-- Check binary log retention
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
-- Recommended: 172800 (48 hours) to 259200 (72 hours)
-- If using older MySQL versions, check:
SHOW VARIABLES LIKE 'expire_logs_days';

-- Check binary log row image setting
SHOW VARIABLES LIKE 'binlog_row_image';
-- Recommended: MINIMAL (reduces log size by logging only changed columns)

-- Check binary log file size limit
SHOW VARIABLES LIKE 'max_binlog_size';
-- Default: 1073741824 (1GB) - usually appropriate

-- ============================================================================
-- STEP 2: Display Current Binary Logs
-- ============================================================================

-- List all binary log files (if binary logging is enabled)
SHOW BINARY LOGS;
-- Shows: Log_name, File_size, Encrypted

-- Check current binary log position
SHOW MASTER STATUS;
-- Shows: File, Position, Binlog_Do_DB, Binlog_Ignore_DB, Executed_Gtid_Set

-- ============================================================================
-- STEP 3: Configuration File Settings
-- ============================================================================

/*
If binary logging is NOT enabled or settings need adjustment, 
edit your MySQL configuration file:

**Linux**: /etc/my.cnf or /etc/mysql/my.cnf
**Windows**: C:\ProgramData\MySQL\MySQL Server 8.0\my.ini
**macOS**: /usr/local/mysql/my.cnf

Add or modify the [mysqld] section:

[mysqld]
# Binary logging for CDC
log_bin = mysql-bin
binlog_format = ROW                    # Required for CDC
binlog_row_image = MINIMAL             # Optimize log size (only changed columns)

# GTID configuration (recommended for failover)
gtid_mode = ON
enforce_gtid_consistency = ON

# Server identification
server_id = 1                          # Must be unique in replication topology

# Binary log retention
binlog_expire_logs_seconds = 259200   # 3 days (adjust based on your needs)
# For MySQL 5.7 and earlier, use:
# expire_logs_days = 3

# Binary log file size
max_binlog_size = 1073741824          # 1GB per file (default, usually fine)

# Performance tuning
sync_binlog = 1                        # Durability (flush to disk per transaction)
binlog_cache_size = 32768             # Per-connection cache (32KB default)
max_binlog_cache_size = 4294967296   # Max cache size (4GB)

# Transaction write set extraction (for Group Replication, optional)
transaction_write_set_extraction = XXHASH64

After editing the configuration file, restart MySQL:

**Linux (systemd)**:
  sudo systemctl restart mysql
  sudo systemctl status mysql

**Linux (SysV)**:
  sudo service mysql restart
  sudo service mysql status

**Windows (as Administrator)**:
  net stop MySQL80
  net start MySQL80

**AWS RDS MySQL**:
  - Modify parameter group to enable log_bin and set binlog_format = ROW
  - Reboot RDS instance from AWS Console

**Azure Database for MySQL**:
  - Update server parameters in Azure Portal
  - Set binlog_format = ROW
  - Automatic restart will be triggered
*/

-- ============================================================================
-- STEP 4: Verify Configuration After Restart
-- ============================================================================

-- Reconnect to MySQL after restart, then verify:

-- Verify binary logging is enabled
SHOW VARIABLES LIKE 'log_bin';
-- Should show: ON

-- Verify ROW format
SHOW VARIABLES LIKE 'binlog_format';
-- Should show: ROW

-- Verify GTID
SHOW VARIABLES LIKE 'gtid_mode';
-- Should show: ON (if configured)

-- Verify server_id is set
SHOW VARIABLES LIKE 'server_id';
-- Should show: non-zero value

-- Check that binary log files are being created
SHOW BINARY LOGS;
-- Should list at least one binary log file (e.g., mysql-bin.000001)

-- ============================================================================
-- STEP 5: Monitor Binary Log Disk Usage
-- ============================================================================

-- Check total binary log size
SELECT 
    CONCAT(ROUND(SUM(file_size)/1024/1024/1024, 2), ' GB') as total_binlog_size,
    COUNT(*) as log_file_count
FROM information_schema.BINARY_LOGS;

-- List individual binary log files with sizes
SELECT 
    log_name,
    CONCAT(ROUND(file_size/1024/1024, 2), ' MB') as size_mb,
    encrypted
FROM information_schema.BINARY_LOGS
ORDER BY log_name;

-- ============================================================================
-- STEP 6: Test Binary Log Capture
-- ============================================================================

-- Create a test database and table to verify binary logging
CREATE DATABASE IF NOT EXISTS cdc_test;
USE cdc_test;

CREATE TABLE test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    test_value VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test record
INSERT INTO test_table (test_value) VALUES ('CDC Test Record');

-- Check master status (position should have advanced)
SHOW MASTER STATUS;

-- View recent binary log events
-- Replace 'mysql-bin.000001' with your current binlog file from SHOW MASTER STATUS
SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 10;

-- Verify the INSERT was captured
SELECT * FROM test_table;

-- Clean up test database (optional)
-- DROP DATABASE cdc_test;

-- ============================================================================
-- STEP 7: Configuration Validation Summary
-- ============================================================================

-- Comprehensive configuration check
SELECT 
    'Binary Logging' as configuration_item,
    @@log_bin as current_value,
    '1 (ON)' as required_value,
    CASE WHEN @@log_bin = 1 THEN 'PASS' ELSE 'FAIL - Enable in config file' END as status
    
UNION ALL

SELECT 
    'Binary Log Format',
    @@binlog_format,
    'ROW',
    CASE WHEN @@binlog_format = 'ROW' THEN 'PASS' ELSE 'FAIL - Set binlog_format = ROW' END

UNION ALL

SELECT 
    'GTID Mode',
    @@gtid_mode,
    'ON',
    CASE WHEN @@gtid_mode = 'ON' THEN 'PASS' ELSE 'WARN - Recommended for failover' END

UNION ALL

SELECT 
    'Server ID',
    CAST(@@server_id AS CHAR),
    'Non-zero unique value',
    CASE WHEN @@server_id > 0 THEN 'PASS' ELSE 'FAIL - Set server_id' END

UNION ALL

SELECT 
    'Binary Log Retention (hours)',
    CAST(@@binlog_expire_logs_seconds / 3600 AS CHAR),
    '48-72 hours',
    CASE 
        WHEN @@binlog_expire_logs_seconds >= 172800 THEN 'PASS'
        ELSE 'WARN - Consider increasing retention'
    END;

-- ============================================================================
-- Notes and Best Practices
-- ============================================================================

/*
1. Binary Log Retention:
   - Set retention to at least 2x your maximum expected downtime
   - Monitor disk usage regularly
   - For production: 48-72 hours recommended

2. Binary Log Format:
   - ROW format is REQUIRED for CDC
   - STATEMENT and MIXED formats do not capture row-level changes
   - ROW format has larger log files but is necessary for change tracking

3. GTID Benefits:
   - Enables consistent replication across MySQL failovers
   - Simplifies checkpoint management in CDC pipelines
   - Highly recommended for production deployments

4. Performance Considerations:
   - Binary logging adds ~5-10% overhead to write operations
   - Use binlog_row_image = MINIMAL to reduce log size
   - Monitor binlog disk I/O and size growth

5. Security:
   - Protect binary log files (contain all data changes)
   - Encrypt binary logs if handling sensitive data
   - Rotate logs regularly based on retention policy

6. Monitoring:
   - Set up alerts for high binlog disk usage (>70%)
   - Monitor binlog generation rate
   - Track binlog age relative to CDC pipeline lag
*/

-- ============================================================================
-- Troubleshooting Common Issues
-- ============================================================================

/*
Issue: Binary logging won't enable
Solution: Check MySQL error log, ensure write permissions on log directory

Issue: GTID won't enable on existing server
Solution: May require special migration procedure, consult MySQL documentation

Issue: Binary logs consuming too much disk space
Solution: Reduce retention period, enable binlog_row_image = MINIMAL, purge old logs

Issue: Binary logs not appearing after config change
Solution: Verify MySQL restart completed successfully, check error log
*/
