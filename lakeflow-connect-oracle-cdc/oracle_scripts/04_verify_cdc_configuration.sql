-- ===================================================================
-- Script: 04_verify_cdc_configuration.sql
-- Purpose: Comprehensive validation of Oracle CDC configuration
-- Can be run as: DBA user or CDC_USER
-- Downtime: No
-- ===================================================================

-- Run as: sqlplus CDC_USER/password@RETAILDB
-- Or as:  sqlplus system/password@RETAILDB

SET PAGESIZE 1000
SET LINESIZE 200
SET ECHO ON
SET FEEDBACK ON

PROMPT
PROMPT ===================================================================
PROMPT ORACLE CDC CONFIGURATION VALIDATION
PROMPT ===================================================================
PROMPT Run Date: 
SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS CURRENT_TIME FROM DUAL;

-- ===================================================================
-- CHECK 1: ARCHIVELOG Mode
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 1: ARCHIVELOG Mode Status
PROMPT ===================================================================

SELECT 
    LOG_MODE AS "Archive Mode",
    FORCE_LOGGING AS "Force Logging",
    FLASHBACK_ON AS "Flashback",
    CASE 
        WHEN LOG_MODE = 'ARCHIVELOG' THEN '✓ PASS'
        ELSE '✗ FAIL - ARCHIVELOG not enabled'
    END AS "Status"
FROM V$DATABASE;

-- Expected: LOG_MODE = 'ARCHIVELOG'

-- ===================================================================
-- CHECK 2: Supplemental Logging (Database Level)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 2: Database-Level Supplemental Logging
PROMPT ===================================================================

SELECT 
    SUPPLEMENTAL_LOG_DATA_MIN AS "Minimal",
    SUPPLEMENTAL_LOG_DATA_PK AS "Primary Key",
    SUPPLEMENTAL_LOG_DATA_UI AS "Unique Index",
    SUPPLEMENTAL_LOG_DATA_FK AS "Foreign Key",
    SUPPLEMENTAL_LOG_DATA_ALL AS "All Columns",
    CASE 
        WHEN SUPPLEMENTAL_LOG_DATA_MIN = 'YES' 
             AND SUPPLEMENTAL_LOG_DATA_PK = 'YES' 
        THEN '✓ PASS'
        ELSE '✗ FAIL - Required logging not enabled'
    END AS "Status"
FROM V$DATABASE;

-- Expected: Minimal = YES, Primary Key = YES

-- ===================================================================
-- CHECK 3: Table-Level Supplemental Logging
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 3: Table-Level Supplemental Logging for RETAIL Schema
PROMPT ===================================================================

SELECT 
    OWNER,
    TABLE_NAME,
    LOG_GROUP_NAME,
    LOG_GROUP_TYPE,
    ALWAYS,
    CASE 
        WHEN LOG_GROUP_TYPE LIKE '%ALL%' THEN '✓ PASS'
        ELSE '⚠ WARN - Consider ALL COLUMN logging'
    END AS "Status"
FROM DBA_LOG_GROUPS
WHERE OWNER = 'RETAIL'
ORDER BY TABLE_NAME;

-- Expected: LOG_GROUP_TYPE = 'ALL COLUMN LOGGING' for each table

-- ===================================================================
-- CHECK 4: CDC User Exists and Has Correct Privileges
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 4: CDC_USER Existence and Privileges
PROMPT ===================================================================

-- Check user exists
SELECT 
    USERNAME,
    ACCOUNT_STATUS,
    CREATED,
    DEFAULT_TABLESPACE,
    CASE 
        WHEN ACCOUNT_STATUS = 'OPEN' THEN '✓ PASS'
        ELSE '✗ FAIL - Account locked or expired'
    END AS "Status"
FROM DBA_USERS
WHERE USERNAME = 'CDC_USER';

PROMPT
PROMPT System Privileges for CDC_USER:

SELECT 
    GRANTEE,
    PRIVILEGE,
    '✓' AS "Granted"
FROM DBA_SYS_PRIVS
WHERE GRANTEE = 'CDC_USER'
ORDER BY PRIVILEGE;

PROMPT
PROMPT Object Privileges for CDC_USER:

SELECT 
    OWNER,
    TABLE_NAME,
    PRIVILEGE,
    '✓' AS "Granted"
FROM DBA_TAB_PRIVS
WHERE GRANTEE = 'CDC_USER'
ORDER BY OWNER, TABLE_NAME, PRIVILEGE;

-- ===================================================================
-- CHECK 5: RETAIL Schema Tables Exist
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 5: RETAIL Schema Tables
PROMPT ===================================================================

SELECT 
    OWNER,
    TABLE_NAME,
    NUM_ROWS,
    LAST_ANALYZED,
    CASE 
        WHEN NUM_ROWS > 0 OR NUM_ROWS IS NULL THEN '✓ PASS'
        ELSE '⚠ WARN - Table is empty'
    END AS "Status"
FROM DBA_TABLES
WHERE OWNER = 'RETAIL'
  AND TABLE_NAME IN ('CUSTOMERS', 'ORDERS', 'PRODUCTS')
ORDER BY TABLE_NAME;

-- Expected: 3 tables (CUSTOMERS, ORDERS, PRODUCTS)

-- ===================================================================
-- CHECK 6: Archive Log Destination and Space
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 6: Archive Log Destination and Space Availability
PROMPT ===================================================================

SELECT 
    NAME AS "Destination",
    SPACE_LIMIT/1024/1024/1024 AS "Limit (GB)",
    SPACE_USED/1024/1024/1024 AS "Used (GB)",
    SPACE_RECLAIMABLE/1024/1024/1024 AS "Reclaimable (GB)",
    ROUND((SPACE_USED / SPACE_LIMIT) * 100, 2) AS "Used %",
    NUMBER_OF_FILES AS "Files",
    CASE 
        WHEN (SPACE_USED / SPACE_LIMIT) < 0.80 THEN '✓ PASS'
        WHEN (SPACE_USED / SPACE_LIMIT) < 0.90 THEN '⚠ WARN - Space running low'
        ELSE '✗ FAIL - Critical space issue'
    END AS "Status"
FROM V$RECOVERY_FILE_DEST;

-- Expected: Used % < 80%

-- ===================================================================
-- CHECK 7: Recent Archive Logs Generated
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 7: Recent Archive Log Generation
PROMPT ===================================================================

SELECT 
    COUNT(*) AS "Archive Logs (Last 24h)",
    ROUND(SUM(BLOCKS * BLOCK_SIZE) / 1024 / 1024 / 1024, 2) AS "Total GB Generated",
    CASE 
        WHEN COUNT(*) > 0 THEN '✓ PASS'
        ELSE '⚠ WARN - No recent archive logs'
    END AS "Status"
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 1;

PROMPT
PROMPT Recent Archive Logs (Last 10):

SELECT 
    SEQUENCE# AS "Seq#",
    TO_CHAR(FIRST_TIME, 'YYYY-MM-DD HH24:MI:SS') AS "First Time",
    TO_CHAR(NEXT_TIME, 'YYYY-MM-DD HH24:MI:SS') AS "Next Time",
    BLOCKS * BLOCK_SIZE / 1024 / 1024 AS "Size MB",
    ARCHIVED AS "Archived",
    APPLIED AS "Applied"
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 1
ORDER BY SEQUENCE# DESC
FETCH FIRST 10 ROWS ONLY;

-- ===================================================================
-- CHECK 8: Current SCN (Baseline for CDC)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 8: Current System Change Number (SCN)
PROMPT ===================================================================

SELECT 
    CURRENT_SCN AS "Current SCN",
    TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF') AS "Timestamp",
    '✓ Note this SCN for CDC baseline' AS "Status"
FROM V$DATABASE;

-- This SCN is the starting point for incremental CDC

-- ===================================================================
-- CHECK 9: Test LogMiner Access (CDC_USER only)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 9: LogMiner Access Test (Skip if not CDC_USER)
PROMPT ===================================================================

-- Note: This test only works if running as CDC_USER
-- Comment out if running as DBA

PROMPT Testing LogMiner session start/stop...

DECLARE
    v_status VARCHAR2(100);
BEGIN
    -- Try to start LogMiner session
    DBMS_LOGMNR.START_LOGMNR(
        OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
    );
    
    -- Check if successful
    SELECT 'LogMiner started successfully' INTO v_status FROM DUAL;
    DBMS_OUTPUT.PUT_LINE('✓ PASS - ' || v_status);
    
    -- End LogMiner session
    DBMS_LOGMNR.END_LOGMNR;
    DBMS_OUTPUT.PUT_LINE('✓ PASS - LogMiner stopped successfully');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ FAIL - LogMiner test failed: ' || SQLERRM);
        -- Try to clean up
        BEGIN
            DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
END;
/

-- ===================================================================
-- CHECK 10: Data Sample (Verify tables have data)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 10: Sample Data Verification
PROMPT ===================================================================

SELECT 
    'CUSTOMERS' AS "Table",
    COUNT(*) AS "Row Count",
    CASE 
        WHEN COUNT(*) > 0 THEN '✓ PASS'
        ELSE '⚠ WARN - No data'
    END AS "Status"
FROM RETAIL.CUSTOMERS
UNION ALL
SELECT 
    'ORDERS',
    COUNT(*),
    CASE 
        WHEN COUNT(*) > 0 THEN '✓ PASS'
        ELSE '⚠ WARN - No data'
    END
FROM RETAIL.ORDERS
UNION ALL
SELECT 
    'PRODUCTS',
    COUNT(*),
    CASE 
        WHEN COUNT(*) > 0 THEN '✓ PASS'
        ELSE '⚠ WARN - No data'
    END
FROM RETAIL.PRODUCTS;

-- ===================================================================
-- CHECK 11: Foreign Key Relationships
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CHECK 11: Foreign Key Constraints
PROMPT ===================================================================

SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    CONSTRAINT_TYPE,
    R_CONSTRAINT_NAME,
    DELETE_RULE,
    STATUS
FROM DBA_CONSTRAINTS
WHERE OWNER = 'RETAIL'
  AND CONSTRAINT_TYPE IN ('R', 'P', 'U')
ORDER BY TABLE_NAME, CONSTRAINT_TYPE;

-- Expected: ORDERS has FK to CUSTOMERS

-- ===================================================================
-- SUMMARY REPORT
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CDC CONFIGURATION VALIDATION SUMMARY
PROMPT ===================================================================

SELECT 
    'ARCHIVELOG Mode' AS "Component",
    CASE WHEN (SELECT LOG_MODE FROM V$DATABASE) = 'ARCHIVELOG' 
         THEN '✓ CONFIGURED' 
         ELSE '✗ NOT CONFIGURED' 
    END AS "Status"
FROM DUAL
UNION ALL
SELECT 
    'Supplemental Logging (DB)',
    CASE WHEN (SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE) = 'YES' 
         THEN '✓ CONFIGURED' 
         ELSE '✗ NOT CONFIGURED' 
    END
FROM DUAL
UNION ALL
SELECT 
    'Supplemental Logging (Tables)',
    CASE WHEN (SELECT COUNT(*) FROM DBA_LOG_GROUPS WHERE OWNER = 'RETAIL') >= 3
         THEN '✓ CONFIGURED' 
         ELSE '✗ NOT CONFIGURED' 
    END
FROM DUAL
UNION ALL
SELECT 
    'CDC User (CDC_USER)',
    CASE WHEN (SELECT COUNT(*) FROM DBA_USERS WHERE USERNAME = 'CDC_USER') = 1
         THEN '✓ CONFIGURED' 
         ELSE '✗ NOT CONFIGURED' 
    END
FROM DUAL
UNION ALL
SELECT 
    'RETAIL Schema Tables',
    CASE WHEN (SELECT COUNT(*) FROM DBA_TABLES WHERE OWNER = 'RETAIL' 
               AND TABLE_NAME IN ('CUSTOMERS', 'ORDERS', 'PRODUCTS')) = 3
         THEN '✓ CONFIGURED' 
         ELSE '✗ NOT CONFIGURED' 
    END
FROM DUAL
UNION ALL
SELECT 
    'Archive Log Space',
    CASE WHEN (SELECT SPACE_USED/SPACE_LIMIT FROM V$RECOVERY_FILE_DEST) < 0.80
         THEN '✓ HEALTHY' 
         ELSE '⚠ CHECK SPACE' 
    END
FROM DUAL;

-- ===================================================================
-- NEXT STEPS
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT NEXT STEPS
PROMPT ===================================================================
PROMPT
PROMPT If all checks show '✓ CONFIGURED' or '✓ PASS':
PROMPT   1. Note the Current SCN from CHECK 8
PROMPT   2. Proceed to Databricks Lakeflow Connect setup
PROMPT   3. Create Unity Catalog connection with CDC_USER credentials
PROMPT   4. Configure ingestion gateway and pipeline
PROMPT
PROMPT If any checks show '✗ FAIL' or '⚠ WARN':
PROMPT   1. Review the specific check that failed
PROMPT   2. Refer to previous setup scripts (01-03)
PROMPT   3. Fix configuration issues
PROMPT   4. Re-run this verification script
PROMPT
PROMPT For production deployment:
PROMPT   1. Implement RMAN backup strategy for archive logs
PROMPT   2. Set up archive log cleanup policies
PROMPT   3. Monitor redo log generation rate
PROMPT   4. Document CDC_USER password in secure location
PROMPT   5. Test CDC with sample transactions (07_cdc_test_operations.sql)
PROMPT ===================================================================

EXIT;
