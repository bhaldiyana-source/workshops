-- ===================================================================
-- Script: 02_configure_supplemental_logging.sql
-- Purpose: Enable supplemental logging for Oracle CDC operations
-- Requirements: DBA or SYSDBA privileges, ARCHIVELOG mode enabled
-- Downtime: No (can be run on live database)
-- ===================================================================

-- Connect as DBA user
-- sqlplus system/password@RETAILDB

-- ===================================================================
-- STEP 1: Check prerequisite - ARCHIVELOG mode
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Verifying prerequisites...
PROMPT ===================================================================

SELECT 
    LOG_MODE AS "Archive Mode",
    SUPPLEMENTAL_LOG_DATA_MIN AS "Supplemental Logging (Minimal)",
    SUPPLEMENTAL_LOG_DATA_PK AS "Supplemental Logging (PK)",
    SUPPLEMENTAL_LOG_DATA_UI AS "Supplemental Logging (UI)",
    FORCE_LOGGING AS "Force Logging"
FROM V$DATABASE;

-- ARCHIVELOG must be enabled before proceeding
-- Check that LOG_MODE = 'ARCHIVELOG'

-- ===================================================================
-- STEP 2: Enable database-level supplemental logging (MINIMAL)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Enabling database-level minimal supplemental logging...
PROMPT ===================================================================

-- This is required for basic CDC functionality
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

PROMPT Minimal supplemental logging enabled.

-- ===================================================================
-- STEP 3: Enable primary key supplemental logging
-- ===================================================================
PROMPT
PROMPT Enabling primary key supplemental logging...

-- This ensures primary key columns are always logged
-- Essential for CDC to identify unique rows
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

PROMPT Primary key supplemental logging enabled.

-- ===================================================================
-- STEP 4: (Optional) Enable unique index supplemental logging
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Optional: Unique index supplemental logging
PROMPT ===================================================================
PROMPT This captures unique index columns for additional CDC flexibility.
PROMPT Uncomment the following line to enable:

-- ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (UNIQUE) COLUMNS;

-- ===================================================================
-- STEP 5: Verify database-level supplemental logging
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Verifying database-level supplemental logging configuration...
PROMPT ===================================================================

SELECT 
    SUPPLEMENTAL_LOG_DATA_MIN AS "Minimal (Required)",
    SUPPLEMENTAL_LOG_DATA_PK AS "Primary Key (Recommended)",
    SUPPLEMENTAL_LOG_DATA_UI AS "Unique Index (Optional)",
    SUPPLEMENTAL_LOG_DATA_FK AS "Foreign Key (Optional)",
    SUPPLEMENTAL_LOG_DATA_ALL AS "All Columns (Optional)"
FROM V$DATABASE;

-- Expected:
-- Minimal = YES
-- Primary Key = YES
-- Others = NO (unless you enabled them)

-- ===================================================================
-- STEP 6: Check redo log generation impact
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Checking redo log statistics (before table-level logging)...
PROMPT ===================================================================

SELECT 
    NAME,
    VALUE
FROM V$SYSSTAT
WHERE NAME LIKE '%redo size%'
ORDER BY NAME;

-- Record these values to compare after enabling table-level logging
-- Typical increase: 5-15%

-- ===================================================================
-- STEP 7: Enable table-level supplemental logging for RETAIL tables
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Enabling table-level supplemental logging for RETAIL schema...
PROMPT ===================================================================

-- For CUSTOMERS table
PROMPT Configuring CUSTOMERS table...
ALTER TABLE RETAIL.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- For ORDERS table
PROMPT Configuring ORDERS table...
ALTER TABLE RETAIL.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- For PRODUCTS table
PROMPT Configuring PRODUCTS table...
ALTER TABLE RETAIL.PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

PROMPT Table-level supplemental logging configured for all RETAIL tables.

-- ===================================================================
-- STEP 8: Verify table-level supplemental logging
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Verifying table-level supplemental logging...
PROMPT ===================================================================

SELECT 
    OWNER,
    TABLE_NAME,
    LOG_GROUP_NAME,
    LOG_GROUP_TYPE,
    ALWAYS,
    GENERATED
FROM DBA_LOG_GROUPS
WHERE OWNER = 'RETAIL'
ORDER BY TABLE_NAME, LOG_GROUP_NAME;

-- Expected: Each table should have LOG_GROUP_TYPE = 'ALL COLUMN LOGGING'

-- ===================================================================
-- STEP 9: View supplemental log group columns
-- ===================================================================
PROMPT
PROMPT Viewing columns included in supplemental logging...

SELECT 
    lg.OWNER,
    lg.TABLE_NAME,
    lg.LOG_GROUP_NAME,
    lgc.COLUMN_NAME,
    lgc.POSITION
FROM DBA_LOG_GROUP_COLUMNS lgc
JOIN DBA_LOG_GROUPS lg 
    ON lgc.OWNER = lg.OWNER 
    AND lgc.LOG_GROUP_NAME = lg.LOG_GROUP_NAME
WHERE lg.OWNER = 'RETAIL'
ORDER BY lg.TABLE_NAME, lgc.POSITION;

-- ===================================================================
-- STEP 10: Test supplemental logging with sample transaction
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Testing supplemental logging with sample update...
PROMPT ===================================================================

-- Update a sample record to generate redo with supplemental logging
UPDATE RETAIL.CUSTOMERS
SET LAST_UPDATED = SYSTIMESTAMP
WHERE CUSTOMER_ID = 1
  AND ROWNUM = 1;

COMMIT;

PROMPT Sample transaction committed.
PROMPT Redo logs now contain supplemental information for CDC capture.

-- ===================================================================
-- STEP 11: Check redo log generation after supplemental logging
-- ===================================================================
PROMPT
PROMPT Checking redo log statistics (after table-level logging)...

SELECT 
    NAME,
    VALUE
FROM V$SYSSTAT
WHERE NAME LIKE '%redo size%'
ORDER BY NAME;

PROMPT
PROMPT Compare with previous values to see impact.
PROMPT Typical increase: 5-15% additional redo generation

-- ===================================================================
-- STEP 12: Monitor redo log switches
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Monitoring redo log switch frequency...
PROMPT ===================================================================

SELECT 
    TRUNC(FIRST_TIME) AS "Date",
    COUNT(*) AS "Log Switches",
    ROUND(SUM(BLOCKS * BLOCK_SIZE) / 1024 / 1024 / 1024, 2) AS "Redo GB Generated"
FROM V$ARCHIVED_LOG
WHERE FIRST_TIME > SYSDATE - 7
GROUP BY TRUNC(FIRST_TIME)
ORDER BY "Date" DESC;

-- Ideal: < 24 log switches per hour
-- If higher, consider increasing redo log size

-- ===================================================================
-- COMPLETION SUMMARY
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT SUPPLEMENTAL LOGGING CONFIGURATION COMPLETE
PROMPT ===================================================================
PROMPT
PROMPT Configuration Summary:
PROMPT   ✓ Database-level minimal supplemental logging: ENABLED
PROMPT   ✓ Primary key supplemental logging: ENABLED  
PROMPT   ✓ Table-level ALL COLUMN logging for RETAIL schema: ENABLED
PROMPT
PROMPT Tables Configured:
PROMPT   ✓ RETAIL.CUSTOMERS
PROMPT   ✓ RETAIL.ORDERS
PROMPT   ✓ RETAIL.PRODUCTS
PROMPT
PROMPT Next Steps:
PROMPT   1. Monitor redo log generation rate (5-15% increase expected)
PROMPT   2. Verify archive log space is sufficient
PROMPT   3. Proceed to script 03_create_cdc_user.sql
PROMPT
PROMPT Performance Impact:
PROMPT   - Minimal CPU impact (< 2%)
PROMPT   - Redo log size increase: 5-15%
PROMPT   - No impact on query performance
PROMPT
PROMPT Important Notes:
PROMPT   - Supplemental logging applies to new transactions only
PROMPT   - No need to restart database
PROMPT   - Can be disabled if CDC is no longer needed
PROMPT ===================================================================

EXIT;
