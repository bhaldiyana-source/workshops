-- ===================================================================
-- Script: 01_enable_archivelog.sql
-- Purpose: Enable ARCHIVELOG mode on Oracle database for CDC
-- Requirements: DBA or SYSDBA privileges
-- Downtime: Yes (requires database restart)
-- ===================================================================

-- Connect as DBA user (e.g., system or sys as sysdba)
-- sqlplus system/password@RETAILDB
-- or
-- sqlplus / as sysdba

-- ===================================================================
-- STEP 1: Check current ARCHIVELOG status
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Checking current ARCHIVELOG status...
PROMPT ===================================================================

SELECT 
    LOG_MODE,
    FORCE_LOGGING,
    FLASHBACK_ON
FROM V$DATABASE;

-- If LOG_MODE = 'ARCHIVELOG', you can skip to verification section
-- If LOG_MODE = 'NOARCHIVELOG', continue with enablement steps

-- ===================================================================
-- STEP 2: Check current database status
-- ===================================================================
PROMPT
PROMPT Checking database status...
SELECT INSTANCE_NAME, STATUS, DATABASE_STATUS FROM V$INSTANCE;

-- ===================================================================
-- STEP 3: Shutdown database
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CAUTION: About to shutdown database for ARCHIVELOG enablement
PROMPT This will disconnect all users and stop the database
PROMPT ===================================================================
PROMPT Press Ctrl+C to cancel, or press Enter to continue...
PAUSE

SHUTDOWN IMMEDIATE;

PROMPT Database shutdown complete.

-- ===================================================================
-- STEP 4: Startup database in MOUNT mode
-- ===================================================================
PROMPT
PROMPT Starting database in MOUNT mode...
STARTUP MOUNT;

PROMPT Database mounted (but not open).

-- ===================================================================
-- STEP 5: Enable ARCHIVELOG mode
-- ===================================================================
PROMPT
PROMPT Enabling ARCHIVELOG mode...
ALTER DATABASE ARCHIVELOG;

PROMPT ARCHIVELOG mode enabled.

-- ===================================================================
-- STEP 6: Open database
-- ===================================================================
PROMPT
PROMPT Opening database for normal operations...
ALTER DATABASE OPEN;

PROMPT Database is now open and operational.

-- ===================================================================
-- STEP 7: Verify ARCHIVELOG is enabled
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Verifying ARCHIVELOG configuration...
PROMPT ===================================================================

SELECT 
    LOG_MODE AS "Archive Mode",
    FORCE_LOGGING AS "Force Logging"
FROM V$DATABASE;

-- Expected: LOG_MODE = 'ARCHIVELOG'

-- ===================================================================
-- STEP 8: Check archive log destination
-- ===================================================================
PROMPT
PROMPT Checking archive log destination and space...

SELECT 
    NAME AS "Destination",
    SPACE_LIMIT/1024/1024/1024 AS "Space Limit (GB)",
    SPACE_USED/1024/1024/1024 AS "Space Used (GB)",
    SPACE_RECLAIMABLE/1024/1024/1024 AS "Space Reclaimable (GB)",
    NUMBER_OF_FILES AS "Archive Log Files"
FROM V$RECOVERY_FILE_DEST;

-- ===================================================================
-- STEP 9: Configure archive log destination size (if needed)
-- ===================================================================
PROMPT
PROMPT Current recovery file destination size shown above.
PROMPT Recommended minimum: 100 GB for production CDC workloads
PROMPT
PROMPT To change size, uncomment and run:
-- ALTER SYSTEM SET DB_RECOVERY_FILE_DEST_SIZE = 100G SCOPE=BOTH;

-- ===================================================================
-- STEP 10: View first few archive logs
-- ===================================================================
PROMPT
PROMPT Checking archive log generation...

SELECT 
    SEQUENCE#,
    FIRST_TIME,
    NEXT_TIME,
    BLOCKS * BLOCK_SIZE / 1024 / 1024 AS SIZE_MB,
    ARCHIVED,
    APPLIED,
    DELETED,
    NAME
FROM V$ARCHIVED_LOG
ORDER BY SEQUENCE# DESC
FETCH FIRST 5 ROWS ONLY;

-- ===================================================================
-- COMPLETION SUMMARY
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT ARCHIVELOG ENABLEMENT COMPLETE
PROMPT ===================================================================
PROMPT
PROMPT Next Steps:
PROMPT   1. Verify LOG_MODE shows 'ARCHIVELOG'
PROMPT   2. Monitor archive log space usage regularly
PROMPT   3. Implement RMAN backup strategy (see backup scripts)
PROMPT   4. Proceed to script 02_configure_supplemental_logging.sql
PROMPT
PROMPT Important: Set up archive log cleanup policy to prevent disk full!
PROMPT See RMAN documentation or DBA team for backup/cleanup procedures.
PROMPT ===================================================================

EXIT;
