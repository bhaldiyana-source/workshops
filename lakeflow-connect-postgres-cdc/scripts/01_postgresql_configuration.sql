-- ============================================================================
-- PostgreSQL CDC Configuration Script
-- ============================================================================
-- This script configures PostgreSQL for Change Data Capture (CDC) using
-- logical replication with Databricks Lakeflow Connect.
--
-- Prerequisites:
-- - PostgreSQL 10 or later
-- - Superuser or admin access
-- - Database restart capability (for wal_level change)
--
-- Usage:
--   psql -h your-host -U postgres -d postgres -f 01_postgresql_configuration.sql
-- ============================================================================

-- ============================================================================
-- PART 1: Verify Current Configuration
-- ============================================================================

\echo '================================================'
\echo 'Part 1: Checking Current PostgreSQL Configuration'
\echo '================================================'
\echo ''

-- Check PostgreSQL version
\echo '1. PostgreSQL Version:'
SELECT version();
\echo ''

-- Check current WAL level
\echo '2. Current WAL Level (must be logical):'
SHOW wal_level;
\echo ''

-- Check replication parameters
\echo '3. Replication Parameters:'
SELECT 
    name,
    setting,
    unit,
    context,
    source
FROM pg_settings 
WHERE name IN (
    'wal_level',
    'max_replication_slots',
    'max_wal_senders',
    'wal_keep_size',      -- PostgreSQL 13+
    'wal_keep_segments'    -- PostgreSQL 12 and earlier
)
ORDER BY name;
\echo ''

-- ============================================================================
-- PART 2: Enable Logical Replication (Configuration File Changes)
-- ============================================================================

\echo '================================================'
\echo 'Part 2: Logical Replication Configuration'
\echo '================================================'
\echo ''

\echo 'IMPORTANT: The following parameters require PostgreSQL restart:'
\echo '- wal_level = logical'
\echo '- max_replication_slots >= 10'
\echo '- max_wal_senders >= 10'
\echo ''
\echo 'For AWS RDS:'
\echo '  1. Modify parameter group: rds.logical_replication = 1'
\echo '  2. Reboot RDS instance'
\echo ''
\echo 'For self-managed PostgreSQL:'
\echo '  1. Edit postgresql.conf (see config/postgresql.conf.sample)'
\echo '  2. Run: sudo systemctl restart postgresql'
\echo ''
\echo 'For Azure Database:'
\echo '  1. Set azure.replication_support = LOGICAL'
\echo '  2. Restart server if required'
\echo ''

-- These ALTER SYSTEM commands will prepare config but require restart
-- Uncomment if you have ALTER SYSTEM privileges

-- ALTER SYSTEM SET wal_level = 'logical';
-- ALTER SYSTEM SET max_replication_slots = 10;
-- ALTER SYSTEM SET max_wal_senders = 10;
-- ALTER SYSTEM SET wal_keep_size = '2GB';  -- PostgreSQL 13+
-- SELECT pg_reload_conf();  -- This alone won't apply wal_level change

\echo 'After restart, reconnect and verify wal_level is logical:'
\echo 'SHOW wal_level;'
\echo ''

-- ============================================================================
-- PART 3: Create Replication User
-- ============================================================================

\echo '================================================'
\echo 'Part 3: Creating Replication User'
\echo '================================================'
\echo ''

-- Create replication user with secure password
-- IMPORTANT: Change the password to a secure value
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lakeflow_replication') THEN
        CREATE ROLE lakeflow_replication WITH REPLICATION LOGIN PASSWORD 'ChangeThisPassword123!';
        RAISE NOTICE 'Created replication user: lakeflow_replication';
    ELSE
        RAISE NOTICE 'Replication user already exists: lakeflow_replication';
    END IF;
END
$$;

-- Grant necessary permissions
GRANT CONNECT ON DATABASE retaildb TO lakeflow_replication;
\echo 'Granted CONNECT privilege on database'

-- ============================================================================
-- PART 4: Grant Schema and Table Permissions
-- ============================================================================

\echo '================================================'
\echo 'Part 4: Granting Schema and Table Permissions'
\echo '================================================'
\echo ''

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO lakeflow_replication;
\echo 'Granted USAGE on schema public'

-- Grant SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO lakeflow_replication;
\echo 'Granted SELECT on all existing tables'

-- Grant SELECT on future tables (important!)
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT SELECT ON TABLES TO lakeflow_replication;
\echo 'Granted SELECT on future tables'

-- ============================================================================
-- PART 5: Verify User and Permissions
-- ============================================================================

\echo '================================================'
\echo 'Part 5: Verifying User and Permissions'
\echo '================================================'
\echo ''

-- List user details
\echo 'Replication User Details:'
SELECT 
    rolname as username,
    rolsuper as is_superuser,
    rolreplication as can_replicate,
    rolconnlimit as connection_limit,
    rolvaliduntil as password_expiry
FROM pg_roles
WHERE rolname = 'lakeflow_replication';
\echo ''

-- Check table permissions
\echo 'Table Permissions for lakeflow_replication:'
SELECT 
    schemaname,
    tablename,
    has_table_privilege('lakeflow_replication', schemaname || '.' || tablename, 'SELECT') as has_select
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;
\echo ''

-- ============================================================================
-- PART 6: Configuration Validation
-- ============================================================================

\echo '================================================'
\echo 'Part 6: Final Configuration Validation'
\echo '================================================'
\echo ''

-- Comprehensive validation query
WITH validation AS (
    SELECT 'wal_level' as check_name, 
           setting as value,
           CASE WHEN setting = 'logical' THEN '✓ PASS' ELSE '✗ FAIL' END as status
    FROM pg_settings WHERE name = 'wal_level'
    
    UNION ALL
    
    SELECT 'max_replication_slots',
           setting,
           CASE WHEN setting::int >= 5 THEN '✓ PASS' ELSE '✗ FAIL' END
    FROM pg_settings WHERE name = 'max_replication_slots'
    
    UNION ALL
    
    SELECT 'max_wal_senders',
           setting,
           CASE WHEN setting::int >= 5 THEN '✓ PASS' ELSE '✗ FAIL' END
    FROM pg_settings WHERE name = 'max_wal_senders'
    
    UNION ALL
    
    SELECT 'replication_user_exists',
           CASE WHEN COUNT(*) > 0 THEN 'Yes' ELSE 'No' END,
           CASE WHEN COUNT(*) > 0 THEN '✓ PASS' ELSE '✗ FAIL' END
    FROM pg_roles 
    WHERE rolreplication = true AND rolname = 'lakeflow_replication'
)
SELECT * FROM validation;
\echo ''

\echo '================================================'
\echo 'PostgreSQL CDC Configuration Complete!'
\echo '================================================'
\echo ''
\echo 'Next Steps:'
\echo '1. Create database and tables (02_create_schema.sql)'
\echo '2. Set replica identity on tables (included in schema creation)'
\echo '3. Create publication and replication slot'
\echo '4. Configure Databricks Lakeflow Connect'
\echo ''
\echo 'REMINDER: Store credentials securely!'
\echo '- User: lakeflow_replication'
\echo '- Password: (the one you set above)'
\echo '- Host: your-postgres-host'
\echo '- Port: 5432'
\echo '- Database: retaildb'
\echo ''
