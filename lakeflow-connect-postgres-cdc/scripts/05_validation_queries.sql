-- ============================================================================
-- PostgreSQL Validation and Monitoring Queries
-- ============================================================================
-- This script contains queries for validating CDC setup and monitoring
-- replication health on the PostgreSQL side.
--
-- Usage:
--   psql -h your-host -U postgres -d retaildb -f 05_validation_queries.sql
-- ============================================================================

\echo '================================================'
\echo 'PostgreSQL CDC Validation and Monitoring'
\echo '================================================'
\echo ''

-- ============================================================================
-- SECTION 1: Configuration Validation
-- ============================================================================

\echo '================================================'
\echo 'SECTION 1: Configuration Validation'
\echo '================================================'
\echo ''

\echo '1.1 WAL Configuration:'
SELECT 
    name,
    setting,
    unit,
    CASE 
        WHEN name = 'wal_level' AND setting = 'logical' THEN '✓'
        WHEN name = 'max_replication_slots' AND setting::int >= 5 THEN '✓'
        WHEN name = 'max_wal_senders' AND setting::int >= 5 THEN '✓'
        ELSE '✗'
    END as status
FROM pg_settings 
WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders')
ORDER BY name;

\echo ''

\echo '1.2 Replica Identity Check:'
SELECT 
    schemaname,
    tablename,
    CASE relreplident
        WHEN 'd' THEN 'DEFAULT ✗'
        WHEN 'f' THEN 'FULL ✓'
        WHEN 'i' THEN 'INDEX ⚠'
        WHEN 'n' THEN 'NOTHING ✗'
    END as replica_identity
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_tables t ON c.relname = t.tablename AND n.nspname = t.schemaname
WHERE schemaname = 'public' 
    AND tablename IN ('customers', 'orders', 'products')
ORDER BY tablename;

\echo ''

\echo '1.3 Publication Status:'
SELECT 
    pubname,
    COUNT(*) as table_count,
    string_agg(schemaname || '.' || tablename, ', ' ORDER BY tablename) as tables
FROM pg_publication_tables
WHERE pubname = 'lakeflow_publication'
GROUP BY pubname;

\echo ''

-- ============================================================================
-- SECTION 2: Replication Slot Monitoring
-- ============================================================================

\echo '================================================'
\echo 'SECTION 2: Replication Slot Monitoring'
\echo '================================================'
\echo ''

\echo '2.1 Replication Slot Status:'
SELECT 
    slot_name,
    plugin,
    database,
    active,
    active_pid,
    restart_lsn,
    confirmed_flush_lsn,
    CASE 
        WHEN active THEN '✓ Active'
        ELSE '⚠ Inactive'
    END as status
FROM pg_replication_slots
WHERE slot_name = 'lakeflow_slot';

\echo ''

\echo '2.2 Replication Lag (WAL Size):'
SELECT 
    slot_name,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    ) as replication_lag_size,
    pg_size_pretty(
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
    ) as pending_wal_size,
    CASE 
        WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) < 10485760 THEN '✓ Normal (< 10MB)'
        WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) < 104857600 THEN '⚠ Warning (< 100MB)'
        ELSE '✗ Critical (> 100MB)'
    END as lag_status
FROM pg_replication_slots
WHERE slot_name = 'lakeflow_slot';

\echo ''

\echo '2.3 WAL Directory Size:'
SELECT 
    COUNT(*) as wal_file_count,
    pg_size_pretty(SUM(size)) as total_wal_size,
    CASE 
        WHEN SUM(size) < 1073741824 THEN '✓ Normal (< 1GB)'
        WHEN SUM(size) < 10737418240 THEN '⚠ Warning (< 10GB)'
        ELSE '✗ Critical (> 10GB)'
    END as wal_size_status
FROM pg_ls_waldir();

\echo ''

-- ============================================================================
-- SECTION 3: Table Statistics
-- ============================================================================

\echo '================================================'
\echo 'SECTION 3: Table Statistics'
\echo '================================================'
\echo ''

\echo '3.1 Record Counts:'
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    MIN(created_date) as oldest_record,
    MAX(last_updated) as most_recent_update
FROM customers
UNION ALL
SELECT 
    'orders',
    COUNT(*),
    MIN(created_date),
    MAX(last_updated)
FROM orders
UNION ALL
SELECT 
    'products',
    COUNT(*),
    MIN(created_date),
    MAX(last_updated)
FROM products
ORDER BY table_name;

\echo ''

\echo '3.2 Recent Activity (Last Hour):'
SELECT 
    'customers' as table_name,
    COUNT(*) as changes_last_hour
FROM customers
WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
UNION ALL
SELECT 'orders', COUNT(*)
FROM orders
WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
UNION ALL
SELECT 'products', COUNT(*)
FROM products
WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY table_name;

\echo ''

\echo '3.3 Table Sizes:'
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as indexes_size
FROM pg_tables
WHERE schemaname = 'public'
    AND tablename IN ('customers', 'orders', 'products')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

\echo ''

-- ============================================================================
-- SECTION 4: Data Quality Checks
-- ============================================================================

\echo '================================================'
\echo 'SECTION 4: Data Quality Checks'
\echo '================================================'
\echo ''

\echo '4.1 NULL Value Check:'
SELECT 'customers' as table_name, 
       SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails,
       SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) as null_states,
       SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) as null_phones
FROM customers
UNION ALL
SELECT 'orders',
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END)
FROM orders
UNION ALL
SELECT 'products',
       SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END)
FROM products;

\echo ''

\echo '4.2 Referential Integrity Check:'
-- Check for orphaned orders (should be 0)
SELECT 
    COUNT(*) as orphaned_orders,
    CASE 
        WHEN COUNT(*) = 0 THEN '✓ No orphans'
        ELSE '✗ Orphans found!'
    END as status
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

\echo ''

\echo '4.3 Duplicate Email Check:'
SELECT 
    email,
    COUNT(*) as duplicate_count
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;
-- Should return no rows

\echo ''

-- ============================================================================
-- SECTION 5: Performance Metrics
-- ============================================================================

\echo '================================================'
\echo 'SECTION 5: Performance Metrics'
\echo '================================================'
\echo ''

\echo '5.1 Connection Statistics:'
SELECT 
    datname as database,
    numbackends as active_connections,
    xact_commit as transactions_committed,
    xact_rollback as transactions_rolled_back,
    blks_read as blocks_read,
    blks_hit as blocks_hit,
    ROUND(
        100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2
    ) as cache_hit_ratio
FROM pg_stat_database
WHERE datname = 'retaildb';

\echo ''

\echo '5.2 Table Access Patterns:'
SELECT 
    schemaname,
    relname as tablename,
    seq_scan as sequential_scans,
    idx_scan as index_scans,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_tup_hot_upd as hot_updates
FROM pg_stat_user_tables
WHERE schemaname = 'public'
    AND relname IN ('customers', 'orders', 'products')
ORDER BY relname;

\echo ''

\echo '5.3 Index Usage:'
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as times_used,
    CASE 
        WHEN idx_scan = 0 THEN '⚠ Unused'
        WHEN idx_scan < 100 THEN '○ Low usage'
        ELSE '✓ Active'
    END as usage_status
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
    AND tablename IN ('customers', 'orders', 'products')
ORDER BY tablename, indexname;

\echo ''

-- ============================================================================
-- SECTION 6: Replication User Verification
-- ============================================================================

\echo '================================================'
\echo 'SECTION 6: Replication User Verification'
\echo '================================================'
\echo ''

\echo '6.1 User Permissions:'
SELECT 
    rolname as username,
    rolsuper as is_superuser,
    rolreplication as can_replicate,
    rolconnlimit as connection_limit
FROM pg_roles
WHERE rolname = 'lakeflow_replication';

\echo ''

\echo '6.2 Table Access for Replication User:'
SELECT 
    schemaname,
    tablename,
    has_table_privilege('lakeflow_replication', schemaname || '.' || tablename, 'SELECT') as has_select,
    CASE 
        WHEN has_table_privilege('lakeflow_replication', schemaname || '.' || tablename, 'SELECT') 
        THEN '✓'
        ELSE '✗'
    END as status
FROM pg_tables
WHERE schemaname = 'public'
    AND tablename IN ('customers', 'orders', 'products')
ORDER BY tablename;

\echo ''

-- ============================================================================
-- SECTION 7: WAL Activity Monitoring
-- ============================================================================

\echo '================================================'
\echo 'SECTION 7: WAL Activity Monitoring'
\echo '================================================'
\echo ''

\echo '7.1 Current WAL Position:'
SELECT 
    pg_current_wal_lsn() as current_wal_lsn,
    pg_walfile_name(pg_current_wal_lsn()) as current_wal_file;

\echo ''

\echo '7.2 WAL Generation Rate (Last Checkpoint):'
SELECT 
    checkpoints_timed,
    checkpoints_req as checkpoints_requested,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend,
    pg_size_pretty(buffers_checkpoint * 8192) as checkpoint_write_size
FROM pg_stat_bgwriter;

\echo ''

-- ============================================================================
-- SECTION 8: Cleanup and Maintenance
-- ============================================================================

\echo '================================================'
\echo 'SECTION 8: Cleanup Commands (DO NOT RUN AUTOMATICALLY)'
\echo '================================================'
\echo ''

\echo 'These commands are for reference only. Run manually if needed:'
\echo ''
\echo '-- Drop replication slot (WARNING: Will lose position!)'
\echo '-- SELECT pg_drop_replication_slot(''lakeflow_slot'');'
\echo ''
\echo '-- Recreate replication slot'
\echo '-- SELECT pg_create_logical_replication_slot(''lakeflow_slot'', ''pgoutput'');'
\echo ''
\echo '-- Drop publication'
\echo '-- DROP PUBLICATION lakeflow_publication;'
\echo ''
\echo '-- Analyze tables for query optimization'
\echo '-- ANALYZE customers;'
\echo '-- ANALYZE orders;'
\echo '-- ANALYZE products;'
\echo ''
\echo '-- Vacuum tables'
\echo '-- VACUUM ANALYZE customers;'
\echo '-- VACUUM ANALYZE orders;'
\echo '-- VACUUM ANALYZE products;'
\echo ''

-- ============================================================================
-- SECTION 9: Health Summary
-- ============================================================================

\echo '================================================'
\echo 'SECTION 9: CDC Health Summary'
\echo '================================================'
\echo ''

WITH health_check AS (
    SELECT 
        'WAL Level' as component,
        CASE WHEN setting = 'logical' THEN 'PASS' ELSE 'FAIL' END as status,
        setting as value
    FROM pg_settings WHERE name = 'wal_level'
    
    UNION ALL
    
    SELECT 
        'Replication Slot',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        COALESCE(string_agg(slot_name, ', '), 'None')
    FROM pg_replication_slots WHERE slot_name = 'lakeflow_slot'
    
    UNION ALL
    
    SELECT 
        'Publication',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        COALESCE(string_agg(pubname, ', '), 'None')
    FROM pg_publication WHERE pubname = 'lakeflow_publication'
    
    UNION ALL
    
    SELECT 
        'Replica Identity',
        CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END,
        CAST(COUNT(*) AS TEXT) || '/3 tables'
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_tables t ON c.relname = t.tablename AND n.nspname = t.schemaname
    WHERE schemaname = 'public' 
        AND tablename IN ('customers', 'orders', 'products')
        AND relreplident = 'f'
    
    UNION ALL
    
    SELECT 
        'Replication User',
        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
        'lakeflow_replication'
    FROM pg_roles WHERE rolname = 'lakeflow_replication' AND rolreplication = true
)
SELECT 
    component,
    status,
    value,
    CASE status
        WHEN 'PASS' THEN '✓'
        ELSE '✗'
    END as indicator
FROM health_check
ORDER BY 
    CASE status WHEN 'FAIL' THEN 1 ELSE 2 END,
    component;

\echo ''
\echo '================================================'
\echo 'Validation Complete!'
\echo '================================================'
\echo ''
\echo 'If all checks show PASS/✓, your CDC setup is healthy.'
\echo 'If any checks show FAIL/✗, review the specific section above.'
\echo ''
\echo 'For Databricks validation, use: databricks_validation_queries.sql'
\echo ''
