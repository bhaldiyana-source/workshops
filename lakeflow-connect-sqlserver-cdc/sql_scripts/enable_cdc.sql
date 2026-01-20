-- ============================================================================
-- Lakeflow Connect SQL Server CDC Workshop
-- Script: enable_cdc.sql
-- Purpose: Enable Change Data Capture (CDC) on RetailDB database and tables
-- ============================================================================

USE RetailDB;
GO

PRINT '============================================================================';
PRINT 'Enabling Change Data Capture (CDC)';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- Step 1: Verify Prerequisites
-- ============================================================================

PRINT 'Step 1: Verifying Prerequisites';
PRINT '-----------------------------------';

-- Check SQL Server version (CDC requires SQL Server 2008+)
DECLARE @version VARCHAR(100);
SET @version = @@VERSION;
PRINT 'SQL Server Version: ' + SUBSTRING(@version, 1, CHARINDEX('-', @version) - 1);

-- Check if user has sufficient permissions
IF IS_SRVROLEMEMBER('sysadmin') = 1 OR IS_MEMBER('db_owner') = 1
BEGIN
    PRINT '✓ User has sufficient permissions (sysadmin or db_owner)';
END
ELSE
BEGIN
    PRINT '✗ ERROR: User lacks required permissions';
    PRINT '  Required: sysadmin server role OR db_owner database role';
    RAISERROR('Insufficient permissions to enable CDC', 16, 1);
    RETURN;
END

-- Check recovery model (must be FULL for CDC)
DECLARE @recovery_model VARCHAR(20);
SELECT @recovery_model = recovery_model_desc FROM sys.databases WHERE name = 'RetailDB';

IF @recovery_model = 'FULL'
BEGIN
    PRINT '✓ Database recovery mode: FULL (required for CDC)';
END
ELSE
BEGIN
    PRINT '⚠ Database recovery mode: ' + @recovery_model + ' (changing to FULL)';
    ALTER DATABASE RetailDB SET RECOVERY FULL;
    PRINT '✓ Recovery mode changed to FULL';
END

PRINT '';

-- ============================================================================
-- Step 2: Enable CDC at Database Level
-- ============================================================================

PRINT 'Step 2: Enabling CDC at Database Level';
PRINT '---------------------------------------';

-- Check if CDC is already enabled on database
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'RetailDB' AND is_cdc_enabled = 1)
BEGIN
    PRINT 'ℹ CDC already enabled on database RetailDB';
END
ELSE
BEGIN
    -- Enable CDC on database
    EXEC sys.sp_cdc_enable_db;
    PRINT '✓ CDC enabled on database RetailDB';
END

-- Verify CDC is enabled
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'RetailDB' AND is_cdc_enabled = 1)
BEGIN
    PRINT '✓ Verification: CDC is enabled on database';
END
ELSE
BEGIN
    PRINT '✗ ERROR: CDC enable operation failed';
    RAISERROR('Failed to enable CDC on database', 16, 1);
    RETURN;
END

PRINT '';

-- ============================================================================
-- Step 3: Enable Change Tracking (Additional Metadata)
-- ============================================================================

PRINT 'Step 3: Enabling Change Tracking';
PRINT '---------------------------------';

-- Enable change tracking at database level
IF NOT EXISTS (SELECT * FROM sys.change_tracking_databases WHERE database_id = DB_ID('RetailDB'))
BEGIN
    ALTER DATABASE RetailDB
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
    
    PRINT '✓ Change tracking enabled (7-day retention)';
END
ELSE
BEGIN
    PRINT 'ℹ Change tracking already enabled';
END

PRINT '';

-- ============================================================================
-- Step 4: Enable CDC on Customers Table
-- ============================================================================

PRINT 'Step 4: Enabling CDC on Customers Table';
PRINT '----------------------------------------';

-- Check if CDC already enabled on customers
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'customers' AND is_tracked_by_cdc = 1)
BEGIN
    PRINT 'ℹ CDC already enabled on customers table';
END
ELSE
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'customers',
        @role_name = NULL,
        @supports_net_changes = 1;
    
    PRINT '✓ CDC enabled on customers table';
END

-- Enable change tracking on customers
IF NOT EXISTS (SELECT * FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('dbo.customers'))
BEGIN
    ALTER TABLE dbo.customers
    ENABLE CHANGE_TRACKING
    WITH (TRACK_COLUMNS_UPDATED = ON);
    
    PRINT '✓ Change tracking enabled on customers table';
END
ELSE
BEGIN
    PRINT 'ℹ Change tracking already enabled on customers';
END

PRINT '';

-- ============================================================================
-- Step 5: Enable CDC on Orders Table
-- ============================================================================

PRINT 'Step 5: Enabling CDC on Orders Table';
PRINT '-------------------------------------';

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'orders' AND is_tracked_by_cdc = 1)
BEGIN
    PRINT 'ℹ CDC already enabled on orders table';
END
ELSE
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'orders',
        @role_name = NULL,
        @supports_net_changes = 1;
    
    PRINT '✓ CDC enabled on orders table';
END

IF NOT EXISTS (SELECT * FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('dbo.orders'))
BEGIN
    ALTER TABLE dbo.orders
    ENABLE CHANGE_TRACKING
    WITH (TRACK_COLUMNS_UPDATED = ON);
    
    PRINT '✓ Change tracking enabled on orders table';
END
ELSE
BEGIN
    PRINT 'ℹ Change tracking already enabled on orders';
END

PRINT '';

-- ============================================================================
-- Step 6: Enable CDC on Products Table
-- ============================================================================

PRINT 'Step 6: Enabling CDC on Products Table';
PRINT '---------------------------------------';

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'products' AND is_tracked_by_cdc = 1)
BEGIN
    PRINT 'ℹ CDC already enabled on products table';
END
ELSE
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'products',
        @role_name = NULL,
        @supports_net_changes = 1;
    
    PRINT '✓ CDC enabled on products table';
END

IF NOT EXISTS (SELECT * FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('dbo.products'))
BEGIN
    ALTER TABLE dbo.products
    ENABLE CHANGE_TRACKING
    WITH (TRACK_COLUMNS_UPDATED = ON);
    
    PRINT '✓ Change tracking enabled on products table';
END
ELSE
BEGIN
    PRINT 'ℹ Change tracking already enabled on products';
END

PRINT '';

-- ============================================================================
-- Step 7: Verify CDC Configuration
-- ============================================================================

PRINT '============================================================================';
PRINT 'CDC Configuration Summary';
PRINT '============================================================================';
PRINT '';

-- Database CDC status
PRINT 'Database CDC Status:';
SELECT 
    name AS database_name,
    is_cdc_enabled,
    CASE WHEN is_cdc_enabled = 1 THEN '✓ Enabled' ELSE '✗ Not Enabled' END AS status
FROM sys.databases
WHERE name = 'RetailDB';

PRINT '';

-- Table CDC status
PRINT 'Table CDC Status:';
SELECT 
    SCHEMA_NAME(t.schema_id) AS schema_name,
    t.name AS table_name,
    t.is_tracked_by_cdc,
    CASE WHEN t.is_tracked_by_cdc = 1 THEN '✓ Enabled' ELSE '✗ Not Enabled' END AS cdc_status,
    ct.capture_instance,
    ct.start_lsn
FROM sys.tables t
LEFT JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
WHERE t.name IN ('customers', 'orders', 'products')
ORDER BY t.name;

PRINT '';

-- CDC change tables created
PRINT 'CDC Change Tables:';
SELECT 
    capture_instance,
    OBJECT_NAME(source_object_id) AS source_table,
    start_lsn,
    create_date
FROM cdc.change_tables
ORDER BY source_table;

PRINT '';

-- CDC jobs status
PRINT 'CDC Jobs:';
EXEC sys.sp_cdc_help_jobs;

PRINT '';
PRINT '✓ CDC configuration completed successfully!';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Verify SQL Server Agent is running (required for CDC jobs)';
PRINT '  2. Test CDC by making changes: INSERT, UPDATE, DELETE';
PRINT '  3. Query CDC tables to verify changes captured';
PRINT '  4. Configure Lakeflow Connect in Databricks';
PRINT '';
GO
