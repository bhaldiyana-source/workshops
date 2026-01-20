-- ============================================================================
-- Create CDC User with Replication Privileges
-- Purpose: Create dedicated user for Lakeflow Connect CDC operations
-- Prerequisites: SUPER privilege or CREATE USER privilege
-- Security: Follow principle of least privilege
-- ============================================================================

-- ============================================================================
-- STEP 1: Create CDC User
-- ============================================================================

-- Create user with strong password
-- Replace 'SecurePassword123!' with your own strong password
-- Replace '%' with specific IP/hostname for production security
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'SecurePassword123!';

-- Production best practice: Restrict to specific hosts
-- Examples:
-- CREATE USER 'cdc_user'@'10.0.1.0/255.255.255.0' IDENTIFIED BY 'SecurePassword123!';  -- Specific subnet
-- CREATE USER 'cdc_user'@'databricks-gateway.company.com' IDENTIFIED BY 'SecurePassword123!';  -- Specific hostname

-- ============================================================================
-- STEP 2: Grant Replication Privileges
-- ============================================================================

-- Grant REPLICATION SLAVE privilege (required for reading binary logs)
-- Grant REPLICATION CLIENT privilege (required for SHOW MASTER STATUS, etc.)
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

-- Explanation of privileges:
-- - REPLICATION SLAVE: Allows reading binary log stream (like a replication slave)
-- - REPLICATION CLIENT: Allows SHOW MASTER STATUS, SHOW BINARY LOGS queries

-- ============================================================================
-- STEP 3: Grant SELECT Privileges on Source Database(s)
-- ============================================================================

-- Grant SELECT on specific database(s) that will be replicated
-- Note: retail_db doesn't exist yet; create it first if needed
-- This command will work once the database is created

-- For retail_db (our workshop database)
GRANT SELECT ON retail_db.* TO 'cdc_user'@'%';

-- For additional databases, repeat with different database names:
-- GRANT SELECT ON another_db.* TO 'cdc_user'@'%';

-- Production option: Grant SELECT only on specific tables
-- GRANT SELECT ON retail_db.customers TO 'cdc_user'@'%';
-- GRANT SELECT ON retail_db.orders TO 'cdc_user'@'%';
-- GRANT SELECT ON retail_db.products TO 'cdc_user'@'%';

-- ============================================================================
-- STEP 4: Apply Privilege Changes
-- ============================================================================

-- Flush privileges to ensure changes take effect immediately
FLUSH PRIVILEGES;

-- ============================================================================
-- STEP 5: Verify User Creation and Privileges
-- ============================================================================

-- Check that user exists
SELECT 
    User, 
    Host,
    account_locked,
    password_expired
FROM mysql.user 
WHERE User = 'cdc_user';

-- View all privileges granted to the CDC user
SHOW GRANTS FOR 'cdc_user'@'%';

-- Expected output should include:
-- GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%'
-- GRANT SELECT ON `retail_db`.* TO 'cdc_user'@'%'

-- ============================================================================
-- STEP 6: Test CDC User Privileges
-- ============================================================================

-- Connect to MySQL with the CDC user credentials:
-- mysql -h your-mysql-host.com -u cdc_user -p
-- Enter password when prompted

-- Once connected as cdc_user, test privileges:

-- Test 1: Verify can view master status
SHOW MASTER STATUS;
-- Should return current binary log file and position

-- Test 2: Verify can list binary logs
SHOW BINARY LOGS;
-- Should list all binary log files

-- Test 3: Verify can view binary log events
-- Replace 'mysql-bin.000001' with your current binary log file
SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 5;
-- Should display binary log events

-- Test 4: Verify can read from retail_db (once created)
-- USE retail_db;
-- SHOW TABLES;
-- SELECT COUNT(*) FROM customers;

-- Test 5: Verify CANNOT write to tables (should fail)
-- This should fail with "INSERT command denied"
-- INSERT INTO retail_db.customers (first_name, last_name, email) VALUES ('Test', 'User', 'test@email.com');

-- Test 6: Verify CANNOT create databases (should fail)
-- This should fail with "CREATE command denied"
-- CREATE DATABASE test_db;

-- ============================================================================
-- STEP 7: Security Best Practices (Optional but Recommended)
-- ============================================================================

-- Set password expiration policy for CDC user
ALTER USER 'cdc_user'@'%' PASSWORD EXPIRE NEVER;
-- Prevents password expiration that could break CDC pipeline

-- Or set specific expiration interval (e.g., 90 days)
-- ALTER USER 'cdc_user'@'%' PASSWORD EXPIRE INTERVAL 90 DAY;

-- Require SSL/TLS connection (if MySQL supports it)
-- ALTER USER 'cdc_user'@'%' REQUIRE SSL;

-- Set connection limit (optional, to prevent connection exhaustion)
-- ALTER USER 'cdc_user'@'%' WITH MAX_USER_CONNECTIONS 10;

-- ============================================================================
-- STEP 8: Additional Users for Multiple Environments (Optional)
-- ============================================================================

-- Create separate CDC users for different environments

-- Development environment
CREATE USER 'cdc_user_dev'@'%' IDENTIFIED BY 'DevPassword123!';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user_dev'@'%';
GRANT SELECT ON retail_db_dev.* TO 'cdc_user_dev'@'%';

-- Staging environment
CREATE USER 'cdc_user_staging'@'%' IDENTIFIED BY 'StagingPassword123!';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user_staging'@'%';
GRANT SELECT ON retail_db_staging.* TO 'cdc_user_staging'@'%';

-- Production environment (most restrictive)
CREATE USER 'cdc_user_prod'@'10.0.1.0/255.255.255.0' IDENTIFIED BY 'ProdPassword123!';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user_prod'@'10.0.1.0/255.255.255.0';
GRANT SELECT ON retail_db.* TO 'cdc_user_prod'@'10.0.1.0/255.255.255.0';

FLUSH PRIVILEGES;

-- ============================================================================
-- STEP 9: Audit and Monitoring
-- ============================================================================

-- View all replication users
SELECT 
    User,
    Host,
    Select_priv,
    Repl_slave_priv,
    Repl_client_priv
FROM mysql.user
WHERE Repl_slave_priv = 'Y' OR Repl_client_priv = 'Y';

-- Monitor CDC user connections
SELECT 
    USER,
    HOST,
    DB,
    COMMAND,
    TIME,
    STATE
FROM information_schema.PROCESSLIST
WHERE USER = 'cdc_user';

-- ============================================================================
-- STEP 10: Cleanup / User Management
-- ============================================================================

-- If you need to revoke privileges (DO NOT run unless you intend to remove access)
/*
REVOKE REPLICATION SLAVE, REPLICATION CLIENT ON *.* FROM 'cdc_user'@'%';
REVOKE SELECT ON retail_db.* FROM 'cdc_user'@'%';
FLUSH PRIVILEGES;
*/

-- If you need to drop the user (DO NOT run unless you intend to remove user)
/*
DROP USER 'cdc_user'@'%';
FLUSH PRIVILEGES;
*/

-- If you need to change the password
/*
ALTER USER 'cdc_user'@'%' IDENTIFIED BY 'NewSecurePassword456!';
FLUSH PRIVILEGES;
*/

-- ============================================================================
-- Notes and Best Practices
-- ============================================================================

/*
1. Password Security:
   - Use strong passwords (minimum 12 characters, mixed case, numbers, symbols)
   - Store passwords in Databricks Secrets, never in code or notebooks
   - Rotate passwords periodically (e.g., every 90 days)

2. Host Restrictions:
   - In production, restrict to specific IP addresses or hostnames
   - Avoid using '%' wildcard for production environments
   - Use CIDR notation for subnets: '10.0.1.0/255.255.255.0'

3. Privilege Principle:
   - CDC user should have ONLY read (SELECT) and replication privileges
   - NEVER grant INSERT, UPDATE, DELETE, or DDL privileges
   - NEVER grant SUPER or administrative privileges

4. Multiple Environments:
   - Use separate users for dev, staging, and production
   - Allows independent credential rotation
   - Enables environment-specific access control

5. SSL/TLS:
   - Require SSL connections in production for encrypted data transfer
   - Configure MySQL to support SSL before requiring it
   - Ensure Databricks can connect via SSL

6. Monitoring:
   - Regularly audit user privileges: SHOW GRANTS FOR 'cdc_user'@'%';
   - Monitor connection attempts and failures in MySQL logs
   - Track CDC user activity in information_schema.PROCESSLIST

7. Documentation:
   - Document which user is used for each environment
   - Keep password rotation schedule documented
   - Maintain runbook for credential issues
*/

-- ============================================================================
-- Troubleshooting
-- ============================================================================

/*
Issue: "Access denied for user 'cdc_user'@'host'"
Solution: Verify user exists, host pattern matches, password is correct

Issue: "SHOW MASTER STATUS command denied"
Solution: Grant REPLICATION CLIENT privilege

Issue: "Binary log read error"
Solution: Grant REPLICATION SLAVE privilege

Issue: "SELECT command denied on table retail_db.customers"
Solution: Grant SELECT ON retail_db.* or specific tables

Issue: Connection from Databricks fails
Solution: Check MySQL firewall rules, security groups, host pattern in user definition
*/

-- ============================================================================
-- End of CDC User Creation Script
-- ============================================================================

-- Final verification query
SELECT 
    'CDC User Configuration Complete' as status,
    User as username,
    Host as allowed_hosts,
    'Use SHOW GRANTS FOR ''cdc_user''@''%'' to verify privileges' as verification_command
FROM mysql.user 
WHERE User = 'cdc_user';
