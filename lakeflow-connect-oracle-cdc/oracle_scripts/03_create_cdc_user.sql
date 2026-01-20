-- ===================================================================
-- Script: 03_create_cdc_user.sql
-- Purpose: Create dedicated CDC user with LogMiner privileges
-- Requirements: DBA or SYSDBA privileges
-- Security: Uses principle of least privilege
-- ===================================================================

-- Connect as DBA user
-- sqlplus system/password@RETAILDB

-- ===================================================================
-- STEP 1: Create CDC user
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Creating CDC user for LogMiner access...
PROMPT ===================================================================

-- Create user with strong password
-- IMPORTANT: Change this password before using in production!
CREATE USER CDC_USER IDENTIFIED BY "Change_This_Password_123!"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    ACCOUNT UNLOCK;

PROMPT CDC_USER created.

-- ===================================================================
-- STEP 2: Grant basic connection privilege
-- ===================================================================
PROMPT
PROMPT Granting basic connection privileges...

GRANT CREATE SESSION TO CDC_USER;

PROMPT Connection privilege granted.

-- ===================================================================
-- STEP 3: Grant LogMiner required privileges
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Granting LogMiner-specific privileges...
PROMPT ===================================================================

-- Required for reading transaction information
GRANT SELECT ANY TRANSACTION TO CDC_USER;

-- Required for executing LogMiner procedures
GRANT EXECUTE ON DBMS_LOGMNR TO CDC_USER;
GRANT EXECUTE ON DBMS_LOGMNR_D TO CDC_USER;

PROMPT LogMiner execution privileges granted.

-- ===================================================================
-- STEP 4: Grant access to required V$ views
-- ===================================================================
PROMPT
PROMPT Granting access to V$ system views...

-- Database info
GRANT SELECT ON V_$DATABASE TO CDC_USER;

-- Archive log information
GRANT SELECT ON V_$ARCHIVED_LOG TO CDC_USER;

-- Online redo log information
GRANT SELECT ON V_$LOG TO CDC_USER;
GRANT SELECT ON V_$LOGFILE TO CDC_USER;

-- LogMiner contents and logs
GRANT SELECT ON V_$LOGMNR_CONTENTS TO CDC_USER;
GRANT SELECT ON V_$LOGMNR_LOGS TO CDC_USER;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO CDC_USER;

PROMPT System view access granted.

-- ===================================================================
-- STEP 5: Grant SELECT access to RETAIL schema tables
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Granting SELECT access to RETAIL schema tables...
PROMPT ===================================================================

-- Grant SELECT on CUSTOMERS table
GRANT SELECT ON RETAIL.CUSTOMERS TO CDC_USER;
PROMPT SELECT granted on RETAIL.CUSTOMERS

-- Grant SELECT on ORDERS table
GRANT SELECT ON RETAIL.ORDERS TO CDC_USER;
PROMPT SELECT granted on RETAIL.ORDERS

-- Grant SELECT on PRODUCTS table
GRANT SELECT ON RETAIL.PRODUCTS TO CDC_USER;
PROMPT SELECT granted on RETAIL.PRODUCTS

-- ===================================================================
-- STEP 6: (Optional) Create synonyms for easier access
-- ===================================================================
PROMPT
PROMPT Creating synonyms for CDC_USER convenience...

-- Connect as CDC_USER to create synonyms
-- Note: This section would need separate connection
-- Uncomment and run as CDC_USER if desired:

-- CREATE SYNONYM CUSTOMERS FOR RETAIL.CUSTOMERS;
-- CREATE SYNONYM ORDERS FOR RETAIL.ORDERS;
-- CREATE SYNONYM PRODUCTS FOR RETAIL.PRODUCTS;

-- ===================================================================
-- STEP 7: Verify CDC_USER privileges
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Verifying CDC_USER privileges...
PROMPT ===================================================================

-- Check system privileges
SELECT PRIVILEGE 
FROM DBA_SYS_PRIVS 
WHERE GRANTEE = 'CDC_USER'
ORDER BY PRIVILEGE;

PROMPT
PROMPT System privileges shown above.

-- Check object privileges
SELECT 
    OWNER,
    TABLE_NAME,
    PRIVILEGE
FROM DBA_TAB_PRIVS
WHERE GRANTEE = 'CDC_USER'
ORDER BY OWNER, TABLE_NAME, PRIVILEGE;

PROMPT
PROMPT Object privileges shown above.

-- ===================================================================
-- STEP 8: Test CDC_USER access (as CDC_USER)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Testing CDC_USER access...
PROMPT ===================================================================
PROMPT
PROMPT To test CDC_USER, connect as CDC_USER and run:
PROMPT   sqlplus CDC_USER/your_password@RETAILDB
PROMPT   @04_verify_cdc_configuration.sql
PROMPT
PROMPT Or run the following test queries:

-- Sample test queries (run as CDC_USER)
PROMPT
PROMPT -- Test 1: Can query RETAIL tables?
PROMPT SELECT COUNT(*) FROM RETAIL.CUSTOMERS;
PROMPT
PROMPT -- Test 2: Can access V$DATABASE?
PROMPT SELECT CURRENT_SCN, LOG_MODE FROM V$DATABASE;
PROMPT
PROMPT -- Test 3: Can access LogMiner views?
PROMPT SELECT * FROM V$LOGMNR_LOGS WHERE ROWNUM <= 1;
PROMPT
PROMPT -- Test 4: Can execute DBMS_LOGMNR?
PROMPT BEGIN
PROMPT   DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);
PROMPT   DBMS_LOGMNR.END_LOGMNR;
PROMPT END;
PROMPT /

-- ===================================================================
-- SECURITY BEST PRACTICES
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT SECURITY BEST PRACTICES
PROMPT ===================================================================
PROMPT
PROMPT 1. PASSWORD MANAGEMENT:
PROMPT    - Change default password immediately
PROMPT    - Use strong password (min 15 chars, mixed case, numbers, symbols)
PROMPT    - Store password in Oracle Wallet or Databricks Secrets
PROMPT    - Never commit passwords to version control
PROMPT
PROMPT 2. PRIVILEGE MANAGEMENT:
PROMPT    - CDC_USER has READ-ONLY access (no write privileges)
PROMPT    - No DBA or administrative privileges
PROMPT    - No access to sensitive system tables
PROMPT    - Limited to specific RETAIL schema tables
PROMPT
PROMPT 3. AUDITING:
PROMPT    - Enable audit trail for CDC_USER actions
PROMPT    - Monitor for unusual query patterns
PROMPT    - Review access logs regularly
PROMPT
PROMPT 4. NETWORK SECURITY:
PROMPT    - Use encrypted connections (SSL/TLS)
PROMPT    - Restrict connections to known Databricks IPs
PROMPT    - Implement VPN or PrivateLink where possible
PROMPT
PROMPT 5. PASSWORD ROTATION:
PROMPT    - Rotate CDC_USER password every 90 days
PROMPT    - Update Databricks Unity Catalog connection after rotation
PROMPT ===================================================================

-- ===================================================================
-- PASSWORD CHANGE TEMPLATE
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT To change CDC_USER password (run as DBA):
PROMPT ===================================================================
PROMPT
PROMPT ALTER USER CDC_USER IDENTIFIED BY "New_Secure_Password_456!";
PROMPT
PROMPT Then update Unity Catalog connection in Databricks.
PROMPT ===================================================================

-- ===================================================================
-- CLEANUP (if needed)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT To remove CDC_USER (if needed):
PROMPT ===================================================================
PROMPT
PROMPT -- WARNING: This will remove the user and all privileges!
PROMPT -- DROP USER CDC_USER CASCADE;
PROMPT ===================================================================

-- ===================================================================
-- COMPLETION SUMMARY
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CDC USER CREATION COMPLETE
PROMPT ===================================================================
PROMPT
PROMPT User Created: CDC_USER
PROMPT Default Password: Change_This_Password_123! (CHANGE IMMEDIATELY!)
PROMPT
PROMPT Privileges Granted:
PROMPT   ✓ CREATE SESSION (connection)
PROMPT   ✓ SELECT ANY TRANSACTION (LogMiner)
PROMPT   ✓ EXECUTE ON DBMS_LOGMNR (LogMiner)
PROMPT   ✓ EXECUTE ON DBMS_LOGMNR_D (LogMiner)
PROMPT   ✓ SELECT ON V$ views (system metadata)
PROMPT   ✓ SELECT ON RETAIL.CUSTOMERS
PROMPT   ✓ SELECT ON RETAIL.ORDERS
PROMPT   ✓ SELECT ON RETAIL.PRODUCTS
PROMPT
PROMPT Next Steps:
PROMPT   1. CHANGE THE DEFAULT PASSWORD immediately!
PROMPT   2. Test CDC_USER access (run 04_verify_cdc_configuration.sql)
PROMPT   3. Store credentials in Databricks Secrets
PROMPT   4. Create Unity Catalog connection in Databricks
PROMPT   5. Proceed with Lakeflow Connect gateway setup
PROMPT
PROMPT IMPORTANT SECURITY REMINDER:
PROMPT   - Never use default password in production
PROMPT   - Store password securely (Databricks Secrets, Oracle Wallet)
PROMPT   - Implement regular password rotation policy
PROMPT ===================================================================

EXIT;
