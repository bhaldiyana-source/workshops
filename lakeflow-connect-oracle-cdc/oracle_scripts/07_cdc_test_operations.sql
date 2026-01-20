-- ===================================================================
-- Script: 07_cdc_test_operations.sql
-- Purpose: Generate test CDC operations to validate pipeline
-- Requirements: Sample data loaded (run 06_insert_sample_data.sql first)
-- Usage: Run sections individually to test different CDC scenarios
-- ===================================================================

-- Connect as RETAIL user or DBA
-- sqlplus RETAIL/password@RETAILDB

SET SERVEROUTPUT ON
SET FEEDBACK ON

PROMPT
PROMPT ===================================================================
PROMPT CDC TEST OPERATIONS SCRIPT
PROMPT ===================================================================
PROMPT
PROMPT This script provides test operations to validate your CDC pipeline.
PROMPT Run each section separately and verify changes appear in Databricks.
PROMPT
PROMPT IMPORTANT: Record current SCN and timestamp before each test!
PROMPT ===================================================================

-- Get current SCN for reference
SELECT 
    CURRENT_SCN AS "Current SCN",
    TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF') AS "Timestamp"
FROM V$DATABASE;

-- ===================================================================
-- TEST 1: Single INSERT Operation
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 1: Single INSERT - Add new customer
PROMPT ===================================================================

-- Record timestamp
SELECT 'Test 1 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

-- Insert new customer
INSERT INTO RETAIL.CUSTOMERS (
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    ADDRESS,
    CITY,
    STATE,
    ZIP_CODE,
    CREATED_DATE,
    LAST_UPDATED
) VALUES (
    10001,
    'CDCTest',
    'InsertUser',
    'cdc.test.insert@email.com',
    '555-9001',
    '10001 CDC Test Avenue',
    'Seattle',
    'WA',
    '98101',
    SYSTIMESTAMP,
    SYSTIMESTAMP
);

COMMIT;

PROMPT ✓ INSERT committed. Customer ID: 10001
PROMPT
PROMPT Expected in Databricks bronze.customers:
PROMPT   - New row with CUSTOMER_ID = 10001
PROMPT   - _change_type metadata (if captured)
PROMPT
PROMPT Verification query:
PROMPT   SELECT * FROM bronze.customers WHERE CUSTOMER_ID = 10001;

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 2: Single UPDATE Operation
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 2: Single UPDATE - Modify customer email
PROMPT ===================================================================

SELECT 'Test 2 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

-- Show before value
SELECT 
    CUSTOMER_ID,
    EMAIL AS "Email Before Update",
    LAST_UPDATED AS "Last Updated Before"
FROM RETAIL.CUSTOMERS
WHERE CUSTOMER_ID = 10001;

-- Update customer
UPDATE RETAIL.CUSTOMERS
SET EMAIL = 'cdc.test.updated@email.com',
    PHONE = '555-9999',
    LAST_UPDATED = SYSTIMESTAMP
WHERE CUSTOMER_ID = 10001;

COMMIT;

PROMPT ✓ UPDATE committed. Customer ID: 10001
PROMPT
PROMPT Expected in Databricks bronze.customers:
PROMPT   - Same row with updated EMAIL and PHONE
PROMPT   - Updated LAST_UPDATED timestamp
PROMPT   - _commit_timestamp reflects this change
PROMPT
PROMPT Verification query:
PROMPT   SELECT * FROM bronze.customers WHERE CUSTOMER_ID = 10001;

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 3: Single DELETE Operation
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 3: Single DELETE - Remove test customer
PROMPT ===================================================================

SELECT 'Test 3 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

-- Delete customer
DELETE FROM RETAIL.CUSTOMERS
WHERE CUSTOMER_ID = 10001;

COMMIT;

PROMPT ✓ DELETE committed. Customer ID: 10001
PROMPT
PROMPT Expected in Databricks bronze.customers:
PROMPT   - Row with CUSTOMER_ID = 10001 should be removed
PROMPT   - Row count should decrease by 1
PROMPT
PROMPT Verification query:
PROMPT   SELECT * FROM bronze.customers WHERE CUSTOMER_ID = 10001;
PROMPT   -- Expected: 0 rows

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 4: Bulk INSERT Operations
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 4: Bulk INSERT - Add 10 new customers
PROMPT ===================================================================

SELECT 'Test 4 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

BEGIN
    FOR i IN 1..10 LOOP
        INSERT INTO RETAIL.CUSTOMERS (
            CUSTOMER_ID,
            FIRST_NAME,
            LAST_NAME,
            EMAIL,
            PHONE,
            CITY,
            STATE,
            CREATED_DATE,
            LAST_UPDATED
        ) VALUES (
            20000 + i,
            'BulkTest' || i,
            'User',
            'bulk.test' || i || '@email.com',
            '555-2' || LPAD(i, 3, '0'),
            'Portland',
            'OR',
            SYSTIMESTAMP,
            SYSTIMESTAMP
        );
    END LOOP;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('✓ 10 customers inserted (IDs 20001-20010)');
END;
/

PROMPT
PROMPT Expected in Databricks bronze.customers:
PROMPT   - 10 new rows with CUSTOMER_IDs 20001-20010
PROMPT   - All rows should have same _commit_timestamp (same transaction)
PROMPT
PROMPT Verification query:
PROMPT   SELECT * FROM bronze.customers 
PROMPT   WHERE CUSTOMER_ID BETWEEN 20001 AND 20010
PROMPT   ORDER BY CUSTOMER_ID;

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 5: Bulk UPDATE Operations
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 5: Bulk UPDATE - Update state for multiple customers
PROMPT ===================================================================

SELECT 'Test 5 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

-- Update multiple customers in single transaction
UPDATE RETAIL.CUSTOMERS
SET STATE = 'TX',
    CITY = 'Austin',
    LAST_UPDATED = SYSTIMESTAMP
WHERE CUSTOMER_ID BETWEEN 20001 AND 20005;

COMMIT;

SELECT 'Rows updated: ' || SQL%ROWCOUNT FROM DUAL;

PROMPT
PROMPT Expected in Databricks bronze.customers:
PROMPT   - 5 rows updated (IDs 20001-20005)
PROMPT   - STATE changed to 'TX', CITY to 'Austin'
PROMPT   - Same _commit_timestamp for all 5 changes
PROMPT
PROMPT Verification query:
PROMPT   SELECT CUSTOMER_ID, CITY, STATE, _commit_timestamp
PROMPT   FROM bronze.customers 
PROMPT   WHERE CUSTOMER_ID BETWEEN 20001 AND 20005;

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 6: Mixed Operations in Single Transaction
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 6: Mixed Operations - INSERT, UPDATE, DELETE in one transaction
PROMPT ===================================================================

SELECT 'Test 6 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

BEGIN
    -- INSERT new customer
    INSERT INTO RETAIL.CUSTOMERS (
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE
    ) VALUES (
        30001, 'Mixed', 'TestInsert', 'mixed.insert@email.com', 'Miami', 'FL'
    );
    
    -- UPDATE existing customer
    UPDATE RETAIL.CUSTOMERS
    SET PHONE = '555-3001'
    WHERE CUSTOMER_ID = 20006;
    
    -- DELETE test customer
    DELETE FROM RETAIL.CUSTOMERS
    WHERE CUSTOMER_ID = 20010;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('✓ Mixed operations committed (INSERT/UPDATE/DELETE)');
END;
/

PROMPT
PROMPT Expected in Databricks bronze.customers:
PROMPT   - New row: CUSTOMER_ID = 30001
PROMPT   - Updated row: CUSTOMER_ID = 20006 with new phone
PROMPT   - Deleted row: CUSTOMER_ID = 20010 should be removed
PROMPT   - All 3 operations share same _commit_timestamp
PROMPT
PROMPT Verification queries:
PROMPT   SELECT * FROM bronze.customers WHERE CUSTOMER_ID = 30001; -- Should exist
PROMPT   SELECT * FROM bronze.customers WHERE CUSTOMER_ID = 20006; -- Phone updated
PROMPT   SELECT * FROM bronze.customers WHERE CUSTOMER_ID = 20010; -- Should NOT exist

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 7: Foreign Key CASCADE DELETE
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 7: CASCADE DELETE - Delete customer with orders
PROMPT ===================================================================

SELECT 'Test 7 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

-- Create customer and orders for cascade test
BEGIN
    -- Insert parent customer
    INSERT INTO RETAIL.CUSTOMERS (
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE
    ) VALUES (
        40001, 'Cascade', 'TestParent', 'cascade.test@email.com', 'Boston', 'MA'
    );
    
    -- Insert child orders
    FOR i IN 1..3 LOOP
        INSERT INTO RETAIL.ORDERS (
            ORDER_ID, CUSTOMER_ID, ORDER_STATUS, TOTAL_AMOUNT, PAYMENT_METHOD
        ) VALUES (
            90000 + i, 40001, 'PENDING', 100 * i, 'Credit Card'
        );
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('✓ Customer 40001 created with 3 orders');
END;
/

-- Wait a moment for CDC to process
PROMPT Waiting 10 seconds for CDC to capture initial data...
EXEC DBMS_LOCK.SLEEP(10);

-- Now delete parent (CASCADE will delete children)
DELETE FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = 40001;
COMMIT;

PROMPT ✓ Customer 40001 deleted (CASCADE DELETE triggered)
PROMPT
PROMPT Expected in Databricks:
PROMPT   bronze.customers: Customer 40001 removed
PROMPT   bronze.orders: Orders 90001, 90002, 90003 also removed
PROMPT
PROMPT Verification queries:
PROMPT   SELECT * FROM bronze.customers WHERE CUSTOMER_ID = 40001;  -- 0 rows
PROMPT   SELECT * FROM bronze.orders WHERE ORDER_ID BETWEEN 90001 AND 90003;  -- 0 rows

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 8: Rapid Sequential Transactions
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 8: Rapid Sequential - Multiple transactions in quick succession
PROMPT ===================================================================

SELECT 'Test 8 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

BEGIN
    FOR i IN 1..5 LOOP
        INSERT INTO RETAIL.CUSTOMERS (
            CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE
        ) VALUES (
            50000 + i, 'Rapid' || i, 'Test', 'rapid' || i || '@email.com', 'Denver', 'CO'
        );
        COMMIT;  -- Separate commit for each insert
        
        DBMS_OUTPUT.PUT_LINE('Transaction ' || i || ' committed');
    END LOOP;
END;
/

PROMPT ✓ 5 rapid sequential transactions committed
PROMPT
PROMPT Expected in Databricks bronze.customers:
PROMPT   - 5 new rows (IDs 50001-50005)
PROMPT   - Each row has different _commit_timestamp (different SCNs)
PROMPT   - Tests CDC latency for rapid changes
PROMPT
PROMPT Verification query:
PROMPT   SELECT CUSTOMER_ID, _commit_timestamp 
PROMPT   FROM bronze.customers 
PROMPT   WHERE CUSTOMER_ID BETWEEN 50001 AND 50005
PROMPT   ORDER BY CUSTOMER_ID;

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 9: Large Text Field Update (CLOB)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 9: Large Field - Update product description (CLOB)
PROMPT ===================================================================

SELECT 'Test 9 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

UPDATE RETAIL.PRODUCTS
SET DESCRIPTION = DESCRIPTION || ' -- UPDATED: This is a much longer description to test CLOB handling in CDC. ' ||
                  'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ' ||
                  'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.',
    LAST_UPDATED = SYSTIMESTAMP
WHERE PRODUCT_ID = 1;

COMMIT;

PROMPT ✓ Product 1 description updated (CLOB field)
PROMPT
PROMPT Expected in Databricks bronze.products:
PROMPT   - Product 1 has updated DESCRIPTION field
PROMPT   - Tests CDC handling of large text fields
PROMPT
PROMPT Verification query:
PROMPT   SELECT PRODUCT_ID, SUBSTRING(DESCRIPTION, 1, 100), LAST_UPDATED
PROMPT   FROM bronze.products WHERE PRODUCT_ID = 1;

PAUSE Press Enter to continue...

-- ===================================================================
-- TEST 10: Status Transition (Realistic Business Workflow)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT TEST 10: Workflow - Simulate order lifecycle
PROMPT ===================================================================

SELECT 'Test 10 Start Time: ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') FROM DUAL;

-- Create new order
INSERT INTO RETAIL.ORDERS (
    ORDER_ID, CUSTOMER_ID, ORDER_STATUS, TOTAL_AMOUNT, PAYMENT_METHOD
) VALUES (
    99001, 1, 'PENDING', 199.99, 'Credit Card'
);
COMMIT;
PROMPT ✓ Order 99001 created (PENDING)

-- Simulate order processing workflow
EXEC DBMS_LOCK.SLEEP(2);
UPDATE RETAIL.ORDERS SET ORDER_STATUS = 'PROCESSING', LAST_UPDATED = SYSTIMESTAMP WHERE ORDER_ID = 99001;
COMMIT;
PROMPT ✓ Order 99001 → PROCESSING

EXEC DBMS_LOCK.SLEEP(2);
UPDATE RETAIL.ORDERS SET ORDER_STATUS = 'SHIPPED', LAST_UPDATED = SYSTIMESTAMP WHERE ORDER_ID = 99001;
COMMIT;
PROMPT ✓ Order 99001 → SHIPPED

EXEC DBMS_LOCK.SLEEP(2);
UPDATE RETAIL.ORDERS SET ORDER_STATUS = 'DELIVERED', LAST_UPDATED = SYSTIMESTAMP WHERE ORDER_ID = 99001;
COMMIT;
PROMPT ✓ Order 99001 → DELIVERED

PROMPT
PROMPT Expected in Databricks bronze.orders:
PROMPT   - Order 99001 with STATUS = 'DELIVERED'
PROMPT   - Multiple _commit_timestamp entries showing state transitions
PROMPT   - Tests realistic business workflow CDC capture
PROMPT
PROMPT Verification query:
PROMPT   SELECT ORDER_ID, ORDER_STATUS, LAST_UPDATED, _commit_timestamp
PROMPT   FROM bronze.orders WHERE ORDER_ID = 99001
PROMPT   ORDER BY _commit_timestamp DESC LIMIT 1;

-- ===================================================================
-- TEST SUMMARY
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT CDC TEST OPERATIONS SUMMARY
PROMPT ===================================================================
PROMPT
PROMPT Tests Completed:
PROMPT   ✓ Test 1: Single INSERT
PROMPT   ✓ Test 2: Single UPDATE
PROMPT   ✓ Test 3: Single DELETE
PROMPT   ✓ Test 4: Bulk INSERT (10 rows)
PROMPT   ✓ Test 5: Bulk UPDATE (5 rows)
PROMPT   ✓ Test 6: Mixed operations (INSERT/UPDATE/DELETE)
PROMPT   ✓ Test 7: Foreign key CASCADE DELETE
PROMPT   ✓ Test 8: Rapid sequential transactions
PROMPT   ✓ Test 9: Large text field update (CLOB)
PROMPT   ✓ Test 10: Order lifecycle workflow
PROMPT
PROMPT Total Operations:
PROMPT   - ~30 INSERTs across CUSTOMERS and ORDERS
PROMPT   - ~10 UPDATEs
PROMPT   - ~5 DELETEs
PROMPT
PROMPT Validation Checklist:
PROMPT   [ ] All INSERT operations appear in bronze tables
PROMPT   [ ] All UPDATE operations reflected correctly
PROMPT   [ ] All DELETE operations removed rows
PROMPT   [ ] CASCADE DELETE propagated to child tables
PROMPT   [ ] Transaction timestamps are consistent
PROMPT   [ ] No duplicate primary keys exist
PROMPT   [ ] CDC latency is acceptable (< 60 seconds typical)
PROMPT
PROMPT Next Steps:
PROMPT   1. Run validation queries in Databricks
PROMPT   2. Check CDC latency metrics
PROMPT   3. Verify data lineage in Unity Catalog
PROMPT   4. Monitor pipeline performance
PROMPT   5. Proceed to production deployment
PROMPT ===================================================================

EXIT;
