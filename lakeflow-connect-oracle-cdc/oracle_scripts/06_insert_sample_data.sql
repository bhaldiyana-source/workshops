-- ===================================================================
-- Script: 06_insert_sample_data.sql
-- Purpose: Load sample data into RETAIL schema for CDC testing
-- Requirements: RETAIL schema must exist (run 05_create_schema.sql first)
-- Downtime: No
-- ===================================================================

-- Connect as RETAIL user or DBA
-- sqlplus RETAIL/password@RETAILDB

SET SERVEROUTPUT ON
SET FEEDBACK ON

PROMPT
PROMPT ===================================================================
PROMPT Loading sample data into RETAIL schema...
PROMPT ===================================================================

-- ===================================================================
-- STEP 1: Insert sample customers (100 records)
-- ===================================================================
PROMPT
PROMPT Inserting 100 sample customers...

DECLARE
    v_states VARCHAR2(100) := 'CA,WA,OR,TX,NY,FL,IL,PA,OH,GA';
    TYPE t_state_array IS TABLE OF VARCHAR2(2) INDEX BY PLS_INTEGER;
    v_state_list t_state_array;
    TYPE t_city_array IS TABLE OF VARCHAR2(50) INDEX BY PLS_INTEGER;
    v_city_list t_city_array;
    v_idx PLS_INTEGER;
BEGIN
    -- Initialize state list
    v_state_list(1) := 'CA'; v_city_list(1) := 'Los Angeles';
    v_state_list(2) := 'WA'; v_city_list(2) := 'Seattle';
    v_state_list(3) := 'OR'; v_city_list(3) := 'Portland';
    v_state_list(4) := 'TX'; v_city_list(4) := 'Austin';
    v_state_list(5) := 'NY'; v_city_list(5) := 'New York';
    v_state_list(6) := 'FL'; v_city_list(6) := 'Miami';
    v_state_list(7) := 'IL'; v_city_list(7) := 'Chicago';
    v_state_list(8) := 'PA'; v_city_list(8) := 'Philadelphia';
    v_state_list(9) := 'OH'; v_city_list(9) := 'Columbus';
    v_state_list(10) := 'GA'; v_city_list(10) := 'Atlanta';
    
    FOR i IN 1..100 LOOP
        v_idx := MOD(i, 10) + 1;
        
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
            i,
            'FirstName' || i,
            'LastName' || i,
            'customer' || i || '@email.com',
            '555-' || LPAD(i, 4, '0'),
            i || ' Main Street, Apt ' || MOD(i, 20),
            v_city_list(v_idx),
            v_state_list(v_idx),
            LPAD(MOD(i * 100, 99999), 5, '0'),
            SYSTIMESTAMP - DBMS_RANDOM.VALUE(0, 365),  -- Random creation within last year
            SYSTIMESTAMP
        );
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('✓ 100 customers inserted');
END;
/

-- ===================================================================
-- STEP 2: Insert sample products (50 records)
-- ===================================================================
PROMPT
PROMPT Inserting 50 sample products...

DECLARE
    TYPE t_cat_array IS TABLE OF VARCHAR2(50) INDEX BY PLS_INTEGER;
    v_cat_list t_cat_array;
    TYPE t_supplier_array IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
    v_supplier_list t_supplier_array;
    v_idx PLS_INTEGER;
BEGIN
    -- Initialize categories and suppliers
    v_cat_list(1) := 'Electronics'; v_supplier_list(1) := 'TechSupply Inc';
    v_cat_list(2) := 'Clothing'; v_supplier_list(2) := 'Fashion Wholesale';
    v_cat_list(3) := 'Books'; v_supplier_list(3) := 'Book Distributors LLC';
    v_cat_list(4) := 'Home & Garden'; v_supplier_list(4) := 'HomeGoods Co';
    v_cat_list(5) := 'Sports'; v_supplier_list(5) := 'Athletic Supply Corp';
    
    FOR i IN 1..50 LOOP
        v_idx := MOD(i, 5) + 1;
        
        INSERT INTO RETAIL.PRODUCTS (
            PRODUCT_ID,
            PRODUCT_NAME,
            CATEGORY,
            PRICE,
            STOCK_QUANTITY,
            SUPPLIER,
            DESCRIPTION,
            IS_ACTIVE,
            CREATED_DATE,
            LAST_UPDATED
        ) VALUES (
            i,
            v_cat_list(v_idx) || ' Product ' || i,
            v_cat_list(v_idx),
            ROUND(DBMS_RANDOM.VALUE(10, 500), 2),
            ROUND(DBMS_RANDOM.VALUE(0, 1000)),
            v_supplier_list(v_idx),
            'High-quality ' || v_cat_list(v_idx) || ' product. ' ||
            'Perfect for everyday use. Item code: PROD-' || LPAD(i, 5, '0'),
            CASE WHEN MOD(i, 10) = 0 THEN 'N' ELSE 'Y' END,  -- 10% inactive
            SYSTIMESTAMP - DBMS_RANDOM.VALUE(0, 180),  -- Random creation within last 6 months
            SYSTIMESTAMP
        );
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('✓ 50 products inserted');
END;
/

-- ===================================================================
-- STEP 3: Insert sample orders (200 records)
-- ===================================================================
PROMPT
PROMPT Inserting 200 sample orders...

DECLARE
    TYPE t_status_array IS TABLE OF VARCHAR2(20) INDEX BY PLS_INTEGER;
    v_status_list t_status_array;
    TYPE t_payment_array IS TABLE OF VARCHAR2(50) INDEX BY PLS_INTEGER;
    v_payment_list t_payment_array;
    v_customer_id NUMBER;
    v_order_date TIMESTAMP;
BEGIN
    -- Initialize order statuses and payment methods
    v_status_list(1) := 'PENDING';
    v_status_list(2) := 'PROCESSING';
    v_status_list(3) := 'SHIPPED';
    v_status_list(4) := 'DELIVERED';
    
    v_payment_list(1) := 'Credit Card';
    v_payment_list(2) := 'Debit Card';
    v_payment_list(3) := 'PayPal';
    v_payment_list(4) := 'Bank Transfer';
    v_payment_list(5) := 'Cash on Delivery';
    
    FOR i IN 1..200 LOOP
        v_customer_id := MOD(i, 100) + 1;  -- Distribute orders among customers
        v_order_date := SYSTIMESTAMP - DBMS_RANDOM.VALUE(0, 90);  -- Orders within last 90 days
        
        INSERT INTO RETAIL.ORDERS (
            ORDER_ID,
            CUSTOMER_ID,
            ORDER_DATE,
            ORDER_STATUS,
            TOTAL_AMOUNT,
            SHIPPING_ADDRESS,
            PAYMENT_METHOD,
            CREATED_DATE,
            LAST_UPDATED
        ) VALUES (
            i,
            v_customer_id,
            v_order_date,
            v_status_list(MOD(i, 4) + 1),
            ROUND(DBMS_RANDOM.VALUE(25, 750), 2),
            (SELECT ADDRESS || ', ' || CITY || ', ' || STATE || ' ' || ZIP_CODE 
             FROM RETAIL.CUSTOMERS WHERE CUSTOMER_ID = v_customer_id),
            v_payment_list(MOD(i, 5) + 1),
            v_order_date,
            v_order_date + DBMS_RANDOM.VALUE(0, 5)  -- Last updated within 5 days of creation
        );
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('✓ 200 orders inserted');
END;
/

-- ===================================================================
-- STEP 4: Verify data load
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Verifying data load...
PROMPT ===================================================================

-- Count records
SELECT 
    'CUSTOMERS' AS "Table",
    COUNT(*) AS "Row Count",
    MIN(CREATED_DATE) AS "Earliest Record",
    MAX(CREATED_DATE) AS "Latest Record"
FROM RETAIL.CUSTOMERS
UNION ALL
SELECT 
    'PRODUCTS',
    COUNT(*),
    MIN(CREATED_DATE),
    MAX(CREATED_DATE)
FROM RETAIL.PRODUCTS
UNION ALL
SELECT 
    'ORDERS',
    COUNT(*),
    MIN(CREATED_DATE),
    MAX(CREATED_DATE)
FROM RETAIL.ORDERS;

-- ===================================================================
-- STEP 5: Sample data preview
-- ===================================================================
PROMPT
PROMPT Sample Customers:

SELECT 
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    CITY,
    STATE
FROM RETAIL.CUSTOMERS
WHERE ROWNUM <= 5
ORDER BY CUSTOMER_ID;

PROMPT
PROMPT Sample Products:

SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    PRICE,
    STOCK_QUANTITY,
    IS_ACTIVE
FROM RETAIL.PRODUCTS
WHERE ROWNUM <= 5
ORDER BY PRODUCT_ID;

PROMPT
PROMPT Sample Orders:

SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_STATUS,
    TOTAL_AMOUNT,
    PAYMENT_METHOD
FROM RETAIL.ORDERS
WHERE ROWNUM <= 5
ORDER BY ORDER_ID;

-- ===================================================================
-- STEP 6: Business analytics queries
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Business Analytics Summary
PROMPT ===================================================================

PROMPT
PROMPT Customers by State:

SELECT 
    STATE,
    COUNT(*) AS "Customer Count"
FROM RETAIL.CUSTOMERS
GROUP BY STATE
ORDER BY COUNT(*) DESC;

PROMPT
PROMPT Products by Category:

SELECT 
    CATEGORY,
    COUNT(*) AS "Product Count",
    ROUND(AVG(PRICE), 2) AS "Avg Price",
    SUM(STOCK_QUANTITY) AS "Total Stock"
FROM RETAIL.PRODUCTS
GROUP BY CATEGORY
ORDER BY COUNT(*) DESC;

PROMPT
PROMPT Orders by Status:

SELECT 
    ORDER_STATUS,
    COUNT(*) AS "Order Count",
    ROUND(SUM(TOTAL_AMOUNT), 2) AS "Total Revenue",
    ROUND(AVG(TOTAL_AMOUNT), 2) AS "Avg Order Value"
FROM RETAIL.ORDERS
GROUP BY ORDER_STATUS
ORDER BY COUNT(*) DESC;

PROMPT
PROMPT Top 10 Customers by Order Count:

SELECT 
    c.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME AS "Customer Name",
    c.EMAIL,
    COUNT(o.ORDER_ID) AS "Order Count",
    ROUND(SUM(o.TOTAL_AMOUNT), 2) AS "Total Spent"
FROM RETAIL.CUSTOMERS c
INNER JOIN RETAIL.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.EMAIL
ORDER BY COUNT(o.ORDER_ID) DESC
FETCH FIRST 10 ROWS ONLY;

-- ===================================================================
-- STEP 7: Data quality checks
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Data Quality Checks
PROMPT ===================================================================

PROMPT
PROMPT Checking for NULL values in required fields:

SELECT 
    'CUSTOMERS - NULL FIRST_NAME' AS "Check",
    COUNT(*) AS "Issue Count"
FROM RETAIL.CUSTOMERS
WHERE FIRST_NAME IS NULL
UNION ALL
SELECT 
    'CUSTOMERS - NULL EMAIL',
    COUNT(*)
FROM RETAIL.CUSTOMERS
WHERE EMAIL IS NULL
UNION ALL
SELECT 
    'ORDERS - NULL CUSTOMER_ID',
    COUNT(*)
FROM RETAIL.ORDERS
WHERE CUSTOMER_ID IS NULL
UNION ALL
SELECT 
    'PRODUCTS - NULL PRICE',
    COUNT(*)
FROM RETAIL.PRODUCTS
WHERE PRICE IS NULL;

PROMPT
PROMPT Checking referential integrity:

SELECT 
    'Orders with invalid customer references' AS "Check",
    COUNT(*) AS "Issue Count"
FROM RETAIL.ORDERS o
LEFT JOIN RETAIL.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL;

-- ===================================================================
-- STEP 8: Gather fresh statistics
-- ===================================================================
PROMPT
PROMPT Gathering table statistics...

BEGIN
    DBMS_STATS.GATHER_TABLE_STATS('RETAIL', 'CUSTOMERS');
    DBMS_STATS.GATHER_TABLE_STATS('RETAIL', 'ORDERS');
    DBMS_STATS.GATHER_TABLE_STATS('RETAIL', 'PRODUCTS');
END;
/

PROMPT Statistics gathered.

-- ===================================================================
-- COMPLETION SUMMARY
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT SAMPLE DATA LOAD COMPLETE
PROMPT ===================================================================
PROMPT
PROMPT Data Loaded:
PROMPT   ✓ 100 customers across 10 states
PROMPT   ✓ 50 products across 5 categories  
PROMPT   ✓ 200 orders with various statuses
PROMPT
PROMPT Data Distribution:
PROMPT   - Customers created over past year (realistic timestamps)
PROMPT   - Products created over past 6 months
PROMPT   - Orders placed over past 90 days
PROMPT   - ~2 orders per customer on average
PROMPT   - 10% of products are inactive
PROMPT
PROMPT Data Quality:
PROMPT   ✓ No NULL values in required fields
PROMPT   ✓ All foreign key references are valid
PROMPT   ✓ All constraints satisfied
PROMPT   ✓ Realistic business data patterns
PROMPT
PROMPT Ready for CDC Testing:
PROMPT   - All tables have supplemental logging enabled
PROMPT   - Sample data represents realistic retail operations
PROMPT   - Data includes temporal patterns for CDC testing
PROMPT   - Ready for Oracle to Databricks CDC pipeline
PROMPT
PROMPT Next Steps:
PROMPT   1. Verify data in Databricks after snapshot
PROMPT   2. Run CDC test operations (07_cdc_test_operations.sql)
PROMPT   3. Monitor CDC latency and throughput
PROMPT   4. Build silver/gold layer transformations
PROMPT ===================================================================

EXIT;
