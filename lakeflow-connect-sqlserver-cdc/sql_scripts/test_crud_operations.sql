-- ============================================================================
-- Lakeflow Connect SQL Server CDC Workshop
-- Script: test_crud_operations.sql
-- Purpose: Test CDC capture with INSERT, UPDATE, DELETE operations
-- ============================================================================

USE RetailDB;
GO

PRINT '============================================================================';
PRINT 'Testing CDC with CRUD Operations';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- Test 1: INSERT Operations
-- ============================================================================

PRINT 'Test 1: INSERT Operations';
PRINT '--------------------------';

-- Insert new customers
INSERT INTO dbo.customers (customer_id, first_name, last_name, email, phone, city, state, zip_code, created_date, last_updated)
VALUES 
    (10001, 'Sarah', 'Connor', 'sarah.connor@email.com', '555-1234', 'Austin', 'TX', '78701', GETDATE(), GETDATE()),
    (10002, 'Kyle', 'Reese', 'kyle.reese@email.com', '555-5678', 'Houston', 'TX', '77001', GETDATE(), GETDATE());

PRINT '✓ Inserted 2 new customers (IDs: 10001, 10002)';

-- Insert new product
INSERT INTO dbo.products (product_id, product_name, category, price, stock_quantity, supplier, created_date, last_updated)
VALUES 
    (3001, 'USB-C Cable', 'Electronics', 19.99, 500, 'Tech Supplies Inc', GETDATE(), GETDATE());

PRINT '✓ Inserted 1 new product (ID: 3001)';

-- Insert new order
INSERT INTO dbo.orders (order_id, customer_id, order_date, order_status, total_amount, payment_method, created_date, last_updated)
VALUES 
    (6001, 10001, GETDATE(), 'PENDING', 19.99, 'Credit Card', GETDATE(), GETDATE());

PRINT '✓ Inserted 1 new order (ID: 6001)';

-- Wait for CDC capture
WAITFOR DELAY '00:00:05';

PRINT '';

-- ============================================================================
-- Test 2: UPDATE Operations
-- ============================================================================

PRINT 'Test 2: UPDATE Operations';
PRINT '--------------------------';

-- Update customer email
UPDATE dbo.customers
SET email = 'john.doe.updated@email.com', last_updated = GETDATE()
WHERE customer_id = 1001;

PRINT '✓ Updated customer 1001 email address';

-- Update customer location
UPDATE dbo.customers
SET city = 'Miami', state = 'FL', last_updated = GETDATE()
WHERE customer_id = 1002;

PRINT '✓ Updated customer 1002 location';

-- Update order status
UPDATE dbo.orders
SET order_status = 'SHIPPED', last_updated = GETDATE()
WHERE order_id = 5001;

PRINT '✓ Updated order 5001 status to SHIPPED';

-- Update product price
UPDATE dbo.products
SET price = 24.99, last_updated = GETDATE()
WHERE product_id = 2002;

PRINT '✓ Updated product 2002 price';

-- Wait for CDC capture
WAITFOR DELAY '00:00:05';

PRINT '';

-- ============================================================================
-- Test 3: DELETE Operations
-- ============================================================================

PRINT 'Test 3: DELETE Operations';
PRINT '--------------------------';

-- Delete order first (due to FK constraint)
DELETE FROM dbo.orders WHERE order_id = 6001;
PRINT '✓ Deleted order 6001';

-- Delete customer
DELETE FROM dbo.customers WHERE customer_id = 10002;
PRINT '✓ Deleted customer 10002';

-- Delete product
DELETE FROM dbo.products WHERE product_id = 3001;
PRINT '✓ Deleted product 3001';

-- Wait for CDC capture
WAITFOR DELAY '00:00:05';

PRINT '';

-- ============================================================================
-- Test 4: Bulk Operations
-- ============================================================================

PRINT 'Test 4: Bulk Operations';
PRINT '------------------------';

-- Bulk update order statuses
UPDATE dbo.orders
SET order_status = 'DELIVERED', last_updated = GETDATE()
WHERE order_status = 'SHIPPED';

PRINT '✓ Bulk updated orders from SHIPPED to DELIVERED';

-- Bulk update product prices (10% discount)
UPDATE dbo.products
SET price = price * 0.9, last_updated = GETDATE()
WHERE category = 'Electronics';

PRINT '✓ Applied 10% discount to all Electronics products';

PRINT '';

-- ============================================================================
-- Verification: View CDC Change Tables
-- ============================================================================

PRINT '============================================================================';
PRINT 'CDC Change Table Verification';
PRINT '============================================================================';
PRINT '';

-- View recent changes in customers CDC table
PRINT 'Recent changes in customers table:';
SELECT TOP 10
    __$start_lsn AS lsn,
    __$operation AS operation,
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT'
        WHEN 3 THEN 'UPDATE (before)'
        WHEN 4 THEN 'UPDATE (after)'
    END AS operation_type,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state
FROM cdc.dbo_customers_CT
ORDER BY __$start_lsn DESC;

PRINT '';

-- Count changes by operation type
PRINT 'Change counts by operation type (customers):';
SELECT 
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT'
        WHEN 3 THEN 'UPDATE (before)'
        WHEN 4 THEN 'UPDATE (after)'
    END AS operation_type,
    COUNT(*) AS change_count
FROM cdc.dbo_customers_CT
GROUP BY __$operation
ORDER BY __$operation;

PRINT '';

-- View recent changes in orders CDC table
PRINT 'Recent changes in orders table:';
SELECT TOP 10
    __$start_lsn AS lsn,
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT'
        WHEN 3 THEN 'UPDATE (before)'
        WHEN 4 THEN 'UPDATE (after)'
    END AS operation_type,
    order_id,
    customer_id,
    order_status,
    total_amount
FROM cdc.dbo_orders_CT
ORDER BY __$start_lsn DESC;

PRINT '';

-- View recent changes in products CDC table
PRINT 'Recent changes in products table:';
SELECT TOP 10
    __$start_lsn AS lsn,
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT'
        WHEN 3 THEN 'UPDATE (before)'
        WHEN 4 THEN 'UPDATE (after)'
    END AS operation_type,
    product_id,
    product_name,
    price
FROM cdc.dbo_products_CT
ORDER BY __$start_lsn DESC;

PRINT '';

-- ============================================================================
-- Verification: Check Current State
-- ============================================================================

PRINT '============================================================================';
PRINT 'Current Table State';
PRINT '============================================================================';
PRINT '';

-- Verify INSERT: Customer 10001 should exist
PRINT 'Verify customer 10001 exists (should return 1 row):';
SELECT customer_id, first_name, last_name, email 
FROM dbo.customers 
WHERE customer_id = 10001;

PRINT '';

-- Verify UPDATE: Customer 1001 email changed
PRINT 'Verify customer 1001 email updated:';
SELECT customer_id, email, last_updated 
FROM dbo.customers 
WHERE customer_id = 1001;

PRINT '';

-- Verify UPDATE: Customer 1002 location changed
PRINT 'Verify customer 1002 location updated:';
SELECT customer_id, city, state, last_updated 
FROM dbo.customers 
WHERE customer_id = 1002;

PRINT '';

-- Verify DELETE: Customer 10002 should NOT exist
PRINT 'Verify customer 10002 deleted (should return 0 rows):';
SELECT customer_id, first_name, last_name 
FROM dbo.customers 
WHERE customer_id = 10002;

PRINT '';

-- Verify UPDATE: Order 5001 status
PRINT 'Verify order 5001 status updated:';
SELECT order_id, order_status, last_updated 
FROM dbo.orders 
WHERE order_id = 5001;

PRINT '';

-- Verify UPDATE: Product 2002 price
PRINT 'Verify product 2002 price updated:';
SELECT product_id, product_name, price, last_updated 
FROM dbo.products 
WHERE product_id = 2002;

PRINT '';

-- Verify DELETE: Product 3001 should NOT exist
PRINT 'Verify product 3001 deleted (should return 0 rows):';
SELECT product_id, product_name 
FROM dbo.products 
WHERE product_id = 3001;

PRINT '';

-- ============================================================================
-- Summary
-- ============================================================================

PRINT '============================================================================';
PRINT 'Test Summary';
PRINT '============================================================================';
PRINT '';

DECLARE @total_changes INT;

SELECT @total_changes = (
    SELECT COUNT(*) FROM cdc.dbo_customers_CT
) + (
    SELECT COUNT(*) FROM cdc.dbo_orders_CT
) + (
    SELECT COUNT(*) FROM cdc.dbo_products_CT
);

PRINT 'Total CDC changes captured: ' + CAST(@total_changes AS VARCHAR);

SELECT 
    'customers' AS table_name,
    COUNT(*) AS changes_captured
FROM cdc.dbo_customers_CT

UNION ALL

SELECT 
    'orders',
    COUNT(*)
FROM cdc.dbo_orders_CT

UNION ALL

SELECT 
    'products',
    COUNT(*)
FROM cdc.dbo_products_CT

ORDER BY table_name;

PRINT '';
PRINT '✓ CRUD operations test completed successfully!';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Run Lakeflow Connect pipeline in Databricks (incremental mode)';
PRINT '  2. Verify changes appear in bronze Delta tables';
PRINT '  3. Validate CDC metadata columns (_commit_timestamp, etc.)';
PRINT '';
GO
