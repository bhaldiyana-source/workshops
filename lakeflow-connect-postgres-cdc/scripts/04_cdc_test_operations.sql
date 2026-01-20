-- ============================================================================
-- CDC Test Operations Script
-- ============================================================================
-- This script contains test INSERT, UPDATE, and DELETE operations to validate
-- Change Data Capture functionality with Lakeflow Connect.
--
-- Prerequisites:
-- - Completed 03_insert_sample_data.sql
-- - Lakeflow Connect pipeline running
-- - Initial snapshot completed
--
-- Usage:
--   psql -h your-host -U postgres -d retaildb -f 04_cdc_test_operations.sql
--
-- Note: Execute operations one section at a time, allowing pipeline to run
--       between operations to observe CDC in action.
-- ============================================================================

\echo '================================================'
\echo 'CDC Test Operations'
\echo '================================================'
\echo ''

-- ============================================================================
-- TEST SET 1: INSERT Operations
-- ============================================================================

\echo '================================================'
\echo 'TEST SET 1: INSERT Operations'
\echo '================================================'
\echo ''

\echo 'Test 1.1: Insert New Customer'
INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, created_date)
VALUES (
    'Emily',
    'Chen',
    'emily.chen@email.com',
    '555-0201',
    '789 Market St',
    'San Francisco',
    'CA',
    '94103',
    CURRENT_TIMESTAMP
);

-- Verify insertion
SELECT customer_id, first_name, last_name, email, created_date
FROM customers
WHERE email = 'emily.chen@email.com';

\echo '✓ Test 1.1 Complete: New customer inserted'
\echo ''

\echo 'Test 1.2: Insert New Product'
INSERT INTO products (product_name, category, price, stock_quantity, supplier, description, created_date)
VALUES (
    'Tablet 10 inch',
    'Electronics',
    449.99,
    60,
    'TechSupply Inc',
    '10-inch tablet with 128GB storage',
    CURRENT_TIMESTAMP
);

SELECT product_id, product_name, category, price, created_date
FROM products
WHERE product_name = 'Tablet 10 inch';

\echo '✓ Test 1.2 Complete: New product inserted'
\echo ''

\echo 'Test 1.3: Insert New Order'
-- Use an existing customer_id (e.g., customer 1)
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
VALUES (
    1,
    CURRENT_TIMESTAMP,
    'PENDING',
    449.99,
    '123 Main St, Seattle, WA 98101',
    'Credit Card',
    CURRENT_TIMESTAMP
);

SELECT order_id, customer_id, order_status, total_amount, created_date
FROM orders
ORDER BY created_date DESC
LIMIT 1;

\echo '✓ Test 1.3 Complete: New order inserted'
\echo ''

\echo '>>> Wait for pipeline to run, then check Databricks bronze tables'
\echo '>>> Verify 3 new records appear with _change_type = INSERT'
\echo ''
\echo 'Press Enter when ready to continue to UPDATE tests...'
\echo ''

-- ============================================================================
-- TEST SET 2: UPDATE Operations
-- ============================================================================

\echo '================================================'
\echo 'TEST SET 2: UPDATE Operations'
\echo '================================================'
\echo ''

\echo 'Test 2.1: Update Customer Address'
UPDATE customers
SET 
    address = '456 New Address Blvd',
    city = 'Oakland',
    state = 'CA',
    zip_code = '94607',
    last_updated = CURRENT_TIMESTAMP
WHERE email = 'john.doe@email.com';

-- Verify update
SELECT customer_id, first_name, last_name, address, city, state, last_updated
FROM customers
WHERE email = 'john.doe@email.com';

\echo '✓ Test 2.1 Complete: Customer address updated'
\echo ''

\echo 'Test 2.2: Update Order Status (PENDING → PROCESSING)'
-- Find a pending order
WITH pending_order AS (
    SELECT order_id
    FROM orders
    WHERE order_status = 'PENDING'
    ORDER BY created_date DESC
    LIMIT 1
)
UPDATE orders
SET 
    order_status = 'PROCESSING',
    last_updated = CURRENT_TIMESTAMP
FROM pending_order
WHERE orders.order_id = pending_order.order_id;

-- Verify update
SELECT order_id, customer_id, order_status, total_amount, last_updated
FROM orders
WHERE order_status = 'PROCESSING'
ORDER BY last_updated DESC
LIMIT 1;

\echo '✓ Test 2.2 Complete: Order status updated'
\echo ''

\echo 'Test 2.3: Update Product Price (Price Drop)'
UPDATE products
SET 
    price = 399.99,
    last_updated = CURRENT_TIMESTAMP
WHERE product_name = 'Tablet 10 inch';

-- Verify update
SELECT product_id, product_name, price, last_updated
FROM products
WHERE product_name = 'Tablet 10 inch';

\echo '✓ Test 2.3 Complete: Product price updated'
\echo ''

\echo 'Test 2.4: Bulk Update (Update Multiple Records)'
UPDATE products
SET 
    stock_quantity = stock_quantity + 50,
    last_updated = CURRENT_TIMESTAMP
WHERE category = 'Stationery';

-- Verify update
SELECT product_name, category, stock_quantity, last_updated
FROM products
WHERE category = 'Stationery'
ORDER BY product_name;

\echo '✓ Test 2.4 Complete: Bulk product stock updated'
\echo ''

\echo '>>> Wait for pipeline to run, then check Databricks bronze tables'
\echo '>>> Verify updated records with _change_type = UPDATE'
\echo '>>> Compare old vs new values using Delta time travel'
\echo ''
\echo 'Press Enter when ready to continue to DELETE tests...'
\echo ''

-- ============================================================================
-- TEST SET 3: DELETE Operations
-- ============================================================================

\echo '================================================'
\echo 'TEST SET 3: DELETE Operations'
\echo '================================================'
\echo ''

\echo 'IMPORTANT: Delete operations will test soft-delete or hard-delete'
\echo 'depending on your Lakeflow Connect pipeline configuration.'
\echo ''

\echo 'Test 3.1: Create Test Customer for Deletion'
INSERT INTO customers (first_name, last_name, email, phone, city, state, created_date)
VALUES (
    'Test',
    'DeleteMe',
    'test.delete@email.com',
    '555-9999',
    'TestCity',
    'CA',
    CURRENT_TIMESTAMP
);

-- Get the customer_id
SELECT customer_id, first_name, last_name, email
FROM customers
WHERE email = 'test.delete@email.com';

\echo '✓ Test 3.1 Complete: Test customer created'
\echo ''
\echo '>>> Wait for pipeline to run and verify record appears in Databricks'
\echo '>>> Note the customer_id for verification later'
\echo ''
\echo 'Press Enter when ready to delete...'
\echo ''

\echo 'Test 3.2: Delete Test Customer'
DELETE FROM customers
WHERE email = 'test.delete@email.com';

-- Verify deletion in PostgreSQL
SELECT COUNT(*) as should_be_zero
FROM customers
WHERE email = 'test.delete@email.com';

\echo '✓ Test 3.2 Complete: Test customer deleted from PostgreSQL'
\echo ''

\echo 'Test 3.3: Create and Delete Test Product'
-- Insert test product
INSERT INTO products (product_name, category, price, stock_quantity, created_date)
VALUES ('DELETE_TEST_PRODUCT', 'Test', 1.00, 1, CURRENT_TIMESTAMP);

-- Show product
SELECT product_id, product_name FROM products WHERE product_name = 'DELETE_TEST_PRODUCT';

-- Delete immediately
DELETE FROM products WHERE product_name = 'DELETE_TEST_PRODUCT';

\echo '✓ Test 3.3 Complete: Test product created and deleted'
\echo ''

\echo '>>> Wait for pipeline to run, then check Databricks bronze tables'
\echo '>>> For SOFT DELETE: Record present with _is_deleted = TRUE'
\echo '>>> For HARD DELETE: Record physically removed'
\echo ''
\echo 'Press Enter when ready to continue to complex scenarios...'
\echo ''

-- ============================================================================
-- TEST SET 4: Complex Scenarios
-- ============================================================================

\echo '================================================'
\echo 'TEST SET 4: Complex CDC Scenarios'
\echo '================================================'
\echo ''

\echo 'Test 4.1: Rapid Fire Updates (Testing Concurrency)'
-- Perform multiple updates in quick succession
UPDATE customers
SET last_updated = CURRENT_TIMESTAMP
WHERE customer_id = 1;

UPDATE customers
SET phone = '555-1111'
WHERE customer_id = 1;

UPDATE customers
SET address = '999 Final Address St'
WHERE customer_id = 1;

-- Verify final state
SELECT customer_id, first_name, last_name, phone, address, last_updated
FROM customers
WHERE customer_id = 1;

\echo '✓ Test 4.1 Complete: Multiple rapid updates executed'
\echo ''

\echo 'Test 4.2: Parent-Child Relationship Test'
-- Insert customer and order in same transaction
BEGIN;

INSERT INTO customers (first_name, last_name, email, city, state, created_date)
VALUES ('Transaction', 'Test', 'transaction.test@email.com', 'Seattle', 'WA', CURRENT_TIMESTAMP)
RETURNING customer_id;

-- Note: In practice, capture RETURNING value and use in next INSERT
-- For this script, we'll do it in separate statements

COMMIT;

-- Get the customer_id
WITH new_customer AS (
    SELECT customer_id FROM customers WHERE email = 'transaction.test@email.com'
)
INSERT INTO orders (customer_id, order_status, total_amount, payment_method, created_date)
SELECT customer_id, 'PENDING', 99.99, 'Credit Card', CURRENT_TIMESTAMP
FROM new_customer;

-- Verify relationship
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    o.order_id,
    o.order_status,
    o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.email = 'transaction.test@email.com';

\echo '✓ Test 4.2 Complete: Parent-child records created'
\echo ''

\echo 'Test 4.3: Update with NULL Values'
UPDATE customers
SET phone = NULL
WHERE customer_id = 5;

SELECT customer_id, first_name, last_name, phone
FROM customers
WHERE customer_id = 5;

\echo '✓ Test 4.3 Complete: Updated field to NULL'
\echo ''

\echo 'Test 4.4: Large Batch Insert (Performance Test)'
-- Insert 100 orders for load testing
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
SELECT 
    (RANDOM() * 9 + 1)::INTEGER as customer_id,  -- Random customer 1-10
    CURRENT_TIMESTAMP - (RANDOM() * INTERVAL '30 days') as order_date,
    CASE (RANDOM() * 4)::INTEGER
        WHEN 0 THEN 'PENDING'
        WHEN 1 THEN 'PROCESSING'
        WHEN 2 THEN 'SHIPPED'
        ELSE 'DELIVERED'
    END as order_status,
    (RANDOM() * 500 + 10)::NUMERIC(10,2) as total_amount,
    '123 Test St, Test City, TS 12345' as shipping_address,
    'Credit Card' as payment_method,
    CURRENT_TIMESTAMP as created_date
FROM generate_series(1, 100);

\echo '✓ Test 4.4 Complete: Batch inserted 100 orders'
\echo ''

-- Verify total order count
SELECT COUNT(*) as total_orders FROM orders;

\echo ''
\echo '>>> Wait for pipeline to run and process all changes'
\echo '>>> Monitor pipeline execution time for large batch'
\echo ''

-- ============================================================================
-- TEST SET 5: Validation Queries
-- ============================================================================

\echo '================================================'
\echo 'TEST SET 5: Validation Queries'
\echo '================================================'
\echo ''

\echo 'Summary of Test Data:'
\echo '--------------------'

-- Current record counts
SELECT 'customers' as table_name, COUNT(*) as record_count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
ORDER BY table_name;

\echo ''
\echo 'Recent Changes (Last Hour):'
\echo '---------------------------'

SELECT 'customers' as table_name, COUNT(*) as changes_last_hour
FROM customers
WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
UNION ALL
SELECT 'products', COUNT(*)
FROM products
WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
UNION ALL
SELECT 'orders', COUNT(*)
FROM orders
WHERE last_updated >= CURRENT_TIMESTAMP - INTERVAL '1 hour';

\echo ''
\echo 'Replication Slot Status:'
\echo '------------------------'

SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as replication_lag
FROM pg_replication_slots
WHERE slot_name = 'lakeflow_slot';

\echo ''

-- ============================================================================
-- Completion
-- ============================================================================

\echo '================================================'
\echo 'CDC Test Operations Complete!'
\echo '================================================'
\echo ''
\echo 'Operations Performed:'
\echo '  [✓] INSERT: 3+ new records'
\echo '  [✓] UPDATE: Single and bulk updates'
\echo '  [✓] DELETE: Test record deletions'
\echo '  [✓] COMPLEX: Rapid updates, relationships, batch inserts'
\echo ''
\echo 'Next Steps in Databricks:'
\echo '  1. Check bronze tables for all change types'
\echo '  2. Verify _change_type metadata'
\echo '  3. Use Delta time travel to compare versions'
\echo '  4. Validate record counts match expected values'
\echo '  5. Check data quality and referential integrity'
\echo ''
\echo 'For cleanup, see: 05_validation_queries.sql'
\echo ''
