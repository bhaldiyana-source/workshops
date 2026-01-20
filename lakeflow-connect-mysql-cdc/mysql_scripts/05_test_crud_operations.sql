-- ============================================================================
-- Test CRUD Operations for CDC Validation
-- Purpose: Perform INSERT, UPDATE, DELETE operations to test CDC pipeline
-- Prerequisites: Sample data loaded (04_insert_sample_data.sql)
-- Note: Run these commands to generate CDC events for testing
-- ============================================================================

-- Switch to retail database
USE retail_db;

-- ============================================================================
-- BASELINE: Check Current State
-- ============================================================================

-- Record current counts before changes
SELECT 
    'BASELINE RECORD COUNTS' as report_type,
    (SELECT COUNT(*) FROM customers) as customers,
    (SELECT COUNT(*) FROM orders) as orders,
    (SELECT COUNT(*) FROM products) as products,
    NOW() as timestamp;

-- ============================================================================
-- TEST 1: INSERT Operations
-- ============================================================================

-- Insert new customers
INSERT INTO customers (first_name, last_name, email, phone, city, state, zip_code)
VALUES 
    ('Jane', 'Smith', 'jane.smith@email.com', '555-2001', 'Seattle', 'WA', '98101'),
    ('Carlos', 'Rodriguez', 'carlos.r@email.com', '555-2002', 'Miami', 'FL', '33101'),
    ('Aisha', 'Patel', 'aisha.p@email.com', '555-2003', 'Austin', 'TX', '78701'),
    ('Li', 'Wang', 'li.wang@email.com', '555-2004', 'Boston', 'MA', '02101');

-- Verify inserts and note customer IDs
SELECT customer_id, first_name, last_name, email, created_date
FROM customers
WHERE email IN ('jane.smith@email.com', 'carlos.r@email.com', 'aisha.p@email.com', 'li.wang@email.com')
ORDER BY customer_id;

-- Insert new products
INSERT INTO products (product_name, category, price, stock_quantity, supplier, description)
VALUES
    ('4K Webcam Pro', 'Electronics', 149.99, 45, 'Tech Supply Co', 'Professional 4K webcam with AI auto-framing'),
    ('Ergonomic Mouse Pad Premium', 'Office Supplies', 39.99, 120, 'Office Depot', 'Memory foam wrist support with wireless charging'),
    ('Desk Cable Organizer Pro', 'Office Supplies', 27.99, 180, 'Office Depot', 'Premium cable management system with clips'),
    ('Portable Monitor 15.6"', 'Electronics', 199.99, 30, 'Tech Supply Co', 'USB-C portable monitor with stand');

-- Verify product inserts
SELECT product_id, product_name, category, price, created_date
FROM products
WHERE product_name LIKE '%Pro%' OR product_name LIKE '%Premium%'
ORDER BY product_id DESC
LIMIT 4;

-- Insert new orders (use customer_ids from above - adjust as needed)
-- Assuming new customer_ids are 16, 17, 18, 19
INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
VALUES
    (16, 'PENDING', 149.99, '123 Pike St, Seattle, WA 98101', 'Credit Card'),
    (17, 'PROCESSING', 227.98, '456 Ocean Dr, Miami, FL 33101', 'PayPal'),
    (18, 'PENDING', 39.99, '789 Congress Ave, Austin, TX 78701', 'Credit Card'),
    (19, 'PROCESSING', 199.99, '321 Boylston St, Boston, MA 02101', 'Debit Card');

-- Verify order inserts
SELECT 
    o.order_id,
    c.first_name,
    c.last_name,
    o.order_status,
    o.total_amount,
    o.created_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id >= 16
ORDER BY o.order_id DESC
LIMIT 5;

-- Summary of INSERT operations
SELECT 
    'INSERT OPERATIONS COMPLETED' as operation,
    '4 customers added' as customers,
    '4 products added' as products,
    '4 orders added' as orders;

-- ============================================================================
-- TEST 2: UPDATE Operations
-- ============================================================================

-- Update customer address (address change scenario)
UPDATE customers
SET city = 'Bellevue',
    zip_code = '98004',
    address = '999 Tech Blvd'
WHERE email = 'jane.smith@email.com';

-- Verify update
SELECT customer_id, first_name, last_name, email, city, zip_code, address, last_updated
FROM customers
WHERE email = 'jane.smith@email.com';

-- Update order status (order fulfillment scenario)
UPDATE orders
SET order_status = 'SHIPPED'
WHERE order_id IN (
    SELECT order_id 
    FROM (
        SELECT order_id 
        FROM orders 
        WHERE order_status = 'PROCESSING' 
        LIMIT 3
    ) AS subquery
);

-- Verify order status updates
SELECT order_id, customer_id, order_status, total_amount, last_updated
FROM orders
WHERE order_status = 'SHIPPED'
ORDER BY last_updated DESC
LIMIT 5;

-- Update product prices (price adjustment scenario)
-- Apply 10% discount to all Electronics
UPDATE products
SET price = ROUND(price * 0.90, 2)
WHERE category = 'Electronics'
  AND product_name LIKE '%Mouse%';

-- Verify price updates
SELECT product_id, product_name, category, price, last_updated
FROM products
WHERE product_name LIKE '%Mouse%'
ORDER BY last_updated DESC;

-- Bulk update: Mark old orders as delivered
UPDATE orders
SET order_status = 'DELIVERED'
WHERE order_status = 'SHIPPED'
  AND order_date < DATE_SUB(NOW(), INTERVAL 7 DAY);

-- Summary of UPDATE operations
SELECT 
    'UPDATE OPERATIONS COMPLETED' as operation,
    'Customer address updated' as customers,
    'Multiple order statuses updated' as orders,
    'Electronics prices discounted' as products;

-- ============================================================================
-- TEST 3: DELETE Operations
-- ============================================================================

-- Create test records specifically for deletion
INSERT INTO customers (first_name, last_name, email, city, state)
VALUES ('Delete', 'TestUser1', 'delete.test1@email.com', 'Test City', 'CA');

SET @delete_customer_id = LAST_INSERT_ID();

-- Insert test orders for the delete test customer
INSERT INTO orders (customer_id, order_status, total_amount, payment_method)
VALUES
    (@delete_customer_id, 'CANCELLED', 50.00, 'Test'),
    (@delete_customer_id, 'CANCELLED', 75.00, 'Test');

-- Verify test records exist
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) as order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.email = 'delete.test1@email.com'
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Delete in correct order: child records first, then parent
-- Delete orders first (respect FK constraint)
DELETE FROM orders
WHERE customer_id = @delete_customer_id;

-- Then delete customer
DELETE FROM customers
WHERE customer_id = @delete_customer_id;

-- Verify deletion
SELECT COUNT(*) as count_should_be_zero
FROM customers
WHERE email = 'delete.test1@email.com';

-- Delete test product
INSERT INTO products (product_name, category, price, stock_quantity)
VALUES ('Test Product DELETE', 'Test Category', 0.01, 0);

SET @delete_product_id = LAST_INSERT_ID();

-- Verify product exists
SELECT product_id, product_name, category, price
FROM products
WHERE product_id = @delete_product_id;

-- Delete the product
DELETE FROM products
WHERE product_id = @delete_product_id;

-- Verify deletion
SELECT COUNT(*) as count_should_be_zero
FROM products
WHERE product_name = 'Test Product DELETE';

-- Summary of DELETE operations
SELECT 
    'DELETE OPERATIONS COMPLETED' as operation,
    'Test customer and orders deleted' as customers_orders,
    'Test product deleted' as products;

-- ============================================================================
-- TEST 4: Mixed CRUD Operations (Real-World Scenario)
-- ============================================================================

-- Scenario: Customer places order, then updates shipping address

-- Step 1: New customer registration
INSERT INTO customers (first_name, last_name, email, phone, city, state)
VALUES ('Mixed', 'CRUD', 'mixed.crud@email.com', '555-9999', 'Denver', 'CO');

SET @new_customer_id = LAST_INSERT_ID();

-- Step 2: Customer places order
INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
VALUES (@new_customer_id, 'PENDING', 299.99, '123 Default St, Denver, CO 80201', 'Credit Card');

SET @new_order_id = LAST_INSERT_ID();

-- Step 3: Customer updates shipping address
UPDATE orders
SET shipping_address = '456 Updated St, Denver, CO 80202'
WHERE order_id = @new_order_id;

-- Step 4: Order progresses through statuses
UPDATE orders SET order_status = 'PROCESSING' WHERE order_id = @new_order_id;

-- Wait a moment (in real scenario)
SELECT SLEEP(1);

UPDATE orders SET order_status = 'SHIPPED' WHERE order_id = @new_order_id;

-- Verify the scenario
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    o.order_id,
    o.order_status,
    o.shipping_address,
    o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = @new_customer_id;

-- ============================================================================
-- TEST 5: Verify Binary Log Capture
-- ============================================================================

-- Check current binary log position
SHOW MASTER STATUS;

-- Verify all operations were logged
SELECT 
    'BINARY LOG VERIFICATION' as check_type,
    @@log_bin as binary_logging_enabled,
    @@binlog_format as binlog_format,
    @@gtid_mode as gtid_enabled;

-- ============================================================================
-- TEST 6: Final State Comparison
-- ============================================================================

-- Compare to baseline
SELECT 
    'FINAL RECORD COUNTS' as report_type,
    (SELECT COUNT(*) FROM customers) as customers,
    (SELECT COUNT(*) FROM orders) as orders,
    (SELECT COUNT(*) FROM products) as products,
    NOW() as timestamp;

-- Calculate net changes
SELECT 
    'NET CHANGES' as report_type,
    (SELECT COUNT(*) FROM customers) - 15 as customers_added,
    (SELECT COUNT(*) FROM orders) - 25 as orders_added,
    (SELECT COUNT(*) FROM products) - 20 as products_added;

-- ============================================================================
-- TEST 7: Recent Changes Summary
-- ============================================================================

-- Recent customer changes
SELECT 
    'Recent Customers' as entity_type,
    customer_id,
    CONCAT(first_name, ' ', last_name) as name,
    email,
    last_updated
FROM customers
ORDER BY last_updated DESC
LIMIT 5;

-- Recent order changes
SELECT 
    'Recent Orders' as entity_type,
    order_id,
    customer_id,
    order_status,
    total_amount,
    last_updated
FROM orders
ORDER BY last_updated DESC
LIMIT 5;

-- Recent product changes
SELECT 
    'Recent Products' as entity_type,
    product_id,
    product_name,
    price,
    last_updated
FROM products
ORDER BY last_updated DESC
LIMIT 5;

-- ============================================================================
-- CLEANUP: Optional Test Data Removal
-- ============================================================================

/*
-- Uncomment to clean up test data (run only if you want to reset)

-- Remove test customer created in mixed CRUD scenario
DELETE FROM orders WHERE customer_id = (SELECT customer_id FROM customers WHERE email = 'mixed.crud@email.com');
DELETE FROM customers WHERE email = 'mixed.crud@email.com';

-- Note: Keep other test data for CDC validation in Databricks
*/

-- ============================================================================
-- CDC Pipeline Testing Checklist
-- ============================================================================

/*
After running this script, trigger your Lakeflow Connect pipeline and verify:

1. INSERT Operations:
   ✓ New customers appear in bronze.customers
   ✓ New products appear in bronze.products
   ✓ New orders appear in bronze.orders
   ✓ _commit_timestamp is populated

2. UPDATE Operations:
   ✓ Customer address changes reflected
   ✓ Order status changes reflected
   ✓ Product price changes reflected
   ✓ _commit_timestamp updated

3. DELETE Operations:
   ✓ Test customer removed from bronze.customers
   ✓ Test orders removed from bronze.orders
   ✓ Test product removed from bronze.products

4. Data Quality:
   ✓ No orphaned orders (all orders have valid customers)
   ✓ Record counts match expectations
   ✓ Referential integrity maintained

5. Performance:
   ✓ Changes appear in bronze within expected latency (5-15 minutes)
   ✓ Pipeline execution completes successfully
   ✓ No errors in pipeline logs
*/

-- ============================================================================
-- End of CRUD Testing Script
-- ============================================================================

SELECT 
    'CRUD TESTING COMPLETED' as status,
    'Run Lakeflow Connect pipeline to verify CDC capture' as next_step,
    NOW() as timestamp;
