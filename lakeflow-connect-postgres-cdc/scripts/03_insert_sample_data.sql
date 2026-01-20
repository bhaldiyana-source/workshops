-- ============================================================================
-- PostgreSQL Sample Data Insertion Script
-- ============================================================================
-- This script populates the retail database with realistic sample data for
-- CDC testing and demonstration.
--
-- Prerequisites:
-- - Completed 02_create_schema.sql
-- - Tables: customers, orders, products created
--
-- Usage:
--   psql -h your-host -U postgres -d retaildb -f 03_insert_sample_data.sql
-- ============================================================================

\echo '================================================'
\echo 'Inserting Sample Data for CDC Workshop'
\echo '================================================'
\echo ''

-- ============================================================================
-- PART 1: Insert Sample Customers
-- ============================================================================

\echo 'Part 1: Inserting Sample Customers...'

INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, created_date)
VALUES 
    ('John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'Seattle', 'WA', '98101', CURRENT_TIMESTAMP - INTERVAL '30 days'),
    ('Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Portland', 'OR', '97201', CURRENT_TIMESTAMP - INTERVAL '28 days'),
    ('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine Rd', 'San Francisco', 'CA', '94102', CURRENT_TIMESTAMP - INTERVAL '25 days'),
    ('Alice', 'Williams', 'alice.williams@email.com', '555-0104', '321 Elm St', 'Los Angeles', 'CA', '90001', CURRENT_TIMESTAMP - INTERVAL '22 days'),
    ('Charlie', 'Brown', 'charlie.brown@email.com', '555-0105', '654 Maple Dr', 'Austin', 'TX', '78701', CURRENT_TIMESTAMP - INTERVAL '20 days'),
    ('Diana', 'Martinez', 'diana.martinez@email.com', '555-0106', '987 Cedar Ln', 'Phoenix', 'AZ', '85001', CURRENT_TIMESTAMP - INTERVAL '18 days'),
    ('Edward', 'Garcia', 'edward.garcia@email.com', '555-0107', '147 Birch Way', 'Denver', 'CO', '80201', CURRENT_TIMESTAMP - INTERVAL '15 days'),
    ('Fiona', 'Rodriguez', 'fiona.rodriguez@email.com', '555-0108', '258 Spruce Ct', 'Boston', 'MA', '02101', CURRENT_TIMESTAMP - INTERVAL '12 days'),
    ('George', 'Wilson', 'george.wilson@email.com', '555-0109', '369 Ash Blvd', 'Chicago', 'IL', '60601', CURRENT_TIMESTAMP - INTERVAL '10 days'),
    ('Helen', 'Lee', 'helen.lee@email.com', '555-0110', '741 Walnut Ave', 'Miami', 'FL', '33101', CURRENT_TIMESTAMP - INTERVAL '8 days');

\echo 'Inserted 10 customers'
\echo ''

-- Verify customer insertion
SELECT COUNT(*) as customer_count FROM customers;

\echo ''

-- ============================================================================
-- PART 2: Insert Sample Products
-- ============================================================================

\echo 'Part 2: Inserting Sample Products...'

INSERT INTO products (product_name, category, price, stock_quantity, supplier, description, created_date)
VALUES 
    -- Electronics
    ('Laptop Pro 15', 'Electronics', 1299.99, 50, 'TechSupply Inc', 'High-performance 15-inch laptop with 16GB RAM', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Wireless Mouse', 'Electronics', 29.99, 200, 'TechSupply Inc', 'Ergonomic wireless mouse with precision tracking', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Mechanical Keyboard', 'Electronics', 89.99, 150, 'TechSupply Inc', 'RGB backlit mechanical gaming keyboard', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('USB-C Hub', 'Electronics', 45.99, 100, 'TechSupply Inc', '7-in-1 USB-C hub with HDMI and ethernet', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Noise Cancelling Headphones', 'Electronics', 249.99, 75, 'TechSupply Inc', 'Premium over-ear headphones with active noise cancellation', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Wireless Charger', 'Electronics', 35.99, 180, 'TechSupply Inc', 'Fast wireless charging pad for smartphones', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('External SSD 1TB', 'Electronics', 129.99, 90, 'TechSupply Inc', 'Portable 1TB SSD with USB 3.2 Gen 2', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Webcam HD', 'Electronics', 79.99, 120, 'TechSupply Inc', '1080p HD webcam with built-in microphone', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    
    -- Furniture
    ('Office Chair', 'Furniture', 249.99, 30, 'OfficeGoods LLC', 'Ergonomic office chair with lumbar support', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Standing Desk', 'Furniture', 399.99, 25, 'OfficeGoods LLC', 'Electric height-adjustable standing desk', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Desk Lamp', 'Furniture', 45.99, 75, 'OfficeGoods LLC', 'LED desk lamp with adjustable brightness', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Monitor Stand', 'Furniture', 34.99, 100, 'OfficeGoods LLC', 'Adjustable monitor stand with storage', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Bookshelf', 'Furniture', 129.99, 40, 'OfficeGoods LLC', '5-tier wooden bookshelf', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    
    -- Stationery
    ('Notebook Pack', 'Stationery', 12.99, 500, 'PaperWorld', 'Pack of 5 college-ruled notebooks', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Pen Set', 'Stationery', 8.99, 600, 'PaperWorld', 'Set of 10 ballpoint pens in assorted colors', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Sticky Notes', 'Stationery', 5.99, 800, 'PaperWorld', 'Pack of 12 sticky note pads', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Desk Organizer', 'Stationery', 19.99, 150, 'PaperWorld', 'Multi-compartment desk organizer', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('Whiteboard', 'Stationery', 29.99, 80, 'PaperWorld', '24x36 inch magnetic whiteboard', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    
    -- Office Supplies
    ('Printer Paper', 'Office Supplies', 24.99, 200, 'SupplyMart', 'Ream of 500 sheets letter-size paper', CURRENT_TIMESTAMP - INTERVAL '60 days'),
    ('File Folders', 'Office Supplies', 14.99, 300, 'SupplyMart', 'Pack of 50 manila file folders', CURRENT_TIMESTAMP - INTERVAL '60 days');

\echo 'Inserted 20 products'
\echo ''

-- Verify product insertion
SELECT COUNT(*) as product_count FROM products;

\echo ''

-- ============================================================================
-- PART 3: Insert Sample Orders
-- ============================================================================

\echo 'Part 3: Inserting Sample Orders...'

-- Orders for customer 1 (John Doe)
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
VALUES 
    (1, CURRENT_TIMESTAMP - INTERVAL '25 days', 'DELIVERED', 1329.98, '123 Main St, Seattle, WA 98101', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '25 days'),
    (1, CURRENT_TIMESTAMP - INTERVAL '10 days', 'SHIPPED', 45.99, '123 Main St, Seattle, WA 98101', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '10 days'),
    (1, CURRENT_TIMESTAMP - INTERVAL '2 days', 'PENDING', 89.99, '123 Main St, Seattle, WA 98101', 'PayPal', CURRENT_TIMESTAMP - INTERVAL '2 days');

-- Orders for customer 2 (Jane Smith)
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
VALUES 
    (2, CURRENT_TIMESTAMP - INTERVAL '20 days', 'DELIVERED', 249.99, '456 Oak Ave, Portland, OR 97201', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '20 days'),
    (2, CURRENT_TIMESTAMP - INTERVAL '5 days', 'PROCESSING', 134.97, '456 Oak Ave, Portland, OR 97201', 'Debit Card', CURRENT_TIMESTAMP - INTERVAL '5 days');

-- Orders for customer 3 (Bob Johnson)
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
VALUES 
    (3, CURRENT_TIMESTAMP - INTERVAL '18 days', 'DELIVERED', 399.99, '789 Pine Rd, San Francisco, CA 94102', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '18 days'),
    (3, CURRENT_TIMESTAMP - INTERVAL '8 days', 'SHIPPED', 79.99, '789 Pine Rd, San Francisco, CA 94102', 'PayPal', CURRENT_TIMESTAMP - INTERVAL '8 days');

-- Orders for customer 4 (Alice Williams)
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
VALUES 
    (4, CURRENT_TIMESTAMP - INTERVAL '15 days', 'DELIVERED', 564.96, '321 Elm St, Los Angeles, CA 90001', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '15 days'),
    (4, CURRENT_TIMESTAMP - INTERVAL '3 days', 'PENDING', 35.99, '321 Elm St, Los Angeles, CA 90001', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '3 days');

-- Orders for customer 5 (Charlie Brown)
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
VALUES 
    (5, CURRENT_TIMESTAMP - INTERVAL '12 days', 'DELIVERED', 129.99, '654 Maple Dr, Austin, TX 78701', 'Debit Card', CURRENT_TIMESTAMP - INTERVAL '12 days');

-- Orders for customers 6-10
INSERT INTO orders (customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date)
VALUES 
    (6, CURRENT_TIMESTAMP - INTERVAL '14 days', 'DELIVERED', 1299.99, '987 Cedar Ln, Phoenix, AZ 85001', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '14 days'),
    (7, CURRENT_TIMESTAMP - INTERVAL '9 days', 'SHIPPED', 249.99, '147 Birch Way, Denver, CO 80201', 'PayPal', CURRENT_TIMESTAMP - INTERVAL '9 days'),
    (8, CURRENT_TIMESTAMP - INTERVAL '7 days', 'PROCESSING', 89.99, '258 Spruce Ct, Boston, MA 02101', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '7 days'),
    (9, CURRENT_TIMESTAMP - INTERVAL '6 days', 'PENDING', 45.99, '369 Ash Blvd, Chicago, IL 60601', 'Debit Card', CURRENT_TIMESTAMP - INTERVAL '6 days'),
    (10, CURRENT_TIMESTAMP - INTERVAL '4 days', 'PROCESSING', 175.98, '741 Walnut Ave, Miami, FL 33101', 'Credit Card', CURRENT_TIMESTAMP - INTERVAL '4 days');

\echo 'Inserted 15 orders'
\echo ''

-- Verify order insertion
SELECT COUNT(*) as order_count FROM orders;

\echo ''

-- ============================================================================
-- PART 4: Data Verification and Summary
-- ============================================================================

\echo '================================================'
\echo 'Sample Data Insertion Complete!'
\echo '================================================'
\echo ''

\echo 'Data Summary:'
\echo '-------------'

-- Count summary
SELECT 
    'customers' as table_name,
    COUNT(*) as record_count,
    MIN(created_date) as oldest_record,
    MAX(created_date) as newest_record
FROM customers

UNION ALL

SELECT 
    'products',
    COUNT(*),
    MIN(created_date),
    MAX(created_date)
FROM products

UNION ALL

SELECT 
    'orders',
    COUNT(*),
    MIN(created_date),
    MAX(created_date)
FROM orders

ORDER BY table_name;

\echo ''
\echo 'Customer Distribution by State:'
SELECT state, COUNT(*) as customer_count
FROM customers
GROUP BY state
ORDER BY customer_count DESC;

\echo ''
\echo 'Product Distribution by Category:'
SELECT category, COUNT(*) as product_count, 
       CONCAT('$', ROUND(AVG(price), 2)) as avg_price
FROM products
GROUP BY category
ORDER BY product_count DESC;

\echo ''
\echo 'Order Status Distribution:'
SELECT order_status, COUNT(*) as order_count,
       CONCAT('$', ROUND(SUM(total_amount), 2)) as total_revenue
FROM orders
GROUP BY order_status
ORDER BY order_count DESC;

\echo ''
\echo 'Top Customers by Order Count:'
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(o.order_id) as order_count,
    CONCAT('$', ROUND(SUM(o.total_amount), 2)) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
ORDER BY order_count DESC
LIMIT 5;

\echo ''
\echo '================================================'
\echo 'Next Steps:'
\echo '================================================'
\echo '1. Configure Databricks Lakeflow Connect'
\echo '2. Create ingestion gateway and pipeline'
\echo '3. Run initial snapshot to load this data'
\echo '4. Test incremental CDC with 04_cdc_test_operations.sql'
\echo ''
