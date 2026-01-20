-- ============================================================================
-- Insert Sample Data for Retail Database
-- Purpose: Populate retail_db with realistic sample data for CDC testing
-- Prerequisites: retail_db database and tables created (03_create_retail_database.sql)
-- ============================================================================

-- Switch to retail database
USE retail_db;

-- ============================================================================
-- STEP 1: Insert Sample Customers
-- ============================================================================

-- Insert 15 sample customers across different states
INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code)
VALUES 
    -- West Coast customers
    ('John', 'Doe', 'john.doe@email.com', '555-0100', '123 Main St', 'New York', 'NY', '10001'),
    ('Sarah', 'Johnson', 'sarah.j@email.com', '555-0101', '456 Oak Ave', 'Los Angeles', 'CA', '90001'),
    ('Michael', 'Brown', 'michael.b@email.com', '555-0102', '789 Pine Rd', 'Chicago', 'IL', '60601'),
    
    -- Mid-West customers
    ('Emily', 'Davis', 'emily.d@email.com', '555-0103', '321 Elm St', 'Houston', 'TX', '77001'),
    ('David', 'Wilson', 'david.w@email.com', '555-0104', '654 Maple Dr', 'Phoenix', 'AZ', '85001'),
    ('Jessica', 'Martinez', 'jessica.m@email.com', '555-0105', '987 Cedar Ln', 'Philadelphia', 'PA', '19019'),
    
    -- East Coast customers
    ('James', 'Anderson', 'james.a@email.com', '555-0106', '147 Birch Ct', 'San Antonio', 'TX', '78201'),
    ('Jennifer', 'Taylor', 'jennifer.t@email.com', '555-0107', '258 Spruce Way', 'San Diego', 'CA', '92101'),
    ('Robert', 'Thomas', 'robert.t@email.com', '555-0108', '369 Willow Pl', 'Dallas', 'TX', '75201'),
    ('Linda', 'Moore', 'linda.m@email.com', '555-0109', '741 Ash Blvd', 'San Jose', 'CA', '95101'),
    
    -- Additional diverse locations
    ('William', 'Jackson', 'william.j@email.com', '555-0110', '852 Oakwood Ave', 'Austin', 'TX', '78701'),
    ('Mary', 'White', 'mary.w@email.com', '555-0111', '963 Pinewood Dr', 'Jacksonville', 'FL', '32099'),
    ('Christopher', 'Harris', 'chris.h@email.com', '555-0112', '159 Elmwood Ct', 'San Francisco', 'CA', '94102'),
    ('Patricia', 'Martin', 'patricia.m@email.com', '555-0113', '357 Maplewood St', 'Columbus', 'OH', '43201'),
    ('Daniel', 'Thompson', 'daniel.t@email.com', '555-0114', '753 Cedarwood Ln', 'Charlotte', 'NC', '28201');

-- Verify customer insertion
SELECT COUNT(*) as total_customers FROM customers;
SELECT * FROM customers ORDER BY customer_id LIMIT 5;

-- ============================================================================
-- STEP 2: Insert Sample Products
-- ============================================================================

-- Insert 20 sample products across multiple categories
INSERT INTO products (product_name, category, price, stock_quantity, supplier, description)
VALUES
    -- Electronics (high-value items)
    ('Laptop Pro 15"', 'Electronics', 1299.99, 50, 'Tech Supply Co', 'High-performance laptop with 16GB RAM and 512GB SSD'),
    ('4K Monitor 27"', 'Electronics', 349.99, 40, 'Tech Supply Co', '4K UHD 27-inch monitor with HDR support'),
    ('Wireless Mouse', 'Electronics', 29.99, 200, 'Tech Supply Co', 'Ergonomic wireless mouse with USB receiver'),
    ('Wireless Keyboard', 'Electronics', 79.99, 80, 'Tech Supply Co', 'Mechanical wireless keyboard with RGB lighting'),
    ('Webcam HD', 'Electronics', 89.99, 60, 'Tech Supply Co', '1080p HD webcam with built-in microphone'),
    ('USB-C Hub', 'Electronics', 49.99, 100, 'Tech Supply Co', '7-in-1 USB-C hub with HDMI and USB 3.0 ports'),
    ('Printer All-in-One', 'Electronics', 299.99, 25, 'Tech Supply Co', 'Wireless all-in-one printer, scanner, and copier'),
    ('Noise-Canceling Headphones', 'Electronics', 199.99, 45, 'Tech Supply Co', 'Premium noise-canceling over-ear headphones'),
    
    -- Furniture (mid-value items)
    ('Office Chair Ergonomic', 'Furniture', 249.99, 30, 'Office Depot', 'Ergonomic office chair with lumbar support and adjustable armrests'),
    ('Standing Desk Adjustable', 'Furniture', 599.99, 15, 'Office Depot', 'Electric height-adjustable standing desk 48x30 inches'),
    ('Desk Lamp LED', 'Furniture', 39.99, 75, 'Lighting Plus', 'LED desk lamp with adjustable brightness and color temperature'),
    ('Ergonomic Footrest', 'Furniture', 45.99, 50, 'Office Depot', 'Adjustable ergonomic footrest with massage surface'),
    ('Monitor Stand Dual', 'Furniture', 89.99, 35, 'Office Depot', 'Dual monitor stand for two 27-inch displays'),
    
    -- Office Supplies (low-value items)
    ('Notebook Set Premium', 'Office Supplies', 15.99, 300, 'Paper Goods Inc', 'Set of 3 premium hardcover notebooks with elastic closure'),
    ('Desk Organizer Mesh', 'Office Supplies', 24.99, 150, 'Office Depot', 'Multi-compartment mesh desk organizer with drawers'),
    ('Whiteboard Small 24x36', 'Office Supplies', 29.99, 90, 'Office Depot', 'Magnetic dry-erase whiteboard with aluminum frame'),
    ('Cable Management Kit', 'Office Supplies', 19.99, 200, 'Office Depot', 'Under-desk cable management tray and clips'),
    ('Ergonomic Mouse Pad', 'Office Supplies', 34.99, 120, 'Office Depot', 'Gel-filled wrist support mouse pad with non-slip base'),
    ('Pen Set Executive', 'Office Supplies', 12.99, 250, 'Paper Goods Inc', 'Set of 10 premium ballpoint pens'),
    ('File Folder Set', 'Office Supplies', 8.99, 400, 'Paper Goods Inc', 'Pack of 25 letter-size file folders');

-- Verify product insertion
SELECT COUNT(*) as total_products FROM products;
SELECT * FROM products ORDER BY price DESC LIMIT 5;

-- ============================================================================
-- STEP 3: Insert Sample Orders
-- ============================================================================

-- Insert 25 sample orders with various statuses
INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
VALUES
    -- Recent orders (PENDING and PROCESSING)
    (1, 'PENDING', 1329.98, '123 Main St, New York, NY 10001', 'Credit Card'),
    (2, 'PROCESSING', 29.99, '456 Oak Ave, Los Angeles, CA 90001', 'PayPal'),
    (3, 'PENDING', 249.99, '789 Pine Rd, Chicago, IL 60601', 'Credit Card'),
    (4, 'PROCESSING', 599.99, '321 Elm St, Houston, TX 77001', 'Credit Card'),
    (5, 'PENDING', 79.98, '654 Maple Dr, Phoenix, AZ 85001', 'Debit Card'),
    
    -- Orders in fulfillment (SHIPPED)
    (1, 'SHIPPED', 349.99, '123 Main St, New York, NY 10001', 'Credit Card'),
    (6, 'SHIPPED', 89.99, '987 Cedar Ln, Philadelphia, PA 19019', 'PayPal'),
    (7, 'SHIPPED', 649.98, '147 Birch Ct, San Antonio, TX 78201', 'Credit Card'),
    (8, 'SHIPPED', 1299.99, '258 Spruce Way, San Diego, CA 92101', 'Credit Card'),
    (9, 'SHIPPED', 199.99, '369 Willow Pl, Dallas, TX 75201', 'PayPal'),
    
    -- Completed orders (DELIVERED)
    (2, 'DELIVERED', 119.98, '456 Oak Ave, Los Angeles, CA 90001', 'PayPal'),
    (10, 'DELIVERED', 24.99, '741 Ash Blvd, San Jose, CA 95101', 'Credit Card'),
    (11, 'DELIVERED', 599.99, '852 Oakwood Ave, Austin, TX 78701', 'Credit Card'),
    (3, 'DELIVERED', 349.99, '789 Pine Rd, Chicago, IL 60601', 'Credit Card'),
    (12, 'DELIVERED', 45.97, '963 Pinewood Dr, Jacksonville, FL 32099', 'PayPal'),
    
    -- More diverse orders
    (13, 'PROCESSING', 299.99, '159 Elmwood Ct, San Francisco, CA 94102', 'Credit Card'),
    (14, 'PENDING', 89.99, '357 Maplewood St, Columbus, OH 43201', 'PayPal'),
    (15, 'SHIPPED', 249.99, '753 Cedarwood Ln, Charlotte, NC 28201', 'Debit Card'),
    (1, 'DELIVERED', 15.99, '123 Main St, New York, NY 10001', 'Credit Card'),
    (4, 'PROCESSING', 199.99, '321 Elm St, Houston, TX 77001', 'PayPal'),
    
    -- Additional orders for variety
    (6, 'PENDING', 1299.99, '987 Cedar Ln, Philadelphia, PA 19019', 'Credit Card'),
    (10, 'SHIPPED', 79.99, '741 Ash Blvd, San Jose, CA 95101', 'PayPal'),
    (11, 'DELIVERED', 34.99, '852 Oakwood Ave, Austin, TX 78701', 'Credit Card'),
    (7, 'CANCELLED', 50.00, '147 Birch Ct, San Antonio, TX 78201', 'Credit Card'),
    (13, 'DELIVERED', 129.98, '159 Elmwood Ct, San Francisco, CA 94102', 'PayPal');

-- Verify order insertion
SELECT COUNT(*) as total_orders FROM orders;
SELECT * FROM orders ORDER BY order_id LIMIT 5;

-- ============================================================================
-- STEP 4: Verify Data Quality and Relationships
-- ============================================================================

-- Check record counts
SELECT 
    'customers' as table_name, COUNT(*) as record_count FROM customers
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'products', COUNT(*) FROM products;

-- Verify all orders have valid customers (referential integrity)
SELECT 
    COUNT(DISTINCT o.customer_id) as orders_distinct_customers,
    COUNT(DISTINCT c.customer_id) as total_customers_with_orders,
    (SELECT COUNT(*) FROM orders) as total_orders
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- Check for any orphaned orders (should be 0)
SELECT COUNT(*) as orphaned_orders_count
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- ============================================================================
-- STEP 5: Sample Analytical Queries
-- ============================================================================

-- Customer order summary
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) as customer_name,
    c.city,
    c.state,
    COUNT(o.order_id) as total_orders,
    ROUND(SUM(o.total_amount), 2) as lifetime_value,
    ROUND(AVG(o.total_amount), 2) as avg_order_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, customer_name, c.city, c.state
ORDER BY lifetime_value DESC;

-- Orders by status
SELECT 
    order_status,
    COUNT(*) as order_count,
    ROUND(SUM(total_amount), 2) as total_value,
    ROUND(AVG(total_amount), 2) as avg_order_value
FROM orders
GROUP BY order_status
ORDER BY order_count DESC;

-- Products by category
SELECT 
    category,
    COUNT(*) as product_count,
    ROUND(MIN(price), 2) as min_price,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(MAX(price), 2) as max_price,
    SUM(stock_quantity) as total_stock
FROM products
GROUP BY category
ORDER BY avg_price DESC;

-- Top 5 customers by order value
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) as customer_name,
    c.email,
    COUNT(o.order_id) as order_count,
    ROUND(SUM(o.total_amount), 2) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, customer_name, c.email
ORDER BY total_spent DESC
LIMIT 5;

-- Recent orders (last 10)
SELECT 
    o.order_id,
    CONCAT(c.first_name, ' ', c.last_name) as customer_name,
    o.order_date,
    o.order_status,
    ROUND(o.total_amount, 2) as amount,
    o.payment_method
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
ORDER BY o.order_date DESC, o.order_id DESC
LIMIT 10;

-- ============================================================================
-- STEP 6: Verify Binary Log Capture
-- ============================================================================

-- Check that inserts were captured in binary log
SHOW MASTER STATUS;
-- Note the current File and Position

-- View recent binary log events (replace with your current binlog file)
-- SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 20;

-- ============================================================================
-- STEP 7: Data Summary
-- ============================================================================

SELECT 'Data Loading Complete' as status;

SELECT 
    'Summary' as report_type,
    (SELECT COUNT(*) FROM customers) as customers,
    (SELECT COUNT(*) FROM orders) as orders,
    (SELECT COUNT(*) FROM products) as products,
    (SELECT ROUND(SUM(total_amount), 2) FROM orders) as total_order_value;

-- ============================================================================
-- Notes
-- ============================================================================

/*
Sample Data Statistics:
- 15 customers across 10+ US states
- 20 products across 3 categories (Electronics, Furniture, Office Supplies)
- 25 orders with various statuses (PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED)
- Order values range from $8.99 to $1,299.99
- All foreign key relationships are valid
- Realistic timestamps for created_date and last_updated

Data Characteristics:
- Email addresses are unique per customer
- Orders reference valid customers (referential integrity)
- Product categories represent different price points
- Order statuses reflect real-world order lifecycle
- Multiple orders per customer for some customers

CDC Testing Scenarios:
- Use this data as baseline for incremental CDC testing
- Modify records to test UPDATE operations
- Insert new records to test INSERT operations
- Delete records to test DELETE operations (respect FK constraints)
- All changes will be captured in MySQL binary log for Lakeflow Connect
*/

-- Ready for CDC pipeline setup (proceed to Lakeflow Connect configuration)
