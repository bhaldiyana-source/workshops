-- ============================================================================
-- Lakeflow Connect SQL Server CDC Workshop
-- Script: sample_data.sql
-- Purpose: Insert sample data into customers, orders, and products tables
-- ============================================================================

USE RetailDB;
GO

PRINT '============================================================================';
PRINT 'Loading Sample Data into RetailDB';
PRINT '============================================================================';
PRINT '';

-- ============================================================================
-- Step 1: Insert Sample Customers
-- ============================================================================

PRINT 'Step 1: Inserting Sample Customers';
PRINT '-----------------------------------';

-- Clear existing data if any (optional - comment out if you want to preserve data)
-- DELETE FROM orders;
-- DELETE FROM customers;
-- DELETE FROM products;

-- Insert customers
INSERT INTO dbo.customers (customer_id, first_name, last_name, email, phone, address, city, state, zip_code, created_date, last_updated)
VALUES
    (1001, 'John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'Seattle', 'WA', '98101', GETDATE(), GETDATE()),
    (1002, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Portland', 'OR', '97201', GETDATE(), GETDATE()),
    (1003, 'Bob', 'Johnson', 'bob.j@email.com', '555-0103', '789 Pine Rd', 'San Francisco', 'CA', '94102', GETDATE(), GETDATE()),
    (1004, 'Alice', 'Williams', 'alice.w@email.com', '555-0104', '321 Elm St', 'Los Angeles', 'CA', '90001', GETDATE(), GETDATE()),
    (1005, 'Charlie', 'Brown', 'charlie.b@email.com', '555-0105', '654 Maple Dr', 'Denver', 'CO', '80201', GETDATE(), GETDATE()),
    (1006, 'Diana', 'Prince', 'diana.p@email.com', '555-0106', '987 Cedar Ln', 'Phoenix', 'AZ', '85001', GETDATE(), GETDATE()),
    (1007, 'Ethan', 'Hunt', 'ethan.h@email.com', '555-0107', '147 Birch Way', 'Austin', 'TX', '78701', GETDATE(), GETDATE()),
    (1008, 'Fiona', 'Gallagher', 'fiona.g@email.com', '555-0108', '258 Willow Ct', 'Chicago', 'IL', '60601', GETDATE(), GETDATE()),
    (1009, 'George', 'Miller', 'george.m@email.com', '555-0109', '369 Spruce Ave', 'Boston', 'MA', '02101', GETDATE(), GETDATE()),
    (1010, 'Hannah', 'Montana', 'hannah.m@email.com', '555-0110', '741 Ash Blvd', 'Miami', 'FL', '33101', GETDATE(), GETDATE());

PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' customers inserted';

-- ============================================================================
-- Step 2: Insert Sample Products
-- ============================================================================

PRINT '';
PRINT 'Step 2: Inserting Sample Products';
PRINT '----------------------------------';

INSERT INTO dbo.products (product_id, product_name, category, price, stock_quantity, supplier, description, created_date, last_updated)
VALUES
    (2001, 'Laptop Pro 15', 'Electronics', 1299.99, 50, 'Tech Supplies Inc', 'High-performance laptop with 15-inch display', GETDATE(), GETDATE()),
    (2002, 'Wireless Mouse', 'Electronics', 29.99, 200, 'Tech Supplies Inc', 'Ergonomic wireless mouse with USB receiver', GETDATE(), GETDATE()),
    (2003, 'Office Chair', 'Furniture', 249.99, 75, 'Office Depot', 'Ergonomic office chair with lumbar support', GETDATE(), GETDATE()),
    (2004, 'Standing Desk', 'Furniture', 599.99, 30, 'Office Depot', 'Adjustable height standing desk', GETDATE(), GETDATE()),
    (2005, 'Monitor 27 inch', 'Electronics', 349.99, 100, 'Tech Supplies Inc', '4K UHD monitor with HDR support', GETDATE(), GETDATE()),
    (2006, 'Keyboard Mechanical', 'Electronics', 89.99, 150, 'Tech Supplies Inc', 'RGB mechanical gaming keyboard', GETDATE(), GETDATE()),
    (2007, 'Desk Lamp LED', 'Furniture', 39.99, 120, 'Office Depot', 'Adjustable LED desk lamp', GETDATE(), GETDATE()),
    (2008, 'Webcam HD', 'Electronics', 79.99, 80, 'Tech Supplies Inc', '1080p HD webcam with built-in microphone', GETDATE(), GETDATE()),
    (2009, 'Filing Cabinet', 'Furniture', 149.99, 40, 'Office Depot', '3-drawer metal filing cabinet', GETDATE(), GETDATE()),
    (2010, 'Headphones Wireless', 'Electronics', 199.99, 60, 'Tech Supplies Inc', 'Noise-cancelling wireless headphones', GETDATE(), GETDATE());

PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' products inserted';

-- ============================================================================
-- Step 3: Insert Sample Orders
-- ============================================================================

PRINT '';
PRINT 'Step 3: Inserting Sample Orders';
PRINT '--------------------------------';

INSERT INTO dbo.orders (order_id, customer_id, order_date, order_status, total_amount, shipping_address, payment_method, created_date, last_updated)
VALUES
    (5001, 1001, DATEADD(day, -10, GETDATE()), 'PENDING', 1329.98, '123 Main St, Seattle, WA 98101', 'Credit Card', GETDATE(), GETDATE()),
    (5002, 1002, DATEADD(day, -9, GETDATE()), 'PROCESSING', 249.99, '456 Oak Ave, Portland, OR 97201', 'PayPal', GETDATE(), GETDATE()),
    (5003, 1003, DATEADD(day, -8, GETDATE()), 'SHIPPED', 949.98, '789 Pine Rd, San Francisco, CA 94102', 'Credit Card', GETDATE(), GETDATE()),
    (5004, 1001, DATEADD(day, -7, GETDATE()), 'DELIVERED', 29.99, '123 Main St, Seattle, WA 98101', 'Debit Card', GETDATE(), GETDATE()),
    (5005, 1004, DATEADD(day, -6, GETDATE()), 'PENDING', 599.99, '321 Elm St, Los Angeles, CA 90001', 'Credit Card', GETDATE(), GETDATE()),
    (5006, 1005, DATEADD(day, -5, GETDATE()), 'PROCESSING', 89.99, '654 Maple Dr, Denver, CO 80201', 'PayPal', GETDATE(), GETDATE()),
    (5007, 1006, DATEADD(day, -4, GETDATE()), 'SHIPPED', 279.98, '987 Cedar Ln, Phoenix, AZ 85001', 'Credit Card', GETDATE(), GETDATE()),
    (5008, 1007, DATEADD(day, -3, GETDATE()), 'DELIVERED', 1299.99, '147 Birch Way, Austin, TX 78701', 'Credit Card', GETDATE(), GETDATE()),
    (5009, 1008, DATEADD(day, -2, GETDATE()), 'PENDING', 349.99, '258 Willow Ct, Chicago, IL 60601', 'Debit Card', GETDATE(), GETDATE()),
    (5010, 1009, DATEADD(day, -1, GETDATE()), 'PROCESSING', 199.99, '369 Spruce Ave, Boston, MA 02101', 'PayPal', GETDATE(), GETDATE()),
    (5011, 1010, GETDATE(), 'PENDING', 429.98, '741 Ash Blvd, Miami, FL 33101', 'Credit Card', GETDATE(), GETDATE()),
    (5012, 1002, GETDATE(), 'PENDING', 79.99, '456 Oak Ave, Portland, OR 97201', 'Credit Card', GETDATE(), GETDATE()),
    (5013, 1003, GETDATE(), 'PROCESSING', 149.99, '789 Pine Rd, San Francisco, CA 94102', 'PayPal', GETDATE(), GETDATE()),
    (5014, 1005, GETDATE(), 'PENDING', 39.99, '654 Maple Dr, Denver, CO 80201', 'Debit Card', GETDATE(), GETDATE()),
    (5015, 1007, GETDATE(), 'PROCESSING', 849.97, '147 Birch Way, Austin, TX 78701', 'Credit Card', GETDATE(), GETDATE());

PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' orders inserted';

-- ============================================================================
-- Step 4: Verify Data Load
-- ============================================================================

PRINT '';
PRINT '============================================================================';
PRINT 'Data Load Summary';
PRINT '============================================================================';
PRINT '';

-- Record counts
SELECT 'customers' AS table_name, COUNT(*) AS record_count FROM dbo.customers
UNION ALL
SELECT 'orders', COUNT(*) FROM dbo.orders
UNION ALL
SELECT 'products', COUNT(*) FROM dbo.products
ORDER BY table_name;

PRINT '';

-- Sample data preview
PRINT 'Sample Customers:';
SELECT TOP 5 
    customer_id, 
    first_name, 
    last_name, 
    email, 
    city, 
    state
FROM dbo.customers
ORDER BY customer_id;

PRINT '';
PRINT 'Sample Products:';
SELECT TOP 5 
    product_id, 
    product_name, 
    category, 
    price, 
    stock_quantity
FROM dbo.products
ORDER BY product_id;

PRINT '';
PRINT 'Sample Orders:';
SELECT TOP 5 
    order_id, 
    customer_id, 
    order_date, 
    order_status, 
    total_amount
FROM dbo.orders
ORDER BY order_id;

PRINT '';
PRINT '✓ Sample data loaded successfully!';
PRINT '';
PRINT 'Data Summary:';
PRINT '  - 10 customers across multiple US states';
PRINT '  - 10 products in Electronics and Furniture categories';
PRINT '  - 15 orders with various statuses';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Run enable_cdc.sql to enable Change Data Capture';
PRINT '  2. Configure Lakeflow Connect in Databricks';
PRINT '  3. Test CDC with test_crud_operations.sql';
PRINT '';
GO
