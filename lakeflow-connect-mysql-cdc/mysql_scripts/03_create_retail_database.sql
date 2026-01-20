-- ============================================================================
-- Create Retail Database Schema
-- Purpose: Create retail_db database with customers, orders, products tables
-- Prerequisites: CREATE DATABASE and CREATE TABLE privileges
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Database
-- ============================================================================

-- Create the retail database with UTF-8 character set
CREATE DATABASE IF NOT EXISTS retail_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci
    COMMENT 'Retail operations database for CDC workshop';

-- Switch to the retail database
USE retail_db;

-- Verify database creation
SELECT 
    SCHEMA_NAME,
    DEFAULT_CHARACTER_SET_NAME,
    DEFAULT_COLLATION_NAME
FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME = 'retail_db';

-- ============================================================================
-- STEP 2: Create Customers Table (Parent Table)
-- ============================================================================

-- Drop table if it exists (for clean re-creation)
DROP TABLE IF EXISTS customers;

-- Create customers table
CREATE TABLE customers (
    -- Primary key
    customer_id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'Unique customer identifier',
    
    -- Customer information
    first_name VARCHAR(50) NOT NULL COMMENT 'Customer first name',
    last_name VARCHAR(50) NOT NULL COMMENT 'Customer last name',
    email VARCHAR(100) UNIQUE NOT NULL COMMENT 'Customer email address (unique)',
    phone VARCHAR(20) COMMENT 'Customer phone number',
    
    -- Address information
    address VARCHAR(200) COMMENT 'Street address',
    city VARCHAR(50) COMMENT 'City',
    state VARCHAR(2) COMMENT 'State code (2 letter abbreviation)',
    zip_code VARCHAR(10) COMMENT 'ZIP/postal code',
    
    -- Audit fields
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation timestamp',
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update timestamp',
    
    -- Indexes for performance
    INDEX idx_email (email),
    INDEX idx_state (state),
    INDEX idx_city_state (city, state),
    INDEX idx_last_name (last_name)
    
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Customer master table';

-- ============================================================================
-- STEP 3: Create Products Table (Independent Table)
-- ============================================================================

-- Drop table if it exists
DROP TABLE IF EXISTS products;

-- Create products table
CREATE TABLE products (
    -- Primary key
    product_id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'Unique product identifier',
    
    -- Product information
    product_name VARCHAR(100) NOT NULL COMMENT 'Product name',
    category VARCHAR(50) COMMENT 'Product category',
    price DECIMAL(10, 2) NOT NULL COMMENT 'Product price (USD)',
    stock_quantity INT DEFAULT 0 COMMENT 'Current stock quantity',
    supplier VARCHAR(100) COMMENT 'Supplier name',
    description TEXT COMMENT 'Product description',
    
    -- Audit fields
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation timestamp',
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update timestamp',
    
    -- Indexes for performance
    INDEX idx_category (category),
    INDEX idx_supplier (supplier),
    INDEX idx_product_name (product_name),
    INDEX idx_price (price)
    
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Product catalog table';

-- ============================================================================
-- STEP 4: Create Orders Table (Child Table with FK to Customers)
-- ============================================================================

-- Drop table if it exists
DROP TABLE IF EXISTS orders;

-- Create orders table
CREATE TABLE orders (
    -- Primary key
    order_id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'Unique order identifier',
    
    -- Foreign key to customers
    customer_id INT NOT NULL COMMENT 'Customer who placed the order',
    
    -- Order information
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Order placement date',
    order_status VARCHAR(20) DEFAULT 'PENDING' COMMENT 'Order status: PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED',
    total_amount DECIMAL(10, 2) NOT NULL COMMENT 'Total order amount (USD)',
    shipping_address VARCHAR(200) COMMENT 'Shipping address',
    payment_method VARCHAR(50) COMMENT 'Payment method: Credit Card, PayPal, Debit Card, etc.',
    
    -- Audit fields
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation timestamp',
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update timestamp',
    
    -- Foreign key constraint
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        ON DELETE RESTRICT 
        ON UPDATE CASCADE,
    
    -- Indexes for performance
    INDEX idx_customer_id (customer_id),
    INDEX idx_order_date (order_date),
    INDEX idx_order_status (order_status),
    INDEX idx_customer_date (customer_id, order_date)
    
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Customer orders table';

-- ============================================================================
-- STEP 5: Verify Table Creation
-- ============================================================================

-- Show all tables in retail_db
SHOW TABLES;

-- Describe table structures
DESCRIBE customers;
DESCRIBE orders;
DESCRIBE products;

-- Verify table metadata
SELECT 
    TABLE_NAME,
    ENGINE,
    TABLE_ROWS,
    AVG_ROW_LENGTH,
    DATA_LENGTH,
    INDEX_LENGTH,
    TABLE_COLLATION,
    TABLE_COMMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'retail_db'
ORDER BY TABLE_NAME;

-- ============================================================================
-- STEP 6: Verify Indexes
-- ============================================================================

-- Check indexes on customers table
SHOW INDEX FROM customers;

-- Check indexes on orders table
SHOW INDEX FROM orders;

-- Check indexes on products table
SHOW INDEX FROM products;

-- ============================================================================
-- STEP 7: Verify Foreign Key Constraints
-- ============================================================================

-- View foreign key constraints
SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME,
    UPDATE_RULE,
    DELETE_RULE
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = 'retail_db'
  AND REFERENCED_TABLE_NAME IS NOT NULL;

-- Expected: orders.customer_id → customers.customer_id (DELETE RESTRICT, UPDATE CASCADE)

-- ============================================================================
-- STEP 8: Test Table Relationships
-- ============================================================================

-- Test 1: Insert a customer (should succeed)
INSERT INTO customers (first_name, last_name, email, city, state)
VALUES ('Test', 'User', 'test.user@email.com', 'Test City', 'CA');

-- Get the customer_id
SET @test_customer_id = LAST_INSERT_ID();
SELECT @test_customer_id as test_customer_id;

-- Test 2: Insert an order for the test customer (should succeed)
INSERT INTO orders (customer_id, order_status, total_amount, payment_method)
VALUES (@test_customer_id, 'PENDING', 100.00, 'Credit Card');

-- Test 3: Verify foreign key relationship
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    o.order_id,
    o.order_status,
    o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = @test_customer_id;

-- Test 4: Try to insert order with invalid customer_id (should fail)
-- Uncomment to test (this will fail with FK constraint violation):
-- INSERT INTO orders (customer_id, order_status, total_amount, payment_method)
-- VALUES (99999, 'PENDING', 50.00, 'PayPal');

-- Test 5: Try to delete customer with orders (should fail due to RESTRICT)
-- Uncomment to test (this will fail with FK constraint violation):
-- DELETE FROM customers WHERE customer_id = @test_customer_id;

-- Clean up test data
DELETE FROM orders WHERE customer_id = @test_customer_id;
DELETE FROM customers WHERE customer_id = @test_customer_id;

-- ============================================================================
-- STEP 9: Grant Permissions to CDC User
-- ============================================================================

-- Grant SELECT privilege to CDC user on all retail_db tables
-- (Run this as a user with GRANT privilege)
GRANT SELECT ON retail_db.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

-- Verify grants
SHOW GRANTS FOR 'cdc_user'@'%';

-- ============================================================================
-- STEP 10: Additional Useful Queries
-- ============================================================================

-- View table sizes
SELECT 
    TABLE_NAME,
    ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS 'Size (MB)',
    TABLE_ROWS,
    ROUND(DATA_LENGTH / TABLE_ROWS, 2) AS 'Avg Row Size (Bytes)'
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'retail_db'
ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;

-- View all constraints
SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    CONSTRAINT_TYPE
FROM information_schema.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'retail_db'
ORDER BY TABLE_NAME, CONSTRAINT_TYPE;

-- ============================================================================
-- Notes and Best Practices
-- ============================================================================

/*
1. Engine Choice:
   - InnoDB is required for foreign key support
   - InnoDB provides ACID transactions and row-level locking
   - InnoDB is the default engine for MySQL 5.5+

2. Character Set:
   - utf8mb4 supports full Unicode including emoji
   - utf8mb4_unicode_ci provides case-insensitive comparisons
   - Required for international character support

3. Primary Keys:
   - AUTO_INCREMENT provides automatic sequential IDs
   - INT is sufficient for most applications (up to 2 billion records)
   - Consider BIGINT for very large tables (up to 9 quintillion records)

4. Foreign Keys:
   - ON DELETE RESTRICT prevents orphaned child records
   - ON UPDATE CASCADE automatically updates child records
   - Ensures referential integrity at database level

5. Indexes:
   - Primary key automatically creates clustered index
   - Add indexes on foreign keys for join performance
   - Add indexes on columns used in WHERE, ORDER BY, GROUP BY
   - Too many indexes can slow down INSERT/UPDATE/DELETE

6. Audit Fields:
   - created_date tracks record creation
   - last_updated automatically updates on any change
   - Essential for troubleshooting and audit trails

7. Data Types:
   - VARCHAR for variable-length strings (use appropriate length)
   - DECIMAL for monetary values (avoids floating-point precision issues)
   - DATETIME for timestamps (includes date and time)
   - TEXT for large text fields

8. Comments:
   - Document table and column purposes
   - Helps team members understand schema
   - Shows in DESCRIBE and information_schema queries
*/

-- ============================================================================
-- Troubleshooting
-- ============================================================================

/*
Issue: "Can't create database 'retail_db'; database exists"
Solution: Drop database first: DROP DATABASE retail_db; or use IF NOT EXISTS

Issue: "Foreign key constraint fails"
Solution: Ensure parent table exists before creating child table, ensure referenced column has index

Issue: "Table doesn't exist" after creation
Solution: Verify you're connected to correct database: SELECT DATABASE();

Issue: AUTO_INCREMENT not working
Solution: Ensure column is PRIMARY KEY or has UNIQUE index

Issue: Character set errors
Solution: Verify MySQL server supports utf8mb4, check my.cnf configuration
*/

-- ============================================================================
-- Schema Summary
-- ============================================================================

SELECT 
    'Schema Creation Complete' as status,
    COUNT(*) as table_count,
    GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME) as tables_created
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'retail_db';

-- Ready for sample data insertion (use 04_insert_sample_data.sql)
