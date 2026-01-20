-- ============================================================================
-- Lakeflow Connect SQL Server CDC Workshop
-- Script: schema_setup.sql
-- Purpose: Create RetailDB database and tables (customers, orders, products)
-- ============================================================================

-- ============================================================================
-- Step 1: Create Database
-- ============================================================================

-- Create the RetailDB database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'RetailDB')
BEGIN
    CREATE DATABASE RetailDB;
    PRINT '✓ Database RetailDB created successfully';
END
ELSE
BEGIN
    PRINT 'ℹ Database RetailDB already exists';
END
GO

-- Switch to the RetailDB database
USE RetailDB;
GO

PRINT '============================================================================';
PRINT 'Creating RetailDB Schema';
PRINT '============================================================================';
GO

-- ============================================================================
-- Step 2: Create Customers Table
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customers' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.customers (
        customer_id INT PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        phone VARCHAR(20),
        address VARCHAR(200),
        city VARCHAR(50),
        state VARCHAR(2),
        zip_code VARCHAR(10),
        created_date DATETIME DEFAULT GETDATE(),
        last_updated DATETIME DEFAULT GETDATE(),
        
        -- Add index for common queries
        INDEX IX_customers_state (state),
        INDEX IX_customers_email (email)
    );
    
    PRINT '✓ Table customers created successfully';
END
ELSE
BEGIN
    PRINT 'ℹ Table customers already exists';
END
GO

-- ============================================================================
-- Step 3: Create Products Table
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'products' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.products (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50),
        price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
        stock_quantity INT DEFAULT 0 CHECK (stock_quantity >= 0),
        supplier VARCHAR(100),
        description TEXT,
        created_date DATETIME DEFAULT GETDATE(),
        last_updated DATETIME DEFAULT GETDATE(),
        
        -- Add index for common queries
        INDEX IX_products_category (category),
        INDEX IX_products_supplier (supplier)
    );
    
    PRINT '✓ Table products created successfully';
END
ELSE
BEGIN
    PRINT 'ℹ Table products already exists';
END
GO

-- ============================================================================
-- Step 4: Create Orders Table
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.orders (
        order_id INT PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATETIME DEFAULT GETDATE(),
        order_status VARCHAR(20) DEFAULT 'PENDING' CHECK (order_status IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
        total_amount DECIMAL(10, 2) CHECK (total_amount >= 0),
        shipping_address VARCHAR(200),
        payment_method VARCHAR(50),
        created_date DATETIME DEFAULT GETDATE(),
        last_updated DATETIME DEFAULT GETDATE(),
        
        -- Foreign key to customers table
        CONSTRAINT FK_orders_customers FOREIGN KEY (customer_id) 
            REFERENCES dbo.customers(customer_id),
        
        -- Add indexes for common queries
        INDEX IX_orders_customer (customer_id),
        INDEX IX_orders_date (order_date),
        INDEX IX_orders_status (order_status)
    );
    
    PRINT '✓ Table orders created successfully';
END
ELSE
BEGIN
    PRINT 'ℹ Table orders already exists';
END
GO

-- ============================================================================
-- Step 5: Verify Schema Creation
-- ============================================================================

PRINT '';
PRINT '============================================================================';
PRINT 'Schema Creation Summary';
PRINT '============================================================================';

-- Check tables created
SELECT 
    t.name AS table_name,
    i.rows AS approximate_row_count,
    (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS column_count,
    (SELECT COUNT(*) FROM sys.indexes idx WHERE idx.object_id = t.object_id AND idx.type > 0) AS index_count
FROM sys.tables t
INNER JOIN sys.sysindexes i ON t.object_id = i.id AND i.indid < 2
WHERE t.schema_id = SCHEMA_ID('dbo')
    AND t.name IN ('customers', 'orders', 'products')
ORDER BY t.name;

PRINT '';
PRINT '✓ Schema setup completed successfully!';
PRINT '';
PRINT 'Next Steps:';
PRINT '  1. Run sample_data.sql to populate tables with test data';
PRINT '  2. Run enable_cdc.sql to enable Change Data Capture';
PRINT '';
GO
