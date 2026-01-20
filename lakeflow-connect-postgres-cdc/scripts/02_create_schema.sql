-- ============================================================================
-- PostgreSQL Schema Creation Script for CDC Workshop
-- ============================================================================
-- This script creates the retail database schema with tables designed for
-- Change Data Capture demonstration.
--
-- Prerequisites:
-- - PostgreSQL 10 or later
-- - Completed 01_postgresql_configuration.sql
-- - Logical replication enabled (wal_level = logical)
--
-- Usage:
--   psql -h your-host -U postgres -d retaildb -f 02_create_schema.sql
-- ============================================================================

-- ============================================================================
-- PART 1: Create Database (if needed)
-- ============================================================================

\echo '================================================'
\echo 'Part 1: Database Setup'
\echo '================================================'
\echo ''

-- Note: CREATE DATABASE must be run in a separate connection
-- Uncomment if database doesn't exist:
-- CREATE DATABASE retaildb;
-- \connect retaildb

\echo 'Connected to database: retaildb'
\echo ''

-- ============================================================================
-- PART 2: Drop Existing Tables (for clean setup)
-- ============================================================================

\echo '================================================'
\echo 'Part 2: Dropping Existing Tables (if any)'
\echo '================================================'
\echo ''

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS orders CASCADE;
\echo 'Dropped table: orders (if existed)'

DROP TABLE IF EXISTS products CASCADE;
\echo 'Dropped table: products (if existed)'

DROP TABLE IF EXISTS customers CASCADE;
\echo 'Dropped table: customers (if existed)'

\echo ''

-- ============================================================================
-- PART 3: Create Customers Table
-- ============================================================================

\echo '================================================'
\echo 'Part 3: Creating Customers Table'
\echo '================================================'
\echo ''

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

\echo 'Created table: customers'

-- Add indexes for performance
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_state ON customers(state);
CREATE INDEX idx_customers_city ON customers(city);
CREATE INDEX idx_customers_last_updated ON customers(last_updated);

\echo 'Created indexes on customers table'

-- Set replica identity to FULL (required for CDC)
ALTER TABLE customers REPLICA IDENTITY FULL;

\echo 'Set REPLICA IDENTITY FULL on customers'

-- Add table comment
COMMENT ON TABLE customers IS 'Customer master data for CDC demonstration';
COMMENT ON COLUMN customers.customer_id IS 'Auto-generated primary key';
COMMENT ON COLUMN customers.email IS 'Unique email address for customer';
COMMENT ON COLUMN customers.last_updated IS 'Timestamp of last update for change tracking';

\echo ''

-- ============================================================================
-- PART 4: Create Products Table
-- ============================================================================

\echo '================================================'
\echo 'Part 4: Creating Products Table'
\echo '================================================'
\echo ''

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price NUMERIC(10, 2) CHECK (price >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    supplier VARCHAR(100),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

\echo 'Created table: products'

-- Add indexes
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_name ON products(product_name);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_last_updated ON products(last_updated);

\echo 'Created indexes on products table'

-- Set replica identity to FULL
ALTER TABLE products REPLICA IDENTITY FULL;

\echo 'Set REPLICA IDENTITY FULL on products'

-- Add table comment
COMMENT ON TABLE products IS 'Product catalog for CDC demonstration';
COMMENT ON COLUMN products.product_id IS 'Auto-generated primary key';
COMMENT ON COLUMN products.price IS 'Product price in USD';
COMMENT ON COLUMN products.stock_quantity IS 'Current inventory quantity';

\echo ''

-- ============================================================================
-- PART 5: Create Orders Table
-- ============================================================================

\echo '================================================'
\echo 'Part 5: Creating Orders Table'
\echo '================================================'
\echo ''

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(20) DEFAULT 'PENDING',
    total_amount NUMERIC(10, 2) CHECK (total_amount >= 0),
    shipping_address VARCHAR(200),
    payment_method VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    
    -- Check constraint for valid order statuses
    CONSTRAINT chk_order_status 
        CHECK (order_status IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED'))
);

\echo 'Created table: orders'

-- Add indexes
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_orders_last_updated ON orders(last_updated);

\echo 'Created indexes on orders table'

-- Set replica identity to FULL
ALTER TABLE orders REPLICA IDENTITY FULL;

\echo 'Set REPLICA IDENTITY FULL on orders'

-- Add table comment
COMMENT ON TABLE orders IS 'Customer orders for CDC demonstration';
COMMENT ON COLUMN orders.order_id IS 'Auto-generated primary key';
COMMENT ON COLUMN orders.customer_id IS 'Foreign key to customers table';
COMMENT ON COLUMN orders.order_status IS 'Current status: PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED';

\echo ''

-- ============================================================================
-- PART 6: Create Publication for CDC
-- ============================================================================

\echo '================================================'
\echo 'Part 6: Creating Publication for Logical Replication'
\echo '================================================'
\echo ''

-- Drop publication if it exists
DROP PUBLICATION IF EXISTS lakeflow_publication;

-- Create publication for all three tables
CREATE PUBLICATION lakeflow_publication 
FOR TABLE customers, orders, products;

\echo 'Created publication: lakeflow_publication'

-- Verify publication
\echo ''
\echo 'Publication Details:'
SELECT * FROM pg_publication WHERE pubname = 'lakeflow_publication';

\echo ''
\echo 'Tables in Publication:'
SELECT 
    pubname,
    schemaname,
    tablename
FROM pg_publication_tables
WHERE pubname = 'lakeflow_publication'
ORDER BY tablename;

\echo ''

-- ============================================================================
-- PART 7: Create Logical Replication Slot
-- ============================================================================

\echo '================================================'
\echo 'Part 7: Creating Logical Replication Slot'
\echo '================================================'
\echo ''

-- Check if slot already exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'lakeflow_slot') THEN
        RAISE NOTICE 'Replication slot already exists: lakeflow_slot';
    ELSE
        -- Create replication slot
        PERFORM pg_create_logical_replication_slot('lakeflow_slot', 'pgoutput');
        RAISE NOTICE 'Created replication slot: lakeflow_slot';
    END IF;
END
$$;

\echo ''
\echo 'Replication Slot Details:'
SELECT 
    slot_name,
    plugin,
    slot_type,
    database,
    active,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'lakeflow_slot';

\echo ''

-- ============================================================================
-- PART 8: Grant Permissions to Replication User
-- ============================================================================

\echo '================================================'
\echo 'Part 8: Granting Permissions to Replication User'
\echo '================================================'
\echo ''

-- Grant SELECT on all tables
GRANT SELECT ON customers TO lakeflow_replication;
GRANT SELECT ON orders TO lakeflow_replication;
GRANT SELECT ON products TO lakeflow_replication;

\echo 'Granted SELECT permissions on all tables'

-- Grant USAGE on sequences (for SERIAL columns)
GRANT USAGE ON SEQUENCE customers_customer_id_seq TO lakeflow_replication;
GRANT USAGE ON SEQUENCE orders_order_id_seq TO lakeflow_replication;
GRANT USAGE ON SEQUENCE products_product_id_seq TO lakeflow_replication;

\echo 'Granted USAGE on sequences'

\echo ''

-- ============================================================================
-- PART 9: Verification and Summary
-- ============================================================================

\echo '================================================'
\echo 'Part 9: Final Verification'
\echo '================================================'
\echo ''

-- List all tables
\echo 'Created Tables:'
\dt

\echo ''
\echo 'Table Details:'

-- Customers table
\echo ''
\echo '1. Customers Table:'
\d customers

-- Products table
\echo ''
\echo '2. Products Table:'
\d products

-- Orders table
\echo ''
\echo '3. Orders Table:'
\d orders

-- Check replica identity
\echo ''
\echo 'Replica Identity Settings:'
SELECT 
    schemaname,
    tablename,
    CASE relreplident
        WHEN 'd' THEN 'DEFAULT'
        WHEN 'f' THEN 'FULL'
        WHEN 'i' THEN 'INDEX'
        WHEN 'n' THEN 'NOTHING'
    END as replica_identity
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_tables t ON c.relname = t.tablename AND n.nspname = t.schemaname
WHERE schemaname = 'public' 
    AND tablename IN ('customers', 'orders', 'products')
ORDER BY tablename;

\echo ''

-- Verification checklist
\echo '================================================'
\echo 'Schema Creation Complete!'
\echo '================================================'
\echo ''
\echo 'Verification Checklist:'
\echo '[✓] customers table created with indexes'
\echo '[✓] products table created with indexes'
\echo '[✓] orders table created with indexes'
\echo '[✓] Foreign key constraint: orders → customers'
\echo '[✓] Replica identity set to FULL on all tables'
\echo '[✓] Publication created: lakeflow_publication'
\echo '[✓] Replication slot created: lakeflow_slot'
\echo '[✓] Permissions granted to lakeflow_replication'
\echo ''
\echo 'Next Steps:'
\echo '1. Run 03_insert_sample_data.sql to populate tables'
\echo '2. Configure Databricks Lakeflow Connect'
\echo '3. Create ingestion pipeline'
\echo '4. Test CDC with 04_cdc_test_operations.sql'
\echo ''
\echo 'Connection Details for Databricks:'
\echo '  Host: your-postgres-host'
\echo '  Port: 5432'
\echo '  Database: retaildb'
\echo '  User: lakeflow_replication'
\echo '  Publication: lakeflow_publication'
\echo '  Replication Slot: lakeflow_slot'
\echo ''
