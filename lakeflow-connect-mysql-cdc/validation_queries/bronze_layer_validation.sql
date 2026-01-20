-- ============================================================================
-- Bronze Layer Validation Queries
-- Purpose: Validate data quality, consistency, and CDC metadata in bronze tables
-- Use these queries to verify CDC pipeline is working correctly
-- ============================================================================

-- ============================================================================
-- 1. RECORD COUNT VALIDATION
-- ============================================================================

-- Compare record counts across all bronze tables
SELECT 
    'customers' as table_name, 
    COUNT(*) as record_count,
    MIN(_commit_timestamp) as earliest_change,
    MAX(_commit_timestamp) as latest_change
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
    'orders', 
    COUNT(*),
    MIN(_commit_timestamp),
    MAX(_commit_timestamp)
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
    'products', 
    COUNT(*),
    MIN(_commit_timestamp),
    MAX(_commit_timestamp)
FROM retail_analytics.bronze.products

ORDER BY table_name;

-- ============================================================================
-- 2. REFERENTIAL INTEGRITY VALIDATION
-- ============================================================================

-- Check for orphaned orders (orders without valid customer references)
SELECT 
    'Orphaned Orders Check' as validation_type,
    COUNT(*) as issue_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS - No orphaned orders'
        ELSE CONCAT('FAIL - ', COUNT(*), ' orphaned orders found')
    END as status
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- List any orphaned orders (if found)
SELECT 
    o.order_id,
    o.customer_id as invalid_customer_id,
    o.order_status,
    o.total_amount,
    o._commit_timestamp
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
LIMIT 10;

-- ============================================================================
-- 3. NULL VALUE CHECKS
-- ============================================================================

-- Check for NULL values in critical fields
SELECT 
    'NULL Value Check - Customers' as validation_type,
    'customer_id' as column_name,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_count,
    CASE 
        WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
    'NULL Value Check - Customers',
    'email',
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END),
    CASE 
        WHEN SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
    'NULL Value Check - Orders',
    'order_id',
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END),
    CASE 
        WHEN SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
    'NULL Value Check - Orders',
    'customer_id',
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
    CASE 
        WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
    'NULL Value Check - Products',
    'product_id',
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END),
    CASE 
        WHEN SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM retail_analytics.bronze.products;

-- ============================================================================
-- 4. DUPLICATE CHECK
-- ============================================================================

-- Check for duplicate customer emails
SELECT 
    'Duplicate Email Check' as validation_type,
    COUNT(*) as duplicate_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS - No duplicate emails'
        ELSE CONCAT('WARNING - ', COUNT(*), ' duplicate emails found')
    END as status
FROM (
    SELECT email, COUNT(*) as count
    FROM retail_analytics.bronze.customers
    GROUP BY email
    HAVING COUNT(*) > 1
);

-- List duplicate emails (if any)
SELECT 
    email,
    COUNT(*) as occurrence_count,
    GROUP_CONCAT(customer_id ORDER BY customer_id) as customer_ids
FROM retail_analytics.bronze.customers
GROUP BY email
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- ============================================================================
-- 5. CDC METADATA VALIDATION
-- ============================================================================

-- Verify _commit_timestamp is populated
SELECT 
    '_commit_timestamp Check - Customers' as validation_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps,
    CASE 
        WHEN SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
    '_commit_timestamp Check - Orders',
    COUNT(*),
    SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END),
    CASE 
        WHEN SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
    '_commit_timestamp Check - Products',
    COUNT(*),
    SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END),
    CASE 
        WHEN SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END
FROM retail_analytics.bronze.products;

-- Check timestamp distribution
SELECT 
    DATE_TRUNC('HOUR', _commit_timestamp) as hour,
    COUNT(*) as change_count
FROM retail_analytics.bronze.customers
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY DATE_TRUNC('HOUR', _commit_timestamp)
ORDER BY hour DESC;

-- ============================================================================
-- 6. DATA TYPE AND RANGE VALIDATION
-- ============================================================================

-- Check for invalid order amounts (negative or zero)
SELECT 
    'Order Amount Validation' as validation_type,
    COUNT(*) as invalid_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS - All amounts valid'
        ELSE CONCAT('WARNING - ', COUNT(*), ' orders with invalid amounts')
    END as status
FROM retail_analytics.bronze.orders
WHERE total_amount <= 0;

-- Check for negative prices
SELECT 
    'Product Price Validation' as validation_type,
    COUNT(*) as invalid_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS - All prices valid'
        ELSE CONCAT('FAIL - ', COUNT(*), ' products with negative prices')
    END as status
FROM retail_analytics.bronze.products
WHERE price < 0;

-- Check for negative stock quantities
SELECT 
    'Stock Quantity Validation' as validation_type,
    COUNT(*) as invalid_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS - All stock quantities valid'
        ELSE CONCAT('WARNING - ', COUNT(*), ' products with negative stock')
    END as status
FROM retail_analytics.bronze.products
WHERE stock_quantity < 0;

-- ============================================================================
-- 7. DELTA TABLE HISTORY VALIDATION
-- ============================================================================

-- Check customers table history
DESCRIBE HISTORY retail_analytics.bronze.customers
LIMIT 10;

-- Check orders table history
DESCRIBE HISTORY retail_analytics.bronze.orders
LIMIT 10;

-- Check products table history
DESCRIBE HISTORY retail_analytics.bronze.products
LIMIT 10;

-- ============================================================================
-- 8. TABLE METADATA VALIDATION
-- ============================================================================

-- Get detailed table metadata
DESCRIBE DETAIL retail_analytics.bronze.customers;
DESCRIBE DETAIL retail_analytics.bronze.orders;
DESCRIBE DETAIL retail_analytics.bronze.products;

-- Check table properties
SELECT 
    'customers' as table_name,
    numFiles as file_count,
    sizeInBytes / 1024 / 1024 as size_mb,
    format
FROM (DESCRIBE DETAIL retail_analytics.bronze.customers)

UNION ALL

SELECT 
    'orders',
    numFiles,
    sizeInBytes / 1024 / 1024,
    format
FROM (DESCRIBE DETAIL retail_analytics.bronze.orders)

UNION ALL

SELECT 
    'products',
    numFiles,
    sizeInBytes / 1024 / 1024,
    format
FROM (DESCRIBE DETAIL retail_analytics.bronze.products);

-- ============================================================================
-- 9. JOIN VALIDATION (Multi-Table Consistency)
-- ============================================================================

-- Validate customer-order relationships
SELECT 
    'Customer-Order Join Validation' as validation_type,
    COUNT(DISTINCT o.customer_id) as customers_with_orders,
    COUNT(o.order_id) as total_orders,
    CASE 
        WHEN COUNT(o.order_id) > 0 THEN 'PASS - Orders properly joined'
        ELSE 'WARNING - No orders joined'
    END as status
FROM retail_analytics.bronze.orders o
JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id;

-- Check for customers without orders (informational, not necessarily an error)
SELECT 
    'Customers Without Orders' as validation_type,
    COUNT(*) as customer_count
FROM retail_analytics.bronze.customers c
LEFT JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

-- ============================================================================
-- 10. RECENT CHANGES VALIDATION
-- ============================================================================

-- Verify recent inserts (last hour)
SELECT 
    'Recent Inserts - Last Hour' as report_type,
    (
        SELECT COUNT(*) 
        FROM retail_analytics.bronze.customers 
        WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
    ) as customers,
    (
        SELECT COUNT(*) 
        FROM retail_analytics.bronze.orders 
        WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
    ) as orders,
    (
        SELECT COUNT(*) 
        FROM retail_analytics.bronze.products 
        WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
    ) as products;

-- List recent customer changes
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    _commit_timestamp,
    _commit_version
FROM retail_analytics.bronze.customers
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
ORDER BY _commit_timestamp DESC;

-- ============================================================================
-- 11. COMPREHENSIVE VALIDATION SUMMARY
-- ============================================================================

-- Create a validation summary view
CREATE OR REPLACE VIEW retail_analytics.bronze.vw_validation_summary AS
SELECT 
    'Record Counts' as validation_category,
    'customers' as table_name,
    CAST(COUNT(*) AS STRING) as value,
    'INFO' as status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
    'Record Counts',
    'orders',
    CAST(COUNT(*) AS STRING),
    'INFO'
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
    'Record Counts',
    'products',
    CAST(COUNT(*) AS STRING),
    'INFO'
FROM retail_analytics.bronze.products

UNION ALL

SELECT 
    'Referential Integrity',
    'orphaned_orders',
    CAST(COUNT(*) AS STRING),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT 
    'CDC Metadata',
    'null_timestamps_customers',
    CAST(SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END) AS STRING),
    CASE WHEN SUM(CASE WHEN _commit_timestamp IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM retail_analytics.bronze.customers;

-- Query the validation summary
SELECT * FROM retail_analytics.bronze.vw_validation_summary
ORDER BY 
    CASE status 
        WHEN 'FAIL' THEN 1 
        WHEN 'WARNING' THEN 2 
        WHEN 'PASS' THEN 3 
        ELSE 4 
    END,
    validation_category,
    table_name;

-- ============================================================================
-- 12. TIME-BASED VALIDATION
-- ============================================================================

-- Verify data freshness (lag check)
SELECT 
    'Data Freshness Check' as validation_type,
    'customers' as table_name,
    MAX(_commit_timestamp) as latest_change,
    TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as lag_minutes,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'PASS'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN 'WARNING'
        ELSE 'CRITICAL'
    END as status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 
    'Data Freshness Check',
    'orders',
    MAX(_commit_timestamp),
    TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'PASS'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN 'WARNING'
        ELSE 'CRITICAL'
    END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 
    'Data Freshness Check',
    'products',
    MAX(_commit_timestamp),
    TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 30 THEN 'PASS'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) <= 60 THEN 'WARNING'
        ELSE 'CRITICAL'
    END
FROM retail_analytics.bronze.products;

-- ============================================================================
-- END OF BRONZE LAYER VALIDATION QUERIES
-- ============================================================================
