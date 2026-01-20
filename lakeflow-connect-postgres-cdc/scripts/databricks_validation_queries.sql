-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks CDC Validation Queries
-- MAGIC
-- MAGIC This notebook contains validation and monitoring queries for the Databricks side of your CDC pipeline.
-- MAGIC
-- MAGIC **Usage:** Run these queries in Databricks SQL Warehouse or attached to a cluster

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 1: Table Existence and Schema Validation

-- COMMAND ----------

-- Verify all bronze tables exist
SHOW TABLES IN retail_analytics.bronze;

-- COMMAND ----------

-- Check table schemas
DESCRIBE TABLE retail_analytics.bronze.customers;

-- COMMAND ----------

DESCRIBE TABLE retail_analytics.bronze.orders;

-- COMMAND ----------

DESCRIBE TABLE retail_analytics.bronze.products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 2: Record Count Validation

-- COMMAND ----------

-- Record counts across all tables
SELECT 'customers' as table_name, COUNT(*) as record_count 
FROM retail_analytics.bronze.customers
UNION ALL
SELECT 'orders', COUNT(*) 
FROM retail_analytics.bronze.orders
UNION ALL
SELECT 'products', COUNT(*) 
FROM retail_analytics.bronze.products
ORDER BY table_name;

-- COMMAND ----------

-- Active records only (excluding soft deletes)
SELECT 'customers' as table_name, 
       COUNT(*) as total_records,
       SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END) as deleted_records,
       SUM(CASE WHEN _is_deleted IS NULL OR _is_deleted = FALSE THEN 1 ELSE 0 END) as active_records
FROM retail_analytics.bronze.customers
UNION ALL
SELECT 'orders', COUNT(*),
       SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END),
       SUM(CASE WHEN _is_deleted IS NULL OR _is_deleted = FALSE THEN 1 ELSE 0 END)
FROM retail_analytics.bronze.orders
UNION ALL
SELECT 'products', COUNT(*),
       SUM(CASE WHEN _is_deleted = TRUE THEN 1 ELSE 0 END),
       SUM(CASE WHEN _is_deleted IS NULL OR _is_deleted = FALSE THEN 1 ELSE 0 END)
FROM retail_analytics.bronze.products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 3: CDC Metadata Analysis

-- COMMAND ----------

-- Change type distribution
SELECT 
  'customers' as table_name,
  _change_type,
  COUNT(*) as operation_count
FROM retail_analytics.bronze.customers
GROUP BY _change_type

UNION ALL

SELECT 'orders', _change_type, COUNT(*)
FROM retail_analytics.bronze.orders
GROUP BY _change_type

UNION ALL

SELECT 'products', _change_type, COUNT(*)
FROM retail_analytics.bronze.products
GROUP BY _change_type

ORDER BY table_name, _change_type;

-- COMMAND ----------

-- Data freshness check (latest commit timestamps)
SELECT 
  'customers' as table_name,
  MAX(_commit_timestamp) as latest_commit,
  CURRENT_TIMESTAMP() as check_time,
  TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as minutes_since_last_commit,
  CASE 
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 15 THEN '🟢 FRESH'
    WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 30 THEN '🟡 MODERATE'
    ELSE '🔴 STALE'
  END as freshness_status
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 'orders', MAX(_commit_timestamp), CURRENT_TIMESTAMP(),
       TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
       CASE 
         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 15 THEN '🟢 FRESH'
         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 30 THEN '🟡 MODERATE'
         ELSE '🔴 STALE'
       END
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 'products', MAX(_commit_timestamp), CURRENT_TIMESTAMP(),
       TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()),
       CASE 
         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 15 THEN '🟢 FRESH'
         WHEN TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) < 30 THEN '🟡 MODERATE'
         ELSE '🔴 STALE'
       END
FROM retail_analytics.bronze.products;

-- COMMAND ----------

-- Recent changes (last hour)
SELECT 
  'customers' as table_name,
  _change_type,
  COUNT(*) as changes_last_hour
FROM retail_analytics.bronze.customers
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
GROUP BY _change_type

UNION ALL

SELECT 'orders', _change_type, COUNT(*)
FROM retail_analytics.bronze.orders
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
GROUP BY _change_type

UNION ALL

SELECT 'products', _change_type, COUNT(*)
FROM retail_analytics.bronze.products
WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
GROUP BY _change_type

ORDER BY table_name, _change_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 4: Data Quality Checks

-- COMMAND ----------

-- NULL value analysis
SELECT 'customers' as table_name, 
       SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_emails,
       SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) as null_states,
       SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) as null_phones,
       COUNT(*) as total_records
FROM retail_analytics.bronze.customers

UNION ALL

SELECT 'orders',
       SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END),
       COUNT(*)
FROM retail_analytics.bronze.orders

UNION ALL

SELECT 'products',
       SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END),
       COUNT(*)
FROM retail_analytics.bronze.products;

-- COMMAND ----------

-- Referential integrity check (orphaned orders)
SELECT 
  o.order_id,
  o.customer_id,
  o.order_status,
  o.total_amount,
  'ORPHANED - No matching customer!' as issue
FROM retail_analytics.bronze.orders o
LEFT JOIN retail_analytics.bronze.customers c 
  ON o.customer_id = c.customer_id 
  AND (c._is_deleted IS NULL OR c._is_deleted = FALSE)
WHERE c.customer_id IS NULL
  AND (o._is_deleted IS NULL OR o._is_deleted = FALSE);

-- Should return 0 rows for healthy data

-- COMMAND ----------

-- Duplicate email check
SELECT 
  email,
  COUNT(*) as duplicate_count,
  STRING_AGG(CAST(customer_id AS STRING), ', ') as customer_ids
FROM retail_analytics.bronze.customers
WHERE _is_deleted IS NULL OR _is_deleted = FALSE
GROUP BY email
HAVING COUNT(*) > 1;

-- Should return 0 rows

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 5: Delta Lake Table Health

-- COMMAND ----------

-- Table history (recent versions)
DESCRIBE HISTORY retail_analytics.bronze.customers
LIMIT 10;

-- COMMAND ----------

DESCRIBE HISTORY retail_analytics.bronze.orders
LIMIT 10;

-- COMMAND ----------

DESCRIBE HISTORY retail_analytics.bronze.products
LIMIT 10;

-- COMMAND ----------

-- Table details (size, files, partitions)
DESCRIBE DETAIL retail_analytics.bronze.customers;

-- COMMAND ----------

DESCRIBE DETAIL retail_analytics.bronze.orders;

-- COMMAND ----------

DESCRIBE DETAIL retail_analytics.bronze.products;

-- COMMAND ----------

-- File statistics
SELECT 
  'customers' as table_name,
  COUNT(*) as file_count,
  SUM(size) as total_bytes,
  ROUND(SUM(size) / 1024 / 1024, 2) as total_mb,
  AVG(size) as avg_file_size_bytes
FROM (
  SELECT input_file_name(), COUNT(*) as records, 
         -- Approximate file size (not exact without table stats)
         COUNT(*) * 1000 as size
  FROM retail_analytics.bronze.customers
  GROUP BY input_file_name()
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 6: Business Logic Validation

-- COMMAND ----------

-- Customer-Order summary
SELECT 
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.city,
  c.state,
  COUNT(o.order_id) as total_orders,
  COALESCE(SUM(o.total_amount), 0) as total_spent,
  COALESCE(AVG(o.total_amount), 0) as avg_order_value,
  MAX(o.order_date) as last_order_date
FROM retail_analytics.bronze.customers c
LEFT JOIN retail_analytics.bronze.orders o 
  ON c.customer_id = o.customer_id
  AND (o._is_deleted IS NULL OR o._is_deleted = FALSE)
WHERE (c._is_deleted IS NULL OR c._is_deleted = FALSE)
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state
ORDER BY total_spent DESC
LIMIT 20;

-- COMMAND ----------

-- Order status distribution
SELECT 
  order_status,
  COUNT(*) as order_count,
  SUM(total_amount) as total_revenue,
  ROUND(AVG(total_amount), 2) as avg_order_value,
  MIN(total_amount) as min_amount,
  MAX(total_amount) as max_amount
FROM retail_analytics.bronze.orders
WHERE _is_deleted IS NULL OR _is_deleted = FALSE
GROUP BY order_status
ORDER BY order_count DESC;

-- COMMAND ----------

-- Product category analysis
SELECT 
  category,
  COUNT(*) as product_count,
  ROUND(AVG(price), 2) as avg_price,
  ROUND(MIN(price), 2) as min_price,
  ROUND(MAX(price), 2) as max_price,
  SUM(stock_quantity) as total_inventory
FROM retail_analytics.bronze.products
WHERE _is_deleted IS NULL OR _is_deleted = FALSE
GROUP BY category
ORDER BY product_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 7: Time Travel Queries

-- COMMAND ----------

-- Compare current vs previous version of a customer
-- First, get the customer at current version
SELECT 'CURRENT VERSION' as version_label, 
       customer_id, first_name, last_name, email, address, city, state
FROM retail_analytics.bronze.customers
WHERE customer_id = 1;

-- COMMAND ----------

-- View customer at previous version (change VERSION AS OF number)
SELECT 'VERSION 0' as version_label,
       customer_id, first_name, last_name, email, address, city, state
FROM retail_analytics.bronze.customers VERSION AS OF 0
WHERE customer_id = 1;

-- COMMAND ----------

-- Query data as of specific timestamp
SELECT customer_id, first_name, last_name, email, city
FROM retail_analytics.bronze.customers 
  TIMESTAMP AS OF '2024-01-15 10:00:00'
WHERE customer_id = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 8: Performance Monitoring

-- COMMAND ----------

-- Query execution profile (run after a query)
-- Shows physical plan and execution metrics
EXPLAIN COST
SELECT c.customer_id, c.first_name, c.last_name, COUNT(o.order_id) as orders
FROM retail_analytics.bronze.customers c
JOIN retail_analytics.bronze.orders o ON c.customer_id = o.customer_id
WHERE c.state = 'CA'
GROUP BY c.customer_id, c.first_name, c.last_name;

-- COMMAND ----------

-- Staging volume contents
LIST '/Volumes/retail_analytics/landing/cdc_staging_volume/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Section 9: Health Summary Dashboard

-- COMMAND ----------

-- Comprehensive health summary
WITH 
table_counts AS (
  SELECT 'customers' as table_name, COUNT(*) as total FROM retail_analytics.bronze.customers
  UNION ALL
  SELECT 'orders', COUNT(*) FROM retail_analytics.bronze.orders
  UNION ALL
  SELECT 'products', COUNT(*) FROM retail_analytics.bronze.products
),
freshness AS (
  SELECT 'customers' as table_name, 
         MAX(_commit_timestamp) as latest,
         TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP()) as lag_min
  FROM retail_analytics.bronze.customers
  UNION ALL
  SELECT 'orders', MAX(_commit_timestamp),
         TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
  FROM retail_analytics.bronze.orders
  UNION ALL
  SELECT 'products', MAX(_commit_timestamp),
         TIMESTAMPDIFF(MINUTE, MAX(_commit_timestamp), CURRENT_TIMESTAMP())
  FROM retail_analytics.bronze.products
),
recent_activity AS (
  SELECT 'customers' as table_name, COUNT(*) as changes_last_hour
  FROM retail_analytics.bronze.customers
  WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
  UNION ALL
  SELECT 'orders', COUNT(*)
  FROM retail_analytics.bronze.orders
  WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
  UNION ALL
  SELECT 'products', COUNT(*)
  FROM retail_analytics.bronze.products
  WHERE _commit_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
)
SELECT 
  tc.table_name,
  tc.total as total_records,
  f.latest as latest_commit,
  f.lag_min as minutes_since_last_change,
  ra.changes_last_hour,
  CASE 
    WHEN f.lag_min < 15 THEN '🟢 HEALTHY'
    WHEN f.lag_min < 30 THEN '🟡 WARNING'
    ELSE '🔴 CRITICAL'
  END as health_status
FROM table_counts tc
JOIN freshness f ON tc.table_name = f.table_name
JOIN recent_activity ra ON tc.table_name = ra.table_name
ORDER BY tc.table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation Complete!
-- MAGIC
-- MAGIC Review the results above to ensure:
-- MAGIC - ✅ All tables exist with expected schemas
-- MAGIC - ✅ Record counts match PostgreSQL source
-- MAGIC - ✅ No referential integrity violations
-- MAGIC - ✅ Data is fresh (recent _commit_timestamp)
-- MAGIC - ✅ CDC metadata is populated correctly
-- MAGIC - ✅ Delta Lake tables are healthy
-- MAGIC
-- MAGIC **Next Steps:**
-- MAGIC - Set up automated monitoring with Databricks SQL dashboards
-- MAGIC - Configure alerts for stale data or integrity issues
-- MAGIC - Implement silver/gold layer transformations
