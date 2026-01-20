# Lakehouse Real-Time CDC with Lakeflow Connect - MySQL Edition

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 150 min       | Estimated duration to complete the lab(s). |
| Level           | 200/300       | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active        | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura  | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | Built using Databricks Runtime 14.3 LTS with Lakeflow Connect and Unity Catalog | Specify a product version if applicable. If not, use **N/A**. | 

---

## Description  
This hands-on workshop demonstrates how to build production-grade Change Data Capture (CDC) pipelines using **Databricks Lakeflow Connect** to incrementally ingest data from MySQL databases. Learn how to move beyond traditional full-load ETL patterns and implement real-time data synchronization that captures inserts, updates, and deletes from operational databases.

Across four comprehensive labs, you'll configure MySQL for CDC operations using binary logs, set up Lakeflow Connect ingestion gateways and pipelines, validate incremental data capture, and implement monitoring strategies for production environments. You'll eliminate the complexity of managing separate CDC tools like Debezium and Kafka by leveraging Databricks' native integration.

By the end, you'll have built a fully functional, high-performance CDC pipeline that continuously syncs operational data from MySQL into Delta Lake tables, enabling near-real-time analytics while maintaining complete data lineage and governance through Unity Catalog.

## Learning Objectives
- Configure MySQL databases for Change Data Capture (CDC) by enabling binary logging, row-based replication, and GTID (Global Transaction Identifiers)
- Create and configure Lakeflow Connect ingestion gateways to read binary logs from MySQL and stage changes in Unity Catalog volumes
- Build incremental ingestion pipelines that apply inserts, updates, and deletes from source databases to target Delta Lake tables
- Validate CDC operations by performing CRUD operations on source tables and verifying propagation to Databricks bronze layer tables
- Implement pipeline scheduling and automation strategies for continuous data synchronization with configurable refresh intervals
- Monitor CDC pipeline performance, track data lineage, and troubleshoot common ingestion issues using Databricks observability tools
- Design multi-table CDC architectures that handle dependencies, referential integrity, and parallel ingestion patterns
- Apply best practices for production CDC deployments including error handling, checkpointing, and disaster recovery strategies

## Requirements & Prerequisites  
Before starting this workshop, ensure you have:  
- Access to a **Databricks workspace** with **Unity Catalog** and **Lakeflow Connect** features enabled
- An available **All-purpose compute cluster** (DBR 14.3 LTS or later) and a **SQL Warehouse** (2X-Small minimum)
- **MySQL database** (MySQL 5.7+, MySQL 8.0+, AWS RDS MySQL, or Azure Database for MySQL) with **appropriate permissions** for CDC configuration
- **Network connectivity** between Databricks workspace and MySQL instance (VPN, PrivateLink, or public access)
- **Catalog creation permissions** in Unity Catalog and ability to create connections and volumes
- **Intermediate MySQL knowledge** - Familiar with MySQL syntax, database administration, and binary log concepts
- **Strong SQL skills** - Comfortable writing SELECT, INSERT, UPDATE, DELETE statements and understanding table schemas
- **Basic Python knowledge** - Able to read simple Python code for validation scripts (optional)
- **Understanding of ETL concepts** - Familiarity with data ingestion patterns, staging layers, and bronze/silver/gold architectures

## Contents  
This repository includes: 
- **1 Lecture - Introduction to Change Data Capture and Lakeflow Connect Architecture** notebook
- **2 Lab - Configuring MySQL for CDC Operations** notebook 
- **3 Lab - Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** notebook 
- **4 Lecture - Understanding Ingestion Modes: Snapshot vs. Incremental** notebook 
- **5 Lab - Validating Incremental CDC with CRUD Operations** notebook 
- **6 Lab - Multi-Table CDC and Dependency Management** notebook
- **7 Lecture - Production Best Practices and Monitoring Strategies** notebook
- **8 Lab - Implementing Scheduled Pipelines and Error Handling** notebook
- MySQL configuration scripts
- Sample database schema and test data
- Monitoring and validation queries
- Troubleshooting guide and FAQ

## Workshop Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                         MySQL (Source)                           │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  retail_db                                                  │ │
│  │  ├─ customers Table    (Binary Log Enabled)                │ │
│  │  ├─ orders Table       (Binary Log Enabled)                │ │
│  │  └─ products Table     (Binary Log Enabled)                │ │
│  │                                                              │ │
│  │  Binary Log (binlog) ──► Row-Based Replication Events      │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                               │
                               │ Read Binary Logs
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Lakehouse Platform                 │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           Lakeflow Connect Ingestion Gateway             │   │
│  │  (Dedicated Compute VM - Reads CDC Changes)             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                               │                                  │
│                               │ Write Staged Changes             │
│                               ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         Unity Catalog Volume (Staging Layer)             │   │
│  │  Stores: Inserts, Updates, Deletes in parquet format    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                               │                                  │
│                               │ Apply Changes                    │
│                               ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │         Lakeflow Connect Ingestion Pipeline              │   │
│  │  (Delta Live Tables - Applies CDC Operations)           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                               │                                  │
│                               │ Upsert/Delete Operations         │
│                               ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Bronze Layer (Delta Tables)                 │   │
│  │  ├─ bronze.customers  (Streaming Table)                 │   │
│  │  ├─ bronze.orders     (Streaming Table)                 │   │
│  │  └─ bronze.products   (Streaming Table)                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  Unity Catalog: Governance, Lineage, Access Control             │
└─────────────────────────────────────────────────────────────────┘
```

## Getting Started

### 1. Verify Prerequisites
Before beginning the workshop, confirm you have:
- [ ] Databricks workspace access with Lakeflow Connect enabled
- [ ] MySQL database (5.7+ or 8.0+) with appropriate permissions
- [ ] Network connectivity established between Databricks and MySQL
- [ ] Unity Catalog configured with a target catalog and schema

### 2. Prepare Your MySQL Environment
1. Connect to your MySQL instance using your preferred client (MySQL Workbench, DBeaver, DataGrip, or mysql CLI)
2. Verify you have SUPER or REPLICATION CLIENT/REPLICATION SLAVE privileges
3. Note your server hostname, port (default 3306), database name, and authentication credentials
4. Ensure binary logging is enabled (required for CDC)

### 3. Set Up Unity Catalog Resources
```sql
-- Create catalog and schemas for the workshop
CREATE CATALOG IF NOT EXISTS retail_analytics;

CREATE SCHEMA IF NOT EXISTS retail_analytics.landing
  COMMENT 'Staging area for CDC volumes';

CREATE SCHEMA IF NOT EXISTS retail_analytics.bronze
  COMMENT 'Bronze layer for ingested tables';
```

### 4. Import Workshop Notebooks
1. Clone or download this repository
2. In Databricks workspace, navigate to **Workspace** → **Import**
3. Import all notebooks maintaining the numbered sequence
4. Attach notebooks to your cluster

### 5. Complete Notebooks Sequentially
⚠️ **Critical:** Complete notebooks in order as each builds on previous configurations.

**Phase 1: Foundation (60 min)**
1. Lecture 1 - Review CDC concepts and Lakeflow Connect architecture
2. Lab 2 - Configure MySQL with binary logging, GTID, and row-based replication

**Phase 2: Pipeline Setup (45 min)**
3. Lab 3 - Create ingestion gateway, Unity Catalog connection, and ingestion pipeline
4. Lecture 4 - Understand snapshot vs. incremental ingestion modes

**Phase 3: Validation (30 min)**
5. Lab 5 - Test CDC with INSERT, UPDATE, DELETE operations and verify in Delta tables

**Phase 4: Production Patterns (15 min)**
6. Lab 6 - Handle multi-table ingestion with dependencies
7. Lecture 7 - Production best practices and monitoring
8. Lab 8 - Implement scheduling, error handling, and observability

## Sample Database Schema

The workshop uses a retail database with three core tables:

### customers Table
```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### orders Table
```sql
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(20),
    total_amount DECIMAL(10, 2),
    shipping_address VARCHAR(200),
    payment_method VARCHAR(50),
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```

### products Table
```sql
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    stock_quantity INT,
    supplier VARCHAR(100),
    description TEXT,
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

## Key Features Demonstrated

### MySQL CDC Configuration
- Enabling binary logging (binlog) at database level
- Configuring row-based replication format (BINLOG_FORMAT = ROW)
- Setting up GTID (Global Transaction Identifiers) for consistent replication
- Configuring binary log retention and expiration policies
- Granting replication privileges to CDC user

### Lakeflow Connect Components
- **Ingestion Gateway**: Dedicated VM that reads MySQL binary logs
- **Unity Catalog Volume**: Staging area for captured changes
- **Ingestion Pipeline**: Delta Live Tables job that applies changes to bronze tables
- **Unity Catalog Connection**: Stores credentials and connection metadata

### CDC Operations Captured
- **INSERT**: New records added to source tables (e.g., new customer registration, new order placement)
- **UPDATE**: Modified records with before/after snapshots (e.g., customer address change, order status update)
- **DELETE**: Removed records with soft-delete or hard-delete patterns (e.g., customer account deletion)
- **Schema Changes**: DDL operations captured through schema evolution (e.g., adding new columns using ALTER TABLE)

### Pipeline Modes
- **Snapshot Mode**: Initial full load of source tables
- **Incremental Mode**: Captures only changes since last checkpoint via binlog
- **Continuous Sync**: Scheduled execution for near-real-time updates

## Lab Walkthrough Summary

### Lab 2: MySQL CDC Configuration
You'll execute the following MySQL commands:

```sql
-- Check current binary log status
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'gtid_mode';

-- If binary logging is not enabled, you'll need to modify my.cnf or my.ini:
-- [mysqld]
-- log_bin = mysql-bin
-- binlog_format = ROW
-- gtid_mode = ON
-- enforce_gtid_consistency = ON
-- server_id = 1
-- binlog_expire_logs_seconds = 172800  # 2 days retention
-- 
-- Then restart MySQL service

-- Create CDC user with replication privileges
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'SecurePassword123!';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON retail_db.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

-- Create the retail database
CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;

-- Create tables (see schema above)
CREATE TABLE customers (...);
CREATE TABLE orders (...);
CREATE TABLE products (...);

-- Verify binary log is capturing changes
SHOW BINARY LOGS;
SHOW MASTER STATUS;

-- Insert sample data
INSERT INTO customers (first_name, last_name, email, phone, city, state)
VALUES 
    ('John', 'Doe', 'john.doe@email.com', '555-0100', 'New York', 'NY'),
    ('Sarah', 'Johnson', 'sarah.j@email.com', '555-0101', 'Los Angeles', 'CA'),
    ('Michael', 'Brown', 'michael.b@email.com', '555-0102', 'Chicago', 'IL');

INSERT INTO products (product_name, category, price, stock_quantity, supplier)
VALUES
    ('Laptop Pro', 'Electronics', 1299.99, 50, 'Tech Supply Co'),
    ('Wireless Mouse', 'Electronics', 29.99, 200, 'Tech Supply Co'),
    ('Office Chair', 'Furniture', 249.99, 30, 'Office Depot');

INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
VALUES
    (1, 'PENDING', 1329.98, '123 Main St, New York, NY 10001', 'Credit Card'),
    (2, 'PROCESSING', 29.99, '456 Oak Ave, Los Angeles, CA 90001', 'PayPal'),
    (3, 'SHIPPED', 249.99, '789 Pine Rd, Chicago, IL 60601', 'Credit Card');
```

### Lab 3: Lakeflow Connect Setup
In Databricks UI, you'll:
1. Navigate to **Data Ingestion** → **MySQL Connector**
2. Create **Ingestion Gateway** named `retail_ingestion_gateway`
3. Select staging location: `retail_analytics.landing`
4. Create **Ingestion Pipeline** named `retail_ingestion_pipeline`
5. Create Unity Catalog connection with MySQL credentials:
   - Host: your-mysql-host.com
   - Port: 3306
   - Database: retail_db
   - Username: cdc_user
   - Password: SecurePassword123!
6. Wait 3-5 minutes for gateway VM provisioning
7. Select tables: `customers`, `orders`, `products`
8. Configure destination: `retail_analytics.bronze` schema
9. Save and run pipeline

### Lab 5: Incremental CDC Testing
Execute these operations on MySQL to test CDC:

```sql
-- INSERT: Add new customer
INSERT INTO customers (first_name, last_name, email, phone, city, state)
VALUES ('Jane', 'Smith', 'jane.smith@email.com', '555-0123', 'Seattle', 'WA');

-- UPDATE: Change order status
UPDATE orders
SET order_status = 'SHIPPED'
WHERE order_id = 1;

-- UPDATE: Modify product price
UPDATE products
SET price = 29.99
WHERE product_id = 2;

-- DELETE: Remove customer record (if no FK constraints)
-- First delete related orders or disable FK checks temporarily
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM customers
WHERE customer_id = 999;
SET FOREIGN_KEY_CHECKS = 1;
```

Then run the Lakeflow Connect pipeline and validate changes in Databricks.

## Validation Queries

Use these queries throughout the workshop to verify CDC operations:

```sql
-- Check staging volume contents
LIST 'dbfs:/Volumes/retail_analytics/landing/ingestion_volume/';

-- Verify record counts after initial snapshot
SELECT 'customers' as table_name, COUNT(*) as record_count FROM bronze.customers
UNION ALL
SELECT 'orders', COUNT(*) FROM bronze.orders
UNION ALL
SELECT 'products', COUNT(*) FROM bronze.products;

-- Validate INSERT operation - verify new customer exists
SELECT * FROM bronze.customers 
WHERE email = 'jane.smith@email.com';

-- Validate UPDATE operation - check order status change
SELECT order_id, order_status, last_updated, _commit_timestamp
FROM bronze.orders 
WHERE order_id = 1
ORDER BY _commit_timestamp DESC;

-- Validate UPDATE operation - check product price change
SELECT product_id, product_name, price, _commit_timestamp
FROM bronze.products 
WHERE product_id = 2
ORDER BY _commit_timestamp DESC;

-- Confirm DELETE operation - should return no records
SELECT * FROM bronze.customers 
WHERE customer_id = 999;

-- Check pipeline execution history
SELECT * FROM system.lakeflow.pipeline_events
WHERE pipeline_name = 'retail_ingestion_pipeline'
ORDER BY timestamp DESC
LIMIT 10;

-- View data lineage for a table
DESCRIBE HISTORY bronze.customers;

-- Check table metadata and statistics
DESCRIBE DETAIL bronze.orders;
```

## Troubleshooting Common Issues

### Issue: Binary Logging Not Enabled
**Symptoms**: Cannot create CDC pipeline, error about binary log
**Solutions**:
- Verify binary logging: `SHOW VARIABLES LIKE 'log_bin';`
- Check my.cnf/my.ini configuration file for `log_bin = mysql-bin`
- Ensure `binlog_format = ROW` (statement-based won't work for CDC)
- Restart MySQL service after configuration changes
- For AWS RDS MySQL, enable binary logging in parameter group
- For Azure Database for MySQL, enable binlog in server parameters

### Issue: CDC Not Capturing Changes
**Symptoms**: Ingestion pipeline shows 0 changes after source modifications
**Solutions**:
- Verify binary log format: `SHOW VARIABLES LIKE 'binlog_format';` (should be ROW)
- Check binary log retention: `SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';`
- Ensure GTID is enabled: `SHOW VARIABLES LIKE 'gtid_mode';`
- Verify CDC user has REPLICATION CLIENT and REPLICATION SLAVE privileges
- Check binary log position: `SHOW MASTER STATUS;`
- Validate ingestion gateway is running and connected

### Issue: Ingestion Gateway Connection Failed
**Symptoms**: Gateway shows "Connection Error" status
**Solutions**:
- Verify network connectivity (firewall rules, security groups)
- Test MySQL credentials manually: `mysql -h host -u cdc_user -p`
- Check Unity Catalog connection configuration
- Ensure MySQL allows connections from Databricks IP ranges
- For AWS RDS, check security group inbound rules for port 3306
- For Azure MySQL, check firewall rules and VNet service endpoints
- Verify SSL/TLS requirements if MySQL requires encrypted connections

### Issue: Schema Evolution Not Working
**Symptoms**: Pipeline fails after ALTER TABLE on source
**Solutions**:
- Verify DDL statements are being logged to binary log
- Check schema evolution is enabled in pipeline settings
- Review pipeline logs for schema mismatch errors
- Test with simple ALTER TABLE ADD COLUMN statement first
- Manually sync schema if auto-evolution fails
- Ensure binary log has sufficient retention for schema changes

### Issue: High Ingestion Latency
**Symptoms**: Changes take long time to appear in bronze tables
**Solutions**:
- Increase ingestion gateway VM size
- Optimize ingestion pipeline schedule frequency
- Check binary log size and purging policies: `SHOW BINARY LOGS;`
- Monitor network bandwidth between MySQL and Databricks
- Consider batch size tuning in pipeline configuration
- Check MySQL server load and replication lag
- Optimize MySQL queries with proper indexes

### Issue: Foreign Key Constraint Violations
**Symptoms**: Orders table fails to ingest due to missing customer references
**Solutions**:
- Ensure parent tables (customers) are ingested before child tables (orders)
- Configure pipeline dependencies in multi-table setup
- Use staging area to handle referential integrity
- Consider disabling constraints on bronze layer (data validation in silver layer)
- Handle orphaned records explicitly in transformation logic

### Issue: Replication User Permission Errors
**Symptoms**: Gateway cannot read binary logs, permission denied
**Solutions**:
- Grant proper privileges: `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';`
- Grant SELECT on source database: `GRANT SELECT ON retail_db.* TO 'cdc_user'@'%';`
- Flush privileges: `FLUSH PRIVILEGES;`
- Verify grants: `SHOW GRANTS FOR 'cdc_user'@'%';`
- Check user host pattern matches connecting IP

## Performance Considerations

### MySQL Optimization
- Keep binary log size manageable with appropriate retention policies
- Set `binlog_expire_logs_seconds` to balance retention vs. disk space (default: 2 days)
- Index source tables on frequently queried columns (customer_id, order_id)
- Monitor binary log disk usage: `SHOW BINARY LOGS;`
- Use `PURGE BINARY LOGS` carefully if disk space is limited
- Configure `max_binlog_size` appropriately (default: 1GB)
- Enable `binlog_row_image = MINIMAL` to reduce log size (captures only changed columns)

### Databricks Optimization
- Use Z-ordering on bronze tables for query performance:
```sql
  OPTIMIZE bronze.orders ZORDER BY (customer_id, order_date);
  OPTIMIZE bronze.customers ZORDER BY (customer_id, state);
```
- Set appropriate Auto Optimize settings on Delta tables
- Configure pipeline parallelism for multi-table ingestion
- Monitor staging volume size and implement cleanup policies
- Partition large tables by date for better query performance

### Cost Optimization
- Right-size ingestion gateway VM based on change volume
- Schedule pipeline execution during off-peak hours if possible
- Use spot instances for non-critical ingestion gateways
- Implement lifecycle policies on staging volumes
- Consider incremental refresh frequency vs. cost trade-offs
- Monitor binary log retention to avoid excessive storage costs

## Production Deployment Checklist

Before moving to production:
- [ ] Binary logging configuration reviewed and approved by DBA
- [ ] Row-based replication format (BINLOG_FORMAT = ROW) verified
- [ ] GTID mode enabled for consistent replication tracking
- [ ] CDC user created with minimal required privileges
- [ ] Network security configured with private connectivity
- [ ] Unity Catalog permissions properly restricted
- [ ] Monitoring and alerting configured for pipeline failures
- [ ] Binary log retention policy set appropriately (balance recovery vs. disk space)
- [ ] Error handling and retry logic tested
- [ ] Disaster recovery plan documented (binlog backup strategy)
- [ ] Change volume capacity planning completed
- [ ] Performance benchmarks established (e.g., 10K orders/hour)
- [ ] Data lineage verified in Unity Catalog
- [ ] Documentation updated with runbook procedures
- [ ] Foreign key relationships and dependencies mapped
- [ ] Backup and restore procedures tested

## Use Case Examples

After completing this workshop, you can apply CDC patterns to:

### Real-Time Customer 360
Sync customer data from operational MySQL CRM database to enable real-time customer analytics and personalization

### Order Processing Analytics
Capture order lifecycle changes (placed → processing → shipped → delivered) for real-time fulfillment dashboards

### Inventory Management
Track product stock levels in real-time from MySQL inventory system to trigger alerts and optimize supply chain decisions

### Customer Behavior Analysis
Combine MySQL CDC data with clickstream events to analyze the full customer journey from browse to purchase

### Fraud Detection
Monitor order patterns and customer changes in real-time from MySQL transactional database to identify suspicious activities

## Additional Resources

### Documentation
- [Databricks Lakeflow Connect Guide](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [MySQL Binary Log Documentation](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
- [MySQL Replication Documentation](https://dev.mysql.com/doc/refman/8.0/en/replication.html)
- [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)

### MySQL-Specific Resources
- [MySQL GTID Documentation](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html)
- [AWS RDS MySQL Binary Log Configuration](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.MySQL.BinaryFormat.html)
- [Azure Database for MySQL Replication](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/concepts-read-replicas)

### Video Resources
- End-to-End Lakeflow Connect Tutorial (Reference video in workshop materials)
- MySQL Binary Log and Replication Best Practices
- Unity Catalog Deep Dive

### Community
- [Databricks Community Forums - Lakeflow Connect](https://community.databricks.com/)
- [MySQL Community Forums](https://forums.mysql.com/)

### Related Workshops
- **Lakebase Core Concepts** - PostgreSQL integration with Unity Catalog
- **Delta Live Tables Advanced** - Building complex data pipelines
- **Unity Catalog Governance** - End-to-end data governance

## Expected Outcomes

Upon completing this workshop, you will have:
- ✅ Configured MySQL with production-ready CDC settings using binary logs for retail database
- ✅ Enabled row-based replication and GTID for consistent change tracking
- ✅ Created CDC user with appropriate replication privileges
- ✅ Built a complete Lakeflow Connect pipeline from MySQL source to bronze layer
- ✅ Validated incremental capture of INSERT, UPDATE, and DELETE operations on customers, orders, and products
- ✅ Implemented scheduling and monitoring for continuous synchronization
- ✅ Applied best practices for error handling and pipeline resilience
- ✅ Gained hands-on experience with Unity Catalog governance for CDC pipelines
- ✅ Understood referential integrity handling in multi-table CDC scenarios

## Next Steps

After completing this workshop, consider exploring:
- **Silver Layer Transformations**: Apply business logic and data quality rules
  - Denormalize orders with customer information
  - Calculate order aggregates by customer
  - Clean and standardize address data
- **Gold Layer Aggregations**: Build analytics-ready dimensional models
  - Create customer dimension with SCD Type 2
  - Build order fact table with metrics
  - Develop product hierarchy dimension
- **Multiple Source Integration**: Combine CDC data from PostgreSQL, SQL Server, Oracle
- **Real-Time Analytics**: Query bronze tables while CDC is actively running
  - Real-time order dashboards
  - Live customer segmentation
  - Dynamic inventory alerts
- **Advanced Monitoring**: Implement custom alerts and dashboards for CDC health
- **Stream Processing**: Add real-time aggregations using Spark Structured Streaming

---

**Ready to begin?** Start with notebook **01 - Introduction to Change Data Capture and Lakeflow Connect Architecture** and follow the sequential learning path.

**Questions or Issues?** Refer to the troubleshooting section or post in the Databricks Community Forums.

**Let's Build Real-Time Data Pipelines with MySQL!** 🚀
