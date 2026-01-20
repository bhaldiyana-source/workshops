# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2: Configuring MySQL for CDC Operations
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will configure a MySQL database for Change Data Capture operations. You'll enable binary logging, set up row-based replication, configure GTID, create a CDC user with appropriate privileges, and set up the retail database schema with sample data.
# MAGIC
# MAGIC **Estimated Time**: 30-45 minutes
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Verify and enable MySQL binary logging
# MAGIC - Configure row-based replication format for CDC
# MAGIC - Enable GTID (Global Transaction Identifiers) for consistent replication
# MAGIC - Create a CDC user with minimal required privileges
# MAGIC - Set up the retail database schema (customers, orders, products)
# MAGIC - Insert sample data and verify binary log capture
# MAGIC - Validate MySQL configuration for production CDC operations
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lecture 1 (Introduction to CDC and Lakeflow Connect)
# MAGIC - Access to MySQL server (5.7+ or 8.0+) with admin privileges
# MAGIC - MySQL client (MySQL Workbench, DBeaver, DataGrip, or mysql CLI)
# MAGIC - SUPER or equivalent privileges to modify server configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Verify Current MySQL Configuration
# MAGIC
# MAGIC Before making changes, let's check the current state of your MySQL server.
# MAGIC
# MAGIC ### Step 1.1: Check Binary Logging Status
# MAGIC
# MAGIC Connect to your MySQL server using your preferred client and run:
# MAGIC
# MAGIC ```sql
# MAGIC -- Check if binary logging is enabled
# MAGIC SHOW VARIABLES LIKE 'log_bin';
# MAGIC -- Expected result: 
# MAGIC --   Variable_name | Value
# MAGIC --   log_bin       | ON     (if enabled)
# MAGIC --   log_bin       | OFF    (if disabled - needs configuration)
# MAGIC ```
# MAGIC
# MAGIC ### Step 1.2: Check Binary Log Format
# MAGIC
# MAGIC ```sql
# MAGIC -- Check current binlog format
# MAGIC SHOW VARIABLES LIKE 'binlog_format';
# MAGIC -- Expected result for CDC:
# MAGIC --   Variable_name  | Value
# MAGIC --   binlog_format  | ROW    (required for CDC)
# MAGIC --   binlog_format  | STATEMENT or MIXED (not suitable for CDC)
# MAGIC ```
# MAGIC
# MAGIC ### Step 1.3: Check GTID Status
# MAGIC
# MAGIC ```sql
# MAGIC -- Check if GTID is enabled
# MAGIC SHOW VARIABLES LIKE 'gtid_mode';
# MAGIC -- Expected result:
# MAGIC --   Variable_name | Value
# MAGIC --   gtid_mode     | ON     (recommended for CDC)
# MAGIC --   gtid_mode     | OFF    (will need to enable)
# MAGIC
# MAGIC -- Check GTID consistency enforcement
# MAGIC SHOW VARIABLES LIKE 'enforce_gtid_consistency';
# MAGIC ```
# MAGIC
# MAGIC ### Step 1.4: Document Your Current Configuration
# MAGIC
# MAGIC Record your findings below:

# COMMAND ----------

# Record your current MySQL configuration here
current_config = {
    "mysql_version": "",        # Example: "8.0.35"
    "log_bin": "",              # ON or OFF
    "binlog_format": "",        # ROW, STATEMENT, or MIXED
    "gtid_mode": "",            # ON or OFF
    "server_id": "",            # Server ID number
    "binlog_retention": ""      # Current retention period
}

print("Current MySQL Configuration:")
for key, value in current_config.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Enable Binary Logging (if not already enabled)
# MAGIC
# MAGIC ⚠️ **Important**: Enabling binary logging requires restarting MySQL. Plan for a maintenance window.
# MAGIC
# MAGIC ### Step 2.1: Locate MySQL Configuration File
# MAGIC
# MAGIC **Configuration file locations by platform:**
# MAGIC - **Linux**: `/etc/my.cnf` or `/etc/mysql/my.cnf`
# MAGIC - **Windows**: `C:\ProgramData\MySQL\MySQL Server 8.0\my.ini`
# MAGIC - **macOS**: `/usr/local/mysql/my.cnf`
# MAGIC - **AWS RDS**: Use parameter groups (no direct file access)
# MAGIC - **Azure MySQL**: Server parameters in Azure Portal
# MAGIC
# MAGIC ### Step 2.2: Add Binary Logging Configuration
# MAGIC
# MAGIC Add these settings to the `[mysqld]` section of your configuration file:
# MAGIC
# MAGIC ```ini
# MAGIC [mysqld]
# MAGIC # Binary logging for CDC
# MAGIC log_bin = mysql-bin
# MAGIC binlog_format = ROW                    # Required for CDC
# MAGIC binlog_row_image = MINIMAL             # Optimize log size (only changed columns)
# MAGIC
# MAGIC # GTID configuration
# MAGIC gtid_mode = ON
# MAGIC enforce_gtid_consistency = ON
# MAGIC
# MAGIC # Server identification
# MAGIC server_id = 1                          # Must be unique in replication topology
# MAGIC
# MAGIC # Binary log retention
# MAGIC binlog_expire_logs_seconds = 259200   # 3 days (adjust based on your needs)
# MAGIC
# MAGIC # Binary log file size
# MAGIC max_binlog_size = 1073741824          # 1GB per file
# MAGIC
# MAGIC # Performance tuning
# MAGIC sync_binlog = 1                        # Durability (flush to disk per transaction)
# MAGIC binlog_cache_size = 32768             # Per-connection cache
# MAGIC ```
# MAGIC
# MAGIC ### Step 2.3: Restart MySQL Service
# MAGIC
# MAGIC **Linux (systemd)**:
# MAGIC ```bash
# MAGIC sudo systemctl restart mysql
# MAGIC sudo systemctl status mysql
# MAGIC ```
# MAGIC
# MAGIC **Linux (SysV)**:
# MAGIC ```bash
# MAGIC sudo service mysql restart
# MAGIC sudo service mysql status
# MAGIC ```
# MAGIC
# MAGIC **Windows**:
# MAGIC ```powershell
# MAGIC # Run as Administrator
# MAGIC net stop MySQL80
# MAGIC net start MySQL80
# MAGIC ```
# MAGIC
# MAGIC **AWS RDS**: Modify parameter group, then reboot instance from AWS Console
# MAGIC
# MAGIC **Azure MySQL**: Update server parameters, automatic restart will be triggered
# MAGIC
# MAGIC ### Step 2.4: Verify Binary Logging is Enabled
# MAGIC
# MAGIC After restart, reconnect to MySQL and verify:
# MAGIC
# MAGIC ```sql
# MAGIC -- Verify binary logging
# MAGIC SHOW VARIABLES LIKE 'log_bin';
# MAGIC -- Should now show: ON
# MAGIC
# MAGIC -- Verify ROW format
# MAGIC SHOW VARIABLES LIKE 'binlog_format';
# MAGIC -- Should show: ROW
# MAGIC
# MAGIC -- Verify GTID
# MAGIC SHOW VARIABLES LIKE 'gtid_mode';
# MAGIC -- Should show: ON
# MAGIC
# MAGIC -- Check binary log files
# MAGIC SHOW BINARY LOGS;
# MAGIC -- Should list binary log files with sizes
# MAGIC
# MAGIC -- Check master status
# MAGIC SHOW MASTER STATUS;
# MAGIC -- Shows current binary log file and position
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Create CDC User with Replication Privileges
# MAGIC
# MAGIC Create a dedicated user for CDC operations with minimal required privileges.
# MAGIC
# MAGIC ### Step 3.1: Create CDC User
# MAGIC
# MAGIC ```sql
# MAGIC -- Create CDC user (use a strong password)
# MAGIC CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'SecurePassword123!';
# MAGIC
# MAGIC -- Note: Replace '%' with specific IP/hostname for production
# MAGIC -- Example: 'cdc_user'@'10.0.1.0/24' for specific subnet
# MAGIC -- Example: 'cdc_user'@'databricks-gateway.company.com' for specific host
# MAGIC ```
# MAGIC
# MAGIC ### Step 3.2: Grant Replication Privileges
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant replication privileges (required for reading binary logs)
# MAGIC GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
# MAGIC
# MAGIC -- REPLICATION SLAVE: Allows reading binary log stream
# MAGIC -- REPLICATION CLIENT: Allows SHOW MASTER STATUS and SHOW BINARY LOGS queries
# MAGIC ```
# MAGIC
# MAGIC ### Step 3.3: Grant SELECT Privileges on Source Database
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant SELECT on the database we'll replicate
# MAGIC -- Note: retail_db doesn't exist yet, we'll create it in next task
# MAGIC -- This will work once we create the database
# MAGIC GRANT SELECT ON retail_db.* TO 'cdc_user'@'%';
# MAGIC
# MAGIC -- Apply privilege changes
# MAGIC FLUSH PRIVILEGES;
# MAGIC ```
# MAGIC
# MAGIC ### Step 3.4: Verify User Privileges
# MAGIC
# MAGIC ```sql
# MAGIC -- Check granted privileges
# MAGIC SHOW GRANTS FOR 'cdc_user'@'%';
# MAGIC
# MAGIC -- Expected output:
# MAGIC -- GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%'
# MAGIC -- GRANT SELECT ON `retail_db`.* TO 'cdc_user'@'%'
# MAGIC ```
# MAGIC
# MAGIC ### Step 3.5: Test CDC User Connection
# MAGIC
# MAGIC From a terminal, test the connection:
# MAGIC
# MAGIC ```bash
# MAGIC mysql -h your-mysql-host.com -u cdc_user -p
# MAGIC # Enter password when prompted
# MAGIC
# MAGIC # Once connected, test replication commands:
# MAGIC SHOW MASTER STATUS;
# MAGIC SHOW BINARY LOGS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Create Retail Database Schema
# MAGIC
# MAGIC Now let's create the database and tables for our CDC workshop.
# MAGIC
# MAGIC ### Step 4.1: Create Database
# MAGIC
# MAGIC ```sql
# MAGIC -- Create the retail database
# MAGIC CREATE DATABASE IF NOT EXISTS retail_db
# MAGIC     CHARACTER SET utf8mb4
# MAGIC     COLLATE utf8mb4_unicode_ci;
# MAGIC
# MAGIC -- Switch to the database
# MAGIC USE retail_db;
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.2: Create Customers Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Create customers table
# MAGIC CREATE TABLE customers (
# MAGIC     customer_id INT PRIMARY KEY AUTO_INCREMENT,
# MAGIC     first_name VARCHAR(50) NOT NULL,
# MAGIC     last_name VARCHAR(50) NOT NULL,
# MAGIC     email VARCHAR(100) UNIQUE NOT NULL,
# MAGIC     phone VARCHAR(20),
# MAGIC     address VARCHAR(200),
# MAGIC     city VARCHAR(50),
# MAGIC     state VARCHAR(2),
# MAGIC     zip_code VARCHAR(10),
# MAGIC     created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
# MAGIC     last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
# MAGIC     INDEX idx_state (state),
# MAGIC     INDEX idx_email (email),
# MAGIC     INDEX idx_city_state (city, state)
# MAGIC ) ENGINE=InnoDB;
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.3: Create Products Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Create products table
# MAGIC CREATE TABLE products (
# MAGIC     product_id INT PRIMARY KEY AUTO_INCREMENT,
# MAGIC     product_name VARCHAR(100) NOT NULL,
# MAGIC     category VARCHAR(50),
# MAGIC     price DECIMAL(10, 2) NOT NULL,
# MAGIC     stock_quantity INT DEFAULT 0,
# MAGIC     supplier VARCHAR(100),
# MAGIC     description TEXT,
# MAGIC     created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
# MAGIC     last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
# MAGIC     INDEX idx_category (category),
# MAGIC     INDEX idx_supplier (supplier)
# MAGIC ) ENGINE=InnoDB;
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.4: Create Orders Table
# MAGIC
# MAGIC ```sql
# MAGIC -- Create orders table (with foreign key to customers)
# MAGIC CREATE TABLE orders (
# MAGIC     order_id INT PRIMARY KEY AUTO_INCREMENT,
# MAGIC     customer_id INT NOT NULL,
# MAGIC     order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
# MAGIC     order_status VARCHAR(20) DEFAULT 'PENDING',
# MAGIC     total_amount DECIMAL(10, 2) NOT NULL,
# MAGIC     shipping_address VARCHAR(200),
# MAGIC     payment_method VARCHAR(50),
# MAGIC     created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
# MAGIC     last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
# MAGIC     FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
# MAGIC     INDEX idx_customer_id (customer_id),
# MAGIC     INDEX idx_order_date (order_date),
# MAGIC     INDEX idx_order_status (order_status)
# MAGIC ) ENGINE=InnoDB;
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.5: Verify Table Creation
# MAGIC
# MAGIC ```sql
# MAGIC -- Show all tables in retail_db
# MAGIC SHOW TABLES;
# MAGIC
# MAGIC -- Describe table structures
# MAGIC DESCRIBE customers;
# MAGIC DESCRIBE orders;
# MAGIC DESCRIBE products;
# MAGIC
# MAGIC -- Verify indexes
# MAGIC SHOW INDEX FROM customers;
# MAGIC SHOW INDEX FROM orders;
# MAGIC SHOW INDEX FROM products;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Insert Sample Data
# MAGIC
# MAGIC Let's populate the tables with sample data to test CDC operations.
# MAGIC
# MAGIC ### Step 5.1: Insert Sample Customers
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert customer records
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code)
# MAGIC VALUES 
# MAGIC     ('John', 'Doe', 'john.doe@email.com', '555-0100', '123 Main St', 'New York', 'NY', '10001'),
# MAGIC     ('Sarah', 'Johnson', 'sarah.j@email.com', '555-0101', '456 Oak Ave', 'Los Angeles', 'CA', '90001'),
# MAGIC     ('Michael', 'Brown', 'michael.b@email.com', '555-0102', '789 Pine Rd', 'Chicago', 'IL', '60601'),
# MAGIC     ('Emily', 'Davis', 'emily.d@email.com', '555-0103', '321 Elm St', 'Houston', 'TX', '77001'),
# MAGIC     ('David', 'Wilson', 'david.w@email.com', '555-0104', '654 Maple Dr', 'Phoenix', 'AZ', '85001'),
# MAGIC     ('Jessica', 'Martinez', 'jessica.m@email.com', '555-0105', '987 Cedar Ln', 'Philadelphia', 'PA', '19019'),
# MAGIC     ('James', 'Anderson', 'james.a@email.com', '555-0106', '147 Birch Ct', 'San Antonio', 'TX', '78201'),
# MAGIC     ('Jennifer', 'Taylor', 'jennifer.t@email.com', '555-0107', '258 Spruce Way', 'San Diego', 'CA', '92101'),
# MAGIC     ('Robert', 'Thomas', 'robert.t@email.com', '555-0108', '369 Willow Pl', 'Dallas', 'TX', '75201'),
# MAGIC     ('Linda', 'Moore', 'linda.m@email.com', '555-0109', '741 Ash Blvd', 'San Jose', 'CA', '95101');
# MAGIC
# MAGIC -- Verify insert
# MAGIC SELECT COUNT(*) as customer_count FROM customers;
# MAGIC SELECT * FROM customers LIMIT 5;
# MAGIC ```
# MAGIC
# MAGIC ### Step 5.2: Insert Sample Products
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert product records
# MAGIC INSERT INTO products (product_name, category, price, stock_quantity, supplier, description)
# MAGIC VALUES
# MAGIC     ('Laptop Pro', 'Electronics', 1299.99, 50, 'Tech Supply Co', 'High-performance laptop for professionals'),
# MAGIC     ('Wireless Mouse', 'Electronics', 29.99, 200, 'Tech Supply Co', 'Ergonomic wireless mouse with USB receiver'),
# MAGIC     ('Office Chair', 'Furniture', 249.99, 30, 'Office Depot', 'Ergonomic office chair with lumbar support'),
# MAGIC     ('Standing Desk', 'Furniture', 599.99, 15, 'Office Depot', 'Adjustable height standing desk'),
# MAGIC     ('USB-C Hub', 'Electronics', 49.99, 100, 'Tech Supply Co', '7-in-1 USB-C hub with HDMI and USB ports'),
# MAGIC     ('Desk Lamp', 'Office Supplies', 39.99, 75, 'Lighting Plus', 'LED desk lamp with adjustable brightness'),
# MAGIC     ('Notebook Set', 'Office Supplies', 15.99, 300, 'Paper Goods Inc', 'Set of 3 premium notebooks'),
# MAGIC     ('Wireless Keyboard', 'Electronics', 79.99, 80, 'Tech Supply Co', 'Mechanical wireless keyboard'),
# MAGIC     ('Monitor 27"', 'Electronics', 349.99, 40, 'Tech Supply Co', '4K UHD 27-inch monitor'),
# MAGIC     ('Desk Organizer', 'Office Supplies', 24.99, 150, 'Office Depot', 'Multi-compartment desk organizer'),
# MAGIC     ('Webcam HD', 'Electronics', 89.99, 60, 'Tech Supply Co', '1080p HD webcam with microphone'),
# MAGIC     ('Cable Management', 'Office Supplies', 19.99, 200, 'Office Depot', 'Under-desk cable management tray'),
# MAGIC     ('Ergonomic Footrest', 'Furniture', 45.99, 50, 'Office Depot', 'Adjustable ergonomic footrest'),
# MAGIC     ('Whiteboard Small', 'Office Supplies', 29.99, 90, 'Office Depot', '24x36 inch magnetic whiteboard'),
# MAGIC     ('Printer All-in-One', 'Electronics', 299.99, 25, 'Tech Supply Co', 'Wireless all-in-one printer/scanner');
# MAGIC
# MAGIC -- Verify insert
# MAGIC SELECT COUNT(*) as product_count FROM products;
# MAGIC SELECT * FROM products WHERE category = 'Electronics';
# MAGIC ```
# MAGIC
# MAGIC ### Step 5.3: Insert Sample Orders
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert order records
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
# MAGIC VALUES
# MAGIC     (1, 'PENDING', 1329.98, '123 Main St, New York, NY 10001', 'Credit Card'),
# MAGIC     (2, 'PROCESSING', 29.99, '456 Oak Ave, Los Angeles, CA 90001', 'PayPal'),
# MAGIC     (3, 'SHIPPED', 249.99, '789 Pine Rd, Chicago, IL 60601', 'Credit Card'),
# MAGIC     (4, 'DELIVERED', 599.99, '321 Elm St, Houston, TX 77001', 'Credit Card'),
# MAGIC     (5, 'PENDING', 79.98, '654 Maple Dr, Phoenix, AZ 85001', 'Debit Card'),
# MAGIC     (1, 'PROCESSING', 349.99, '123 Main St, New York, NY 10001', 'Credit Card'),
# MAGIC     (6, 'SHIPPED', 89.99, '987 Cedar Ln, Philadelphia, PA 19019', 'PayPal'),
# MAGIC     (7, 'DELIVERED', 24.99, '147 Birch Ct, San Antonio, TX 78201', 'Credit Card'),
# MAGIC     (8, 'PENDING', 1299.99, '258 Spruce Way, San Diego, CA 92101', 'Credit Card'),
# MAGIC     (2, 'PROCESSING', 119.98, '456 Oak Ave, Los Angeles, CA 90001', 'PayPal');
# MAGIC
# MAGIC -- Verify insert
# MAGIC SELECT COUNT(*) as order_count FROM orders;
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     o.order_status,
# MAGIC     o.total_amount
# MAGIC FROM orders o
# MAGIC JOIN customers c ON o.customer_id = c.customer_id
# MAGIC LIMIT 10;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Verify Binary Log Capture
# MAGIC
# MAGIC Verify that our data operations are being captured in the binary log.
# MAGIC
# MAGIC ### Step 6.1: Check Binary Log Status
# MAGIC
# MAGIC ```sql
# MAGIC -- Check current binary log position
# MAGIC SHOW MASTER STATUS;
# MAGIC -- Note the File and Position values
# MAGIC
# MAGIC -- List all binary log files
# MAGIC SHOW BINARY LOGS;
# MAGIC -- You should see file sizes increasing as data is written
# MAGIC ```
# MAGIC
# MAGIC ### Step 6.2: View Recent Binary Log Events
# MAGIC
# MAGIC ```sql
# MAGIC -- Show recent events in the current binary log
# MAGIC SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 10;
# MAGIC -- Replace 'mysql-bin.000001' with your current binlog file from SHOW MASTER STATUS
# MAGIC
# MAGIC -- Look for:
# MAGIC -- - Table_map events (showing which table is being modified)
# MAGIC -- - Write_rows events (INSERT operations)
# MAGIC -- - Update_rows events (UPDATE operations)
# MAGIC -- - Delete_rows events (DELETE operations)
# MAGIC ```
# MAGIC
# MAGIC ### Step 6.3: Verify Row-Based Logging
# MAGIC
# MAGIC Perform a test update and verify it's captured:
# MAGIC
# MAGIC ```sql
# MAGIC -- Get current binlog position
# MAGIC SHOW MASTER STATUS;
# MAGIC -- Note the Position value
# MAGIC
# MAGIC -- Perform a test update
# MAGIC UPDATE customers 
# MAGIC SET city = 'Brooklyn' 
# MAGIC WHERE customer_id = 1;
# MAGIC
# MAGIC -- Check binlog position again
# MAGIC SHOW MASTER STATUS;
# MAGIC -- Position should have increased, confirming the change was logged
# MAGIC
# MAGIC -- Verify the update
# MAGIC SELECT customer_id, first_name, last_name, city 
# MAGIC FROM customers 
# MAGIC WHERE customer_id = 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7: Production Readiness Validation
# MAGIC
# MAGIC Run these validation queries to confirm your MySQL instance is production-ready for CDC.
# MAGIC
# MAGIC ### Validation Checklist
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Binary logging enabled with ROW format
# MAGIC SELECT 
# MAGIC     @@log_bin as binary_logging_enabled,
# MAGIC     @@binlog_format as binlog_format,
# MAGIC     @@gtid_mode as gtid_enabled,
# MAGIC     @@server_id as server_id,
# MAGIC     @@binlog_expire_logs_seconds as retention_seconds,
# MAGIC     @@binlog_expire_logs_seconds / 3600 as retention_hours;
# MAGIC -- Expected: log_bin=1, binlog_format=ROW, gtid_mode=ON
# MAGIC
# MAGIC -- 2. CDC user exists with correct privileges
# MAGIC SHOW GRANTS FOR 'cdc_user'@'%';
# MAGIC -- Should show REPLICATION SLAVE, REPLICATION CLIENT, SELECT on retail_db
# MAGIC
# MAGIC -- 3. All tables created successfully
# MAGIC SELECT 
# MAGIC     TABLE_NAME,
# MAGIC     ENGINE,
# MAGIC     TABLE_ROWS,
# MAGIC     AUTO_INCREMENT,
# MAGIC     CREATE_TIME
# MAGIC FROM information_schema.TABLES
# MAGIC WHERE TABLE_SCHEMA = 'retail_db'
# MAGIC ORDER BY TABLE_NAME;
# MAGIC -- Should show customers, orders, products with InnoDB engine
# MAGIC
# MAGIC -- 4. Binary logs are being created
# MAGIC SHOW BINARY LOGS;
# MAGIC -- Should list at least one binary log file
# MAGIC
# MAGIC -- 5. Sample data loaded
# MAGIC SELECT 
# MAGIC     'customers' as table_name, COUNT(*) as row_count FROM customers
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*) FROM products
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*) FROM orders;
# MAGIC -- Should show at least 10 customers, 15 products, 10 orders
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8: Document Configuration
# MAGIC
# MAGIC Record your MySQL configuration details for use in the next lab.

# COMMAND ----------

# Document your MySQL configuration for Lakeflow Connect setup
mysql_config = {
    "hostname": "",                 # Example: "mysql-prod.company.com"
    "port": 3306,                   # Default MySQL port
    "database": "retail_db",
    "cdc_username": "cdc_user",
    "cdc_password": "",             # Store securely - will use Databricks Secrets
    "binlog_format": "ROW",
    "gtid_enabled": True,
    "binary_log_retention_hours": 72,  # 3 days
    "network_connectivity": "",     # Example: "VPN", "PrivateLink", "Public"
    "ssl_required": False           # Set to True if SSL is required
}

print("MySQL Configuration for Lakeflow Connect:")
print("-" * 50)
for key, value in mysql_config.items():
    if key != "cdc_password":  # Don't print password
        print(f"{key:30s}: {value}")
    else:
        print(f"{key:30s}: ********")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ **MySQL CDC Configuration**:
# MAGIC - Enabled binary logging with ROW format
# MAGIC - Configured GTID for consistent replication
# MAGIC - Set appropriate binary log retention (3 days)
# MAGIC
# MAGIC ✅ **Security Setup**:
# MAGIC - Created CDC user with minimal required privileges
# MAGIC - Configured REPLICATION SLAVE and REPLICATION CLIENT grants
# MAGIC - Granted SELECT access only to retail_db
# MAGIC
# MAGIC ✅ **Database Schema**:
# MAGIC - Created retail_db database
# MAGIC - Created customers, orders, products tables
# MAGIC - Established foreign key relationships
# MAGIC - Added appropriate indexes for performance
# MAGIC
# MAGIC ✅ **Sample Data**:
# MAGIC - Inserted 10+ customer records
# MAGIC - Inserted 15+ product records
# MAGIC - Inserted 10+ order records with relationships
# MAGIC
# MAGIC ✅ **Validation**:
# MAGIC - Verified binary log capture of data changes
# MAGIC - Confirmed row-based replication is working
# MAGIC - Validated configuration is production-ready
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Binary logging with ROW format is essential** for CDC - it captures before/after values for every change
# MAGIC
# MAGIC 2. **GTID simplifies failover** by providing consistent transaction identifiers across MySQL servers
# MAGIC
# MAGIC 3. **Retention policy is critical** - set binary log retention to at least 2x your maximum expected downtime
# MAGIC
# MAGIC 4. **Least privilege principle** - CDC user only needs replication and read access, not write permissions
# MAGIC
# MAGIC 5. **Indexes improve performance** - both for operational queries and CDC operations
# MAGIC
# MAGIC ## Common Issues and Solutions
# MAGIC
# MAGIC | Issue | Solution |
# MAGIC |-------|----------|
# MAGIC | Can't enable binary logging | Check if you have SUPER privilege; may need to contact DBA |
# MAGIC | MySQL won't start after config change | Review configuration syntax; check MySQL error log |
# MAGIC | CDC user can't see binary logs | Verify REPLICATION CLIENT privilege was granted |
# MAGIC | Tables not appearing | Check if you're connected to correct database: USE retail_db; |
# MAGIC | Foreign key constraint fails | Ensure parent record exists before inserting child record |
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 3: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** where you'll:
# MAGIC - Create Unity Catalog resources (catalog, schemas, volumes)
# MAGIC - Configure Ingestion Gateway to connect to your MySQL instance
# MAGIC - Build Ingestion Pipeline to sync retail_db tables
# MAGIC - Execute initial snapshot load to populate bronze tables
# MAGIC
# MAGIC **Before moving on, ensure**:
# MAGIC - [ ] Binary logging is enabled and working
# MAGIC - [ ] CDC user can connect and run SHOW MASTER STATUS
# MAGIC - [ ] All three tables (customers, orders, products) exist with sample data
# MAGIC - [ ] You've documented your MySQL connection details
# MAGIC - [ ] Network connectivity between Databricks and MySQL is confirmed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [MySQL Binary Log Configuration](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
# MAGIC - [MySQL Replication Privileges](https://dev.mysql.com/doc/refman/8.0/en/replication-privilege-checks.html)
# MAGIC - [MySQL GTID Documentation](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html)
# MAGIC - [AWS RDS MySQL Binary Log Setup](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.MySQL.BinaryFormat.html)
# MAGIC - [Azure MySQL Server Parameters](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/concepts-server-parameters)
