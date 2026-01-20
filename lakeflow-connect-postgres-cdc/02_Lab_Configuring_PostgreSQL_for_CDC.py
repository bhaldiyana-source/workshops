# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2: Configuring PostgreSQL for CDC Operations
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will configure your PostgreSQL database to support Change Data Capture (CDC) using logical replication.
# MAGIC
# MAGIC **Duration:** 30 minutes
# MAGIC
# MAGIC **Objectives:**
# MAGIC - Verify PostgreSQL version and current configuration
# MAGIC - Enable logical replication (WAL level)
# MAGIC - Create replication user with appropriate permissions
# MAGIC - Set replica identity to FULL for target tables
# MAGIC - Create publications for tables to be replicated
# MAGIC - Create logical replication slot
# MAGIC - Validate CDC readiness
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - PostgreSQL database (version 10+) with superuser or admin access
# MAGIC - psql, pgAdmin, DBeaver, or similar PostgreSQL client
# MAGIC - Completed Lecture 1: Introduction to CDC and Lakeflow Connect

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Verify PostgreSQL Environment
# MAGIC
# MAGIC ### Step 1.1: Check PostgreSQL Version
# MAGIC
# MAGIC Connect to your PostgreSQL database and verify the version:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT version();
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC PostgreSQL 14.x on x86_64-pc-linux-gnu, compiled by gcc...
# MAGIC ```
# MAGIC
# MAGIC ✅ **Requirement:** PostgreSQL 10 or later

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Check Current WAL Level
# MAGIC
# MAGIC Run the following query to check the current WAL level:
# MAGIC
# MAGIC ```sql
# MAGIC SHOW wal_level;
# MAGIC ```
# MAGIC
# MAGIC **Possible Values:**
# MAGIC - `minimal` - Basic crash recovery only ❌
# MAGIC - `replica` - Physical replication support ❌
# MAGIC - `logical` - Logical replication enabled ✅
# MAGIC
# MAGIC **If wal_level is NOT 'logical', proceed to Part 2. If already 'logical', skip to Part 3.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3: Check Replication-Related Parameters
# MAGIC
# MAGIC ```sql
# MAGIC SELECT name, setting, unit, context 
# MAGIC FROM pg_settings 
# MAGIC WHERE name IN (
# MAGIC   'wal_level',
# MAGIC   'max_replication_slots',
# MAGIC   'max_wal_senders',
# MAGIC   'wal_keep_size',           -- PostgreSQL 13+
# MAGIC   'wal_keep_segments'         -- PostgreSQL 12 and earlier
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Note the 'context' column:**
# MAGIC - `postmaster` = Requires PostgreSQL restart
# MAGIC - `sighup` = Can be changed with pg_reload_conf()
# MAGIC - `user` = Can be changed per session

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Enable Logical Replication
# MAGIC
# MAGIC ### ⚠️ Warning
# MAGIC Changing `wal_level` to `logical` **requires a PostgreSQL restart**. Plan accordingly.
# MAGIC
# MAGIC ### Option A: AWS RDS PostgreSQL
# MAGIC
# MAGIC #### Step 2.1: Modify Parameter Group
# MAGIC
# MAGIC 1. Open AWS RDS Console
# MAGIC 2. Navigate to **Parameter Groups**
# MAGIC 3. Create custom parameter group (if using default)
# MAGIC    - Family: `postgres14` (match your version)
# MAGIC    - Name: `retail-cdc-params`
# MAGIC 4. Modify the following parameters:
# MAGIC
# MAGIC ```
# MAGIC rds.logical_replication = 1
# MAGIC max_replication_slots = 10
# MAGIC max_wal_senders = 10
# MAGIC ```
# MAGIC
# MAGIC #### Step 2.2: Apply to Database Instance
# MAGIC
# MAGIC 1. Select your RDS instance
# MAGIC 2. **Modify** → **DB parameter group** → Select `retail-cdc-params`
# MAGIC 3. **Apply immediately** or during maintenance window
# MAGIC 4. **Reboot** the instance for changes to take effect
# MAGIC
# MAGIC #### Step 2.3: Wait for Reboot
# MAGIC
# MAGIC Wait 5-10 minutes for instance to become available.
# MAGIC
# MAGIC #### Step 2.4: Verify Changes
# MAGIC
# MAGIC ```sql
# MAGIC SHOW wal_level;
# MAGIC -- Should return: logical
# MAGIC
# MAGIC SHOW max_replication_slots;
# MAGIC -- Should return: 10
# MAGIC
# MAGIC SHOW max_wal_senders;
# MAGIC -- Should return: 10
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Self-Managed PostgreSQL (On-Premises or EC2)
# MAGIC
# MAGIC #### Step 2.1: Locate Configuration File
# MAGIC
# MAGIC Find your `postgresql.conf` file:
# MAGIC
# MAGIC ```sql
# MAGIC SHOW config_file;
# MAGIC -- Example output: /etc/postgresql/14/main/postgresql.conf
# MAGIC ```
# MAGIC
# MAGIC #### Step 2.2: Edit postgresql.conf
# MAGIC
# MAGIC ```bash
# MAGIC # SSH to PostgreSQL server
# MAGIC sudo nano /etc/postgresql/14/main/postgresql.conf
# MAGIC
# MAGIC # Add or modify these lines:
# MAGIC wal_level = logical
# MAGIC max_replication_slots = 10
# MAGIC max_wal_senders = 10
# MAGIC wal_keep_size = 2GB           # PostgreSQL 13+
# MAGIC # wal_keep_segments = 128     # PostgreSQL 12 and earlier
# MAGIC max_wal_size = 2GB
# MAGIC ```
# MAGIC
# MAGIC #### Step 2.3: Restart PostgreSQL
# MAGIC
# MAGIC ```bash
# MAGIC # For systemd (most modern Linux)
# MAGIC sudo systemctl restart postgresql
# MAGIC
# MAGIC # Or for older systems
# MAGIC sudo service postgresql restart
# MAGIC
# MAGIC # Verify service is running
# MAGIC sudo systemctl status postgresql
# MAGIC ```
# MAGIC
# MAGIC #### Step 2.4: Verify Changes
# MAGIC
# MAGIC Reconnect and verify:
# MAGIC
# MAGIC ```sql
# MAGIC SHOW wal_level;
# MAGIC -- Should return: logical
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option C: Azure Database for PostgreSQL
# MAGIC
# MAGIC #### Step 2.1: Set Server Parameters
# MAGIC
# MAGIC 1. Open Azure Portal
# MAGIC 2. Navigate to your PostgreSQL server
# MAGIC 3. Select **Server parameters**
# MAGIC 4. Set the following:
# MAGIC
# MAGIC ```
# MAGIC azure.replication_support = LOGICAL
# MAGIC max_replication_slots = 10
# MAGIC max_wal_senders = 10
# MAGIC ```
# MAGIC
# MAGIC #### Step 2.2: Restart Server
# MAGIC
# MAGIC Some parameters require restart. Azure will prompt if needed.
# MAGIC
# MAGIC #### Step 2.3: Verify
# MAGIC
# MAGIC ```sql
# MAGIC SHOW wal_level;
# MAGIC -- Should return: logical
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Create Replication User
# MAGIC
# MAGIC ### Step 3.1: Create Dedicated Replication User
# MAGIC
# MAGIC **Best Practice:** Create a dedicated user for CDC rather than using superuser.
# MAGIC
# MAGIC ```sql
# MAGIC -- Create replication user
# MAGIC CREATE ROLE lakeflow_replication WITH REPLICATION LOGIN PASSWORD 'YourSecurePassword123!';
# MAGIC
# MAGIC -- Grant connection to database
# MAGIC GRANT CONNECT ON DATABASE retaildb TO lakeflow_replication;
# MAGIC
# MAGIC -- Grant usage on schema
# MAGIC GRANT USAGE ON SCHEMA public TO lakeflow_replication;
# MAGIC
# MAGIC -- Grant SELECT on all existing tables
# MAGIC GRANT SELECT ON ALL TABLES IN SCHEMA public TO lakeflow_replication;
# MAGIC
# MAGIC -- Grant SELECT on future tables (important!)
# MAGIC ALTER DEFAULT PRIVILEGES IN SCHEMA public 
# MAGIC GRANT SELECT ON TABLES TO lakeflow_replication;
# MAGIC ```
# MAGIC
# MAGIC ### Step 3.2: Verify User Creation
# MAGIC
# MAGIC ```sql
# MAGIC -- List roles and their privileges
# MAGIC \du lakeflow_replication
# MAGIC
# MAGIC -- Or with SQL
# MAGIC SELECT 
# MAGIC   rolname,
# MAGIC   rolsuper,
# MAGIC   rolreplication,
# MAGIC   rolconnlimit
# MAGIC FROM pg_roles
# MAGIC WHERE rolname = 'lakeflow_replication';
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC rolname              | rolsuper | rolreplication | rolconnlimit
# MAGIC ---------------------+----------+----------------+-------------
# MAGIC lakeflow_replication | f        | t              | -1
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.3: Update pg_hba.conf (Self-Managed Only)
# MAGIC
# MAGIC **Skip this step for AWS RDS or Azure** - they handle connection security via security groups.
# MAGIC
# MAGIC For self-managed PostgreSQL:
# MAGIC
# MAGIC ```bash
# MAGIC # Edit pg_hba.conf
# MAGIC sudo nano /etc/postgresql/14/main/pg_hba.conf
# MAGIC
# MAGIC # Add entry for Databricks IP ranges
# MAGIC # Example (replace with actual Databricks IPs for your region):
# MAGIC host    replication    lakeflow_replication    10.0.0.0/8    md5
# MAGIC host    retaildb       lakeflow_replication    10.0.0.0/8    md5
# MAGIC
# MAGIC # Reload configuration
# MAGIC sudo systemctl reload postgresql
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Create Sample Database and Tables
# MAGIC
# MAGIC ### Step 4.1: Create Database (if needed)
# MAGIC
# MAGIC ```sql
# MAGIC -- Connect as superuser
# MAGIC CREATE DATABASE retaildb;
# MAGIC
# MAGIC -- Connect to retaildb
# MAGIC \c retaildb
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.2: Create Customers Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE customers (
# MAGIC     customer_id SERIAL PRIMARY KEY,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     email VARCHAR(100) UNIQUE NOT NULL,
# MAGIC     phone VARCHAR(20),
# MAGIC     address VARCHAR(200),
# MAGIC     city VARCHAR(50),
# MAGIC     state VARCHAR(2),
# MAGIC     zip_code VARCHAR(10),
# MAGIC     created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC     last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Add index for performance
# MAGIC CREATE INDEX idx_customers_email ON customers(email);
# MAGIC CREATE INDEX idx_customers_state ON customers(state);
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.3: Create Orders Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE orders (
# MAGIC     order_id SERIAL PRIMARY KEY,
# MAGIC     customer_id INTEGER NOT NULL,
# MAGIC     order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC     order_status VARCHAR(20) DEFAULT 'PENDING',
# MAGIC     total_amount NUMERIC(10, 2),
# MAGIC     shipping_address VARCHAR(200),
# MAGIC     payment_method VARCHAR(50),
# MAGIC     created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC     last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC     FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
# MAGIC );
# MAGIC
# MAGIC -- Add indexes
# MAGIC CREATE INDEX idx_orders_customer ON orders(customer_id);
# MAGIC CREATE INDEX idx_orders_date ON orders(order_date);
# MAGIC CREATE INDEX idx_orders_status ON orders(order_status);
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.4: Create Products Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE products (
# MAGIC     product_id SERIAL PRIMARY KEY,
# MAGIC     product_name VARCHAR(100) NOT NULL,
# MAGIC     category VARCHAR(50),
# MAGIC     price NUMERIC(10, 2),
# MAGIC     stock_quantity INTEGER DEFAULT 0,
# MAGIC     supplier VARCHAR(100),
# MAGIC     description TEXT,
# MAGIC     created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC     last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Add indexes
# MAGIC CREATE INDEX idx_products_category ON products(category);
# MAGIC CREATE INDEX idx_products_name ON products(product_name);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.5: Insert Sample Data
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert sample customers
# MAGIC INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code)
# MAGIC VALUES 
# MAGIC   ('John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'Seattle', 'WA', '98101'),
# MAGIC   ('Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Portland', 'OR', '97201'),
# MAGIC   ('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine Rd', 'San Francisco', 'CA', '94102'),
# MAGIC   ('Alice', 'Williams', 'alice.williams@email.com', '555-0104', '321 Elm St', 'Los Angeles', 'CA', '90001'),
# MAGIC   ('Charlie', 'Brown', 'charlie.brown@email.com', '555-0105', '654 Maple Dr', 'Austin', 'TX', '78701');
# MAGIC
# MAGIC -- Insert sample products
# MAGIC INSERT INTO products (product_name, category, price, stock_quantity, supplier, description)
# MAGIC VALUES 
# MAGIC   ('Laptop Pro 15', 'Electronics', 1299.99, 50, 'TechSupply Inc', 'High-performance laptop'),
# MAGIC   ('Wireless Mouse', 'Electronics', 29.99, 200, 'TechSupply Inc', 'Ergonomic wireless mouse'),
# MAGIC   ('Office Chair', 'Furniture', 249.99, 30, 'OfficeGoods LLC', 'Ergonomic office chair'),
# MAGIC   ('Desk Lamp', 'Furniture', 45.99, 75, 'OfficeGoods LLC', 'LED desk lamp'),
# MAGIC   ('Notebook Pack', 'Stationery', 12.99, 500, 'PaperWorld', 'Pack of 5 notebooks');
# MAGIC
# MAGIC -- Insert sample orders
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, shipping_address, payment_method)
# MAGIC VALUES 
# MAGIC   (1, 'SHIPPED', 1329.98, '123 Main St, Seattle, WA 98101', 'Credit Card'),
# MAGIC   (2, 'PROCESSING', 45.99, '456 Oak Ave, Portland, OR 97201', 'PayPal'),
# MAGIC   (3, 'DELIVERED', 249.99, '789 Pine Rd, San Francisco, CA 94102', 'Credit Card'),
# MAGIC   (1, 'PENDING', 12.99, '123 Main St, Seattle, WA 98101', 'Credit Card'),
# MAGIC   (4, 'SHIPPED', 75.98, '321 Elm St, Los Angeles, CA 90001', 'Debit Card');
# MAGIC ```
# MAGIC
# MAGIC ### Step 4.6: Verify Data
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) as customer_count FROM customers;
# MAGIC SELECT COUNT(*) as order_count FROM orders;
# MAGIC SELECT COUNT(*) as product_count FROM products;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Configure Replica Identity
# MAGIC
# MAGIC ### Why Replica Identity FULL?
# MAGIC
# MAGIC Replica identity determines what data is logged in WAL for UPDATE and DELETE operations:
# MAGIC
# MAGIC | Mode | Data Logged | CDC Use Case |
# MAGIC |------|-------------|--------------|
# MAGIC | DEFAULT | Only primary key | ❌ Insufficient for CDC |
# MAGIC | FULL | All columns | ✅ Required for CDC |
# MAGIC | INDEX | Indexed columns | ⚠️ May work but not recommended |
# MAGIC | NOTHING | No data | ❌ Not usable for CDC |
# MAGIC
# MAGIC ### Step 5.1: Check Current Replica Identity
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   schemaname,
# MAGIC   tablename,
# MAGIC   CASE relreplident
# MAGIC     WHEN 'd' THEN 'DEFAULT'
# MAGIC     WHEN 'f' THEN 'FULL'
# MAGIC     WHEN 'i' THEN 'INDEX'
# MAGIC     WHEN 'n' THEN 'NOTHING'
# MAGIC   END as replica_identity
# MAGIC FROM pg_class c
# MAGIC JOIN pg_namespace n ON c.relnamespace = n.oid
# MAGIC JOIN pg_tables t ON c.relname = t.tablename AND n.nspname = t.schemaname
# MAGIC WHERE schemaname = 'public' 
# MAGIC   AND tablename IN ('customers', 'orders', 'products');
# MAGIC ```
# MAGIC
# MAGIC ### Step 5.2: Set Replica Identity to FULL
# MAGIC
# MAGIC ```sql
# MAGIC -- Set replica identity for all three tables
# MAGIC ALTER TABLE customers REPLICA IDENTITY FULL;
# MAGIC ALTER TABLE orders REPLICA IDENTITY FULL;
# MAGIC ALTER TABLE products REPLICA IDENTITY FULL;
# MAGIC ```
# MAGIC
# MAGIC ### Step 5.3: Verify Changes
# MAGIC
# MAGIC ```sql
# MAGIC -- Re-run the check query from Step 5.1
# MAGIC -- All three tables should show 'FULL'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Create Publication
# MAGIC
# MAGIC ### What is a Publication?
# MAGIC
# MAGIC A **publication** defines which tables (and optionally which operations) to replicate.
# MAGIC
# MAGIC ### Step 6.1: Create Publication
# MAGIC
# MAGIC ```sql
# MAGIC -- Create publication for all three tables
# MAGIC CREATE PUBLICATION lakeflow_publication 
# MAGIC FOR TABLE customers, orders, products;
# MAGIC ```
# MAGIC
# MAGIC **Alternative: Publish all tables in schema**
# MAGIC ```sql
# MAGIC -- Use this if you want all current and future tables
# MAGIC CREATE PUBLICATION lakeflow_publication_all 
# MAGIC FOR ALL TABLES;
# MAGIC ```
# MAGIC
# MAGIC ### Step 6.2: Verify Publication
# MAGIC
# MAGIC ```sql
# MAGIC -- List all publications
# MAGIC SELECT * FROM pg_publication;
# MAGIC
# MAGIC -- List tables in publication
# MAGIC SELECT 
# MAGIC   pubname,
# MAGIC   schemaname,
# MAGIC   tablename
# MAGIC FROM pg_publication_tables
# MAGIC WHERE pubname = 'lakeflow_publication'
# MAGIC ORDER BY tablename;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC pubname               | schemaname | tablename
# MAGIC ----------------------+------------+-----------
# MAGIC lakeflow_publication | public     | customers
# MAGIC lakeflow_publication | public     | orders
# MAGIC lakeflow_publication | public     | products
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Create Replication Slot
# MAGIC
# MAGIC ### What is a Replication Slot?
# MAGIC
# MAGIC A **replication slot** tracks the CDC consumer's position in the WAL stream.
# MAGIC
# MAGIC ### Step 7.1: Create Logical Replication Slot
# MAGIC
# MAGIC ```sql
# MAGIC -- Create replication slot using pgoutput plugin
# MAGIC SELECT pg_create_logical_replication_slot('lakeflow_slot', 'pgoutput');
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC pg_create_logical_replication_slot
# MAGIC ------------------------------------
# MAGIC (lakeflow_slot, 0/1A2B3C4)
# MAGIC ```
# MAGIC
# MAGIC The second value is the starting LSN (Log Sequence Number).
# MAGIC
# MAGIC ### Step 7.2: Verify Replication Slot
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   slot_name,
# MAGIC   plugin,
# MAGIC   slot_type,
# MAGIC   active,
# MAGIC   restart_lsn,
# MAGIC   confirmed_flush_lsn
# MAGIC FROM pg_replication_slots;
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC slot_name    | plugin   | slot_type | active | restart_lsn | confirmed_flush_lsn
# MAGIC -------------+----------+-----------+--------+-------------+--------------------
# MAGIC lakeflow_slot| pgoutput | logical   | f      | 0/1A2B3C4   | 
# MAGIC ```
# MAGIC
# MAGIC - `active: f` is normal (Lakeflow Connect hasn't connected yet)
# MAGIC - `restart_lsn` shows the starting position

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7.3: Monitor Replication Slot (Important!)
# MAGIC
# MAGIC ⚠️ **Warning:** Inactive replication slots can cause WAL files to accumulate and fill disk.
# MAGIC
# MAGIC **Monitoring Query:**
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   slot_name,
# MAGIC   active,
# MAGIC   pg_size_pretty(
# MAGIC     pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
# MAGIC   ) as lag_size,
# MAGIC   pg_size_pretty(
# MAGIC     pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
# MAGIC   ) as pending_size
# MAGIC FROM pg_replication_slots
# MAGIC WHERE slot_name = 'lakeflow_slot';
# MAGIC ```
# MAGIC
# MAGIC **Drop Slot if Needed:**
# MAGIC ```sql
# MAGIC -- Only if you need to recreate or remove
# MAGIC SELECT pg_drop_replication_slot('lakeflow_slot');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Validation and Testing
# MAGIC
# MAGIC ### Step 8.1: Comprehensive Configuration Check
# MAGIC
# MAGIC Run this validation query to verify all CDC prerequisites:
# MAGIC
# MAGIC ```sql
# MAGIC -- WAL Configuration
# MAGIC SELECT 'WAL Level' as check_name, 
# MAGIC        setting as value,
# MAGIC        CASE WHEN setting = 'logical' THEN '✓ PASS' ELSE '✗ FAIL' END as status
# MAGIC FROM pg_settings WHERE name = 'wal_level'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Replication Slots
# MAGIC SELECT 'Max Replication Slots',
# MAGIC        setting,
# MAGIC        CASE WHEN setting::int >= 5 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC FROM pg_settings WHERE name = 'max_replication_slots'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- WAL Senders
# MAGIC SELECT 'Max WAL Senders',
# MAGIC        setting,
# MAGIC        CASE WHEN setting::int >= 5 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC FROM pg_settings WHERE name = 'max_wal_senders'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Replication User
# MAGIC SELECT 'Replication User Exists',
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'Yes' ELSE 'No' END,
# MAGIC        CASE WHEN COUNT(*) > 0 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC FROM pg_roles 
# MAGIC WHERE rolreplication = true AND rolname = 'lakeflow_replication'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Publication
# MAGIC SELECT 'Publication Exists',
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'Yes' ELSE 'No' END,
# MAGIC        CASE WHEN COUNT(*) > 0 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC FROM pg_publication 
# MAGIC WHERE pubname = 'lakeflow_publication'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Replication Slot
# MAGIC SELECT 'Replication Slot Exists',
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'Yes' ELSE 'No' END,
# MAGIC        CASE WHEN COUNT(*) > 0 THEN '✓ PASS' ELSE '✗ FAIL' END
# MAGIC FROM pg_replication_slots 
# MAGIC WHERE slot_name = 'lakeflow_slot';
# MAGIC ```
# MAGIC
# MAGIC **All checks should show '✓ PASS'**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8.2: Test Replication User Permissions
# MAGIC
# MAGIC ```sql
# MAGIC -- Connect as replication user to verify permissions
# MAGIC -- In a new psql session:
# MAGIC -- psql -h your-host -U lakeflow_replication -d retaildb
# MAGIC
# MAGIC -- Test SELECT access
# MAGIC SELECT COUNT(*) FROM customers;
# MAGIC SELECT COUNT(*) FROM orders;
# MAGIC SELECT COUNT(*) FROM products;
# MAGIC
# MAGIC -- All three should succeed without errors
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8.3: Generate Test Changes
# MAGIC
# MAGIC Generate some changes to test CDC capture later:
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert new customer
# MAGIC INSERT INTO customers (first_name, last_name, email, city, state)
# MAGIC VALUES ('Test', 'User', 'test.user@email.com', 'Denver', 'CO');
# MAGIC
# MAGIC -- Update existing customer
# MAGIC UPDATE customers 
# MAGIC SET city = 'Bellevue', last_updated = CURRENT_TIMESTAMP
# MAGIC WHERE email = 'john.doe@email.com';
# MAGIC
# MAGIC -- Insert new order
# MAGIC INSERT INTO orders (customer_id, order_status, total_amount, payment_method)
# MAGIC VALUES (1, 'PENDING', 99.99, 'Credit Card');
# MAGIC ```
# MAGIC
# MAGIC These changes will be captured by CDC once Lakeflow Connect is configured.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue: Cannot Change wal_level
# MAGIC
# MAGIC **Error:** `ERROR: parameter "wal_level" cannot be changed without restarting the server`
# MAGIC
# MAGIC **Solution:** 
# MAGIC - PostgreSQL restart is required
# MAGIC - For RDS: Modify parameter group and reboot instance
# MAGIC - For self-managed: Edit postgresql.conf and `sudo systemctl restart postgresql`
# MAGIC
# MAGIC ### Issue: Permission Denied Creating Publication
# MAGIC
# MAGIC **Error:** `ERROR: permission denied for table customers`
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC -- Grant SELECT on all tables
# MAGIC GRANT SELECT ON ALL TABLES IN SCHEMA public TO lakeflow_replication;
# MAGIC
# MAGIC -- Grant for future tables
# MAGIC ALTER DEFAULT PRIVILEGES IN SCHEMA public 
# MAGIC GRANT SELECT ON TABLES TO lakeflow_replication;
# MAGIC ```
# MAGIC
# MAGIC ### Issue: Replication Slot Already Exists
# MAGIC
# MAGIC **Error:** `ERROR: replication slot "lakeflow_slot" already exists`
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC -- Check if active
# MAGIC SELECT * FROM pg_replication_slots WHERE slot_name = 'lakeflow_slot';
# MAGIC
# MAGIC -- If not active and safe to drop:
# MAGIC SELECT pg_drop_replication_slot('lakeflow_slot');
# MAGIC
# MAGIC -- Recreate
# MAGIC SELECT pg_create_logical_replication_slot('lakeflow_slot', 'pgoutput');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Completion Checklist
# MAGIC
# MAGIC Verify you've completed all steps:
# MAGIC
# MAGIC - [ ] Verified PostgreSQL version (10+)
# MAGIC - [ ] Set `wal_level = logical` (restart completed if needed)
# MAGIC - [ ] Set `max_replication_slots >= 10`
# MAGIC - [ ] Set `max_wal_senders >= 10`
# MAGIC - [ ] Created `lakeflow_replication` user with REPLICATION privilege
# MAGIC - [ ] Granted SELECT permissions on all tables
# MAGIC - [ ] Created sample tables (customers, orders, products)
# MAGIC - [ ] Inserted sample data
# MAGIC - [ ] Set REPLICA IDENTITY FULL on all tables
# MAGIC - [ ] Created publication `lakeflow_publication`
# MAGIC - [ ] Created replication slot `lakeflow_slot`
# MAGIC - [ ] Verified all configurations with validation query
# MAGIC - [ ] Tested replication user permissions
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC You've successfully configured PostgreSQL for CDC operations! Your database is now ready for:
# MAGIC - Logical replication
# MAGIC - Change data capture
# MAGIC - Integration with Databricks Lakeflow Connect
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **Lab 3: Setting Up Lakeflow Connect Ingestion Gateway and Pipeline** where you'll:
# MAGIC - Create Unity Catalog resources
# MAGIC - Create ingestion gateway
# MAGIC - Configure Unity Catalog connection to PostgreSQL
# MAGIC - Create and run your first CDC pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Great job! Your PostgreSQL database is CDC-ready!** ✅
