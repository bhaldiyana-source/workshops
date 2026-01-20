# Databricks notebook source
# MAGIC %md
# MAGIC # Ways to Access Your Lakebase Database
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Identify different methods to connect to Lakebase databases
# MAGIC - Understand authentication and credential management best practices
# MAGIC - Describe connection string format and parameters
# MAGIC - Explain security considerations for database access
# MAGIC - Determine when to use single connections vs. connection pools
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lab 2: Creating and Exploring a Lakebase PostgreSQL Database
# MAGIC - Basic understanding of database connectivity concepts
# MAGIC - Familiarity with Python programming

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Lakebase databases can be accessed through multiple methods, each suited for different use cases:
# MAGIC
# MAGIC 1. **Databricks SQL Warehouse** - For SQL analytics and BI tools
# MAGIC 2. **Databricks Notebooks** - For data engineering and ML workflows
# MAGIC 3. **Python Applications** - For operational apps and APIs
# MAGIC 4. **External Tools** - For database management and development
# MAGIC
# MAGIC In this lecture, we'll explore each method, focusing on programmatic access patterns you'll use in subsequent labs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection Methods Overview
# MAGIC
# MAGIC ### 1. SQL Warehouse (Serverless SQL)
# MAGIC
# MAGIC **What it is**: Databricks SQL Warehouses provide managed compute for SQL queries
# MAGIC
# MAGIC **Use cases**:
# MAGIC - Ad-hoc queries and data exploration
# MAGIC - Business intelligence dashboards
# MAGIC - Reports and scheduled queries
# MAGIC - Integration with BI tools (Tableau, Power BI)
# MAGIC
# MAGIC **Advantages**:
# MAGIC - No connection management required
# MAGIC - Integrated with Unity Catalog
# MAGIC - Automatic query optimization
# MAGIC - Built-in security and governance
# MAGIC
# MAGIC **How to use**:
# MAGIC ```sql
# MAGIC -- In Databricks SQL or notebooks
# MAGIC USE CATALOG workshop_catalog;
# MAGIC USE SCHEMA workshop;
# MAGIC SELECT * FROM customers;
# MAGIC ```
# MAGIC
# MAGIC **Best for**: Analytical queries, reporting, and BI workloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Databricks Notebooks
# MAGIC
# MAGIC **What it is**: Interactive computational environment with built-in database connectivity
# MAGIC
# MAGIC **Use cases**:
# MAGIC - Data exploration and analysis
# MAGIC - ETL/ELT workflows
# MAGIC - ML feature engineering
# MAGIC - Prototyping applications
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Mix SQL and Python in the same notebook
# MAGIC - Visualizations built-in
# MAGIC - Collaborative development
# MAGIC - Version control integration
# MAGIC
# MAGIC **Best for**: Development, analysis, and batch processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Python Applications (psycopg3)
# MAGIC
# MAGIC **What it is**: Direct PostgreSQL protocol connections from Python code
# MAGIC
# MAGIC **Use cases**:
# MAGIC - Operational applications (APIs, web apps)
# MAGIC - Real-time data processing
# MAGIC - Microservices
# MAGIC - AI agents requiring database access
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Full transactional control
# MAGIC - Connection pooling for concurrency
# MAGIC - Low latency (<10ms)
# MAGIC - Standard PostgreSQL libraries
# MAGIC
# MAGIC **Best for**: Production applications requiring high-concurrency operational access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. External Tools
# MAGIC
# MAGIC **What it is**: Database clients and IDEs using PostgreSQL protocol
# MAGIC
# MAGIC **Examples**:
# MAGIC - pgAdmin (GUI management tool)
# MAGIC - DBeaver (multi-database IDE)
# MAGIC - DataGrip (JetBrains IDE)
# MAGIC - psql (command-line client)
# MAGIC
# MAGIC **Use cases**:
# MAGIC - Database administration
# MAGIC - Schema design and migration
# MAGIC - Query development
# MAGIC - Troubleshooting
# MAGIC
# MAGIC **Best for**: Database development and administration tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection String Format
# MAGIC
# MAGIC ### PostgreSQL Connection String Anatomy
# MAGIC
# MAGIC Lakebase uses the standard PostgreSQL connection format:
# MAGIC
# MAGIC ```
# MAGIC postgresql://[username]:[password]@[host]:[port]/[database]?[parameters]
# MAGIC ```
# MAGIC
# MAGIC ### Components Explained
# MAGIC
# MAGIC | Component | Description | Example |
# MAGIC |-----------|-------------|---------|
# MAGIC | **Protocol** | Database protocol | `postgresql://` or `postgres://` |
# MAGIC | **Username** | Database user | `lakebase_user` |
# MAGIC | **Password** | User password | `your_secure_password` |
# MAGIC | **Host** | Database endpoint | `xxxxx.cloud.databricks.com` |
# MAGIC | **Port** | Connection port | `5432` (default PostgreSQL port) |
# MAGIC | **Database** | Database name | `workshop_db` |
# MAGIC | **Parameters** | Optional connection params | `sslmode=require` |
# MAGIC
# MAGIC ### Example Connection Strings
# MAGIC
# MAGIC ```python
# MAGIC # Basic connection string
# MAGIC conn_string = "postgresql://user:pass@host.databricks.com:5432/mydb"
# MAGIC
# MAGIC # With SSL (recommended for production)
# MAGIC conn_string = "postgresql://user:pass@host.databricks.com:5432/mydb?sslmode=require"
# MAGIC
# MAGIC # With additional parameters
# MAGIC conn_string = "postgresql://user:pass@host.databricks.com:5432/mydb?sslmode=require&connect_timeout=10"
# MAGIC ```
# MAGIC
# MAGIC ### Alternative: Connection Parameters Dictionary
# MAGIC
# MAGIC Instead of a connection string, you can use a dictionary:
# MAGIC
# MAGIC ```python
# MAGIC conn_params = {
# MAGIC     'host': 'xxxxx.cloud.databricks.com',
# MAGIC     'port': 5432,
# MAGIC     'dbname': 'workshop_db',
# MAGIC     'user': 'lakebase_user',
# MAGIC     'password': 'your_secure_password',
# MAGIC     'sslmode': 'require',
# MAGIC     'connect_timeout': 10
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication and Credential Management
# MAGIC
# MAGIC ### Authentication Methods
# MAGIC
# MAGIC #### 1. Username and Password
# MAGIC
# MAGIC **How it works**: Standard database authentication
# MAGIC
# MAGIC **When to use**: Development, testing, simple applications
# MAGIC
# MAGIC **Security considerations**:
# MAGIC - Never hardcode credentials in code
# MAGIC - Always use secrets management
# MAGIC - Rotate credentials regularly
# MAGIC
# MAGIC #### 2. Databricks Personal Access Tokens (PATs)
# MAGIC
# MAGIC **How it works**: OAuth-style token authentication
# MAGIC
# MAGIC **When to use**: Applications running within Databricks
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Token-based (no password storage)
# MAGIC - Scoped permissions
# MAGIC - Easy revocation
# MAGIC
# MAGIC #### 3. Service Principals
# MAGIC
# MAGIC **How it works**: Machine-to-machine authentication
# MAGIC
# MAGIC **When to use**: Production applications, automation, CI/CD
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Not tied to individual users
# MAGIC - Fine-grained permissions
# MAGIC - Audit trail
# MAGIC
# MAGIC ### Credential Storage Best Practices
# MAGIC
# MAGIC ❌ **NEVER DO THIS**:
# MAGIC ```python
# MAGIC # Hard-coded credentials - INSECURE!
# MAGIC password = "my_password_123"
# MAGIC conn_string = f"postgresql://user:{password}@host:5432/db"
# MAGIC ```
# MAGIC
# MAGIC ✅ **DO THIS INSTEAD**:
# MAGIC
# MAGIC #### Option 1: Databricks Secrets
# MAGIC
# MAGIC ```python
# MAGIC # Store credentials in Databricks Secrets
# MAGIC from databricks.sdk.runtime import dbutils
# MAGIC
# MAGIC # Retrieve from secrets scope
# MAGIC db_host = dbutils.secrets.get(scope="lakebase", key="db_host")
# MAGIC db_user = dbutils.secrets.get(scope="lakebase", key="db_user")
# MAGIC db_password = dbutils.secrets.get(scope="lakebase", key="db_password")
# MAGIC
# MAGIC conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:5432/mydb"
# MAGIC ```
# MAGIC
# MAGIC #### Option 2: Environment Variables (for external applications)
# MAGIC
# MAGIC ```python
# MAGIC import os
# MAGIC
# MAGIC # Read from environment variables
# MAGIC db_host = os.environ['LAKEBASE_HOST']
# MAGIC db_user = os.environ['LAKEBASE_USER']
# MAGIC db_password = os.environ['LAKEBASE_PASSWORD']
# MAGIC
# MAGIC conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:5432/mydb"
# MAGIC ```
# MAGIC
# MAGIC #### Option 3: Configuration Files (encrypted)
# MAGIC
# MAGIC ```python
# MAGIC import configparser
# MAGIC
# MAGIC # Load from encrypted config file
# MAGIC config = configparser.ConfigParser()
# MAGIC config.read('/secure/path/config.ini')
# MAGIC
# MAGIC conn_params = {
# MAGIC     'host': config['lakebase']['host'],
# MAGIC     'user': config['lakebase']['user'],
# MAGIC     'password': config['lakebase']['password']
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Database Connectivity with psycopg3
# MAGIC
# MAGIC ### What is psycopg3?
# MAGIC
# MAGIC **psycopg3** is the modern PostgreSQL adapter for Python:
# MAGIC - Latest version of the popular psycopg library
# MAGIC - Full PostgreSQL protocol support
# MAGIC - Synchronous and asynchronous APIs
# MAGIC - Connection pooling built-in
# MAGIC - Type safety and conversion
# MAGIC
# MAGIC ### Installation
# MAGIC
# MAGIC ```python
# MAGIC # Install in Databricks notebook
# MAGIC %pip install psycopg[binary]
# MAGIC ```
# MAGIC
# MAGIC Or for external applications:
# MAGIC ```bash
# MAGIC pip install psycopg[binary]
# MAGIC ```
# MAGIC
# MAGIC ### Basic Connection Pattern
# MAGIC
# MAGIC ```python
# MAGIC import psycopg
# MAGIC
# MAGIC # Connect to database
# MAGIC with psycopg.connect(
# MAGIC     host="xxxxx.cloud.databricks.com",
# MAGIC     port=5432,
# MAGIC     dbname="workshop_db",
# MAGIC     user="lakebase_user",
# MAGIC     password="secure_password"
# MAGIC ) as conn:
# MAGIC     # Execute queries
# MAGIC     with conn.cursor() as cur:
# MAGIC         cur.execute("SELECT * FROM customers LIMIT 5")
# MAGIC         results = cur.fetchall()
# MAGIC         for row in results:
# MAGIC             print(row)
# MAGIC ```
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC - **Context managers**: Automatic connection cleanup
# MAGIC - **Cursors**: Execute queries and fetch results
# MAGIC - **Transactions**: Explicit commit/rollback control
# MAGIC - **Type conversion**: Automatic Python ↔ PostgreSQL type mapping

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single Connection vs Connection Pool
# MAGIC
# MAGIC ### Single Connection Pattern
# MAGIC
# MAGIC **What it is**: One connection per database operation
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Application Thread
# MAGIC       │
# MAGIC       ▼
# MAGIC   Connection ──────► Lakebase Database
# MAGIC       │
# MAGIC   (close after use)
# MAGIC ```
# MAGIC
# MAGIC **When to use**:
# MAGIC - Simple scripts and one-off tasks
# MAGIC - Low-frequency operations
# MAGIC - Single-threaded applications
# MAGIC - Development and testing
# MAGIC
# MAGIC **Advantages**:
# MAGIC - Simple to implement
# MAGIC - Easy to understand
# MAGIC - No connection management overhead
# MAGIC
# MAGIC **Limitations**:
# MAGIC - High connection overhead for frequent operations
# MAGIC - Not suitable for concurrent requests
# MAGIC - Slower for high-throughput workloads
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC import psycopg
# MAGIC
# MAGIC def get_customer(customer_id):
# MAGIC     # Create connection
# MAGIC     with psycopg.connect(conn_string) as conn:
# MAGIC         with conn.cursor() as cur:
# MAGIC             cur.execute(
# MAGIC                 "SELECT * FROM customers WHERE customer_id = %s",
# MAGIC                 (customer_id,)
# MAGIC             )
# MAGIC             return cur.fetchone()
# MAGIC     # Connection automatically closed
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Connection Pool Pattern
# MAGIC
# MAGIC **What it is**: Pre-created pool of reusable connections
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Application Threads
# MAGIC   │   │   │   │
# MAGIC   ▼   ▼   ▼   ▼
# MAGIC ┌─────────────────┐
# MAGIC │ Connection Pool │
# MAGIC │  ┌───┐ ┌───┐   │
# MAGIC │  │ 1 │ │ 2 │   │  ──────► Lakebase Database
# MAGIC │  └───┘ └───┘   │
# MAGIC │  ┌───┐ ┌───┐   │
# MAGIC │  │ 3 │ │ 4 │   │
# MAGIC │  └───┘ └───┘   │
# MAGIC └─────────────────┘
# MAGIC   (reuse connections)
# MAGIC ```
# MAGIC
# MAGIC **When to use**:
# MAGIC - Production applications
# MAGIC - High-concurrency workloads
# MAGIC - Web APIs and microservices
# MAGIC - Multi-threaded applications
# MAGIC - Long-running services
# MAGIC
# MAGIC **Advantages**:
# MAGIC - **Performance**: Reuse existing connections
# MAGIC - **Concurrency**: Handle multiple simultaneous requests
# MAGIC - **Resource efficiency**: Limit total connections
# MAGIC - **Reliability**: Automatic connection recovery
# MAGIC
# MAGIC **Configuration parameters**:
# MAGIC - **min_size**: Minimum connections to maintain
# MAGIC - **max_size**: Maximum connections allowed
# MAGIC - **timeout**: Wait time for available connection
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC from psycopg_pool import ConnectionPool
# MAGIC
# MAGIC # Create pool (done once at startup)
# MAGIC pool = ConnectionPool(
# MAGIC     conninfo=conn_string,
# MAGIC     min_size=2,
# MAGIC     max_size=10,
# MAGIC     timeout=5.0
# MAGIC )
# MAGIC
# MAGIC def get_customer(customer_id):
# MAGIC     # Get connection from pool
# MAGIC     with pool.connection() as conn:
# MAGIC         with conn.cursor() as cur:
# MAGIC             cur.execute(
# MAGIC                 "SELECT * FROM customers WHERE customer_id = %s",
# MAGIC                 (customer_id,)
# MAGIC             )
# MAGIC             return cur.fetchone()
# MAGIC     # Connection returned to pool (not closed)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Single Connection vs Connection Pool
# MAGIC
# MAGIC | Aspect | Single Connection | Connection Pool |
# MAGIC |--------|-------------------|-----------------|
# MAGIC | **Setup Complexity** | Simple | Moderate |
# MAGIC | **Connection Overhead** | High (create/destroy each time) | Low (reuse connections) |
# MAGIC | **Concurrency** | Limited | High |
# MAGIC | **Resource Usage** | Lower (one at a time) | Higher (multiple active) |
# MAGIC | **Latency** | Higher (connection time) | Lower (connection ready) |
# MAGIC | **Best For** | Scripts, batch jobs | APIs, web apps, services |
# MAGIC | **Throughput** | Low-moderate | High |
# MAGIC | **Connection Limit** | No explicit limit | Configurable max_size |
# MAGIC | **Error Recovery** | Manual | Automatic |
# MAGIC | **Thread Safety** | Manual handling needed | Built-in thread safety |
# MAGIC
# MAGIC ### Decision Guide
# MAGIC
# MAGIC **Use Single Connection when**:
# MAGIC - Running one-off scripts
# MAGIC - Processing small batches
# MAGIC - Operations are infrequent
# MAGIC - Single-threaded execution
# MAGIC - Simplicity is priority
# MAGIC
# MAGIC **Use Connection Pool when**:
# MAGIC - Building production applications
# MAGIC - Handling concurrent requests
# MAGIC - Running web APIs or services
# MAGIC - Processing continuous workloads
# MAGIC - Performance is critical

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security Best Practices
# MAGIC
# MAGIC ### Connection Security
# MAGIC
# MAGIC ✅ **Always use SSL/TLS encryption**
# MAGIC ```python
# MAGIC # Require SSL for all connections
# MAGIC conn_params = {
# MAGIC     'host': 'host.databricks.com',
# MAGIC     'sslmode': 'require',  # or 'verify-full' for cert validation
# MAGIC     # ... other params
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ✅ **Set connection timeouts**
# MAGIC ```python
# MAGIC conn_params = {
# MAGIC     'connect_timeout': 10,  # seconds
# MAGIC     'keepalives': 1,
# MAGIC     'keepalives_idle': 30
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ✅ **Limit connection lifetime**
# MAGIC ```python
# MAGIC pool = ConnectionPool(
# MAGIC     conninfo=conn_string,
# MAGIC     max_lifetime=3600,  # recycle connections after 1 hour
# MAGIC     max_idle=300  # close idle connections after 5 minutes
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Credential Security
# MAGIC
# MAGIC ✅ **Use Databricks Secrets** (recommended)
# MAGIC ```python
# MAGIC password = dbutils.secrets.get(scope="lakebase", key="db_password")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Rotate credentials regularly**
# MAGIC - Set up automated rotation schedules
# MAGIC - Update secrets without code changes
# MAGIC - Monitor for unauthorized access
# MAGIC
# MAGIC ✅ **Use service principals for production**
# MAGIC - Create dedicated service accounts
# MAGIC - Grant minimum required permissions
# MAGIC - Enable audit logging
# MAGIC
# MAGIC ### Network Security
# MAGIC
# MAGIC ✅ **Use Private Link** (enterprise feature)
# MAGIC - Keeps traffic within cloud provider network
# MAGIC - Prevents internet exposure
# MAGIC
# MAGIC ✅ **Configure IP allowlists**
# MAGIC - Restrict access to known IP ranges
# MAGIC - Update as application infrastructure changes
# MAGIC
# MAGIC ✅ **Monitor connection attempts**
# MAGIC - Log failed authentication attempts
# MAGIC - Set up alerts for suspicious activity
# MAGIC
# MAGIC ### Application Security
# MAGIC
# MAGIC ✅ **Use parameterized queries** (prevent SQL injection)
# MAGIC ```python
# MAGIC # SECURE: Parameterized query
# MAGIC cur.execute("SELECT * FROM customers WHERE email = %s", (email,))
# MAGIC
# MAGIC # INSECURE: String concatenation - NEVER DO THIS!
# MAGIC # cur.execute(f"SELECT * FROM customers WHERE email = '{email}'")
# MAGIC ```
# MAGIC
# MAGIC ✅ **Implement least privilege**
# MAGIC - Grant only necessary database permissions
# MAGIC - Use read-only users for read operations
# MAGIC - Separate admin and application credentials
# MAGIC
# MAGIC ✅ **Handle errors securely**
# MAGIC ```python
# MAGIC try:
# MAGIC     # database operation
# MAGIC     pass
# MAGIC except psycopg.Error as e:
# MAGIC     # Log error details securely (don't expose to users)
# MAGIC     logger.error(f"Database error: {e}")
# MAGIC     # Return generic error to user
# MAGIC     raise ApplicationError("Unable to process request")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection Parameters Reference
# MAGIC
# MAGIC ### Common Connection Parameters
# MAGIC
# MAGIC | Parameter | Description | Default | Example |
# MAGIC |-----------|-------------|---------|---------|
# MAGIC | `host` | Database server hostname | - | `xxxxx.cloud.databricks.com` |
# MAGIC | `port` | Connection port | `5432` | `5432` |
# MAGIC | `dbname` | Database name | - | `workshop_db` |
# MAGIC | `user` | Username | - | `lakebase_user` |
# MAGIC | `password` | Password | - | `secure_password` |
# MAGIC | `sslmode` | SSL mode | `prefer` | `require`, `verify-full` |
# MAGIC | `connect_timeout` | Connection timeout (sec) | - | `10` |
# MAGIC | `application_name` | Application identifier | - | `my_app_v1` |
# MAGIC | `keepalives` | Enable TCP keepalives | `1` | `1` |
# MAGIC | `keepalives_idle` | Keepalive idle time (sec) | - | `30` |
# MAGIC
# MAGIC ### Pool-Specific Parameters
# MAGIC
# MAGIC | Parameter | Description | Recommended |
# MAGIC |-----------|-------------|-------------|
# MAGIC | `min_size` | Minimum pool connections | `2-5` |
# MAGIC | `max_size` | Maximum pool connections | `10-50` |
# MAGIC | `timeout` | Wait for connection (sec) | `5-30` |
# MAGIC | `max_lifetime` | Max connection age (sec) | `3600` |
# MAGIC | `max_idle` | Max idle time (sec) | `300` |
# MAGIC | `max_waiting` | Max waiting clients | `0` (unlimited) |
# MAGIC
# MAGIC ### SSL/TLS Modes
# MAGIC
# MAGIC | Mode | Description | Security Level |
# MAGIC |------|-------------|----------------|
# MAGIC | `disable` | No SSL | ❌ Not recommended |
# MAGIC | `allow` | Try SSL, fallback to plain | ⚠️ Low |
# MAGIC | `prefer` | Prefer SSL if available | ⚠️ Medium |
# MAGIC | `require` | Require SSL | ✅ Good |
# MAGIC | `verify-ca` | Require SSL + verify CA | ✅ Better |
# MAGIC | `verify-full` | Require SSL + verify host | ✅ Best |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling and Retry Logic
# MAGIC
# MAGIC ### Common Connection Errors
# MAGIC
# MAGIC ```python
# MAGIC import psycopg
# MAGIC from psycopg import OperationalError, InterfaceError
# MAGIC import time
# MAGIC
# MAGIC def connect_with_retry(conn_params, max_retries=3):
# MAGIC     """Connect with exponential backoff retry logic."""
# MAGIC     for attempt in range(max_retries):
# MAGIC         try:
# MAGIC             conn = psycopg.connect(**conn_params)
# MAGIC             return conn
# MAGIC         except OperationalError as e:
# MAGIC             if attempt < max_retries - 1:
# MAGIC                 wait_time = 2 ** attempt  # exponential backoff
# MAGIC                 print(f"Connection failed, retrying in {wait_time}s...")
# MAGIC                 time.sleep(wait_time)
# MAGIC             else:
# MAGIC                 raise
# MAGIC         except InterfaceError as e:
# MAGIC             print(f"Interface error: {e}")
# MAGIC             raise
# MAGIC ```
# MAGIC
# MAGIC ### Transaction Error Handling
# MAGIC
# MAGIC ```python
# MAGIC def safe_transaction(conn, operations):
# MAGIC     """Execute operations in a transaction with rollback on error."""
# MAGIC     try:
# MAGIC         with conn.cursor() as cur:
# MAGIC             for operation in operations:
# MAGIC                 cur.execute(operation)
# MAGIC             conn.commit()
# MAGIC             return True
# MAGIC     except psycopg.Error as e:
# MAGIC         conn.rollback()
# MAGIC         print(f"Transaction failed: {e}")
# MAGIC         return False
# MAGIC ```
# MAGIC
# MAGIC ### Pool Exhaustion Handling
# MAGIC
# MAGIC ```python
# MAGIC from psycopg_pool import PoolTimeout
# MAGIC
# MAGIC try:
# MAGIC     with pool.connection(timeout=5) as conn:
# MAGIC         # perform operations
# MAGIC         pass
# MAGIC except PoolTimeout:
# MAGIC     print("Pool exhausted - no connections available")
# MAGIC     # Handle gracefully: queue request, return error, etc.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Tips
# MAGIC
# MAGIC ### Connection Optimization
# MAGIC
# MAGIC 1. **Reuse connections** via connection pooling
# MAGIC 2. **Minimize connection parameters** in queries
# MAGIC 3. **Use prepared statements** for repeated queries
# MAGIC 4. **Enable keepalives** to prevent idle disconnections
# MAGIC
# MAGIC ### Query Optimization
# MAGIC
# MAGIC 1. **Use indexes** on frequently queried columns
# MAGIC 2. **Fetch only needed columns** (avoid SELECT *)
# MAGIC 3. **Batch operations** when possible
# MAGIC 4. **Use transactions** for multiple related operations
# MAGIC
# MAGIC ### Pool Sizing Guidelines
# MAGIC
# MAGIC ```
# MAGIC Optimal pool size = (Core count * 2) + Disk spindles
# MAGIC ```
# MAGIC
# MAGIC For Lakebase serverless architecture:
# MAGIC - **Start with**: `min_size=2`, `max_size=10`
# MAGIC - **Monitor**: Connection usage and wait times
# MAGIC - **Adjust**: Based on actual workload patterns
# MAGIC
# MAGIC ### Monitoring Metrics
# MAGIC
# MAGIC Track these metrics for optimal performance:
# MAGIC - Connection wait times
# MAGIC - Pool utilization (active/idle ratio)
# MAGIC - Query latency (p50, p95, p99)
# MAGIC - Connection errors and retries
# MAGIC - Transaction rollback rate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Multiple access methods** available: SQL Warehouse, Notebooks, Python, External tools
# MAGIC
# MAGIC ✅ **Connection strings** follow standard PostgreSQL format
# MAGIC
# MAGIC ✅ **Security first**: Always use secrets management, SSL/TLS, and parameterized queries
# MAGIC
# MAGIC ✅ **psycopg3** is the recommended Python adapter for Lakebase
# MAGIC
# MAGIC ✅ **Single connections** are simple but limited; use for scripts and low-concurrency tasks
# MAGIC
# MAGIC ✅ **Connection pools** are essential for production applications and high-concurrency workloads
# MAGIC
# MAGIC ✅ **Proper error handling** and retry logic improve application reliability
# MAGIC
# MAGIC ✅ **Monitor and optimize** connection usage for best performance
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand connection methods and best practices, you're ready for hands-on implementation:
# MAGIC
# MAGIC - **Lab 4: Single Connection Python to Access Lakebase** - Learn basic psycopg3 patterns
# MAGIC - **Lab 5: Python Connection Pool for Lakebase** - Implement production-ready connection pooling
# MAGIC - **Lab 6: Sync Tables from Unity Catalog to Lakebase** - Bridge analytical and operational data
