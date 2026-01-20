# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Single Connection Python to Access Lakebase
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Install and configure the psycopg3 library for Python
# MAGIC - Establish secure connections to Lakebase using credentials
# MAGIC - Perform CRUD operations (Create, Read, Update, Delete) using Python
# MAGIC - Implement transaction management with commit and rollback
# MAGIC - Handle errors and exceptions gracefully
# MAGIC - Apply connection cleanup best practices
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lab 2: Creating and Exploring a Lakebase PostgreSQL Database
# MAGIC - Completion of Lecture 3: Ways to Access Your Database
# MAGIC - Basic Python programming skills
# MAGIC - Database credentials from Lab 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you'll learn to access Lakebase databases programmatically using Python and the **psycopg3** library. This is the foundation for building operational applications, APIs, and data-driven services.
# MAGIC
# MAGIC ### What You'll Build
# MAGIC
# MAGIC We'll create Python functions to:
# MAGIC 1. Connect to your Lakebase database
# MAGIC 2. Query customer and order data
# MAGIC 3. Insert new records
# MAGIC 4. Update existing data
# MAGIC 5. Delete records
# MAGIC 6. Handle transactions and errors
# MAGIC
# MAGIC ### Time Estimate
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install psycopg3
# MAGIC
# MAGIC First, let's install the PostgreSQL adapter for Python.

# COMMAND ----------

# Install psycopg3 with binary extensions for better performance
%pip install psycopg[binary] --quiet

# Restart Python to ensure the package is available
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import Required Libraries

# COMMAND ----------

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import json
from datetime import datetime, date
from decimal import Decimal

# For secure credential management
from databricks.sdk.runtime import dbutils

print("✅ Libraries imported successfully")
print(f"psycopg version: {psycopg.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Configure Database Credentials
# MAGIC
# MAGIC ### Important Security Note
# MAGIC
# MAGIC In production, **never hardcode credentials**. Instead:
# MAGIC 1. Store credentials in **Databricks Secrets**
# MAGIC 2. Use **Service Principals** for applications
# MAGIC 3. Implement **credential rotation**
# MAGIC
# MAGIC ### Setting Up Secrets (One-Time Setup)
# MAGIC
# MAGIC If you haven't already, create a secrets scope and store your credentials:
# MAGIC
# MAGIC ```bash
# MAGIC # Using Databricks CLI
# MAGIC databricks secrets create-scope --scope lakebase
# MAGIC databricks secrets put --scope lakebase --key db_host
# MAGIC databricks secrets put --scope lakebase --key db_port
# MAGIC databricks secrets put --scope lakebase --key db_name
# MAGIC databricks secrets put --scope lakebase --key db_user
# MAGIC databricks secrets put --scope lakebase --key db_password
# MAGIC ```
# MAGIC
# MAGIC ### For This Lab
# MAGIC
# MAGIC We'll use a hybrid approach for demonstration:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📝 TODO: Update Your Connection Parameters
# MAGIC
# MAGIC Replace the placeholder values below with your actual database connection information from Lab 2.

# COMMAND ----------

# Option 1: For production - retrieve from Databricks Secrets (recommended)
# Uncomment and use these lines after setting up your secrets:
# db_host = dbutils.secrets.get(scope="lakebase", key="db_host")
# db_port = dbutils.secrets.get(scope="lakebase", key="db_port")
# db_name = dbutils.secrets.get(scope="lakebase", key="db_name")
# db_user = dbutils.secrets.get(scope="lakebase", key="db_user")
# db_password = dbutils.secrets.get(scope="lakebase", key="db_password")

# Option 2: For this lab - use widgets for configuration (temporary)
# Create widgets to input connection parameters
dbutils.widgets.text("db_host", "your-workspace.cloud.databricks.com", "Database Host")
dbutils.widgets.text("db_port", "5432", "Port")
dbutils.widgets.text("db_name", "workshop_db", "Database Name")
dbutils.widgets.text("db_user", "lakebase_user", "Username")
dbutils.widgets.text("db_password", "", "Password")

# Retrieve values from widgets
db_host = dbutils.widgets.get("db_host")
db_port = dbutils.widgets.get("db_port")
db_name = dbutils.widgets.get("db_name")
db_user = dbutils.widgets.get("db_user")
db_password = dbutils.widgets.get("db_password")

# Validate that credentials are provided
if not db_password or db_host == "your-workspace.cloud.databricks.com":
    print("⚠️  Please update the widget values with your actual database credentials!")
else:
    print("✅ Connection parameters configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Connection Parameters

# COMMAND ----------

# Build connection parameters dictionary
conn_params = {
    'host': db_host,
    'port': int(db_port),
    'dbname': db_name,
    'user': db_user,
    'password': db_password,
    'sslmode': 'require',  # Always use SSL for security
    'connect_timeout': 10,  # Timeout after 10 seconds
    'application_name': 'lakebase_lab_single_connection'  # For monitoring
}

# Alternative: Build connection string
conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"

print("✅ Connection configuration ready")
print(f"   Host: {db_host}")
print(f"   Database: {db_name}")
print(f"   User: {db_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Database Connection
# MAGIC
# MAGIC Let's verify we can connect to the database.

# COMMAND ----------

def test_connection(conn_params):
    """Test database connectivity."""
    try:
        # Establish connection
        with psycopg.connect(**conn_params) as conn:
            # Create a cursor
            with conn.cursor() as cur:
                # Execute a simple query
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                
                # Get current timestamp
                cur.execute("SELECT CURRENT_TIMESTAMP;")
                current_time = cur.fetchone()[0]
                
                print("✅ Connection successful!")
                print(f"   PostgreSQL version: {version[:50]}...")
                print(f"   Server time: {current_time}")
                return True
    except psycopg.OperationalError as e:
        print(f"❌ Connection failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

# Test the connection
test_connection(conn_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Read Operations (SELECT)
# MAGIC
# MAGIC Let's query data from the tables we created in Lab 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic SELECT Query

# COMMAND ----------

def get_all_customers(conn_params, limit=10):
    """Retrieve all customers with a limit."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            # Execute query
            cur.execute("""
                SELECT customer_id, email, first_name, last_name, 
                       country, loyalty_points, signup_date
                FROM customers
                ORDER BY signup_date DESC
                LIMIT %s
            """, (limit,))
            
            # Fetch all results
            results = cur.fetchall()
            
            # Display results
            print(f"Found {len(results)} customers:\n")
            for row in results:
                print(f"ID: {row[0]:<5} | {row[1]:<30} | {row[2]} {row[3]:<15} | {row[4]} | Points: {row[5]}")
            
            return results

# Get all customers
customers = get_all_customers(conn_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT with Dictionary Cursor
# MAGIC
# MAGIC Using `dict_row` makes results easier to work with:

# COMMAND ----------

def get_customers_as_dict(conn_params):
    """Retrieve customers as dictionary objects."""
    with psycopg.connect(**conn_params) as conn:
        # Use dict_row for dictionary-style access
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT customer_id, email, first_name, last_name, 
                       country, loyalty_points
                FROM customers
                ORDER BY loyalty_points DESC
            """)
            
            results = cur.fetchall()
            
            print("Top customers by loyalty points:\n")
            for customer in results:
                print(f"{customer['first_name']} {customer['last_name']}:")
                print(f"  Email: {customer['email']}")
                print(f"  Country: {customer['country']}")
                print(f"  Points: {customer['loyalty_points']}")
                print()
            
            return results

# Get customers as dictionaries
customer_dicts = get_customers_as_dict(conn_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT with JOIN

# COMMAND ----------

def get_customer_orders(conn_params, customer_id):
    """Get all orders for a specific customer."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT 
                    o.order_id,
                    o.order_date,
                    o.total_amount,
                    o.order_status,
                    c.first_name || ' ' || c.last_name AS customer_name,
                    c.email
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.customer_id = %s
                ORDER BY o.order_date DESC
            """, (customer_id,))
            
            orders = cur.fetchall()
            
            if orders:
                print(f"Orders for {orders[0]['customer_name']} ({orders[0]['email']}):\n")
                for order in orders:
                    print(f"Order #{order['order_id']}")
                    print(f"  Date: {order['order_date']}")
                    print(f"  Amount: ${order['total_amount']}")
                    print(f"  Status: {order['order_status']}")
                    print()
            else:
                print(f"No orders found for customer {customer_id}")
            
            return orders

# Get orders for customer 1
orders = get_customer_orders(conn_params, customer_id=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate Query

# COMMAND ----------

def get_order_statistics(conn_params):
    """Get order statistics by status."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT 
                    order_status,
                    COUNT(*) AS num_orders,
                    SUM(total_amount) AS total_revenue,
                    AVG(total_amount) AS avg_order_value,
                    MIN(total_amount) AS min_order,
                    MAX(total_amount) AS max_order
                FROM orders
                GROUP BY order_status
                ORDER BY total_revenue DESC
            """)
            
            stats = cur.fetchall()
            
            print("Order Statistics by Status:\n")
            print(f"{'Status':<15} {'Count':<8} {'Total Revenue':<15} {'Avg Value':<12}")
            print("-" * 60)
            for row in stats:
                print(f"{row['order_status']:<15} {row['num_orders']:<8} "
                      f"${float(row['total_revenue']):<14.2f} ${float(row['avg_order_value']):<11.2f}")
            
            return stats

# Get statistics
stats = get_order_statistics(conn_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Operations (INSERT)
# MAGIC
# MAGIC Let's insert new records into the database.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert a Single Customer

# COMMAND ----------

def insert_customer(conn_params, customer_data):
    """Insert a new customer and return the customer_id."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO customers 
                (customer_id, email, first_name, last_name, country, signup_date, loyalty_points)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING customer_id
            """, (
                customer_data['customer_id'],
                customer_data['email'],
                customer_data['first_name'],
                customer_data['last_name'],
                customer_data['country'],
                customer_data['signup_date'],
                customer_data.get('loyalty_points', 0)
            ))
            
            # Get the returned customer_id
            customer_id = cur.fetchone()[0]
            
            # Commit the transaction
            conn.commit()
            
            print(f"✅ Customer inserted successfully!")
            print(f"   Customer ID: {customer_id}")
            print(f"   Name: {customer_data['first_name']} {customer_data['last_name']}")
            
            return customer_id

# Insert a new customer
new_customer = {
    'customer_id': 100,
    'email': 'frank.miller@email.com',
    'first_name': 'Frank',
    'last_name': 'Miller',
    'country': 'DE',
    'signup_date': date.today(),
    'loyalty_points': 0
}

customer_id = insert_customer(conn_params, new_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Multiple Records (Batch)

# COMMAND ----------

def insert_products_batch(conn_params, products):
    """Insert multiple products efficiently."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            # Use executemany for batch inserts
            cur.executemany("""
                INSERT INTO products 
                (product_id, product_name, category, price, stock_quantity, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, products)
            
            # Commit the transaction
            conn.commit()
            
            print(f"✅ Inserted {len(products)} products successfully!")

# Insert multiple products
new_products = [
    (201, 'Wireless Headphones', 'Electronics', 79.99, 120, True),
    (202, 'Phone Case', 'Accessories', 15.99, 300, True),
    (203, 'Screen Protector', 'Accessories', 9.99, 500, True),
]

insert_products_batch(conn_params, new_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Update Operations (UPDATE)
# MAGIC
# MAGIC Let's modify existing records.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update a Single Record

# COMMAND ----------

def update_customer_loyalty_points(conn_params, customer_id, points_to_add):
    """Add loyalty points to a customer."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            # Update the record
            cur.execute("""
                UPDATE customers
                SET 
                    loyalty_points = loyalty_points + %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE customer_id = %s
                RETURNING customer_id, loyalty_points
            """, (points_to_add, customer_id))
            
            # Get updated values
            result = cur.fetchone()
            
            if result:
                conn.commit()
                print(f"✅ Updated customer {result[0]}")
                print(f"   New loyalty points: {result[1]}")
                return result[1]
            else:
                print(f"❌ Customer {customer_id} not found")
                return None

# Add 500 points to customer 1
new_points = update_customer_loyalty_points(conn_params, customer_id=1, points_to_add=500)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update with Conditional Logic

# COMMAND ----------

def update_order_status(conn_params, order_id, new_status):
    """Update order status and timestamp."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # Check current status
            cur.execute("""
                SELECT order_id, order_status, total_amount
                FROM orders
                WHERE order_id = %s
            """, (order_id,))
            
            order = cur.fetchone()
            
            if not order:
                print(f"❌ Order {order_id} not found")
                return False
            
            old_status = order['order_status']
            
            # Update status
            cur.execute("""
                UPDATE orders
                SET 
                    order_status = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE order_id = %s
            """, (new_status, order_id))
            
            conn.commit()
            
            print(f"✅ Order {order_id} updated")
            print(f"   Status changed: {old_status} → {new_status}")
            return True

# Update order status
update_order_status(conn_params, order_id=1002, new_status='delivered')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bulk Update

# COMMAND ----------

def deactivate_out_of_stock_products(conn_params):
    """Mark products with zero stock as inactive."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE products
                SET is_active = FALSE
                WHERE stock_quantity = 0 AND is_active = TRUE
            """)
            
            updated_count = cur.rowcount
            conn.commit()
            
            print(f"✅ Deactivated {updated_count} out-of-stock products")
            return updated_count

# Deactivate out of stock items
count = deactivate_out_of_stock_products(conn_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Delete Operations (DELETE)
# MAGIC
# MAGIC Let's remove records from the database.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete a Single Record

# COMMAND ----------

def delete_customer(conn_params, customer_id):
    """Delete a customer by ID."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            # Check if customer has orders
            cur.execute("""
                SELECT COUNT(*) FROM orders WHERE customer_id = %s
            """, (customer_id,))
            
            order_count = cur.fetchone()[0]
            
            if order_count > 0:
                print(f"⚠️  Cannot delete customer {customer_id}: has {order_count} orders")
                print("   Delete orders first, or use CASCADE")
                return False
            
            # Delete the customer
            cur.execute("""
                DELETE FROM customers WHERE customer_id = %s
            """, (customer_id,))
            
            if cur.rowcount > 0:
                conn.commit()
                print(f"✅ Customer {customer_id} deleted")
                return True
            else:
                print(f"❌ Customer {customer_id} not found")
                return False

# Delete the test customer we inserted
delete_customer(conn_params, customer_id=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conditional Delete

# COMMAND ----------

def delete_old_test_products(conn_params):
    """Delete test products (IDs >= 200)."""
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM products
                WHERE product_id >= 200
                RETURNING product_id, product_name
            """)
            
            deleted = cur.fetchall()
            conn.commit()
            
            print(f"✅ Deleted {len(deleted)} test products:")
            for product_id, name in deleted:
                print(f"   - {product_id}: {name}")
            
            return len(deleted)

# Delete test products
count = delete_old_test_products(conn_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Transaction Management
# MAGIC
# MAGIC Transactions ensure data consistency with ACID properties.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Process an Order (Multi-Step Transaction)

# COMMAND ----------

def process_order(conn_params, order_data, product_updates):
    """
    Process an order with multiple steps in a transaction.
    
    Steps:
    1. Create order record
    2. Update product stock
    3. Award loyalty points
    
    If any step fails, roll back all changes.
    """
    try:
        with psycopg.connect(**conn_params) as conn:
            # Set autocommit to False (default) for transaction control
            conn.autocommit = False
            
            with conn.cursor() as cur:
                print("Starting transaction...")
                
                # Step 1: Insert order
                cur.execute("""
                    INSERT INTO orders 
                    (order_id, customer_id, order_date, total_amount, order_status, shipping_address)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    order_data['order_id'],
                    order_data['customer_id'],
                    order_data['order_date'],
                    order_data['total_amount'],
                    order_data['order_status'],
                    order_data['shipping_address']
                ))
                print(f"  ✓ Order {order_data['order_id']} created")
                
                # Step 2: Update product stock for each item
                for product_id, quantity in product_updates:
                    cur.execute("""
                        UPDATE products
                        SET stock_quantity = stock_quantity - %s
                        WHERE product_id = %s AND stock_quantity >= %s
                    """, (quantity, product_id, quantity))
                    
                    if cur.rowcount == 0:
                        raise Exception(f"Insufficient stock for product {product_id}")
                    
                    print(f"  ✓ Updated stock for product {product_id} (-{quantity})")
                
                # Step 3: Award loyalty points
                points_to_award = int(order_data['total_amount'] / 10)  # 1 point per $10
                cur.execute("""
                    UPDATE customers
                    SET loyalty_points = loyalty_points + %s
                    WHERE customer_id = %s
                """, (points_to_award, order_data['customer_id']))
                print(f"  ✓ Awarded {points_to_award} loyalty points")
                
                # Commit all changes
                conn.commit()
                print(f"\n✅ Transaction committed successfully!")
                print(f"   Order {order_data['order_id']} processed")
                return True
                
    except Exception as e:
        # Rollback on any error
        conn.rollback()
        print(f"\n❌ Transaction rolled back due to error:")
        print(f"   {e}")
        return False

# Process a new order
new_order = {
    'order_id': 2001,
    'customer_id': 2,
    'order_date': datetime.now(),
    'total_amount': Decimal('149.98'),
    'order_status': 'processing',
    'shipping_address': '456 Oak Ave, Toronto, ON M5V 2T6'
}

# Product updates: [(product_id, quantity), ...]
product_updates = [
    (102, 2),  # 2 wireless mice
    (103, 1),  # 1 USB-C cable
]

success = process_order(conn_params, new_order, product_updates)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrating Rollback on Error

# COMMAND ----------

def test_transaction_rollback(conn_params):
    """Demonstrate automatic rollback on error."""
    try:
        with psycopg.connect(**conn_params) as conn:
            conn.autocommit = False
            
            with conn.cursor() as cur:
                print("Starting transaction with intentional error...\n")
                
                # This will succeed
                cur.execute("""
                    INSERT INTO customers 
                    (customer_id, email, first_name, last_name, country, signup_date)
                    VALUES (999, 'test@example.com', 'Test', 'User', 'US', CURRENT_DATE)
                """)
                print("  ✓ Customer 999 inserted")
                
                # This will fail (duplicate key)
                cur.execute("""
                    INSERT INTO customers 
                    (customer_id, email, first_name, last_name, country, signup_date)
                    VALUES (999, 'another@example.com', 'Another', 'Test', 'US', CURRENT_DATE)
                """)
                print("  ✓ Second customer inserted")
                
                conn.commit()
                
    except psycopg.errors.UniqueViolation as e:
        conn.rollback()
        print(f"\n❌ Error occurred: Duplicate customer_id")
        print(f"   Transaction rolled back - no changes applied")
        
        # Verify customer 999 does not exist
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM customers WHERE customer_id = 999")
                count = cur.fetchone()[0]
                print(f"\n✅ Verified: Customer 999 count = {count} (should be 0)")

# Test rollback behavior
test_transaction_rollback(conn_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Error Handling Best Practices

# COMMAND ----------

def robust_query_with_error_handling(conn_params, customer_id):
    """
    Example of comprehensive error handling for database operations.
    """
    try:
        # Attempt connection
        conn = psycopg.connect(**conn_params)
        
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                # Execute query
                cur.execute("""
                    SELECT * FROM customers WHERE customer_id = %s
                """, (customer_id,))
                
                result = cur.fetchone()
                
                if result:
                    print(f"✅ Found customer: {result['first_name']} {result['last_name']}")
                    return result
                else:
                    print(f"ℹ️  Customer {customer_id} not found")
                    return None
                    
        except psycopg.Error as e:
            print(f"❌ Database error: {e}")
            conn.rollback()
            return None
            
        finally:
            # Always close the connection
            conn.close()
            
    except psycopg.OperationalError as e:
        print(f"❌ Connection error: {e}")
        print("   Check your credentials and network connectivity")
        return None
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

# Test error handling
result = robust_query_with_error_handling(conn_params, customer_id=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Helper Functions for Common Patterns

# COMMAND ----------

class LakebaseConnection:
    """
    Helper class to encapsulate Lakebase connection patterns.
    """
    
    def __init__(self, conn_params):
        self.conn_params = conn_params
    
    def execute_query(self, query, params=None, fetch='all'):
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters (tuple or dict)
            fetch: 'all', 'one', or 'none'
        """
        try:
            with psycopg.connect(**self.conn_params) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query, params)
                    
                    if fetch == 'all':
                        return cur.fetchall()
                    elif fetch == 'one':
                        return cur.fetchone()
                    else:
                        return None
        except psycopg.Error as e:
            print(f"Query error: {e}")
            return None
    
    def execute_update(self, query, params=None):
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Returns number of affected rows.
        """
        try:
            with psycopg.connect(**self.conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    affected = cur.rowcount
                    conn.commit()
                    return affected
        except psycopg.Error as e:
            print(f"Update error: {e}")
            return 0
    
    def execute_transaction(self, operations):
        """
        Execute multiple operations in a transaction.
        
        Args:
            operations: List of (query, params) tuples
        """
        try:
            with psycopg.connect(**self.conn_params) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    for query, params in operations:
                        cur.execute(query, params)
                    conn.commit()
                    return True
        except psycopg.Error as e:
            conn.rollback()
            print(f"Transaction error: {e}")
            return False

# Example usage
db = LakebaseConnection(conn_params)

# Simple query
customers = db.execute_query("SELECT * FROM customers LIMIT 3")
print(f"Found {len(customers)} customers")

# Update
affected = db.execute_update(
    "UPDATE customers SET loyalty_points = loyalty_points + %s WHERE customer_id = %s",
    (10, 1)
)
print(f"Updated {affected} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ Installed and configured psycopg3 for Python
# MAGIC
# MAGIC ✅ Established secure connections to Lakebase
# MAGIC
# MAGIC ✅ Performed READ operations with SELECT queries
# MAGIC
# MAGIC ✅ Implemented CREATE operations with INSERT statements
# MAGIC
# MAGIC ✅ Executed UPDATE operations to modify data
# MAGIC
# MAGIC ✅ Performed DELETE operations to remove records
# MAGIC
# MAGIC ✅ Managed transactions with commit and rollback
# MAGIC
# MAGIC ✅ Implemented comprehensive error handling
# MAGIC
# MAGIC ✅ Created reusable helper functions and classes
# MAGIC
# MAGIC ### Key Concepts Learned
# MAGIC
# MAGIC - **Context managers** (`with` statements) ensure proper connection cleanup
# MAGIC - **Parameterized queries** prevent SQL injection attacks
# MAGIC - **Transactions** maintain data consistency with ACID guarantees
# MAGIC - **Error handling** makes applications robust and reliable
# MAGIC - **Dictionary cursors** make result processing easier
# MAGIC
# MAGIC ### Best Practices Applied
# MAGIC
# MAGIC 🔒 Secure credential management with Databricks Secrets
# MAGIC
# MAGIC 🔒 Always use SSL/TLS for connections
# MAGIC
# MAGIC 🔒 Never concatenate user input into SQL queries
# MAGIC
# MAGIC 🔄 Always close connections (use context managers)
# MAGIC
# MAGIC 🔄 Handle errors gracefully with try/except blocks
# MAGIC
# MAGIC 🔄 Use transactions for multi-step operations
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand single connection patterns, you're ready to learn about:
# MAGIC
# MAGIC - **Lab 5: Python Connection Pool** - Implement connection pooling for high-concurrency workloads
# MAGIC - **Lab 6: Sync Tables from Unity Catalog** - Bridge analytical and operational data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Remove widgets
dbutils.widgets.removeAll()

print("✅ Lab complete!")
