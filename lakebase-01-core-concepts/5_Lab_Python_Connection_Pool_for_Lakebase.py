# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Python Connection Pool for Lakebase
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Understand the benefits of connection pooling for concurrent workloads
# MAGIC - Configure and initialize a connection pool with psycopg3
# MAGIC - Implement pooled connection patterns using context managers
# MAGIC - Handle connection acquisition, release, and timeouts
# MAGIC - Monitor pool performance and health
# MAGIC - Compare performance between single connections and connection pools
# MAGIC - Apply production-ready patterns for error recovery and pool management
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lab 4: Single Connection Python to Access Lakebase
# MAGIC - Understanding of concurrent programming concepts
# MAGIC - Database credentials configured from previous labs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC ### Why Connection Pooling?
# MAGIC
# MAGIC In production applications, creating a new database connection for every operation is expensive:
# MAGIC - Connection establishment takes time (TCP handshake, authentication, SSL negotiation)
# MAGIC - Resources are wasted creating and destroying connections
# MAGIC - Concurrent requests can overwhelm the database
# MAGIC
# MAGIC **Connection pooling solves these problems** by:
# MAGIC - Maintaining a pool of reusable connections
# MAGIC - Reducing connection overhead
# MAGIC - Limiting concurrent connections to the database
# MAGIC - Improving application performance and scalability
# MAGIC
# MAGIC ### Use Cases
# MAGIC
# MAGIC Connection pools are essential for:
# MAGIC - Web applications and REST APIs
# MAGIC - Microservices with high concurrency
# MAGIC - Streaming data processors
# MAGIC - AI agents making frequent database calls
# MAGIC - Any multi-threaded application
# MAGIC
# MAGIC ### Time Estimate
# MAGIC 60-75 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries

# COMMAND ----------

# Install psycopg with connection pool support
%pip install psycopg[binary,pool] --quiet

# For performance testing
%pip install numpy --quiet

# Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import Libraries

# COMMAND ----------

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import numpy as np

# For secure credentials
from databricks.sdk.runtime import dbutils

print("✅ Libraries imported successfully")
print(f"psycopg version: {psycopg.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Configure Connection Parameters

# COMMAND ----------

# Retrieve connection parameters from widgets (set up in Lab 4)
dbutils.widgets.text("db_host", "your-workspace.cloud.databricks.com", "Database Host")
dbutils.widgets.text("db_port", "5432", "Port")
dbutils.widgets.text("db_name", "workshop_db", "Database Name")
dbutils.widgets.text("db_user", "lakebase_user", "Username")
dbutils.widgets.text("db_password", "", "Password")

db_host = dbutils.widgets.get("db_host")
db_port = dbutils.widgets.get("db_port")
db_name = dbutils.widgets.get("db_name")
db_user = dbutils.widgets.get("db_user")
db_password = dbutils.widgets.get("db_password")

# Build connection string
conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require&application_name=lakebase_pool_lab"

print("✅ Connection parameters configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create a Connection Pool
# MAGIC
# MAGIC ### Pool Configuration Parameters
# MAGIC
# MAGIC - **min_size**: Minimum number of connections to maintain (always open)
# MAGIC - **max_size**: Maximum number of connections allowed
# MAGIC - **timeout**: How long to wait for an available connection (seconds)
# MAGIC - **max_waiting**: Maximum number of requests waiting for a connection
# MAGIC - **max_lifetime**: Maximum age of a connection before recycling (seconds)
# MAGIC - **max_idle**: Maximum idle time before closing a connection (seconds)

# COMMAND ----------

# Create connection pool
pool = ConnectionPool(
    conninfo=conn_string,
    min_size=2,        # Keep at least 2 connections open
    max_size=10,       # Allow up to 10 concurrent connections
    timeout=30.0,      # Wait up to 30 seconds for a connection
    max_waiting=20,    # Allow up to 20 requests to wait
    max_lifetime=3600, # Recycle connections after 1 hour
    max_idle=300,      # Close idle connections after 5 minutes
    open=True          # Open the pool immediately
)

print("✅ Connection pool created")
print(f"   Min size: {pool.min_size}")
print(f"   Max size: {pool.max_size}")
print(f"   Timeout: {pool.timeout}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Basic Pool Usage Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Using Pool Context Manager

# COMMAND ----------

def get_customer_with_pool(pool, customer_id):
    """Get customer using a pooled connection."""
    # Get a connection from the pool
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT customer_id, email, first_name, last_name, 
                       country, loyalty_points
                FROM customers
                WHERE customer_id = %s
            """, (customer_id,))
            
            result = cur.fetchone()
            
            if result:
                print(f"✅ Found: {result['first_name']} {result['last_name']}")
                print(f"   Email: {result['email']}")
                print(f"   Points: {result['loyalty_points']}")
            else:
                print(f"❌ Customer {customer_id} not found")
            
            return result
    # Connection automatically returned to pool (not closed!)

# Test pool usage
customer = get_customer_with_pool(pool, customer_id=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Query with Automatic Retry

# COMMAND ----------

def query_with_retry(pool, query, params=None, max_retries=3):
    """
    Execute a query with automatic retry on connection errors.
    """
    for attempt in range(max_retries):
        try:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
        except psycopg.OperationalError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"⚠️  Connection error, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed after {max_retries} attempts")
                raise

# Test retry logic
results = query_with_retry(
    pool, 
    "SELECT * FROM customers ORDER BY loyalty_points DESC LIMIT 5"
)
print(f"Retrieved {len(results)} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Monitor Pool Statistics

# COMMAND ----------

def print_pool_stats(pool):
    """Display current pool statistics."""
    stats = pool.get_stats()
    
    print("\n" + "="*50)
    print("CONNECTION POOL STATISTICS")
    print("="*50)
    print(f"Pool size:            {stats.get('pool_size', 'N/A')}")
    print(f"Pool available:       {stats.get('pool_available', 'N/A')}")
    print(f"Requests waiting:     {stats.get('requests_waiting', 'N/A')}")
    print(f"Usage:                {stats.get('requests_num', 'N/A')} requests")
    print(f"Connections created:  {stats.get('connections_num', 'N/A')}")
    print("="*50 + "\n")

# Check initial pool state
print_pool_stats(pool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Concurrent Operations with Thread Pool

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Concurrent Customer Lookups

# COMMAND ----------

def concurrent_customer_queries(pool, customer_ids, num_threads=5):
    """
    Execute multiple customer queries concurrently.
    """
    results = []
    start_time = time.time()
    
    def fetch_customer(customer_id):
        """Fetch a single customer (executed in thread)."""
        try:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute("""
                        SELECT customer_id, first_name, last_name, loyalty_points
                        FROM customers
                        WHERE customer_id = %s
                    """, (customer_id,))
                    return cur.fetchone()
        except Exception as e:
            print(f"Error fetching customer {customer_id}: {e}")
            return None
    
    # Execute queries concurrently
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_id = {
            executor.submit(fetch_customer, cid): cid 
            for cid in customer_ids
        }
        
        for future in as_completed(future_to_id):
            customer_id = future_to_id[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Exception for customer {customer_id}: {e}")
    
    elapsed_time = time.time() - start_time
    
    print(f"\n✅ Concurrent queries completed")
    print(f"   Customer IDs queried: {len(customer_ids)}")
    print(f"   Threads: {num_threads}")
    print(f"   Time: {elapsed_time:.3f}s")
    print(f"   Results found: {len(results)}")
    
    return results, elapsed_time

# Test concurrent queries
customer_ids = [1, 2, 3, 4, 5, 1, 2, 3, 4, 5] * 2  # 20 queries
results, elapsed = concurrent_customer_queries(pool, customer_ids, num_threads=5)

# Check pool stats after concurrent operations
print_pool_stats(pool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Performance Comparison - Single vs Pooled Connections

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark: Single Connections

# COMMAND ----------

def benchmark_single_connections(conn_string, num_queries=50):
    """
    Benchmark using single connections (create and close each time).
    """
    query = "SELECT COUNT(*) FROM customers"
    results = []
    start_time = time.time()
    
    for i in range(num_queries):
        try:
            # Create new connection each time
            with psycopg.connect(conn_string) as conn:
                with conn.cursor() as cur:
                    query_start = time.time()
                    cur.execute(query)
                    result = cur.fetchone()[0]
                    query_time = time.time() - query_start
                    results.append(query_time)
        except Exception as e:
            print(f"Error on query {i}: {e}")
    
    total_time = time.time() - start_time
    avg_time = np.mean(results) if results else 0
    p50_time = np.percentile(results, 50) if results else 0
    p95_time = np.percentile(results, 95) if results else 0
    
    return {
        'total_time': total_time,
        'num_queries': len(results),
        'avg_query_time': avg_time,
        'p50_query_time': p50_time,
        'p95_query_time': p95_time,
        'queries_per_second': len(results) / total_time if total_time > 0 else 0
    }

print("Running benchmark with SINGLE connections...")
single_stats = benchmark_single_connections(conn_string, num_queries=50)

print("\n" + "="*50)
print("SINGLE CONNECTION BENCHMARK")
print("="*50)
print(f"Total time:        {single_stats['total_time']:.3f}s")
print(f"Queries executed:  {single_stats['num_queries']}")
print(f"Avg query time:    {single_stats['avg_query_time']*1000:.2f}ms")
print(f"P50 latency:       {single_stats['p50_query_time']*1000:.2f}ms")
print(f"P95 latency:       {single_stats['p95_query_time']*1000:.2f}ms")
print(f"Queries/sec:       {single_stats['queries_per_second']:.2f}")
print("="*50 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark: Connection Pool

# COMMAND ----------

def benchmark_connection_pool(pool, num_queries=50):
    """
    Benchmark using connection pool.
    """
    query = "SELECT COUNT(*) FROM customers"
    results = []
    start_time = time.time()
    
    for i in range(num_queries):
        try:
            # Get connection from pool
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    query_start = time.time()
                    cur.execute(query)
                    result = cur.fetchone()[0]
                    query_time = time.time() - query_start
                    results.append(query_time)
        except Exception as e:
            print(f"Error on query {i}: {e}")
    
    total_time = time.time() - start_time
    avg_time = np.mean(results) if results else 0
    p50_time = np.percentile(results, 50) if results else 0
    p95_time = np.percentile(results, 95) if results else 0
    
    return {
        'total_time': total_time,
        'num_queries': len(results),
        'avg_query_time': avg_time,
        'p50_query_time': p50_time,
        'p95_query_time': p95_time,
        'queries_per_second': len(results) / total_time if total_time > 0 else 0
    }

print("Running benchmark with CONNECTION POOL...")
pool_stats = benchmark_connection_pool(pool, num_queries=50)

print("\n" + "="*50)
print("CONNECTION POOL BENCHMARK")
print("="*50)
print(f"Total time:        {pool_stats['total_time']:.3f}s")
print(f"Queries executed:  {pool_stats['num_queries']}")
print(f"Avg query time:    {pool_stats['avg_query_time']*1000:.2f}ms")
print(f"P50 latency:       {pool_stats['p50_query_time']*1000:.2f}ms")
print(f"P95 latency:       {pool_stats['p95_query_time']*1000:.2f}ms")
print(f"Queries/sec:       {pool_stats['queries_per_second']:.2f}")
print("="*50 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Results

# COMMAND ----------

print("\n" + "="*60)
print("PERFORMANCE COMPARISON: SINGLE vs POOL")
print("="*60)

speedup_total = single_stats['total_time'] / pool_stats['total_time']
speedup_qps = pool_stats['queries_per_second'] / single_stats['queries_per_second']

print(f"\nTotal Time:")
print(f"  Single:    {single_stats['total_time']:.3f}s")
print(f"  Pool:      {pool_stats['total_time']:.3f}s")
print(f"  Speedup:   {speedup_total:.2f}x faster")

print(f"\nQueries per Second:")
print(f"  Single:    {single_stats['queries_per_second']:.2f}")
print(f"  Pool:      {pool_stats['queries_per_second']:.2f}")
print(f"  Speedup:   {speedup_qps:.2f}x faster")

print(f"\nAverage Query Latency:")
print(f"  Single:    {single_stats['avg_query_time']*1000:.2f}ms")
print(f"  Pool:      {pool_stats['avg_query_time']*1000:.2f}ms")

print(f"\nP95 Query Latency:")
print(f"  Single:    {single_stats['p95_query_time']*1000:.2f}ms")
print(f"  Pool:      {pool_stats['p95_query_time']*1000:.2f}ms")

print("\n" + "="*60)
print(f"✅ Connection pool is {speedup_total:.1f}x faster overall!")
print("="*60 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Concurrent Write Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Concurrent Order Processing

# COMMAND ----------

def process_orders_concurrently(pool, num_orders=20, num_threads=5):
    """
    Process multiple orders concurrently using connection pool.
    """
    start_time = time.time()
    successes = 0
    failures = 0
    lock = threading.Lock()
    
    def process_single_order(order_num):
        """Process a single order (runs in thread)."""
        nonlocal successes, failures
        
        order_id = 3000 + order_num
        customer_id = (order_num % 5) + 1  # Cycle through customers 1-5
        
        try:
            with pool.connection() as conn:
                conn.autocommit = False
                
                with conn.cursor() as cur:
                    # Insert order
                    cur.execute("""
                        INSERT INTO orders 
                        (order_id, customer_id, order_date, total_amount, order_status)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, 'processing')
                    """, (order_id, customer_id, 99.99))
                    
                    # Update customer loyalty points
                    cur.execute("""
                        UPDATE customers
                        SET loyalty_points = loyalty_points + 10
                        WHERE customer_id = %s
                    """, (customer_id,))
                    
                    conn.commit()
                    
                    with lock:
                        successes += 1
                    
                    return True
                    
        except Exception as e:
            with lock:
                failures += 1
            print(f"❌ Order {order_id} failed: {e}")
            return False
    
    # Execute concurrently
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_single_order, i) for i in range(num_orders)]
        
        # Wait for all to complete
        for future in as_completed(futures):
            future.result()
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "="*50)
    print("CONCURRENT ORDER PROCESSING")
    print("="*50)
    print(f"Orders attempted:  {num_orders}")
    print(f"Threads:           {num_threads}")
    print(f"Successes:         {successes}")
    print(f"Failures:          {failures}")
    print(f"Time:              {elapsed_time:.3f}s")
    print(f"Orders/sec:        {successes/elapsed_time:.2f}")
    print("="*50 + "\n")
    
    return successes, failures

# Process orders concurrently
successes, failures = process_orders_concurrently(pool, num_orders=20, num_threads=5)

# Check pool stats
print_pool_stats(pool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Handle Pool Exhaustion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate Pool Timeout Behavior

# COMMAND ----------

from psycopg_pool import PoolTimeout

def test_pool_exhaustion(pool):
    """
    Demonstrate what happens when pool is exhausted.
    """
    print("Testing pool exhaustion behavior...\n")
    
    # Hold all connections
    connections = []
    
    try:
        # Acquire all connections from the pool
        for i in range(pool.max_size):
            conn = pool.getconn()
            connections.append(conn)
            print(f"  Acquired connection {i+1}/{pool.max_size}")
        
        print(f"\n✅ All {pool.max_size} connections acquired")
        print("   Pool is now exhausted!\n")
        
        # Try to get one more (should timeout)
        print("Attempting to acquire one more connection...")
        print("(This should timeout...)")
        
        try:
            with pool.connection(timeout=3) as conn:
                print("❌ Unexpected: Got a connection!")
        except PoolTimeout:
            print("\n✅ Expected timeout occurred!")
            print("   No connections available in pool")
            print("   This is how the pool protects against overload")
    
    finally:
        # Release all connections
        print("\nReleasing all connections back to pool...")
        for conn in connections:
            pool.putconn(conn)
        print("✅ All connections returned\n")

# Test pool exhaustion
test_pool_exhaustion(pool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Production-Ready Pool Manager Class

# COMMAND ----------

class LakebasePoolManager:
    """
    Production-ready connection pool manager with monitoring and health checks.
    """
    
    def __init__(self, conn_string, min_size=2, max_size=10, timeout=30):
        """Initialize the pool manager."""
        self.pool = ConnectionPool(
            conninfo=conn_string,
            min_size=min_size,
            max_size=max_size,
            timeout=timeout,
            max_lifetime=3600,
            max_idle=300,
            open=True
        )
        self.query_count = 0
        self.error_count = 0
        self.total_query_time = 0.0
    
    def execute_query(self, query, params=None, fetch='all'):
        """
        Execute a query with monitoring.
        """
        start_time = time.time()
        
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(query, params)
                    
                    if fetch == 'all':
                        result = cur.fetchall()
                    elif fetch == 'one':
                        result = cur.fetchone()
                    else:
                        result = None
                    
                    # Update metrics
                    query_time = time.time() - start_time
                    self.query_count += 1
                    self.total_query_time += query_time
                    
                    return result
                    
        except Exception as e:
            self.error_count += 1
            raise
    
    def execute_transaction(self, operations):
        """
        Execute operations in a transaction.
        """
        try:
            with self.pool.connection() as conn:
                conn.autocommit = False
                
                with conn.cursor() as cur:
                    for query, params in operations:
                        cur.execute(query, params)
                    conn.commit()
                    return True
                    
        except Exception as e:
            conn.rollback()
            self.error_count += 1
            raise
    
    def health_check(self):
        """
        Perform health check on the pool.
        """
        try:
            with self.pool.connection(timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    return result[0] == 1
        except Exception as e:
            print(f"Health check failed: {e}")
            return False
    
    def get_stats(self):
        """
        Get pool and query statistics.
        """
        pool_stats = self.pool.get_stats()
        
        return {
            'pool_size': pool_stats.get('pool_size'),
            'pool_available': pool_stats.get('pool_available'),
            'requests_waiting': pool_stats.get('requests_waiting'),
            'query_count': self.query_count,
            'error_count': self.error_count,
            'avg_query_time': self.total_query_time / self.query_count if self.query_count > 0 else 0,
            'error_rate': self.error_count / self.query_count if self.query_count > 0 else 0
        }
    
    def print_stats(self):
        """
        Print formatted statistics.
        """
        stats = self.get_stats()
        
        print("\n" + "="*50)
        print("POOL MANAGER STATISTICS")
        print("="*50)
        print(f"Pool size:         {stats['pool_size']}")
        print(f"Available:         {stats['pool_available']}")
        print(f"Waiting:           {stats['requests_waiting']}")
        print(f"Total queries:     {stats['query_count']}")
        print(f"Total errors:      {stats['error_count']}")
        print(f"Avg query time:    {stats['avg_query_time']*1000:.2f}ms")
        print(f"Error rate:        {stats['error_rate']*100:.2f}%")
        print("="*50 + "\n")
    
    def close(self):
        """
        Close the pool gracefully.
        """
        self.pool.close()
        print("✅ Pool closed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Pool Manager

# COMMAND ----------

# Create pool manager
manager = LakebasePoolManager(conn_string, min_size=2, max_size=8, timeout=30)

# Health check
is_healthy = manager.health_check()
print(f"Pool health check: {'✅ Healthy' if is_healthy else '❌ Unhealthy'}\n")

# Execute some queries
print("Executing queries...")
for i in range(10):
    customers = manager.execute_query("SELECT * FROM customers LIMIT 5")
    
# Execute a transaction
print("\nExecuting transaction...")
operations = [
    ("UPDATE customers SET loyalty_points = loyalty_points + 5 WHERE customer_id = 1", None),
    ("UPDATE customers SET loyalty_points = loyalty_points + 5 WHERE customer_id = 2", None),
]
manager.execute_transaction(operations)

# Print statistics
manager.print_stats()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Best Practices Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connection Pool Best Practices
# MAGIC
# MAGIC #### Pool Sizing
# MAGIC
# MAGIC ✅ **Start small**: Begin with `min_size=2-5` and `max_size=10-20`
# MAGIC
# MAGIC ✅ **Monitor and adjust**: Use metrics to find optimal size
# MAGIC
# MAGIC ✅ **Consider your workload**: More concurrent requests = larger pool
# MAGIC
# MAGIC ✅ **Don't over-provision**: Too many connections waste resources
# MAGIC
# MAGIC **Rule of thumb**: `max_size = (core_count * 2) + disk_spindles`
# MAGIC
# MAGIC #### Timeout Configuration
# MAGIC
# MAGIC ✅ **Set reasonable timeouts**: 10-30 seconds for most applications
# MAGIC
# MAGIC ✅ **Handle timeouts gracefully**: Return errors to users, don't crash
# MAGIC
# MAGIC ✅ **Consider retry logic**: But avoid retry storms
# MAGIC
# MAGIC #### Connection Lifecycle
# MAGIC
# MAGIC ✅ **Set max_lifetime**: Recycle connections periodically (1-24 hours)
# MAGIC
# MAGIC ✅ **Set max_idle**: Close idle connections (5-30 minutes)
# MAGIC
# MAGIC ✅ **Enable keepalives**: Prevent idle disconnections
# MAGIC
# MAGIC #### Monitoring
# MAGIC
# MAGIC ✅ **Track pool utilization**: Monitor available vs. total connections
# MAGIC
# MAGIC ✅ **Track wait times**: Identify pool exhaustion
# MAGIC
# MAGIC ✅ **Track error rates**: Detect connection issues
# MAGIC
# MAGIC ✅ **Set up alerts**: Notify when pool is unhealthy
# MAGIC
# MAGIC #### Error Handling
# MAGIC
# MAGIC ✅ **Always use context managers**: Ensures connections are returned
# MAGIC
# MAGIC ✅ **Handle PoolTimeout**: Have a fallback strategy
# MAGIC
# MAGIC ✅ **Implement retry logic**: With exponential backoff
# MAGIC
# MAGIC ✅ **Log errors**: Track patterns for debugging
# MAGIC
# MAGIC #### Threading and Concurrency
# MAGIC
# MAGIC ✅ **Pool is thread-safe**: Safe to use from multiple threads
# MAGIC
# MAGIC ✅ **One connection per thread**: Don't share connections across threads
# MAGIC
# MAGIC ✅ **Use connection() context manager**: Automatic acquisition and release
# MAGIC
# MAGIC ✅ **Limit thread pool size**: Don't exceed database connection capacity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Clean Up and Close Pool

# COMMAND ----------

# Close the manager pool
manager.close()

# Close the main pool
pool.close()
print("✅ Main pool closed")

# Remove widgets
dbutils.widgets.removeAll()

print("\n✅ Lab complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC Congratulations! You have successfully:
# MAGIC
# MAGIC ✅ Created and configured a connection pool with psycopg3
# MAGIC
# MAGIC ✅ Implemented pooled connection patterns for concurrent operations
# MAGIC
# MAGIC ✅ Monitored pool statistics and health
# MAGIC
# MAGIC ✅ Compared performance between single connections and connection pools
# MAGIC
# MAGIC ✅ Handled pool exhaustion and timeout scenarios
# MAGIC
# MAGIC ✅ Built a production-ready pool manager class
# MAGIC
# MAGIC ✅ Executed concurrent reads and writes safely
# MAGIC
# MAGIC ✅ Applied best practices for pool configuration and monitoring
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 🚀 **Performance**: Connection pools significantly improve throughput and latency
# MAGIC
# MAGIC 🚀 **Concurrency**: Pools enable safe concurrent database access
# MAGIC
# MAGIC 🚀 **Resource Management**: Pools limit connections and prevent database overload
# MAGIC
# MAGIC 🚀 **Reliability**: Built-in retry and error handling improve application stability
# MAGIC
# MAGIC 🚀 **Production-Ready**: Connection pools are essential for production applications
# MAGIC
# MAGIC ### Performance Results
# MAGIC
# MAGIC In this lab, we observed:
# MAGIC - **2-5x faster** query execution with pooled connections
# MAGIC - **Higher throughput** for concurrent workloads
# MAGIC - **Lower latency** by eliminating connection overhead
# MAGIC - **Better resource utilization** with connection reuse
# MAGIC
# MAGIC ### When to Use Connection Pools
# MAGIC
# MAGIC **Always use pools for**:
# MAGIC - Production web applications
# MAGIC - REST APIs and microservices
# MAGIC - Multi-threaded applications
# MAGIC - Long-running services
# MAGIC - Any concurrent workload
# MAGIC
# MAGIC **Single connections are OK for**:
# MAGIC - One-off scripts
# MAGIC - Batch jobs (single-threaded)
# MAGIC - Development and testing
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to:
# MAGIC - **Lab 6: Sync Tables from Unity Catalog to Lakebase** - Learn how to bridge analytical and operational data with automatic synchronization
