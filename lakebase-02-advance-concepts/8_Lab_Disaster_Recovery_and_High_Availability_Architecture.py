# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Disaster Recovery and High Availability Architecture
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Design multi-region replication strategies
# MAGIC - Implement automated failover mechanisms
# MAGIC - Configure backup and restore procedures
# MAGIC - Test disaster recovery scenarios
# MAGIC - Monitor system health and availability
# MAGIC - Implement data synchronization patterns
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of previous labs
# MAGIC - Understanding of distributed systems
# MAGIC - Access to multiple Databricks workspaces (optional)
# MAGIC
# MAGIC ## Time Estimate
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Availability Architecture Patterns
# MAGIC
# MAGIC ```
# MAGIC Active-Passive:
# MAGIC ┌─────────────┐         ┌─────────────┐
# MAGIC │  Primary    │ replicate│  Secondary  │
# MAGIC │  (Active)   │────────►│  (Standby)  │
# MAGIC └─────────────┘         └─────────────┘
# MAGIC
# MAGIC Active-Active:
# MAGIC ┌─────────────┐ ◄──sync──► ┌─────────────┐
# MAGIC │  Region A   │            │  Region B   │
# MAGIC │  (Active)   │            │  (Active)   │
# MAGIC └─────────────┘            └─────────────┘
# MAGIC ```

# COMMAND ----------

# Configuration
from pyspark.sql.functions import *
from datetime import datetime

current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_name = current_user.split("@")[0].replace(".", "_")
SCHEMA = f"ha_lab_{user_name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS main.{SCHEMA}")
spark.sql(f"USE SCHEMA main.{SCHEMA}")

print(f"✅ HA Lab environment: main.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Implement Backup Strategy

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create production table
# MAGIC CREATE OR REPLACE TABLE production_orders (
# MAGIC   order_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   order_total DECIMAL(12,2),
# MAGIC   order_date TIMESTAMP,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO production_orders VALUES
# MAGIC ('ORD-001', 'CUST-101', 1250.00, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC ('ORD-002', 'CUST-102', 750.50, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
# MAGIC ('ORD-003', 'CUST-103', 2100.00, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Backup with Delta Lake Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create backup snapshot
# MAGIC CREATE OR REPLACE TABLE backup_orders_snapshot AS
# MAGIC SELECT *, CURRENT_TIMESTAMP as backup_timestamp
# MAGIC FROM production_orders;
# MAGIC
# MAGIC SELECT * FROM backup_orders_snapshot;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Point-in-Time Recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query historical version
# MAGIC SELECT * FROM production_orders VERSION AS OF 0;
# MAGIC
# MAGIC -- Restore from backup
# MAGIC CREATE OR REPLACE TABLE production_orders AS
# MAGIC SELECT order_id, customer_id, order_total, order_date, created_at
# MAGIC FROM backup_orders_snapshot;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Replication

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-Region Replication Pattern

# COMMAND ----------

def replicate_to_secondary(source_table, target_table, replication_mode="full"):
    """
    Replicate data to secondary region
    """
    if replication_mode == "full":
        # Full replication
        source_df = spark.table(source_table)
        source_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        print(f"✅ Full replication completed: {source_table} → {target_table}")
    
    elif replication_mode == "incremental":
        # Incremental replication
        spark.sql(f"""
            MERGE INTO {target_table} target
            USING {source_table} source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✅ Incremental replication completed: {source_table} → {target_table}")

# Create secondary table
spark.sql("CREATE TABLE IF NOT EXISTS secondary_orders LIKE production_orders")

# Replicate
replicate_to_secondary("production_orders", "secondary_orders", "full")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Implement Health Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create health check log
# MAGIC CREATE TABLE IF NOT EXISTS system_health_log (
# MAGIC   check_id STRING,
# MAGIC   component_name STRING,
# MAGIC   status STRING,
# MAGIC   response_time_ms BIGINT,
# MAGIC   error_message STRING,
# MAGIC   timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

import time

def health_check(component_name, check_func):
    """
    Perform health check and log results
    """
    check_id = f"CHK-{int(time.time())}"
    start = time.time()
    
    try:
        check_func()
        response_time = int((time.time() - start) * 1000)
        status = "healthy"
        error_msg = "null"
    except Exception as e:
        response_time = int((time.time() - start) * 1000)
        status = "unhealthy"
        error_msg = str(e).replace("'", "''")
    
    # Log health check
    spark.sql(f"""
        INSERT INTO system_health_log VALUES (
            '{check_id}',
            '{component_name}',
            '{status}',
            {response_time},
            '{error_msg}',
            CURRENT_TIMESTAMP
        )
    """)
    
    return status == "healthy"

# Test health checks
health_check("production_orders", lambda: spark.table("production_orders").count())
health_check("secondary_orders", lambda: spark.table("secondary_orders").count())

print("✅ Health checks completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View health check results
# MAGIC SELECT 
# MAGIC   component_name,
# MAGIC   status,
# MAGIC   response_time_ms,
# MAGIC   timestamp
# MAGIC FROM system_health_log
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Automated Failover Logic

# COMMAND ----------

class FailoverManager:
    """
    Manages failover between primary and secondary systems
    """
    
    def __init__(self, primary_table, secondary_table):
        self.primary_table = primary_table
        self.secondary_table = secondary_table
        self.active_table = primary_table
    
    def check_primary_health(self):
        """Check if primary is healthy"""
        try:
            count = spark.table(self.primary_table).count()
            return True
        except:
            return False
    
    def failover_to_secondary(self):
        """Failover to secondary"""
        print(f"⚠️  PRIMARY UNHEALTHY - Initiating failover...")
        self.active_table = self.secondary_table
        print(f"✅ Failover complete. Active table: {self.active_table}")
    
    def failback_to_primary(self):
        """Failback to primary"""
        print(f"🔄 PRIMARY RECOVERED - Initiating failback...")
        
        # Sync secondary changes to primary
        spark.sql(f"""
            MERGE INTO {self.primary_table} target
            USING {self.secondary_table} source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        self.active_table = self.primary_table
        print(f"✅ Failback complete. Active table: {self.active_table}")
    
    def get_active_table(self):
        """Get currently active table"""
        # Check health and failover if needed
        if self.active_table == self.primary_table:
            if not self.check_primary_health():
                self.failover_to_secondary()
        
        return self.active_table

# Test failover manager
fm = FailoverManager("production_orders", "secondary_orders")
active = fm.get_active_table()
print(f"Active table: {active}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Disaster Recovery Scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1: Accidental Data Deletion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulate accidental deletion
# MAGIC DELETE FROM production_orders WHERE order_id = 'ORD-001';
# MAGIC
# MAGIC SELECT 'After deletion' as status, COUNT(*) as count FROM production_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore from previous version
# MAGIC RESTORE TABLE production_orders TO VERSION AS OF 1;
# MAGIC
# MAGIC SELECT 'After restore' as status, COUNT(*) as count FROM production_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2: Data Corruption

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulate data corruption
# MAGIC UPDATE production_orders SET order_total = -999.99 WHERE order_id = 'ORD-002';
# MAGIC
# MAGIC SELECT * FROM production_orders WHERE order_total < 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore clean data from backup
# MAGIC MERGE INTO production_orders target
# MAGIC USING backup_orders_snapshot source
# MAGIC ON target.order_id = source.order_id
# MAGIC WHEN MATCHED AND target.order_total < 0 THEN UPDATE SET *;
# MAGIC
# MAGIC SELECT * FROM production_orders WHERE order_id = 'ORD-002';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Implement Monitoring Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- System availability metrics
# MAGIC SELECT 
# MAGIC   component_name,
# MAGIC   COUNT(*) as total_checks,
# MAGIC   SUM(CASE WHEN status = 'healthy' THEN 1 ELSE 0 END) as healthy_checks,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN status = 'healthy' THEN 1 ELSE 0 END) / COUNT(*), 2) as availability_pct,
# MAGIC   AVG(response_time_ms) as avg_response_ms
# MAGIC FROM system_health_log
# MAGIC WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
# MAGIC GROUP BY component_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. ✅ Implemented backup and restore procedures
# MAGIC 2. ✅ Configured replication strategies
# MAGIC 3. ✅ Built automated failover mechanisms
# MAGIC 4. ✅ Tested disaster recovery scenarios
# MAGIC 5. ✅ Created health monitoring system
# MAGIC 6. ✅ Used Delta Lake time travel for recovery
# MAGIC
# MAGIC ## Next Steps
# MAGIC - **Lab 9**: Advanced Security Patterns
