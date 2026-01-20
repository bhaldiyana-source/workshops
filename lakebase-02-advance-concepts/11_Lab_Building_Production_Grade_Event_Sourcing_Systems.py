# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building Production-Grade Event Sourcing Systems
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Design and implement event store architecture
# MAGIC - Build event projections for different read models
# MAGIC - Implement CQRS pattern with command and query separation
# MAGIC - Handle event versioning and schema evolution
# MAGIC - Create snapshots for performance optimization
# MAGIC - Test eventual consistency patterns
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lecture 10
# MAGIC - Understanding of event-driven architecture
# MAGIC - Knowledge of eventual consistency concepts
# MAGIC
# MAGIC ## Time Estimate
# MAGIC 35-40 minutes

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import uuid

# Configuration
current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_name = current_user.split("@")[0].replace(".", "_")
SCHEMA = f"event_sourcing_lab_{user_name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS main.{SCHEMA}")
spark.sql(f"USE SCHEMA main.{SCHEMA}")

print(f"✅ Event Sourcing Lab: main.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Event Store
# MAGIC
# MAGIC The event store is the source of truth for all state changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event store table
# MAGIC CREATE TABLE IF NOT EXISTS event_store (
# MAGIC   event_id STRING PRIMARY KEY,
# MAGIC   aggregate_id STRING NOT NULL,
# MAGIC   aggregate_type STRING NOT NULL,
# MAGIC   event_type STRING NOT NULL,
# MAGIC   event_version INT DEFAULT 1,
# MAGIC   event_data STRING,
# MAGIC   event_metadata STRING,
# MAGIC   sequence_number BIGINT,
# MAGIC   event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   correlation_id STRING,
# MAGIC   causation_id STRING
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (aggregate_type);
# MAGIC
# MAGIC -- Create index for fast lookups
# MAGIC CREATE INDEX IF NOT EXISTS idx_aggregate 
# MAGIC ON event_store (aggregate_id, sequence_number);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Event Classes

# COMMAND ----------

class DomainEvent:
    """Base class for domain events"""
    
    def __init__(self, aggregate_id, event_data, correlation_id=None):
        self.event_id = f"EVT-{uuid.uuid4().hex[:16].upper()}"
        self.aggregate_id = aggregate_id
        self.event_data = event_data
        self.event_timestamp = datetime.now()
        self.correlation_id = correlation_id or self.event_id
    
    def to_dict(self):
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "event_data": json.dumps(self.event_data),
            "event_timestamp": self.event_timestamp,
            "correlation_id": self.correlation_id
        }

# Account Events
class AccountCreated(DomainEvent):
    aggregate_type = "Account"
    event_type = "AccountCreated"
    event_version = 1

class MoneyDeposited(DomainEvent):
    aggregate_type = "Account"
    event_type = "MoneyDeposited"
    event_version = 1

class MoneyWithdrawn(DomainEvent):
    aggregate_type = "Account"
    event_type = "MoneyWithdrawn"
    event_version = 1

class AccountClosed(DomainEvent):
    aggregate_type = "Account"
    event_type = "AccountClosed"
    event_version = 1

print("✅ Event classes defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Implement Event Store Operations

# COMMAND ----------

class EventStore:
    """Event store operations"""
    
    @staticmethod
    def append_event(event: DomainEvent):
        """Append event to store"""
        event_dict = event.to_dict()
        
        # Get next sequence number
        max_seq = spark.sql(f"""
            SELECT COALESCE(MAX(sequence_number), 0) as max_seq
            FROM event_store
            WHERE aggregate_id = '{event.aggregate_id}'
        """).collect()[0]['max_seq']
        
        sequence_number = max_seq + 1
        
        # Insert event
        spark.sql(f"""
            INSERT INTO event_store VALUES (
                '{event_dict['event_id']}',
                '{event_dict['aggregate_id']}',
                '{event_dict['aggregate_type']}',
                '{event_dict['event_type']}',
                {event_dict['event_version']},
                '{event_dict['event_data']}',
                NULL,
                {sequence_number},
                CURRENT_TIMESTAMP,
                '{event_dict['correlation_id']}',
                NULL
            )
        """)
        
        print(f"✅ Event appended: {event.event_type} (seq: {sequence_number})")
        return sequence_number
    
    @staticmethod
    def get_events(aggregate_id, from_sequence=0):
        """Get events for an aggregate"""
        events = spark.sql(f"""
            SELECT *
            FROM event_store
            WHERE aggregate_id = '{aggregate_id}'
              AND sequence_number > {from_sequence}
            ORDER BY sequence_number
        """).collect()
        
        return events

print("✅ Event store operations ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Implement Account Aggregate

# COMMAND ----------

class Account:
    """Account aggregate root"""
    
    def __init__(self, account_id):
        self.account_id = account_id
        self.balance = 0
        self.status = "pending"
        self.version = 0
        self.uncommitted_events = []
    
    def create_account(self, initial_balance):
        """Create new account"""
        if self.status != "pending":
            raise ValueError("Account already created")
        
        event = AccountCreated(
            self.account_id,
            {"initial_balance": initial_balance}
        )
        self.apply_event(event)
        self.uncommitted_events.append(event)
    
    def deposit(self, amount):
        """Deposit money"""
        if amount <= 0:
            raise ValueError("Amount must be positive")
        if self.status != "active":
            raise ValueError("Account not active")
        
        event = MoneyDeposited(
            self.account_id,
            {"amount": amount}
        )
        self.apply_event(event)
        self.uncommitted_events.append(event)
    
    def withdraw(self, amount):
        """Withdraw money"""
        if amount <= 0:
            raise ValueError("Amount must be positive")
        if self.status != "active":
            raise ValueError("Account not active")
        if self.balance < amount:
            raise ValueError("Insufficient funds")
        
        event = MoneyWithdrawn(
            self.account_id,
            {"amount": amount}
        )
        self.apply_event(event)
        self.uncommitted_events.append(event)
    
    def close_account(self):
        """Close account"""
        if self.balance != 0:
            raise ValueError("Cannot close account with balance")
        
        event = AccountClosed(self.account_id, {})
        self.apply_event(event)
        self.uncommitted_events.append(event)
    
    def apply_event(self, event):
        """Apply event to update state"""
        if isinstance(event, AccountCreated):
            self.balance = event.event_data["initial_balance"]
            self.status = "active"
        elif isinstance(event, MoneyDeposited):
            self.balance += event.event_data["amount"]
        elif isinstance(event, MoneyWithdrawn):
            self.balance -= event.event_data["amount"]
        elif isinstance(event, AccountClosed):
            self.status = "closed"
        
        self.version += 1
    
    @staticmethod
    def load_from_history(account_id):
        """Reconstruct account from event history"""
        account = Account(account_id)
        events = EventStore.get_events(account_id)
        
        for event_row in events:
            # Reconstruct event object
            event_data = json.loads(event_row['event_data'])
            event_type = event_row['event_type']
            
            if event_type == "AccountCreated":
                event = AccountCreated(account_id, event_data)
            elif event_type == "MoneyDeposited":
                event = MoneyDeposited(account_id, event_data)
            elif event_type == "MoneyWithdrawn":
                event = MoneyWithdrawn(account_id, event_data)
            elif event_type == "AccountClosed":
                event = AccountClosed(account_id, event_data)
            
            account.apply_event(event)
        
        account.uncommitted_events = []
        return account
    
    def save(self):
        """Save uncommitted events"""
        for event in self.uncommitted_events:
            EventStore.append_event(event)
        self.uncommitted_events = []

print("✅ Account aggregate implemented")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Test Event Sourcing

# COMMAND ----------

# Create new account
account_id = f"ACC-{uuid.uuid4().hex[:8].upper()}"
account = Account(account_id)

print(f"Creating account {account_id}...")
account.create_account(initial_balance=1000.0)
account.save()

print(f"Current balance: ${account.balance}")

# COMMAND ----------

# Deposit money
print("\nDepositing $500...")
account.deposit(500.0)
account.save()
print(f"Current balance: ${account.balance}")

# COMMAND ----------

# Withdraw money
print("\nWithdrawing $200...")
account.withdraw(200.0)
account.save()
print(f"Current balance: ${account.balance}")

# COMMAND ----------

# Reload account from events
print("\n🔄 Reloading account from event history...")
reloaded_account = Account.load_from_history(account_id)
print(f"Reloaded balance: ${reloaded_account.balance}")
print(f"Account status: {reloaded_account.status}")
print(f"Version: {reloaded_account.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: View Event Stream

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   sequence_number,
# MAGIC   event_type,
# MAGIC   event_data,
# MAGIC   event_timestamp
# MAGIC FROM event_store
# MAGIC ORDER BY aggregate_id, sequence_number;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Read Model Projections

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Account balance projection (current state)
# MAGIC CREATE OR REPLACE TABLE account_balance_view AS
# MAGIC SELECT 
# MAGIC   aggregate_id as account_id,
# MAGIC   MAX(sequence_number) as last_event_sequence,
# MAGIC   CURRENT_TIMESTAMP as updated_at
# MAGIC FROM event_store
# MAGIC WHERE aggregate_type = 'Account'
# MAGIC GROUP BY aggregate_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Projection from Events

# COMMAND ----------

def rebuild_account_projections():
    """Rebuild all account projections from events"""
    
    accounts = spark.sql("""
        SELECT DISTINCT aggregate_id
        FROM event_store
        WHERE aggregate_type = 'Account'
    """).collect()
    
    projections = []
    
    for row in accounts:
        account_id = row['aggregate_id']
        account = Account.load_from_history(account_id)
        
        projections.append({
            "account_id": account_id,
            "balance": account.balance,
            "status": account.status,
            "version": account.version,
            "updated_at": datetime.now()
        })
    
    # Save projections
    projection_df = spark.createDataFrame(projections)
    projection_df.write.format("delta").mode("overwrite").saveAsTable("account_projections")
    
    print(f"✅ Rebuilt {len(projections)} account projections")

rebuild_account_projections()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query projection
# MAGIC SELECT * FROM account_projections;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Implement Snapshots for Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Snapshot table
# MAGIC CREATE TABLE IF NOT EXISTS account_snapshots (
# MAGIC   account_id STRING,
# MAGIC   snapshot_data STRING,
# MAGIC   snapshot_version BIGINT,
# MAGIC   snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

def create_snapshot(account: Account, version_threshold=10):
    """Create snapshot if version exceeds threshold"""
    
    if account.version % version_threshold == 0:
        snapshot_data = {
            "balance": account.balance,
            "status": account.status,
            "version": account.version
        }
        
        spark.sql(f"""
            INSERT INTO account_snapshots VALUES (
                '{account.account_id}',
                '{json.dumps(snapshot_data)}',
                {account.version},
                CURRENT_TIMESTAMP
            )
        """)
        
        print(f"📸 Snapshot created for {account.account_id} at version {account.version}")

# Create snapshot
create_snapshot(reloaded_account, version_threshold=3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Implement CQRS Pattern

# COMMAND ----------

class CommandHandler:
    """Handle commands (writes)"""
    
    @staticmethod
    def handle_create_account(account_id, initial_balance):
        account = Account(account_id)
        account.create_account(initial_balance)
        account.save()
        return {"success": True, "account_id": account_id}
    
    @staticmethod
    def handle_deposit(account_id, amount):
        account = Account.load_from_history(account_id)
        account.deposit(amount)
        account.save()
        rebuild_account_projections()  # Update read model
        return {"success": True, "new_balance": account.balance}

class QueryHandler:
    """Handle queries (reads)"""
    
    @staticmethod
    def get_account_balance(account_id):
        result = spark.sql(f"""
            SELECT * FROM account_projections
            WHERE account_id = '{account_id}'
        """).collect()
        
        if result:
            return {
                "account_id": account_id,
                "balance": float(result[0]['balance']),
                "status": result[0]['status']
            }
        return None
    
    @staticmethod
    def get_account_history(account_id):
        events = spark.sql(f"""
            SELECT 
                event_type,
                event_data,
                event_timestamp,
                sequence_number
            FROM event_store
            WHERE aggregate_id = '{account_id}'
            ORDER BY sequence_number
        """).collect()
        
        return [dict(row.asDict()) for row in events]

print("✅ CQRS handlers implemented")

# COMMAND ----------

# Test CQRS
new_account_id = f"ACC-{uuid.uuid4().hex[:8].upper()}"

# Command: Create account
result = CommandHandler.handle_create_account(new_account_id, 5000.0)
print(f"Command result: {result}")

# Query: Get balance
balance = QueryHandler.get_account_balance(new_account_id)
print(f"Query result: {balance}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Event Versioning and Schema Evolution

# COMMAND ----------

# New version of MoneyDeposited event with additional metadata
class MoneyDepositedV2(DomainEvent):
    aggregate_type = "Account"
    event_type = "MoneyDeposited"
    event_version = 2  # Version 2
    
    def __init__(self, aggregate_id, event_data, correlation_id=None):
        # V2 includes deposit_source field
        if "deposit_source" not in event_data:
            event_data["deposit_source"] = "unknown"
        super().__init__(aggregate_id, event_data, correlation_id)

print("✅ Event versioning example ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. ✅ **Event store** is the source of truth for all state changes
# MAGIC 2. ✅ **Aggregates** encapsulate business logic and generate events
# MAGIC 3. ✅ **Projections** provide optimized read models
# MAGIC 4. ✅ **Snapshots** improve performance for long event streams
# MAGIC 5. ✅ **CQRS** separates command and query responsibilities
# MAGIC 6. ✅ **Event versioning** handles schema evolution
# MAGIC 7. ✅ **Eventual consistency** is acceptable for read models
# MAGIC
# MAGIC ## Best Practices
# MAGIC
# MAGIC - ✅ Events are immutable - never modify past events
# MAGIC - ✅ Use sequence numbers for ordering guarantees
# MAGIC - ✅ Create snapshots periodically for performance
# MAGIC - ✅ Rebuild projections from events for data recovery
# MAGIC - ✅ Version events to handle schema changes
# MAGIC - ✅ Monitor event store size and archive old events
# MAGIC - ✅ Test event replay scenarios
# MAGIC
# MAGIC ## Workshop Complete!
# MAGIC
# MAGIC Congratulations! You've completed the Advanced Lakehouse with PostgreSQL workshop. You've learned:
# MAGIC - Federation architecture and optimization
# MAGIC - CDC and real-time sync patterns
# MAGIC - Transaction management with sagas
# MAGIC - Query performance optimization
# MAGIC - Disaster recovery strategies
# MAGIC - Advanced security patterns
# MAGIC - Event sourcing and CQRS
