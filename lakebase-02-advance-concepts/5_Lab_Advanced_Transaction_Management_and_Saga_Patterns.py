# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Advanced Transaction Management and Saga Patterns
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Understand distributed transaction challenges across PostgreSQL and Delta Lake
# MAGIC - Implement the Saga pattern for long-running transactions
# MAGIC - Design compensation logic for transaction rollback
# MAGIC - Handle idempotent operations to prevent duplicate processing
# MAGIC - Implement retry mechanisms with exponential backoff
# MAGIC - Manage eventual consistency in distributed systems
# MAGIC - Build transaction orchestration patterns
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of previous labs
# MAGIC - Understanding of ACID properties
# MAGIC - Familiarity with distributed systems concepts
# MAGIC - Knowledge of PostgreSQL transactions
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Understand distributed transaction challenges
# MAGIC 2. Implement Saga pattern for order processing
# MAGIC 3. Build compensation logic for failures
# MAGIC 4. Create idempotent operations
# MAGIC 5. Implement retry and circuit breaker patterns
# MAGIC
# MAGIC ### Time Estimate
# MAGIC 25-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Distributed Transaction Challenge
# MAGIC
# MAGIC ### Traditional Two-Phase Commit (2PC) Limitations
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐         ┌──────────────┐
# MAGIC │ PostgreSQL   │         │  Delta Lake  │
# MAGIC │   (OLTP)     │         │   (OLAP)     │
# MAGIC └──────┬───────┘         └──────┬───────┘
# MAGIC        │                        │
# MAGIC        │   2PC not practical    │
# MAGIC        │   across systems       │
# MAGIC        └────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Why 2PC doesn't work well:**
# MAGIC - Long locks reduce throughput
# MAGIC - Coordinator failure blocks all participants
# MAGIC - Not suitable for long-running operations
# MAGIC - Doesn't scale across cloud boundaries
# MAGIC
# MAGIC ### Enter the Saga Pattern
# MAGIC
# MAGIC A saga is a sequence of local transactions where each transaction updates a single service and publishes events to trigger the next transaction.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Environment Setup

# COMMAND ----------

# Import required libraries
import psycopg
from psycopg import sql
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import time
import json
import uuid
from enum import Enum

# Configuration
current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_name = current_user.split("@")[0].replace(".", "_")

CATALOG = "main"
SCHEMA = f"saga_lab_{user_name}"

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✅ Environment configured: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Transaction State Machine
# MAGIC
# MAGIC A saga transaction progresses through states:

# COMMAND ----------

class SagaState(Enum):
    """Saga transaction states"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPENSATING = "COMPENSATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"

class StepState(Enum):
    """Individual step states"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"

print("✅ State machines defined")
print(f"   Saga States: {[s.value for s in SagaState]}")
print(f"   Step States: {[s.value for s in StepState]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Saga Orchestration Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Saga instances table
# MAGIC CREATE TABLE IF NOT EXISTS saga_instances (
# MAGIC   saga_id STRING PRIMARY KEY,
# MAGIC   saga_type STRING NOT NULL,
# MAGIC   saga_state STRING NOT NULL,
# MAGIC   payload STRING,
# MAGIC   current_step INT DEFAULT 0,
# MAGIC   total_steps INT NOT NULL,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   completed_at TIMESTAMP,
# MAGIC   error_message STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Saga steps table
# MAGIC CREATE TABLE IF NOT EXISTS saga_steps (
# MAGIC   step_id STRING PRIMARY KEY,
# MAGIC   saga_id STRING NOT NULL,
# MAGIC   step_number INT NOT NULL,
# MAGIC   step_name STRING NOT NULL,
# MAGIC   step_state STRING NOT NULL,
# MAGIC   step_data STRING,
# MAGIC   compensation_data STRING,
# MAGIC   started_at TIMESTAMP,
# MAGIC   completed_at TIMESTAMP,
# MAGIC   error_message STRING,
# MAGIC   retry_count INT DEFAULT 0,
# MAGIC   max_retries INT DEFAULT 3
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Saga events table (audit log)
# MAGIC CREATE TABLE IF NOT EXISTS saga_events (
# MAGIC   event_id STRING PRIMARY KEY,
# MAGIC   saga_id STRING NOT NULL,
# MAGIC   event_type STRING NOT NULL,
# MAGIC   event_data STRING,
# MAGIC   timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Example Business Scenario
# MAGIC
# MAGIC ### Order Processing Saga
# MAGIC
# MAGIC ```
# MAGIC Order Process Flow:
# MAGIC 1. Reserve Inventory      → Compensation: Release Inventory
# MAGIC 2. Process Payment        → Compensation: Refund Payment
# MAGIC 3. Create Shipping Label  → Compensation: Cancel Shipping
# MAGIC 4. Update Order Status    → Compensation: Revert Status
# MAGIC 5. Send Confirmation      → Compensation: Send Cancellation
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Business Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders table
# MAGIC CREATE TABLE IF NOT EXISTS orders (
# MAGIC   order_id STRING PRIMARY KEY,
# MAGIC   customer_id STRING NOT NULL,
# MAGIC   product_id STRING NOT NULL,
# MAGIC   quantity INT NOT NULL,
# MAGIC   total_amount DECIMAL(12,2) NOT NULL,
# MAGIC   status STRING NOT NULL,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Inventory table
# MAGIC CREATE TABLE IF NOT EXISTS inventory (
# MAGIC   product_id STRING PRIMARY KEY,
# MAGIC   product_name STRING NOT NULL,
# MAGIC   available_quantity INT NOT NULL,
# MAGIC   reserved_quantity INT DEFAULT 0,
# MAGIC   unit_price DECIMAL(10,2) NOT NULL,
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Payments table
# MAGIC CREATE TABLE IF NOT EXISTS payments (
# MAGIC   payment_id STRING PRIMARY KEY,
# MAGIC   order_id STRING NOT NULL,
# MAGIC   amount DECIMAL(12,2) NOT NULL,
# MAGIC   status STRING NOT NULL,
# MAGIC   payment_method STRING,
# MAGIC   transaction_ref STRING,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Shipments table
# MAGIC CREATE TABLE IF NOT EXISTS shipments (
# MAGIC   shipment_id STRING PRIMARY KEY,
# MAGIC   order_id STRING NOT NULL,
# MAGIC   tracking_number STRING,
# MAGIC   carrier STRING,
# MAGIC   status STRING NOT NULL,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Sample Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample inventory
# MAGIC INSERT INTO inventory VALUES
# MAGIC ('PROD-001', 'Laptop Pro', 50, 0, 1299.99, CURRENT_TIMESTAMP),
# MAGIC ('PROD-002', 'Wireless Mouse', 200, 0, 29.99, CURRENT_TIMESTAMP),
# MAGIC ('PROD-003', 'USB-C Cable', 500, 0, 12.99, CURRENT_TIMESTAMP),
# MAGIC ('PROD-004', 'Monitor 27"', 30, 0, 399.99, CURRENT_TIMESTAMP);
# MAGIC
# MAGIC SELECT 'Sample data created' as status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Implement Saga Step Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Reserve Inventory

# COMMAND ----------

def reserve_inventory(saga_id: str, product_id: str, quantity: int) -> dict:
    """
    Reserve inventory for an order
    Returns: dict with success status and reservation data
    """
    try:
        # Check available inventory
        result = spark.sql(f"""
            SELECT 
                product_id,
                available_quantity,
                reserved_quantity,
                unit_price
            FROM inventory
            WHERE product_id = '{product_id}'
        """).collect()
        
        if not result:
            return {
                "success": False,
                "error": f"Product {product_id} not found"
            }
        
        row = result[0]
        available = row['available_quantity']
        
        if available < quantity:
            return {
                "success": False,
                "error": f"Insufficient inventory. Available: {available}, Requested: {quantity}"
            }
        
        # Reserve inventory
        spark.sql(f"""
            UPDATE inventory
            SET 
                available_quantity = available_quantity - {quantity},
                reserved_quantity = reserved_quantity + {quantity},
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id = '{product_id}'
        """)
        
        return {
            "success": True,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": float(row['unit_price']),
            "saga_id": saga_id
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Compensation function
def release_inventory(compensation_data: dict) -> dict:
    """
    Release reserved inventory (compensation)
    """
    try:
        product_id = compensation_data['product_id']
        quantity = compensation_data['quantity']
        
        spark.sql(f"""
            UPDATE inventory
            SET 
                available_quantity = available_quantity + {quantity},
                reserved_quantity = reserved_quantity - {quantity},
                updated_at = CURRENT_TIMESTAMP
            WHERE product_id = '{product_id}'
        """)
        
        return {"success": True, "compensated": "inventory_released"}
    except Exception as e:
        return {"success": False, "error": str(e)}

print("✅ Inventory step functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Process Payment

# COMMAND ----------

def process_payment(saga_id: str, order_id: str, amount: float) -> dict:
    """
    Process payment for an order
    """
    try:
        payment_id = f"PAY-{uuid.uuid4().hex[:12].upper()}"
        transaction_ref = f"TXN-{uuid.uuid4().hex[:16].upper()}"
        
        # Simulate payment processing
        # In real system, this would call a payment gateway
        time.sleep(0.1)  # Simulate API call
        
        # Insert payment record
        spark.sql(f"""
            INSERT INTO payments VALUES (
                '{payment_id}',
                '{order_id}',
                {amount},
                'completed',
                'credit_card',
                '{transaction_ref}',
                CURRENT_TIMESTAMP
            )
        """)
        
        return {
            "success": True,
            "payment_id": payment_id,
            "transaction_ref": transaction_ref,
            "amount": amount
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Compensation function
def refund_payment(compensation_data: dict) -> dict:
    """
    Refund payment (compensation)
    """
    try:
        payment_id = compensation_data['payment_id']
        
        spark.sql(f"""
            UPDATE payments
            SET status = 'refunded'
            WHERE payment_id = '{payment_id}'
        """)
        
        return {"success": True, "compensated": "payment_refunded"}
    except Exception as e:
        return {"success": False, "error": str(e)}

print("✅ Payment step functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Shipment

# COMMAND ----------

def create_shipment(saga_id: str, order_id: str) -> dict:
    """
    Create shipment and generate tracking number
    """
    try:
        shipment_id = f"SHIP-{uuid.uuid4().hex[:12].upper()}"
        tracking_number = f"TRK-{uuid.uuid4().hex[:16].upper()}"
        
        # Insert shipment record
        spark.sql(f"""
            INSERT INTO shipments VALUES (
                '{shipment_id}',
                '{order_id}',
                '{tracking_number}',
                'FedEx',
                'label_created',
                CURRENT_TIMESTAMP
            )
        """)
        
        return {
            "success": True,
            "shipment_id": shipment_id,
            "tracking_number": tracking_number
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Compensation function
def cancel_shipment(compensation_data: dict) -> dict:
    """
    Cancel shipment (compensation)
    """
    try:
        shipment_id = compensation_data['shipment_id']
        
        spark.sql(f"""
            UPDATE shipments
            SET status = 'cancelled'
            WHERE shipment_id = '{shipment_id}'
        """)
        
        return {"success": True, "compensated": "shipment_cancelled"}
    except Exception as e:
        return {"success": False, "error": str(e)}

print("✅ Shipment step functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Update Order Status

# COMMAND ----------

def update_order_status(order_id: str, status: str) -> dict:
    """
    Update order status
    """
    try:
        spark.sql(f"""
            UPDATE orders
            SET 
                status = '{status}',
                updated_at = CURRENT_TIMESTAMP
            WHERE order_id = '{order_id}'
        """)
        
        return {
            "success": True,
            "order_id": order_id,
            "status": status
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Compensation function
def revert_order_status(compensation_data: dict) -> dict:
    """
    Revert order status (compensation)
    """
    try:
        order_id = compensation_data['order_id']
        
        spark.sql(f"""
            UPDATE orders
            SET 
                status = 'cancelled',
                updated_at = CURRENT_TIMESTAMP
            WHERE order_id = '{order_id}'
        """)
        
        return {"success": True, "compensated": "order_reverted"}
    except Exception as e:
        return {"success": False, "error": str(e)}

print("✅ Order status functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Implement Saga Orchestrator

# COMMAND ----------

class SagaOrchestrator:
    """
    Orchestrates saga execution with compensation
    """
    
    def __init__(self, saga_type: str, payload: dict):
        self.saga_id = f"SAGA-{uuid.uuid4().hex[:12].upper()}"
        self.saga_type = saga_type
        self.payload = payload
        self.steps = []
        self.compensation_data = []
        
    def add_step(self, name: str, action_fn, compensation_fn):
        """Add a step to the saga"""
        self.steps.append({
            "name": name,
            "action": action_fn,
            "compensation": compensation_fn
        })
    
    def log_event(self, event_type: str, event_data: dict):
        """Log saga event"""
        event_id = f"EVT-{uuid.uuid4().hex[:12].upper()}"
        spark.sql(f"""
            INSERT INTO saga_events VALUES (
                '{event_id}',
                '{self.saga_id}',
                '{event_type}',
                '{json.dumps(event_data)}',
                CURRENT_TIMESTAMP
            )
        """)
    
    def create_saga_instance(self):
        """Create saga instance record"""
        spark.sql(f"""
            INSERT INTO saga_instances VALUES (
                '{self.saga_id}',
                '{self.saga_type}',
                '{SagaState.PENDING.value}',
                '{json.dumps(self.payload)}',
                0,
                {len(self.steps)},
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                NULL,
                NULL
            )
        """)
        self.log_event("SAGA_CREATED", {"saga_id": self.saga_id})
    
    def update_saga_state(self, state: SagaState, error_message: str = None):
        """Update saga state"""
        error_clause = f", error_message = '{error_message}'" if error_message else ""
        completed_clause = ", completed_at = CURRENT_TIMESTAMP" if state in [SagaState.COMPLETED, SagaState.COMPENSATED] else ""
        
        spark.sql(f"""
            UPDATE saga_instances
            SET 
                saga_state = '{state.value}',
                updated_at = CURRENT_TIMESTAMP
                {error_clause}
                {completed_clause}
            WHERE saga_id = '{self.saga_id}'
        """)
    
    def execute(self) -> dict:
        """
        Execute saga with automatic compensation on failure
        """
        self.create_saga_instance()
        self.update_saga_state(SagaState.RUNNING)
        
        step_number = 0
        
        try:
            # Execute each step
            for step in self.steps:
                step_number += 1
                step_id = f"STEP-{uuid.uuid4().hex[:12].upper()}"
                
                print(f"📍 Executing step {step_number}/{len(self.steps)}: {step['name']}")
                
                # Create step record
                spark.sql(f"""
                    INSERT INTO saga_steps VALUES (
                        '{step_id}',
                        '{self.saga_id}',
                        {step_number},
                        '{step['name']}',
                        '{StepState.RUNNING.value}',
                        NULL,
                        NULL,
                        CURRENT_TIMESTAMP,
                        NULL,
                        NULL,
                        0,
                        3
                    )
                """)
                
                # Execute step action
                result = step['action']()
                
                if not result.get('success', False):
                    # Step failed - trigger compensation
                    error_msg = result.get('error', 'Unknown error')
                    print(f"❌ Step {step['name']} failed: {error_msg}")
                    
                    spark.sql(f"""
                        UPDATE saga_steps
                        SET 
                            step_state = '{StepState.FAILED.value}',
                            error_message = '{error_msg}',
                            completed_at = CURRENT_TIMESTAMP
                        WHERE step_id = '{step_id}'
                    """)
                    
                    self.log_event("STEP_FAILED", {
                        "step": step['name'],
                        "error": error_msg
                    })
                    
                    # Trigger compensation
                    self.compensate()
                    
                    return {
                        "success": False,
                        "saga_id": self.saga_id,
                        "failed_step": step['name'],
                        "error": error_msg,
                        "compensated": True
                    }
                
                # Step succeeded
                print(f"✅ Step {step['name']} completed successfully")
                
                spark.sql(f"""
                    UPDATE saga_steps
                    SET 
                        step_state = '{StepState.COMPLETED.value}',
                        step_data = '{json.dumps(result)}',
                        completed_at = CURRENT_TIMESTAMP
                    WHERE step_id = '{step_id}'
                """)
                
                # Store compensation data
                self.compensation_data.append({
                    "step": step['name'],
                    "compensation_fn": step['compensation'],
                    "data": result
                })
                
                # Update saga progress
                spark.sql(f"""
                    UPDATE saga_instances
                    SET 
                        current_step = {step_number},
                        updated_at = CURRENT_TIMESTAMP
                    WHERE saga_id = '{self.saga_id}'
                """)
                
                self.log_event("STEP_COMPLETED", {
                    "step": step['name'],
                    "result": result
                })
            
            # All steps completed successfully
            self.update_saga_state(SagaState.COMPLETED)
            self.log_event("SAGA_COMPLETED", {"saga_id": self.saga_id})
            
            print(f"\n🎉 Saga {self.saga_id} completed successfully!")
            
            return {
                "success": True,
                "saga_id": self.saga_id,
                "message": "All steps completed successfully"
            }
            
        except Exception as e:
            print(f"❌ Saga execution error: {str(e)}")
            self.update_saga_state(SagaState.FAILED, str(e))
            self.compensate()
            
            return {
                "success": False,
                "saga_id": self.saga_id,
                "error": str(e),
                "compensated": True
            }
    
    def compensate(self):
        """
        Execute compensation for all completed steps in reverse order
        """
        print(f"\n🔄 Starting compensation for saga {self.saga_id}")
        self.update_saga_state(SagaState.COMPENSATING)
        
        # Compensate in reverse order
        for comp_data in reversed(self.compensation_data):
            step_name = comp_data['step']
            compensation_fn = comp_data['compensation_fn']
            data = comp_data['data']
            
            print(f"   Compensating: {step_name}")
            
            try:
                result = compensation_fn(data)
                
                if result.get('success', False):
                    print(f"   ✅ {step_name} compensated")
                    
                    spark.sql(f"""
                        UPDATE saga_steps
                        SET 
                            step_state = '{StepState.COMPENSATED.value}',
                            compensation_data = '{json.dumps(result)}'
                        WHERE saga_id = '{self.saga_id}' 
                        AND step_name = '{step_name}'
                    """)
                else:
                    print(f"   ⚠️ {step_name} compensation failed")
                    
            except Exception as e:
                print(f"   ❌ Error compensating {step_name}: {str(e)}")
        
        self.update_saga_state(SagaState.COMPENSATED)
        self.log_event("SAGA_COMPENSATED", {"saga_id": self.saga_id})
        print(f"✅ Compensation completed for saga {self.saga_id}\n")

print("✅ Saga Orchestrator implemented")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Execute Order Processing Saga

# COMMAND ----------

# MAGIC %md
# MAGIC ### Successful Order Saga

# COMMAND ----------

def execute_order_saga_success():
    """
    Execute a successful order processing saga
    """
    # Order details
    order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"
    customer_id = "CUST-001"
    product_id = "PROD-002"  # Wireless Mouse
    quantity = 5
    
    # Get product price
    product = spark.sql(f"SELECT unit_price FROM inventory WHERE product_id = '{product_id}'").collect()[0]
    total_amount = float(product['unit_price']) * quantity
    
    # Create order record
    spark.sql(f"""
        INSERT INTO orders VALUES (
            '{order_id}',
            '{customer_id}',
            '{product_id}',
            {quantity},
            {total_amount},
            'pending',
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        )
    """)
    
    # Create and configure saga
    saga = SagaOrchestrator("ORDER_PROCESSING", {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity
    })
    
    # Add steps
    saga.add_step(
        "reserve_inventory",
        lambda: reserve_inventory(saga.saga_id, product_id, quantity),
        release_inventory
    )
    
    saga.add_step(
        "process_payment",
        lambda: process_payment(saga.saga_id, order_id, total_amount),
        refund_payment
    )
    
    saga.add_step(
        "create_shipment",
        lambda: create_shipment(saga.saga_id, order_id),
        cancel_shipment
    )
    
    saga.add_step(
        "update_order_status",
        lambda: update_order_status(order_id, 'confirmed'),
        revert_order_status
    )
    
    # Execute saga
    result = saga.execute()
    
    return result

# Execute successful saga
print("=" * 60)
print("Executing SUCCESSFUL Order Saga")
print("=" * 60)
result = execute_order_saga_success()
print(f"\nResult: {json.dumps(result, indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Successful Saga Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check saga instance
# MAGIC SELECT * FROM saga_instances ORDER BY created_at DESC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check saga steps
# MAGIC SELECT 
# MAGIC   step_number,
# MAGIC   step_name,
# MAGIC   step_state,
# MAGIC   started_at,
# MAGIC   completed_at,
# MAGIC   error_message
# MAGIC FROM saga_steps
# MAGIC WHERE saga_id = (SELECT saga_id FROM saga_instances ORDER BY created_at DESC LIMIT 1)
# MAGIC ORDER BY step_number;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check inventory changes
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   available_quantity,
# MAGIC   reserved_quantity
# MAGIC FROM inventory
# MAGIC WHERE product_id = 'PROD-002';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failed Order Saga (with Compensation)

# COMMAND ----------

def execute_order_saga_failure():
    """
    Execute an order saga that will fail at payment step
    This demonstrates the compensation mechanism
    """
    order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"
    customer_id = "CUST-002"
    product_id = "PROD-003"  # USB-C Cable
    quantity = 10
    
    product = spark.sql(f"SELECT unit_price FROM inventory WHERE product_id = '{product_id}'").collect()[0]
    total_amount = float(product['unit_price']) * quantity
    
    # Create order
    spark.sql(f"""
        INSERT INTO orders VALUES (
            '{order_id}',
            '{customer_id}',
            '{product_id}',
            {quantity},
            {total_amount},
            'pending',
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        )
    """)
    
    # Create saga
    saga = SagaOrchestrator("ORDER_PROCESSING", {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity
    })
    
    # Add steps with simulated failure
    saga.add_step(
        "reserve_inventory",
        lambda: reserve_inventory(saga.saga_id, product_id, quantity),
        release_inventory
    )
    
    # This step will fail
    saga.add_step(
        "process_payment",
        lambda: {"success": False, "error": "Payment gateway timeout"},  # Simulated failure
        refund_payment
    )
    
    saga.add_step(
        "create_shipment",
        lambda: create_shipment(saga.saga_id, order_id),
        cancel_shipment
    )
    
    saga.add_step(
        "update_order_status",
        lambda: update_order_status(order_id, 'confirmed'),
        revert_order_status
    )
    
    # Execute saga (will fail and compensate)
    result = saga.execute()
    
    return result

# Execute failed saga with compensation
print("\n" + "=" * 60)
print("Executing FAILED Order Saga (with Compensation)")
print("=" * 60)
result = execute_order_saga_failure()
print(f"\nResult: {json.dumps(result, indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Compensation Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check failed saga
# MAGIC SELECT 
# MAGIC   saga_id,
# MAGIC   saga_type,
# MAGIC   saga_state,
# MAGIC   current_step,
# MAGIC   total_steps,
# MAGIC   error_message
# MAGIC FROM saga_instances 
# MAGIC WHERE saga_state IN ('FAILED', 'COMPENSATED')
# MAGIC ORDER BY created_at DESC 
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check compensated steps
# MAGIC SELECT 
# MAGIC   step_number,
# MAGIC   step_name,
# MAGIC   step_state,
# MAGIC   error_message
# MAGIC FROM saga_steps
# MAGIC WHERE saga_id = (
# MAGIC   SELECT saga_id FROM saga_instances 
# MAGIC   WHERE saga_state = 'COMPENSATED' 
# MAGIC   ORDER BY created_at DESC LIMIT 1
# MAGIC )
# MAGIC ORDER BY step_number;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify inventory was released (compensation worked)
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   available_quantity,
# MAGIC   reserved_quantity
# MAGIC FROM inventory
# MAGIC WHERE product_id = 'PROD-003';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Implement Retry with Exponential Backoff

# COMMAND ----------

import time
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=10.0):
    """
    Decorator for retry logic with exponential backoff
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    
                    # If result indicates failure, retry
                    if isinstance(result, dict) and not result.get('success', True):
                        if attempt < max_retries - 1:
                            print(f"   Attempt {attempt + 1} failed, retrying in {delay:.1f}s...")
                            time.sleep(delay)
                            delay = min(delay * 2, max_delay)  # Exponential backoff
                            continue
                    
                    return result
                    
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        print(f"   Attempt {attempt + 1} failed: {str(e)}")
                        print(f"   Retrying in {delay:.1f}s...")
                        time.sleep(delay)
                        delay = min(delay * 2, max_delay)
                    else:
                        raise
            
            # All retries exhausted
            if last_exception:
                raise last_exception
            
            return result
        
        return wrapper
    return decorator

# Example usage
@retry_with_backoff(max_retries=3, base_delay=0.5)
def unreliable_operation():
    """Simulated unreliable operation"""
    import random
    if random.random() < 0.7:  # 70% failure rate
        return {"success": False, "error": "Transient failure"}
    return {"success": True, "data": "operation completed"}

# Test retry mechanism
print("Testing retry with exponential backoff:")
try:
    result = unreliable_operation()
    print(f"✅ Operation succeeded: {result}")
except Exception as e:
    print(f"❌ Operation failed after all retries: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Implement Idempotency

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Idempotency tracking table
# MAGIC CREATE TABLE IF NOT EXISTS idempotency_keys (
# MAGIC   idempotency_key STRING PRIMARY KEY,
# MAGIC   operation_type STRING NOT NULL,
# MAGIC   status STRING NOT NULL,
# MAGIC   response_data STRING,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC   completed_at TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

def idempotent_operation(idempotency_key: str, operation_type: str, operation_fn):
    """
    Execute operation with idempotency guarantee
    """
    # Check if operation already executed
    existing = spark.sql(f"""
        SELECT status, response_data
        FROM idempotency_keys
        WHERE idempotency_key = '{idempotency_key}'
    """).collect()
    
    if existing:
        # Operation already executed
        row = existing[0]
        print(f"⚡ Operation already executed (idempotency key: {idempotency_key})")
        return {
            "success": True,
            "cached": True,
            "status": row['status'],
            "data": json.loads(row['response_data']) if row['response_data'] else None
        }
    
    # Record operation start
    spark.sql(f"""
        INSERT INTO idempotency_keys VALUES (
            '{idempotency_key}',
            '{operation_type}',
            'in_progress',
            NULL,
            CURRENT_TIMESTAMP,
            NULL
        )
    """)
    
    try:
        # Execute operation
        result = operation_fn()
        
        # Record completion
        spark.sql(f"""
            UPDATE idempotency_keys
            SET 
                status = 'completed',
                response_data = '{json.dumps(result)}',
                completed_at = CURRENT_TIMESTAMP
            WHERE idempotency_key = '{idempotency_key}'
        """)
        
        return {
            "success": True,
            "cached": False,
            "data": result
        }
        
    except Exception as e:
        # Record failure
        spark.sql(f"""
            UPDATE idempotency_keys
            SET 
                status = 'failed',
                response_data = '{json.dumps({"error": str(e)})}',
                completed_at = CURRENT_TIMESTAMP
            WHERE idempotency_key = '{idempotency_key}'
        """)
        
        return {
            "success": False,
            "error": str(e)
        }

# Test idempotency
print("Testing idempotent operation:")
key = f"TEST-{uuid.uuid4().hex[:8]}"

# First execution
result1 = idempotent_operation(
    key,
    "test_operation",
    lambda: {"value": "computed_result"}
)
print(f"First call: {result1}")

# Second execution (should return cached result)
result2 = idempotent_operation(
    key,
    "test_operation",
    lambda: {"value": "computed_result"}
)
print(f"Second call: {result2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Monitor Saga Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Saga execution statistics
# MAGIC SELECT 
# MAGIC   saga_state,
# MAGIC   COUNT(*) as count,
# MAGIC   AVG(UNIX_TIMESTAMP(completed_at) - UNIX_TIMESTAMP(created_at)) as avg_duration_seconds,
# MAGIC   MIN(UNIX_TIMESTAMP(completed_at) - UNIX_TIMESTAMP(created_at)) as min_duration_seconds,
# MAGIC   MAX(UNIX_TIMESTAMP(completed_at) - UNIX_TIMESTAMP(created_at)) as max_duration_seconds
# MAGIC FROM saga_instances
# MAGIC WHERE completed_at IS NOT NULL
# MAGIC GROUP BY saga_state;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step failure analysis
# MAGIC SELECT 
# MAGIC   step_name,
# MAGIC   step_state,
# MAGIC   COUNT(*) as count,
# MAGIC   AVG(retry_count) as avg_retries
# MAGIC FROM saga_steps
# MAGIC GROUP BY step_name, step_state
# MAGIC ORDER BY step_name, step_state;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Saga event timeline
# MAGIC SELECT 
# MAGIC   saga_id,
# MAGIC   event_type,
# MAGIC   timestamp,
# MAGIC   event_data
# MAGIC FROM saga_events
# MAGIC WHERE saga_id IN (SELECT saga_id FROM saga_instances ORDER BY created_at DESC LIMIT 3)
# MAGIC ORDER BY saga_id, timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC In this lab, you learned:
# MAGIC
# MAGIC 1. ✅ **Distributed transaction challenges** and why 2PC doesn't scale
# MAGIC 2. ✅ **Saga pattern** for managing long-running distributed transactions
# MAGIC 3. ✅ **Compensation logic** to undo completed steps on failure
# MAGIC 4. ✅ **State management** for saga orchestration
# MAGIC 5. ✅ **Retry mechanisms** with exponential backoff
# MAGIC 6. ✅ **Idempotency** to handle duplicate requests safely
# MAGIC 7. ✅ **Event logging** for audit trails and debugging
# MAGIC 8. ✅ **Monitoring** saga performance and failure rates
# MAGIC
# MAGIC ## Best Practices
# MAGIC
# MAGIC - ✅ Design compensation logic for every saga step
# MAGIC - ✅ Make operations idempotent
# MAGIC - ✅ Log all state changes for debugging
# MAGIC - ✅ Implement timeouts for long-running steps
# MAGIC - ✅ Use retry with exponential backoff for transient failures
# MAGIC - ✅ Monitor saga completion rates and durations
# MAGIC - ✅ Test failure scenarios thoroughly
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Lecture 6**: Performance Optimization for Federated Queries
# MAGIC - Learn query pushdown and caching strategies
# MAGIC - Optimize cross-system query performance
