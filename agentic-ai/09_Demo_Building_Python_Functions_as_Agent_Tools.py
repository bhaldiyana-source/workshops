# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building Python Functions as Agent Tools
# MAGIC
# MAGIC ## Overview
# MAGIC Create Python Unity Catalog functions with complex logic and external integrations.
# MAGIC
# MAGIC ## What You'll Build
# MAGIC - Customer analytics function
# MAGIC - Data enrichment function
# MAGIC - Register as UC functions
# MAGIC
# MAGIC ## Duration
# MAGIC 30 minutes

# COMMAND ----------

# Define Python function
def calculate_customer_metrics(customer_id: int) -> str:
    """
    Calculate comprehensive metrics for a customer.
    
    Args:
        customer_id: Customer ID
        
    Returns:
        JSON string with metrics
    """
    # Simulated calculation
    metrics = {
        "customer_id": customer_id,
        "total_orders": 15,
        "avg_order_value": 166.67,
        "lifetime_value": 2500.00,
        "churn_risk": "Low"
    }
    
    import json
    return json.dumps(metrics)

# Test
print(calculate_customer_metrics(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register as UC Function
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.catalog import FunctionInfo, FunctionParameterInfo
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC function_info = FunctionInfo(
# MAGIC     name="main.default.calculate_customer_metrics",
# MAGIC     catalog_name="main",
# MAGIC     schema_name="default",
# MAGIC     input_params=[FunctionParameterInfo(name="customer_id", type_name="BIGINT")],
# MAGIC     data_type="STRING",
# MAGIC     comment="Calculates customer metrics"
# MAGIC )
# MAGIC
# MAGIC w.functions.create(function_info=function_info)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Created Python UC functions
# MAGIC ✅ Registered for agent use
# MAGIC ✅ Ready for testing
