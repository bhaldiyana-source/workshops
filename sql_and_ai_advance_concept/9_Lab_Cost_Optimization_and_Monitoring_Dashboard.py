# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 9: Cost Optimization and Monitoring Dashboard
# MAGIC
# MAGIC ## Lab Overview
# MAGIC Build cost tracking infrastructure, implement caching, create monitoring dashboards, and set up alerts for AI Function usage.
# MAGIC
# MAGIC ## Duration: 45-60 minutes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC CREATE SCHEMA IF NOT EXISTS lab9_cost_monitoring;
# MAGIC USE SCHEMA lab9_cost_monitoring;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Cost Tracking Infrastructure

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cost tracking table
# MAGIC CREATE TABLE IF NOT EXISTS ai_function_costs (
# MAGIC   cost_id STRING DEFAULT uuid(),
# MAGIC   execution_id STRING,
# MAGIC   function_name STRING,
# MAGIC   model_endpoint STRING,
# MAGIC   input_tokens INT,
# MAGIC   output_tokens INT,
# MAGIC   total_tokens INT,
# MAGIC   cost_per_token DECIMAL(10,6),
# MAGIC   total_cost DECIMAL(10,4),
# MAGIC   execution_timestamp TIMESTAMP,
# MAGIC   user_name STRING,
# MAGIC   tags MAP<STRING, STRING>
# MAGIC )
# MAGIC PARTITIONED BY (DATE(execution_timestamp));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample cost data
# MAGIC INSERT INTO ai_function_costs
# MAGIC SELECT 
# MAGIC   uuid(),
# MAGIC   CONCAT('exec_', id),
# MAGIC   'AI_CLASSIFY',
# MAGIC   'databricks-meta-llama-3-1-70b-instruct',
# MAGIC   CAST(RAND() * 1000 + 100 AS INT),
# MAGIC   CAST(RAND() * 100 + 10 AS INT),
# MAGIC   CAST(RAND() * 1100 + 110 AS INT),
# MAGIC   0.0001,
# MAGIC   CAST((RAND() * 1100 + 110) * 0.0001 AS DECIMAL(10,4)),
# MAGIC   current_timestamp() - INTERVAL CAST(RAND() * 24 AS INT) HOURS,
# MAGIC   'user_demo',
# MAGIC   map('pipeline', 'demo_pipeline', 'env', 'production')
# MAGIC FROM RANGE(1, 101) t(id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Cost Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily cost summary
# MAGIC CREATE OR REPLACE VIEW cost_dashboard_daily AS
# MAGIC SELECT 
# MAGIC   DATE(execution_timestamp) as date,
# MAGIC   function_name,
# MAGIC   COUNT(*) as execution_count,
# MAGIC   SUM(total_tokens) as total_tokens,
# MAGIC   SUM(total_cost) as total_cost,
# MAGIC   AVG(total_cost) as avg_cost_per_execution,
# MAGIC   MAX(total_cost) as max_cost_single_execution
# MAGIC FROM ai_function_costs
# MAGIC WHERE execution_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(execution_timestamp), function_name
# MAGIC ORDER BY date DESC, total_cost DESC;
# MAGIC
# MAGIC SELECT * FROM cost_dashboard_daily LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cost by user
# MAGIC CREATE OR REPLACE VIEW cost_by_user AS
# MAGIC SELECT 
# MAGIC   user_name,
# MAGIC   COUNT(*) as total_executions,
# MAGIC   SUM(total_tokens) as total_tokens_used,
# MAGIC   SUM(total_cost) as total_cost,
# MAGIC   ROUND(100.0 * SUM(total_cost) / SUM(SUM(total_cost)) OVER(), 2) as cost_percentage
# MAGIC FROM ai_function_costs
# MAGIC WHERE execution_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY user_name
# MAGIC ORDER BY total_cost DESC;
# MAGIC
# MAGIC SELECT * FROM cost_by_user;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Implement Budget Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create budget table
# MAGIC CREATE TABLE IF NOT EXISTS cost_budgets (
# MAGIC   budget_id STRING DEFAULT uuid(),
# MAGIC   budget_name STRING,
# MAGIC   budget_period STRING,
# MAGIC   budget_amount DECIMAL(10,2),
# MAGIC   alert_threshold_pct INT,
# MAGIC   is_active BOOLEAN
# MAGIC );
# MAGIC
# MAGIC INSERT INTO cost_budgets (budget_name, budget_period, budget_amount, alert_threshold_pct, is_active)
# MAGIC VALUES ('Daily AI Functions', 'daily', 100.00, 80, TRUE);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Budget monitoring query
# MAGIC CREATE OR REPLACE VIEW budget_alerts AS
# MAGIC WITH current_spend AS (
# MAGIC   SELECT 
# MAGIC     SUM(total_cost) as daily_spend
# MAGIC   FROM ai_function_costs
# MAGIC   WHERE DATE(execution_timestamp) = CURRENT_DATE()
# MAGIC )
# MAGIC SELECT 
# MAGIC   b.budget_name,
# MAGIC   b.budget_amount,
# MAGIC   cs.daily_spend as current_spend,
# MAGIC   b.budget_amount - cs.daily_spend as remaining_budget,
# MAGIC   ROUND(100.0 * cs.daily_spend / b.budget_amount, 2) as budget_utilized_pct,
# MAGIC   CASE 
# MAGIC     WHEN 100.0 * cs.daily_spend / b.budget_amount >= 100 THEN 'EXCEEDED'
# MAGIC     WHEN 100.0 * cs.daily_spend / b.budget_amount >= b.alert_threshold_pct THEN 'WARNING'
# MAGIC     ELSE 'OK'
# MAGIC   END as alert_status
# MAGIC FROM cost_budgets b
# MAGIC CROSS JOIN current_spend cs
# MAGIC WHERE b.is_active = TRUE;
# MAGIC
# MAGIC SELECT * FROM budget_alerts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Caching Strategy

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Implement result caching
# MAGIC CREATE TABLE IF NOT EXISTS ai_result_cache (
# MAGIC   cache_key STRING PRIMARY KEY,
# MAGIC   function_name STRING,
# MAGIC   input_hash STRING,
# MAGIC   result STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   last_accessed TIMESTAMP,
# MAGIC   access_count INT,
# MAGIC   cost_saved DECIMAL(10,4)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Cost Records Tracked' as metric, COUNT(*) as value FROM ai_function_costs
# MAGIC UNION ALL
# MAGIC SELECT 'Total Cost', CAST(SUM(total_cost) AS STRING) FROM ai_function_costs
# MAGIC UNION ALL
# MAGIC SELECT 'Budget Alerts Configured', CAST(COUNT(*) AS STRING) FROM cost_budgets
# MAGIC UNION ALL
# MAGIC SELECT 'Dashboard Views Created', '3';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC Continue to **Lab 10: Compliance-Aware AI Pipeline**
