# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge Lab 11: Multi-Tenant RAG System
# MAGIC
# MAGIC ## Challenge Overview
# MAGIC Build a complete multi-tenant Retrieval-Augmented Generation (RAG) system with isolated vector stores, cross-tenant search capabilities, cost allocation, and performance isolation.
# MAGIC
# MAGIC ## Challenge Duration
# MAGIC 60-90 minutes
# MAGIC
# MAGIC ## Challenge Requirements
# MAGIC This is a challenge lab with minimal guidance. You must design and implement:
# MAGIC 1. Tenant isolation architecture
# MAGIC 2. Per-tenant vector stores
# MAGIC 3. Cross-tenant search (with permission checks)
# MAGIC 4. Cost allocation and quota management
# MAGIC 5. Performance SLAs per tenant

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC CREATE SCHEMA IF NOT EXISTS challenge11_multi_tenant;
# MAGIC USE SCHEMA challenge11_multi_tenant;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Design Tenant Architecture
# MAGIC
# MAGIC Create tables for:
# MAGIC - Tenants and their configurations
# MAGIC - Tenant quotas and limits
# MAGIC - Per-tenant document storage
# MAGIC - Per-tenant vector indexes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Design and implement your tenant architecture
# MAGIC CREATE TABLE IF NOT EXISTS tenants (
# MAGIC   tenant_id STRING PRIMARY KEY,
# MAGIC   tenant_name STRING,
# MAGIC   tier STRING, -- 'free', 'pro', 'enterprise'
# MAGIC   created_at TIMESTAMP,
# MAGIC   status STRING
# MAGIC );
# MAGIC
# MAGIC -- TODO: Add tenant documents table with isolation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Implement Tenant Isolation
# MAGIC
# MAGIC - Ensure data isolation between tenants
# MAGIC - Implement row-level security
# MAGIC - Create tenant-specific views

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Cost Allocation System
# MAGIC
# MAGIC Track and allocate costs per tenant for:
# MAGIC - AI Function usage
# MAGIC - Storage
# MAGIC - Compute

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Cross-Tenant Search
# MAGIC
# MAGIC Build search that can:
# MAGIC - Search within a single tenant
# MAGIC - Search across tenants (with proper authorization)
# MAGIC - Maintain performance isolation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Quota Management
# MAGIC
# MAGIC Implement:
# MAGIC - Per-tenant API quotas
# MAGIC - Storage limits
# MAGIC - Rate limiting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Validation
# MAGIC
# MAGIC Your implementation must:
# MAGIC - Support at least 3 tenants
# MAGIC - Provide complete data isolation
# MAGIC - Track costs per tenant
# MAGIC - Enforce quotas
# MAGIC - Maintain < 2 second query latency

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges
# MAGIC - Implement tenant onboarding automation
# MAGIC - Add multi-region support
# MAGIC - Create tenant analytics dashboard
