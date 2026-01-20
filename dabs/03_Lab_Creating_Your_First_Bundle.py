# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1 Lab: Creating Your First Bundle
# MAGIC
# MAGIC ## Lab Objectives
# MAGIC In this hands-on lab, you will:
# MAGIC - Create a complete Databricks Asset Bundle from scratch
# MAGIC - Configure multiple environments (dev and prod)
# MAGIC - Deploy and test your bundle
# MAGIC - Practice the full bundle development lifecycle
# MAGIC
# MAGIC ## Estimated Time
# MAGIC 45 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Module 1 lecture and demo
# MAGIC - Databricks CLI installed and authenticated
# MAGIC - Access to a Databricks workspace
# MAGIC - Text editor or IDE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Setup
# MAGIC
# MAGIC Before starting, verify your environment is ready:

# COMMAND ----------

# MAGIC %sh
# MAGIC # Check Databricks CLI version
# MAGIC databricks --version
# MAGIC
# MAGIC # Check authentication
# MAGIC databricks auth profiles

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Initialize Your Bundle (10 minutes)
# MAGIC
# MAGIC ### Task
# MAGIC Create a new bundle for a data processing pipeline that will:
# MAGIC - Process customer data
# MAGIC - Generate daily reports
# MAGIC - Run on a schedule
# MAGIC
# MAGIC ### Steps
# MAGIC
# MAGIC 1. **Create a project directory**
# MAGIC    ```bash
# MAGIC    mkdir customer-analytics-bundle
# MAGIC    cd customer-analytics-bundle
# MAGIC    ```
# MAGIC
# MAGIC 2. **Initialize bundle with a template**
# MAGIC    ```bash
# MAGIC    databricks bundle init default-python
# MAGIC    ```
# MAGIC
# MAGIC 3. **Initialize git repository**
# MAGIC    ```bash
# MAGIC    git init
# MAGIC    git add .
# MAGIC    git commit -m "Initial commit: Bundle scaffolding"
# MAGIC    ```
# MAGIC
# MAGIC ### Validation
# MAGIC Check that you have:
# MAGIC - [ ] `databricks.yml` file exists
# MAGIC - [ ] `src/` directory exists
# MAGIC - [ ] `.gitignore` file exists
# MAGIC - [ ] Git repository initialized

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Create Notebooks (15 minutes)
# MAGIC
# MAGIC ### Task
# MAGIC Create three notebooks for your data pipeline:
# MAGIC
# MAGIC ### Notebook 1: Data Ingestion
# MAGIC Create `src/01_ingest_data.py`:

# COMMAND ----------

# Example notebook content for src/01_ingest_data.py
ingest_notebook = '''# Databricks notebook source
%md
# Customer Data Ingestion

Ingests raw customer data from source systems

# COMMAND ----------

# Get parameters
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("date", "", "Processing Date")

env = dbutils.widgets.get("environment")
catalog = dbutils.widgets.get("catalog")
date = dbutils.widgets.get("date")

print(f"Environment: {env}")
print(f"Catalog: {catalog}")
print(f"Date: {date}")

# COMMAND ----------

# Create sample customer data
from pyspark.sql.functions import current_date, lit
from datetime import datetime

data = [
    (1, "Alice Smith", "alice@email.com", "Premium", 5000),
    (2, "Bob Johnson", "bob@email.com", "Standard", 2500),
    (3, "Charlie Brown", "charlie@email.com", "Premium", 7500),
    (4, "Diana Prince", "diana@email.com", "Basic", 1000),
    (5, "Ethan Hunt", "ethan@email.com", "Premium", 9000),
]

df = spark.createDataFrame(data, 
    ["customer_id", "name", "email", "tier", "lifetime_value"]
)

# Add metadata
df = df.withColumn("ingestion_date", current_date())
df = df.withColumn("environment", lit(env))

display(df)

# COMMAND ----------

# Write to Delta table
table_name = f"{catalog}.bronze.customers"

df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .saveAsTable(table_name)

print(f"Data ingested to {table_name}")
print(f"Record count: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - ✅ Raw customer data ingested
# MAGIC - ✅ Data written to bronze layer
# MAGIC - ✅ Metadata added
# MAGIC '''
# MAGIC
# MAGIC print("Create this file as: src/01_ingest_data.py")
# MAGIC print("=" * 60)
# MAGIC print(ingest_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook 2: Data Transformation
# MAGIC Create `src/02_transform_data.py`:

# COMMAND ----------

# Example notebook content for src/02_transform_data.py
transform_notebook = '''# Databricks notebook source
%md
# Customer Data Transformation

Transforms and enriches customer data

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# Read bronze data
bronze_table = f"{catalog}.bronze.customers"
df = spark.table(bronze_table)

print(f"Reading from: {bronze_table}")
print(f"Record count: {df.count()}")

# COMMAND ----------

# Apply transformations
from pyspark.sql.functions import when, col

# Categorize customers by value
df_transformed = df.withColumn(
    "value_segment",
    when(col("lifetime_value") >= 7000, "High Value")
    .when(col("lifetime_value") >= 3000, "Medium Value")
    .otherwise("Low Value")
)

# Add email domain
from pyspark.sql.functions import split
df_transformed = df_transformed.withColumn(
    "email_domain",
    split(col("email"), "@")[1]
)

display(df_transformed)

# COMMAND ----------

# Write to silver layer
silver_table = f"{catalog}.silver.customers_enriched"

df_transformed.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable(silver_table)

print(f"Data transformed and saved to {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - ✅ Data read from bronze layer
# MAGIC - ✅ Transformations applied
# MAGIC - ✅ Enriched data written to silver layer
# MAGIC '''
# MAGIC
# MAGIC print("Create this file as: src/02_transform_data.py")
# MAGIC print("=" * 60)
# MAGIC print(transform_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook 3: Analytics
# MAGIC Create `src/03_analytics.py`:

# COMMAND ----------

# Example notebook content for src/03_analytics.py
analytics_notebook = '''# Databricks notebook source
%md
# Customer Analytics

Generates analytics and reports

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# Read silver data
silver_table = f"{catalog}.silver.customers_enriched"
df = spark.table(silver_table)

# COMMAND ----------

# Analysis 1: Customer tier distribution
print("=== Customer Tier Distribution ===")
tier_dist = df.groupBy("tier").count().orderBy("count", ascending=False)
display(tier_dist)

# COMMAND ----------

# Analysis 2: Value segment breakdown
print("=== Value Segment Breakdown ===")
segment_dist = df.groupBy("value_segment").count().orderBy("count", ascending=False)
display(segment_dist)

# COMMAND ----------

# Analysis 3: Average lifetime value by tier
print("=== Average Lifetime Value by Tier ===")
from pyspark.sql.functions import avg, round

avg_value = df.groupBy("tier") \\
    .agg(round(avg("lifetime_value"), 2).alias("avg_lifetime_value")) \\
    .orderBy("avg_lifetime_value", ascending=False)
display(avg_value)

# COMMAND ----------

# Create gold layer aggregate table
gold_table = f"{catalog}.gold.customer_metrics"

# Calculate metrics
from pyspark.sql.functions import count, sum, avg

metrics = df.groupBy("tier", "value_segment").agg(
    count("*").alias("customer_count"),
    sum("lifetime_value").alias("total_value"),
    avg("lifetime_value").alias("avg_value")
)

metrics.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .saveAsTable(gold_table)

print(f"Metrics saved to {gold_table}")
display(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC - ✅ Analytics generated
# MAGIC - ✅ Metrics calculated
# MAGIC - ✅ Gold layer table created
# MAGIC '''
# MAGIC
# MAGIC print("Create this file as: src/03_analytics.py")
# MAGIC print("=" * 60)
# MAGIC print(analytics_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Configure the Bundle (15 minutes)
# MAGIC
# MAGIC ### Task
# MAGIC Create a comprehensive `databricks.yml` configuration file.

# COMMAND ----------

# Complete databricks.yml configuration
databricks_yml = '''bundle:
  name: customer-analytics

variables:
  catalog:
    description: "Unity Catalog name"
    default: "dev_catalog"
  
  num_workers:
    description: "Number of cluster workers"
    default: 2
  
  warehouse_id:
    description: "SQL Warehouse ID for queries"
    default: ""

targets:
  # Development environment
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "dev_catalog"
      num_workers: 1
  
  # Production environment
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "prod_catalog"
      num_workers: 4
    
    permissions:
      - level: CAN_VIEW
        group_name: "data-team"

resources:
  jobs:
    customer_analytics_pipeline:
      name: "Customer Analytics Pipeline - ${bundle.target}"
      
      # Schedule: Run daily at 2 AM UTC
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "UTC"
        pause_status: PAUSED
      
      # Email notifications
      email_notifications:
        on_start:
          - data-team@company.com
        on_success:
          - data-team@company.com
        on_failure:
          - data-team@company.com
          - oncall@company.com
      
      # Job timeout (1 hour)
      timeout_seconds: 3600
      
      # Maximum concurrent runs
      max_concurrent_runs: 1
      
      tasks:
        # Task 1: Ingest data
        - task_key: ingest_data
          notebook_task:
            notebook_path: ./src/01_ingest_data.py
            base_parameters:
              environment: ${bundle.target}
              catalog: ${var.catalog}
              date: "{{job.start_time}}"
          
          new_cluster:
            num_workers: ${var.num_workers}
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            autotermination_minutes: 30
        
        # Task 2: Transform data (depends on Task 1)
        - task_key: transform_data
          depends_on:
            - task_key: ingest_data
          
          notebook_task:
            notebook_path: ./src/02_transform_data.py
            base_parameters:
              catalog: ${var.catalog}
          
          new_cluster:
            num_workers: ${var.num_workers}
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            autotermination_minutes: 30
        
        # Task 3: Generate analytics (depends on Task 2)
        - task_key: analytics
          depends_on:
            - task_key: transform_data
          
          notebook_task:
            notebook_path: ./src/03_analytics.py
            base_parameters:
              catalog: ${var.catalog}
          
          new_cluster:
            num_workers: ${var.num_workers}
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            autotermination_minutes: 30
      
      # Job-level permissions
      permissions:
        - level: CAN_VIEW
          group_name: "data-team"
        - level: CAN_MANAGE_RUN
          user_name: "admin@company.com"
        - level: IS_OWNER
          user_name: "${workspace.current_user.userName}"
'''

print("Create/update this file as: databricks.yml")
print("=" * 60)
print(databricks_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checklist
# MAGIC
# MAGIC After creating all files, verify:
# MAGIC - [ ] All three notebooks created in `src/` directory
# MAGIC - [ ] `databricks.yml` configured with all resources
# MAGIC - [ ] Variable interpolation used correctly
# MAGIC - [ ] Task dependencies defined properly
# MAGIC - [ ] Email notifications configured (update with your email)
# MAGIC - [ ] Permissions defined

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Validate and Deploy (10 minutes)
# MAGIC
# MAGIC ### Task 1: Validate Configuration
# MAGIC
# MAGIC ```bash
# MAGIC # Validate bundle
# MAGIC databricks bundle validate -t dev
# MAGIC
# MAGIC # If there are errors, fix them and validate again
# MAGIC databricks bundle validate -t dev --verbose
# MAGIC ```
# MAGIC
# MAGIC ### Task 2: Deploy to Development
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy to dev
# MAGIC databricks bundle deploy -t dev
# MAGIC ```
# MAGIC
# MAGIC ### Task 3: Verify Deployment
# MAGIC
# MAGIC ```bash
# MAGIC # Check deployment status
# MAGIC databricks bundle summary
# MAGIC
# MAGIC # List deployed resources
# MAGIC databricks bundle list
# MAGIC ```
# MAGIC
# MAGIC ### Task 4: Run the Pipeline
# MAGIC
# MAGIC ```bash
# MAGIC # Run the job
# MAGIC databricks bundle run customer_analytics_pipeline -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Verify Results (5 minutes)
# MAGIC
# MAGIC ### Task
# MAGIC Verify that your pipeline created the expected tables and data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check bronze layer
# MAGIC SELECT * FROM dev_catalog.bronze.customers LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check silver layer
# MAGIC SELECT * FROM dev_catalog.silver.customers_enriched LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check gold layer
# MAGIC SELECT * FROM dev_catalog.gold.customer_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Exercise: Make Changes and Redeploy (Optional)
# MAGIC
# MAGIC ### Task
# MAGIC Make modifications to your bundle and practice redeployment:
# MAGIC
# MAGIC 1. **Add a new metric** to the analytics notebook
# MAGIC 2. **Update the configuration** to add a new parameter
# MAGIC 3. **Commit your changes** to git
# MAGIC 4. **Redeploy** the updated bundle
# MAGIC 5. **Run** the pipeline again
# MAGIC
# MAGIC ### Commands
# MAGIC ```bash
# MAGIC # Make your changes to the files
# MAGIC
# MAGIC # Commit changes
# MAGIC git add .
# MAGIC git commit -m "Added new analytics metric"
# MAGIC
# MAGIC # Validate
# MAGIC databricks bundle validate -t dev
# MAGIC
# MAGIC # Redeploy
# MAGIC databricks bundle deploy -t dev --auto-approve
# MAGIC
# MAGIC # Run
# MAGIC databricks bundle run customer_analytics_pipeline -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Solutions and Hints
# MAGIC
# MAGIC ### Common Issues and Solutions
# MAGIC
# MAGIC **Issue: Validation fails with YAML syntax error**
# MAGIC - Check indentation (use 2 spaces consistently)
# MAGIC - Ensure colons have a space after them
# MAGIC - Verify quotes around special characters
# MAGIC
# MAGIC **Issue: Deployment fails with permission error**
# MAGIC - Verify you have permission to create jobs
# MAGIC - Check your Databricks CLI authentication
# MAGIC - Ensure workspace URL is correct
# MAGIC
# MAGIC **Issue: Tables not found**
# MAGIC - Verify catalog exists and you have access
# MAGIC - Check schema names match configuration
# MAGIC - Ensure job completed successfully
# MAGIC
# MAGIC **Issue: Job fails to run**
# MAGIC - Check cluster configuration
# MAGIC - Verify notebook paths are correct
# MAGIC - Review job run logs in Databricks UI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Completion Checklist
# MAGIC
# MAGIC ### Required Tasks
# MAGIC - [ ] Created bundle directory structure
# MAGIC - [ ] Created three notebook files
# MAGIC - [ ] Configured databricks.yml with job definition
# MAGIC - [ ] Validated bundle configuration
# MAGIC - [ ] Deployed to development environment
# MAGIC - [ ] Ran the pipeline successfully
# MAGIC - [ ] Verified all three tables were created
# MAGIC - [ ] Committed code to git
# MAGIC
# MAGIC ### Bonus Tasks
# MAGIC - [ ] Modified notebooks and redeployed
# MAGIC - [ ] Tested with different parameters
# MAGIC - [ ] Reviewed job run history in UI
# MAGIC - [ ] Configured production target
# MAGIC
# MAGIC ### Knowledge Check
# MAGIC - [ ] Can explain bundle lifecycle
# MAGIC - [ ] Understand variable interpolation
# MAGIC - [ ] Can troubleshoot deployment issues
# MAGIC - [ ] Comfortable with CLI commands

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC Congratulations! You've successfully:
# MAGIC
# MAGIC ✅ Created a complete Databricks Asset Bundle
# MAGIC ✅ Configured a multi-task job with dependencies
# MAGIC ✅ Implemented a medallion architecture (bronze → silver → gold)
# MAGIC ✅ Used variable interpolation for environment-specific configs
# MAGIC ✅ Deployed and ran your bundle
# MAGIC ✅ Verified the results
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Bundles enable Infrastructure as Code** for Databricks resources
# MAGIC 2. **Variable interpolation** makes configurations flexible and reusable
# MAGIC 3. **Task dependencies** define execution order in jobs
# MAGIC 4. **Validation** catches errors before deployment
# MAGIC 5. **Git integration** enables version control and collaboration

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC You're now ready to move to Module 2, where you'll learn:
# MAGIC - Strategies for migrating existing jobs to DABs
# MAGIC - How to analyze and convert legacy workflows
# MAGIC - Best practices for incremental migration
# MAGIC - Handling complex dependencies
# MAGIC
# MAGIC **Continue to:** `04_Lecture_Job_Migration_Strategies.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Practice (Optional)
# MAGIC
# MAGIC If you want more practice, try these challenges:
# MAGIC
# MAGIC ### Challenge 1: Add Error Handling
# MAGIC Enhance the notebooks with proper error handling and logging
# MAGIC
# MAGIC ### Challenge 2: Add Data Quality Checks
# MAGIC Implement data validation in the transformation notebook
# MAGIC
# MAGIC ### Challenge 3: Create a Production Deployment
# MAGIC Configure and deploy to a production environment
# MAGIC
# MAGIC ### Challenge 4: Add Monitoring
# MAGIC Set up email notifications and alerting
# MAGIC
# MAGIC ### Challenge 5: Optimize Cluster Configuration
# MAGIC Experiment with different cluster sizes and autoscaling
