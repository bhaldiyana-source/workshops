# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2 Lab: Migrating Existing Jobs to DABs
# MAGIC
# MAGIC ## Lab Objectives
# MAGIC In this hands-on lab, you will:
# MAGIC - Export and analyze an existing job
# MAGIC - Identify migration challenges
# MAGIC - Refactor notebooks for parameterization
# MAGIC - Create a complete bundle
# MAGIC - Deploy and validate the migrated job
# MAGIC
# MAGIC ## Estimated Time
# MAGIC 60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Scenario
# MAGIC
# MAGIC You have an existing "Customer Segmentation" job that needs to be migrated to DABs. The job:
# MAGIC - Segments customers based on purchase behavior
# MAGIC - Runs weekly on Sundays at 3 AM
# MAGIC - Uses two notebooks in sequence
# MAGIC - Has hardcoded values that need parameterization
# MAGIC - Uses an existing cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Export and Analyze (15 minutes)
# MAGIC
# MAGIC ### Task 1.1: Export Job Configuration
# MAGIC
# MAGIC ```bash
# MAGIC # List all jobs to find the job ID
# MAGIC databricks jobs list | grep "Customer Segmentation"
# MAGIC
# MAGIC # Export the job (replace XXXXX with actual job ID)
# MAGIC databricks jobs get --job-id XXXXX > customer_segmentation_original.json
# MAGIC ```
# MAGIC
# MAGIC ### Task 1.2: Analyze the Configuration
# MAGIC
# MAGIC Review the exported JSON and document:
# MAGIC - [ ] Job name and schedule
# MAGIC - [ ] Number of tasks
# MAGIC - [ ] Task dependencies
# MAGIC - [ ] Cluster configuration
# MAGIC - [ ] Email notifications
# MAGIC - [ ] Libraries used
# MAGIC - [ ] Parameters passed

# COMMAND ----------

# Example exported job for reference
example_exported_job = '''{
  "job_id": 456789,
  "settings": {
    "name": "Customer Segmentation",
    "schedule": {
      "quartz_cron_expression": "0 0 3 ? * SUN",
      "timezone_id": "UTC"
    },
    "tasks": [
      {
        "task_key": "calculate_rfm",
        "notebook_task": {
          "notebook_path": "/Users/analyst@company.com/Segmentation/calculate_rfm"
        },
        "existing_cluster_id": "0401-234567-xyz9876"
      },
      {
        "task_key": "assign_segments",
        "depends_on": [{"task_key": "calculate_rfm"}],
        "notebook_task": {
          "notebook_path": "/Users/analyst@company.com/Segmentation/assign_segments"
        },
        "existing_cluster_id": "0401-234567-xyz9876"
      }
    ],
    "email_notifications": {
      "on_failure": ["analytics@company.com"]
    }
  }
}'''

print("Example Exported Job Structure:")
print(example_exported_job)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Create Original Notebooks (15 minutes)
# MAGIC
# MAGIC For this lab, we'll simulate the original notebooks with hardcoded values.
# MAGIC
# MAGIC ### Task 2.1: Create First Notebook
# MAGIC
# MAGIC Create `original_notebooks/calculate_rfm.py` with the following content:

# COMMAND ----------

# Original notebook 1 - Calculate RFM scores (with issues to fix)
original_rfm_notebook = '''# Databricks notebook source
# Calculate RFM (Recency, Frequency, Monetary) Scores

# COMMAND ----------

# Hardcoded values - NEED TO FIX
catalog = "prod_catalog"
transactions_table = "sales.transactions"
output_table = "analytics.customer_rfm"

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Hardcoded reference date - NEED TO FIX
reference_date = "2026-01-01"

# COMMAND ----------

# Read transactions
df = spark.table(f"{catalog}.{transactions_table}")

# Calculate RFM metrics
rfm = df.groupBy("customer_id").agg(
    F.datediff(F.lit(reference_date), F.max("transaction_date")).alias("recency"),
    F.count("*").alias("frequency"),
    F.sum("amount").alias("monetary")
)

# Calculate RFM scores (1-5 scale)
from pyspark.sql import Window

rfm_with_scores = rfm.withColumn(
    "r_score",
    F.ntile(5).over(Window.orderBy(F.col("recency").desc()))
).withColumn(
    "f_score", 
    F.ntile(5).over(Window.orderBy("frequency"))
).withColumn(
    "m_score",
    F.ntile(5).over(Window.orderBy("monetary"))
)

display(rfm_with_scores)

# COMMAND ----------

# Write results - Hardcoded table name
rfm_with_scores.write.mode("overwrite").saveAsTable(f"{catalog}.{output_table}")

print(f"RFM scores calculated for {rfm_with_scores.count()} customers")
'''

print("ORIGINAL NOTEBOOK 1: Calculate RFM")
print("="*60)
print("Issues to fix:")
print("  ❌ Hardcoded catalog name")
print("  ❌ Hardcoded table names")
print("  ❌ Hardcoded reference date")
print("  ❌ No error handling")
print("  ❌ No parameterization")
print("\n" + "="*60)
print(original_rfm_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Create Second Notebook
# MAGIC
# MAGIC Create `original_notebooks/assign_segments.py`:

# COMMAND ----------

# Original notebook 2 - Assign customer segments (with issues to fix)
original_segments_notebook = '''# Databricks notebook source
# Assign Customer Segments

# COMMAND ----------

# Hardcoded values
catalog = "prod_catalog"
rfm_table = "analytics.customer_rfm"
segments_table = "analytics.customer_segments"

# COMMAND ----------

from pyspark.sql import functions as F

# Read RFM scores
rfm_df = spark.table(f"{catalog}.{rfm_table}")

# COMMAND ----------

# Define segmentation logic
segments_df = rfm_df.withColumn(
    "segment",
    F.when(
        (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
        "Champions"
    ).when(
        (F.col("r_score") >= 3) & (F.col("f_score") >= 3),
        "Loyal Customers"
    ).when(
        (F.col("r_score") >= 4) & (F.col("f_score") <= 2),
        "New Customers"
    ).when(
        (F.col("r_score") <= 2) & (F.col("f_score") >= 3),
        "At Risk"
    ).when(
        (F.col("r_score") <= 2) & (F.col("f_score") <= 2),
        "Lost"
    ).otherwise("Other")
)

# Add segment metadata
segments_df = segments_df.withColumn("segmented_at", F.current_timestamp())

display(segments_df)

# COMMAND ----------

# Segment distribution
segment_counts = segments_df.groupBy("segment").count().orderBy("count", ascending=False)
display(segment_counts)

# COMMAND ----------

# Write results
segments_df.write.mode("overwrite").saveAsTable(f"{catalog}.{segments_table}")

print(f"Segmented {segments_df.count()} customers")
'''

print("ORIGINAL NOTEBOOK 2: Assign Segments")
print("="*60)
print("Issues to fix:")
print("  ❌ Hardcoded catalog and table names")
print("  ❌ No parameterization")
print("  ❌ No error handling")
print("\n" + "="*60)
print(original_segments_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Refactor Notebooks (20 minutes)
# MAGIC
# MAGIC ### Task 3.1: Refactor First Notebook
# MAGIC
# MAGIC Create `src/calculate_rfm.py` with proper parameterization:

# COMMAND ----------

# Refactored notebook 1
refactored_rfm = '''# Databricks notebook source
%md
# Calculate RFM Scores

Calculates Recency, Frequency, Monetary scores for customer segmentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Define widgets
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("reference_date", "", "Reference Date")

# Get parameters
catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")
reference_date = dbutils.widgets.get("reference_date")

# Use current date if not specified
if not reference_date:
    from datetime import datetime
    reference_date = datetime.now().strftime("%Y-%m-%d")

print(f"Environment: {environment}")
print(f"Catalog: {catalog}")
print(f"Reference Date: {reference_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate RFM Metrics

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

try:
    # Read transactions
    transactions_table = f"{catalog}.sales.transactions"
    df = spark.table(transactions_table)
    print(f"Read {df.count()} transactions from {transactions_table}")
    
    # Calculate RFM metrics
    rfm = df.groupBy("customer_id").agg(
        F.datediff(F.lit(reference_date), F.max("transaction_date")).alias("recency"),
        F.count("*").alias("frequency"),
        F.sum("amount").alias("monetary")
    )
    
    print(f"Calculated RFM for {rfm.count()} customers")
    
except Exception as e:
    print(f"Error calculating RFM: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate RFM Scores

# COMMAND ----------

try:
    # Calculate quintile scores (1-5)
    rfm_with_scores = rfm \\
        .withColumn("r_score", F.ntile(5).over(Window.orderBy(F.col("recency").desc()))) \\
        .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency"))) \\
        .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary")))
    
    # Add metadata
    rfm_with_scores = rfm_with_scores \\
        .withColumn("reference_date", F.lit(reference_date)) \\
        .withColumn("calculated_at", F.current_timestamp()) \\
        .withColumn("environment", F.lit(environment))
    
    display(rfm_with_scores)
    
except Exception as e:
    print(f"Error calculating scores: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results

# COMMAND ----------

try:
    output_table = f"{catalog}.analytics.customer_rfm"
    
    rfm_with_scores.write \\
        .format("delta") \\
        .mode("overwrite") \\
        .option("overwriteSchema", "true") \\
        .saveAsTable(output_table)
    
    print(f"✅ RFM scores written to {output_table}")
    print(f"Customer count: {rfm_with_scores.count()}")
    
except Exception as e:
    print(f"❌ Error writing results: {str(e)}")
    raise
'''

print("YOUR TASK: Create this as src/calculate_rfm.py")
print("="*60)
print(refactored_rfm)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Refactor Second Notebook
# MAGIC
# MAGIC Create `src/assign_segments.py` with proper parameterization:

# COMMAND ----------

# Refactored notebook 2
refactored_segments = '''# Databricks notebook source
%md
# Assign Customer Segments

Assigns customers to segments based on RFM scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("environment", "dev", "Environment")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")

print(f"Environment: {environment}")
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read RFM Scores

# COMMAND ----------

from pyspark.sql import functions as F

try:
    rfm_table = f"{catalog}.analytics.customer_rfm"
    rfm_df = spark.table(rfm_table)
    print(f"Read {rfm_df.count()} customers from {rfm_table}")
    
except Exception as e:
    print(f"Error reading RFM data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assign Segments

# COMMAND ----------

try:
    # Segmentation logic
    segments_df = rfm_df.withColumn(
        "segment",
        F.when(
            (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
            "Champions"
        ).when(
            (F.col("r_score") >= 3) & (F.col("f_score") >= 3),
            "Loyal Customers"
        ).when(
            (F.col("r_score") >= 4) & (F.col("f_score") <= 2),
            "New Customers"
        ).when(
            (F.col("r_score") <= 2) & (F.col("f_score") >= 3),
            "At Risk"
        ).when(
            (F.col("r_score") <= 2) & (F.col("f_score") <= 2),
            "Lost"
        ).otherwise("Other")
    )
    
    # Add metadata
    segments_df = segments_df \\
        .withColumn("segmented_at", F.current_timestamp()) \\
        .withColumn("environment", F.lit(environment))
    
    print("Segment distribution:")
    segment_counts = segments_df.groupBy("segment").count().orderBy("count", ascending=False)
    display(segment_counts)
    
except Exception as e:
    print(f"Error assigning segments: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results

# COMMAND ----------

try:
    output_table = f"{catalog}.analytics.customer_segments"
    
    segments_df.write \\
        .format("delta") \\
        .mode("overwrite") \\
        .option("overwriteSchema", "true") \\
        .saveAsTable(output_table)
    
    print(f"✅ Segments written to {output_table}")
    print(f"Customer count: {segments_df.count()}")
    
    # Show segment summary
    final_counts = segments_df.groupBy("segment").count().orderBy("count", ascending=False)
    display(final_counts)
    
except Exception as e:
    print(f"❌ Error writing results: {str(e)}")
    raise
'''

print("YOUR TASK: Create this as src/assign_segments.py")
print("="*60)
print(refactored_segments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Create Bundle Configuration (15 minutes)
# MAGIC
# MAGIC ### Task 4.1: Create databricks.yml
# MAGIC
# MAGIC Create a complete bundle configuration:

# COMMAND ----------

# Complete databricks.yml for the lab
lab_bundle_config = '''bundle:
  name: customer-segmentation

variables:
  catalog:
    description: "Unity Catalog name"
    default: "dev_catalog"
  
  num_workers:
    description: "Number of cluster workers"
    default: 2

targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "dev_catalog"
      num_workers: 1
  
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "prod_catalog"
      num_workers: 4

resources:
  jobs:
    customer_segmentation:
      name: "Customer Segmentation - ${bundle.target}"
      
      # Weekly schedule: Sunday at 3 AM UTC
      schedule:
        quartz_cron_expression: "0 0 3 ? * SUN"
        timezone_id: "UTC"
        pause_status: PAUSED
      
      email_notifications:
        on_failure:
          - analytics@company.com
        on_success:
          - analytics@company.com
      
      max_concurrent_runs: 1
      timeout_seconds: 7200
      
      tasks:
        # Task 1: Calculate RFM scores
        - task_key: calculate_rfm
          notebook_task:
            notebook_path: ./src/calculate_rfm.py
            base_parameters:
              catalog: ${var.catalog}
              environment: ${bundle.target}
              reference_date: "{{job.start_time.format('yyyy-MM-dd')}}"
          
          new_cluster:
            num_workers: ${var.num_workers}
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            autotermination_minutes: 30
        
        # Task 2: Assign segments (depends on Task 1)
        - task_key: assign_segments
          depends_on:
            - task_key: calculate_rfm
          
          notebook_task:
            notebook_path: ./src/assign_segments.py
            base_parameters:
              catalog: ${var.catalog}
              environment: ${bundle.target}
          
          new_cluster:
            num_workers: ${var.num_workers}
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            autotermination_minutes: 30
      
      permissions:
        - level: CAN_VIEW
          group_name: "analytics-team"
        - level: CAN_MANAGE_RUN
          user_name: "${workspace.current_user.userName}"
'''

print("YOUR TASK: Create this as databricks.yml")
print("="*60)
print(lab_bundle_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Deploy and Test (15 minutes)
# MAGIC
# MAGIC ### Task 5.1: Validate Bundle
# MAGIC
# MAGIC ```bash
# MAGIC # Navigate to bundle directory
# MAGIC cd customer-segmentation
# MAGIC
# MAGIC # Validate
# MAGIC databricks bundle validate -t dev
# MAGIC ```
# MAGIC
# MAGIC ### Task 5.2: Deploy to Development
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy
# MAGIC databricks bundle deploy -t dev
# MAGIC ```
# MAGIC
# MAGIC ### Task 5.3: Run the Job
# MAGIC
# MAGIC ```bash
# MAGIC # Run the job
# MAGIC databricks bundle run customer_segmentation -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Validate Results (10 minutes)
# MAGIC
# MAGIC ### Task 6.1: Check RFM Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check RFM scores
# MAGIC SELECT *
# MAGIC FROM dev_catalog.analytics.customer_rfm
# MAGIC WHERE environment = 'dev'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RFM score distribution
# MAGIC SELECT r_score, f_score, m_score, count(*) as customer_count
# MAGIC FROM dev_catalog.analytics.customer_rfm
# MAGIC WHERE environment = 'dev'
# MAGIC GROUP BY r_score, f_score, m_score
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2: Check Segments Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check segment distribution
# MAGIC SELECT 
# MAGIC   segment,
# MAGIC   count(*) as customer_count,
# MAGIC   round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage
# MAGIC FROM dev_catalog.analytics.customer_segments
# MAGIC WHERE environment = 'dev'
# MAGIC GROUP BY segment
# MAGIC ORDER BY customer_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Exercise: Add Monitoring (Optional)
# MAGIC
# MAGIC ### Task: Create a Monitoring Notebook
# MAGIC
# MAGIC Create `src/monitor_segmentation.py` that:
# MAGIC - Compares current vs previous segmentation
# MAGIC - Identifies customers who changed segments
# MAGIC - Generates summary statistics
# MAGIC - Sends alerts if significant changes detected

# COMMAND ----------

# Monitoring notebook template
monitoring_notebook = '''# Databricks notebook source
%md
# Segmentation Monitoring

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F

# Read current and previous segmentation
current = spark.table(f"{catalog}.analytics.customer_segments") \\
    .filter(F.col("environment") == "dev")

# Compare with previous run (implement your logic)
# This is a template - customize as needed

# Calculate segment changes
# Send alerts if thresholds exceeded
# Generate reports

print("Monitoring complete")
'''

print("BONUS: Create monitoring notebook")
print(monitoring_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Completion Checklist
# MAGIC
# MAGIC ### Core Requirements
# MAGIC - [ ] Analyzed original job configuration
# MAGIC - [ ] Identified hardcoded values and issues
# MAGIC - [ ] Created refactored notebook 1 with parameterization
# MAGIC - [ ] Created refactored notebook 2 with parameterization
# MAGIC - [ ] Created complete databricks.yml
# MAGIC - [ ] Validated bundle configuration
# MAGIC - [ ] Deployed to development environment
# MAGIC - [ ] Ran the migrated job successfully
# MAGIC - [ ] Verified RFM table created correctly
# MAGIC - [ ] Verified segments table created correctly
# MAGIC - [ ] Committed code to git
# MAGIC
# MAGIC ### Bonus Tasks
# MAGIC - [ ] Created monitoring notebook
# MAGIC - [ ] Added data quality checks
# MAGIC - [ ] Deployed to production
# MAGIC - [ ] Created documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC Congratulations! You've successfully migrated a multi-task job to DABs!
# MAGIC
# MAGIC ### Key Accomplishments
# MAGIC
# MAGIC ✅ **Exported** existing job configuration
# MAGIC ✅ **Analyzed** job structure and dependencies
# MAGIC ✅ **Refactored** notebooks with proper parameterization
# MAGIC ✅ **Created** complete bundle structure
# MAGIC ✅ **Deployed** to development environment
# MAGIC ✅ **Validated** results match expectations
# MAGIC
# MAGIC ### Skills Demonstrated
# MAGIC
# MAGIC 1. Job configuration analysis
# MAGIC 2. Notebook refactoring
# MAGIC 3. Parameterization best practices
# MAGIC 4. Task dependency management
# MAGIC 5. Bundle deployment workflow
# MAGIC 6. Result validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC You're now ready for Module 3, where you'll learn:
# MAGIC - Advanced resource management (DLT, clusters, Unity Catalog)
# MAGIC - Managing complex dependencies
# MAGIC - Working with multiple resource types
# MAGIC - Library and wheel management
# MAGIC
# MAGIC **Continue to:** `07_Lecture_Advanced_Resources.py`
