# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2 Demo: Migrating a Simple Job to DABs
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we'll walk through the complete process of migrating an existing Databricks job to a DABs-managed bundle. You'll see:
# MAGIC - How to export an existing job configuration
# MAGIC - How to analyze the job for migration
# MAGIC - How to convert the configuration to DABs format
# MAGIC - How to test and deploy the migrated job
# MAGIC
# MAGIC ## Scenario
# MAGIC We have an existing job that:
# MAGIC - Runs daily sales reports
# MAGIC - Uses a single notebook
# MAGIC - Reads from a Delta table
# MAGIC - Writes results to another table
# MAGIC - Sends email notifications

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Export the Existing Job
# MAGIC
# MAGIC First, let's export the current job configuration:
# MAGIC
# MAGIC ```bash
# MAGIC # Find the job ID (from UI or list command)
# MAGIC databricks jobs list | grep "Daily Sales Report"
# MAGIC
# MAGIC # Export the job configuration
# MAGIC databricks jobs get --job-id 789012 > original_job.json
# MAGIC ```

# COMMAND ----------

# Example of exported job configuration
original_job_config = '''{
  "job_id": 789012,
  "creator_user_name": "user@company.com",
  "created_time": 1704067200000,
  "settings": {
    "name": "Daily Sales Report",
    "email_notifications": {
      "on_success": ["sales-team@company.com"],
      "on_failure": ["oncall@company.com"]
    },
    "schedule": {
      "quartz_cron_expression": "0 0 6 * * ?",
      "timezone_id": "America/Los_Angeles",
      "pause_status": "UNPAUSED"
    },
    "max_concurrent_runs": 1,
    "tasks": [
      {
        "task_key": "generate_sales_report",
        "notebook_task": {
          "notebook_path": "/Users/user@company.com/Reports/daily_sales",
          "source": "WORKSPACE"
        },
        "existing_cluster_id": "0301-121415-abcd1234",
        "timeout_seconds": 3600,
        "email_notifications": {}
      }
    ]
  }
}'''

print("Original Job Configuration:")
print(original_job_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Analyze the Job
# MAGIC
# MAGIC Let's identify what needs to be migrated:
# MAGIC
# MAGIC ### Job Characteristics
# MAGIC - **Name**: Daily Sales Report
# MAGIC - **Schedule**: Daily at 6 AM Pacific Time
# MAGIC - **Tasks**: Single notebook task
# MAGIC - **Cluster**: Uses existing cluster (needs conversion)
# MAGIC - **Notifications**: Email on success/failure
# MAGIC - **Concurrency**: Max 1 concurrent run
# MAGIC
# MAGIC ### Migration Considerations
# MAGIC 1. **Notebook Path**: Currently in user's personal folder, should move to bundle
# MAGIC 2. **Cluster**: Using existing cluster ID, need to define cluster config
# MAGIC 3. **Environment**: Single environment, need to support dev/prod
# MAGIC 4. **Hardcoded Values**: Check notebook for hardcoded paths/values

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Review the Notebook Content
# MAGIC
# MAGIC Let's look at the existing notebook to identify any issues:

# COMMAND ----------

# Original notebook content
original_notebook = '''# Databricks notebook source
# Daily Sales Report

# COMMAND ----------

# Hardcoded values - PROBLEM!
catalog = "prod_catalog"
schema = "sales"
report_table = "daily_sales_summary"

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, current_date
from datetime import datetime, timedelta

# Calculate yesterday's date
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# Read sales data
sales_df = spark.read.table(f"{catalog}.{schema}.transactions")

# Filter for yesterday
daily_sales = sales_df.filter(sales_df.transaction_date == yesterday)

# COMMAND ----------

# Calculate metrics
sales_summary = daily_sales.groupBy("store_id", "region").agg(
    sum("amount").alias("total_sales"),
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_transaction_value")
)

# Add metadata
sales_summary = sales_summary.withColumn("report_date", current_date())

display(sales_summary)

# COMMAND ----------

# Write to report table - PROBLEM: Hardcoded table name
sales_summary.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable(f"{catalog}.{schema}.{report_table}")

print(f"Report generated for {yesterday}")
print(f"Total stores: {sales_summary.count()}")
'''

print("Original Notebook Issues Identified:")
print("=" * 60)
print("❌ Hardcoded catalog name ('prod_catalog')")
print("❌ Hardcoded schema and table names")
print("❌ No parameterization")
print("❌ Lives in user's personal folder")
print("❌ No error handling")
print("\n" + "=" * 60)
print("\nNotebook Content:")
print(original_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create the Bundle Structure
# MAGIC
# MAGIC Let's create a proper bundle structure:
# MAGIC
# MAGIC ```bash
# MAGIC mkdir daily-sales-report-bundle
# MAGIC cd daily-sales-report-bundle
# MAGIC mkdir -p src/notebooks
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Refactor the Notebook
# MAGIC
# MAGIC Create an improved version with parameterization:

# COMMAND ----------

refactored_notebook = '''# Databricks notebook source
%md
# Daily Sales Report

Generates daily sales summary by store and region

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Define parameters with defaults
dbutils.widgets.text("catalog", "dev_catalog", "Catalog")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("report_date", "", "Report Date (YYYY-MM-DD)")

# Get parameter values
catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")
report_date = dbutils.widgets.get("report_date")

print(f"Running in environment: {environment}")
print(f"Using catalog: {catalog}")
print(f"Report date: {report_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Sales Data

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, current_date, lit
from datetime import datetime, timedelta

# If no date specified, use yesterday
if not report_date:
    report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"No date specified, using yesterday: {report_date}")

try:
    # Read sales data
    sales_df = spark.read.table(f"{catalog}.sales.transactions")
    print(f"Read {sales_df.count()} total transactions")
    
    # Filter for report date
    daily_sales = sales_df.filter(sales_df.transaction_date == report_date)
    print(f"Filtered to {daily_sales.count()} transactions for {report_date}")
    
except Exception as e:
    print(f"Error reading sales data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Metrics

# COMMAND ----------

try:
    # Calculate summary metrics
    sales_summary = daily_sales.groupBy("store_id", "region").agg(
        sum("amount").alias("total_sales"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_transaction_value")
    )
    
    # Add metadata
    sales_summary = sales_summary \\
        .withColumn("report_date", lit(report_date)) \\
        .withColumn("environment", lit(environment)) \\
        .withColumn("generated_at", current_date())
    
    print(f"Generated summary for {sales_summary.count()} stores")
    display(sales_summary)
    
except Exception as e:
    print(f"Error calculating metrics: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results

# COMMAND ----------

try:
    # Write to report table
    output_table = f"{catalog}.sales.daily_sales_summary"
    
    sales_summary.write \\
        .format("delta") \\
        .mode("append") \\
        .option("mergeSchema", "true") \\
        .saveAsTable(output_table)
    
    print(f"✅ Report successfully written to {output_table}")
    print(f"Report date: {report_date}")
    print(f"Total stores: {sales_summary.count()}")
    print(f"Environment: {environment}")
    
except Exception as e:
    print(f"❌ Error writing results: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Report generation completed successfully!
# MAGIC '''
# MAGIC
# MAGIC print("Refactored Notebook:")
# MAGIC print("=" * 60)
# MAGIC print("✅ Parameterized catalog, environment, date")
# MAGIC print("✅ Error handling added")
# MAGIC print("✅ Better logging and output")
# MAGIC print("✅ Metadata tracking")
# MAGIC print("✅ Flexible date handling")
# MAGIC print("\n" + "=" * 60)
# MAGIC print("\nSave as: src/notebooks/daily_sales_report.py")
# MAGIC print(refactored_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create the Bundle Configuration
# MAGIC
# MAGIC Now let's create the `databricks.yml` file:

# COMMAND ----------

bundle_config = '''bundle:
  name: daily-sales-report

variables:
  catalog:
    description: "Unity Catalog name"
    default: "dev_catalog"
  
  notification_email:
    description: "Email for job notifications"
    default: "data-team@company.com"

targets:
  # Development environment
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "dev_catalog"
      notification_email: "dev-team@company.com"
  
  # Production environment
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "prod_catalog"
      notification_email: "sales-team@company.com"

resources:
  jobs:
    daily_sales_report:
      name: "Daily Sales Report - ${bundle.target}"
      
      # Schedule: Daily at 6 AM Pacific Time
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "America/Los_Angeles"
        pause_status: PAUSED  # Start paused, unpause after testing
      
      # Email notifications
      email_notifications:
        on_success:
          - ${var.notification_email}
        on_failure:
          - ${var.notification_email}
          - oncall@company.com
      
      # Job settings
      max_concurrent_runs: 1
      timeout_seconds: 3600
      
      tasks:
        - task_key: generate_sales_report
          notebook_task:
            notebook_path: ./src/notebooks/daily_sales_report.py
            base_parameters:
              catalog: ${var.catalog}
              environment: ${bundle.target}
              # Use job start time as report date
              report_date: "{{job.start_time.format('yyyy-MM-dd')}}"
          
          # Define cluster instead of using existing
          new_cluster:
            num_workers: 2
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            
            # Auto-termination for cost savings
            autotermination_minutes: 30
            
            # Spark configuration
            spark_conf:
              "spark.databricks.delta.preview.enabled": "true"
              "spark.sql.adaptive.enabled": "true"
      
      # Job-level permissions
      permissions:
        - level: CAN_VIEW
          group_name: "sales-team"
        - level: CAN_MANAGE_RUN
          group_name: "data-engineers"
        - level: IS_OWNER
          user_name: "${workspace.current_user.userName}"
'''

print("Bundle Configuration (databricks.yml):")
print("=" * 60)
print(bundle_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate the Bundle
# MAGIC
# MAGIC ```bash
# MAGIC # Validate the bundle configuration
# MAGIC databricks bundle validate -t dev
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Validation OK!
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Deploy to Development
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy to dev environment
# MAGIC databricks bundle deploy -t dev
# MAGIC ```
# MAGIC
# MAGIC **What happens:**
# MAGIC 1. Notebook uploaded to workspace
# MAGIC 2. Job created with name "Daily Sales Report - dev"
# MAGIC 3. Job starts in PAUSED state
# MAGIC 4. Permissions applied

# COMMAND ----------

# Example deployment output
deployment_output = '''
Uploading bundle files to /Workspace/Users/user@company.com/.bundle/daily-sales-report/dev...
Uploading src/notebooks/daily_sales_report.py...

Deploying resources...
  Creating job daily_sales_report...
  Job URL: https://your-workspace.cloud.databricks.com/#job/987654

Deployment complete!

Summary:
  ✅ 1 job created
  ✅ 1 notebook uploaded
  ✅ Permissions configured
'''

print("Deployment Output:")
print(deployment_output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Test the Migrated Job
# MAGIC
# MAGIC Before running on schedule, test manually:
# MAGIC
# MAGIC ```bash
# MAGIC # Run the job once
# MAGIC databricks bundle run daily_sales_report -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Compare Results
# MAGIC
# MAGIC Compare the output of the new job with the original:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the results from new job
# MAGIC SELECT * 
# MAGIC FROM dev_catalog.sales.daily_sales_summary 
# MAGIC WHERE environment = 'dev'
# MAGIC ORDER BY report_date DESC, total_sales DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Side-by-Side Validation
# MAGIC
# MAGIC Run both old and new jobs for a few days to validate:

# COMMAND ----------

validation_query = '''
-- Compare outputs between old and new jobs
SELECT 
  a.store_id,
  a.region,
  a.total_sales as new_job_sales,
  b.total_sales as old_job_sales,
  abs(a.total_sales - b.total_sales) as difference,
  CASE 
    WHEN abs(a.total_sales - b.total_sales) < 0.01 THEN '✅ Match'
    ELSE '❌ Mismatch'
  END as validation_status
FROM dev_catalog.sales.daily_sales_summary a
LEFT JOIN prod_catalog.sales.daily_sales_summary b
  ON a.store_id = b.store_id 
  AND a.region = b.region
  AND a.report_date = b.report_date
WHERE a.environment = 'dev'
  AND a.report_date = current_date() - 1
ORDER BY difference DESC;
'''

print("Validation Query:")
print(validation_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Deploy to Production
# MAGIC
# MAGIC Once validated in dev, deploy to production:
# MAGIC
# MAGIC ```bash
# MAGIC # Validate for production
# MAGIC databricks bundle validate -t prod
# MAGIC
# MAGIC # Deploy to production
# MAGIC databricks bundle deploy -t prod
# MAGIC
# MAGIC # Unpause the schedule
# MAGIC databricks jobs update --job-id <prod-job-id> --pause-status UNPAUSED
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Monitor and Cutover
# MAGIC
# MAGIC ### Cutover Plan
# MAGIC
# MAGIC 1. **Week 1**: Run both jobs in parallel
# MAGIC 2. **Week 2**: Validate results match daily
# MAGIC 3. **Week 3**: Switch primary to new job
# MAGIC 4. **Week 4**: Disable old job
# MAGIC 5. **Week 5**: Delete old job after final validation

# COMMAND ----------

monitoring_checklist = '''
# Monitoring Checklist

Daily Tasks:
[ ] Check job run status
[ ] Compare outputs between old and new jobs
[ ] Review email notifications
[ ] Check for errors or warnings
[ ] Validate data quality

Weekly Tasks:
[ ] Review performance metrics
[ ] Check cluster costs
[ ] Validate with stakeholders
[ ] Update documentation
[ ] Review and address feedback

Cutover Decision Points:
[ ] 100% output match for 5 consecutive days
[ ] No errors or warnings
[ ] Stakeholder approval
[ ] Rollback plan documented
[ ] On-call team briefed
'''

print(monitoring_checklist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Decommission Old Job
# MAGIC
# MAGIC After successful migration and validation:
# MAGIC
# MAGIC ```bash
# MAGIC # Pause the old job
# MAGIC databricks jobs update --job-id 789012 --pause-status PAUSED
# MAGIC
# MAGIC # Wait 1-2 weeks for any issues
# MAGIC
# MAGIC # Delete the old job
# MAGIC databricks jobs delete --job-id 789012
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Summary
# MAGIC
# MAGIC ### What We Changed
# MAGIC
# MAGIC | Aspect | Before | After |
# MAGIC |--------|--------|-------|
# MAGIC | **Location** | User folder | Bundle structure |
# MAGIC | **Parameterization** | Hardcoded | Fully parameterized |
# MAGIC | **Cluster** | Existing cluster ID | Defined in bundle |
# MAGIC | **Environment** | Single (prod) | Multi-environment |
# MAGIC | **Version Control** | None | Git repository |
# MAGIC | **Error Handling** | Minimal | Comprehensive |
# MAGIC | **Monitoring** | Basic | Enhanced logging |
# MAGIC | **Deployment** | Manual | Automated |
# MAGIC
# MAGIC ### Benefits Gained
# MAGIC
# MAGIC ✅ **Version Control**: All changes tracked in Git
# MAGIC ✅ **Consistency**: Same job definition across environments
# MAGIC ✅ **Testability**: Easy to test in dev before prod
# MAGIC ✅ **Maintainability**: Clear structure and documentation
# MAGIC ✅ **Scalability**: Easy to add more jobs to bundle
# MAGIC ✅ **Collaboration**: Team can contribute via PRs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Migration Issues and Solutions
# MAGIC
# MAGIC ### Issue 1: Notebook Import Paths
# MAGIC **Problem**: Notebooks use absolute paths to import other notebooks
# MAGIC ```python
# MAGIC # ❌ Before
# MAGIC %run /Users/user@company.com/Utils/helper_functions
# MAGIC ```
# MAGIC
# MAGIC **Solution**: Use relative paths
# MAGIC ```python
# MAGIC # ✅ After
# MAGIC %run ./utils/helper_functions
# MAGIC ```
# MAGIC
# MAGIC ### Issue 2: Library Dependencies
# MAGIC **Problem**: Libraries installed ad-hoc on cluster
# MAGIC
# MAGIC **Solution**: Define in bundle
# MAGIC ```yaml
# MAGIC tasks:
# MAGIC   - libraries:
# MAGIC       - pypi:
# MAGIC           package: pandas==2.0.0
# MAGIC       - pypi:
# MAGIC           package: scikit-learn>=1.0.0
# MAGIC ```
# MAGIC
# MAGIC ### Issue 3: Data Paths
# MAGIC **Problem**: Hardcoded mount points or DBFS paths
# MAGIC
# MAGIC **Solution**: Parameterize and use Unity Catalog
# MAGIC ```python
# MAGIC # ❌ Before
# MAGIC df = spark.read.csv("/mnt/raw/data.csv")
# MAGIC
# MAGIC # ✅ After
# MAGIC input_path = dbutils.widgets.get("input_path")
# MAGIC df = spark.read.csv(input_path)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Demonstrated
# MAGIC
# MAGIC 1. ✅ **Exported original job** for reference
# MAGIC 2. ✅ **Analyzed before migrating** to identify issues
# MAGIC 3. ✅ **Refactored notebook** with parameterization
# MAGIC 4. ✅ **Created proper bundle structure**
# MAGIC 5. ✅ **Tested in development first**
# MAGIC 6. ✅ **Validated results** against original
# MAGIC 7. ✅ **Parallel runs** during transition
# MAGIC 8. ✅ **Gradual cutover** to minimize risk
# MAGIC 9. ✅ **Monitored closely** after migration
# MAGIC 10. ✅ **Documented everything** for future reference

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Now it's your turn! In the lab, you'll:
# MAGIC - Migrate your own job to DABs
# MAGIC - Practice the complete migration workflow
# MAGIC - Handle real-world migration challenges
# MAGIC
# MAGIC **Continue to:** `06_Lab_Migrating_Existing_Jobs.py`
