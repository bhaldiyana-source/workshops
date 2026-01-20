# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1 Demo: Creating Your First Bundle
# MAGIC
# MAGIC ## Overview
# MAGIC In this demo, we'll walk through creating a simple Databricks Asset Bundle from scratch. You'll see:
# MAGIC - How to initialize a new bundle
# MAGIC - The structure of a bundle configuration file
# MAGIC - How to deploy and run your first bundle
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks CLI installed and authenticated
# MAGIC - Access to a Databricks workspace
# MAGIC - Basic command line knowledge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set Up Your Project Directory
# MAGIC
# MAGIC First, let's create a directory for our bundle project.
# MAGIC
# MAGIC ```bash
# MAGIC # Create project directory
# MAGIC mkdir my-first-bundle
# MAGIC cd my-first-bundle
# MAGIC
# MAGIC # Initialize git repository (optional but recommended)
# MAGIC git init
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Initialize the Bundle
# MAGIC
# MAGIC Let's see what templates are available and initialize our bundle.
# MAGIC
# MAGIC ```bash
# MAGIC # List available templates
# MAGIC databricks bundle init --list
# MAGIC
# MAGIC # Initialize with default Python template
# MAGIC databricks bundle init default-python
# MAGIC ```
# MAGIC
# MAGIC This will create:
# MAGIC - `databricks.yml` - Main configuration file
# MAGIC - `src/` - Directory for source code
# MAGIC - `.gitignore` - Git ignore file
# MAGIC - README.md - Documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Explore the Generated Bundle Structure
# MAGIC
# MAGIC After initialization, you'll have a structure like this:
# MAGIC
# MAGIC ```
# MAGIC my-first-bundle/
# MAGIC ├── databricks.yml          # Bundle configuration
# MAGIC ├── README.md               # Documentation
# MAGIC ├── .gitignore              # Git ignore rules
# MAGIC └── src/
# MAGIC     └── notebook.py         # Sample notebook
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Examine the databricks.yml File
# MAGIC
# MAGIC The generated `databricks.yml` will look similar to this:

# COMMAND ----------

# Display the structure of a typical databricks.yml file
sample_bundle_config = """
bundle:
  name: my-first-bundle

include:
  - resources/*.yml

targets:
  # Development environment
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
    
  # Production environment  
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com

resources:
  jobs:
    my_first_job:
      name: "My First Job - ${bundle.target}"
      
      tasks:
        - task_key: main_task
          notebook_task:
            notebook_path: ./src/notebook.py
            source: WORKSPACE
          
          new_cluster:
            num_workers: 1
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
"""

print(sample_bundle_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create a Simple Notebook
# MAGIC
# MAGIC Let's create a simple notebook that our job will run:

# COMMAND ----------

# MAGIC %md
# MAGIC **Create file: `src/notebook.py`**
# MAGIC
# MAGIC ```python
# MAGIC # Databricks notebook source
# MAGIC print("Hello from my first DABs job!")
# MAGIC
# MAGIC # Get parameters
# MAGIC dbutils.widgets.text("environment", "dev", "Environment")
# MAGIC env = dbutils.widgets.get("environment")
# MAGIC
# MAGIC print(f"Running in environment: {env}")
# MAGIC
# MAGIC # Simple data transformation
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC # Create sample data
# MAGIC data = [
# MAGIC     ("Alice", 34, "Engineering"),
# MAGIC     ("Bob", 45, "Sales"),
# MAGIC     ("Charlie", 28, "Engineering"),
# MAGIC     ("Diana", 31, "Marketing")
# MAGIC ]
# MAGIC
# MAGIC df = spark.createDataFrame(data, ["name", "age", "department"])
# MAGIC
# MAGIC # Show the data
# MAGIC display(df)
# MAGIC
# MAGIC # Simple aggregation
# MAGIC dept_summary = df.groupBy("department").count()
# MAGIC display(dept_summary)
# MAGIC
# MAGIC print("Job completed successfully!")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Customize Your Bundle Configuration
# MAGIC
# MAGIC Let's enhance the `databricks.yml` with more configuration:

# COMMAND ----------

enhanced_bundle_config = """
bundle:
  name: my-first-bundle

# Define variables that can be used throughout the bundle
variables:
  catalog:
    description: "Unity Catalog name"
    default: "dev_catalog"
  
  warehouse_id:
    description: "SQL Warehouse ID"
    default: ""

# Include additional YAML files
include:
  - resources/*.yml

targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "dev_catalog"
    
  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    
    variables:
      catalog: "prod_catalog"
    
    # Production-specific permissions
    permissions:
      - level: CAN_VIEW
        group_name: "data-team"

resources:
  jobs:
    my_first_job:
      name: "My First Job - ${bundle.target}"
      
      # Job schedule (runs daily at 2 AM)
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "UTC"
        pause_status: PAUSED
      
      # Email notifications
      email_notifications:
        on_failure:
          - your-email@company.com
      
      tasks:
        - task_key: main_task
          notebook_task:
            notebook_path: ./src/notebook.py
            base_parameters:
              environment: ${bundle.target}
              catalog: ${var.catalog}
          
          new_cluster:
            num_workers: ${var.num_workers}
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            
            # Auto-termination
            autotermination_minutes: 30
            
            # Spark configuration
            spark_conf:
              "spark.databricks.delta.preview.enabled": "true"
      
      # Job-level permissions
      permissions:
        - level: CAN_VIEW
          group_name: "data-team"
        - level: CAN_MANAGE_RUN
          user_name: "admin@company.com"
      
      # Maximum concurrent runs
      max_concurrent_runs: 1
      
      # Job timeout
      timeout_seconds: 3600
"""

print(enhanced_bundle_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate Your Bundle
# MAGIC
# MAGIC Before deploying, always validate your bundle configuration:
# MAGIC
# MAGIC ```bash
# MAGIC # Basic validation
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # Validate with verbose output
# MAGIC databricks bundle validate --verbose
# MAGIC
# MAGIC # Validate for specific target
# MAGIC databricks bundle validate -t dev
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Validation OK!
# MAGIC ```
# MAGIC
# MAGIC **Common Validation Errors:**
# MAGIC - YAML syntax errors (indentation, missing colons)
# MAGIC - Invalid resource references
# MAGIC - Missing required fields
# MAGIC - Invalid variable interpolation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Deploy Your Bundle
# MAGIC
# MAGIC Now let's deploy the bundle to the development environment:
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy to dev environment
# MAGIC databricks bundle deploy -t dev
# MAGIC ```
# MAGIC
# MAGIC **What happens during deployment:**
# MAGIC 1. CLI validates the bundle configuration
# MAGIC 2. Source files are uploaded to workspace
# MAGIC 3. Resources (jobs, pipelines, etc.) are created or updated
# MAGIC 4. Permissions are applied
# MAGIC 5. Deployment summary is displayed
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Uploading bundle files to /Workspace/Users/your.email@company.com/.bundle/my-first-bundle/dev...
# MAGIC Deploying resources...
# MAGIC   Creating job my_first_job...
# MAGIC Deployment complete!
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: View Your Deployed Resources
# MAGIC
# MAGIC After deployment, you can view your resources in the Databricks workspace:
# MAGIC
# MAGIC ```bash
# MAGIC # List deployed resources
# MAGIC databricks bundle summary
# MAGIC
# MAGIC # Get detailed deployment info
# MAGIC databricks bundle list
# MAGIC ```
# MAGIC
# MAGIC You can also check in the Databricks UI:
# MAGIC 1. Go to **Workflows** in the left sidebar
# MAGIC 2. Find your job: "My First Job - dev"
# MAGIC 3. Click on it to see the configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Run Your Job
# MAGIC
# MAGIC Now let's run the deployed job:
# MAGIC
# MAGIC ```bash
# MAGIC # Run the job
# MAGIC databricks bundle run my_first_job -t dev
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC Run started: https://your-workspace.cloud.databricks.com/#job/123456/run/1
# MAGIC Waiting for run to complete...
# MAGIC Run completed successfully!
# MAGIC ```
# MAGIC
# MAGIC **Alternative: Run from UI**
# MAGIC 1. Navigate to the job in Databricks UI
# MAGIC 2. Click "Run now"
# MAGIC 3. Monitor the run progress

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Watch the Run in Real-Time
# MAGIC
# MAGIC You can monitor the job run directly from CLI:

# COMMAND ----------

# Example of what you'd see in the job run output
example_job_output = """
========================================
Job Run Details
========================================
Job Name: My First Job - dev
Run ID: 123456
Status: RUNNING

Task: main_task
Status: RUNNING
Start Time: 2026-01-11 14:30:00 UTC

========================================
Task Output:
========================================
Hello from my first DABs job!
Running in environment: dev

+-------+---+------------+
|   name|age|  department|
+-------+---+------------+
|  Alice| 34| Engineering|
|    Bob| 45|       Sales|
|Charlie| 28| Engineering|
|  Diana| 31|   Marketing|
+-------+---+------------+

+------------+-----+
|  department|count|
+------------+-----+
| Engineering|    2|
|       Sales|    1|
|   Marketing|    1|
+------------+-----+

Job completed successfully!

========================================
Task Status: SUCCESS
Duration: 45 seconds
========================================
"""

print(example_job_output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Make Changes and Redeploy
# MAGIC
# MAGIC Let's modify the notebook and redeploy:
# MAGIC
# MAGIC 1. Edit `src/notebook.py` to add more functionality
# MAGIC 2. Save your changes
# MAGIC 3. Redeploy the bundle
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy with auto-approval
# MAGIC databricks bundle deploy -t dev --auto-approve
# MAGIC
# MAGIC # Run the updated job
# MAGIC databricks bundle run my_first_job -t dev
# MAGIC ```
# MAGIC
# MAGIC The bundle will update the existing resources with your changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Deploy to Production
# MAGIC
# MAGIC When ready, deploy to production:
# MAGIC
# MAGIC ```bash
# MAGIC # Validate for production
# MAGIC databricks bundle validate -t prod
# MAGIC
# MAGIC # Deploy to production
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC Notice how the same code is deployed but with production-specific configurations:
# MAGIC - Different workspace
# MAGIC - Production catalog
# MAGIC - Different permissions
# MAGIC - Same job logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Clean Up (Optional)
# MAGIC
# MAGIC If you want to remove the deployed resources:
# MAGIC
# MAGIC ```bash
# MAGIC # Destroy dev environment resources
# MAGIC databricks bundle destroy -t dev
# MAGIC
# MAGIC # You'll be prompted to confirm
# MAGIC ```
# MAGIC
# MAGIC **Warning:** This will permanently delete:
# MAGIC - All jobs defined in the bundle
# MAGIC - Uploaded workspace files
# MAGIC - Any other resources created by the bundle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common Issues
# MAGIC
# MAGIC ### Issue 1: Authentication Error
# MAGIC ```
# MAGIC Error: No valid authentication found
# MAGIC ```
# MAGIC **Solution:**
# MAGIC ```bash
# MAGIC databricks auth login --host https://your-workspace.cloud.databricks.com
# MAGIC ```
# MAGIC
# MAGIC ### Issue 2: Validation Fails
# MAGIC ```
# MAGIC Error: invalid YAML syntax
# MAGIC ```
# MAGIC **Solution:**
# MAGIC - Check indentation (use spaces, not tabs)
# MAGIC - Verify all colons have space after them
# MAGIC - Check for missing quotes around special characters
# MAGIC
# MAGIC ### Issue 3: Permission Denied
# MAGIC ```
# MAGIC Error: User does not have permission to create job
# MAGIC ```
# MAGIC **Solution:**
# MAGIC - Verify your workspace permissions
# MAGIC - Contact workspace admin to grant necessary permissions
# MAGIC
# MAGIC ### Issue 4: Resource Already Exists
# MAGIC ```
# MAGIC Error: Job with name already exists
# MAGIC ```
# MAGIC **Solution:**
# MAGIC - Use unique names with bundle.target: `"Job - ${bundle.target}"`
# MAGIC - Or delete the existing resource first

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Demonstrated
# MAGIC
# MAGIC ✅ **Variable Interpolation**: Used `${bundle.target}` for environment-specific naming
# MAGIC
# MAGIC ✅ **Environment Separation**: Separate configs for dev and prod
# MAGIC
# MAGIC ✅ **Validation Before Deployment**: Always validate before deploying
# MAGIC
# MAGIC ✅ **Version Control**: Initialize git repository for tracking changes
# MAGIC
# MAGIC ✅ **Clear Naming**: Descriptive names for bundle and resources
# MAGIC
# MAGIC ✅ **Documentation**: README and comments in configuration
# MAGIC
# MAGIC ✅ **Permissions Management**: Defined proper access controls
# MAGIC
# MAGIC ✅ **Auto-termination**: Clusters automatically shut down to save costs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Example: Full Workflow
# MAGIC
# MAGIC Here's the complete workflow in one sequence:

# COMMAND ----------

complete_workflow = """
# 1. Create project
mkdir my-first-bundle && cd my-first-bundle

# 2. Initialize bundle
databricks bundle init default-python

# 3. Create your notebook (src/notebook.py)
# [Edit the file with your code]

# 4. Customize databricks.yml
# [Edit with your configurations]

# 5. Initialize git
git init
git add .
git commit -m "Initial bundle setup"

# 6. Validate
databricks bundle validate -t dev

# 7. Deploy to dev
databricks bundle deploy -t dev

# 8. Run the job
databricks bundle run my_first_job -t dev

# 9. Make changes
# [Edit your code]

# 10. Redeploy
databricks bundle deploy -t dev --auto-approve

# 11. When ready, deploy to prod
databricks bundle validate -t prod
databricks bundle deploy -t prod

# 12. Monitor in UI or CLI
databricks workspace list-runs my_first_job
"""

print(complete_workflow)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this demo, we:
# MAGIC
# MAGIC 1. ✅ Created a new bundle from scratch
# MAGIC 2. ✅ Explored the bundle directory structure
# MAGIC 3. ✅ Wrote a simple notebook
# MAGIC 4. ✅ Configured the databricks.yml file
# MAGIC 5. ✅ Validated the bundle configuration
# MAGIC 6. ✅ Deployed to development environment
# MAGIC 7. ✅ Ran the deployed job
# MAGIC 8. ✅ Learned to make changes and redeploy
# MAGIC 9. ✅ Saw how to promote to production
# MAGIC 10. ✅ Learned troubleshooting tips

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Now it's your turn! In the lab exercise, you'll:
# MAGIC - Create your own bundle from scratch
# MAGIC - Define multiple resources
# MAGIC - Deploy to your workspace
# MAGIC - Practice the full deployment workflow
# MAGIC
# MAGIC **Ready?** Move on to: `03_Lab_Creating_Your_First_Bundle.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Bundle Templates Gallery](https://github.com/databricks/bundle-templates)
# MAGIC - [CLI Command Reference](https://docs.databricks.com/dev-tools/cli/)
# MAGIC - [Bundle Configuration Schema](https://docs.databricks.com/dev-tools/bundles/settings.html)
# MAGIC - [Example Bundles](https://github.com/databricks/bundle-examples)
