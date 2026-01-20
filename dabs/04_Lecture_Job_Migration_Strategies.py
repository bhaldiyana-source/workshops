# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2: Job Migration Strategies
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand different migration patterns for moving jobs to DABs
# MAGIC - Analyze existing workspace resources for migration
# MAGIC - Plan a phased migration approach
# MAGIC - Handle dependencies and libraries during migration
# MAGIC - Choose the right migration strategy for your use case

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Migrate to DABs?
# MAGIC
# MAGIC ### Current Challenges with Traditional Job Management
# MAGIC
# MAGIC - **Manual Configuration**: Jobs created through UI are error-prone
# MAGIC - **No Version Control**: Changes aren't tracked, hard to rollback
# MAGIC - **Environment Inconsistency**: Dev and prod configs drift over time
# MAGIC - **Difficult Collaboration**: Hard for teams to work together
# MAGIC - **No Testing**: Can't validate changes before deployment
# MAGIC - **Manual Promotion**: Time-consuming and risky environment promotions
# MAGIC
# MAGIC ### Benefits of Migration
# MAGIC
# MAGIC - ✅ **Infrastructure as Code**: All configs in version control
# MAGIC - ✅ **Automated Deployments**: CI/CD pipeline integration
# MAGIC - ✅ **Environment Parity**: Same code across all environments
# MAGIC - ✅ **Team Collaboration**: Code reviews and pull requests
# MAGIC - ✅ **Rapid Rollback**: Revert to previous versions easily
# MAGIC - ✅ **Consistency**: Templates and standards across projects

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Strategy Overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              Migration Strategy Selection                │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                            │
# MAGIC            ┌───────────────┼───────────────┐
# MAGIC            │               │               │
# MAGIC     ┌──────▼─────┐  ┌─────▼──────┐  ┌────▼──────┐
# MAGIC     │  Lift and  │  │  Refactor  │  │Incremental│
# MAGIC     │   Shift    │  │    and     │  │ Migration │
# MAGIC     │            │  │  Optimize  │  │           │
# MAGIC     └──────┬─────┘  └─────┬──────┘  └────┬──────┘
# MAGIC            │               │               │
# MAGIC     Quick  │        Modern │        Safe   │
# MAGIC     Move   │        Best   │        Gradual│
# MAGIC            │        Practices       │
# MAGIC            │               │               │
# MAGIC            └───────────────┼───────────────┘
# MAGIC                            │
# MAGIC                   ┌────────▼────────┐
# MAGIC                   │  DABs-Managed   │
# MAGIC                   │    Resources    │
# MAGIC                   └─────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Pattern 1: Lift and Shift
# MAGIC
# MAGIC ### Description
# MAGIC Migrate existing jobs "as-is" with minimal changes to the logic.
# MAGIC
# MAGIC ### When to Use
# MAGIC - **Quick wins needed**: Fast migration to establish DABs presence
# MAGIC - **Stable jobs**: Jobs that work well and don't need refactoring
# MAGIC - **Initial migration**: First step before optimization
# MAGIC - **Time constraints**: Limited time for refactoring
# MAGIC
# MAGIC ### Process
# MAGIC 1. Export existing job configuration (JSON)
# MAGIC 2. Convert JSON to YAML format
# MAGIC 3. Add to bundle structure
# MAGIC 4. Deploy and validate
# MAGIC 5. Plan optimization for later
# MAGIC
# MAGIC ### Advantages
# MAGIC - ✅ Fast migration
# MAGIC - ✅ Minimal risk
# MAGIC - ✅ Low effort
# MAGIC - ✅ Immediate DABs benefits
# MAGIC
# MAGIC ### Disadvantages
# MAGIC - ⚠️ May not follow best practices
# MAGIC - ⚠️ Technical debt carried forward
# MAGIC - ⚠️ Configuration may not be optimal
# MAGIC - ⚠️ Might need future refactoring

# COMMAND ----------

# Example: Lift and Shift Migration
# Original job (exported JSON)
original_job_json = '''{
  "job_id": 123456,
  "settings": {
    "name": "Daily ETL Job",
    "tasks": [{
      "task_key": "etl_task",
      "notebook_task": {
        "notebook_path": "/Users/user@company.com/ETL/daily_etl"
      },
      "existing_cluster_id": "0301-121415-abcd1234"
    }],
    "schedule": {
      "quartz_cron_expression": "0 0 2 * * ?",
      "timezone_id": "UTC"
    }
  }
}'''

# Converted to DABs (YAML)
dabs_lift_shift = '''resources:
  jobs:
    daily_etl_job:
      name: "Daily ETL Job - ${bundle.target}"
      
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "UTC"
      
      tasks:
        - task_key: etl_task
          notebook_task:
            notebook_path: ./notebooks/daily_etl.py
          
          # Changed from existing_cluster_id to new_cluster
          new_cluster:
            num_workers: 2
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
'''

print("Original Job (JSON):")
print(original_job_json)
print("\n" + "="*60 + "\n")
print("Migrated to DABs (YAML):")
print(dabs_lift_shift)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Pattern 2: Refactor and Optimize
# MAGIC
# MAGIC ### Description
# MAGIC Migrate while modernizing code, structure, and configurations.
# MAGIC
# MAGIC ### When to Use
# MAGIC - **Technical debt exists**: Jobs need improvement
# MAGIC - **Performance issues**: Opportunity to optimize
# MAGIC - **Best practices**: Want to adopt modern patterns
# MAGIC - **Major changes planned**: Good time for refactoring
# MAGIC
# MAGIC ### Process
# MAGIC 1. Analyze existing job
# MAGIC 2. Identify improvement opportunities
# MAGIC 3. Redesign with best practices
# MAGIC 4. Implement in DABs format
# MAGIC 5. Test thoroughly
# MAGIC 6. Deploy and monitor
# MAGIC
# MAGIC ### Improvements to Consider
# MAGIC - **Modularization**: Break monolithic notebooks into modules
# MAGIC - **Error Handling**: Add proper exception handling
# MAGIC - **Parameterization**: Make jobs more configurable
# MAGIC - **Data Quality**: Add validation and quality checks
# MAGIC - **Monitoring**: Add logging and observability
# MAGIC - **Testing**: Implement unit and integration tests

# COMMAND ----------

# Example: Refactor and Optimize Migration

# BEFORE: Monolithic notebook
before_refactor = '''# Databricks notebook source
# Everything in one notebook

# Read data
df = spark.read.csv("/data/input.csv")

# Transform (no error handling)
df = df.filter(df.amount > 0)
df = df.withColumn("processed_date", current_date())

# Write (hardcoded paths)
df.write.mode("overwrite").saveAsTable("my_catalog.my_schema.output")

print("Done!")
'''

# AFTER: Modular, parameterized, with error handling
after_refactor_config = '''# Better structure with multiple notebooks and configuration

# databricks.yml
resources:
  jobs:
    optimized_etl:
      name: "Optimized ETL - ${bundle.target}"
      
      tasks:
        # Task 1: Data validation
        - task_key: validate_input
          notebook_task:
            notebook_path: ./src/validation/validate_input.py
            base_parameters:
              catalog: ${var.catalog}
              input_path: ${var.input_path}
              date: "{{job.start_time}}"
          new_cluster:
            num_workers: 1
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
        
        # Task 2: Transform data
        - task_key: transform
          depends_on:
            - task_key: validate_input
          notebook_task:
            notebook_path: ./src/transform/transform.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            num_workers: 4
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
        
        # Task 3: Data quality checks
        - task_key: quality_check
          depends_on:
            - task_key: transform
          notebook_task:
            notebook_path: ./src/quality/quality_check.py
            base_parameters:
              catalog: ${var.catalog}
          new_cluster:
            num_workers: 1
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
'''

print("BEFORE - Monolithic Approach:")
print(before_refactor)
print("\n" + "="*60 + "\n")
print("AFTER - Modular, Optimized Approach:")
print(after_refactor_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Pattern 3: Incremental Migration
# MAGIC
# MAGIC ### Description
# MAGIC Gradual migration of jobs to DABs while maintaining existing jobs.
# MAGIC
# MAGIC ### When to Use
# MAGIC - **Large-scale migration**: Many jobs to migrate
# MAGIC - **Risk aversion**: Need to minimize disruption
# MAGIC - **Learning phase**: Team is learning DABs
# MAGIC - **Complex dependencies**: Jobs have intricate relationships
# MAGIC
# MAGIC ### Process
# MAGIC 1. **Phase 1**: Migrate non-critical dev jobs
# MAGIC 2. **Phase 2**: Migrate test/staging environments
# MAGIC 3. **Phase 3**: Migrate production non-critical jobs
# MAGIC 4. **Phase 4**: Migrate production critical jobs
# MAGIC 5. **Phase 5**: Decommission old jobs
# MAGIC
# MAGIC ### Risk Mitigation
# MAGIC - Run both versions in parallel initially
# MAGIC - Compare outputs for validation
# MAGIC - Gradual traffic shifting
# MAGIC - Easy rollback capability

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Pattern 4: Greenfield with DABs
# MAGIC
# MAGIC ### Description
# MAGIC Start new projects with DABs from day one.
# MAGIC
# MAGIC ### When to Use
# MAGIC - **New projects**: Starting fresh
# MAGIC - **No legacy**: No existing jobs to migrate
# MAGIC - **Best practices**: Want to do it right from start
# MAGIC - **Reference implementation**: Creating templates for team
# MAGIC
# MAGIC ### Advantages
# MAGIC - No technical debt
# MAGIC - Modern architecture from start
# MAGIC - Sets pattern for future projects
# MAGIC - Easier to maintain

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing Existing Jobs for Migration
# MAGIC
# MAGIC ### Step 1: Inventory Your Jobs
# MAGIC
# MAGIC Create an inventory of all jobs in your workspace:

# COMMAND ----------

# Example: Job inventory script
job_inventory_script = '''# List all jobs in workspace
databricks jobs list --output json > jobs_inventory.json

# Analyze the inventory
import json

with open("jobs_inventory.json") as f:
    jobs = json.load(f)

print(f"Total jobs: {len(jobs)}")

# Categorize by complexity
simple_jobs = []
medium_jobs = []
complex_jobs = []

for job in jobs:
    task_count = len(job.get("settings", {}).get("tasks", []))
    
    if task_count == 1:
        simple_jobs.append(job)
    elif task_count <= 5:
        medium_jobs.append(job)
    else:
        complex_jobs.append(job)

print(f"Simple jobs (1 task): {len(simple_jobs)}")
print(f"Medium jobs (2-5 tasks): {len(medium_jobs)}")
print(f"Complex jobs (6+ tasks): {len(complex_jobs)}")
'''

print(job_inventory_script)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Categorize Jobs by Migration Complexity
# MAGIC
# MAGIC | Category | Characteristics | Migration Priority | Strategy |
# MAGIC |----------|----------------|-------------------|----------|
# MAGIC | **Easy** | Single task, no dependencies, dev only | High | Lift and Shift |
# MAGIC | **Medium** | Multiple tasks, some dependencies | Medium | Refactor slightly |
# MAGIC | **Complex** | Many tasks, external dependencies, critical | Low | Incremental |
# MAGIC | **Critical** | Business-critical, high frequency | Last | Careful planning |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Export Job Configurations
# MAGIC
# MAGIC Export existing job configurations for analysis:

# COMMAND ----------

export_script = '''# Export specific job configuration
databricks jobs get --job-id 123456 > job_123456.json

# Export all jobs (script)
import subprocess
import json

# Get all jobs
result = subprocess.run(
    ["databricks", "jobs", "list", "--output", "json"],
    capture_output=True,
    text=True
)
jobs = json.loads(result.stdout)

# Export each job
for job in jobs:
    job_id = job["job_id"]
    result = subprocess.run(
        ["databricks", "jobs", "get", "--job-id", str(job_id)],
        capture_output=True,
        text=True
    )
    
    with open(f"exports/job_{job_id}.json", "w") as f:
        f.write(result.stdout)
    
    print(f"Exported job {job_id}: {job['settings']['name']}")
'''

print(export_script)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Dependencies During Migration
# MAGIC
# MAGIC ### Types of Dependencies
# MAGIC
# MAGIC 1. **Library Dependencies**
# MAGIC    - Python packages (pip, conda)
# MAGIC    - JAR files
# MAGIC    - Wheel files
# MAGIC
# MAGIC 2. **Data Dependencies**
# MAGIC    - Input tables/files
# MAGIC    - External data sources
# MAGIC    - Unity Catalog objects
# MAGIC
# MAGIC 3. **Job Dependencies**
# MAGIC    - Upstream jobs
# MAGIC    - Downstream consumers
# MAGIC    - Shared resources
# MAGIC
# MAGIC 4. **Infrastructure Dependencies**
# MAGIC    - Existing clusters
# MAGIC    - Instance pools
# MAGIC    - Secrets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managing Library Dependencies in DABs

# COMMAND ----------

library_deps_example = '''# In databricks.yml

resources:
  jobs:
    my_job:
      name: "Job with Dependencies"
      
      tasks:
        - task_key: main
          notebook_task:
            notebook_path: ./src/main.py
          
          # Option 1: PyPI packages
          libraries:
            - pypi:
                package: pandas==2.0.0
            - pypi:
                package: scikit-learn>=1.0.0
          
          # Option 2: Python wheel (from workspace)
          # Build and include wheel in bundle
          libraries:
            - whl: ./dist/mypackage-0.1.0-py3-none-any.whl
          
          # Option 3: JAR files
          libraries:
            - jar: ./libs/custom-lib.jar
          
          # Option 4: Maven coordinates
          libraries:
            - maven:
                coordinates: com.example:my-lib:1.0.0
          
          new_cluster:
            num_workers: 2
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
            
            # Cluster-level libraries (available to all tasks)
            libraries:
              - pypi:
                  package: numpy==1.24.0
'''

print(library_deps_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Migration Challenges and Solutions
# MAGIC
# MAGIC ### Challenge 1: Hardcoded Paths and Values
# MAGIC
# MAGIC **Problem**: Notebooks have hardcoded paths, connection strings, etc.
# MAGIC
# MAGIC **Solution**: Use parameterization and variables

# COMMAND ----------

hardcoded_problem = '''# ❌ BEFORE: Hardcoded values
df = spark.read.csv("/mnt/raw/data/customers.csv")
df.write.saveAsTable("prod_catalog.gold.customers")
'''

parameterized_solution = '''# ✅ AFTER: Parameterized
# In notebook:
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("input_path", "/mnt/raw/data")

catalog = dbutils.widgets.get("catalog")
input_path = dbutils.widgets.get("input_path")

df = spark.read.csv(f"{input_path}/customers.csv")
df.write.saveAsTable(f"{catalog}.gold.customers")

# In databricks.yml:
# tasks:
#   - notebook_task:
#       base_parameters:
#         catalog: ${var.catalog}
#         input_path: ${var.input_path}
'''

print("PROBLEM:")
print(hardcoded_problem)
print("\n" + "="*60 + "\n")
print("SOLUTION:")
print(parameterized_solution)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 2: Shared Clusters
# MAGIC
# MAGIC **Problem**: Jobs use existing shared clusters by ID
# MAGIC
# MAGIC **Solution**: Define cluster configurations in bundle

# COMMAND ----------

cluster_migration = '''# ❌ BEFORE: Using existing cluster ID
tasks:
  - existing_cluster_id: "0301-121415-abcd1234"

# ✅ AFTER: Define cluster in bundle
tasks:
  - new_cluster:
      num_workers: 2
      spark_version: 13.3.x-scala2.12
      node_type_id: i3.xlarge
      autotermination_minutes: 30
      
      # Or use job cluster
      # Allows multiple tasks to share cluster
  - job_cluster_key: "shared_cluster"

# Define job cluster at job level
job_clusters:
  - job_cluster_key: "shared_cluster"
    new_cluster:
      num_workers: 4
      spark_version: 13.3.x-scala2.12
      node_type_id: i3.xlarge
'''

print(cluster_migration)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 3: Secret Management
# MAGIC
# MAGIC **Problem**: Secrets stored in various ways
# MAGIC
# MAGIC **Solution**: Use Databricks Secrets consistently

# COMMAND ----------

secret_management = '''# In databricks.yml
resources:
  jobs:
    my_job:
      tasks:
        - notebook_task:
            notebook_path: ./src/main.py
            # Pass secret scope and key as parameters
            base_parameters:
              db_host: "{{secrets/my_scope/db_host}}"
              db_token: "{{secrets/my_scope/db_token}}"

# In notebook:
db_host = dbutils.widgets.get("db_host")
db_token = dbutils.widgets.get("db_token")

# Use secrets
connection = create_connection(db_host, db_token)
'''

print(secret_management)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migration Planning Template
# MAGIC
# MAGIC ### Pre-Migration Checklist
# MAGIC
# MAGIC - [ ] Inventory all jobs in workspace
# MAGIC - [ ] Categorize by complexity
# MAGIC - [ ] Identify dependencies
# MAGIC - [ ] Export configurations
# MAGIC - [ ] Analyze notebooks for hardcoded values
# MAGIC - [ ] Document current behavior
# MAGIC - [ ] Set up test environment
# MAGIC - [ ] Create rollback plan
# MAGIC
# MAGIC ### Migration Phases
# MAGIC
# MAGIC **Phase 1: Preparation (Week 1)**
# MAGIC - Set up DABs environment
# MAGIC - Train team on DABs
# MAGIC - Create templates and standards
# MAGIC - Select pilot jobs
# MAGIC
# MAGIC **Phase 2: Pilot (Week 2-3)**
# MAGIC - Migrate 3-5 simple jobs
# MAGIC - Test thoroughly
# MAGIC - Document learnings
# MAGIC - Refine process
# MAGIC
# MAGIC **Phase 3: Scale (Week 4-8)**
# MAGIC - Migrate remaining non-critical jobs
# MAGIC - Establish CI/CD pipelines
# MAGIC - Monitor and adjust
# MAGIC
# MAGIC **Phase 4: Critical Jobs (Week 9-12)**
# MAGIC - Migrate critical production jobs
# MAGIC - Extensive testing
# MAGIC - Parallel runs for validation
# MAGIC - Gradual cutover
# MAGIC
# MAGIC **Phase 5: Cleanup (Week 13+)**
# MAGIC - Decommission old jobs
# MAGIC - Documentation updates
# MAGIC - Team training
# MAGIC - Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Migration
# MAGIC
# MAGIC ### 1. Start Small
# MAGIC - Begin with simple, non-critical jobs
# MAGIC - Gain confidence before tackling complex jobs
# MAGIC - Learn from each migration
# MAGIC
# MAGIC ### 2. Test Thoroughly
# MAGIC - Deploy to dev first
# MAGIC - Compare outputs with original job
# MAGIC - Run in parallel during transition
# MAGIC - Validate all edge cases
# MAGIC
# MAGIC ### 3. Document Everything
# MAGIC - Document migration decisions
# MAGIC - Keep notes on challenges
# MAGIC - Create runbooks
# MAGIC - Update team wikis
# MAGIC
# MAGIC ### 4. Communicate Clearly
# MAGIC - Inform stakeholders
# MAGIC - Set expectations
# MAGIC - Regular status updates
# MAGIC - Celebrate milestones
# MAGIC
# MAGIC ### 5. Plan for Rollback
# MAGIC - Keep old jobs available initially
# MAGIC - Have rollback procedures ready
# MAGIC - Test rollback process
# MAGIC - Monitor closely after cutover
# MAGIC
# MAGIC ### 6. Optimize Gradually
# MAGIC - Don't over-optimize initially
# MAGIC - Focus on working migration first
# MAGIC - Optimize in subsequent iterations
# MAGIC - Measure improvements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Choose the right strategy**: Lift and Shift, Refactor, or Incremental
# MAGIC
# MAGIC 2. **Analyze before migrating**: Understand current state and dependencies
# MAGIC
# MAGIC 3. **Handle dependencies carefully**: Libraries, data, jobs, infrastructure
# MAGIC
# MAGIC 4. **Parameterize configurations**: Remove hardcoded values
# MAGIC
# MAGIC 5. **Test thoroughly**: Validate in dev before production
# MAGIC
# MAGIC 6. **Migrate incrementally**: Start small, scale gradually
# MAGIC
# MAGIC 7. **Document the process**: Capture learnings for future migrations
# MAGIC
# MAGIC 8. **Plan for rollback**: Always have a backup plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC In the next session, we'll:
# MAGIC - **Demo**: Walk through migrating a real job step-by-step
# MAGIC - **Lab**: Hands-on practice migrating your own jobs
# MAGIC
# MAGIC Then we'll cover:
# MAGIC - Advanced resource management
# MAGIC - Python-defined bundles
# MAGIC - Multi-environment deployment
# MAGIC - CI/CD integration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Migration Guide](https://docs.databricks.com/dev-tools/bundles/migrate.html)
# MAGIC - [Best Practices](https://docs.databricks.com/dev-tools/bundles/best-practices.html)
# MAGIC - [Troubleshooting Guide](https://docs.databricks.com/dev-tools/bundles/troubleshooting.html)
# MAGIC - [Community Examples](https://github.com/databricks/bundle-examples)
