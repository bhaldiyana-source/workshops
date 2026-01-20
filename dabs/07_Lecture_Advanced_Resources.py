# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3: Advanced Resource Management
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Manage Delta Live Tables (DLT) pipelines in DABs
# MAGIC - Integrate Unity Catalog resources (schemas, volumes, models)
# MAGIC - Configure shared and job clusters
# MAGIC - Manage Python wheels and libraries
# MAGIC - Work with multiple resource types in a single bundle
# MAGIC - Handle complex resource dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resource Types in DABs
# MAGIC
# MAGIC DABs supports a wide range of Databricks resources:
# MAGIC
# MAGIC ### Compute Resources
# MAGIC - **Jobs**: Scheduled workflows and tasks
# MAGIC - **Pipelines**: Delta Live Tables pipelines
# MAGIC - **Clusters**: Interactive and shared clusters
# MAGIC
# MAGIC ### Data & Catalog Resources
# MAGIC - **Schemas**: Unity Catalog schemas
# MAGIC - **Volumes**: Unity Catalog volumes for file storage
# MAGIC - **Quality Monitors**: Data quality monitoring
# MAGIC
# MAGIC ### ML Resources
# MAGIC - **Experiments**: MLflow experiments
# MAGIC - **Models**: Registered models in Unity Catalog
# MAGIC - **Model Serving Endpoints**: Real-time model serving
# MAGIC
# MAGIC ### Analytics Resources
# MAGIC - **Dashboards**: AI/BI dashboards
# MAGIC - **Alerts**: SQL alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Tables (DLT) in DABs
# MAGIC
# MAGIC ### What is Delta Live Tables?
# MAGIC DLT is a declarative framework for building reliable, maintainable, and testable data processing pipelines.
# MAGIC
# MAGIC ### DLT Benefits
# MAGIC - **Declarative**: Define what, not how
# MAGIC - **Automatic Data Quality**: Built-in expectations
# MAGIC - **Automatic Pipeline Management**: DAG inference
# MAGIC - **Change Data Capture (CDC)**: Streaming and batch CDC

# COMMAND ----------

# Example: DLT Pipeline Configuration
dlt_pipeline_config = '''resources:
  pipelines:
    customer_data_pipeline:
      name: "Customer Data Pipeline - ${bundle.target}"
      
      # Target schema where tables will be created
      target: "${var.catalog}.${bundle.target}_customer_data"
      
      # Pipeline mode: triggered or continuous
      continuous: false
      
      # Development vs production mode
      development: ${var.is_development}
      
      # Cluster configuration
      clusters:
        - label: "default"
          num_workers: ${var.num_workers}
          spark_version: "13.3.x-scala2.12"
          node_type_id: "i3.xlarge"
          autoscale:
            min_workers: 1
            max_workers: 5
      
      # Notebook libraries (DLT notebooks)
      libraries:
        - notebook:
            path: ./pipelines/bronze_layer.py
        - notebook:
            path: ./pipelines/silver_layer.py
        - notebook:
            path: ./pipelines/gold_layer.py
      
      # Configuration parameters
      configuration:
        pipeline.environment: ${bundle.target}
        pipeline.catalog: ${var.catalog}
        source.path: ${var.source_path}
      
      # Email notifications
      notifications:
        - email_recipients:
            - data-team@company.com
          alerts:
            - "on-update-failure"
            - "on-update-fatal-failure"
      
      # Permissions
      permissions:
        - level: CAN_VIEW
          group_name: "data-team"
        - level: CAN_MANAGE
          user_name: "${workspace.current_user.userName}"
'''

print("DLT Pipeline Configuration:")
print(dlt_pipeline_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Notebook Example

# COMMAND ----------

# Example DLT notebook
dlt_bronze_notebook = '''# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# Get configuration
catalog = spark.conf.get("pipeline.catalog")
source_path = spark.conf.get("source.path")

# COMMAND ----------

# Bronze layer: Raw data ingestion
@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from source systems",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{source_path}/schemas/customers")
        .load(f"{source_path}/customers")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# Bronze transactions with data quality expectations
@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data"
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_date", "transaction_date IS NOT NULL")
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{source_path}/schemas/transactions")
        .load(f"{source_path}/transactions")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
'''

print("DLT Bronze Layer Notebook:")
print(dlt_bronze_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Resources
# MAGIC
# MAGIC ### Managing Schemas

# COMMAND ----------

# Unity Catalog Schema Configuration
uc_schema_config = '''resources:
  schemas:
    # Bronze layer schema
    bronze_schema:
      name: "${var.catalog}.${bundle.target}_bronze"
      comment: "Bronze layer - raw data ingestion"
      
      # Schema properties
      properties:
        layer: "bronze"
        environment: "${bundle.target}"
        owner: "data-engineering"
      
      # Grants
      grants:
        - principal: "data_engineers"
          privileges: ["ALL_PRIVILEGES"]
        - principal: "data_analysts"
          privileges: ["SELECT", "USE_SCHEMA"]
    
    # Silver layer schema
    silver_schema:
      name: "${var.catalog}.${bundle.target}_silver"
      comment: "Silver layer - cleaned and transformed data"
      
      properties:
        layer: "silver"
        environment: "${bundle.target}"
      
      grants:
        - principal: "data_engineers"
          privileges: ["ALL_PRIVILEGES"]
        - principal: "data_analysts"
          privileges: ["SELECT", "USE_SCHEMA"]
    
    # Gold layer schema
    gold_schema:
      name: "${var.catalog}.${bundle.target}_gold"
      comment: "Gold layer - business-level aggregates"
      
      properties:
        layer: "gold"
        environment: "${bundle.target}"
      
      grants:
        - principal: "data_engineers"
          privileges: ["ALL_PRIVILEGES"]
        - principal: "data_analysts"
          privileges: ["SELECT", "USE_SCHEMA"]
        - principal: "business_users"
          privileges: ["SELECT"]
'''

print("Unity Catalog Schema Configuration:")
print(uc_schema_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managing Volumes

# COMMAND ----------

# Unity Catalog Volume Configuration
uc_volume_config = '''resources:
  volumes:
    # Landing zone for raw files
    raw_data_volume:
      name: "${var.catalog}.${bundle.target}_bronze.raw_data"
      comment: "Landing zone for incoming data files"
      volume_type: "EXTERNAL"
      storage_location: "s3://my-bucket/raw-data/${bundle.target}/"
      
      grants:
        - principal: "data_engineers"
          privileges: ["ALL_PRIVILEGES"]
        - principal: "ingestion_service"
          privileges: ["READ_VOLUME", "WRITE_VOLUME"]
    
    # Processed data storage
    processed_volume:
      name: "${var.catalog}.${bundle.target}_silver.processed"
      comment: "Storage for processed data files"
      volume_type: "MANAGED"
      
      grants:
        - principal: "data_engineers"
          privileges: ["ALL_PRIVILEGES"]
        - principal: "ml_team"
          privileges: ["READ_VOLUME"]
'''

print("Unity Catalog Volume Configuration:")
print(uc_volume_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Management
# MAGIC
# MAGIC ### Job Clusters vs All-Purpose Clusters

# COMMAND ----------

# Job Clusters: Created for job, terminated after
job_cluster_example = '''resources:
  jobs:
    data_pipeline:
      name: "Data Pipeline"
      
      # Define job clusters that can be shared across tasks
      job_clusters:
        - job_cluster_key: "shared_cluster"
          new_cluster:
            num_workers: 4
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            
            # Custom tags
            custom_tags:
              project: "data-pipeline"
              environment: "${bundle.target}"
            
            # Spark configuration
            spark_conf:
              "spark.databricks.delta.preview.enabled": "true"
              "spark.sql.adaptive.enabled": "true"
            
            # Autoscaling
            autoscale:
              min_workers: 2
              max_workers: 8
      
      tasks:
        - task_key: "task1"
          job_cluster_key: "shared_cluster"  # Use the job cluster
          notebook_task:
            notebook_path: ./src/task1.py
        
        - task_key: "task2"
          job_cluster_key: "shared_cluster"  # Reuse same cluster
          notebook_task:
            notebook_path: ./src/task2.py
'''

# All-Purpose Clusters: Interactive clusters
all_purpose_cluster = '''resources:
  clusters:
    development_cluster:
      cluster_name: "Development Cluster - ${bundle.target}"
      
      # Cluster configuration
      spark_version: "13.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 2
      
      # Auto-termination
      autotermination_minutes: 30
      
      # Libraries pre-installed
      libraries:
        - pypi:
            package: "pandas==2.0.0"
        - pypi:
            package: "scikit-learn>=1.0.0"
      
      # Cluster permissions
      permissions:
        - level: CAN_ATTACH_TO
          group_name: "data-team"
        - level: CAN_RESTART
          user_name: "${workspace.current_user.userName}"
'''

print("Job Cluster Configuration:")
print(job_cluster_example)
print("\n" + "="*60 + "\n")
print("All-Purpose Cluster Configuration:")
print(all_purpose_cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library Management
# MAGIC
# MAGIC ### Python Wheels in DABs

# COMMAND ----------

# Managing Python wheels
wheel_management = '''# Project structure with Python package
my-bundle/
├── databricks.yml
├── setup.py
├── src/
│   └── my_package/
│       ├── __init__.py
│       ├── transformations.py
│       └── utils.py
└── notebooks/
    └── main.py

# setup.py
from setuptools import setup, find_packages

setup(
    name="my-data-package",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=2.0.0",
        "pyspark>=3.4.0",
    ]
)

# databricks.yml with wheel
bundle:
  name: my-bundle

# Build wheel automatically
artifacts:
  my_package:
    type: whl
    build: "python setup.py bdist_wheel"
    path: dist/*.whl

resources:
  jobs:
    my_job:
      tasks:
        - notebook_task:
            notebook_path: ./notebooks/main.py
          
          # Reference the wheel artifact
          libraries:
            - whl: ../dist/my_data_package-0.1.0-py3-none-any.whl
'''

print("Python Wheel Management:")
print(wheel_management)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Resources
# MAGIC
# MAGIC ### Experiments and Models

# COMMAND ----------

# MLflow resources configuration
mlflow_config = '''resources:
  # MLflow Experiment
  experiments:
    customer_churn_experiment:
      name: "/Shared/experiments/customer-churn-${bundle.target}"
      description: "Customer churn prediction experiment"
      
      # Experiment tags
      tags:
        project: "customer-analytics"
        environment: "${bundle.target}"
      
      permissions:
        - level: CAN_EDIT
          group_name: "ml-team"
        - level: CAN_READ
          group_name: "data-scientists"
  
  # Registered Model
  models:
    customer_churn_model:
      name: "${var.catalog}.${bundle.target}_models.customer_churn"
      description: "Customer churn prediction model"
      
      # Model tags
      tags:
        - key: "task"
          value: "classification"
        - key: "algorithm"
          value: "xgboost"
      
      grants:
        - principal: "ml_engineers"
          privileges: ["ALL_PRIVILEGES"]
        - principal: "data_scientists"
          privileges: ["SELECT"]
  
  # Model Serving Endpoint
  model_serving_endpoints:
    churn_prediction_endpoint:
      name: "churn-prediction-${bundle.target}"
      
      config:
        served_models:
          - model_name: "${var.catalog}.${bundle.target}_models.customer_churn"
            model_version: "1"
            workload_size: "Small"
            scale_to_zero_enabled: true
        
        traffic_config:
          routes:
            - served_model_name: "${var.catalog}.${bundle.target}_models.customer_churn-1"
              traffic_percentage: 100
      
      permissions:
        - level: CAN_QUERY
          group_name: "application-services"
        - level: CAN_MANAGE
          group_name: "ml-team"
'''

print("MLflow Resources Configuration:")
print(mlflow_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Bundle with Multiple Resources

# COMMAND ----------

# Comprehensive bundle with all resource types
complete_bundle = '''bundle:
  name: customer-analytics-platform

variables:
  catalog:
    default: "dev_catalog"
  num_workers:
    default: 2

targets:
  dev:
    variables:
      catalog: "dev_catalog"
      num_workers: 1
  prod:
    variables:
      catalog: "prod_catalog"
      num_workers: 4

resources:
  # Unity Catalog Schemas
  schemas:
    bronze_schema:
      name: "${var.catalog}.${bundle.target}_bronze"
      comment: "Bronze layer"
    
    silver_schema:
      name: "${var.catalog}.${bundle.target}_silver"
      comment: "Silver layer"
  
  # Volumes
  volumes:
    raw_data:
      name: "${var.catalog}.${bundle.target}_bronze.raw_data"
      volume_type: "MANAGED"
  
  # DLT Pipeline
  pipelines:
    data_pipeline:
      name: "Customer Data Pipeline - ${bundle.target}"
      target: "${var.catalog}.${bundle.target}_silver"
      continuous: false
      
      libraries:
        - notebook:
            path: ./pipelines/bronze.py
        - notebook:
            path: ./pipelines/silver.py
  
  # Jobs
  jobs:
    analytics_job:
      name: "Customer Analytics - ${bundle.target}"
      
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "UTC"
      
      tasks:
        - task_key: "generate_features"
          notebook_task:
            notebook_path: ./jobs/generate_features.py
          new_cluster:
            num_workers: ${var.num_workers}
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
  
  # ML Experiment
  experiments:
    churn_experiment:
      name: "/Shared/experiments/churn-${bundle.target}"
      description: "Churn prediction"
  
  # Registered Model
  models:
    churn_model:
      name: "${var.catalog}.models.churn"
      description: "Churn prediction model"
  
  # Development Cluster
  clusters:
    dev_cluster:
      cluster_name: "Dev Cluster - ${bundle.target}"
      spark_version: "13.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 2
      autotermination_minutes: 30
'''

print("Complete Bundle with Multiple Resources:")
print(complete_bundle)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resource Dependencies
# MAGIC
# MAGIC ### Handling Dependencies Between Resources
# MAGIC
# MAGIC Resources often depend on each other. DABs handles this automatically in most cases.

# COMMAND ----------

# Resource dependency example
dependency_example = '''# Schema must exist before pipeline can write to it
resources:
  schemas:
    my_schema:
      name: "${var.catalog}.my_schema"
  
  pipelines:
    my_pipeline:
      # Pipeline will write to the schema above
      # DABs ensures schema is created first
      target: "${var.catalog}.my_schema"
      libraries:
        - notebook:
            path: ./pipeline.py
  
  jobs:
    my_job:
      tasks:
        # Task reads from pipeline output
        # Ensure pipeline runs before job
        - task_key: "analyze_data"
          notebook_task:
            notebook_path: ./analyze.py
            base_parameters:
              source_schema: "${var.catalog}.my_schema"
'''

print("Resource Dependency Example:")
print(dependency_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### 1. Resource Organization
# MAGIC - Group related resources in same bundle
# MAGIC - Use clear, consistent naming
# MAGIC - Separate concerns (data, ML, analytics)
# MAGIC
# MAGIC ### 2. DLT Pipelines
# MAGIC - Use development mode for testing
# MAGIC - Implement data quality expectations
# MAGIC - Leverage auto-scaling for cost optimization
# MAGIC - Use continuous mode for streaming
# MAGIC
# MAGIC ### 3. Unity Catalog
# MAGIC - Always use Unity Catalog for governance
# MAGIC - Define schemas and volumes in bundles
# MAGIC - Manage permissions explicitly
# MAGIC - Use external volumes for existing data
# MAGIC
# MAGIC ### 4. Cluster Management
# MAGIC - Use job clusters for batch workloads
# MAGIC - Use all-purpose clusters for development only
# MAGIC - Enable autoscaling for variable workloads
# MAGIC - Set appropriate auto-termination
# MAGIC
# MAGIC ### 5. Library Management
# MAGIC - Build Python wheels for reusable code
# MAGIC - Pin library versions
# MAGIC - Use artifacts for automatic wheel building
# MAGIC - Share wheels across tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - DABs supports comprehensive resource types
# MAGIC - DLT pipelines enable declarative data engineering
# MAGIC - Unity Catalog integration provides governance
# MAGIC - Job clusters optimize cost and performance
# MAGIC - Python wheels enable code reuse
# MAGIC - MLflow resources support end-to-end ML
# MAGIC - Resource dependencies are handled automatically

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC - **Demo**: Build a complete pipeline with DLT and Unity Catalog
# MAGIC - **Lab**: Create your own multi-resource bundle
# MAGIC
# MAGIC Then:
# MAGIC - Python-defined bundles
# MAGIC - Multi-environment deployment
# MAGIC - CI/CD integration
