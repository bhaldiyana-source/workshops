# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1: Foundations of Databricks Asset Bundles (DABs)
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand what Databricks Asset Bundles are and why they matter
# MAGIC - Explain the architecture and components of DABs
# MAGIC - Describe the bundle lifecycle and key CLI commands
# MAGIC - Understand YAML configuration structure
# MAGIC - Identify use cases for DABs in your organization

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are Databricks Asset Bundles?
# MAGIC
# MAGIC Databricks Asset Bundles (DABs) are a declarative way to define, validate, deploy, and run Databricks resources as code. Think of them as "Infrastructure as Code" specifically designed for data and AI workloads.
# MAGIC
# MAGIC ### The Problem DABs Solve
# MAGIC
# MAGIC **Before DABs:**
# MAGIC - Manual job creation through UI (error-prone, not repeatable)
# MAGIC - No version control for job configurations
# MAGIC - Difficult to maintain consistency across environments
# MAGIC - Hard to collaborate on workflow definitions
# MAGIC - No standard way to test changes before deployment
# MAGIC - Manual deployment processes
# MAGIC
# MAGIC **With DABs:**
# MAGIC - Define everything in code (YAML or Python)
# MAGIC - Version control with Git
# MAGIC - Automated deployments across environments
# MAGIC - Code reviews for configuration changes
# MAGIC - CI/CD integration
# MAGIC - Consistent, repeatable deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ## DABs Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Git Repository                           │
# MAGIC │  ┌───────────────────────────────────────────────────────┐ │
# MAGIC │  │  databricks.yml (Bundle Configuration)                │ │
# MAGIC │  │  - Bundle metadata                                     │ │
# MAGIC │  │  - Target definitions (dev, staging, prod)            │ │
# MAGIC │  │  - Resource definitions (jobs, pipelines, etc.)       │ │
# MAGIC │  └───────────────────────────────────────────────────────┘ │
# MAGIC │  ┌───────────────────────────────────────────────────────┐ │
# MAGIC │  │  src/ (Source Files)                                   │ │
# MAGIC │  │  - Notebooks                                           │ │
# MAGIC │  │  - Python files                                        │ │
# MAGIC │  │  - Libraries                                           │ │
# MAGIC │  └───────────────────────────────────────────────────────┘ │
# MAGIC │  ┌───────────────────────────────────────────────────────┐ │
# MAGIC │  │  resources/ (Additional Configs)                       │ │
# MAGIC │  │  - Job definitions                                     │ │
# MAGIC │  │  - Pipeline configurations                             │ │
# MAGIC │  └───────────────────────────────────────────────────────┘ │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC                            ↓
# MAGIC                  Databricks CLI
# MAGIC                            ↓
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │              Databricks Workspace                           │
# MAGIC │  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐      │
# MAGIC │  │    Jobs     │  │  Pipelines   │  │   Clusters   │      │
# MAGIC │  └─────────────┘  └──────────────┘  └──────────────┘      │
# MAGIC │  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐      │
# MAGIC │  │  Notebooks  │  │   Volumes    │  │   Schemas    │      │
# MAGIC │  └─────────────┘  └──────────────┘  └──────────────┘      │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Components of a Bundle
# MAGIC
# MAGIC ### 1. Bundle Configuration File (`databricks.yml`)
# MAGIC The main configuration file that defines your bundle structure.
# MAGIC
# MAGIC ### 2. Source Files
# MAGIC - Notebooks (`.py`, `.sql`, `.scala`, `.r`)
# MAGIC - Python packages and wheels
# MAGIC - Libraries and dependencies
# MAGIC
# MAGIC ### 3. Resource Definitions
# MAGIC - **Jobs**: Scheduled or triggered workflows
# MAGIC - **Pipelines**: Delta Live Tables pipelines
# MAGIC - **Clusters**: Shared compute resources
# MAGIC - **Experiments**: MLflow experiments
# MAGIC - **Models**: Registered ML models
# MAGIC - **Dashboards**: AI/BI dashboards
# MAGIC - **Schemas**: Unity Catalog schemas
# MAGIC - **Volumes**: Unity Catalog volumes
# MAGIC
# MAGIC ### 4. Target Configurations
# MAGIC Environment-specific settings (dev, staging, production)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bundle Configuration Structure
# MAGIC
# MAGIC Here's a basic `databricks.yml` structure:
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: my-bundle
# MAGIC   
# MAGIC # Define different deployment targets
# MAGIC targets:
# MAGIC   dev:
# MAGIC     workspace:
# MAGIC       host: https://your-workspace.cloud.databricks.com
# MAGIC     
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://prod-workspace.cloud.databricks.com
# MAGIC
# MAGIC # Define resources
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     my_job:
# MAGIC       name: "My Job - ${bundle.target}"
# MAGIC       tasks:
# MAGIC         - task_key: main_task
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/main.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bundle Lifecycle
# MAGIC
# MAGIC ### 1. **Initialize** (`databricks bundle init`)
# MAGIC - Create a new bundle from scratch or template
# MAGIC - Sets up directory structure
# MAGIC - Creates starter `databricks.yml`
# MAGIC
# MAGIC ### 2. **Develop**
# MAGIC - Write your notebooks and code
# MAGIC - Define resources in YAML
# MAGIC - Configure environment-specific settings
# MAGIC
# MAGIC ### 3. **Validate** (`databricks bundle validate`)
# MAGIC - Check YAML syntax
# MAGIC - Verify resource definitions
# MAGIC - Test variable interpolation
# MAGIC
# MAGIC ### 4. **Deploy** (`databricks bundle deploy`)
# MAGIC - Upload source files to workspace
# MAGIC - Create or update resources
# MAGIC - Apply environment-specific configurations
# MAGIC
# MAGIC ### 5. **Run** (`databricks bundle run`)
# MAGIC - Execute jobs defined in bundle
# MAGIC - Trigger pipelines
# MAGIC
# MAGIC ### 6. **Destroy** (`databricks bundle destroy`)
# MAGIC - Remove deployed resources
# MAGIC - Clean up workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key CLI Commands
# MAGIC
# MAGIC ### Bundle Management
# MAGIC ```bash
# MAGIC # Initialize a new bundle
# MAGIC databricks bundle init
# MAGIC
# MAGIC # List available templates
# MAGIC databricks bundle init --list
# MAGIC
# MAGIC # Initialize from specific template
# MAGIC databricks bundle init default-python
# MAGIC ```
# MAGIC
# MAGIC ### Development Workflow
# MAGIC ```bash
# MAGIC # Validate bundle configuration
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # Validate with verbose output
# MAGIC databricks bundle validate --verbose
# MAGIC
# MAGIC # Deploy to development environment
# MAGIC databricks bundle deploy -t dev
# MAGIC
# MAGIC # Deploy with auto-approval
# MAGIC databricks bundle deploy -t dev --auto-approve
# MAGIC
# MAGIC # Deploy and watch for changes
# MAGIC databricks bundle deploy -t dev --watch
# MAGIC ```
# MAGIC
# MAGIC ### Running Resources
# MAGIC ```bash
# MAGIC # Run a specific job
# MAGIC databricks bundle run my_job -t dev
# MAGIC
# MAGIC # List all bundle deployments
# MAGIC databricks bundle list
# MAGIC
# MAGIC # Generate deployment summary
# MAGIC databricks bundle summary
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variable Interpolation
# MAGIC
# MAGIC DABs support dynamic variable interpolation to make configurations flexible:
# MAGIC
# MAGIC ### Built-in Variables
# MAGIC ```yaml
# MAGIC # Reference bundle name
# MAGIC name: "${bundle.name}_job"
# MAGIC
# MAGIC # Reference target environment
# MAGIC database: "${bundle.target}_db"
# MAGIC
# MAGIC # Reference workspace
# MAGIC path: "${workspace.file_path}/config"
# MAGIC ```
# MAGIC
# MAGIC ### Custom Variables
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: "Unity Catalog name"
# MAGIC     default: "dev_catalog"
# MAGIC   
# MAGIC   num_workers:
# MAGIC     description: "Number of cluster workers"
# MAGIC     default: 2
# MAGIC
# MAGIC # Use variables in resources
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     my_job:
# MAGIC       tasks:
# MAGIC         - new_cluster:
# MAGIC             num_workers: ${var.num_workers}
# MAGIC ```
# MAGIC
# MAGIC ### Target-Specific Overrides
# MAGIC ```yaml
# MAGIC targets:
# MAGIC   dev:
# MAGIC     variables:
# MAGIC       catalog: "dev_catalog"
# MAGIC       num_workers: 1
# MAGIC   
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       catalog: "prod_catalog"
# MAGIC       num_workers: 4
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resource Types Supported
# MAGIC
# MAGIC ### Jobs
# MAGIC Define Databricks jobs with tasks, clusters, schedules, and dependencies.
# MAGIC
# MAGIC ### Pipelines (Delta Live Tables)
# MAGIC Configure DLT pipelines with notebooks, settings, and targets.
# MAGIC
# MAGIC ### Clusters
# MAGIC Define shared cluster configurations for interactive use.
# MAGIC
# MAGIC ### Experiments (MLflow)
# MAGIC Create and manage MLflow experiments.
# MAGIC
# MAGIC ### Model Serving Endpoints
# MAGIC Deploy models to serving endpoints.
# MAGIC
# MAGIC ### Registered Models
# MAGIC Manage Unity Catalog registered models.
# MAGIC
# MAGIC ### Dashboards (AI/BI)
# MAGIC Define and deploy Databricks dashboards.
# MAGIC
# MAGIC ### Unity Catalog Resources
# MAGIC - Schemas
# MAGIC - Volumes
# MAGIC - Quality Monitors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benefits of Using DABs
# MAGIC
# MAGIC ### 1. **Version Control**
# MAGIC - Track all changes in Git
# MAGIC - See history of configuration changes
# MAGIC - Revert to previous versions easily
# MAGIC
# MAGIC ### 2. **Code Reviews**
# MAGIC - Review infrastructure changes like code
# MAGIC - Catch errors before deployment
# MAGIC - Share knowledge across team
# MAGIC
# MAGIC ### 3. **Consistency**
# MAGIC - Same structure across all projects
# MAGIC - Standardized naming conventions
# MAGIC - Reduced configuration drift
# MAGIC
# MAGIC ### 4. **Multi-Environment**
# MAGIC - Single source of truth
# MAGIC - Easy environment promotion
# MAGIC - Environment-specific configurations
# MAGIC
# MAGIC ### 5. **CI/CD Integration**
# MAGIC - Automate testing
# MAGIC - Automated deployments
# MAGIC - Faster feedback loops
# MAGIC
# MAGIC ### 6. **Collaboration**
# MAGIC - Multiple team members can contribute
# MAGIC - Clear ownership and accountability
# MAGIC - Better documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Cases for DABs
# MAGIC
# MAGIC ### 1. **Data Engineering Pipelines**
# MAGIC - ETL/ELT workflows
# MAGIC - Batch processing jobs
# MAGIC - Streaming pipelines with DLT
# MAGIC
# MAGIC ### 2. **Machine Learning Operations (MLOps)**
# MAGIC - Training pipelines
# MAGIC - Model deployment
# MAGIC - Feature engineering workflows
# MAGIC - Model serving endpoints
# MAGIC
# MAGIC ### 3. **Analytics and BI**
# MAGIC - Scheduled reports
# MAGIC - Dashboard data refresh
# MAGIC - Data quality checks
# MAGIC
# MAGIC ### 4. **Data Quality and Governance**
# MAGIC - Quality monitoring pipelines
# MAGIC - Data validation workflows
# MAGIC - Compliance reporting
# MAGIC
# MAGIC ### 5. **Multi-Team Collaboration**
# MAGIC - Shared infrastructure definitions
# MAGIC - Standardized deployment processes
# MAGIC - Cross-functional projects

# COMMAND ----------

# MAGIC %md
# MAGIC ## DABs vs Traditional Approaches
# MAGIC
# MAGIC | Aspect | Traditional Approach | DABs Approach |
# MAGIC |--------|---------------------|---------------|
# MAGIC | **Job Creation** | Manual UI clicks | Code definition |
# MAGIC | **Version Control** | None or manual exports | Native Git integration |
# MAGIC | **Environment Promotion** | Manual recreation | Automated deployment |
# MAGIC | **Configuration Management** | UI-based settings | YAML/Python files |
# MAGIC | **Testing** | Manual verification | Automated validation |
# MAGIC | **Collaboration** | Email/screenshots | Pull requests |
# MAGIC | **Rollback** | Difficult/manual | Git revert |
# MAGIC | **Documentation** | Separate docs | Code as documentation |
# MAGIC | **Consistency** | Varies by developer | Enforced by templates |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Getting Started
# MAGIC
# MAGIC ### 1. **Start Small**
# MAGIC - Begin with a simple, non-critical job
# MAGIC - Learn the basics before scaling up
# MAGIC - Iterate and improve
# MAGIC
# MAGIC ### 2. **Use Templates**
# MAGIC - Leverage official Databricks templates
# MAGIC - Create organization-specific templates
# MAGIC - Maintain consistency
# MAGIC
# MAGIC ### 3. **Follow Naming Conventions**
# MAGIC - Use descriptive bundle names
# MAGIC - Include environment in resource names
# MAGIC - Be consistent across projects
# MAGIC
# MAGIC ### 4. **Organize Your Files**
# MAGIC - Separate source code from configuration
# MAGIC - Use logical directory structure
# MAGIC - Keep related resources together
# MAGIC
# MAGIC ### 5. **Document Your Bundles**
# MAGIC - Include README files
# MAGIC - Comment your YAML configurations
# MAGIC - Document deployment procedures
# MAGIC
# MAGIC ### 6. **Test in Development First**
# MAGIC - Always deploy to dev environment first
# MAGIC - Validate thoroughly before promoting
# MAGIC - Use automated tests when possible

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites for Using DABs
# MAGIC
# MAGIC ### Required Tools
# MAGIC - **Databricks CLI** (version 0.218.0+)
# MAGIC - **Git** for version control
# MAGIC - **Python** 3.8 or later
# MAGIC - **IDE** (VS Code, PyCharm, etc.)
# MAGIC
# MAGIC ### Required Access
# MAGIC - Databricks workspace access
# MAGIC - Permission to create jobs and clusters
# MAGIC - Unity Catalog access (recommended)
# MAGIC
# MAGIC ### Required Knowledge
# MAGIC - Basic YAML syntax
# MAGIC - Databricks concepts (jobs, notebooks, clusters)
# MAGIC - Git basics
# MAGIC - Command line familiarity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installing Databricks CLI
# MAGIC
# MAGIC The Databricks CLI is required to work with bundles. Here's how to install it:
# MAGIC
# MAGIC ### macOS/Linux
# MAGIC ```bash
# MAGIC curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
# MAGIC ```
# MAGIC
# MAGIC ### Windows (PowerShell)
# MAGIC ```powershell
# MAGIC iwr -useb https://raw.githubusercontent.com/databricks/setup-cli/main/install.ps1 | iex
# MAGIC ```
# MAGIC
# MAGIC ### Verify Installation
# MAGIC ```bash
# MAGIC databricks --version
# MAGIC ```
# MAGIC
# MAGIC ### Authentication
# MAGIC ```bash
# MAGIC # Configure authentication profile
# MAGIC databricks auth login --host https://your-workspace.cloud.databricks.com
# MAGIC
# MAGIC # List configured profiles
# MAGIC databricks auth profiles
# MAGIC
# MAGIC # Test authentication
# MAGIC databricks workspace current-user
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Bundle Directory Structure
# MAGIC
# MAGIC ```
# MAGIC my-bundle/
# MAGIC ├── databricks.yml           # Main bundle configuration
# MAGIC ├── README.md                # Documentation
# MAGIC ├── .gitignore               # Git ignore rules
# MAGIC ├── src/                     # Source code
# MAGIC │   ├── notebooks/           # Databricks notebooks
# MAGIC │   │   ├── etl.py
# MAGIC │   │   └── analysis.py
# MAGIC │   ├── libraries/           # Python packages
# MAGIC │   │   └── my_package/
# MAGIC │   └── utils/               # Utility functions
# MAGIC ├── resources/               # Resource definitions
# MAGIC │   ├── jobs/
# MAGIC │   │   └── daily_etl.yml
# MAGIC │   └── pipelines/
# MAGIC │       └── streaming_pipeline.yml
# MAGIC ├── config/                  # Configuration files
# MAGIC │   ├── dev.yml
# MAGIC │   ├── staging.yml
# MAGIC │   └── prod.yml
# MAGIC └── tests/                   # Test files
# MAGIC     ├── unit/
# MAGIC     └── integration/
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **DABs enable Infrastructure as Code** for Databricks resources
# MAGIC
# MAGIC 2. **Bundle lifecycle**: init → develop → validate → deploy → run → destroy
# MAGIC
# MAGIC 3. **Core components**: `databricks.yml`, source files, resource definitions
# MAGIC
# MAGIC 4. **Supports multiple resource types**: jobs, pipelines, clusters, models, dashboards, Unity Catalog
# MAGIC
# MAGIC 5. **Variable interpolation** enables flexible, environment-specific configurations
# MAGIC
# MAGIC 6. **CLI commands** provide full control over bundle lifecycle
# MAGIC
# MAGIC 7. **Best practices**: start small, use templates, follow conventions, test thoroughly
# MAGIC
# MAGIC 8. **Benefits**: version control, code reviews, consistency, automation, collaboration

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC In the next module, we'll:
# MAGIC - **Demo**: Create your first bundle from scratch
# MAGIC - **Lab**: Hands-on practice with bundle creation and deployment
# MAGIC
# MAGIC Then we'll move on to:
# MAGIC - Migrating existing jobs to DABs
# MAGIC - Advanced resource management
# MAGIC - Python-defined bundles
# MAGIC - Multi-environment deployments
# MAGIC - CI/CD integration
# MAGIC - Production best practices

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Official DABs Documentation](https://docs.databricks.com/dev-tools/bundles/)
# MAGIC - [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/)
# MAGIC - [DABs Release Notes](https://docs.databricks.com/release-notes/dev-tools/bundles.html)
# MAGIC - [GitHub: Databricks CLI](https://github.com/databricks/cli)
# MAGIC - [Community Forum](https://community.databricks.com/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Questions for Reflection
# MAGIC
# MAGIC 1. What problems in your current workflow could DABs solve?
# MAGIC 2. Which resources in your workspace would benefit from version control?
# MAGIC 3. How could your team benefit from standardized deployment processes?
# MAGIC 4. What would be a good first candidate for migration to DABs?
