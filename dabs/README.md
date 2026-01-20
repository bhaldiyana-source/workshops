# Code Migration with Databricks Asset Bundles (DABs) Workshop

## Overview

This comprehensive workshop provides hands-on experience with **Databricks Asset Bundles (DABs)**, focusing on code migration strategies and modern DevOps practices for data and AI projects. Participants will learn how to migrate existing Databricks workloads to a structured, version-controlled, and CI/CD-ready framework using the latest DABs features.

## What are Databricks Asset Bundles?

Databricks Asset Bundles (DABs) enable you to programmatically define, validate, deploy, and run Databricks resources as code. DABs bring software engineering best practices to data engineering, analytics, and machine learning workflows:

- **Infrastructure as Code (IaC)**: Define all Databricks resources declaratively
- **Version Control**: Track changes to jobs, notebooks, and configurations in Git
- **Multi-Environment Deployment**: Seamlessly deploy across dev, staging, and production
- **CI/CD Integration**: Automate testing and deployment pipelines
- **Team Collaboration**: Enable code reviews and collaborative development

## Workshop Prerequisites

### Required Tools
- **Databricks CLI**: Version 0.218.0 or later
  ```bash
  # Install/upgrade Databricks CLI
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
  ```
- **Git**: For version control
- **Python**: Version 3.8 or later
- **IDE/Editor**: VS Code, PyCharm, or similar

### Required Access
- Databricks workspace with appropriate permissions
- Unity Catalog enabled workspace (recommended)
- Permission to create and manage jobs, clusters, and workflows

### Recommended Knowledge
- Basic understanding of Databricks notebooks and jobs
- Familiarity with YAML configuration files
- Basic Git operations
- Python programming fundamentals

## Latest DABs Features (2025-2026)

### 🆕 Python Support for Bundle Definitions
Define jobs and pipelines programmatically in Python instead of YAML:
- Dynamic configuration generation
- Type-safe resource definitions
- Reusable components and mutators
- Complex conditional logic

### 🆕 Workspace Integration
- Deploy and manage bundles directly from the Databricks workspace UI
- Use workspace terminals to run bundle commands
- Real-time deployment status and feedback

### 🆕 Bind/Unbind Support
Link existing workspace resources to DABs for unified management:
- Clusters
- Dashboards (AI/BI)
- Registered models
- Volumes and schemas
- Quality monitors
- Model serving endpoints

### 🆕 Dynamic Versioning for Wheels
Automatic version management for Python packages:
- Timestamp-based versioning
- No need to manually update `setup.py` or `pyproject.toml`
- Simplified deployment workflows

### 🆕 Enhanced Resource Support
- Unity Catalog schemas and volumes
- AI/BI dashboards
- Quality monitors
- Model serving endpoints
- Delta Live Tables (DLT) pipelines
- MLflow experiments and models

### 🆕 Complex Variables
- Variable interpolation and references
- Environment-specific configurations
- Dynamic value resolution

## Workshop Structure

### Module 1: Foundations of DABs (30 mins)
**Lecture Topics:**
- Understanding DABs architecture
- YAML configuration structure
- CLI commands and workflow
- Bundle lifecycle (init, validate, deploy, run, destroy)

**Hands-On:**
- Installing and configuring Databricks CLI
- Creating your first bundle from a template
- Understanding the `databricks.yml` structure

### Module 2: Migrating Existing Jobs to DABs (45 mins)
**Lecture Topics:**
- Migration strategies and patterns
- Analyzing existing workspace resources
- Planning multi-environment deployments
- Handling dependencies and libraries

**Hands-On Lab:**
- Identifying jobs and workflows to migrate
- Converting existing job configurations to bundle format
- Managing notebook dependencies
- Setting up environment-specific configurations

### Module 3: Advanced Resource Management (45 mins)
**Lecture Topics:**
- Managing multiple resource types (jobs, DLT, clusters)
- Unity Catalog integration
- Workspace file management
- Library and dependency management

**Hands-On Lab:**
- Creating bundles with DLT pipelines
- Defining Unity Catalog schemas and volumes
- Managing Python wheels and dependencies
- Setting up shared cluster configurations

### Module 4: Python-Defined Bundles (45 mins)
**Lecture Topics:**
- Python support for DABs
- Creating custom mutators
- Dynamic configuration generation
- Type-safe resource definitions

**Hands-On Lab:**
- Converting YAML bundles to Python
- Creating reusable components
- Implementing custom mutators
- Dynamic job generation

### Module 5: Multi-Environment Deployment (45 mins)
**Lecture Topics:**
- Environment configuration strategies
- Target-specific overrides
- Secret management
- Deployment best practices

**Hands-On Lab:**
- Setting up dev, staging, and production targets
- Configuring environment-specific variables
- Managing secrets and credentials
- Implementing deployment gates

### Module 6: CI/CD Integration (60 mins)
**Lecture Topics:**
- Git workflow strategies
- GitHub Actions integration
- Azure DevOps pipelines
- GitLab CI/CD
- Testing strategies

**Hands-On Lab:**
- Setting up Git repository
- Creating CI/CD pipeline
- Implementing automated testing
- Setting up deployment workflows
- Rollback strategies

### Module 7: Advanced Migration Patterns (45 mins)
**Lecture Topics:**
- Bind/unbind existing resources
- Incremental migration strategies
- Managing legacy workflows
- Handling complex dependencies

**Hands-On Lab:**
- Binding existing dashboards and models
- Migrating complex job orchestrations
- Managing dependencies across bundles
- Implementing phased migration

### Module 8: Production Best Practices (45 mins)
**Lecture Topics:**
- Bundle organization patterns
- Documentation standards
- Monitoring and observability
- Troubleshooting common issues
- Security considerations

**Hands-On Lab:**
- Implementing bundle templates
- Setting up deployment monitoring
- Creating runbooks
- Performance optimization

## Workshop Files Structure

```
dabs/
├── README.md                                    # This file
├── 01_Lecture_Foundations_of_DABs.py           # Module 1 lecture
├── 02_Demo_First_Bundle.py                     # Module 1 demo
├── 03_Lab_Creating_Your_First_Bundle.py        # Module 1 lab
├── 04_Lecture_Job_Migration_Strategies.py      # Module 2 lecture
├── 05_Demo_Migrating_Simple_Job.py             # Module 2 demo
├── 06_Lab_Migrating_Existing_Jobs.py           # Module 2 lab
├── 07_Lecture_Advanced_Resources.py            # Module 3 lecture
├── 08_Demo_DLT_and_Unity_Catalog.py           # Module 3 demo
├── 09_Lab_Complex_Resource_Management.py       # Module 3 lab
├── 10_Lecture_Python_Defined_Bundles.py        # Module 4 lecture
├── 11_Demo_Python_Mutators.py                  # Module 4 demo
├── 12_Lab_Python_Bundle_Implementation.py      # Module 4 lab
├── 13_Lecture_Multi_Environment_Deployment.py  # Module 5 lecture
├── 14_Demo_Environment_Configuration.py        # Module 5 demo
├── 15_Lab_Multi_Environment_Setup.py           # Module 5 lab
├── 16_Lecture_CICD_Integration.py             # Module 6 lecture
├── 17_Demo_GitHub_Actions_Pipeline.py          # Module 6 demo
├── 18_Lab_Setting_Up_CICD.py                   # Module 6 lab
├── 19_Lecture_Advanced_Migration.py            # Module 7 lecture
├── 20_Demo_Bind_Unbind_Resources.py           # Module 7 demo
├── 21_Lab_Complex_Migration_Scenario.py        # Module 7 lab
├── 22_Lecture_Production_Best_Practices.py     # Module 8 lecture
├── 23_Demo_Production_Pipeline.py              # Module 8 demo
├── 24_Lab_Production_Implementation.py         # Module 8 lab
├── examples/                                    # Example bundles
│   ├── simple-job/
│   │   ├── databricks.yml
│   │   └── src/
│   ├── dlt-pipeline/
│   │   ├── databricks.yml
│   │   └── src/
│   ├── python-bundle/
│   │   ├── databricks.yml
│   │   ├── bundle.py
│   │   └── src/
│   └── multi-env/
│       ├── databricks.yml
│       └── environments/
└── templates/                                   # Bundle templates
    ├── basic-job-template/
    ├── dlt-template/
    └── full-stack-template/
```

## Key Code Migration Patterns

### Pattern 1: Lift and Shift
**Use Case:** Quick migration with minimal changes
- Export existing job configurations
- Convert JSON to YAML format
- Deploy as-is with DABs structure

### Pattern 2: Refactor and Optimize
**Use Case:** Modernize while migrating
- Reorganize notebooks into logical modules
- Implement proper error handling
- Add testing and validation layers
- Optimize cluster configurations

### Pattern 3: Incremental Migration
**Use Case:** Large-scale migration with minimal risk
- Migrate non-critical jobs first
- Run parallel deployments (legacy + DABs)
- Gradually increase DABs adoption
- Bind existing resources as needed

### Pattern 4: Greenfield with DABs
**Use Case:** New projects with best practices from start
- Design bundle structure first
- Implement Python-based configurations
- Set up CI/CD from day one
- Use templates for consistency

## Common Migration Scenarios

### Scenario 1: Single Job Migration
```yaml
# databricks.yml
bundle:
  name: my-migrated-job

targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com

resources:
  jobs:
    my_job:
      name: "My Migrated Job - ${bundle.target}"
      tasks:
        - task_key: main_task
          notebook_task:
            notebook_path: ./notebooks/main.py
          new_cluster:
            num_workers: 2
            spark_version: 13.3.x-scala2.12
            node_type_id: i3.xlarge
```

### Scenario 2: DLT Pipeline Migration
```yaml
resources:
  pipelines:
    my_dlt_pipeline:
      name: "My DLT Pipeline - ${bundle.target}"
      target: "${bundle.target}_pipeline_db"
      libraries:
        - notebook:
            path: ./dlt/bronze_layer.py
        - notebook:
            path: ./dlt/silver_layer.py
        - notebook:
            path: ./dlt/gold_layer.py
      configuration:
        environment: ${bundle.target}
```

### Scenario 3: Python-Based Bundle
```python
# bundle.py
from databricks.bundles import Bundle, Job, Task, NewCluster

def get_bundle() -> Bundle:
    bundle = Bundle(name="python-defined-bundle")
    
    # Define job programmatically
    job = Job(
        name=f"My Job - {bundle.target}",
        tasks=[
            Task(
                task_key="main_task",
                notebook_task={
                    "notebook_path": "./notebooks/main.py"
                },
                new_cluster=NewCluster(
                    num_workers=2,
                    spark_version="13.3.x-scala2.12",
                    node_type_id="i3.xlarge"
                )
            )
        ]
    )
    
    bundle.add_resource(job)
    return bundle
```

## Essential DABs Commands

### Bundle Lifecycle Commands
```bash
# Initialize new bundle from template
databricks bundle init

# Validate bundle configuration
databricks bundle validate

# Deploy bundle to target environment
databricks bundle deploy -t dev

# Run a specific job from bundle
databricks bundle run my_job -t dev

# Destroy deployed bundle resources
databricks bundle destroy -t dev

# Generate deployment summary
databricks bundle summary
```

### Resource Management
```bash
# Bind existing resource to bundle
databricks bundle bind job my_existing_job

# Unbind resource from bundle
databricks bundle unbind job my_existing_job

# List all bundle deployments
databricks bundle list
```

### Development Workflow
```bash
# Deploy and watch for changes
databricks bundle deploy -t dev --watch

# Deploy with automatic approval
databricks bundle deploy -t dev --auto-approve

# Generate bundle schema
databricks bundle schema
```

## Testing Strategies

### 1. Configuration Testing
- Validate YAML syntax
- Test variable interpolation
- Verify resource dependencies

### 2. Integration Testing
- Deploy to development environment
- Run smoke tests on deployed resources
- Validate data pipeline outputs

### 3. End-to-End Testing
- Full workflow execution
- Data quality validation
- Performance benchmarking

### 4. Automated Testing in CI/CD
```yaml
# Example GitHub Actions workflow
name: DABs CI/CD
on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      - name: Validate bundle
        run: databricks bundle validate
      
  deploy-dev:
    needs: validate
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to dev
        run: databricks bundle deploy -t dev
      
  deploy-prod:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to production
        run: databricks bundle deploy -t prod
```

## Best Practices

### 1. Bundle Organization
- ✅ Use clear, descriptive bundle names
- ✅ Organize files in logical directory structure
- ✅ Separate configuration from source code
- ✅ Use templates for consistency
- ✅ Document bundle structure in README

### 2. Environment Management
- ✅ Define separate targets for each environment
- ✅ Use environment-specific variables
- ✅ Never hardcode credentials or secrets
- ✅ Use Databricks secrets for sensitive data
- ✅ Implement proper access controls

### 3. Version Control
- ✅ Store bundles in Git repositories
- ✅ Use meaningful commit messages
- ✅ Implement branch protection rules
- ✅ Require code reviews for production changes
- ✅ Tag releases appropriately

### 4. CI/CD Integration
- ✅ Automate bundle validation on PR
- ✅ Deploy automatically to dev on merge
- ✅ Require manual approval for production
- ✅ Implement rollback procedures
- ✅ Monitor deployment metrics

### 5. Migration Strategy
- ✅ Start with simple, low-risk jobs
- ✅ Test thoroughly in development
- ✅ Document migration process
- ✅ Plan for rollback scenarios
- ✅ Communicate changes to stakeholders

### 6. Security Considerations
- ✅ Use Unity Catalog for data governance
- ✅ Implement least-privilege access
- ✅ Rotate credentials regularly
- ✅ Audit bundle deployments
- ✅ Encrypt sensitive configurations

## Troubleshooting Guide

### Common Issues and Solutions

#### Issue: Bundle validation fails
```bash
# Solution: Check YAML syntax and indentation
databricks bundle validate --verbose
```

#### Issue: Deployment fails with permission errors
```bash
# Solution: Verify CLI authentication and workspace permissions
databricks auth profiles
databricks workspace current-user
```

#### Issue: Resource already exists
```bash
# Solution: Use bind command to link existing resource
databricks bundle bind job existing_job_name
```

#### Issue: Variable interpolation not working
```bash
# Solution: Verify variable syntax and scope
# Use ${var.variable_name} or ${bundle.target}
```

#### Issue: Cluster configuration errors
```bash
# Solution: Validate cluster specifications match workspace capabilities
# Check instance types, Spark versions, and region availability
```

## Resources and References

### Official Documentation
- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/)
- [DABs Release Notes](https://docs.databricks.com/release-notes/dev-tools/bundles.html)

### Blog Posts and Tutorials
- [Announcing Python Support for Databricks Asset Bundles](https://www.databricks.com/blog/announcing-python-support-databricks-asset-bundles-streamline-deployments)
- [Announcing Databricks Asset Bundles Now in the Workspace](https://www.databricks.com/blog/announcing-databricks-asset-bundles-now-workspace)
- [Announcing General Availability of Databricks Asset Bundles](https://www.databricks.com/blog/announcing-general-availability-databricks-asset-bundles)

### Community Resources
- [Databricks Community Forum](https://community.databricks.com/)
- [GitHub: Databricks CLI](https://github.com/databricks/cli)
- [Stack Overflow: databricks-asset-bundles tag](https://stackoverflow.com/questions/tagged/databricks-asset-bundles)

### Video Tutorials
- [Databricks Asset Bundle Full Course](https://www.youtube.com/results?search_query=databricks+asset+bundles)
- [DABs Best Practices Webinar Series](https://www.databricks.com/resources/webinar)

## Workshop Success Criteria

By the end of this workshop, participants will be able to:
- ✅ Explain the benefits and architecture of Databricks Asset Bundles
- ✅ Create and configure bundles for various Databricks resources
- ✅ Migrate existing jobs and workflows to DABs
- ✅ Implement multi-environment deployment strategies
- ✅ Set up CI/CD pipelines for automated deployments
- ✅ Use Python to define bundles programmatically
- ✅ Apply best practices for production deployments
- ✅ Troubleshoot common DABs issues
- ✅ Design scalable bundle architectures for complex projects

## Next Steps After Workshop

### Immediate Actions
1. **Audit Current Workspace**: Identify candidates for migration
2. **Create Migration Plan**: Prioritize jobs and set timelines
3. **Set Up Development Environment**: Install tools and configure access
4. **Start Small**: Migrate one simple job to gain confidence

### Short-term Goals (1-2 weeks)
1. Migrate 3-5 non-critical jobs to DABs
2. Set up basic CI/CD pipeline
3. Document migration process for your team
4. Train team members on DABs basics

### Long-term Goals (1-3 months)
1. Migrate all production workloads to DABs
2. Implement comprehensive testing strategies
3. Establish governance and best practices
4. Optimize bundle structure and performance

## Feedback and Support

We value your feedback! Please share your workshop experience and suggestions:
- Workshop feedback form: [Link]
- Questions and discussion: [Link to forum/channel]
- Report issues: [Link to GitHub/support]

## Contributors and Acknowledgments

This workshop was developed to help organizations adopt modern DevOps practices for their data and AI workloads on Databricks. Special thanks to the Databricks engineering team for continuous innovation in the DABs platform.

---

**Workshop Version**: 1.0  
**Last Updated**: January 2026  
**Databricks CLI Version**: 0.218.0+  
**Target Audience**: Data Engineers, Analytics Engineers, MLOps Engineers, DevOps Engineers

---

## License

This workshop material is provided for educational purposes. Please refer to your organization's policies for usage guidelines.
