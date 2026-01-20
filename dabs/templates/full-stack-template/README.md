# Full-Stack Template

Complete template with DLT pipeline, jobs, ML experiments, and clusters.

## Setup Instructions

1. **Copy template**:
   ```bash
   cp -r templates/full-stack-template my-project
   cd my-project
   ```

2. **Customize databricks.yml**:
   Replace all `{{placeholders}}`

3. **Add your logic**:
   - `src/pipelines/`: DLT notebooks
   - `src/jobs/`: Job notebooks

4. **Deploy**:
   ```bash
   databricks bundle deploy -t dev
   ```

## Features

- ✅ Complete medallion architecture
- ✅ Scheduled analytics jobs
- ✅ ML experiment tracking
- ✅ Development cluster
- ✅ Multi-environment support
- ✅ Production service principal
