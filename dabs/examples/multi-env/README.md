# Multi-Environment Example

Example showing configuration for dev, staging, and production environments.

## Features

- Three environment targets
- Environment-specific variables
- Service principal for production
- Parameterized configurations

## Usage

```bash
# Deploy to dev
databricks bundle deploy -t dev

# Deploy to staging
databricks bundle deploy -t staging

# Deploy to prod
databricks bundle deploy -t prod
```
