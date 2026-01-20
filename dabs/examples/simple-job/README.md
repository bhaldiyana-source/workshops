# Simple Job Example

This example demonstrates a basic Databricks job managed with DABs.

## Structure

```
simple-job/
├── databricks.yml     # Bundle configuration
├── src/
│   └── main.py       # Main notebook
└── README.md         # This file
```

## Features

- Single task job
- Parameterized notebook
- Environment-specific configuration
- Email notifications
- Scheduled execution

## Usage

```bash
# Validate
databricks bundle validate -t dev

# Deploy
databricks bundle deploy -t dev

# Run
databricks bundle run simple_job -t dev
```

## Customization

1. Update `databricks.yml` with your workspace URLs
2. Modify `src/main.py` with your logic
3. Adjust schedule as needed
4. Add email addresses for notifications
