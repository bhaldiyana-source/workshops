# Basic Job Template

Template for creating a simple Databricks job with DABs.

## Setup Instructions

1. **Copy this template**:
   ```bash
   cp -r templates/basic-job-template my-new-job
   cd my-new-job
   ```

2. **Customize databricks.yml**:
   Replace the following placeholders:
   - `{{project_name}}`: Your project name (e.g., "my-data-pipeline")
   - `{{dev_workspace_url}}`: Dev workspace URL
   - `{{prod_workspace_url}}`: Prod workspace URL
   - `{{job_name}}`: Job identifier (e.g., "daily_report")
   - `{{job_display_name}}`: Display name (e.g., "Daily Report")
   - `{{cron_expression}}`: Schedule (e.g., "0 0 8 * * ?")
   - `{{timezone}}`: Timezone (e.g., "UTC")
   - `{{notification_email}}`: Email for notifications
   - `{{notebook_name}}`: Notebook filename (e.g., "main")
   - `{{viewer_group}}`: Group with view access

3. **Customize src/main.py**:
   Replace placeholders and add your business logic

4. **Deploy**:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```

## Template Features

- ✅ Multi-environment support (dev/prod)
- ✅ Parameterized notebooks
- ✅ Email notifications
- ✅ Scheduled execution
- ✅ Auto-terminating clusters
- ✅ Permissions management
- ✅ Error handling template

## Customization

- Adjust cluster size via `num_workers` variable
- Modify schedule in `schedule.quartz_cron_expression`
- Add more tasks by duplicating task configuration
- Add library dependencies in `libraries` section
