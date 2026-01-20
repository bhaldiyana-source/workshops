# Lab 1: Hello World Databricks App

## Overview

This lab introduces you to building Databricks Apps with Streamlit. You'll learn the fundamentals of:
- On-Behalf-Of (OBO) authentication
- Environment variable management
- Database connectivity
- User context access

## Learning Objectives

By completing this lab, you will:
- ✅ Understand how Databricks Apps handle authentication automatically
- ✅ Learn to access user context and environment variables
- ✅ Connect to Databricks SQL Warehouses using OBO tokens
- ✅ Implement proper error handling and logging
- ✅ Build a simple interactive Streamlit interface

## Prerequisites

- Access to a Databricks workspace (AWS, Azure, or GCP)
- A running SQL Warehouse
- Databricks CLI installed and configured

## Project Structure

```
lab1-hello-world/
├── app.py              # Main Streamlit application
├── app.yaml            # Databricks app configuration
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Configuration

### Step 1: Update app.yaml

Edit `app.yaml` and replace the placeholder with your SQL Warehouse ID:

```yaml
resources:
  - name: warehouse_connection
    warehouse:
      warehouse_id: "YOUR_WAREHOUSE_ID_HERE"  # Replace this
```

To find your SQL Warehouse ID:
1. Navigate to **SQL Warehouses** in your Databricks workspace
2. Click on your warehouse
3. Copy the **Warehouse ID** from the **Connection Details** tab

### Step 2: Review Dependencies

The `requirements.txt` includes:
- `streamlit`: Web application framework
- `databricks-sql-connector`: SQL Warehouse connectivity
- `databricks-sdk`: Databricks SDK for Python
- `pandas`: Data manipulation
- `pyyaml`: YAML configuration parsing

## Deployment

### Using Databricks CLI

```bash
# Navigate to the lab directory
cd apps/lab1-hello-world

# Deploy the app
databricks apps deploy /Users/<your-email>/lab1-hello-world

# View app logs
databricks apps logs /Users/<your-email>/lab1-hello-world

# Update the app (after making changes)
databricks apps update /Users/<your-email>/lab1-hello-world
```

### Using Databricks UI

1. Navigate to **Workspace** in your Databricks workspace
2. Right-click on a folder and select **Create** → **App**
3. Upload the lab files or connect to a Git repository
4. Configure the SQL Warehouse in the app settings
5. Click **Deploy**

## Local Testing (Optional)

You can test the app locally before deploying:

```bash
# Set environment variables
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_USER_EMAIL="your-email@company.com"

# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run app.py
```

**Note:** Local testing won't replicate OBO behavior exactly. The token you provide will be used for all queries.

## Features

### 1. User Information Display
- Shows the authenticated user's display name, email, and user ID
- Demonstrates OBO authentication in action

### 2. Environment Variables
- View Databricks platform-provided environment variables
- Understand what's automatically available to your app

### 3. Connection Test
- Interactive button to test SQL Warehouse connectivity
- Executes a simple query to verify permissions
- Shows query results in a table

### 4. Educational Content
- Expandable sections explaining key concepts
- Architecture diagrams
- Links to next steps

## Key Concepts Explained

### On-Behalf-Of (OBO) Permissions

When a user accesses your Databricks App:

1. User authenticates to Databricks workspace
2. App receives the user's OAuth token automatically via `DATABRICKS_TOKEN` environment variable
3. All database queries execute with the user's permissions
4. Unity Catalog enforces row-level and column-level security based on user permissions

**Code Example:**

```python
from utils.auth import get_user_context

# Get current user's information
user = get_user_context()
print(f"Hello, {user.display_name}!")  # Personalized greeting
```

### Environment Variables

Databricks Apps automatically provides these environment variables:

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_TOKEN` | User's OAuth token (OBO) |
| `DATABRICKS_SERVER_HOSTNAME` | Server hostname |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP path |
| `DATABRICKS_USER_EMAIL` | User's email address |
| `DATABRICKS_USER_ID` | User's unique identifier |

### Database Connectivity

The app uses shared utilities for database connections:

```python
from utils.database import DatabaseConnection

# Create connection (automatically uses OBO token)
db = DatabaseConnection()

# Execute query
results = db.execute_query("SELECT * FROM my_catalog.my_schema.my_table")
```

Benefits:
- Automatic retry logic with exponential backoff
- Proper resource cleanup
- Connection pooling
- Error handling

## Troubleshooting

### Issue: "Missing required connection parameters"

**Solution:** Ensure your `app.yaml` has the correct `warehouse_id` configured.

### Issue: "Permission Denied" errors

**Solution:** 
- Verify you have `USE` permission on the SQL Warehouse
- Check Unity Catalog permissions for any tables you're querying
- Ensure the warehouse is running

### Issue: App won't start

**Solution:**
- Check app logs: `databricks apps logs /path/to/app`
- Verify all dependencies in `requirements.txt` are available
- Ensure port 8080 is not blocked

### Issue: Connection timeouts

**Solution:**
- Check if the SQL Warehouse is running
- Increase the warehouse size if needed
- Review connection timeout settings in `DatabaseConnection`

## Security Best Practices

✅ **DO:**
- Use environment variables for configuration
- Let Databricks manage OBO tokens automatically
- Implement proper error handling
- Log user actions for audit trails

❌ **DON'T:**
- Hardcode credentials or tokens
- Store sensitive data in source code
- Override OBO authentication without reason
- Expose detailed error messages to end users

## Next Steps

After completing this lab:

1. **Lab 2: Data Explorer** - Build an interactive Unity Catalog browser
2. **Lab 3: ML Model Interface** - Create a Gradio interface for MLflow models
3. **Lab 4: Multi-User Dashboard** - Develop a Dash dashboard with user-specific views
4. **Lab 5: RESTful API** - Build a FastAPI-based data API

## Additional Resources

- [Databricks Apps Documentation](https://docs.databricks.com/apps/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Unity Catalog Permissions](https://docs.databricks.com/data-governance/unity-catalog/)
- [SQL Warehouses](https://docs.databricks.com/sql/admin/sql-endpoints.html)

## Support

For issues or questions:
- Check the main [Apps Workshop README](../README.md)
- Review Databricks documentation
- Contact your Databricks administrator

---

**Happy Building! 🚀**
