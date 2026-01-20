# Databricks Apps Workshop

## Overview
This workshop provides a comprehensive guide to building and deploying interactive web applications on Databricks Apps. Learn how to create data-driven UIs, manage permissions, and deploy production-ready applications using modern web frameworks.

---

## Table of Contents
1. [Introduction to Databricks Apps](#introduction-to-databricks-apps)
2. [Architecture and Components](#architecture-and-components)
3. [Setting Up Your Development Environment](#setting-up-your-development-environment)
4. [Building Your First App](#building-your-first-app)
5. [Authentication and Authorization](#authentication-and-authorization)
6. [On Behalf Of Permissions](#on-behalf-of-permissions)
7. [Data Access Patterns](#data-access-patterns)
8. [App Configuration and Environment Variables](#app-configuration-and-environment-variables)
9. [Supported Frameworks](#supported-frameworks)
10. [Deployment and Management](#deployment-and-management)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

---

## Introduction to Databricks Apps

### What are Databricks Apps?

Databricks Apps is a platform feature that enables you to build and host custom web applications directly within your Databricks workspace. These applications can:

- **Access Databricks Data**: Connect directly to Unity Catalog tables, volumes, and other data sources
- **Leverage Databricks Compute**: Execute queries and computations using Databricks SQL warehouses
- **Integrate with MLflow**: Serve and interact with machine learning models
- **Provide Custom UIs**: Create interactive dashboards and tools for end users

### Key Benefits

- **Unified Platform**: Keep your data, analytics, and applications in one place
- **Enterprise Security**: Built-in authentication, authorization, and audit logging
- **Scalability**: Leverage Databricks infrastructure for serving applications
- **Developer Flexibility**: Use popular web frameworks like Streamlit, Dash, Gradio, Flask, and FastAPI

---

## Architecture and Components

### High-Level Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Databricks Workspace                │
│                                                       │
│  ┌──────────────────┐         ┌──────────────────┐  │
│  │   Databricks     │         │   Unity Catalog  │  │
│  │   Apps Runtime   │────────▶│   (Data Layer)   │  │
│  └──────────────────┘         └──────────────────┘  │
│           │                                           │
│           │                    ┌──────────────────┐  │
│           └───────────────────▶│  SQL Warehouses  │  │
│                                └──────────────────┘  │
│                                                       │
│  ┌──────────────────┐         ┌──────────────────┐  │
│  │   Your App UI    │────────▶│   MLflow Models  │  │
│  │  (Web Framework) │         │   (Optional)     │  │
│  └──────────────────┘         └──────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Core Components

1. **App Source Code**: Your application code (Python, JavaScript, etc.)
2. **App Configuration File**: `app.yaml` defining runtime settings
3. **Dependencies**: Requirements file (`requirements.txt`, `package.json`, etc.)
4. **Static Assets**: Images, CSS, JavaScript files (optional)
5. **Environment Variables**: Secure configuration management

---

## Setting Up Your Development Environment

### Prerequisites

- Active Databricks workspace (AWS, Azure, or GCP)
- Databricks CLI installed and configured
- Python 3.8+ or Node.js (depending on your framework)
- Git for version control

### Installing Databricks CLI

```bash
# Install via pip
pip install databricks-cli

# Configure authentication
databricks configure --token
```

### Project Structure

```
my-databricks-app/
├── app.yaml                 # App configuration
├── app.py                   # Main application file
├── requirements.txt         # Python dependencies
├── .databricks/            # CLI configuration (optional)
└── README.md               # Documentation
```

---

## Building Your First App

### Step 1: Create App Configuration (`app.yaml`)

```yaml
# app.yaml
command: ["python", "app.py"]

resources:
  - name: warehouse_connection
    warehouse:
      warehouse_id: "your-warehouse-id"

env:
  - name: APP_NAME
    value: "My First App"
```

### Step 2: Create Your Application

**Example: Simple Streamlit App**

```python
# app.py
import streamlit as st
from databricks import sql
import os

st.title("My First Databricks App")

# Connect to Databricks SQL Warehouse
with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM main.default.my_table LIMIT 10")
        result = cursor.fetchall()
        st.dataframe(result)
```

### Step 3: Define Dependencies

```txt
# requirements.txt
streamlit>=1.28.0
databricks-sql-connector>=2.9.0
pandas>=2.0.0
```

### Step 4: Test Locally (Optional)

```bash
# Set environment variables
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_TOKEN="your-token"

# Run the app
streamlit run app.py
```

---

## Authentication and Authorization

### Authentication Methods

Databricks Apps automatically handles authentication for users accessing your application:

1. **Workspace Authentication**: Users must be authenticated to your Databricks workspace
2. **SSO Integration**: Supports SAML, OIDC, and other enterprise SSO providers
3. **Service Principal Access**: Apps can run as service principals for automated workflows

### Getting User Context

```python
# Access the authenticated user's information
import os

# Get current user
user_email = os.getenv("DATABRICKS_USER_EMAIL")
user_id = os.getenv("DATABRICKS_USER_ID")

print(f"Current user: {user_email}")
```

### Unity Catalog Permissions

Apps respect Unity Catalog permissions. Users can only access data they have been granted permission to view.

```python
# Query respects user's Unity Catalog permissions
cursor.execute("SELECT * FROM catalog.schema.table")
# Returns only data the user has SELECT permission on
```

---

## On Behalf Of Permissions

### What is "On Behalf Of"?

**On Behalf Of (OBO)** is a critical security feature that ensures:
- App queries execute with the **end user's credentials**, not the app owner's
- Each user sees only data they're authorized to access
- Audit logs correctly attribute actions to the actual user

### How It Works

```
User Access Flow:
1. User authenticates to Databricks workspace
2. User opens your Databricks App
3. App receives user's OAuth token automatically
4. App executes queries "on behalf of" the user
5. Data access governed by user's Unity Catalog permissions
```

### Implementation

**Automatic OBO (Recommended)**

```python
# app.py
from databricks import sql
import os

# Token is automatically injected - represents the current user
connection = sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")  # This is the USER's token
)

# All queries run with user's permissions
with connection.cursor() as cursor:
    # This respects the current user's permissions
    cursor.execute("SELECT * FROM sensitive_data.hr.salaries")
    results = cursor.fetchall()
```

### OBO Best Practices

1. **Always Use Environment Variables**: Never hardcode tokens or credentials
2. **Trust the Platform**: Let Databricks handle token management
3. **Test with Multiple Users**: Verify different users see different data
4. **Audit Logging**: All queries are logged with the actual user's identity

### OBO vs Service Principal

| Aspect | On Behalf Of | Service Principal |
|--------|--------------|-------------------|
| Permissions | End user's permissions | Service principal's permissions |
| Audit Trail | Shows actual user | Shows service principal |
| Use Case | Interactive apps | Automated processes |
| Data Access | User-specific | Uniform access |

### When NOT to Use OBO

Consider using a **Service Principal** instead when:
- Building automated reports or dashboards
- All users should see the same data regardless of their permissions
- App performs background processing or scheduled tasks

```yaml
# app.yaml - Using Service Principal
resources:
  - name: warehouse_connection
    warehouse:
      warehouse_id: "your-warehouse-id"
    service_principal:
      client_id: "your-service-principal-id"
```

---

## Data Access Patterns

### Accessing Unity Catalog Tables

```python
from databricks.sdk import WorkspaceClient
from databricks import sql

# Method 1: SQL Connector (Recommended for SQL queries)
with sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM catalog.schema.table")
    df = cursor.fetchall_arrow().to_pandas()

# Method 2: Databricks SDK
w = WorkspaceClient()
tables = w.tables.list(catalog_name="main", schema_name="default")
for table in tables:
    print(table.name)
```

### Accessing Volumes

```python
import os

# Unity Catalog Volumes are mounted at /Volumes
volume_path = "/Volumes/catalog/schema/volume_name"

# Read files from volume
with open(f"{volume_path}/data.csv", "r") as f:
    data = f.read()
```

### Accessing MLflow Models

```python
import mlflow

# Load a model from Model Registry
model_name = "catalog.schema.model_name"
model_version = "1"

model = mlflow.pyfunc.load_model(f"models:/{model_name}/{model_version}")

# Make predictions
predictions = model.predict(input_data)
```

---

## App Configuration and Environment Variables

### App Configuration File (`app.yaml`)

```yaml
# Complete app.yaml example
command: ["streamlit", "run", "app.py", "--server.port", "8080"]

resources:
  - name: warehouse
    warehouse:
      warehouse_id: "abc123def456"
  
  - name: model_serving
    model_serving_endpoint:
      endpoint_name: "my-model-endpoint"

env:
  - name: APP_ENVIRONMENT
    value: "production"
  
  - name: LOG_LEVEL
    value: "INFO"
  
  # Reference secrets from Databricks Secrets
  - name: API_KEY
    secret:
      scope: "my-scope"
      key: "api-key"

# Optional: Specify custom port
# port: 8080

# Optional: Health check endpoint
# health_check:
#   endpoint: "/health"
```

### Environment Variables Reference

**Automatically Available:**

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_TOKEN` | Current user's OAuth token (OBO) |
| `DATABRICKS_SERVER_HOSTNAME` | Server hostname for connections |
| `DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `DATABRICKS_USER_EMAIL` | Authenticated user's email |
| `DATABRICKS_USER_ID` | Authenticated user's ID |

### Using Secrets

```python
import os

# Access secrets via environment variables
api_key = os.getenv("API_KEY")  # From secrets scope

# Never hardcode secrets!
# ❌ BAD: api_key = "abc123xyz"
# ✅ GOOD: api_key = os.getenv("API_KEY")
```

---

## Supported Frameworks

### Streamlit

**Best For**: Data exploration, dashboards, ML demos

```python
# app.py
import streamlit as st

st.title("Streamlit on Databricks")
st.write("Hello, World!")

# requirements.txt
# streamlit>=1.28.0
```

### Dash (Plotly)

**Best For**: Interactive dashboards, complex visualizations

```python
# app.py
from dash import Dash, html, dcc
import plotly.express as px

app = Dash(__name__)

app.layout = html.Div([
    html.H1("Dash on Databricks"),
    dcc.Graph(figure=px.line(x=[1,2,3], y=[1,4,9]))
])

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8080)

# requirements.txt
# dash>=2.14.0
# plotly>=5.17.0
```

### Gradio

**Best For**: ML model interfaces, quick prototypes

```python
# app.py
import gradio as gr

def greet(name):
    return f"Hello {name}!"

demo = gr.Interface(fn=greet, inputs="text", outputs="text")
demo.launch(server_name="0.0.0.0", server_port=8080)

# requirements.txt
# gradio>=4.0.0
```

### Flask

**Best For**: RESTful APIs, custom web apps

```python
# app.py
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "Flask on Databricks"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

# requirements.txt
# flask>=3.0.0
```

### FastAPI

**Best For**: High-performance APIs, async operations

```python
# app.py
from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "FastAPI on Databricks"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

# requirements.txt
# fastapi>=0.104.0
# uvicorn>=0.24.0
```

---

## Deployment and Management

### Deploying Your App

#### Using Databricks CLI

```bash
# Navigate to your app directory
cd my-databricks-app

# Deploy the app
databricks apps deploy /Users/your-email@company.com/my-app

# Update an existing app
databricks apps update /Users/your-email@company.com/my-app

# Delete an app
databricks apps delete /Users/your-email@company.com/my-app
```

#### Using Databricks UI

1. Navigate to **Workspace** in your Databricks workspace
2. Right-click on a folder and select **Create** → **App**
3. Upload your app files or connect to a Git repository
4. Configure app settings (warehouse, environment variables)
5. Click **Deploy**

### Managing Apps

```bash
# List all apps
databricks apps list

# Get app details
databricks apps get /Users/your-email@company.com/my-app

# View app logs
databricks apps logs /Users/your-email@company.com/my-app

# Start/Stop app
databricks apps start /Users/your-email@company.com/my-app
databricks apps stop /Users/your-email@company.com/my-app
```

### App Permissions

```bash
# Grant access to users or groups
databricks apps permissions update /Users/your-email@company.com/my-app \
  --json '{
    "access_control_list": [
      {
        "user_name": "user@company.com",
        "permission_level": "CAN_USE"
      },
      {
        "group_name": "data-team",
        "permission_level": "CAN_MANAGE"
      }
    ]
  }'
```

**Permission Levels:**
- `CAN_VIEW`: View app configuration
- `CAN_USE`: Access and use the app
- `CAN_MANAGE`: Modify and manage the app

---

## Best Practices

### Security

1. **Never Hardcode Credentials**: Always use environment variables and secrets
2. **Use OBO by Default**: Let queries run with user permissions
3. **Validate Input**: Sanitize all user inputs to prevent injection attacks
4. **Implement HTTPS**: Databricks Apps use HTTPS by default
5. **Regular Updates**: Keep dependencies updated for security patches

```python
# ❌ BAD: Hardcoded credentials
connection = sql.connect(
    server_hostname="workspace.databricks.com",
    access_token="dapi123456789"  # DON'T DO THIS!
)

# ✅ GOOD: Environment variables
connection = sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    access_token=os.getenv("DATABRICKS_TOKEN")
)
```

### Performance

1. **Connection Pooling**: Reuse database connections
2. **Caching**: Cache expensive computations
3. **Lazy Loading**: Load data only when needed
4. **Optimize Queries**: Use efficient SQL queries
5. **Resource Management**: Choose appropriate warehouse size

```python
# Streamlit caching example
import streamlit as st

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    with sql.connect(...) as conn:
        return pd.read_sql("SELECT * FROM large_table", conn)
```

### Code Organization

```
databricks-app/
├── app.yaml
├── requirements.txt
├── app.py                    # Main entry point
├── config/
│   └── settings.py          # Configuration
├── components/
│   ├── __init__.py
│   ├── charts.py            # Reusable chart components
│   └── tables.py            # Reusable table components
├── utils/
│   ├── __init__.py
│   ├── database.py          # Database utilities
│   └── auth.py              # Authentication utilities
└── tests/
    └── test_app.py          # Unit tests
```

### Logging and Monitoring

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Log important events
logger.info(f"User {user_email} accessed the app")
logger.error(f"Failed to fetch data: {error_message}")
```

### Error Handling

```python
import streamlit as st
from databricks import sql
import traceback

try:
    with sql.connect(...) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table")
        data = cursor.fetchall()
except Exception as e:
    logger.error(f"Database error: {str(e)}")
    st.error("Failed to load data. Please try again or contact support.")
    # Don't expose internal errors to users
    # Show user-friendly messages instead
```

---

## Troubleshooting

### Common Issues

#### Issue: App Won't Start

**Symptoms**: App deployment fails or app doesn't respond

**Solutions**:
- Check `app.yaml` syntax
- Verify all dependencies in `requirements.txt`
- Check app logs: `databricks apps logs /path/to/app`
- Ensure correct port is specified (default: 8080)

#### Issue: Permission Denied Errors

**Symptoms**: "Access denied" when querying tables

**Solutions**:
- Verify user has Unity Catalog permissions
- Check warehouse access permissions
- Ensure OBO token is being used correctly
- Grant necessary permissions in Unity Catalog

```sql
-- Grant permissions in Unity Catalog
GRANT SELECT ON TABLE catalog.schema.table TO `user@company.com`;
GRANT USAGE ON CATALOG catalog TO `user@company.com`;
GRANT USAGE ON SCHEMA catalog.schema TO `user@company.com`;
```

#### Issue: Connection Timeouts

**Symptoms**: SQL queries timeout or hang

**Solutions**:
- Check SQL warehouse status (ensure it's running)
- Increase warehouse size if needed
- Optimize query performance
- Implement connection retry logic

```python
import time
from databricks import sql

def connect_with_retry(max_retries=3):
    for attempt in range(max_retries):
        try:
            return sql.connect(
                server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                access_token=os.getenv("DATABRICKS_TOKEN")
            )
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            raise
```

#### Issue: Environment Variables Not Available

**Symptoms**: `None` values when accessing `os.getenv()`

**Solutions**:
- Verify variables are defined in `app.yaml`
- Check secret scope and key names
- Ensure proper secret permissions
- Test locally with `.env` file

#### Issue: Slow App Performance

**Symptoms**: App is slow to load or respond

**Solutions**:
- Implement caching for expensive operations
- Optimize database queries (use EXPLAIN, add indexes)
- Use appropriate SQL warehouse size
- Profile your code to identify bottlenecks
- Lazy load data components

### Debugging Tips

```python
# Enable debug mode in Streamlit
import streamlit as st

# Show debug information
if st.checkbox("Show Debug Info"):
    st.write("Environment Variables:")
    st.write({
        "HOST": os.getenv("DATABRICKS_HOST"),
        "USER": os.getenv("DATABRICKS_USER_EMAIL"),
        "HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH")
    })
```

### Getting Help

- **Documentation**: [Databricks Apps Documentation](https://docs.databricks.com)
- **Community Forums**: [Databricks Community](https://community.databricks.com)
- **Support**: Contact Databricks support for enterprise help
- **GitHub**: Check framework-specific repositories for issues

---

## Workshop Labs

All labs are **fully implemented and production-ready**! Each includes complete source code, configuration files, and comprehensive documentation.

### Lab 1: Hello World App ✅
**Framework**: Streamlit | **Time**: 30 min | **Level**: Beginner

Build a simple Streamlit app that displays user information and tests database connectivity.

📁 Location: [`lab1-hello-world/`](lab1-hello-world/)  
📖 Documentation: [Lab 1 README](lab1-hello-world/README.md)

**Features**: User context display, connection testing, environment variables, educational content

---

### Lab 2: Data Explorer ✅
**Framework**: Streamlit | **Time**: 60 min | **Level**: Intermediate

Create an interactive data explorer with hierarchical Unity Catalog browsing and custom query execution.

📁 Location: [`lab2-data-explorer/`](lab2-data-explorer/)  
📖 Documentation: [Lab 2 README](lab2-data-explorer/README.md)

**Features**: Catalog browsing, query builder, CSV export, query history, caching

---

### Lab 3: ML Model Interface ✅
**Framework**: Gradio | **Time**: 45 min | **Level**: Intermediate

Build an interactive interface for MLflow models with JSON and CSV prediction support.

📁 Location: [`lab3-ml-model-interface/`](lab3-ml-model-interface/)  
📖 Documentation: [Lab 3 README](lab3-ml-model-interface/README.md)

**Features**: Model loading, JSON/CSV predictions, prediction history, model metadata

---

### Lab 4: Multi-User Dashboard ✅
**Framework**: Dash + Plotly | **Time**: 90 min | **Level**: Advanced

Develop an interactive dashboard with user-specific data views and real-time updates.

📁 Location: [`lab4-multi-user-dashboard/`](lab4-multi-user-dashboard/)  
📖 Documentation: [Lab 4 README](lab4-multi-user-dashboard/README.md)

**Features**: KPI cards, interactive filters, multiple charts, auto-refresh, Bootstrap styling

---

### Lab 5: API with FastAPI ✅
**Framework**: FastAPI | **Time**: 90 min | **Level**: Advanced

Create a production-ready RESTful API with automatic OpenAPI documentation.

📁 Location: [`lab5-api-fastapi/`](lab5-api-fastapi/)  
📖 Documentation: [Lab 5 README](lab5-api-fastapi/README.md)

**Features**: REST endpoints, auto-generated docs, health checks, pagination, CSV/JSON export

---

## 🚀 Quick Start

**New to the workshop?** Start here:

1. 📘 Read the [Quick Start Guide](QUICK_START.md) (5 minutes)
2. 🔧 Deploy Lab 1 to get familiar with the basics
3. 📚 Check the [Workshop Summary](WORKSHOP_SUMMARY.md) for the complete overview

**Ready to dive in?** Each lab folder contains:
- `app.py` - Complete application code
- `app.yaml` - Databricks configuration
- `requirements.txt` - Python dependencies
- `README.md` - Detailed lab instructions

---

## Additional Resources

### Sample Apps Repository
```bash
# Clone sample apps
git clone https://github.com/databricks/databricks-apps-examples.git
```

### Useful Libraries

- **databricks-sql-connector**: SQL warehouse connectivity
- **databricks-sdk**: Comprehensive Python SDK for Databricks
- **mlflow**: Model management and serving
- **pandas**: Data manipulation
- **plotly**: Interactive visualizations
- **streamlit**: Rapid app development

### Related Documentation

- [Unity Catalog Permissions](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [SQL Warehouses](https://docs.databricks.com/sql/admin/sql-endpoints.html)
- [MLflow Model Registry](https://docs.databricks.com/mlflow/model-registry.html)
- [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html)

---

## Conclusion

Databricks Apps enables you to build powerful, secure, and scalable web applications directly on the Databricks platform. By leveraging features like On Behalf Of permissions, Unity Catalog integration, and modern web frameworks, you can create data-driven applications that provide value to your entire organization.

### Key Takeaways

✅ **Unified Platform**: Build apps where your data lives  
✅ **Enterprise Security**: Automatic authentication and fine-grained authorization  
✅ **On Behalf Of**: Queries execute with user permissions  
✅ **Flexible Frameworks**: Use Streamlit, Dash, Gradio, Flask, FastAPI, and more  
✅ **Easy Deployment**: Simple CLI-based deployment and management  

---

## Next Steps

1. **Set up your development environment**
2. **Build your first simple app**
3. **Explore On Behalf Of permissions with multiple test users**
4. **Deploy to production**
5. **Share with your organization**

Happy Building! 🚀
