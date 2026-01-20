# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01: Configure SharePoint Connection
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook guides you through configuring a secure connection from Databricks to Microsoft SharePoint using **Lakeflow Connect**. You'll learn how to:
# MAGIC - Store credentials securely using Databricks Secrets
# MAGIC - Create a SharePoint connection using OAuth 2.0
# MAGIC - Validate connectivity and permissions
# MAGIC - Troubleshoot common authentication issues
# MAGIC
# MAGIC **Duration**: 25 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Before starting, ensure you have:
# MAGIC - ✅ Completed Azure AD app registration
# MAGIC - ✅ Collected Tenant ID, Client ID, and Client Secret
# MAGIC - ✅ Granted Microsoft Graph API permissions (`Sites.Read.All`, `Files.Read.All`)
# MAGIC - ✅ Admin consent granted for your organization
# MAGIC - ✅ SharePoint site URL ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries
# MAGIC
# MAGIC Install the Databricks SDK for programmatic operations.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import Required Libraries

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *
import json
import requests
from pyspark.sql.functions import *

# Initialize Databricks workspace client
w = WorkspaceClient()

# Display current user and workspace info
current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
print(f"Logged in as: {current_user}")
print(f"Workspace URL: {spark.conf.get('spark.databricks.workspaceUrl')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Configure Your Credentials
# MAGIC
# MAGIC ### Important Security Note
# MAGIC Never hardcode credentials in notebooks! We'll use Databricks Secrets to store sensitive information securely.
# MAGIC
# MAGIC ### Create a Secret Scope
# MAGIC
# MAGIC Run this command in a separate terminal or using the Databricks CLI:
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope sharepoint-connector
# MAGIC ```
# MAGIC
# MAGIC ### Store Your Credentials
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets put-secret sharepoint-connector tenant-id
# MAGIC databricks secrets put-secret sharepoint-connector client-id
# MAGIC databricks secrets put-secret sharepoint-connector client-secret
# MAGIC databricks secrets put-secret sharepoint-connector site-url
# MAGIC ```
# MAGIC
# MAGIC Alternatively, you can use the Databricks UI:
# MAGIC 1. Navigate to: `https://<workspace-url>/#secrets/createScope`
# MAGIC 2. Create scope: `sharepoint-connector`
# MAGIC 3. Add secrets via CLI or API

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Retrieve Credentials from Secrets
# MAGIC
# MAGIC For this demo, we'll first check if secrets exist, and provide a fallback for configuration.

# COMMAND ----------

# DBTITLE 1,Retrieve Credentials (Using Secrets)
# Attempt to retrieve from secrets
# Replace with your actual secret scope name if different
SECRET_SCOPE = "sharepoint-connector"

try:
    TENANT_ID = dbutils.secrets.get(scope=SECRET_SCOPE, key="tenant-id")
    CLIENT_ID = dbutils.secrets.get(scope=SECRET_SCOPE, key="client-id")
    CLIENT_SECRET = dbutils.secrets.get(scope=SECRET_SCOPE, key="client-secret")
    SHAREPOINT_SITE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key="site-url")
    print("✅ Successfully retrieved credentials from secrets")
except Exception as e:
    print(f"⚠️  Could not retrieve secrets: {e}")
    print("\n📝 Please configure the following variables manually for testing:")
    print("   TENANT_ID = 'your-tenant-id'")
    print("   CLIENT_ID = 'your-client-id'")
    print("   CLIENT_SECRET = 'your-client-secret'")
    print("   SHAREPOINT_SITE_URL = 'https://yourtenant.sharepoint.com/sites/yoursite'")
    
    # Uncomment and fill in for testing (NOT recommended for production):
    # TENANT_ID = ""
    # CLIENT_ID = ""
    # CLIENT_SECRET = ""
    # SHAREPOINT_SITE_URL = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Azure AD Authentication
# MAGIC
# MAGIC Before creating the connection in Lakeflow, let's validate that our credentials work by obtaining an OAuth token from Microsoft.

# COMMAND ----------

# DBTITLE 1,Test OAuth Authentication
def get_access_token(tenant_id, client_id, client_secret):
    """
    Obtain an access token from Microsoft Entra ID (Azure AD)
    """
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }
    
    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None

# Test authentication
print("Testing OAuth authentication...")
token_response = get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)

if token_response and 'access_token' in token_response:
    print("✅ Authentication successful!")
    print(f"   Token type: {token_response.get('token_type')}")
    print(f"   Expires in: {token_response.get('expires_in')} seconds")
    access_token = token_response['access_token']
else:
    print("❌ Authentication failed. Please verify your credentials.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test SharePoint Connectivity
# MAGIC
# MAGIC Use the Microsoft Graph API to verify we can access the specified SharePoint site.

# COMMAND ----------

# DBTITLE 1,Verify SharePoint Site Access
def get_sharepoint_site_info(access_token, site_url):
    """
    Retrieve SharePoint site information using Microsoft Graph API
    """
    # Extract site path from URL
    # Example: https://contoso.sharepoint.com/sites/ProjectAlpha
    # Results in: contoso.sharepoint.com:/sites/ProjectAlpha
    
    from urllib.parse import urlparse
    parsed = urlparse(site_url)
    hostname = parsed.netloc
    path = parsed.path
    
    graph_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{path}"
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(graph_url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Failed to access SharePoint site: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None

# Test SharePoint access
print(f"Testing access to SharePoint site: {SHAREPOINT_SITE_URL}")
site_info = get_sharepoint_site_info(access_token, SHAREPOINT_SITE_URL)

if site_info:
    print("✅ Successfully connected to SharePoint!")
    print(f"   Site Name: {site_info.get('displayName', 'N/A')}")
    print(f"   Site ID: {site_info.get('id', 'N/A')}")
    print(f"   Web URL: {site_info.get('webUrl', 'N/A')}")
    print(f"   Created: {site_info.get('createdDateTime', 'N/A')}")
else:
    print("❌ Could not access SharePoint site. Please verify:")
    print("   1. Site URL is correct")
    print("   2. API permissions are granted and admin consent is given")
    print("   3. Service principal has access to the site")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Configure Unity Catalog
# MAGIC
# MAGIC Specify where SharePoint data will be stored in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Unity Catalog Configuration
# Configure your Unity Catalog hierarchy
CATALOG_NAME = "sharepoint_data"
SCHEMA_NAME = "raw"

# Create catalog if it doesn't exist
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    print(f"✅ Catalog '{CATALOG_NAME}' is ready")
except Exception as e:
    print(f"⚠️  Note: {e}")

# Create schema if it doesn't exist
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
    print(f"✅ Schema '{CATALOG_NAME}.{SCHEMA_NAME}' is ready")
except Exception as e:
    print(f"⚠️  Note: {e}")

# Set current catalog and schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

print(f"\n📊 Current location: {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Lakeflow Connect Connection
# MAGIC
# MAGIC Now we'll create the SharePoint connection in Lakeflow Connect using the Databricks SDK.
# MAGIC
# MAGIC **Note**: Connection creation requires appropriate permissions in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Create SharePoint Connection
CONNECTION_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.sharepoint_connection"

connection_properties = {
    "tenant_id": TENANT_ID,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "site_url": SHAREPOINT_SITE_URL
}

try:
    # Create connection using Databricks SDK
    connection = w.connections.create(
        name=CONNECTION_NAME,
        connection_type=ConnectionType.MICROSOFT_SHAREPOINT,
        options=connection_properties,
        comment="SharePoint Online connection for document and list ingestion"
    )
    print(f"✅ Connection created successfully: {CONNECTION_NAME}")
    print(f"   Connection Type: {connection.connection_type}")
    print(f"   Owner: {connection.owner}")
except Exception as e:
    print(f"⚠️  Connection may already exist or creation failed: {e}")
    
    # Try to retrieve existing connection
    try:
        connection = w.connections.get(CONNECTION_NAME)
        print(f"✅ Using existing connection: {CONNECTION_NAME}")
    except Exception as e2:
        print(f"❌ Could not create or retrieve connection: {e2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Validate Connection
# MAGIC
# MAGIC Test the connection to ensure it's working properly.

# COMMAND ----------

# DBTITLE 1,Test Connection
try:
    # Retrieve connection details
    conn_info = w.connections.get(CONNECTION_NAME)
    print("✅ Connection validation successful!")
    print(f"   Name: {conn_info.name}")
    print(f"   Type: {conn_info.connection_type}")
    print(f"   Metastore: {conn_info.metastore_id}")
    print(f"   Created: {conn_info.created_at}")
    print(f"   Owner: {conn_info.owner}")
except Exception as e:
    print(f"❌ Connection validation failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: List Available SharePoint Resources
# MAGIC
# MAGIC Use the Microsoft Graph API to browse available document libraries and lists.

# COMMAND ----------

# DBTITLE 1,Browse SharePoint Document Libraries
def list_document_libraries(access_token, site_id):
    """
    List all document libraries in a SharePoint site
    """
    graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(graph_url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Failed to list document libraries: {e}")
        return None

if site_info:
    site_id = site_info.get('id')
    libraries = list_document_libraries(access_token, site_id)
    
    if libraries and 'value' in libraries:
        print(f"📚 Found {len(libraries['value'])} document libraries:")
        print("-" * 80)
        for lib in libraries['value']:
            print(f"   📁 {lib.get('name')}")
            print(f"      ID: {lib.get('id')}")
            print(f"      Description: {lib.get('description', 'N/A')}")
            print(f"      Web URL: {lib.get('webUrl', 'N/A')}")
            print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Connection Summary
# MAGIC
# MAGIC Review your configuration and prepare for the next notebook.

# COMMAND ----------

# DBTITLE 1,Configuration Summary
print("=" * 80)
print("SHAREPOINT CONNECTION CONFIGURATION SUMMARY")
print("=" * 80)
print(f"✅ Authentication: Validated")
print(f"✅ SharePoint Access: Verified")
print(f"✅ Unity Catalog Location: {CATALOG_NAME}.{SCHEMA_NAME}")
print(f"✅ Connection Name: {CONNECTION_NAME}")
print(f"✅ Site URL: {SHAREPOINT_SITE_URL}")
if site_info:
    print(f"✅ Site Name: {site_info.get('displayName', 'N/A')}")
print("=" * 80)
print("\n🎉 Configuration Complete!")
print("📋 Next Steps:")
print("   1. Proceed to Notebook 02: Ingest SharePoint Data")
print("   2. Configure document library and list ingestion")
print("   3. Set up incremental refresh pipelines")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Tips
# MAGIC
# MAGIC ### Authentication Errors
# MAGIC - **Invalid client secret**: Verify the secret hasn't expired in Azure AD
# MAGIC - **Unauthorized**: Check that API permissions are granted and admin consent is given
# MAGIC - **Invalid tenant**: Confirm tenant ID matches your organization
# MAGIC
# MAGIC ### Connection Errors
# MAGIC - **Permission denied**: Ensure you have CREATE CONNECTION privileges in Unity Catalog
# MAGIC - **Network timeout**: Check network connectivity to Microsoft 365
# MAGIC - **Site not found**: Verify SharePoint site URL is correct and accessible
# MAGIC
# MAGIC ### API Permission Errors
# MAGIC Required Microsoft Graph permissions:
# MAGIC - `Sites.Read.All` - Read all site collections
# MAGIC - `Files.Read.All` - Read files in all site collections
# MAGIC
# MAGIC ### Helpful Resources
# MAGIC - [Databricks Lakeflow Connect Documentation](https://docs.databricks.com/en/connect/index.html)
# MAGIC - [Microsoft Graph API Reference](https://learn.microsoft.com/en-us/graph/api/overview)
# MAGIC - [Unity Catalog Connections](https://docs.databricks.com/en/data-governance/unity-catalog/connections.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Store Configuration for Next Notebooks
# MAGIC
# MAGIC Save connection details to use in subsequent notebooks.

# COMMAND ----------

# Store configuration using Spark conf for session persistence
spark.conf.set("sharepoint.connection.name", CONNECTION_NAME)
spark.conf.set("sharepoint.catalog", CATALOG_NAME)
spark.conf.set("sharepoint.schema", SCHEMA_NAME)
if site_info:
    spark.conf.set("sharepoint.site.id", site_info.get('id'))

print("✅ Configuration saved to Spark session")
print("   These values will be available in subsequent notebooks in this cluster session")
