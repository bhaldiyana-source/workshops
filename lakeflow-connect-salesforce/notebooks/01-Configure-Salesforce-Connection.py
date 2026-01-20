# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1: Configure Salesforce Connection
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will configure secure OAuth 2.0 connectivity between Databricks and Salesforce using Lakeflow Connect. You'll store credentials securely, create a connection, and validate authentication.
# MAGIC
# MAGIC **Duration:** 25 minutes
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Store Salesforce OAuth credentials securely in Databricks Secrets
# MAGIC - Create a Salesforce connection using Lakeflow Connect
# MAGIC - Validate connectivity with test queries to Salesforce REST API
# MAGIC - Test authentication flow and token refresh mechanisms
# MAGIC - Troubleshoot common authentication errors
# MAGIC - Apply security best practices for production deployments
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Salesforce Connected App created (see README for setup steps)
# MAGIC - Consumer Key (Client ID) and Consumer Secret from Connected App
# MAGIC - Salesforce username and password + security token
# MAGIC - Databricks workspace with Unity Catalog enabled
# MAGIC - Workspace Admin or Metastore Admin permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Gather Required Credentials
# MAGIC
# MAGIC Before proceeding, ensure you have collected the following information from your Salesforce setup:
# MAGIC
# MAGIC | Credential | Where to Find | Example |
# MAGIC |------------|---------------|---------|
# MAGIC | **Instance URL** | Your Salesforce login URL | `https://yourcompany.my.salesforce.com` |
# MAGIC | **Consumer Key** | Connected App → Manage Consumer Details | `3MVG9...` (long string) |
# MAGIC | **Consumer Secret** | Connected App → Manage Consumer Details | `1234567890...` |
# MAGIC | **Username** | Your Salesforce login email | `user@yourcompany.com` |
# MAGIC | **Password** | Your Salesforce password | `YourPassword` |
# MAGIC | **Security Token** | Setup → Reset My Security Token | `AbCdEfGh123456` |
# MAGIC
# MAGIC ⚠️ **Important:** Password + Security Token must be concatenated (e.g., if password is `Pass123` and token is `TokenABC`, use `Pass123TokenABC`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Configure Databricks Secrets
# MAGIC
# MAGIC ### Why Use Databricks Secrets?
# MAGIC
# MAGIC Never hardcode credentials in notebooks or code. Databricks Secrets provide:
# MAGIC - ✅ Encrypted storage with AES-256 encryption
# MAGIC - ✅ Fine-grained access controls via secret scopes
# MAGIC - ✅ Audit logging of secret access
# MAGIC - ✅ Integration with Azure Key Vault, AWS Secrets Manager, or Databricks-managed secrets
# MAGIC - ✅ Redacted values in notebook outputs
# MAGIC
# MAGIC ### Step 2.1: Create Secret Scope (Using Databricks CLI)
# MAGIC
# MAGIC Run these commands in your local terminal (not in this notebook):
# MAGIC
# MAGIC ```bash
# MAGIC # Install Databricks CLI if not already installed
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # Configure authentication
# MAGIC databricks configure --token
# MAGIC # Enter your Databricks workspace URL and personal access token
# MAGIC
# MAGIC # Create a secret scope
# MAGIC databricks secrets create-scope --scope salesforce_credentials
# MAGIC ```
# MAGIC
# MAGIC ### Step 2.2: Store Salesforce Credentials
# MAGIC
# MAGIC ```bash
# MAGIC # Store Consumer Key
# MAGIC databricks secrets put --scope salesforce_credentials --key consumer_key
# MAGIC # Paste your Consumer Key when prompted, save and exit
# MAGIC
# MAGIC # Store Consumer Secret
# MAGIC databricks secrets put --scope salesforce_credentials --key consumer_secret
# MAGIC # Paste your Consumer Secret when prompted, save and exit
# MAGIC
# MAGIC # Store Username
# MAGIC databricks secrets put --scope salesforce_credentials --key username
# MAGIC # Paste your Salesforce username when prompted, save and exit
# MAGIC
# MAGIC # Store Password + Security Token (concatenated)
# MAGIC databricks secrets put --scope salesforce_credentials --key password_token
# MAGIC # Paste your password + security token when prompted, save and exit
# MAGIC
# MAGIC # Store Instance URL
# MAGIC databricks secrets put --scope salesforce_credentials --key instance_url
# MAGIC # Paste your Salesforce instance URL when prompted, save and exit
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Verify Secrets Are Stored
# MAGIC
# MAGIC Run the cell below to verify secrets exist (values will be redacted in output):

# COMMAND ----------

# Verify secrets are accessible (values will be redacted)
try:
    consumer_key = dbutils.secrets.get(scope="salesforce_credentials", key="consumer_key")
    consumer_secret = dbutils.secrets.get(scope="salesforce_credentials", key="consumer_secret")
    username = dbutils.secrets.get(scope="salesforce_credentials", key="username")
    password_token = dbutils.secrets.get(scope="salesforce_credentials", key="password_token")
    instance_url = dbutils.secrets.get(scope="salesforce_credentials", key="instance_url")
    
    print("✅ All secrets successfully retrieved!")
    print(f"   Consumer Key: [REDACTED]")
    print(f"   Consumer Secret: [REDACTED]")
    print(f"   Username: [REDACTED]")
    print(f"   Password+Token: [REDACTED]")
    print(f"   Instance URL: {instance_url}")
    
except Exception as e:
    print(f"❌ Error retrieving secrets: {e}")
    print("\nPlease ensure you have:")
    print("1. Created the 'salesforce_credentials' secret scope")
    print("2. Stored all required secrets with correct key names")
    print("3. Granted yourself access to the secret scope")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Test Salesforce Authentication
# MAGIC
# MAGIC ### Understanding OAuth 2.0 Password Flow
# MAGIC
# MAGIC Salesforce supports multiple OAuth flows. We'll use the **Username-Password Flow** for simplicity:
# MAGIC
# MAGIC ```
# MAGIC 1. Client sends credentials to Salesforce token endpoint
# MAGIC 2. Salesforce validates and returns access_token + refresh_token
# MAGIC 3. Client uses access_token for API requests
# MAGIC 4. When access_token expires, use refresh_token to get new one
# MAGIC ```
# MAGIC
# MAGIC **Token Endpoint:** `https://login.salesforce.com/services/oauth2/token`  
# MAGIC (Use `https://test.salesforce.com/...` for sandbox environments)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.1: Obtain Access Token

# COMMAND ----------

import requests
import json
from pyspark.sql import Row

# Retrieve credentials from secrets
consumer_key = dbutils.secrets.get(scope="salesforce_credentials", key="consumer_key")
consumer_secret = dbutils.secrets.get(scope="salesforce_credentials", key="consumer_secret")
username = dbutils.secrets.get(scope="salesforce_credentials", key="username")
password_token = dbutils.secrets.get(scope="salesforce_credentials", key="password_token")
instance_url = dbutils.secrets.get(scope="salesforce_credentials", key="instance_url")

# OAuth token endpoint (use test.salesforce.com for sandbox)
token_url = "https://login.salesforce.com/services/oauth2/token"

# Request parameters for password flow
auth_params = {
    "grant_type": "password",
    "client_id": consumer_key,
    "client_secret": consumer_secret,
    "username": username,
    "password": password_token
}

try:
    # Request access token
    print("🔐 Requesting OAuth token from Salesforce...")
    response = requests.post(token_url, data=auth_params)
    response.raise_for_status()
    
    # Parse response
    token_data = response.json()
    access_token = token_data["access_token"]
    instance_url_from_token = token_data["instance_url"]
    
    print("✅ Authentication successful!")
    print(f"   Instance URL: {instance_url_from_token}")
    print(f"   Access Token: {access_token[:20]}... [REDACTED]")
    print(f"   Token Type: {token_data.get('token_type', 'Bearer')}")
    
    # Store for later use
    spark.conf.set("salesforce.access.token", access_token)
    spark.conf.set("salesforce.instance.url", instance_url_from_token)
    
except requests.exceptions.HTTPError as e:
    print(f"❌ Authentication failed!")
    print(f"   Status Code: {e.response.status_code}")
    print(f"   Error: {e.response.text}")
    print("\n💡 Common issues:")
    print("   - Invalid Consumer Key/Secret (check Connected App)")
    print("   - Incorrect username/password")
    print("   - Missing or wrong security token")
    print("   - IP restrictions on Connected App")
    print("   - User not assigned to Connected App profile")
except Exception as e:
    print(f"❌ Unexpected error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Test API Connectivity with Sample Query

# COMMAND ----------

# Test query: Get basic org information
def test_salesforce_connection(access_token, instance_url):
    """
    Test Salesforce API connectivity by querying organization details
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Query organization information
        print("📡 Testing API connectivity...")
        org_query = f"{instance_url}/services/data/v59.0/query?q=SELECT+Id,Name,OrganizationType+FROM+Organization"
        response = requests.get(org_query, headers=headers)
        response.raise_for_status()
        
        org_data = response.json()
        if org_data["totalSize"] > 0:
            org_record = org_data["records"][0]
            print("✅ API connection successful!")
            print(f"   Organization ID: {org_record['Id']}")
            print(f"   Organization Name: {org_record['Name']}")
            print(f"   Organization Type: {org_record['OrganizationType']}")
            return True
        else:
            print("⚠️ Query returned no results")
            return False
            
    except requests.exceptions.HTTPError as e:
        print(f"❌ API request failed!")
        print(f"   Status Code: {e.response.status_code}")
        print(f"   Error: {e.response.text}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

# Execute test
access_token = spark.conf.get("salesforce.access.token")
instance_url = spark.conf.get("salesforce.instance.url")
connection_valid = test_salesforce_connection(access_token, instance_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.3: Query Available Salesforce Objects

# COMMAND ----------

def list_salesforce_objects(access_token, instance_url, object_type="standard"):
    """
    List available Salesforce objects (standard or custom)
    
    Args:
        access_token: OAuth access token
        instance_url: Salesforce instance URL
        object_type: "standard", "custom", or "all"
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Get object metadata
        print(f"📋 Fetching {object_type} Salesforce objects...")
        sobjects_url = f"{instance_url}/services/data/v59.0/sobjects"
        response = requests.get(sobjects_url, headers=headers)
        response.raise_for_status()
        
        sobjects_data = response.json()
        
        # Filter based on type
        objects = []
        for obj in sobjects_data["sobjects"]:
            obj_name = obj["name"]
            is_custom = obj["custom"]
            
            if object_type == "all":
                objects.append((obj_name, "Custom" if is_custom else "Standard", obj.get("label", "")))
            elif object_type == "custom" and is_custom:
                objects.append((obj_name, "Custom", obj.get("label", "")))
            elif object_type == "standard" and not is_custom:
                objects.append((obj_name, "Standard", obj.get("label", "")))
        
        # Display as DataFrame
        df = spark.createDataFrame(objects, ["API_Name", "Type", "Label"])
        print(f"✅ Found {len(objects)} {object_type} objects")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching objects: {e}")
        return None

# List standard objects (commonly used in CDC)
access_token = spark.conf.get("salesforce.access.token")
instance_url = spark.conf.get("salesforce.instance.url")

standard_objects_df = list_salesforce_objects(access_token, instance_url, "standard")

# Display common objects
if standard_objects_df:
    common_objects = ["Account", "Contact", "Lead", "Opportunity", "Case", "Campaign", "Task", "Event"]
    display(standard_objects_df.filter(standard_objects_df.API_Name.isin(common_objects)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Create Lakeflow Connect Connection
# MAGIC
# MAGIC ### Understanding Lakeflow Connect for Salesforce
# MAGIC
# MAGIC Lakeflow Connect provides managed connectivity to Salesforce with:
# MAGIC - Automated token refresh
# MAGIC - API rate limit management
# MAGIC - Retry logic for transient failures
# MAGIC - Integration with Unity Catalog for governance
# MAGIC
# MAGIC ### Step 4.1: Define Connection Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connection Configuration Parameters
# MAGIC
# MAGIC ```python
# MAGIC connection_config = {
# MAGIC     "name": "salesforce_production",
# MAGIC     "connection_type": "salesforce",
# MAGIC     "options": {
# MAGIC         "clientId": "<consumer_key>",
# MAGIC         "clientSecret": "<consumer_secret>",
# MAGIC         "username": "<username>",
# MAGIC         "password": "<password_with_token>",
# MAGIC         "instanceUrl": "<instance_url>",
# MAGIC         "apiVersion": "59.0"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ⚠️ **Note:** At the time of this workshop's creation, Salesforce connector configuration in Lakeflow Connect may vary by Databricks platform. Refer to official documentation for the latest connection syntax.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Validate Connection and API Limits

# COMMAND ----------

def check_api_limits(access_token, instance_url):
    """
    Check current API usage and limits
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Query API usage from Limits endpoint
        limits_url = f"{instance_url}/services/data/v59.0/limits"
        response = requests.get(limits_url, headers=headers)
        response.raise_for_status()
        
        limits_data = response.json()
        
        # Extract relevant limits
        daily_api_requests = limits_data.get("DailyApiRequests", {})
        
        print("📊 API Usage Information:")
        print(f"   Daily API Limit: {daily_api_requests.get('Max', 'N/A')}")
        print(f"   Used: {daily_api_requests.get('Remaining', 'N/A')}")
        
        remaining = daily_api_requests.get("Remaining", 0)
        max_calls = daily_api_requests.get("Max", 1)
        usage_pct = ((max_calls - remaining) / max_calls * 100) if max_calls > 0 else 0
        
        print(f"   Usage: {usage_pct:.1f}%")
        
        if usage_pct > 80:
            print("   ⚠️ Warning: API usage above 80%")
        elif usage_pct > 90:
            print("   🔴 Critical: API usage above 90%")
        else:
            print("   ✅ API usage healthy")
            
        return limits_data
        
    except Exception as e:
        print(f"❌ Error checking API limits: {e}")
        return None

# Check current API usage
access_token = spark.conf.get("salesforce.access.token")
instance_url = spark.conf.get("salesforce.instance.url")
limits_info = check_api_limits(access_token, instance_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Connection Security Best Practices
# MAGIC
# MAGIC ### Production Checklist
# MAGIC
# MAGIC #### 1. Secret Management
# MAGIC - ✅ Use Databricks Secrets (never hardcode credentials)
# MAGIC - ✅ Implement secret rotation policy (every 6-12 months)
# MAGIC - ✅ Use Azure Key Vault or AWS Secrets Manager for enterprise deployments
# MAGIC - ✅ Grant minimal access to secret scopes (principle of least privilege)
# MAGIC
# MAGIC #### 2. Salesforce Connected App Security
# MAGIC - ✅ Enable IP restrictions to limit access from known Databricks IPs
# MAGIC - ✅ Set refresh token policy to "Refresh token is valid until revoked"
# MAGIC - ✅ Use "Admin approved users are pre-authorized" for permitted users
# MAGIC - ✅ Enable "Require Secret for Web Server Flow" if using web flow
# MAGIC
# MAGIC #### 3. User Permissions
# MAGIC - ✅ Create dedicated Salesforce integration user
# MAGIC - ✅ Grant read-only permissions (avoid write unless necessary)
# MAGIC - ✅ Limit object and field-level security to only required data
# MAGIC - ✅ Document which objects and fields are accessed
# MAGIC
# MAGIC #### 4. Network Security
# MAGIC - ✅ Use PrivateLink/Private Endpoint for Salesforce (if available)
# MAGIC - ✅ Configure VPN or network peering for on-premises connections
# MAGIC - ✅ Implement network security groups to restrict egress
# MAGIC
# MAGIC #### 5. Monitoring and Auditing
# MAGIC - ✅ Enable Salesforce Event Monitoring (EventLogFile)
# MAGIC - ✅ Track API usage daily to avoid hitting limits
# MAGIC - ✅ Set up alerts for authentication failures
# MAGIC - ✅ Log connection access in Databricks audit logs
# MAGIC - ✅ Review Connected App usage reports monthly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Troubleshooting Common Issues
# MAGIC
# MAGIC ### Authentication Failures
# MAGIC
# MAGIC | Error | Cause | Solution |
# MAGIC |-------|-------|----------|
# MAGIC | `invalid_client_id` | Consumer Key incorrect | Verify Consumer Key from Connected App |
# MAGIC | `invalid_client` | Consumer Secret incorrect | Verify Consumer Secret from Connected App |
# MAGIC | `authentication failure` | Wrong password/token | Check password and security token |
# MAGIC | `invalid_grant` | Security token changed | Reset security token and update secret |
# MAGIC | `IP restricted` | Request from unapproved IP | Add Databricks IPs to Connected App or relax restrictions |
# MAGIC | `user not assigned` | User not in Connected App | Add user profile/permission set to Connected App |
# MAGIC
# MAGIC ### API Issues
# MAGIC
# MAGIC | Error | Cause | Solution |
# MAGIC |-------|-------|----------|
# MAGIC | `REQUEST_LIMIT_EXCEEDED` | Daily API limit reached | Wait for limit reset or upgrade Salesforce edition |
# MAGIC | `UNABLE_TO_LOCK_ROW` | Record locked by another process | Implement retry logic with exponential backoff |
# MAGIC | `INVALID_SESSION_ID` | Access token expired | Implement token refresh logic |
# MAGIC
# MAGIC ### Network Issues
# MAGIC
# MAGIC | Error | Cause | Solution |
# MAGIC |-------|-------|----------|
# MAGIC | Connection timeout | Network connectivity | Check firewall rules and security groups |
# MAGIC | SSL certificate error | Certificate validation | Verify SSL certificates and trust chain |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What You Accomplished ✅
# MAGIC
# MAGIC In this lab, you:
# MAGIC 1. Stored Salesforce credentials securely in Databricks Secrets
# MAGIC 2. Obtained OAuth access token using password flow
# MAGIC 3. Tested API connectivity with sample queries
# MAGIC 4. Listed available Salesforce objects
# MAGIC 5. Checked API usage and limits
# MAGIC 6. Learned security best practices for production deployments
# MAGIC
# MAGIC ### Key Takeaways 💡
# MAGIC
# MAGIC - **Security First**: Always use Databricks Secrets for credential storage
# MAGIC - **OAuth 2.0**: Salesforce uses OAuth for secure authentication
# MAGIC - **API Limits**: Monitor daily API usage to avoid hitting limits
# MAGIC - **Token Management**: Access tokens expire; implement refresh logic
# MAGIC - **Testing**: Always validate connectivity before building pipelines
# MAGIC
# MAGIC ### Next Steps 🚀
# MAGIC
# MAGIC Proceed to **Lab 2: Ingest Salesforce Data** where you will:
# MAGIC - Configure ingestion pipelines for Salesforce objects
# MAGIC - Implement incremental refresh with SystemModstamp
# MAGIC - Ingest data into Unity Catalog Delta tables
# MAGIC - Handle relationships between objects
# MAGIC - Use Bulk API for large data extractions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Need Help?** Refer to the troubleshooting guide at `resources/troubleshooting-guide.md`
