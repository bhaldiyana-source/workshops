# Salesforce Lakeflow Connect Troubleshooting Guide

## Table of Contents
- [Authentication Issues](#authentication-issues)
- [Connection Problems](#connection-problems)
- [Data Ingestion Errors](#data-ingestion-errors)
- [API Limit Issues](#api-limit-issues)
- [Performance Problems](#performance-problems)
- [Data Quality Issues](#data-quality-issues)

---

## Authentication Issues

### Error: `invalid_client_id`

**Symptom:** OAuth authentication fails with `invalid_client_id` error

**Cause:** Consumer Key (Client ID) is incorrect or not found

**Solutions:**
1. **Verify Consumer Key:**
   - Navigate to Salesforce Setup → App Manager
   - Find your Connected App and click dropdown → View
   - Click "Manage Consumer Details"
   - Copy the exact Consumer Key (it's a long string starting with `3MVG...`)

2. **Check for Typos:**
   - Consumer Keys are case-sensitive
   - Ensure no extra spaces before/after the key
   - Verify you're using the Consumer Key, not Consumer Secret

3. **Verify Connected App Status:**
   - Ensure the Connected App is active (not deleted)
   - Check that OAuth is enabled in the Connected App settings

**Test:**
```bash
# Re-store the Consumer Key in Databricks Secrets
databricks secrets put --scope salesforce_credentials --key consumer_key
# Paste the correct key and save
```

---

### Error: `invalid_client` or `invalid_client_credentials`

**Symptom:** OAuth authentication fails with `invalid_client` error

**Cause:** Consumer Secret is incorrect

**Solutions:**
1. **Regenerate Consumer Secret:**
   - In Salesforce Setup → App Manager → Your Connected App
   - Click "Manage Consumer Details"
   - Note the current Consumer Secret
   - If necessary, regenerate it (be aware this will invalidate existing integrations)

2. **Update Secret in Databricks:**
   ```bash
   databricks secrets put --scope salesforce_credentials --key consumer_secret
   # Paste the correct secret
   ```

3. **Check Secret Expiry:**
   - Some Salesforce configurations expire secrets after a period
   - Verify with your Salesforce admin if secret rotation policies are in place

---

### Error: `authentication failure` or `invalid_grant`

**Symptom:** OAuth fails with authentication failure message

**Cause:** Incorrect username, password, or security token

**Solutions:**
1. **Verify Username:**
   - Use your Salesforce login username (usually an email)
   - Ensure it's the full username, not a display name

2. **Check Password + Security Token:**
   - Password and security token must be concatenated without space
   - Example: If password is `Pass123` and token is `AbcDef987`, use `Pass123AbcDef987`

3. **Reset Security Token:**
   - In Salesforce: Setup → Personal Setup → My Personal Information → Reset My Security Token
   - Check your email for the new token
   - Update Databricks secret with new password+token combination

4. **Verify Account Status:**
   - Ensure the user account is active
   - Check for password expiry
   - Confirm account is not locked due to failed login attempts

**Test:**
```python
# Test in notebook
username = dbutils.secrets.get(scope="salesforce_credentials", key="username")
password_token = dbutils.secrets.get(scope="salesforce_credentials", key="password_token")
print(f"Username: {username}")
print(f"Password+Token length: {len(password_token)}")  # Should be > 8 characters
```

---

### Error: `IP restricted` or `login_must_use_security_token`

**Symptom:** Authentication fails due to IP restrictions

**Cause:** Request is coming from an IP address not whitelisted in the Connected App or user profile

**Solutions:**
1. **Add Databricks IPs to Connected App:**
   - In Connected App: Edit Policies → IP Relaxation
   - Select "Relax IP restrictions" OR add specific Databricks IP ranges

2. **Find Databricks IP Addresses:**
   - Check Databricks workspace network settings
   - Contact Databricks support for current NAT gateway IPs
   - Alternatively, temporarily set to "Relax IP restrictions" for testing

3. **Check User Profile IP Restrictions:**
   - Setup → Profiles → [User's Profile] → Login IP Ranges
   - Add Databricks IPs or remove restrictions for the integration user

4. **Use Dedicated Integration User:**
   - Create a separate Salesforce user for integrations
   - Assign a profile without IP restrictions
   - Use this user's credentials in Databricks

---

### Error: `user not assigned to permission set`

**Symptom:** User cannot access Connected App

**Cause:** User is not assigned to the Connected App

**Solutions:**
1. **Assign User to Connected App:**
   - Navigate to the Connected App
   - Click "Manage" → "Manage Profiles" or "Manage Permission Sets"
   - Add the user's profile or permission set
   - Save changes

2. **Verify OAuth Policies:**
   - In Connected App: Edit Policies
   - Ensure "Permitted Users" is set appropriately:
     - "All users may self-authorize" (for testing)
     - "Admin approved users are pre-authorized" (for production - requires step 1 above)

---

## Connection Problems

### Error: Connection timeout

**Symptom:** Requests to Salesforce timeout after 30-60 seconds

**Cause:** Network connectivity issues between Databricks and Salesforce

**Solutions:**
1. **Check Instance URL:**
   ```python
   instance_url = dbutils.secrets.get(scope="salesforce_credentials", key="instance_url")
   print(f"Instance URL: {instance_url}")
   # Should be https://yourcompany.my.salesforce.com (no trailing slash)
   ```

2. **Verify Network Connectivity:**
   - Test from notebook:
   ```python
   import requests
   response = requests.get("https://login.salesforce.com")
   print(f"Status: {response.status_code}")  # Should be 200
   ```

3. **Check Firewall Rules:**
   - Ensure outbound HTTPS (port 443) is allowed from Databricks
   - Verify no proxy configuration is blocking requests

4. **Use Correct Login URL:**
   - Production: `https://login.salesforce.com`
   - Sandbox: `https://test.salesforce.com`
   - Custom domains: `https://yourdomain.my.salesforce.com`

---

### Error: SSL Certificate verification failed

**Symptom:** SSL/TLS handshake errors

**Cause:** Certificate validation issues

**Solutions:**
1. **Update Databricks Runtime:**
   - Use DBR 14.3 LTS or later with updated SSL certificates

2. **Check Salesforce Custom Domain:**
   - If using custom domain, ensure SSL certificates are valid
   - Verify domain is properly configured in Salesforce

3. **Temporary Workaround (Not Recommended for Production):**
   ```python
   import requests
   from requests.adapters import HTTPAdapter
   from requests.packages.urllib3.util.retry import Retry
   
   session = requests.Session()
   # Only for debugging - do not use in production
   session.verify = False  
   ```

---

## Data Ingestion Errors

### Error: `INVALID_FIELD` in SOQL query

**Symptom:** Query fails with invalid field name error

**Cause:** Field does not exist or user doesn't have access

**Solutions:**
1. **Verify Field API Name:**
   - Use Describe API to get correct field names:
   ```python
   describe_url = f"{instance_url}/services/data/v59.0/sobjects/Account/describe"
   headers = {"Authorization": f"Bearer {access_token}"}
   response = requests.get(describe_url, headers=headers)
   fields = [f["name"] for f in response.json()["fields"]]
   print(fields)
   ```

2. **Check Field-Level Security:**
   - In Salesforce: Setup → Object Manager → [Object] → Fields & Relationships
   - Click on the field → "Set Field-Level Security"
   - Ensure the integration user's profile has "Visible" checked

3. **Custom Fields:**
   - Custom fields end with `__c`
   - Check spelling: `CustomField__c` not `CustomField`

---

### Error: `REQUEST_LIMIT_EXCEEDED`

**Symptom:** API requests fail with limit exceeded error

**Cause:** Daily API limit reached

**Solutions:**
1. **Check Current Usage:**
   ```python
   limits_url = f"{instance_url}/services/data/v59.0/limits"
   response = requests.get(limits_url, headers={"Authorization": f"Bearer {access_token}"})
   api_limits = response.json()["DailyApiRequests"]
   print(f"Used: {api_limits['Max'] - api_limits['Remaining']} of {api_limits['Max']}")
   ```

2. **Optimize Queries:**
   - Use selective WHERE clauses
   - Reduce frequency of full refreshes
   - Implement incremental refresh with SystemModstamp

3. **Use Bulk API:**
   - Bulk API doesn't count against same limits as REST API
   - Recommended for objects with >2,000 records

4. **Upgrade Salesforce Edition:**
   - Developer: 15,000 calls/day
   - Professional: 1,000 + 1,000 per user
   - Enterprise: 1,000 + 1,000 per user
   - Unlimited: 5,000 + 5,000 per user

5. **Wait for Limit Reset:**
   - API limits reset at midnight Pacific Time
   - Schedule ingestion jobs for off-peak hours

**See:** [salesforce-api-limits.md](salesforce-api-limits.md) for detailed limits

---

### Error: `UNABLE_TO_LOCK_ROW`

**Symptom:** Query or update fails with row lock error

**Cause:** Record is locked by another process

**Solutions:**
1. **Implement Retry Logic:**
   ```python
   import time
   from requests.exceptions import HTTPError
   
   def query_with_retry(url, headers, max_retries=3):
       for attempt in range(max_retries):
           try:
               response = requests.get(url, headers=headers)
               response.raise_for_status()
               return response.json()
           except HTTPError as e:
               if "UNABLE_TO_LOCK_ROW" in str(e) and attempt < max_retries - 1:
                   time.sleep(2 ** attempt)  # Exponential backoff
                   continue
               raise
   ```

2. **Schedule During Low Activity:**
   - Run ingestion jobs when Salesforce users are least active
   - Avoid peak business hours

3. **Use Bulk API:**
   - Bulk API is less likely to encounter lock contention
   - Processes records asynchronously

---

### Error: Empty result set (no records returned)

**Symptom:** Query executes successfully but returns 0 records

**Cause:** Multiple possible reasons

**Solutions:**
1. **Check WHERE Clause:**
   ```sql
   -- Too restrictive?
   SELECT Id FROM Account WHERE IsDeleted = false AND CreatedDate = TODAY
   ```

2. **Verify Object Permissions:**
   - Integration user must have Read permission on the object
   - Check: Setup → Profiles → [User Profile] → Object Settings

3. **Check Sharing Rules:**
   - User might not have access to records due to sharing rules
   - Consider using a user with "View All" permission

4. **Test Without Filters:**
   ```sql
   SELECT Id, Name FROM Account LIMIT 10
   ```

5. **Verify Data Exists:**
   - Log in to Salesforce UI as the integration user
   - Navigate to the object and verify records are visible

---

## API Limit Issues

### Monitoring API Usage

**Best Practice:** Proactively monitor API usage to avoid hitting limits

```python
def monitor_api_usage(access_token, instance_url):
    """Check and alert on API usage"""
    limits_url = f"{instance_url}/services/data/v59.0/limits"
    response = requests.get(limits_url, headers={"Authorization": f"Bearer {access_token}"})
    
    daily_api = response.json()["DailyApiRequests"]
    used = daily_api["Max"] - daily_api["Remaining"]
    usage_pct = (used / daily_api["Max"]) * 100
    
    if usage_pct > 90:
        print(f"🔴 CRITICAL: API usage at {usage_pct:.1f}%")
    elif usage_pct > 80:
        print(f"⚠️ WARNING: API usage at {usage_pct:.1f}%")
    else:
        print(f"✅ API usage healthy: {usage_pct:.1f}%")
    
    return usage_pct

# Run this before major ingestion jobs
monitor_api_usage(access_token, instance_url)
```

---

## Performance Problems

### Slow query execution

**Symptom:** SOQL queries take >30 seconds

**Solutions:**
1. **Add Selective Filters:**
   - Use indexed fields in WHERE clause (Id, Name, CreatedDate, SystemModstamp)
   - Avoid querying all records without filters

2. **Limit Field Selection:**
   - Don't select fields you don't need
   - Avoid large text fields if possible

3. **Use Bulk API for Large Datasets:**
   - REST API: Good for <2,000 records
   - Bulk API: Designed for large extractions

4. **Partition Large Queries:**
   ```sql
   -- Instead of one large query
   SELECT ... FROM Opportunity WHERE CreatedDate >= 2020-01-01
   
   -- Partition by date range
   SELECT ... FROM Opportunity WHERE CreatedDate >= 2020-01-01 AND CreatedDate < 2021-01-01
   SELECT ... FROM Opportunity WHERE CreatedDate >= 2021-01-01 AND CreatedDate < 2022-01-01
   ```

---

### Slow data ingestion to Delta tables

**Symptom:** Writing to Delta tables takes excessive time

**Solutions:**
1. **Optimize Cluster Configuration:**
   - Use compute-optimized instance types
   - Scale cluster based on data volume
   - Enable autoscaling

2. **Partition Delta Tables:**
   ```python
   df.write.format("delta") \
       .partitionBy("CreatedDate") \
       .mode("append") \
       .saveAsTable("sf_accounts")
   ```

3. **Optimize Merge Operations:**
   ```python
   # Use Delta merge instead of full replace
   from delta.tables import DeltaTable
   
   target = DeltaTable.forName(spark, "sf_accounts")
   target.alias("target").merge(
       new_df.alias("source"),
       "target.Id = source.Id"
   ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
   ```

4. **Batch Processing:**
   - Process records in batches of 10,000-50,000
   - Avoid very small frequent writes

---

## Data Quality Issues

### Missing records after ingestion

**Symptom:** Source has more records than target Delta table

**Diagnostic Steps:**
1. **Count Records:**
   ```python
   # Salesforce
   soql = "SELECT COUNT(Id) FROM Account WHERE IsDeleted = false"
   sf_count = query_salesforce_rest(soql, access_token, instance_url)[0]["expr0"]
   
   # Delta table
   delta_count = spark.table("sf_accounts").count()
   
   print(f"Salesforce: {sf_count}, Delta: {delta_count}, Diff: {sf_count - delta_count}")
   ```

2. **Check for Query Limits:**
   - REST API returns max 2,000 records per request
   - Ensure pagination is implemented correctly

3. **Verify Incremental Logic:**
   - Check checkpoint timestamp is correct
   - Ensure no records fall between checkpoint gaps

4. **Look for Errors in Logs:**
   - Review Databricks job logs for exceptions
   - Check for partial failures

---

### Duplicate records in Delta table

**Symptom:** Multiple rows with same Id

**Causes & Solutions:**
1. **Missing Merge Logic:**
   - Always use MERGE for upsert operations
   - Don't use APPEND mode for incremental loads

2. **Incorrect Merge Key:**
   ```python
   # Ensure merge on unique identifier
   target.alias("target").merge(
       source.alias("source"),
       "target.Id = source.Id"  # Id must be unique
   )
   ```

3. **Clean Up Duplicates:**
   ```python
   from pyspark.sql.window import Window
   from pyspark.sql.functions import row_number, desc
   
   # Keep only latest record per Id
   window = Window.partitionBy("Id").orderBy(desc("SystemModstamp"))
   deduplicated = df.withColumn("rn", row_number().over(window)) \
       .filter(col("rn") == 1) \
       .drop("rn")
   
   deduplicated.write.format("delta").mode("overwrite").saveAsTable("sf_accounts")
   ```

---

### Null values in non-nullable fields

**Symptom:** Required fields contain NULL values

**Solutions:**
1. **Check Field-Level Security:**
   - User might not have access to the field
   - Results in NULL even if value exists

2. **Handle Nulls in ETL:**
   ```python
   from pyspark.sql.functions import coalesce, lit
   
   # Replace NULL with default value
   df = df.withColumn("Industry", coalesce(col("Industry"), lit("Unknown")))
   ```

3. **Validate Before Insert:**
   ```python
   # Check for nulls in required fields
   required_fields = ["Id", "Name", "CreatedDate"]
   for field in required_fields:
       null_count = df.filter(col(field).isNull()).count()
       if null_count > 0:
           print(f"⚠️ Warning: {null_count} records with NULL {field}")
   ```

---

## Getting Help

### Enable Debug Logging

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Add to your code
logger.debug(f"Requesting: {url}")
logger.debug(f"Headers: {headers}")
logger.debug(f"Response: {response.text}")
```

### Collect Diagnostic Information

When reporting issues, include:
- Databricks Runtime version
- Salesforce edition (Developer, Professional, Enterprise, Unlimited)
- API version used (e.g., v59.0)
- Complete error message and stack trace
- Sample SOQL query that fails
- Relevant code snippet
- API usage statistics

### Resources
- [Databricks Documentation](https://docs.databricks.com/)
- [Salesforce REST API Guide](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/)
- [Databricks Community Forum](https://community.databricks.com/)
- [Salesforce Developer Forum](https://developer.salesforce.com/forums)

---

**Last Updated:** January 2026  
**Workshop Version:** 1.0
