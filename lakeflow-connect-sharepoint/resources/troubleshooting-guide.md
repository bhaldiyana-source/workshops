# SharePoint Connector Troubleshooting Guide

## Table of Contents
1. [Authentication Issues](#authentication-issues)
2. [Connection Problems](#connection-problems)
3. [API and Permission Errors](#api-and-permission-errors)
4. [Data Ingestion Issues](#data-ingestion-issues)
5. [Performance Problems](#performance-problems)
6. [Network and Connectivity](#network-and-connectivity)
7. [Schema and Data Quality](#schema-and-data-quality)
8. [Common Error Messages](#common-error-messages)

---

## Authentication Issues

### Error: "Invalid client secret"

**Symptoms**:
```
HTTP 401: The provided client credentials are invalid
```

**Possible Causes**:
- Client secret has expired
- Client secret was copied incorrectly
- Wrong client secret is being used

**Solutions**:
1. **Verify secret hasn't expired**:
   - Go to Azure Portal > App registrations > Your app > Certificates & secrets
   - Check expiration date of client secret
   - If expired, generate a new secret

2. **Recreate the secret**:
   ```bash
   # In Azure Portal, create new client secret
   # Copy the value immediately (shown only once)
   # Update Databricks secret
   databricks secrets put-secret sharepoint-connector client-secret
   ```

3. **Verify secret value**:
   - Ensure no leading/trailing spaces
   - Copy the "Value" not the "Secret ID"
   - Check for special characters that might have been escaped

**Prevention**:
- Set calendar reminders for secret expiration
- Use secrets with longer expiration (12-24 months)
- Rotate secrets before expiration

---

### Error: "AADSTS700016: Application not found in the directory"

**Symptoms**:
```
AADSTS700016: Application with identifier 'xxx' was not found in the directory
```

**Possible Causes**:
- Incorrect tenant ID
- Application deleted or not yet propagated
- Application in different tenant

**Solutions**:
1. **Verify Tenant ID**:
   - Azure Portal > Microsoft Entra ID > Overview
   - Copy "Tenant ID" (should match tenant in error)

2. **Verify Application ID**:
   - Azure Portal > App registrations > Your app
   - Copy "Application (client) ID"
   - Ensure it matches what's in your configuration

3. **Check application status**:
   - Ensure app registration shows as "Active"
   - Verify app wasn't deleted
   - Wait 5-10 minutes for propagation if just created

**Verification**:
```python
# Test with correct credentials
token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
print(f"Testing authentication to: {token_url}")
```

---

### Error: "Insufficient privileges to complete the operation"

**Symptoms**:
```
Code: accessDenied
Message: Insufficient privileges to complete the operation.
```

**Possible Causes**:
- Application permissions not granted
- Admin consent not provided
- Using delegated instead of application permissions

**Solutions**:
1. **Grant admin consent**:
   - Azure Portal > App registrations > Your app > API permissions
   - Click "Grant admin consent for [Your Organization]"
   - Confirm the action

2. **Verify permission type**:
   - Ensure permissions are "Application" not "Delegated"
   - Required: `Sites.Read.All`, `Files.Read.All` (Application type)

3. **Check user role**:
   - You must be Global Administrator or Privileged Role Administrator
   - Or have "Cloud Application Administrator" role

**Visual Check**:
API Permissions should show:
```
Sites.Read.All    | Application | Granted for [Org]
Files.Read.All    | Application | Granted for [Org]
```

---

## Connection Problems

### Error: "Connection timed out"

**Symptoms**:
```
ConnectionTimeoutError: Connection to graph.microsoft.com timed out
```

**Possible Causes**:
- Network connectivity issues
- Firewall blocking outbound HTTPS
- DNS resolution problems
- Databricks cluster network configuration

**Solutions**:
1. **Test connectivity**:
   ```python
   import requests
   try:
       response = requests.get("https://graph.microsoft.com/v1.0/$metadata", timeout=10)
       print(f"Status: {response.status_code}")
   except Exception as e:
       print(f"Error: {e}")
   ```

2. **Check firewall rules**:
   - Ensure outbound HTTPS (port 443) is allowed
   - Whitelist domains:
     - `*.microsoftonline.com`
     - `*.microsoft.com`
     - `graph.microsoft.com`
     - `*.sharepoint.com`

3. **Verify DNS resolution**:
   ```bash
   %sh
   nslookup graph.microsoft.com
   nslookup login.microsoftonline.com
   ```

4. **Check cluster configuration**:
   - Review network settings in cluster configuration
   - Verify no proxy configuration is interfering
   - Test from different cluster if available

---

### Error: "Cannot create connection in Unity Catalog"

**Symptoms**:
```
PermissionDenied: User does not have CREATE CONNECTION privilege on catalog
```

**Possible Causes**:
- Insufficient Unity Catalog permissions
- Catalog doesn't exist
- Workspace not enabled for Lakeflow Connect

**Solutions**:
1. **Check permissions**:
   ```sql
   SHOW GRANTS ON CATALOG sharepoint_data;
   ```
   You need: `CREATE CONNECTION` privilege

2. **Grant permissions** (as admin):
   ```sql
   GRANT CREATE CONNECTION ON CATALOG sharepoint_data TO `user@company.com`;
   ```

3. **Verify catalog exists**:
   ```sql
   SHOW CATALOGS;
   CREATE CATALOG IF NOT EXISTS sharepoint_data;
   ```

4. **Check Lakeflow Connect availability**:
   - Lakeflow Connect requires Unity Catalog enabled
   - Available on Databricks Runtime 14.3+
   - Contact workspace admin to verify feature is enabled

---

## API and Permission Errors

### Error: "The user or administrator has not consented"

**Symptoms**:
```
AADSTS65001: The user or administrator has not consented to use the application
```

**Possible Causes**:
- Admin consent not granted
- Consent was revoked
- Application permissions changed

**Solutions**:
1. **Re-grant admin consent**:
   - Azure Portal > App registrations > Your app > API permissions
   - Remove all permissions
   - Re-add required permissions:
     - Microsoft Graph > Application permissions
     - `Sites.Read.All`
     - `Files.Read.All`
   - Click "Grant admin consent"

2. **Verify consent status**:
   - Each permission should show green checkmark
   - Status column should say "Granted for [Organization]"

3. **Wait for propagation**:
   - Changes can take 5-15 minutes to propagate
   - Try authenticating again after waiting

---

### Error: "Access denied to SharePoint site"

**Symptoms**:
```
Access denied. You do not have permission to perform this action or access this resource.
```

**Possible Causes**:
- Service principal lacks access to specific site
- Site permissions not configured
- Site requires additional authentication

**Solutions**:
1. **Grant site access to service principal**:
   ```powershell
   # Using SharePoint Online Management Shell
   Connect-SPOService -Url https://yourtenant-admin.sharepoint.com
   
   # Grant site collection admin
   Set-SPOUser -Site https://yourtenant.sharepoint.com/sites/yoursite `
                -LoginName "Databricks-SharePoint-Connector" `
                -IsSiteCollectionAdmin $true
   ```

2. **Use SharePoint Admin Center**:
   - Navigate to SharePoint Admin Center
   - Select the site
   - Go to Permissions
   - Add the app registration with read permissions

3. **Verify API permissions are sufficient**:
   - `Sites.Read.All` grants access to all sites tenant-wide
   - If restricted, use `Sites.Selected` with explicit site grants

---

### Error: "Throttling - Too Many Requests"

**Symptoms**:
```
HTTP 429: Too Many Requests
Retry-After: 120
```

**Possible Causes**:
- Exceeded Microsoft Graph API throttling limits
- Too many concurrent requests
- No retry logic implemented

**Throttling Limits**:
- **Per app per tenant**: ~5,000 requests per 10 minutes
- **Per user per app**: ~2,000 requests per 10 minutes
- **Per tenant**: Variable, based on tenant size

**Solutions**:
1. **Implement exponential backoff**:
   ```python
   import time
   from requests.exceptions import HTTPError
   
   def make_request_with_retry(url, headers, max_retries=3):
       for attempt in range(max_retries):
           try:
               response = requests.get(url, headers=headers)
               response.raise_for_status()
               return response.json()
           except HTTPError as e:
               if e.response.status_code == 429:
                   retry_after = int(e.response.headers.get('Retry-After', 60))
                   print(f"Throttled. Waiting {retry_after} seconds...")
                   time.sleep(retry_after)
               else:
                   raise
       raise Exception("Max retries exceeded")
   ```

2. **Reduce request frequency**:
   - Add delays between requests: `time.sleep(0.1)`
   - Batch operations where possible
   - Use delta queries instead of full scans

3. **Use pagination efficiently**:
   ```python
   # Use $top to control page size
   params = {
       '$top': 100,  # Don't exceed 1000
       '$skip': 0
   }
   ```

4. **Monitor rate limit headers**:
   ```python
   print(f"Rate Limit: {response.headers.get('RateLimit-Limit')}")
   print(f"Remaining: {response.headers.get('RateLimit-Remaining')}")
   ```

---

## Data Ingestion Issues

### Error: "No data returned from SharePoint"

**Symptoms**:
- Query succeeds but returns 0 records
- Empty DataFrame or list

**Possible Causes**:
- Library or list is actually empty
- Filters excluding all data
- Site ID or library ID incorrect

**Solutions**:
1. **Verify data exists in SharePoint**:
   - Open SharePoint site in browser
   - Navigate to the library/list
   - Confirm items are present

2. **Check IDs are correct**:
   ```python
   # Verify site ID
   print(f"Using Site ID: {SITE_ID}")
   
   # List all libraries
   libraries = list_document_libraries(access_token, SITE_ID)
   for lib in libraries['value']:
       print(f"{lib['name']}: {lib['id']}")
   ```

3. **Remove filters temporarily**:
   ```python
   # Test without filters
   graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
   response = requests.get(graph_url, headers=headers)
   print(f"Items returned: {len(response.json().get('value', []))}")
   ```

4. **Check for hidden items**:
   ```python
   # Include hidden items
   params = {'$filter': 'startswith(name, \'.\')'}
   ```

---

### Error: "Schema mismatch on merge"

**Symptoms**:
```
AnalysisException: Cannot resolve column 'new_column' in table
```

**Possible Causes**:
- SharePoint columns added or removed
- Data type changes
- Column renamed

**Solutions**:
1. **Enable schema evolution**:
   ```python
   df.write \
       .format("delta") \
       .mode("append") \
       .option("mergeSchema", "true") \
       .saveAsTable(table_name)
   ```

2. **Refresh schema manually**:
   ```sql
   -- View current schema
   DESCRIBE TABLE sharepoint_documents;
   
   -- Allow schema evolution
   ALTER TABLE sharepoint_documents 
   SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
   ```

3. **Handle new columns in merge**:
   ```python
   # Add missing columns to target table first
   for col_name in new_df.columns:
       if col_name not in existing_df.columns:
           spark.sql(f"""
               ALTER TABLE {table_name} 
               ADD COLUMN {col_name} STRING
           """)
   ```

---

### Error: "Duplicate key on insert"

**Symptoms**:
```
DuplicateKeyException: Duplicate entry for key 'PRIMARY'
```

**Possible Causes**:
- Running full load instead of merge
- ID column not unique
- Concurrent writes

**Solutions**:
1. **Use merge instead of insert**:
   ```python
   from delta.tables import DeltaTable
   
   delta_table = DeltaTable.forName(spark, table_name)
   delta_table.alias("target").merge(
       new_df.alias("source"),
       "target.id = source.id"
   ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
   ```

2. **De-duplicate before insert**:
   ```python
   # Remove duplicates, keep most recent
   from pyspark.sql.window import Window
   
   window = Window.partitionBy("id").orderBy(col("modified_datetime").desc())
   deduped_df = new_df.withColumn("row_num", row_number().over(window)) \
                      .filter("row_num = 1") \
                      .drop("row_num")
   ```

3. **Check ID uniqueness**:
   ```python
   # Verify IDs are unique
   total = df.count()
   unique = df.select("id").distinct().count()
   print(f"Total: {total}, Unique IDs: {unique}")
   if total != unique:
       print("⚠️  Duplicate IDs detected!")
   ```

---

## Performance Problems

### Issue: "Slow data ingestion"

**Symptoms**:
- Ingestion takes hours for moderate datasets
- API calls timing out
- Cluster appears idle

**Possible Causes**:
- Sequential processing
- No parallelization
- Large page sizes
- Insufficient cluster resources

**Solutions**:
1. **Parallelize API calls**:
   ```python
   from concurrent.futures import ThreadPoolExecutor
   
   def fetch_library_items(library_id):
       # Fetch items from one library
       return get_library_items(access_token, site_id, library_id)
   
   # Parallel processing
   with ThreadPoolExecutor(max_workers=5) as executor:
       results = executor.map(fetch_library_items, library_ids)
   ```

2. **Optimize batch size**:
   ```python
   # Use appropriate $top value
   params = {
       '$top': 200,  # Balance between calls and size
       '$select': 'id,name,size,lastModifiedDateTime'  # Only needed fields
   }
   ```

3. **Use delta queries**:
   ```python
   # More efficient than scanning all items
   graph_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/delta"
   ```

4. **Scale cluster**:
   - Increase worker nodes for parallel processing
   - Use compute-optimized instance types
   - Enable autoscaling

---

### Issue: "High memory usage"

**Symptoms**:
- Out of memory errors
- Cluster crashing
- Slow Spark operations

**Solutions**:
1. **Process in batches**:
   ```python
   batch_size = 1000
   for i in range(0, total_items, batch_size):
       batch_df = spark.createDataFrame(items[i:i+batch_size])
       batch_df.write.format("delta").mode("append").saveAsTable(table_name)
   ```

2. **Avoid collect()**:
   ```python
   # BAD: Brings all data to driver
   all_data = df.collect()
   
   # GOOD: Keep distributed
   df.write.format("delta").saveAsTable(table_name)
   ```

3. **Partition large tables**:
   ```python
   df.write \
       .format("delta") \
       .partitionBy("library_name", "year", "month") \
       .saveAsTable(table_name)
   ```

---

## Network and Connectivity

### Issue: "Intermittent connection failures"

**Symptoms**:
- Requests succeed sometimes, fail others
- "Connection reset by peer" errors
- Timeouts on specific operations

**Solutions**:
1. **Implement robust retry logic**:
   ```python
   from requests.adapters import HTTPAdapter
   from requests.packages.urllib3.util.retry import Retry
   
   session = requests.Session()
   retry = Retry(
       total=5,
       backoff_factor=1,
       status_forcelist=[429, 500, 502, 503, 504]
   )
   adapter = HTTPAdapter(max_retries=retry)
   session.mount('https://', adapter)
   ```

2. **Increase timeouts**:
   ```python
   response = requests.get(url, headers=headers, timeout=300)  # 5 minutes
   ```

3. **Check network stability**:
   ```bash
   %sh
   # Test connectivity
   ping -c 5 graph.microsoft.com
   curl -I https://graph.microsoft.com/v1.0
   ```

---

## Schema and Data Quality

### Issue: "Unexpected null values"

**Symptoms**:
- Critical fields have null values
- Data quality checks failing

**Solutions**:
1. **Handle missing values explicitly**:
   ```python
   from pyspark.sql.functions import coalesce, lit
   
   df = df.withColumn("modified_by", 
                      coalesce(col("modified_by"), lit("Unknown")))
   ```

2. **Add data quality checks**:
   ```python
   # Check for nulls
   null_counts = df.select([
       count(when(col(c).isNull(), c)).alias(c) 
       for c in df.columns
   ])
   display(null_counts)
   ```

3. **Use DLT expectations**:
   ```python
   @dlt.expect_or_drop("valid_name", "name IS NOT NULL")
   @dlt.expect_or_warn("has_size", "size > 0")
   ```

---

## Common Error Messages

### Quick Reference Table

| Error Code | Message | Quick Fix |
|------------|---------|-----------|
| AADSTS700016 | Application not found | Verify tenant ID and app ID |
| AADSTS65001 | User has not consented | Grant admin consent in Azure |
| AADSTS7000215 | Invalid client secret | Generate new secret |
| HTTP 401 | Unauthorized | Check credentials and permissions |
| HTTP 403 | Forbidden | Grant API permissions |
| HTTP 404 | Not found | Verify site URL and IDs |
| HTTP 429 | Too many requests | Implement retry with backoff |
| HTTP 500 | Internal server error | Retry request, may be transient |
| HTTP 503 | Service unavailable | Check Microsoft 365 service health |

---

## Diagnostic Commands

### Test OAuth Authentication
```python
def test_authentication():
    try:
        token = get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        print("✅ Authentication successful")
        return True
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return False

test_authentication()
```

### Test SharePoint Access
```python
def test_sharepoint_access(token, site_url):
    from urllib.parse import urlparse
    parsed = urlparse(site_url)
    graph_url = f"https://graph.microsoft.com/v1.0/sites/{parsed.netloc}:{parsed.path}"
    
    try:
        response = requests.get(
            graph_url,
            headers={'Authorization': f'Bearer {token}'}
        )
        response.raise_for_status()
        print("✅ SharePoint access successful")
        return True
    except Exception as e:
        print(f"❌ SharePoint access failed: {e}")
        return False

test_sharepoint_access(access_token, SHAREPOINT_SITE_URL)
```

### Check Table Health
```sql
-- Check for issues
DESCRIBE DETAIL sharepoint_documents;

-- View recent operations
DESCRIBE HISTORY sharepoint_documents LIMIT 10;

-- Check for corruption
VACUUM sharepoint_documents DRY RUN;

-- Optimize table
OPTIMIZE sharepoint_documents;

-- Generate statistics
ANALYZE TABLE sharepoint_documents COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## Getting Additional Help

### Databricks Support
- **Documentation**: https://docs.databricks.com/en/connect/
- **Community Forum**: https://community.databricks.com/
- **Support Portal**: Open ticket through workspace

### Microsoft Support
- **Graph API Docs**: https://learn.microsoft.com/en-us/graph/
- **Service Health**: https://admin.microsoft.com/servicehealth
- **Support**: https://support.microsoft.com/

### Logs and Diagnostics
1. **Cluster Logs**: Check driver and worker logs for errors
2. **Event Logs**: Review Unity Catalog audit logs
3. **Metrics**: Monitor query performance and API latency

### When Opening a Support Ticket
Include:
- Error message (full stack trace)
- Databricks Runtime version
- Cluster configuration
- Code snippet reproducing the issue
- Tenant ID (not client secret!)
- Timestamps of failures

---

**Last Updated**: January 2026  
**Version**: 1.0  
**Maintained by**: Ajit Kalura
