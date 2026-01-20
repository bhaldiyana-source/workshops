# Salesforce API Limits Guide

## Overview

Understanding Salesforce API limits is critical for building reliable data pipelines. This guide covers daily API limits, best practices for staying within limits, and strategies for optimizing API usage.

---

## Daily API Limits by Edition

### Limits Overview

Salesforce enforces daily API call limits based on your organization's edition and number of user licenses.

| Edition | Base API Calls | Per User License | Example Calculation |
|---------|---------------|------------------|---------------------|
| **Developer** | 15,000 | N/A | 15,000 calls/day |
| **Professional** | 1,000 | +1,000 | 1,000 + (100 users × 1,000) = 101,000 calls/day |
| **Enterprise** | 1,000 | +1,000 | 1,000 + (250 users × 1,000) = 251,000 calls/day |
| **Unlimited** | 5,000 | +5,000 | 5,000 + (500 users × 5,000) = 2,505,000 calls/day |
| **Performance** | 5,000 | +5,000 | Same as Unlimited |

### What Counts as an API Call?

**REST API Calls (count against limit):**
- Query requests (`/services/data/vXX.0/query`)
- Describe requests (`/services/data/vXX.0/sobjects/Account/describe`)
- Individual record CRUD operations
- Search requests
- Metadata API calls

**Bulk API Calls (different limits):**
- Bulk API 2.0 has separate limits
- Batch submissions count differently
- More efficient for large data loads (>2,000 records)

**Free API Calls (don't count):**
- OAuth token requests
- Login attempts
- Some metadata operations

---

## API Types and When to Use Them

### REST API

**Best For:**
- Small datasets (<2,000 records)
- Real-time single record operations
- Exploratory queries
- Testing and development

**Characteristics:**
- Synchronous responses
- Each query = 1 API call + 1 per 2,000 records
- Fast for small result sets
- Subject to daily API limits

**Example:**
```python
# REST API query - uses API calls
soql = "SELECT Id, Name FROM Account LIMIT 100"
response = requests.get(
    f"{instance_url}/services/data/v59.0/query",
    headers={"Authorization": f"Bearer {access_token}"},
    params={"q": soql}
)
```

---

### Bulk API 2.0

**Best For:**
- Large datasets (>2,000 records)
- Scheduled batch ingestion
- Production data pipelines
- Initial full loads

**Characteristics:**
- Asynchronous processing
- Much higher throughput
- Different (higher) limits than REST API
- Batch-based processing

**Limits:**
- 15,000 batches per 24 hours (rolling)
- 10,000 records per batch
- 150,000,000 records processed per 24 hours

**Example:**
```python
# Bulk API job creation
bulk_job = {
    "object": "Account",
    "operation": "query",
    "query": "SELECT Id, Name FROM Account WHERE CreatedDate > YESTERDAY"
}

# Submit job (asynchronous)
response = requests.post(
    f"{instance_url}/services/data/v59.0/jobs/query",
    headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    },
    json=bulk_job
)
```

---

## Monitoring API Usage

### Check Current Usage via API

```python
def check_api_usage(access_token, instance_url):
    """
    Query Salesforce Limits API to check current API usage
    Returns usage statistics and percentage
    """
    limits_url = f"{instance_url}/services/data/v59.0/limits"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    response = requests.get(limits_url, headers=headers)
    limits = response.json()
    
    # Daily API Request limits
    daily_api = limits.get("DailyApiRequests", {})
    max_calls = daily_api.get("Max", 0)
    remaining = daily_api.get("Remaining", 0)
    used = max_calls - remaining
    usage_pct = (used / max_calls * 100) if max_calls > 0 else 0
    
    print(f"API Usage Report:")
    print(f"  Max Daily Calls: {max_calls:,}")
    print(f"  Used: {used:,}")
    print(f"  Remaining: {remaining:,}")
    print(f"  Usage: {usage_pct:.1f}%")
    
    # Bulk API limits
    bulk_api = limits.get("DailyBulkApiBatches", {})
    print(f"\nBulk API Batches:")
    print(f"  Max: {bulk_api.get('Max', 0):,}")
    print(f"  Remaining: {bulk_api.get('Remaining', 0):,}")
    
    return {
        "max": max_calls,
        "used": used,
        "remaining": remaining,
        "usage_pct": usage_pct
    }

# Usage
usage = check_api_usage(access_token, instance_url)

if usage["usage_pct"] > 90:
    print("🔴 CRITICAL: API usage above 90%")
elif usage["usage_pct"] > 80:
    print("⚠️ WARNING: API usage above 80%")
```

### Monitor in Salesforce UI

**System Overview:**
1. Navigate to Setup
2. Search for "System Overview" in Quick Find
3. View "API Usage - Last 7 Days" chart
4. Check current usage percentage

**API Usage Notifications:**
1. Setup → Email Administration → Deliverability
2. Enable "API Usage Exceeded" email alerts
3. Set threshold (e.g., 80%, 90%)

---

## Best Practices for API Optimization

### 1. Implement Incremental Refresh

**❌ Bad Practice - Full Reload Every Hour:**
```sql
-- This uses too many API calls
SELECT Id, Name, Industry, AnnualRevenue, ...50 fields...
FROM Account
WHERE IsDeleted = false
```
*Result: 100,000 records = ~50 API calls every hour = 1,200 calls/day*

**✅ Good Practice - Incremental with SystemModstamp:**
```sql
-- Only fetch changed records
SELECT Id, Name, Industry, AnnualRevenue, ...50 fields...
FROM Account
WHERE SystemModstamp > 2025-01-09T10:00:00Z
  AND IsDeleted = false
ORDER BY SystemModstamp ASC
```
*Result: Only 500 changed records = ~1 API call per hour = 24 calls/day*

**Savings: 98% reduction in API calls**

---

### 2. Use Selective Queries

**❌ Bad Practice:**
```sql
-- Fetches all records, wastes API calls
SELECT Id, Name FROM Account
```

**✅ Good Practice:**
```sql
-- Use indexed fields in WHERE clause
SELECT Id, Name FROM Account
WHERE CreatedDate >= LAST_N_DAYS:30
  AND IsDeleted = false
```

**Tip:** Indexed fields include:
- `Id`
- `CreatedDate`
- `SystemModstamp`
- `Name` (on some objects)
- Custom fields marked as "External ID"

---

### 3. Limit Field Selection

**❌ Bad Practice:**
```sql
SELECT Id, Name, Description, ... (50+ fields) FROM Account
```

**✅ Good Practice:**
```sql
-- Only select fields you actually need
SELECT Id, Name, Industry, AnnualRevenue FROM Account
```

**Impact:**
- Reduces payload size
- Faster query execution
- Lower network transfer
- Same API call count but better performance

---

### 4. Use Bulk API for Large Datasets

| Dataset Size | Recommended API | API Calls | Time |
|--------------|-----------------|-----------|------|
| <100 records | REST API | 1 | <1 sec |
| 100-2,000 records | REST API | 1-2 | 1-5 sec |
| 2,000-10,000 records | Bulk API | ~1 batch | 30-60 sec |
| >10,000 records | Bulk API | Multiple batches | Minutes |

**Example: 100,000 Account records**

REST API:
- ~50 API calls (pagination)
- 50 calls × 5 objects = 250 daily API calls
- May hit rate limits with multiple objects

Bulk API:
- 10 batches (10,000 records each)
- 10 batches × 5 objects = 50 batches
- Well within Bulk API limits (15,000/day)

---

### 5. Schedule Jobs During Off-Peak Hours

**Peak Hours (High User Activity):**
- 9 AM - 5 PM local business hours
- End of month/quarter
- During marketing campaigns

**Off-Peak Hours (Recommended for Batch Jobs):**
- 12 AM - 6 AM local time
- Weekends
- Holidays

**Benefits:**
- Less contention for API limits
- Faster query execution
- Reduced impact on users

**Databricks Job Scheduling:**
```python
# Schedule ingestion for 2 AM daily
{
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/Los_Angeles"
  }
}
```

---

### 6. Batch Multiple Operations

**❌ Bad Practice - Individual Queries:**
```python
# 5 separate API calls
accounts = query("SELECT Id, Name FROM Account LIMIT 100")
contacts = query("SELECT Id, Name FROM Contact LIMIT 100")
opportunities = query("SELECT Id, Name FROM Opportunity LIMIT 100")
leads = query("SELECT Id, Name FROM Lead LIMIT 100")
cases = query("SELECT Id, Name FROM Case LIMIT 100")
```
*Total: 5 API calls per run*

**✅ Good Practice - Single Composite Request:**
```python
# Use Composite API - 1 API call
composite_request = {
    "compositeRequest": [
        {"method": "GET", "url": "/services/data/v59.0/query?q=SELECT+Id,Name+FROM+Account+LIMIT+100", "referenceId": "accounts"},
        {"method": "GET", "url": "/services/data/v59.0/query?q=SELECT+Id,Name+FROM+Contact+LIMIT+100", "referenceId": "contacts"},
        # ... more requests
    ]
}

response = requests.post(
    f"{instance_url}/services/data/v59.0/composite",
    headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
    json=composite_request
)
```
*Total: 1 API call (up to 25 subrequests)*

---

### 7. Implement Caching

**Strategy:**
- Cache reference data (e.g., picklist values, object metadata)
- Refresh cache daily instead of per query
- Store in Delta tables

**Example:**
```python
from datetime import datetime, timedelta

def get_account_describe(access_token, instance_url, cache_hours=24):
    """
    Get Account object metadata with caching
    """
    cache_table = "sf_metadata_cache"
    
    # Check if cached metadata exists and is recent
    try:
        cached = spark.sql(f"""
            SELECT metadata 
            FROM {cache_table} 
            WHERE object_name = 'Account' 
              AND last_updated > current_timestamp() - INTERVAL {cache_hours} HOURS
        """).first()
        
        if cached:
            print("✅ Using cached metadata (saves 1 API call)")
            return json.loads(cached["metadata"])
    except:
        pass
    
    # Cache miss - fetch from Salesforce
    print("📡 Fetching fresh metadata from Salesforce")
    describe_url = f"{instance_url}/services/data/v59.0/sobjects/Account/describe"
    response = requests.get(describe_url, headers={"Authorization": f"Bearer {access_token}"})
    metadata = response.json()
    
    # Update cache
    spark.createDataFrame([{
        "object_name": "Account",
        "metadata": json.dumps(metadata),
        "last_updated": datetime.now()
    }]).write.format("delta").mode("append").saveAsTable(cache_table)
    
    return metadata
```

**Savings: 1 API call per hour × 24 hours = 24 calls/day per object**

---

## API Limit Troubleshooting

### Scenario 1: Hitting Daily Limits

**Symptoms:**
- `REQUEST_LIMIT_EXCEEDED` errors
- Jobs failing mid-day

**Solutions:**
1. **Identify Heavy API Consumers:**
   ```sql
   -- Check which objects use most API calls
   SELECT object_name, COUNT(*) as query_count
   FROM ingestion_logs
   WHERE date = CURRENT_DATE()
   GROUP BY object_name
   ORDER BY query_count DESC
   ```

2. **Switch to Bulk API:**
   - Migrate large object ingestion to Bulk API
   - Target: Objects with >10K records

3. **Reduce Frequency:**
   - Change from hourly to every 4 hours
   - Only run critical jobs during business hours

4. **Upgrade Salesforce Edition:**
   - Consider upgrading if consistently hitting limits
   - ROI: Cost of downtime vs. upgrade cost

---

### Scenario 2: Slow Performance Near Limit

**Symptoms:**
- Queries take longer as limit approaches
- Intermittent timeouts

**Solutions:**
1. **Implement Rate Limiting:**
   ```python
   import time
   
   def query_with_rate_limit(soql, max_calls_per_minute=60):
       """Query with rate limiting to avoid overwhelming API"""
       time.sleep(60 / max_calls_per_minute)  # Space out requests
       return query_salesforce_rest(soql, access_token, instance_url)
   ```

2. **Monitor and Alert:**
   ```python
   usage = check_api_usage(access_token, instance_url)
   
   if usage["usage_pct"] > 80:
       # Send alert to ops team
       send_alert(f"Salesforce API usage at {usage['usage_pct']:.1f}%")
       
       # Pause non-critical jobs
       if usage["usage_pct"] > 90:
           print("⚠️ Pausing non-critical ingestion jobs")
           # Skip optional objects
   ```

---

## Cost-Benefit Analysis

### API Call Cost Calculator

**Example Scenario:**
- 100,000 Account records
- 50,000 Contact records
- 75,000 Opportunity records
- Sync every hour

**Option 1: Full Refresh with REST API**
```
Accounts: 100,000 / 2,000 = 50 calls
Contacts: 50,000 / 2,000 = 25 calls
Opportunities: 75,000 / 2,000 = 38 calls
Total per run: 113 calls
Per day: 113 × 24 = 2,712 calls/day
```

**Option 2: Incremental with REST API**
```
Accounts: ~500 changed / 2,000 = 1 call
Contacts: ~300 changed / 2,000 = 1 call
Opportunities: ~1,000 changed / 2,000 = 1 call
Total per run: 3 calls
Per day: 3 × 24 = 72 calls/day
```

**Option 3: Incremental with Bulk API (every 6 hours)**
```
Accounts: 1 batch × 4 runs = 4 batches
Contacts: 1 batch × 4 runs = 4 batches
Opportunities: 1 batch × 4 runs = 4 batches
Total per day: 12 batches (uses Bulk API limit, not REST)
REST API calls: ~0
```

**Recommendation:** Option 3 (Incremental with Bulk API)
- Lowest API usage
- No impact on REST API limits
- Best for production

---

## Summary Checklist

### ✅ Before Going to Production

- [ ] Implement incremental refresh with SystemModstamp
- [ ] Use Bulk API for objects with >10K records
- [ ] Schedule jobs during off-peak hours
- [ ] Set up API usage monitoring and alerts
- [ ] Implement retry logic for rate limit errors
- [ ] Cache metadata and reference data
- [ ] Document expected daily API usage
- [ ] Test failure scenarios when limits are exceeded
- [ ] Configure alerts at 80% and 90% thresholds
- [ ] Review and optimize SOQL queries for selectivity

### 📊 Ongoing Monitoring

- [ ] Check API usage daily
- [ ] Review query performance weekly
- [ ] Analyze API usage trends monthly
- [ ] Optimize slow or expensive queries
- [ ] Archive historical data to reduce query size

---

## Additional Resources

- [Salesforce API Request Limits](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm)
- [Bulk API 2.0 Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/)
- [REST API Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/)
- [Query Performance Best Practices](https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/langCon_apex_SOQL_query_perf_best_practices.htm)

---

**Last Updated:** January 2026  
**Workshop Version:** 1.0
