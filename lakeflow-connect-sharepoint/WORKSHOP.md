# Databricks Lakeflow Connect SharePoint Connector Workshop
## Detailed Workshop Guide

### Workshop Overview

This comprehensive workshop teaches participants how to integrate Microsoft SharePoint data with Databricks using Lakeflow Connect. Through three progressive notebooks, participants will learn end-to-end integration, from initial connection setup through advanced analytics and machine learning.

**Total Duration**: 90 minutes (25 + 35 + 30 minutes)  
**Level**: Intermediate  
**Prerequisites**: Basic SQL, Python, and SharePoint knowledge

---

## Learning Objectives

By the end of this workshop, participants will be able to:

1. **Configure OAuth 2.0 authentication** between Databricks and SharePoint
2. **Create and manage Lakeflow Connect connections** for SharePoint
3. **Ingest SharePoint documents and lists** into Delta Lake tables
4. **Implement incremental refresh patterns** for efficient data synchronization
5. **Build analytics pipelines** using SQL and Delta Live Tables
6. **Apply AI/ML techniques** to SharePoint document metadata
7. **Create dashboards and visualizations** from SharePoint data

---

## Module 1: Configure SharePoint Connection (25 minutes)

### Objective
Establish a secure, authenticated connection from Databricks to SharePoint Online using OAuth 2.0 and Microsoft Graph API.

### Topics Covered
- Azure AD application registration
- OAuth 2.0 client credentials flow
- Databricks Secrets management
- Lakeflow Connect connection creation
- Connection validation and troubleshooting

### Hands-On Exercises

#### Exercise 1.1: Azure AD App Registration (5 minutes)
**Task**: Register an Azure AD application for Databricks to access SharePoint

**Steps**:
1. Navigate to Azure Portal > Microsoft Entra ID > App registrations
2. Create new registration named "Databricks-SharePoint-Connector"
3. Note the Application (client) ID and Tenant ID
4. Create a client secret and save the value

**Expected Outcome**: You should have three values:
- Tenant ID (GUID format)
- Client ID (GUID format)
- Client Secret (alphanumeric string)

**Validation**: All three values should be non-empty GUIDs or strings

---

#### Exercise 1.2: Configure API Permissions (5 minutes)
**Task**: Grant necessary Microsoft Graph API permissions

**Steps**:
1. In your app registration, go to API permissions
2. Add Microsoft Graph > Application permissions
3. Add `Sites.Read.All` and `Files.Read.All`
4. Click "Grant admin consent"

**Expected Outcome**: Both permissions show "Granted" status with green checkmarks

**Validation**: Verify in Azure Portal that admin consent is granted

---

#### Exercise 1.3: Store Credentials in Databricks Secrets (5 minutes)
**Task**: Securely store SharePoint credentials using Databricks Secrets

**Steps**:
1. Create a secret scope named `sharepoint-connector`
2. Store four secrets:
   - `tenant-id`
   - `client-id`
   - `client-secret`
   - `site-url`

**Using Databricks CLI**:
```bash
databricks secrets create-scope sharepoint-connector
databricks secrets put-secret sharepoint-connector tenant-id
databricks secrets put-secret sharepoint-connector client-id
databricks secrets put-secret sharepoint-connector client-secret
databricks secrets put-secret sharepoint-connector site-url
```

**Expected Outcome**: All secrets stored successfully

**Validation**: Run in notebook:
```python
dbutils.secrets.get(scope="sharepoint-connector", key="tenant-id")
# Should return: "[REDACTED]"
```

---

#### Exercise 1.4: Test OAuth Authentication (5 minutes)
**Task**: Verify credentials by obtaining an access token

**Steps**:
1. Open Notebook 01
2. Run cells up to "Test OAuth Authentication"
3. Verify you receive an access token
4. Check token expiration (should be ~60-90 minutes)

**Expected Outcome**: 
```
✅ Authentication successful!
   Token type: Bearer
   Expires in: 3599 seconds
```

**Troubleshooting**:
- If authentication fails, verify tenant ID and client secret
- Ensure client secret hasn't expired
- Check for typos in credentials

---

#### Exercise 1.5: Create Lakeflow Connect Connection (5 minutes)
**Task**: Create a persistent connection object in Unity Catalog

**Steps**:
1. Run the "Create SharePoint Connection" cell
2. Verify connection creation succeeds
3. Test the connection

**Expected Outcome**:
```
✅ Connection created successfully: sharepoint_data.raw.sharepoint_connection
   Connection Type: MICROSOFT_SHAREPOINT
   Owner: <your-username>
```

**Validation**: Connection should appear in Unity Catalog explorer

---

### Module 1 Assessment

**Knowledge Check**:
1. What OAuth flow is used for service-to-service authentication?
   - Answer: Client Credentials Flow
2. Which Microsoft Graph permissions are required?
   - Answer: Sites.Read.All, Files.Read.All
3. Where should credentials be stored in production?
   - Answer: Databricks Secrets (never hardcoded)

**Practical Check**:
- Can you successfully retrieve a connection object?
- Does your connection show in Unity Catalog?
- Can you access SharePoint site information?

---

## Module 2: Ingest SharePoint Data (35 minutes)

### Objective
Learn to ingest SharePoint documents and lists into Delta Lake, implementing incremental refresh patterns for production use.

### Topics Covered
- Microsoft Graph API for SharePoint
- Document library ingestion
- SharePoint list ingestion
- Incremental refresh with watermarking
- Delta merge (upsert) operations
- Metadata tracking

### Hands-On Exercises

#### Exercise 2.1: Browse SharePoint Resources (5 minutes)
**Task**: Explore available document libraries and lists in your SharePoint site

**Steps**:
1. Open Notebook 02
2. Run cells to authenticate and get site information
3. Execute "List Document Libraries" cell
4. Execute "List SharePoint Lists" cell

**Expected Outcome**:
```
📚 Found 3 document libraries:
   1. Documents
   2. Site Assets
   3. Form Templates

📋 Found 5 user-accessible lists:
   1. Tasks
   2. Issues
   3. Project Tracker
```

**Validation**: Verify list matches what you see in SharePoint UI

---

#### Exercise 2.2: Ingest Document Library Metadata (10 minutes)
**Task**: Extract metadata from a SharePoint document library and save to Delta Lake

**Steps**:
1. Select a document library to ingest (default: "Documents")
2. Run the "Fetch Document Metadata" cell
3. Review the sample documents displayed
4. Execute "Transform to DataFrame and Save" cell
5. Verify table creation: `sharepoint_data.raw.sharepoint_documents`

**Expected Outcome**:
```
✅ Saved 47 documents to: sharepoint_data.raw.sharepoint_documents
```

**Schema Validation**:
```sql
DESCRIBE TABLE sharepoint_data.raw.sharepoint_documents;
```

Should show columns:
- id, name, size, created_datetime, modified_datetime
- created_by, modified_by, web_url
- mime_type, is_folder, parent_path
- site_id, library_id, library_name, ingestion_timestamp

**Data Validation**:
```sql
SELECT COUNT(*), COUNT(DISTINCT name) FROM sharepoint_documents;
```

---

#### Exercise 2.3: Implement Incremental Refresh (10 minutes)
**Task**: Build an incremental refresh pipeline that syncs only changed documents

**Steps**:
1. Note the current watermark (last sync time)
2. Run the "Incremental Refresh with Watermarking" function
3. Modify a document in SharePoint (optional)
4. Re-run the incremental refresh
5. Verify only new/modified documents are synced

**Expected Outcome**:
```
🔄 Fetching documents modified since: 2024-03-15T10:30:00Z
📊 Found 3 modified items
✅ Merged 3 documents into sharepoint_data.raw.sharepoint_documents
```

**Key Concept**: Delta merge performs an UPSERT operation:
- Updates existing documents (matched on ID)
- Inserts new documents

**Validation**:
```sql
SELECT id, name, modified_datetime, ingestion_timestamp 
FROM sharepoint_documents 
ORDER BY modified_datetime DESC 
LIMIT 10;
```

---

#### Exercise 2.4: Ingest SharePoint List Data (10 minutes)
**Task**: Load structured data from a SharePoint list

**Steps**:
1. Select a SharePoint list to ingest
2. Inspect the list schema (columns)
3. Run "Fetch List Items" cell
4. Execute "Transform List Data to Delta Table" cell
5. Query the resulting table

**Expected Outcome**:
```
✅ Saved 124 list items to: sharepoint_data.raw.sharepoint_list_tasks
```

**Schema**: List tables have dynamic schemas based on SharePoint columns:
- Core fields: item_id, created_datetime, modified_datetime, created_by, modified_by
- Custom fields: field_title, field_status, field_priority, etc.

**Validation**:
```sql
SELECT * FROM sharepoint_list_tasks LIMIT 10;
```

---

#### Exercise 2.5: Create Metadata Tracking (5 minutes)
**Task**: Build an audit table to track ingestion jobs

**Steps**:
1. Run "Ingestion Metadata Table" cell
2. Review the metadata schema
3. Query the metadata table

**Expected Outcome**:
```
✅ Ingestion metadata saved to: sharepoint_data.raw.sharepoint_ingestion_metadata
```

**Metadata Fields**:
- sync_id (unique job identifier)
- source_type (library or list)
- source_id, source_name
- sync_start_time, sync_end_time
- records_processed
- last_modified_watermark
- status (completed/failed)
- error_message

**Use Case**: Track sync history, monitor for failures, retrieve watermarks

---

### Module 2 Assessment

**Knowledge Check**:
1. What is the benefit of incremental refresh over full reload?
   - Answer: Efficiency - only processes changed data, reduces API calls
2. What Delta operation is used for upsert?
   - Answer: MERGE (whenMatchedUpdate, whenNotMatchedInsert)
3. Why track metadata in a separate table?
   - Answer: Audit trail, watermark storage, error monitoring

**Practical Check**:
- Do you have documents and lists ingested into Delta tables?
- Can you perform an incremental refresh successfully?
- Does your metadata table show sync history?

---

## Module 3: Analyze and Transform SharePoint Data (30 minutes)

### Objective
Build analytics, visualizations, and ML workflows on top of SharePoint data integrated into the lakehouse.

### Topics Covered
- SQL analytics on SharePoint metadata
- Time-based analysis and trends
- Data integration with lakehouse tables
- Delta Live Tables (DLT) pipelines
- Document classification
- Dashboard creation

### Hands-On Exercises

#### Exercise 3.1: Exploratory Data Analysis (5 minutes)
**Task**: Understand your SharePoint data distribution and statistics

**Steps**:
1. Open Notebook 03
2. Run "Document Overview Statistics" query
3. Execute "Documents by File Type" query
4. Review "Top Contributors" analysis

**Expected Insights**:
- Total documents, folders, and contributors
- Total storage used
- Most common file types
- Most active users

**Sample Query**:
```sql
SELECT 
  COUNT(*) as total_documents,
  ROUND(SUM(size) / 1024.0 / 1024.0 / 1024.0, 2) as total_gb,
  COUNT(DISTINCT modified_by) as contributors
FROM sharepoint_documents
WHERE is_folder = false;
```

---

#### Exercise 3.2: Time-Based Activity Analysis (5 minutes)
**Task**: Analyze document activity patterns over time

**Steps**:
1. Run "Document Activity by Month" query
2. Execute visualization cell for activity trends
3. Review weekly activity chart

**Expected Outcome**: Interactive Plotly chart showing:
- Document modifications over time
- Active users per period
- Trends and patterns

**Business Questions Answered**:
- When is SharePoint most actively used?
- Are there seasonal patterns?
- How has usage trended over time?

---

#### Exercise 3.3: Document Lifecycle Analysis (5 minutes)
**Task**: Understand document age and freshness

**Steps**:
1. Run "Calculate Document Age and Freshness" cell
2. Execute "Document Freshness Distribution" query
3. Review freshness categories

**Freshness Categories**:
- Very Fresh (< 1 week)
- Fresh (< 1 month)
- Moderate (< 3 months)
- Aging (< 6 months)
- Stale (> 6 months)

**Business Value**: Identify stale content for archival or review

**Validation**:
```sql
SELECT freshness_category, COUNT(*) as document_count
FROM documents_with_age
GROUP BY freshness_category;
```

---

#### Exercise 3.4: Data Integration (5 minutes)
**Task**: Join SharePoint data with other lakehouse tables

**Steps**:
1. Review sample employee table creation
2. Run "Join SharePoint Activity with Employee Data" query
3. Analyze activity by department

**Expected Outcome**:
```
department    | employee_name  | documents_modified | last_activity
Engineering   | John Smith     | 45                 | 2024-03-20
Product       | Jane Doe       | 32                 | 2024-03-19
```

**Business Value**: Connect SharePoint activity to organizational structure

---

#### Exercise 3.5: Delta Live Tables Pipeline (5 minutes)
**Task**: Review and understand a production DLT pipeline

**Concepts**:
- **Bronze Layer**: Raw SharePoint data (as-is from source)
- **Silver Layer**: Cleaned, validated, enriched data
- **Gold Layer**: Aggregated metrics and business logic

**Pipeline Structure**:
```python
@dlt.table
def sharepoint_documents_bronze():
    # Raw data
    
@dlt.table
@dlt.expect_or_drop("valid_document", "name IS NOT NULL")
def sharepoint_documents_silver():
    # Cleaned with quality checks
    
@dlt.table
def sharepoint_daily_metrics():
    # Aggregated metrics
```

**Deployment**: Copy DLT code to a new notebook and create a DLT pipeline

---

#### Exercise 3.6: Document Classification (5 minutes)
**Task**: Classify documents by type and content patterns

**Steps**:
1. Run "Extract File Extensions and Classify Documents" cell
2. Execute "Document Category Distribution" query
3. View pie chart visualization

**Categories**:
- Documents (docx, doc, txt)
- Spreadsheets (xlsx, xls, csv)
- Presentations (pptx, ppt)
- PDFs
- Images (jpg, png)
- Videos (mp4, avi)
- Archives (zip, rar)
- Other

**Business Application**: Content governance, storage optimization

---

### Module 3 Assessment

**Knowledge Check**:
1. What are the three layers in a medallion architecture?
   - Answer: Bronze (raw), Silver (cleaned), Gold (aggregated)
2. What is the purpose of document freshness analysis?
   - Answer: Identify stale content for archival or review
3. How can SharePoint data be enriched in the lakehouse?
   - Answer: Join with employee, department, or project data

**Practical Check**:
- Can you query SharePoint data with SQL?
- Can you create visualizations from the data?
- Do you understand how to build a DLT pipeline?

---

## Advanced Topics (Optional Extensions)

### Extension 1: Real-Time Streaming Ingestion
Implement change notifications using Microsoft Graph webhooks for near real-time sync.

**Approach**:
1. Register webhook subscription in SharePoint
2. Create Databricks endpoint to receive notifications
3. Trigger incremental refresh on change events

### Extension 2: Full-Text Search
Index document content (not just metadata) for full-text search capabilities.

**Approach**:
1. Download document content using Graph API
2. Extract text from various file formats
3. Create vector embeddings for semantic search
4. Use Databricks Vector Search for queries

### Extension 3: Document Recommendations
Build an ML model to recommend related documents based on usage patterns.

**Approach**:
1. Track document access patterns
2. Generate embeddings from document metadata
3. Use collaborative filtering or content-based filtering
4. Deploy model with Databricks Model Serving

### Extension 4: Compliance and Governance
Implement data classification and sensitivity labeling.

**Approach**:
1. Scan document content for PII or sensitive data
2. Apply Unity Catalog tags for classification
3. Set up row-level security based on departments
4. Create audit reports for compliance

---

## Production Deployment Checklist

### Security
- [ ] Credentials stored in Databricks Secrets (never hardcoded)
- [ ] Client secret rotation schedule established (every 6-12 months)
- [ ] Unity Catalog access controls configured
- [ ] Row-level security implemented if needed
- [ ] Audit logging enabled

### Performance
- [ ] Incremental refresh implemented (not full reload)
- [ ] Appropriate partition strategy for large datasets
- [ ] API throttling limits understood and respected
- [ ] Cluster sizing appropriate for workload

### Reliability
- [ ] Error handling and retry logic implemented
- [ ] Metadata tracking for all ingestion jobs
- [ ] Alerts configured for failed syncs
- [ ] Data quality checks in place

### Operations
- [ ] Ingestion jobs scheduled (daily/hourly)
- [ ] Monitoring dashboard created
- [ ] Runbook for common issues documented
- [ ] Backup and disaster recovery plan

---

## Workshop Evaluation

### Self-Assessment

Rate your confidence (1-5) on each objective:

1. Configure OAuth 2.0 authentication: ___/5
2. Create Lakeflow Connect connections: ___/5
3. Ingest SharePoint data into Delta Lake: ___/5
4. Implement incremental refresh: ___/5
5. Build analytics pipelines: ___/5
6. Apply ML to SharePoint data: ___/5
7. Create dashboards: ___/5

### Reflection Questions

1. What was the most valuable thing you learned?
2. What challenges did you encounter?
3. How will you apply this in your work?
4. What additional features would you like to explore?

---

## Next Steps

### Continue Learning
1. **Databricks Academy**: Complete Delta Live Tables course
2. **Microsoft Learn**: Explore Graph API documentation
3. **Practice**: Build your own SharePoint integration project

### Build Your Project
1. Identify a SharePoint site to integrate
2. Define business requirements and KPIs
3. Implement ingestion pipeline
4. Build analytics and dashboards
5. Deploy to production

### Community Resources
- Databricks Community Forum
- Microsoft Graph API Community
- Stack Overflow - Databricks and SharePoint tags

---

## Appendix A: Common Commands

### Databricks CLI
```bash
# Create secret scope
databricks secrets create-scope sharepoint-connector

# Add secret
databricks secrets put-secret sharepoint-connector tenant-id

# List scopes
databricks secrets list-scopes

# List secrets in scope
databricks secrets list-secrets sharepoint-connector
```

### SQL Queries
```sql
-- View all tables
SHOW TABLES IN sharepoint_data.raw;

-- Check table schema
DESCRIBE TABLE sharepoint_documents;

-- View table history
DESCRIBE HISTORY sharepoint_documents;

-- Count documents
SELECT COUNT(*) FROM sharepoint_documents;

-- Recent activity
SELECT name, modified_by, modified_datetime 
FROM sharepoint_documents 
ORDER BY modified_datetime DESC 
LIMIT 20;
```

### Python Snippets
```python
# Read from Delta table
df = spark.table("sharepoint_data.raw.sharepoint_documents")

# Filter documents
docs_df = df.filter("is_folder = false")

# Get latest watermark
latest_sync = spark.sql("""
    SELECT MAX(last_modified_watermark) 
    FROM sharepoint_ingestion_metadata 
    WHERE status = 'completed'
""").collect()[0][0]
```

---

## Appendix B: Troubleshooting Matrix

| Symptom | Possible Cause | Solution |
|---------|---------------|----------|
| Authentication fails | Invalid client secret | Regenerate secret in Azure AD |
| Permission denied | Missing Graph API permissions | Add Sites.Read.All, Files.Read.All |
| Connection timeout | Network/firewall issue | Check connectivity to graph.microsoft.com |
| Throttling errors | Too many API requests | Implement exponential backoff |
| Schema mismatch | SharePoint columns changed | Run schema refresh |
| No data returned | Empty library/list | Verify data exists in SharePoint |
| Slow performance | Large dataset, no partitioning | Implement partitioning strategy |

---

## Workshop Feedback

We'd love to hear your feedback! Please share:
- What worked well?
- What could be improved?
- Topics you'd like to see added?
- Technical difficulties encountered?

Contact: Ajit Kalura

---

**Version**: 1.0  
**Last Updated**: January 2026  
**License**: Educational Use
