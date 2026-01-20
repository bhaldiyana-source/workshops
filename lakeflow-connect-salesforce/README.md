# Databricks Lakeflow Connect Salesforce Connector Workshop

## Overview

This hands-on workshop teaches you how to integrate Salesforce data into Databricks using **Lakeflow Connect**. You'll learn to configure connections, ingest Salesforce objects and metadata, and build analytics pipelines that combine CRM data with your lakehouse architecture.

### What You'll Build

By completing this workshop, you will:
- ✅ Configure secure OAuth 2.0 connectivity to Salesforce
- ✅ Ingest standard and custom Salesforce objects into Delta tables
- ✅ Build incremental refresh pipelines for Salesforce data
- ✅ Create analytics and visualizations from CRM data
- ✅ Apply AI/ML to predict customer behavior and sales outcomes

## Workshop Structure

| Notebook | Title | Duration | Description |
|----------|-------|----------|-------------|
| 01 | Configure Salesforce Connection | 25 min | Set up OAuth authentication and create Salesforce connection |
| 02 | Ingest Salesforce Data | 35 min | Configure ingestion pipelines for objects, fields, and relationships |
| 03 | Analyze and Transform | 30 min | Build analytics pipelines and predictive models |

## Prerequisites

### Required Access
- **Databricks Workspace** (AWS, Azure, or GCP) with Unity Catalog enabled
- **Workspace Admin** or **Metastore Admin** permissions
- **Salesforce** account (Developer, Professional, Enterprise, or Unlimited edition)
- **System Administrator** or appropriate Salesforce permissions

### Required Knowledge
- Basic SQL and Python programming
- Understanding of Salesforce object model (Accounts, Contacts, Opportunities, etc.)
- Familiarity with Delta Lake and Unity Catalog concepts
- Basic knowledge of CRM data structures and relationships

### Technical Requirements
- Databricks Runtime 14.3 or higher
- Salesforce Connected App with API access enabled
- API call limits appropriate for your org size
- Network connectivity between Databricks and Salesforce

## Setup Instructions

### Step 1: Create Salesforce Connected App

1. Log in to Salesforce and navigate to **Setup**
2. In Quick Find, search for **App Manager**
3. Click **New Connected App**
4. Configure the basic information:
   - **Connected App Name**: `Databricks Lakeflow Connect`
   - **API Name**: Auto-populated
   - **Contact Email**: Your email address
5. Enable OAuth Settings:
   - Check **Enable OAuth Settings**
   - **Callback URL**: `https://login.salesforce.com/services/oauth2/callback`
   - **Selected OAuth Scopes**: Add the following:
     - `Access and manage your data (api)`
     - `Perform requests on your behalf at any time (refresh_token, offline_access)`
     - `Access your basic information (id, profile, email, address, phone)`
6. Click **Save** and then **Continue**

### Step 2: Configure Connected App Policies

1. After saving, click **Manage** on your Connected App
2. Click **Edit Policies**
3. Configure OAuth policies:
   - **Permitted Users**: Admin approved users are pre-authorized
   - **IP Relaxation**: Relax IP restrictions (or configure allowed IP ranges)
   - **Refresh Token Policy**: Refresh token is valid until revoked
4. Click **Save**

### Step 3: Retrieve OAuth Credentials

1. Go back to **App Manager** and find your Connected App
2. Click the dropdown and select **View**
3. In the **API (Enable OAuth Settings)** section, click **Manage Consumer Details**
4. Verify your identity (you may need to enter a verification code)
5. Copy the following:
   - **Consumer Key** (Client ID)
   - **Consumer Secret** (Client Secret)

### Step 4: Assign Users to Connected App

1. In your Connected App, click **Manage**
2. Click **Manage Profiles** or **Manage Permission Sets**
3. Add the profiles/permission sets that should have access
4. Click **Save**

### Step 5: Gather Required Information

Collect the following information for the workshop:
- **Salesforce Instance URL**: Your Salesforce domain (e.g., `https://yourcompany.my.salesforce.com`)
- **Consumer Key**: From Step 3
- **Consumer Secret**: From Step 3
- **Username**: Your Salesforce username
- **Password + Security Token**: Your password concatenated with security token
  - To get your security token: Setup → Personal Setup → My Personal Information → Reset My Security Token

### Step 6: Clone This Repository

```bash
# Option 1: Using Databricks Repos
# In your Databricks workspace:
# 1. Click "Repos" in the sidebar
# 2. Click "Add Repo"
# 3. Enter the repository URL
# 4. Click "Create Repo"

# Option 2: Import notebooks manually
# 1. Download the notebooks from this repository
# 2. In Databricks, click "Workspace" in the sidebar
# 3. Navigate to your desired folder
# 4. Click the dropdown menu > "Import"
# 5. Upload the notebook files
```

## Repository Contents

```
databricks-salesforce-workshop/
│
├── README.md                                          # This file
├── WORKSHOP.md                                        # Detailed workshop description
├── notebooks/
│   ├── 01-Configure-Salesforce-Connection.py         # Connection setup
│   ├── 02-Ingest-Salesforce-Data.py                  # Data ingestion
│   └── 03-Analyze-and-Transform-Salesforce-Data.py   # Analytics and ML
│
├── config/
│   ├── oauth_config_template.json                    # OAuth configuration template
│   ├── salesforce_objects_schema.json                # Standard object schemas
│   └── field_mapping_examples.json                   # Field mapping templates
│
├── resources/
│   ├── images/                                        # Screenshots and diagrams
│   ├── troubleshooting-guide.md                      # Common issues and solutions
│   └── salesforce-api-limits.md                      # API usage and limits guide
│
└── sample-data/
    ├── sample_accounts.csv                           # Sample Account data
    ├── sample_opportunities.csv                      # Sample Opportunity data
    └── sample_custom_objects.json                    # Custom object examples
```

## Getting Started

### Quick Start (15 minutes)

1. **Complete Prerequisites**: Ensure you have Salesforce Connected App created
2. **Import Notebooks**: Add this repository to your Databricks workspace
3. **Open Notebook 01**: Start with `01-Configure-Salesforce-Connection`
4. **Follow Along**: Execute cells step-by-step with your credentials
5. **Proceed Sequentially**: Complete notebooks in order (01 → 02 → 03)

### Detailed Walkthrough

#### Notebook 01: Configure Salesforce Connection (25 minutes)

Learn how to:
- Store Salesforce credentials securely using Databricks Secrets
- Create a Salesforce connection in Lakeflow Connect
- Validate connectivity and API access
- Test authentication with sample queries
- Understand Salesforce API versions and endpoints
- Troubleshoot authentication issues

#### Notebook 02: Ingest Salesforce Data (35 minutes)

Learn how to:
- Discover and browse Salesforce objects (standard and custom)
- Configure object ingestion pipelines
- Set up field-level selection and filtering
- Implement incremental refresh using SystemModstamp
- Handle Salesforce relationships (Lookups and Master-Detail)
- Ingest custom objects and fields
- Monitor API usage and ingestion job status
- Handle large object extraction with bulk API

#### Notebook 03: Analyze and Transform (30 minutes)

Learn how to:
- Query Salesforce data with SQL in Unity Catalog
- Join multiple Salesforce objects (Accounts, Contacts, Opportunities)
- Build sales pipeline analytics
- Create customer 360 views
- Build Delta Live Tables pipelines for CRM data
- Create dashboards for sales metrics
- Apply ML for lead scoring and opportunity prediction
- Implement churn prediction models

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         Salesforce                           │
│  ┌──────────┐  ┌──────────┐  ┌─────────┐  ┌────────────┐   │
│  │ Accounts │  │ Contacts │  │  Leads  │  │   Custom   │   │
│  │          │  │          │  │         │  │  Objects   │   │
│  └──────────┘  └──────────┘  └─────────┘  └────────────┘   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │Opportunities │  │   Cases      │  │   Activities     │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ Salesforce REST/Bulk API
                            │ (OAuth 2.0)
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    Databricks Lakehouse                      │
│                                                               │
│  ┌────────────────────────────────────────────────────┐     │
│  │            Lakeflow Connect                        │     │
│  │  ┌──────────────────────────────────────────┐     │     │
│  │  │     Salesforce Connector                 │     │     │
│  │  │  • OAuth 2.0 Authentication              │     │     │
│  │  │  • Object Discovery                      │     │     │
│  │  │  • Incremental Sync (SystemModstamp)     │     │     │
│  │  │  • Relationship Resolution               │     │     │
│  │  │  • API Rate Limit Management             │     │     │
│  │  └──────────────────────────────────────────┘     │     │
│  └────────────────────────────────────────────────────┘     │
│                            │                                  │
│                            ▼                                  │
│  ┌────────────────────────────────────────────────────┐     │
│  │            Unity Catalog                           │     │
│  │  ┌──────────────────────────────────────────┐     │     │
│  │  │  catalog.schema.sf_accounts              │     │     │
│  │  │  catalog.schema.sf_contacts              │     │     │
│  │  │  catalog.schema.sf_opportunities         │     │     │
│  │  │  catalog.schema.sf_leads                 │     │     │
│  │  │  catalog.schema.sf_custom_objects        │     │     │
│  │  └──────────────────────────────────────────┘     │     │
│  └────────────────────────────────────────────────────┘     │
│                            │                                  │
│                            ▼                                  │
│  ┌────────────────────────────────────────────────────┐     │
│  │       Analytics & ML Workflows                     │     │
│  │  • Sales Pipeline Analytics                        │     │
│  │  • Customer 360 Views                              │     │
│  │  • Lead Scoring Models                             │     │
│  │  • Opportunity Win Prediction                      │     │
│  │  • Churn Prediction                                │     │
│  │  • Revenue Forecasting                             │     │
│  └────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────┘
```

## Key Concepts

### Lakeflow Connect
A managed service in Databricks that simplifies connectivity to external data sources. It handles:
- Authentication and credential management
- Schema discovery and evolution
- Incremental data loading with change data capture
- API rate limit management
- Error handling and retry logic

### Salesforce Object Types

This workshop covers ingesting:
- **Standard Objects**: Account, Contact, Lead, Opportunity, Case, Campaign, etc.
- **Custom Objects**: Your organization's custom objects (e.g., `CustomProduct__c`)
- **Activities**: Task, Event, Email messages
- **Relationships**: Lookup and Master-Detail relationships
- **History Objects**: Field history tracking (e.g., `AccountHistory`)

### Incremental Refresh Strategies

Learn to implement efficient sync patterns:
- **SystemModstamp**: Track last modified timestamp
- **Watermarking**: Track last successful sync timestamp
- **Change Data Capture**: Use Salesforce CDC events (advanced)
- **Merge Strategies**: Upsert patterns for updates and soft deletes
- **Full Refresh**: For small objects or periodic reconciliation

### Salesforce API Considerations

- **API Call Limits**: Understand daily API limits based on your Salesforce edition
- **REST API**: For standard object queries and smaller datasets
- **Bulk API**: For large data extractions (recommended for >2000 records)
- **API Version**: Use the latest API version for best performance
- **Query Optimization**: Use selective queries to reduce API calls

## Best Practices

### Security
- ✅ Use Databricks Secrets for credential storage (never hardcode)
- ✅ Apply principle of least privilege for Salesforce user permissions
- ✅ Use Connected App with IP restrictions when possible
- ✅ Rotate client secrets regularly (every 6-12 months)
- ✅ Enable Unity Catalog access controls on ingested data
- ✅ Audit connection usage and data access patterns
- ✅ Consider using OAuth 2.0 JWT flow for production workloads

### Performance
- ✅ Use Bulk API for large object extractions (>10K records)
- ✅ Implement incremental refresh with SystemModstamp
- ✅ Partition large tables by date fields (CreatedDate, CloseDate)
- ✅ Limit field selection to only required fields
- ✅ Monitor API usage against your daily limits
- ✅ Schedule ingestion during off-peak hours
- ✅ Use appropriate Spark configurations for large datasets
- ✅ Implement retry logic for API rate limiting

### Data Quality
- ✅ Validate required fields are not null
- ✅ Check for duplicate records using Salesforce IDs
- ✅ Monitor data freshness with SystemModstamp
- ✅ Implement data quality checks on critical fields
- ✅ Handle deleted records appropriately (soft vs hard deletes)
- ✅ Verify relationship integrity after ingestion

### Data Governance
- ✅ Document Salesforce object mappings and relationships
- ✅ Tag tables with appropriate metadata (PII, sensitivity)
- ✅ Implement data retention policies aligned with compliance
- ✅ Set up data lineage tracking
- ✅ Monitor data freshness and completeness
- ✅ Establish ownership and stewardship for CRM data

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Authentication failed | Invalid credentials or expired token | Verify Consumer Key/Secret and security token |
| Permission denied | Insufficient object/field permissions | Check user profile and field-level security |
| API limit exceeded | Too many API calls | Monitor usage, use Bulk API, or upgrade edition |
| Connection timeout | Network connectivity | Verify firewall rules and Salesforce instance URL |
| Missing fields | Field-level security | Grant read access to required fields |
| Empty result set | SOQL query too restrictive | Review WHERE clauses and object permissions |
| Relationship not resolved | Missing related object | Ensure related objects are also ingested |
| Slow ingestion | Using REST API for large datasets | Switch to Bulk API for objects >10K records |

### API Limit Management

**Daily API Limits by Edition:**
- Developer: 15,000 calls per day
- Professional: 1,000 base + 1,000 per user license
- Enterprise: 1,000 base + 1,000 per user license
- Unlimited: 5,000 base + 5,000 per user license

**Best Practices:**
- Monitor API usage in Salesforce: Setup → System Overview → API Usage
- Use Bulk API for large extractions (doesn't count against limits the same way)
- Schedule incremental refreshes instead of full reloads
- Batch multiple object ingestions in single pipeline

See [troubleshooting-guide.md](resources/troubleshooting-guide.md) for detailed solutions.

## Additional Resources

### Databricks Documentation
- [Lakeflow Connect Overview](https://docs.databricks.com/en/connect/index.html)
- [Unity Catalog Guide](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Databricks SQL](https://docs.databricks.com/en/sql/index.html)

### Salesforce Documentation
- [REST API Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/)
- [Bulk API 2.0 Guide](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/)
- [SOQL and SOSL Reference](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/)
- [Connected Apps](https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm)
- [Object Reference](https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/)

### Community
- [Databricks Community Forum](https://community.databricks.com/)
- [Salesforce Trailblazer Community](https://trailblazers.salesforce.com/)
- [Stack Overflow - Databricks](https://stackoverflow.com/questions/tagged/databricks)
- [Stack Overflow - Salesforce](https://stackoverflow.com/questions/tagged/salesforce)

## Use Cases Covered

### Sales Analytics
- Pipeline health monitoring and forecasting
- Win/loss analysis by product, region, and rep
- Sales cycle duration and conversion rates
- Quota attainment tracking

### Customer 360
- Unified customer view across accounts, contacts, and activities
- Customer lifetime value calculation
- Cross-sell and upsell opportunity identification
- Customer segmentation and profiling

### Predictive Analytics
- Lead scoring using ML models
- Opportunity win probability prediction
- Customer churn prediction
- Revenue forecasting with time series analysis

### Operational Analytics
- Case resolution time and SLA compliance
- Campaign ROI and attribution analysis
- User adoption and activity metrics
- Data quality monitoring and alerts

## Sample Queries

### Sales Pipeline Analysis
```sql
-- Analyze opportunity pipeline by stage
SELECT 
  StageName,
  COUNT(*) as opportunity_count,
  SUM(Amount) as total_amount,
  AVG(Amount) as avg_amount,
  AVG(Probability) as avg_probability
FROM catalog.schema.sf_opportunities
WHERE IsClosed = false
GROUP BY StageName
ORDER BY total_amount DESC;
```

### Customer 360 View
```sql
-- Create customer 360 view
SELECT 
  a.Id as account_id,
  a.Name as account_name,
  a.Industry,
  COUNT(DISTINCT c.Id) as contact_count,
  COUNT(DISTINCT o.Id) as opportunity_count,
  SUM(o.Amount) as total_opportunity_value,
  COUNT(DISTINCT cs.Id) as case_count
FROM catalog.schema.sf_accounts a
LEFT JOIN catalog.schema.sf_contacts c ON a.Id = c.AccountId
LEFT JOIN catalog.schema.sf_opportunities o ON a.Id = o.AccountId
LEFT JOIN catalog.schema.sf_cases cs ON a.Id = cs.AccountId
GROUP BY a.Id, a.Name, a.Industry;
```

## Support

### Workshop Issues
For issues with workshop content:
- Open an issue in this repository
- Contact the workshop developer: Ajit Kalura

### Product Support
For Databricks product issues:
- Contact Databricks Support through your workspace
- Visit [Databricks Help Center](https://help.databricks.com/)

For Salesforce issues:
- Contact Salesforce Support
- Visit [Salesforce Help](https://help.salesforce.com/)

## License

This workshop is provided for educational purposes. Please review your organization's policies regarding data handling and external connections.

## Changelog

### Version 1.0 (January 2026)
- Initial release
- Covers Runtime 14.3+ with Unity Catalog
- Three progressive notebooks
- OAuth 2.0 authentication setup
- Incremental refresh patterns with SystemModstamp
- Bulk API integration examples
- Analytics and ML examples for sales and customer data

## Contributing

Contributions are welcome! Please:
1. Fork this repository
2. Create a feature branch
3. Submit a pull request with detailed description

## Acknowledgments

- Databricks Product Team for Lakeflow Connect
- Salesforce Developer Relations team
- Workshop participants and reviewers

---

**Ready to get started?** Open `01-Configure-Salesforce-Connection` and begin your journey connecting Salesforce to Databricks! 🚀
