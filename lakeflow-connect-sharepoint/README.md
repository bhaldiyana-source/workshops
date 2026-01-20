# Databricks Lakeflow Connect SharePoint Connector Workshop

## Overview

This hands-on workshop teaches you how to integrate Microsoft SharePoint data into Databricks using **Lakeflow Connect**. You'll learn to configure connections, ingest SharePoint documents and metadata, and build analytics pipelines that combine SharePoint data with your lakehouse architecture.

### What You'll Build

By completing this workshop, you will:
- ✅ Configure secure OAuth 2.0 connectivity to SharePoint Online
- ✅ Ingest SharePoint documents, lists, and metadata into Delta tables
- ✅ Build incremental refresh pipelines for SharePoint data
- ✅ Create analytics and visualizations from SharePoint content
- ✅ Apply AI/ML to analyze SharePoint documents

## Workshop Structure

| Notebook | Title | Duration | Description |
|----------|-------|----------|-------------|
| 01 | Configure SharePoint Connection | 25 min | Set up OAuth authentication and create SharePoint connection |
| 02 | Ingest SharePoint Data | 35 min | Configure ingestion pipelines for documents, lists, and metadata |
| 03 | Analyze and Transform | 30 min | Build analytics pipelines and visualizations |

## Prerequisites

### Required Access
- **Databricks Workspace** (AWS, Azure, or GCP) with Unity Catalog enabled
- **Workspace Admin** or **Metastore Admin** permissions
- **Microsoft SharePoint Online** environment
- **SharePoint Site Collection Administrator** or appropriate site permissions

### Required Knowledge
- Basic SQL and Python programming
- Understanding of SharePoint site structures (sites, libraries, lists)
- Familiarity with Delta Lake and Unity Catalog concepts

### Technical Requirements
- Databricks Runtime 14.3 or higher
- Microsoft Entra ID (Azure AD) application registration
- Network connectivity between Databricks and Microsoft 365

## Setup Instructions

### Step 1: Register Azure AD Application

1. Navigate to [Azure Portal](https://portal.azure.com) > **Microsoft Entra ID** > **App registrations**
2. Click **New registration**
3. Configure the application:
   - **Name**: `Databricks-SharePoint-Connector`
   - **Supported account types**: Accounts in this organizational directory only
   - **Redirect URI**: Leave blank for now
4. Click **Register**

### Step 2: Configure API Permissions

1. In your registered app, go to **API permissions**
2. Click **Add a permission** > **Microsoft Graph**
3. Select **Application permissions**
4. Add the following permissions:
   - `Sites.Read.All` - Read items in all site collections
   - `Files.Read.All` - Read files in all site collections
   - `User.Read.All` - Read all users' full profiles (optional)
5. Click **Grant admin consent** for your organization

### Step 3: Create Client Secret

1. Go to **Certificates & secrets** > **Client secrets**
2. Click **New client secret**
3. Add a description (e.g., "Databricks Lakeflow Connect")
4. Select expiration period (recommend 12-24 months)
5. Click **Add**
6. **Important**: Copy the secret value immediately (you won't be able to see it again)

### Step 4: Gather Required Information

Collect the following information for the workshop:
- **Tenant ID**: Found in Azure AD > Overview
- **Application (Client) ID**: Found in your app registration > Overview
- **Client Secret**: The value you copied in Step 3
- **SharePoint Site URL**: The URL of the SharePoint site you want to connect to

### Step 5: Clone This Repository

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
databricks-sharepoint-workshop/
│
├── README.md                                          # This file
├── WORKSHOP.md                                        # Detailed workshop description
├── notebooks/
│   ├── 01-Configure-SharePoint-Connection.py         # Connection setup
│   ├── 02-Ingest-SharePoint-Data.py                  # Data ingestion
│   └── 03-Analyze-and-Transform-SharePoint-Data.py   # Analytics and ML
│
├── config/
│   ├── oauth_config_template.json                    # OAuth configuration template
│   └── sharepoint_schema_examples.json               # Sample SharePoint schemas
│
├── resources/
│   ├── images/                                        # Screenshots and diagrams
│   └── troubleshooting-guide.md                      # Common issues and solutions
│
└── sample-data/
    └── sample_sharepoint_metadata.csv                # Sample data for testing
```

## Getting Started

### Quick Start (15 minutes)

1. **Complete Prerequisites**: Ensure you have Azure AD app registration completed
2. **Import Notebooks**: Add this repository to your Databricks workspace
3. **Open Notebook 01**: Start with `01-Configure-SharePoint-Connection`
4. **Follow Along**: Execute cells step-by-step with your credentials
5. **Proceed Sequentially**: Complete notebooks in order (01 → 02 → 03)

### Detailed Walkthrough

#### Notebook 01: Configure SharePoint Connection (25 minutes)

Learn how to:
- Store credentials securely using Databricks Secrets
- Create a SharePoint connection in Lakeflow Connect
- Validate connectivity and permissions
- Troubleshoot authentication issues

#### Notebook 02: Ingest SharePoint Data (35 minutes)

Learn how to:
- Browse SharePoint site hierarchies
- Configure document library ingestion
- Set up SharePoint list ingestion
- Implement incremental refresh patterns
- Monitor ingestion job status
- Handle metadata and custom columns

#### Notebook 03: Analyze and Transform (30 minutes)

Learn how to:
- Query SharePoint metadata with SQL
- Join SharePoint data with lakehouse tables
- Build Delta Live Tables pipelines
- Create visualizations and dashboards
- Apply AI for document classification
- Implement content analysis workflows

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Microsoft 365 / SharePoint                │
│  ┌────────────┐  ┌────────────┐  ┌─────────────────────┐   │
│  │   Sites    │  │  Libraries │  │       Lists         │   │
│  └────────────┘  └────────────┘  └─────────────────────┘   │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ Microsoft Graph API
                            │ (OAuth 2.0)
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    Databricks Lakehouse                      │
│                                                               │
│  ┌────────────────────────────────────────────────────┐     │
│  │            Lakeflow Connect                        │     │
│  │  ┌──────────────────────────────────────────┐     │     │
│  │  │     SharePoint Connector                 │     │     │
│  │  │  • Authentication                        │     │     │
│  │  │  • Schema Discovery                      │     │     │
│  │  │  • Incremental Sync                      │     │     │
│  │  └──────────────────────────────────────────┘     │     │
│  └────────────────────────────────────────────────────┘     │
│                            │                                  │
│                            ▼                                  │
│  ┌────────────────────────────────────────────────────┐     │
│  │            Unity Catalog                           │     │
│  │  ┌──────────────────────────────────────────┐     │     │
│  │  │  catalog.schema.sharepoint_documents     │     │     │
│  │  │  catalog.schema.sharepoint_lists         │     │     │
│  │  │  catalog.schema.sharepoint_metadata      │     │     │
│  │  └──────────────────────────────────────────┘     │     │
│  └────────────────────────────────────────────────────┘     │
│                            │                                  │
│                            ▼                                  │
│  ┌────────────────────────────────────────────────────┐     │
│  │       Analytics & ML Workflows                     │     │
│  │  • SQL Analytics                                   │     │
│  │  • Delta Live Tables                               │     │
│  │  • ML Models                                       │     │
│  │  • Dashboards                                      │     │
│  └────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────┘
```

## Key Concepts

### Lakeflow Connect
A managed service in Databricks that simplifies connectivity to external data sources. It handles:
- Authentication and credential management
- Schema discovery and evolution
- Incremental data loading
- Error handling and retry logic

### SharePoint Data Types

This workshop covers ingesting:
- **Documents**: Files stored in document libraries (metadata only)
- **Lists**: Structured data in SharePoint lists
- **Metadata**: Custom columns, properties, and taxonomy
- **Site Information**: Site structure and permissions

### Incremental Refresh

Learn to implement efficient sync patterns:
- **Modified Date Tracking**: Sync only changed items
- **Change Tokens**: Use SharePoint change log
- **Watermarking**: Track last successful sync
- **Merge Strategies**: Upsert patterns for updates

## Best Practices

### Security
- ✅ Use Databricks Secrets for credential storage
- ✅ Apply principle of least privilege for SharePoint permissions
- ✅ Rotate client secrets regularly (every 6-12 months)
- ✅ Enable Unity Catalog access controls
- ✅ Audit connection usage and data access

### Performance
- ✅ Use incremental refresh instead of full reloads
- ✅ Partition large document libraries by date or category
- ✅ Limit initial ingestion scope for testing
- ✅ Monitor API throttling limits
- ✅ Optimize Spark configurations for large datasets

### Data Governance
- ✅ Document SharePoint source mappings
- ✅ Tag tables with appropriate metadata
- ✅ Implement data quality checks
- ✅ Set up data retention policies
- ✅ Monitor data freshness and lineage

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Authentication failed | Invalid credentials or expired secret | Verify client ID/secret in Azure AD |
| Permission denied | Insufficient API permissions | Check Graph API permissions and admin consent |
| Connection timeout | Network connectivity | Verify firewall rules and DNS resolution |
| Throttling errors | Too many API requests | Implement retry logic and rate limiting |
| Schema mismatch | SharePoint columns changed | Refresh schema in Lakeflow Connect |

See [troubleshooting-guide.md](resources/troubleshooting-guide.md) for detailed solutions.

## Additional Resources

### Databricks Documentation
- [Lakeflow Connect Overview](https://docs.databricks.com/en/connect/index.html)
- [Unity Catalog Guide](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)

### Microsoft Documentation
- [Microsoft Graph API](https://learn.microsoft.com/en-us/graph/overview)
- [SharePoint REST API](https://learn.microsoft.com/en-us/sharepoint/dev/sp-add-ins/get-to-know-the-sharepoint-rest-service)
- [Azure AD App Registration](https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)

### Community
- [Databricks Community Forum](https://community.databricks.com/)
- [Stack Overflow - Databricks](https://stackoverflow.com/questions/tagged/databricks)

## Support

### Workshop Issues
For issues with workshop content:
- Open an issue in this repository
- Contact the workshop developer: Ajit Kalura

### Product Support
For Databricks product issues:
- Contact Databricks Support through your workspace
- Visit [Databricks Help Center](https://help.databricks.com/)

## License

This workshop is provided for educational purposes. Please review your organization's policies regarding data handling and external connections.

## Changelog

### Version 1.0 (January 2026)
- Initial release
- Covers Runtime 14.3+ with Unity Catalog
- Three progressive notebooks
- OAuth 2.0 authentication setup
- Incremental refresh patterns
- Analytics and ML examples

## Contributing

Contributions are welcome! Please:
1. Fork this repository
2. Create a feature branch
3. Submit a pull request with detailed description

## Acknowledgments

- Databricks Product Team for Lakeflow Connect
- Microsoft Graph API team
- Workshop participants and reviewers

---

**Ready to get started?** Open `01-Configure-SharePoint-Connection` and begin your journey connecting SharePoint to Databricks! 🚀
