# Lab 2: Data Explorer

## Overview

This lab builds an interactive data exploration tool for Unity Catalog using Streamlit. You'll learn advanced techniques for browsing metadata, building dynamic queries, and respecting user permissions through OBO authentication.

## Learning Objectives

By completing this lab, you will:
- ✅ Browse Unity Catalog hierarchically (catalogs → schemas → tables)
- ✅ Query metadata using SHOW and DESCRIBE commands
- ✅ Execute dynamic SQL queries with user permissions
- ✅ Build a visual query builder interface
- ✅ Implement caching for performance optimization
- ✅ Create an intuitive data exploration UI

## Prerequisites

- Completion of Lab 1 (Hello World)
- SQL Warehouse with read access
- Unity Catalog with at least one catalog and table
- Understanding of SQL and Unity Catalog concepts

## Project Structure

```
lab2-data-explorer/
├── app.py              # Main Streamlit application
├── app.yaml            # Databricks app configuration
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Configuration

### Step 1: Update app.yaml

Edit `app.yaml` and replace the placeholder:

```yaml
resources:
  - name: warehouse_connection
    warehouse:
      warehouse_id: "YOUR_WAREHOUSE_ID_HERE"
```

### Step 2: Verify Permissions

Ensure your user has:
- `USE` permission on the SQL Warehouse
- `USE CATALOG` on at least one catalog
- `USE SCHEMA` on at least one schema
- `SELECT` on at least one table

## Features

### 1. Browse Catalog Tab

Interactive hierarchical browsing:

- **Catalog Selection**: Lists all catalogs you have access to
- **Schema Selection**: Shows schemas within the selected catalog
- **Table Selection**: Displays tables in the selected schema
- **Table Details**: Shows column names, data types, and comments
- **Data Preview**: Load and preview table data with configurable row limits
- **CSV Export**: Download preview data as CSV

**Key Implementation:**

```python
@st.cache_data(ttl=600)
def get_catalogs() -> List[str]:
    """Cached for 10 minutes to improve performance."""
    db = DatabaseConnection()
    explorer = UnityCatalogExplorer(db)
    return explorer.list_catalogs()
```

### 2. Custom Query Tab

SQL query editor with execution:

- **Syntax Highlighting**: SQL code editor
- **Smart Defaults**: Pre-populates with selected table
- **Query Execution**: Run any valid SQL query
- **Results Display**: Tabular results with statistics
- **Error Handling**: User-friendly error messages
- **CSV Export**: Download query results

**Query Examples:**

```sql
-- Get current user and catalog context
SELECT current_user() as user, 
       current_catalog() as catalog,
       current_database() as schema;

-- Query a table with filters
SELECT * 
FROM main.default.my_table 
WHERE date >= '2024-01-01'
LIMIT 100;

-- Aggregate data
SELECT category, COUNT(*) as count
FROM main.sales.transactions
GROUP BY category
ORDER BY count DESC;
```

### 3. Query Builder Tab

Visual interface for building queries:

- **Dropdown Selection**: Choose catalog, schema, and table
- **Column Selection**: Select specific columns or all
- **WHERE Clause Builder**: Add filters with various operators
- **ORDER BY**: Sort results by any column
- **LIMIT Control**: Specify result set size
- **Query Preview**: See generated SQL before execution
- **One-Click Execute**: Run built query instantly

**Supported Operators:**
- Equality: `=`, `!=`
- Comparison: `>`, `<`, `>=`, `<=`
- Pattern Matching: `LIKE`
- Set Membership: `IN`

### 4. Sidebar Features

- **User Context**: Displays current user information
- **Permissions Notice**: Reminds users about OBO security
- **Query History**: Shows last 5 executed queries with row counts

## Deployment

```bash
# Navigate to lab directory
cd apps/lab2-data-explorer

# Deploy the app
databricks apps deploy /Users/<your-email>/lab2-data-explorer

# View logs
databricks apps logs /Users/<your-email>/lab2-data-explorer
```

## Key Technical Concepts

### Caching Strategy

The app uses Streamlit's caching to optimize performance:

```python
@st.cache_data(ttl=600)  # 10 minutes
def get_catalogs():
    # Expensive metadata query
    pass

@st.cache_data(ttl=300)   # 5 minutes
def get_table_schema():
    # Moderately expensive query
    pass
```

**Benefits:**
- Reduces database load
- Improves app responsiveness
- Automatic cache invalidation after TTL

### Session State Management

Maintains user selections across interactions:

```python
if 'selected_catalog' not in st.session_state:
    st.session_state.selected_catalog = None

# Later...
st.session_state.selected_catalog = selected_catalog
```

### Dynamic SQL Generation

Safely builds SQL queries with validation:

```python
# Validate identifiers to prevent SQL injection
if validate_sql_identifier(table_name):
    query = f"SELECT * FROM {catalog}.{schema}.{table}"
else:
    raise ValueError("Invalid identifier")
```

### Unity Catalog Metadata Queries

Common patterns for exploring Unity Catalog:

```sql
-- List catalogs
SHOW CATALOGS;

-- List schemas
SHOW SCHEMAS IN catalog_name;

-- List tables
SHOW TABLES IN catalog_name.schema_name;

-- Describe table
DESCRIBE catalog_name.schema_name.table_name;

-- Get detailed table info
DESCRIBE EXTENDED catalog_name.schema_name.table_name;
```

## Security Considerations

### On-Behalf-Of (OBO) in Action

Every query respects user permissions:

1. **Catalog Visibility**: Users only see catalogs they have access to
2. **Schema Filtering**: Only accessible schemas appear
3. **Table Access**: Tables require SELECT permission
4. **Data Security**: Row-level and column-level security automatically applied

**Example Scenario:**

- User A sees: `main`, `finance`, `hr` catalogs
- User B sees: `main`, `engineering` catalogs
- Same app, different data based on permissions!

### Input Validation

Prevents SQL injection attacks:

```python
def validate_sql_identifier(identifier: str) -> bool:
    """Only allow safe characters in identifiers."""
    return bool(re.match(r'^[a-zA-Z0-9_`]+$', identifier))
```

### Audit Logging

All user actions are logged:

```python
log_user_action("table_preview", {
    "table": f"{catalog}.{schema}.{table}",
    "rows": len(data)
})
```

## Performance Optimization

### Best Practices Implemented

1. **Connection Pooling**: Reuse database connections
2. **Result Caching**: Cache metadata queries
3. **Lazy Loading**: Load data only when requested
4. **Pagination**: Limit result set sizes
5. **Efficient Queries**: Use LIMIT clauses appropriately

### Performance Tips

For large datasets:
- Use appropriate LIMIT values (start small)
- Apply filters in the database, not in memory
- Select only needed columns
- Consider creating views for complex queries

## Troubleshooting

### Issue: "No catalogs accessible"

**Causes:**
- User lacks `USE CATALOG` permission
- SQL Warehouse not connected properly
- Network/connectivity issues

**Solutions:**
```sql
-- Grant catalog access
GRANT USE CATALOG ON CATALOG main TO `user@example.com`;
```

### Issue: Empty schema or table lists

**Causes:**
- Lack of `USE SCHEMA` or `SELECT` permissions
- Empty catalog/schema
- Permission propagation delay

**Solutions:**
- Verify permissions with workspace admin
- Check if schemas/tables actually exist
- Wait a moment and refresh (clear cache)

### Issue: Query execution errors

**Common Errors:**

| Error | Cause | Solution |
|-------|-------|----------|
| "Table or view not found" | Wrong name or no access | Verify spelling and permissions |
| "Permission denied" | Lack of SELECT permission | Grant SELECT on table |
| "Timeout" | Query too expensive | Add LIMIT, optimize query |
| "Invalid syntax" | SQL syntax error | Check SQL syntax |

### Issue: Slow performance

**Solutions:**
- Increase cache TTL for stable data
- Use smaller LIMIT values
- Upgrade SQL Warehouse size
- Add filters to reduce data volume

## Usage Examples

### Example 1: Exploring a New Catalog

1. Open the **Browse Catalog** tab
2. Select `main` catalog
3. Select `default` schema
4. Choose a table from the list
5. Click **Load Preview** to see data
6. Download as CSV if needed

### Example 2: Running Analytics Queries

1. Go to **Custom Query** tab
2. Write an analytical query:
   ```sql
   SELECT 
     DATE_TRUNC('month', order_date) as month,
     SUM(amount) as total_sales
   FROM main.sales.orders
   GROUP BY 1
   ORDER BY 1 DESC
   LIMIT 12
   ```
3. Click **Execute Query**
4. Review results and download

### Example 3: Building Filtered Queries

1. Switch to **Query Builder** tab
2. Select catalog, schema, and table
3. Choose specific columns
4. Add a WHERE filter (e.g., `status = 'active'`)
5. Set order by `created_at`
6. Set limit to 500
7. Review generated SQL
8. Click **Execute Built Query**

## Best Practices

### For Users

✅ **DO:**
- Start with small LIMIT values
- Use filters to reduce data volume
- Cache frequently accessed tables
- Download results for offline analysis

❌ **DON'T:**
- Query entire tables without LIMIT
- Share query results containing sensitive data
- Run expensive queries during peak hours
- Bypass security by caching credentials

### For Administrators

✅ **DO:**
- Grant minimum necessary permissions
- Monitor query patterns and costs
- Set up appropriate SQL Warehouse sizes
- Configure audit logging

❌ **DON'T:**
- Grant excessive permissions
- Share admin credentials
- Ignore performance issues
- Disable OBO authentication

## Extension Ideas

Want to enhance this lab? Try:

1. **Advanced Filters**: Add date pickers, multi-select filters
2. **Visualizations**: Add charts and graphs to results
3. **Saved Queries**: Let users save favorite queries
4. **Export Formats**: Support JSON, Parquet, Excel exports
5. **Collaboration**: Share queries with team members
6. **Scheduling**: Run queries on a schedule

## Next Steps

After completing this lab:

1. **Lab 3: ML Model Interface** - Build a Gradio interface for MLflow models
2. **Lab 4: Multi-User Dashboard** - Create role-based dashboards with Dash
3. **Lab 5: RESTful API** - Develop a FastAPI backend for data access

## Additional Resources

- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/)
- [SQL Language Reference](https://docs.databricks.com/sql/language-manual/)
- [Streamlit Caching](https://docs.streamlit.io/library/advanced-features/caching)
- [Query Optimization Guide](https://docs.databricks.com/optimizations/index.html)

---

**Happy Exploring! 🔍**
