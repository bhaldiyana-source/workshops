# Lab 4: Multi-User Dashboard

## Overview

This lab demonstrates building multi-user dashboards with Dash and Plotly that respect On-Behalf-Of (OBO) permissions. Each user sees data filtered by their Unity Catalog permissions, enabling secure, role-based data visualizations.

## Learning Objectives

By completing this lab, you will:
- ✅ Build interactive dashboards with Dash and Plotly
- ✅ Implement user-specific data views with OBO permissions
- ✅ Create dynamic KPIs and visualizations
- ✅ Add real-time data refresh capabilities
- ✅ Build responsive layouts with Bootstrap components
- ✅ Manage multi-user state effectively

## Prerequisites

- Completion of Lab 1 and Lab 2
- Unity Catalog table with data
- Understanding of data visualization concepts
- Basic knowledge of callbacks and state management

## Project Structure

```
lab4-multi-user-dashboard/
├── app.py                  # Main Dash application
├── app.yaml                # Databricks app configuration
├── requirements.txt        # Python dependencies
├── components/
│   ├── __init__.py
│   └── charts.py          # Chart building utilities
└── README.md              # This file
```

## Configuration

### Step 1: Update app.yaml

Edit `app.yaml` and configure your data source:

```yaml
env:
  - name: WAREHOUSE_ID
    value: "YOUR_WAREHOUSE_ID_HERE"
  
  - name: CATALOG_NAME
    value: "main"
  
  - name: SCHEMA_NAME
    value: "sales"
  
  - name: SAMPLE_TABLE
    value: "transactions"
```

### Step 2: Prepare Sample Data (Optional)

If you don't have data, create a sample table:

```sql
CREATE TABLE main.sales.transactions AS
SELECT 
  date_add('2024-01-01', CAST(rand() * 365 AS INT)) as date,
  CAST(rand() * 10000 AS INT) as sales,
  CAST(rand() * 100 AS INT) as quantity,
  element_at(array('Electronics', 'Clothing', 'Food', 'Books', 'Home'), 
             CAST(rand() * 5 + 1 AS INT)) as category,
  element_at(array('North', 'South', 'East', 'West'), 
             CAST(rand() * 4 + 1 AS INT)) as region,
  CAST(rand() * 50000 AS DECIMAL(10,2)) as revenue
FROM range(1000);
```

### Step 3: Set Permissions

Grant users access to the table:

```sql
GRANT SELECT ON TABLE main.sales.transactions TO `user@example.com`;
GRANT USE CATALOG ON CATALOG main TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA main.sales TO `user@example.com`;
```

## Deployment

```bash
# Navigate to lab directory
cd apps/lab4-multi-user-dashboard

# Deploy the app
databricks apps deploy /Users/<your-email>/lab4-multi-user-dashboard

# View logs
databricks apps logs /Users/<your-email>/lab4-multi-user-dashboard
```

## Features

### 1. User Context Header

- Displays current user information
- Shows user's display name and email
- Branded navigation bar with Databricks colors

### 2. OBO Permissions Alert

Prominent alert explaining that:
- Data is filtered by user permissions
- Different users see different data
- Unity Catalog enforces access control

### 3. KPI Cards

Four key performance indicators:
- **Total Revenue**: Sum of all revenue
- **Total Orders**: Count of transactions
- **Average Order Value**: Revenue per order
- **Growth Rate**: Period-over-period comparison

### 4. Interactive Filters

Three filter controls:
- **Date Range Picker**: Select start and end dates
- **Category Dropdown**: Filter by product category
- **Region Dropdown**: Filter by geographic region

**Refresh Button**: Manually refresh data from Databricks

### 5. Visualizations

**Revenue Over Time (Line Chart)**:
- Shows revenue trends across selected date range
- Interactive hover for detailed values
- Responsive to filters

**Sales by Category (Pie Chart)**:
- Breakdown of sales by product category
- Donut chart visualization
- Percentage labels

**Regional Performance (Bar Chart)**:
- Compares revenue across regions
- Color-coded by performance
- Sortable by value

**Daily Trends (Line Chart)**:
- Shows quantity sold over time
- Trend analysis
- Multi-metric support

### 6. Data Preview Table

- Shows first 10 rows of filtered data
- Responsive table design
- Striped rows for readability

### 7. Auto-Refresh

- Automatic data refresh every 60 seconds
- Configurable interval
- Manual refresh button available

## On-Behalf-Of (OBO) in Action

### How It Works

```python
def get_user_data() -> pd.DataFrame:
    """
    Query automatically filtered by user's permissions.
    """
    db = DatabaseConnection()  # Uses OBO token automatically
    
    query = f"SELECT * FROM {catalog}.{schema}.{table}"
    
    # This query respects:
    # - User's SELECT permissions
    # - Row-level security policies
    # - Column-level security policies
    
    results = db.execute_query_as_dict(query)
    return pd.DataFrame(results)
```

### Multi-User Scenarios

**Scenario 1: Regional Managers**
- Manager A (North region): Sees only North region data
- Manager B (South region): Sees only South region data
- Same dashboard, different data!

**Scenario 2: Department Access**
- Finance team: Sees all financial columns
- Sales team: Sensitive columns are NULL or hidden
- Automatic based on Unity Catalog grants

**Scenario 3: Time-Based Access**
- Current quarter data: All users
- Historical data: Analysts only
- Real-time data: Operations team only

## Technical Implementation

### Dash Callbacks

```python
@callback(
    [Output('chart', 'figure')],
    [Input('filter', 'value')]
)
def update_chart(filter_value):
    # Load user-specific data with OBO
    df = get_user_data()
    
    # Apply filters
    filtered_df = df[df['category'] == filter_value]
    
    # Create chart
    fig = px.line(filtered_df, x='date', y='revenue')
    
    return [fig]
```

### State Management

```python
# Store data in browser memory
dcc.Store(id='data-store')

# Load once, use multiple times
@callback(
    Output('data-store', 'data'),
    Input('refresh-button', 'n_clicks')
)
def load_data(n_clicks):
    df = get_user_data()
    return df.to_json()  # Serialize for storage
```

### Responsive Layout

Uses Bootstrap grid system:

```python
dbc.Row([
    dbc.Col([chart1], width=12, md=8),  # 8 columns on medium+
    dbc.Col([chart2], width=12, md=4)   # 4 columns on medium+
])
```

## Performance Optimization

### 1. Client-Side Caching

```python
# Store data once, filter on client side
dcc.Store(id='data-store')
```

### 2. Selective Updates

```python
# Only update changed components
@callback(
    Output('specific-chart', 'figure'),
    Input('specific-filter', 'value'),
    prevent_initial_call=True
)
```

### 3. Data Aggregation

```python
# Aggregate in database, not in Python
query = """
SELECT 
  date,
  SUM(revenue) as total_revenue
FROM table
GROUP BY date
"""
```

### 4. Pagination

```python
# Limit initial data load
query = f"SELECT * FROM {table} LIMIT 1000"
```

## Customization Guide

### Adding New Charts

```python
# In layout
dbc.Col([
    dbc.Card([
        dbc.CardHeader("New Chart"),
        dbc.CardBody([
            dcc.Graph(id='new-chart')
        ])
    ])
])

# Add callback
@callback(
    Output('new-chart', 'figure'),
    Input('data-store', 'data')
)
def update_new_chart(json_data):
    df = pd.read_json(json_data, orient='split')
    fig = px.scatter(df, x='x', y='y')
    return fig
```

### Adding Filters

```python
# Add dropdown
dcc.Dropdown(
    id='new-filter',
    options=[...],
    value='default'
)

# Update callback inputs
@callback(
    Output(...),
    Input('new-filter', 'value')
)
def update_with_filter(filter_value):
    # Apply filter
    pass
```

### Styling

Customize with CSS:

```python
app = Dash(
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "custom.css"  # Your custom styles
    ]
)
```

## Security Best Practices

### 1. Always Use OBO

```python
# ✅ GOOD: Uses OBO token
db = DatabaseConnection()

# ❌ BAD: Hardcoded credentials
db = DatabaseConnection(token="hardcoded-token")
```

### 2. Validate User Input

```python
def sanitize_filter(value):
    """Prevent SQL injection in filters."""
    if not re.match(r'^[a-zA-Z0-9_]+$', value):
        raise ValueError("Invalid filter value")
    return value
```

### 3. Limit Data Exposure

```python
# Limit rows
query = f"SELECT * FROM {table} LIMIT 1000"

# Select specific columns
query = f"SELECT id, name, amount FROM {table}"
```

### 4. Log User Actions

```python
log_user_action("dashboard_accessed", {
    "filters": filters,
    "date_range": date_range
})
```

## Troubleshooting

### Issue: "No data available"

**Causes:**
- User lacks SELECT permission
- Table is empty
- Filters too restrictive

**Solutions:**
- Check Unity Catalog permissions
- Verify table has data
- Reset filters

### Issue: Charts not updating

**Causes:**
- Callback not triggering
- Data not refreshing
- Browser cache issues

**Solutions:**
- Check callback inputs/outputs
- Click refresh button
- Clear browser cache

### Issue: Slow performance

**Solutions:**
- Reduce data volume with LIMIT
- Add database indexes
- Implement pagination
- Increase SQL Warehouse size

### Issue: Different users see same data

**Causes:**
- Not using OBO correctly
- Using cached data incorrectly
- Service principal instead of OBO

**Solutions:**
- Verify DatabaseConnection uses OBO token
- Clear server-side caches
- Check app.yaml configuration

## Extension Ideas

Enhance this dashboard with:

1. **Export Functionality**: Download charts as images or data as CSV
2. **Drill-Down**: Click charts to filter other charts
3. **Custom Metrics**: Let users define their own KPIs
4. **Annotations**: Add comments and notes to charts
5. **Alerts**: Set thresholds and get notifications
6. **Comparison Mode**: Compare multiple time periods
7. **Mobile Optimization**: Improve mobile responsiveness
8. **Dark Mode**: Add theme switcher

## Best Practices Summary

✅ **DO:**
- Use OBO for all database queries
- Implement proper error handling
- Add loading indicators
- Log user interactions
- Test with multiple user profiles
- Optimize queries before visualization

❌ **DON'T:**
- Hardcode credentials
- Cache data across users
- Load entire tables without LIMIT
- Ignore mobile users
- Skip input validation
- Expose raw error messages

## Next Steps

After completing this lab:

1. **Lab 5: RESTful API** - Build FastAPI backend for data access
2. Explore advanced Dash features (DataTables, more chart types)
3. Integrate with MLflow models from Lab 3
4. Add real-time data streaming

## Additional Resources

- [Dash Documentation](https://dash.plotly.com/)
- [Plotly Python](https://plotly.com/python/)
- [Dash Bootstrap Components](https://dash-bootstrap-components.opensource.faculty.ai/)
- [Unity Catalog Row/Column Security](https://docs.databricks.com/data-governance/unity-catalog/row-and-column-filters.html)

---

**Happy Visualizing! 📊**
