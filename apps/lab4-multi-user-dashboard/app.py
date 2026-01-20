"""
Lab 4: Multi-User Dashboard
A Dash application demonstrating user-specific data views with On-Behalf-Of permissions.
Features interactive charts, KPIs, and role-based content.
"""

from dash import Dash, html, dcc, Input, Output, State, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.auth import get_user_context, log_user_action
from utils.database import DatabaseConnection
from utils.logging_config import setup_logging, get_logger
from components.charts import ChartBuilder

# Configure logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_logger(__name__)

# Initialize Dash app
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
    suppress_callback_exceptions=True,
    title="Lab 4: Multi-User Dashboard"
)

# Get user context
user = get_user_context()
logger.info(f"Dashboard initialized for user: {user.user_email}")


def get_user_data() -> pd.DataFrame:
    """
    Fetch user-specific data from Databricks.
    This respects OBO permissions - each user sees only their authorized data.
    """
    try:
        catalog = os.getenv("CATALOG_NAME", "main")
        schema = os.getenv("SCHEMA_NAME", "default")
        table = os.getenv("SAMPLE_TABLE", "sample_data")
        
        db = DatabaseConnection()
        
        # Query with OBO - automatically filtered by user permissions
        query = f"""
        SELECT *
        FROM {catalog}.{schema}.{table}
        LIMIT 1000
        """
        
        results = db.execute_query_as_dict(query)
        df = pd.DataFrame(results) if results else pd.DataFrame()
        
        log_user_action("dashboard_data_loaded", {"rows": len(df)})
        return df
        
    except Exception as e:
        logger.error(f"Error loading user data: {str(e)}")
        # Return sample data for demo
        return generate_sample_data()


def generate_sample_data() -> pd.DataFrame:
    """Generate sample data for demonstration."""
    import numpy as np
    
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='D')
    n_records = len(dates)
    
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home']
    regions = ['North', 'South', 'East', 'West']
    
    df = pd.DataFrame({
        'date': dates,
        'sales': np.random.randint(1000, 10000, n_records),
        'quantity': np.random.randint(10, 100, n_records),
        'category': np.random.choice(categories, n_records),
        'region': np.random.choice(regions, n_records),
        'customer_count': np.random.randint(5, 50, n_records),
        'revenue': np.random.uniform(5000, 50000, n_records)
    })
    
    return df


def create_header():
    """Create dashboard header with user info."""
    return dbc.Navbar(
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.I(className="fas fa-chart-line me-2"),
                        html.Span("Multi-User Dashboard", className="navbar-brand mb-0 h1")
                    ])
                ], width="auto"),
                dbc.Col([
                    html.Div([
                        html.I(className="fas fa-user me-2"),
                        html.Span(f"{user.display_name}", className="text-white me-3"),
                        html.Small(f"({user.user_email})", className="text-white-50")
                    ], className="text-end")
                ], width="auto")
            ], justify="between", className="w-100")
        ], fluid=True),
        color="danger",
        dark=True,
        className="mb-4"
    )


def create_kpi_cards(df: pd.DataFrame):
    """Create KPI summary cards."""
    if df.empty:
        total_revenue = 0
        total_orders = 0
        avg_order_value = 0
        growth_rate = 0
    else:
        total_revenue = df.get('revenue', df.get('sales', [0])).sum()
        total_orders = len(df)
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        
        # Calculate growth rate (comparing first half vs second half)
        if 'date' in df.columns:
            df_sorted = df.sort_values('date')
            mid_point = len(df_sorted) // 2
            first_half = df_sorted.iloc[:mid_point].get('revenue', df_sorted.iloc[:mid_point].get('sales', [0])).sum()
            second_half = df_sorted.iloc[mid_point:].get('revenue', df_sorted.iloc[mid_point:].get('sales', [0])).sum()
            growth_rate = ((second_half - first_half) / first_half * 100) if first_half > 0 else 0
        else:
            growth_rate = 0
    
    cards = dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-dollar-sign fa-2x text-success mb-2"),
                        html.H3(f"${total_revenue:,.0f}", className="mb-0"),
                        html.P("Total Revenue", className="text-muted mb-0")
                    ])
                ])
            ], className="text-center shadow-sm")
        ], width=12, md=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-shopping-cart fa-2x text-primary mb-2"),
                        html.H3(f"{total_orders:,}", className="mb-0"),
                        html.P("Total Orders", className="text-muted mb-0")
                    ])
                ])
            ], className="text-center shadow-sm")
        ], width=12, md=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className="fas fa-receipt fa-2x text-info mb-2"),
                        html.H3(f"${avg_order_value:,.0f}", className="mb-0"),
                        html.P("Avg Order Value", className="text-muted mb-0")
                    ])
                ])
            ], className="text-center shadow-sm")
        ], width=12, md=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className=f"fas fa-{'arrow-up' if growth_rate > 0 else 'arrow-down'} fa-2x text-{'success' if growth_rate > 0 else 'danger'} mb-2"),
                        html.H3(f"{growth_rate:+.1f}%", className="mb-0"),
                        html.P("Growth Rate", className="text-muted mb-0")
                    ])
                ])
            ], className="text-center shadow-sm")
        ], width=12, md=3)
    ], className="mb-4")
    
    return cards


def create_filters(df: pd.DataFrame):
    """Create filter controls."""
    if df.empty:
        categories = []
        regions = []
        date_range = [datetime.now(), datetime.now()]
    else:
        categories = sorted(df['category'].unique()) if 'category' in df.columns else []
        regions = sorted(df['region'].unique()) if 'region' in df.columns else []
        
        if 'date' in df.columns:
            date_range = [df['date'].min(), df['date'].max()]
        else:
            date_range = [datetime.now() - timedelta(days=365), datetime.now()]
    
    filters = dbc.Card([
        dbc.CardHeader(html.H5("Filters", className="mb-0")),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Date Range"),
                    dcc.DatePickerRange(
                        id='date-range',
                        start_date=date_range[0],
                        end_date=date_range[1],
                        display_format='YYYY-MM-DD',
                        className="w-100"
                    )
                ], width=12, md=4),
                
                dbc.Col([
                    html.Label("Category"),
                    dcc.Dropdown(
                        id='category-filter',
                        options=[{'label': 'All', 'value': 'all'}] + 
                                [{'label': cat, 'value': cat} for cat in categories],
                        value='all',
                        clearable=False
                    )
                ], width=12, md=4),
                
                dbc.Col([
                    html.Label("Region"),
                    dcc.Dropdown(
                        id='region-filter',
                        options=[{'label': 'All', 'value': 'all'}] + 
                                [{'label': reg, 'value': reg} for reg in regions],
                        value='all',
                        clearable=False
                    )
                ], width=12, md=4)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Button(
                        [html.I(className="fas fa-sync-alt me-2"), "Refresh Data"],
                        id="refresh-button",
                        color="primary",
                        className="mt-3"
                    )
                ], width="auto")
            ])
        ])
    ], className="mb-4 shadow-sm")
    
    return filters


# Layout
app.layout = html.Div([
    # Store for data
    dcc.Store(id='data-store'),
    dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0),  # Auto-refresh every minute
    
    # Header
    create_header(),
    
    # Main content
    dbc.Container([
        # Info alert about OBO
        dbc.Alert([
            html.I(className="fas fa-info-circle me-2"),
            html.Strong("On-Behalf-Of Permissions: "),
            "This dashboard shows data filtered by your Unity Catalog permissions. "
            "Different users may see different data based on their access rights."
        ], color="info", className="mb-4"),
        
        # KPI Cards
        html.Div(id='kpi-cards'),
        
        # Filters
        html.Div(id='filters'),
        
        # Charts
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Revenue Over Time", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(id='revenue-chart')
                    ])
                ], className="shadow-sm")
            ], width=12, md=8),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Sales by Category", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(id='category-chart')
                    ])
                ], className="shadow-sm")
            ], width=12, md=4)
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Regional Performance", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(id='region-chart')
                    ])
                ], className="shadow-sm")
            ], width=12, md=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Daily Trends", className="mb-0")),
                    dbc.CardBody([
                        dcc.Graph(id='trend-chart')
                    ])
                ], className="shadow-sm")
            ], width=12, md=6)
        ], className="mb-4"),
        
        # Data table
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Data Preview", className="mb-0")),
                    dbc.CardBody([
                        html.Div(id='data-table')
                    ])
                ], className="shadow-sm")
            ])
        ])
    ], fluid=True),
    
    # Footer
    html.Footer([
        dbc.Container([
            html.Hr(),
            html.P([
                "Databricks Apps Workshop - Lab 4: Multi-User Dashboard | ",
                html.Small(f"Last updated: ", id="last-update"),
                html.Small(" | ", className="mx-2"),
                html.Small(f"User: {user.user_email}")
            ], className="text-center text-muted")
        ])
    ])
])


# Callbacks
@callback(
    Output('data-store', 'data'),
    [Input('refresh-button', 'n_clicks'),
     Input('interval-component', 'n_intervals')]
)
def load_data(n_clicks, n_intervals):
    """Load data from Databricks."""
    df = get_user_data()
    return df.to_json(date_format='iso', orient='split')


@callback(
    [Output('kpi-cards', 'children'),
     Output('filters', 'children'),
     Output('revenue-chart', 'figure'),
     Output('category-chart', 'figure'),
     Output('region-chart', 'figure'),
     Output('trend-chart', 'figure'),
     Output('data-table', 'children'),
     Output('last-update', 'children')],
    [Input('data-store', 'data'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('category-filter', 'value'),
     Input('region-filter', 'value')]
)
def update_dashboard(json_data, start_date, end_date, category, region):
    """Update all dashboard components based on filters."""
    # Load data
    df = pd.read_json(json_data, orient='split')
    
    if df.empty:
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        return (
            create_kpi_cards(df),
            create_filters(df),
            empty_fig, empty_fig, empty_fig, empty_fig,
            html.P("No data to display"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    
    # Apply filters
    filtered_df = df.copy()
    
    if 'date' in filtered_df.columns and start_date and end_date:
        filtered_df['date'] = pd.to_datetime(filtered_df['date'])
        filtered_df = filtered_df[
            (filtered_df['date'] >= start_date) &
            (filtered_df['date'] <= end_date)
        ]
    
    if category != 'all' and 'category' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['category'] == category]
    
    if region != 'all' and 'region' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['region'] == region]
    
    # Create visualizations
    revenue_col = 'revenue' if 'revenue' in filtered_df.columns else 'sales'
    
    # Revenue over time
    if 'date' in filtered_df.columns:
        revenue_fig = px.line(
            filtered_df.groupby('date')[revenue_col].sum().reset_index(),
            x='date', y=revenue_col,
            title="",
            template="plotly_white"
        )
        revenue_fig.update_traces(line_color='#FF3621')
    else:
        revenue_fig = go.Figure()
    
    # Category breakdown
    if 'category' in filtered_df.columns:
        category_fig = px.pie(
            filtered_df.groupby('category')[revenue_col].sum().reset_index(),
            values=revenue_col, names='category',
            title="",
            hole=0.4,
            template="plotly_white"
        )
    else:
        category_fig = go.Figure()
    
    # Regional performance
    if 'region' in filtered_df.columns:
        region_fig = px.bar(
            filtered_df.groupby('region')[revenue_col].sum().reset_index(),
            x='region', y=revenue_col,
            title="",
            template="plotly_white",
            color=revenue_col,
            color_continuous_scale='Reds'
        )
    else:
        region_fig = go.Figure()
    
    # Daily trends
    if 'date' in filtered_df.columns and 'quantity' in filtered_df.columns:
        trend_fig = go.Figure()
        trend_fig.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=filtered_df['quantity'],
            name='Quantity',
            line=dict(color='#1f77b4')
        ))
        trend_fig.update_layout(template="plotly_white")
    else:
        trend_fig = go.Figure()
    
    # Data table (first 10 rows)
    table = dbc.Table.from_dataframe(
        filtered_df.head(10),
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        size='sm'
    )
    
    return (
        create_kpi_cards(filtered_df),
        create_filters(df),
        revenue_fig,
        category_fig,
        region_fig,
        trend_fig,
        table,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )


if __name__ == '__main__':
    logger.info("Starting Multi-User Dashboard application")
    app.run_server(host='0.0.0.0', port=8080, debug=False)
