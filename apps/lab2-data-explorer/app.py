"""
Lab 2: Data Explorer
An interactive Databricks App for exploring Unity Catalog data with user permissions.
Demonstrates OBO authentication, dynamic queries, and metadata browsing.
"""

import streamlit as st
import pandas as pd
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.auth import get_user_context, log_user_action
from utils.database import DatabaseConnection, UnityCatalogExplorer, validate_sql_identifier
from utils.logging_config import setup_logging, get_logger

# Configure logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_logger(__name__)

# Page configuration
st.set_page_config(
    page_title="Lab 2: Data Explorer",
    page_icon="🔍",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF3621;
        margin-bottom: 0.5rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    .sql-code {
        background-color: #1e1e1e;
        color: #d4d4d4;
        padding: 1rem;
        border-radius: 0.5rem;
        font-family: monospace;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


# Initialize session state
if 'selected_catalog' not in st.session_state:
    st.session_state.selected_catalog = None
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = None
if 'query_history' not in st.session_state:
    st.session_state.query_history = []


@st.cache_data(ttl=600)
def get_catalogs() -> List[str]:
    """Get list of accessible catalogs (cached for 10 minutes)."""
    try:
        db = DatabaseConnection()
        explorer = UnityCatalogExplorer(db)
        return explorer.list_catalogs()
    except Exception as e:
        logger.error(f"Error fetching catalogs: {str(e)}")
        return []


@st.cache_data(ttl=600)
def get_schemas(catalog: str) -> List[str]:
    """Get list of schemas in a catalog (cached for 10 minutes)."""
    try:
        db = DatabaseConnection()
        explorer = UnityCatalogExplorer(db)
        return explorer.list_schemas(catalog)
    except Exception as e:
        logger.error(f"Error fetching schemas: {str(e)}")
        return []


@st.cache_data(ttl=600)
def get_tables(catalog: str, schema: str) -> List[Dict[str, Any]]:
    """Get list of tables in a schema (cached for 10 minutes)."""
    try:
        db = DatabaseConnection()
        explorer = UnityCatalogExplorer(db)
        return explorer.list_tables(catalog, schema)
    except Exception as e:
        logger.error(f"Error fetching tables: {str(e)}")
        return []


@st.cache_data(ttl=300)
def get_table_schema(catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """Get table schema information (cached for 5 minutes)."""
    try:
        db = DatabaseConnection()
        explorer = UnityCatalogExplorer(db)
        return explorer.get_table_schema(catalog, schema, table)
    except Exception as e:
        logger.error(f"Error fetching table schema: {str(e)}")
        return []


def execute_custom_query(query: str) -> Optional[pd.DataFrame]:
    """Execute a custom SQL query."""
    try:
        db = DatabaseConnection()
        results = db.execute_query_as_dict(query)
        
        # Add to query history
        st.session_state.query_history.append({
            'query': query,
            'rows': len(results)
        })
        
        return pd.DataFrame(results) if results else pd.DataFrame()
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        raise


def main():
    """Main application function."""
    
    # Header
    st.markdown('<div class="main-header">🔍 Unity Catalog Data Explorer</div>', unsafe_allow_html=True)
    st.caption("Lab 2: Interactive data exploration with On-Behalf-Of permissions")
    
    # Get user context
    user = get_user_context()
    log_user_action("data_explorer_accessed")
    
    # Sidebar - User Info
    with st.sidebar:
        st.header("👤 User Context")
        st.write(f"**{user.display_name}**")
        st.caption(f"{user.user_email}")
        
        st.divider()
        
        st.header("🔐 Permissions")
        st.info("You can only see data you have permission to access. All queries execute with your credentials (OBO).")
        
        st.divider()
        
        # Query History
        st.header("📜 Query History")
        if st.session_state.query_history:
            for i, item in enumerate(reversed(st.session_state.query_history[-5:]), 1):
                with st.expander(f"Query {len(st.session_state.query_history) - i + 1}", expanded=False):
                    st.code(item['query'], language='sql')
                    st.caption(f"Returned {item['rows']} rows")
        else:
            st.caption("No queries executed yet")
    
    # Main tabs
    tab1, tab2, tab3 = st.tabs(["🗂️ Browse Catalog", "✍️ Custom Query", "📊 Query Builder"])
    
    with tab1:
        browse_catalog_tab()
    
    with tab2:
        custom_query_tab()
    
    with tab3:
        query_builder_tab()


def browse_catalog_tab():
    """Browse Unity Catalog hierarchically."""
    st.header("Browse Unity Catalog")
    st.write("Navigate through catalogs, schemas, and tables. Only objects you have permission to see will appear.")
    
    # Catalog selection
    st.subheader("1️⃣ Select Catalog")
    catalogs = get_catalogs()
    
    if not catalogs:
        st.warning("No catalogs accessible. Check your permissions.")
        return
    
    selected_catalog = st.selectbox(
        "Choose a catalog",
        options=catalogs,
        key="catalog_selector"
    )
    
    if selected_catalog:
        st.session_state.selected_catalog = selected_catalog
        
        # Schema selection
        st.subheader("2️⃣ Select Schema")
        schemas = get_schemas(selected_catalog)
        
        if not schemas:
            st.warning(f"No schemas found in catalog '{selected_catalog}'")
            return
        
        selected_schema = st.selectbox(
            "Choose a schema",
            options=schemas,
            key="schema_selector"
        )
        
        if selected_schema:
            st.session_state.selected_schema = selected_schema
            
            # Table selection
            st.subheader("3️⃣ Select Table")
            tables = get_tables(selected_catalog, selected_schema)
            
            if not tables:
                st.warning(f"No tables found in schema '{selected_catalog}.{selected_schema}'")
                return
            
            # Display tables as cards
            table_names = [t.get('tableName', t.get('table', 'unknown')) for t in tables]
            
            selected_table = st.selectbox(
                "Choose a table",
                options=table_names,
                key="table_selector"
            )
            
            if selected_table:
                st.session_state.selected_table = selected_table
                display_table_details(selected_catalog, selected_schema, selected_table)


def display_table_details(catalog: str, schema: str, table: str):
    """Display detailed information about a table."""
    st.subheader(f"📋 Table: `{catalog}.{schema}.{table}`")
    
    # Get table schema
    table_schema = get_table_schema(catalog, schema, table)
    
    if table_schema:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.write("**Column Information**")
            schema_df = pd.DataFrame(table_schema)
            st.dataframe(schema_df, use_container_width=True, hide_index=True)
        
        with col2:
            st.write("**Statistics**")
            st.metric("Total Columns", len(table_schema))
    
    # Preview data
    st.subheader("📊 Data Preview")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        limit = st.slider("Rows to preview", min_value=10, max_value=1000, value=100, step=10)
    with col2:
        st.write("")  # Spacer
        st.write("")  # Spacer
        preview_button = st.button("Load Preview", type="primary", use_container_width=True)
    
    if preview_button:
        with st.spinner(f"Loading preview of {catalog}.{schema}.{table}..."):
            try:
                db = DatabaseConnection()
                explorer = UnityCatalogExplorer(db)
                preview_data = explorer.preview_table(catalog, schema, table, limit=limit)
                
                if preview_data:
                    df = pd.DataFrame(preview_data)
                    st.dataframe(df, use_container_width=True)
                    
                    # Download option
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"{table}_preview.csv",
                        mime="text/csv"
                    )
                    
                    log_user_action("table_preview", {
                        "table": f"{catalog}.{schema}.{table}",
                        "rows": len(preview_data)
                    })
                else:
                    st.info("Table is empty or returned no results")
                    
            except Exception as e:
                st.error(f"Error loading preview: {str(e)}")
                if "permission" in str(e).lower():
                    st.info("💡 You may not have SELECT permission on this table")


def custom_query_tab():
    """Execute custom SQL queries."""
    st.header("Custom SQL Query")
    st.write("Write and execute SQL queries against Unity Catalog. Queries run with your permissions.")
    
    # Query input
    default_query = "SELECT current_user() as user, current_database() as database, current_catalog() as catalog"
    
    if st.session_state.selected_catalog and st.session_state.selected_schema and st.session_state.selected_table:
        default_query = f"""SELECT *
FROM {st.session_state.selected_catalog}.{st.session_state.selected_schema}.{st.session_state.selected_table}
LIMIT 100"""
    
    query = st.text_area(
        "SQL Query",
        value=default_query,
        height=150,
        help="Enter your SQL query here"
    )
    
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        execute_button = st.button("▶️ Execute Query", type="primary", use_container_width=True)
    with col2:
        clear_button = st.button("🗑️ Clear Results", use_container_width=True)
    
    if clear_button:
        st.rerun()
    
    if execute_button and query.strip():
        with st.spinner("Executing query..."):
            try:
                df = execute_custom_query(query)
                
                if df is not None and not df.empty:
                    st.success(f"✅ Query executed successfully! Returned {len(df)} rows.")
                    
                    # Display results
                    st.subheader("Results")
                    st.dataframe(df, use_container_width=True)
                    
                    # Statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Rows", len(df))
                    with col2:
                        st.metric("Columns", len(df.columns))
                    with col3:
                        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                        st.metric("Memory", f"{memory_mb:.2f} MB")
                    
                    # Download option
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results as CSV",
                        data=csv,
                        file_name="query_results.csv",
                        mime="text/csv"
                    )
                    
                    log_user_action("custom_query", {"rows": len(df), "columns": len(df.columns)})
                else:
                    st.info("Query executed but returned no results")
                    
            except Exception as e:
                st.error(f"❌ Query execution failed: {str(e)}")
                
                # Helpful tips
                st.info("💡 **Troubleshooting Tips:**\n"
                       "- Verify you have SELECT permission on the tables\n"
                       "- Check that catalog, schema, and table names are correct\n"
                       "- Ensure the SQL syntax is valid\n"
                       "- Try using a simpler query first")


def query_builder_tab():
    """Visual query builder interface."""
    st.header("Visual Query Builder")
    st.write("Build queries using a visual interface without writing SQL.")
    
    # Table selection
    col1, col2, col3 = st.columns(3)
    
    with col1:
        catalogs = get_catalogs()
        if catalogs:
            catalog = st.selectbox("Catalog", catalogs, key="qb_catalog")
        else:
            st.warning("No catalogs available")
            return
    
    with col2:
        if catalog:
            schemas = get_schemas(catalog)
            if schemas:
                schema = st.selectbox("Schema", schemas, key="qb_schema")
            else:
                st.warning("No schemas available")
                return
    
    with col3:
        if catalog and schema:
            tables = get_tables(catalog, schema)
            table_names = [t.get('tableName', t.get('table', 'unknown')) for t in tables]
            if table_names:
                table = st.selectbox("Table", table_names, key="qb_table")
            else:
                st.warning("No tables available")
                return
    
    if catalog and schema and table:
        # Get columns
        table_schema = get_table_schema(catalog, schema, table)
        if table_schema:
            columns = [col.get('col_name', col.get('column', 'unknown')) for col in table_schema]
            
            # Column selection
            st.subheader("Select Columns")
            select_all = st.checkbox("Select all columns", value=True)
            
            if select_all:
                selected_columns = columns
            else:
                selected_columns = st.multiselect("Choose columns", columns, default=columns[:5])
            
            # Filters
            st.subheader("Add Filters (Optional)")
            add_filter = st.checkbox("Add WHERE clause")
            
            where_clause = ""
            if add_filter:
                filter_column = st.selectbox("Filter column", columns)
                filter_operator = st.selectbox("Operator", ["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"])
                filter_value = st.text_input("Value", "")
                
                if filter_value:
                    if filter_operator == "LIKE":
                        where_clause = f"WHERE {filter_column} LIKE '%{filter_value}%'"
                    elif filter_operator == "IN":
                        where_clause = f"WHERE {filter_column} IN ({filter_value})"
                    else:
                        where_clause = f"WHERE {filter_column} {filter_operator} '{filter_value}'"
            
            # Limit
            col1, col2 = st.columns([1, 1])
            with col1:
                limit = st.number_input("Limit rows", min_value=1, max_value=10000, value=100)
            with col2:
                order_by = st.selectbox("Order by (optional)", ["None"] + columns)
            
            # Build query
            columns_str = ", ".join(selected_columns) if selected_columns else "*"
            query = f"SELECT {columns_str}\nFROM {catalog}.{schema}.{table}"
            
            if where_clause:
                query += f"\n{where_clause}"
            
            if order_by != "None":
                query += f"\nORDER BY {order_by}"
            
            query += f"\nLIMIT {limit}"
            
            # Display generated query
            st.subheader("Generated Query")
            st.code(query, language='sql')
            
            # Execute
            if st.button("▶️ Execute Built Query", type="primary"):
                with st.spinner("Executing query..."):
                    try:
                        df = execute_custom_query(query)
                        
                        if df is not None and not df.empty:
                            st.success(f"✅ Query executed successfully! Returned {len(df)} rows.")
                            st.dataframe(df, use_container_width=True)
                            
                            # Download option
                            csv = df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Results",
                                data=csv,
                                file_name="query_builder_results.csv",
                                mime="text/csv"
                            )
                        else:
                            st.info("Query returned no results")
                            
                    except Exception as e:
                        st.error(f"❌ Query execution failed: {str(e)}")


if __name__ == "__main__":
    main()
