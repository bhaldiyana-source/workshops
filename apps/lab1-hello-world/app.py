"""
Lab 1: Hello World Databricks App
A simple Streamlit application demonstrating basic Databricks connectivity,
user authentication (OBO), and environment variable access.
"""

import streamlit as st
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.auth import get_user_context, log_user_action
from utils.database import DatabaseConnection
from utils.logging_config import setup_logging

# Configure logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))

# Page configuration
st.set_page_config(
    page_title="Lab 1: Hello World",
    page_icon="👋",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #FF3621;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .info-box {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def main():
    """Main application function."""
    
    # Header
    st.markdown('<div class="main-header">👋 Welcome to Databricks Apps!</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Lab 1: Hello World Application</div>', unsafe_allow_html=True)
    
    # Get user context
    user = get_user_context()
    log_user_action("app_accessed", {"lab": "lab1-hello-world"})
    
    # User Information Section
    st.header("🔐 User Information")
    
    if user.is_authenticated:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Display Name", user.display_name)
        
        with col2:
            st.metric("Email", user.user_email or "N/A")
        
        with col3:
            st.metric("User ID", user.user_id or "N/A")
        
        st.markdown('<div class="success-box">✅ Successfully authenticated via On-Behalf-Of (OBO) permissions</div>', 
                   unsafe_allow_html=True)
    else:
        st.markdown('<div class="error-box">⚠️ User authentication information not available</div>', 
                   unsafe_allow_html=True)
    
    # Environment Variables Section
    st.header("🔧 Environment Variables")
    
    with st.expander("View Databricks Environment Variables", expanded=False):
        env_vars = {
            "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", "Not set"),
            "DATABRICKS_SERVER_HOSTNAME": os.getenv("DATABRICKS_SERVER_HOSTNAME", "Not set"),
            "DATABRICKS_HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH", "Not set"),
            "DATABRICKS_USER_EMAIL": os.getenv("DATABRICKS_USER_EMAIL", "Not set"),
            "DATABRICKS_USER_ID": os.getenv("DATABRICKS_USER_ID", "Not set"),
        }
        
        for key, value in env_vars.items():
            # Mask token for security
            if "TOKEN" in key:
                display_value = "***" if value != "Not set" else "Not set"
            else:
                display_value = value
            
            st.text(f"{key}: {display_value}")
    
    # Connection Test Section
    st.header("🔌 Connection Test")
    
    if st.button("Test Databricks Connection", type="primary"):
        with st.spinner("Testing connection to Databricks SQL Warehouse..."):
            try:
                db = DatabaseConnection()
                
                # Test query
                result = db.execute_query("SELECT 'Hello from Databricks!' as message, current_user() as user")
                
                if result:
                    st.markdown('<div class="success-box">✅ Connection successful!</div>', 
                               unsafe_allow_html=True)
                    
                    st.subheader("Query Result:")
                    st.table(result)
                    
                    log_user_action("connection_test", {"status": "success"})
                else:
                    st.warning("Connection succeeded but returned no results")
                    
            except ValueError as e:
                st.markdown(f'<div class="error-box">⚠️ Configuration Error: {str(e)}</div>', 
                           unsafe_allow_html=True)
                st.info("💡 Make sure your app.yaml is configured with a valid warehouse_id")
                
            except Exception as e:
                st.markdown(f'<div class="error-box">❌ Connection Failed: {str(e)}</div>', 
                           unsafe_allow_html=True)
                log_user_action("connection_test", {"status": "failed", "error": str(e)})
    
    # Information Section
    st.header("📚 About This Lab")
    
    with st.expander("What This Lab Demonstrates", expanded=True):
        st.markdown("""
        ### Key Concepts
        
        1. **On-Behalf-Of (OBO) Authentication**
           - Automatic user authentication
           - Queries execute with user's credentials
           - No token management required
        
        2. **Environment Variables**
           - Access Databricks connection parameters
           - Secure credential handling
           - Platform-managed configuration
        
        3. **Database Connectivity**
           - Connection to SQL Warehouses
           - Retry logic with exponential backoff
           - Proper resource management
        
        4. **User Context**
           - Access authenticated user information
           - Audit logging
           - Personalized experiences
        
        ### Architecture
        
        ```
        User → Databricks Apps → SQL Warehouse → Unity Catalog
                     ↓
            User's OBO Token (automatic)
                     ↓
            Queries respect user's permissions
        ```
        
        ### Next Steps
        
        - Explore **Lab 2** to build an interactive Data Explorer
        - Learn how to query Unity Catalog tables
        - Build dashboards with user-specific data views
        """)
    
    # Footer
    st.divider()
    st.caption("Databricks Apps Workshop - Lab 1: Hello World | Built with Streamlit")


if __name__ == "__main__":
    main()
