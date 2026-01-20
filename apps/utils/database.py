"""Database connection and query utilities."""

import os
import time
import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from databricks import sql
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages Databricks SQL warehouse connections with retry logic."""
    
    def __init__(
        self,
        server_hostname: Optional[str] = None,
        http_path: Optional[str] = None,
        access_token: Optional[str] = None,
        max_retries: int = 3,
        timeout: int = 120
    ):
        """
        Initialize database connection parameters.
        
        Args:
            server_hostname: Databricks server hostname
            http_path: SQL warehouse HTTP path
            access_token: Access token (uses OBO token by default)
            max_retries: Maximum connection retry attempts
            timeout: Connection timeout in seconds
        """
        self.server_hostname = server_hostname or os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = access_token or os.getenv("DATABRICKS_TOKEN")
        self.max_retries = max_retries
        self.timeout = timeout
        
        if not all([self.server_hostname, self.http_path, self.access_token]):
            raise ValueError(
                "Missing required connection parameters. Ensure DATABRICKS_SERVER_HOSTNAME, "
                "DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN are set."
            )
    
    @contextmanager
    def connect(self):
        """
        Create a connection with automatic retry and cleanup.
        
        Yields:
            SQL connection object
        """
        connection = None
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Connecting to Databricks SQL warehouse (attempt {attempt + 1}/{self.max_retries})")
                connection = sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    access_token=self.access_token,
                    _socket_timeout=self.timeout
                )
                logger.info("Successfully connected to Databricks SQL warehouse")
                yield connection
                return
            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if connection:
                    try:
                        connection.close()
                    except:
                        pass
                
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("All connection attempts failed")
                    raise
            finally:
                if connection:
                    try:
                        connection.close()
                    except Exception as e:
                        logger.warning(f"Error closing connection: {str(e)}")
    
    def execute_query(self, query: str) -> List[tuple]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            
        Returns:
            List of result tuples
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            try:
                logger.debug(f"Executing query: {query[:100]}...")
                cursor.execute(query)
                results = cursor.fetchall()
                logger.info(f"Query returned {len(results)} rows")
                return results
            finally:
                cursor.close()
    
    def execute_query_as_dict(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dictionaries.
        
        Args:
            query: SQL query string
            
        Returns:
            List of result dictionaries
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            try:
                logger.debug(f"Executing query: {query[:100]}...")
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                logger.info(f"Query returned {len(results)} rows")
                return [dict(zip(columns, row)) for row in results]
            finally:
                cursor.close()


class UnityCatalogExplorer:
    """Utilities for exploring Unity Catalog metadata."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize Unity Catalog explorer.
        
        Args:
            db_connection: DatabaseConnection instance
        """
        self.db = db_connection
    
    def list_catalogs(self) -> List[str]:
        """
        List all accessible catalogs.
        
        Returns:
            List of catalog names
        """
        try:
            results = self.db.execute_query("SHOW CATALOGS")
            return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Error listing catalogs: {str(e)}")
            return []
    
    def list_schemas(self, catalog: str) -> List[str]:
        """
        List all schemas in a catalog.
        
        Args:
            catalog: Catalog name
            
        Returns:
            List of schema names
        """
        try:
            query = f"SHOW SCHEMAS IN {catalog}"
            results = self.db.execute_query(query)
            return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Error listing schemas in {catalog}: {str(e)}")
            return []
    
    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        """
        List all tables in a schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of table information dictionaries
        """
        try:
            query = f"SHOW TABLES IN {catalog}.{schema}"
            results = self.db.execute_query_as_dict(query)
            return results
        except Exception as e:
            logger.error(f"Error listing tables in {catalog}.{schema}: {str(e)}")
            return []
    
    def get_table_schema(self, catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            List of column information dictionaries
        """
        try:
            query = f"DESCRIBE {catalog}.{schema}.{table}"
            results = self.db.execute_query_as_dict(query)
            return results
        except Exception as e:
            logger.error(f"Error describing table {catalog}.{schema}.{table}: {str(e)}")
            return []
    
    def preview_table(self, catalog: str, schema: str, table: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get a preview of table data.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            limit: Maximum number of rows to return
            
        Returns:
            List of row dictionaries
        """
        try:
            query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
            results = self.db.execute_query_as_dict(query)
            return results
        except Exception as e:
            logger.error(f"Error previewing table {catalog}.{schema}.{table}: {str(e)}")
            return []


def validate_sql_identifier(identifier: str) -> bool:
    """
    Validate SQL identifier to prevent injection attacks.
    
    Args:
        identifier: SQL identifier (catalog, schema, table, column name)
        
    Returns:
        True if valid, False otherwise
    """
    import re
    # Allow alphanumeric, underscores, and backticks (for escaped identifiers)
    pattern = r'^[a-zA-Z0-9_`]+$'
    return bool(re.match(pattern, identifier))


def sanitize_sql_string(value: str) -> str:
    """
    Sanitize string value for SQL queries.
    
    Args:
        value: String value
        
    Returns:
        Sanitized string
    """
    # Escape single quotes by doubling them
    return value.replace("'", "''")
