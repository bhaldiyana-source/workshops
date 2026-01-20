"""
Lab 5: RESTful API with FastAPI
A production-ready API for accessing Databricks data with OBO authentication.
Features OpenAPI documentation, health checks, and comprehensive error handling.
"""

from fastapi import FastAPI, Depends, HTTPException, Query, Header, status
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime
import io
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.auth import get_user_context, log_user_action
from utils.database import DatabaseConnection, UnityCatalogExplorer, validate_sql_identifier
from utils.logging_config import setup_logging, get_logger
from routers import data, health

# Configure logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_logger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Databricks Data API",
    description="RESTful API for accessing Databricks Unity Catalog data with OBO permissions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(data.router, prefix="/api/v1", tags=["Data"])


# Pydantic models
class UserInfo(BaseModel):
    """User information model."""
    email: Optional[str] = Field(None, description="User email address")
    user_id: Optional[str] = Field(None, description="User ID")
    display_name: str = Field(..., description="User display name")
    is_authenticated: bool = Field(..., description="Authentication status")


class QueryRequest(BaseModel):
    """SQL query request model."""
    query: str = Field(..., description="SQL query to execute", example="SELECT * FROM main.default.table LIMIT 10")
    format: str = Field("json", description="Output format: json, csv, or parquet", example="json")


class QueryResponse(BaseModel):
    """SQL query response model."""
    data: List[Dict[str, Any]] = Field(..., description="Query results")
    row_count: int = Field(..., description="Number of rows returned")
    column_count: int = Field(..., description="Number of columns")
    columns: List[str] = Field(..., description="Column names")
    execution_time_ms: float = Field(..., description="Query execution time in milliseconds")


class CatalogInfo(BaseModel):
    """Unity Catalog information model."""
    name: str = Field(..., description="Catalog name")


class SchemaInfo(BaseModel):
    """Schema information model."""
    name: str = Field(..., description="Schema name")
    catalog: str = Field(..., description="Parent catalog name")


class TableInfo(BaseModel):
    """Table information model."""
    name: str = Field(..., description="Table name")
    catalog: str = Field(..., description="Catalog name")
    schema: str = Field(..., description="Schema name")
    table_type: Optional[str] = Field(None, description="Table type (MANAGED, EXTERNAL, VIEW)")


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: str = Field(..., description="Error timestamp")


# Dependency for getting database connection
def get_db_connection() -> DatabaseConnection:
    """Get database connection with OBO authentication."""
    try:
        return DatabaseConnection()
    except Exception as e:
        logger.error(f"Failed to create database connection: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection unavailable"
        )


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """API root endpoint with basic information."""
    user = get_user_context()
    return {
        "message": "Databricks Data API",
        "version": "1.0.0",
        "user": user.user_email or "Unknown",
        "docs": "/docs",
        "health": "/health",
        "timestamp": datetime.now().isoformat()
    }


# User info endpoint
@app.get("/user", response_model=UserInfo, tags=["User"])
async def get_user_info():
    """
    Get current authenticated user information.
    
    Returns user context based on OBO authentication.
    """
    user = get_user_context()
    log_user_action("api_user_info_requested")
    
    return UserInfo(
        email=user.user_email,
        user_id=user.user_id,
        display_name=user.display_name,
        is_authenticated=user.is_authenticated
    )


# Catalogs endpoint
@app.get("/catalogs", response_model=List[CatalogInfo], tags=["Metadata"])
async def list_catalogs(db: DatabaseConnection = Depends(get_db_connection)):
    """
    List all accessible Unity Catalog catalogs.
    
    Returns only catalogs the authenticated user has permission to access.
    """
    try:
        explorer = UnityCatalogExplorer(db)
        catalogs = explorer.list_catalogs()
        
        log_user_action("api_catalogs_listed", {"count": len(catalogs)})
        
        return [CatalogInfo(name=cat) for cat in catalogs]
    except Exception as e:
        logger.error(f"Error listing catalogs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Schemas endpoint
@app.get("/catalogs/{catalog}/schemas", response_model=List[SchemaInfo], tags=["Metadata"])
async def list_schemas(
    catalog: str,
    db: DatabaseConnection = Depends(get_db_connection)
):
    """
    List all schemas in a catalog.
    
    Returns only schemas the authenticated user has permission to access.
    """
    if not validate_sql_identifier(catalog):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid catalog name"
        )
    
    try:
        explorer = UnityCatalogExplorer(db)
        schemas = explorer.list_schemas(catalog)
        
        log_user_action("api_schemas_listed", {"catalog": catalog, "count": len(schemas)})
        
        return [SchemaInfo(name=schema, catalog=catalog) for schema in schemas]
    except Exception as e:
        logger.error(f"Error listing schemas: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Tables endpoint
@app.get("/catalogs/{catalog}/schemas/{schema}/tables", response_model=List[TableInfo], tags=["Metadata"])
async def list_tables(
    catalog: str,
    schema: str,
    db: DatabaseConnection = Depends(get_db_connection)
):
    """
    List all tables in a schema.
    
    Returns only tables the authenticated user has permission to access.
    """
    if not validate_sql_identifier(catalog) or not validate_sql_identifier(schema):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid catalog or schema name"
        )
    
    try:
        explorer = UnityCatalogExplorer(db)
        tables = explorer.list_tables(catalog, schema)
        
        log_user_action("api_tables_listed", {
            "catalog": catalog,
            "schema": schema,
            "count": len(tables)
        })
        
        return [
            TableInfo(
                name=table.get('tableName', table.get('table', 'unknown')),
                catalog=catalog,
                schema=schema,
                table_type=table.get('tableType', table.get('type'))
            )
            for table in tables
        ]
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Table data endpoint
@app.get("/catalogs/{catalog}/schemas/{schema}/tables/{table}/data", tags=["Data"])
async def get_table_data(
    catalog: str,
    schema: str,
    table: str,
    limit: int = Query(100, ge=1, le=10000, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    format: str = Query("json", regex="^(json|csv)$", description="Output format"),
    db: DatabaseConnection = Depends(get_db_connection)
):
    """
    Get data from a table.
    
    Supports pagination with limit and offset parameters.
    Returns data in JSON or CSV format.
    """
    # Validate identifiers
    for identifier in [catalog, schema, table]:
        if not validate_sql_identifier(identifier):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid identifier: {identifier}"
            )
    
    try:
        # Build query with pagination
        query = f"""
        SELECT * 
        FROM {catalog}.{schema}.{table}
        LIMIT {limit}
        OFFSET {offset}
        """
        
        start_time = datetime.now()
        results = db.execute_query_as_dict(query)
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        log_user_action("api_table_data_fetched", {
            "table": f"{catalog}.{schema}.{table}",
            "rows": len(results),
            "format": format
        })
        
        # Return in requested format
        if format == "csv":
            df = pd.DataFrame(results)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return StreamingResponse(
                iter([csv_buffer.getvalue()]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={table}.csv"
                }
            )
        else:
            # JSON format
            return {
                "data": results,
                "row_count": len(results),
                "column_count": len(results[0].keys()) if results else 0,
                "columns": list(results[0].keys()) if results else [],
                "execution_time_ms": execution_time,
                "pagination": {
                    "limit": limit,
                    "offset": offset
                }
            }
            
    except Exception as e:
        logger.error(f"Error fetching table data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


# Query execution endpoint
@app.post("/query", response_model=QueryResponse, tags=["Query"])
async def execute_query(
    request: QueryRequest,
    db: DatabaseConnection = Depends(get_db_connection)
):
    """
    Execute a custom SQL query.
    
    Executes the query with the authenticated user's permissions (OBO).
    Only SELECT queries are recommended for security.
    """
    # Basic validation - check if query is SELECT
    query_lower = request.query.strip().lower()
    if not query_lower.startswith('select'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only SELECT queries are supported"
        )
    
    try:
        start_time = datetime.now()
        results = db.execute_query_as_dict(request.query)
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        log_user_action("api_query_executed", {
            "query": request.query[:100],
            "rows": len(results)
        })
        
        return QueryResponse(
            data=results,
            row_count=len(results),
            column_count=len(results[0].keys()) if results else 0,
            columns=list(results[0].keys()) if results else [],
            execution_time_ms=execution_time
        )
        
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query execution failed: {str(e)}"
        )


# Exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {str(exc)}")
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc) if os.getenv("DEBUG") == "true" else None,
            timestamp=datetime.now().isoformat()
        ).dict()
    )


# Startup event
@app.on_event("startup")
async def startup_event():
    """Application startup event."""
    logger.info("FastAPI application starting up")
    logger.info(f"User: {get_user_context().user_email}")
    log_user_action("api_startup")


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event."""
    logger.info("FastAPI application shutting down")
    log_user_action("api_shutdown")


if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting FastAPI application")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info"
    )
