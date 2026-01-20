# Lab 5: RESTful API with FastAPI

## Overview

This lab demonstrates building production-ready RESTful APIs with FastAPI for accessing Databricks data. The API respects On-Behalf-Of (OBO) permissions, provides comprehensive documentation, and includes health checks and monitoring capabilities.

## Learning Objectives

By completing this lab, you will:
- ✅ Build RESTful APIs with FastAPI
- ✅ Implement OBO authentication for API endpoints
- ✅ Create OpenAPI/Swagger documentation automatically
- ✅ Add health check and monitoring endpoints
- ✅ Handle errors gracefully with proper HTTP status codes
- ✅ Support multiple output formats (JSON, CSV)
- ✅ Implement pagination for large datasets

## Prerequisites

- Completion of previous labs (especially Lab 1 and Lab 2)
- Understanding of REST API concepts
- Unity Catalog with accessible data
- Basic knowledge of HTTP methods and status codes

## Project Structure

```
lab5-api-fastapi/
├── app.py                  # Main FastAPI application
├── app.yaml                # Databricks app configuration
├── requirements.txt        # Python dependencies
├── routers/
│   ├── __init__.py
│   ├── health.py          # Health check endpoints
│   └── data.py            # Data access endpoints
└── README.md              # This file
```

## Configuration

### Step 1: Update app.yaml

```yaml
env:
  - name: WAREHOUSE_ID
    value: "YOUR_WAREHOUSE_ID_HERE"
  
  - name: CATALOG_NAME
    value: "main"
  
  - name: SCHEMA_NAME
    value: "default"
  
  - name: SAMPLE_TABLE
    value: "your_table"
```

### Step 2: Deploy

```bash
cd apps/lab5-api-fastapi
databricks apps deploy /Users/<your-email>/lab5-api-fastapi
```

## API Endpoints

### Root & Documentation

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API root with basic information |
| `/docs` | GET | Interactive Swagger UI documentation |
| `/redoc` | GET | Alternative ReDoc documentation |

### Health Checks

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health status |
| `/health/ready` | GET | Readiness check |
| `/health/live` | GET | Liveness check |

### User Information

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/user` | GET | Get authenticated user info |

### Unity Catalog Metadata

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/catalogs` | GET | List accessible catalogs |
| `/catalogs/{catalog}/schemas` | GET | List schemas in catalog |
| `/catalogs/{catalog}/schemas/{schema}/tables` | GET | List tables in schema |

### Data Access

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/catalogs/{catalog}/schemas/{schema}/tables/{table}/data` | GET | Get table data with pagination |
| `/query` | POST | Execute custom SQL query |

## Usage Examples

### 1. Check API Health

```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00",
  "version": "1.0.0",
  "environment": {
    "app_name": "Lab 5: RESTful API",
    "log_level": "INFO"
  }
}
```

### 2. Get User Information

```bash
curl http://localhost:8080/user
```

Response:
```json
{
  "email": "user@example.com",
  "user_id": "123456",
  "display_name": "John Doe",
  "is_authenticated": true
}
```

### 3. List Catalogs

```bash
curl http://localhost:8080/catalogs
```

Response:
```json
[
  {"name": "main"},
  {"name": "sales"},
  {"name": "analytics"}
]
```

### 4. List Tables

```bash
curl http://localhost:8080/catalogs/main/schemas/default/tables
```

Response:
```json
[
  {
    "name": "customers",
    "catalog": "main",
    "schema": "default",
    "table_type": "MANAGED"
  },
  {
    "name": "orders",
    "catalog": "main",
    "schema": "default",
    "table_type": "MANAGED"
  }
]
```

### 5. Get Table Data (JSON)

```bash
curl "http://localhost:8080/catalogs/main/schemas/default/tables/customers/data?limit=10"
```

Response:
```json
{
  "data": [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
  ],
  "row_count": 2,
  "column_count": 3,
  "columns": ["id", "name", "age"],
  "execution_time_ms": 45.2,
  "pagination": {
    "limit": 10,
    "offset": 0
  }
}
```

### 6. Get Table Data (CSV)

```bash
curl "http://localhost:8080/catalogs/main/schemas/default/tables/customers/data?format=csv" > customers.csv
```

### 7. Execute Custom Query

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM main.default.customers WHERE age > 25 LIMIT 5",
    "format": "json"
  }'
```

Response:
```json
{
  "data": [...],
  "row_count": 5,
  "column_count": 3,
  "columns": ["id", "name", "age"],
  "execution_time_ms": 120.5
}
```

### 8. Pagination Example

```bash
# First page
curl "http://localhost:8080/catalogs/main/schemas/default/tables/customers/data?limit=10&offset=0"

# Second page
curl "http://localhost:8080/catalogs/main/schemas/default/tables/customers/data?limit=10&offset=10"

# Third page
curl "http://localhost:8080/catalogs/main/schemas/default/tables/customers/data?limit=10&offset=20"
```

## Interactive API Documentation

FastAPI automatically generates interactive API documentation:

### Swagger UI (`/docs`)

1. Navigate to `http://your-app-url/docs`
2. See all available endpoints
3. Try endpoints directly in the browser
4. View request/response schemas
5. Test with your actual data

### ReDoc (`/redoc`)

1. Navigate to `http://your-app-url/redoc`
2. Alternative documentation format
3. Better for printing/reading
4. Organized by tags

## Python Client Example

```python
import requests

# Base URL
base_url = "http://your-app-url"

# Get user info
response = requests.get(f"{base_url}/user")
user = response.json()
print(f"Logged in as: {user['display_name']}")

# List catalogs
response = requests.get(f"{base_url}/catalogs")
catalogs = response.json()
for catalog in catalogs:
    print(f"Catalog: {catalog['name']}")

# Execute query
query_request = {
    "query": "SELECT * FROM main.default.sales LIMIT 100",
    "format": "json"
}
response = requests.post(f"{base_url}/query", json=query_request)
results = response.json()
print(f"Retrieved {results['row_count']} rows")
```

## JavaScript/TypeScript Client Example

```javascript
const BASE_URL = 'http://your-app-url';

// Get user info
async function getUserInfo() {
  const response = await fetch(`${BASE_URL}/user`);
  const user = await response.json();
  console.log(`Logged in as: ${user.display_name}`);
}

// List tables
async function listTables(catalog, schema) {
  const response = await fetch(
    `${BASE_URL}/catalogs/${catalog}/schemas/${schema}/tables`
  );
  const tables = await response.json();
  return tables;
}

// Execute query
async function executeQuery(query) {
  const response = await fetch(`${BASE_URL}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, format: 'json' })
  });
  const results = await response.json();
  return results.data;
}
```

## On-Behalf-Of (OBO) in APIs

### How It Works

Every API request uses the authenticated user's credentials:

```python
@app.get("/data")
async def get_data(db: DatabaseConnection = Depends(get_db_connection)):
    # db connection automatically uses the requesting user's OBO token
    # Query results filtered by user's Unity Catalog permissions
    results = db.execute_query("SELECT * FROM table")
    return results
```

### Multi-User Scenario

```python
# User A (has access to all regions):
GET /catalogs/sales/schemas/default/tables/transactions/data
# Returns: All transactions from all regions

# User B (has access to North region only):
GET /catalogs/sales/schemas/default/tables/transactions/data
# Returns: Only North region transactions (filtered by Unity Catalog)
```

## Error Handling

### HTTP Status Codes

| Code | Meaning | When Used |
|------|---------|-----------|
| 200 | OK | Successful request |
| 400 | Bad Request | Invalid input/query |
| 401 | Unauthorized | Authentication failed |
| 403 | Forbidden | No permission |
| 404 | Not Found | Resource doesn't exist |
| 500 | Internal Server Error | Server error |
| 503 | Service Unavailable | Database unavailable |

### Error Response Format

```json
{
  "error": "Query execution failed",
  "detail": "Table not found: main.default.nonexistent",
  "timestamp": "2024-01-15T10:30:00"
}
```

### Handling Errors in Client

```python
response = requests.get(f"{base_url}/catalogs/main/schemas/default/tables")

if response.status_code == 200:
    tables = response.json()
    print(tables)
elif response.status_code == 403:
    print("Permission denied")
elif response.status_code == 404:
    print("Catalog or schema not found")
else:
    error = response.json()
    print(f"Error: {error['error']}")
```

## Security Best Practices

### 1. Authentication

```python
# OBO token automatically provided by Databricks Apps
# No manual token management required

def get_db_connection():
    # Uses DATABRICKS_TOKEN environment variable (OBO)
    return DatabaseConnection()
```

### 2. Input Validation

```python
# Validate SQL identifiers
if not validate_sql_identifier(table_name):
    raise HTTPException(status_code=400, detail="Invalid table name")

# Use Pydantic models for request validation
class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=10000)
```

### 3. Query Restrictions

```python
# Only allow SELECT queries
if not query.strip().lower().startswith('select'):
    raise HTTPException(status_code=400, detail="Only SELECT queries allowed")
```

### 4. Rate Limiting (Optional)

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@app.get("/query")
@limiter.limit("10/minute")
async def execute_query(request: Request):
    # Limited to 10 requests per minute
    pass
```

## Performance Optimization

### 1. Pagination

```python
# Always use pagination for large datasets
@app.get("/data")
async def get_data(
    limit: int = Query(100, le=10000),
    offset: int = Query(0, ge=0)
):
    query = f"SELECT * FROM table LIMIT {limit} OFFSET {offset}"
    return execute(query)
```

### 2. Streaming Responses

```python
from fastapi.responses import StreamingResponse

@app.get("/large-export")
async def export_data():
    def generate():
        # Stream data in chunks
        for chunk in data_chunks:
            yield chunk
    
    return StreamingResponse(generate(), media_type="text/csv")
```

### 3. Async Operations

```python
@app.get("/data")
async def get_data():
    # Use async for I/O operations
    results = await fetch_data_async()
    return results
```

## Deployment Considerations

### Environment Variables

```yaml
# app.yaml
env:
  - name: LOG_LEVEL
    value: "INFO"  # Use "DEBUG" in development
  
  - name: DEBUG
    value: "false"  # Hide error details in production
  
  - name: CORS_ORIGINS
    value: "https://your-frontend.com"  # Restrict CORS
```

### Monitoring

```python
# Add request logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"{request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Status: {response.status_code}")
    return response
```

### Health Checks

```python
# Kubernetes/Docker health checks
@app.get("/health/live")
async def liveness():
    return {"alive": True}

@app.get("/health/ready")
async def readiness():
    # Check database connectivity
    try:
        db = DatabaseConnection()
        return {"ready": True}
    except:
        raise HTTPException(status_code=503)
```

## Testing the API

### Manual Testing with curl

```bash
# Health check
curl http://localhost:8080/health

# Get data
curl http://localhost:8080/catalogs

# Post query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT 1 as test"}'
```

### Automated Testing with pytest

```python
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_list_catalogs():
    response = client.get("/catalogs")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
```

## Extension Ideas

1. **Authentication**: Add API key or JWT authentication
2. **Caching**: Implement Redis caching for frequent queries
3. **WebSockets**: Add real-time data streaming
4. **GraphQL**: Provide GraphQL endpoint as alternative
5. **Batch Operations**: Support multiple queries in one request
6. **File Upload**: Add CSV/JSON file upload for data import
7. **Export Formats**: Support more formats (Excel, Parquet)
8. **Query History**: Track and display user's query history

## Troubleshooting

### Issue: "Service Unavailable" errors

**Solution:**
- Check SQL Warehouse is running
- Verify connection credentials
- Check network connectivity

### Issue: "Permission Denied" on endpoints

**Solution:**
- Verify Unity Catalog permissions
- Check user has SELECT on tables
- Ensure OBO token is valid

### Issue: Slow API responses

**Solutions:**
- Add pagination
- Optimize SQL queries
- Increase warehouse size
- Implement caching

## Next Steps

After completing this lab:

1. Integrate APIs with frontend applications
2. Build client SDKs in various languages
3. Add advanced features (caching, rate limiting)
4. Deploy to production with monitoring
5. Create API documentation for consumers

## Additional Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic Models](https://docs.pydantic.dev/)
- [OpenAPI Specification](https://swagger.io/specification/)
- [REST API Best Practices](https://restfulapi.net/)

---

**Happy API Building! 🚀**
