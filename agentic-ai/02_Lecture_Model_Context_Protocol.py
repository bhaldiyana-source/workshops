# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Model Context Protocol (MCP)
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the Model Context Protocol (MCP), an open standard for connecting AI agents to tools and data sources. You'll learn MCP architecture, how to build MCP servers, and integrate them with agents on Databricks.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand what MCP is and why it's important for agent ecosystems
# MAGIC - Describe MCP architecture and components
# MAGIC - Design effective tools using MCP patterns
# MAGIC - Integrate MCP servers with Databricks services
# MAGIC - Apply best practices for security and performance
# MAGIC
# MAGIC ## Duration
# MAGIC 75 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed previous lectures on agent foundations and UC functions
# MAGIC - Understanding of client-server architecture
# MAGIC - Basic knowledge of async Python programming
# MAGIC - Familiarity with REST APIs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is Model Context Protocol (MCP)?
# MAGIC
# MAGIC MCP is an **open protocol** that standardizes how AI applications connect to data sources and tools.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 The MCP Vision
# MAGIC
# MAGIC **Problem**: Every AI application builds custom integrations
# MAGIC - Each agent framework has its own tool format
# MAGIC - Tools must be rewritten for different systems
# MAGIC - No standard way to share capabilities
# MAGIC
# MAGIC **Solution**: MCP provides a universal adapter
# MAGIC - One tool implementation works everywhere
# MAGIC - Standard protocol for tool discovery and execution
# MAGIC - Separation of concerns (tools vs agents)
# MAGIC
# MAGIC ### 1.2 Key Benefits
# MAGIC
# MAGIC #### ✅ Standardization
# MAGIC - Common protocol across all agent frameworks
# MAGIC - Consistent tool schemas and execution
# MAGIC - Interoperability between systems
# MAGIC
# MAGIC #### ✅ Extensibility
# MAGIC - Easy to add new capabilities
# MAGIC - Plug-and-play architecture
# MAGIC - Community-driven tool ecosystem
# MAGIC
# MAGIC #### ✅ Separation of Concerns
# MAGIC - Tools live in dedicated servers
# MAGIC - Agents don't bundle tool logic
# MAGIC - Clear boundaries and responsibilities
# MAGIC
# MAGIC #### ✅ Reusability
# MAGIC - Write once, use in any agent
# MAGIC - Share tools across teams
# MAGIC - Build on existing implementations
# MAGIC
# MAGIC #### ✅ Security
# MAGIC - Controlled access to resources
# MAGIC - Authentication and authorization
# MAGIC - Audit logging at tool level

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. MCP Architecture
# MAGIC
# MAGIC MCP uses a client-server model with standardized communication.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────┐
# MAGIC │                    MCP ARCHITECTURE                          │
# MAGIC ├──────────────────────────────────────────────────────────────┤
# MAGIC │                                                              │
# MAGIC │  ┌─────────────────┐         ┌──────────────────────┐      │
# MAGIC │  │   MCP CLIENT    │         │    MCP SERVER        │      │
# MAGIC │  │                 │         │                      │      │
# MAGIC │  │  ┌───────────┐  │         │  ┌───────────────┐  │      │
# MAGIC │  │  │   Agent   │  │  MCP    │  │  Tool Host    │  │      │
# MAGIC │  │  │   (LLM)   │◄─┼────────►│  │               │  │      │
# MAGIC │  │  └───────────┘  │ Protocol│  │  ┌──────────┐ │  │      │
# MAGIC │  │                 │         │  │  │ Tool 1   │ │  │      │
# MAGIC │  │  - Discovery    │         │  │  ├──────────┤ │  │      │
# MAGIC │  │  - Execution    │         │  │  │ Tool 2   │ │  │      │
# MAGIC │  │  - Processing   │         │  │  ├──────────┤ │  │      │
# MAGIC │  │                 │         │  │  │ Tool 3   │ │  │      │
# MAGIC │  └─────────────────┘         │  │  └──────────┘ │  │      │
# MAGIC │                              │  │                │  │      │
# MAGIC │                              │  │  ┌──────────┐  │  │      │
# MAGIC │                              │  │  │Resources │  │  │      │
# MAGIC │                              │  │  │- DB      │  │  │      │
# MAGIC │                              │  │  │- APIs    │  │  │      │
# MAGIC │                              │  │  │- Files   │  │  │      │
# MAGIC │                              │  │  └──────────┘  │  │      │
# MAGIC │                              │  └───────────────┘  │      │
# MAGIC │                              └──────────────────────┘      │
# MAGIC │                                                              │
# MAGIC │  Communication Flow:                                        │
# MAGIC │  1. Client discovers available tools                        │
# MAGIC │  2. Client requests tool execution with parameters          │
# MAGIC │  3. Server executes tool and returns results                │
# MAGIC │  4. Client processes response                               │
# MAGIC │                                                              │
# MAGIC └──────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 MCP Components
# MAGIC
# MAGIC #### 1. MCP Server
# MAGIC **Hosts and executes tools**
# MAGIC
# MAGIC Responsibilities:
# MAGIC - Register available tools with schemas
# MAGIC - Handle tool execution requests
# MAGIC - Manage authentication and permissions
# MAGIC - Return structured responses
# MAGIC - Provide resource access
# MAGIC
# MAGIC #### 2. MCP Client (Agent)
# MAGIC **Consumes tools from servers**
# MAGIC
# MAGIC Responsibilities:
# MAGIC - Discover available tools
# MAGIC - Select appropriate tools
# MAGIC - Send execution requests
# MAGIC - Process tool responses
# MAGIC - Handle errors gracefully
# MAGIC
# MAGIC #### 3. Tools
# MAGIC **Discrete capabilities exposed via MCP**
# MAGIC
# MAGIC Characteristics:
# MAGIC - Well-defined input schema (JSON Schema)
# MAGIC - Predictable output format
# MAGIC - Clear description for LLM
# MAGIC - Can be stateful or stateless
# MAGIC - Support synchronous or async execution
# MAGIC
# MAGIC #### 4. Resources
# MAGIC **Data sources accessible through MCP**
# MAGIC
# MAGIC Types:
# MAGIC - Databases and data warehouses
# MAGIC - File systems and object storage
# MAGIC - External APIs
# MAGIC - Vector stores
# MAGIC - Custom data sources

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 MCP Protocol Messages
# MAGIC
# MAGIC **Tool Discovery:**
# MAGIC ```json
# MAGIC {
# MAGIC   "jsonrpc": "2.0",
# MAGIC   "method": "tools/list",
# MAGIC   "id": 1
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Tool Execution:**
# MAGIC ```json
# MAGIC {
# MAGIC   "jsonrpc": "2.0",
# MAGIC   "method": "tools/call",
# MAGIC   "params": {
# MAGIC     "name": "query_database",
# MAGIC     "arguments": {
# MAGIC       "query": "SELECT * FROM sales WHERE region = 'West'"
# MAGIC     }
# MAGIC   },
# MAGIC   "id": 2
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Tool Response:**
# MAGIC ```json
# MAGIC {
# MAGIC   "jsonrpc": "2.0",
# MAGIC   "result": {
# MAGIC     "content": [
# MAGIC       {
# MAGIC         "type": "text",
# MAGIC         "text": "Retrieved 1,234 records..."
# MAGIC       }
# MAGIC     ]
# MAGIC   },
# MAGIC   "id": 2
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Building an MCP Server
# MAGIC
# MAGIC Let's build a basic MCP server step by step.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Basic MCP Server Structure

# COMMAND ----------

# Example: Basic MCP Server
# Note: This is simplified for demonstration. Production servers use the official MCP SDK.

import asyncio
import json
from typing import List, Dict, Any
from dataclasses import dataclass, asdict

@dataclass
class Tool:
    """MCP Tool definition"""
    name: str
    description: str
    inputSchema: Dict[str, Any]

@dataclass
class TextContent:
    """Text content response"""
    type: str = "text"
    text: str = ""

class BasicMCPServer:
    """Basic MCP Server implementation"""
    
    def __init__(self, name: str):
        self.name = name
        self.tools = {}
        self.handlers = {}
    
    def register_tool(self, tool: Tool, handler):
        """Register a tool with its handler function"""
        self.tools[tool.name] = tool
        self.handlers[tool.name] = handler
    
    async def list_tools(self) -> List[Dict]:
        """List all available tools"""
        return [asdict(tool) for tool in self.tools.values()]
    
    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> List[TextContent]:
        """Execute a tool by name"""
        if name not in self.handlers:
            raise ValueError(f"Unknown tool: {name}")
        
        handler = self.handlers[name]
        result = await handler(arguments)
        
        return [TextContent(type="text", text=str(result))]
    
    async def handle_request(self, request: Dict) -> Dict:
        """Handle incoming MCP request"""
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")
        
        try:
            if method == "tools/list":
                result = await self.list_tools()
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await self.call_tool(tool_name, arguments)
            else:
                raise ValueError(f"Unknown method: {method}")
            
            return {
                "jsonrpc": "2.0",
                "result": result,
                "id": request_id
            }
        
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32000,
                    "message": str(e)
                },
                "id": request_id
            }

# Create server instance
server = BasicMCPServer("databricks-demo-server")

print("Basic MCP Server Structure Created")
print("=" * 60)
print(f"Server Name: {server.name}")
print("Ready to register tools...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Registering Tools

# COMMAND ----------

# Example: Register tools with the MCP server

# Define tool schemas
calculator_tool = Tool(
    name="calculate",
    description="Performs basic mathematical calculations",
    inputSchema={
        "type": "object",
        "properties": {
            "expression": {
                "type": "string",
                "description": "Mathematical expression to evaluate (e.g., '2 + 2', '10 * 5')"
            }
        },
        "required": ["expression"]
    }
)

greeting_tool = Tool(
    name="greet",
    description="Generates a personalized greeting message",
    inputSchema={
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Name of the person to greet"
            },
            "language": {
                "type": "string",
                "description": "Language for greeting (en, es, fr)",
                "enum": ["en", "es", "fr"]
            }
        },
        "required": ["name"]
    }
)

# Define tool handlers
async def calculate_handler(args: Dict) -> str:
    """Handle calculation requests"""
    expression = args.get("expression", "")
    try:
        # Safe eval for simple math (in production, use safer parser)
        result = eval(expression, {"__builtins__": {}}, {})
        return f"Result: {result}"
    except Exception as e:
        return f"Error: {str(e)}"

async def greet_handler(args: Dict) -> str:
    """Handle greeting requests"""
    name = args.get("name", "Friend")
    language = args.get("language", "en")
    
    greetings = {
        "en": f"Hello, {name}!",
        "es": f"¡Hola, {name}!",
        "fr": f"Bonjour, {name}!"
    }
    
    return greetings.get(language, greetings["en"])

# Register tools
server.register_tool(calculator_tool, calculate_handler)
server.register_tool(greeting_tool, greet_handler)

print("Tools Registered Successfully")
print("=" * 60)

# List available tools
async def show_tools():
    tools = await server.list_tools()
    for tool in tools:
        print(f"\nTool: {tool['name']}")
        print(f"  Description: {tool['description']}")
        print(f"  Parameters:")
        for prop_name, prop_def in tool['inputSchema']['properties'].items():
            required = "required" if prop_name in tool['inputSchema'].get('required', []) else "optional"
            print(f"    - {prop_name} ({required}): {prop_def['description']}")

# Run async function
import nest_asyncio
nest_asyncio.apply()

asyncio.run(show_tools())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Testing Tool Execution

# COMMAND ----------

# Example: Test tool execution
async def test_mcp_server():
    """Test the MCP server with sample requests"""
    
    print("MCP Server Tool Execution Tests")
    print("=" * 60)
    
    # Test 1: List tools
    print("\n1. Listing Available Tools")
    print("-" * 60)
    list_request = {
        "jsonrpc": "2.0",
        "method": "tools/list",
        "id": 1
    }
    response = await server.handle_request(list_request)
    print(f"Request: {json.dumps(list_request, indent=2)}")
    print(f"Response: Found {len(response['result'])} tools")
    
    # Test 2: Call calculator tool
    print("\n2. Calling Calculator Tool")
    print("-" * 60)
    calc_request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "calculate",
            "arguments": {
                "expression": "15 * 7 + 3"
            }
        },
        "id": 2
    }
    response = await server.handle_request(calc_request)
    print(f"Expression: 15 * 7 + 3")
    print(f"Result: {response['result'][0]['text']}")
    
    # Test 3: Call greeting tool
    print("\n3. Calling Greeting Tool")
    print("-" * 60)
    greet_request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "greet",
            "arguments": {
                "name": "Alice",
                "language": "es"
            }
        },
        "id": 3
    }
    response = await server.handle_request(greet_request)
    print(f"Name: Alice, Language: Spanish")
    print(f"Result: {response['result'][0]['text']}")
    
    # Test 4: Error handling
    print("\n4. Error Handling")
    print("-" * 60)
    error_request = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "nonexistent_tool",
            "arguments": {}
        },
        "id": 4
    }
    response = await server.handle_request(error_request)
    print(f"Calling nonexistent tool")
    print(f"Error: {response['error']['message']}")
    
    print("\n" + "=" * 60)
    print("✓ All tests completed")

# Run tests
asyncio.run(test_mcp_server())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Databricks-Native MCP Server
# MAGIC
# MAGIC Building an MCP server that integrates with Databricks services.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Databricks MCP Server Design

# COMMAND ----------

# Example: Databricks MCP Server with Native Integration
class DatabricksMCPServer:
    """MCP Server with Databricks service integration"""
    
    def __init__(self, server_name: str = "databricks-mcp"):
        self.server_name = server_name
        self.tools = []
        self.setup_tools()
    
    def setup_tools(self):
        """Define Databricks-integrated tools"""
        
        self.tools = [
            {
                "name": "sql_query",
                "description": "Execute a SQL query against Databricks SQL Warehouse. Use this for querying data from Delta tables.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute"
                        },
                        "warehouse_id": {
                            "type": "string",
                            "description": "SQL Warehouse ID for execution"
                        }
                    },
                    "required": ["query", "warehouse_id"]
                }
            },
            {
                "name": "vector_search",
                "description": "Search a vector index for relevant documents using semantic similarity.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "index_name": {
                            "type": "string",
                            "description": "Full name of vector index (catalog.schema.index)"
                        },
                        "query": {
                            "type": "string",
                            "description": "Search query text"
                        },
                        "num_results": {
                            "type": "integer",
                            "description": "Number of results to return",
                            "default": 5
                        }
                    },
                    "required": ["index_name", "query"]
                }
            },
            {
                "name": "call_uc_function",
                "description": "Call a Unity Catalog function to perform operations defined in your data lakehouse.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "function_name": {
                            "type": "string",
                            "description": "Full name of UC function (catalog.schema.function_name)"
                        },
                        "parameters": {
                            "type": "object",
                            "description": "Parameters to pass to the function"
                        }
                    },
                    "required": ["function_name"]
                }
            },
            {
                "name": "run_notebook",
                "description": "Execute a Databricks notebook with parameters. Use for complex data processing tasks.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "notebook_path": {
                            "type": "string",
                            "description": "Path to notebook in workspace"
                        },
                        "parameters": {
                            "type": "object",
                            "description": "Parameters to pass to notebook"
                        },
                        "timeout_seconds": {
                            "type": "integer",
                            "description": "Execution timeout in seconds",
                            "default": 300
                        }
                    },
                    "required": ["notebook_path"]
                }
            },
            {
                "name": "get_table_metadata",
                "description": "Retrieve metadata about a Delta table including schema, statistics, and properties.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Full table name (catalog.schema.table)"
                        }
                    },
                    "required": ["table_name"]
                }
            }
        ]
    
    async def list_tools(self) -> List[Dict]:
        """Return all available tools"""
        return self.tools
    
    async def execute_sql_query(self, query: str, warehouse_id: str) -> Dict:
        """Execute SQL query (simulated)"""
        # In production, use databricks.sdk:
        # from databricks.sdk import WorkspaceClient
        # w = WorkspaceClient()
        # result = w.statement_execution.execute_statement(
        #     warehouse_id=warehouse_id,
        #     statement=query
        # )
        
        return {
            "success": True,
            "rows": [
                {"region": "West", "revenue": 1100000},
                {"region": "East", "revenue": 800000}
            ],
            "row_count": 2
        }
    
    async def vector_search(self, index_name: str, query: str, num_results: int = 5) -> Dict:
        """Perform vector search (simulated)"""
        # In production, use databricks.vector_search
        
        return {
            "success": True,
            "results": [
                {"text": "Document 1 content...", "score": 0.95},
                {"text": "Document 2 content...", "score": 0.87}
            ],
            "count": 2
        }
    
    async def call_uc_function(self, function_name: str, parameters: Dict) -> Dict:
        """Call Unity Catalog function (simulated)"""
        return {
            "success": True,
            "result": f"Called {function_name} with parameters: {parameters}"
        }
    
    async def call_tool(self, name: str, arguments: Dict) -> Dict:
        """Route tool calls to appropriate handlers"""
        
        if name == "sql_query":
            return await self.execute_sql_query(
                arguments["query"],
                arguments["warehouse_id"]
            )
        elif name == "vector_search":
            return await self.vector_search(
                arguments["index_name"],
                arguments["query"],
                arguments.get("num_results", 5)
            )
        elif name == "call_uc_function":
            return await self.call_uc_function(
                arguments["function_name"],
                arguments.get("parameters", {})
            )
        else:
            return {"success": False, "error": f"Unknown tool: {name}"}

# Initialize Databricks MCP Server
dbx_server = DatabricksMCPServer()

print("Databricks MCP Server Initialized")
print("=" * 60)

# Show available tools
async def show_databricks_tools():
    tools = await dbx_server.list_tools()
    print(f"\nAvailable Tools: {len(tools)}\n")
    for i, tool in enumerate(tools, 1):
        print(f"{i}. {tool['name']}")
        print(f"   {tool['description']}")
        print()

asyncio.run(show_databricks_tools())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Testing Databricks MCP Server

# COMMAND ----------

# Example: Test Databricks MCP Server
async def test_databricks_mcp():
    """Test Databricks MCP server tools"""
    
    print("Testing Databricks MCP Server")
    print("=" * 60)
    
    # Test 1: SQL Query
    print("\n1. SQL Query Tool")
    print("-" * 60)
    result = await dbx_server.call_tool(
        "sql_query",
        {
            "query": "SELECT region, SUM(revenue) FROM sales GROUP BY region",
            "warehouse_id": "abc123"
        }
    )
    print(f"Query: SELECT region, SUM(revenue)...")
    print(f"Rows returned: {result['row_count']}")
    print(f"Data: {result['rows']}")
    
    # Test 2: Vector Search
    print("\n2. Vector Search Tool")
    print("-" * 60)
    result = await dbx_server.call_tool(
        "vector_search",
        {
            "index_name": "main.default.docs_index",
            "query": "How do I configure Delta Lake?",
            "num_results": 3
        }
    )
    print(f"Query: 'How do I configure Delta Lake?'")
    print(f"Results found: {result['count']}")
    
    # Test 3: UC Function Call
    print("\n3. Unity Catalog Function Tool")
    print("-" * 60)
    result = await dbx_server.call_tool(
        "call_uc_function",
        {
            "function_name": "main.agents.calculate_revenue",
            "parameters": {"region": "West", "start_date": "2024-01-01"}
        }
    )
    print(f"Function: main.agents.calculate_revenue")
    print(f"Result: {result['result']}")
    
    print("\n" + "=" * 60)
    print("✓ All Databricks MCP tests completed")

# Run tests
asyncio.run(test_databricks_mcp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MCP Best Practices
# MAGIC
# MAGIC Design patterns for production MCP servers.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Tool Design Principles
# MAGIC
# MAGIC #### Single Responsibility
# MAGIC ✅ **Do**: Each tool does one thing well
# MAGIC ```python
# MAGIC # Good
# MAGIC get_customer_orders(customer_id)
# MAGIC calculate_total_revenue(date_range)
# MAGIC send_email_notification(recipient, message)
# MAGIC
# MAGIC # Bad
# MAGIC process_customer_data(customer_id, action_type, date_range, send_email=False)
# MAGIC ```
# MAGIC
# MAGIC #### Clear Naming
# MAGIC ✅ **Do**: Use descriptive, action-oriented names
# MAGIC - `query_sales_data` not `qsd`
# MAGIC - `search_documentation` not `search`
# MAGIC - `update_customer_record` not `update`
# MAGIC
# MAGIC #### Comprehensive Schemas
# MAGIC ✅ **Do**: Provide detailed input schemas
# MAGIC ```python
# MAGIC {
# MAGIC     "type": "object",
# MAGIC     "properties": {
# MAGIC         "customer_id": {
# MAGIC             "type": "integer",
# MAGIC             "description": "Unique customer identifier (positive integer)",
# MAGIC             "minimum": 1
# MAGIC         },
# MAGIC         "include_history": {
# MAGIC             "type": "boolean",
# MAGIC             "description": "Whether to include full order history",
# MAGIC             "default": False
# MAGIC         }
# MAGIC     },
# MAGIC     "required": ["customer_id"]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# Example: Well-designed tool
class WellDesignedTool:
    """Example of a well-designed MCP tool"""
    
    @staticmethod
    def get_tool_definition():
        return {
            "name": "analyze_sales_performance",
            "description": """
                Analyzes sales performance metrics for a specified time period and region.
                
                Use this tool when you need to:
                - Compare sales across different regions
                - Identify trends in specific time periods
                - Generate performance reports
                
                The tool returns aggregated metrics including total revenue, order count,
                average order value, and growth rates.
            """.strip(),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "format": "date",
                        "description": "Start date in YYYY-MM-DD format",
                        "example": "2024-01-01"
                    },
                    "end_date": {
                        "type": "string",
                        "format": "date",
                        "description": "End date in YYYY-MM-DD format",
                        "example": "2024-12-31"
                    },
                    "region": {
                        "type": "string",
                        "description": "Geographic region to analyze",
                        "enum": ["North", "South", "East", "West", "All"],
                        "default": "All"
                    },
                    "include_breakdown": {
                        "type": "boolean",
                        "description": "Include detailed breakdown by product category",
                        "default": False
                    }
                },
                "required": ["start_date", "end_date"]
            }
        }
    
    @staticmethod
    async def execute(arguments: Dict) -> Dict:
        """Execute the tool with validation and error handling"""
        # Validate inputs
        if not arguments.get("start_date") or not arguments.get("end_date"):
            return {
                "success": False,
                "error": "start_date and end_date are required"
            }
        
        # Execute analysis (simulated)
        result = {
            "success": True,
            "metrics": {
                "total_revenue": 2500000.0,
                "order_count": 1500,
                "avg_order_value": 1666.67,
                "growth_rate": 15.5
            },
            "period": f"{arguments['start_date']} to {arguments['end_date']}",
            "region": arguments.get("region", "All")
        }
        
        if arguments.get("include_breakdown", False):
            result["breakdown"] = {
                "Electronics": 1000000.0,
                "Clothing": 800000.0,
                "Home": 700000.0
            }
        
        return result

# Display tool definition
tool_def = WellDesignedTool.get_tool_definition()
print("Well-Designed Tool Example")
print("=" * 60)
print(f"Name: {tool_def['name']}")
print(f"\nDescription:\n{tool_def['description']}")
print(f"\nParameters:")
for prop_name, prop_def in tool_def['inputSchema']['properties'].items():
    required = "✓" if prop_name in tool_def['inputSchema'].get('required', []) else " "
    print(f"  [{required}] {prop_name} ({prop_def['type']})")
    print(f"      {prop_def['description']}")

# Test execution
async def test_tool():
    result = await WellDesignedTool.execute({
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "region": "West",
        "include_breakdown": True
    })
    print(f"\nExecution Result:")
    print(json.dumps(result, indent=2))

asyncio.run(test_tool())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Security Best Practices
# MAGIC
# MAGIC #### Authentication
# MAGIC - Require API keys or tokens
# MAGIC - Validate credentials on every request
# MAGIC - Use short-lived tokens
# MAGIC
# MAGIC #### Authorization
# MAGIC - Implement role-based access control
# MAGIC - Verify permissions per tool
# MAGIC - Log all access attempts
# MAGIC
# MAGIC #### Input Validation
# MAGIC - Validate all parameters against schema
# MAGIC - Sanitize string inputs
# MAGIC - Limit input sizes
# MAGIC - Check for injection attacks
# MAGIC
# MAGIC #### Rate Limiting
# MAGIC - Prevent abuse with rate limits
# MAGIC - Track usage per client
# MAGIC - Implement backoff strategies

# COMMAND ----------

# Example: Secure MCP server with authentication
class SecureMCPServer:
    """MCP Server with security features"""
    
    def __init__(self):
        self.api_keys = {
            "key_abc123": {"user": "agent1", "permissions": ["read", "execute"]},
            "key_xyz789": {"user": "agent2", "permissions": ["read"]}
        }
        self.rate_limits = {}  # Track requests per client
        self.audit_log = []
    
    def authenticate(self, api_key: str) -> Dict:
        """Verify API key"""
        if api_key not in self.api_keys:
            return {"authenticated": False, "error": "Invalid API key"}
        return {
            "authenticated": True,
            "user": self.api_keys[api_key]["user"],
            "permissions": self.api_keys[api_key]["permissions"]
        }
    
    def authorize(self, user: str, permissions: List[str], required_permission: str) -> bool:
        """Check if user has required permission"""
        return required_permission in permissions
    
    def check_rate_limit(self, user: str) -> bool:
        """Check if user is within rate limits"""
        # Simplified rate limiting
        current_minute = int(asyncio.get_event_loop().time() / 60)
        
        if user not in self.rate_limits:
            self.rate_limits[user] = {}
        
        if current_minute not in self.rate_limits[user]:
            self.rate_limits[user] = {current_minute: 0}
        
        # Allow 60 requests per minute
        if self.rate_limits[user].get(current_minute, 0) >= 60:
            return False
        
        self.rate_limits[user][current_minute] = self.rate_limits[user].get(current_minute, 0) + 1
        return True
    
    def log_request(self, user: str, tool: str, success: bool):
        """Log tool execution for audit"""
        from datetime import datetime
        self.audit_log.append({
            "timestamp": datetime.now().isoformat(),
            "user": user,
            "tool": tool,
            "success": success
        })
    
    async def handle_request(self, request: Dict, api_key: str) -> Dict:
        """Handle request with security checks"""
        
        # 1. Authentication
        auth_result = self.authenticate(api_key)
        if not auth_result["authenticated"]:
            return {"error": "Authentication failed"}
        
        user = auth_result["user"]
        permissions = auth_result["permissions"]
        
        # 2. Rate limiting
        if not self.check_rate_limit(user):
            return {"error": "Rate limit exceeded"}
        
        # 3. Authorization
        tool_name = request.get("params", {}).get("name")
        required_permission = "execute"
        
        if not self.authorize(user, permissions, required_permission):
            self.log_request(user, tool_name, False)
            return {"error": "Permission denied"}
        
        # 4. Execute tool
        try:
            # Tool execution logic here
            result = {"success": True, "data": "Tool executed"}
            self.log_request(user, tool_name, True)
            return result
        except Exception as e:
            self.log_request(user, tool_name, False)
            return {"error": str(e)}

# Demonstrate security features
secure_server = SecureMCPServer()

print("Secure MCP Server Features")
print("=" * 60)

# Test authentication
print("\n1. Authentication:")
auth_valid = secure_server.authenticate("key_abc123")
auth_invalid = secure_server.authenticate("key_invalid")
print(f"  Valid key: {auth_valid['authenticated']}")
print(f"  Invalid key: {auth_invalid['authenticated']}")

# Test authorization
print("\n2. Authorization:")
has_exec = secure_server.authorize("agent1", ["read", "execute"], "execute")
no_exec = secure_server.authorize("agent2", ["read"], "execute")
print(f"  Agent1 can execute: {has_exec}")
print(f"  Agent2 can execute: {no_exec}")

# Test rate limiting
print("\n3. Rate Limiting:")
within_limit = secure_server.check_rate_limit("agent1")
print(f"  First request: {within_limit}")

print("\n✓ Security features demonstrated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Performance Optimization
# MAGIC
# MAGIC #### Async Operations
# MAGIC - Use async/await for I/O operations
# MAGIC - Parallel tool execution when possible
# MAGIC - Non-blocking server design
# MAGIC
# MAGIC #### Connection Pooling
# MAGIC - Reuse database connections
# MAGIC - Pool API client instances
# MAGIC - Avoid connection overhead
# MAGIC
# MAGIC #### Caching
# MAGIC - Cache expensive operations
# MAGIC - TTL-based cache invalidation
# MAGIC - Cache-aside pattern
# MAGIC
# MAGIC #### Timeouts
# MAGIC - Set reasonable timeouts
# MAGIC - Handle timeout errors gracefully
# MAGIC - Provide timeout configuration

# COMMAND ----------

# Example: Performance-optimized MCP server
import time
from functools import lru_cache

class OptimizedMCPServer:
    """MCP Server with performance optimizations"""
    
    def __init__(self):
        self.connection_pool = self.create_connection_pool()
        self.cache = {}
    
    def create_connection_pool(self):
        """Create reusable connection pool"""
        # Simulated connection pool
        return {"connections": [], "size": 10}
    
    @lru_cache(maxsize=100)
    def cached_query(self, query: str) -> Dict:
        """Cache frequent queries"""
        # Simulated query with caching
        return {"cached": True, "result": "data"}
    
    async def execute_with_timeout(self, func, timeout_seconds: int = 30):
        """Execute function with timeout"""
        try:
            result = await asyncio.wait_for(func(), timeout=timeout_seconds)
            return {"success": True, "result": result}
        except asyncio.TimeoutError:
            return {"success": False, "error": "Operation timed out"}
    
    async def parallel_tool_execution(self, tools: List[Dict]) -> List[Dict]:
        """Execute multiple tools in parallel"""
        tasks = [self.execute_tool(tool) for tool in tools]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    async def execute_tool(self, tool: Dict) -> Dict:
        """Execute single tool (simulated)"""
        await asyncio.sleep(0.1)  # Simulate work
        return {"tool": tool["name"], "result": "completed"}

# Demonstrate performance features
opt_server = OptimizedMCPServer()

print("Performance Optimization Features")
print("=" * 60)

# Test caching
print("\n1. Caching:")
start = time.time()
result1 = opt_server.cached_query("SELECT * FROM sales")
time1 = time.time() - start

start = time.time()
result2 = opt_server.cached_query("SELECT * FROM sales")  # Cached
time2 = time.time() - start

print(f"  First call: {time1*1000:.2f}ms")
print(f"  Cached call: {time2*1000:.2f}ms")
print(f"  Speedup: {time1/time2:.1f}x faster")

# Test parallel execution
async def test_parallel():
    print("\n2. Parallel Execution:")
    tools = [{"name": f"tool_{i}"} for i in range(5)]
    
    start = time.time()
    results = await opt_server.parallel_tool_execution(tools)
    duration = time.time() - start
    
    print(f"  Executed {len(tools)} tools in {duration*1000:.0f}ms")
    print(f"  Average per tool: {duration*1000/len(tools):.0f}ms")

asyncio.run(test_parallel())

print("\n✓ Performance optimizations demonstrated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC ✅ **MCP provides a universal standard for AI tool integration**
# MAGIC - Common protocol across frameworks
# MAGIC - Plug-and-play architecture
# MAGIC - Separation of tools and agents
# MAGIC
# MAGIC ✅ **MCP architecture has clear components**
# MAGIC - Servers host and execute tools
# MAGIC - Clients (agents) consume tools
# MAGIC - Tools are discrete capabilities
# MAGIC - Resources provide data access
# MAGIC
# MAGIC ✅ **Building MCP servers is straightforward**
# MAGIC - Define tool schemas with JSON Schema
# MAGIC - Implement handlers for each tool
# MAGIC - Handle requests and responses
# MAGIC - Support tool discovery
# MAGIC
# MAGIC ✅ **Databricks integration is powerful**
# MAGIC - Native SQL Warehouse access
# MAGIC - Vector Search integration
# MAGIC - UC Function calling
# MAGIC - Notebook execution
# MAGIC
# MAGIC ✅ **Best practices ensure production readiness**
# MAGIC - Single responsibility per tool
# MAGIC - Comprehensive documentation
# MAGIC - Security (auth, authorization, rate limiting)
# MAGIC - Performance (async, caching, pooling)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Next Steps
# MAGIC
# MAGIC Now you understand MCP and can build tool servers!
# MAGIC
# MAGIC **Continue to:**
# MAGIC - **03_Lecture_Agent_Frameworks**: Learn about LangChain, LlamaIndex, and integration
# MAGIC - **10_Demo_Setting_Up_MCP_Servers**: Build a complete MCP server
# MAGIC - **17_Lab_Custom_MCP_Server**: Create your own domain-specific server
# MAGIC
# MAGIC **Practice by:**
# MAGIC 1. Designing 3-5 tools for your use case
# MAGIC 2. Building a simple MCP server
# MAGIC 3. Testing with agent frameworks
# MAGIC
# MAGIC **Additional Resources:**
# MAGIC - [Model Context Protocol Specification](https://modelcontextprotocol.io/)
# MAGIC - [MCP GitHub Repository](https://github.com/modelcontextprotocol)
# MAGIC - [Databricks SDK Documentation](https://databricks-sdk-py.readthedocs.io/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this lecture, we covered:
# MAGIC
# MAGIC - ✅ What MCP is and why it matters
# MAGIC - ✅ MCP architecture and components
# MAGIC - ✅ Building basic MCP servers
# MAGIC - ✅ Databricks-native MCP integration
# MAGIC - ✅ Best practices for design, security, and performance
# MAGIC
# MAGIC You now have the knowledge to build production MCP servers that extend agent capabilities!
