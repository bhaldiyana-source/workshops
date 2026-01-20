# Agentic AI on Databricks: Comprehensive Workshop

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 480 minutes   | Estimated duration to complete all modules (8 hours). |
| Level           | 200/300       | Target difficulty level (200 = intermediate, 300 = advanced). |
| Lab Status      | Active        | Workshop is actively maintained and updated. |
| Course Location | N/A           | Lab is only available in this repo. |
| Developer       | Ajit Kalura   | Primary developer(s) of the lab. |
| Reviewer        | Ajit Kalura   | Subject matter expert reviewer(s). |
| Product Version | Databricks Runtime 14.3+, MLflow 2.10+, Python 3.10+ | Required versions. |

---

## Description

This comprehensive workshop teaches you to build production-grade agentic AI systems on Databricks. You'll master agent fundamentals, Unity Catalog function calling, Model Context Protocol (MCP) servers, multi-agent systems, RAG integration, and production deployment patterns. Learn to build, evaluate, and deploy intelligent AI agents that can reason, use tools, and collaborate to solve complex tasks using the full power of the Databricks Intelligence Platform.

## Learning Objectives

By the end of this workshop, you will be able to:

### Foundation
- Understand agent architecture and reasoning patterns
- Implement tool-calling with Unity Catalog functions
- Build and deploy MCP servers for extensible tool ecosystems
- Design effective agent prompts and workflows

### Technical Skills
- Create SQL and Python Unity Catalog functions as agent tools
- Integrate LangChain and LlamaIndex with Databricks
- Build RAG-enhanced agents with Vector Search
- Implement multi-agent systems with specialized roles
- Use MLflow for agent tracking and evaluation

### Production Capabilities
- Deploy agents using Model Serving
- Implement agent evaluation frameworks
- Monitor agent performance and quality
- Handle security and governance in agent systems
- Build compound AI systems with orchestration

### Advanced Techniques
- Implement agent memory and state management
- Create Genie agents for conversational analytics
- Build custom MCP servers for domain-specific tools
- Optimize agent performance and reliability
- Implement human-in-the-loop patterns

## Requirements & Prerequisites

Before starting this workshop, ensure you have:
- **Databricks workspace** with Unity Catalog enabled
- **Model Serving** access (serverless or GPU clusters)
- **Unity Catalog permissions** for CREATE FUNCTION and EXECUTE
- **Intermediate Python** knowledge
- **Understanding of LLMs** and prompting
- **Familiarity with REST APIs** (recommended)
- **Basic ML/MLflow knowledge** (recommended but not required)

## Contents

### Lectures (Theory)
1. **Foundations of Agentic AI** - Architecture, reasoning, and tool use
2. **Unity Catalog Functions as Tools** - SQL and Python function creation
3. **Model Context Protocol (MCP)** - Extensible tool ecosystems
4. **Agent Frameworks** - LangChain, LlamaIndex, and Databricks integrations
5. **Multi-Agent Systems** - Orchestration and collaboration patterns
6. **RAG for Agents** - Knowledge retrieval and grounding
7. **Agent Evaluation** - Testing, benchmarking, and quality assurance
8. **Production Deployment** - Serving, monitoring, and governance

### Demos (Guided Implementation)
1. **Building SQL Functions as Agent Tools** - Simple tool creation
2. **Building Python Functions as Agent Tools** - Complex tool logic
3. **Setting Up MCP Servers** - Custom tool providers
4. **Single Agent with LangChain** - Tool-calling agent
5. **Multi-Agent System** - Specialized agent collaboration
6. **RAG-Enhanced Agent** - Vector Search integration
7. **Genie Agent Creation** - Conversational analytics
8. **Agent with MLflow** - Tracking and evaluation

### Labs (Hands-on Practice)
1. **UC Functions for Tools** - Build your own tool library
2. **Custom MCP Server** - Domain-specific tools
3. **Research Assistant Agent** - Information gathering and synthesis
4. **Data Analysis Agent** - SQL generation and execution
5. **Multi-Agent Workflow** - Coordinated task completion
6. **RAG Agent Pipeline** - Document processing and Q&A
7. **Agent Evaluation Suite** - Quality metrics and testing
8. **Production Deployment** - End-to-end agent serving

### Resources
- Agent design patterns library
- Tool/function templates
- Evaluation frameworks
- Production deployment templates
- Troubleshooting guide

---

## Workshop Structure

### Module 1: Foundations of Agentic AI (45 minutes)

**Lecture: Understanding AI Agents**

#### What Are AI Agents?

AI Agents are autonomous systems that can:
- **Perceive**: Understand their environment and context
- **Reason**: Plan and make decisions using LLMs
- **Act**: Use tools to accomplish tasks
- **Learn**: Adapt based on feedback and results

#### Agent Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    AI AGENT ARCHITECTURE                 │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  1. USER INPUT                                          │
│     └─> Natural language request                        │
│                                                          │
│  2. REASONING (LLM)                                     │
│     ├─> Understand intent                               │
│     ├─> Plan approach                                   │
│     └─> Decide on actions                               │
│                                                          │
│  3. TOOL SELECTION                                      │
│     ├─> Identify needed tools                           │
│     ├─> Prepare parameters                              │
│     └─> Execute tool calls                              │
│                                                          │
│  4. TOOL EXECUTION                                      │
│     ├─> Unity Catalog Functions                         │
│     ├─> MCP Server Tools                                │
│     ├─> Vector Search                                   │
│     └─> External APIs                                   │
│                                                          │
│  5. OBSERVATION                                         │
│     ├─> Process tool results                            │
│     ├─> Evaluate success                                │
│     └─> Determine next steps                            │
│                                                          │
│  6. ITERATION                                           │
│     └─> Repeat 2-5 until task complete                  │
│                                                          │
│  7. RESPONSE                                            │
│     └─> Generate final answer                           │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

#### Agent Reasoning Patterns

**1. ReAct (Reasoning + Acting)**
- Interleaves reasoning and action
- Most common pattern for tool-using agents
- Steps: Thought → Action → Observation → Thought → ...

**2. Chain-of-Thought**
- Breaks complex problems into steps
- Reasons through each step explicitly
- Useful for planning and decomposition

**3. Tree-of-Thought**
- Explores multiple reasoning paths
- Evaluates alternatives
- Backtracks when needed

**4. Reflection**
- Reviews own outputs
- Self-critiques and improves
- Iterates until quality threshold met

#### Key Agent Components

**Tools (Functions):**
- Discrete capabilities the agent can use
- Well-defined inputs and outputs
- Examples: search, calculate, query database, call API

**Memory:**
- Short-term: Conversation history
- Long-term: Vector database of experiences
- Working memory: Current task context

**Planning:**
- Task decomposition
- Action sequencing
- Goal tracking

**Guardrails:**
- Input validation
- Output filtering
- Safety constraints
- Permission management

---

### Module 2: Unity Catalog Functions as Agent Tools (60 minutes)

**Lecture: UC Functions for Tool Calling**

#### Why Unity Catalog Functions?

✅ **Centralized Tool Management**
- Single source of truth for agent capabilities
- Versioned and governed
- Discoverable across workspace

✅ **Security & Governance**
- Fine-grained access control
- Audit logging
- Data lineage tracking

✅ **Multi-Language Support**
- SQL functions for data queries
- Python functions for complex logic
- Seamless integration

✅ **Built for Databricks**
- Native integration with AI Playground
- Works with Model Serving
- Access to Delta tables and Vector Search

#### Creating SQL Functions as Tools

**Simple Query Function:**
```sql
CREATE OR REPLACE FUNCTION catalog.schema.get_customer_orders(
  customer_id BIGINT
  COMMENT 'The ID of the customer'
)
RETURNS TABLE(order_id BIGINT, order_date DATE, total_amount DECIMAL(10,2))
COMMENT 'Retrieves all orders for a specific customer'
RETURN 
  SELECT order_id, order_date, total_amount
  FROM catalog.schema.orders
  WHERE customer_id = customer_id
  ORDER BY order_date DESC;
```

**Aggregation Function:**
```sql
CREATE OR REPLACE FUNCTION catalog.schema.calculate_revenue_by_region(
  start_date DATE COMMENT 'Start of date range',
  end_date DATE COMMENT 'End of date range'
)
RETURNS TABLE(region STRING, total_revenue DECIMAL(12,2))
COMMENT 'Calculates total revenue by region for a date range'
RETURN
  SELECT 
    region,
    SUM(total_amount) as total_revenue
  FROM catalog.schema.orders o
  JOIN catalog.schema.customers c ON o.customer_id = c.customer_id
  WHERE order_date BETWEEN start_date AND end_date
  GROUP BY region
  ORDER BY total_revenue DESC;
```

**Key SQL Function Best Practices:**
- Use descriptive function names (verb_noun pattern)
- Add comprehensive COMMENT metadata
- Document each parameter with inline COMMENT
- Return structured data (tables) when possible
- Keep queries focused and performant
- Use parameterized queries (never string concatenation)

#### Creating Python Functions as Tools

**Simple Python Function:**
```python
from pyspark.sql import SparkSession
from databricks.sdk.runtime import *

spark = SparkSession.builder.getOrCreate()

def calculate_customer_lifetime_value(customer_id: int) -> float:
    """
    Calculates the lifetime value of a customer.
    
    Args:
        customer_id: The ID of the customer
        
    Returns:
        The total lifetime value in dollars
    """
    result = spark.sql(f"""
        SELECT SUM(total_amount) as ltv
        FROM catalog.schema.orders
        WHERE customer_id = {customer_id}
    """).collect()
    
    return float(result[0]['ltv']) if result else 0.0

# Register as UC function
spark.udf.registerJavaFunction(
    name="catalog.schema.calculate_customer_ltv",
    javaClassName="calculate_customer_lifetime_value"
)
```

**Using DatabricksFunctionClient:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import FunctionInfo, FunctionParameterInfo

w = WorkspaceClient()

# Define function metadata
function_info = FunctionInfo(
    name="catalog.schema.analyze_sentiment",
    catalog_name="catalog",
    schema_name="schema",
    input_params=[
        FunctionParameterInfo(
            name="text",
            type_name="STRING",
            comment="Text to analyze"
        )
    ],
    data_type="STRING",
    full_data_type="STRING",
    comment="Analyzes sentiment of text using ML model",
    routine_body="EXTERNAL",
    routine_definition="return analyze_sentiment(text)",
    external_name="sentiment_analyzer"
)

# Create function
w.functions.create(function_info=function_info)
```

**Complex Python Function with External API:**
```python
import requests
from typing import Dict, Any

def enrich_company_data(company_name: str) -> Dict[str, Any]:
    """
    Enriches company data using external API.
    
    Args:
        company_name: Name of the company
        
    Returns:
        Dictionary with company details
    """
    # Call external API
    api_key = dbutils.secrets.get("scope", "api_key")
    response = requests.get(
        f"https://api.example.com/companies/{company_name}",
        headers={"Authorization": f"Bearer {api_key}"}
    )
    
    if response.status_code == 200:
        data = response.json()
        return {
            "name": data.get("name"),
            "industry": data.get("industry"),
            "employee_count": data.get("employees"),
            "revenue": data.get("revenue")
        }
    else:
        return {"error": "Company not found"}
```

**Key Python Function Best Practices:**
- Type hints for all parameters and returns
- Comprehensive docstrings
- Error handling and validation
- Use dbutils.secrets for credentials
- Return structured data (Dict, List) or primitives
- Keep functions focused (single responsibility)

#### Testing Functions in AI Playground

**Steps:**
1. Navigate to AI Playground in Databricks
2. Select a foundation model (e.g., DBRX, Llama 3)
3. Click "Add Tools" → "Unity Catalog Functions"
4. Select your catalog/schema
5. Choose functions to enable
6. Test with natural language prompts

**Example Prompts:**
```
"Show me all orders for customer ID 12345"
"Calculate total revenue by region for Q1 2024"
"What is the lifetime value of customer 67890?"
"Analyze the sentiment of this review: [text]"
```

#### Function Discovery

Agents can discover available tools:
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List all functions in a schema
functions = w.functions.list(
    catalog_name="catalog",
    schema_name="schema"
)

for func in functions:
    print(f"Function: {func.name}")
    print(f"Description: {func.comment}")
    print(f"Parameters: {func.input_params}")
    print("---")
```

---

### Module 3: Model Context Protocol (MCP) (75 minutes)

**Lecture: Extensible Tool Ecosystems with MCP**

#### What is MCP?

The **Model Context Protocol (MCP)** is an open protocol that standardizes how AI agents connect to external tools and data sources. Think of it as a universal adapter for AI agents.

**Key Benefits:**
- **Standardization**: Common protocol for all tools
- **Extensibility**: Easy to add new capabilities
- **Separation of Concerns**: Tools live outside agent code
- **Reusability**: Same tools work across different agents
- **Security**: Controlled access to resources

#### MCP Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    MCP ARCHITECTURE                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────┐         ┌─────────────────┐          │
│  │   AI Agent   │ ◄─MCP──►│   MCP Server    │          │
│  │  (Client)    │         │   (Tool Host)   │          │
│  └──────────────┘         └─────────────────┘          │
│         │                          │                     │
│         │                          │                     │
│         │                    ┌─────▼─────┐              │
│         │                    │   Tools   │              │
│         │                    ├───────────┤              │
│         │                    │ • Search  │              │
│         │                    │ • Database│              │
│         │                    │ • APIs    │              │
│         │                    │ • Files   │              │
│         │                    │ • Custom  │              │
│         │                    └───────────┘              │
│         │                                                │
│         └──────────► Requests & Responses                │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

#### MCP Components

**1. MCP Server:**
- Hosts tools and resources
- Handles tool execution
- Manages authentication and permissions
- Implements MCP protocol

**2. MCP Client (Agent):**
- Discovers available tools
- Sends tool execution requests
- Processes responses
- Integrated into agent framework

**3. Tools:**
- Discrete capabilities exposed via MCP
- Well-defined schemas (inputs/outputs)
- Can be stateful or stateless

**4. Resources:**
- Data sources accessible via MCP
- Files, databases, APIs, etc.
- Streamed or fetched on demand

#### Building an MCP Server

**Basic MCP Server Structure:**
```python
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import asyncio

# Initialize MCP server
server = Server("databricks-tools")

# Define tools
@server.list_tools()
async def list_tools() -> list[Tool]:
    """List all available tools"""
    return [
        Tool(
            name="query_database",
            description="Executes SQL query against Databricks",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="search_documentation",
            description="Searches Databricks documentation",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    }
                },
                "required": ["query"]
            }
        )
    ]

# Implement tool handlers
@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute tool by name"""
    
    if name == "query_database":
        query = arguments["query"]
        result = execute_sql_query(query)
        return [TextContent(type="text", text=str(result))]
    
    elif name == "search_documentation":
        query = arguments["query"]
        results = search_docs(query)
        return [TextContent(type="text", text=str(results))]
    
    else:
        raise ValueError(f"Unknown tool: {name}")

# Helper functions
def execute_sql_query(query: str) -> dict:
    """Execute SQL against Databricks"""
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        warehouse_id="warehouse_id",
        statement=query
    )
    return result.result.data_array

def search_docs(query: str) -> list:
    """Search documentation"""
    # Implement vector search against docs
    pass

# Start server
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
```

#### MCP Server with Databricks Integration

**Databricks-Native MCP Server:**
```python
from mcp.server import Server
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
import os

class DatabricksMCPServer:
    """MCP Server integrated with Databricks services"""
    
    def __init__(self):
        self.server = Server("databricks-mcp")
        self.workspace = WorkspaceClient()
        self.vector_search = VectorSearchClient()
        
        # Register tools
        self.register_tools()
    
    def register_tools(self):
        """Register all Databricks tools"""
        
        @self.server.list_tools()
        async def list_tools():
            return [
                Tool(
                    name="sql_query",
                    description="Execute SQL query on Databricks SQL Warehouse",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "warehouse_id": {"type": "string"}
                        },
                        "required": ["query", "warehouse_id"]
                    }
                ),
                Tool(
                    name="vector_search",
                    description="Search vector index for relevant documents",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "index_name": {"type": "string"},
                            "query": {"type": "string"},
                            "num_results": {"type": "integer"}
                        },
                        "required": ["index_name", "query"]
                    }
                ),
                Tool(
                    name="call_uc_function",
                    description="Call a Unity Catalog function",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "function_name": {"type": "string"},
                            "parameters": {"type": "object"}
                        },
                        "required": ["function_name"]
                    }
                ),
                Tool(
                    name="run_notebook",
                    description="Execute a Databricks notebook",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "notebook_path": {"type": "string"},
                            "parameters": {"type": "object"}
                        },
                        "required": ["notebook_path"]
                    }
                )
            ]
        
        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict):
            if name == "sql_query":
                return await self.execute_sql(arguments)
            elif name == "vector_search":
                return await self.vector_search_query(arguments)
            elif name == "call_uc_function":
                return await self.call_uc_function(arguments)
            elif name == "run_notebook":
                return await self.run_notebook(arguments)
    
    async def execute_sql(self, args: dict):
        """Execute SQL query"""
        result = self.workspace.statement_execution.execute_statement(
            warehouse_id=args["warehouse_id"],
            statement=args["query"]
        )
        return [TextContent(type="text", text=str(result.result.data_array))]
    
    async def vector_search_query(self, args: dict):
        """Search vector index"""
        index = self.vector_search.get_index(
            index_name=args["index_name"]
        )
        results = index.similarity_search(
            query_text=args["query"],
            num_results=args.get("num_results", 5)
        )
        return [TextContent(type="text", text=str(results))]
    
    async def call_uc_function(self, args: dict):
        """Call Unity Catalog function"""
        # Implementation depends on function signature
        pass
    
    async def run_notebook(self, args: dict):
        """Run Databricks notebook"""
        run = self.workspace.jobs.submit(
            run_name="MCP Notebook Execution",
            tasks=[{
                "task_key": "notebook_task",
                "notebook_task": {
                    "notebook_path": args["notebook_path"],
                    "base_parameters": args.get("parameters", {})
                }
            }]
        )
        return [TextContent(type="text", text=f"Job submitted: {run.run_id}")]
```

#### Using MCP in Agents

**LangChain MCP Integration:**
```python
from langchain.agents import initialize_agent, AgentType
from langchain_mcp import MCPToolkit
from langchain_community.llms import Databricks

# Connect to MCP server
mcp_toolkit = MCPToolkit(
    server_path="path/to/mcp_server.py"
)

# Get tools from MCP
tools = mcp_toolkit.get_tools()

# Initialize agent with MCP tools
llm = Databricks(endpoint_name="databricks-dbrx-instruct")

agent = initialize_agent(
    tools=tools,
    llm=llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

# Use agent
response = agent.run("Find all high-value customers in California")
```

#### MCP Best Practices

✅ **Tool Design:**
- Single responsibility per tool
- Clear, descriptive names
- Comprehensive input schemas
- Structured output formats
- Error handling with meaningful messages

✅ **Security:**
- Authentication for sensitive operations
- Rate limiting
- Input validation and sanitization
- Audit logging
- Least privilege access

✅ **Performance:**
- Async operations where possible
- Connection pooling
- Caching for expensive operations
- Timeouts and retries
- Monitoring and metrics

✅ **Documentation:**
- Clear tool descriptions
- Example usage
- Input/output schemas
- Error codes and handling
- Version information

---

### Module 4: Agent Frameworks (60 minutes)

**Lecture: LangChain, LlamaIndex, and Databricks**

#### Agent Framework Landscape

**LangChain:**
- General-purpose agent framework
- Rich ecosystem of integrations
- Focus on chains and agents
- Strong community support

**LlamaIndex:**
- Specialized in RAG and data indexing
- Excellent for knowledge retrieval
- Native vector store support
- Query engines and agents

**Databricks Native:**
- MLflow AI Gateway
- Unity Catalog integration
- Model Serving
- Built-in tool calling

#### LangChain on Databricks

**Basic Agent Setup:**
```python
from langchain.agents import create_databricks_tool_calling_agent
from langchain_community.chat_models import ChatDatabricks
from langchain.agents import AgentExecutor
from langchain_community.tools.databricks import UCFunctionToolkit

# Initialize LLM
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-70b-instruct",
    temperature=0.1
)

# Load Unity Catalog functions as tools
toolkit = UCFunctionToolkit(
    warehouse_id="your_warehouse_id",
    catalog="main",
    schema="agent_tools"
)

tools = toolkit.get_tools()

# Create agent
agent = create_databricks_tool_calling_agent(llm=llm, tools=tools)

# Create executor
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    max_iterations=10
)

# Run agent
result = agent_executor.invoke({
    "input": "What were our total sales in California last month?"
})

print(result["output"])
```

**Custom Tools with LangChain:**
```python
from langchain.tools import BaseTool
from typing import Optional, Type
from pydantic import BaseModel, Field

class CustomerSearchInput(BaseModel):
    """Input for customer search tool"""
    name: str = Field(description="Customer name to search for")
    state: Optional[str] = Field(description="State to filter by")

class CustomerSearchTool(BaseTool):
    """Tool for searching customers"""
    
    name = "search_customers"
    description = "Searches for customers by name and optional state"
    args_schema: Type[BaseModel] = CustomerSearchInput
    
    def _run(self, name: str, state: Optional[str] = None) -> str:
        """Execute customer search"""
        from databricks.sdk import WorkspaceClient
        
        w = WorkspaceClient()
        
        query = f"""
            SELECT customer_id, name, state, total_purchases
            FROM main.sales.customers
            WHERE name LIKE '%{name}%'
        """
        
        if state:
            query += f" AND state = '{state}'"
        
        result = w.statement_execution.execute_statement(
            warehouse_id="warehouse_id",
            statement=query
        )
        
        return str(result.result.data_array)
    
    async def _arun(self, name: str, state: Optional[str] = None) -> str:
        """Async version"""
        raise NotImplementedError("Async not implemented")

# Add to agent
tools = [CustomerSearchTool()]
agent = create_databricks_tool_calling_agent(llm=llm, tools=tools)
```

#### LlamaIndex on Databricks

**RAG Agent with LlamaIndex:**
```python
from llama_index.core import VectorStoreIndex, ServiceContext
from llama_index.llms.databricks import Databricks
from llama_index.vector_stores.databricks import DatabricksVectorStore
from llama_index.agent import OpenAIAgent
from llama_index.tools import QueryEngineTool, ToolMetadata

# Initialize LLM
llm = Databricks(
    model="databricks-meta-llama-3-70b-instruct",
    api_key=databricks_token
)

# Setup vector store
vector_store = DatabricksVectorStore(
    index_name="main.default.docs_vector_index"
)

# Create index
index = VectorStoreIndex.from_vector_store(
    vector_store=vector_store,
    service_context=ServiceContext.from_defaults(llm=llm)
)

# Create query engine
query_engine = index.as_query_engine(
    similarity_top_k=5
)

# Wrap as tool
query_tool = QueryEngineTool(
    query_engine=query_engine,
    metadata=ToolMetadata(
        name="documentation_search",
        description="Searches internal documentation for relevant information"
    )
)

# Create agent with tools
agent = OpenAIAgent.from_tools(
    tools=[query_tool],
    llm=llm,
    verbose=True
)

# Use agent
response = agent.chat("How do I configure Delta Lake table properties?")
print(response)
```

**Multi-Index Agent:**
```python
from llama_index.core import VectorStoreIndex
from llama_index.tools import QueryEngineTool

# Create multiple specialized indices
sales_index = VectorStoreIndex.from_vector_store(...)
product_index = VectorStoreIndex.from_vector_store(...)
customer_index = VectorStoreIndex.from_vector_store(...)

# Create query engines
sales_engine = sales_index.as_query_engine()
product_engine = product_index.as_query_engine()
customer_engine = customer_index.as_query_engine()

# Create tools
tools = [
    QueryEngineTool(
        query_engine=sales_engine,
        metadata=ToolMetadata(
            name="sales_search",
            description="Search sales data and reports"
        )
    ),
    QueryEngineTool(
        query_engine=product_engine,
        metadata=ToolMetadata(
            name="product_search",
            description="Search product catalog and specifications"
        )
    ),
    QueryEngineTool(
        query_engine=customer_engine,
        metadata=ToolMetadata(
            name="customer_search",
            description="Search customer information and history"
        )
    )
]

# Create router agent that picks the right tool
agent = OpenAIAgent.from_tools(tools=tools, llm=llm)

response = agent.chat("Find customers in Texas who bought Product X")
```

#### MLflow Integration

**Logging Agents with MLflow:**
```python
import mlflow
from mlflow.models import infer_signature

# Start MLflow run
with mlflow.start_run():
    # Create agent
    agent = create_databricks_tool_calling_agent(llm=llm, tools=tools)
    
    # Log parameters
    mlflow.log_param("llm_model", "llama-3-70b")
    mlflow.log_param("temperature", 0.1)
    mlflow.log_param("max_iterations", 10)
    mlflow.log_param("num_tools", len(tools))
    
    # Log agent
    mlflow.langchain.log_model(
        lc_model=agent,
        artifact_path="agent",
        registered_model_name="sales_assistant_agent"
    )
    
    # Test and log metrics
    test_questions = [
        "What were sales in Q1?",
        "Who are our top 10 customers?",
        "Show me revenue by region"
    ]
    
    for question in test_questions:
        result = agent_executor.invoke({"input": question})
        # Log metrics, traces, etc.
```

**Serving Agents:**
```python
# Load and serve
logged_agent = mlflow.langchain.load_model("runs:/run_id/agent")

# Create serving endpoint
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

endpoint = w.serving_endpoints.create(
    name="sales-assistant-agent",
    config={
        "served_models": [{
            "model_name": "sales_assistant_agent",
            "model_version": "1",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
)
```

---

### Module 5: Multi-Agent Systems (75 minutes)

**Lecture: Orchestration and Collaboration**

#### Why Multi-Agent Systems?

Single agents have limitations:
- Complexity in single prompts
- Lack of specialization
- Limited parallelization
- Difficult to maintain

Multi-agent systems offer:
- **Specialization**: Each agent has specific expertise
- **Decomposition**: Break complex tasks into subtasks
- **Parallel Execution**: Multiple agents work simultaneously
- **Modularity**: Easy to update individual agents

#### Multi-Agent Patterns

**1. Sequential Pattern**
```
User → Agent A → Agent B → Agent C → Response
```
Each agent processes output from previous agent.

**2. Hierarchical Pattern**
```
         Supervisor Agent
         /      |       \
     Agent A  Agent B  Agent C
```
Supervisor delegates tasks and synthesizes results.

**3. Collaborative Pattern**
```
     Agent A ←→ Agent B
         ↕          ↕
     Agent C ←→ Agent D
```
Agents communicate and collaborate peer-to-peer.

**4. Competitive Pattern**
```
     User Input
     /    |    \
  Agent A B    C
     \    |    /
      Evaluator
```
Multiple agents propose solutions, best one selected.

#### Implementing Multi-Agent with LangChain

**Hierarchical Multi-Agent System:**
```python
from langchain.agents import AgentExecutor, create_openai_functions_agent
from langchain_community.chat_models import ChatDatabricks
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from typing import TypedDict, Annotated, Sequence
import operator
from langgraph.graph import StateGraph, END

# Define state
class AgentState(TypedDict):
    messages: Annotated[Sequence[str], operator.add]
    next: str
    final_response: str

# Create specialized agents
def create_research_agent():
    """Agent specialized in information gathering"""
    llm = ChatDatabricks(endpoint="databricks-dbrx-instruct")
    
    tools = [
        # Vector search tool
        # Web search tool
        # Database query tool
    ]
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a research specialist. Your job is to gather 
        relevant information to answer questions. Use available tools to 
        find accurate, up-to-date information."""),
        MessagesPlaceholder(variable_name="messages"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])
    
    agent = create_openai_functions_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)

def create_analysis_agent():
    """Agent specialized in data analysis"""
    llm = ChatDatabricks(endpoint="databricks-dbrx-instruct")
    
    tools = [
        # SQL execution tool
        # Statistical analysis tool
        # Visualization tool
    ]
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a data analyst. Analyze data to extract 
        insights, identify patterns, and create summaries."""),
        MessagesPlaceholder(variable_name="messages"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])
    
    agent = create_openai_functions_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)

def create_writing_agent():
    """Agent specialized in content generation"""
    llm = ChatDatabricks(endpoint="databricks-dbrx-instruct")
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a professional writer. Create clear, 
        well-structured content based on research and analysis provided."""),
        MessagesPlaceholder(variable_name="messages"),
    ])
    
    agent = create_openai_functions_agent(llm, [], prompt)
    return AgentExecutor(agent=agent, tools=[], verbose=True)

# Create supervisor agent
def create_supervisor():
    """Supervisor that coordinates other agents"""
    llm = ChatDatabricks(endpoint="databricks-dbrx-instruct")
    
    system_prompt = """You are a supervisor managing three specialized agents:
    - research_agent: Gathers information
    - analysis_agent: Analyzes data
    - writing_agent: Creates final output
    
    Given the user request, determine which agent should act next.
    When the task is complete, respond with FINISH.
    """
    
    options = ["research_agent", "analysis_agent", "writing_agent", "FINISH"]
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        MessagesPlaceholder(variable_name="messages"),
        ("user", "Who should act next? Options: {options}")
    ]).partial(options=", ".join(options))
    
    chain = prompt | llm
    return chain

# Build workflow graph
workflow = StateGraph(AgentState)

# Add agents as nodes
research_agent = create_research_agent()
analysis_agent = create_analysis_agent()
writing_agent = create_writing_agent()
supervisor = create_supervisor()

# Define node functions
def research_node(state: AgentState):
    result = research_agent.invoke({"input": state["messages"][-1]})
    return {
        "messages": [result["output"]],
        "next": "supervisor"
    }

def analysis_node(state: AgentState):
    result = analysis_agent.invoke({"input": state["messages"][-1]})
    return {
        "messages": [result["output"]],
        "next": "supervisor"
    }

def writing_node(state: AgentState):
    result = writing_agent.invoke({"input": "\n\n".join(state["messages"])})
    return {
        "messages": [result["output"]],
        "final_response": result["output"],
        "next": "FINISH"
    }

def supervisor_node(state: AgentState):
    result = supervisor.invoke({"messages": state["messages"]})
    next_agent = result.content  # Parse which agent to call next
    return {"next": next_agent}

# Add nodes to graph
workflow.add_node("research_agent", research_node)
workflow.add_node("analysis_agent", analysis_node)
workflow.add_node("writing_agent", writing_node)
workflow.add_node("supervisor", supervisor_node)

# Add edges
workflow.add_edge("research_agent", "supervisor")
workflow.add_edge("analysis_agent", "supervisor")
workflow.add_edge("writing_agent", END)

# Conditional edges from supervisor
workflow.add_conditional_edges(
    "supervisor",
    lambda x: x["next"],
    {
        "research_agent": "research_agent",
        "analysis_agent": "analysis_agent",
        "writing_agent": "writing_agent",
        "FINISH": END
    }
)

# Set entry point
workflow.set_entry_point("supervisor")

# Compile graph
app = workflow.compile()

# Execute multi-agent workflow
result = app.invoke({
    "messages": ["Create a comprehensive report on Q4 2024 sales performance"],
    "next": "supervisor"
})

print(result["final_response"])
```

#### Collaborative Agents

**Peer-to-Peer Collaboration:**
```python
from langchain.memory import ConversationBufferMemory
from langchain.schema import HumanMessage, AIMessage

class CollaborativeAgentSystem:
    """System where agents collaborate as peers"""
    
    def __init__(self):
        self.agents = {
            "data_agent": self.create_data_agent(),
            "ml_agent": self.create_ml_agent(),
            "business_agent": self.create_business_agent()
        }
        self.shared_memory = ConversationBufferMemory()
    
    def create_data_agent(self):
        """Agent that handles data operations"""
        # Implementation...
        pass
    
    def create_ml_agent(self):
        """Agent that handles ML tasks"""
        # Implementation...
        pass
    
    def create_business_agent(self):
        """Agent that handles business logic"""
        # Implementation...
        pass
    
    async def collaborate(self, task: str, max_rounds: int = 5):
        """Agents collaborate to solve task"""
        
        self.shared_memory.chat_memory.add_user_message(task)
        
        for round in range(max_rounds):
            print(f"\n=== Round {round + 1} ===")
            
            # Each agent contributes
            for agent_name, agent in self.agents.items():
                print(f"\n{agent_name} thinking...")
                
                # Get current context
                context = self.shared_memory.load_memory_variables({})
                
                # Agent processes and contributes
                response = await agent.ainvoke({
                    "input": task,
                    "context": context
                })
                
                # Add to shared memory
                self.shared_memory.chat_memory.add_ai_message(
                    f"[{agent_name}]: {response['output']}"
                )
                
                # Check if task is complete
                if self.is_task_complete(response):
                    return response['output']
        
        # Synthesize final response
        return self.synthesize_responses()
    
    def is_task_complete(self, response: dict) -> bool:
        """Check if task is complete"""
        # Logic to determine completion
        pass
    
    def synthesize_responses(self) -> str:
        """Combine agent outputs into final response"""
        # Synthesis logic
        pass

# Usage
system = CollaborativeAgentSystem()
result = await system.collaborate(
    "Build a churn prediction model and deploy it"
)
```

#### Agent Communication Protocols

**Message Passing:**
```python
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

class MessageType(Enum):
    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"

@dataclass
class AgentMessage:
    """Standard message format between agents"""
    sender: str
    receiver: str
    message_type: MessageType
    content: Any
    metadata: Optional[dict] = None

class MessageBus:
    """Central message bus for agent communication"""
    
    def __init__(self):
        self.subscribers = {}
        self.message_history = []
    
    def subscribe(self, agent_name: str, handler):
        """Register agent to receive messages"""
        self.subscribers[agent_name] = handler
    
    async def send(self, message: AgentMessage):
        """Send message to target agent"""
        self.message_history.append(message)
        
        if message.receiver in self.subscribers:
            handler = self.subscribers[message.receiver]
            await handler(message)
        elif message.receiver == "broadcast":
            for handler in self.subscribers.values():
                await handler(message)
    
    def get_history(self, agent_name: Optional[str] = None):
        """Get message history"""
        if agent_name:
            return [m for m in self.message_history 
                   if m.sender == agent_name or m.receiver == agent_name]
        return self.message_history
```

---

### Module 6: RAG for Agents (60 minutes)

**Lecture: Knowledge Retrieval and Grounding**

#### RAG-Enhanced Agents

**Benefits:**
- Grounded in factual knowledge
- Access to proprietary data
- Reduced hallucinations
- Up-to-date information
- Explainable responses

**RAG Agent Architecture:**
```
User Query → Agent (LLM)
              ↓
         Decide if RAG needed
              ↓
    Vector Search (Databricks)
              ↓
         Retrieve Context
              ↓
    Agent generates response
    (grounded in retrieved docs)
```

#### Implementing RAG Agents

**Complete RAG Agent:**
```python
from databricks.vector_search.client import VectorSearchClient
from langchain.agents import create_databricks_tool_calling_agent
from langchain.tools import Tool
from langchain_community.chat_models import ChatDatabricks
from langchain.schema import Document

class RAGAgent:
    """Agent with RAG capabilities"""
    
    def __init__(
        self,
        vector_index_name: str,
        llm_endpoint: str = "databricks-meta-llama-3-70b-instruct"
    ):
        self.vector_search = VectorSearchClient()
        self.index = self.vector_search.get_index(
            index_name=vector_index_name
        )
        self.llm = ChatDatabricks(endpoint=llm_endpoint)
        
        # Create RAG tool
        self.rag_tool = self.create_rag_tool()
        
        # Create agent with RAG tool
        self.agent = create_databricks_tool_calling_agent(
            llm=self.llm,
            tools=[self.rag_tool]
        )
    
    def create_rag_tool(self) -> Tool:
        """Create tool for RAG retrieval"""
        
        def search_knowledge_base(query: str) -> str:
            """Search vector index for relevant information"""
            
            # Perform similarity search
            results = self.index.similarity_search(
                query_text=query,
                num_results=5
            )
            
            # Format results
            context = "\n\n".join([
                f"Document {i+1}:\n{result['text']}"
                for i, result in enumerate(results['data_array'])
            ])
            
            return context
        
        return Tool(
            name="search_knowledge_base",
            description="""Search the company knowledge base for relevant 
            information. Use this when you need factual information about 
            products, policies, procedures, or historical data.""",
            func=search_knowledge_base
        )
    
    def query(self, question: str) -> dict:
        """Query the RAG agent"""
        result = self.agent.invoke({"input": question})
        return result

# Usage
rag_agent = RAGAgent(
    vector_index_name="main.default.company_docs_index"
)

response = rag_agent.query(
    "What is our refund policy for enterprise customers?"
)
print(response["output"])
```

**Advanced RAG with Reranking:**
```python
from sentence_transformers import CrossEncoder

class AdvancedRAGAgent(RAGAgent):
    """RAG agent with reranking"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
    
    def search_and_rerank(self, query: str, num_results: int = 5) -> str:
        """Search and rerank results"""
        
        # Initial retrieval (get more candidates)
        initial_results = self.index.similarity_search(
            query_text=query,
            num_results=num_results * 3
        )
        
        # Prepare for reranking
        pairs = [[query, result['text']] 
                for result in initial_results['data_array']]
        
        # Rerank
        scores = self.reranker.predict(pairs)
        
        # Get top results after reranking
        ranked_indices = scores.argsort()[::-1][:num_results]
        
        top_results = [
            initial_results['data_array'][i] 
            for i in ranked_indices
        ]
        
        # Format context
        context = "\n\n".join([
            f"Document {i+1} (relevance: {scores[ranked_indices[i]]:.2f}):\n{result['text']}"
            for i, result in enumerate(top_results)
        ])
        
        return context
```

**Multi-Index RAG Agent:**
```python
class MultiIndexRAGAgent:
    """Agent with access to multiple knowledge bases"""
    
    def __init__(self):
        self.indices = {
            "products": "main.default.product_docs_index",
            "policies": "main.default.policy_docs_index",
            "technical": "main.default.technical_docs_index",
            "sales": "main.default.sales_data_index"
        }
        
        self.vector_search = VectorSearchClient()
        self.llm = ChatDatabricks(endpoint="databricks-meta-llama-3-70b-instruct")
        
        # Create tools for each index
        self.tools = [self.create_index_tool(name, index_name) 
                     for name, index_name in self.indices.items()]
        
        self.agent = create_databricks_tool_calling_agent(
            llm=self.llm,
            tools=self.tools
        )
    
    def create_index_tool(self, name: str, index_name: str) -> Tool:
        """Create tool for specific index"""
        
        def search_func(query: str) -> str:
            index = self.vector_search.get_index(index_name=index_name)
            results = index.similarity_search(
                query_text=query,
                num_results=3
            )
            return self.format_results(results)
        
        descriptions = {
            "products": "Search product documentation and specifications",
            "policies": "Search company policies and procedures",
            "technical": "Search technical documentation and guides",
            "sales": "Search sales data and customer information"
        }
        
        return Tool(
            name=f"search_{name}",
            description=descriptions[name],
            func=search_func
        )
    
    def format_results(self, results: dict) -> str:
        """Format search results"""
        return "\n\n".join([
            result['text'] 
            for result in results['data_array']
        ])
```

---

### Module 7: Genie Agents (45 minutes)

**Lecture: Conversational Analytics Agents**

#### What is Genie?

Databricks **Genie** is a conversational AI interface that allows users to:
- Ask questions in natural language
- Get SQL queries and results automatically
- Create visualizations through chat
- Build dashboards conversationally
- Access governed data through natural language

#### Creating Genie Spaces

**Genie Space Configuration:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import CreateGenieSpaceRequest

w = WorkspaceClient()

# Create Genie space
space = w.genie.create_space(
    display_name="Sales Analytics Genie",
    description="Natural language interface for sales data",
    warehouse_id="your_warehouse_id",
    catalog_name="main",
    schema_name="sales",
    instructions="""
    You are a sales analytics assistant. Help users analyze:
    - Sales performance by region, product, and time period
    - Customer behavior and trends
    - Revenue metrics and KPIs
    
    Available tables:
    - orders: Contains all customer orders
    - customers: Customer information
    - products: Product catalog
    - regions: Regional data
    
    Always provide clear explanations with your results.
    """
)

print(f"Created Genie space: {space.space_id}")
```

#### Enhancing Genie with Custom Instructions

**Best Practices for Instructions:**
```python
instructions = """
# Role and Purpose
You are an expert sales analyst helping business users understand sales data.

# Available Data
- orders table: order_id, customer_id, order_date, total_amount, region, status
- customers table: customer_id, name, state, segment, lifetime_value
- products table: product_id, name, category, price, cost

# Guidelines
1. Always show both SQL and results
2. Include relevant filters (date ranges, regions)
3. Format currency values appropriately
4. Provide context and interpretation
5. Suggest follow-up questions

# Common Queries
- Sales trends: "What were sales by month for the last year?"
- Top performers: "Who are the top 10 customers?"
- Regional analysis: "Compare sales across regions"
- Product analysis: "What are our best-selling products?"

# Response Format
1. Understanding: Restate the question
2. Query: Show SQL generated
3. Results: Present data (with visualization if applicable)
4. Insights: Highlight key findings
5. Follow-ups: Suggest related questions
"""
```

#### Genie API Integration

**Programmatic Genie Interaction:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import GenieMessageRequest

class GenieAgent:
    """Wrapper for Genie API"""
    
    def __init__(self, space_id: str):
        self.w = WorkspaceClient()
        self.space_id = space_id
    
    def ask(self, question: str) -> dict:
        """Ask Genie a question"""
        
        # Create conversation
        conversation = self.w.genie.create_conversation(
            space_id=self.space_id
        )
        
        # Send message
        message = self.w.genie.create_message(
            space_id=self.space_id,
            conversation_id=conversation.conversation_id,
            content=question
        )
        
        # Wait for response
        response = self.w.genie.wait_get_message_query_result(
            space_id=self.space_id,
            conversation_id=conversation.conversation_id,
            message_id=message.message_id
        )
        
        return {
            "sql": response.query.query,
            "results": response.query.result.data_array,
            "visualization": response.query.result.visualization
        }
    
    def get_conversation_history(self, conversation_id: str) -> list:
        """Get full conversation history"""
        
        messages = self.w.genie.list_messages(
            space_id=self.space_id,
            conversation_id=conversation_id
        )
        
        return list(messages)

# Usage
genie = GenieAgent(space_id="your_space_id")

result = genie.ask("What were our top 5 products by revenue last quarter?")

print(f"SQL Generated:\n{result['sql']}\n")
print(f"Results:\n{result['results']}")
```

#### Combining Genie with Custom Agents

**Hybrid Agent with Genie:**
```python
class HybridGenieAgent:
    """Agent that uses Genie for SQL and other tools for additional tasks"""
    
    def __init__(self, genie_space_id: str):
        self.genie = GenieAgent(space_id=genie_space_id)
        self.llm = ChatDatabricks(endpoint="databricks-dbrx-instruct")
        
        # Create tools including Genie
        self.tools = [
            self.create_genie_tool(),
            self.create_visualization_tool(),
            self.create_export_tool()
        ]
        
        self.agent = create_databricks_tool_calling_agent(
            llm=self.llm,
            tools=self.tools
        )
    
    def create_genie_tool(self) -> Tool:
        """Tool that leverages Genie for SQL generation"""
        
        def ask_genie(question: str) -> str:
            result = self.genie.ask(question)
            return f"SQL: {result['sql']}\n\nResults: {result['results']}"
        
        return Tool(
            name="genie_query",
            description="Use Genie to answer data questions with SQL",
            func=ask_genie
        )
    
    def create_visualization_tool(self) -> Tool:
        """Tool for creating visualizations"""
        # Implementation...
        pass
    
    def create_export_tool(self) -> Tool:
        """Tool for exporting data"""
        # Implementation...
        pass

# Usage
hybrid_agent = HybridGenieAgent(genie_space_id="your_space_id")

response = hybrid_agent.agent.invoke({
    "input": """Analyze Q4 sales performance and create a visualization 
    showing trends by region. Export the data to a CSV file."""
})
```

---

### Module 8: Agent Evaluation (60 minutes)

**Lecture: Testing, Benchmarking, and Quality Assurance**

#### Why Agent Evaluation?

Agents are complex systems that require rigorous evaluation:
- **Correctness**: Do agents produce accurate results?
- **Reliability**: Do they consistently perform well?
- **Safety**: Do they avoid harmful outputs?
- **Efficiency**: Do they use tools appropriately?
- **User Satisfaction**: Do they meet user needs?

#### Evaluation Dimensions

**1. Task Success Rate**
- Did the agent complete the task?
- Was the goal achieved?

**2. Tool Use Accuracy**
- Were appropriate tools selected?
- Were tools used correctly?
- Were tool calls necessary?

**3. Answer Quality**
- Factual accuracy
- Completeness
- Relevance
- Clarity

**4. Efficiency**
- Number of steps/iterations
- Tokens consumed
- Latency
- Cost

**5. Safety & Alignment**
- No harmful content
- Follows instructions
- Respects constraints
- Appropriate refusals

#### Building Evaluation Datasets

**Evaluation Dataset Structure:**
```python
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class EvaluationExample:
    """Single evaluation example"""
    question: str
    expected_tools: List[str]
    expected_answer_contains: List[str]
    ground_truth: Optional[str] = None
    category: str = "general"
    difficulty: str = "medium"

# Create evaluation dataset
eval_dataset = [
    EvaluationExample(
        question="What were total sales in California last month?",
        expected_tools=["calculate_revenue_by_region"],
        expected_answer_contains=["California", "total", "sales"],
        ground_truth="$1,234,567",
        category="sales_query",
        difficulty="easy"
    ),
    EvaluationExample(
        question="Compare customer lifetime value across segments and recommend retention strategies",
        expected_tools=["calculate_customer_ltv", "segment_analysis"],
        expected_answer_contains=["lifetime value", "segment", "retention", "recommendation"],
        category="analysis",
        difficulty="hard"
    ),
    # Add more examples...
]
```

#### Evaluation Metrics

**Automated Evaluation Framework:**
```python
import mlflow
from typing import Dict, List
from langchain.evaluation import load_evaluator

class AgentEvaluator:
    """Comprehensive agent evaluation framework"""
    
    def __init__(self, agent, eval_dataset: List[EvaluationExample]):
        self.agent = agent
        self.eval_dataset = eval_dataset
        
        # Initialize evaluators
        self.qa_evaluator = load_evaluator("qa")
        self.criteria_evaluator = load_evaluator("criteria", 
                                                 criteria="correctness")
    
    def evaluate(self) -> Dict:
        """Run full evaluation suite"""
        
        results = []
        
        with mlflow.start_run(run_name="agent_evaluation"):
            for example in self.eval_dataset:
                result = self.evaluate_single(example)
                results.append(result)
                
                # Log to MLflow
                mlflow.log_metrics({
                    f"{example.category}_success": result["success"],
                    f"{example.category}_tool_accuracy": result["tool_accuracy"],
                    f"{example.category}_latency": result["latency"]
                })
            
            # Calculate aggregate metrics
            metrics = self.calculate_aggregate_metrics(results)
            
            # Log aggregate metrics
            mlflow.log_metrics(metrics)
            
            # Log evaluation details
            mlflow.log_dict(results, "evaluation_results.json")
        
        return metrics
    
    def evaluate_single(self, example: EvaluationExample) -> Dict:
        """Evaluate single example"""
        
        import time
        start_time = time.time()
        
        # Run agent
        response = self.agent.invoke({"input": example.question})
        
        latency = time.time() - start_time
        
        # Extract information
        answer = response.get("output", "")
        tools_used = self.extract_tools_used(response)
        
        # Evaluate components
        eval_result = {
            "question": example.question,
            "answer": answer,
            "tools_used": tools_used,
            "latency": latency,
            
            # Task success
            "success": self.check_task_success(answer, example),
            
            # Tool accuracy
            "tool_accuracy": self.evaluate_tool_use(
                tools_used, 
                example.expected_tools
            ),
            
            # Answer quality
            "answer_quality": self.evaluate_answer_quality(
                answer,
                example
            ),
            
            # Factual accuracy (if ground truth available)
            "factual_accuracy": self.evaluate_factual_accuracy(
                answer,
                example.ground_truth
            ) if example.ground_truth else None,
            
            "category": example.category,
            "difficulty": example.difficulty
        }
        
        return eval_result
    
    def check_task_success(self, answer: str, example: EvaluationExample) -> bool:
        """Check if task was successfully completed"""
        
        # Check if expected content is present
        for expected in example.expected_answer_contains:
            if expected.lower() not in answer.lower():
                return False
        
        return True
    
    def evaluate_tool_use(self, tools_used: List[str], 
                         expected_tools: List[str]) -> float:
        """Evaluate tool selection and use"""
        
        # Calculate precision and recall of tool use
        tools_used_set = set(tools_used)
        expected_tools_set = set(expected_tools)
        
        if not expected_tools_set:
            return 1.0 if not tools_used_set else 0.5
        
        correct_tools = tools_used_set & expected_tools_set
        
        precision = len(correct_tools) / len(tools_used_set) if tools_used_set else 0
        recall = len(correct_tools) / len(expected_tools_set)
        
        # F1 score
        if precision + recall == 0:
            return 0.0
        
        f1 = 2 * (precision * recall) / (precision + recall)
        return f1
    
    def evaluate_answer_quality(self, answer: str, 
                                example: EvaluationExample) -> Dict:
        """Evaluate answer quality using LLM"""
        
        # Use LLM to evaluate quality
        result = self.criteria_evaluator.evaluate_strings(
            prediction=answer,
            input=example.question,
            reference=example.ground_truth
        )
        
        return {
            "score": result["score"],
            "reasoning": result["reasoning"]
        }
    
    def evaluate_factual_accuracy(self, answer: str, 
                                  ground_truth: str) -> float:
        """Evaluate factual accuracy against ground truth"""
        
        if not ground_truth:
            return None
        
        result = self.qa_evaluator.evaluate_strings(
            prediction=answer,
            reference=ground_truth
        )
        
        return result["score"]
    
    def extract_tools_used(self, response: Dict) -> List[str]:
        """Extract tools used from agent response"""
        
        # Parse intermediate steps to find tool calls
        tools = []
        if "intermediate_steps" in response:
            for step in response["intermediate_steps"]:
                if hasattr(step[0], 'tool'):
                    tools.append(step[0].tool)
        
        return tools
    
    def calculate_aggregate_metrics(self, results: List[Dict]) -> Dict:
        """Calculate aggregate metrics across all examples"""
        
        metrics = {
            "overall_success_rate": sum(r["success"] for r in results) / len(results),
            "avg_tool_accuracy": sum(r["tool_accuracy"] for r in results) / len(results),
            "avg_latency": sum(r["latency"] for r in results) / len(results),
            "total_evaluated": len(results)
        }
        
        # By category
        categories = set(r["category"] for r in results)
        for category in categories:
            cat_results = [r for r in results if r["category"] == category]
            metrics[f"{category}_success_rate"] = sum(r["success"] for r in cat_results) / len(cat_results)
        
        # By difficulty
        difficulties = set(r["difficulty"] for r in results)
        for difficulty in difficulties:
            diff_results = [r for r in results if r["difficulty"] == difficulty]
            metrics[f"{difficulty}_success_rate"] = sum(r["success"] for r in diff_results) / len(diff_results)
        
        return metrics

# Usage
evaluator = AgentEvaluator(agent=my_agent, eval_dataset=eval_dataset)
metrics = evaluator.evaluate()

print(f"Overall Success Rate: {metrics['overall_success_rate']:.2%}")
print(f"Avg Tool Accuracy: {metrics['avg_tool_accuracy']:.2%}")
print(f"Avg Latency: {metrics['avg_latency']:.2f}s")
```

#### MLflow Agent Evaluation

**Using MLflow's Built-in Evaluation:**
```python
import mlflow
import pandas as pd

# Prepare evaluation data
eval_data = pd.DataFrame([
    {
        "inputs": example.question,
        "ground_truth": example.ground_truth
    }
    for example in eval_dataset
])

# Define metrics
def tool_accuracy_metric(eval_df, builtin_metrics):
    """Custom metric for tool accuracy"""
    # Implementation...
    pass

# Evaluate with MLflow
with mlflow.start_run():
    results = mlflow.evaluate(
        model=agent_uri,
        data=eval_data,
        targets="ground_truth",
        model_type="question-answering",
        extra_metrics=[tool_accuracy_metric],
        evaluators="default"
    )
    
    print(results.metrics)
```

---

### Module 9: Production Deployment (90 minutes)

**Lecture: Serving, Monitoring, and Governance**

#### Production Requirements

**Scalability:**
- Handle concurrent requests
- Auto-scaling
- Load balancing

**Reliability:**
- High availability
- Error handling
- Fallback strategies

**Security:**
- Authentication & authorization
- Data encryption
- Audit logging

**Monitoring:**
- Performance metrics
- Quality metrics
- Cost tracking

**Governance:**
- Access control
- Compliance
- Lineage tracking

#### Deploying Agents with Model Serving

**Package Agent for Serving:**
```python
import mlflow
from mlflow.models import infer_signature
import cloudpickle

class ProductionAgent(mlflow.pyfunc.PythonModel):
    """Production-ready agent wrapper"""
    
    def load_context(self, context):
        """Load agent and dependencies"""
        
        # Load agent artifacts
        self.agent = cloudpickle.load(
            open(context.artifacts["agent"], "rb")
        )
        
        # Initialize monitoring
        self.request_count = 0
        self.error_count = 0
    
    def predict(self, context, model_input):
        """Handle prediction requests"""
        
        try:
            self.request_count += 1
            
            # Extract input
            if isinstance(model_input, pd.DataFrame):
                question = model_input["question"][0]
            else:
                question = model_input
            
            # Run agent
            result = self.agent.invoke({"input": question})
            
            # Format response
            response = {
                "answer": result["output"],
                "tools_used": self.extract_tools(result),
                "confidence": self.calculate_confidence(result),
                "request_id": str(uuid.uuid4())
            }
            
            # Log metrics
            self.log_metrics(response)
            
            return response
            
        except Exception as e:
            self.error_count += 1
            return {
                "error": str(e),
                "answer": "I encountered an error processing your request.",
                "request_id": str(uuid.uuid4())
            }
    
    def extract_tools(self, result):
        """Extract tools used"""
        # Implementation...
        pass
    
    def calculate_confidence(self, result):
        """Calculate confidence score"""
        # Implementation...
        pass
    
    def log_metrics(self, response):
        """Log metrics for monitoring"""
        # Implementation...
        pass

# Log model
with mlflow.start_run():
    # Save agent
    agent_path = "agent.pkl"
    cloudpickle.dump(agent, open(agent_path, "wb"))
    
    # Create signature
    signature = infer_signature(
        model_input={"question": "What were sales last month?"},
        model_output={"answer": "$1.2M", "confidence": 0.95}
    )
    
    # Log model
    mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=ProductionAgent(),
        artifacts={"agent": agent_path},
        signature=signature,
        registered_model_name="production_sales_agent"
    )
```

**Deploy to Model Serving:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

# Create serving endpoint
endpoint = w.serving_endpoints.create(
    name="sales-agent-endpoint",
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name="production_sales_agent",
                entity_version="1",
                workload_size="Small",
                scale_to_zero_enabled=True
            )
        ]
    )
)

print(f"Endpoint created: {endpoint.name}")
print(f"URL: {endpoint.endpoint_url}")
```

**Invoke Production Agent:**
```python
import requests
import os

def query_agent(question: str) -> dict:
    """Query production agent endpoint"""
    
    url = f"{endpoint_url}/invocations"
    
    headers = {
        "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "inputs": {
            "question": question
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Request failed: {response.text}")

# Usage
result = query_agent("What were our top products last quarter?")
print(result["answer"])
```

#### Monitoring Agents in Production

**Comprehensive Monitoring:**
```python
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient

class AgentMonitor:
    """Monitor agent performance and quality"""
    
    def __init__(self, endpoint_name: str):
        self.endpoint_name = endpoint_name
        self.w = WorkspaceClient()
        
        # Create monitoring tables
        self.setup_monitoring_tables()
    
    def setup_monitoring_tables(self):
        """Create tables for monitoring data"""
        
        spark.sql("""
            CREATE TABLE IF NOT EXISTS agent_requests (
                request_id STRING,
                endpoint_name STRING,
                timestamp TIMESTAMP,
                question STRING,
                answer STRING,
                tools_used ARRAY<STRING>,
                latency_ms DOUBLE,
                tokens_used INT,
                cost DOUBLE,
                success BOOLEAN,
                error STRING
            ) USING DELTA
            PARTITIONED BY (DATE(timestamp))
        """)
        
        spark.sql("""
            CREATE TABLE IF NOT EXISTS agent_metrics (
                endpoint_name STRING,
                timestamp TIMESTAMP,
                metric_name STRING,
                metric_value DOUBLE
            ) USING DELTA
            PARTITIONED BY (DATE(timestamp))
        """)
    
    def log_request(self, request_data: dict):
        """Log individual request"""
        
        spark.createDataFrame([request_data]) \
            .write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("agent_requests")
    
    def calculate_metrics(self, window_hours: int = 1) -> dict:
        """Calculate aggregate metrics"""
        
        metrics = spark.sql(f"""
            SELECT 
                COUNT(*) as total_requests,
                SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests,
                AVG(latency_ms) as avg_latency_ms,
                PERCENTILE(latency_ms, 0.95) as p95_latency_ms,
                SUM(tokens_used) as total_tokens,
                SUM(cost) as total_cost
            FROM agent_requests
            WHERE endpoint_name = '{self.endpoint_name}'
              AND timestamp >= NOW() - INTERVAL {window_hours} HOURS
        """).collect()[0]
        
        return metrics.asDict()
    
    def get_quality_metrics(self) -> dict:
        """Calculate quality metrics"""
        
        # Tool usage distribution
        tool_usage = spark.sql(f"""
            SELECT 
                tool,
                COUNT(*) as usage_count
            FROM agent_requests
            LATERAL VIEW explode(tools_used) as tool
            WHERE endpoint_name = '{self.endpoint_name}'
              AND timestamp >= CURRENT_DATE
            GROUP BY tool
            ORDER BY usage_count DESC
        """).collect()
        
        # Success rate by question type (could be classified)
        # Average response quality (could use LLM evaluation)
        
        return {
            "tool_usage": tool_usage,
            # Additional metrics...
        }
    
    def create_monitoring_dashboard(self):
        """SQL queries for monitoring dashboard"""
        
        queries = {
            "request_volume": """
                SELECT 
                    DATE_TRUNC('hour', timestamp) as hour,
                    COUNT(*) as requests
                FROM agent_requests
                WHERE endpoint_name = '{endpoint}'
                  AND timestamp >= NOW() - INTERVAL 24 HOURS
                GROUP BY hour
                ORDER BY hour
            """,
            
            "success_rate": """
                SELECT 
                    DATE_TRUNC('hour', timestamp) as hour,
                    AVG(CASE WHEN success THEN 1.0 ELSE 0.0 END) * 100 as success_rate
                FROM agent_requests
                WHERE endpoint_name = '{endpoint}'
                  AND timestamp >= NOW() - INTERVAL 24 HOURS
                GROUP BY hour
                ORDER BY hour
            """,
            
            "latency_percentiles": """
                SELECT 
                    PERCENTILE(latency_ms, 0.50) as p50,
                    PERCENTILE(latency_ms, 0.90) as p90,
                    PERCENTILE(latency_ms, 0.95) as p95,
                    PERCENTILE(latency_ms, 0.99) as p99
                FROM agent_requests
                WHERE endpoint_name = '{endpoint}'
                  AND timestamp >= NOW() - INTERVAL 1 HOUR
            """,
            
            "cost_tracking": """
                SELECT 
                    DATE(timestamp) as date,
                    SUM(cost) as daily_cost,
                    SUM(tokens_used) as daily_tokens
                FROM agent_requests
                WHERE endpoint_name = '{endpoint}'
                  AND timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
                GROUP BY date
                ORDER BY date
            """
        }
        
        return queries

# Usage
monitor = AgentMonitor(endpoint_name="sales-agent-endpoint")

# Get current metrics
metrics = monitor.calculate_metrics(window_hours=1)
print(f"Success Rate: {metrics['successful_requests'] / metrics['total_requests']:.2%}")
print(f"Avg Latency: {metrics['avg_latency_ms']:.0f}ms")
print(f"P95 Latency: {metrics['p95_latency_ms']:.0f}ms")
```

#### Governance and Security

**Access Control:**
```python
from databricks.sdk.service.catalog import PrivilegeAssignment, Privilege

# Grant access to UC functions (tools)
w.grants.update(
    full_name="catalog.schema.get_customer_data",
    updates=[
        PrivilegeAssignment(
            principal="agent-service-principal",
            privileges=[Privilege.EXECUTE]
        )
    ]
)

# Grant access to serving endpoint
w.serving_endpoints.update_permissions(
    serving_endpoint_id="sales-agent-endpoint",
    access_control_list=[
        {
            "user_name": "data-science-team",
            "permission_level": "CAN_QUERY"
        }
    ]
)
```

**Audit Logging:**
```python
def log_audit_event(event_type: str, details: dict):
    """Log audit events for compliance"""
    
    audit_record = {
        "timestamp": datetime.now(),
        "event_type": event_type,
        "user": current_user(),
        "endpoint": endpoint_name,
        "details": details,
        "ip_address": request.remote_addr
    }
    
    spark.createDataFrame([audit_record]) \
        .write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("audit_logs")
```

---

## Best Practices

### Agent Design Principles

✅ **Clear Scope and Purpose**
- Define specific agent capabilities
- Set clear boundaries
- Document intended use cases

✅ **Tool Design**
- Single responsibility per tool
- Clear, descriptive names
- Comprehensive documentation
- Idempotent when possible
- Proper error handling

✅ **Prompt Engineering**
- Clear instructions and examples
- Explicit reasoning steps
- Output format specifications
- Constraint definitions

✅ **Error Handling**
- Graceful degradation
- Meaningful error messages
- Retry logic for transient failures
- Fallback strategies

✅ **Testing**
- Unit tests for tools
- Integration tests for workflows
- Evaluation datasets
- Continuous evaluation

### Security Best Practices

✅ **Principle of Least Privilege**
- Minimal necessary permissions
- Fine-grained access control
- Regular permission audits

✅ **Input Validation**
- Sanitize user inputs
- Validate tool parameters
- Prevent injection attacks

✅ **Output Filtering**
- Check for sensitive data
- Apply content filters
- Redact PII when needed

✅ **Authentication & Authorization**
- Strong authentication
- Token management
- Session handling
- Audit logging

### Performance Optimization

✅ **Efficient Tool Use**
- Minimize unnecessary tool calls
- Batch operations when possible
- Cache expensive operations
- Parallel execution

✅ **Prompt Optimization**
- Concise prompts
- Relevant context only
- Structured outputs
- Token management

✅ **Resource Management**
- Connection pooling
- Timeouts
- Rate limiting
- Cost monitoring

---

## Getting Started

### Recommended Learning Path

**Week 1: Foundations**
1. Complete Module 1 (Agentic AI Foundations)
2. Work through Module 2 (UC Functions)
3. Build basic tools (Lab 1)

**Week 2: Core Skills**
4. Complete Module 3 (MCP)
5. Study Module 4 (Agent Frameworks)
6. Build single agent (Labs 2-4)

**Week 3: Advanced Capabilities**
7. Complete Module 5 (Multi-Agent)
8. Study Module 6 (RAG)
9. Build complex workflows (Labs 5-6)

**Week 4: Production**
10. Complete Modules 7-9
11. Build evaluation suite (Lab 7)
12. Deploy to production (Lab 8)

### Quick Start

```python
# 1. Create a simple UC function
spark.sql("""
    CREATE FUNCTION main.default.get_weather(city STRING)
    RETURNS STRING
    RETURN SELECT 'Sunny, 72°F'
""")

# 2. Create agent with LangChain
from langchain_community.chat_models import ChatDatabricks
from langchain_community.tools.databricks import UCFunctionToolkit
from langchain.agents import create_databricks_tool_calling_agent, AgentExecutor

llm = ChatDatabricks(endpoint="databricks-dbrx-instruct")
toolkit = UCFunctionToolkit(warehouse_id="...", catalog="main", schema="default")
tools = toolkit.get_tools()

agent = create_databricks_tool_calling_agent(llm=llm, tools=tools)
agent_executor = AgentExecutor(agent=agent, tools=tools)

# 3. Test your agent
result = agent_executor.invoke({"input": "What's the weather in San Francisco?"})
print(result["output"])
```

---

## Additional Resources

### Documentation
- [Databricks AI Functions](https://docs.databricks.com/ai-functions/)
- [Unity Catalog Functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-udf.html)
- [Model Serving](https://docs.databricks.com/machine-learning/model-serving/index.html)
- [LangChain Databricks](https://python.langchain.com/docs/integrations/platforms/databricks)
- [MLflow](https://mlflow.org/docs/latest/index.html)

### Tools & Libraries
- **LangChain**: Agent framework
- **LlamaIndex**: RAG and indexing
- **MLflow**: Experiment tracking and serving
- **MCP**: Model Context Protocol

### Community
- Databricks Community Forums
- GitHub Discussions
- Stack Overflow

---

## Support

For questions or issues:
- Open an issue in this repository
- Contact: Ajit Kalura
- Databricks Community Forums

---

**Ready to build production agentic AI systems?** Start with Module 1! 🚀
