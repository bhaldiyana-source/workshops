# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Agent Frameworks
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture explores popular agent frameworks - LangChain, LlamaIndex, and Databricks native capabilities. You'll learn how these frameworks simplify agent development, enable RAG, and integrate with MLflow for production deployment.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand major agent frameworks and their strengths
# MAGIC - Use LangChain to build tool-calling agents
# MAGIC - Implement RAG agents with LlamaIndex
# MAGIC - Integrate agents with MLflow for tracking and serving
# MAGIC - Choose the right framework for your use case
# MAGIC
# MAGIC ## Duration
# MAGIC 60 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed previous lectures (Foundations, UC Functions, MCP)
# MAGIC - Python programming knowledge
# MAGIC - Understanding of LLMs and APIs
# MAGIC - Basic MLflow familiarity (helpful but not required)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Agent Framework Landscape
# MAGIC
# MAGIC Overview of the major frameworks for building AI agents.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Framework Comparison
# MAGIC
# MAGIC | Framework | Best For | Strengths | Databricks Integration |
# MAGIC |-----------|----------|-----------|------------------------|
# MAGIC | **LangChain** | General-purpose agents | • Rich ecosystem<br>• Many integrations<br>• Active community<br>• Agent patterns | ✅ Native support<br>✅ UC Functions toolkit<br>✅ Model Serving ready |
# MAGIC | **LlamaIndex** | RAG and data indexing | • Specialized in RAG<br>• Query engines<br>• Data connectors<br>• Index management | ✅ Vector Search support<br>✅ Delta tables<br>✅ Agent patterns |
# MAGIC | **Databricks Native** | Platform integration | • Native UC support<br>• MLflow integration<br>• Optimized performance<br>• Governance | ✅ Built-in<br>✅ AI Playground<br>✅ Model Serving |
# MAGIC
# MAGIC ### 1.2 When to Use Each Framework
# MAGIC
# MAGIC **LangChain:**
# MAGIC - Building conversational agents with tools
# MAGIC - Complex multi-step workflows
# MAGIC - Need for extensive integrations
# MAGIC - Flexible agent patterns (ReAct, Plan-and-Execute, etc.)
# MAGIC
# MAGIC **LlamaIndex:**
# MAGIC - RAG-focused applications
# MAGIC - Document Q&A systems
# MAGIC - Knowledge base search
# MAGIC - Multiple data source integration
# MAGIC
# MAGIC **Databricks Native:**
# MAGIC - Simple function calling scenarios
# MAGIC - Direct UC function integration
# MAGIC - Maximum governance requirements
# MAGIC - AI Playground prototyping

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. LangChain on Databricks
# MAGIC
# MAGIC LangChain is the most popular framework for building agent applications.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Core LangChain Concepts
# MAGIC
# MAGIC #### Components
# MAGIC
# MAGIC **1. LLMs / Chat Models**
# MAGIC - Interface to language models
# MAGIC - Databricks endpoints supported
# MAGIC - Standardized API
# MAGIC
# MAGIC **2. Prompts**
# MAGIC - Template management
# MAGIC - Dynamic prompt construction
# MAGIC - Few-shot examples
# MAGIC
# MAGIC **3. Tools**
# MAGIC - External capabilities
# MAGIC - UC Functions as tools
# MAGIC - Custom tool creation
# MAGIC
# MAGIC **4. Agents**
# MAGIC - Orchestration logic
# MAGIC - Multiple agent types
# MAGIC - Tool selection and execution
# MAGIC
# MAGIC **5. Memory**
# MAGIC - Conversation history
# MAGIC - Long-term storage
# MAGIC - Context management
# MAGIC
# MAGIC **6. Chains**
# MAGIC - Sequence operations
# MAGIC - Composable workflows
# MAGIC - Reusable patterns

# COMMAND ----------

# Example: LangChain basics (conceptual code)
# Note: This demonstrates the pattern. Actual execution requires LangChain installation.

langchain_example = """
# Basic LangChain setup on Databricks

from langchain_community.chat_models import ChatDatabricks
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_community.tools.databricks import UCFunctionToolkit

# 1. Initialize LLM with Databricks endpoint
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-70b-instruct",
    temperature=0.1,
    max_tokens=1000
)

# 2. Load UC Functions as tools
toolkit = UCFunctionToolkit(
    warehouse_id="your_warehouse_id",
    catalog="main",
    schema="agents"
)
tools = toolkit.get_tools()

# 3. Create prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful AI assistant with access to tools."),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

# 4. Create agent
agent = create_tool_calling_agent(llm, tools, prompt)

# 5. Create executor
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    max_iterations=10,
    handle_parsing_errors=True
)

# 6. Run agent
response = agent_executor.invoke({
    "input": "What were our sales in Q4 2024?"
})

print(response["output"])
"""

print("LangChain on Databricks - Basic Pattern")
print("=" * 60)
print(langchain_example)
print("\n✓ This pattern integrates LLM, tools, and agent logic")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Creating Custom Tools in LangChain

# COMMAND ----------

# Example: Custom LangChain tool
from typing import Optional, Type
from pydantic import BaseModel, Field

# Simulated LangChain imports (would be real in production)
class BaseTool:
    """Mock BaseTool for demonstration"""
    name: str
    description: str
    args_schema: Type[BaseModel]
    
    def _run(self, **kwargs):
        raise NotImplementedError

# Define input schema
class CustomerSearchInput(BaseModel):
    """Input schema for customer search tool"""
    name: str = Field(description="Customer name to search for (partial match supported)")
    state: Optional[str] = Field(default=None, description="Optional state filter (e.g., 'CA', 'NY')")
    min_ltv: Optional[float] = Field(default=0.0, description="Minimum lifetime value filter")

class CustomerSearchTool(BaseTool):
    """Tool for searching customers in the database"""
    
    name = "search_customers"
    description = """
    Search for customers by name with optional filters.
    
    Use this tool when you need to:
    - Find customers by name (partial match)
    - Filter customers by state
    - Find high-value customers (by lifetime value)
    
    Returns customer details including ID, name, email, state, and lifetime value.
    """
    args_schema: Type[BaseModel] = CustomerSearchInput
    
    def _run(
        self, 
        name: str, 
        state: Optional[str] = None,
        min_ltv: Optional[float] = 0.0
    ) -> str:
        """
        Execute customer search
        
        Args:
            name: Customer name to search
            state: Optional state filter
            min_ltv: Minimum lifetime value
            
        Returns:
            Formatted string with customer details
        """
        # In production, this would query actual database
        # Using Databricks SDK or Spark SQL
        
        # Simulated results
        results = [
            {
                "customer_id": 12345,
                "name": "John Smith",
                "email": "john.smith@example.com",
                "state": "CA",
                "lifetime_value": 25000.00
            },
            {
                "customer_id": 67890,
                "name": "Jane Smith",
                "email": "jane.smith@example.com",
                "state": "NY",
                "lifetime_value": 32000.00
            }
        ]
        
        # Apply filters
        filtered = [
            r for r in results 
            if name.lower() in r["name"].lower() 
            and (not state or r["state"] == state)
            and r["lifetime_value"] >= min_ltv
        ]
        
        if not filtered:
            return "No customers found matching criteria"
        
        # Format results
        output = f"Found {len(filtered)} customer(s):\n\n"
        for customer in filtered:
            output += f"ID: {customer['customer_id']}\n"
            output += f"Name: {customer['name']}\n"
            output += f"Email: {customer['email']}\n"
            output += f"State: {customer['state']}\n"
            output += f"Lifetime Value: ${customer['lifetime_value']:,.2f}\n"
            output += "-" * 40 + "\n"
        
        return output
    
    async def _arun(self, name: str, state: Optional[str] = None, min_ltv: Optional[float] = 0.0) -> str:
        """Async version (not implemented in this example)"""
        raise NotImplementedError("Async not implemented")

# Demonstrate the tool
print("Custom LangChain Tool Example")
print("=" * 60)

tool = CustomerSearchTool()

print(f"Tool Name: {tool.name}")
print(f"Description: {tool.description.strip()}")
print("\nInput Schema:")
for field_name, field in CustomerSearchInput.__fields__.items():
    print(f"  - {field_name}: {field.annotation} - {field.field_info.description}")

print("\n\nTest Execution:")
print("-" * 60)
result = tool._run(name="Smith", state="CA", min_ltv=20000)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 LangChain Agent Types
# MAGIC
# MAGIC #### ReAct Agent
# MAGIC - Most common agent type
# MAGIC - Reasoning + Acting loop
# MAGIC - Best for tool-using tasks
# MAGIC
# MAGIC ```python
# MAGIC from langchain.agents import create_react_agent, AgentExecutor
# MAGIC
# MAGIC agent = create_react_agent(llm, tools, prompt)
# MAGIC executor = AgentExecutor(agent=agent, tools=tools)
# MAGIC ```
# MAGIC
# MAGIC #### OpenAI Functions Agent
# MAGIC - Uses function calling API
# MAGIC - More structured tool use
# MAGIC - Better for complex schemas
# MAGIC
# MAGIC ```python
# MAGIC from langchain.agents import create_openai_functions_agent
# MAGIC
# MAGIC agent = create_openai_functions_agent(llm, tools, prompt)
# MAGIC ```
# MAGIC
# MAGIC #### Structured Chat Agent
# MAGIC - Handles structured input/output
# MAGIC - Good for complex conversations
# MAGIC - Memory integration
# MAGIC
# MAGIC ```python
# MAGIC from langchain.agents import create_structured_chat_agent
# MAGIC
# MAGIC agent = create_structured_chat_agent(llm, tools, prompt)
# MAGIC ```

# COMMAND ----------

# Example: Simulated agent execution trace
def simulate_agent_trace():
    """Simulate what an agent execution looks like"""
    
    trace = """
Agent Execution Trace
================================================================================

Input: "Find high-value customers in California and calculate their total LTV"

Step 1: Reasoning
-----------------
Thought: I need to search for customers in California and then calculate totals.
Action: search_customers
Action Input: {"name": "", "state": "CA", "min_ltv": 50000}

Step 2: Tool Execution
----------------------
Tool: search_customers
Result: Found 15 customers in CA with LTV > $50k

Step 3: Reasoning
-----------------
Thought: Now I need to calculate the total lifetime value for these customers.
Action: calculate_total_ltv
Action Input: {"customer_ids": [12345, 67890, ...]}

Step 4: Tool Execution
----------------------
Tool: calculate_total_ltv
Result: Total LTV: $2,450,000

Step 5: Final Answer
--------------------
Thought: I have all the information needed to answer the question.
Final Answer: I found 15 high-value customers in California with a combined 
lifetime value of $2,450,000. The average LTV per customer is $163,333.

================================================================================
Execution completed in 3.2 seconds
Tools used: 2 (search_customers, calculate_total_ltv)
"""
    return trace

print(simulate_agent_trace())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. LlamaIndex on Databricks
# MAGIC
# MAGIC LlamaIndex specializes in RAG (Retrieval-Augmented Generation) applications.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 LlamaIndex Core Concepts
# MAGIC
# MAGIC #### Documents and Nodes
# MAGIC - Documents: Source data (PDFs, text files, etc.)
# MAGIC - Nodes: Chunked pieces of documents
# MAGIC - Metadata: Additional context per node
# MAGIC
# MAGIC #### Indexes
# MAGIC - VectorStoreIndex: Semantic search
# MAGIC - TreeIndex: Hierarchical summaries
# MAGIC - KeywordTableIndex: Keyword matching
# MAGIC - ListIndex: Sequential processing
# MAGIC
# MAGIC #### Query Engines
# MAGIC - Interface for querying indexes
# MAGIC - Different retrieval strategies
# MAGIC - Response synthesis
# MAGIC
# MAGIC #### Agents
# MAGIC - Query engines as tools
# MAGIC - Multi-index routing
# MAGIC - Complex reasoning

# COMMAND ----------

# Example: LlamaIndex RAG pattern (conceptual)
llamaindex_example = """
# LlamaIndex RAG Agent on Databricks

from llama_index.core import VectorStoreIndex, ServiceContext, Document
from llama_index.llms.databricks import Databricks
from llama_index.vector_stores.databricks import DatabricksVectorStore
from llama_index.agent import OpenAIAgent
from llama_index.tools import QueryEngineTool, ToolMetadata

# 1. Initialize LLM
llm = Databricks(
    model="databricks-meta-llama-3-70b-instruct",
    api_key=databricks_token
)

# 2. Connect to Databricks Vector Search
vector_store = DatabricksVectorStore(
    index_name="main.default.docs_vector_index"
)

# 3. Create index from vector store
index = VectorStoreIndex.from_vector_store(
    vector_store=vector_store,
    service_context=ServiceContext.from_defaults(llm=llm)
)

# 4. Create query engine
query_engine = index.as_query_engine(
    similarity_top_k=5,
    response_mode="compact"
)

# 5. Wrap query engine as tool
query_tool = QueryEngineTool(
    query_engine=query_engine,
    metadata=ToolMetadata(
        name="documentation_search",
        description="Search internal documentation for relevant information"
    )
)

# 6. Create agent with RAG tool
agent = OpenAIAgent.from_tools(
    tools=[query_tool],
    llm=llm,
    verbose=True
)

# 7. Use agent
response = agent.chat("How do I configure Delta Lake table properties?")
print(response)
"""

print("LlamaIndex RAG Agent Pattern")
print("=" * 60)
print(llamaindex_example)
print("\n✓ This pattern enables agents to query knowledge bases")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Multi-Index RAG Agent

# COMMAND ----------

# Example: Multi-index agent pattern
multi_index_pattern = """
# Multi-Index RAG Agent Pattern

from llama_index.core import VectorStoreIndex
from llama_index.tools import QueryEngineTool, ToolMetadata

# Create specialized indices for different data sources
sales_index = VectorStoreIndex.from_vector_store(
    DatabricksVectorStore(index_name="main.default.sales_docs")
)

product_index = VectorStoreIndex.from_vector_store(
    DatabricksVectorStore(index_name="main.default.product_docs")
)

support_index = VectorStoreIndex.from_vector_store(
    DatabricksVectorStore(index_name="main.default.support_docs")
)

# Create query engines
sales_engine = sales_index.as_query_engine()
product_engine = product_index.as_query_engine()
support_engine = support_index.as_query_engine()

# Wrap as tools with clear descriptions
tools = [
    QueryEngineTool(
        query_engine=sales_engine,
        metadata=ToolMetadata(
            name="sales_search",
            description="Search sales data, reports, and performance metrics"
        )
    ),
    QueryEngineTool(
        query_engine=product_engine,
        metadata=ToolMetadata(
            name="product_search",
            description="Search product catalog, specifications, and documentation"
        )
    ),
    QueryEngineTool(
        query_engine=support_engine,
        metadata=ToolMetadata(
            name="support_search",
            description="Search customer support tickets, FAQs, and solutions"
        )
    )
]

# Create agent that routes to appropriate index
agent = OpenAIAgent.from_tools(tools=tools, llm=llm)

# Agent automatically selects the right index
response = agent.chat("What are the specifications for Product X?")
# Uses product_search tool

response = agent.chat("How many tickets did we resolve last month?")
# Uses support_search tool
"""

print("Multi-Index RAG Agent Pattern")
print("=" * 60)
print(multi_index_pattern)
print("\n✓ Agent automatically routes queries to appropriate knowledge base")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 RAG Best Practices
# MAGIC
# MAGIC #### Chunking Strategy
# MAGIC - **Size**: 512-1024 tokens per chunk
# MAGIC - **Overlap**: 50-100 tokens between chunks
# MAGIC - **Boundaries**: Respect document structure (paragraphs, sections)
# MAGIC
# MAGIC #### Retrieval Configuration
# MAGIC - **Top-K**: Start with 3-5 results
# MAGIC - **Similarity Threshold**: Filter low-relevance results
# MAGIC - **Reranking**: Use cross-encoder for better quality
# MAGIC
# MAGIC #### Response Synthesis
# MAGIC - **Compact**: Concatenate context efficiently
# MAGIC - **Refine**: Iteratively improve answer
# MAGIC - **Tree Summarize**: Hierarchical synthesis
# MAGIC
# MAGIC #### Metadata Usage
# MAGIC - **Filtering**: Pre-filter by metadata before vector search
# MAGIC - **Citation**: Track source documents
# MAGIC - **Freshness**: Include timestamps for recency

# COMMAND ----------

# Example: RAG configuration
rag_config = {
    "chunking": {
        "chunk_size": 512,
        "chunk_overlap": 50,
        "separator": "\n\n"
    },
    "retrieval": {
        "similarity_top_k": 5,
        "similarity_threshold": 0.7,
        "rerank": True,
        "rerank_top_n": 3
    },
    "synthesis": {
        "response_mode": "compact",
        "streaming": False,
        "use_async": True
    },
    "metadata_filters": {
        "document_type": ["policy", "procedure"],
        "department": "sales",
        "date_range": {"start": "2024-01-01", "end": "2024-12-31"}
    }
}

print("RAG Configuration Example")
print("=" * 60)
import json
print(json.dumps(rag_config, indent=2))
print("\n✓ Well-configured RAG improves answer quality and relevance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MLflow Integration
# MAGIC
# MAGIC MLflow enables tracking, versioning, and serving of agents.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Logging Agents with MLflow

# COMMAND ----------

# Example: Log agent with MLflow
mlflow_logging_example = """
import mlflow
from mlflow.models import infer_signature

# Start MLflow run
with mlflow.start_run(run_name="sales_assistant_agent"):
    
    # 1. Log parameters
    mlflow.log_param("llm_model", "llama-3-70b")
    mlflow.log_param("temperature", 0.1)
    mlflow.log_param("max_iterations", 10)
    mlflow.log_param("num_tools", len(tools))
    mlflow.log_param("agent_type", "react")
    
    # 2. Log agent configuration
    config = {
        "llm_endpoint": "databricks-meta-llama-3-70b-instruct",
        "tools": [tool.name for tool in tools],
        "system_prompt": prompt.messages[0].content
    }
    mlflow.log_dict(config, "agent_config.json")
    
    # 3. Log the agent model
    mlflow.langchain.log_model(
        lc_model=agent_executor,
        artifact_path="agent",
        registered_model_name="sales_assistant_agent",
        input_example={"input": "What were sales in Q4?"},
        signature=infer_signature(
            model_input={"input": "test question"},
            model_output={"output": "test response"}
        )
    )
    
    # 4. Log evaluation metrics
    test_questions = [
        "What were sales in Q4?",
        "Who are our top customers?",
        "Show revenue by region"
    ]
    
    for i, question in enumerate(test_questions):
        result = agent_executor.invoke({"input": question})
        mlflow.log_metric(f"test_{i}_latency", result.get("latency", 0))
        mlflow.log_metric(f"test_{i}_tool_calls", len(result.get("intermediate_steps", [])))
    
    # 5. Log artifacts (optional)
    # mlflow.log_artifact("evaluation_results.json")
    # mlflow.log_artifact("test_traces.txt")
    
    print(f"Agent logged to MLflow run: {mlflow.active_run().info.run_id}")
"""

print("Logging Agents with MLflow")
print("=" * 60)
print(mlflow_logging_example)
print("\n✓ MLflow tracks agents like any other ML model")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Serving Agents with Model Serving

# COMMAND ----------

# Example: Deploy agent to Model Serving
serving_example = """
# Deploy Agent to Databricks Model Serving

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput
)

w = WorkspaceClient()

# Create serving endpoint
endpoint = w.serving_endpoints.create(
    name="sales-assistant-agent",
    config=EndpointCoreConfigInput(
        served_entities=[
            ServedEntityInput(
                entity_name="sales_assistant_agent",
                entity_version="1",  # Or use "latest"
                workload_size="Small",
                scale_to_zero_enabled=True
            )
        ]
    )
)

print(f"Endpoint created: {endpoint.name}")
print(f"Status: {endpoint.state.config_update}")
print(f"URL: {endpoint.endpoint_url}")

# Wait for endpoint to be ready
import time
while True:
    status = w.serving_endpoints.get(name="sales-assistant-agent")
    if status.state.ready == "READY":
        break
    print("Waiting for endpoint...")
    time.sleep(30)

print("✓ Endpoint is ready!")

# Query the endpoint
import requests
import os

response = requests.post(
    f"{endpoint.endpoint_url}/invocations",
    headers={
        "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
        "Content-Type": "application/json"
    },
    json={
        "inputs": {
            "input": "What were our top products last quarter?"
        }
    }
)

result = response.json()
print(f"Agent response: {result}")
"""

print("Serving Agents with Model Serving")
print("=" * 60)
print(serving_example)
print("\n✓ Agents can be deployed as scalable REST endpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Agent Versioning and A/B Testing

# COMMAND ----------

# Example: Version management
versioning_example = """
# Agent Version Management

# Register multiple versions
with mlflow.start_run(run_name="agent_v1"):
    mlflow.langchain.log_model(
        agent_v1, 
        "agent",
        registered_model_name="sales_agent"
    )
    # Version 1 registered

with mlflow.start_run(run_name="agent_v2_improved"):
    mlflow.langchain.log_model(
        agent_v2,
        "agent", 
        registered_model_name="sales_agent"
    )
    # Version 2 registered

# Serve both versions for A/B testing
endpoint_config = EndpointCoreConfigInput(
    served_entities=[
        ServedEntityInput(
            entity_name="sales_agent",
            entity_version="1",
            workload_size="Small",
            scale_to_zero_enabled=True,
            environment_vars={
                "AGENT_VERSION": "v1"
            }
        ),
        ServedEntityInput(
            entity_name="sales_agent",
            entity_version="2",
            workload_size="Small",
            scale_to_zero_enabled=True,
            environment_vars={
                "AGENT_VERSION": "v2"
            }
        )
    ],
    # Route 80% traffic to v1, 20% to v2
    traffic_config={
        "routes": [
            {"served_model_name": "sales_agent-1", "traffic_percentage": 80},
            {"served_model_name": "sales_agent-2", "traffic_percentage": 20}
        ]
    }
)

# Create or update endpoint
w.serving_endpoints.create_or_update(
    name="sales-agent-ab-test",
    config=endpoint_config
)

print("✓ A/B test deployed: 80% v1, 20% v2")
"""

print("Agent Versioning and A/B Testing")
print("=" * 60)
print(versioning_example)
print("\n✓ Test new agent versions safely in production")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Framework Selection Guide
# MAGIC
# MAGIC Choosing the right framework for your use case.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Decision Matrix
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │              FRAMEWORK SELECTION GUIDE                      │
# MAGIC ├─────────────────────────────────────────────────────────────┤
# MAGIC │                                                             │
# MAGIC │  Your Requirements              → Recommended Framework    │
# MAGIC │  ───────────────────────────────────────────────────────   │
# MAGIC │                                                             │
# MAGIC │  Tool-calling agents            → LangChain               │
# MAGIC │  • Multiple tools                                           │
# MAGIC │  • Complex workflows                                        │
# MAGIC │  • Flexible patterns                                        │
# MAGIC │                                                             │
# MAGIC │  RAG applications               → LlamaIndex               │
# MAGIC │  • Document Q&A                                             │
# MAGIC │  • Knowledge search                                         │
# MAGIC │  • Multiple data sources                                    │
# MAGIC │                                                             │
# MAGIC │  Simple function calling        → Databricks Native        │
# MAGIC │  • UC Functions only                                        │
# MAGIC │  • AI Playground testing                                    │
# MAGIC │  • Maximum governance                                       │
# MAGIC │                                                             │
# MAGIC │  Hybrid (Tools + RAG)           → LangChain + LlamaIndex   │
# MAGIC │  • RAG as one of many tools                                 │
# MAGIC │  • Complex agent with knowledge                             │
# MAGIC │  • Best of both worlds                                      │
# MAGIC │                                                             │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Use Case Examples
# MAGIC
# MAGIC **Use Case 1: Customer Support Agent**
# MAGIC - **Framework**: LangChain
# MAGIC - **Why**: Needs multiple tools (search tickets, update records, send emails)
# MAGIC - **Tools**: UC Functions for database ops, external API for email
# MAGIC
# MAGIC **Use Case 2: Research Assistant**
# MAGIC - **Framework**: LlamaIndex
# MAGIC - **Why**: Primarily document search and synthesis
# MAGIC - **Data**: Multiple document indices (papers, reports, articles)
# MAGIC
# MAGIC **Use Case 3: Data Analyst Agent**
# MAGIC - **Framework**: LangChain with UC Functions
# MAGIC - **Why**: SQL generation and execution
# MAGIC - **Tools**: UC Functions for queries, visualization, metrics
# MAGIC
# MAGIC **Use Case 4: Comprehensive Business Agent**
# MAGIC - **Framework**: LangChain + LlamaIndex
# MAGIC - **Why**: Needs both tools and knowledge retrieval
# MAGIC - **Capabilities**: Query data, search documents, send reports

# COMMAND ----------

# Example: Framework selection helper
def recommend_framework(requirements: dict) -> dict:
    """
    Recommend framework based on requirements
    
    Args:
        requirements: Dict with keys:
            - needs_tools: bool
            - needs_rag: bool
            - num_tools: int
            - complexity: str (simple, medium, complex)
            - governance_critical: bool
    
    Returns:
        Dict with recommendation and rationale
    """
    needs_tools = requirements.get("needs_tools", False)
    needs_rag = requirements.get("needs_rag", False)
    num_tools = requirements.get("num_tools", 0)
    complexity = requirements.get("complexity", "medium")
    governance_critical = requirements.get("governance_critical", False)
    
    # Decision logic
    if governance_critical and not needs_rag and num_tools <= 3:
        return {
            "framework": "Databricks Native",
            "rationale": "Simple requirements with maximum governance needs",
            "alternatives": ["LangChain for more flexibility"]
        }
    
    elif needs_rag and not needs_tools:
        return {
            "framework": "LlamaIndex",
            "rationale": "RAG-focused application without complex tool requirements",
            "alternatives": ["LangChain for future tool addition"]
        }
    
    elif needs_tools and needs_rag:
        return {
            "framework": "LangChain + LlamaIndex",
            "rationale": "Hybrid system with both tool calling and RAG capabilities",
            "alternatives": ["LangChain with RAG tools"]
        }
    
    elif needs_tools and num_tools > 3 or complexity == "complex":
        return {
            "framework": "LangChain",
            "rationale": "Complex tool orchestration requirements",
            "alternatives": ["Databricks Native for simpler subset"]
        }
    
    else:
        return {
            "framework": "LangChain",
            "rationale": "General-purpose choice with good flexibility",
            "alternatives": ["LlamaIndex if RAG becomes primary focus"]
        }

# Test recommendations
test_cases = [
    {
        "name": "Customer Support",
        "requirements": {
            "needs_tools": True,
            "needs_rag": True,
            "num_tools": 5,
            "complexity": "medium",
            "governance_critical": False
        }
    },
    {
        "name": "Simple Query Agent",
        "requirements": {
            "needs_tools": True,
            "needs_rag": False,
            "num_tools": 2,
            "complexity": "simple",
            "governance_critical": True
        }
    },
    {
        "name": "Research Assistant",
        "requirements": {
            "needs_tools": False,
            "needs_rag": True,
            "num_tools": 0,
            "complexity": "medium",
            "governance_critical": False
        }
    }
]

print("Framework Recommendation Examples")
print("=" * 60)

for case in test_cases:
    print(f"\nUse Case: {case['name']}")
    print("-" * 60)
    recommendation = recommend_framework(case['requirements'])
    print(f"Recommended: {recommendation['framework']}")
    print(f"Rationale: {recommendation['rationale']}")
    print(f"Alternatives: {', '.join(recommendation['alternatives'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC ✅ **LangChain is best for general-purpose agents**
# MAGIC - Rich ecosystem and integrations
# MAGIC - Multiple agent patterns (ReAct, etc.)
# MAGIC - Custom tool creation
# MAGIC - Production-ready with MLflow
# MAGIC
# MAGIC ✅ **LlamaIndex excels at RAG applications**
# MAGIC - Document indexing and retrieval
# MAGIC - Multiple query engines
# MAGIC - Vector Search integration
# MAGIC - Can be combined with LangChain
# MAGIC
# MAGIC ✅ **Databricks Native provides simple integration**
# MAGIC - Direct UC Function access
# MAGIC - AI Playground testing
# MAGIC - Maximum governance
# MAGIC - Good for prototyping
# MAGIC
# MAGIC ✅ **MLflow enables production deployment**
# MAGIC - Track agent experiments
# MAGIC - Version management
# MAGIC - Model Serving deployment
# MAGIC - A/B testing support
# MAGIC
# MAGIC ✅ **Framework choice depends on requirements**
# MAGIC - Tools vs RAG vs Hybrid
# MAGIC - Complexity level
# MAGIC - Governance needs
# MAGIC - Future extensibility

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Next Steps
# MAGIC
# MAGIC You now understand the major agent frameworks!
# MAGIC
# MAGIC **Continue to:**
# MAGIC - **04_Lecture_Multi_Agent_Systems**: Learn about agent collaboration
# MAGIC - **11_Demo_Single_Agent_with_LangChain**: Build your first LangChain agent
# MAGIC - **13_Demo_RAG_Enhanced_Agent**: Create a RAG agent with LlamaIndex
# MAGIC
# MAGIC **Practice by:**
# MAGIC 1. Building a simple LangChain agent with 2-3 UC Functions
# MAGIC 2. Creating a LlamaIndex RAG agent with Vector Search
# MAGIC 3. Logging and serving an agent with MLflow
# MAGIC
# MAGIC **Additional Resources:**
# MAGIC - [LangChain Documentation](https://python.langchain.com/)
# MAGIC - [LlamaIndex Documentation](https://docs.llamaindex.ai/)
# MAGIC - [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
# MAGIC - [Databricks GenAI Cookbook](https://ai-cookbook.io/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this lecture, we covered:
# MAGIC
# MAGIC - ✅ Overview of agent frameworks (LangChain, LlamaIndex, Databricks)
# MAGIC - ✅ Building agents with LangChain and UC Functions
# MAGIC - ✅ Creating RAG agents with LlamaIndex
# MAGIC - ✅ MLflow integration for tracking and serving
# MAGIC - ✅ Framework selection guide
# MAGIC
# MAGIC You're now ready to build production agents with the right framework for your needs!
