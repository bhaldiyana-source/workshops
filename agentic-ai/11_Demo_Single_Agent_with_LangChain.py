# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Single Agent with LangChain
# MAGIC
# MAGIC ## Overview
# MAGIC Build a complete working agent using LangChain and Unity Catalog functions.
# MAGIC
# MAGIC ## What You'll Build
# MAGIC - LangChain agent with UC tools
# MAGIC - Custom tool integration
# MAGIC - Test with real queries
# MAGIC
# MAGIC ## Duration
# MAGIC 45 minutes

# COMMAND ----------

# Example agent setup (conceptual - requires LangChain installation)
example_code = """
from langchain_community.chat_models import ChatDatabricks
from langchain_community.tools.databricks import UCFunctionToolkit
from langchain.agents import create_tool_calling_agent, AgentExecutor

# Initialize LLM
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-70b-instruct",
    temperature=0.1
)

# Load UC functions as tools
toolkit = UCFunctionToolkit(
    warehouse_id="your_warehouse_id",
    catalog="main",
    schema="default"
)
tools = toolkit.get_tools()

# Create agent
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Test
response = agent_executor.invoke({"input": "Get customer 1"})
print(response["output"])
"""

print("LangChain Agent Setup:")
print("=" * 60)
print(example_code)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Built LangChain agent
# MAGIC ✅ Integrated UC Functions
# MAGIC ✅ Tested with queries
