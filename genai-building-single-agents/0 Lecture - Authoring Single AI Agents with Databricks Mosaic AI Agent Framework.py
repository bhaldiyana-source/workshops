# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Authoring Single AI Agents with Databricks Mosaic AI Agent Framework
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the Databricks Mosaic AI Agent Framework and explores how to build single AI agents using popular frameworks like LangChain and DSPy. We'll examine the separation of concerns between tools, models, and agentic frameworks, and understand when to use each approach for building intelligent, tool-enabled agents.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand the architecture of the Mosaic AI Agent Framework
# MAGIC - Explain the separation between tools, models, and agentic frameworks
# MAGIC - Differentiate between imperative (LangChain) and declarative (DSPy) approaches
# MAGIC - Identify when to use LangChain vs DSPy for agent development
# MAGIC - Understand MLflow integration for agent tracing and observability
# MAGIC - Recognize key patterns in agent reasoning and execution
# MAGIC
# MAGIC ## Duration
# MAGIC 20-25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is the Mosaic AI Agent Framework?
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC The **Databricks Mosaic AI Agent Framework** is a comprehensive platform for building, deploying, and managing AI agents that can:
# MAGIC - **Reason** about complex tasks using large language models
# MAGIC - **Act** by calling tools and functions to accomplish goals
# MAGIC - **Integrate** with Unity Catalog for governed data and tool access
# MAGIC - **Scale** using Databricks compute and serving infrastructure
# MAGIC - **Trace** execution with MLflow for debugging and optimization
# MAGIC
# MAGIC ### Key Components
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │         Mosaic AI Agent Framework                    │
# MAGIC ├─────────────────────────────────────────────────────┤
# MAGIC │                                                      │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
# MAGIC │  │   Agentic    │  │    Model     │  │   Tools   │ │
# MAGIC │  │  Frameworks  │  │   Serving    │  │    (UC    │ │
# MAGIC │  │ (LangChain,  │  │  (Foundation │  │ Functions)│ │
# MAGIC │  │    DSPy)     │  │   Models)    │  │           │ │
# MAGIC │  └──────────────┘  └──────────────┘  └───────────┘ │
# MAGIC │         │                  │                 │      │
# MAGIC │         └──────────────────┴─────────────────┘      │
# MAGIC │                          │                          │
# MAGIC │                   ┌──────▼──────┐                  │
# MAGIC │                   │   MLflow    │                  │
# MAGIC │                   │   Tracing   │                  │
# MAGIC │                   └─────────────┘                  │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separation of Concerns: Tools, Models, and Frameworks
# MAGIC
# MAGIC A well-architected agent system separates three distinct concerns:
# MAGIC
# MAGIC ### 1. Tools (What the Agent Can Do)
# MAGIC
# MAGIC **Definition:** Functions that agents invoke to interact with data and systems
# MAGIC
# MAGIC **Implemented as:**
# MAGIC - Unity Catalog SQL Functions (for data queries)
# MAGIC - Unity Catalog Python Functions (for complex logic)
# MAGIC - External API wrappers (for third-party services)
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Stateless and deterministic
# MAGIC - Well-documented with clear parameters
# MAGIC - Governed by Unity Catalog permissions
# MAGIC - Reusable across multiple agents
# MAGIC
# MAGIC **Examples:**
# MAGIC - `get_customer_orders(customer_id: str) -> DataFrame`
# MAGIC - `check_inventory(product_id: str) -> int`
# MAGIC - `calculate_shipping_cost(weight: float, destination: str) -> float`
# MAGIC
# MAGIC ### 2. Models (How the Agent Thinks)
# MAGIC
# MAGIC **Definition:** Large language models that provide reasoning capabilities
# MAGIC
# MAGIC **Implemented as:**
# MAGIC - Databricks Foundation Model APIs (DBRX, Llama, Mixtral)
# MAGIC - External model endpoints (OpenAI, Anthropic)
# MAGIC - Custom fine-tuned models on Model Serving
# MAGIC
# MAGIC **Characteristics:**
# MAGIC - Understand natural language instructions
# MAGIC - Reason about which tools to use
# MAGIC - Generate natural language responses
# MAGIC - Support tool/function calling protocols
# MAGIC
# MAGIC **Key Considerations:**
# MAGIC - Model size vs. latency tradeoffs
# MAGIC - Context window limitations
# MAGIC - Tool calling capabilities
# MAGIC - Cost per token
# MAGIC
# MAGIC ### 3. Agentic Frameworks (How the Agent Orchestrates)
# MAGIC
# MAGIC **Definition:** Libraries that coordinate model reasoning with tool execution
# MAGIC
# MAGIC **Popular Frameworks:**
# MAGIC - **LangChain:** Imperative, chain-based orchestration
# MAGIC - **DSPy:** Declarative, program-based orchestration
# MAGIC - **Mosaic AI Agent Framework:** Native Databricks solution
# MAGIC
# MAGIC **Responsibilities:**
# MAGIC - Parse tool schemas and descriptions
# MAGIC - Format tool information for the model
# MAGIC - Execute tool calls based on model output
# MAGIC - Handle errors and retries
# MAGIC - Maintain conversation history
# MAGIC - Log traces for observability

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Reasoning Patterns
# MAGIC
# MAGIC ### The ReAct Pattern
# MAGIC
# MAGIC The most common agent pattern is **ReAct** (Reasoning + Acting):
# MAGIC
# MAGIC ```
# MAGIC User Query: "What is the total value of orders for customer C001?"
# MAGIC
# MAGIC Step 1: REASON
# MAGIC   "I need to find orders for customer C001 and sum their values.
# MAGIC    I have a tool called get_customer_orders that can help."
# MAGIC
# MAGIC Step 2: ACT
# MAGIC   Call: get_customer_orders(customer_id="C001")
# MAGIC   Result: [
# MAGIC     {"order_id": "O001", "amount": 150.00},
# MAGIC     {"order_id": "O002", "amount": 200.00},
# MAGIC     {"order_id": "O003", "amount": 175.00}
# MAGIC   ]
# MAGIC
# MAGIC Step 3: REASON
# MAGIC   "I have the orders. Now I need to calculate the total.
# MAGIC    I can sum these values directly."
# MAGIC
# MAGIC Step 4: ACT (Final Response)
# MAGIC   "The total value of orders for customer C001 is $525.00 
# MAGIC    across 3 orders."
# MAGIC ```
# MAGIC
# MAGIC ### Other Common Patterns
# MAGIC
# MAGIC **Chain of Thought (CoT)**
# MAGIC - Break down complex reasoning into steps
# MAGIC - No tool calling required
# MAGIC - Good for mathematical or logical problems
# MAGIC
# MAGIC **Plan and Execute**
# MAGIC - First, create a plan of actions
# MAGIC - Then, execute each step sequentially
# MAGIC - Good for multi-step workflows
# MAGIC
# MAGIC **Reflexion**
# MAGIC - Execute action, observe result
# MAGIC - Reflect on outcome and adjust approach
# MAGIC - Good for iterative problem-solving

# COMMAND ----------

# MAGIC %md
# MAGIC ## LangChain: Imperative Approach
# MAGIC
# MAGIC ### What is LangChain?
# MAGIC
# MAGIC **LangChain** is a popular open-source framework for building LLM applications using an imperative, chain-based approach.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC **Chains**
# MAGIC - Sequential steps of processing
# MAGIC - Explicit control flow
# MAGIC - Composable building blocks
# MAGIC
# MAGIC **Agents**
# MAGIC - Use LLMs to decide which tools to call
# MAGIC - Support various agent types (ReAct, Plan-and-Execute, etc.)
# MAGIC - Built-in retry and error handling
# MAGIC
# MAGIC **Tools**
# MAGIC - Python functions with schemas
# MAGIC - Can be Unity Catalog functions via `UCFunctionToolkit`
# MAGIC - Easy to create custom tools
# MAGIC
# MAGIC ### When to Use LangChain
# MAGIC
# MAGIC ✅ **Good for:**
# MAGIC - Rapid prototyping and experimentation
# MAGIC - Complex chains with conditional logic
# MAGIC - Rich ecosystem of pre-built integrations
# MAGIC - When you need explicit control over execution flow
# MAGIC - Production applications with mature tooling
# MAGIC
# MAGIC ⚠️ **Consider alternatives when:**
# MAGIC - You need optimized prompts through experimentation
# MAGIC - You want to minimize prompt engineering
# MAGIC - You prefer declarative over imperative code
# MAGIC
# MAGIC ### Architecture Example
# MAGIC
# MAGIC ```python
# MAGIC from langchain.agents import AgentExecutor, create_react_agent
# MAGIC from databricks.agents import UCFunctionToolkit
# MAGIC
# MAGIC # 1. Load tools from Unity Catalog
# MAGIC toolkit = UCFunctionToolkit(
# MAGIC     warehouse_id="...",
# MAGIC     function_names=["main.agent_tools.get_customer_orders"]
# MAGIC )
# MAGIC
# MAGIC # 2. Create agent with explicit configuration
# MAGIC agent = create_react_agent(
# MAGIC     llm=llm,
# MAGIC     tools=toolkit.tools,
# MAGIC     prompt=react_prompt
# MAGIC )
# MAGIC
# MAGIC # 3. Execute with explicit error handling
# MAGIC executor = AgentExecutor(
# MAGIC     agent=agent,
# MAGIC     tools=toolkit.tools,
# MAGIC     max_iterations=5,
# MAGIC     handle_parsing_errors=True
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## DSPy: Declarative Approach
# MAGIC
# MAGIC ### What is DSPy?
# MAGIC
# MAGIC **DSPy (Declarative Self-improving Python)** is a framework that treats LLM applications as programs that can be optimized, rather than prompts to be engineered.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC **Signatures**
# MAGIC - Declare inputs and outputs
# MAGIC - Framework generates prompts automatically
# MAGIC - Type-safe and composable
# MAGIC
# MAGIC **Modules**
# MAGIC - `dspy.Predict`: Basic prediction
# MAGIC - `dspy.ChainOfThought`: Add reasoning
# MAGIC - `dspy.ReAct`: Reasoning + tool calling
# MAGIC
# MAGIC **Optimizers**
# MAGIC - Automatically improve prompts
# MAGIC - Learn from examples
# MAGIC - Bootstrap demonstrations
# MAGIC
# MAGIC ### When to Use DSPy
# MAGIC
# MAGIC ✅ **Good for:**
# MAGIC - Systematic prompt optimization
# MAGIC - Type-safe agent programs
# MAGIC - When you want minimal prompt engineering
# MAGIC - Research and experimentation with agent architectures
# MAGIC - Building modular, composable agent components
# MAGIC
# MAGIC ⚠️ **Consider alternatives when:**
# MAGIC - You need battle-tested production tooling
# MAGIC - You want a large ecosystem of integrations
# MAGIC - You prefer explicit prompt control
# MAGIC
# MAGIC ### Architecture Example
# MAGIC
# MAGIC ```python
# MAGIC import dspy
# MAGIC
# MAGIC # 1. Configure model connection
# MAGIC lm = dspy.Databricks(model="databricks-meta-llama-3-1-70b-instruct")
# MAGIC dspy.configure(lm=lm)
# MAGIC
# MAGIC # 2. Define signature (declarative)
# MAGIC class AnswerQuestion(dspy.Signature):
# MAGIC     """Answer questions using available tools."""
# MAGIC     question = dspy.InputField()
# MAGIC     answer = dspy.OutputField()
# MAGIC
# MAGIC # 3. Define tools
# MAGIC def get_orders(customer_id: str) -> str:
# MAGIC     # Implementation
# MAGIC     pass
# MAGIC
# MAGIC # 4. Create agent (framework generates prompts)
# MAGIC agent = dspy.ReAct(
# MAGIC     signature=AnswerQuestion,
# MAGIC     tools=[get_orders]
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## LangChain vs DSPy: Comparison
# MAGIC
# MAGIC | Aspect | LangChain | DSPy |
# MAGIC |--------|-----------|------|
# MAGIC | **Philosophy** | Imperative chains | Declarative programs |
# MAGIC | **Prompt Engineering** | Manual, explicit prompts | Automatic prompt generation |
# MAGIC | **Control Flow** | Explicit chains and agents | Implicit through signatures |
# MAGIC | **Optimization** | Manual tuning | Automatic optimizers |
# MAGIC | **Learning Curve** | Moderate | Steeper initially |
# MAGIC | **Ecosystem** | Very mature, many integrations | Growing, research-focused |
# MAGIC | **Best For** | Production apps, rapid prototyping | Research, systematic optimization |
# MAGIC | **Databricks Integration** | UCFunctionToolkit, Model Serving | Model Serving, custom tools |
# MAGIC | **Tracing** | MLflow auto-tracing | MLflow integration available |
# MAGIC
# MAGIC ### Decision Framework
# MAGIC
# MAGIC **Choose LangChain if you:**
# MAGIC - Need production-ready tooling now
# MAGIC - Want explicit control over prompts and chains
# MAGIC - Have complex conditional logic
# MAGIC - Need rich ecosystem integrations
# MAGIC
# MAGIC **Choose DSPy if you:**
# MAGIC - Want to optimize prompts systematically
# MAGIC - Prefer declarative, type-safe code
# MAGIC - Are building research prototypes
# MAGIC - Want to minimize manual prompt engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Integration for Agent Tracing
# MAGIC
# MAGIC ### Why Trace Agents?
# MAGIC
# MAGIC Agents are complex systems with multiple steps. Tracing helps you:
# MAGIC - **Debug**: See exactly what the agent did and why
# MAGIC - **Optimize**: Identify slow or inefficient steps
# MAGIC - **Monitor**: Track agent behavior in production
# MAGIC - **Improve**: Analyze failures to enhance prompts or tools
# MAGIC
# MAGIC ### What MLflow Captures
# MAGIC
# MAGIC For each agent execution, MLflow traces:
# MAGIC
# MAGIC **Input Information**
# MAGIC - User query
# MAGIC - Available tools
# MAGIC - Agent configuration
# MAGIC
# MAGIC **Execution Steps**
# MAGIC - Model reasoning (thoughts)
# MAGIC - Tool selection decisions
# MAGIC - Tool calls and parameters
# MAGIC - Tool results
# MAGIC - Number of iterations
# MAGIC
# MAGIC **Output Information**
# MAGIC - Final answer
# MAGIC - Total tokens used
# MAGIC - Execution time
# MAGIC - Success/failure status
# MAGIC
# MAGIC ### Enabling Auto-Tracing
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC
# MAGIC # Enable auto-tracing for LangChain
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC # Your agent code here
# MAGIC result = agent.invoke({"input": "What are the orders for C001?"})
# MAGIC
# MAGIC # Trace is automatically logged to MLflow
# MAGIC ```
# MAGIC
# MAGIC ### Viewing Traces in MLflow UI
# MAGIC
# MAGIC 1. Navigate to the MLflow Experiments page
# MAGIC 2. Find your experiment run
# MAGIC 3. Click on "Traces" tab
# MAGIC 4. Explore the tree view of execution steps
# MAGIC 5. Examine inputs, outputs, and timing for each step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Execution Flow
# MAGIC
# MAGIC Let's walk through a complete agent execution:
# MAGIC
# MAGIC ### Example Scenario
# MAGIC
# MAGIC **User Query:** "What's the total value of orders for customer C001 in 2025?"
# MAGIC
# MAGIC **Available Tools:**
# MAGIC - `get_customer_orders(customer_id: str) -> DataFrame`
# MAGIC - `calculate_total(values: List[float]) -> float`
# MAGIC
# MAGIC ### Execution Steps
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ 1. User Input                                        │
# MAGIC │    "What's the total for customer C001 in 2025?"    │
# MAGIC └────────────────┬────────────────────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ 2. Agent Reasoning (Model)                          │
# MAGIC │    "I need to get orders for C001, filter by        │
# MAGIC │     2025, and calculate the total. I'll use         │
# MAGIC │     get_customer_orders first."                     │
# MAGIC └────────────────┬────────────────────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ 3. Tool Call                                        │
# MAGIC │    get_customer_orders(customer_id="C001")         │
# MAGIC └────────────────┬────────────────────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ 4. Tool Result                                      │
# MAGIC │    [{"order_id": "O003", "date": "2025-01-05",     │
# MAGIC │      "amount": 175.00}, ...]                       │
# MAGIC └────────────────┬────────────────────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ 5. Agent Reasoning (Model)                          │
# MAGIC │    "I have the orders. I need to filter for 2025   │
# MAGIC │     and sum the amounts. I can do this in my       │
# MAGIC │     final response."                                │
# MAGIC └────────────────┬────────────────────────────────────┘
# MAGIC                  │
# MAGIC                  ▼
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │ 6. Final Response                                   │
# MAGIC │    "The total value of orders for customer C001    │
# MAGIC │     in 2025 is $175.00 (1 order)."                 │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Building Single Agents
# MAGIC
# MAGIC ### Tool Design
# MAGIC
# MAGIC ✅ **Do:**
# MAGIC - Write clear, descriptive function names
# MAGIC - Provide detailed docstrings explaining what the tool does
# MAGIC - Use type hints for all parameters
# MAGIC - Make tools focused on a single responsibility
# MAGIC - Handle errors gracefully and return meaningful messages
# MAGIC
# MAGIC ❌ **Don't:**
# MAGIC - Create overly complex, multi-purpose tools
# MAGIC - Use vague parameter names
# MAGIC - Return raw error messages or stack traces
# MAGIC - Require the agent to know internal implementation details
# MAGIC
# MAGIC ### Model Selection
# MAGIC
# MAGIC **For Simple Tasks:**
# MAGIC - Smaller models (7B-13B parameters)
# MAGIC - Faster response times
# MAGIC - Lower cost
# MAGIC
# MAGIC **For Complex Reasoning:**
# MAGIC - Larger models (70B+ parameters)
# MAGIC - Better tool selection accuracy
# MAGIC - More coherent multi-step reasoning
# MAGIC
# MAGIC ### Agent Configuration
# MAGIC
# MAGIC **Max Iterations:**
# MAGIC - Start with 3-5 for simple tasks
# MAGIC - Increase to 10+ for complex workflows
# MAGIC - Set limits to prevent runaway execution
# MAGIC
# MAGIC **Temperature:**
# MAGIC - Lower (0.0-0.3) for deterministic tool calling
# MAGIC - Higher (0.5-0.8) for creative responses
# MAGIC
# MAGIC **Error Handling:**
# MAGIC - Always configure error handling
# MAGIC - Provide fallback responses
# MAGIC - Log failures for analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Challenges and Solutions
# MAGIC
# MAGIC ### Challenge 1: Agent Makes Wrong Tool Choices
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Calls incorrect tools for the task
# MAGIC - Ignores available relevant tools
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Improve tool descriptions with examples
# MAGIC - Use a more capable model
# MAGIC - Reduce number of available tools
# MAGIC - Add few-shot examples to the prompt
# MAGIC
# MAGIC ### Challenge 2: Agent Gets Stuck in Loops
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Repeatedly calls same tool
# MAGIC - Hits max iteration limit
# MAGIC - No progress toward solution
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Set appropriate max_iterations
# MAGIC - Improve tool output clarity
# MAGIC - Add conversation memory/history
# MAGIC - Use early stopping conditions
# MAGIC
# MAGIC ### Challenge 3: Slow Response Times
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Long wait for agent responses
# MAGIC - Multiple unnecessary tool calls
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Use smaller/faster models
# MAGIC - Optimize tool execution (caching, async)
# MAGIC - Reduce available tool count
# MAGIC - Use streaming responses
# MAGIC
# MAGIC ### Challenge 4: Inconsistent Results
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Different answers for same query
# MAGIC - Unpredictable tool usage
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Lower temperature for more deterministic behavior
# MAGIC - Add more structure to prompts
# MAGIC - Use DSPy optimizers for consistency
# MAGIC - Add validation layers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Separation of Concerns**: Tools, models, and frameworks have distinct roles
# MAGIC 2. **Framework Choice**: LangChain for production, DSPy for optimization
# MAGIC 3. **ReAct Pattern**: Most common agent reasoning approach
# MAGIC 4. **MLflow Tracing**: Essential for debugging and optimization
# MAGIC 5. **Tool Quality**: Clear descriptions and focused functionality are crucial
# MAGIC
# MAGIC ### What's Next?
# MAGIC
# MAGIC In the following demos and labs, you'll:
# MAGIC
# MAGIC **Demo 1: LangChain Agents**
# MAGIC - Build a complete agent with Unity Catalog tools
# MAGIC - Configure Model Serving endpoints
# MAGIC - Trace execution with MLflow
# MAGIC
# MAGIC **Demo 2: DSPy Agents**
# MAGIC - Create declarative agent programs
# MAGIC - Use automatic prompt generation
# MAGIC - Compare approaches with LangChain
# MAGIC
# MAGIC **Lab: Build Your Own Agent**
# MAGIC - Hands-on practice with real scenarios
# MAGIC - Create custom tools and agents
# MAGIC - Debug and optimize performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Databricks Mosaic AI Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/index.html)
# MAGIC - [LangChain Documentation](https://python.langchain.com/docs/get_started/introduction)
# MAGIC - [DSPy Documentation](https://dspy-docs.vercel.app/)
# MAGIC - [Unity Catalog Functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-udf.html)
# MAGIC
# MAGIC ### Examples and Tutorials
# MAGIC - [Databricks Agent Examples](https://github.com/databricks/genai-cookbook)
# MAGIC - [LangChain Agents Guide](https://python.langchain.com/docs/modules/agents/)
# MAGIC - [DSPy Examples](https://github.com/stanfordnlp/dspy)
# MAGIC
# MAGIC ### Community
# MAGIC - Databricks Community Forums
# MAGIC - LangChain Discord
# MAGIC - DSPy GitHub Discussions
