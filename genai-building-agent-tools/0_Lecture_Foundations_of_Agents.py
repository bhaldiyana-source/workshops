# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Foundations of Agents
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the fundamental concepts of AI agents and how they use tools to accomplish complex tasks. We'll explore agent architectures, tool calling mechanisms, and the role of Unity Catalog functions as agent tools on Databricks.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain what AI agents are and how they differ from traditional AI models
# MAGIC - Understand the agent execution loop and tool calling process
# MAGIC - Identify use cases for agents on Databricks
# MAGIC - Recognize the components needed to build effective agent tools
# MAGIC
# MAGIC ## Duration
# MAGIC 15-20 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are AI Agents?
# MAGIC
# MAGIC ### Traditional AI Models vs. Agents
# MAGIC
# MAGIC **Traditional AI Models:**
# MAGIC - Take input and produce output
# MAGIC - Single-step prediction or generation
# MAGIC - Limited to knowledge in training data
# MAGIC - Cannot interact with external systems
# MAGIC
# MAGIC **AI Agents:**
# MAGIC - Can reason about tasks and break them into steps
# MAGIC - Use tools to interact with external systems
# MAGIC - Iterate and refine their approach
# MAGIC - Access real-time data beyond training cutoff
# MAGIC
# MAGIC ### Key Characteristics of Agents
# MAGIC
# MAGIC 1. **Autonomy**: Agents can operate independently and make decisions
# MAGIC 2. **Reactivity**: They respond to changes in their environment
# MAGIC 3. **Goal-oriented**: They work toward specific objectives
# MAGIC 4. **Tool Use**: They leverage external functions to accomplish tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Agent Execution Loop
# MAGIC
# MAGIC The typical agent follows a repeating cycle:
# MAGIC
# MAGIC ```
# MAGIC 1. Receive User Request
# MAGIC    ↓
# MAGIC 2. Reason about the Request
# MAGIC    ↓
# MAGIC 3. Decide on Action (Use Tool or Respond)
# MAGIC    ↓
# MAGIC 4. Execute Tool (if needed)
# MAGIC    ↓
# MAGIC 5. Observe Results
# MAGIC    ↓
# MAGIC 6. Repeat or Provide Final Answer
# MAGIC ```
# MAGIC
# MAGIC ### Example: Customer Support Agent
# MAGIC
# MAGIC **User Request:** "What's the status of my order #12345?"
# MAGIC
# MAGIC **Agent Reasoning:**
# MAGIC - I need to look up order information
# MAGIC - I have access to a `get_order_status` tool
# MAGIC - I'll call that tool with the order ID
# MAGIC
# MAGIC **Tool Execution:**
# MAGIC - Call `get_order_status(order_id="12345")`
# MAGIC - Receive: `{"status": "shipped", "tracking": "1Z999..."}`
# MAGIC
# MAGIC **Agent Response:**
# MAGIC - "Your order #12345 has been shipped! Tracking number: 1Z999..."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Tool Calling
# MAGIC
# MAGIC ### What is a Tool?
# MAGIC
# MAGIC A **tool** is a function that an AI agent can invoke to:
# MAGIC - Retrieve information from databases or APIs
# MAGIC - Perform calculations or data transformations
# MAGIC - Execute actions in external systems
# MAGIC - Access real-time or domain-specific data
# MAGIC
# MAGIC ### Tool Schema
# MAGIC
# MAGIC For an agent to use a tool, it needs to understand:
# MAGIC - **Name**: What the tool is called
# MAGIC - **Description**: What the tool does
# MAGIC - **Parameters**: What inputs it requires
# MAGIC - **Return Type**: What kind of output it produces
# MAGIC
# MAGIC ### Example Tool Schema
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "get_customer_orders",
# MAGIC   "description": "Retrieves all orders for a given customer ID",
# MAGIC   "parameters": {
# MAGIC     "customer_id": {
# MAGIC       "type": "string",
# MAGIC       "description": "The unique identifier for the customer"
# MAGIC     }
# MAGIC   },
# MAGIC   "returns": {
# MAGIC     "type": "array",
# MAGIC     "description": "List of order objects with details"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Architecture Patterns
# MAGIC
# MAGIC ### 1. Simple Reflex Agent
# MAGIC - Responds directly to user input
# MAGIC - Minimal reasoning or planning
# MAGIC - Best for straightforward tasks
# MAGIC
# MAGIC ### 2. Model-Based Agent
# MAGIC - Maintains state across interactions
# MAGIC - Understands context and history
# MAGIC - Can handle multi-turn conversations
# MAGIC
# MAGIC ### 3. Goal-Based Agent
# MAGIC - Works toward specific objectives
# MAGIC - Plans sequences of actions
# MAGIC - Evaluates outcomes against goals
# MAGIC
# MAGIC ### 4. Utility-Based Agent
# MAGIC - Optimizes for specific metrics
# MAGIC - Weighs trade-offs between options
# MAGIC - Chooses actions with highest expected value
# MAGIC
# MAGIC ### On Databricks
# MAGIC Databricks supports building agents that combine these patterns using:
# MAGIC - **Unity Catalog Functions** as tools
# MAGIC - **AI Playground** for testing and development
# MAGIC - **MLflow** for tracking and deployment
# MAGIC - **Mosaic AI Agent Framework** for production agents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Cases for Agents on Databricks
# MAGIC
# MAGIC ### 1. Data Analytics Assistant
# MAGIC **Scenario:** Business users ask natural language questions about their data
# MAGIC
# MAGIC **Tools Needed:**
# MAGIC - `query_sales_data()` - Execute SQL queries on sales tables
# MAGIC - `get_customer_metrics()` - Retrieve customer analytics
# MAGIC - `calculate_growth_rate()` - Compute period-over-period growth
# MAGIC
# MAGIC **Example Interaction:**
# MAGIC - User: "How did our sales perform last quarter?"
# MAGIC - Agent: Calls `query_sales_data(period="Q4-2025")` → Returns results
# MAGIC - Agent: "Sales in Q4 2025 were $2.3M, up 15% from Q3"
# MAGIC
# MAGIC ### 2. Customer Support Automation
# MAGIC **Scenario:** Automated handling of common support requests
# MAGIC
# MAGIC **Tools Needed:**
# MAGIC - `get_order_status()` - Look up order information
# MAGIC - `check_inventory()` - Verify product availability
# MAGIC - `process_return()` - Initiate return requests
# MAGIC
# MAGIC ### 3. Data Quality Monitor
# MAGIC **Scenario:** Proactive monitoring and alerting for data issues
# MAGIC
# MAGIC **Tools Needed:**
# MAGIC - `check_data_freshness()` - Verify when tables were last updated
# MAGIC - `detect_anomalies()` - Identify outliers in metrics
# MAGIC - `validate_schema()` - Ensure data structure compliance
# MAGIC
# MAGIC ### 4. Personalized Recommendations
# MAGIC **Scenario:** Generate customized product or content suggestions
# MAGIC
# MAGIC **Tools Needed:**
# MAGIC - `get_user_history()` - Retrieve past interactions
# MAGIC - `find_similar_items()` - Compute similarity scores
# MAGIC - `apply_business_rules()` - Filter based on inventory, promotions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Components of Effective Agent Tools
# MAGIC
# MAGIC ### 1. Clear Purpose
# MAGIC - Each tool should have a single, well-defined responsibility
# MAGIC - Avoid creating tools that try to do too many things
# MAGIC
# MAGIC ✅ **Good:** `get_customer_orders(customer_id)`
# MAGIC ❌ **Bad:** `get_all_customer_data(customer_id, include_orders, include_payments, ...)`
# MAGIC
# MAGIC ### 2. Descriptive Documentation
# MAGIC - Tool descriptions help the agent decide when to use them
# MAGIC - Be specific about what the tool does and when to use it
# MAGIC
# MAGIC ✅ **Good:** "Retrieves all orders placed by a customer within the last 90 days"
# MAGIC ❌ **Bad:** "Gets orders"
# MAGIC
# MAGIC ### 3. Well-Defined Parameters
# MAGIC - Use appropriate data types (string, integer, boolean, etc.)
# MAGIC - Include descriptions for each parameter
# MAGIC - Specify required vs. optional parameters
# MAGIC
# MAGIC ### 4. Reliable Error Handling
# MAGIC - Handle edge cases gracefully
# MAGIC - Return informative error messages
# MAGIC - Don't expose sensitive system details
# MAGIC
# MAGIC ### 5. Efficient Execution
# MAGIC - Tools should complete quickly (ideally < 30 seconds)
# MAGIC - Optimize queries and computations
# MAGIC - Consider pagination for large result sets

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Agents Use Tools: A Detailed Example
# MAGIC
# MAGIC Let's walk through a complete interaction where an agent uses multiple tools:
# MAGIC
# MAGIC ### User Request
# MAGIC "I need to analyze our top 5 customers from last month and see if any of them have outstanding issues."
# MAGIC
# MAGIC ### Agent Reasoning & Execution
# MAGIC
# MAGIC **Step 1: Break Down the Task**
# MAGIC - Need to identify top 5 customers from last month
# MAGIC - Need to check for outstanding issues
# MAGIC - Need to present analysis
# MAGIC
# MAGIC **Step 2: First Tool Call**
# MAGIC ```python
# MAGIC # Agent decides to call:
# MAGIC get_top_customers(
# MAGIC     period="2025-12",
# MAGIC     metric="revenue",
# MAGIC     limit=5
# MAGIC )
# MAGIC
# MAGIC # Returns:
# MAGIC [
# MAGIC     {"customer_id": "C001", "name": "Acme Corp", "revenue": 150000},
# MAGIC     {"customer_id": "C042", "name": "TechStart Inc", "revenue": 120000},
# MAGIC     ...
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Second Tool Call (for each customer)**
# MAGIC ```python
# MAGIC # Agent calls for each customer:
# MAGIC check_customer_issues(customer_id="C001")
# MAGIC
# MAGIC # Returns:
# MAGIC {
# MAGIC     "open_tickets": 2,
# MAGIC     "critical_issues": 0,
# MAGIC     "last_contact": "2025-12-15"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Step 4: Synthesize and Respond**
# MAGIC The agent combines the information and responds:
# MAGIC
# MAGIC "Here's the analysis of our top 5 customers from December 2025:
# MAGIC
# MAGIC 1. **Acme Corp** ($150K) - 2 open tickets, no critical issues
# MAGIC 2. **TechStart Inc** ($120K) - No open issues
# MAGIC ...
# MAGIC
# MAGIC Action needed: Follow up with Acme Corp on their pending tickets."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Functions as Agent Tools
# MAGIC
# MAGIC On Databricks, **Unity Catalog Functions** serve as the primary mechanism for creating agent tools.
# MAGIC
# MAGIC ### What are Unity Catalog Functions?
# MAGIC
# MAGIC Unity Catalog (UC) Functions are:
# MAGIC - **Registered functions** stored in Unity Catalog
# MAGIC - **Shareable** across users and workspaces
# MAGIC - **Governed** with access controls and lineage
# MAGIC - **Discoverable** with metadata and documentation
# MAGIC
# MAGIC ### Types of UC Functions for Agents
# MAGIC
# MAGIC 1. **SQL Functions**
# MAGIC    - Defined using SQL syntax
# MAGIC    - Query tables and views
# MAGIC    - Perform data transformations
# MAGIC
# MAGIC 2. **Python Functions**
# MAGIC    - Defined in Python
# MAGIC    - Complex logic and computations
# MAGIC    - Integration with external APIs
# MAGIC
# MAGIC ### Benefits for Agent Development
# MAGIC
# MAGIC - **Centralized Management**: All tools in one catalog
# MAGIC - **Version Control**: Track changes to function definitions
# MAGIC - **Security**: Fine-grained access control
# MAGIC - **Reusability**: Same functions across multiple agents
# MAGIC - **Testing**: Use AI Playground to test before deployment

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Playground for Agent Development
# MAGIC
# MAGIC The **Databricks AI Playground** is an interactive environment for:
# MAGIC
# MAGIC ### Building and Testing Agents
# MAGIC - Select foundation models (e.g., DBRX, Llama, GPT-4)
# MAGIC - Attach UC functions as tools
# MAGIC - Test agent interactions in real-time
# MAGIC - Debug tool calling behavior
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC 1. **Tool Attachment**
# MAGIC    - Browse and attach UC functions
# MAGIC    - See function schemas and descriptions
# MAGIC    - Enable/disable tools as needed
# MAGIC
# MAGIC 2. **Conversation Testing**
# MAGIC    - Send test queries to the agent
# MAGIC    - Observe which tools are called
# MAGIC    - View tool inputs and outputs
# MAGIC
# MAGIC 3. **Debugging View**
# MAGIC    - See the agent's reasoning process
# MAGIC    - Identify when tools are called incorrectly
# MAGIC    - Understand why certain tools were chosen
# MAGIC
# MAGIC 4. **Iteration**
# MAGIC    - Quickly modify function definitions
# MAGIC    - Re-test without redeployment
# MAGIC    - Refine tool descriptions for better performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Agent Tool Design
# MAGIC
# MAGIC ### 1. Start Simple
# MAGIC - Begin with a small set of well-defined tools
# MAGIC - Add complexity gradually as needed
# MAGIC - Test each tool individually before combining
# MAGIC
# MAGIC ### 2. Write Clear Descriptions
# MAGIC - The agent relies on descriptions to choose tools
# MAGIC - Include when to use the tool and what it returns
# MAGIC - Mention any limitations or constraints
# MAGIC
# MAGIC ### 3. Design for Composability
# MAGIC - Create tools that can work together
# MAGIC - Output from one tool should be usable by others
# MAGIC - Avoid tight coupling between tools
# MAGIC
# MAGIC ### 4. Handle Errors Gracefully
# MAGIC - Return meaningful error messages
# MAGIC - Don't crash on unexpected input
# MAGIC - Guide the agent on how to recover
# MAGIC
# MAGIC ### 5. Optimize for Performance
# MAGIC - Keep execution time under 30 seconds
# MAGIC - Use appropriate indexes on tables
# MAGIC - Limit result set sizes
# MAGIC - Cache results when appropriate
# MAGIC
# MAGIC ### 6. Document Thoroughly
# MAGIC - Include examples in comments
# MAGIC - Document expected behavior
# MAGIC - Note any dependencies or prerequisites

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Pitfalls to Avoid
# MAGIC
# MAGIC ### 1. Overly Complex Tools
# MAGIC **Problem:** Tool tries to do too much, making it hard for the agent to use correctly
# MAGIC
# MAGIC **Solution:** Break into smaller, focused tools
# MAGIC
# MAGIC ### 2. Vague Descriptions
# MAGIC **Problem:** Agent doesn't understand when to use the tool
# MAGIC
# MAGIC **Solution:** Provide specific, detailed descriptions with use cases
# MAGIC
# MAGIC ### 3. Missing Error Handling
# MAGIC **Problem:** Tools crash on invalid input, breaking the agent loop
# MAGIC
# MAGIC **Solution:** Validate inputs and return helpful error messages
# MAGIC
# MAGIC ### 4. Slow Execution
# MAGIC **Problem:** Tools take too long, causing timeouts or poor UX
# MAGIC
# MAGIC **Solution:** Optimize queries, add indexes, limit result sizes
# MAGIC
# MAGIC ### 5. Insufficient Testing
# MAGIC **Problem:** Tools work in isolation but fail when used by agent
# MAGIC
# MAGIC **Solution:** Test in AI Playground with realistic scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC In the upcoming notebooks, we'll dive deeper into:
# MAGIC
# MAGIC ### Lecture 1: Unity Catalog Functions as Agent Tools
# MAGIC - Detailed exploration of UC functions
# MAGIC - SQL vs Python functions
# MAGIC - Registration and management
# MAGIC - Security and governance
# MAGIC
# MAGIC ### Demo 2: Building SQL Functions as Agent Tools
# MAGIC - Hands-on creation of SQL-based tools
# MAGIC - Testing in AI Playground
# MAGIC - Common patterns and examples
# MAGIC
# MAGIC ### Demo 3: Building Python Functions as Agent Tools
# MAGIC - Creating Python-based tools
# MAGIC - Using DatabricksFunctionClient()
# MAGIC - Advanced patterns and techniques
# MAGIC
# MAGIC ### Lab 4: Building AI Agent Tools
# MAGIC - Guided exercises
# MAGIC - Troubleshooting scenarios
# MAGIC - Real-world applications

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **AI agents** extend LLMs with the ability to use tools and interact with external systems
# MAGIC
# MAGIC 2. The **agent execution loop** involves reasoning, tool selection, execution, and iteration
# MAGIC
# MAGIC 3. **Effective tools** are focused, well-documented, and handle errors gracefully
# MAGIC
# MAGIC 4. **Unity Catalog Functions** provide a governed, discoverable way to create agent tools on Databricks
# MAGIC
# MAGIC 5. The **AI Playground** enables rapid iteration and testing of agent behaviors
# MAGIC
# MAGIC 6. **Best practices** include clear descriptions, simple designs, and thorough testing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Databricks AI Playground Documentation](https://docs.databricks.com/)
# MAGIC - [Unity Catalog Functions Guide](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions.html)
# MAGIC - [Mosaic AI Agent Framework](https://docs.databricks.com/)
# MAGIC - [LangChain Documentation](https://python.langchain.com/)
# MAGIC - [Agent Design Patterns](https://www.databricks.com/blog)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Ready to continue?** Open the next notebook: **1 Lecture - Unity Catalog Functions as Agent Tools**
# MAGIC
