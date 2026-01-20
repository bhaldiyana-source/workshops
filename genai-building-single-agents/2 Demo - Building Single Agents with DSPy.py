# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Building Single Agents with DSPy
# MAGIC
# MAGIC ## Overview
# MAGIC This demo introduces **DSPy (Declarative Self-improving Python)**, a framework that treats AI applications as programs rather than prompts. You'll learn how to build agents using a declarative approach, leverage automatic prompt generation, and compare the DSPy methodology with the imperative LangChain approach you learned in the previous demo.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Understand DSPy's philosophy and key concepts
# MAGIC - Configure DSPy with Databricks Model Serving
# MAGIC - Use `dspy.Predict` for simple predictions
# MAGIC - Build reasoning chains with `dspy.ChainOfThought`
# MAGIC - Define custom tool functions for agents
# MAGIC - Implement `dspy.ReAct` for tool-enabled reasoning
# MAGIC - Interpret agent trajectories and decision-making
# MAGIC - Compare DSPy with LangChain approaches
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demo 1 (LangChain) for comparison context
# MAGIC - Access to Databricks Model Serving
# MAGIC - Basic understanding of Python classes and decorators
# MAGIC
# MAGIC ## Duration
# MAGIC 30-40 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install Required Packages

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet dspy-ai databricks-sdk mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction: What is DSPy?
# MAGIC
# MAGIC ### The Problem with Traditional Prompting
# MAGIC
# MAGIC When building LLM applications, we typically:
# MAGIC 1. Write detailed prompts
# MAGIC 2. Test and iterate manually
# MAGIC 3. Hard-code examples and instructions
# MAGIC 4. Struggle with consistency and optimization
# MAGIC
# MAGIC ### The DSPy Solution
# MAGIC
# MAGIC **DSPy** treats prompts as **parameters** that can be:
# MAGIC - Automatically generated from signatures
# MAGIC - Optimized through compilers
# MAGIC - Improved with training data
# MAGIC - Composed into modular programs
# MAGIC
# MAGIC ### Key Differences from LangChain
# MAGIC
# MAGIC | Aspect | LangChain | DSPy |
# MAGIC |--------|-----------|------|
# MAGIC | Approach | Imperative (chains) | Declarative (signatures) |
# MAGIC | Prompts | Explicit templates | Auto-generated |
# MAGIC | Optimization | Manual tuning | Automatic compilers |
# MAGIC | Composition | Chain objects | Python modules |
# MAGIC | Best For | Quick prototyping, explicit control | Systematic optimization, type safety |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure DSPy with Databricks

# COMMAND ----------

import dspy
import os
from databricks.sdk import WorkspaceClient

# Initialize Databricks workspace client
w = WorkspaceClient()

# Get the current host and token for API calls
host = w.config.host
token = w.config.token

# Configure DSPy to use Databricks Model Serving
# We'll use the Databricks class which supports Foundation Model APIs
lm = dspy.Databricks(
    model="databricks-meta-llama-3-1-70b-instruct",
    api_key=token,
    api_base=f"{host}/serving-endpoints",
    model_type="chat",  # Use chat mode for better tool calling
)

# Set as the global LM
dspy.configure(lm=lm)

print("✓ DSPy configured with Databricks Model Serving")
print(f"  Model: databricks-meta-llama-3-1-70b-instruct")
print(f"  Host: {host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: DSPy Basics - Signatures and Predict
# MAGIC
# MAGIC Let's start with the fundamentals of DSPy.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Simple Prediction with Signature

# COMMAND ----------

# Define a signature: input -> output
class BasicQA(dspy.Signature):
    """Answer questions with short, factual responses."""
    question = dspy.InputField()
    answer = dspy.OutputField()

# Create a predictor
predictor = dspy.Predict(BasicQA)

# Make a prediction (DSPy generates the prompt automatically)
result = predictor(question="What is the capital of France?")
print(f"Question: What is the capital of France?")
print(f"Answer: {result.answer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Multi-Field Signatures

# COMMAND ----------

# Define a more complex signature
class ClassifyText(dspy.Signature):
    """Classify text into categories."""
    text = dspy.InputField(desc="The text to classify")
    categories = dspy.InputField(desc="Comma-separated list of possible categories")
    classification = dspy.OutputField(desc="The selected category")
    confidence = dspy.OutputField(desc="Confidence level: high, medium, or low")

# Create predictor
classifier = dspy.Predict(ClassifyText)

# Classify some text
result = classifier(
    text="I love this product! It exceeded my expectations.",
    categories="positive, negative, neutral"
)

print(f"Classification: {result.classification}")
print(f"Confidence: {result.confidence}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Chain of Thought Reasoning
# MAGIC
# MAGIC DSPy's `ChainOfThought` module automatically adds reasoning steps.

# COMMAND ----------

# Define a signature for a reasoning task
class MathProblem(dspy.Signature):
    """Solve math word problems step by step."""
    problem = dspy.InputField()
    solution = dspy.OutputField(desc="The numerical answer")

# Compare Predict vs ChainOfThought
basic_solver = dspy.Predict(MathProblem)
cot_solver = dspy.ChainOfThought(MathProblem)

problem = "A store has 45 items. They sell 12 in the morning and 18 in the afternoon. How many items remain?"

print("=== Basic Predict (no reasoning) ===")
result1 = basic_solver(problem=problem)
print(f"Solution: {result1.solution}\n")

print("=== ChainOfThought (with reasoning) ===")
result2 = cot_solver(problem=problem)
print(f"Rationale: {result2.rationale}")
print(f"Solution: {result2.solution}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Defining Custom Tools
# MAGIC
# MAGIC Before building a ReAct agent, let's define some simple tools (Python functions).

# COMMAND ----------

# Tool 1: Calculate arithmetic
def calculate(expression: str) -> str:
    """
    Evaluates a mathematical expression and returns the result.
    
    Args:
        expression: A string containing a mathematical expression (e.g., "45 - 12 - 18")
    
    Returns:
        The result of the calculation as a string
    """
    try:
        # Safe evaluation (restricted to basic arithmetic)
        result = eval(expression, {"__builtins__": {}}, {})
        return f"The result of {expression} is {result}"
    except Exception as e:
        return f"Error calculating {expression}: {str(e)}"

# Tool 2: Customer database lookup (simulated)
def get_customer_tier(customer_id: str) -> str:
    """
    Looks up the tier status of a customer.
    
    Args:
        customer_id: The customer ID (e.g., "C001")
    
    Returns:
        The customer's tier level
    """
    # Simulated database
    customers = {
        "C001": "Gold",
        "C002": "Silver",
        "C003": "Gold",
        "C004": "Bronze",
        "C005": "Silver",
    }
    
    tier = customers.get(customer_id, "Unknown")
    return f"Customer {customer_id} has tier: {tier}"

# Tool 3: Get order count (simulated)
def get_order_count(customer_id: str) -> str:
    """
    Returns the number of orders for a customer.
    
    Args:
        customer_id: The customer ID (e.g., "C001")
    
    Returns:
        The number of orders
    """
    # Simulated database
    order_counts = {
        "C001": 3,
        "C002": 2,
        "C003": 3,
        "C004": 1,
        "C005": 1,
    }
    
    count = order_counts.get(customer_id, 0)
    return f"Customer {customer_id} has {count} orders"

# Test the tools
print(calculate("45 - 12 - 18"))
print(get_customer_tier("C001"))
print(get_order_count("C001"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Building a ReAct Agent with DSPy
# MAGIC
# MAGIC Now let's create a ReAct agent that can use tools to answer questions.

# COMMAND ----------

# Define the agent signature
class AnswerQuestion(dspy.Signature):
    """Answer questions by using available tools when needed."""
    question = dspy.InputField()
    answer = dspy.OutputField(desc="A clear, concise answer to the question")

# Create a ReAct agent with tools
agent = dspy.ReAct(
    signature=AnswerQuestion,
    tools=[calculate, get_customer_tier, get_order_count],
    max_iters=5  # Maximum reasoning iterations
)

print("✓ ReAct agent created with 3 tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: Simple Calculation

# COMMAND ----------

question = "What is 125 + 87?"
print(f"Question: {question}\n")

result = agent(question=question)

print(f"\n{'='*60}")
print(f"Answer: {result.answer}")
print(f"{'='*60}")

# View the agent's trajectory (reasoning steps)
if hasattr(result, 'trajectory'):
    print("\n--- Agent Trajectory ---")
    for i, step in enumerate(result.trajectory, 1):
        print(f"\nStep {i}:")
        if hasattr(step, 'thought'):
            print(f"  Thought: {step.thought}")
        if hasattr(step, 'action'):
            print(f"  Action: {step.action}")
        if hasattr(step, 'observation'):
            print(f"  Observation: {step.observation}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Customer Information Lookup

# COMMAND ----------

question = "What tier is customer C003?"
print(f"Question: {question}\n")

result = agent(question=question)

print(f"\n{'='*60}")
print(f"Answer: {result.answer}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: Multi-Tool Query

# COMMAND ----------

question = "How many orders does customer C001 have, and what is their tier?"
print(f"Question: {question}\n")

result = agent(question=question)

print(f"\n{'='*60}")
print(f"Answer: {result.answer}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 4: Complex Reasoning

# COMMAND ----------

question = "If customer C001 has 3 orders and customer C002 has 2 orders, what's the total?"
print(f"Question: {question}\n")

result = agent(question=question)

print(f"\n{'='*60}")
print(f"Answer: {result.answer}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Building a Custom ReAct Module
# MAGIC
# MAGIC For more control, you can create custom DSPy modules that compose multiple components.

# COMMAND ----------

class CustomerAssistant(dspy.Module):
    """A customer service assistant that can look up customer information."""
    
    def __init__(self):
        super().__init__()
        
        # Define the tools this assistant can use
        self.tools = [get_customer_tier, get_order_count]
        
        # Create a ReAct agent as a component
        self.agent = dspy.ReAct(
            signature="question -> answer",
            tools=self.tools,
            max_iters=5
        )
    
    def forward(self, question):
        """Process a customer question."""
        # Use the ReAct agent
        result = self.agent(question=question)
        
        # We could add post-processing here
        return result

# Instantiate the assistant
assistant = CustomerAssistant()

# Test it
question = "Is customer C005 a Gold tier member?"
result = assistant(question=question)
print(f"Question: {question}")
print(f"Answer: {result.answer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Advanced - Custom Signature with Type Hints

# COMMAND ----------

from typing import Literal

class CustomerQuery(dspy.Signature):
    """Answer customer-related questions using available data."""
    
    customer_id: str = dspy.InputField(desc="Customer ID (e.g., C001)")
    query_type: Literal["tier", "orders", "both"] = dspy.InputField(desc="Type of information requested")
    response: str = dspy.OutputField(desc="The requested information")

# Create a specialized predictor
customer_query_predictor = dspy.ChainOfThought(CustomerQuery)

# Test with structured inputs
result = customer_query_predictor(
    customer_id="C001",
    query_type="both"
)

print(f"Rationale: {result.rationale}")
print(f"Response: {result.response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Comparing DSPy and LangChain
# MAGIC
# MAGIC Let's directly compare the same task in both frameworks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Task
# MAGIC
# MAGIC Build an agent that can:
# MAGIC 1. Look up customer information
# MAGIC 2. Perform calculations
# MAGIC 3. Answer multi-step questions
# MAGIC
# MAGIC ### LangChain Approach (from Demo 1)
# MAGIC
# MAGIC ```python
# MAGIC # 1. Define tools explicitly
# MAGIC from langchain.tools import tool
# MAGIC
# MAGIC @tool
# MAGIC def get_customer_tier(customer_id: str) -> str:
# MAGIC     """Get the tier of a customer."""
# MAGIC     # implementation
# MAGIC     pass
# MAGIC
# MAGIC # 2. Create explicit prompt template
# MAGIC from langchain.prompts import PromptTemplate
# MAGIC
# MAGIC prompt = PromptTemplate.from_template("""
# MAGIC You are an assistant. Use these tools:
# MAGIC {tools}
# MAGIC
# MAGIC Question: {input}
# MAGIC Thought: {agent_scratchpad}
# MAGIC """)
# MAGIC
# MAGIC # 3. Create agent with explicit configuration
# MAGIC from langchain.agents import create_react_agent, AgentExecutor
# MAGIC
# MAGIC agent = create_react_agent(llm, tools, prompt)
# MAGIC executor = AgentExecutor(
# MAGIC     agent=agent,
# MAGIC     tools=tools,
# MAGIC     max_iterations=5,
# MAGIC     handle_parsing_errors=True
# MAGIC )
# MAGIC
# MAGIC # 4. Execute
# MAGIC result = executor.invoke({"input": "What tier is C001?"})
# MAGIC ```
# MAGIC
# MAGIC ### DSPy Approach (What We Just Did)
# MAGIC
# MAGIC ```python
# MAGIC # 1. Define tools (same functions)
# MAGIC def get_customer_tier(customer_id: str) -> str:
# MAGIC     """Get the tier of a customer."""
# MAGIC     # implementation
# MAGIC     pass
# MAGIC
# MAGIC # 2. Define signature (NO prompt template)
# MAGIC class AnswerQuestion(dspy.Signature):
# MAGIC     """Answer questions using tools."""
# MAGIC     question = dspy.InputField()
# MAGIC     answer = dspy.OutputField()
# MAGIC
# MAGIC # 3. Create agent (automatic prompt generation)
# MAGIC agent = dspy.ReAct(
# MAGIC     signature=AnswerQuestion,
# MAGIC     tools=[get_customer_tier],
# MAGIC     max_iters=5
# MAGIC )
# MAGIC
# MAGIC # 4. Execute
# MAGIC result = agent(question="What tier is C001?")
# MAGIC ```
# MAGIC
# MAGIC ### Key Differences
# MAGIC
# MAGIC | What | LangChain | DSPy |
# MAGIC |------|-----------|------|
# MAGIC | **Prompt** | Explicit template with placeholders | Auto-generated from signature |
# MAGIC | **Tools** | Decorator or Tool class | Plain Python functions |
# MAGIC | **Configuration** | Multiple objects (agent, executor) | Single module |
# MAGIC | **Execution** | `invoke()` with dict | Direct call with kwargs |
# MAGIC | **Optimization** | Manual prompt tuning | Can use optimizers/compilers |
# MAGIC | **Type Safety** | Limited | Strong typing with signatures |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Understanding Agent Trajectories
# MAGIC
# MAGIC DSPy agents create "trajectories" - the sequence of thoughts, actions, and observations.

# COMMAND ----------

# Create a detailed agent for analysis
class DetailedQA(dspy.Signature):
    """Answer questions step by step using available tools."""
    question = dspy.InputField()
    answer = dspy.OutputField(desc="Final answer with supporting details")

detailed_agent = dspy.ReAct(
    signature=DetailedQA,
    tools=[calculate, get_customer_tier, get_order_count],
    max_iters=6
)

# Execute with a complex query
question = "Customer C001 is Gold tier. Customer C002 is Silver tier. How many Gold tier customers are mentioned?"
print(f"Question: {question}\n")

result = detailed_agent(question=question)

print(f"\nFinal Answer: {result.answer}")

# Analyze the trajectory
print("\n" + "="*60)
print("TRAJECTORY ANALYSIS")
print("="*60)

if hasattr(result, 'trajectory'):
    print(f"\nTotal Steps: {len(result.trajectory)}")
    
    for i, step in enumerate(result.trajectory, 1):
        print(f"\n--- Step {i} ---")
        
        # Extract available fields
        step_dict = step if isinstance(step, dict) else step.__dict__
        
        for key, value in step_dict.items():
            if not key.startswith('_') and value:
                print(f"{key.capitalize()}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Best Practices for DSPy Agents

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Best Practice 1: Clear Signatures
# MAGIC
# MAGIC ```python
# MAGIC # Good: Descriptive signature with field descriptions
# MAGIC class GoodSignature(dspy.Signature):
# MAGIC     """Clear description of what this does."""
# MAGIC     input_field = dspy.InputField(desc="Specific description")
# MAGIC     output_field = dspy.OutputField(desc="What to expect")
# MAGIC
# MAGIC # Bad: Vague signature
# MAGIC class BadSignature(dspy.Signature):
# MAGIC     """Does stuff."""
# MAGIC     x = dspy.InputField()
# MAGIC     y = dspy.OutputField()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Best Practice 2: Focused Tools
# MAGIC
# MAGIC ```python
# MAGIC # Good: Single-purpose tool with clear docstring
# MAGIC def get_customer_tier(customer_id: str) -> str:
# MAGIC     """
# MAGIC     Get the membership tier for a customer.
# MAGIC     
# MAGIC     Args:
# MAGIC         customer_id: Customer ID like 'C001'
# MAGIC     
# MAGIC     Returns:
# MAGIC         Tier name: Gold, Silver, or Bronze
# MAGIC     """
# MAGIC     # implementation
# MAGIC     pass
# MAGIC
# MAGIC # Bad: Multi-purpose tool that's hard to understand
# MAGIC def get_data(id: str, type: str) -> str:
# MAGIC     """Gets data."""
# MAGIC     # implementation
# MAGIC     pass
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Best Practice 3: Composition Over Configuration

# COMMAND ----------

# Good: Modular composition
class OrderAnalyzer(dspy.Module):
    def __init__(self):
        super().__init__()
        self.info_lookup = dspy.ChainOfThought("customer_id -> customer_info")
        self.order_counter = dspy.ReAct(
            signature="customer_id -> order_count",
            tools=[get_order_count],
            max_iters=3
        )
    
    def forward(self, customer_id):
        info = self.info_lookup(customer_id=customer_id)
        count = self.order_counter(customer_id=customer_id)
        
        return dspy.Prediction(
            customer_info=info.customer_info,
            order_count=count.order_count
        )

# This approach makes it easy to:
# - Test each component separately
# - Reuse components in other modules
# - Optimize individual pieces

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: When to Use DSPy vs LangChain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use DSPy When:
# MAGIC
# MAGIC ✅ **You want systematic optimization**
# MAGIC - Planning to use optimizers to improve performance
# MAGIC - Have training data for bootstrapping
# MAGIC - Want automated prompt engineering
# MAGIC
# MAGIC ✅ **You prefer declarative code**
# MAGIC - Type-safe signatures over string templates
# MAGIC - Modular composition over chain building
# MAGIC - Python classes over configuration objects
# MAGIC
# MAGIC ✅ **You're doing research or experimentation**
# MAGIC - Testing different agent architectures
# MAGIC - Exploring optimization techniques
# MAGIC - Building novel agent patterns
# MAGIC
# MAGIC ### Use LangChain When:
# MAGIC
# MAGIC ✅ **You need production-ready features now**
# MAGIC - Extensive ecosystem of integrations
# MAGIC - Battle-tested in production
# MAGIC - Rich documentation and examples
# MAGIC
# MAGIC ✅ **You want explicit control**
# MAGIC - Custom prompt templates
# MAGIC - Specific error handling logic
# MAGIC - Complex chain orchestration
# MAGIC
# MAGIC ✅ **You're building standard applications**
# MAGIC - RAG systems
# MAGIC - Chatbots
# MAGIC - Document processing pipelines
# MAGIC
# MAGIC ### Use Both When:
# MAGIC
# MAGIC 🔄 **Prototyping then production**
# MAGIC - DSPy for experimentation and optimization
# MAGIC - LangChain for deployment
# MAGIC
# MAGIC 🔄 **Different components**
# MAGIC - DSPy for agent reasoning
# MAGIC - LangChain for data integration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC 1. ✅ **DSPy Philosophy**: Treat prompts as parameters, not code
# MAGIC 2. ✅ **Signatures**: Declare inputs/outputs instead of writing prompts
# MAGIC 3. ✅ **Modules**: Compose reusable components
# MAGIC 4. ✅ **ReAct Pattern**: Reasoning + Acting with tools
# MAGIC 5. ✅ **Trajectories**: Understand agent decision-making process
# MAGIC 6. ✅ **Comparison**: Know when to use DSPy vs LangChain
# MAGIC
# MAGIC ### Key Advantages of DSPy
# MAGIC
# MAGIC **Declarative**
# MAGIC - Write what you want, not how to get it
# MAGIC - Signatures instead of prompt templates
# MAGIC - Type-safe and composable
# MAGIC
# MAGIC **Optimizable**
# MAGIC - Automatic prompt generation
# MAGIC - Can be improved with compilers
# MAGIC - Systematic rather than ad-hoc tuning
# MAGIC
# MAGIC **Modular**
# MAGIC - Easy to compose and reuse
# MAGIC - Test components independently
# MAGIC - Clear separation of concerns
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Continue to **Lab 3: Building a LangChain Agent** where you'll apply what you've learned to build your own agent in a hands-on exercise.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [DSPy Official Docs](https://dspy-docs.vercel.app/)
# MAGIC - [DSPy GitHub Repository](https://github.com/stanfordnlp/dspy)
# MAGIC - [DSPy Paper (ArXiv)](https://arxiv.org/abs/2310.03714)
# MAGIC
# MAGIC ### Tutorials
# MAGIC - [DSPy Intro Tutorial](https://dspy-docs.vercel.app/docs/quick-start/installation)
# MAGIC - [Building Agents with DSPy](https://dspy-docs.vercel.app/docs/building-blocks/agents)
# MAGIC - [DSPy Optimizers Guide](https://dspy-docs.vercel.app/docs/building-blocks/optimizers)
# MAGIC
# MAGIC ### Examples
# MAGIC - [DSPy Examples Collection](https://github.com/stanfordnlp/dspy/tree/main/examples)
# MAGIC - [Comparing Frameworks](https://github.com/stanfordnlp/dspy/blob/main/examples/agents/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: DSPy Optimizers (Preview)
# MAGIC
# MAGIC One of DSPy's most powerful features is **optimizers** - algorithms that automatically improve your agent's prompts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Bootstrap Few-Shot Optimizer
# MAGIC
# MAGIC ```python
# MAGIC import dspy
# MAGIC from dspy.teleprompt import BootstrapFewShot
# MAGIC
# MAGIC # Define your program
# MAGIC class SimpleQA(dspy.Module):
# MAGIC     def __init__(self):
# MAGIC         super().__init__()
# MAGIC         self.generate_answer = dspy.ChainOfThought("question -> answer")
# MAGIC     
# MAGIC     def forward(self, question):
# MAGIC         return self.generate_answer(question=question)
# MAGIC
# MAGIC # Create training examples
# MAGIC train_examples = [
# MAGIC     dspy.Example(
# MAGIC         question="What tier is customer C001?",
# MAGIC         answer="Gold"
# MAGIC     ).with_inputs("question"),
# MAGIC     # More examples...
# MAGIC ]
# MAGIC
# MAGIC # Set up optimizer
# MAGIC optimizer = BootstrapFewShot(metric=lambda x, y: x.answer == y.answer)
# MAGIC
# MAGIC # Optimize the program
# MAGIC optimized_program = optimizer.compile(
# MAGIC     SimpleQA(),
# MAGIC     trainset=train_examples
# MAGIC )
# MAGIC
# MAGIC # The optimized program now has better prompts!
# MAGIC result = optimized_program(question="What tier is customer C002?")
# MAGIC ```
# MAGIC
# MAGIC **This is a preview** - optimizers are an advanced topic beyond this demo's scope, but they demonstrate DSPy's unique value proposition.
