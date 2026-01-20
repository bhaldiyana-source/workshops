# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Foundations of Agentic AI
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture introduces the fundamental concepts of agentic AI systems. You'll learn about agent architecture, reasoning patterns, and key components that enable AI agents to perceive, reason, act, and learn.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand what AI agents are and how they differ from traditional AI systems
# MAGIC - Identify the core components of agent architecture
# MAGIC - Recognize different reasoning patterns (ReAct, Chain-of-Thought, Tree-of-Thought, Reflection)
# MAGIC - Understand how agents use tools, memory, planning, and guardrails
# MAGIC
# MAGIC ## Duration
# MAGIC 45 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Basic understanding of Large Language Models (LLMs)
# MAGIC - Familiarity with Python programming
# MAGIC - Understanding of API concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What Are AI Agents?
# MAGIC
# MAGIC AI Agents are autonomous systems that go beyond simple question-answering. They can:
# MAGIC
# MAGIC ### Core Capabilities
# MAGIC
# MAGIC **🔍 Perceive**: Understand their environment and context
# MAGIC - Parse user requests and extract intent
# MAGIC - Understand available resources and tools
# MAGIC - Recognize when additional information is needed
# MAGIC
# MAGIC **🧠 Reason**: Plan and make decisions using LLMs
# MAGIC - Break complex tasks into steps
# MAGIC - Evaluate different approaches
# MAGIC - Learn from previous attempts
# MAGIC
# MAGIC **🛠️ Act**: Use tools to accomplish tasks
# MAGIC - Execute functions and APIs
# MAGIC - Query databases and search systems
# MAGIC - Interact with external services
# MAGIC
# MAGIC **📚 Learn**: Adapt based on feedback and results
# MAGIC - Improve based on outcomes
# MAGIC - Adjust strategies when approaches fail
# MAGIC - Build knowledge from experiences

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agent Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    AI AGENT ARCHITECTURE                         │
# MAGIC ├─────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                  │
# MAGIC │  1. USER INPUT                                                   │
# MAGIC │     └─> Natural language request                                │
# MAGIC │         Example: "Analyze Q4 sales and create a report"         │
# MAGIC │                                                                  │
# MAGIC │  2. REASONING (LLM Core)                                        │
# MAGIC │     ├─> Understand intent: Create sales analysis report         │
# MAGIC │     ├─> Plan approach: Query data → Analyze → Generate report   │
# MAGIC │     └─> Decide on actions: Which tools to use and when          │
# MAGIC │                                                                  │
# MAGIC │  3. TOOL SELECTION                                              │
# MAGIC │     ├─> Identify needed tools: SQL query, analysis, writing     │
# MAGIC │     ├─> Prepare parameters: Date ranges, filters, formats       │
# MAGIC │     └─> Execute tool calls: Invoke selected functions           │
# MAGIC │                                                                  │
# MAGIC │  4. TOOL EXECUTION                                              │
# MAGIC │     ├─> Unity Catalog Functions: SQL queries, Python UDFs       │
# MAGIC │     ├─> MCP Server Tools: Custom business logic                 │
# MAGIC │     ├─> Vector Search: Knowledge retrieval (RAG)                │
# MAGIC │     └─> External APIs: Third-party integrations                 │
# MAGIC │                                                                  │
# MAGIC │  5. OBSERVATION                                                 │
# MAGIC │     ├─> Process tool results: Parse outputs                     │
# MAGIC │     ├─> Evaluate success: Did it work as expected?              │
# MAGIC │     └─> Determine next steps: Continue or adjust?               │
# MAGIC │                                                                  │
# MAGIC │  6. ITERATION                                                   │
# MAGIC │     └─> Repeat steps 2-5 until task complete                    │
# MAGIC │         - Maximum iterations to prevent infinite loops          │
# MAGIC │         - Early stopping when goal achieved                     │
# MAGIC │                                                                  │
# MAGIC │  7. RESPONSE GENERATION                                         │
# MAGIC │     └─> Synthesize final answer for user                        │
# MAGIC │         - Summarize findings                                    │
# MAGIC │         - Present in requested format                           │
# MAGIC │                                                                  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agent Reasoning Patterns
# MAGIC
# MAGIC Different reasoning patterns enable agents to tackle various types of problems effectively.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 ReAct (Reasoning + Acting)
# MAGIC
# MAGIC **The most common pattern for tool-using agents**
# MAGIC
# MAGIC ReAct interleaves reasoning (thinking) and acting (tool use) in a loop:
# MAGIC
# MAGIC ```
# MAGIC Thought: I need to find sales data for Q4 2024
# MAGIC Action: query_sales_database(start_date="2024-10-01", end_date="2024-12-31")
# MAGIC Observation: Retrieved 15,432 records, total revenue $2.3M
# MAGIC
# MAGIC Thought: Now I should analyze trends by region
# MAGIC Action: calculate_revenue_by_region(data)
# MAGIC Observation: West: $1.1M, East: $800K, Central: $400K
# MAGIC
# MAGIC Thought: I have enough information to create the report
# MAGIC Action: generate_report(data, analysis)
# MAGIC Observation: Report generated successfully
# MAGIC
# MAGIC Final Answer: Q4 2024 sales were $2.3M, with West region leading...
# MAGIC ```
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC - Transparent reasoning process
# MAGIC - Easy to debug and understand
# MAGIC - Natural for tool-using tasks
# MAGIC - Adapts based on observations

# COMMAND ----------

# Example: ReAct Pattern Simulation
class ReActAgent:
    """Simplified ReAct agent to demonstrate the pattern"""
    
    def __init__(self):
        self.max_iterations = 10
        self.thoughts = []
        self.actions = []
        self.observations = []
    
    def think(self, context: str) -> str:
        """Generate a thought based on current context"""
        thought = f"Thought: Based on {context}, I should take action"
        self.thoughts.append(thought)
        return thought
    
    def act(self, action_description: str) -> str:
        """Execute an action (simulated)"""
        action = f"Action: {action_description}"
        self.actions.append(action)
        return action
    
    def observe(self, result: str) -> str:
        """Process observation from action"""
        observation = f"Observation: {result}"
        self.observations.append(observation)
        return observation
    
    def run(self, task: str):
        """Execute ReAct loop"""
        print(f"Task: {task}\n")
        
        # Simplified 3-step ReAct cycle
        # Step 1
        thought1 = self.think("the user's request")
        print(thought1)
        action1 = self.act("query_database()")
        print(action1)
        obs1 = self.observe("Retrieved 1000 records")
        print(obs1 + "\n")
        
        # Step 2
        thought2 = self.think("the retrieved data")
        print(thought2)
        action2 = self.act("analyze_data()")
        print(action2)
        obs2 = self.observe("Analysis complete, found key insights")
        print(obs2 + "\n")
        
        # Step 3
        thought3 = self.think("the analysis results")
        print(thought3)
        print("Final Answer: Here are the insights from the analysis...")

# Demonstrate ReAct pattern
agent = ReActAgent()
agent.run("Analyze sales data and provide insights")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Chain-of-Thought (CoT)
# MAGIC
# MAGIC **Breaking complex problems into sequential steps**
# MAGIC
# MAGIC Chain-of-Thought prompts the LLM to show its reasoning process step-by-step:
# MAGIC
# MAGIC ```
# MAGIC Question: Should we expand to the European market?
# MAGIC
# MAGIC Let me think through this step by step:
# MAGIC
# MAGIC 1. First, I need to assess our current market position
# MAGIC    - We have 30% market share in North America
# MAGIC    - Revenue growth has been 20% YoY
# MAGIC
# MAGIC 2. Next, evaluate European market opportunity
# MAGIC    - Market size is $500M
# MAGIC    - Competition includes 3 major players
# MAGIC
# MAGIC 3. Then, consider our capabilities
# MAGIC    - We have distribution partnerships ready
# MAGIC    - Product needs localization (2-3 months)
# MAGIC
# MAGIC 4. Finally, analyze risks and costs
# MAGIC    - Initial investment: $2M
# MAGIC    - Break-even expected in 18 months
# MAGIC
# MAGIC Conclusion: Yes, expansion is advisable given...
# MAGIC ```
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC - Improves reasoning quality
# MAGIC - Makes logic transparent
# MAGIC - Helps with complex analytical tasks
# MAGIC - Easier to verify correctness

# COMMAND ----------

# Example: Chain-of-Thought for Business Decision
def chain_of_thought_analysis(question: str) -> dict:
    """Demonstrate Chain-of-Thought reasoning structure"""
    
    analysis = {
        "question": question,
        "reasoning_steps": [
            {
                "step": 1,
                "focus": "Assess Current State",
                "findings": [
                    "Current market share: 30%",
                    "YoY growth: 20%",
                    "Customer satisfaction: 4.5/5"
                ]
            },
            {
                "step": 2,
                "focus": "Evaluate Opportunity",
                "findings": [
                    "Market size: $500M",
                    "Growth rate: 15% annually",
                    "Competitive landscape: Fragmented"
                ]
            },
            {
                "step": 3,
                "focus": "Assess Capabilities",
                "findings": [
                    "Ready partnerships: 3 distributors",
                    "Localization needed: 2-3 months",
                    "Team capacity: Can support"
                ]
            },
            {
                "step": 4,
                "focus": "Risk and Cost Analysis",
                "findings": [
                    "Initial investment: $2M",
                    "Break-even: 18 months",
                    "Key risks: Regulatory, currency"
                ]
            }
        ],
        "conclusion": "Expansion recommended with phased approach"
    }
    
    return analysis

# Example usage
result = chain_of_thought_analysis("Should we expand to European market?")

print(f"Question: {result['question']}\n")
print("Chain-of-Thought Analysis:")
print("=" * 50)

for step in result['reasoning_steps']:
    print(f"\nStep {step['step']}: {step['focus']}")
    for finding in step['findings']:
        print(f"  • {finding}")

print(f"\n{'=' * 50}")
print(f"Conclusion: {result['conclusion']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Tree-of-Thought (ToT)
# MAGIC
# MAGIC **Exploring multiple reasoning paths**
# MAGIC
# MAGIC Tree-of-Thought explores different approaches in parallel and evaluates alternatives:
# MAGIC
# MAGIC ```
# MAGIC                    Initial Problem
# MAGIC                          |
# MAGIC        +-----------------+-----------------+
# MAGIC        |                 |                 |
# MAGIC   Approach A        Approach B        Approach C
# MAGIC        |                 |                 |
# MAGIC   [Evaluate]        [Evaluate]        [Evaluate]
# MAGIC        |                 |                 |
# MAGIC   Score: 7/10       Score: 9/10       Score: 5/10
# MAGIC        |                 |                 |
# MAGIC        |           [Selected]              |
# MAGIC        |                 |                 |
# MAGIC        |         Refine & Expand          |
# MAGIC        |                 |                 |
# MAGIC        |         Final Solution           |
# MAGIC ```
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC - Explores multiple strategies
# MAGIC - Can backtrack if approach fails
# MAGIC - Better for open-ended problems
# MAGIC - Finds optimal solutions

# COMMAND ----------

# Example: Tree-of-Thought for Strategy Selection
class TreeOfThought:
    """Simplified Tree-of-Thought reasoning"""
    
    def __init__(self):
        self.approaches = []
    
    def generate_approaches(self, problem: str) -> list:
        """Generate multiple potential approaches"""
        approaches = [
            {
                "id": "A",
                "name": "Data-Driven Approach",
                "description": "Use historical data and ML models",
                "pros": ["Objective", "Scalable", "Predictive"],
                "cons": ["Requires data", "Time to build"],
                "score": 0
            },
            {
                "id": "B",
                "name": "Expert Consultation",
                "description": "Gather insights from domain experts",
                "pros": ["Deep insights", "Quick", "Contextual"],
                "cons": ["Subjective", "Limited scale"],
                "score": 0
            },
            {
                "id": "C",
                "name": "Hybrid Approach",
                "description": "Combine data analysis with expert input",
                "pros": ["Balanced", "Comprehensive", "Validated"],
                "cons": ["Resource intensive", "Slower"],
                "score": 0
            }
        ]
        return approaches
    
    def evaluate_approach(self, approach: dict) -> float:
        """Evaluate approach based on criteria"""
        # Simplified scoring (in reality, this would use LLM evaluation)
        score = len(approach['pros']) * 3 - len(approach['cons']) * 1.5
        return max(0, min(10, score))
    
    def select_best(self, approaches: list) -> dict:
        """Select highest scoring approach"""
        for approach in approaches:
            approach['score'] = self.evaluate_approach(approach)
        
        best = max(approaches, key=lambda x: x['score'])
        return best

# Demonstrate Tree-of-Thought
tot = TreeOfThought()
problem = "How should we prioritize product features for next quarter?"

print(f"Problem: {problem}\n")
print("=" * 60)

approaches = tot.generate_approaches(problem)

print("\nGenerating and Evaluating Approaches:\n")
for approach in approaches:
    score = tot.evaluate_approach(approach)
    approach['score'] = score
    print(f"Approach {approach['id']}: {approach['name']}")
    print(f"  Description: {approach['description']}")
    print(f"  Pros: {', '.join(approach['pros'])}")
    print(f"  Cons: {', '.join(approach['cons'])}")
    print(f"  Score: {score}/10\n")

best = tot.select_best(approaches)
print("=" * 60)
print(f"\n✓ Selected Approach: {best['name']} (Score: {best['score']}/10)")
print(f"  Rationale: {best['description']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Reflection Pattern
# MAGIC
# MAGIC **Self-critique and iterative improvement**
# MAGIC
# MAGIC Reflection allows agents to review and improve their own outputs:
# MAGIC
# MAGIC ```
# MAGIC Initial Output:
# MAGIC "Our Q4 sales increased by 20%"
# MAGIC
# MAGIC Reflection:
# MAGIC - Is this complete? No, missing context about targets
# MAGIC - Is this accurate? Yes, but needs comparison to previous quarters
# MAGIC - Is this actionable? No, needs insights about drivers
# MAGIC
# MAGIC Improved Output:
# MAGIC "Our Q4 sales increased by 20% to $2.3M, exceeding our target of 
# MAGIC $2.0M. This growth was primarily driven by strong performance in 
# MAGIC the West region (+35%) and successful holiday campaigns. However, 
# MAGIC East region declined by 5%, requiring attention in Q1."
# MAGIC ```
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC - Improves output quality
# MAGIC - Catches errors and omissions
# MAGIC - Ensures completeness
# MAGIC - Enhances reliability

# COMMAND ----------

# Example: Reflection Pattern for Quality Improvement
class ReflectionAgent:
    """Agent that critiques and improves its own outputs"""
    
    def generate_initial_response(self, query: str) -> str:
        """Generate initial response (simplified)"""
        return "Sales increased by 20% in Q4."
    
    def reflect(self, response: str) -> dict:
        """Critique the response"""
        critique = {
            "completeness": {
                "score": 3,
                "issues": [
                    "Missing absolute values",
                    "No comparison to targets",
                    "No breakdown by category"
                ]
            },
            "accuracy": {
                "score": 8,
                "issues": [
                    "Statement is correct but lacks context"
                ]
            },
            "actionability": {
                "score": 2,
                "issues": [
                    "No insights about drivers",
                    "No recommendations provided",
                    "No areas of concern identified"
                ]
            }
        }
        return critique
    
    def improve(self, original: str, critique: dict) -> str:
        """Generate improved response based on critique"""
        improved = """
Q4 Sales Performance Analysis:

Overall Performance:
• Sales increased by 20% to $2.3M in Q4 2024
• Exceeded target of $2.0M by 15%
• Best quarterly performance this year

Breakdown by Region:
• West: $1.1M (+35% YoY) - Strong growth from new partnerships
• East: $800K (-5% YoY) - Declined due to increased competition
• Central: $400K (+10% YoY) - Steady growth, meeting expectations

Key Drivers:
• Holiday campaign ROI: 250%
• New product launch contributed $400K
• Enterprise segment grew 45%

Recommendations:
• Investigate East region decline and develop recovery plan
• Scale successful West region strategies to other regions
• Increase Q1 budget allocation to build on momentum
"""
        return improved.strip()

# Demonstrate Reflection
agent = ReflectionAgent()

query = "How did Q4 sales perform?"

print("Query:", query)
print("\n" + "=" * 60)

# Initial response
initial = agent.generate_initial_response(query)
print("\n1. INITIAL RESPONSE:")
print(initial)

# Reflection
print("\n" + "=" * 60)
print("\n2. REFLECTION & CRITIQUE:")
critique = agent.reflect(initial)
for aspect, details in critique.items():
    print(f"\n{aspect.title()}: {details['score']}/10")
    for issue in details['issues']:
        print(f"  ⚠ {issue}")

# Improved response
print("\n" + "=" * 60)
print("\n3. IMPROVED RESPONSE:")
improved = agent.improve(initial, critique)
print(improved)

print("\n" + "=" * 60)
print("\n✓ Response quality improved through reflection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Key Agent Components
# MAGIC
# MAGIC Every production agent system requires these core components:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Tools (Functions)
# MAGIC
# MAGIC **Discrete capabilities the agent can use**
# MAGIC
# MAGIC Tools are the "hands" of an agent - they enable interaction with the world:
# MAGIC
# MAGIC **Characteristics of Good Tools:**
# MAGIC - **Single Responsibility**: Each tool does one thing well
# MAGIC - **Clear Interface**: Well-defined inputs and outputs
# MAGIC - **Documented**: Description helps LLM understand when to use it
# MAGIC - **Error Handling**: Graceful failures with helpful messages
# MAGIC - **Idempotent**: Safe to call multiple times (when possible)
# MAGIC
# MAGIC **Examples:**
# MAGIC - `query_database(sql)`: Execute SQL queries
# MAGIC - `search_documents(query)`: Vector search for information
# MAGIC - `send_email(to, subject, body)`: Send notifications
# MAGIC - `calculate_metrics(data)`: Compute business metrics
# MAGIC - `create_visualization(data, type)`: Generate charts

# COMMAND ----------

# Example: Well-Designed Tool
from typing import Dict, List, Any
import json

class AgentTool:
    """Base class for agent tools"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    def get_schema(self) -> dict:
        """Return tool schema for LLM"""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.get_parameters()
        }
    
    def get_parameters(self) -> dict:
        """Override in subclass"""
        raise NotImplementedError
    
    def execute(self, **kwargs) -> Any:
        """Override in subclass"""
        raise NotImplementedError


class DatabaseQueryTool(AgentTool):
    """Tool for querying databases"""
    
    def __init__(self):
        super().__init__(
            name="query_database",
            description="Execute a SQL query against the sales database. Use this tool when you need to retrieve or analyze sales data."
        )
    
    def get_parameters(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL query to execute"
                }
            },
            "required": ["query"]
        }
    
    def execute(self, query: str) -> Dict[str, Any]:
        """Execute query (simulated)"""
        # In production, this would connect to actual database
        try:
            # Simulate query execution
            if "sales" in query.lower():
                result = {
                    "success": True,
                    "rows": [
                        {"region": "West", "revenue": 1100000},
                        {"region": "East", "revenue": 800000},
                        {"region": "Central", "revenue": 400000}
                    ],
                    "row_count": 3
                }
            else:
                result = {
                    "success": True,
                    "rows": [],
                    "row_count": 0
                }
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to execute query"
            }


# Example usage
tool = DatabaseQueryTool()

print("Tool Schema (for LLM):")
print(json.dumps(tool.get_schema(), indent=2))

print("\n" + "=" * 60)
print("\nExample Execution:")
result = tool.execute("SELECT region, SUM(revenue) as revenue FROM sales GROUP BY region")
print(json.dumps(result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Memory
# MAGIC
# MAGIC **Enabling agents to remember and learn**
# MAGIC
# MAGIC Memory allows agents to maintain context and improve over time:
# MAGIC
# MAGIC **Types of Memory:**
# MAGIC
# MAGIC 1. **Short-term Memory** (Conversation History)
# MAGIC    - Recent exchanges in current session
# MAGIC    - Maintained in prompt context
# MAGIC    - Limited by context window
# MAGIC
# MAGIC 2. **Long-term Memory** (Persistent Storage)
# MAGIC    - Facts learned from past interactions
# MAGIC    - Stored in vector databases
# MAGIC    - Retrieved when relevant
# MAGIC
# MAGIC 3. **Working Memory** (Task Context)
# MAGIC    - Current task state and progress
# MAGIC    - Intermediate results
# MAGIC    - Active goals

# COMMAND ----------

# Example: Agent Memory System
from datetime import datetime
from collections import deque

class AgentMemory:
    """Simple agent memory system"""
    
    def __init__(self, max_short_term=10):
        # Short-term: Recent conversation
        self.short_term = deque(maxlen=max_short_term)
        
        # Long-term: Facts and learnings
        self.long_term = []
        
        # Working: Current task context
        self.working = {}
    
    def add_interaction(self, user_input: str, agent_response: str):
        """Add to short-term memory"""
        interaction = {
            "timestamp": datetime.now(),
            "user": user_input,
            "agent": agent_response
        }
        self.short_term.append(interaction)
    
    def store_fact(self, fact: str, source: str = "learned"):
        """Add to long-term memory"""
        fact_entry = {
            "fact": fact,
            "source": source,
            "timestamp": datetime.now(),
            "confidence": 1.0
        }
        self.long_term.append(fact_entry)
    
    def set_working_context(self, key: str, value: Any):
        """Update working memory"""
        self.working[key] = value
    
    def get_context(self) -> dict:
        """Get full memory context"""
        return {
            "recent_conversation": list(self.short_term)[-3:],  # Last 3 exchanges
            "relevant_facts": self.long_term[-5:],  # Recent facts
            "current_task": self.working
        }
    
    def format_for_prompt(self) -> str:
        """Format memory for inclusion in prompt"""
        context = self.get_context()
        
        prompt_parts = []
        
        # Recent conversation
        if context['recent_conversation']:
            prompt_parts.append("Recent Conversation:")
            for interaction in context['recent_conversation']:
                prompt_parts.append(f"User: {interaction['user']}")
                prompt_parts.append(f"Assistant: {interaction['agent']}\n")
        
        # Relevant facts
        if context['relevant_facts']:
            prompt_parts.append("\nRelevant Facts:")
            for fact in context['relevant_facts']:
                prompt_parts.append(f"- {fact['fact']}")
        
        # Current task
        if context['current_task']:
            prompt_parts.append("\nCurrent Task Context:")
            for key, value in context['current_task'].items():
                prompt_parts.append(f"- {key}: {value}")
        
        return "\n".join(prompt_parts)


# Demonstrate memory system
memory = AgentMemory()

# Simulate conversation
memory.add_interaction(
    "What were Q4 sales?",
    "Q4 sales were $2.3M, up 20% from Q3"
)

memory.add_interaction(
    "Which region performed best?",
    "West region led with $1.1M, growing 35% YoY"
)

# Store learned facts
memory.store_fact("West region has strongest growth trajectory", source="analysis")
memory.store_fact("East region needs attention due to 5% decline", source="analysis")

# Set working context for current task
memory.set_working_context("task", "Create Q4 report")
memory.set_working_context("progress", "Data collected, starting analysis")

# Show memory in prompt format
print("Memory Context for LLM Prompt:")
print("=" * 60)
print(memory.format_for_prompt())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Planning
# MAGIC
# MAGIC **Breaking down complex tasks**
# MAGIC
# MAGIC Planning enables agents to tackle multi-step problems systematically:
# MAGIC
# MAGIC **Planning Strategies:**
# MAGIC
# MAGIC 1. **Task Decomposition**
# MAGIC    - Break goals into subtasks
# MAGIC    - Identify dependencies
# MAGIC    - Order execution steps
# MAGIC
# MAGIC 2. **Action Sequencing**
# MAGIC    - Determine tool call order
# MAGIC    - Handle conditional logic
# MAGIC    - Manage parallel execution
# MAGIC
# MAGIC 3. **Goal Tracking**
# MAGIC    - Monitor progress toward objectives
# MAGIC    - Detect when goals are achieved
# MAGIC    - Identify blockers

# COMMAND ----------

# Example: Agent Planning System
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional

class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"

@dataclass
class Task:
    id: str
    description: str
    status: TaskStatus = TaskStatus.PENDING
    dependencies: List[str] = None
    result: Optional[Any] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []

class AgentPlanner:
    """Plan and manage complex multi-step tasks"""
    
    def __init__(self):
        self.tasks = {}
        self.execution_order = []
    
    def decompose_goal(self, goal: str) -> List[Task]:
        """Break down high-level goal into tasks"""
        # In production, LLM would do this decomposition
        
        if "create report" in goal.lower():
            tasks = [
                Task(
                    id="task_1",
                    description="Query sales data from database",
                    dependencies=[]
                ),
                Task(
                    id="task_2", 
                    description="Calculate key metrics and trends",
                    dependencies=["task_1"]
                ),
                Task(
                    id="task_3",
                    description="Generate visualizations",
                    dependencies=["task_2"]
                ),
                Task(
                    id="task_4",
                    description="Write executive summary",
                    dependencies=["task_2", "task_3"]
                ),
                Task(
                    id="task_5",
                    description="Format and deliver final report",
                    dependencies=["task_4"]
                )
            ]
        else:
            tasks = [
                Task(
                    id="task_1",
                    description=goal,
                    dependencies=[]
                )
            ]
        
        for task in tasks:
            self.tasks[task.id] = task
        
        return tasks
    
    def get_execution_order(self) -> List[str]:
        """Determine execution order based on dependencies"""
        ordered = []
        completed = set()
        
        def can_execute(task_id: str) -> bool:
            task = self.tasks[task_id]
            return all(dep in completed for dep in task.dependencies)
        
        while len(completed) < len(self.tasks):
            # Find next executable task
            for task_id, task in self.tasks.items():
                if task_id not in completed and can_execute(task_id):
                    ordered.append(task_id)
                    completed.add(task_id)
                    break
            else:
                # No task can be executed (circular dependency or all done)
                break
        
        self.execution_order = ordered
        return ordered
    
    def execute_plan(self):
        """Execute tasks in order"""
        order = self.get_execution_order()
        
        print("Executing Plan:")
        print("=" * 60)
        
        for i, task_id in enumerate(order, 1):
            task = self.tasks[task_id]
            task.status = TaskStatus.IN_PROGRESS
            
            print(f"\nStep {i}: {task.description}")
            print(f"  Dependencies: {task.dependencies if task.dependencies else 'None'}")
            
            # Simulate task execution
            task.status = TaskStatus.COMPLETED
            task.result = f"Result from {task_id}"
            
            print(f"  Status: ✓ {task.status.value}")
        
        print("\n" + "=" * 60)
        print("✓ Plan execution completed")


# Demonstrate planning
planner = AgentPlanner()

goal = "Create comprehensive Q4 sales report with visualizations"
print(f"Goal: {goal}\n")

# Decompose goal into tasks
tasks = planner.decompose_goal(goal)

print("Task Decomposition:")
print("=" * 60)
for task in tasks:
    deps = f" (depends on: {', '.join(task.dependencies)})" if task.dependencies else ""
    print(f"{task.id}: {task.description}{deps}")

print("\n")

# Execute plan
planner.execute_plan()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Guardrails
# MAGIC
# MAGIC **Safety and control mechanisms**
# MAGIC
# MAGIC Guardrails ensure agents operate safely and within acceptable boundaries:
# MAGIC
# MAGIC **Types of Guardrails:**
# MAGIC
# MAGIC 1. **Input Validation**
# MAGIC    - Sanitize user inputs
# MAGIC    - Check for malicious content
# MAGIC    - Validate parameters
# MAGIC
# MAGIC 2. **Output Filtering**
# MAGIC    - Remove sensitive information (PII, credentials)
# MAGIC    - Check for harmful content
# MAGIC    - Ensure appropriate responses
# MAGIC
# MAGIC 3. **Safety Constraints**
# MAGIC    - Limit resource usage (tokens, API calls, cost)
# MAGIC    - Prevent infinite loops
# MAGIC    - Rate limiting
# MAGIC
# MAGIC 4. **Permission Management**
# MAGIC    - Verify access rights
# MAGIC    - Require approval for sensitive actions
# MAGIC    - Audit all operations

# COMMAND ----------

# Example: Agent Guardrails System
import re
from typing import Tuple

class AgentGuardrails:
    """Safety and control mechanisms for agents"""
    
    def __init__(self):
        self.max_iterations = 10
        self.max_cost = 1.00  # dollars
        self.current_iterations = 0
        self.current_cost = 0.0
        
        # Sensitive patterns to detect
        self.pii_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'
        }
        
        # Forbidden operations
        self.forbidden_keywords = [
            'DROP TABLE',
            'DELETE FROM',
            'TRUNCATE',
            'ALTER TABLE'
        ]
    
    def validate_input(self, user_input: str) -> Tuple[bool, str]:
        """Validate user input for safety"""
        # Check for SQL injection attempts
        for keyword in self.forbidden_keywords:
            if keyword in user_input.upper():
                return False, f"Input contains forbidden operation: {keyword}"
        
        # Check for excessive length
        if len(user_input) > 10000:
            return False, "Input exceeds maximum length"
        
        return True, "Input validated"
    
    def filter_output(self, output: str) -> str:
        """Remove sensitive information from output"""
        filtered = output
        
        # Redact PII
        for pii_type, pattern in self.pii_patterns.items():
            filtered = re.sub(pattern, f'[REDACTED {pii_type.upper()}]', filtered)
        
        return filtered
    
    def check_iteration_limit(self) -> Tuple[bool, str]:
        """Check if iteration limit exceeded"""
        self.current_iterations += 1
        
        if self.current_iterations > self.max_iterations:
            return False, "Maximum iterations exceeded"
        
        return True, f"Iteration {self.current_iterations}/{self.max_iterations}"
    
    def check_cost_limit(self, cost: float) -> Tuple[bool, str]:
        """Check if cost limit exceeded"""
        self.current_cost += cost
        
        if self.current_cost > self.max_cost:
            return False, f"Cost limit exceeded: ${self.current_cost:.2f} > ${self.max_cost:.2f}"
        
        return True, f"Current cost: ${self.current_cost:.2f}/{self.max_cost:.2f}"
    
    def verify_permission(self, action: str, user_role: str) -> Tuple[bool, str]:
        """Verify user has permission for action"""
        # Simplified permission system
        permissions = {
            "admin": ["read", "write", "delete", "execute"],
            "analyst": ["read", "execute"],
            "viewer": ["read"]
        }
        
        action_type = "read"  # Simplified
        if "delete" in action.lower() or "drop" in action.lower():
            action_type = "delete"
        elif "update" in action.lower() or "insert" in action.lower():
            action_type = "write"
        elif "execute" in action.lower() or "run" in action.lower():
            action_type = "execute"
        
        user_permissions = permissions.get(user_role, [])
        
        if action_type in user_permissions:
            return True, f"Permission granted for {action_type}"
        else:
            return False, f"Permission denied: {user_role} cannot perform {action_type}"


# Demonstrate guardrails
guardrails = AgentGuardrails()

print("GUARDRAILS DEMONSTRATION")
print("=" * 60)

# Test 1: Input validation
print("\n1. Input Validation:")
test_inputs = [
    "What were our sales last quarter?",
    "DROP TABLE customers; --",
    "Show me customer data with email addresses"
]

for inp in test_inputs:
    valid, message = guardrails.validate_input(inp)
    status = "✓" if valid else "✗"
    print(f"{status} '{inp[:50]}...' - {message}")

# Test 2: Output filtering
print("\n2. Output Filtering:")
sensitive_output = """
Customer Report:
- John Doe (john.doe@example.com, 555-123-4567)
- SSN: 123-45-6789
- Credit Card: 4532-1234-5678-9010
"""
filtered = guardrails.filter_output(sensitive_output)
print("Original output (truncated):")
print(sensitive_output[:100] + "...")
print("\nFiltered output:")
print(filtered)

# Test 3: Iteration limit
print("\n3. Iteration Limit Check:")
for i in range(3):
    valid, message = guardrails.check_iteration_limit()
    print(f"  {message}")

# Test 4: Cost tracking
print("\n4. Cost Tracking:")
for i in range(3):
    cost = 0.15
    valid, message = guardrails.check_cost_limit(cost)
    status = "✓" if valid else "✗"
    print(f"  {status} {message}")

# Test 5: Permission check
print("\n5. Permission Verification:")
actions = [
    ("Query sales data", "analyst"),
    ("Delete customer records", "analyst"),
    ("Execute analysis", "viewer")
]

for action, role in actions:
    valid, message = guardrails.verify_permission(action, role)
    status = "✓" if valid else "✗"
    print(f"  {status} {action} (as {role}): {message}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agent vs. Traditional AI Systems
# MAGIC
# MAGIC Understanding how agents differ from traditional approaches:
# MAGIC
# MAGIC | Aspect | Traditional AI/LLM | Agentic AI |
# MAGIC |--------|-------------------|------------|
# MAGIC | **Interaction** | Single request-response | Multi-step reasoning loop |
# MAGIC | **Capabilities** | Fixed to model knowledge | Can use external tools |
# MAGIC | **Decision Making** | Direct answer generation | Plans and executes strategies |
# MAGIC | **Error Handling** | Returns error message | Can retry with different approach |
# MAGIC | **Learning** | Static after training | Adapts based on outcomes |
# MAGIC | **Autonomy** | Fully directed by prompt | Semi-autonomous task completion |
# MAGIC | **Use Cases** | Q&A, classification, generation | Research, analysis, automation |

# COMMAND ----------

# Comparison Example: Traditional vs Agentic Approach

print("TRADITIONAL AI APPROACH")
print("=" * 60)
print("""
User: "Analyze Q4 sales and send report to executives"

Traditional LLM Response:
"I don't have access to your sales data or email system. 
I can provide guidance on how to perform this analysis:
1. Export Q4 sales data from your database
2. Use Excel or BI tools to create visualizations
3. Write an executive summary
4. Email using your email client
"
""")

print("\n" + "=" * 60)
print("\nAGENTIC AI APPROACH")
print("=" * 60)
print("""
User: "Analyze Q4 sales and send report to executives"

Agent Execution:

Thought: I need to gather Q4 sales data first
Action: query_database("SELECT * FROM sales WHERE quarter = 'Q4'")
Observation: Retrieved 15,432 records, $2.3M total revenue

Thought: Now analyze for key insights
Action: calculate_metrics(data)
Observation: 20% growth, West region +35%, East region -5%

Thought: Create visualizations
Action: generate_charts(data, metrics)
Observation: Created 4 charts: trend, regional, product, comparison

Thought: Write executive summary
Action: generate_summary(data, metrics, charts)
Observation: Summary created with key findings

Thought: Format and send report
Action: send_email(
    to="executives@company.com",
    subject="Q4 Sales Analysis",
    body=report,
    attachments=[charts]
)
Observation: Email sent successfully

Final Response: "I've completed the Q4 sales analysis and sent 
the report to executives. Key highlights: 20% growth to $2.3M, 
West region leading with 35% increase, East region needs attention 
with 5% decline."
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Real-World Agent Applications
# MAGIC
# MAGIC Examples of how agents are used in practice:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Service Agent
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Query customer database
# MAGIC - Search knowledge base
# MAGIC - Check order status
# MAGIC - Process returns/refunds
# MAGIC - Escalate to human when needed
# MAGIC
# MAGIC **Example Flow:**
# MAGIC ```
# MAGIC Customer: "I want to return my order #12345"
# MAGIC
# MAGIC Agent:
# MAGIC 1. Query order database → Found order
# MAGIC 2. Check return policy → Eligible (within 30 days)
# MAGIC 3. Generate return label → Created
# MAGIC 4. Process refund → Initiated ($49.99)
# MAGIC 5. Send confirmation email → Sent
# MAGIC
# MAGIC Response: "I've processed your return for order #12345. 
# MAGIC Your return label has been emailed, and you'll receive 
# MAGIC your $49.99 refund within 5-7 business days."
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Analysis Agent
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Generate and execute SQL queries
# MAGIC - Perform statistical analysis
# MAGIC - Create visualizations
# MAGIC - Identify trends and anomalies
# MAGIC - Generate insights and recommendations
# MAGIC
# MAGIC **Example Flow:**
# MAGIC ```
# MAGIC User: "Which products are underperforming this quarter?"
# MAGIC
# MAGIC Agent:
# MAGIC 1. Query sales data → Retrieved all product sales
# MAGIC 2. Calculate benchmarks → Identified performance thresholds
# MAGIC 3. Identify underperformers → Found 8 products below target
# MAGIC 4. Analyze reasons → Examined price, competition, seasonality
# MAGIC 5. Generate recommendations → Proposed 3 action items
# MAGIC
# MAGIC Response: "Found 8 underperforming products representing 
# MAGIC $400K in missed revenue. Main issues: pricing misalignment 
# MAGIC (3 products), increased competition (4 products), seasonal 
# MAGIC decline (1 product). Recommendations: [detailed actions]"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Research Assistant Agent
# MAGIC
# MAGIC **Capabilities:**
# MAGIC - Search multiple data sources
# MAGIC - Synthesize information
# MAGIC - Verify facts
# MAGIC - Track sources
# MAGIC - Generate comprehensive reports
# MAGIC
# MAGIC **Example Flow:**
# MAGIC ```
# MAGIC User: "Research emerging trends in renewable energy"
# MAGIC
# MAGIC Agent:
# MAGIC 1. Search academic databases → 150 relevant papers
# MAGIC 2. Search news sources → 50 recent articles
# MAGIC 3. Query industry reports → 12 market analyses
# MAGIC 4. Identify key trends → Found 5 major trends
# MAGIC 5. Synthesize findings → Created comprehensive report
# MAGIC 6. Cite sources → Added 30 references
# MAGIC
# MAGIC Response: "Completed research on renewable energy trends. 
# MAGIC Key findings: 1) Solar+storage integration accelerating, 
# MAGIC 2) Green hydrogen emerging as solution for heavy industry,
# MAGIC 3) Offshore wind capacity doubling by 2027... [full report 
# MAGIC with citations]"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC ✅ **AI Agents are autonomous systems** that can perceive, reason, act, and learn
# MAGIC
# MAGIC ✅ **Agent Architecture** involves a continuous loop of reasoning, tool use, and observation
# MAGIC
# MAGIC ✅ **Reasoning Patterns** include:
# MAGIC - **ReAct**: Interleaving thinking and acting (most common)
# MAGIC - **Chain-of-Thought**: Breaking down complex problems
# MAGIC - **Tree-of-Thought**: Exploring multiple approaches
# MAGIC - **Reflection**: Self-critique and improvement
# MAGIC
# MAGIC ✅ **Core Components** every agent needs:
# MAGIC - **Tools**: Functions for interacting with the world
# MAGIC - **Memory**: Short-term, long-term, and working memory
# MAGIC - **Planning**: Task decomposition and sequencing
# MAGIC - **Guardrails**: Safety and control mechanisms
# MAGIC
# MAGIC ✅ **Agents vs Traditional AI**: Agents can execute multi-step tasks autonomously using tools

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Next Steps
# MAGIC
# MAGIC Now that you understand agent foundations, you're ready to build agents on Databricks!
# MAGIC
# MAGIC **Continue to the next lecture:**
# MAGIC - **01_Lecture_Unity_Catalog_Functions_as_Tools**: Learn how to create tools for agents using Unity Catalog
# MAGIC
# MAGIC **Key concepts to remember:**
# MAGIC 1. Agents use reasoning patterns to solve problems
# MAGIC 2. Tools extend agent capabilities
# MAGIC 3. Memory enables context and learning
# MAGIC 4. Planning breaks complex tasks into steps
# MAGIC 5. Guardrails ensure safe operation
# MAGIC
# MAGIC **Additional Resources:**
# MAGIC - [ReAct Paper](https://arxiv.org/abs/2210.03629)
# MAGIC - [LangChain Agent Documentation](https://python.langchain.com/docs/modules/agents/)
# MAGIC - [Databricks AI Functions](https://docs.databricks.com/ai-functions/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this lecture, we covered:
# MAGIC
# MAGIC - ✅ What AI agents are and their core capabilities
# MAGIC - ✅ Agent architecture and execution flow
# MAGIC - ✅ Four key reasoning patterns (ReAct, CoT, ToT, Reflection)
# MAGIC - ✅ Essential agent components (Tools, Memory, Planning, Guardrails)
# MAGIC - ✅ How agents differ from traditional AI systems
# MAGIC - ✅ Real-world agent applications
# MAGIC
# MAGIC You now have a solid foundation in agentic AI concepts and are ready to start building agents on Databricks!
