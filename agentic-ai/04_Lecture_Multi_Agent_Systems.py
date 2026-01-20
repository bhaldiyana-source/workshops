# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture: Multi-Agent Systems
# MAGIC
# MAGIC ## Overview
# MAGIC This lecture explores multi-agent systems where multiple specialized agents collaborate to solve complex problems. You'll learn different multi-agent patterns, orchestration strategies, and how to build systems where agents work together effectively.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Understand why multi-agent systems are beneficial
# MAGIC - Identify different multi-agent patterns (Sequential, Hierarchical, Collaborative, Competitive)
# MAGIC - Design agent collaboration strategies
# MAGIC - Implement multi-agent orchestration
# MAGIC - Handle inter-agent communication
# MAGIC
# MAGIC ## Duration
# MAGIC 75 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed previous lectures on agent foundations and frameworks
# MAGIC - Understanding of single-agent systems
# MAGIC - Familiarity with LangChain or similar frameworks
# MAGIC - Knowledge of async programming concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Why Multi-Agent Systems?
# MAGIC
# MAGIC Single agents have limitations that multi-agent systems can overcome.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Limitations of Single Agents
# MAGIC
# MAGIC **Complexity Overload**
# MAGIC - Single prompt trying to do everything
# MAGIC - Difficult to maintain and debug
# MAGIC - Context window limitations
# MAGIC
# MAGIC **Lack of Specialization**
# MAGIC - Jack of all trades, master of none
# MAGIC - Can't optimize for specific tasks
# MAGIC - Generic prompts less effective
# MAGIC
# MAGIC **Sequential Execution**
# MAGIC - Can't work on multiple tasks simultaneously
# MAGIC - Inefficient for parallel work
# MAGIC - Long execution times
# MAGIC
# MAGIC **Difficult Scaling**
# MAGIC - Adding capabilities makes agent more complex
# MAGIC - Hard to test and validate
# MAGIC - Brittle prompts

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Benefits of Multi-Agent Systems
# MAGIC
# MAGIC ✅ **Specialization**
# MAGIC - Each agent has specific expertise
# MAGIC - Optimized prompts per domain
# MAGIC - Better performance on focused tasks
# MAGIC
# MAGIC ✅ **Decomposition**
# MAGIC - Break complex problems into subtasks
# MAGIC - Manage complexity through division
# MAGIC - Easier to reason about
# MAGIC
# MAGIC ✅ **Parallel Execution**
# MAGIC - Multiple agents work simultaneously
# MAGIC - Faster overall completion
# MAGIC - Better resource utilization
# MAGIC
# MAGIC ✅ **Modularity**
# MAGIC - Easy to update individual agents
# MAGIC - Add/remove capabilities
# MAGIC - Independent testing
# MAGIC
# MAGIC ✅ **Scalability**
# MAGIC - Scale specific agents based on need
# MAGIC - Add agents without redesigning system
# MAGIC - Flexible architecture

# COMMAND ----------

# Example: Single agent vs Multi-agent comparison
def single_agent_workflow():
    """Single agent handling everything"""
    return """
SINGLE AGENT WORKFLOW
========================================

User: "Research Q4 trends, analyze our data, and write a report"

Agent (trying to do everything):
1. Searching for industry trends... (using web search)
2. Querying our sales data... (using SQL)
3. Performing statistical analysis... (using Python)
4. Creating visualizations... (using plotting)
5. Writing executive summary... (text generation)
6. Formatting final report... (document creation)

Issues:
❌ Long, complex prompt
❌ Sequential execution (slow)
❌ If one step fails, whole task fails
❌ Hard to optimize each step
❌ Difficult to debug problems
"""

def multi_agent_workflow():
    """Multiple specialized agents"""
    return """
MULTI-AGENT WORKFLOW
========================================

User: "Research Q4 trends, analyze our data, and write a report"

Supervisor Agent: "I'll coordinate three specialized agents"

[PARALLEL EXECUTION]
├─> Research Agent:
│   "Finding industry trends and market data"
│   ✓ Specialized in information gathering
│
├─> Analysis Agent:
│   "Querying and analyzing our sales data"
│   ✓ Specialized in data analysis
│
└─> ... (other work happening concurrently)

[SYNTHESIS]
Writing Agent: "Combining insights into polished report"
✓ Specialized in content generation

Benefits:
✅ Specialized, focused agents
✅ Parallel execution (faster)
✅ Isolated failures don't stop everything
✅ Each agent optimized for its task
✅ Easy to debug and improve
"""

print(single_agent_workflow())
print("\n" + "="*60 + "\n")
print(multi_agent_workflow())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Multi-Agent Patterns
# MAGIC
# MAGIC Different architectures for agent collaboration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Sequential Pattern
# MAGIC
# MAGIC **Agents work in sequence, passing results forward**
# MAGIC
# MAGIC ```
# MAGIC User Input → Agent A → Agent B → Agent C → Final Output
# MAGIC ```
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Clear pipeline of operations
# MAGIC - Each step depends on previous
# MAGIC - Linear workflow
# MAGIC
# MAGIC **Example: Content Creation Pipeline**
# MAGIC ```
# MAGIC Research Agent → Analysis Agent → Writing Agent → Editor Agent
# MAGIC     ↓                ↓                ↓               ↓
# MAGIC  Find data      Analyze data    Draft content   Polish output
# MAGIC ```
# MAGIC
# MAGIC **Pros:**
# MAGIC - Simple to understand
# MAGIC - Clear data flow
# MAGIC - Easy to debug
# MAGIC
# MAGIC **Cons:**
# MAGIC - Sequential (no parallelism)
# MAGIC - One failure stops pipeline
# MAGIC - Can be slow for long chains

# COMMAND ----------

# Example: Sequential multi-agent system
class SequentialAgentSystem:
    """Sequential multi-agent pipeline"""
    
    def __init__(self):
        self.agents = []
        self.results = []
    
    def add_agent(self, name: str, handler):
        """Add agent to pipeline"""
        self.agents.append({"name": name, "handler": handler})
    
    def execute(self, input_data: str):
        """Execute pipeline sequentially"""
        current_input = input_data
        
        print(f"Sequential Pipeline Execution")
        print("=" * 60)
        print(f"Initial Input: {input_data}\n")
        
        for i, agent in enumerate(self.agents, 1):
            print(f"Step {i}: {agent['name']}")
            print("-" * 60)
            
            # Execute agent
            result = agent['handler'](current_input)
            self.results.append({
                "agent": agent['name'],
                "input": current_input,
                "output": result
            })
            
            print(f"Output: {result[:100]}...")
            print()
            
            # Output becomes input for next agent
            current_input = result
        
        print("=" * 60)
        print(f"✓ Pipeline completed with {len(self.agents)} agents")
        
        return current_input

# Create sequential system
sequential_system = SequentialAgentSystem()

# Define agent handlers
def research_agent(input_text):
    return f"Research findings on: {input_text}\n- Key trend 1\n- Key trend 2\n- Key trend 3"

def analysis_agent(input_text):
    return f"Analysis of data:\n{input_text}\nInsight: Strong growth in Q4"

def writing_agent(input_text):
    return f"Report:\n{input_text}\nConclusion: Positive outlook for next quarter"

# Add agents to pipeline
sequential_system.add_agent("Research Agent", research_agent)
sequential_system.add_agent("Analysis Agent", analysis_agent)
sequential_system.add_agent("Writing Agent", writing_agent)

# Execute
final_output = sequential_system.execute("Q4 2024 sales performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Hierarchical Pattern
# MAGIC
# MAGIC **Supervisor agent coordinates specialized worker agents**
# MAGIC
# MAGIC ```
# MAGIC               Supervisor Agent
# MAGIC              (orchestrates work)
# MAGIC                     |
# MAGIC        +------------+------------+
# MAGIC        |            |            |
# MAGIC    Agent A      Agent B      Agent C
# MAGIC   (research)   (analysis)    (writing)
# MAGIC ```
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Need dynamic task routing
# MAGIC - Different agents for different subtasks
# MAGIC - Supervisor makes decisions
# MAGIC
# MAGIC **Example: Business Intelligence System**
# MAGIC ```
# MAGIC Supervisor: Breaks down complex questions
# MAGIC    ├─> Data Agent: Queries databases
# MAGIC    ├─> Analysis Agent: Runs calculations
# MAGIC    ├─> Visualization Agent: Creates charts
# MAGIC    └─> Synthesis: Combines all results
# MAGIC ```
# MAGIC
# MAGIC **Pros:**
# MAGIC - Flexible task routing
# MAGIC - Can skip unnecessary agents
# MAGIC - Supervisor provides oversight
# MAGIC
# MAGIC **Cons:**
# MAGIC - Supervisor is single point of failure
# MAGIC - Still largely sequential
# MAGIC - Supervisor needs good decision-making

# COMMAND ----------

# Example: Hierarchical multi-agent system
from typing import List, Dict
from enum import Enum

class AgentType(Enum):
    RESEARCH = "research"
    ANALYSIS = "analysis"
    WRITING = "writing"

class HierarchicalAgentSystem:
    """Hierarchical multi-agent with supervisor"""
    
    def __init__(self):
        self.supervisor = SupervisorAgent()
        self.workers = {
            AgentType.RESEARCH: ResearchAgent(),
            AgentType.ANALYSIS: AnalysisAgent(),
            AgentType.WRITING: WritingAgent()
        }
    
    def execute(self, task: str):
        """Execute task using hierarchical coordination"""
        print("Hierarchical Multi-Agent System")
        print("=" * 60)
        print(f"Task: {task}\n")
        
        # Supervisor creates plan
        plan = self.supervisor.create_plan(task)
        print("Supervisor Plan:")
        for i, step in enumerate(plan, 1):
            print(f"  {i}. {step['description']} (Agent: {step['agent'].value})")
        print()
        
        # Execute plan
        context = {}
        for i, step in enumerate(plan, 1):
            print(f"Step {i}: {step['description']}")
            print("-" * 60)
            
            # Get appropriate worker
            agent = self.workers[step['agent']]
            
            # Execute step
            result = agent.execute(step['instruction'], context)
            context[step['agent']] = result
            
            print(f"Result: {result[:80]}...")
            print()
        
        # Supervisor synthesizes final answer
        final_answer = self.supervisor.synthesize(context)
        
        print("=" * 60)
        print(f"✓ Task completed\n")
        print(f"Final Answer:\n{final_answer}")
        
        return final_answer

class SupervisorAgent:
    """Supervisor that coordinates workers"""
    
    def create_plan(self, task: str) -> List[Dict]:
        """Break down task into steps"""
        # Simplified planning logic
        return [
            {
                "description": "Gather relevant information",
                "agent": AgentType.RESEARCH,
                "instruction": f"Research: {task}"
            },
            {
                "description": "Analyze the data",
                "agent": AgentType.ANALYSIS,
                "instruction": f"Analyze findings for: {task}"
            },
            {
                "description": "Create comprehensive report",
                "agent": AgentType.WRITING,
                "instruction": f"Write report on: {task}"
            }
        ]
    
    def synthesize(self, context: Dict) -> str:
        """Combine results from all agents"""
        return f"Final synthesis combining:\n" + \
               f"- Research insights\n" + \
               f"- Analysis findings\n" + \
               f"- Professional writing"

class ResearchAgent:
    def execute(self, instruction: str, context: Dict) -> str:
        return f"Research completed: Found 5 key trends and 10 data points"

class AnalysisAgent:
    def execute(self, instruction: str, context: Dict) -> str:
        return f"Analysis shows 23% growth with strong Q4 performance"

class WritingAgent:
    def execute(self, instruction: str, context: Dict) -> str:
        return f"Report draft created with executive summary and detailed findings"

# Demonstrate hierarchical system
hierarchical_system = HierarchicalAgentSystem()
hierarchical_system.execute("Analyze our Q4 2024 sales performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Collaborative Pattern
# MAGIC
# MAGIC **Agents work as peers, sharing information**
# MAGIC
# MAGIC ```
# MAGIC     Agent A ←→ Agent B
# MAGIC        ↕          ↕
# MAGIC     Agent C ←→ Agent D
# MAGIC
# MAGIC All agents can communicate
# MAGIC ```
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Need peer-to-peer collaboration
# MAGIC - Agents must share insights
# MAGIC - Iterative problem solving
# MAGIC
# MAGIC **Example: Product Development Team**
# MAGIC ```
# MAGIC Engineering Agent ←→ Design Agent
# MAGIC       ↕                    ↕
# MAGIC Business Agent  ←→ Marketing Agent
# MAGIC
# MAGIC All agents contribute to product decisions
# MAGIC ```
# MAGIC
# MAGIC **Pros:**
# MAGIC - Rich collaboration
# MAGIC - Agents learn from each other
# MAGIC - Flexible communication
# MAGIC
# MAGIC **Cons:**
# MAGIC - Complex coordination
# MAGIC - Can be chatty (many messages)
# MAGIC - Need termination conditions

# COMMAND ----------

# Example: Collaborative agent system
import asyncio
from datetime import datetime

class Message:
    """Message between agents"""
    def __init__(self, sender: str, content: str, recipients: List[str] = None):
        self.sender = sender
        self.content = content
        self.recipients = recipients or ["all"]
        self.timestamp = datetime.now()

class CollaborativeAgent:
    """Agent that can collaborate with peers"""
    
    def __init__(self, name: str, expertise: str):
        self.name = name
        self.expertise = expertise
        self.messages_received = []
        self.shared_context = {}
    
    async def receive_message(self, message: Message):
        """Receive message from another agent"""
        self.messages_received.append(message)
        
        # Update shared context based on message
        if message.sender not in self.shared_context:
            self.shared_context[message.sender] = []
        self.shared_context[message.sender].append(message.content)
    
    async def contribute(self, task: str) -> Message:
        """Contribute to solving the task"""
        # Consider input from other agents
        context_summary = self.summarize_context()
        
        # Generate contribution based on expertise
        contribution = f"[{self.name}] {self.expertise} perspective: "
        contribution += f"Based on input from {len(self.shared_context)} agents, "
        contribution += f"my recommendation is..."
        
        return Message(self.name, contribution)
    
    def summarize_context(self) -> str:
        """Summarize what agent has learned from others"""
        return f"Insights from {len(self.shared_context)} collaborators"

class CollaborativeAgentSystem:
    """System where agents collaborate as peers"""
    
    def __init__(self):
        self.agents = []
        self.message_history = []
    
    def add_agent(self, agent: CollaborativeAgent):
        """Add agent to collaborative system"""
        self.agents.append(agent)
    
    async def collaborate(self, task: str, rounds: int = 3):
        """Run collaborative problem solving"""
        print("Collaborative Multi-Agent System")
        print("=" * 60)
        print(f"Task: {task}")
        print(f"Agents: {len(self.agents)} collaborators\n")
        
        for round_num in range(rounds):
            print(f"Round {round_num + 1}:")
            print("-" * 60)
            
            # Each agent contributes
            round_messages = []
            for agent in self.agents:
                message = await agent.contribute(task)
                round_messages.append(message)
                print(f"{agent.name}: {message.content[:60]}...")
            
            # Broadcast messages to all agents
            for agent in self.agents:
                for message in round_messages:
                    if message.sender != agent.name:
                        await agent.receive_message(message)
            
            self.message_history.extend(round_messages)
            print()
        
        # Synthesize final decision
        print("=" * 60)
        print(f"✓ Collaboration complete after {rounds} rounds")
        print(f"Total messages exchanged: {len(self.message_history)}")
        
        return self.synthesize_decision()
    
    def synthesize_decision(self) -> str:
        """Combine insights from all agents"""
        return f"Collaborative decision incorporating perspectives from all {len(self.agents)} agents"

# Create collaborative system
collab_system = CollaborativeAgentSystem()

# Add specialized agents
collab_system.add_agent(CollaborativeAgent("Data Agent", "Data analysis"))
collab_system.add_agent(CollaborativeAgent("ML Agent", "Machine learning"))
collab_system.add_agent(CollaborativeAgent("Business Agent", "Business strategy"))

# Run collaboration
async def run_collaboration():
    result = await collab_system.collaborate("Build a customer churn prediction system", rounds=2)
    return result

# Execute (using event loop)
import nest_asyncio
nest_asyncio.apply()
result = asyncio.run(run_collaboration())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Competitive Pattern
# MAGIC
# MAGIC **Multiple agents propose solutions, best one selected**
# MAGIC
# MAGIC ```
# MAGIC        User Input
# MAGIC       /    |    \
# MAGIC   Agent  Agent  Agent
# MAGIC     A      B      C
# MAGIC       \    |    /
# MAGIC       Evaluator
# MAGIC           ↓
# MAGIC    Best Solution
# MAGIC ```
# MAGIC
# MAGIC **When to Use:**
# MAGIC - Need diverse approaches
# MAGIC - Quality over consensus
# MAGIC - Have evaluation criteria
# MAGIC
# MAGIC **Example: Strategic Planning**
# MAGIC ```
# MAGIC Three agents propose strategies:
# MAGIC - Conservative Agent: Low-risk approach
# MAGIC - Aggressive Agent: High-growth approach
# MAGIC - Balanced Agent: Middle ground
# MAGIC
# MAGIC Evaluator picks best based on criteria
# MAGIC ```
# MAGIC
# MAGIC **Pros:**
# MAGIC - Diverse solutions
# MAGIC - Best idea wins
# MAGIC - Parallel generation
# MAGIC
# MAGIC **Cons:**
# MAGIC - Wasteful (discards work)
# MAGIC - Need good evaluation
# MAGIC - More expensive

# COMMAND ----------

# Example: Competitive multi-agent system
from typing import Tuple

class CompetitiveAgent:
    """Agent that proposes solutions competitively"""
    
    def __init__(self, name: str, strategy: str):
        self.name = name
        self.strategy = strategy
    
    async def propose_solution(self, problem: str) -> Dict:
        """Propose solution based on agent's strategy"""
        # Simulate solution generation
        proposal = {
            "agent": self.name,
            "strategy": self.strategy,
            "solution": f"{self.strategy} solution for: {problem}",
            "estimated_cost": 0,
            "estimated_time": 0,
            "risk_level": "",
            "expected_return": 0
        }
        
        # Different strategies have different profiles
        if "conservative" in self.strategy.lower():
            proposal.update({
                "estimated_cost": 100000,
                "estimated_time": 6,
                "risk_level": "Low",
                "expected_return": 150000
            })
        elif "aggressive" in self.strategy.lower():
            proposal.update({
                "estimated_cost": 500000,
                "estimated_time": 3,
                "risk_level": "High",
                "expected_return": 1000000
            })
        else:  # balanced
            proposal.update({
                "estimated_cost": 250000,
                "estimated_time": 4,
                "risk_level": "Medium",
                "expected_return": 500000
            })
        
        return proposal

class SolutionEvaluator:
    """Evaluates and selects best solution"""
    
    def __init__(self, criteria_weights: Dict[str, float]):
        self.criteria_weights = criteria_weights
    
    def evaluate(self, proposals: List[Dict]) -> Tuple[Dict, List[Dict]]:
        """Score proposals and select best"""
        scored_proposals = []
        
        for proposal in proposals:
            score = self.calculate_score(proposal)
            proposal['score'] = score
            scored_proposals.append(proposal)
        
        # Sort by score
        scored_proposals.sort(key=lambda x: x['score'], reverse=True)
        
        return scored_proposals[0], scored_proposals
    
    def calculate_score(self, proposal: Dict) -> float:
        """Calculate score based on weighted criteria"""
        # Normalize values
        roi = proposal['expected_return'] / proposal['estimated_cost']
        time_score = 1.0 / proposal['estimated_time']  # Faster is better
        risk_score = {"Low": 1.0, "Medium": 0.6, "High": 0.3}[proposal['risk_level']]
        
        # Apply weights
        score = (
            self.criteria_weights.get('roi', 0.4) * roi +
            self.criteria_weights.get('speed', 0.3) * time_score +
            self.criteria_weights.get('risk', 0.3) * risk_score
        )
        
        return score

class CompetitiveAgentSystem:
    """System where agents compete with solutions"""
    
    def __init__(self, evaluator: SolutionEvaluator):
        self.agents = []
        self.evaluator = evaluator
    
    def add_agent(self, agent: CompetitiveAgent):
        """Add competing agent"""
        self.agents.append(agent)
    
    async def solve(self, problem: str):
        """Have agents compete to solve problem"""
        print("Competitive Multi-Agent System")
        print("=" * 60)
        print(f"Problem: {problem}")
        print(f"Competing Agents: {len(self.agents)}\n")
        
        # Gather proposals (in parallel)
        print("Phase 1: Gathering Proposals")
        print("-" * 60)
        proposals = []
        for agent in self.agents:
            proposal = await agent.propose_solution(problem)
            proposals.append(proposal)
            print(f"{agent.name}:")
            print(f"  Strategy: {proposal['strategy']}")
            print(f"  Cost: ${proposal['estimated_cost']:,}")
            print(f"  Time: {proposal['estimated_time']} months")
            print(f"  Risk: {proposal['risk_level']}")
            print(f"  ROI: {proposal['expected_return']/proposal['estimated_cost']:.1f}x")
            print()
        
        # Evaluate proposals
        print("Phase 2: Evaluation")
        print("-" * 60)
        best, all_scored = self.evaluator.evaluate(proposals)
        
        for i, proposal in enumerate(all_scored, 1):
            status = "★ SELECTED" if proposal == best else ""
            print(f"{i}. {proposal['agent']}: Score {proposal['score']:.2f} {status}")
        
        print("\n" + "=" * 60)
        print(f"✓ Winner: {best['agent']} ({best['strategy']})")
        print(f"Score: {best['score']:.2f}")
        
        return best

# Create competitive system
evaluator = SolutionEvaluator({
    'roi': 0.5,      # 50% weight on return on investment
    'speed': 0.3,    # 30% weight on speed
    'risk': 0.2      # 20% weight on risk aversion
})

competitive_system = CompetitiveAgentSystem(evaluator)

# Add competing agents
competitive_system.add_agent(CompetitiveAgent("Conservative Agent", "Conservative, low-risk"))
competitive_system.add_agent(CompetitiveAgent("Aggressive Agent", "Aggressive, high-reward"))
competitive_system.add_agent(CompetitiveAgent("Balanced Agent", "Balanced approach"))

# Run competition
async def run_competition():
    result = await competitive_system.solve("Expand to European market")
    return result

winner = asyncio.run(run_competition())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agent Communication Protocols
# MAGIC
# MAGIC How agents share information effectively.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Message Types
# MAGIC
# MAGIC **Request**
# MAGIC - Agent asks another for help
# MAGIC - Contains task specification
# MAGIC - Expects response
# MAGIC
# MAGIC **Response**
# MAGIC - Reply to a request
# MAGIC - Contains result or data
# MAGIC - References original request
# MAGIC
# MAGIC **Notification**
# MAGIC - Broadcast information
# MAGIC - No response expected
# MAGIC - Updates shared state
# MAGIC
# MAGIC **Query**
# MAGIC - Ask for information
# MAGIC - Read-only operation
# MAGIC - Quick response expected

# COMMAND ----------

# Example: Message protocol implementation
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Any
from datetime import datetime
import uuid

class MessageType(Enum):
    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"
    QUERY = "query"

@dataclass
class AgentMessage:
    """Standard message format for inter-agent communication"""
    message_type: MessageType
    sender: str
    receiver: str
    content: Any
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_id: Optional[str] = None  # For tracking request-response
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict = field(default_factory=dict)
    
    def create_response(self, content: Any) -> 'AgentMessage':
        """Create response message to this message"""
        return AgentMessage(
            message_type=MessageType.RESPONSE,
            sender=self.receiver,  # Swap sender/receiver
            receiver=self.sender,
            content=content,
            parent_id=self.message_id
        )

class MessageBus:
    """Central message bus for agent communication"""
    
    def __init__(self):
        self.subscribers = {}
        self.message_history = []
    
    def subscribe(self, agent_name: str, handler):
        """Register agent to receive messages"""
        self.subscribers[agent_name] = handler
        print(f"Agent '{agent_name}' subscribed to message bus")
    
    async def send(self, message: AgentMessage):
        """Route message to recipient"""
        self.message_history.append(message)
        
        # Broadcast to all if receiver is "all"
        if message.receiver == "all":
            for name, handler in self.subscribers.items():
                if name != message.sender:
                    await handler(message)
        # Send to specific recipient
        elif message.receiver in self.subscribers:
            handler = self.subscribers[message.receiver]
            await handler(message)
        else:
            print(f"Warning: No subscriber for '{message.receiver}'")
    
    def get_conversation(self, agent_name: str) -> List[AgentMessage]:
        """Get conversation history for an agent"""
        return [
            m for m in self.message_history
            if m.sender == agent_name or m.receiver == agent_name
        ]

# Demonstrate message protocol
print("Agent Communication Protocol")
print("=" * 60)

message_bus = MessageBus()

# Create sample messages
msg1 = AgentMessage(
    message_type=MessageType.REQUEST,
    sender="AnalysisAgent",
    receiver="DataAgent",
    content={"action": "query", "table": "sales", "filters": {"region": "West"}}
)

print(f"\n1. Request Message:")
print(f"   From: {msg1.sender} → To: {msg1.receiver}")
print(f"   Type: {msg1.message_type.value}")
print(f"   Content: {msg1.content}")
print(f"   ID: {msg1.message_id[:8]}...")

msg2 = msg1.create_response(
    content={"rows": 1234, "total_revenue": 2500000, "status": "success"}
)

print(f"\n2. Response Message:")
print(f"   From: {msg2.sender} → To: {msg2.receiver}")
print(f"   Type: {msg2.message_type.value}")
print(f"   Parent: {msg2.parent_id[:8]}...")
print(f"   Content: {msg2.content}")

msg3 = AgentMessage(
    message_type=MessageType.NOTIFICATION,
    sender="SupervisorAgent",
    receiver="all",
    content={"event": "task_completed", "task_id": "task_123"}
)

print(f"\n3. Notification Message:")
print(f"   From: {msg3.sender} → To: {msg3.receiver}")
print(f"   Type: {msg3.message_type.value}")
print(f"   Content: {msg3.content}")

print("\n" + "=" * 60)
print("✓ Structured communication enables reliable collaboration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC ✅ **Multi-agent systems overcome single-agent limitations**
# MAGIC - Specialization improves performance
# MAGIC - Decomposition manages complexity
# MAGIC - Parallel execution increases speed
# MAGIC - Modularity enables scalability
# MAGIC
# MAGIC ✅ **Four main patterns for multi-agent collaboration**
# MAGIC - Sequential: Linear pipeline
# MAGIC - Hierarchical: Supervisor + workers
# MAGIC - Collaborative: Peer-to-peer
# MAGIC - Competitive: Best solution wins
# MAGIC
# MAGIC ✅ **Communication protocols are essential**
# MAGIC - Structured message formats
# MAGIC - Message types (request, response, notification)
# MAGIC - Central message bus for routing
# MAGIC - Conversation tracking
# MAGIC
# MAGIC ✅ **Pattern choice depends on requirements**
# MAGIC - Sequential: Clear pipeline
# MAGIC - Hierarchical: Dynamic routing
# MAGIC - Collaborative: Rich interaction
# MAGIC - Competitive: Multiple approaches

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Next Steps
# MAGIC
# MAGIC You now understand multi-agent systems!
# MAGIC
# MAGIC **Continue to:**
# MAGIC - **05_Lecture_RAG_for_Agents**: Learn about RAG integration
# MAGIC - **12_Demo_Multi_Agent_System**: Build a hierarchical system
# MAGIC - **20_Lab_Multi_Agent_Workflow**: Create coordinated agents
# MAGIC
# MAGIC **Practice by:**
# MAGIC 1. Design a multi-agent system for your use case
# MAGIC 2. Choose appropriate pattern
# MAGIC 3. Implement communication protocol
# MAGIC
# MAGIC **Additional Resources:**
# MAGIC - [LangGraph Documentation](https://python.langchain.com/docs/langgraph)
# MAGIC - [Multi-Agent Systems Papers](https://arxiv.org/search/?query=multi-agent+systems)
# MAGIC - [AutoGen Framework](https://microsoft.github.io/autogen/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC In this lecture, we covered:
# MAGIC
# MAGIC - ✅ Why multi-agent systems are beneficial
# MAGIC - ✅ Four multi-agent patterns (Sequential, Hierarchical, Collaborative, Competitive)
# MAGIC - ✅ Agent orchestration strategies
# MAGIC - ✅ Communication protocols for agents
# MAGIC - ✅ Pattern selection guide
# MAGIC
# MAGIC You're now ready to design and build sophisticated multi-agent systems!
