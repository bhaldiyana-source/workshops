# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building a LangChain Agent
# MAGIC
# MAGIC ## Overview
# MAGIC In this hands-on lab, you'll build your own AI agent using LangChain on Databricks. You'll create Unity Catalog functions, configure an agent, and test it with real scenarios. This lab reinforces the concepts from the previous demos and gives you practical experience building production-ready agents.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Create Unity Catalog SQL functions from business requirements
# MAGIC - Configure a LangChain agent with appropriate parameters
# MAGIC - Integrate UC functions as tools using UCFunctionToolkit
# MAGIC - Test and debug agent behavior
# MAGIC - Analyze agent performance using MLflow traces
# MAGIC - Optimize agent configuration for your use case
# MAGIC
# MAGIC ## Lab Scenario
# MAGIC
# MAGIC You work for **HealthCare Plus**, a healthcare service provider. You need to build an AI agent that can:
# MAGIC - Look up patient appointment information
# MAGIC - Check doctor availability
# MAGIC - Retrieve prescription details
# MAGIC - Answer questions about healthcare services
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1 and 2
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Access to Databricks Model Serving
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC Complete each section marked with **TODO**. Reference the demo notebooks if you need help. Try to solve challenges independently first, then check the hints if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Install Required Packages
# MAGIC
# MAGIC Run this cell to install necessary dependencies.

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet databricks-agents langchain langchain-community mlflow databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC **TODO 1:** Set up your workspace parameters.

# COMMAND ----------

# TODO: Complete this configuration section

import os
from pyspark.sql import SparkSession

# Get current user
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0].replace(".", "_")

# TODO: Set your catalog name (use "main" or your catalog)
CATALOG_NAME = "main"  # TODO: Update if needed

# TODO: Create a unique schema name for your lab
SCHEMA_NAME = f"{username}_healthcare_lab"  # TODO: Update if you prefer a different name

# TODO: Get your warehouse ID from SQL Warehouses page
WAREHOUSE_ID = "YOUR_WAREHOUSE_ID"  # TODO: IMPORTANT - Update this!

print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")
print(f"Warehouse: {WAREHOUSE_ID}")

# Verify configuration
assert WAREHOUSE_ID != "YOUR_WAREHOUSE_ID", "❌ Please update WAREHOUSE_ID with your actual warehouse ID!"
print("✓ Configuration complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample Healthcare Data
# MAGIC
# MAGIC We'll create three tables: patients, appointments, and prescriptions.

# COMMAND ----------

# Create the schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_NAME}")

print(f"✓ Using schema: {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Patients Table

# COMMAND ----------

# Create patients table
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.patients (
  patient_id STRING,
  name STRING,
  age INT,
  insurance_type STRING,
  primary_doctor STRING,
  phone STRING
)
""")

# Insert sample data
patients_data = [
    ("P001", "John Smith", 45, "Premium", "Dr. Williams", "555-0101"),
    ("P002", "Sarah Johnson", 32, "Basic", "Dr. Chen", "555-0102"),
    ("P003", "Michael Brown", 58, "Premium", "Dr. Williams", "555-0103"),
    ("P004", "Emily Davis", 28, "Standard", "Dr. Patel", "555-0104"),
    ("P005", "Robert Wilson", 67, "Premium", "Dr. Chen", "555-0105"),
]

spark.createDataFrame(
    patients_data,
    ["patient_id", "name", "age", "insurance_type", "primary_doctor", "phone"]
).write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.patients")

print("✓ Patients table created")
spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.patients").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Appointments Table

# COMMAND ----------

# Create appointments table
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.appointments (
  appointment_id STRING,
  patient_id STRING,
  doctor_name STRING,
  appointment_date DATE,
  appointment_time STRING,
  reason STRING,
  status STRING
)
""")

# Insert sample data
appointments_data = [
    ("A001", "P001", "Dr. Williams", "2025-01-15", "10:00 AM", "Annual Checkup", "scheduled"),
    ("A002", "P001", "Dr. Williams", "2024-12-20", "2:00 PM", "Follow-up", "completed"),
    ("A003", "P002", "Dr. Chen", "2025-01-12", "9:30 AM", "Consultation", "scheduled"),
    ("A004", "P003", "Dr. Williams", "2025-01-20", "11:00 AM", "Blood Test Review", "scheduled"),
    ("A005", "P004", "Dr. Patel", "2025-01-10", "3:00 PM", "Vaccination", "scheduled"),
    ("A006", "P005", "Dr. Chen", "2024-12-15", "10:30 AM", "Physical Exam", "completed"),
    ("A007", "P002", "Dr. Chen", "2025-01-25", "2:30 PM", "Follow-up", "scheduled"),
]

spark.createDataFrame(
    appointments_data,
    ["appointment_id", "patient_id", "doctor_name", "appointment_date", "appointment_time", "reason", "status"]
).write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.appointments")

print("✓ Appointments table created")
spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.appointments").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Prescriptions Table

# COMMAND ----------

# Create prescriptions table
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG_NAME}.{SCHEMA_NAME}.prescriptions (
  prescription_id STRING,
  patient_id STRING,
  medication STRING,
  dosage STRING,
  frequency STRING,
  prescribed_date DATE,
  doctor_name STRING
)
""")

# Insert sample data
prescriptions_data = [
    ("RX001", "P001", "Lisinopril", "10mg", "Once daily", "2024-12-20", "Dr. Williams"),
    ("RX002", "P002", "Amoxicillin", "500mg", "Twice daily", "2024-12-18", "Dr. Chen"),
    ("RX003", "P003", "Metformin", "500mg", "Twice daily", "2024-11-15", "Dr. Williams"),
    ("RX004", "P003", "Atorvastatin", "20mg", "Once daily", "2024-11-15", "Dr. Williams"),
    ("RX005", "P005", "Aspirin", "81mg", "Once daily", "2024-12-15", "Dr. Chen"),
]

spark.createDataFrame(
    prescriptions_data,
    ["prescription_id", "patient_id", "medication", "dosage", "frequency", "prescribed_date", "doctor_name"]
).write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.prescriptions")

print("✓ Prescriptions table created")
spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.prescriptions").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog Functions
# MAGIC
# MAGIC **TODO 2:** Create SQL functions that will serve as tools for your agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 1: Get Patient Information
# MAGIC
# MAGIC **TODO:** Complete the function below to retrieve patient information by patient_id.

# COMMAND ----------

# TODO: Create a function that returns patient information
# HINT: Look at the get_customer_info function from Demo 1 for reference

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.get_patient_info(patient_id STRING)
RETURNS STRUCT<patient_id: STRING, name: STRING, age: INT, insurance_type: STRING, primary_doctor: STRING, phone: STRING>
LANGUAGE SQL
COMMENT 'TODO: Write a clear description for the agent to understand when to use this function'
RETURN (
  -- TODO: Write the SQL query to return patient information
  -- HINT: SELECT STRUCT(...) FROM patients WHERE ...
  SELECT STRUCT(patient_id, name, age, insurance_type, primary_doctor, phone)
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.patients
  WHERE patient_id = get_patient_info.patient_id
  LIMIT 1
)
""")

print("✓ Function created: get_patient_info")

# Test your function
print("\nTesting get_patient_info:")
result = spark.sql(f"SELECT {CATALOG_NAME}.{SCHEMA_NAME}.get_patient_info('P001') AS patient_info")
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 2: Get Patient Appointments
# MAGIC
# MAGIC **TODO:** Create a function to retrieve all appointments for a patient.

# COMMAND ----------

# TODO: Create a function that returns patient appointments
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.get_patient_appointments(patient_id STRING)
RETURNS TABLE(appointment_id STRING, doctor_name STRING, appointment_date DATE, appointment_time STRING, reason STRING, status STRING)
LANGUAGE SQL
COMMENT 'Retrieves all appointments for a specific patient. Use this to check upcoming or past appointments. Input: patient_id (e.g., "P001"). Returns: table of appointments with details including date, time, doctor, reason, and status.'
RETURN (
  -- TODO: Write the SQL query
  -- HINT: SELECT columns FROM appointments WHERE ... ORDER BY ...
  SELECT appointment_id, doctor_name, appointment_date, appointment_time, reason, status
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.appointments
  WHERE patient_id = get_patient_appointments.patient_id
  ORDER BY appointment_date DESC
)
""")

print("✓ Function created: get_patient_appointments")

# Test your function
print("\nTesting get_patient_appointments:")
result = spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.get_patient_appointments('P001')")
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 3: Get Patient Prescriptions
# MAGIC
# MAGIC **TODO:** Create a function to retrieve prescriptions for a patient.

# COMMAND ----------

# TODO: Create a function that returns patient prescriptions
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.get_patient_prescriptions(patient_id STRING)
RETURNS TABLE(prescription_id STRING, medication STRING, dosage STRING, frequency STRING, prescribed_date DATE, doctor_name STRING)
LANGUAGE SQL
COMMENT 'Retrieves all prescriptions for a specific patient. Use this to check current medications, dosages, and prescription history. Input: patient_id (e.g., "P001"). Returns: table of prescriptions with medication details.'
RETURN (
  -- TODO: Write the SQL query
  SELECT prescription_id, medication, dosage, frequency, prescribed_date, doctor_name
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.prescriptions
  WHERE patient_id = get_patient_prescriptions.patient_id
  ORDER BY prescribed_date DESC
)
""")

print("✓ Function created: get_patient_prescriptions")

# Test your function
print("\nTesting get_patient_prescriptions:")
result = spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.get_patient_prescriptions('P003')")
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function 4: Get Doctor's Schedule (Challenge)
# MAGIC
# MAGIC **TODO:** Create a function to check upcoming appointments for a specific doctor.
# MAGIC
# MAGIC **Challenge:** Only return appointments that are scheduled (not completed) and in the future or upcoming.

# COMMAND ----------

# TODO: Create a function that returns a doctor's upcoming appointments
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SCHEMA_NAME}.get_doctor_schedule(doctor_name STRING)
RETURNS TABLE(appointment_id STRING, patient_id STRING, appointment_date DATE, appointment_time STRING, reason STRING)
LANGUAGE SQL
COMMENT 'Retrieves upcoming scheduled appointments for a specific doctor. Use this to check doctor availability or appointment schedule. Input: doctor_name (e.g., "Dr. Williams"). Returns: table of scheduled appointments.'
RETURN (
  -- TODO: Write the SQL query
  -- HINT: Filter by doctor_name AND status = 'scheduled'
  SELECT appointment_id, patient_id, appointment_date, appointment_time, reason
  FROM {CATALOG_NAME}.{SCHEMA_NAME}.appointments
  WHERE doctor_name = get_doctor_schedule.doctor_name
    AND status = 'scheduled'
  ORDER BY appointment_date, appointment_time
)
""")

print("✓ Function created: get_doctor_schedule")

# Test your function
print("\nTesting get_doctor_schedule:")
result = spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.get_doctor_schedule('Dr. Williams')")
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Set Up LangChain Agent
# MAGIC
# MAGIC **TODO 3:** Configure the LangChain agent with your functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure the LLM

# COMMAND ----------

# TODO: Import necessary libraries and configure the LLM
from langchain_community.chat_models import ChatDatabricks
from databricks.agents import UCFunctionToolkit
from langchain.agents import AgentExecutor, create_react_agent
from langchain.prompts import PromptTemplate
import mlflow

# Enable MLflow autologging
mlflow.langchain.autolog()

# TODO: Configure the LLM
# HINT: Use ChatDatabricks with an appropriate endpoint and temperature
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    temperature=0.1,  # TODO: Adjust if needed
    max_tokens=2000
)

print("✓ LLM configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load UC Functions as Tools

# COMMAND ----------

# TODO: Create UCFunctionToolkit with your functions
# HINT: You need to provide warehouse_id and function_names list

toolkit = UCFunctionToolkit(
    warehouse_id=WAREHOUSE_ID,
    function_names=[
        # TODO: Add your function names in the format "catalog.schema.function_name"
        f"{CATALOG_NAME}.{SCHEMA_NAME}.get_patient_info",
        f"{CATALOG_NAME}.{SCHEMA_NAME}.get_patient_appointments",
        f"{CATALOG_NAME}.{SCHEMA_NAME}.get_patient_prescriptions",
        f"{CATALOG_NAME}.{SCHEMA_NAME}.get_doctor_schedule",
    ]
)

tools = toolkit.tools

print(f"✓ Loaded {len(tools)} tools:")
for tool in tools:
    print(f"  - {tool.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Agent Prompt
# MAGIC
# MAGIC **TODO:** Customize the prompt for the healthcare domain.

# COMMAND ----------

# TODO: Create a customized prompt for your healthcare agent
# HINT: Modify the prompt from Demo 1 to fit the healthcare context

react_prompt = PromptTemplate.from_template("""
You are a helpful AI assistant for HealthCare Plus. You help staff and patients with:
- Looking up patient information
- Checking appointment schedules
- Reviewing prescription details
- Answering questions about healthcare services

Always be professional, accurate, and protect patient privacy. Use the available tools to get factual information.

TOOLS:
------
You have access to the following tools:

{tools}

To use a tool, use the following format:

Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action

When you have enough information to answer the question, use this format:

Thought: Do I need to use a tool? No
Final Answer: [your answer here]

Begin!

Question: {input}
Thought: {agent_scratchpad}
""")

print("✓ Prompt template created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Agent Executor

# COMMAND ----------

# TODO: Create the agent and agent executor
# HINT: Use create_react_agent and AgentExecutor from Demo 1

agent = create_react_agent(
    llm=llm,
    tools=tools,
    prompt=react_prompt
)

agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    max_iterations=5,  # TODO: Adjust if needed
    handle_parsing_errors=True,
    return_intermediate_steps=True
)

print("✓ Agent executor created and ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test Your Agent
# MAGIC
# MAGIC **TODO 4:** Test your agent with various queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: Simple Patient Lookup

# COMMAND ----------

# TODO: Test your agent with a simple query
# Example: "What is the primary doctor for patient P001?"

with mlflow.start_run(run_name="test_patient_lookup") as run:
    query = "What is the primary doctor for patient P001?"  # TODO: Try your own query
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Appointment Information

# COMMAND ----------

# TODO: Test with an appointment-related query
with mlflow.start_run(run_name="test_appointments") as run:
    query = "How many appointments does patient P001 have scheduled?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: Prescription Lookup

# COMMAND ----------

# TODO: Test with a prescription query
with mlflow.start_run(run_name="test_prescriptions") as run:
    query = "What medications is patient P003 currently taking?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 4: Doctor Schedule

# COMMAND ----------

# TODO: Test the doctor schedule function
with mlflow.start_run(run_name="test_doctor_schedule") as run:
    query = "What appointments does Dr. Williams have scheduled?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 5: Complex Multi-Tool Query

# COMMAND ----------

# TODO: Test with a complex query that requires multiple tools
with mlflow.start_run(run_name="test_complex") as run:
    query = "Tell me about patient P002 - who is their doctor, what appointments do they have, and are they on any medications?"
    
    print(f"Query: {query}\n")
    response = agent_executor.invoke({"input": query})
    
    print(f"\n{'='*60}")
    print(f"Final Answer: {response['output']}")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 6: Your Custom Query
# MAGIC
# MAGIC **TODO:** Write your own test query that exercises the agent's capabilities.

# COMMAND ----------

# TODO: Write and test your own query
with mlflow.start_run(run_name="test_custom") as run:
    query = "YOUR CUSTOM QUERY HERE"  # TODO: Replace with your query
    
    print(f"Query: {query}\n")
    # TODO: Uncomment and complete
    # response = agent_executor.invoke({"input": query})
    # print(f"\n{'='*60}")
    # print(f"Final Answer: {response['output']}")
    # print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Analyze MLflow Traces
# MAGIC
# MAGIC **TODO 5:** Examine your agent's reasoning using MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Traces in MLflow UI
# MAGIC
# MAGIC **TODO:** 
# MAGIC 1. Click the "Experiment" icon in the right sidebar
# MAGIC 2. Find your most recent runs
# MAGIC 3. Click on one of the runs
# MAGIC 4. Navigate to the "Traces" tab
# MAGIC 5. Analyze the agent's decision-making process
# MAGIC
# MAGIC **Questions to answer:**
# MAGIC - How many iterations did the agent take?
# MAGIC - Did it choose the right tools?
# MAGIC - Were the reasoning steps logical?
# MAGIC - How many tokens were used?

# COMMAND ----------

# View recent runs programmatically
experiment_name = f"/Users/{current_user}/healthcare-agent-lab"

try:
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment:
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=10
        )
        print("Recent Agent Runs:")
        display(runs[["run_id", "start_time", "tags.mlflow.runName", "status"]])
    else:
        print("No experiment found. Make sure you've run some agent queries first.")
except Exception as e:
    print(f"Could not retrieve runs: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Optimization Challenges
# MAGIC
# MAGIC **TODO 6:** Complete these challenges to optimize your agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 1: Handle Edge Cases
# MAGIC
# MAGIC Test how your agent handles:
# MAGIC - Non-existent patient IDs
# MAGIC - Ambiguous queries
# MAGIC - Out-of-scope questions

# COMMAND ----------

# TODO: Test with edge cases
test_queries = [
    "What about patient P999?",  # Non-existent patient
    "When is the appointment?",  # Ambiguous - which patient?
    "What's the weather today?",  # Out of scope
]

# TODO: Run these queries and observe agent behavior
for query in test_queries:
    print(f"\nTesting: {query}")
    # TODO: Uncomment to test
    # response = agent_executor.invoke({"input": query})
    # print(f"Response: {response['output']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 2: Adjust Temperature
# MAGIC
# MAGIC **TODO:** Create a new agent with different temperature settings and compare results.

# COMMAND ----------

# TODO: Create agents with different temperatures and compare
# HINT: Try temperature 0.0 (very deterministic) vs 0.7 (more creative)

# Example:
# strict_llm = ChatDatabricks(endpoint="...", temperature=0.0)
# creative_llm = ChatDatabricks(endpoint="...", temperature=0.7)

# Test the same query with both and compare outputs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 3: Add a New Function
# MAGIC
# MAGIC **TODO:** Create a new UC function that counts total appointments per doctor.
# MAGIC
# MAGIC Requirements:
# MAGIC - Function name: `count_doctor_appointments`
# MAGIC - Input: `doctor_name` (STRING)
# MAGIC - Output: STRUCT with `doctor_name` and `appointment_count`
# MAGIC - Count only scheduled appointments

# COMMAND ----------

# TODO: Create the new function
# HINT: Use CREATE OR REPLACE FUNCTION with COUNT() aggregate

# TODO: Add it to your toolkit and test

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 4: Improve Error Messages
# MAGIC
# MAGIC **TODO:** Modify your functions to return helpful error messages when data isn't found.
# MAGIC
# MAGIC HINT: Use CASE WHEN or COALESCE in your SQL functions.

# COMMAND ----------

# TODO: Update one of your functions to handle missing data gracefully
# Example: If patient doesn't exist, return a struct with null values and an error message

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Reflection and Documentation
# MAGIC
# MAGIC **TODO 7:** Answer these questions based on your experience.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflection Questions
# MAGIC
# MAGIC **TODO:** Answer these questions in the markdown cell below:
# MAGIC
# MAGIC 1. What was the most challenging part of building the agent?
# MAGIC 2. How well did your agent handle multi-step reasoning?
# MAGIC 3. What would you improve about the agent's performance?
# MAGIC 4. Which temperature setting worked best for your use case?
# MAGIC 5. How could you extend this agent with additional capabilities?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your Answers:
# MAGIC
# MAGIC **TODO: Write your answers here**
# MAGIC
# MAGIC 1. Most challenging part: [Your answer]
# MAGIC 2. Multi-step reasoning: [Your answer]
# MAGIC 3. Improvements: [Your answer]
# MAGIC 4. Best temperature: [Your answer]
# MAGIC 5. Extensions: [Your answer]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus Challenges (Optional)
# MAGIC
# MAGIC If you have extra time, try these advanced challenges:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 1: Add Python UC Functions
# MAGIC
# MAGIC Create a Python UC function that:
# MAGIC - Calculates patient age in years and months
# MAGIC - Determines if a patient is due for an annual checkup (last checkup > 1 year ago)
# MAGIC - Formats phone numbers in a standard format

# COMMAND ----------

# TODO: Create Python UC function (advanced)
# HINT: Look at Demo 3 from the agent-tools course for Python function examples

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 2: Build a Conversation Chain
# MAGIC
# MAGIC Extend your agent to maintain conversation history and context across multiple queries.

# COMMAND ----------

# TODO: Implement conversation memory
# HINT: Use ConversationBufferMemory from langchain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus 3: Create a Custom Evaluation Metric
# MAGIC
# MAGIC Build a function that evaluates agent responses for:
# MAGIC - Accuracy (did it use the right tools?)
# MAGIC - Completeness (did it answer fully?)
# MAGIC - Efficiency (did it minimize tool calls?)

# COMMAND ----------

# TODO: Create evaluation function

def evaluate_agent_response(query, response, expected_tools=None):
    """
    Evaluate agent response quality.
    
    Args:
        query: The user question
        response: The agent's response object
        expected_tools: Optional list of tools that should have been used
    
    Returns:
        Dict with evaluation metrics
    """
    # TODO: Implement evaluation logic
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What You Built
# MAGIC
# MAGIC ✅ Healthcare AI agent with multiple tools
# MAGIC ✅ Unity Catalog functions for data access  
# MAGIC ✅ LangChain agent with ReAct pattern
# MAGIC ✅ MLflow tracing for observability
# MAGIC ✅ Custom prompts for domain-specific behavior
# MAGIC
# MAGIC ### Key Learnings
# MAGIC
# MAGIC - How to translate business requirements into UC functions
# MAGIC - Configuring and testing LangChain agents
# MAGIC - Debugging agent behavior with traces
# MAGIC - Optimizing agent performance through configuration
# MAGIC
# MAGIC ### Production Considerations
# MAGIC
# MAGIC Before deploying to production, consider:
# MAGIC - **Security**: Add proper authentication and authorization
# MAGIC - **Error Handling**: Robust error handling for all edge cases
# MAGIC - **Monitoring**: Set up alerts for agent failures
# MAGIC - **Rate Limiting**: Prevent excessive API calls
# MAGIC - **Testing**: Comprehensive test suite for agent behavior
# MAGIC - **Documentation**: Clear docs for users and maintainers
# MAGIC
# MAGIC ### Congratulations!
# MAGIC
# MAGIC You've successfully built a production-ready AI agent on Databricks! 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC **TODO:** When you're done with the lab, optionally clean up resources.

# COMMAND ----------

# TODO: Uncomment to clean up (BE CAREFUL - this deletes your schema!)
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME} CASCADE")
# print(f"✓ Cleaned up schema: {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Databricks Mosaic AI Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/index.html)
# MAGIC - [LangChain Documentation](https://python.langchain.com/)
# MAGIC - [Unity Catalog Functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-udf.html)
# MAGIC - [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html)
