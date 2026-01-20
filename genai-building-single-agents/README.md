# Building Single Agents

<!-- Replace the template below with information for your lab(s) -->
| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 90 minutes   | Estimated duration to complete the lab(s). |
| Level           | 300 | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version         | N/A          | Specify a product version if applicable If not, use **N/A**. | 

---

## Description
This set of demonstrations and/or labs explores how to build and reason with single AI agents using both **LangChain** and **DSPy**, two complementary frameworks for developing modular, tool-enabled AI systems on Databricks. You’ll first see how to integrate **Unity Catalog (UC) functions** as reusable tools within the LangChain framework, allowing an agent powered by **Mosaic AI Model Serving** to reason and take action. Then, you’ll learn how **DSPy (Declarative Self-improving Python)** provides a declarative, code-first approach to constructing reliable and maintainable AI applications through modular components such as `dspy.Predict`, `dspy.ChainOfThought`, and `dspy.ReAct`. Together, these demonstrations showcase both imperative and declarative approaches to building intelligent, tool-aware agents on the Databricks platform. Finally, you will able to test your knowledge with the LangChain via a hands-on lab. 


## Learning Objectives
- Understand the separation of tasks between tools, models, and agentic frameworks
- Know the process of registering, testing, and integrating Unity Catalog functions with LangChain using the `UCFunctionToolkit`
- Configure and execute a LangChain agent with tool-calling capabilities
- Know how to view and interpret the trace summary of agent execution and analyze decision-making using MLflow
- Explain what DSPy is and how it differs from traditional prompt engineering
- Configure DSPy to work with Databricks Model Serving endpoints
- Create simple reasoning chains using `dspy.ChainOfThought`
- Define custom tools (functions) that agents can use to interact with data
- Build a ReAct agent using `dspy.ReAct` that combines reasoning and tool execution
- Interpret agent trajectories and understand the reasoning process behind agent decisions

## Requirements & Prerequisites  
<!-- Example list below – update or replace with the specific requirements for your lab -->
Before starting this lab, ensure you have:  
- A **Databricks** workspace  
- A SQL warehouse  
- Write access to a catalog you own in Unity Catalog  
- Basic understanding of 
- Basic knowledge of SQL and Python  



## Contents  
<!-- Replace the example below with the actual files included in your lab -->
This repository includes: 
- **0 Lecture - Authoring Single AI Agents with Databricks Mosaic AI Agent Framework**
- **1 Demo - Building Single Agents with LangChain**
- **2 Demo - Building Single Agents with DSPy**
- **3 Lab - Building a LangChain Agent**
- **demo_agent.json**
- **lab_agent.json**
- Images and supporting materials


## Getting Started  
<!-- Replace the example below with the actual files included in your lab -->
1. Open **0 Lecture - Authoring Single AI Agents with Databricks Mosaic AI Agent Framework** and complete the notebooks in sequential order for better understanding of underlying content.  
2. Follow the instructions step by step.   
