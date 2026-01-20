# Using Single Agents with MLflow

<!-- Replace the template below with information for your lab(s) -->
| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 50 minutes   | Estimated duration to complete the lab(s). |
| Level           | 200/300 | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version         | N/A         | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  
These demonstrations explore how to use MLflow tracing to achieve deep observability when building and deploying AI agents in production. You’ll learn how to monitor, debug, and optimize agent behavior by tracking inputs, outputs, and execution details with MLflow's tracing capabilities. Building on foundational tracing concepts, you will then be introduced to  advanced techniques such as tagging strategies for improved trace management and Unity Catalog registration for creating reproducible, production-ready agents. Together, these practices ensure transparency, reliability, and governance across your AI systems.

## Learning Objectives
- Configure MLflow experiments with both default and custom artifact locations for agent tracing
- Implement automatic tracing for LangChain agents using `mlflow.langchain.autolog()`
- Interpret trace outputs including token counts, latency metrics, and execution timelines
- Apply the `@mlflow.trace` decorator to add custom tracing to Python functions
- Analyze parent-child span relationships in complex multi-step agent workflows
- Implement MLflow tagging strategies to organize and manage agent traces effectively
- Create custom trace functions with proper validation and error handling
- Log agent models to MLflow with appropriate configuration and dependencies
- Register agent models to Unity Catalog for governance and reproducibility
- Deploy and inference agents from both MLflow and Unity Catalog registries

## Requirements & Prerequisites  
<!-- Example list below – update or replace with the specific requirements for your lab -->
Before starting this lab, ensure you have:  
- A **Databricks** workspace  
- A SQL warehouse  
- Write access to a catalog you own in Unity Catalog  
- Basic knowledge of SQL and Python
- Basic knowledge of agents and tool calling with UC functions



## Contents  
<!-- Replace the example below with the actual files included in your lab -->
This repository includes: 
- **0 Lecture - Building Agents on Databricks with MLflow**
- **1 Demo - Tracing Single Agents with MLflow**
- **2 Demo - Tagging & Reproducible Agents** 
- **3 Lab - Building Reproducible Agents**
- Agent files (**demo_agent1.py**, **demo_agent2.py**, **lab_agent.py**, and **lab_agent_update.py**)
- Images and supporting materials


## Getting Started  
<!-- Replace the example below with the actual files included in your lab -->
1. Open **0 Lecture - Building Agents on Databricks with MLflow** and complete the notebooks in sequential order for better understanding of underlying content.   
2. Follow the instructions step by step.   
