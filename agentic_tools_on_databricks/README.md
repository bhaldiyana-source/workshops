# Building Agent Tools on Databricks

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 90 minutes   | Estimated duration to complete the lab(s). |
| Level           | 200 | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Matthew McCoy | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Menaf Gul, Michelle McSweeney , Patrick Zier, Sam Le Corre | Subject matter expert reviewer(s), separated by commas.|
| Product Version         | N/A          | Specify a product version if applicable If not, use **N/A**. | 
---

## Description  
This is an introduction to using Agent Tools on Databricks. You will be exposed to best practices with writing SQL and Python UC functions how they can be tested with the AI Playground. You will be exposed to two methods for creating and executing functions using either SQL syntax or `DatabricksFunctionClient()`. You will also gain some hands-on experience with building and troubleshooting tool calling with in the AI Playground via the accompanying lab. 

## Learning Objectives  

- Use both SQL and `DatabricksFunctionClient()` to register and test UC functions in a Notebook
- Understand how to attach tools in the AI Playground
- Identify when a SQL/Python tool has been used by an LLM in AI Playground 
- Troubleshoot poorly constructed functions 

## Requirements & Prerequisites  
Before starting this lab, ensure you have:  
- A **Databricks** workspace  
- Serverless or classic compute for running the notebooks
- Write access to a catalog you own in Unity Catalog
- Basic knowledge of SQL and Python

## Contents  
This repository includes:
- **0 Lecture - Foundations of Agents**
- **1 Lecture - Unity Catalog Functions as Agent Tools**
- **2 Demo - Building SQL Functions as Agent Tools with AI Playground**
- **3 Demo - Building Python Functions as Agent Tools with AI Playground**
- **4 Lab - Building AI Agent Tools with Unity Catalog Functions**
- Images and supporting materials

## Getting Started  
1. Open **0 Lecture - Foundations of Agents** and complete the notebooks in sequential order for better understanding of underlying content.   
2. Follow the instructions step by step.   
3. Complete the hands-on lab exercises to reinforce your learning.

## Lab Structure

### 0. Foundations of Agents (Lecture)
Introduction to AI agents, their architecture, and how they use tools to accomplish tasks.

### 1. Unity Catalog Functions as Agent Tools (Lecture)
Deep dive into Unity Catalog functions and how they serve as tools for AI agents.

### 2. Building SQL Functions as Agent Tools (Demo)
Hands-on demonstration of creating and testing SQL-based UC functions in AI Playground.

### 3. Building Python Functions as Agent Tools (Demo)
Hands-on demonstration of creating and testing Python-based UC functions with DatabricksFunctionClient.

### 4. Building AI Agent Tools (Lab)
Guided exercises to build, test, and troubleshoot your own agent tools.

## Additional Resources
- [Databricks AI Playground Documentation](https://docs.databricks.com/)
- [Unity Catalog Functions Guide](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions.html)
- [AI Agents on Databricks](https://docs.databricks.com/)

