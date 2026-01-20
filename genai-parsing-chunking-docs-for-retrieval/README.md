# Parsing and Chunking Documents for Retrieval

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 90 minutes    | Estimated duration to complete the lab(s). |
| Level           | 200           | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active        | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura     | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | v2.0          | * `ai_document_parse()` must be version 2 | 
---

## Description  
This course teaches you how to **parse unstructured documents into structured data**, **transform and clean** the parsed content, and **chunk it for retrieval workflows** using Databricks AI tools. You will learn to automate document processing, prepare data for language models, and build robust retrieval pipelines for downstream analytics and search.

## Learning Objectives
- Parse multi-format documents (PDF, DOCX) using AI-powered functions in SQL and Python  
- Inspect and understand the parsed output schema and metadata  
- Clean and transform parsed text for use with LLMs  
- Compare semantic cleaning (LLM-powered) and fast concatenation methods  
- Chunk cleaned text for retrieval workflows using LangChain  
- Store chunked results for embedding and vector search  
- Save processed data to Delta tables for further analysis  

## Requirements & Prerequisites  
Before starting this lab, ensure you have:  
- A **Databricks** workspace  
- A SQL warehouse  
- Write access to a catalog you own in Unity Catalog  
- Intermediate level Python and Spark experience 

## Contents  
This repository includes: 
- **1_Demo_Parse_Documents_to_Structured_Data.py** - Learn to parse PDF/DOCX documents using `ai_document_parse()` v2
- **2_Demo_Transform_and_Chunk_Parsed_Content.py** - Transform parsed content and chunk for retrieval
- **3_Lab_Parse_Transform_and_Chunk_Documents.py** - Hands-on exercises with solutions
- **utils/helper_functions.py** - Reusable Python functions for document processing
- **Documents/** - Sample documents for practice (5 sample files with README)

## Getting Started  
1. Open **1_Demo_Parse_Documents_to_Structured_Data.py** to learn document parsing basics
2. Continue with **2_Demo_Transform_and_Chunk_Parsed_Content.py** for transformation and chunking
3. Complete the hands-on exercises in **3_Lab_Parse_Transform_and_Chunk_Documents.py**
4. Use the helper functions in **utils/helper_functions.py** in your own projects   
