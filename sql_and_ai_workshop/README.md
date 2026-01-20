# Databricks SQL and AI Functions Workshop

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 120 minutes   | Estimated duration to complete the lab(s). |
| Level           | 200 | Target difficulty level for participants (100 = beginner, 200 = intermediate, 300 = advanced). |
| Lab Status      | Active | See descriptions in main repo README. |
| Course Location | N/A           | Indicates where the lab is hosted. <br> - **N/A** - Lab is only available in this repo. <br> - **Course name** - Lab has been moved into a full Databricks Academy course. |
| Developer       | Ajit Kalura | Primary developer(s) of the lab, separated by commas.  |
| Reviewer        | Ajit Kalura | Subject matter expert reviewer(s), separated by commas.|
| Product Version | DBR 14.3+     | Databricks Runtime 14.3 or higher recommended. | 

---

## Description
This comprehensive workshop explores the powerful integration of **Databricks SQL** with **AI Functions**, enabling you to seamlessly incorporate artificial intelligence capabilities directly within your SQL workflows. You'll learn how to leverage Unity Catalog AI Functions to perform advanced data analysis, text processing, embeddings generation, and intelligent data transformations without leaving the SQL environment. The workshop covers both built-in AI functions and custom function creation, demonstrating how to combine traditional SQL analytics with modern AI capabilities. Through hands-on demonstrations and labs, you'll discover how Databricks SQL AI Functions can accelerate your data intelligence workflows, from sentiment analysis and text summarization to semantic search and entity extraction.

## Learning Objectives
- Understand the architecture and capabilities of Databricks SQL AI Functions within Unity Catalog
- Learn how to invoke foundation models directly from SQL queries using AI Functions
- Master the use of built-in AI Functions for common tasks like text generation, embeddings, and classification
- Create and register custom AI Functions in Unity Catalog for reusable AI-powered SQL operations
- Implement semantic search and similarity matching using vector embeddings in SQL
- Perform batch AI operations on large datasets efficiently using SQL AI Functions
- Integrate AI Functions with traditional SQL analytics for enhanced business intelligence
- Understand security, governance, and cost management considerations for AI Functions
- Build end-to-end data pipelines that combine SQL transformations with AI-powered insights
- Monitor and optimize AI Function performance in production SQL workloads

## Requirements & Prerequisites  
Before starting this lab, ensure you have:  
- A **Databricks** workspace with Unity Catalog enabled
- A **SQL warehouse** (Pro or Serverless recommended for AI Functions)
- **CREATE FUNCTION** privileges in a Unity Catalog schema
- **EXECUTE** privileges on foundation models or Model Serving endpoints
- Basic to intermediate knowledge of **SQL** and relational databases
- Familiarity with **Python** for custom function development (helpful but not required)
- Understanding of basic AI/ML concepts like embeddings and prompts (helpful but not required)
- Access to Databricks Model Serving or external foundation model endpoints

## Contents  
This repository includes: 
- **0 Lecture - Introduction to Databricks SQL AI Functions**
  - Overview of AI Functions architecture
  - Unity Catalog integration patterns
  - Security and governance considerations
- **1 Demo - Using Built-in AI Functions**
  - Text generation and summarization
  - Sentiment analysis and classification
  - Named entity recognition
  - Translation and text transformation
- **2 Demo - Working with Embeddings and Semantic Search**
  - Generating text embeddings in SQL
  - Building vector similarity searches
  - Semantic clustering and grouping
- **3 Demo - Creating Custom AI Functions**
  - Registering Python UDFs as AI Functions
  - Connecting to Model Serving endpoints
  - Building reusable AI Function libraries
- **4 Lab - Building an AI-Powered Analytics Pipeline**
  - Hands-on exercise combining SQL and AI Functions
  - End-to-end workflow from raw data to AI insights
- **5 Lab - Advanced AI Function Patterns**
  - Batch processing optimization
  - Error handling and retry logic
  - Performance tuning for production workloads
- **sample_data.sql** - Sample datasets for workshop exercises
- **function_templates.sql** - Reusable AI Function templates
- **Images and supporting materials**

## Getting Started  
1. Open **0 Lecture - Introduction to Databricks SQL AI Functions** to understand the foundational concepts and architecture.
2. Work through demos **1-3** sequentially to build familiarity with different AI Function capabilities.
3. Complete **Lab 4** to apply your knowledge in building a complete AI-powered analytics solution.
4. Proceed to **Lab 5** for advanced patterns and production-ready implementations.
5. Use **function_templates.sql** as a reference for creating your own custom AI Functions.
6. Experiment with **sample_data.sql** to practice different AI Function scenarios.

## Workshop Structure

### Part 1: Fundamentals (30 minutes)
- Introduction to AI Functions in Databricks SQL
- Understanding Unity Catalog integration
- Setting up your environment

### Part 2: Built-in AI Functions (30 minutes)
- Exploring text generation capabilities
- Working with sentiment analysis
- Implementing classification tasks

### Part 3: Embeddings and Semantic Search (30 minutes)
- Generating and storing embeddings
- Building similarity search queries
- Vector operations in SQL

### Part 4: Hands-on Labs (30 minutes)
- Building complete AI-powered pipelines
- Advanced patterns and optimization
- Production considerations

## Key Topics Covered

### AI Functions Overview
- `AI_QUERY()` - Execute prompts against foundation models
- `AI_GENERATE_TEXT()` - Generate text completions
- `AI_EXTRACT()` - Extract structured information from text
- `AI_CLASSIFY()` - Classify text into categories
- `AI_SIMILARITY()` - Calculate semantic similarity
- `AI_EMBED()` - Generate text embeddings

### Custom AI Functions
- Creating Python UDFs with AI capabilities
- Registering functions in Unity Catalog
- Managing function versions and permissions
- Connecting to Model Serving endpoints

### Best Practices
- Query optimization for AI Functions
- Cost management strategies
- Error handling and retry logic
- Monitoring and observability
- Security and governance

## Sample Use Cases
- **Customer Feedback Analysis**: Automatically classify and extract insights from customer reviews
- **Document Intelligence**: Summarize and extract key information from large document collections
- **Semantic Search**: Build intelligent search experiences using embeddings
- **Content Moderation**: Detect and flag inappropriate content at scale
- **Data Enrichment**: Enhance existing datasets with AI-generated insights
- **Multilingual Analytics**: Translate and analyze content across languages

## Additional Resources
- [Databricks AI Functions Documentation](https://docs.databricks.com/sql/language-manual/sql-ref-functions-ai.html)
- [Unity Catalog Functions Guide](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html)
- [Model Serving Documentation](https://docs.databricks.com/machine-learning/model-serving/index.html)
- [SQL Warehouse Configuration](https://docs.databricks.com/sql/admin/sql-endpoints.html)
- [Databricks AI Best Practices](https://docs.databricks.com/machine-learning/index.html)

## Troubleshooting

### Common Issues
- **Permission Errors**: Ensure you have CREATE FUNCTION and EXECUTE privileges
- **Model Endpoint Issues**: Verify Model Serving endpoint is running and accessible
- **Performance Concerns**: Review query plans and consider batching strategies
- **Cost Management**: Monitor token usage and implement rate limiting

### Getting Help
- Check the Databricks documentation for latest updates
- Review workshop notebooks for detailed examples
- Consult with your Databricks account team for enterprise support

## License
[Specify your license information here]

## Contributing
[Add contribution guidelines if applicable]

## Feedback
We welcome feedback on this workshop! Please submit issues or suggestions through [your feedback mechanism].

---

**Ready to get started?** Open the first lecture notebook and begin your journey into AI-powered SQL analytics with Databricks!