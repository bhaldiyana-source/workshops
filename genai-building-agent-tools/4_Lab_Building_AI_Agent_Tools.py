# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building AI Agent Tools with Unity Catalog Functions
# MAGIC
# MAGIC ## Overview
# MAGIC This hands-on lab provides guided exercises to reinforce your learning about building AI agent tools on Databricks. You'll create functions from scratch, troubleshoot broken functions, and test them in the AI Playground.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Design and implement SQL and Python UC functions independently
# MAGIC - Write effective function descriptions for agent tools
# MAGIC - Debug and fix common issues in agent functions
# MAGIC - Test functions effectively in AI Playground
# MAGIC - Apply best practices for production-ready agent tools
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Lectures 0-1 and Demos 2-3
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Serverless or classic compute cluster
# MAGIC
# MAGIC ## Lab Structure
# MAGIC - **Exercises 1-5**: Build functions from requirements
# MAGIC - **Solutions**: Provided after each exercise
# MAGIC - **Challenge**: More complex scenarios
# MAGIC
# MAGIC ## Duration
# MAGIC 45-60 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Lab Environment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Setup catalog and schema
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS agent_tools_lab;
# MAGIC USE SCHEMA agent_tools_lab;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create employee table
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC   employee_id STRING,
# MAGIC   name STRING,
# MAGIC   department STRING,
# MAGIC   salary DOUBLE,
# MAGIC   hire_date DATE,
# MAGIC   manager_id STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO employees VALUES
# MAGIC   ('E001', 'Alice Johnson', 'Engineering', 120000, '2020-01-15', 'E010'),
# MAGIC   ('E002', 'Bob Smith', 'Engineering', 95000, '2021-03-20', 'E010'),
# MAGIC   ('E003', 'Carol Williams', 'Sales', 85000, '2019-07-10', 'E011'),
# MAGIC   ('E004', 'David Brown', 'Sales', 90000, '2020-11-05', 'E011'),
# MAGIC   ('E005', 'Emma Davis', 'Marketing', 78000, '2021-05-12', 'E012'),
# MAGIC   ('E006', 'Frank Miller', 'Engineering', 110000, '2018-09-01', 'E010'),
# MAGIC   ('E007', 'Grace Lee', 'HR', 72000, '2022-02-14', 'E013'),
# MAGIC   ('E008', 'Henry Wilson', 'Sales', 88000, '2021-08-20', 'E011'),
# MAGIC   ('E009', 'Ivy Martinez', 'Marketing', 82000, '2020-12-03', 'E012'),
# MAGIC   ('E010', 'Jack Taylor', 'Engineering', 150000, '2015-01-10', NULL),
# MAGIC   ('E011', 'Karen Anderson', 'Sales', 140000, '2016-04-15', NULL),
# MAGIC   ('E012', 'Leo Thomas', 'Marketing', 130000, '2017-06-20', NULL),
# MAGIC   ('E013', 'Maria Garcia', 'HR', 125000, '2018-03-25', NULL);
# MAGIC
# MAGIC SELECT * FROM employees ORDER BY department, salary DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create projects table
# MAGIC CREATE OR REPLACE TABLE projects (
# MAGIC   project_id STRING,
# MAGIC   project_name STRING,
# MAGIC   department STRING,
# MAGIC   budget DOUBLE,
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   status STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO projects VALUES
# MAGIC   ('P001', 'Website Redesign', 'Engineering', 250000, '2024-01-01', '2024-06-30', 'completed'),
# MAGIC   ('P002', 'Mobile App Launch', 'Engineering', 500000, '2024-03-01', '2024-12-31', 'in_progress'),
# MAGIC   ('P003', 'Q4 Sales Campaign', 'Sales', 150000, '2024-10-01', '2024-12-31', 'completed'),
# MAGIC   ('P004', 'Brand Refresh', 'Marketing', 180000, '2024-07-01', '2025-01-31', 'in_progress'),
# MAGIC   ('P005', 'Customer Portal', 'Engineering', 400000, '2025-01-01', '2025-08-31', 'planning'),
# MAGIC   ('P006', 'Sales Training Program', 'HR', 75000, '2024-09-01', '2024-11-30', 'completed'),
# MAGIC   ('P007', 'Market Expansion', 'Sales', 300000, '2025-02-01', '2025-12-31', 'planning');
# MAGIC
# MAGIC SELECT * FROM projects ORDER BY status, start_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create support tickets table
# MAGIC CREATE OR REPLACE TABLE support_tickets (
# MAGIC   ticket_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   issue_type STRING,
# MAGIC   priority STRING,
# MAGIC   status STRING,
# MAGIC   created_date TIMESTAMP,
# MAGIC   resolved_date TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO support_tickets VALUES
# MAGIC   ('T001', 'Acme Corp', 'Technical', 'High', 'resolved', '2025-01-01 09:00:00', '2025-01-01 14:30:00'),
# MAGIC   ('T002', 'TechStart Inc', 'Billing', 'Medium', 'resolved', '2025-01-02 10:15:00', '2025-01-03 11:00:00'),
# MAGIC   ('T003', 'Global Systems', 'Technical', 'Critical', 'open', '2025-01-08 08:30:00', NULL),
# MAGIC   ('T004', 'DataCo', 'Feature Request', 'Low', 'open', '2025-01-07 15:45:00', NULL),
# MAGIC   ('T005', 'CloudNet', 'Technical', 'High', 'in_progress', '2025-01-08 11:00:00', NULL),
# MAGIC   ('T006', 'Acme Corp', 'Billing', 'Medium', 'resolved', '2025-01-05 13:20:00', '2025-01-05 16:45:00'),
# MAGIC   ('T007', 'SmartTech', 'Technical', 'Medium', 'in_progress', '2025-01-09 09:30:00', NULL);
# MAGIC
# MAGIC SELECT * FROM support_tickets ORDER BY created_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 1: Employee Count by Department (SQL)
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a SQL function that:
# MAGIC - **Function Name**: `get_employee_count_by_department`
# MAGIC - **Parameter**: `dept_name` (STRING) - The department name
# MAGIC - **Returns**: INT - The count of employees in that department
# MAGIC - **Description**: Write a clear description that explains when an agent should use this function
# MAGIC
# MAGIC ## Your Task
# MAGIC Write the CREATE FUNCTION statement below.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 1: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION get_employee_count_by_department...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Your Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with different departments
# MAGIC -- Uncomment once your function is created
# MAGIC
# MAGIC -- SELECT get_employee_count_by_department('Engineering');
# MAGIC -- SELECT get_employee_count_by_department('Sales');
# MAGIC -- SELECT get_employee_count_by_department('Marketing');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 Test in AI Playground
# MAGIC
# MAGIC Try these queries:
# MAGIC - "How many employees are in Engineering?"
# MAGIC - "What's the employee count for the Sales department?"
# MAGIC - "Tell me how many people work in Marketing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 1
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to reveal solution</summary>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 1
# MAGIC CREATE OR REPLACE FUNCTION get_employee_count_by_department(dept_name STRING)
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Returns the total number of employees in a specific department. Use this when asked about department size, headcount, or how many people work in a particular department.'
# MAGIC RETURN (
# MAGIC   SELECT COUNT(*)
# MAGIC   FROM employees
# MAGIC   WHERE department = dept_name
# MAGIC );
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT 
# MAGIC   'Engineering' as department,
# MAGIC   get_employee_count_by_department('Engineering') as count
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Sales' as department,
# MAGIC   get_employee_count_by_department('Sales') as count;

# COMMAND ----------

# MAGIC %md
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 2: Calculate Average Salary by Department (SQL)
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a SQL function that:
# MAGIC - **Function Name**: `calculate_avg_salary_by_department`
# MAGIC - **Parameter**: `dept_name` (STRING) - The department name
# MAGIC - **Returns**: DOUBLE - The average salary in that department
# MAGIC - **Special Requirements**: 
# MAGIC   - Return NULL if department doesn't exist
# MAGIC   - Round result to 2 decimal places
# MAGIC - **Description**: Write a clear, detailed description
# MAGIC
# MAGIC ## Your Task
# MAGIC Write the function below.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 2: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION calculate_avg_salary_by_department...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Your Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test your function
# MAGIC -- Uncomment once created
# MAGIC
# MAGIC -- SELECT calculate_avg_salary_by_department('Engineering');
# MAGIC -- SELECT calculate_avg_salary_by_department('NonExistent');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 2
# MAGIC CREATE OR REPLACE FUNCTION calculate_avg_salary_by_department(dept_name STRING)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Calculates the average salary for employees in a specific department, rounded to 2 decimal places. Returns NULL if the department has no employees. Use this when asked about average pay, typical salary, or compensation levels in a department.'
# MAGIC RETURN (
# MAGIC   SELECT ROUND(AVG(salary), 2)
# MAGIC   FROM employees
# MAGIC   WHERE department = dept_name
# MAGIC );
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT 
# MAGIC   department,
# MAGIC   calculate_avg_salary_by_department(department) as avg_salary
# MAGIC FROM employees
# MAGIC GROUP BY department
# MAGIC ORDER BY avg_salary DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 3: Calculate Years of Service (Python)
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a Python function that:
# MAGIC - **Function Name**: `calculate_years_of_service`
# MAGIC - **Parameter**: `hire_date` (DATE) - Employee's hire date
# MAGIC - **Returns**: INT - Number of complete years of service
# MAGIC - **Logic**: 
# MAGIC   - Calculate years between hire date and current date
# MAGIC   - Return complete years only (integer)
# MAGIC   - Handle NULL dates appropriately
# MAGIC - **Description**: Explain when to use this function
# MAGIC
# MAGIC ## Hints
# MAGIC - Use Python's datetime functionality
# MAGIC - Remember to handle NULL inputs
# MAGIC - Consider using date.today() for current date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 3: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION calculate_years_of_service(hire_date DATE)
# MAGIC -- RETURNS INT
# MAGIC -- LANGUAGE PYTHON
# MAGIC -- ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Your Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with employee data
# MAGIC -- Uncomment once created
# MAGIC
# MAGIC -- SELECT 
# MAGIC --   name,
# MAGIC --   hire_date,
# MAGIC --   calculate_years_of_service(hire_date) as years_of_service
# MAGIC -- FROM employees
# MAGIC -- ORDER BY years_of_service DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 3
# MAGIC CREATE OR REPLACE FUNCTION calculate_years_of_service(hire_date DATE)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculates the number of complete years an employee has been with the company based on their hire date. Returns 0 for employees hired less than a year ago. Use this when asked about employee tenure, seniority, or length of service.'
# MAGIC AS $$
# MAGIC   if hire_date is None:
# MAGIC       return None
# MAGIC   
# MAGIC   from datetime import date
# MAGIC   
# MAGIC   # Get today's date
# MAGIC   today = date.today()
# MAGIC   
# MAGIC   # Calculate years difference
# MAGIC   years = today.year - hire_date.year
# MAGIC   
# MAGIC   # Adjust if birthday hasn't occurred this year
# MAGIC   if (today.month, today.day) < (hire_date.month, hire_date.day):
# MAGIC       years -= 1
# MAGIC   
# MAGIC   return max(0, years)  # Return 0 if negative
# MAGIC $$;
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT 
# MAGIC   name,
# MAGIC   hire_date,
# MAGIC   calculate_years_of_service(hire_date) as years_of_service
# MAGIC FROM employees
# MAGIC ORDER BY years_of_service DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 4: Get Open Tickets by Priority (SQL)
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a SQL function that:
# MAGIC - **Function Name**: `get_open_tickets_by_priority`
# MAGIC - **Parameter**: `priority_level` (STRING) - Priority level (e.g., 'High', 'Critical')
# MAGIC - **Returns**: TABLE with columns:
# MAGIC   - `ticket_id` (STRING)
# MAGIC   - `customer_name` (STRING)
# MAGIC   - `issue_type` (STRING)
# MAGIC   - `status` (STRING)
# MAGIC   - `created_date` (TIMESTAMP)
# MAGIC - **Logic**:
# MAGIC   - Only return tickets with status 'open' or 'in_progress'
# MAGIC   - Filter by the specified priority level
# MAGIC   - Order by created_date (oldest first)
# MAGIC   - Limit to 20 results
# MAGIC - **Description**: Write a detailed description

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 4: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION get_open_tickets_by_priority...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Your Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with different priorities
# MAGIC -- Uncomment once created
# MAGIC
# MAGIC -- SELECT * FROM get_open_tickets_by_priority('High');
# MAGIC -- SELECT * FROM get_open_tickets_by_priority('Critical');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 4
# MAGIC CREATE OR REPLACE FUNCTION get_open_tickets_by_priority(priority_level STRING)
# MAGIC RETURNS TABLE(
# MAGIC   ticket_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   issue_type STRING,
# MAGIC   status STRING,
# MAGIC   created_date TIMESTAMP
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Retrieves open or in-progress support tickets filtered by priority level (Critical, High, Medium, Low). Returns up to 20 tickets sorted by creation date (oldest first). Use this when asked about pending issues, urgent tickets, or what needs attention at a specific priority level.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     ticket_id,
# MAGIC     customer_name,
# MAGIC     issue_type,
# MAGIC     status,
# MAGIC     created_date
# MAGIC   FROM support_tickets
# MAGIC   WHERE priority = priority_level
# MAGIC     AND status IN ('open', 'in_progress')
# MAGIC   ORDER BY created_date ASC
# MAGIC   LIMIT 20
# MAGIC );
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT * FROM get_open_tickets_by_priority('High');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 5: Categorize Project Budget (Python)
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a Python function that:
# MAGIC - **Function Name**: `categorize_project_budget`
# MAGIC - **Parameter**: `budget_amount` (DOUBLE) - Project budget in dollars
# MAGIC - **Returns**: STRING - Budget category
# MAGIC - **Logic**:
# MAGIC   - "Small" if budget < 100,000
# MAGIC   - "Medium" if 100,000 <= budget < 300,000
# MAGIC   - "Large" if 300,000 <= budget < 500,000
# MAGIC   - "Very Large" if budget >= 500,000
# MAGIC   - Return NULL for NULL or negative budgets
# MAGIC - **Description**: Clear explanation for agents

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 5: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION categorize_project_budget...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Your Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with project budgets
# MAGIC -- Uncomment once created
# MAGIC
# MAGIC -- SELECT 
# MAGIC --   project_name,
# MAGIC --   budget,
# MAGIC --   categorize_project_budget(budget) as budget_category
# MAGIC -- FROM projects
# MAGIC -- ORDER BY budget DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 5
# MAGIC CREATE OR REPLACE FUNCTION categorize_project_budget(budget_amount DOUBLE)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Categorizes a project budget into size categories: Small (<100K), Medium (100K-300K), Large (300K-500K), or Very Large (>=500K). Returns NULL for invalid budgets. Use this when analyzing project scale, grouping projects by budget size, or reporting on investment levels.'
# MAGIC AS $$
# MAGIC   if budget_amount is None or budget_amount < 0:
# MAGIC       return None
# MAGIC   
# MAGIC   if budget_amount < 100000:
# MAGIC       return 'Small'
# MAGIC   elif budget_amount < 300000:
# MAGIC       return 'Medium'
# MAGIC   elif budget_amount < 500000:
# MAGIC       return 'Large'
# MAGIC   else:
# MAGIC       return 'Very Large'
# MAGIC $$;
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT 
# MAGIC   project_name,
# MAGIC   budget,
# MAGIC   categorize_project_budget(budget) as budget_category
# MAGIC FROM projects
# MAGIC ORDER BY budget DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Exercise 6: Troubleshooting Challenge
# MAGIC
# MAGIC ## Scenario
# MAGIC A colleague created the following function, but it's not working correctly in the AI Playground. The agent never calls it, even when it should.
# MAGIC
# MAGIC ## The Broken Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This function has problems - can you identify and fix them?
# MAGIC CREATE OR REPLACE FUNCTION bad_get_data(id STRING)
# MAGIC RETURNS TABLE(x STRING, y STRING, z DOUBLE)
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Gets data'
# MAGIC RETURN (
# MAGIC   SELECT employee_id, name, salary
# MAGIC   FROM employees
# MAGIC   WHERE employee_id = id
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Task
# MAGIC
# MAGIC Identify the problems with this function and rewrite it correctly.
# MAGIC
# MAGIC ### Problems to find:
# MAGIC 1. Function naming
# MAGIC 2. Parameter naming
# MAGIC 3. Column aliases vs return type
# MAGIC 4. Description quality
# MAGIC 5. Any other issues?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 6: Write your improved version here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Exercise 6

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Exercise 6
# MAGIC -- Problems identified:
# MAGIC -- 1. Function name "bad_get_data" is vague
# MAGIC -- 2. Parameter name "id" is ambiguous
# MAGIC -- 3. Column names don't match return type (x, y, z)
# MAGIC -- 4. Description "Gets data" is too vague
# MAGIC -- 5. No LIMIT clause
# MAGIC
# MAGIC -- Fixed version:
# MAGIC CREATE OR REPLACE FUNCTION get_employee_details(employee_id STRING)
# MAGIC RETURNS TABLE(
# MAGIC   employee_id STRING,
# MAGIC   name STRING,
# MAGIC   salary DOUBLE
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Retrieves detailed information for a specific employee including their ID, full name, and current salary. Use this when asked about a specific employee by their ID or when you need to look up employee information.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     employee_id,
# MAGIC     name,
# MAGIC     salary
# MAGIC   FROM employees
# MAGIC   WHERE employee_id = get_employee_details.employee_id
# MAGIC   LIMIT 1
# MAGIC );
# MAGIC
# MAGIC -- Test the fixed version
# MAGIC SELECT * FROM get_employee_details('E001');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Challenge Exercise: Multi-Table Query Function
# MAGIC
# MAGIC ## Advanced Challenge
# MAGIC
# MAGIC Create a SQL function that provides a comprehensive department summary by joining multiple tables.
# MAGIC
# MAGIC ## Requirements
# MAGIC - **Function Name**: `get_department_summary`
# MAGIC - **Parameter**: `dept_name` (STRING)
# MAGIC - **Returns**: TABLE with:
# MAGIC   - `department` (STRING) - Department name
# MAGIC   - `employee_count` (BIGINT) - Number of employees
# MAGIC   - `avg_salary` (DOUBLE) - Average salary
# MAGIC   - `total_project_budget` (DOUBLE) - Sum of project budgets
# MAGIC   - `active_projects` (BIGINT) - Count of in_progress projects
# MAGIC - **Logic**:
# MAGIC   - Join employees and projects tables
# MAGIC   - Aggregate data for the specified department
# MAGIC   - Handle NULL values appropriately
# MAGIC - **Description**: Comprehensive description for agent

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Challenge: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION get_department_summary...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Challenge Exercise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Challenge Exercise
# MAGIC CREATE OR REPLACE FUNCTION get_department_summary(dept_name STRING)
# MAGIC RETURNS TABLE(
# MAGIC   department STRING,
# MAGIC   employee_count BIGINT,
# MAGIC   avg_salary DOUBLE,
# MAGIC   total_project_budget DOUBLE,
# MAGIC   active_projects BIGINT
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Provides a comprehensive summary of a department including employee count, average salary, total project budgets, and count of active (in_progress) projects. Joins employee and project data. Use this when asked for an overview, summary, or complete picture of a department.'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     dept_name as department,
# MAGIC     COUNT(DISTINCT e.employee_id) as employee_count,
# MAGIC     ROUND(AVG(e.salary), 2) as avg_salary,
# MAGIC     COALESCE(SUM(CASE WHEN p.budget IS NOT NULL THEN p.budget ELSE 0 END), 0) as total_project_budget,
# MAGIC     COUNT(DISTINCT CASE WHEN p.status = 'in_progress' THEN p.project_id END) as active_projects
# MAGIC   FROM employees e
# MAGIC   LEFT JOIN projects p ON e.department = p.department
# MAGIC   WHERE e.department = dept_name
# MAGIC   GROUP BY dept_name
# MAGIC );
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT * FROM get_department_summary('Engineering');
# MAGIC SELECT * FROM get_department_summary('Sales');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bonus Challenge: Calculate Ticket Resolution Time (Python)
# MAGIC
# MAGIC ## Requirements
# MAGIC Create a Python function that:
# MAGIC - **Function Name**: `calculate_resolution_time_hours`
# MAGIC - **Parameters**: 
# MAGIC   - `created_date` (TIMESTAMP)
# MAGIC   - `resolved_date` (TIMESTAMP)
# MAGIC - **Returns**: DOUBLE - Hours between creation and resolution
# MAGIC - **Logic**:
# MAGIC   - Calculate the difference in hours
# MAGIC   - Round to 2 decimal places
# MAGIC   - Return NULL if either date is NULL
# MAGIC   - Return NULL if resolved_date < created_date
# MAGIC - **Description**: Detailed explanation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bonus Challenge: Write your solution here
# MAGIC
# MAGIC -- CREATE OR REPLACE FUNCTION calculate_resolution_time_hours...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution for Bonus Challenge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Solution for Bonus Challenge
# MAGIC CREATE OR REPLACE FUNCTION calculate_resolution_time_hours(
# MAGIC   created_date TIMESTAMP,
# MAGIC   resolved_date TIMESTAMP
# MAGIC )
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculates the time taken to resolve a support ticket in hours, rounded to 2 decimal places. Returns NULL if either date is missing or if resolved date is before created date. Use this for calculating resolution metrics, SLA compliance, or support team performance.'
# MAGIC AS $$
# MAGIC   if created_date is None or resolved_date is None:
# MAGIC       return None
# MAGIC   
# MAGIC   # Calculate time difference
# MAGIC   time_diff = resolved_date - created_date
# MAGIC   
# MAGIC   # Check if resolved date is after created date
# MAGIC   if time_diff.total_seconds() < 0:
# MAGIC       return None
# MAGIC   
# MAGIC   # Convert to hours
# MAGIC   hours = time_diff.total_seconds() / 3600
# MAGIC   
# MAGIC   return round(hours, 2)
# MAGIC $$;
# MAGIC
# MAGIC -- Test the solution
# MAGIC SELECT 
# MAGIC   ticket_id,
# MAGIC   customer_name,
# MAGIC   priority,
# MAGIC   created_date,
# MAGIC   resolved_date,
# MAGIC   calculate_resolution_time_hours(created_date, resolved_date) as resolution_hours
# MAGIC FROM support_tickets
# MAGIC WHERE resolved_date IS NOT NULL
# MAGIC ORDER BY resolution_hours DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # AI Playground Testing Session
# MAGIC
# MAGIC ## Complete Testing Guide
# MAGIC
# MAGIC Now that you've created all your functions, it's time to test them comprehensively in the AI Playground.
# MAGIC
# MAGIC ### Step 1: Attach All Functions
# MAGIC
# MAGIC Navigate to AI Playground and attach these functions:
# MAGIC - `get_employee_count_by_department`
# MAGIC - `calculate_avg_salary_by_department`
# MAGIC - `calculate_years_of_service`
# MAGIC - `get_open_tickets_by_priority`
# MAGIC - `categorize_project_budget`
# MAGIC - `get_employee_details`
# MAGIC - `get_department_summary`
# MAGIC - `calculate_resolution_time_hours`
# MAGIC
# MAGIC ### Step 2: Test Simple Queries
# MAGIC
# MAGIC **Employee Queries:**
# MAGIC - "How many employees are in Engineering?"
# MAGIC - "What's the average salary in Sales?"
# MAGIC - "How long has employee E001 been with the company?"
# MAGIC
# MAGIC **Project Queries:**
# MAGIC - "Categorize the Mobile App Launch project budget"
# MAGIC - "What size is a $450,000 project?"
# MAGIC
# MAGIC **Support Queries:**
# MAGIC - "Show me all high priority open tickets"
# MAGIC - "Are there any critical tickets that need attention?"
# MAGIC
# MAGIC ### Step 3: Test Complex Queries
# MAGIC
# MAGIC **Multi-Step Reasoning:**
# MAGIC - "Which department has the most employees and what's their average salary?"
# MAGIC - "Give me a complete overview of the Engineering department"
# MAGIC - "Which resolved tickets took the longest to fix?"
# MAGIC
# MAGIC **Comparative Queries:**
# MAGIC - "Compare employee counts across all departments"
# MAGIC - "Which department has the highest average salary?"
# MAGIC
# MAGIC ### Step 4: Observe and Debug
# MAGIC
# MAGIC For each query, check:
# MAGIC - ✅ Was the correct function called?
# MAGIC - ✅ Were the parameters correct?
# MAGIC - ✅ Did the function return expected results?
# MAGIC - ✅ Did the agent interpret the results correctly?
# MAGIC
# MAGIC ### Step 5: Iterate
# MAGIC
# MAGIC If functions aren't being used correctly:
# MAGIC 1. Review the function description
# MAGIC 2. Make it more specific
# MAGIC 3. Add usage examples to the description
# MAGIC 4. Test again

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary and Best Practices Checklist
# MAGIC
# MAGIC ### Function Design Checklist
# MAGIC
# MAGIC ✅ **Naming**
# MAGIC - [ ] Function names use action verbs (get_, calculate_, check_)
# MAGIC - [ ] Names are specific and descriptive
# MAGIC - [ ] Parameter names are clear and unambiguous
# MAGIC
# MAGIC ✅ **Documentation**
# MAGIC - [ ] Function has detailed COMMENT
# MAGIC - [ ] Description explains WHAT the function does
# MAGIC - [ ] Description explains WHEN to use it
# MAGIC - [ ] Description mentions what is returned
# MAGIC - [ ] Parameters have comments (if multiple)
# MAGIC
# MAGIC ✅ **Implementation**
# MAGIC - [ ] Handles NULL values
# MAGIC - [ ] Validates inputs
# MAGIC - [ ] Uses LIMIT for table returns
# MAGIC - [ ] Returns appropriate data types
# MAGIC - [ ] Includes error handling
# MAGIC
# MAGIC ✅ **Testing**
# MAGIC - [ ] Tested with valid inputs
# MAGIC - [ ] Tested with NULL/invalid inputs
# MAGIC - [ ] Tested edge cases
# MAGIC - [ ] Tested in AI Playground
# MAGIC - [ ] Verified agent calls it correctly
# MAGIC
# MAGIC ✅ **Performance**
# MAGIC - [ ] Executes quickly (< 5 seconds ideal)
# MAGIC - [ ] Result sets are limited
# MAGIC - [ ] Queries are optimized

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Practice Ideas
# MAGIC
# MAGIC ### More Exercises to Try
# MAGIC
# MAGIC 1. **Create a function to find senior employees**
# MAGIC    - Get employees with > 5 years of service
# MAGIC    - Sort by tenure
# MAGIC
# MAGIC 2. **Build a project status checker**
# MAGIC    - Check if a project is on schedule
# MAGIC    - Compare current date to end date
# MAGIC
# MAGIC 3. **Implement a salary tier classifier**
# MAGIC    - Categorize salaries into Junior/Mid/Senior
# MAGIC    - Based on department averages
# MAGIC
# MAGIC 4. **Create a ticket urgency scorer**
# MAGIC    - Calculate urgency based on priority and age
# MAGIC    - Older critical tickets = higher score
# MAGIC
# MAGIC 5. **Build a department comparison function**
# MAGIC    - Compare two departments side by side
# MAGIC    - Show metrics for both

# COMMAND ----------

# MAGIC %md
# MAGIC ## View All Your Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all functions you created
# MAGIC SHOW FUNCTIONS IN agent_tools_lab;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get detailed info about a specific function
# MAGIC DESCRIBE FUNCTION EXTENDED get_department_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment to clean up lab resources
# MAGIC
# MAGIC -- DROP FUNCTION IF EXISTS get_employee_count_by_department;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_avg_salary_by_department;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_years_of_service;
# MAGIC -- DROP FUNCTION IF EXISTS get_open_tickets_by_priority;
# MAGIC -- DROP FUNCTION IF EXISTS categorize_project_budget;
# MAGIC -- DROP FUNCTION IF EXISTS get_employee_details;
# MAGIC -- DROP FUNCTION IF EXISTS get_department_summary;
# MAGIC -- DROP FUNCTION IF EXISTS calculate_resolution_time_hours;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS employees;
# MAGIC -- DROP TABLE IF EXISTS projects;
# MAGIC -- DROP TABLE IF EXISTS support_tickets;
# MAGIC
# MAGIC -- DROP SCHEMA IF EXISTS agent_tools_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the Building AI Agent Tools lab!
# MAGIC
# MAGIC ### What You've Learned
# MAGIC
# MAGIC 1. ✅ Created SQL functions for data retrieval and aggregation
# MAGIC 2. ✅ Built Python functions for business logic and calculations
# MAGIC 3. ✅ Wrote effective function descriptions for agents
# MAGIC 4. ✅ Debugged and fixed problematic functions
# MAGIC 5. ✅ Tested functions in AI Playground
# MAGIC 6. ✅ Applied best practices for production-ready tools
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - **Build your own agent tools** for real use cases
# MAGIC - **Explore advanced patterns** like chaining multiple tools
# MAGIC - **Deploy agents to production** using Mosaic AI Agent Framework
# MAGIC - **Monitor and optimize** agent performance
# MAGIC
# MAGIC ### Additional Resources
# MAGIC
# MAGIC - [Databricks AI Playground Documentation](https://docs.databricks.com/)
# MAGIC - [Unity Catalog Functions Guide](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions.html)
# MAGIC - [Mosaic AI Agent Framework](https://docs.databricks.com/)
# MAGIC - [Best Practices for Agent Development](https://www.databricks.com/blog)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Thank you for completing this lab!**
# MAGIC
# MAGIC If you have questions or feedback, please reach out to the lab developers.
# MAGIC
