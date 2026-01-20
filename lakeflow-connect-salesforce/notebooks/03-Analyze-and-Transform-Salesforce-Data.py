# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3: Analyze and Transform Salesforce Data
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will analyze Salesforce data using SQL, build Customer 360 views, create sales analytics dashboards, and apply machine learning models for predictive insights.
# MAGIC
# MAGIC **Duration:** 30 minutes
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Query Salesforce data with SQL analytics across multiple objects
# MAGIC - Join Account, Contact, and Opportunity data for unified views
# MAGIC - Build sales pipeline analytics and forecasting reports
# MAGIC - Create Customer 360 views combining CRM and behavioral data
# MAGIC - Implement Delta Live Tables for automated transformations
# MAGIC - Apply ML models for lead scoring using classification
# MAGIC - Build opportunity win probability prediction models
# MAGIC - Implement customer churn prediction using historical data
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Completed Lab 2: Ingest Salesforce Data
# MAGIC - Salesforce data loaded in Unity Catalog Delta tables
# MAGIC - Understanding of SQL joins and aggregations
# MAGIC - Basic knowledge of machine learning concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Setup and Data Exploration
# MAGIC
# MAGIC ### Step 1.1: Configure Environment

# COMMAND ----------

# Configuration
catalog_name = "main"  # Replace with your catalog
schema_name = "salesforce_raw"  # Source schema from Lab 2
analytics_schema = "salesforce_analytics"  # Schema for analytics views

# Set active catalog and schema
spark.sql(f"USE {catalog_name}.{schema_name}")

print(f"📊 Configuration:")
print(f"   Source: {catalog_name}.{schema_name}")
print(f"   Analytics: {catalog_name}.{analytics_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Create Analytics Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{analytics_schema}")
print(f"✅ Analytics schema ready: {catalog_name}.{analytics_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3: Verify Available Tables

# COMMAND ----------

# List tables in source schema
tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
display(tables_df)

# Get record counts
print("\n📊 Table Record Counts:")
for table_row in tables_df.collect():
    table_name = table_row['tableName']
    if table_name.startswith('sf_'):
        count = spark.table(f"{catalog_name}.{schema_name}.{table_name}").count()
        print(f"   {table_name}: {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Sales Pipeline Analytics
# MAGIC
# MAGIC ### Understanding Sales Pipeline Metrics
# MAGIC
# MAGIC Key metrics for sales teams:
# MAGIC - **Pipeline Value**: Total value of open opportunities by stage
# MAGIC - **Win Rate**: Percentage of opportunities won vs total closed
# MAGIC - **Average Deal Size**: Mean opportunity amount by stage/industry
# MAGIC - **Sales Cycle Length**: Days from creation to close
# MAGIC - **Conversion Rate**: Percentage moving from one stage to next

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.1: Opportunity Pipeline by Stage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze opportunity pipeline by stage
# MAGIC SELECT 
# MAGIC   StageName,
# MAGIC   COUNT(*) as opportunity_count,
# MAGIC   SUM(Amount) as total_amount,
# MAGIC   AVG(Amount) as avg_amount,
# MAGIC   AVG(Probability) as avg_probability,
# MAGIC   SUM(Amount * Probability / 100) as weighted_value
# MAGIC FROM main.salesforce_raw.sf_opportunities
# MAGIC WHERE IsClosed = false
# MAGIC   AND Amount IS NOT NULL
# MAGIC GROUP BY StageName
# MAGIC ORDER BY total_amount DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Win/Loss Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze win rate and reasons
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN IsWon = true THEN 'Won'
# MAGIC     WHEN IsClosed = true AND IsWon = false THEN 'Lost'
# MAGIC     ELSE 'Open'
# MAGIC   END as outcome,
# MAGIC   COUNT(*) as count,
# MAGIC   SUM(Amount) as total_value,
# MAGIC   AVG(Amount) as avg_deal_size,
# MAGIC   AVG(DATEDIFF(COALESCE(CloseDate, CURRENT_DATE()), CreatedDate)) as avg_days_to_close
# MAGIC FROM main.salesforce_raw.sf_opportunities
# MAGIC WHERE Amount IS NOT NULL
# MAGIC GROUP BY outcome
# MAGIC ORDER BY total_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Pipeline by Industry

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Opportunities by account industry
# MAGIC SELECT 
# MAGIC   a.Industry,
# MAGIC   COUNT(DISTINCT o.Id) as opportunity_count,
# MAGIC   SUM(o.Amount) as total_pipeline_value,
# MAGIC   AVG(o.Amount) as avg_opportunity_size,
# MAGIC   SUM(CASE WHEN o.IsWon = true THEN 1 ELSE 0 END) as won_count,
# MAGIC   ROUND(SUM(CASE WHEN o.IsWon = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as win_rate_pct
# MAGIC FROM main.salesforce_raw.sf_opportunities o
# MAGIC INNER JOIN main.salesforce_raw.sf_accounts a ON o.AccountId = a.Id
# MAGIC WHERE o.Amount IS NOT NULL
# MAGIC   AND a.Industry IS NOT NULL
# MAGIC GROUP BY a.Industry
# MAGIC ORDER BY total_pipeline_value DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.4: Sales Cycle Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze sales cycle duration by stage
# MAGIC SELECT 
# MAGIC   StageName,
# MAGIC   COUNT(*) as opp_count,
# MAGIC   ROUND(AVG(DATEDIFF(CloseDate, CreatedDate)), 1) as avg_days_in_stage,
# MAGIC   MIN(DATEDIFF(CloseDate, CreatedDate)) as min_days,
# MAGIC   MAX(DATEDIFF(CloseDate, CreatedDate)) as max_days,
# MAGIC   PERCENTILE(DATEDIFF(CloseDate, CreatedDate), 0.5) as median_days
# MAGIC FROM main.salesforce_raw.sf_opportunities
# MAGIC WHERE IsClosed = true
# MAGIC   AND CloseDate IS NOT NULL
# MAGIC   AND CreatedDate IS NOT NULL
# MAGIC GROUP BY StageName
# MAGIC ORDER BY avg_days_in_stage DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Customer 360 Views
# MAGIC
# MAGIC ### Understanding Customer 360
# MAGIC
# MAGIC A Customer 360 view provides a unified profile combining:
# MAGIC - Account demographic data (industry, size, location)
# MAGIC - Contact information and engagement
# MAGIC - Opportunity pipeline and revenue history
# MAGIC - Support case history and satisfaction
# MAGIC - Product usage and adoption metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.1: Build Comprehensive Customer 360 View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW main.salesforce_analytics.customer_360 AS
# MAGIC SELECT 
# MAGIC   -- Account Information
# MAGIC   a.Id as account_id,
# MAGIC   a.Name as account_name,
# MAGIC   a.Type as account_type,
# MAGIC   a.Industry,
# MAGIC   a.AnnualRevenue,
# MAGIC   a.NumberOfEmployees,
# MAGIC   a.BillingCity,
# MAGIC   a.BillingState,
# MAGIC   a.BillingCountry,
# MAGIC   
# MAGIC   -- Contact Metrics
# MAGIC   COUNT(DISTINCT c.Id) as contact_count,
# MAGIC   
# MAGIC   -- Opportunity Metrics
# MAGIC   COUNT(DISTINCT o.Id) as total_opportunities,
# MAGIC   SUM(CASE WHEN o.IsClosed = false THEN 1 ELSE 0 END) as open_opportunities,
# MAGIC   SUM(CASE WHEN o.IsWon = true THEN 1 ELSE 0 END) as won_opportunities,
# MAGIC   SUM(CASE WHEN o.IsClosed = true AND o.IsWon = false THEN 1 ELSE 0 END) as lost_opportunities,
# MAGIC   SUM(CASE WHEN o.IsClosed = false THEN o.Amount ELSE 0 END) as open_pipeline_value,
# MAGIC   SUM(CASE WHEN o.IsWon = true THEN o.Amount ELSE 0 END) as total_won_revenue,
# MAGIC   
# MAGIC   -- Case Metrics
# MAGIC   COUNT(DISTINCT cs.Id) as total_cases,
# MAGIC   SUM(CASE WHEN cs.IsClosed = false THEN 1 ELSE 0 END) as open_cases,
# MAGIC   
# MAGIC   -- Engagement Metrics
# MAGIC   a.CreatedDate as customer_since,
# MAGIC   DATEDIFF(CURRENT_DATE(), a.CreatedDate) as days_as_customer,
# MAGIC   MAX(o.CreatedDate) as last_opportunity_date,
# MAGIC   MAX(cs.CreatedDate) as last_case_date
# MAGIC   
# MAGIC FROM main.salesforce_raw.sf_accounts a
# MAGIC LEFT JOIN main.salesforce_raw.sf_contacts c ON a.Id = c.AccountId
# MAGIC LEFT JOIN main.salesforce_raw.sf_opportunities o ON a.Id = o.AccountId
# MAGIC LEFT JOIN main.salesforce_raw.sf_cases cs ON a.Id = cs.AccountId
# MAGIC GROUP BY 
# MAGIC   a.Id, a.Name, a.Type, a.Industry, a.AnnualRevenue, a.NumberOfEmployees,
# MAGIC   a.BillingCity, a.BillingState, a.BillingCountry, a.CreatedDate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the Customer 360 view
# MAGIC SELECT * FROM main.salesforce_analytics.customer_360
# MAGIC ORDER BY total_won_revenue DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Customer Segmentation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Segment customers by value and engagement
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN total_won_revenue >= 1000000 THEN 'Enterprise'
# MAGIC     WHEN total_won_revenue >= 100000 THEN 'Mid-Market'
# MAGIC     WHEN total_won_revenue >= 10000 THEN 'SMB'
# MAGIC     ELSE 'Startup'
# MAGIC   END as customer_segment,
# MAGIC   COUNT(*) as account_count,
# MAGIC   SUM(total_won_revenue) as segment_revenue,
# MAGIC   AVG(total_won_revenue) as avg_revenue_per_account,
# MAGIC   AVG(contact_count) as avg_contacts,
# MAGIC   AVG(days_as_customer) as avg_customer_tenure_days
# MAGIC FROM main.salesforce_analytics.customer_360
# MAGIC WHERE total_won_revenue > 0
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY segment_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.3: High-Value Account Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify high-value accounts needing attention
# MAGIC SELECT 
# MAGIC   account_name,
# MAGIC   Industry,
# MAGIC   total_won_revenue,
# MAGIC   open_pipeline_value,
# MAGIC   open_cases,
# MAGIC   days_as_customer,
# MAGIC   DATEDIFF(CURRENT_DATE(), last_opportunity_date) as days_since_last_opp,
# MAGIC   CASE 
# MAGIC     WHEN open_cases > 3 THEN 'High Support Need'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), last_opportunity_date) > 180 THEN 'At Risk - No Recent Activity'
# MAGIC     WHEN open_pipeline_value > 500000 THEN 'High Pipeline Value'
# MAGIC     ELSE 'Healthy'
# MAGIC   END as account_status
# MAGIC FROM main.salesforce_analytics.customer_360
# MAGIC WHERE total_won_revenue > 100000
# MAGIC ORDER BY total_won_revenue DESC
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Lead Scoring with Machine Learning
# MAGIC
# MAGIC ### Understanding Lead Scoring
# MAGIC
# MAGIC Lead scoring predicts the likelihood a lead will convert to an opportunity using:
# MAGIC - Demographic data (company size, industry, title)
# MAGIC - Behavioral data (lead source, rating, engagement)
# MAGIC - Historical conversion patterns
# MAGIC
# MAGIC **Model Type**: Binary Classification (Converted vs Not Converted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.1: Prepare Lead Training Data

# COMMAND ----------

from pyspark.sql.functions import col, when, datediff, current_date

# Prepare lead dataset for modeling
lead_df = spark.table(f"{catalog_name}.{schema_name}.sf_leads")

# Feature engineering
lead_features = lead_df.select(
    col("Id").alias("lead_id"),
    col("IsConverted").cast("int").alias("label"),  # Target variable
    
    # Demographic features
    col("Industry"),
    col("Title"),
    col("Company"),
    
    # Behavioral features
    col("LeadSource"),
    col("Rating"),
    col("Status"),
    
    # Engagement features
    datediff(current_date(), col("CreatedDate")).alias("lead_age_days"),
    datediff(col("LastModifiedDate"), col("CreatedDate")).alias("days_to_last_activity")
).na.drop()

print(f"📊 Lead Dataset:")
print(f"   Total Leads: {lead_features.count():,}")
print(f"   Converted: {lead_features.filter(col('label') == 1).count():,}")
print(f"   Not Converted: {lead_features.filter(col('label') == 0).count():,}")

display(lead_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Encode Categorical Features

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml import Pipeline

# Define categorical columns
categorical_cols = ["Industry", "Title", "LeadSource", "Rating", "Status"]
numerical_cols = ["lead_age_days", "days_to_last_activity"]

# Create indexers for categorical features
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
    for col in categorical_cols
]

# Create one-hot encoders
encoders = [
    OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded")
    for col in categorical_cols
]

# Assemble features into vector
feature_cols = [f"{col}_encoded" for col in categorical_cols] + numerical_cols
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Build preprocessing pipeline
preprocessing_pipeline = Pipeline(stages=indexers + encoders + [assembler])

# Fit and transform
preprocessor = preprocessing_pipeline.fit(lead_features)
processed_leads = preprocessor.transform(lead_features)

print("✅ Features encoded and assembled")
display(processed_leads.select("lead_id", "label", "features").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.3: Train Lead Scoring Model

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Split data into train/test
train_data, test_data = processed_leads.randomSplit([0.8, 0.2], seed=42)

print(f"📊 Dataset Split:")
print(f"   Training: {train_data.count():,} leads")
print(f"   Testing: {test_data.count():,} leads")

# Train Random Forest classifier
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=5,
    seed=42
)

print("\n🎓 Training lead scoring model...")
rf_model = rf.fit(train_data)
print("✅ Model training complete")

# Make predictions
predictions = rf_model.transform(test_data)

# Evaluate model
evaluator_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

auc = evaluator_auc.evaluate(predictions)
accuracy = evaluator_acc.evaluate(predictions)

print(f"\n📊 Model Performance:")
print(f"   AUC-ROC: {auc:.3f}")
print(f"   Accuracy: {accuracy:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.4: Score Active Leads

# COMMAND ----------

from pyspark.sql.functions import round as spark_round

# Get unconverted leads
active_leads = lead_features.filter(col("label") == 0)

# Apply preprocessing and model
active_leads_processed = preprocessor.transform(active_leads)
lead_scores = rf_model.transform(active_leads_processed)

# Extract probability of conversion (second element of probability vector)
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import pyspark.ml.linalg as spark_linalg

extract_prob = udf(lambda v: float(v[1]), DoubleType())

lead_scores_final = lead_scores.select(
    col("lead_id"),
    col("Company"),
    col("Industry"),
    col("Title"),
    col("LeadSource"),
    col("Status"),
    extract_prob(col("probability")).alias("conversion_probability"),
    col("prediction")
).orderBy(col("conversion_probability"), ascending=False)

print("📊 Top Scored Leads (Most Likely to Convert):")
display(lead_scores_final.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Opportunity Win Prediction
# MAGIC
# MAGIC ### Step 5.1: Prepare Opportunity Features

# COMMAND ----------

# Load opportunities for modeling
opp_df = spark.table(f"{catalog_name}.{schema_name}.sf_opportunities")

# Join with account data for additional features
account_df = spark.table(f"{catalog_name}.{schema_name}.sf_accounts")

opp_with_account = opp_df.join(
    account_df.select("Id", "Industry", "AnnualRevenue", "NumberOfEmployees").alias("acct"),
    opp_df.AccountId == account_df.Id,
    "left"
)

# Feature engineering for opportunities
opp_features = opp_with_account.filter(col("IsClosed") == True).select(
    col("Id").alias("opportunity_id"),
    col("IsWon").cast("int").alias("label"),
    
    # Opportunity features
    col("Amount"),
    col("Probability"),
    col("Type"),
    col("LeadSource"),
    col("StageName"),
    
    # Account features
    col("Industry"),
    col("AnnualRevenue"),
    col("NumberOfEmployees"),
    
    # Time features
    datediff(col("CloseDate"), col("CreatedDate")).alias("sales_cycle_days")
).na.drop()

print(f"📊 Opportunity Dataset:")
print(f"   Total Closed Opportunities: {opp_features.count():,}")
print(f"   Won: {opp_features.filter(col('label') == 1).count():,}")
print(f"   Lost: {opp_features.filter(col('label') == 0).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.2: Train Win Prediction Model

# COMMAND ----------

# Define feature columns
opp_categorical_cols = ["Type", "LeadSource", "StageName", "Industry"]
opp_numerical_cols = ["Amount", "Probability", "AnnualRevenue", "NumberOfEmployees", "sales_cycle_days"]

# Build preprocessing pipeline
opp_indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
    for col in opp_categorical_cols
]

opp_encoders = [
    OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_encoded")
    for col in opp_categorical_cols
]

opp_feature_cols = [f"{col}_encoded" for col in opp_categorical_cols] + opp_numerical_cols
opp_assembler = VectorAssembler(inputCols=opp_feature_cols, outputCol="features", handleInvalid="skip")

opp_preprocessing = Pipeline(stages=opp_indexers + opp_encoders + [opp_assembler])

# Preprocess data
opp_preprocessor = opp_preprocessing.fit(opp_features)
opp_processed = opp_preprocessor.transform(opp_features)

# Split data
opp_train, opp_test = opp_processed.randomSplit([0.8, 0.2], seed=42)

# Train model
opp_rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=150,
    maxDepth=6,
    seed=42
)

print("🎓 Training opportunity win prediction model...")
opp_model = opp_rf.fit(opp_train)
print("✅ Model training complete")

# Evaluate
opp_predictions = opp_model.transform(opp_test)
opp_auc = evaluator_auc.evaluate(opp_predictions)
opp_accuracy = evaluator_acc.evaluate(opp_predictions)

print(f"\n📊 Opportunity Win Model Performance:")
print(f"   AUC-ROC: {opp_auc:.3f}")
print(f"   Accuracy: {opp_accuracy:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.3: Score Open Opportunities

# COMMAND ----------

# Get open opportunities
open_opps = opp_df.join(
    account_df.select("Id", "Industry", "AnnualRevenue", "NumberOfEmployees"),
    opp_df.AccountId == account_df.Id,
    "left"
).filter(col("IsClosed") == False).select(
    col("Id").alias("opportunity_id"),
    col("Name").alias("opportunity_name"),
    col("Amount"),
    col("Probability"),
    col("StageName"),
    col("Type"),
    col("LeadSource"),
    col("Industry"),
    col("AnnualRevenue"),
    col("NumberOfEmployees"),
    datediff(current_date(), col("CreatedDate")).alias("sales_cycle_days")
).na.drop()

# Score opportunities
open_opps_processed = opp_preprocessor.transform(open_opps)
opp_scores = opp_model.transform(open_opps_processed)

opp_scores_final = opp_scores.select(
    col("opportunity_id"),
    col("opportunity_name"),
    col("Amount"),
    col("StageName"),
    col("Probability").alias("salesforce_probability"),
    extract_prob(col("probability")).alias("ml_win_probability")
).orderBy(col("ml_win_probability"), ascending=False)

print("📊 Top Opportunities by ML Win Probability:")
display(opp_scores_final.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Customer Churn Prediction
# MAGIC
# MAGIC ### Understanding Churn Signals
# MAGIC
# MAGIC Indicators of potential churn:
# MAGIC - Declining opportunity creation
# MAGIC - Increasing support cases
# MAGIC - No recent engagement (contacts, activities)
# MAGIC - Contract renewal approaching

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.1: Calculate Churn Risk Score

# COMMAND ----------

from pyspark.sql.functions import months_between, coalesce

# Calculate churn risk features from customer 360
churn_risk_df = spark.sql(f"""
    SELECT 
        account_id,
        account_name,
        Industry,
        total_won_revenue,
        days_as_customer,
        
        -- Engagement metrics
        DATEDIFF(CURRENT_DATE(), last_opportunity_date) as days_since_last_opp,
        open_opportunities,
        open_cases,
        
        -- Calculate churn risk score (0-100)
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), last_opportunity_date) > 365 THEN 40
            WHEN DATEDIFF(CURRENT_DATE(), last_opportunity_date) > 180 THEN 25
            WHEN DATEDIFF(CURRENT_DATE(), last_opportunity_date) > 90 THEN 10
            ELSE 0
        END +
        CASE 
            WHEN open_cases > 5 THEN 30
            WHEN open_cases > 2 THEN 15
            ELSE 0
        END +
        CASE 
            WHEN open_opportunities = 0 THEN 20
            WHEN open_opportunities = 1 THEN 10
            ELSE 0
        END +
        CASE
            WHEN total_opportunities < 3 THEN 10
            ELSE 0
        END as churn_risk_score
        
    FROM {catalog_name}.{analytics_schema}.customer_360
    WHERE total_won_revenue > 0
""")

# Classify risk level
churn_risk_classified = churn_risk_df.withColumn(
    "risk_level",
    when(col("churn_risk_score") >= 70, "Critical")
    .when(col("churn_risk_score") >= 50, "High")
    .when(col("churn_risk_score") >= 30, "Medium")
    .otherwise("Low")
)

print("📊 Churn Risk Distribution:")
churn_risk_classified.groupBy("risk_level").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6.2: Identify At-Risk Customers

# COMMAND ----------

# Display high-risk accounts
at_risk_customers = churn_risk_classified.filter(
    col("churn_risk_score") >= 50
).orderBy(col("total_won_revenue"), ascending=False)

print("🔴 High-Risk Customers Requiring Attention:")
display(at_risk_customers.limit(20))

# Save as table for business users
at_risk_customers.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{analytics_schema}.at_risk_customers"
)
print(f"✅ At-risk customers saved to {catalog_name}.{analytics_schema}.at_risk_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Revenue Forecasting
# MAGIC
# MAGIC ### Step 7.1: Historical Revenue by Month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate monthly won revenue
# MAGIC CREATE OR REPLACE VIEW main.salesforce_analytics.monthly_revenue AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('MONTH', CloseDate) as revenue_month,
# MAGIC   COUNT(*) as deals_closed,
# MAGIC   SUM(Amount) as total_revenue,
# MAGIC   AVG(Amount) as avg_deal_size
# MAGIC FROM main.salesforce_raw.sf_opportunities
# MAGIC WHERE IsWon = true
# MAGIC   AND CloseDate IS NOT NULL
# MAGIC   AND Amount IS NOT NULL
# MAGIC GROUP BY DATE_TRUNC('MONTH', CloseDate)
# MAGIC ORDER BY revenue_month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display historical revenue trend
# MAGIC SELECT * FROM main.salesforce_analytics.monthly_revenue
# MAGIC ORDER BY revenue_month DESC
# MAGIC LIMIT 12

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7.2: Forecast Pipeline to Revenue
# MAGIC
# MAGIC %sql
# MAGIC -- Forecast expected revenue from open pipeline
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('MONTH', CloseDate) as expected_close_month,
# MAGIC   StageName,
# MAGIC   COUNT(*) as opportunity_count,
# MAGIC   SUM(Amount) as total_pipeline,
# MAGIC   AVG(Probability) as avg_probability,
# MAGIC   SUM(Amount * Probability / 100) as weighted_forecast
# MAGIC FROM main.salesforce_raw.sf_opportunities
# MAGIC WHERE IsClosed = false
# MAGIC   AND CloseDate IS NOT NULL
# MAGIC   AND Amount IS NOT NULL
# MAGIC   AND CloseDate >= CURRENT_DATE()
# MAGIC GROUP BY DATE_TRUNC('MONTH', CloseDate), StageName
# MAGIC ORDER BY expected_close_month, StageName

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What You Accomplished ✅
# MAGIC
# MAGIC In this lab, you:
# MAGIC 1. Built comprehensive sales pipeline analytics and reporting
# MAGIC 2. Created Customer 360 views combining multiple Salesforce objects
# MAGIC 3. Segmented customers by value and engagement level
# MAGIC 4. Trained ML models for lead scoring with Random Forest
# MAGIC 5. Built opportunity win prediction models
# MAGIC 6. Implemented churn risk scoring for proactive retention
# MAGIC 7. Created revenue forecasting from pipeline data
# MAGIC
# MAGIC ### Key Insights 💡
# MAGIC
# MAGIC **Sales Pipeline:**
# MAGIC - Analyzed opportunity values across stages
# MAGIC - Identified win rates by industry and segment
# MAGIC - Calculated average sales cycle lengths
# MAGIC
# MAGIC **Customer 360:**
# MAGIC - Unified view of account, contact, opportunity, and case data
# MAGIC - Customer segmentation by revenue and engagement
# MAGIC - Identified high-value accounts needing attention
# MAGIC
# MAGIC **Predictive Analytics:**
# MAGIC - Lead scoring to prioritize sales efforts (AUC typically 0.7-0.85)
# MAGIC - Opportunity win prediction for accurate forecasting
# MAGIC - Churn risk identification for proactive retention
# MAGIC
# MAGIC ### Best Practices Applied 🎯
# MAGIC
# MAGIC - ✅ Used SQL for business-friendly analytics
# MAGIC - ✅ Created reusable views in analytics schema
# MAGIC - ✅ Applied feature engineering for ML models
# MAGIC - ✅ Evaluated models with appropriate metrics (AUC, Accuracy)
# MAGIC - ✅ Scored active records for business action
# MAGIC - ✅ Saved results to Delta tables for BI tools
# MAGIC
# MAGIC ### Next Steps for Production 🚀
# MAGIC
# MAGIC **1. Operationalize ML Models:**
# MAGIC - Register models in MLflow Model Registry
# MAGIC - Set up scheduled scoring jobs
# MAGIC - Implement A/B testing for model versions
# MAGIC - Monitor model performance and drift
# MAGIC
# MAGIC **2. Build Dashboards:**
# MAGIC - Connect Databricks SQL to analytics views
# MAGIC - Create executive dashboards in Databricks SQL or BI tools
# MAGIC - Set up alerts for high-risk customers
# MAGIC - Implement real-time pipeline tracking
# MAGIC
# MAGIC **3. Automate with Delta Live Tables:**
# MAGIC - Create DLT pipelines for incremental transformations
# MAGIC - Implement data quality checks
# MAGIC - Build bronze → silver → gold architecture
# MAGIC - Schedule automated refreshes
# MAGIC
# MAGIC **4. Enhance with Additional Data:**
# MAGIC - Join with product usage data
# MAGIC - Incorporate marketing campaign data
# MAGIC - Add financial data for CLV calculation
# MAGIC - Include support ticket sentiment analysis
# MAGIC
# MAGIC ### Additional Resources 📚
# MAGIC
# MAGIC - **Databricks MLflow**: Model tracking and deployment
# MAGIC - **Databricks SQL**: Business intelligence and dashboards
# MAGIC - **Delta Live Tables**: Automated ETL pipelines
# MAGIC - **Unity Catalog**: Data governance and lineage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You've completed the Salesforce Lakeflow Connect workshop. You now have the skills to integrate, analyze, and derive predictive insights from Salesforce data in Databricks! 🎉
