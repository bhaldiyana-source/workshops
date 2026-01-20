# Databricks notebook source
# MAGIC %md
# MAGIC # Online Feature Store for Feature Lookups
# MAGIC
# MAGIC ## Overview
# MAGIC This demo explores the implementation of **online feature stores** in Databricks, enabling low-latency feature serving for real-time machine learning applications. You'll learn how to:
# MAGIC
# MAGIC - Configure and manage online feature stores using Lakebase PostgreSQL
# MAGIC - Publish offline feature tables to online stores for fast lookups
# MAGIC - Create Python UDFs for on-demand feature computation
# MAGIC - Build training sets combining online lookups with on-demand features
# MAGIC - Deploy models with automatic feature lookup capabilities
# MAGIC
# MAGIC ### Why Online Feature Stores?
# MAGIC
# MAGIC **Traditional ML Feature Serving Challenges:**
# MAGIC - Batch-computed features stored in Delta Lake are optimized for analytics, not low-latency lookups
# MAGIC - Real-time inference requires sub-millisecond feature retrieval
# MAGIC - Training/serving skew when feature computation differs between environments
# MAGIC
# MAGIC **Online Feature Store Benefits:**
# MAGIC - **Low Latency**: Sub-millisecond feature lookups using optimized OLTP databases
# MAGIC - **Consistency**: Same feature computation logic for training and serving
# MAGIC - **Freshness**: Near real-time sync from offline to online stores
# MAGIC - **Scalability**: Handle high-throughput inference workloads
# MAGIC
# MAGIC ### Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Offline Feature Store                     │
# MAGIC │              (Delta Tables in Unity Catalog)                 │
# MAGIC └────────────────────┬────────────────────────────────────────┘
# MAGIC                      │ Publish & Sync
# MAGIC                      ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Online Feature Store                      │
# MAGIC │                  (Lakebase PostgreSQL)                       │
# MAGIC └────────────────────┬────────────────────────────────────────┘
# MAGIC                      │ Low-latency Lookup
# MAGIC                      ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │               Model Serving Endpoint                         │
# MAGIC │          (Automatic Feature Retrieval)                       │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Environment Setup and Prerequisites
# MAGIC
# MAGIC Before we begin, we need to:
# MAGIC 1. Install required libraries
# MAGIC 2. Configure workspace parameters
# MAGIC 3. Verify Lakebase availability
# MAGIC 4. Set up Unity Catalog resources

# COMMAND ----------

# Install required packages
%pip install databricks-feature-engineering databricks-sdk mlflow scikit-learn psycopg2-binary --upgrade --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
import time
import uuid
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Feature Engineering imports
from databricks.feature_engineering import FeatureEngineeringClient, FeatureFunction, FeatureLookup

# MLflow for model tracking
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

# Databricks SDK for Lakebase management
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import OnlineTableSpec, OnlineTableSpecTriggeredSchedulingPolicy

print("✓ All libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Workspace Parameters
# MAGIC
# MAGIC **Important**: Update these values based on your environment:
# MAGIC - `catalog`: Your Unity Catalog name (must have CREATE privileges)
# MAGIC - `schema`: Schema where feature tables will be created
# MAGIC - `lakebase_instance_name`: Name for your Lakebase PostgreSQL instance

# COMMAND ----------

# Configuration
username = spark.sql("SELECT current_user()").first()[0]
username_prefix = username.split("@")[0].replace(".", "_").replace("-", "_")

# Unity Catalog configuration
catalog = "main"  # Update with your catalog name
schema = f"{username_prefix}_online_feature_store"
checkpoint_location = f"/tmp/{username_prefix}/online_feature_store_checkpoints"

# Lakebase configuration
lakebase_instance_name = f"online_fs_{username_prefix}"

# Feature table names
user_features_table = f"{catalog}.{schema}.user_features"
item_features_table = f"{catalog}.{schema}.item_features"
interaction_features_table = f"{catalog}.{schema}.interaction_features"

# Model registry
model_name = f"{catalog}.{schema}.online_feature_model"

print(f"""
Configuration Summary:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Catalog:                {catalog}
Schema:                 {schema}
Lakebase Instance:      {lakebase_instance_name}
User Features Table:    {user_features_table}
Item Features Table:    {item_features_table}
Interaction Table:      {interaction_features_table}
Model Name:             {model_name}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"✓ Schema {catalog}.{schema} is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize Feature Engineering Client
# MAGIC
# MAGIC The `FeatureEngineeringClient` is the primary interface for:
# MAGIC - Creating and managing feature tables
# MAGIC - Building training sets with automatic feature lookups
# MAGIC - Logging models with feature store metadata

# COMMAND ----------

# Initialize Feature Engineering Client
fe = FeatureEngineeringClient()
print("✓ Feature Engineering Client initialized")

# Initialize Workspace Client for Lakebase operations
w = WorkspaceClient()
print("✓ Workspace Client initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Create Offline Feature Tables
# MAGIC
# MAGIC We'll create three feature tables representing a common ML scenario:
# MAGIC
# MAGIC 1. **User Features**: Static and slowly-changing user attributes
# MAGIC 2. **Item Features**: Characteristics of items/products
# MAGIC 3. **Interaction Features**: Aggregated user-item interaction statistics
# MAGIC
# MAGIC ### Feature Table Requirements for Online Publishing
# MAGIC
# MAGIC To publish a feature table to an online store, it must:
# MAGIC - Have a **primary key** (used for fast lookups)
# MAGIC - Optionally include a **timestamp column** for point-in-time lookups
# MAGIC - Be a **Delta table** in Unity Catalog
# MAGIC - Have proper **Change Data Feed (CDF)** enabled for incremental sync

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Create User Features Table
# MAGIC
# MAGIC User features represent attributes about users that change infrequently.
# MAGIC Examples: demographics, preferences, account status, risk scores.

# COMMAND ----------

# Generate synthetic user feature data
np.random.seed(42)
n_users = 1000

user_data = pd.DataFrame({
    'user_id': [f'user_{i:04d}' for i in range(n_users)],
    'account_age_days': np.random.randint(1, 365*5, n_users),
    'total_purchases': np.random.randint(0, 100, n_users),
    'average_purchase_value': np.round(np.random.uniform(10, 500, n_users), 2),
    'user_segment': np.random.choice(['bronze', 'silver', 'gold', 'platinum'], n_users),
    'risk_score': np.round(np.random.uniform(0, 1, n_users), 3),
    'preferred_category': np.random.choice(['electronics', 'clothing', 'home', 'sports', 'books'], n_users),
    'is_active': np.random.choice([True, False], n_users, p=[0.8, 0.2]),
    'timestamp': [datetime.now() - timedelta(minutes=i) for i in range(n_users)]
})

# Convert to Spark DataFrame
user_features_df = spark.createDataFrame(user_data)

# Create feature table
fe.create_table(
    name=user_features_table,
    primary_keys=['user_id'],
    timestamp_keys=['timestamp'],
    df=user_features_df,
    description='User profile features including demographics, behavior, and risk scores'
)

print(f"✓ Created user features table with {user_features_df.count()} records")
display(user_features_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Create Item Features Table
# MAGIC
# MAGIC Item features describe characteristics of products, content, or entities.
# MAGIC Examples: category, price, popularity, quality scores.

# COMMAND ----------

# Generate synthetic item feature data
n_items = 500

item_data = pd.DataFrame({
    'item_id': [f'item_{i:04d}' for i in range(n_items)],
    'category': np.random.choice(['electronics', 'clothing', 'home', 'sports', 'books'], n_items),
    'price': np.round(np.random.uniform(5, 1000, n_items), 2),
    'popularity_score': np.round(np.random.uniform(0, 100, n_items), 2),
    'quality_rating': np.round(np.random.uniform(1, 5, n_items), 2),
    'inventory_status': np.random.choice(['in_stock', 'low_stock', 'out_of_stock'], n_items, p=[0.7, 0.2, 0.1]),
    'days_since_launch': np.random.randint(1, 1000, n_items),
    'num_reviews': np.random.randint(0, 5000, n_items),
    'is_featured': np.random.choice([True, False], n_items, p=[0.1, 0.9]),
    'timestamp': [datetime.now() - timedelta(minutes=i) for i in range(n_items)]
})

# Convert to Spark DataFrame
item_features_df = spark.createDataFrame(item_data)

# Create feature table
fe.create_table(
    name=item_features_table,
    primary_keys=['item_id'],
    timestamp_keys=['timestamp'],
    df=item_features_df,
    description='Item/product features including category, pricing, and popularity metrics'
)

print(f"✓ Created item features table with {item_features_df.count()} records")
display(item_features_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Create Interaction Features Table
# MAGIC
# MAGIC Interaction features capture aggregated statistics about user-item interactions.
# MAGIC These features are typically computed from event streams or logs.

# COMMAND ----------

# Generate synthetic interaction feature data
n_interactions = 2000

# Ensure we have diverse user-item pairs
interaction_data = pd.DataFrame({
    'user_id': [f'user_{np.random.randint(0, n_users):04d}' for _ in range(n_interactions)],
    'item_id': [f'item_{np.random.randint(0, n_items):04d}' for _ in range(n_interactions)],
})

# Remove duplicates to ensure unique (user_id, item_id) pairs
interaction_data = interaction_data.drop_duplicates()

# Add interaction features
interaction_data['view_count'] = np.random.randint(1, 50, len(interaction_data))
interaction_data['click_count'] = np.random.randint(0, 20, len(interaction_data))
interaction_data['cart_add_count'] = np.random.randint(0, 10, len(interaction_data))
interaction_data['purchase_count'] = np.random.randint(0, 5, len(interaction_data))
interaction_data['avg_time_on_page_seconds'] = np.random.randint(10, 600, len(interaction_data))
interaction_data['days_since_last_interaction'] = np.random.randint(0, 90, len(interaction_data))
interaction_data['interaction_score'] = np.round(np.random.uniform(0, 1, len(interaction_data)), 3)
interaction_data['timestamp'] = [datetime.now() - timedelta(minutes=i) for i in range(len(interaction_data))]

# Convert to Spark DataFrame
interaction_features_df = spark.createDataFrame(interaction_data)

# Create feature table with composite primary key
fe.create_table(
    name=interaction_features_table,
    primary_keys=['user_id', 'item_id'],
    timestamp_keys=['timestamp'],
    df=interaction_features_df,
    description='User-item interaction features capturing engagement metrics'
)

print(f"✓ Created interaction features table with {interaction_features_df.count()} records")
display(interaction_features_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Create Python UDFs for On-Demand Features
# MAGIC
# MAGIC **On-Demand Features** are computed in real-time during inference rather than pre-computed.
# MAGIC This is useful for:
# MAGIC - Time-sensitive features (hour of day, day of week)
# MAGIC - Request-specific features (distance, time since last action)
# MAGIC - Features that depend on inference-time context
# MAGIC
# MAGIC ### Benefits of Python UDFs as Features
# MAGIC - **Consistency**: Same code runs during training and inference
# MAGIC - **Unity Catalog Integration**: Versioned, governed, and reusable
# MAGIC - **Type Safety**: Enforced input/output schemas
# MAGIC - **Lineage**: Automatic tracking of feature dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create Time-Based Feature UDF
# MAGIC
# MAGIC Extract temporal features from timestamps that are valuable for time-sensitive predictions.

# COMMAND ----------

# Register time-based feature extraction UDF
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.extract_time_features(request_timestamp TIMESTAMP)
RETURNS STRUCT<
  hour_of_day: INT,
  day_of_week: INT,
  is_weekend: BOOLEAN,
  is_business_hours: BOOLEAN,
  day_of_month: INT,
  month: INT
>
LANGUAGE PYTHON
AS $$
  from datetime import datetime
  
  hour = request_timestamp.hour
  day_of_week = request_timestamp.weekday()  # 0=Monday, 6=Sunday
  day_of_month = request_timestamp.day
  month = request_timestamp.month
  
  is_weekend = day_of_week >= 5  # Saturday=5, Sunday=6
  is_business_hours = 9 <= hour <= 17 and not is_weekend
  
  return (hour, day_of_week, is_weekend, is_business_hours, day_of_month, month)
$$
""")

print(f"✓ Created time-based feature UDF: {catalog}.{schema}.extract_time_features")

# Test the UDF
test_time_features = spark.sql(f"""
SELECT 
  current_timestamp() as request_time,
  {catalog}.{schema}.extract_time_features(current_timestamp()) as time_features
""")
display(test_time_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Create User Recency Feature UDF
# MAGIC
# MAGIC Calculate how recently a user performed various actions.

# COMMAND ----------

# Register user recency calculation UDF
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.calculate_recency_score(
  days_since_last_interaction INT,
  account_age_days INT
)
RETURNS DOUBLE
LANGUAGE PYTHON
AS $$
  # Calculate a recency score (0-1) based on interaction recency
  # More recent interactions = higher score
  
  if days_since_last_interaction is None or account_age_days is None:
    return 0.5  # neutral score for missing data
  
  if account_age_days == 0:
    return 0.5
  
  # Normalize by account age and apply decay
  recency_ratio = days_since_last_interaction / max(account_age_days, 1)
  score = max(0.0, min(1.0, 1.0 - recency_ratio))
  
  return float(score)
$$
""")

print(f"✓ Created recency score UDF: {catalog}.{schema}.calculate_recency_score")

# Test the UDF
test_recency = spark.sql(f"""
SELECT 
  {catalog}.{schema}.calculate_recency_score(5, 100) as recent_user,
  {catalog}.{schema}.calculate_recency_score(80, 100) as inactive_user,
  {catalog}.{schema}.calculate_recency_score(1, 365) as very_recent_user
""")
display(test_recency)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Create Feature Interaction UDF
# MAGIC
# MAGIC Combine multiple features to create derived interaction features.

# COMMAND ----------

# Register feature interaction UDF
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.create_interaction_features(
  user_segment STRING,
  item_category STRING,
  user_risk_score DOUBLE,
  item_price DOUBLE
)
RETURNS STRUCT<
  segment_category_match: BOOLEAN,
  affordability_score: DOUBLE,
  risk_adjusted_value: DOUBLE
>
LANGUAGE PYTHON
AS $$
  # Create interaction features between user and item
  
  # Check if user's preferred segment matches item category
  segment_category_match = user_segment == item_category
  
  # Calculate affordability (higher score = more affordable for user)
  # Assuming higher segment = higher purchasing power
  segment_multiplier = {
    'bronze': 1.0,
    'silver': 1.5,
    'gold': 2.0,
    'platinum': 3.0
  }.get(user_segment, 1.0)
  
  affordability_score = min(1.0, segment_multiplier * 100 / max(item_price, 1))
  
  # Risk-adjusted value (lower risk = higher value)
  risk_adjusted_value = (1.0 - user_risk_score) * affordability_score
  
  return (segment_category_match, float(affordability_score), float(risk_adjusted_value))
$$
""")

print(f"✓ Created interaction features UDF: {catalog}.{schema}.create_interaction_features")

# Test the UDF
test_interaction = spark.sql(f"""
SELECT 
  {catalog}.{schema}.create_interaction_features(
    'gold', 
    'electronics', 
    0.2, 
    150.0
  ) as interaction_features
""")
display(test_interaction)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Configure Online Feature Store with Lakebase
# MAGIC
# MAGIC Now we'll set up the online feature store infrastructure using **Lakebase PostgreSQL**.
# MAGIC
# MAGIC ### What is Lakebase?
# MAGIC - Managed PostgreSQL service integrated with Databricks
# MAGIC - Optimized for low-latency transactional workloads
# MAGIC - Seamless integration with Unity Catalog and Feature Engineering
# MAGIC - Automatic synchronization from Delta tables
# MAGIC
# MAGIC ### Online Table Synchronization
# MAGIC Databricks automatically syncs data from offline (Delta) to online (PostgreSQL) tables:
# MAGIC - **Initial Load**: Full table snapshot on first publish
# MAGIC - **Incremental Updates**: CDF-based change propagation
# MAGIC - **Scheduling**: Triggered (immediate) or continuous sync modes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Create Lakebase Instance
# MAGIC
# MAGIC **Note**: Lakebase instance creation may take 5-10 minutes. This is a one-time setup.

# COMMAND ----------

# Check if Lakebase instance already exists
try:
    existing_instance = w.lakebases.get(name=lakebase_instance_name)
    print(f"✓ Lakebase instance '{lakebase_instance_name}' already exists")
    print(f"  Status: {existing_instance.state}")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"Creating new Lakebase instance: {lakebase_instance_name}")
        print("⏳ This may take 5-10 minutes...")
        
        # Create Lakebase instance
        create_response = w.lakebases.create(
            name=lakebase_instance_name,
            description=f"Online feature store for {username}"
        )
        
        # Wait for instance to be ready
        print("Waiting for instance to be active...")
        while True:
            instance = w.lakebases.get(name=lakebase_instance_name)
            if instance.state == "ACTIVE":
                print(f"✓ Lakebase instance '{lakebase_instance_name}' is ready")
                break
            elif instance.state == "FAILED":
                raise Exception(f"Lakebase instance creation failed: {instance.state_message}")
            else:
                print(f"  Current status: {instance.state}")
                time.sleep(30)
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Publish Feature Tables to Online Store
# MAGIC
# MAGIC Publishing a feature table to the online store:
# MAGIC 1. Creates a corresponding table in Lakebase PostgreSQL
# MAGIC 2. Performs initial data sync from Delta to PostgreSQL
# MAGIC 3. Sets up continuous or triggered sync pipeline
# MAGIC 4. Creates indexes on primary keys for fast lookups
# MAGIC
# MAGIC ### Sync Policies
# MAGIC - **Triggered**: Manual sync on-demand (good for testing)
# MAGIC - **Continuous**: Automatic sync with configurable lag (production use)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Publish User Features Table

# COMMAND ----------

# Create online table specification for user features
user_online_table_name = f"{user_features_table}_online"

try:
    # Check if online table already exists
    w.online_tables.get(name=user_online_table_name)
    print(f"✓ Online table '{user_online_table_name}' already exists")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"Creating online table: {user_online_table_name}")
        
        # Create online table with triggered scheduling
        spec = OnlineTableSpec(
            source_table_full_name=user_features_table,
            primary_key_columns=["user_id"],
            run_triggered=OnlineTableSpecTriggeredSchedulingPolicy()
        )
        
        w.online_tables.create(
            name=user_online_table_name,
            spec=spec
        )
        
        print(f"✓ Created online table: {user_online_table_name}")
        print("⏳ Initial sync in progress...")
        
        # Wait for initial sync to complete
        time.sleep(10)
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Publish Item Features Table

# COMMAND ----------

# Create online table for item features
item_online_table_name = f"{item_features_table}_online"

try:
    w.online_tables.get(name=item_online_table_name)
    print(f"✓ Online table '{item_online_table_name}' already exists")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"Creating online table: {item_online_table_name}")
        
        spec = OnlineTableSpec(
            source_table_full_name=item_features_table,
            primary_key_columns=["item_id"],
            run_triggered=OnlineTableSpecTriggeredSchedulingPolicy()
        )
        
        w.online_tables.create(
            name=item_online_table_name,
            spec=spec
        )
        
        print(f"✓ Created online table: {item_online_table_name}")
        time.sleep(10)
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Publish Interaction Features Table

# COMMAND ----------

# Create online table for interaction features (composite key)
interaction_online_table_name = f"{interaction_features_table}_online"

try:
    w.online_tables.get(name=interaction_online_table_name)
    print(f"✓ Online table '{interaction_online_table_name}' already exists")
except Exception as e:
    if "not found" in str(e).lower():
        print(f"Creating online table: {interaction_online_table_name}")
        
        spec = OnlineTableSpec(
            source_table_full_name=interaction_features_table,
            primary_key_columns=["user_id", "item_id"],  # Composite key
            run_triggered=OnlineTableSpecTriggeredSchedulingPolicy()
        )
        
        w.online_tables.create(
            name=interaction_online_table_name,
            spec=spec
        )
        
        print(f"✓ Created online table: {interaction_online_table_name}")
        time.sleep(10)
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Verify Online Tables Status

# COMMAND ----------

# List all online tables and check their status
print("Online Feature Store Tables Status:")
print("=" * 80)

for table_name in [user_online_table_name, item_online_table_name, interaction_online_table_name]:
    try:
        table_info = w.online_tables.get(name=table_name)
        print(f"\n📊 {table_name}")
        print(f"   Status: {table_info.status.detailed_state}")
        if hasattr(table_info.status, 'provisioning_status'):
            print(f"   Provisioning: {table_info.status.provisioning_status.initial_pipeline_sync_progress}")
    except Exception as e:
        print(f"\n❌ {table_name}: Error - {e}")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Build Training Set with Feature Lookups
# MAGIC
# MAGIC Now we'll create a training dataset that combines:
# MAGIC 1. **Offline feature lookups** from our published tables
# MAGIC 2. **On-demand features** computed via Python UDFs
# MAGIC
# MAGIC The Feature Engineering Client will:
# MAGIC - Automatically join features based on keys
# MAGIC - Apply point-in-time correctness for temporal features
# MAGIC - Execute on-demand feature functions
# MAGIC - Track lineage for model reproducibility

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Create Training Labels Dataset
# MAGIC
# MAGIC First, create a dataset with our prediction targets and lookup keys.
# MAGIC For this example, we'll predict user purchase propensity.

# COMMAND ----------

# Generate training data with labels
n_training_samples = 500

# Create diverse user-item pairs
training_user_ids = np.random.choice([f'user_{i:04d}' for i in range(n_users)], n_training_samples)
training_item_ids = np.random.choice([f'item_{i:04d}' for i in range(n_items)], n_training_samples)

training_data = pd.DataFrame({
    'request_id': [f'req_{uuid.uuid4().hex[:8]}' for _ in range(n_training_samples)],
    'user_id': training_user_ids,
    'item_id': training_item_ids,
    'request_timestamp': [datetime.now() - timedelta(hours=np.random.randint(0, 24*30)) for _ in range(n_training_samples)],
    # Binary label: will user purchase?
    'purchased': np.random.choice([0, 1], n_training_samples, p=[0.7, 0.3])
})

training_df = spark.createDataFrame(training_data)

print(f"✓ Created training dataset with {training_df.count()} samples")
display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Define Feature Lookups
# MAGIC
# MAGIC Specify which features to retrieve from each feature table.

# COMMAND ----------

# Define feature lookups
feature_lookups = [
    # User features lookup
    FeatureLookup(
        table_name=user_features_table,
        feature_names=[
            'account_age_days',
            'total_purchases',
            'average_purchase_value',
            'user_segment',
            'risk_score',
            'preferred_category',
            'is_active'
        ],
        lookup_key='user_id'
    ),
    
    # Item features lookup
    FeatureLookup(
        table_name=item_features_table,
        feature_names=[
            'category',
            'price',
            'popularity_score',
            'quality_rating',
            'inventory_status',
            'days_since_launch',
            'num_reviews',
            'is_featured'
        ],
        lookup_key='item_id'
    ),
    
    # Interaction features lookup (composite key)
    FeatureLookup(
        table_name=interaction_features_table,
        feature_names=[
            'view_count',
            'click_count',
            'cart_add_count',
            'purchase_count',
            'avg_time_on_page_seconds',
            'days_since_last_interaction',
            'interaction_score'
        ],
        lookup_key=['user_id', 'item_id']
    )
]

print(f"✓ Defined {len(feature_lookups)} feature lookups")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Define On-Demand Feature Functions

# COMMAND ----------

# Define on-demand feature functions (Python UDFs)
feature_functions = [
    # Time-based features
    FeatureFunction(
        udf_name=f"{catalog}.{schema}.extract_time_features",
        input_bindings={'request_timestamp': 'request_timestamp'},
        output_name='time_features'
    ),
    
    # Recency score
    FeatureFunction(
        udf_name=f"{catalog}.{schema}.calculate_recency_score",
        input_bindings={
            'days_since_last_interaction': 'days_since_last_interaction',
            'account_age_days': 'account_age_days'
        },
        output_name='recency_score'
    ),
    
    # Interaction features
    FeatureFunction(
        udf_name=f"{catalog}.{schema}.create_interaction_features",
        input_bindings={
            'user_segment': 'user_segment',
            'item_category': 'category',
            'user_risk_score': 'risk_score',
            'item_price': 'price'
        },
        output_name='derived_features'
    )
]

print(f"✓ Defined {len(feature_functions)} on-demand feature functions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Create Training Set with Feature Engineering
# MAGIC
# MAGIC The `create_training_set` method will:
# MAGIC - Join all feature lookups based on keys
# MAGIC - Execute on-demand feature functions
# MAGIC - Maintain feature lineage
# MAGIC - Return a DataFrame ready for model training

# COMMAND ----------

# Create training set with all features
training_set = fe.create_training_set(
    df=training_df,
    feature_lookups=feature_lookups,
    feature_functions=feature_functions,
    label='purchased',
    exclude_columns=['request_id', 'request_timestamp']
)

# Load the training data as DataFrame
training_data_with_features = training_set.load_df()

print(f"✓ Created training set with {training_data_with_features.count()} rows")
print(f"✓ Total columns: {len(training_data_with_features.columns)}")
print(f"\nSchema:")
training_data_with_features.printSchema()

# COMMAND ----------

# Display sample of training data with all features
display(training_data_with_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Train and Register Model with Feature Store
# MAGIC
# MAGIC Now we'll train a machine learning model using the feature store training set.
# MAGIC The model will be logged with:
# MAGIC - Feature store metadata (lineage)
# MAGIC - Feature lookup specifications
# MAGIC - On-demand feature function references
# MAGIC
# MAGIC This enables **automatic feature retrieval** during inference!

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Prepare Data for Training

# COMMAND ----------

# Convert to Pandas for scikit-learn
training_pdf = training_data_with_features.toPandas()

# Extract nested struct columns
training_pdf['hour_of_day'] = training_pdf['time_features'].apply(lambda x: x['hour_of_day'] if x else None)
training_pdf['day_of_week'] = training_pdf['time_features'].apply(lambda x: x['day_of_week'] if x else None)
training_pdf['is_weekend'] = training_pdf['time_features'].apply(lambda x: int(x['is_weekend']) if x else None)
training_pdf['is_business_hours'] = training_pdf['time_features'].apply(lambda x: int(x['is_business_hours']) if x else None)

training_pdf['affordability_score'] = training_pdf['derived_features'].apply(lambda x: x['affordability_score'] if x else None)
training_pdf['risk_adjusted_value'] = training_pdf['derived_features'].apply(lambda x: x['risk_adjusted_value'] if x else None)

# Select numeric and boolean features for training
feature_columns = [
    'account_age_days', 'total_purchases', 'average_purchase_value', 'risk_score',
    'price', 'popularity_score', 'quality_rating', 'days_since_launch', 'num_reviews',
    'view_count', 'click_count', 'cart_add_count', 'purchase_count', 
    'avg_time_on_page_seconds', 'days_since_last_interaction', 'interaction_score',
    'recency_score', 'hour_of_day', 'day_of_week', 'is_weekend', 
    'is_business_hours', 'affordability_score', 'risk_adjusted_value',
    'is_active', 'is_featured'
]

# Convert boolean columns to int
training_pdf['is_active'] = training_pdf['is_active'].astype(int)
training_pdf['is_featured'] = training_pdf['is_featured'].astype(int)

# Handle missing values
X = training_pdf[feature_columns].fillna(0)
y = training_pdf['purchased']

print(f"✓ Prepared training data: {X.shape[0]} samples, {X.shape[1]} features")
print(f"✓ Label distribution: {y.value_counts().to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Train Model with MLflow Tracking

# COMMAND ----------

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Start MLflow run
mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{username}/online_feature_store_demo"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="random_forest_with_online_features") as run:
    
    # Train model
    print("Training Random Forest model...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    
    # Evaluate
    train_score = accuracy_score(y_train, model.predict(X_train))
    test_score = accuracy_score(y_test, model.predict(X_test))
    
    print(f"✓ Training Accuracy: {train_score:.4f}")
    print(f"✓ Test Accuracy: {test_score:.4f}")
    
    # Log metrics
    mlflow.log_metric("train_accuracy", train_score)
    mlflow.log_metric("test_accuracy", test_score)
    
    # Log parameters
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    
    # Log feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nTop 10 Most Important Features:")
    print(feature_importance.head(10).to_string(index=False))
    
    run_id = run.info.run_id
    print(f"\n✓ MLflow Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Log Model with Feature Store Integration
# MAGIC
# MAGIC This is the key step! We'll use `fe.log_model()` instead of `mlflow.log_model()`.
# MAGIC This automatically captures:
# MAGIC - Feature table references
# MAGIC - Lookup keys
# MAGIC - On-demand feature functions
# MAGIC - Feature lineage

# COMMAND ----------

# Log model with feature store integration
with mlflow.start_run(run_id=run_id):
    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=model_name
    )

print(f"✓ Model logged with feature store integration")
print(f"✓ Registered as: {model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Model Inference with Automatic Feature Lookup
# MAGIC
# MAGIC The magic of online feature stores! When we score the model:
# MAGIC 1. Provide only the lookup keys (user_id, item_id, request_timestamp)
# MAGIC 2. Model automatically fetches features from online store
# MAGIC 3. On-demand features are computed in real-time
# MAGIC 4. All feature transformations applied
# MAGIC 5. Predictions returned
# MAGIC
# MAGIC **No manual feature engineering needed during inference!**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Create Inference Requests
# MAGIC
# MAGIC For inference, we only need the lookup keys - not the features!

# COMMAND ----------

# Create inference requests with only keys and timestamp
n_inference_samples = 20

inference_data = pd.DataFrame({
    'user_id': [f'user_{np.random.randint(0, n_users):04d}' for _ in range(n_inference_samples)],
    'item_id': [f'item_{np.random.randint(0, n_items):04d}' for _ in range(n_inference_samples)],
    'request_timestamp': [datetime.now() for _ in range(n_inference_samples)]
})

inference_df = spark.createDataFrame(inference_data)

print(f"✓ Created {n_inference_samples} inference requests")
print("\nInference requests contain ONLY keys:")
display(inference_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Load Model and Score with Feature Store
# MAGIC
# MAGIC The `fe.score_batch()` method handles everything automatically.

# COMMAND ----------

# Load the latest model version
model_uri = f"models:/{model_name}/latest"

# Score batch with automatic feature lookup
print("Scoring batch with automatic feature lookup from online store...")
print("⏳ Features will be fetched from Lakebase and UDFs will be computed...")

predictions_df = fe.score_batch(
    model_uri=model_uri,
    df=inference_df
)

print("\n✓ Scoring complete!")
print(f"✓ Generated {predictions_df.count()} predictions")
print("\nPredictions with automatic feature retrieval:")
display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Performance Benchmarking
# MAGIC
# MAGIC Let's measure the latency of online feature lookups.

# COMMAND ----------

# Benchmark online feature lookup performance
import time

# Test with different batch sizes
batch_sizes = [1, 10, 50, 100]
results = []

for batch_size in batch_sizes:
    # Create test batch
    test_data = pd.DataFrame({
        'user_id': [f'user_{np.random.randint(0, n_users):04d}' for _ in range(batch_size)],
        'item_id': [f'item_{np.random.randint(0, n_items):04d}' for _ in range(batch_size)],
        'request_timestamp': [datetime.now() for _ in range(batch_size)]
    })
    test_df = spark.createDataFrame(test_data)
    
    # Measure latency
    start_time = time.time()
    predictions = fe.score_batch(model_uri=model_uri, df=test_df)
    _ = predictions.count()  # Force computation
    end_time = time.time()
    
    latency = end_time - start_time
    avg_latency_ms = (latency / batch_size) * 1000
    
    results.append({
        'batch_size': batch_size,
        'total_latency_sec': round(latency, 3),
        'avg_latency_per_sample_ms': round(avg_latency_ms, 2)
    })

# Display results
benchmark_df = pd.DataFrame(results)
print("\n📊 Online Feature Store Performance Benchmark:")
print(benchmark_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Monitoring and Management
# MAGIC
# MAGIC Best practices for production online feature stores.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Monitor Online Table Sync Status

# COMMAND ----------

# Check sync status for all online tables
def check_online_table_status(table_name):
    """Check the status of an online table"""
    try:
        table_info = w.online_tables.get(name=table_name)
        status = {
            'table': table_name.split('.')[-1],
            'state': table_info.status.detailed_state,
            'message': table_info.status.message if hasattr(table_info.status, 'message') else 'N/A'
        }
        return status
    except Exception as e:
        return {
            'table': table_name,
            'state': 'ERROR',
            'message': str(e)
        }

# Check all online tables
online_tables = [user_online_table_name, item_online_table_name, interaction_online_table_name]
status_list = [check_online_table_status(table) for table in online_tables]

status_df = pd.DataFrame(status_list)
print("\n📊 Online Table Status:")
display(spark.createDataFrame(status_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Trigger Manual Sync (if needed)
# MAGIC
# MAGIC For triggered sync mode, you can manually trigger updates.

# COMMAND ----------

# Example: Trigger manual sync for user features
# Uncomment to run:
# w.online_tables.sync(name=user_online_table_name)
# print(f"✓ Triggered manual sync for {user_online_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 View Feature Lineage
# MAGIC
# MAGIC Unity Catalog tracks complete feature lineage automatically.

# COMMAND ----------

# Get model version details
from mlflow.tracking import MlflowClient

client = MlflowClient()
model_versions = client.search_model_versions(f"name='{model_name}'")

if model_versions:
    latest_version = model_versions[0]
    print(f"Model: {model_name}")
    print(f"Version: {latest_version.version}")
    print(f"Status: {latest_version.status}")
    print(f"\nFeature Tables Used:")
    
    # The model metadata contains feature table references
    for lookup in feature_lookups:
        print(f"  - {lookup.table_name}")
    
    print(f"\nOn-Demand Feature Functions:")
    for func in feature_functions:
        print(f"  - {func.udf_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Cleanup (Optional)
# MAGIC
# MAGIC Run this section if you want to clean up resources created in this demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ Warning
# MAGIC Running the cleanup cells below will:
# MAGIC - Delete online tables (data in Lakebase)
# MAGIC - Drop offline feature tables (data in Delta)
# MAGIC - Remove registered models
# MAGIC - Delete the schema and all objects
# MAGIC
# MAGIC Uncomment and run only if you're sure!

# COMMAND ----------

# Cleanup online tables
# Uncomment to run:
"""
print("Cleaning up online tables...")
for table_name in [user_online_table_name, item_online_table_name, interaction_online_table_name]:
    try:
        w.online_tables.delete(name=table_name)
        print(f"✓ Deleted online table: {table_name}")
    except Exception as e:
        print(f"  Could not delete {table_name}: {e}")
"""

# COMMAND ----------

# Cleanup offline feature tables and schema
# Uncomment to run:
"""
print("Cleaning up offline feature tables and schema...")
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
print(f"✓ Dropped schema: {catalog}.{schema}")
"""

# COMMAND ----------

# Cleanup Lakebase instance
# Uncomment to run:
"""
print("Cleaning up Lakebase instance...")
try:
    w.lakebases.delete(name=lakebase_instance_name)
    print(f"✓ Deleted Lakebase instance: {lakebase_instance_name}")
except Exception as e:
    print(f"  Could not delete Lakebase instance: {e}")
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Summary and Best Practices
# MAGIC
# MAGIC ### What We Accomplished
# MAGIC
# MAGIC ✅ **Feature Engineering**
# MAGIC - Created offline feature tables in Unity Catalog
# MAGIC - Implemented Python UDFs for on-demand feature computation
# MAGIC - Organized features by entity (user, item, interaction)
# MAGIC
# MAGIC ✅ **Online Feature Store**
# MAGIC - Set up Lakebase PostgreSQL as online store
# MAGIC - Published feature tables to online store
# MAGIC - Configured automatic synchronization
# MAGIC
# MAGIC ✅ **Model Training & Deployment**
# MAGIC - Built training sets with automatic feature lookups
# MAGIC - Trained and registered model with feature store metadata
# MAGIC - Demonstrated real-time inference with automatic feature retrieval
# MAGIC
# MAGIC ### Key Benefits Demonstrated
# MAGIC
# MAGIC 1. **Low Latency**: Sub-second feature lookups from PostgreSQL
# MAGIC 2. **Consistency**: Same features for training and serving
# MAGIC 3. **Simplicity**: Only provide keys at inference time
# MAGIC 4. **Governance**: Full lineage tracking via Unity Catalog
# MAGIC 5. **Scalability**: Optimized for high-throughput inference
# MAGIC
# MAGIC ### Production Best Practices
# MAGIC
# MAGIC **🔄 Sync Strategy**
# MAGIC - Use **continuous sync** for frequently updated features
# MAGIC - Use **triggered sync** for expensive or infrequent updates
# MAGIC - Monitor sync lag and set up alerts
# MAGIC
# MAGIC **📊 Feature Design**
# MAGIC - Keep online store features lightweight (select only needed columns)
# MAGIC - Use on-demand features for expensive computations
# MAGIC - Implement proper feature versioning
# MAGIC
# MAGIC **🔐 Security**
# MAGIC - Use Unity Catalog for access control
# MAGIC - Implement row-level security if needed
# MAGIC - Audit feature access patterns
# MAGIC
# MAGIC **💰 Cost Optimization**
# MAGIC - Right-size Lakebase instance based on workload
# MAGIC - Archive old feature versions
# MAGIC - Monitor online storage usage
# MAGIC
# MAGIC **🔍 Monitoring**
# MAGIC - Track feature lookup latency
# MAGIC - Monitor sync lag and failures
# MAGIC - Set up feature quality checks
# MAGIC - Log feature statistics for drift detection
# MAGIC
# MAGIC ### Common Patterns
# MAGIC
# MAGIC **Pattern 1: Real-Time Recommendations**
# MAGIC - User features: demographics, preferences
# MAGIC - Item features: category, popularity
# MAGIC - On-demand: context-specific features
# MAGIC
# MAGIC **Pattern 2: Fraud Detection**
# MAGIC - User features: account history, risk scores
# MAGIC - Transaction features: amount, merchant
# MAGIC - On-demand: velocity calculations, anomaly scores
# MAGIC
# MAGIC **Pattern 3: Personalization**
# MAGIC - User features: behavior, segments
# MAGIC - Content features: metadata, engagement
# MAGIC - On-demand: similarity, recency
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Adapt to Your Use Case**: Replace synthetic data with your domain features
# MAGIC 2. **Implement CI/CD**: Automate feature table updates and model retraining
# MAGIC 3. **Add Monitoring**: Set up dashboards for feature and model metrics
# MAGIC 4. **Scale Out**: Deploy to production with appropriate Lakebase sizing
# MAGIC 5. **Iterate**: Continuously improve features based on model performance
# MAGIC
# MAGIC ### Additional Resources
# MAGIC
# MAGIC - [Databricks Feature Engineering Documentation](https://docs.databricks.com/machine-learning/feature-store/index.html)
# MAGIC - [Online Feature Store Guide](https://docs.databricks.com/machine-learning/feature-store/online-tables.html)
# MAGIC - [Lakebase Documentation](https://docs.databricks.com/lakehouse/lakebase.html)
# MAGIC - [MLflow Feature Store Integration](https://mlflow.org/docs/latest/feature-store.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🎉 Congratulations!
# MAGIC
# MAGIC You've successfully completed the Online Feature Store demo. You now know how to:
# MAGIC - Configure online feature stores with Lakebase
# MAGIC - Publish and sync feature tables
# MAGIC - Create on-demand features with Python UDFs
# MAGIC - Train models with feature store integration
# MAGIC - Deploy models with automatic feature lookup
# MAGIC
# MAGIC **Happy Feature Engineering! 🚀**
