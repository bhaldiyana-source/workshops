# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2: Ingest Salesforce Data
# MAGIC
# MAGIC ## Lab Overview
# MAGIC In this hands-on lab, you will configure Lakeflow Connect ingestion pipelines to bring Salesforce data into Unity Catalog Delta tables. You'll learn to discover objects, configure incremental refresh, handle relationships, and use the Bulk API for large datasets.
# MAGIC
# MAGIC **Duration:** 35 minutes
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Discover standard and custom Salesforce objects
# MAGIC - Configure object ingestion pipelines with field selection
# MAGIC - Implement incremental refresh using SystemModstamp watermarking
# MAGIC - Handle Salesforce relationships (Lookup and Master-Detail)
# MAGIC - Use Bulk API for large object extraction (>10K records)
# MAGIC - Monitor API usage and track ingestion job status
# MAGIC - Validate data landed successfully in Unity Catalog Delta tables
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Completed Lab 1: Configure Salesforce Connection
# MAGIC - Valid Salesforce OAuth access token
# MAGIC - Unity Catalog enabled in your workspace
# MAGIC - CREATE TABLE and CREATE SCHEMA permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Setup and Configuration
# MAGIC
# MAGIC ### Step 1.1: Define Target Catalog and Schema

# COMMAND ----------

# Configuration parameters
catalog_name = "main"  # Replace with your catalog name
schema_name = "salesforce_raw"  # Target schema for Salesforce data
checkpoint_location = "/tmp/salesforce_checkpoints"  # Checkpoint location for incremental loads

# Display configuration
print(f"📊 Ingestion Configuration:")
print(f"   Target Catalog: {catalog_name}")
print(f"   Target Schema: {schema_name}")
print(f"   Full Path: {catalog_name}.{schema_name}")
print(f"   Checkpoint Location: {checkpoint_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Create Target Schema

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
print(f"✅ Schema {catalog_name}.{schema_name} is ready")

# Set as default schema for this session
spark.sql(f"USE {catalog_name}.{schema_name}")
print(f"📍 Using schema: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3: Retrieve Salesforce Connection Details

# COMMAND ----------

import requests
import json
from datetime import datetime

# Retrieve credentials from secrets (set in Lab 1)
consumer_key = dbutils.secrets.get(scope="salesforce_credentials", key="consumer_key")
consumer_secret = dbutils.secrets.get(scope="salesforce_credentials", key="consumer_secret")
username = dbutils.secrets.get(scope="salesforce_credentials", key="username")
password_token = dbutils.secrets.get(scope="salesforce_credentials", key="password_token")
instance_url = dbutils.secrets.get(scope="salesforce_credentials", key="instance_url")

# Obtain fresh access token
token_url = "https://login.salesforce.com/services/oauth2/token"
auth_params = {
    "grant_type": "password",
    "client_id": consumer_key,
    "client_secret": consumer_secret,
    "username": username,
    "password": password_token
}

response = requests.post(token_url, data=auth_params)
response.raise_for_status()
token_data = response.json()
access_token = token_data["access_token"]
instance_url = token_data["instance_url"]

print("✅ Salesforce authentication successful")
print(f"   Instance: {instance_url}")

# Store in Spark config
spark.conf.set("salesforce.access.token", access_token)
spark.conf.set("salesforce.instance.url", instance_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Discover Salesforce Objects and Fields
# MAGIC
# MAGIC ### Understanding Salesforce Object Types
# MAGIC
# MAGIC - **Standard Objects**: Built-in objects like Account, Contact, Opportunity, Lead
# MAGIC - **Custom Objects**: Organization-specific objects ending with `__c` (e.g., `CustomProduct__c`)
# MAGIC - **Relationship Fields**: Lookup and Master-Detail relationships ending with `__r`
# MAGIC - **System Fields**: Id, CreatedDate, LastModifiedDate, SystemModstamp (critical for CDC)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.1: List Common Standard Objects

# COMMAND ----------

def describe_salesforce_object(object_name, access_token, instance_url):
    """
    Get detailed field information for a Salesforce object
    
    Returns: DataFrame with field name, type, label, and properties
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        describe_url = f"{instance_url}/services/data/v59.0/sobjects/{object_name}/describe"
        response = requests.get(describe_url, headers=headers)
        response.raise_for_status()
        
        metadata = response.json()
        
        # Extract field information
        fields_data = []
        for field in metadata["fields"]:
            fields_data.append({
                "field_name": field["name"],
                "field_type": field["type"],
                "label": field["label"],
                "required": not field["nillable"],
                "updateable": field["updateable"],
                "relationship": field.get("relationshipName", "")
            })
        
        df = spark.createDataFrame(fields_data)
        return df, metadata
        
    except Exception as e:
        print(f"❌ Error describing object {object_name}: {e}")
        return None, None

# Describe Account object
access_token = spark.conf.get("salesforce.access.token")
instance_url = spark.conf.get("salesforce.instance.url")

print("📋 Describing Account object...")
account_fields_df, account_metadata = describe_salesforce_object("Account", access_token, instance_url)

if account_fields_df:
    print(f"✅ Found {account_fields_df.count()} fields in Account object")
    print("\nKey fields:")
    display(account_fields_df.filter(
        account_fields_df.field_name.isin(["Id", "Name", "Industry", "AnnualRevenue", 
                                            "NumberOfEmployees", "Type", "CreatedDate", 
                                            "LastModifiedDate", "SystemModstamp"])
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Ingest Standard Objects
# MAGIC
# MAGIC ### Ingestion Strategy
# MAGIC
# MAGIC We'll use different approaches based on data volume:
# MAGIC - **REST API**: For small objects (<2,000 records) or exploratory queries
# MAGIC - **Bulk API**: For large objects (>2,000 records) or production ingestion
# MAGIC
# MAGIC ### Step 3.1: Initial Full Load - Account Object

# COMMAND ----------

def query_salesforce_rest(soql_query, access_token, instance_url):
    """
    Execute SOQL query using Salesforce REST API
    Returns data as list of records
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    all_records = []
    query_url = f"{instance_url}/services/data/v59.0/query"
    
    try:
        # Initial query
        response = requests.get(query_url, headers=headers, params={"q": soql_query})
        response.raise_for_status()
        result = response.json()
        
        all_records.extend(result["records"])
        
        # Handle pagination with nextRecordsUrl
        while not result["done"]:
            next_url = f"{instance_url}{result['nextRecordsUrl']}"
            response = requests.get(next_url, headers=headers)
            response.raise_for_status()
            result = response.json()
            all_records.extend(result["records"])
        
        # Remove Salesforce metadata attributes
        cleaned_records = []
        for record in all_records:
            cleaned_record = {k: v for k, v in record.items() if k != "attributes"}
            cleaned_records.append(cleaned_record)
        
        return cleaned_records
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return []

# Query Account data
access_token = spark.conf.get("salesforce.access.token")
instance_url = spark.conf.get("salesforce.instance.url")

# SOQL query for Account fields
soql = """
    SELECT Id, Name, Type, Industry, AnnualRevenue, NumberOfEmployees, 
           BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry,
           Phone, Website, OwnerId, CreatedDate, CreatedById, 
           LastModifiedDate, LastModifiedById, SystemModstamp, IsDeleted
    FROM Account
    WHERE IsDeleted = false
    ORDER BY SystemModstamp ASC
"""

print("🔍 Querying Account object...")
account_records = query_salesforce_rest(soql, access_token, instance_url)
print(f"✅ Retrieved {len(account_records)} Account records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Write Account Data to Delta Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, BooleanType
from pyspark.sql.functions import col, to_timestamp

# Convert to DataFrame
if account_records:
    account_df = spark.createDataFrame(account_records)
    
    # Convert timestamp strings to timestamp type
    timestamp_cols = ["CreatedDate", "LastModifiedDate", "SystemModstamp"]
    for ts_col in timestamp_cols:
        if ts_col in account_df.columns:
            account_df = account_df.withColumn(ts_col, to_timestamp(col(ts_col)))
    
    # Add ingestion metadata
    account_df = account_df.withColumn("_ingestion_timestamp", to_timestamp(lit(datetime.now())))
    
    # Write to Delta table
    target_table = "sf_accounts"
    account_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    print(f"✅ Account data written to {catalog_name}.{schema_name}.{target_table}")
    print(f"   Records: {account_df.count()}")
    print(f"   Columns: {len(account_df.columns)}")
    
    # Display sample
    display(spark.table(target_table).limit(5))
else:
    print("⚠️ No Account records to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.3: Ingest Contact Object

# COMMAND ----------

# Query Contact data with relationship to Account
soql_contact = """
    SELECT Id, FirstName, LastName, Email, Phone, MobilePhone, Title, Department,
           AccountId, Account.Name, OwnerId, 
           MailingStreet, MailingCity, MailingState, MailingPostalCode, MailingCountry,
           CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, 
           SystemModstamp, IsDeleted
    FROM Contact
    WHERE IsDeleted = false
    ORDER BY SystemModstamp ASC
"""

print("🔍 Querying Contact object...")
contact_records = query_salesforce_rest(soql_contact, access_token, instance_url)
print(f"✅ Retrieved {len(contact_records)} Contact records")

# Flatten nested Account relationship
if contact_records:
    flattened_contacts = []
    for record in contact_records:
        flat_record = {k: v for k, v in record.items() if k != "Account"}
        if "Account" in record and record["Account"]:
            flat_record["Account_Name"] = record["Account"].get("Name")
        flattened_contacts.append(flat_record)
    
    contact_df = spark.createDataFrame(flattened_contacts)
    
    # Convert timestamps
    for ts_col in ["CreatedDate", "LastModifiedDate", "SystemModstamp"]:
        if ts_col in contact_df.columns:
            contact_df = contact_df.withColumn(ts_col, to_timestamp(col(ts_col)))
    
    contact_df = contact_df.withColumn("_ingestion_timestamp", to_timestamp(lit(datetime.now())))
    
    # Write to Delta
    contact_df.write.format("delta").mode("overwrite").saveAsTable("sf_contacts")
    print(f"✅ Contact data written to {catalog_name}.{schema_name}.sf_contacts")
    display(spark.table("sf_contacts").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.4: Ingest Opportunity Object

# COMMAND ----------

# Query Opportunity data
soql_opportunity = """
    SELECT Id, Name, AccountId, Account.Name, StageName, Amount, Probability,
           CloseDate, Type, LeadSource, IsClosed, IsWon, ForecastCategory,
           OwnerId, CreatedDate, CreatedById, 
           LastModifiedDate, LastModifiedById, SystemModstamp, IsDeleted
    FROM Opportunity
    WHERE IsDeleted = false
    ORDER BY SystemModstamp ASC
"""

print("🔍 Querying Opportunity object...")
opportunity_records = query_salesforce_rest(soql_opportunity, access_token, instance_url)
print(f"✅ Retrieved {len(opportunity_records)} Opportunity records")

if opportunity_records:
    # Flatten Account relationship
    flattened_opps = []
    for record in opportunity_records:
        flat_record = {k: v for k, v in record.items() if k != "Account"}
        if "Account" in record and record["Account"]:
            flat_record["Account_Name"] = record["Account"].get("Name")
        flattened_opps.append(flat_record)
    
    opportunity_df = spark.createDataFrame(flattened_opps)
    
    # Convert timestamps and dates
    for ts_col in ["CreatedDate", "LastModifiedDate", "SystemModstamp", "CloseDate"]:
        if ts_col in opportunity_df.columns:
            opportunity_df = opportunity_df.withColumn(ts_col, to_timestamp(col(ts_col)))
    
    opportunity_df = opportunity_df.withColumn("_ingestion_timestamp", to_timestamp(lit(datetime.now())))
    
    # Write to Delta
    opportunity_df.write.format("delta").mode("overwrite").saveAsTable("sf_opportunities")
    print(f"✅ Opportunity data written to {catalog_name}.{schema_name}.sf_opportunities")
    display(spark.table("sf_opportunities").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.5: Ingest Lead Object

# COMMAND ----------

soql_lead = """
    SELECT Id, FirstName, LastName, Email, Phone, Company, Title, Industry,
           Status, Rating, LeadSource, IsConverted, ConvertedDate,
           ConvertedAccountId, ConvertedContactId, ConvertedOpportunityId,
           Street, City, State, PostalCode, Country,
           OwnerId, CreatedDate, LastModifiedDate, SystemModstamp, IsDeleted
    FROM Lead
    WHERE IsDeleted = false
    ORDER BY SystemModstamp ASC
"""

print("🔍 Querying Lead object...")
lead_records = query_salesforce_rest(soql_lead, access_token, instance_url)
print(f"✅ Retrieved {len(lead_records)} Lead records")

if lead_records:
    lead_df = spark.createDataFrame(lead_records)
    
    for ts_col in ["CreatedDate", "LastModifiedDate", "SystemModstamp", "ConvertedDate"]:
        if ts_col in lead_df.columns:
            lead_df = lead_df.withColumn(ts_col, to_timestamp(col(ts_col)))
    
    lead_df = lead_df.withColumn("_ingestion_timestamp", to_timestamp(lit(datetime.now())))
    lead_df.write.format("delta").mode("overwrite").saveAsTable("sf_leads")
    print(f"✅ Lead data written to {catalog_name}.{schema_name}.sf_leads")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.6: Ingest Case Object

# COMMAND ----------

soql_case = """
    SELECT Id, CaseNumber, AccountId, ContactId, Subject, Status, Priority,
           Origin, Type, Reason, IsClosed, ClosedDate,
           OwnerId, CreatedDate, LastModifiedDate, SystemModstamp, IsDeleted
    FROM Case
    WHERE IsDeleted = false
    ORDER BY SystemModstamp ASC
"""

print("🔍 Querying Case object...")
case_records = query_salesforce_rest(soql_case, access_token, instance_url)
print(f"✅ Retrieved {len(case_records)} Case records")

if case_records:
    case_df = spark.createDataFrame(case_records)
    
    for ts_col in ["CreatedDate", "LastModifiedDate", "SystemModstamp", "ClosedDate"]:
        if ts_col in case_df.columns:
            case_df = case_df.withColumn(ts_col, to_timestamp(col(ts_col)))
    
    case_df = case_df.withColumn("_ingestion_timestamp", to_timestamp(lit(datetime.now())))
    case_df.write.format("delta").mode("overwrite").saveAsTable("sf_cases")
    print(f"✅ Case data written to {catalog_name}.{schema_name}.sf_cases")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Implement Incremental Refresh
# MAGIC
# MAGIC ### Understanding SystemModstamp
# MAGIC
# MAGIC `SystemModstamp` is a system-maintained timestamp that tracks the last modification of any record field:
# MAGIC - Updated automatically on INSERT, UPDATE, DELETE
# MAGIC - Indexed for efficient queries
# MAGIC - Ideal for incremental refresh watermarking
# MAGIC - Use `ORDER BY SystemModstamp ASC` for consistent ordering
# MAGIC
# MAGIC ### Incremental Refresh Strategy
# MAGIC
# MAGIC ```
# MAGIC 1. Track last successful SystemModstamp in checkpoint table
# MAGIC 2. Query: WHERE SystemModstamp > last_checkpoint
# MAGIC 3. Merge new/updated records into target Delta table
# MAGIC 4. Update checkpoint with max(SystemModstamp) from batch
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.1: Create Checkpoint Table

# COMMAND ----------

# Create checkpoint tracking table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.sf_ingestion_checkpoints (
        object_name STRING,
        last_systemmodstamp TIMESTAMP,
        last_ingestion_time TIMESTAMP,
        records_ingested LONG,
        PRIMARY KEY (object_name)
    ) USING DELTA
""")

print(f"✅ Checkpoint table created: {catalog_name}.{schema_name}.sf_ingestion_checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Function for Incremental Ingestion

# COMMAND ----------

from pyspark.sql.functions import lit, max as spark_max, current_timestamp

def incremental_ingest_object(
    object_name, 
    soql_select_fields, 
    access_token, 
    instance_url, 
    target_table_name,
    merge_keys=["Id"]
):
    """
    Perform incremental ingestion of a Salesforce object using SystemModstamp watermarking
    
    Args:
        object_name: Salesforce object API name (e.g., "Account")
        soql_select_fields: Comma-separated list of fields to select
        access_token: OAuth access token
        instance_url: Salesforce instance URL
        target_table_name: Target Delta table name (without catalog.schema prefix)
        merge_keys: List of fields to use for merge (default: ["Id"])
    """
    
    # Get last checkpoint
    checkpoint_df = spark.table(f"{catalog_name}.{schema_name}.sf_ingestion_checkpoints")
    last_checkpoint = checkpoint_df.filter(col("object_name") == object_name)
    
    if last_checkpoint.count() > 0:
        last_systemmodstamp = last_checkpoint.select("last_systemmodstamp").first()[0]
        print(f"📍 Last checkpoint for {object_name}: {last_systemmodstamp}")
        
        # Query only records modified since checkpoint
        soql = f"""
            SELECT {soql_select_fields}
            FROM {object_name}
            WHERE SystemModstamp > {last_systemmodstamp.isoformat()}
            AND IsDeleted = false
            ORDER BY SystemModstamp ASC
        """
    else:
        print(f"📍 No checkpoint found for {object_name} - performing initial load")
        soql = f"""
            SELECT {soql_select_fields}
            FROM {object_name}
            WHERE IsDeleted = false
            ORDER BY SystemModstamp ASC
        """
    
    # Query Salesforce
    print(f"🔍 Querying {object_name} for incremental changes...")
    records = query_salesforce_rest(soql, access_token, instance_url)
    
    if not records:
        print(f"✅ No new records for {object_name}")
        return 0
    
    print(f"📥 Retrieved {len(records)} new/updated records")
    
    # Create DataFrame
    new_df = spark.createDataFrame(records)
    
    # Convert timestamps
    timestamp_cols = ["CreatedDate", "LastModifiedDate", "SystemModstamp"]
    for ts_col in timestamp_cols:
        if ts_col in new_df.columns:
            new_df = new_df.withColumn(ts_col, to_timestamp(col(ts_col)))
    
    new_df = new_df.withColumn("_ingestion_timestamp", current_timestamp())
    
    # Merge into target table
    full_target_table = f"{catalog_name}.{schema_name}.{target_table_name}"
    
    # Check if target table exists
    if spark.catalog.tableExists(full_target_table):
        # Use MERGE for upsert
        from delta.tables import DeltaTable
        
        target_table = DeltaTable.forName(spark, full_target_table)
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Perform merge
        target_table.alias("target").merge(
            new_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        print(f"✅ Merged {len(records)} records into {full_target_table}")
    else:
        # Initial load - write as new table
        new_df.write.format("delta").mode("overwrite").saveAsTable(full_target_table)
        print(f"✅ Created new table {full_target_table} with {len(records)} records")
    
    # Update checkpoint
    max_systemmodstamp = new_df.select(spark_max("SystemModstamp")).first()[0]
    
    checkpoint_update = spark.createDataFrame([{
        "object_name": object_name,
        "last_systemmodstamp": max_systemmodstamp,
        "last_ingestion_time": datetime.now(),
        "records_ingested": len(records)
    }])
    
    # Merge checkpoint
    if spark.catalog.tableExists(f"{catalog_name}.{schema_name}.sf_ingestion_checkpoints"):
        checkpoint_table = DeltaTable.forName(spark, f"{catalog_name}.{schema_name}.sf_ingestion_checkpoints")
        checkpoint_table.alias("target").merge(
            checkpoint_update.alias("source"),
            "target.object_name = source.object_name"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    print(f"✅ Checkpoint updated: {max_systemmodstamp}")
    
    return len(records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.3: Run Incremental Ingestion for Account

# COMMAND ----------

access_token = spark.conf.get("salesforce.access.token")
instance_url = spark.conf.get("salesforce.instance.url")

account_fields = """
    Id, Name, Type, Industry, AnnualRevenue, NumberOfEmployees,
    BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry,
    Phone, Website, OwnerId, CreatedDate, CreatedById,
    LastModifiedDate, LastModifiedById, SystemModstamp, IsDeleted
"""

records_ingested = incremental_ingest_object(
    object_name="Account",
    soql_select_fields=account_fields,
    access_token=access_token,
    instance_url=instance_url,
    target_table_name="sf_accounts",
    merge_keys=["Id"]
)

print(f"\n📊 Ingestion Summary:")
print(f"   Object: Account")
print(f"   Records Ingested: {records_ingested}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.4: Run Incremental Ingestion for All Objects

# COMMAND ----------

# Define objects to ingest
objects_config = [
    {
        "name": "Account",
        "fields": "Id, Name, Type, Industry, AnnualRevenue, NumberOfEmployees, BillingCity, BillingState, Phone, Website, OwnerId, CreatedDate, LastModifiedDate, SystemModstamp, IsDeleted",
        "target_table": "sf_accounts"
    },
    {
        "name": "Contact",
        "fields": "Id, FirstName, LastName, Email, Phone, Title, AccountId, OwnerId, CreatedDate, LastModifiedDate, SystemModstamp, IsDeleted",
        "target_table": "sf_contacts"
    },
    {
        "name": "Opportunity",
        "fields": "Id, Name, AccountId, StageName, Amount, Probability, CloseDate, Type, IsClosed, IsWon, OwnerId, CreatedDate, LastModifiedDate, SystemModstamp, IsDeleted",
        "target_table": "sf_opportunities"
    },
    {
        "name": "Lead",
        "fields": "Id, FirstName, LastName, Email, Company, Title, Industry, Status, Rating, LeadSource, OwnerId, CreatedDate, LastModifiedDate, SystemModstamp, IsDeleted",
        "target_table": "sf_leads"
    },
    {
        "name": "Case",
        "fields": "Id, CaseNumber, AccountId, ContactId, Subject, Status, Priority, Origin, IsClosed, OwnerId, CreatedDate, LastModifiedDate, SystemModstamp, IsDeleted",
        "target_table": "sf_cases"
    }
]

# Run incremental ingestion for all objects
print("🚀 Starting incremental ingestion for all objects...\n")

ingestion_summary = []

for obj_config in objects_config:
    print(f"{'='*60}")
    print(f"Processing: {obj_config['name']}")
    print(f"{'='*60}")
    
    try:
        records = incremental_ingest_object(
            object_name=obj_config["name"],
            soql_select_fields=obj_config["fields"],
            access_token=access_token,
            instance_url=instance_url,
            target_table_name=obj_config["target_table"],
            merge_keys=["Id"]
        )
        
        ingestion_summary.append({
            "object_name": obj_config["name"],
            "target_table": obj_config["target_table"],
            "records_ingested": records,
            "status": "Success"
        })
        
    except Exception as e:
        print(f"❌ Error ingesting {obj_config['name']}: {e}")
        ingestion_summary.append({
            "object_name": obj_config["name"],
            "target_table": obj_config["target_table"],
            "records_ingested": 0,
            "status": f"Failed: {str(e)}"
        })
    
    print()

# Display summary
print(f"\n{'='*60}")
print("📊 INGESTION SUMMARY")
print(f"{'='*60}")

summary_df = spark.createDataFrame(ingestion_summary)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Validate Ingested Data
# MAGIC
# MAGIC ### Step 5.1: Check Table Counts

# COMMAND ----------

# Query all tables and show record counts
tables_to_check = ["sf_accounts", "sf_contacts", "sf_opportunities", "sf_leads", "sf_cases"]

print("📊 Table Record Counts:\n")

table_stats = []
for table_name in tables_to_check:
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    if spark.catalog.tableExists(full_table_name):
        count = spark.table(full_table_name).count()
        table_stats.append({"table_name": table_name, "record_count": count})
        print(f"   {table_name}: {count:,} records")
    else:
        print(f"   {table_name}: Table does not exist")

# Display as chart
if table_stats:
    stats_df = spark.createDataFrame(table_stats)
    display(stats_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.2: Verify Data Quality

# COMMAND ----------

# Check for null IDs (should be zero)
print("🔍 Data Quality Checks:\n")

for table_name in tables_to_check:
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    if spark.catalog.tableExists(full_table_name):
        df = spark.table(full_table_name)
        
        null_ids = df.filter(col("Id").isNull()).count()
        total_records = df.count()
        
        if null_ids > 0:
            print(f"❌ {table_name}: {null_ids} records with null Id")
        else:
            print(f"✅ {table_name}: All {total_records:,} records have valid Id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.3: Check Checkpoint Status

# COMMAND ----------

print("📍 Checkpoint Status:\n")

checkpoint_df = spark.table(f"{catalog_name}.{schema_name}.sf_ingestion_checkpoints")
display(checkpoint_df.orderBy("last_ingestion_time", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Monitor API Usage

# COMMAND ----------

def check_api_usage(access_token, instance_url):
    """Check current Salesforce API usage"""
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        limits_url = f"{instance_url}/services/data/v59.0/limits"
        response = requests.get(limits_url, headers=headers)
        response.raise_for_status()
        
        limits = response.json()
        daily_api = limits.get("DailyApiRequests", {})
        
        max_calls = daily_api.get("Max", 0)
        remaining = daily_api.get("Remaining", 0)
        used = max_calls - remaining
        usage_pct = (used / max_calls * 100) if max_calls > 0 else 0
        
        print(f"📊 API Usage:")
        print(f"   Daily Limit: {max_calls:,}")
        print(f"   Used: {used:,}")
        print(f"   Remaining: {remaining:,}")
        print(f"   Usage: {usage_pct:.1f}%")
        
        if usage_pct > 90:
            print(f"   🔴 Critical: Usage above 90%")
        elif usage_pct > 80:
            print(f"   ⚠️ Warning: Usage above 80%")
        else:
            print(f"   ✅ Usage healthy")
            
    except Exception as e:
        print(f"❌ Error checking API usage: {e}")

# Check current API usage
access_token = spark.conf.get("salesforce.access.token")
instance_url = spark.conf.get("salesforce.instance.url")
check_api_usage(access_token, instance_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC
# MAGIC ### What You Accomplished ✅
# MAGIC
# MAGIC In this lab, you:
# MAGIC 1. Created Unity Catalog schema for Salesforce data
# MAGIC 2. Discovered and described Salesforce objects and fields
# MAGIC 3. Ingested 5 standard objects (Account, Contact, Opportunity, Lead, Case) into Delta tables
# MAGIC 4. Implemented incremental refresh using SystemModstamp watermarking
# MAGIC 5. Created checkpoint tracking for reliable incremental loads
# MAGIC 6. Validated data quality and completeness
# MAGIC 7. Monitored API usage to avoid limits
# MAGIC
# MAGIC ### Key Takeaways 💡
# MAGIC
# MAGIC - **SystemModstamp**: Critical field for incremental CDC patterns
# MAGIC - **Merge Strategy**: Use Delta MERGE for upsert operations
# MAGIC - **Checkpointing**: Track last successful load for reliability
# MAGIC - **API Management**: Monitor usage to stay within limits
# MAGIC - **Data Quality**: Always validate after ingestion
# MAGIC
# MAGIC ### Best Practices Implemented
# MAGIC
# MAGIC - ✅ Used REST API for initial loads and incremental updates
# MAGIC - ✅ Implemented idempotent merge logic with Delta tables
# MAGIC - ✅ Tracked ingestion metadata with timestamps
# MAGIC - ✅ Validated data quality post-ingestion
# MAGIC - ✅ Monitored API consumption
# MAGIC
# MAGIC ### Next Steps 🚀
# MAGIC
# MAGIC Proceed to **Lab 3: Analyze and Transform Salesforce Data** where you will:
# MAGIC - Query Salesforce data with SQL analytics
# MAGIC - Join multiple objects for Customer 360 views
# MAGIC - Build sales pipeline dashboards
# MAGIC - Create Delta Live Tables for transformations
# MAGIC - Apply ML models for lead scoring and churn prediction
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Need Help?** Refer to:
# MAGIC - `resources/troubleshooting-guide.md` for common issues
# MAGIC - `resources/salesforce-api-limits.md` for API optimization
