# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Production-Ready Delta Lake Pipeline
# MAGIC
# MAGIC ## Overview
# MAGIC This comprehensive demo brings together all concepts from the previous labs into a complete, production-ready data pipeline. You'll build a medallion architecture (Bronze-Silver-Gold) with data quality checks, error handling, monitoring, and all the optimization techniques you've learned.
# MAGIC
# MAGIC ## What You'll Build
# MAGIC A complete e-commerce analytics pipeline that:
# MAGIC - Ingests raw order data (Bronze layer)
# MAGIC - Cleanses and validates data (Silver layer)
# MAGIC - Creates business metrics (Gold layer)
# MAGIC - Implements comprehensive data quality checks
# MAGIC - Handles errors gracefully
# MAGIC - Monitors pipeline health
# MAGIC - Applies all optimization techniques
# MAGIC
# MAGIC ## Architecture: Medallion Pattern
# MAGIC
# MAGIC ```
# MAGIC Raw Data Sources
# MAGIC       ↓
# MAGIC  BRONZE Layer (Raw ingestion)
# MAGIC   - Exact copy of source
# MAGIC   - No transformations
# MAGIC   - Full history
# MAGIC       ↓
# MAGIC  SILVER Layer (Cleansed & Validated)
# MAGIC   - Data quality checks
# MAGIC   - Standardization
# MAGIC   - Deduplication
# MAGIC   - Enrichment
# MAGIC       ↓
# MAGIC   GOLD Layer (Business Metrics)
# MAGIC   - Aggregations
# MAGIC   - Business KPIs
# MAGIC   - Optimized for BI
# MAGIC ```
# MAGIC
# MAGIC ## Duration
# MAGIC 35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ecommerce_prod;
# MAGIC USE ecommerce_prod;
# MAGIC SELECT current_database() as current_db;

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import json

# Configure for production
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

print("✅ Production environment configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate Simulated Source Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Raw Order Data (would come from source system)

# COMMAND ----------

def generate_raw_orders(num_orders=10000):
    """Generate realistic raw order data with some data quality issues"""
    
    raw_data = (spark.range(0, num_orders)
        .withColumn("order_id", col("id") + 1000000)
        .withColumn("customer_id", (rand() * 5000).cast("int"))
        .withColumn("order_timestamp", 
                   expr("timestamp_add(current_timestamp(), cast(-(rand() * 8640000) as int))"))  # Last 100 days
        .withColumn("order_date", to_date("order_timestamp"))
        
        # Product and amount
        .withColumn("product_id", (rand() * 1000).cast("int"))
        .withColumn("quantity", (rand() * 5 + 1).cast("int"))
        .withColumn("unit_price", (rand() * 200 + 10).cast("decimal(10,2)"))
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        
        # Customer info
        .withColumn("customer_email", concat(lit("customer"), col("customer_id"), lit("@email.com")))
        
        # Geographic info
        .withColumn("country", expr("""
            CASE 
                WHEN rand() < 0.40 THEN 'USA'
                WHEN rand() < 0.70 THEN 'UK'
                WHEN rand() < 0.85 THEN 'Germany'
                WHEN rand() < 0.95 THEN 'France'
                ELSE 'Canada'
            END
        """))
        
        # Order status
        .withColumn("status", expr("""
            CASE 
                WHEN rand() < 0.70 THEN 'completed'
                WHEN rand() < 0.90 THEN 'shipped'
                WHEN rand() < 0.95 THEN 'pending'
                ELSE 'cancelled'
            END
        """))
        
        # Payment method
        .withColumn("payment_method", expr("""
            CASE 
                WHEN rand() < 0.50 THEN 'credit_card'
                WHEN rand() < 0.80 THEN 'debit_card'
                WHEN rand() < 0.95 THEN 'paypal'
                ELSE 'bank_transfer'
            END
        """))
        
        # Data quality issues (intentional for demo)
        .withColumn("total_amount", 
                   when(rand() < 0.02, lit(None))  # 2% missing amounts
                   .otherwise(col("total_amount")))
        .withColumn("customer_email",
                   when(rand() < 0.01, lit("invalid_email"))  # 1% bad emails
                   .otherwise(col("customer_email")))
        
        # Metadata
        .withColumn("source_system", lit("ecommerce_api"))
        .withColumn("ingestion_timestamp", current_timestamp())
        
        .drop("id")
    )
    
    return raw_data

print("✅ Data generator ready")

# COMMAND ----------

# Generate sample data
raw_orders = generate_raw_orders(10000)
print(f"Generated {raw_orders.count():,} raw orders")
display(raw_orders.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bronze Layer - Raw Data Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Strategy
# MAGIC - Ingest data exactly as received
# MAGIC - No transformations or validations
# MAGIC - Preserve full history
# MAGIC - Add metadata for lineage
# MAGIC - Enable CDF for downstream tracking

# COMMAND ----------

def ingest_to_bronze(source_df, bronze_table, partition_col="order_date"):
    """
    Ingest raw data to Bronze layer with minimal transformation
    """
    print(f"📥 Ingesting to Bronze: {bronze_table}")
    
    # Add ingestion metadata
    bronze_df = source_df.withColumn("_ingested_at", current_timestamp()) \
                         .withColumn("_ingested_date", current_date())
    
    # Write to Bronze with optimizations
    (bronze_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy(partition_col)
        .option("mergeSchema", "true")
        .option("optimizeWrite", "true")
        .saveAsTable(bronze_table))
    
    # Enable CDF for tracking
    spark.sql(f"""
        ALTER TABLE {bronze_table}
        SET TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.logRetentionDuration' = '90 days'
        )
    """)
    
    count = spark.table(bronze_table).count()
    print(f"✅ Bronze ingestion complete: {count:,} total rows")
    
    return bronze_df

# COMMAND ----------

# Ingest to Bronze
bronze_orders = ingest_to_bronze(raw_orders, "bronze_orders", "order_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Bronze table
# MAGIC SELECT * FROM bronze_orders 
# MAGIC WHERE order_date = current_date() 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL bronze_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Layer - Data Quality & Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Strategy
# MAGIC - Validate data quality
# MAGIC - Cleanse and standardize
# MAGIC - Remove duplicates
# MAGIC - Enrich with additional data
# MAGIC - Quarantine bad records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Data Quality Rules

# COMMAND ----------

class DataQualityChecker:
    """Data quality validation for orders"""
    
    @staticmethod
    def validate_orders(df):
        """Apply data quality checks and tag results"""
        
        validated_df = df \
            .withColumn("dq_valid_amount", 
                       when(col("total_amount").isNotNull() & (col("total_amount") > 0), True)
                       .otherwise(False)) \
            .withColumn("dq_valid_email",
                       when(col("customer_email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"), True)
                       .otherwise(False)) \
            .withColumn("dq_valid_quantity",
                       when(col("quantity") > 0, True)
                       .otherwise(False)) \
            .withColumn("dq_valid_dates",
                       when(col("order_date") <= current_date(), True)
                       .otherwise(False)) \
            .withColumn("dq_passed_all",
                       col("dq_valid_amount") & 
                       col("dq_valid_email") & 
                       col("dq_valid_quantity") & 
                       col("dq_valid_dates"))
        
        return validated_df
    
    @staticmethod
    def get_quality_metrics(validated_df):
        """Calculate data quality metrics"""
        
        total_records = validated_df.count()
        
        metrics = validated_df.agg(
            count("*").alias("total_records"),
            sum(when(col("dq_passed_all"), 1).otherwise(0)).alias("valid_records"),
            sum(when(~col("dq_passed_all"), 1).otherwise(0)).alias("invalid_records"),
            sum(when(~col("dq_valid_amount"), 1).otherwise(0)).alias("invalid_amount"),
            sum(when(~col("dq_valid_email"), 1).otherwise(0)).alias("invalid_email"),
            sum(when(~col("dq_valid_quantity"), 1).otherwise(0)).alias("invalid_quantity"),
            sum(when(~col("dq_valid_dates"), 1).otherwise(0)).alias("invalid_dates")
        ).collect()[0]
        
        print("📊 Data Quality Metrics:")
        print("=" * 60)
        print(f"Total Records: {metrics['total_records']:,}")
        print(f"Valid Records: {metrics['valid_records']:,} ({metrics['valid_records']/metrics['total_records']*100:.2f}%)")
        print(f"Invalid Records: {metrics['invalid_records']:,} ({metrics['invalid_records']/metrics['total_records']*100:.2f}%)")
        print()
        print("Quality Issues:")
        print(f"  Invalid Amount: {metrics['invalid_amount']:,}")
        print(f"  Invalid Email: {metrics['invalid_email']:,}")
        print(f"  Invalid Quantity: {metrics['invalid_quantity']:,}")
        print(f"  Invalid Dates: {metrics['invalid_dates']:,}")
        print()
        
        return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform Bronze to Silver

# COMMAND ----------

def transform_bronze_to_silver(bronze_table, silver_table, quarantine_table):
    """
    Transform and validate data from Bronze to Silver
    """
    print(f"🔄 Processing Bronze → Silver: {bronze_table} → {silver_table}")
    
    # Read from Bronze
    bronze_df = spark.table(bronze_table)
    
    # Apply data quality checks
    print("📋 Applying data quality checks...")
    validated_df = DataQualityChecker.validate_orders(bronze_df)
    
    # Get quality metrics
    metrics = DataQualityChecker.get_quality_metrics(validated_df)
    
    # Separate valid and invalid records
    valid_df = validated_df.filter(col("dq_passed_all"))
    invalid_df = validated_df.filter(~col("dq_passed_all"))
    
    # Cleanse valid records
    print("🧹 Cleansing valid records...")
    silver_df = (valid_df
        # Standardize text fields
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("payment_method", lower(trim(col("payment_method"))))
        
        # Calculate derived fields
        .withColumn("order_year", year("order_date"))
        .withColumn("order_month", month("order_date"))
        .withColumn("order_quarter", quarter("order_date"))
        
        # Add revenue category
        .withColumn("revenue_category", 
                   when(col("total_amount") < 50, "low")
                   .when(col("total_amount") < 200, "medium")
                   .otherwise("high"))
        
        # Remove DQ columns from final silver
        .drop("dq_valid_amount", "dq_valid_email", "dq_valid_quantity", 
              "dq_valid_dates", "dq_passed_all")
        
        # Add Silver metadata
        .withColumn("_silver_processed_at", current_timestamp())
    )
    
    # Deduplicate (keep latest by order_id)
    print("🔍 Removing duplicates...")
    silver_df = silver_df.dropDuplicates(["order_id"])
    
    # Write valid records to Silver
    print(f"✅ Writing {silver_df.count():,} valid records to Silver...")
    (silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .option("optimizeWrite", "true")
        .saveAsTable(silver_table))
    
    # Quarantine invalid records
    if invalid_df.count() > 0:
        print(f"⚠️ Quarantining {invalid_df.count():,} invalid records...")
        (invalid_df
            .withColumn("_quarantined_at", current_timestamp())
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(quarantine_table))
    
    # Optimize Silver table
    print("🔧 Optimizing Silver table...")
    spark.sql(f"OPTIMIZE {silver_table}")
    
    print(f"✅ Silver transformation complete")
    
    return silver_df

# COMMAND ----------

# Transform to Silver
silver_orders = transform_bronze_to_silver(
    "bronze_orders",
    "silver_orders",
    "quarantine_orders"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Silver data
# MAGIC SELECT * FROM silver_orders 
# MAGIC ORDER BY order_timestamp DESC 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check quarantined records
# MAGIC SELECT 
# MAGIC     COUNT(*) as quarantined_count,
# MAGIC     SUM(CAST(NOT dq_valid_amount AS INT)) as amount_issues,
# MAGIC     SUM(CAST(NOT dq_valid_email AS INT)) as email_issues
# MAGIC FROM quarantine_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Gold Layer - Business Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Strategy
# MAGIC - Create business-friendly aggregations
# MAGIC - Optimize for BI and reporting
# MAGIC - Use MERGE for incremental updates
# MAGIC - Apply Z-ordering for query performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Daily Sales Summary

# COMMAND ----------

def create_daily_sales_summary(silver_table, gold_table):
    """
    Create daily sales summary metrics (Gold layer)
    """
    print(f"📊 Creating Gold metrics: {gold_table}")
    
    # Read from Silver
    silver_df = spark.table(silver_table)
    
    # Calculate daily metrics
    daily_summary = (silver_df
        .filter(col("status").isin("completed", "shipped"))  # Only successful orders
        .groupBy(
            "order_date",
            "country",
            "payment_method"
        )
        .agg(
            count("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            min("total_amount").alias("min_order_value"),
            max("total_amount").alias("max_order_value"),
            sum("quantity").alias("total_items_sold")
        )
        .withColumn("revenue_per_customer", 
                   col("total_revenue") / col("unique_customers"))
        .withColumn("_gold_created_at", current_timestamp())
    )
    
    # MERGE into Gold (upsert pattern)
    from delta.tables import DeltaTable
    
    if spark.catalog.tableExists(gold_table):
        print("🔄 Merging into existing Gold table...")
        gold_delta = DeltaTable.forName(spark, gold_table)
        
        (gold_delta.alias("target")
            .merge(
                daily_summary.alias("source"),
                """target.order_date = source.order_date AND 
                   target.country = source.country AND 
                   target.payment_method = source.payment_method"""
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        print("✨ Creating new Gold table...")
        (daily_summary
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("order_date")
            .saveAsTable(gold_table))
    
    # Optimize with Z-ordering for common queries
    print("🔧 Optimizing Gold table...")
    spark.sql(f"OPTIMIZE {gold_table} ZORDER BY (country, payment_method)")
    
    print(f"✅ Gold metrics created: {spark.table(gold_table).count():,} rows")
    
    return daily_summary

# COMMAND ----------

# Create Gold metrics
gold_daily_summary = create_daily_sales_summary("silver_orders", "gold_daily_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Gold metrics
# MAGIC SELECT 
# MAGIC     order_date,
# MAGIC     country,
# MAGIC     payment_method,
# MAGIC     order_count,
# MAGIC     unique_customers,
# MAGIC     ROUND(total_revenue, 2) as total_revenue,
# MAGIC     ROUND(avg_order_value, 2) as avg_order_value
# MAGIC FROM gold_daily_sales
# MAGIC ORDER BY order_date DESC, total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Customer Lifetime Value (CLV) Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customer metrics Gold table
# MAGIC CREATE OR REPLACE TABLE gold_customer_metrics AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     COUNT(DISTINCT order_id) as lifetime_orders,
# MAGIC     SUM(total_amount) as lifetime_value,
# MAGIC     AVG(total_amount) as avg_order_value,
# MAGIC     MIN(order_date) as first_order_date,
# MAGIC     MAX(order_date) as last_order_date,
# MAGIC     DATEDIFF(MAX(order_date), MIN(order_date)) as customer_lifetime_days,
# MAGIC     COUNT(DISTINCT country) as countries_purchased_from,
# MAGIC     current_timestamp() as _gold_created_at
# MAGIC FROM silver_orders
# MAGIC WHERE status IN ('completed', 'shipped')
# MAGIC GROUP BY customer_id;
# MAGIC
# MAGIC -- Optimize
# MAGIC OPTIMIZE gold_customer_metrics
# MAGIC ZORDER BY (lifetime_value);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View top customers
# MAGIC SELECT * FROM gold_customer_metrics
# MAGIC ORDER BY lifetime_value DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Pipeline Orchestration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete End-to-End Pipeline

# COMMAND ----------

def run_complete_pipeline(incremental=False):
    """
    Execute complete medallion pipeline
    """
    print("=" * 70)
    print("🏭 Starting Production Pipeline")
    print("=" * 70)
    print(f"Mode: {'Incremental' if incremental else 'Full Refresh'}")
    print(f"Started at: {datetime.now()}")
    print()
    
    try:
        # Step 1: Bronze ingestion
        print("STEP 1: Bronze Layer Ingestion")
        print("-" * 70)
        
        if incremental:
            # In production, read from actual source with CDC
            print("📥 Reading incremental data from source...")
            new_data = generate_raw_orders(1000)  # Simulate new data
        else:
            # Full refresh
            print("📥 Reading full dataset from source...")
            new_data = generate_raw_orders(10000)
        
        bronze_result = ingest_to_bronze(new_data, "bronze_orders")
        print()
        
        # Step 2: Silver transformation
        print("STEP 2: Silver Layer Transformation")
        print("-" * 70)
        silver_result = transform_bronze_to_silver(
            "bronze_orders",
            "silver_orders",
            "quarantine_orders"
        )
        print()
        
        # Step 3: Gold aggregations
        print("STEP 3: Gold Layer Aggregation")
        print("-" * 70)
        gold_result = create_daily_sales_summary(
            "silver_orders",
            "gold_daily_sales"
        )
        print()
        
        # Step 4: Customer metrics
        print("STEP 4: Customer Metrics (Gold)")
        print("-" * 70)
        spark.sql("""
            CREATE OR REPLACE TABLE gold_customer_metrics AS
            SELECT 
                customer_id,
                COUNT(DISTINCT order_id) as lifetime_orders,
                SUM(total_amount) as lifetime_value,
                AVG(total_amount) as avg_order_value,
                MIN(order_date) as first_order_date,
                MAX(order_date) as last_order_date,
                current_timestamp() as _gold_created_at
            FROM silver_orders
            WHERE status IN ('completed', 'shipped')
            GROUP BY customer_id
        """)
        print("✅ Customer metrics updated")
        print()
        
        # Step 5: Pipeline summary
        print("=" * 70)
        print("📊 Pipeline Summary")
        print("=" * 70)
        
        bronze_count = spark.table("bronze_orders").count()
        silver_count = spark.table("silver_orders").count()
        gold_daily_count = spark.table("gold_daily_sales").count()
        gold_customer_count = spark.table("gold_customer_metrics").count()
        quarantine_count = spark.table("quarantine_orders").count()
        
        print(f"Bronze Records: {bronze_count:,}")
        print(f"Silver Records: {silver_count:,}")
        print(f"Gold Daily Records: {gold_daily_count:,}")
        print(f"Gold Customer Records: {gold_customer_count:,}")
        print(f"Quarantined Records: {quarantine_count:,}")
        print()
        
        data_quality_pct = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
        print(f"Data Quality: {data_quality_pct:.2f}%")
        print()
        
        print("=" * 70)
        print(f"✅ Pipeline completed successfully!")
        print(f"Finished at: {datetime.now()}")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        print()
        print("=" * 70)
        print(f"❌ Pipeline failed: {e}")
        print("=" * 70)
        raise

# COMMAND ----------

# Run complete pipeline
run_complete_pipeline(incremental=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Monitoring and Alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Health Monitor

# COMMAND ----------

def monitor_pipeline_health():
    """
    Monitor pipeline health and data quality
    """
    print("🏥 Pipeline Health Monitor")
    print("=" * 70)
    
    # Check table counts
    tables = {
        "Bronze": "bronze_orders",
        "Silver": "silver_orders",
        "Gold Daily": "gold_daily_sales",
        "Gold Customers": "gold_customer_metrics",
        "Quarantine": "quarantine_orders"
    }
    
    print("\n📊 Table Statistics:")
    print("-" * 70)
    print(f"{'Layer':<20} {'Records':<15} {'Size (MB)':<15} {'Files':<10}")
    print("-" * 70)
    
    for layer_name, table_name in tables.items():
        if spark.catalog.tableExists(table_name):
            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            count = spark.table(table_name).count()
            size_mb = detail["sizeInBytes"] / (1024 * 1024)
            files = detail["numFiles"]
            
            print(f"{layer_name:<20} {count:<15,} {size_mb:<15.2f} {files:<10}")
    
    # Data quality metrics
    bronze_count = spark.table("bronze_orders").count()
    silver_count = spark.table("silver_orders").count()
    quarantine_count = spark.table("quarantine_orders").count()
    
    print()
    print("📋 Data Quality Metrics:")
    print("-" * 70)
    
    quality_pct = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
    quarantine_pct = (quarantine_count / bronze_count * 100) if bronze_count > 0 else 0
    
    print(f"Overall Data Quality: {quality_pct:.2f}%")
    print(f"Quarantine Rate: {quarantine_pct:.2f}%")
    
    # Alert if quality is low
    if quality_pct < 95:
        print()
        print("⚠️ ALERT: Data quality below 95% threshold!")
        print(f"   Investigate quarantine_orders table")
    
    if quarantine_pct > 5:
        print()
        print("⚠️ ALERT: High quarantine rate (>5%)!")
        print(f"   Check data source quality")
    
    # Latest refresh time
    print()
    print("🕐 Last Refresh Times:")
    print("-" * 70)
    
    for layer_name, table_name in tables.items():
        if spark.catalog.tableExists(table_name):
            history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
            last_update = history["timestamp"]
            operation = history["operation"]
            
            print(f"{layer_name:<20} {last_update} ({operation})")
    
    print()
    print("=" * 70)
    print("✅ Health check complete")
    print()

# COMMAND ----------

# Run health monitor
monitor_pipeline_health()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Business Analytics (Gold Layer Queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily revenue by country
# MAGIC SELECT 
# MAGIC     order_date,
# MAGIC     country,
# MAGIC     SUM(total_revenue) as revenue,
# MAGIC     SUM(order_count) as orders,
# MAGIC     ROUND(AVG(avg_order_value), 2) as avg_order_value
# MAGIC FROM gold_daily_sales
# MAGIC GROUP BY order_date, country
# MAGIC ORDER BY order_date DESC, revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top customers by lifetime value
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     lifetime_orders,
# MAGIC     ROUND(lifetime_value, 2) as lifetime_value,
# MAGIC     ROUND(avg_order_value, 2) as avg_order_value,
# MAGIC     first_order_date,
# MAGIC     last_order_date,
# MAGIC     customer_lifetime_days
# MAGIC FROM gold_customer_metrics
# MAGIC WHERE lifetime_orders >= 3
# MAGIC ORDER BY lifetime_value DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment Method Analysis
# MAGIC
# MAGIC %sql
# MAGIC -- Revenue by payment method
# MAGIC SELECT 
# MAGIC     payment_method,
# MAGIC     SUM(total_revenue) as total_revenue,
# MAGIC     SUM(order_count) as total_orders,
# MAGIC     ROUND(AVG(avg_order_value), 2) as avg_order_value
# MAGIC FROM gold_daily_sales
# MAGIC GROUP BY payment_method
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways: Production Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### Architecture Patterns
# MAGIC
# MAGIC ✅ **Medallion Architecture (Bronze-Silver-Gold)**
# MAGIC - Bronze: Raw data, no transformations
# MAGIC - Silver: Cleansed, validated, enriched
# MAGIC - Gold: Business aggregations, optimized for BI
# MAGIC
# MAGIC ✅ **Data Quality**
# MAGIC - Validate at Silver layer
# MAGIC - Quarantine bad records
# MAGIC - Track quality metrics
# MAGIC - Alert on degradation
# MAGIC
# MAGIC ✅ **Optimization Techniques Applied**
# MAGIC - Partitioning by date
# MAGIC - Z-ordering for frequent filters
# MAGIC - OPTIMIZE for file compaction
# MAGIC - AUTO OPTIMIZE for writes
# MAGIC - Appropriate shuffle partitions
# MAGIC
# MAGIC ✅ **Error Handling**
# MAGIC - Graceful failure handling
# MAGIC - Quarantine table for bad data
# MAGIC - Comprehensive logging
# MAGIC - Alerting on issues
# MAGIC
# MAGIC ✅ **Monitoring**
# MAGIC - Table health metrics
# MAGIC - Data quality tracking
# MAGIC - Pipeline execution monitoring
# MAGIC - Storage and performance metrics
# MAGIC
# MAGIC ✅ **Incremental Processing**
# MAGIC - MERGE for upserts in Gold
# MAGIC - Change Data Feed enabled
# MAGIC - Efficient incremental updates
# MAGIC - Metadata tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to cleanup
# spark.sql("DROP DATABASE IF EXISTS ecommerce_prod CASCADE")
# print("✅ Production database dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the entire Delta Lake workshop and built a production-ready data pipeline!
# MAGIC
# MAGIC ### What You've Learned:
# MAGIC
# MAGIC 1. **Module 1**: Delta Lake architecture and fundamentals
# MAGIC 2. **Module 2**: Creating your first Delta tables
# MAGIC 3. **Module 3**: CRUD operations and ACID transactions
# MAGIC 4. **Module 4**: Partitioning strategies
# MAGIC 5. **Module 5**: Working with large datasets
# MAGIC 6. **Module 6**: Advanced optimization techniques
# MAGIC 7. **Module 7**: Time travel and data recovery
# MAGIC 8. **Module 8**: Table maintenance
# MAGIC 9. **Module 9**: Schema evolution and Change Data Feed
# MAGIC 10. **Module 10**: Production-ready pipeline (this demo!)
# MAGIC
# MAGIC ### You Can Now:
# MAGIC - Build production Delta Lake pipelines
# MAGIC - Implement medallion architecture
# MAGIC - Optimize for performance and cost
# MAGIC - Handle data quality issues
# MAGIC - Monitor and maintain Delta tables
# MAGIC - Implement CDC patterns
# MAGIC - Recover from data issues
# MAGIC
# MAGIC ### Next Steps:
# MAGIC - Apply these patterns to your real-world projects
# MAGIC - Explore Delta Live Tables for declarative ETL
# MAGIC - Learn about Unity Catalog integration
# MAGIC - Implement ML pipelines with Delta
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Resources:**
# MAGIC - [Delta Lake Documentation](https://docs.delta.io/)
# MAGIC - [Databricks Best Practices](https://docs.databricks.com/delta/best-practices.html)
# MAGIC - [Delta Lake GitHub](https://github.com/delta-io/delta)
# MAGIC
# MAGIC **Thank you for completing this workshop!** 🚀
