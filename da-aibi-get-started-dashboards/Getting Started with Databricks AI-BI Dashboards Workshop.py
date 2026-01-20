# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started with Databricks AI/BI Dashboards Workshop
# MAGIC
# MAGIC ## Workshop Overview
# MAGIC
# MAGIC Welcome to the Databricks AI/BI Dashboards workshop! In this hands-on lab, you'll learn how to build interactive, multi-page dashboards that bring your data to life.
# MAGIC
# MAGIC **Duration:** 90 minutes  
# MAGIC **Level:** Beginner (100)
# MAGIC
# MAGIC ## What You'll Build
# MAGIC
# MAGIC By the end of this workshop, you'll have created a complete sales analytics dashboard featuring:
# MAGIC - 📊 Interactive visualizations (charts, counters, maps)
# MAGIC - 🗺️ Geographic visualizations with zip code mapping
# MAGIC - 🔀 Sankey diagrams for customer journey analysis
# MAGIC - 🎛️ Dynamic filters and parameters
# MAGIC - 📄 Multiple dashboard pages with navigation
# MAGIC - 🎨 Custom theming and styling
# MAGIC - 🤖 AI-powered exploration with Genie
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Create and prepare sample datasets for dashboards
# MAGIC - Build a multi-page interactive dashboard from scratch
# MAGIC - Add datasets, SQL queries, and parameters for dynamic visualizations
# MAGIC - Design widgets: counters, filters, charts, maps, and Sankey diagrams
# MAGIC - Publish and share dashboards securely
# MAGIC - Explore data using Databricks One and Genie
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Before starting, ensure you have:
# MAGIC - A Databricks workspace
# MAGIC - A SQL warehouse (Serverless recommended)
# MAGIC - Write access to a Unity Catalog catalog
# MAGIC - Basic SQL knowledge
# MAGIC
# MAGIC ## Workshop Structure
# MAGIC
# MAGIC 1. **Setup & Data Preparation** - Configure your environment and load sample data
# MAGIC 2. **Dashboard Basics** - Create your first dashboard and connect datasets
# MAGIC 3. **Building Visualizations** - Add counters, charts, and KPIs
# MAGIC 4. **Interactive Elements** - Create filters and parameters
# MAGIC 5. **Advanced Visualizations** - Build maps and Sankey diagrams
# MAGIC 6. **Multi-Page Design** - Organize content across multiple pages
# MAGIC 7. **Theming & Styling** - Apply custom themes
# MAGIC 8. **Publishing** - Share your dashboard with others
# MAGIC 9. **AI Exploration** - Use Databricks One and Genie
# MAGIC
# MAGIC Let's get started! 🚀

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1: Setup & Data Preparation
# MAGIC
# MAGIC In this section, we'll set up your environment and load the sample data needed for the workshop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.1: Configure Your Catalog and Schema
# MAGIC
# MAGIC First, let's define where we'll store our workshop data. Update the variables below with your catalog name.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Replace `"main"` with your catalog name (you must have CREATE SCHEMA privileges)
# MAGIC 2. The schema `ai_bi_workshop` will be created automatically
# MAGIC 3. Run the cell to set your configuration

# COMMAND ----------

# Configuration - UPDATE THIS WITH YOUR CATALOG
dbutils.widgets.text("catalog_name", "ask_demos", "Catalog")
dbutils.widgets.text("schema_name", "ai_bi_workshop", "Schema")
catalog_name="ask_demos"

# Display configuration
print("=" * 60)
print("WORKSHOP CONFIGURATION")
print("=" * 60)
print(f"Catalog: {catalog_name}")
print(f"Schema:  {schema_name}")
print(f"Full Path: {catalog_name}.{schema_name}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.2: Create Schema
# MAGIC
# MAGIC Now we'll create a dedicated schema for our workshop tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog_name};
# MAGIC -- Create schema for the workshop
# MAGIC CREATE SCHEMA IF NOT EXISTS ${schema_name}
# MAGIC COMMENT 'Schema for AI/BI Dashboard Workshop';
# MAGIC
# MAGIC -- Verify schema creation
# MAGIC DESCRIBE SCHEMA ${schema_name};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.3: Create Workshop Tables
# MAGIC
# MAGIC We'll create several tables to support our dashboard:
# MAGIC - **sales_transactions** - Sales data with products, customers, and locations
# MAGIC - **customers** - Customer demographic and segment information
# MAGIC - **products** - Product catalog
# MAGIC - **store_locations** - Store information with geographic coordinates
# MAGIC - **geography** - Zip code data for mapping
# MAGIC - **customer_journey** - Customer interaction touchpoints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sales Transactions Table
# MAGIC CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.sales_transactions (
# MAGIC   transaction_id STRING,
# MAGIC   transaction_date DATE,
# MAGIC   customer_id STRING,
# MAGIC   product_id STRING,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   quantity INT,
# MAGIC   unit_price DECIMAL(10,2),
# MAGIC   total_amount DECIMAL(10,2),
# MAGIC   payment_method STRING,
# MAGIC   store_id STRING,
# MAGIC   store_name STRING,
# MAGIC   zip_code STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   region STRING,
# MAGIC   sales_channel STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Sales transaction data for dashboard workshop';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers Table
# MAGIC CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.customers (
# MAGIC   customer_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   customer_segment STRING,
# MAGIC   loyalty_tier STRING,
# MAGIC   signup_date DATE,
# MAGIC   lifetime_value DECIMAL(10,2),
# MAGIC   zip_code STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   region STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Customer information for dashboard workshop';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Products Table
# MAGIC CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.products (
# MAGIC   product_id STRING,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   product_subcategory STRING,
# MAGIC   brand STRING,
# MAGIC   unit_price DECIMAL(10,2),
# MAGIC   cost DECIMAL(10,2),
# MAGIC   margin DECIMAL(5,2),
# MAGIC   stock_quantity INT,
# MAGIC   supplier STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Product catalog for dashboard workshop';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Store Locations Table
# MAGIC CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.store_locations (
# MAGIC   store_id STRING,
# MAGIC   store_name STRING,
# MAGIC   store_type STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   zip_code STRING,
# MAGIC   region STRING,
# MAGIC   latitude DECIMAL(10,6),
# MAGIC   longitude DECIMAL(10,6),
# MAGIC   opening_date DATE,
# MAGIC   square_footage INT,
# MAGIC   manager_name STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Store location data for dashboard workshop';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geography Table
# MAGIC CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.geography (
# MAGIC   zip_code STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   latitude DECIMAL(10,6),
# MAGIC   longitude DECIMAL(10,6),
# MAGIC   region STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Geographic data for mapping in dashboard workshop';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer Journey Table
# MAGIC CREATE OR REPLACE TABLE ${catalog_name}.${schema_name}.customer_journey (
# MAGIC   journey_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   interaction_date DATE,
# MAGIC   touchpoint STRING,
# MAGIC   channel STRING,
# MAGIC   outcome STRING,
# MAGIC   value DECIMAL(10,2)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Customer journey touchpoint data for dashboard workshop';

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Tables Created Successfully!**
# MAGIC
# MAGIC All required tables have been created. Now let's load sample data into them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.4: Load Sample Data
# MAGIC
# MAGIC We'll now populate our tables with realistic sample data.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
random.seed(42)

print("Loading sample data...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Products

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType
)

# Product data with Decimal for decimal fields
products_data = [
    ("P001", "Laptop Pro 15", "Electronics", "Computers", "TechBrand", Decimal("1299.99"), Decimal("800.00"), Decimal("38.46"), 45, "Global Tech Supply"),
    ("P002", "Wireless Mouse", "Electronics", "Accessories", "TechBrand", Decimal("29.99"), Decimal("12.00"), Decimal("59.99"), 250, "Global Tech Supply"),
    ("P003", "USB-C Cable", "Electronics", "Accessories", "TechBrand", Decimal("19.99"), Decimal("5.00"), Decimal("74.99"), 500, "Cable Co"),
    ("P004", "Office Chair Deluxe", "Furniture", "Seating", "ComfortPlus", Decimal("299.99"), Decimal("150.00"), Decimal("50.00"), 80, "Furniture Depot"),
    ("P005", "Standing Desk", "Furniture", "Desks", "ComfortPlus", Decimal("599.99"), Decimal("300.00"), Decimal("50.00"), 35, "Furniture Depot"),
    ("P006", "LED Desk Lamp", "Furniture", "Lighting", "BrightLight", Decimal("49.99"), Decimal("20.00"), Decimal("59.98"), 120, "Lighting Inc"),
    ("P007", "Notebook Set", "Office Supplies", "Paper", "WriteWell", Decimal("12.99"), Decimal("4.00"), Decimal("69.21"), 400, "Paper Plus"),
    ("P008", "Gel Pen Pack", "Office Supplies", "Writing", "WriteWell", Decimal("8.99"), Decimal("2.50"), Decimal("72.19"), 600, "Paper Plus"),
    ("P009", "Wireless Keyboard", "Electronics", "Accessories", "TechBrand", Decimal("79.99"), Decimal("35.00"), Decimal("56.25"), 180, "Global Tech Supply"),
    ("P010", "Monitor 27 inch", "Electronics", "Displays", "TechBrand", Decimal("349.99"), Decimal("200.00"), Decimal("42.86"), 60, "Global Tech Supply"),
    ("P011", "Webcam HD", "Electronics", "Accessories", "TechBrand", Decimal("89.99"), Decimal("40.00"), Decimal("55.55"), 95, "Global Tech Supply"),
    ("P012", "Headphones Noise-Cancel", "Electronics", "Audio", "SoundMax", Decimal("199.99"), Decimal("90.00"), Decimal("55.00"), 110, "Audio Suppliers"),
    ("P013", "Desk Organizer", "Office Supplies", "Organization", "OrganizePro", Decimal("24.99"), Decimal("10.00"), Decimal("59.98"), 200, "Office Goods"),
    ("P014", "Whiteboard Small", "Office Supplies", "Presentation", "WriteWell", Decimal("39.99"), Decimal("18.00"), Decimal("54.99"), 75, "Office Goods"),
    ("P015", "Ergonomic Mouse Pad", "Office Supplies", "Accessories", "ComfortPlus", Decimal("15.99"), Decimal("5.00"), Decimal("68.73"), 300, "Office Goods"),
    ("P016", "Laptop Stand", "Electronics", "Accessories", "TechBrand", Decimal("44.99"), Decimal("18.00"), Decimal("59.99"), 140, "Global Tech Supply"),
    ("P017", "Filing Cabinet", "Furniture", "Storage", "ComfortPlus", Decimal("179.99"), Decimal("90.00"), Decimal("50.00"), 40, "Furniture Depot"),
    ("P018", "Bookshelf 5-Tier", "Furniture", "Storage", "ComfortPlus", Decimal("129.99"), Decimal("60.00"), Decimal("53.85"), 55, "Furniture Depot"),
    ("P019", "Paper Shredder", "Office Supplies", "Equipment", "SecureDoc", Decimal("99.99"), Decimal("50.00"), Decimal("50.00"), 45, "Office Equipment Co"),
    ("P020", "Label Maker", "Office Supplies", "Equipment", "OrganizePro", Decimal("34.99"), Decimal("15.00"), Decimal("57.13"), 85, "Office Equipment Co"),
]

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("unit_price", DecimalType(10,2), True),
    StructField("cost", DecimalType(10,2), True),
    StructField("margin", DecimalType(5,2), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier", StringType(), True),
])

# Create DataFrame
products_df = spark.createDataFrame(products_data, products_schema)

# 1) Register as temp view (session-scoped temp "table")
products_df.createOrReplaceTempView("tmp_products")

# 2) Use SQL INSERT INTO ... SELECT ... to load into the target table
spark.sql(f"""
    INSERT INTO {catalog_name}.{schema_name}.products
    SELECT
        product_id,
        product_name,
        product_category,
        product_subcategory,
        brand,
        unit_price,
        cost,
        margin,
        stock_quantity,
        supplier
    FROM tmp_products
""")

print(f"✓ Loaded {products_df.count()} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Store Locations

# COMMAND ----------

from pyspark.sql.functions import to_date, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType, DoubleType
)

# Store location data
stores_data = [
    ("S001", "Downtown Store", "Flagship", "123 Main St", "New York", "NY", "10001", "Northeast", 40.7506, -73.9971, "2020-01-15", 15000, "Sarah Johnson"),
    ("S002", "Brooklyn Hub", "Standard", "456 Bedford Ave", "Brooklyn", "NY", "11215", "Northeast", 40.6632, -73.9855, "2020-03-20", 8500, "Michael Chen"),
    ("S003", "Chicago Loop", "Flagship", "789 State St", "Chicago", "IL", "60601", "Midwest", 41.8842, -87.6219, "2019-11-10", 12000, "Jennifer Williams"),
    ("S004", "San Francisco Bay", "Standard", "321 Market St", "San Francisco", "CA", "94102", "West", 37.7799, -122.4193, "2021-02-14", 9500, "David Martinez"),
    ("S005", "LA Downtown", "Flagship", "555 Figueroa St", "Los Angeles", "CA", "90012", "West", 34.0622, -118.2437, "2019-08-05", 14000, "Lisa Anderson"),
    ("S006", "Seattle Center", "Standard", "888 Pine St", "Seattle", "WA", "98101", "West", 47.6101, -122.3341, "2020-06-22", 7500, "Robert Taylor"),
    ("S007", "Boston Commons", "Standard", "234 Boylston St", "Boston", "MA", "02116", "Northeast", 42.3496, -71.0746, "2021-01-08", 8000, "Emily White"),
    ("S008", "Miami Beach", "Standard", "777 Ocean Dr", "Miami", "FL", "33139", "Southeast", 25.7814, -80.1346, "2020-09-15", 8500, "Carlos Rodriguez"),
    ("S009", "Atlanta Midtown", "Standard", "999 Peachtree St", "Atlanta", "GA", "30309", "Southeast", 33.7840, -84.3848, "2021-04-12", 9000, "Amanda Davis"),
    ("S010", "Denver Tech", "Standard", "444 17th St", "Denver", "CO", "80202", "West", 39.7539, -104.9910, "2020-12-01", 8000, "James Wilson"),
]

stores_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("latitude", DoubleType(), True),   # was DecimalType(10,6)
    StructField("longitude", DoubleType(), True),  # was DecimalType(10,6)
    StructField("opening_date", StringType(), True),
    StructField("square_footage", IntegerType(), True),
    StructField("manager_name", StringType(), True),
])

# Create DataFrame
stores_df = spark.createDataFrame(stores_data, stores_schema)

# Cast opening_date to proper DATE type
stores_df = stores_df.withColumn("opening_date", to_date(col("opening_date")))

# Temp view
stores_df.createOrReplaceTempView("tmp_store_locations")

# Insert into target table
spark.sql(f"""
    INSERT INTO {catalog_name}.{schema_name}.store_locations
    SELECT
        store_id,
        store_name,
        store_type,
        address,
        city,
        state,
        zip_code,
        region,
        latitude,
        longitude,
        opening_date,
        square_footage,
        manager_name
    FROM tmp_store_locations
""")

print(f"✓ Inserted {stores_df.count()} stores into {catalog_name}.{schema_name}.store_locations")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Geography Data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

geography_data = [
    ("10001", "New York", "NY", 40.7506, -73.9971, "Northeast"),
    ("11215", "Brooklyn", "NY", 40.6632, -73.9855, "Northeast"),
    ("60601", "Chicago", "IL", 41.8842, -87.6219, "Midwest"),
    ("94102", "San Francisco", "CA", 37.7799, -122.4193, "West"),
    ("90012", "Los Angeles", "CA", 34.0622, -118.2437, "West"),
    ("98101", "Seattle", "WA", 47.6101, -122.3341, "West"),
    ("02116", "Boston", "MA", 42.3496, -71.0746, "Northeast"),
    ("33139", "Miami", "FL", 25.7814, -80.1346, "Southeast"),
    ("30309", "Atlanta", "GA", 33.7840, -84.3848, "Southeast"),
    ("80202", "Denver", "CO", 39.7539, -104.9910, "West"),
    ("75201", "Dallas", "TX", 32.7797, -96.7954, "South"),
    ("77002", "Houston", "TX", 29.7604, -95.3698, "South"),
    ("19102", "Philadelphia", "PA", 39.9526, -75.1652, "Northeast"),
    ("85001", "Phoenix", "AZ", 33.4484, -112.0740, "West"),
    ("92101", "San Diego", "CA", 32.7157, -117.1611, "West"),
    ("78701", "Austin", "TX", 30.2711, -97.7437, "South"),
    ("32801", "Orlando", "FL", 28.5383, -81.3792, "Southeast"),
    ("28202", "Charlotte", "NC", 35.2271, -80.8431, "Southeast"),
    ("97201", "Portland", "OR", 45.5098, -122.6890, "West"),
    ("63101", "Saint Louis", "MO", 38.6270, -90.1994, "Midwest"),
]

geography_schema = StructType([
    StructField("zip_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("region", StringType(), True),
])

geography_df = spark.createDataFrame(geography_data, geography_schema)

# Cast to DECIMAL(10,6) to match the table definition
geography_df = geography_df.select(
    col("zip_code"),
    col("city"),
    col("state"),
    col("latitude").cast("DECIMAL(10,6)").alias("latitude"),
    col("longitude").cast("DECIMAL(10,6)").alias("longitude"),
    col("region"),
)

geography_df.createOrReplaceTempView("tmp_geography")

spark.sql(f"""
    INSERT OVERWRITE {catalog_name}.{schema_name}.geography
    SELECT
      zip_code,
      city,
      state,
      latitude,
      longitude,
      region
    FROM tmp_geography
""")

print(f"✓ Loaded {geography_df.count()} zip codes into {catalog_name}.{schema_name}.geography")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customer Data

# COMMAND ----------

from decimal import Decimal
from datetime import datetime, timedelta
import random

from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, DateType
)
from pyspark.sql.functions import col, to_date

num_customers = 300
first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Barbara",
               "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee"]
segments = ["Enterprise", "SMB", "Individual"]
loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]

# geography_df already exists from previous step
geo_zips = geography_df.select("zip_code", "city", "state", "region").collect()

# Check if we have geography data
if not geo_zips:
    raise ValueError("geography_df is empty. Please ensure geography data exists before running this code.")

customers_list = []
for i in range(1, num_customers + 1):
    customer_id = f"C{i:04d}"
    first = random.choice(first_names)
    last = random.choice(last_names)
    customer_name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}{i}@email.com"
    phone = f"555-{random.randint(100,999)}-{random.randint(1000,9999)}"
    segment = random.choice(segments)
    loyalty = random.choice(loyalty_tiers)

    days_ago = random.randint(0, 1095)
    signup_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

    # Generate as Decimal to match DECIMAL(10,2)
    lifetime_value = Decimal(str(random.uniform(100, 50000))).quantize(Decimal('0.01'))

    geo_info = random.choice(geo_zips)
    customers_list.append((
        customer_id,
        customer_name,
        email,
        phone,
        segment,
        loyalty,
        signup_date,
        lifetime_value,
        geo_info.zip_code,
        geo_info.city,
        geo_info.state,
        geo_info.region
    ))

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("signup_date", StringType(), True),  # Will be converted to DateType
    StructField("lifetime_value", DecimalType(10,2), True),
    StructField("zip_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
])

customers_df = spark.createDataFrame(customers_list, customers_schema)

# Convert signup_date to DATE type
customers_df = customers_df.withColumn(
    "signup_date",
    to_date(col("signup_date"), "yyyy-MM-dd")
)

customers_df.createOrReplaceTempView("tmp_customers")

spark.sql(f"""
    INSERT OVERWRITE {catalog_name}.{schema_name}.customers
    SELECT
      customer_id,
      customer_name,
      email,
      phone,
      customer_segment,
      loyalty_tier,
      signup_date,
      lifetime_value,
      zip_code,
      city,
      state,
      region
    FROM tmp_customers
""")

print(f"✓ Loaded {customers_df.count()} customers into {catalog_name}.{schema_name}.customers")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Sales Transactions

# COMMAND ----------

from decimal import Decimal
from datetime import datetime, timedelta
import random

from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType
)
from pyspark.sql.functions import col, to_date

num_transactions = 3000

products_list = products_df.select(
    "product_id", "product_name", "product_category", "unit_price"
).collect()

customers_list_ids = customers_df.select("customer_id").collect()

stores_list = stores_df.select(
    "store_id", "store_name", "zip_code", "city", "state", "region"
).collect()

payment_methods = ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "PayPal"]
channels = ["In-Store", "Online", "Mobile App", "Phone Order"]

transactions_list = []
for i in range(1, num_transactions + 1):
    transaction_id = f"T{i:06d}"
    days_ago = random.randint(0, 365)
    transaction_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

    customer = random.choice(customers_list_ids)
    product = random.choice(products_list)
    store = random.choice(stores_list)

    quantity = random.randint(1, 5)

    # product.unit_price is already Decimal from products_df
    unit_price = Decimal(str(product.unit_price))
    total_amount = (unit_price * Decimal(quantity)).quantize(Decimal("0.01"))

    payment = random.choice(payment_methods)
    channel = random.choice(channels)

    transactions_list.append((
        transaction_id,
        transaction_date,
        customer.customer_id,
        product.product_id,
        product.product_name,
        product.product_category,
        quantity,
        unit_price,
        total_amount,
        payment,
        store.store_id,
        store.store_name,
        store.zip_code,
        store.city,
        store.state,
        store.region,
        channel
    ))

transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10,2), True),
    StructField("total_amount", DecimalType(10,2), True),
    StructField("payment_method", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sales_channel", StringType(), True),
])

transactions_df = spark.createDataFrame(transactions_list, transactions_schema)

transactions_df = transactions_df.withColumn(
    "transaction_date", to_date(col("transaction_date"))
)

transactions_df.write.mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.sales_transactions"
)

print(f"✓ Loaded {transactions_df.count()} transactions")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customer Journey Data

# COMMAND ----------

from decimal import Decimal
from datetime import datetime, timedelta
import random

from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from pyspark.sql.functions import col, to_date

touchpoints = ["Social Media", "Email", "Website Visit", "Store Visit", "Customer Service", "Mobile App"]
channels_journey = ["Digital", "Physical", "Phone", "Social"]
outcomes = ["Purchase", "Inquiry", "Support", "Browse"]

# Extract customer IDs from the customers dataframe
customers_list_ids = customers_df.select("customer_id").collect()

# Check if we have customer data
if not customers_list_ids:
    raise ValueError("customers_df is empty. Please ensure customer data exists before running this code.")

num_journeys = 1500
journeys_list = []

for i in range(1, num_journeys + 1):
    journey_id = f"J{i:05d}"
    customer = random.choice(customers_list_ids)
    days_ago = random.randint(0, 180)
    interaction_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    touchpoint = random.choice(touchpoints)
    channel = random.choice(channels_journey)
    outcome = random.choice(outcomes)

    # Generate as Decimal to match DECIMAL(10,2); round to 2 decimal places
    if outcome == "Purchase":
        value = Decimal(str(random.uniform(50, 2000))).quantize(Decimal('0.01'))
    else:
        value = Decimal("0.00")

    journeys_list.append((
        journey_id,
        customer.customer_id,
        interaction_date,
        touchpoint,
        channel,
        outcome,
        value
    ))

journeys_schema = StructType([
    StructField("journey_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("interaction_date", StringType(), True),
    StructField("touchpoint", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("value", DecimalType(10,2), True),
])

journeys_df = spark.createDataFrame(journeys_list, journeys_schema)

# Convert interaction_date to DATE type
journeys_df = journeys_df.withColumn(
    "interaction_date",
    to_date(col("interaction_date"), "yyyy-MM-dd")
)

journeys_df.write.mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.customer_journey"
)

print(f"✓ Loaded {journeys_df.count()} customer journey records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.5: Verify Data Load
# MAGIC
# MAGIC Let's verify that all data has been loaded successfully.

# COMMAND ----------

print("=" * 70)
print("DATA LOAD SUMMARY")
print("=" * 70)
print(f"Catalog.Schema: {catalog_name}.{schema_name}")
print("-" * 70)
print(f"Products:              {spark.table(f'{catalog_name}.{schema_name}.products').count():>6,} records")
print(f"Stores:                {spark.table(f'{catalog_name}.{schema_name}.store_locations').count():>6,} records")
print(f"Geography:             {spark.table(f'{catalog_name}.{schema_name}.geography').count():>6,} records")
print(f"Customers:             {spark.table(f'{catalog_name}.{schema_name}.customers').count():>6,} records")
print(f"Sales Transactions:    {spark.table(f'{catalog_name}.{schema_name}.sales_transactions').count():>6,} records")
print(f"Customer Journeys:     {spark.table(f'{catalog_name}.{schema_name}.customer_journey').count():>6,} records")
print("=" * 70)
print("✅ All data loaded successfully!")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.6: Preview Sample Data
# MAGIC
# MAGIC Let's take a look at some of the data we'll be working with.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent sales transactions
# MAGIC SELECT 
# MAGIC   transaction_id,
# MAGIC   transaction_date,
# MAGIC   customer_id,
# MAGIC   product_name,
# MAGIC   product_category,
# MAGIC   quantity,
# MAGIC   total_amount,
# MAGIC   sales_channel,
# MAGIC   city,
# MAGIC   state,
# MAGIC   region
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC ORDER BY transaction_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.7: Create Summary Queries
# MAGIC
# MAGIC Before building the dashboard, let's create some useful SQL queries that we'll use for visualizations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total Revenue by Region
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   COUNT(DISTINCT transaction_id) as transaction_count,
# MAGIC   SUM(total_amount) as total_revenue,
# MAGIC   AVG(total_amount) as avg_transaction_value
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY region
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sales by Product Category
# MAGIC SELECT 
# MAGIC   product_category,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   SUM(quantity) as units_sold
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY product_category
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sales Trend Over Time
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', transaction_date) as month,
# MAGIC   SUM(total_amount) as monthly_revenue,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY DATE_TRUNC('month', transaction_date)
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2: Building Your First Dashboard
# MAGIC
# MAGIC Now that our data is ready, let's create an AI/BI dashboard!
# MAGIC
# MAGIC ## What is Databricks AI/BI?
# MAGIC
# MAGIC Databricks AI/BI is a powerful, AI-powered business intelligence platform that enables you to:
# MAGIC - Build interactive dashboards with a drag-and-drop interface
# MAGIC - Query data using natural language with Genie
# MAGIC - Create complex visualizations without writing code
# MAGIC - Share insights securely across your organization
# MAGIC
# MAGIC ## Step 2.1: Access AI/BI Dashboards
# MAGIC
# MAGIC **Instructions:**
# MAGIC
# MAGIC 1. In your Databricks workspace, click the **AI/BI** icon in the left sidebar (or navigate to the AI/BI menu)
# MAGIC 2. Click **+ New dashboard** button
# MAGIC 3. Name your dashboard: **"Sales Analytics Workshop"**
# MAGIC 4. Select your SQL warehouse from the dropdown
# MAGIC 5. Click **Create**
# MAGIC
# MAGIC You should now see a blank canvas ready for widgets!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2.2: Connect Your Dataset
# MAGIC
# MAGIC Before adding visualizations, we need to connect our data:
# MAGIC
# MAGIC 1. Click **+ Add** in the top toolbar
# MAGIC 2. Select **Dataset**
# MAGIC 3. Choose **SQL query** as the data source
# MAGIC 4. Name it: `sales_data`
# MAGIC 5. Paste this SQL query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM main.ai_bi_workshop.sales_transactions
# MAGIC ```
# MAGIC
# MAGIC 6. Replace `main` with your catalog name if different
# MAGIC 7. Click **Save**
# MAGIC
# MAGIC **💡 Tip:** Datasets in AI/BI act as reusable data sources. You can reference the same dataset across multiple widgets!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2.3: Create Your First Visualization - Revenue Counter
# MAGIC
# MAGIC Let's add a counter to show total revenue:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Counter** widget type
# MAGIC 3. **Configure the counter:**
# MAGIC    - **Dataset**: Select `sales_data`
# MAGIC    - **Value**: Choose `total_amount` field
# MAGIC    - **Aggregation**: Select `SUM`
# MAGIC    - **Label**: "Total Revenue"
# MAGIC    - **Format**: Currency (USD)
# MAGIC 4. Click **Save**
# MAGIC 5. Drag and position the counter at the top-left of your dashboard
# MAGIC
# MAGIC You should now see your total revenue displayed prominently!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3: Adding More Visualizations
# MAGIC
# MAGIC Let's build out more visualizations to create a comprehensive dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.1: Add KPI Counters
# MAGIC
# MAGIC Create three more counters next to your revenue counter:
# MAGIC
# MAGIC ### Counter 2: Total Transactions
# MAGIC - **Value**: `transaction_id`
# MAGIC - **Aggregation**: `COUNT DISTINCT`
# MAGIC - **Label**: "Total Transactions"
# MAGIC - **Format**: Number with comma separator
# MAGIC
# MAGIC ### Counter 3: Average Order Value
# MAGIC - **Value**: `total_amount`
# MAGIC - **Aggregation**: `AVERAGE`
# MAGIC - **Label**: "Avg Order Value"
# MAGIC - **Format**: Currency (USD)
# MAGIC
# MAGIC ### Counter 4: Unique Customers
# MAGIC - **Value**: `customer_id`
# MAGIC - **Aggregation**: `COUNT DISTINCT`
# MAGIC - **Label**: "Unique Customers"
# MAGIC - **Format**: Number
# MAGIC
# MAGIC **💡 Tip:** Arrange these counters in a row at the top of your dashboard for a clean KPI summary!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.2: Create a Bar Chart - Revenue by Region
# MAGIC
# MAGIC Now let's add a bar chart to show revenue by region:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Bar chart**
# MAGIC 3. **Configure the chart:**
# MAGIC    - **Dataset**: Select `sales_data`
# MAGIC    - **X-axis**: `region`
# MAGIC    - **Y-axis**: `total_amount` with `SUM` aggregation
# MAGIC    - **Title**: "Revenue by Region"
# MAGIC    - **Sort**: By value (descending)
# MAGIC 4. Click **Save**
# MAGIC 5. Resize and position the chart below your counters
# MAGIC
# MAGIC **💡 Customization Tips:**
# MAGIC - Change bar colors in the **Style** tab
# MAGIC - Add data labels to show exact values
# MAGIC - Enable horizontal layout if preferred

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.3: Create a Line Chart - Sales Trend
# MAGIC
# MAGIC Add a line chart to show sales over time:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Line chart**
# MAGIC 3. **Configure the chart:**
# MAGIC    - **Dataset**: `sales_data`
# MAGIC    - **X-axis**: `transaction_date` (Date truncated to month)
# MAGIC    - **Y-axis**: `total_amount` with `SUM` aggregation
# MAGIC    - **Title**: "Monthly Sales Trend"
# MAGIC 4. **Advanced settings:**
# MAGIC    - Enable data markers
# MAGIC    - Add smoothing if desired
# MAGIC 5. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.4: Create a Pie Chart - Revenue by Product Category
# MAGIC
# MAGIC Add a pie chart for product category breakdown:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Pie chart**
# MAGIC 3. **Configure the chart:**
# MAGIC    - **Dataset**: `sales_data`
# MAGIC    - **Label**: `product_category`
# MAGIC    - **Value**: `total_amount` with `SUM` aggregation
# MAGIC    - **Title**: "Revenue by Product Category"
# MAGIC 4. **Style options:**
# MAGIC    - Show percentages
# MAGIC    - Display legend
# MAGIC    - Choose a color palette
# MAGIC 5. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3.5: Create a Data Table
# MAGIC
# MAGIC Add a table showing top products:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Table**
# MAGIC 3. Create a new dataset or query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   product_name,
# MAGIC   product_category,
# MAGIC   SUM(quantity) as units_sold,
# MAGIC   SUM(total_amount) as revenue
# MAGIC FROM main.ai_bi_workshop.sales_transactions
# MAGIC GROUP BY product_name, product_category
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10
# MAGIC ```
# MAGIC
# MAGIC 4. **Configure columns:**
# MAGIC    - Format revenue as currency
# MAGIC    - Format units_sold with comma separators
# MAGIC    - Add conditional formatting (green for high revenue)
# MAGIC 5. **Title**: "Top 10 Products by Revenue"
# MAGIC 6. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 4: Adding Interactive Filters
# MAGIC
# MAGIC Let's make the dashboard interactive by adding filters and parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4.1: Create a Date Range Parameter
# MAGIC
# MAGIC Parameters allow users to dynamically filter data:
# MAGIC
# MAGIC 1. Click **+ Add** → **Parameter**
# MAGIC 2. **Configure parameter:**
# MAGIC    - **Name**: `date_range`
# MAGIC    - **Display name**: "Date Range"
# MAGIC    - **Type**: Date range
# MAGIC    - **Default**: Last 30 days
# MAGIC 3. Click **Create**
# MAGIC
# MAGIC Now update your queries to use this parameter by adding:
# MAGIC ```sql
# MAGIC WHERE transaction_date BETWEEN :date_range_start AND :date_range_end
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4.2: Create a Region Filter
# MAGIC
# MAGIC Add a dropdown filter for regions:
# MAGIC
# MAGIC 1. Click **+ Add** → **Filter**
# MAGIC 2. **Configure filter:**
# MAGIC    - **Name**: "Region"
# MAGIC    - **Field**: `region` from `sales_data`
# MAGIC    - **Type**: Multi-select dropdown
# MAGIC    - **Default**: All regions selected
# MAGIC 3. Click **Create**
# MAGIC
# MAGIC The filter will automatically apply to all visualizations using the same dataset!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4.3: Create Additional Filters
# MAGIC
# MAGIC Add these filters for more interactivity:
# MAGIC
# MAGIC ### Product Category Filter
# MAGIC - **Field**: `product_category`
# MAGIC - **Type**: Multi-select dropdown
# MAGIC - **Display name**: "Product Category"
# MAGIC
# MAGIC ### Sales Channel Filter
# MAGIC - **Field**: `sales_channel`
# MAGIC - **Type**: Multi-select dropdown
# MAGIC - **Display name**: "Sales Channel"
# MAGIC
# MAGIC **💡 Tip:** Place all filters in a horizontal bar at the top of your dashboard for easy access!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 5: Advanced Visualizations
# MAGIC
# MAGIC Now let's add some advanced visualizations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5.1: Create a Geographic Map
# MAGIC
# MAGIC Visualize sales on a map using geographic data:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Map**
# MAGIC 3. Create a new dataset with this query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   g.city,
# MAGIC   g.state,
# MAGIC   g.latitude,
# MAGIC   g.longitude,
# MAGIC   SUM(s.total_amount) as revenue,
# MAGIC   COUNT(DISTINCT s.transaction_id) as transaction_count
# MAGIC FROM main.ai_bi_workshop.sales_transactions s
# MAGIC JOIN main.ai_bi_workshop.geography g 
# MAGIC   ON s.zip_code = g.zip_code
# MAGIC GROUP BY g.city, g.state, g.latitude, g.longitude
# MAGIC ```
# MAGIC
# MAGIC 4. **Configure map:**
# MAGIC    - **Latitude**: `latitude`
# MAGIC    - **Longitude**: `longitude`
# MAGIC    - **Size**: `revenue` (larger circles = more revenue)
# MAGIC    - **Color**: `revenue` (gradient coloring)
# MAGIC    - **Tooltip**: Show city, state, and revenue
# MAGIC    - **Title**: "Sales by Location"
# MAGIC 5. **Styling:**
# MAGIC    - Choose a base map style (light, dark, or satellite)
# MAGIC    - Adjust circle size and opacity
# MAGIC    - Set color gradient (e.g., blue to red)
# MAGIC 6. Click **Save**
# MAGIC
# MAGIC **💡 Tip:** Maps are great for identifying geographic patterns and opportunities!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5.2: Create a Sankey Diagram
# MAGIC
# MAGIC Sankey diagrams show customer journey flow:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Sankey**
# MAGIC 3. Create a new dataset with this query:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   touchpoint as source,
# MAGIC   outcome as target,
# MAGIC   COUNT(*) as value
# MAGIC FROM main.ai_bi_workshop.customer_journey
# MAGIC GROUP BY touchpoint, outcome
# MAGIC ORDER BY value DESC
# MAGIC ```
# MAGIC
# MAGIC 4. **Configure Sankey:**
# MAGIC    - **Source**: `source`
# MAGIC    - **Target**: `target`
# MAGIC    - **Value**: `value`
# MAGIC    - **Title**: "Customer Journey Flow"
# MAGIC 5. **Styling:**
# MAGIC    - Adjust node colors
# MAGIC    - Set link opacity
# MAGIC    - Show values on hover
# MAGIC 6. Click **Save**
# MAGIC
# MAGIC **Understanding Sankey Diagrams:**
# MAGIC - Width of flows represents volume
# MAGIC - Shows how customers move through touchpoints
# MAGIC - Helps identify conversion paths and drop-offs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5.3: Create a Heatmap
# MAGIC
# MAGIC Add a heatmap for day/time analysis:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Heatmap**
# MAGIC 3. Create a dataset:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   DAYOFWEEK(transaction_date) as day_of_week,
# MAGIC   HOUR(transaction_date) as hour_of_day,
# MAGIC   SUM(total_amount) as revenue
# MAGIC FROM main.ai_bi_workshop.sales_transactions
# MAGIC WHERE transaction_date >= CURRENT_DATE - INTERVAL 90 DAYS
# MAGIC GROUP BY day_of_week, hour_of_day
# MAGIC ```
# MAGIC
# MAGIC 4. **Configure heatmap:**
# MAGIC    - **X-axis**: `hour_of_day`
# MAGIC    - **Y-axis**: `day_of_week`
# MAGIC    - **Value**: `revenue`
# MAGIC    - **Title**: "Sales Heatmap by Day and Hour"
# MAGIC 5. **Styling:**
# MAGIC    - Choose color scheme (gradient)
# MAGIC    - Show values in cells
# MAGIC 6. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5.4: Create a Funnel Chart
# MAGIC
# MAGIC Create a conversion funnel:
# MAGIC
# MAGIC 1. Click **+ Add** → **Visualization**
# MAGIC 2. Select **Funnel**
# MAGIC 3. Create a dataset:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   outcome as stage,
# MAGIC   COUNT(*) as count
# MAGIC FROM main.ai_bi_workshop.customer_journey
# MAGIC GROUP BY outcome
# MAGIC ORDER BY 
# MAGIC   CASE outcome
# MAGIC     WHEN 'Browse' THEN 1
# MAGIC     WHEN 'Inquiry' THEN 2
# MAGIC     WHEN 'Support' THEN 3
# MAGIC     WHEN 'Purchase' THEN 4
# MAGIC   END
# MAGIC ```
# MAGIC
# MAGIC 4. **Configure funnel:**
# MAGIC    - **Stage**: `stage`
# MAGIC    - **Value**: `count`
# MAGIC    - **Title**: "Customer Conversion Funnel"
# MAGIC    - Show percentages
# MAGIC 5. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 6: Multi-Page Dashboard Design
# MAGIC
# MAGIC Organize your dashboard across multiple pages for better navigation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6.1: Create Dashboard Pages
# MAGIC
# MAGIC Let's organize content into logical pages:
# MAGIC
# MAGIC 1. Click the **Pages** icon in the top toolbar
# MAGIC 2. Click **+ Add page**
# MAGIC 3. Create these pages:
# MAGIC
# MAGIC ### Page 1: Executive Summary
# MAGIC - KPI counters (Revenue, Transactions, AOV, Customers)
# MAGIC - Revenue by Region bar chart
# MAGIC - Monthly Sales Trend line chart
# MAGIC - Top Products table
# MAGIC
# MAGIC ### Page 2: Geographic Analysis
# MAGIC - Sales map visualization
# MAGIC - Revenue by State bar chart
# MAGIC - Store performance table
# MAGIC
# MAGIC ### Page 3: Product Performance
# MAGIC - Revenue by Category pie chart
# MAGIC - Product comparison bar chart
# MAGIC - Product details table
# MAGIC - Category trends over time
# MAGIC
# MAGIC ### Page 4: Customer Insights
# MAGIC - Customer journey Sankey diagram
# MAGIC - Conversion funnel
# MAGIC - Customer segment analysis
# MAGIC - Loyalty tier distribution
# MAGIC
# MAGIC **💡 Tip:** Use consistent styling and layout across pages for a professional look!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6.2: Add Page Navigation
# MAGIC
# MAGIC Make it easy to navigate between pages:
# MAGIC
# MAGIC 1. Add a **Tab** widget at the top of each page
# MAGIC 2. Link tabs to corresponding pages
# MAGIC 3. Alternatively, use **Buttons** for custom navigation
# MAGIC 4. Add a **Text** widget with dashboard title on each page

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6.3: Organize with Containers
# MAGIC
# MAGIC Use containers to group related widgets:
# MAGIC
# MAGIC 1. Click **+ Add** → **Container**
# MAGIC 2. Drag widgets into the container
# MAGIC 3. Style the container:
# MAGIC    - Add border
# MAGIC    - Set background color
# MAGIC    - Add title
# MAGIC
# MAGIC **Example Containers:**
# MAGIC - "Key Performance Indicators" (for KPI counters)
# MAGIC - "Regional Performance" (for region charts)
# MAGIC - "Product Analysis" (for product widgets)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 7: Theming and Styling
# MAGIC
# MAGIC Apply a custom theme to make your dashboard visually appealing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7.1: Apply a Dashboard Theme
# MAGIC
# MAGIC Customize your dashboard's appearance:
# MAGIC
# MAGIC 1. Click the **Customize** (paintbrush) icon in the toolbar
# MAGIC 2. Choose **Theme**
# MAGIC 3. Select from preset themes or create custom:
# MAGIC
# MAGIC ### Recommended Theme Settings:
# MAGIC
# MAGIC **Colors:**
# MAGIC - **Primary color**: Choose your brand color (e.g., #1E88E5)
# MAGIC - **Secondary color**: Complementary color
# MAGIC - **Background**: Light (#F5F5F5) or Dark (#1E1E1E)
# MAGIC - **Text**: High contrast for readability
# MAGIC
# MAGIC **Typography:**
# MAGIC - **Heading font**: Sans-serif (e.g., Roboto, Inter)
# MAGIC - **Body font**: Same or complementary
# MAGIC - **Font sizes**: Consistent hierarchy
# MAGIC
# MAGIC **Spacing:**
# MAGIC - **Widget padding**: 16px
# MAGIC - **Page margins**: 24px
# MAGIC - **Container spacing**: 8px
# MAGIC
# MAGIC 4. Click **Apply theme**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7.2: Customize Individual Widgets
# MAGIC
# MAGIC Fine-tune individual widget styles:
# MAGIC
# MAGIC 1. Select a widget
# MAGIC 2. Click **Edit** → **Style** tab
# MAGIC 3. Customize:
# MAGIC    - Border style and color
# MAGIC    - Shadow effects
# MAGIC    - Background color
# MAGIC    - Text alignment
# MAGIC    - Padding and margins
# MAGIC
# MAGIC **💡 Best Practices:**
# MAGIC - Use consistent colors from your theme
# MAGIC - Maintain visual hierarchy (larger = more important)
# MAGIC - Ensure adequate contrast for accessibility
# MAGIC - Test on different screen sizes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7.3: Add Branding Elements
# MAGIC
# MAGIC Make the dashboard yours:
# MAGIC
# MAGIC 1. Add a **Text** widget for your logo/title
# MAGIC 2. Use **Markdown** for rich formatting:
# MAGIC
# MAGIC ```markdown
# MAGIC # Sales Analytics Dashboard
# MAGIC **Q4 2024 Performance**
# MAGIC
# MAGIC Last updated: {{current_date}}
# MAGIC ```
# MAGIC
# MAGIC 3. Add an **Image** widget for company logo (optional)
# MAGIC 4. Include footer with dashboard version/owner

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 8: Publishing and Sharing
# MAGIC
# MAGIC Make your dashboard available to others.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8.1: Publish Your Dashboard
# MAGIC
# MAGIC Publish to make your dashboard available:
# MAGIC
# MAGIC 1. Click **Publish** in the top-right corner
# MAGIC 2. Review changes
# MAGIC 3. Add a **version note** describing updates
# MAGIC 4. Click **Publish dashboard**
# MAGIC
# MAGIC **What happens when you publish?**
# MAGIC - Dashboard becomes available at a stable URL
# MAGIC - Users with permissions can view it
# MAGIC - Draft changes won't affect published version
# MAGIC - You can have multiple published versions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8.2: Set Permissions
# MAGIC
# MAGIC Control who can access your dashboard:
# MAGIC
# MAGIC 1. Click **Share** button
# MAGIC 2. Add users or groups
# MAGIC 3. Set permission levels:
# MAGIC    - **Can view**: See dashboard and interact with filters
# MAGIC    - **Can run**: View and refresh data
# MAGIC    - **Can edit**: Modify dashboard design
# MAGIC    - **Can manage**: Full control including permissions
# MAGIC 4. Click **Save**
# MAGIC
# MAGIC **💡 Security Tip:** Use Unity Catalog permissions to control data access. Dashboard permissions control dashboard access only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8.3: Create a Schedule (Optional)
# MAGIC
# MAGIC Automatically refresh and email dashboard:
# MAGIC
# MAGIC 1. Click **Schedule** in dashboard settings
# MAGIC 2. **Configure schedule:**
# MAGIC    - Frequency (daily, weekly, monthly)
# MAGIC    - Time of day
# MAGIC    - Recipients
# MAGIC    - Format (PDF or link)
# MAGIC 3. **Enable schedule**
# MAGIC
# MAGIC **Use cases:**
# MAGIC - Daily executive summaries
# MAGIC - Weekly team reports
# MAGIC - Monthly business reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8.4: Embed Dashboard (Optional)
# MAGIC
# MAGIC Embed your dashboard in other applications:
# MAGIC
# MAGIC 1. Click **Share** → **Embed**
# MAGIC 2. Copy the embed code
# MAGIC 3. Paste into your website/application
# MAGIC 4. Configure embedding options:
# MAGIC    - Show/hide filters
# MAGIC    - Enable/disable downloads
# MAGIC    - Set default parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 9: Exploring with Databricks One and Genie
# MAGIC
# MAGIC Discover how business users can explore your data using AI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9.1: Introduction to Databricks One
# MAGIC
# MAGIC **What is Databricks One?**
# MAGIC
# MAGIC Databricks One is a unified interface that brings together all your data and AI assets in one place:
# MAGIC - 🎯 **Single Access Point**: Find dashboards, Genie spaces, notebooks, and models
# MAGIC - 🔍 **Universal Search**: Search across all your data assets
# MAGIC - 📊 **Personalized Home**: See recently viewed items and recommendations
# MAGIC - 🤝 **Collaboration**: Share insights and discoveries easily
# MAGIC
# MAGIC **For Business Users:**
# MAGIC - No SQL or coding required
# MAGIC - Natural language questions with Genie
# MAGIC - Explore dashboards intuitively
# MAGIC - Self-service analytics
# MAGIC
# MAGIC ## Accessing Databricks One
# MAGIC
# MAGIC 1. Navigate to your Databricks workspace home
# MAGIC 2. Look for the **Home** or **One** section
# MAGIC 3. You'll see:
# MAGIC    - Your recently accessed dashboards
# MAGIC    - Genie spaces you've created or have access to
# MAGIC    - Recommended content
# MAGIC    - Search bar for finding assets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9.2: Introduction to Genie
# MAGIC
# MAGIC **What is Genie?**
# MAGIC
# MAGIC Genie is Databricks' AI-powered conversational analytics tool that allows anyone to:
# MAGIC - Ask questions in natural language
# MAGIC - Get instant visualizations
# MAGIC - Explore data without SQL knowledge
# MAGIC - Save and share insights
# MAGIC
# MAGIC **How Genie Works:**
# MAGIC 1. You ask a question in plain English
# MAGIC 2. Genie translates it to SQL
# MAGIC 3. Query runs on your data
# MAGIC 4. Results displayed as charts or tables
# MAGIC 5. You can refine and iterate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9.3: Create a Genie Space
# MAGIC
# MAGIC Let's create a Genie space for our sales data:
# MAGIC
# MAGIC **Instructions:**
# MAGIC
# MAGIC 1. From Databricks One or AI/BI menu, click **+ Create Genie**
# MAGIC 2. **Configure Genie space:**
# MAGIC    - **Name**: "Sales Data Explorer"
# MAGIC    - **Description**: "Ask questions about sales, products, and customers"
# MAGIC    - **Tables**: Select these tables:
# MAGIC      - `sales_transactions`
# MAGIC      - `customers`
# MAGIC      - `products`
# MAGIC      - `store_locations`
# MAGIC      - `geography`
# MAGIC 3. **Add instructions** (optional but recommended):
# MAGIC
# MAGIC ```
# MAGIC This Genie space provides access to retail sales data.
# MAGIC
# MAGIC You can ask questions about:
# MAGIC - Sales performance by region, product, or time period
# MAGIC - Customer segments and behavior
# MAGIC - Product performance and categories
# MAGIC - Store locations and performance
# MAGIC
# MAGIC Example questions:
# MAGIC - "What were total sales last month?"
# MAGIC - "Show me top 10 products by revenue"
# MAGIC - "Which region has the highest average order value?"
# MAGIC - "How many customers are in the Gold loyalty tier?"
# MAGIC ```
# MAGIC
# MAGIC 4. Click **Create**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9.4: Ask Questions with Genie
# MAGIC
# MAGIC Now let's explore our data using natural language:
# MAGIC
# MAGIC **Try These Questions:**
# MAGIC
# MAGIC ### Basic Questions:
# MAGIC 1. *"What is the total revenue?"*
# MAGIC 2. *"How many transactions were there last month?"*
# MAGIC 3. *"Show me sales by region"*
# MAGIC 4. *"What are the top 5 products by revenue?"*
# MAGIC
# MAGIC ### Intermediate Questions:
# MAGIC 5. *"Compare sales between Electronics and Furniture categories"*
# MAGIC 6. *"What is the average order value by customer segment?"*
# MAGIC 7. *"Show monthly sales trend for the last 6 months"*
# MAGIC 8. *"Which stores have the highest revenue?"*
# MAGIC
# MAGIC ### Advanced Questions:
# MAGIC 9. *"What percentage of revenue comes from each loyalty tier?"*
# MAGIC 10. *"Show me year-over-year sales growth by category"*
# MAGIC 11. *"Which products have the highest profit margin?"*
# MAGIC 12. *"Identify customers with lifetime value over $10,000"*
# MAGIC
# MAGIC **💡 Tips for Better Results:**
# MAGIC - Be specific about time periods
# MAGIC - Use proper field names when known
# MAGIC - Ask follow-up questions to refine results
# MAGIC - Save useful queries for future reference

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9.5: Refine and Save Insights
# MAGIC
# MAGIC Work with Genie results:
# MAGIC
# MAGIC **Refining Results:**
# MAGIC 1. Click **Edit** to see the generated SQL
# MAGIC 2. Click **Regenerate** if results aren't quite right
# MAGIC 3. Adjust visualization type (table, bar chart, line chart, etc.)
# MAGIC 4. Add filters or sorting
# MAGIC
# MAGIC **Saving Insights:**
# MAGIC 1. Click **Save** on a useful query
# MAGIC 2. Give it a descriptive name
# MAGIC 3. Add to a collection or folder
# MAGIC 4. Share with colleagues
# MAGIC
# MAGIC **Creating Dashboards from Genie:**
# MAGIC 1. Select multiple saved queries
# MAGIC 2. Click **Create dashboard**
# MAGIC 3. Genie converts them to dashboard widgets
# MAGIC 4. Customize layout and styling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9.6: Genie Best Practices
# MAGIC
# MAGIC **For Data Creators (You):**
# MAGIC - ✅ Use clear, descriptive table and column names
# MAGIC - ✅ Add comments and descriptions to tables
# MAGIC - ✅ Provide sample questions in Genie space instructions
# MAGIC - ✅ Test common questions before sharing
# MAGIC - ✅ Set up proper permissions on underlying tables
# MAGIC
# MAGIC **For Genie Users:**
# MAGIC - ✅ Start with simple questions
# MAGIC - ✅ Review generated SQL to learn
# MAGIC - ✅ Use specific time ranges
# MAGIC - ✅ Iterate on questions for better results
# MAGIC - ✅ Save and organize useful queries
# MAGIC
# MAGIC **Common Pitfalls to Avoid:**
# MAGIC - ❌ Overly complex questions in one query
# MAGIC - ❌ Ambiguous terminology
# MAGIC - ❌ Assuming field names without checking
# MAGIC - ❌ Forgetting to specify time periods

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9.7: Share Your Genie Space
# MAGIC
# MAGIC Make your Genie space available to others:
# MAGIC
# MAGIC 1. Open your Genie space
# MAGIC 2. Click **Share**
# MAGIC 3. Add users or groups
# MAGIC 4. Set permissions:
# MAGIC    - **Can view**: See saved queries only
# MAGIC    - **Can use**: Ask own questions
# MAGIC    - **Can manage**: Edit space settings
# MAGIC 5. Click **Share**
# MAGIC
# MAGIC **💡 Business Impact:**
# MAGIC - Empowers non-technical users to explore data
# MAGIC - Reduces burden on data teams
# MAGIC - Accelerates insights and decision-making
# MAGIC - Democratizes data access across organization

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 10: Cleanup (Optional)
# MAGIC
# MAGIC If you'd like to clean up the workshop resources, run the cells below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Warning
# MAGIC
# MAGIC The following cells will **permanently delete** the workshop schema and all data. Only run these if you're sure you want to remove all workshop resources.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment and run to drop all workshop tables
# MAGIC -- DROP SCHEMA IF EXISTS ${catalog_name}.${schema_name} CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Congratulations! 🎉
# MAGIC
# MAGIC You've completed the Databricks AI/BI Dashboards Workshop!
# MAGIC
# MAGIC ## What You've Learned
# MAGIC
# MAGIC ✅ Created and populated sample datasets  
# MAGIC ✅ Built a multi-page interactive dashboard  
# MAGIC ✅ Created various visualizations (counters, charts, maps, Sankey diagrams)  
# MAGIC ✅ Added interactive filters and parameters  
# MAGIC ✅ Applied custom themes and styling  
# MAGIC ✅ Published and shared dashboards  
# MAGIC ✅ Explored data with Databricks One and Genie  
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC **Enhance Your Dashboard:**
# MAGIC - Add more complex SQL queries
# MAGIC - Create calculated fields and metrics
# MAGIC - Build drill-down capabilities
# MAGIC - Add more advanced visualizations
# MAGIC
# MAGIC **Use Your Own Data:**
# MAGIC - Connect to your organization's data
# MAGIC - Build dashboards for your use cases
# MAGIC - Share with your team
# MAGIC
# MAGIC **Learn More:**
# MAGIC - [Databricks AI/BI Documentation](https://docs.databricks.com/dashboards/)
# MAGIC - [Genie Documentation](https://docs.databricks.com/genie/)
# MAGIC - [Dashboard Best Practices](https://docs.databricks.com/dashboards/best-practices.html)
# MAGIC
# MAGIC ## Resources
# MAGIC
# MAGIC - Workshop GitHub Repository: [Link to repo]
# MAGIC - Databricks Community Forum
# MAGIC - Databricks Academy Courses
# MAGIC
# MAGIC ## Feedback
# MAGIC
# MAGIC We'd love to hear your feedback on this workshop! Please share your thoughts and suggestions.
# MAGIC
# MAGIC **Thank you for participating!** 🚀

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Quick Reference - SQL Queries for Dashboard
# MAGIC
# MAGIC Here are some useful queries you can use in your dashboards:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 1: Total Revenue and Key Metrics
# MAGIC SELECT 
# MAGIC   SUM(total_amount) as total_revenue,
# MAGIC   COUNT(DISTINCT transaction_id) as total_transactions,
# MAGIC   AVG(total_amount) as avg_order_value,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 2: Revenue by Region with Growth
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   COUNT(DISTINCT transaction_id) as transactions,
# MAGIC   AVG(total_amount) as avg_order_value
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY region
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 3: Monthly Sales Trend
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', transaction_date) as month,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   COUNT(DISTINCT customer_id) as customers,
# MAGIC   COUNT(DISTINCT transaction_id) as transactions
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY DATE_TRUNC('month', transaction_date)
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 4: Product Category Performance
# MAGIC SELECT 
# MAGIC   product_category,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   SUM(quantity) as units_sold,
# MAGIC   COUNT(DISTINCT transaction_id) as transactions,
# MAGIC   ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) as revenue_pct
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY product_category
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 5: Top 10 Products
# MAGIC SELECT 
# MAGIC   product_name,
# MAGIC   product_category,
# MAGIC   SUM(quantity) as units_sold,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY product_name, product_category
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 6: Customer Segment Analysis
# MAGIC SELECT 
# MAGIC   c.customer_segment,
# MAGIC   c.loyalty_tier,
# MAGIC   COUNT(DISTINCT c.customer_id) as customer_count,
# MAGIC   AVG(c.lifetime_value) as avg_lifetime_value,
# MAGIC   SUM(s.total_amount) as total_revenue
# MAGIC FROM ${catalog_name}.${schema_name}.customers c
# MAGIC LEFT JOIN ${catalog_name}.${schema_name}.sales_transactions s 
# MAGIC   ON c.customer_id = s.customer_id
# MAGIC GROUP BY c.customer_segment, c.loyalty_tier
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 7: Sales Channel Performance
# MAGIC SELECT 
# MAGIC   sales_channel,
# MAGIC   COUNT(DISTINCT transaction_id) as transactions,
# MAGIC   SUM(total_amount) as revenue,
# MAGIC   AVG(total_amount) as avg_order_value,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers
# MAGIC FROM ${catalog_name}.${schema_name}.sales_transactions
# MAGIC GROUP BY sales_channel
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query 8: Store Performance
# MAGIC SELECT 
# MAGIC   sl.store_name,
# MAGIC   sl.store_type,
# MAGIC   sl.city,
# MAGIC   sl.state,
# MAGIC   sl.region,
# MAGIC   COUNT(DISTINCT s.transaction_id) as transactions,
# MAGIC   SUM(s.total_amount) as revenue,
# MAGIC   COUNT(DISTINCT s.customer_id) as unique_customers
# MAGIC FROM ${catalog_name}.${schema_name}.store_locations sl
# MAGIC LEFT JOIN ${catalog_name}.${schema_name}.sales_transactions s 
# MAGIC   ON sl.store_id = s.store_id
# MAGIC GROUP BY sl.store_name, sl.store_type, sl.city, sl.state, sl.region
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **End of Workshop**
# MAGIC
