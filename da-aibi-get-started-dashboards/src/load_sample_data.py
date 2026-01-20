# Databricks notebook source
# MAGIC %md
# MAGIC # Load Sample Data for AI/BI Dashboard Workshop
# MAGIC
# MAGIC This notebook generates realistic sample data for the workshop tables.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Tables must be created first using `setup_tables.sql`
# MAGIC - Update the catalog and schema variables below

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set your catalog and schema names here.

# COMMAND ----------

# Update these variables with your catalog and schema
catalog_name = "main"  # Change this to your catalog
schema_name = "aibi_workshop"  # Change this to your schema

# Create full table names
sales_table = f"{catalog_name}.{schema_name}.sales_transactions"
customers_table = f"{catalog_name}.{schema_name}.customers"
products_table = f"{catalog_name}.{schema_name}.products"
stores_table = f"{catalog_name}.{schema_name}.store_locations"
geography_table = f"{catalog_name}.{schema_name}.geography"
journey_table = f"{catalog_name}.{schema_name}.customer_journey"

print(f"Will load data into schema: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Product Data
# MAGIC Create a catalog of products across different categories.

# COMMAND ----------

# Product data
products_data = [
    ("P001", "Laptop Pro 15", "Electronics", "Computers", "TechBrand", 1299.99, 800.00, 38.46, 45, "Global Tech Supply"),
    ("P002", "Wireless Mouse", "Electronics", "Accessories", "TechBrand", 29.99, 12.00, 59.99, 250, "Global Tech Supply"),
    ("P003", "USB-C Cable", "Electronics", "Accessories", "TechBrand", 19.99, 5.00, 74.99, 500, "Cable Co"),
    ("P004", "Office Chair Deluxe", "Furniture", "Seating", "ComfortPlus", 299.99, 150.00, 50.00, 80, "Furniture Depot"),
    ("P005", "Standing Desk", "Furniture", "Desks", "ComfortPlus", 599.99, 300.00, 50.00, 35, "Furniture Depot"),
    ("P006", "LED Desk Lamp", "Furniture", "Lighting", "BrightLight", 49.99, 20.00, 59.98, 120, "Lighting Inc"),
    ("P007", "Notebook Set", "Office Supplies", "Paper", "WriteWell", 12.99, 4.00, 69.21, 400, "Paper Plus"),
    ("P008", "Gel Pen Pack", "Office Supplies", "Writing", "WriteWell", 8.99, 2.50, 72.19, 600, "Paper Plus"),
    ("P009", "Wireless Keyboard", "Electronics", "Accessories", "TechBrand", 79.99, 35.00, 56.25, 180, "Global Tech Supply"),
    ("P010", "Monitor 27 inch", "Electronics", "Displays", "TechBrand", 349.99, 200.00, 42.86, 60, "Global Tech Supply"),
    ("P011", "Webcam HD", "Electronics", "Accessories", "TechBrand", 89.99, 40.00, 55.55, 95, "Global Tech Supply"),
    ("P012", "Headphones Noise-Cancel", "Electronics", "Audio", "SoundMax", 199.99, 90.00, 55.00, 110, "Audio Suppliers"),
    ("P013", "Desk Organizer", "Office Supplies", "Organization", "OrganizePro", 24.99, 10.00, 59.98, 200, "Office Goods"),
    ("P014", "Whiteboard Small", "Office Supplies", "Presentation", "WriteWell", 39.99, 18.00, 54.99, 75, "Office Goods"),
    ("P015", "Ergonomic Mouse Pad", "Office Supplies", "Accessories", "ComfortPlus", 15.99, 5.00, 68.73, 300, "Office Goods"),
    ("P016", "Laptop Stand", "Electronics", "Accessories", "TechBrand", 44.99, 18.00, 59.99, 140, "Global Tech Supply"),
    ("P017", "Filing Cabinet", "Furniture", "Storage", "ComfortPlus", 179.99, 90.00, 50.00, 40, "Furniture Depot"),
    ("P018", "Bookshelf 5-Tier", "Furniture", "Storage", "ComfortPlus", 129.99, 60.00, 53.85, 55, "Furniture Depot"),
    ("P019", "Paper Shredder", "Office Supplies", "Equipment", "SecureDoc", 99.99, 50.00, 50.00, 45, "Office Equipment Co"),
    ("P020", "Label Maker", "Office Supplies", "Equipment", "OrganizePro", 34.99, 15.00, 57.13, 85, "Office Equipment Co"),
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

products_df = spark.createDataFrame(products_data, products_schema)
products_df.write.mode("overwrite").saveAsTable(products_table)
print(f"✓ Loaded {products_df.count()} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Store Location Data
# MAGIC Create store locations across different regions.

# COMMAND ----------

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
    StructField("latitude", DecimalType(10,6), True),
    StructField("longitude", DecimalType(10,6), True),
    StructField("opening_date", StringType(), True),
    StructField("square_footage", IntegerType(), True),
    StructField("manager_name", StringType(), True),
])

stores_df = spark.createDataFrame(stores_data, stores_schema)
stores_df = stores_df.withColumn("opening_date", to_date(col("opening_date")))
stores_df.write.mode("overwrite").saveAsTable(stores_table)
print(f"✓ Loaded {stores_df.count()} stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Geography Data
# MAGIC Load zip code data from the CSV file.

# COMMAND ----------

# Load the zip code CSV
geography_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"/Workspace{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 2)[0]}/zip_code_csv/demo_zip_codes.csv")

# Add region mapping based on state
region_mapping = {
    "NY": "Northeast", "MA": "Northeast", "PA": "Northeast", "NJ": "Northeast", "CT": "Northeast",
    "IL": "Midwest", "OH": "Midwest", "MI": "Midwest", "WI": "Midwest", "MN": "Midwest", "IN": "Midwest", "MO": "Midwest",
    "CA": "West", "WA": "West", "OR": "West", "CO": "West", "AZ": "West", "NM": "West",
    "TX": "South", "FL": "Southeast", "GA": "Southeast", "NC": "Southeast", "TN": "Southeast", "LA": "Southeast",
    "MD": "Northeast", "DC": "Northeast", "KY": "Southeast", "OK": "South"
}

# Create a UDF to map state to region
def get_region(state):
    return region_mapping.get(state, "Other")

get_region_udf = udf(get_region, StringType())
geography_df = geography_df.withColumn("region", get_region_udf(col("state")))

geography_df.write.mode("overwrite").saveAsTable(geography_table)
print(f"✓ Loaded {geography_df.count()} zip codes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer Data
# MAGIC Create customer records with demographics and segments.

# COMMAND ----------

# Generate 500 customers
num_customers = 500

# Sample data pools
first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Barbara",
               "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
               "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
               "Donald", "Ashley", "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle"]

last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
              "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"]

segments = ["Enterprise", "SMB", "Individual"]
loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]

# Get zip codes for customers
geo_zips = geography_df.select("zip_code", "city", "state", "region").collect()

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
    
    # Random signup date in the last 3 years
    days_ago = random.randint(0, 1095)
    signup_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    lifetime_value = round(random.uniform(100, 50000), 2)
    
    # Random zip code
    geo_info = random.choice(geo_zips)
    
    customers_list.append((
        customer_id, customer_name, email, phone, segment, loyalty,
        signup_date, lifetime_value, geo_info.zip_code, geo_info.city, geo_info.state, geo_info.region
    ))

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("lifetime_value", DecimalType(10,2), True),
    StructField("zip_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
])

customers_df = spark.createDataFrame(customers_list, customers_schema)
customers_df = customers_df.withColumn("signup_date", to_date(col("signup_date")))
customers_df.write.mode("overwrite").saveAsTable(customers_table)
print(f"✓ Loaded {customers_df.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sales Transaction Data
# MAGIC Create sales transactions across time, customers, products, and stores.

# COMMAND ----------

# Generate 5000 transactions over the last year
num_transactions = 5000

# Get reference data
products_list = products_df.select("product_id", "product_name", "product_category", "unit_price").collect()
customers_list_ids = customers_df.select("customer_id").collect()
stores_list = stores_df.select("store_id", "store_name", "zip_code", "city", "state", "region").collect()

payment_methods = ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "PayPal"]
channels = ["In-Store", "Online", "Mobile App", "Phone Order"]

transactions_list = []
for i in range(1, num_transactions + 1):
    transaction_id = f"T{i:06d}"
    
    # Random date in the last 365 days
    days_ago = random.randint(0, 365)
    transaction_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    # Random customer, product, store
    customer = random.choice(customers_list_ids)
    product = random.choice(products_list)
    store = random.choice(stores_list)
    
    # Random quantity and calculate total
    quantity = random.randint(1, 5)
    unit_price = float(product.unit_price)
    total_amount = round(quantity * unit_price, 2)
    
    payment = random.choice(payment_methods)
    channel = random.choice(channels)
    
    transactions_list.append((
        transaction_id, transaction_date, customer.customer_id,
        product.product_id, product.product_name, product.product_category,
        quantity, unit_price, total_amount, payment,
        store.store_id, store.store_name, store.zip_code, store.city, store.state, store.region,
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
transactions_df = transactions_df.withColumn("transaction_date", to_date(col("transaction_date")))
transactions_df.write.mode("overwrite").saveAsTable(sales_table)
print(f"✓ Loaded {transactions_df.count()} transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customer Journey Data
# MAGIC Create customer interaction touchpoint data for Sankey diagrams.

# COMMAND ----------

# Generate customer journey data
touchpoints = ["Social Media", "Email", "Website Visit", "Store Visit", "Customer Service", "Mobile App"]
channels = ["Digital", "Physical", "Phone", "Social"]
outcomes = ["Purchase", "Inquiry", "Support", "Browse"]

num_journeys = 2000
journeys_list = []

for i in range(1, num_journeys + 1):
    journey_id = f"J{i:05d}"
    customer = random.choice(customers_list_ids)
    
    days_ago = random.randint(0, 180)
    interaction_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    touchpoint = random.choice(touchpoints)
    channel = random.choice(channels)
    outcome = random.choice(outcomes)
    
    # Value is higher for purchases
    if outcome == "Purchase":
        value = round(random.uniform(50, 2000), 2)
    else:
        value = 0.0
    
    journeys_list.append((
        journey_id, customer.customer_id, interaction_date,
        touchpoint, channel, outcome, value
    ))

journeys_schema = StructType([
    StructField("journey_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("interaction_date", StringType(), True),
    StructField("touchpoint", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("value", DecimalType(10,2), True),
])

journeys_df = spark.createDataFrame(journeys_list, journeys_schema)
journeys_df = journeys_df.withColumn("interaction_date", to_date(col("interaction_date")))
journeys_df.write.mode("overwrite").saveAsTable(journey_table)
print(f"✓ Loaded {journeys_df.count()} customer journey records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Load
# MAGIC Display record counts for all tables.

# COMMAND ----------

print("=" * 60)
print("DATA LOAD SUMMARY")
print("=" * 60)
print(f"Products:              {spark.table(products_table).count():,} records")
print(f"Stores:                {spark.table(stores_table).count():,} records")
print(f"Geography:             {spark.table(geography_table).count():,} records")
print(f"Customers:             {spark.table(customers_table).count():,} records")
print(f"Sales Transactions:    {spark.table(sales_table).count():,} records")
print(f"Customer Journeys:     {spark.table(journey_table).count():,} records")
print("=" * 60)
print("✓ All data loaded successfully!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview
# MAGIC View a sample of the loaded data.

# COMMAND ----------

print("Sample Sales Transactions:")
spark.table(sales_table).orderBy(desc("transaction_date")).limit(10).display()

# COMMAND ----------

print("Sample Customers:")
spark.table(customers_table).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Data loading is complete! You can now:
# MAGIC 1. Return to the main workshop notebook
# MAGIC 2. Start building your AI/BI Dashboard
# MAGIC 3. Create visualizations using this data
# MAGIC
