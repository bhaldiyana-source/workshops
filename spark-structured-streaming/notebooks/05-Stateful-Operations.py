# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05: Stateful Operations
# MAGIC
# MAGIC ## Overview
# MAGIC Learn stateful operations: aggregations, joins, and deduplication that maintain state across records.
# MAGIC
# MAGIC **Duration**: 50 minutes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Streaming Aggregations

# COMMAND ----------

# Create streaming data
orders_stream = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("order_id", col("value"))
    .withColumn("category", when(col("value") % 3 == 0, "Electronics")
                            .when(col("value") % 3 == 1, "Clothing")
                            .otherwise("Books"))
    .withColumn("amount", (col("value") % 100 + 10).cast("double"))
    .withColumn("region", when(col("value") % 2 == 0, "US").otherwise("EU")))

# Streaming aggregation
agg_query = (orders_stream
    .groupBy("category", "region")
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        max("amount").alias("max_order")
    ))

display(agg_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Stream-to-Stream Joins

# COMMAND ----------

# Create two streaming sources
stream1 = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("order_id", col("value"))
    .withColumn("order_time", col("timestamp"))
    .withColumn("amount", (col("value") % 100 + 50).cast("double")))

stream2 = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 3)
    .load()
    .withColumn("shipment_id", col("value") + 1000)
    .withColumn("order_id", col("value"))
    .withColumn("ship_time", col("timestamp")))

# Join with watermarks
joined = (stream1
    .withWatermark("order_time", "10 minutes")
    .join(
        stream2.withWatermark("ship_time", "15 minutes"),
        expr("""
            order_id = order_id AND
            ship_time >= order_time AND
            ship_time <= order_time + interval 1 hour
        """),
        "inner"
    ))

print("✅ Stream-to-stream join configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Stream-to-Static Joins

# COMMAND ----------

# Create static dimension table
product_catalog = spark.createDataFrame([
    (1, "Laptop", "Electronics"),
    (2, "Shirt", "Clothing"),
    (3, "Novel", "Books")
], ["product_id", "product_name", "category"])

# Stream with static join
enriched = (orders_stream
    .withColumn("product_id", (col("order_id") % 3 + 1).cast("int"))
    .join(product_catalog, "product_id", "left")
    .select("order_id", "product_name", "amount", "region"))

display(enriched)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplication

# COMMAND ----------

# Deduplication with watermark
deduped = (orders_stream
    .withColumn("event_id", col("order_id"))
    .withWatermark("timestamp", "1 hour")
    .dropDuplicates(["event_id"]))

print("✅ Deduplication configured with 1-hour watermark")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. State Management Considerations

# COMMAND ----------

# Example: Managing state size
managed_state = (orders_stream
    .withWatermark("timestamp", "2 hours")  # Limit state retention
    .groupBy("category")
    .agg(
        approx_count_distinct("order_id").alias("unique_orders"),  # Approx for efficiency
        sum("amount").alias("total_revenue")
    ))

print("State management best practices:")
print("- Use watermarks to limit state growth")
print("- Use approximate aggregations when appropriate")
print("- Monitor state store size")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Streaming aggregations with maintained state
# MAGIC ✅ Stream-to-stream joins with watermarks
# MAGIC ✅ Stream-to-static joins for enrichment
# MAGIC ✅ Deduplication strategies
# MAGIC ✅ State management best practices
# MAGIC
# MAGIC **Next**: Notebook 06 - Writing to Sinks

# COMMAND ----------

for query in spark.streams.active:
    query.stop()
