# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03: Stateless Transformations
# MAGIC
# MAGIC ## Overview
# MAGIC Learn to apply stateless transformations to streaming DataFrames. These operations process each record independently without maintaining state.
# MAGIC
# MAGIC **Duration**: 40 minutes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Basic Transformations

# COMMAND ----------

# Create streaming source
streaming_df = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load())

# Filter transformation
filtered = streaming_df.filter(col("value") > 500)

# Select transformation
selected = streaming_df.select(
    col("timestamp"),
    col("value"),
    (col("value") * 2).alias("doubled")
)

# Multiple transformations
transformed = (streaming_df
    .filter(col("value") > 100)
    .withColumn("value_squared", col("value") * col("value"))
    .withColumn("category", 
        when(col("value") < 300, "low")
        .when(col("value") < 700, "medium")
        .otherwise("high")))

display(transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. JSON Parsing from Kafka

# COMMAND ----------

# Simulate Kafka messages with JSON
kafka_sim = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .select(to_json(struct(
        (col("value") % 10 + 1).alias("sensor_id"),
        ((col("value") % 30) + 60).cast("double").alias("temperature"),
        col("timestamp").cast("string").alias("event_time")
    )).alias("value")))

# Define schema for JSON
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("event_time", StringType())
])

# Parse JSON
parsed = (kafka_sim
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("temp_fahrenheit", col("temperature") * 9/5 + 32))

display(parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Working with Complex Types

# COMMAND ----------

# Create data with nested structures
complex_stream = (spark.readStream
    .format("rate")
    .load()
    .select(
        col("value"),
        array(col("value"), col("value") + 1, col("value") + 2).alias("readings"),
        struct(
            col("value").alias("id"),
            col("timestamp").alias("ts")
        ).alias("metadata")
    ))

# Explode arrays
exploded = (complex_stream
    .select(col("value"), explode(col("readings")).alias("reading")))

# Access nested fields
nested_access = (complex_stream
    .select(
        col("value"),
        col("metadata.id").alias("meta_id"),
        col("metadata.ts").alias("meta_ts")
    ))

display(nested_access)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Python UDFs on Streams

# COMMAND ----------

# Define Python UDF
@udf(returnType=StringType())
def classify_temperature(temp):
    if temp < 65: return "cold"
    elif temp < 75: return "comfortable"
    else: return "hot"

# Apply UDF
with_udf = (streaming_df
    .withColumn("temp_celsius", (col("value") % 50 + 10).cast("double"))
    .withColumn("classification", classify_temperature(col("temp_celsius"))))

display(with_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Basic transformations (filter, select, withColumn)
# MAGIC ✅ JSON parsing from Kafka
# MAGIC ✅ Complex types (arrays, structs)
# MAGIC ✅ Python UDFs on streaming data
# MAGIC
# MAGIC **Next**: Notebook 04 - Windowing and Watermarks

# COMMAND ----------

for query in spark.streams.active:
    query.stop()
