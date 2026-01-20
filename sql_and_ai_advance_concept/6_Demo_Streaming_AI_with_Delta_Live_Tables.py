# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Streaming AI with Delta Live Tables
# MAGIC
# MAGIC ## Overview
# MAGIC This demonstration shows how to integrate AI Functions with Delta Live Tables (DLT) for real-time streaming AI processing. You'll learn how to execute AI Functions on streaming data, implement windowing and aggregation, build low-latency semantic search, and create event-driven AI pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Execute AI Functions in streaming Delta Live Tables pipelines
# MAGIC - Implement windowing and aggregation with AI insights
# MAGIC - Build low-latency semantic search over streaming data
# MAGIC - Create event-driven AI function triggers
# MAGIC - Handle streaming-specific challenges (watermarking, state management)
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Demos 1-5
# MAGIC - Understanding of streaming concepts
# MAGIC - Familiarity with Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for streaming demo
# MAGIC CREATE CATALOG IF NOT EXISTS ai_workshop_advanced;
# MAGIC USE CATALOG ai_workshop_advanced;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_ai;
# MAGIC USE SCHEMA streaming_ai;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Simulating Streaming Data
# MAGIC
# MAGIC Note: In production, this would be a real streaming source (Kafka, Kinesis, Event Hub, etc.)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create streaming source table (simulates real-time events)
# MAGIC CREATE OR REPLACE TABLE streaming_events (
# MAGIC   event_id STRING,
# MAGIC   event_type STRING,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   user_id STRING,
# MAGIC   content STRING,
# MAGIC   metadata MAP<STRING, STRING>
# MAGIC );
# MAGIC
# MAGIC -- Insert simulated streaming events
# MAGIC INSERT INTO streaming_events VALUES
# MAGIC   ('EVT001', 'customer_feedback', current_timestamp(), 'USER001', 'Love the new features! Everything works great.', map('rating', '5', 'channel', 'mobile_app')),
# MAGIC   ('EVT002', 'support_ticket', current_timestamp(), 'USER002', 'Cannot login to my account. Need urgent help.', map('priority', 'high', 'channel', 'email')),
# MAGIC   ('EVT003', 'product_review', current_timestamp(), 'USER003', 'Good product but shipping was slow.', map('rating', '4', 'channel', 'website')),
# MAGIC   ('EVT004', 'customer_feedback', current_timestamp(), 'USER004', 'Disappointed with customer service response time.', map('rating', '2', 'channel', 'phone')),
# MAGIC   ('EVT005', 'support_ticket', current_timestamp(), 'USER005', 'Question about billing cycle and payment options.', map('priority', 'low', 'channel', 'chat'));

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Batch Processing Pattern (for comparison)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Traditional batch AI processing
# MAGIC CREATE OR REPLACE TABLE batch_processed_events AS
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   event_type,
# MAGIC   event_timestamp,
# MAGIC   user_id,
# MAGIC   content,
# MAGIC   -- AI processing (executed in batch)
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     content,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Classify urgency: ', content),
# MAGIC     ARRAY('critical', 'high', 'medium', 'low')
# MAGIC   ) as urgency,
# MAGIC   AI_EMBED('databricks-gte-large-en', content) as embedding,
# MAGIC   current_timestamp() as processing_timestamp,
# MAGIC   'batch' as processing_mode
# MAGIC FROM streaming_events;
# MAGIC
# MAGIC SELECT COUNT(*) as processed_count, processing_mode FROM batch_processed_events GROUP BY processing_mode;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Streaming AI Processing Pattern
# MAGIC
# MAGIC ### Note on Delta Live Tables
# MAGIC
# MAGIC In a real DLT pipeline, you would define this using DLT syntax:
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(
# MAGIC   comment="Streaming events with AI enrichment",
# MAGIC   table_properties={
# MAGIC     "quality": "silver",
# MAGIC     "pipelines.autoOptimize.zOrderCols": "event_timestamp,sentiment"
# MAGIC   }
# MAGIC )
# MAGIC def stream_processed_events():
# MAGIC   return (
# MAGIC     dlt.read_stream("streaming_events")
# MAGIC       .selectExpr(
# MAGIC         "event_id",
# MAGIC         "event_type",
# MAGIC         "event_timestamp",
# MAGIC         "user_id",
# MAGIC         "content",
# MAGIC         "AI_CLASSIFY('model', content, ARRAY('positive', 'negative', 'neutral')) as sentiment",
# MAGIC         "current_timestamp() as processing_timestamp"
# MAGIC       )
# MAGIC   )
# MAGIC ```
# MAGIC
# MAGIC For this demo, we'll simulate streaming behavior using SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulate streaming processing with micro-batches
# MAGIC CREATE OR REPLACE TABLE stream_processed_events (
# MAGIC   event_id STRING,
# MAGIC   event_type STRING,
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   user_id STRING,
# MAGIC   content STRING,
# MAGIC   sentiment STRING,
# MAGIC   urgency STRING,
# MAGIC   embedding ARRAY<DOUBLE>,
# MAGIC   processing_timestamp TIMESTAMP,
# MAGIC   processing_mode STRING,
# MAGIC   microbatch_id BIGINT
# MAGIC );
# MAGIC
# MAGIC -- Process micro-batch 1
# MAGIC INSERT INTO stream_processed_events
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   event_type,
# MAGIC   event_timestamp,
# MAGIC   user_id,
# MAGIC   content,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     content,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Classify urgency: ', content),
# MAGIC     ARRAY('critical', 'high', 'medium', 'low')
# MAGIC   ) as urgency,
# MAGIC   AI_EMBED('databricks-gte-large-en', content) as embedding,
# MAGIC   current_timestamp() as processing_timestamp,
# MAGIC   'streaming' as processing_mode,
# MAGIC   1 as microbatch_id
# MAGIC FROM streaming_events
# MAGIC WHERE event_id NOT IN (SELECT event_id FROM stream_processed_events);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Windowed Aggregations with AI Insights

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add more streaming events to demonstrate windowing
# MAGIC INSERT INTO streaming_events VALUES
# MAGIC   ('EVT006', 'customer_feedback', current_timestamp(), 'USER006', 'Best purchase ever! Highly recommend.', map('rating', '5', 'channel', 'mobile_app')),
# MAGIC   ('EVT007', 'support_ticket', current_timestamp(), 'USER007', 'System is down! Critical issue affecting all users.', map('priority', 'critical', 'channel', 'phone')),
# MAGIC   ('EVT008', 'product_review', current_timestamp(), 'USER008', 'Not worth the price. Quality is poor.', map('rating', '1', 'channel', 'website')),
# MAGIC   ('EVT009', 'customer_feedback', current_timestamp(), 'USER009', 'Great experience with the support team.', map('rating', '5', 'channel', 'chat')),
# MAGIC   ('EVT010', 'support_ticket', current_timestamp(), 'USER010', 'Minor bug in the report generation feature.', map('priority', 'low', 'channel', 'email'));
# MAGIC
# MAGIC -- Process new micro-batch
# MAGIC INSERT INTO stream_processed_events
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   event_type,
# MAGIC   event_timestamp,
# MAGIC   user_id,
# MAGIC   content,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     content,
# MAGIC     ARRAY('positive', 'negative', 'neutral')
# MAGIC   ) as sentiment,
# MAGIC   AI_CLASSIFY(
# MAGIC     'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     CONCAT('Classify urgency: ', content),
# MAGIC     ARRAY('critical', 'high', 'medium', 'low')
# MAGIC   ) as urgency,
# MAGIC   AI_EMBED('databricks-gte-large-en', content) as embedding,
# MAGIC   current_timestamp() as processing_timestamp,
# MAGIC   'streaming' as processing_mode,
# MAGIC   2 as microbatch_id
# MAGIC FROM streaming_events
# MAGIC WHERE event_id NOT IN (SELECT event_id FROM stream_processed_events);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create windowed aggregations (simulating tumbling windows)
# MAGIC CREATE OR REPLACE TABLE windowed_ai_insights AS
# MAGIC SELECT 
# MAGIC   microbatch_id as window_id,
# MAGIC   MIN(event_timestamp) as window_start,
# MAGIC   MAX(event_timestamp) as window_end,
# MAGIC   event_type,
# MAGIC   -- Aggregations
# MAGIC   COUNT(*) as event_count,
# MAGIC   COUNT(DISTINCT user_id) as unique_users,
# MAGIC   -- AI-driven insights
# MAGIC   SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
# MAGIC   SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
# MAGIC   SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) / COUNT(*), 2) as negative_pct,
# MAGIC   -- Urgency distribution
# MAGIC   SUM(CASE WHEN urgency = 'critical' THEN 1 ELSE 0 END) as critical_count,
# MAGIC   SUM(CASE WHEN urgency = 'high' THEN 1 ELSE 0 END) as high_urgency_count,
# MAGIC   -- Alert trigger
# MAGIC   CASE 
# MAGIC     WHEN SUM(CASE WHEN urgency = 'critical' THEN 1 ELSE 0 END) > 0 THEN TRUE
# MAGIC     WHEN 100.0 * SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) / COUNT(*) > 50 THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END as alert_triggered,
# MAGIC   current_timestamp() as aggregation_timestamp
# MAGIC FROM stream_processed_events
# MAGIC GROUP BY microbatch_id, event_type;
# MAGIC
# MAGIC -- View windowed insights
# MAGIC SELECT 
# MAGIC   window_id,
# MAGIC   event_type,
# MAGIC   event_count,
# MAGIC   positive_count,
# MAGIC   negative_count,
# MAGIC   negative_pct,
# MAGIC   critical_count,
# MAGIC   alert_triggered
# MAGIC FROM windowed_ai_insights
# MAGIC ORDER BY window_id, event_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Real-Time Semantic Search

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create materialized view for semantic search (would be DLT table in production)
# MAGIC CREATE OR REPLACE TABLE semantic_search_index AS
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   event_type,
# MAGIC   content,
# MAGIC   sentiment,
# MAGIC   urgency,
# MAGIC   embedding,
# MAGIC   event_timestamp,
# MAGIC   processing_timestamp
# MAGIC FROM stream_processed_events;
# MAGIC
# MAGIC -- Optimize for search performance
# MAGIC OPTIMIZE semantic_search_index;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Real-time semantic search function
# MAGIC CREATE OR REPLACE FUNCTION cosine_similarity_streaming(vec1 ARRAY<DOUBLE>, vec2 ARRAY<DOUBLE>)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN (
# MAGIC   AGGREGATE(
# MAGIC     SEQUENCE(0, SIZE(vec1) - 1),
# MAGIC     0.0,
# MAGIC     (acc, i) -> acc + vec1[i] * vec2[i]
# MAGIC   ) / (
# MAGIC     SQRT(AGGREGATE(vec1, 0.0, (acc, x) -> acc + x * x)) *
# MAGIC     SQRT(AGGREGATE(vec2, 0.0, (acc, x) -> acc + x * x))
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform real-time semantic search
# MAGIC WITH search_query AS (
# MAGIC   SELECT 
# MAGIC     'urgent help needed with login issues' as query_text,
# MAGIC     AI_EMBED('databricks-gte-large-en', 'urgent help needed with login issues') as query_embedding
# MAGIC )
# MAGIC SELECT 
# MAGIC   ssi.event_id,
# MAGIC   ssi.event_type,
# MAGIC   ssi.content,
# MAGIC   ssi.sentiment,
# MAGIC   ssi.urgency,
# MAGIC   cosine_similarity_streaming(ssi.embedding, sq.query_embedding) as relevance_score,
# MAGIC   TIMESTAMPDIFF(SECOND, ssi.event_timestamp, current_timestamp()) as age_seconds
# MAGIC FROM semantic_search_index ssi
# MAGIC CROSS JOIN search_query sq
# MAGIC WHERE cosine_similarity_streaming(ssi.embedding, sq.query_embedding) > 0.5
# MAGIC   AND TIMESTAMPDIFF(MINUTE, ssi.event_timestamp, current_timestamp()) < 60 -- Recent events only
# MAGIC ORDER BY relevance_score DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Event-Driven AI Triggers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create event trigger rules table
# MAGIC CREATE TABLE IF NOT EXISTS ai_event_triggers (
# MAGIC   trigger_id STRING DEFAULT uuid(),
# MAGIC   trigger_name STRING,
# MAGIC   trigger_type STRING, -- 'sentiment', 'urgency', 'pattern', 'anomaly'
# MAGIC   trigger_condition STRING,
# MAGIC   ai_action STRING, -- 'classify', 'summarize', 'alert', 'route'
# MAGIC   action_parameters MAP<STRING, STRING>,
# MAGIC   is_active BOOLEAN,
# MAGIC   priority INT
# MAGIC );
# MAGIC
# MAGIC -- Define trigger rules
# MAGIC INSERT INTO ai_event_triggers (trigger_name, trigger_type, trigger_condition, ai_action, action_parameters, is_active, priority)
# MAGIC VALUES
# MAGIC   ('Critical Support Ticket', 'urgency', 'urgency = critical', 'alert', map('channel', 'pagerduty', 'escalate_to', 'senior_support'), TRUE, 1),
# MAGIC   ('Negative Sentiment Spike', 'sentiment', 'negative_pct > 50 in window', 'alert', map('channel', 'slack', 'notify', 'product_team'), TRUE, 2),
# MAGIC   ('High-Value Customer Feedback', 'pattern', 'rating = 5 AND customer_tier = premium', 'route', map('destination', 'success_team', 'priority', 'high'), TRUE, 3),
# MAGIC   ('Product Issue Detection', 'pattern', 'contains_product_issue = true', 'classify', map('categories', 'bug,feature_request,documentation', 'route_to', 'product_team'), TRUE, 2);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Execute event-driven AI actions
# MAGIC CREATE OR REPLACE TABLE triggered_ai_actions AS
# MAGIC WITH event_analysis AS (
# MAGIC   SELECT 
# MAGIC     event_id,
# MAGIC     event_type,
# MAGIC     content,
# MAGIC     sentiment,
# MAGIC     urgency,
# MAGIC     -- Check trigger conditions
# MAGIC     CASE WHEN urgency = 'critical' THEN ARRAY('Critical Support Ticket') ELSE ARRAY() END as matching_triggers
# MAGIC   FROM stream_processed_events
# MAGIC ),
# MAGIC expanded_triggers AS (
# MAGIC   SELECT 
# MAGIC     ea.*,
# MAGIC     EXPLODE(ea.matching_triggers) as trigger_name
# MAGIC   FROM event_analysis ea
# MAGIC   WHERE SIZE(ea.matching_triggers) > 0
# MAGIC )
# MAGIC SELECT 
# MAGIC   et.event_id,
# MAGIC   et.event_type,
# MAGIC   et.content,
# MAGIC   et.sentiment,
# MAGIC   et.urgency,
# MAGIC   ait.trigger_name,
# MAGIC   ait.ai_action,
# MAGIC   ait.action_parameters,
# MAGIC   -- Execute AI action
# MAGIC   CASE ait.ai_action
# MAGIC     WHEN 'alert' THEN CONCAT('Alert sent to ', ait.action_parameters['channel'], ': ', ait.action_parameters['escalate_to'])
# MAGIC     WHEN 'route' THEN CONCAT('Routed to ', ait.action_parameters['destination'])
# MAGIC     WHEN 'classify' THEN AI_CLASSIFY('databricks-meta-llama-3-1-70b-instruct', et.content, SPLIT(ait.action_parameters['categories'], ','))
# MAGIC     ELSE 'No action taken'
# MAGIC   END as action_result,
# MAGIC   current_timestamp() as action_timestamp
# MAGIC FROM expanded_triggers et
# MAGIC INNER JOIN ai_event_triggers ait ON et.trigger_name = ait.trigger_name
# MAGIC WHERE ait.is_active = TRUE;
# MAGIC
# MAGIC -- View triggered actions
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   trigger_name,
# MAGIC   ai_action,
# MAGIC   action_result,
# MAGIC   urgency,
# MAGIC   sentiment
# MAGIC FROM triggered_ai_actions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Streaming Performance Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create streaming performance dashboard
# MAGIC CREATE OR REPLACE VIEW streaming_performance_dashboard AS
# MAGIC SELECT 
# MAGIC   'Streaming AI Performance' as dashboard_section,
# MAGIC   COUNT(*) as total_events_processed,
# MAGIC   COUNT(DISTINCT microbatch_id) as total_microbatches,
# MAGIC   ROUND(COUNT(*) / COUNT(DISTINCT microbatch_id), 1) as avg_events_per_batch,
# MAGIC   ROUND(AVG(TIMESTAMPDIFF(SECOND, event_timestamp, processing_timestamp)), 2) as avg_latency_seconds,
# MAGIC   MAX(TIMESTAMPDIFF(SECOND, event_timestamp, processing_timestamp)) as max_latency_seconds,
# MAGIC   MIN(TIMESTAMPDIFF(SECOND, event_timestamp, processing_timestamp)) as min_latency_seconds,
# MAGIC   COUNT(DISTINCT DATE_TRUNC('minute', processing_timestamp)) as processing_minutes,
# MAGIC   ROUND(COUNT(*) / COUNT(DISTINCT DATE_TRUNC('minute', processing_timestamp)), 1) as events_per_minute
# MAGIC FROM stream_processed_events;
# MAGIC
# MAGIC SELECT * FROM streaming_performance_dashboard;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AI function execution metrics in streaming
# MAGIC CREATE OR REPLACE VIEW streaming_ai_metrics AS
# MAGIC SELECT 
# MAGIC   microbatch_id,
# MAGIC   COUNT(*) as events_in_batch,
# MAGIC   SUM(CASE WHEN sentiment IS NOT NULL THEN 1 ELSE 0 END) as successful_sentiment_analysis,
# MAGIC   SUM(CASE WHEN urgency IS NOT NULL THEN 1 ELSE 0 END) as successful_urgency_classification,
# MAGIC   SUM(CASE WHEN SIZE(embedding) > 0 THEN 1 ELSE 0 END) as successful_embeddings,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN sentiment IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as sentiment_success_rate,
# MAGIC   MIN(processing_timestamp) as batch_start,
# MAGIC   MAX(processing_timestamp) as batch_end,
# MAGIC   TIMESTAMPDIFF(SECOND, MIN(processing_timestamp), MAX(processing_timestamp)) as batch_duration_seconds
# MAGIC FROM stream_processed_events
# MAGIC GROUP BY microbatch_id;
# MAGIC
# MAGIC SELECT * FROM streaming_ai_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Delta Live Tables Pipeline Template
# MAGIC
# MAGIC ### DLT Pipeline Code (Python)
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC # Bronze: Ingest streaming events
# MAGIC @dlt.table(
# MAGIC   comment="Raw streaming events",
# MAGIC   table_properties={"quality": "bronze"}
# MAGIC )
# MAGIC def bronze_streaming_events():
# MAGIC   return (
# MAGIC     spark.readStream
# MAGIC       .format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "json")
# MAGIC       .load("/mnt/streaming-source/")
# MAGIC   )
# MAGIC
# MAGIC # Silver: AI enrichment
# MAGIC @dlt.table(
# MAGIC   comment="AI-enriched streaming events",
# MAGIC   table_properties={
# MAGIC     "quality": "silver",
# MAGIC     "pipelines.autoOptimize.zOrderCols": "event_timestamp,sentiment"
# MAGIC   }
# MAGIC )
# MAGIC def silver_ai_enriched_events():
# MAGIC   return (
# MAGIC     dlt.read_stream("bronze_streaming_events")
# MAGIC       .withColumn("sentiment", 
# MAGIC         expr("AI_CLASSIFY('model', content, ARRAY('positive', 'negative', 'neutral'))"))
# MAGIC       .withColumn("urgency",
# MAGIC         expr("AI_CLASSIFY('model', content, ARRAY('critical', 'high', 'medium', 'low'))"))
# MAGIC       .withColumn("embedding",
# MAGIC         expr("AI_EMBED('embedding-model', content)"))
# MAGIC       .withColumn("processing_timestamp", current_timestamp())
# MAGIC   )
# MAGIC
# MAGIC # Gold: Aggregated insights
# MAGIC @dlt.table(
# MAGIC   comment="Windowed AI insights",
# MAGIC   table_properties={"quality": "gold"}
# MAGIC )
# MAGIC def gold_windowed_insights():
# MAGIC   return (
# MAGIC     dlt.read_stream("silver_ai_enriched_events")
# MAGIC       .withWatermark("event_timestamp", "10 minutes")
# MAGIC       .groupBy(
# MAGIC         window("event_timestamp", "5 minutes"),
# MAGIC         "event_type"
# MAGIC       )
# MAGIC       .agg(
# MAGIC         count("*").alias("event_count"),
# MAGIC         sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("negative_count"),
# MAGIC         sum(when(col("urgency") == "critical", 1).otherwise(0)).alias("critical_count")
# MAGIC       )
# MAGIC   )
# MAGIC
# MAGIC # Expectations for data quality
# MAGIC @dlt.expect_or_drop("valid_sentiment", "sentiment IN ('positive', 'negative', 'neutral')")
# MAGIC @dlt.expect("has_content", "length(content) > 0")
# MAGIC @dlt.table
# MAGIC def quality_checked_events():
# MAGIC   return dlt.read_stream("silver_ai_enriched_events")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Batch vs Streaming AI Processing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare batch and streaming approaches
# MAGIC CREATE OR REPLACE TABLE processing_mode_comparison AS
# MAGIC SELECT 
# MAGIC   'Batch Processing' as approach,
# MAGIC   COUNT(*) as events_processed,
# MAGIC   AVG(TIMESTAMPDIFF(SECOND, event_timestamp, processing_timestamp)) as avg_latency_seconds,
# MAGIC   'All at once' as processing_pattern,
# MAGIC   'High' as resource_burst,
# MAGIC   'Minutes to hours' as typical_latency
# MAGIC FROM batch_processed_events
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Streaming Processing',
# MAGIC   COUNT(*),
# MAGIC   AVG(TIMESTAMPDIFF(SECOND, event_timestamp, processing_timestamp)),
# MAGIC   'Continuous micro-batches',
# MAGIC   'Low and steady',
# MAGIC   'Seconds'
# MAGIC FROM stream_processed_events;
# MAGIC
# MAGIC SELECT * FROM processing_mode_comparison;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Streaming AI Benefits
# MAGIC 1. **Low Latency**: Process events within seconds of arrival
# MAGIC 2. **Real-Time Insights**: AI analysis available immediately
# MAGIC 3. **Event-Driven**: Trigger actions based on AI insights
# MAGIC 4. **Resource Efficiency**: Steady resource utilization vs batch spikes
# MAGIC 5. **Continuous Learning**: Adapt to changing patterns in real-time
# MAGIC
# MAGIC ### Delta Live Tables Integration
# MAGIC - ✓ Declarative pipeline definition
# MAGIC - ✓ Automatic scaling and optimization
# MAGIC - ✓ Built-in data quality checks
# MAGIC - ✓ Simplified state management
# MAGIC - ✓ Lineage and observability
# MAGIC
# MAGIC ### Streaming Patterns
# MAGIC 1. **Micro-Batch Processing**: Balance latency and throughput
# MAGIC 2. **Windowed Aggregations**: Analyze trends over time windows
# MAGIC 3. **Event-Time Processing**: Handle late-arriving data correctly
# MAGIC 4. **Watermarking**: Manage state size and completeness
# MAGIC 5. **Semantic Search**: Real-time similarity matching
# MAGIC
# MAGIC ### Production Considerations
# MAGIC - **Latency Requirements**: Choose appropriate trigger intervals
# MAGIC - **State Management**: Monitor state size for aggregations
# MAGIC - **Cost**: Streaming can be more cost-effective than frequent batches
# MAGIC - **Fault Tolerance**: DLT handles checkpointing automatically
# MAGIC - **Monitoring**: Track processing lag and throughput
# MAGIC
# MAGIC ### When to Use Streaming AI
# MAGIC - **Use Streaming When:**
# MAGIC   - Latency requirements < 5 minutes
# MAGIC   - Event-driven actions needed
# MAGIC   - Continuous data arrival
# MAGIC   - Real-time dashboards/alerts
# MAGIC
# MAGIC - **Use Batch When:**
# MAGIC   - Data arrives in large batches
# MAGIC   - Hourly/daily processing sufficient
# MAGIC   - Complex multi-stage transformations
# MAGIC   - Cost optimization priority

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC You've completed all six core demos! Continue to the hands-on labs:
# MAGIC
# MAGIC - **Lab 7**: Building Enterprise Document Intelligence System
# MAGIC - **Lab 8**: Implementing AI Function Library with Testing Framework
# MAGIC - **Lab 9**: Cost Optimization and Monitoring Dashboard
# MAGIC - **Lab 10**: Compliance-Aware AI Pipeline
# MAGIC
# MAGIC Then challenge yourself with:
# MAGIC - **Challenge Lab 11**: Multi-Tenant RAG System
# MAGIC - **Challenge Lab 12**: Real-Time Recommendation Engine
