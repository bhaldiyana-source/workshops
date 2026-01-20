# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Monitoring and Managing Zerobus Ingestion Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC In this lab, you'll learn how to monitor, troubleshoot, and optimize Zerobus ingestion pipelines for production deployment. You'll query system tables for ingestion metrics, track performance trends, identify bottlenecks, set up alerts, and implement operational best practices.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Query Zerobus system tables for ingestion metrics and metadata
# MAGIC - Monitor throughput, latency, and error rates in real-time
# MAGIC - Identify and diagnose common ingestion failures
# MAGIC - Set up automated alerts for pipeline health issues
# MAGIC - Optimize ingestion performance through tuning and configuration
# MAGIC - Implement operational dashboards for production monitoring
# MAGIC - Track cost and resource utilization
# MAGIC - Troubleshoot authentication, schema, and network issues
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Labs 2-4
# MAGIC - Active Zerobus ingestion with historical data
# MAGIC - Access to system tables (admin privileges recommended)
# MAGIC
# MAGIC ## Duration
# MAGIC 30-35 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Understanding Zerobus System Tables
# MAGIC
# MAGIC Databricks provides system tables for monitoring Zerobus ingestion:
# MAGIC - `system.ingestion.zerobus_events`: Individual ingestion events and metrics
# MAGIC - `system.ingestion.zerobus_requests`: HTTP request logs and status
# MAGIC - `system.ingestion.zerobus_errors`: Failed ingestion attempts
# MAGIC - `system.ingestion.zerobus_quotas`: Rate limits and usage quotas

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** System table availability may vary by workspace configuration. Check with your administrator if tables are not accessible.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List available system tables (if you have access)
# MAGIC -- Uncomment to run:
# MAGIC -- SHOW TABLES IN system.ingestion;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Monitor Ingestion Metrics from Delta Table
# MAGIC
# MAGIC Even without system tables, we can derive valuable metrics from our target Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Basic ingestion statistics
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_events,
# MAGIC   COUNT(DISTINCT sensor_id) as unique_sensors,
# MAGIC   MIN(ingestion_time) as first_ingestion,
# MAGIC   MAX(ingestion_time) as last_ingestion,
# MAGIC   DATEDIFF(MAX(ingestion_time), MIN(ingestion_time)) as days_of_data
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ingestion volume over time (last 24 hours, by hour)
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('hour', ingestion_time) as hour,
# MAGIC   COUNT(*) as event_count,
# MAGIC   COUNT(DISTINCT sensor_id) as active_sensors,
# MAGIC   ROUND(AVG(temperature), 2) as avg_temperature,
# MAGIC   ROUND(AVG(humidity), 2) as avg_humidity,
# MAGIC   MIN(ingestion_time) as first_event,
# MAGIC   MAX(ingestion_time) as last_event
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_TRUNC('hour', ingestion_time)
# MAGIC ORDER BY hour DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Events per sensor (to identify hot sensors)
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   location,
# MAGIC   COUNT(*) as event_count,
# MAGIC   MIN(timestamp) as first_event,
# MAGIC   MAX(timestamp) as last_event,
# MAGIC   ROUND((MAX(UNIX_TIMESTAMP(timestamp)) - MIN(UNIX_TIMESTAMP(timestamp))) / 60, 2) as duration_minutes,
# MAGIC   ROUND(COUNT(*) / ((MAX(UNIX_TIMESTAMP(timestamp)) - MIN(UNIX_TIMESTAMP(timestamp))) / 60), 2) as events_per_minute
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY sensor_id, location
# MAGIC ORDER BY event_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Calculate Ingestion Latency
# MAGIC
# MAGIC Latency = Time difference between event timestamp and ingestion time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ingestion latency analysis
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('minute', ingestion_time) as minute,
# MAGIC   COUNT(*) as event_count,
# MAGIC   ROUND(AVG(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp)), 2) as avg_latency_seconds,
# MAGIC   ROUND(MIN(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp)), 2) as min_latency_seconds,
# MAGIC   ROUND(MAX(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp)), 2) as max_latency_seconds,
# MAGIC   ROUND(PERCENTILE(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp), 0.50), 2) as p50_latency,
# MAGIC   ROUND(PERCENTILE(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp), 0.95), 2) as p95_latency,
# MAGIC   ROUND(PERCENTILE(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp), 0.99), 2) as p99_latency
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC   AND timestamp <= ingestion_time  -- Filter out future events
# MAGIC GROUP BY DATE_TRUNC('minute', ingestion_time)
# MAGIC ORDER BY minute DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# MAGIC %md
# MAGIC **Latency Targets:**
# MAGIC - **Excellent**: < 2 seconds (p95)
# MAGIC - **Good**: 2-5 seconds (p95)
# MAGIC - **Acceptable**: 5-10 seconds (p95)
# MAGIC - **Poor**: > 10 seconds (p95)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Detect Data Quality Issues

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data quality metrics
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_events,
# MAGIC   
# MAGIC   -- Completeness checks
# MAGIC   SUM(CASE WHEN sensor_id IS NULL THEN 1 ELSE 0 END) as missing_sensor_id,
# MAGIC   SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) as missing_timestamp,
# MAGIC   SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) as missing_temperature,
# MAGIC   SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END) as missing_humidity,
# MAGIC   SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) as missing_location,
# MAGIC   
# MAGIC   -- Validity checks
# MAGIC   SUM(CASE WHEN temperature < -50 OR temperature > 150 THEN 1 ELSE 0 END) as invalid_temperature,
# MAGIC   SUM(CASE WHEN humidity < 0 OR humidity > 100 THEN 1 ELSE 0 END) as invalid_humidity,
# MAGIC   SUM(CASE WHEN pressure < 900 OR pressure > 1100 THEN 1 ELSE 0 END) as invalid_pressure,
# MAGIC   
# MAGIC   -- Timeliness checks
# MAGIC   SUM(CASE WHEN timestamp > ingestion_time THEN 1 ELSE 0 END) as future_events,
# MAGIC   SUM(CASE WHEN ingestion_time > timestamp + INTERVAL 1 HOUR THEN 1 ELSE 0 END) as delayed_events,
# MAGIC   
# MAGIC   -- Battery health
# MAGIC   SUM(CASE WHEN battery_level < 20 THEN 1 ELSE 0 END) as low_battery,
# MAGIC   SUM(CASE WHEN battery_level < 10 THEN 1 ELSE 0 END) as critical_battery
# MAGIC   
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 24 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sensors with data quality issues
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   location,
# MAGIC   COUNT(*) as total_events,
# MAGIC   SUM(CASE WHEN temperature IS NULL OR humidity IS NULL OR pressure IS NULL THEN 1 ELSE 0 END) as missing_readings,
# MAGIC   SUM(CASE WHEN temperature < -50 OR temperature > 150 THEN 1 ELSE 0 END) as invalid_temp,
# MAGIC   ROUND(AVG(CASE WHEN battery_level IS NOT NULL THEN battery_level ELSE 100 END), 2) as avg_battery,
# MAGIC   MAX(timestamp) as last_seen
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY sensor_id, location
# MAGIC HAVING missing_readings > 0 OR invalid_temp > 0 OR avg_battery < 20
# MAGIC ORDER BY missing_readings DESC, invalid_temp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Monitor Table Growth and Storage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table statistics
# MAGIC DESCRIBE DETAIL zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table size and file statistics
# MAGIC SELECT 
# MAGIC   format_number(numFiles, 0) as file_count,
# MAGIC   format_number(sizeInBytes / 1024 / 1024, 2) as size_mb,
# MAGIC   format_number(sizeInBytes / 1024 / 1024 / 1024, 3) as size_gb,
# MAGIC   format_number(sizeInBytes / numFiles / 1024 / 1024, 2) as avg_file_size_mb
# MAGIC FROM (DESCRIBE DETAIL zerobus_workshop.streaming_ingestion.sensor_events);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table growth over time
# MAGIC SELECT 
# MAGIC   version,
# MAGIC   timestamp,
# MAGIC   operation,
# MAGIC   operationMetrics.numOutputRows as rows_added,
# MAGIC   operationMetrics.numOutputBytes as bytes_added,
# MAGIC   format_number(operationMetrics.numOutputBytes / 1024 / 1024, 2) as mb_added
# MAGIC FROM (DESCRIBE HISTORY zerobus_workshop.streaming_ingestion.sensor_events)
# MAGIC WHERE operation IN ('WRITE', 'STREAMING UPDATE')
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Monitoring Dashboard Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create view for real-time ingestion metrics
# MAGIC CREATE OR REPLACE VIEW zerobus_workshop.streaming_ingestion.v_ingestion_metrics AS
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('minute', ingestion_time) as minute,
# MAGIC   COUNT(*) as events_ingested,
# MAGIC   COUNT(DISTINCT sensor_id) as active_sensors,
# MAGIC   ROUND(AVG(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp)), 2) as avg_latency_sec,
# MAGIC   ROUND(PERCENTILE(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp), 0.95), 2) as p95_latency_sec,
# MAGIC   SUM(CASE WHEN temperature IS NULL OR humidity IS NULL THEN 1 ELSE 0 END) as missing_data_count,
# MAGIC   SUM(CASE WHEN battery_level < 20 THEN 1 ELSE 0 END) as low_battery_count
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_TRUNC('minute', ingestion_time);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create view for sensor health
# MAGIC CREATE OR REPLACE VIEW zerobus_workshop.streaming_ingestion.v_sensor_health AS
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   location,
# MAGIC   MAX(timestamp) as last_event_time,
# MAGIC   ROUND((current_timestamp() - MAX(timestamp)) / 60, 2) as minutes_since_last_event,
# MAGIC   COUNT(*) as events_last_hour,
# MAGIC   ROUND(AVG(temperature), 2) as avg_temperature,
# MAGIC   ROUND(AVG(humidity), 2) as avg_humidity,
# MAGIC   ROUND(AVG(CASE WHEN battery_level IS NOT NULL THEN battery_level ELSE 100 END), 2) as avg_battery_level,
# MAGIC   CASE 
# MAGIC     WHEN MAX(timestamp) < current_timestamp() - INTERVAL 15 MINUTES THEN 'OFFLINE'
# MAGIC     WHEN AVG(CASE WHEN battery_level IS NOT NULL THEN battery_level ELSE 100 END) < 10 THEN 'CRITICAL'
# MAGIC     WHEN AVG(CASE WHEN battery_level IS NOT NULL THEN battery_level ELSE 100 END) < 20 THEN 'WARNING'
# MAGIC     ELSE 'HEALTHY'
# MAGIC   END as health_status
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY sensor_id, location;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the monitoring views
# MAGIC SELECT * FROM zerobus_workshop.streaming_ingestion.v_ingestion_metrics
# MAGIC ORDER BY minute DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sensor health dashboard
# MAGIC SELECT 
# MAGIC   health_status,
# MAGIC   COUNT(*) as sensor_count,
# MAGIC   ROUND(AVG(minutes_since_last_event), 2) as avg_minutes_silent,
# MAGIC   ROUND(AVG(avg_battery_level), 2) as avg_battery
# MAGIC FROM zerobus_workshop.streaming_ingestion.v_sensor_health
# MAGIC GROUP BY health_status
# MAGIC ORDER BY 
# MAGIC   CASE health_status 
# MAGIC     WHEN 'CRITICAL' THEN 1 
# MAGIC     WHEN 'WARNING' THEN 2 
# MAGIC     WHEN 'OFFLINE' THEN 3 
# MAGIC     WHEN 'HEALTHY' THEN 4 
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Implement Alert Conditions

# COMMAND ----------

from pyspark.sql.functions import *
import json

def check_ingestion_health():
    """
    Check ingestion pipeline health and return alert conditions.
    """
    # Get recent metrics
    recent_metrics = spark.sql("""
        SELECT 
            COUNT(*) as event_count,
            COUNT(DISTINCT sensor_id) as sensor_count,
            AVG(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp)) as avg_latency,
            PERCENTILE(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp), 0.95) as p95_latency,
            SUM(CASE WHEN battery_level < 20 THEN 1 ELSE 0 END) as low_battery_count
        FROM zerobus_workshop.streaming_ingestion.sensor_events
        WHERE ingestion_time >= current_timestamp() - INTERVAL 5 MINUTES
    """).collect()[0]
    
    alerts = []
    
    # Alert: No recent data
    if recent_metrics['event_count'] == 0:
        alerts.append({
            "severity": "CRITICAL",
            "condition": "No data ingested in last 5 minutes",
            "metric": "event_count",
            "value": 0,
            "threshold": 1
        })
    
    # Alert: High latency
    if recent_metrics['p95_latency'] and recent_metrics['p95_latency'] > 10:
        alerts.append({
            "severity": "WARNING",
            "condition": "High ingestion latency (p95)",
            "metric": "p95_latency_seconds",
            "value": round(recent_metrics['p95_latency'], 2),
            "threshold": 10
        })
    
    # Alert: Low battery devices
    if recent_metrics['low_battery_count'] > 5:
        alerts.append({
            "severity": "WARNING",
            "condition": "Multiple sensors with low battery",
            "metric": "low_battery_count",
            "value": recent_metrics['low_battery_count'],
            "threshold": 5
        })
    
    return {
        "timestamp": datetime.now().isoformat(),
        "health_status": "CRITICAL" if any(a['severity'] == 'CRITICAL' for a in alerts) else
                        "WARNING" if alerts else "HEALTHY",
        "alerts": alerts,
        "metrics": {
            "event_count": recent_metrics['event_count'],
            "sensor_count": recent_metrics['sensor_count'],
            "avg_latency": round(recent_metrics['avg_latency'], 2) if recent_metrics['avg_latency'] else None,
            "p95_latency": round(recent_metrics['p95_latency'], 2) if recent_metrics['p95_latency'] else None
        }
    }

# Run health check
health_report = check_ingestion_health()

print("PIPELINE HEALTH CHECK")
print("=" * 80)
print(f"Timestamp: {health_report['timestamp']}")
print(f"Status: {health_report['health_status']}")
print(f"\nMetrics:")
for metric, value in health_report['metrics'].items():
    print(f"  {metric}: {value}")

if health_report['alerts']:
    print(f"\n⚠️  ALERTS ({len(health_report['alerts'])}):")
    for alert in health_report['alerts']:
        print(f"  [{alert['severity']}] {alert['condition']}")
        print(f"    - {alert['metric']}: {alert['value']} (threshold: {alert['threshold']})")
else:
    print("\n✅ No alerts - Pipeline is healthy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Detect Offline Sensors

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find sensors that haven't reported recently
# MAGIC WITH latest_events AS (
# MAGIC   SELECT 
# MAGIC     sensor_id,
# MAGIC     location,
# MAGIC     MAX(timestamp) as last_event_time
# MAGIC   FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC   GROUP BY sensor_id, location
# MAGIC )
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   location,
# MAGIC   last_event_time,
# MAGIC   ROUND((UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(last_event_time)) / 60, 2) as minutes_offline,
# MAGIC   CASE 
# MAGIC     WHEN last_event_time < current_timestamp() - INTERVAL 1 HOUR THEN 'CRITICAL - Offline > 1 hour'
# MAGIC     WHEN last_event_time < current_timestamp() - INTERVAL 30 MINUTES THEN 'WARNING - Offline > 30 min'
# MAGIC     WHEN last_event_time < current_timestamp() - INTERVAL 15 MINUTES THEN 'ATTENTION - Offline > 15 min'
# MAGIC     ELSE 'ONLINE'
# MAGIC   END as status
# MAGIC FROM latest_events
# MAGIC WHERE last_event_time < current_timestamp() - INTERVAL 15 MINUTES
# MAGIC ORDER BY minutes_offline DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Performance Optimization Recommendations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file statistics (small files indicate need for optimization)
# MAGIC SELECT 
# MAGIC   COUNT(*) as file_count,
# MAGIC   ROUND(SUM(size_bytes) / 1024 / 1024, 2) as total_size_mb,
# MAGIC   ROUND(AVG(size_bytes) / 1024 / 1024, 2) as avg_file_size_mb,
# MAGIC   ROUND(MIN(size_bytes) / 1024 / 1024, 2) as min_file_size_mb,
# MAGIC   ROUND(MAX(size_bytes) / 1024 / 1024, 2) as max_file_size_mb,
# MAGIC   CASE 
# MAGIC     WHEN AVG(size_bytes) / 1024 / 1024 < 1 THEN 'OPTIMIZE RECOMMENDED - Many small files'
# MAGIC     WHEN COUNT(*) > 1000 THEN 'OPTIMIZE RECOMMENDED - Too many files'
# MAGIC     ELSE 'File layout is healthy'
# MAGIC   END as recommendation
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     file_path,
# MAGIC     size_bytes
# MAGIC   FROM delta.`/databricks-datasets/iot-stream/data-device` 
# MAGIC   LIMIT 0
# MAGIC );
# MAGIC
# MAGIC -- Note: This is a template query. Actual file statistics require access to table path

# COMMAND ----------

# Python function to analyze table and provide recommendations
def analyze_table_performance(catalog, schema, table):
    """
    Analyze table performance and provide optimization recommendations.
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Get table details
    table_details = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]
    
    recommendations = []
    
    # Check file count
    num_files = table_details['numFiles']
    size_gb = table_details['sizeInBytes'] / (1024**3)
    avg_file_size_mb = (table_details['sizeInBytes'] / num_files) / (1024**2) if num_files > 0 else 0
    
    if num_files > 1000:
        recommendations.append({
            "priority": "HIGH",
            "issue": f"Too many files ({num_files})",
            "action": "Run OPTIMIZE command",
            "command": f"OPTIMIZE {full_table_name}"
        })
    
    if avg_file_size_mb < 10 and num_files > 100:
        recommendations.append({
            "priority": "MEDIUM",
            "issue": f"Small files (avg {avg_file_size_mb:.2f} MB)",
            "action": "Run OPTIMIZE with ZORDER",
            "command": f"OPTIMIZE {full_table_name} ZORDER BY (sensor_id, timestamp)"
        })
    
    # Check for old transaction log files
    history = spark.sql(f"DESCRIBE HISTORY {full_table_name}").count()
    if history > 100:
        recommendations.append({
            "priority": "LOW",
            "issue": f"Many historical versions ({history})",
            "action": "Run VACUUM to clean old files",
            "command": f"VACUUM {full_table_name} RETAIN 168 HOURS"
        })
    
    return {
        "table": full_table_name,
        "statistics": {
            "num_files": num_files,
            "size_gb": round(size_gb, 3),
            "avg_file_size_mb": round(avg_file_size_mb, 2),
            "history_versions": history
        },
        "recommendations": recommendations
    }

# Analyze our sensor table
analysis = analyze_table_performance("zerobus_workshop", "streaming_ingestion", "sensor_events")

print("TABLE PERFORMANCE ANALYSIS")
print("=" * 80)
print(f"Table: {analysis['table']}")
print(f"\nStatistics:")
for key, value in analysis['statistics'].items():
    print(f"  {key}: {value}")

if analysis['recommendations']:
    print(f"\n📋 RECOMMENDATIONS ({len(analysis['recommendations'])}):")
    for rec in analysis['recommendations']:
        print(f"\n  [{rec['priority']}] {rec['issue']}")
        print(f"  Action: {rec['action']}")
        print(f"  Command: {rec['command']}")
else:
    print("\n✅ Table performance is optimal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Automated Monitoring Notebook Job
# MAGIC
# MAGIC This code can be scheduled to run periodically.

# COMMAND ----------

def run_monitoring_job():
    """
    Complete monitoring job that can be scheduled.
    """
    from datetime import datetime
    import json
    
    timestamp = datetime.now().isoformat()
    
    # 1. Check ingestion health
    health_check = check_ingestion_health()
    
    # 2. Check data quality
    quality_check = spark.sql("""
        SELECT 
            COUNT(*) as total_events,
            SUM(CASE WHEN temperature IS NULL THEN 1 ELSE 0 END) as missing_temperature,
            SUM(CASE WHEN temperature < -50 OR temperature > 150 THEN 1 ELSE 0 END) as invalid_temperature
        FROM zerobus_workshop.streaming_ingestion.sensor_events
        WHERE ingestion_time >= current_timestamp() - INTERVAL 5 MINUTES
    """).collect()[0]
    
    # 3. Check for offline sensors
    offline_sensors = spark.sql("""
        SELECT COUNT(DISTINCT sensor_id) as count
        FROM (
            SELECT sensor_id, MAX(timestamp) as last_seen
            FROM zerobus_workshop.streaming_ingestion.sensor_events
            GROUP BY sensor_id
            HAVING MAX(timestamp) < current_timestamp() - INTERVAL 15 MINUTES
        )
    """).collect()[0]['count']
    
    # Compile report
    monitoring_report = {
        "timestamp": timestamp,
        "health_status": health_check['health_status'],
        "metrics": {
            "ingestion": health_check['metrics'],
            "data_quality": {
                "total_events": quality_check['total_events'],
                "missing_temperature": quality_check['missing_temperature'],
                "invalid_temperature": quality_check['invalid_temperature']
            },
            "sensors": {
                "offline_count": offline_sensors
            }
        },
        "alerts": health_check['alerts']
    }
    
    # Log report (in production, send to monitoring system)
    print(json.dumps(monitoring_report, indent=2))
    
    # Store in monitoring table (optional)
    monitoring_df = spark.createDataFrame([{
        "timestamp": timestamp,
        "health_status": monitoring_report['health_status'],
        "report_json": json.dumps(monitoring_report)
    }])
    
    # Uncomment to persist monitoring history:
    # monitoring_df.write.mode("append").saveAsTable("zerobus_workshop.streaming_ingestion.monitoring_history")
    
    return monitoring_report

# Run monitoring job
print("Running monitoring job...")
report = run_monitoring_job()
print("\n✅ Monitoring job complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Troubleshooting Common Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 1: No Data Arriving
# MAGIC
# MAGIC **Checklist:**
# MAGIC 1. Verify endpoint URL is correct
# MAGIC 2. Check authentication token is valid and not expired
# MAGIC 3. Confirm table has INSERT permissions for the token
# MAGIC 4. Check network connectivity from client to Databricks
# MAGIC 5. Review HTTP response codes from ingestion attempts
# MAGIC
# MAGIC **Diagnostic Queries:**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check when was last successful ingestion
# MAGIC SELECT 
# MAGIC   MAX(ingestion_time) as last_ingestion,
# MAGIC   ROUND((UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(ingestion_time))) / 60, 2) as minutes_ago
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 2: High Latency
# MAGIC
# MAGIC **Possible Causes:**
# MAGIC - Small batch sizes (too many HTTP requests)
# MAGIC - Network latency between client and Databricks
# MAGIC - Table optimization needed (too many small files)
# MAGIC - Resource contention on serverless compute
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Increase batch size to 500-1000 events
# MAGIC - Use parallel ingestion with multiple threads
# MAGIC - Run OPTIMIZE on target table
# MAGIC - Check workspace compute availability

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify latency patterns
# MAGIC SELECT 
# MAGIC   HOUR(ingestion_time) as hour_of_day,
# MAGIC   COUNT(*) as event_count,
# MAGIC   ROUND(AVG(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp)), 2) as avg_latency,
# MAGIC   ROUND(PERCENTILE(UNIX_TIMESTAMP(ingestion_time) - UNIX_TIMESTAMP(timestamp), 0.95), 2) as p95_latency
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND timestamp <= ingestion_time
# MAGIC GROUP BY HOUR(ingestion_time)
# MAGIC ORDER BY hour_of_day;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue 3: Schema Validation Errors
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - 400 Bad Request errors
# MAGIC - Events not appearing in target table
# MAGIC
# MAGIC **Solutions:**
# MAGIC - Verify JSON field names match table columns exactly
# MAGIC - Check data types are compatible
# MAGIC - Add new columns to table before sending new fields
# MAGIC - Review error messages in HTTP responses

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify current table schema
# MAGIC DESCRIBE zerobus_workshop.streaming_ingestion.sensor_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ Monitored ingestion metrics using Delta table queries  
# MAGIC ✅ Calculated latency percentiles (p50, p95, p99)  
# MAGIC ✅ Detected data quality issues and anomalies  
# MAGIC ✅ Created monitoring views and dashboards  
# MAGIC ✅ Implemented automated health checks and alerts  
# MAGIC ✅ Identified offline sensors and battery issues  
# MAGIC ✅ Analyzed table performance and optimization opportunities  
# MAGIC ✅ Built reusable monitoring job for scheduling  
# MAGIC
# MAGIC ### Monitoring Best Practices
# MAGIC
# MAGIC **1. Metrics to Track:**
# MAGIC - **Volume**: Events per minute/hour, unique sensors
# MAGIC - **Latency**: p50, p95, p99 ingestion lag
# MAGIC - **Quality**: Missing fields, invalid values, outliers
# MAGIC - **Availability**: Offline sensors, failed ingestions
# MAGIC - **Performance**: Table size, file count, query times
# MAGIC
# MAGIC **2. Alert Thresholds:**
# MAGIC - **CRITICAL**: No data for > 15 minutes, p95 latency > 60s
# MAGIC - **WARNING**: p95 latency > 10s, > 5% invalid data
# MAGIC - **INFO**: Sensor offline > 30 minutes, battery < 20%
# MAGIC
# MAGIC **3. Operational Tasks:**
# MAGIC - **Daily**: Review dashboard, check for anomalies
# MAGIC - **Weekly**: Analyze trends, optimize tables (OPTIMIZE, VACUUM)
# MAGIC - **Monthly**: Capacity planning, cost review
# MAGIC
# MAGIC **4. Performance Optimization:**
# MAGIC - Run OPTIMIZE weekly for tables with frequent writes
# MAGIC - Use ZORDER BY on commonly filtered columns
# MAGIC - VACUUM old files after retention period
# MAGIC - Monitor and adjust partitioning strategy
# MAGIC
# MAGIC ### Production Checklist
# MAGIC
# MAGIC ✅ Monitoring dashboard deployed and accessible  
# MAGIC ✅ Automated health check job scheduled (every 5-15 minutes)  
# MAGIC ✅ Alerts configured and tested  
# MAGIC ✅ Runbook documented for common issues  
# MAGIC ✅ On-call rotation and escalation procedures defined  
# MAGIC ✅ Table optimization scheduled (weekly OPTIMIZE)  
# MAGIC ✅ Backup and disaster recovery plan tested  
# MAGIC
# MAGIC ### Next Lab
# MAGIC
# MAGIC Proceed to **Lab 6: Building a Real-Time IoT Dashboard** to see:
# MAGIC - Complete end-to-end IoT application
# MAGIC - Real-time visualization with Databricks SQL
# MAGIC - Alerting on streaming data
# MAGIC - Production deployment patterns
# MAGIC - Integration with external systems
