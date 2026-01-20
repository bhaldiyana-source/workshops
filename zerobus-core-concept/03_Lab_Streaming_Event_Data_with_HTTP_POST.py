# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Streaming Event Data with HTTP POST to Zerobus
# MAGIC
# MAGIC ## Overview
# MAGIC In this lab, you'll learn production-grade patterns for streaming event data to Zerobus endpoints. You'll implement single-event and batch ingestion patterns, handle authentication and errors gracefully, optimize throughput with batching strategies, and build reusable ingestion functions for production applications.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By completing this lab, you will be able to:
# MAGIC - Send single and batch JSON events to Zerobus endpoints using HTTP POST
# MAGIC - Implement production-grade error handling with retry logic and exponential backoff
# MAGIC - Optimize ingestion throughput by tuning batch sizes and parallelization
# MAGIC - Handle various event payload formats and data types
# MAGIC - Monitor ingestion performance and identify bottlenecks
# MAGIC - Build reusable Python modules for event streaming applications
# MAGIC - Troubleshoot common ingestion failures and implement resilience patterns
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 2: Creating Your First Zerobus Ingest Connection
# MAGIC - Have endpoint URL and authentication token ready
# MAGIC - Target Delta table (sensor_events) created and accessible
# MAGIC
# MAGIC ## Duration
# MAGIC 45-50 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Environment Setup and Configuration
# MAGIC
# MAGIC Let's start by importing required libraries and setting up our connection parameters.

# COMMAND ----------

import requests
import json
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("✅ Libraries imported successfully")

# COMMAND ----------

# Configuration parameters
# Replace these with your values from Lab 2
ENDPOINT_URL = dbutils.widgets.get("endpoint_url") if dbutils.widgets.get("endpoint_url") else ""
AUTH_TOKEN = dbutils.widgets.get("auth_token") if dbutils.widgets.get("auth_token") else ""

# Create widgets for easy configuration
dbutils.widgets.text("endpoint_url", "", "Zerobus Endpoint URL")
dbutils.widgets.text("auth_token", "", "Authentication Token")

if not ENDPOINT_URL or not AUTH_TOKEN:
    print("⚠️  Please enter your endpoint URL and authentication token in the widgets above!")
    print("These values should be from Lab 2.")
else:
    print("✅ Configuration loaded")
    print(f"Endpoint: {ENDPOINT_URL}")
    print(f"Token: {AUTH_TOKEN[:10]}...{AUTH_TOKEN[-4:]} (masked)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Single Event Ingestion
# MAGIC
# MAGIC Let's start with the simplest case: sending a single event to Zerobus.

# COMMAND ----------

def send_single_event(endpoint_url: str, auth_token: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Send a single event to Zerobus endpoint.
    
    Args:
        endpoint_url: Zerobus HTTP endpoint
        auth_token: Authentication token (PAT or service principal)
        event: Event dictionary matching target table schema
        
    Returns:
        Response dictionary with status and metadata
    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Zerobus expects an array of events, even for single event
        payload = [event]
        
        response = requests.post(
            endpoint_url,
            headers=headers,
            json=payload,
            timeout=10
        )
        
        response.raise_for_status()  # Raise exception for 4xx/5xx status codes
        
        return {
            "success": True,
            "status_code": response.status_code,
            "response": response.json(),
            "event_count": 1
        }
        
    except requests.exceptions.HTTPError as e:
        return {
            "success": False,
            "status_code": e.response.status_code if e.response else None,
            "error": str(e),
            "response_text": e.response.text if e.response else None
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

# Test single event ingestion
test_event = {
    "sensor_id": "TEST-SINGLE-001",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "temperature": 23.5,
    "humidity": 50.0,
    "pressure": 1013.25,
    "location": "Test Lab - Single Event",
    "ingestion_time": datetime.now(timezone.utc).isoformat()
}

print("Sending single test event...")
print(f"Event: {json.dumps(test_event, indent=2)}")
print("-" * 80)

result = send_single_event(ENDPOINT_URL, AUTH_TOKEN, test_event)

if result["success"]:
    print("✅ SUCCESS!")
    print(f"Status: {result['status_code']}")
    print(f"Response: {json.dumps(result['response'], indent=2)}")
else:
    print("❌ FAILED!")
    print(f"Error: {result['error']}")
    if "response_text" in result:
        print(f"Response: {result['response_text']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Batch Event Ingestion
# MAGIC
# MAGIC Batch ingestion is the recommended approach for production workloads. It significantly improves throughput and reduces HTTP request overhead.

# COMMAND ----------

def send_batch_events(endpoint_url: str, auth_token: str, events: List[Dict[str, Any]], 
                      batch_size: int = 100) -> List[Dict[str, Any]]:
    """
    Send events in batches to Zerobus endpoint.
    
    Args:
        endpoint_url: Zerobus HTTP endpoint
        auth_token: Authentication token
        events: List of event dictionaries
        batch_size: Number of events per batch (default 100, max recommended 1000)
        
    Returns:
        List of result dictionaries for each batch
    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    results = []
    total_events = len(events)
    
    # Process events in batches
    for i in range(0, total_events, batch_size):
        batch = events[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total_events + batch_size - 1) // batch_size
        
        try:
            start_time = time.time()
            response = requests.post(
                endpoint_url,
                headers=headers,
                json=batch,
                timeout=30
            )
            end_time = time.time()
            
            response.raise_for_status()
            
            result = {
                "success": True,
                "batch_num": batch_num,
                "total_batches": total_batches,
                "status_code": response.status_code,
                "event_count": len(batch),
                "duration_seconds": round(end_time - start_time, 3),
                "throughput_eps": round(len(batch) / (end_time - start_time), 2),
                "response": response.json()
            }
            results.append(result)
            
            logger.info(f"Batch {batch_num}/{total_batches}: {len(batch)} events, "
                       f"{result['duration_seconds']}s, {result['throughput_eps']} events/sec")
            
        except Exception as e:
            result = {
                "success": False,
                "batch_num": batch_num,
                "error": str(e),
                "event_count": len(batch)
            }
            results.append(result)
            logger.error(f"Batch {batch_num}/{total_batches} failed: {str(e)}")
    
    return results

# Generate sample batch events
def generate_sensor_events(count: int, sensor_ids: List[str]) -> List[Dict[str, Any]]:
    """Generate synthetic sensor events for testing."""
    events = []
    base_time = datetime.now(timezone.utc)
    
    for i in range(count):
        event = {
            "sensor_id": random.choice(sensor_ids),
            "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
            "temperature": round(random.uniform(18.0, 28.0), 2),
            "humidity": round(random.uniform(30.0, 70.0), 2),
            "pressure": round(random.uniform(1000.0, 1020.0), 2),
            "location": f"Building {random.choice(['A', 'B', 'C'])} - Floor {random.randint(1, 5)}",
            "ingestion_time": datetime.now(timezone.utc).isoformat()
        }
        events.append(event)
    
    return events

# Test batch ingestion
sensor_ids = [f"SENSOR-{i:03d}" for i in range(1, 11)]  # 10 sensors
test_events = generate_sensor_events(500, sensor_ids)

print(f"Generated {len(test_events)} test events")
print(f"Sensors: {len(sensor_ids)}")
print(f"Batch size: 100 events per request")
print("-" * 80)

batch_results = send_batch_events(ENDPOINT_URL, AUTH_TOKEN, test_events, batch_size=100)

# Summary statistics
successful_batches = sum(1 for r in batch_results if r["success"])
total_events_sent = sum(r["event_count"] for r in batch_results if r["success"])
total_duration = sum(r["duration_seconds"] for r in batch_results if r["success"])
avg_throughput = sum(r["throughput_eps"] for r in batch_results if r["success"]) / len([r for r in batch_results if r["success"]]) if successful_batches > 0 else 0

print("\n" + "=" * 80)
print("BATCH INGESTION SUMMARY")
print("=" * 80)
print(f"Total batches: {len(batch_results)}")
print(f"Successful batches: {successful_batches}")
print(f"Failed batches: {len(batch_results) - successful_batches}")
print(f"Total events sent: {total_events_sent}")
print(f"Total duration: {total_duration:.2f} seconds")
print(f"Average throughput: {avg_throughput:.2f} events/second")
print(f"Overall throughput: {total_events_sent / total_duration:.2f} events/second" if total_duration > 0 else "N/A")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Production-Grade Error Handling with Retry Logic
# MAGIC
# MAGIC Production applications must handle transient failures gracefully. Let's implement retry logic with exponential backoff.

# COMMAND ----------

def send_with_retry(endpoint_url: str, auth_token: str, events: List[Dict[str, Any]],
                   max_retries: int = 3, backoff_factor: float = 2.0) -> Dict[str, Any]:
    """
    Send events with automatic retry on transient failures.
    
    Args:
        endpoint_url: Zerobus HTTP endpoint
        auth_token: Authentication token
        events: List of events to send
        max_retries: Maximum number of retry attempts
        backoff_factor: Exponential backoff multiplier (wait time = backoff_factor ^ attempt)
        
    Returns:
        Result dictionary with success status and metadata
    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    # Transient error codes that should be retried
    RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries}: Sending {len(events)} events")
            
            start_time = time.time()
            response = requests.post(
                endpoint_url,
                headers=headers,
                json=events,
                timeout=30
            )
            end_time = time.time()
            
            # Success!
            if response.status_code == 200:
                return {
                    "success": True,
                    "attempts": attempt + 1,
                    "status_code": response.status_code,
                    "event_count": len(events),
                    "duration_seconds": round(end_time - start_time, 3),
                    "response": response.json()
                }
            
            # Check if error is retryable
            if response.status_code not in RETRYABLE_STATUS_CODES:
                # Non-retryable error (e.g., 401, 403, 400)
                return {
                    "success": False,
                    "attempts": attempt + 1,
                    "status_code": response.status_code,
                    "error": f"Non-retryable error: {response.status_code}",
                    "response_text": response.text
                }
            
            # Retryable error - log and continue
            last_error = f"HTTP {response.status_code}: {response.text}"
            logger.warning(f"Retryable error on attempt {attempt + 1}: {last_error}")
            
        except requests.exceptions.Timeout as e:
            last_error = f"Timeout: {str(e)}"
            logger.warning(f"Timeout on attempt {attempt + 1}")
            
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {str(e)}"
            logger.warning(f"Connection error on attempt {attempt + 1}")
            
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"
            logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            # Don't retry unexpected errors
            return {
                "success": False,
                "attempts": attempt + 1,
                "error": last_error
            }
        
        # Wait before retrying (exponential backoff)
        if attempt < max_retries - 1:
            wait_time = backoff_factor ** attempt
            logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
            time.sleep(wait_time)
    
    # All retries exhausted
    return {
        "success": False,
        "attempts": max_retries,
        "error": f"Max retries ({max_retries}) exceeded. Last error: {last_error}"
    }

# Test retry logic with sample events
test_events_retry = generate_sensor_events(50, ["RETRY-TEST-001", "RETRY-TEST-002"])

print("Testing retry logic...")
print(f"Events: {len(test_events_retry)}")
print(f"Max retries: 3")
print(f"Backoff factor: 2.0 (waits: 1s, 2s, 4s)")
print("-" * 80)

retry_result = send_with_retry(ENDPOINT_URL, AUTH_TOKEN, test_events_retry, max_retries=3)

if retry_result["success"]:
    print("\n✅ SUCCESS!")
    print(f"Attempts needed: {retry_result['attempts']}")
    print(f"Duration: {retry_result['duration_seconds']}s")
    print(f"Response: {json.dumps(retry_result['response'], indent=2)}")
else:
    print("\n❌ FAILED after retries")
    print(f"Attempts: {retry_result['attempts']}")
    print(f"Error: {retry_result['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Parallel Batch Ingestion
# MAGIC
# MAGIC For high-throughput scenarios, we can parallelize batch ingestion using multiple threads.

# COMMAND ----------

def send_batches_parallel(endpoint_url: str, auth_token: str, events: List[Dict[str, Any]],
                          batch_size: int = 100, max_workers: int = 5) -> List[Dict[str, Any]]:
    """
    Send events in parallel batches for maximum throughput.
    
    Args:
        endpoint_url: Zerobus HTTP endpoint
        auth_token: Authentication token
        events: List of events to send
        batch_size: Events per batch
        max_workers: Number of parallel threads
        
    Returns:
        List of result dictionaries for each batch
    """
    # Split events into batches
    batches = [events[i:i + batch_size] for i in range(0, len(events), batch_size)]
    
    results = []
    start_time = time.time()
    
    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batch jobs
        future_to_batch = {
            executor.submit(send_with_retry, endpoint_url, auth_token, batch, 3): idx 
            for idx, batch in enumerate(batches)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                result = future.result()
                result["batch_idx"] = batch_idx
                results.append(result)
                
                if result["success"]:
                    logger.info(f"✅ Batch {batch_idx + 1}/{len(batches)} completed: "
                               f"{result['event_count']} events, {result['duration_seconds']}s")
                else:
                    logger.error(f"❌ Batch {batch_idx + 1}/{len(batches)} failed: {result['error']}")
                    
            except Exception as e:
                logger.error(f"❌ Batch {batch_idx + 1} exception: {str(e)}")
                results.append({
                    "success": False,
                    "batch_idx": batch_idx,
                    "error": str(e)
                })
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Calculate statistics
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    total_events = sum(r.get("event_count", 0) for r in successful)
    
    print("\n" + "=" * 80)
    print("PARALLEL BATCH INGESTION SUMMARY")
    print("=" * 80)
    print(f"Total batches: {len(batches)}")
    print(f"Successful batches: {len(successful)}")
    print(f"Failed batches: {len(failed)}")
    print(f"Total events sent: {total_events}")
    print(f"Total duration: {total_duration:.2f} seconds")
    print(f"Overall throughput: {total_events / total_duration:.2f} events/second")
    print(f"Parallelization: {max_workers} concurrent threads")
    
    return results

# Test parallel ingestion with larger dataset
print("Generating 2000 test events for parallel ingestion...")
large_dataset = generate_sensor_events(2000, [f"PARALLEL-{i:03d}" for i in range(1, 21)])

print(f"Events: {len(large_dataset)}")
print(f"Batch size: 200")
print(f"Parallel workers: 5")
print("-" * 80)

parallel_results = send_batches_parallel(
    ENDPOINT_URL, 
    AUTH_TOKEN, 
    large_dataset,
    batch_size=200,
    max_workers=5
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Handling Different Event Formats
# MAGIC
# MAGIC Zerobus supports various JSON structures. Let's test different payload formats.

# COMMAND ----------

# Test different event formats

# Format 1: Minimal required fields only
minimal_event = {
    "sensor_id": "MINIMAL-001",
    "timestamp": datetime.now(timezone.utc).isoformat()
}

# Format 2: All fields populated
complete_event = {
    "sensor_id": "COMPLETE-001",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "temperature": 22.5,
    "humidity": 45.0,
    "pressure": 1013.25,
    "location": "Test Lab - Complete",
    "ingestion_time": datetime.now(timezone.utc).isoformat()
}

# Format 3: With additional metadata (will be ignored if not in schema)
extended_event = {
    "sensor_id": "EXTENDED-001",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "temperature": 23.0,
    "humidity": 50.0,
    "pressure": 1012.0,
    "location": "Test Lab - Extended",
    "ingestion_time": datetime.now(timezone.utc).isoformat(),
    "battery_level": 85.5,  # Extra field not in schema
    "firmware_version": "2.1.0"  # Extra field not in schema
}

test_formats = [
    ("Minimal (required fields only)", minimal_event),
    ("Complete (all schema fields)", complete_event),
    ("Extended (extra fields)", extended_event)
]

print("Testing different event formats...")
print("=" * 80)

for format_name, event in test_formats:
    print(f"\n{format_name}:")
    print(f"Event: {json.dumps(event, indent=2)}")
    
    result = send_single_event(ENDPOINT_URL, AUTH_TOKEN, event)
    
    if result["success"]:
        print(f"✅ SUCCESS - Status: {result['status_code']}")
    else:
        print(f"❌ FAILED - Error: {result.get('error', 'Unknown')}")
        if "response_text" in result:
            print(f"Response: {result['response_text']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Monitoring Ingestion Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check recent ingestion stats
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('minute', ingestion_time) as minute,
# MAGIC   COUNT(*) as event_count,
# MAGIC   COUNT(DISTINCT sensor_id) as unique_sensors,
# MAGIC   AVG(temperature) as avg_temp,
# MAGIC   MIN(timestamp) as earliest_event,
# MAGIC   MAX(timestamp) as latest_event
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY DATE_TRUNC('minute', ingestion_time)
# MAGIC ORDER BY minute DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event count by sensor (last hour)
# MAGIC SELECT 
# MAGIC   sensor_id,
# MAGIC   COUNT(*) as event_count,
# MAGIC   MIN(timestamp) as first_event,
# MAGIC   MAX(timestamp) as last_event,
# MAGIC   ROUND(AVG(temperature), 2) as avg_temperature
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY sensor_id
# MAGIC ORDER BY event_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total events ingested in this lab
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_events,
# MAGIC   COUNT(DISTINCT sensor_id) as unique_sensors,
# MAGIC   MIN(ingestion_time) as first_ingestion,
# MAGIC   MAX(ingestion_time) as last_ingestion,
# MAGIC   ROUND((MAX(UNIX_TIMESTAMP(ingestion_time)) - MIN(UNIX_TIMESTAMP(ingestion_time))) / 60, 2) as duration_minutes
# MAGIC FROM zerobus_workshop.streaming_ingestion.sensor_events
# MAGIC WHERE ingestion_time >= current_timestamp() - INTERVAL 2 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Building a Reusable Ingestion Class
# MAGIC
# MAGIC Let's create a reusable Python class for production applications.

# COMMAND ----------

class ZerobusIngestionClient:
    """
    Production-ready client for Zerobus event ingestion.
    
    Features:
    - Automatic batching
    - Retry with exponential backoff
    - Connection pooling
    - Performance metrics
    - Error tracking
    """
    
    def __init__(self, endpoint_url: str, auth_token: str, 
                 default_batch_size: int = 100, max_retries: int = 3):
        self.endpoint_url = endpoint_url
        self.auth_token = auth_token
        self.default_batch_size = default_batch_size
        self.max_retries = max_retries
        
        # Create session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        })
        
        # Metrics
        self.total_events_sent = 0
        self.total_requests = 0
        self.failed_requests = 0
        
    def send_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Send a single event."""
        return self.send_events([event])
    
    def send_events(self, events: List[Dict[str, Any]], 
                    batch_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Send multiple events with automatic batching.
        
        Args:
            events: List of event dictionaries
            batch_size: Optional override for batch size
            
        Returns:
            Aggregated result dictionary
        """
        batch_size = batch_size or self.default_batch_size
        results = []
        
        # Process in batches
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            result = self._send_batch_with_retry(batch)
            results.append(result)
        
        # Aggregate results
        successful = [r for r in results if r["success"]]
        return {
            "success": len(successful) == len(results),
            "total_batches": len(results),
            "successful_batches": len(successful),
            "failed_batches": len(results) - len(successful),
            "total_events": len(events),
            "total_events_sent": sum(r.get("event_count", 0) for r in successful)
        }
    
    def _send_batch_with_retry(self, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Send a batch with retry logic."""
        RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(
                    self.endpoint_url,
                    json=batch,
                    timeout=30
                )
                
                self.total_requests += 1
                
                if response.status_code == 200:
                    self.total_events_sent += len(batch)
                    return {
                        "success": True,
                        "event_count": len(batch),
                        "attempts": attempt + 1
                    }
                
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    self.failed_requests += 1
                    return {
                        "success": False,
                        "error": f"Non-retryable: {response.status_code}",
                        "response": response.text
                    }
                
                # Retryable - wait and retry
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.failed_requests += 1
                    return {"success": False, "error": str(e)}
                time.sleep(2 ** attempt)
        
        self.failed_requests += 1
        return {"success": False, "error": "Max retries exceeded"}
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get client performance metrics."""
        return {
            "total_events_sent": self.total_events_sent,
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
            "success_rate": (self.total_requests - self.failed_requests) / self.total_requests if self.total_requests > 0 else 0
        }
    
    def close(self):
        """Close the session."""
        self.session.close()

# Test the client
print("Testing ZerobusIngestionClient...")
print("-" * 80)

client = ZerobusIngestionClient(
    endpoint_url=ENDPOINT_URL,
    auth_token=AUTH_TOKEN,
    default_batch_size=100,
    max_retries=3
)

# Generate and send test events
test_events = generate_sensor_events(300, [f"CLIENT-{i:03d}" for i in range(1, 11)])

print(f"Sending {len(test_events)} events using client...")
result = client.send_events(test_events)

print("\nResult:")
print(f"  Success: {result['success']}")
print(f"  Total batches: {result['total_batches']}")
print(f"  Successful batches: {result['successful_batches']}")
print(f"  Events sent: {result['total_events_sent']}/{result['total_events']}")

print("\nClient metrics:")
metrics = client.get_metrics()
for key, value in metrics.items():
    print(f"  {key}: {value}")

client.close()
print("\n✅ Client test complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What You Accomplished
# MAGIC
# MAGIC ✅ Sent single and batch events to Zerobus endpoints  
# MAGIC ✅ Implemented production-grade error handling with retry logic  
# MAGIC ✅ Optimized throughput with batching and parallelization  
# MAGIC ✅ Tested various event payload formats  
# MAGIC ✅ Built reusable Python class for event ingestion  
# MAGIC ✅ Monitored ingestion performance with SQL queries  
# MAGIC
# MAGIC ### Best Practices Learned
# MAGIC
# MAGIC **1. Batching:**
# MAGIC - Use batch sizes of 100-1,000 events for optimal performance
# MAGIC - Larger batches reduce HTTP overhead but increase payload size
# MAGIC - Balance based on your event size and latency requirements
# MAGIC
# MAGIC **2. Error Handling:**
# MAGIC - Implement retry logic with exponential backoff
# MAGIC - Distinguish retryable (5xx, 429) from non-retryable (4xx) errors
# MAGIC - Set appropriate timeouts and max retry limits
# MAGIC
# MAGIC **3. Performance:**
# MAGIC - Parallel ingestion can 3-5x throughput
# MAGIC - Use connection pooling (requests.Session)
# MAGIC - Monitor metrics to identify bottlenecks
# MAGIC
# MAGIC **4. Production Patterns:**
# MAGIC - Encapsulate ingestion logic in reusable classes
# MAGIC - Track metrics (events sent, failures, latency)
# MAGIC - Log errors for debugging and monitoring
# MAGIC
# MAGIC ### Performance Results
# MAGIC
# MAGIC From this lab:
# MAGIC - **Single event**: ~100-500ms per request
# MAGIC - **Batch (100 events)**: ~1,000-10,000 events/second
# MAGIC - **Parallel batches**: ~30,000-50,000 events/second
# MAGIC
# MAGIC ### Next Lab
# MAGIC
# MAGIC Proceed to **Lab 4: Schema Evolution and Data Transformations** to learn how to:
# MAGIC - Handle evolving event schemas
# MAGIC - Implement automatic schema merging
# MAGIC - Add data quality checks
# MAGIC - Transform streaming data with Delta Live Tables
