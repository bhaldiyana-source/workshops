#!/usr/bin/env python3
"""
IoT Sensor Event Generator

Simulates IoT sensor data and sends it to a Zerobus endpoint.
Generates realistic temperature, humidity, pressure, and air quality readings.

Usage:
    python iot_sensor_generator.py --endpoint URL --token TOKEN [options]

Example:
    python iot_sensor_generator.py \
        --endpoint https://my-workspace.databricks.com/api/2.0/zerobus/v1/ingest/catalog/schema/connection \
        --token dapi1234567890abcdef \
        --sensors 10 \
        --rate 60 \
        --duration 300
"""

import argparse
import json
import random
import time
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any
import requests


class IoTSensorSimulator:
    """Simulates IoT sensors with realistic data patterns."""
    
    def __init__(self, num_sensors: int = 10, locations: List[str] = None):
        self.sensors = []
        self.locations = locations or [
            "Building A - Floor 1",
            "Building A - Floor 2", 
            "Building B - Floor 1",
            "Building C - Floor 1",
            "Data Center"
        ]
        
        # Initialize sensors
        for i in range(num_sensors):
            self.sensors.append({
                'sensor_id': f'SENSOR-{i+1:03d}',
                'location': random.choice(self.locations),
                'baseline_temp': random.uniform(20.0, 24.0),
                'baseline_humidity': random.uniform(40.0, 60.0),
                'baseline_pressure': random.uniform(1010.0, 1016.0),
                'battery_level': random.uniform(80.0, 100.0),
                'battery_drain_rate': random.uniform(0.01, 0.05)  # % per reading
            })
    
    def generate_reading(self, sensor: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a single sensor reading with realistic variations."""
        
        # Add time-based variations
        hour = datetime.now().hour
        
        # Temperature variations (higher during day)
        if 9 <= hour <= 17:  # Business hours
            temp_offset = random.uniform(0.5, 2.0)
        else:
            temp_offset = random.uniform(-1.0, 0.0)
        
        # Random variations
        temp_variation = random.gauss(0, 0.3)
        humidity_variation = random.gauss(0, 2.0)
        pressure_variation = random.gauss(0, 0.5)
        
        # Occasionally simulate anomalies (5% chance)
        if random.random() < 0.05:
            temp_anomaly = random.choice([5.0, -3.0])
        else:
            temp_anomaly = 0
        
        # Generate reading
        reading = {
            'sensor_id': sensor['sensor_id'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'temperature': round(sensor['baseline_temp'] + temp_offset + temp_variation + temp_anomaly, 2),
            'humidity': round(max(0, min(100, sensor['baseline_humidity'] + humidity_variation)), 2),
            'pressure': round(sensor['baseline_pressure'] + pressure_variation, 2),
            'location': sensor['location'],
            'ingestion_time': datetime.now(timezone.utc).isoformat()
        }
        
        # Add optional fields with 80% probability
        if random.random() < 0.8:
            reading['battery_level'] = round(max(0, sensor['battery_level']), 2)
            reading['signal_strength'] = random.randint(-75, -30)
            reading['firmware_version'] = f"2.{random.randint(0, 3)}.0"
            reading['device_status'] = 'active' if sensor['battery_level'] > 10 else 'low_battery'
        
        # Update battery level
        sensor['battery_level'] -= sensor['battery_drain_rate']
        if sensor['battery_level'] < 0:
            sensor['battery_level'] = 100  # Simulate battery replacement
        
        return reading
    
    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of sensor readings."""
        readings = []
        for _ in range(batch_size):
            sensor = random.choice(self.sensors)
            readings.append(self.generate_reading(sensor))
        return readings


def send_to_zerobus(endpoint_url: str, auth_token: str, events: List[Dict[str, Any]], 
                    max_retries: int = 3) -> bool:
    """
    Send events to Zerobus endpoint with retry logic.
    
    Args:
        endpoint_url: Zerobus HTTP endpoint
        auth_token: Authentication token
        events: List of event dictionaries
        max_retries: Maximum retry attempts
        
    Returns:
        True if successful, False otherwise
    """
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                endpoint_url,
                headers=headers,
                json=events,
                timeout=30
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                # Retryable errors
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"  Retry after {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
            else:
                # Non-retryable error
                print(f"  Error {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"  Timeout - retrying (attempt {attempt + 1}/{max_retries})")
                time.sleep(2 ** attempt)
            else:
                print("  Request timeout after max retries")
                return False
        except Exception as e:
            print(f"  Exception: {str(e)}")
            return False
    
    return False


def main():
    parser = argparse.ArgumentParser(
        description='IoT Sensor Event Generator for Zerobus',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send 100 events per minute for 10 minutes from 5 sensors
  python iot_sensor_generator.py --endpoint URL --token TOKEN --sensors 5 --rate 100 --duration 600
  
  # Continuous operation with 20 sensors
  python iot_sensor_generator.py --endpoint URL --token TOKEN --sensors 20 --rate 200 --duration 0
        """
    )
    
    parser.add_argument('--endpoint', required=True, help='Zerobus endpoint URL')
    parser.add_argument('--token', required=True, help='Authentication token')
    parser.add_argument('--sensors', type=int, default=10, help='Number of sensors to simulate (default: 10)')
    parser.add_argument('--rate', type=int, default=60, help='Events per minute (default: 60)')
    parser.add_argument('--batch-size', type=int, default=50, help='Events per batch (default: 50)')
    parser.add_argument('--duration', type=int, default=300, help='Duration in seconds (0 = infinite, default: 300)')
    parser.add_argument('--verbose', action='store_true', help='Show detailed output')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.rate < 1:
        print("Error: rate must be >= 1")
        sys.exit(1)
    if args.batch_size < 1 or args.batch_size > 1000:
        print("Error: batch-size must be between 1 and 1000")
        sys.exit(1)
    
    # Initialize simulator
    print("=" * 80)
    print("IoT SENSOR EVENT GENERATOR")
    print("=" * 80)
    print(f"Sensors: {args.sensors}")
    print(f"Rate: {args.rate} events/minute")
    print(f"Batch size: {args.batch_size}")
    print(f"Duration: {'infinite' if args.duration == 0 else f'{args.duration} seconds'}")
    print(f"Endpoint: {args.endpoint}")
    print("=" * 80)
    
    simulator = IoTSensorSimulator(num_sensors=args.sensors)
    
    # Calculate timing
    interval = 60.0 / args.rate  # seconds between events
    batches_per_minute = args.rate / args.batch_size
    batch_interval = 60.0 / batches_per_minute
    
    print(f"\nGenerating {args.batch_size} events every {batch_interval:.1f} seconds...")
    print("Press Ctrl+C to stop\n")
    
    # Statistics
    total_sent = 0
    total_failed = 0
    start_time = time.time()
    
    try:
        while True:
            batch_start = time.time()
            
            # Generate and send batch
            batch = simulator.generate_batch(args.batch_size)
            success = send_to_zerobus(args.endpoint, args.token, batch)
            
            if success:
                total_sent += len(batch)
                status = "✓"
            else:
                total_failed += len(batch)
                status = "✗"
            
            elapsed = time.time() - start_time
            throughput = total_sent / elapsed if elapsed > 0 else 0
            
            # Print progress
            print(f"{status} Sent {len(batch)} events | "
                  f"Total: {total_sent} | Failed: {total_failed} | "
                  f"Throughput: {throughput:.1f} events/sec")
            
            # Check duration
            if args.duration > 0 and elapsed >= args.duration:
                print(f"\nDuration limit reached ({args.duration}s)")
                break
            
            # Wait for next batch
            batch_duration = time.time() - batch_start
            sleep_time = max(0, batch_interval - batch_duration)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\n\nStopping generator...")
    
    # Final statistics
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Duration: {elapsed:.1f} seconds")
    print(f"Events sent: {total_sent}")
    print(f"Events failed: {total_failed}")
    print(f"Success rate: {total_sent/(total_sent+total_failed)*100:.1f}%" if (total_sent + total_failed) > 0 else "N/A")
    print(f"Average throughput: {total_sent/elapsed:.1f} events/second")
    print("=" * 80)


if __name__ == '__main__':
    main()
