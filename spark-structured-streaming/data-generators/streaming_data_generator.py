"""
Streaming Data Generator

Generates continuous streaming data for testing Spark Structured Streaming applications.
Can write to local files or DBFS with configurable rate and format.

Usage:
    python streaming_data_generator.py --format json --rate 10 --duration 60 --output /tmp/streaming_data/

Author: Ajit Kalura
"""

import json
import time
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path


class StreamingDataGenerator:
    """Generate streaming data with configurable rate and format"""
    
    def __init__(self, format="json", rate=10, output_path="/tmp/streaming_data/"):
        self.format = format.lower()
        self.rate = rate  # records per second
        self.output_path = output_path
        self.record_count = 0
        self.batch_count = 0
        
    def generate_iot_sensor_record(self):
        """Generate IoT sensor data"""
        sensor_id = random.randint(1, 20)
        temperature = round(random.uniform(60, 85), 2)
        humidity = random.randint(30, 70)
        pressure = round(random.uniform(1000, 1020), 2)
        timestamp = datetime.now().isoformat()
        
        return {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure,
            "timestamp": timestamp,
            "device_status": random.choice(["normal", "warning", "critical"])
        }
    
    def generate_clickstream_record(self):
        """Generate web clickstream data"""
        user_id = random.randint(1, 100)
        session_id = f"sess_{random.randint(1, 50):05d}"
        event_type = random.choice(["page_view", "click", "scroll", "purchase"])
        page_url = random.choice(["/home", "/products", "/cart", "/checkout", "/about"])
        timestamp = datetime.now().isoformat()
        
        return {
            "event_id": f"evt_{self.record_count:08d}",
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "page_url": page_url,
            "timestamp": timestamp,
            "duration_ms": random.randint(100, 5000),
            "ip_address": f"192.168.1.{random.randint(1, 255)}"
        }
    
    def generate_transaction_record(self):
        """Generate financial transaction data"""
        transaction_id = f"txn_{self.record_count:010d}"
        customer_id = random.randint(1, 1000)
        amount = round(random.uniform(10, 1000), 2)
        category = random.choice(["groceries", "electronics", "clothing", "dining", "travel"])
        timestamp = datetime.now().isoformat()
        
        return {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "amount": amount,
            "category": category,
            "timestamp": timestamp,
            "merchant": f"Merchant_{random.randint(1, 50)}",
            "status": random.choice(["approved", "pending", "declined"])
        }
    
    def write_json_batch(self, records):
        """Write records as JSON files"""
        output_file = f"{self.output_path}/batch_{self.batch_count:06d}.json"
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')
        
        print(f"Wrote batch {self.batch_count}: {len(records)} records to {output_file}")
        self.batch_count += 1
    
    def write_csv_batch(self, records):
        """Write records as CSV files"""
        import csv
        output_file = f"{self.output_path}/batch_{self.batch_count:06d}.csv"
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        
        if records:
            keys = records[0].keys()
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(records)
            
            print(f"Wrote batch {self.batch_count}: {len(records)} records to {output_file}")
            self.batch_count += 1
    
    def run(self, duration=60, data_type="iot"):
        """
        Run the generator for specified duration
        
        Args:
            duration: How long to generate data (seconds)
            data_type: Type of data to generate (iot, clickstream, transaction)
        """
        print(f"Starting {data_type} data generator...")
        print(f"Rate: {self.rate} records/second")
        print(f"Format: {self.format}")
        print(f"Duration: {duration} seconds")
        print(f"Output: {self.output_path}")
        print("-" * 60)
        
        start_time = time.time()
        batch_interval = 1.0  # Write batch every second
        batch_records = []
        
        # Select generator function
        if data_type == "iot":
            generator_func = self.generate_iot_sensor_record
        elif data_type == "clickstream":
            generator_func = self.generate_clickstream_record
        elif data_type == "transaction":
            generator_func = self.generate_transaction_record
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        while (time.time() - start_time) < duration:
            batch_start = time.time()
            
            # Generate records for this second
            for _ in range(self.rate):
                record = generator_func()
                batch_records.append(record)
                self.record_count += 1
                
                # Small delay to distribute throughout the second
                time.sleep(1.0 / self.rate)
            
            # Write batch
            if self.format == "json":
                self.write_json_batch(batch_records)
            elif self.format == "csv":
                self.write_csv_batch(batch_records)
            else:
                raise ValueError(f"Unsupported format: {self.format}")
            
            batch_records = []
            
            # Sleep to maintain rate
            elapsed = time.time() - batch_start
            if elapsed < batch_interval:
                time.sleep(batch_interval - elapsed)
        
        print("-" * 60)
        print(f"✅ Generator completed")
        print(f"Total records generated: {self.record_count}")
        print(f"Total batches written: {self.batch_count}")
        print(f"Average rate: {self.record_count / duration:.2f} records/second")


def main():
    parser = argparse.ArgumentParser(description="Generate streaming data for testing")
    parser.add_argument("--format", choices=["json", "csv"], default="json",
                       help="Output format (json or csv)")
    parser.add_argument("--rate", type=int, default=10,
                       help="Records per second")
    parser.add_argument("--duration", type=int, default=60,
                       help="Duration in seconds")
    parser.add_argument("--output", default="/tmp/streaming_data/",
                       help="Output directory path")
    parser.add_argument("--type", choices=["iot", "clickstream", "transaction"],
                       default="iot", help="Type of data to generate")
    
    args = parser.parse_args()
    
    generator = StreamingDataGenerator(
        format=args.format,
        rate=args.rate,
        output_path=args.output
    )
    
    generator.run(duration=args.duration, data_type=args.type)


if __name__ == "__main__":
    main()
