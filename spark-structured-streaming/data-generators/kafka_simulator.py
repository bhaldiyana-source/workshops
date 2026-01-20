"""
Kafka Simulator

Simulates Kafka message generation for testing without requiring a Kafka cluster.
Generates JSON messages with Kafka-like structure (key, value, timestamp).

Usage:
    python kafka_simulator.py --topic sensor_events --rate 20 --duration 120

Author: Ajit Kalura
"""

import json
import time
import random
import argparse
from datetime import datetime


class KafkaSimulator:
    """Simulate Kafka messages for testing"""
    
    def __init__(self, topic="test_topic", rate=10):
        self.topic = topic
        self.rate = rate
        self.message_count = 0
        self.partition_count = 3  # Simulate 3 partitions
        
    def generate_sensor_message(self):
        """Generate IoT sensor message"""
        sensor_id = random.randint(1, 50)
        partition = sensor_id % self.partition_count
        
        value = {
            "sensor_id": sensor_id,
            "temperature": round(random.uniform(60, 90), 2),
            "humidity": random.randint(30, 80),
            "pressure": round(random.uniform(1000, 1020), 2),
            "battery_level": random.randint(20, 100),
            "event_time": datetime.now().isoformat()
        }
        
        return {
            "key": f"sensor_{sensor_id}",
            "value": json.dumps(value),
            "topic": self.topic,
            "partition": partition,
            "offset": self.message_count,
            "timestamp": int(time.time() * 1000),
            "timestampType": 0  # CreateTime
        }
    
    def generate_event_message(self):
        """Generate application event message"""
        user_id = random.randint(1, 200)
        partition = user_id % self.partition_count
        
        value = {
            "user_id": user_id,
            "event_type": random.choice(["login", "logout", "page_view", "click", "purchase"]),
            "event_data": {
                "page": random.choice(["/home", "/products", "/cart"]),
                "duration_ms": random.randint(100, 5000)
            },
            "event_time": datetime.now().isoformat()
        }
        
        return {
            "key": f"user_{user_id}",
            "value": json.dumps(value),
            "topic": self.topic,
            "partition": partition,
            "offset": self.message_count,
            "timestamp": int(time.time() * 1000),
            "timestampType": 0
        }
    
    def print_message(self, message):
        """Print message in Kafka-like format"""
        value_preview = message['value'][:100] + "..." if len(message['value']) > 100 else message['value']
        print(f"[{message['topic']}][{message['partition']}][{message['offset']}] "
              f"key={message['key']}, value={value_preview}")
    
    def run(self, duration=60, message_type="sensor", output_file=None):
        """
        Run the simulator
        
        Args:
            duration: How long to generate messages (seconds)
            message_type: Type of messages (sensor, event)
            output_file: Optional file to write messages (for later consumption)
        """
        print(f"Kafka Simulator Started")
        print(f"Topic: {self.topic}")
        print(f"Rate: {self.rate} messages/second")
        print(f"Message Type: {message_type}")
        print(f"Duration: {duration} seconds")
        print("-" * 80)
        
        # Select generator
        if message_type == "sensor":
            generator_func = self.generate_sensor_message
        elif message_type == "event":
            generator_func = self.generate_event_message
        else:
            raise ValueError(f"Unknown message type: {message_type}")
        
        start_time = time.time()
        messages = []
        
        while (time.time() - start_time) < duration:
            # Generate messages for this second
            for _ in range(self.rate):
                message = generator_func()
                messages.append(message)
                self.print_message(message)
                self.message_count += 1
                
                time.sleep(1.0 / self.rate)
        
        print("-" * 80)
        print(f"✅ Simulator completed")
        print(f"Total messages: {self.message_count}")
        print(f"Average rate: {self.message_count / duration:.2f} messages/second")
        
        # Optionally write to file
        if output_file:
            with open(output_file, 'w') as f:
                for msg in messages:
                    f.write(json.dumps(msg) + '\n')
            print(f"Messages written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Simulate Kafka message generation")
    parser.add_argument("--topic", default="test_topic",
                       help="Kafka topic name")
    parser.add_argument("--rate", type=int, default=10,
                       help="Messages per second")
    parser.add_argument("--duration", type=int, default=60,
                       help="Duration in seconds")
    parser.add_argument("--type", choices=["sensor", "event"], default="sensor",
                       help="Type of messages to generate")
    parser.add_argument("--output", help="Optional output file path")
    
    args = parser.parse_args()
    
    simulator = KafkaSimulator(topic=args.topic, rate=args.rate)
    simulator.run(duration=args.duration, message_type=args.type, output_file=args.output)


if __name__ == "__main__":
    main()
