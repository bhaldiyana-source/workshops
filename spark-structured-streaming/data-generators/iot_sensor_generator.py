"""
IoT Sensor Data Generator

Generates realistic IoT sensor data with multiple sensor types and patterns.
Includes anomaly generation for testing alerting systems.

Usage:
    python iot_sensor_generator.py --sensors 20 --rate 50 --duration 300 --output /tmp/iot_data/

Author: Ajit Kalura
"""

import json
import time
import random
import math
import argparse
from datetime import datetime
from pathlib import Path


class IoTSensorGenerator:
    """Generate realistic IoT sensor data"""
    
    def __init__(self, num_sensors=10, rate=10, output_path="/tmp/iot_data/"):
        self.num_sensors = num_sensors
        self.rate = rate  # readings per second (across all sensors)
        self.output_path = output_path
        self.reading_count = 0
        self.batch_count = 0
        self.sensor_states = {}  # Track state for each sensor
        
        # Initialize sensor states
        for i in range(num_sensors):
            self.sensor_states[i] = {
                "last_temp": random.uniform(68, 75),
                "last_humidity": random.randint(40, 60),
                "last_pressure": random.uniform(1010, 1015),
                "battery_level": 100,
                "status": "normal"
            }
    
    def generate_temperature_reading(self, sensor_id):
        """Generate realistic temperature reading with drift"""
        state = self.sensor_states[sensor_id]
        
        # Simulate gradual temperature change
        drift = random.uniform(-0.5, 0.5)
        new_temp = state["last_temp"] + drift
        
        # Occasionally inject anomalies
        if random.random() < 0.02:  # 2% chance of anomaly
            new_temp += random.uniform(-15, 15)
            state["status"] = "warning"
        else:
            state["status"] = "normal"
        
        # Bound temperature
        new_temp = max(50, min(100, new_temp))
        state["last_temp"] = new_temp
        
        return round(new_temp, 2)
    
    def generate_humidity_reading(self, sensor_id):
        """Generate realistic humidity reading"""
        state = self.sensor_states[sensor_id]
        
        # Simulate humidity changes
        drift = random.randint(-2, 2)
        new_humidity = state["last_humidity"] + drift
        
        # Bound humidity
        new_humidity = max(20, min(90, new_humidity))
        state["last_humidity"] = new_humidity
        
        return new_humidity
    
    def generate_pressure_reading(self, sensor_id):
        """Generate realistic pressure reading"""
        state = self.sensor_states[sensor_id]
        
        # Simulate pressure changes
        drift = random.uniform(-0.2, 0.2)
        new_pressure = state["last_pressure"] + drift
        
        # Bound pressure
        new_pressure = max(1000, min(1030, new_pressure))
        state["last_pressure"] = new_pressure
        
        return round(new_pressure, 2)
    
    def update_battery(self, sensor_id):
        """Update battery level"""
        state = self.sensor_states[sensor_id]
        
        # Drain battery slightly
        state["battery_level"] = max(0, state["battery_level"] - random.uniform(0, 0.01))
        
        # Simulate battery alerts
        if state["battery_level"] < 20:
            state["status"] = "low_battery"
        
        return round(state["battery_level"], 1)
    
    def generate_sensor_reading(self):
        """Generate complete sensor reading"""
        sensor_id = random.randint(0, self.num_sensors - 1)
        
        reading = {
            "sensor_id": f"sensor_{sensor_id:03d}",
            "device_type": random.choice(["indoor", "outdoor", "industrial"]),
            "temperature": self.generate_temperature_reading(sensor_id),
            "temperature_unit": "celsius",
            "humidity": self.generate_humidity_reading(sensor_id),
            "humidity_unit": "percent",
            "pressure": self.generate_pressure_reading(sensor_id),
            "pressure_unit": "hPa",
            "battery_level": self.update_battery(sensor_id),
            "signal_strength": random.randint(-90, -30),  # dBm
            "status": self.sensor_states[sensor_id]["status"],
            "timestamp": datetime.now().isoformat(),
            "location": {
                "latitude": round(37.7749 + random.uniform(-0.1, 0.1), 6),
                "longitude": round(-122.4194 + random.uniform(-0.1, 0.1), 6)
            }
        }
        
        return reading
    
    def write_batch(self, records):
        """Write batch of records to JSON file"""
        output_file = f"{self.output_path}/batch_{self.batch_count:06d}.json"
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')
        
        # Print summary
        anomalies = sum(1 for r in records if r['status'] != 'normal')
        print(f"Batch {self.batch_count:04d}: {len(records)} readings "
              f"({anomalies} anomalies) -> {output_file}")
        
        self.batch_count += 1
    
    def run(self, duration=300):
        """
        Run the IoT sensor generator
        
        Args:
            duration: How long to generate data (seconds)
        """
        print("IoT Sensor Data Generator")
        print("=" * 60)
        print(f"Number of Sensors: {self.num_sensors}")
        print(f"Reading Rate: {self.rate} readings/second (total)")
        print(f"Duration: {duration} seconds")
        print(f"Output Path: {self.output_path}")
        print(f"Expected Total Readings: ~{self.rate * duration}")
        print("=" * 60)
        
        start_time = time.time()
        batch_interval = 5.0  # Write batch every 5 seconds
        batch_records = []
        last_batch_time = start_time
        
        while (time.time() - start_time) < duration:
            # Generate reading
            reading = self.generate_sensor_reading()
            batch_records.append(reading)
            self.reading_count += 1
            
            # Write batch periodically
            if (time.time() - last_batch_time) >= batch_interval:
                if batch_records:
                    self.write_batch(batch_records)
                    batch_records = []
                last_batch_time = time.time()
            
            # Sleep to maintain rate
            time.sleep(1.0 / self.rate)
        
        # Write final batch
        if batch_records:
            self.write_batch(batch_records)
        
        print("=" * 60)
        print(f"✅ Generation completed")
        print(f"Total Readings: {self.reading_count}")
        print(f"Total Batches: {self.batch_count}")
        print(f"Actual Rate: {self.reading_count / duration:.2f} readings/second")
        
        # Status summary
        print("\nFinal Sensor States:")
        for sensor_id in range(min(5, self.num_sensors)):  # Show first 5
            state = self.sensor_states[sensor_id]
            print(f"  Sensor {sensor_id:03d}: Temp={state['last_temp']:.1f}°C, "
                  f"Humidity={state['last_humidity']}%, "
                  f"Battery={state['battery_level']:.1f}%")


def main():
    parser = argparse.ArgumentParser(description="Generate IoT sensor data")
    parser.add_argument("--sensors", type=int, default=10,
                       help="Number of sensors")
    parser.add_argument("--rate", type=int, default=10,
                       help="Total readings per second")
    parser.add_argument("--duration", type=int, default=300,
                       help="Duration in seconds")
    parser.add_argument("--output", default="/tmp/iot_data/",
                       help="Output directory path")
    
    args = parser.parse_args()
    
    generator = IoTSensorGenerator(
        num_sensors=args.sensors,
        rate=args.rate,
        output_path=args.output
    )
    
    generator.run(duration=args.duration)


if __name__ == "__main__":
    main()
