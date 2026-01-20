#!/usr/bin/env python3
"""
Application Log Event Generator

Simulates application log events from a microservices architecture
and sends them to a Zerobus endpoint.

Usage:
    python application_log_generator.py --endpoint URL --token TOKEN [options]

Example:
    python application_log_generator.py \
        --endpoint https://my-workspace.databricks.com/api/2.0/zerobus/v1/ingest/catalog/schema/connection \
        --token dapi1234567890abcdef \
        --services 10 \
        --rate 200 \
        --duration 600
"""

import argparse
import json
import random
import time
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any
from uuid import uuid4
import requests


class ApplicationLogSimulator:
    """Simulates application logs from microservices."""
    
    def __init__(self, num_services: int = 10):
        self.services = [
            'api-gateway', 'auth-service', 'user-service', 
            'product-service', 'order-service', 'payment-service',
            'notification-service', 'search-service', 'analytics-service',
            'cache-service', 'db-proxy', 'file-service'
        ][:num_services]
        
        self.log_levels = {
            'DEBUG': 0.20,
            'INFO': 0.50,
            'WARN': 0.20,
            'ERROR': 0.08,
            'FATAL': 0.02
        }
        
        self.environments = ['production', 'staging', 'development']
        
        # Log message templates by level
        self.messages = {
            'DEBUG': [
                'Processing request: method={method} path={path}',
                'Cache lookup: key={key} hit={hit}',
                'Database query: table={table} duration={duration}ms',
                'Function call: {function} params={params}',
                'State transition: {from_state} -> {to_state}'
            ],
            'INFO': [
                'Request completed: status={status} duration={duration}ms',
                'User login successful: user_id={user_id}',
                'Payment processed: transaction_id={transaction_id} amount=${amount}',
                'Order created: order_id={order_id} items={items}',
                'Email sent: recipient={email} template={template}',
                'Cache invalidated: key={key}',
                'Health check passed'
            ],
            'WARN': [
                'High memory usage: {usage}% of {total}MB',
                'Slow query detected: {duration}ms threshold=500ms',
                'Rate limit approaching: {current}/{max} requests',
                'Deprecated API usage: {endpoint}',
                'Connection pool nearly exhausted: {active}/{max} connections',
                'Response time degraded: p95={p95}ms threshold=1000ms'
            ],
            'ERROR': [
                'Database connection failed: {error}',
                'External API call failed: {service} status={status}',
                'Payment gateway error: {gateway} error_code={code}',
                'Authentication failed: {reason}',
                'Validation error: {field} {message}',
                'Resource not found: {resource_type} id={id}',
                'Timeout exceeded: operation={operation} timeout={timeout}s'
            ],
            'FATAL': [
                'Service crashed: {error}',
                'Database connection pool exhausted',
                'Out of memory: heap={heap}MB limit={limit}MB',
                'Unrecoverable error in critical path: {error}',
                'Circuit breaker opened: {service} failures={failures}'
            ]
        }
        
        self.http_methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
        self.paths = [
            '/api/v1/users', '/api/v1/products', '/api/v1/orders',
            '/api/v1/payments', '/api/v1/search', '/api/v1/auth/login'
        ]
        self.status_codes = [200, 201, 204, 400, 401, 403, 404, 500, 502, 503]
        
    def generate_log_entry(self) -> Dict[str, Any]:
        """Generate a single log entry."""
        
        # Select log level based on weights
        level = random.choices(
            list(self.log_levels.keys()),
            weights=list(self.log_levels.values())
        )[0]
        
        service = random.choice(self.services)
        environment = random.choices(
            self.environments,
            weights=[0.7, 0.2, 0.1]  # More production logs
        )[0]
        
        # Select and format message template
        message_template = random.choice(self.messages[level])
        
        # Generate values for placeholders
        values = {
            'method': random.choice(self.http_methods),
            'path': random.choice(self.paths),
            'status': random.choice(self.status_codes),
            'duration': random.randint(10, 2000),
            'key': f'cache_{random.randint(1000, 9999)}',
            'hit': random.choice(['true', 'false']),
            'table': random.choice(['users', 'products', 'orders', 'payments']),
            'function': f'process_{random.choice(["order", "payment", "user"])}',
            'params': json.dumps({'id': random.randint(1, 1000)}),
            'from_state': random.choice(['pending', 'processing', 'completed']),
            'to_state': random.choice(['processing', 'completed', 'failed']),
            'user_id': f'user_{random.randint(1, 10000)}',
            'transaction_id': f'txn_{uuid4().hex[:12]}',
            'amount': round(random.uniform(10, 1000), 2),
            'order_id': f'order_{uuid4().hex[:12]}',
            'items': random.randint(1, 10),
            'email': f'user{random.randint(1, 1000)}@example.com',
            'template': random.choice(['welcome', 'order_confirmation', 'password_reset']),
            'usage': random.randint(70, 95),
            'total': random.choice([1024, 2048, 4096]),
            'current': random.randint(80, 100),
            'max': 100,
            'endpoint': random.choice(self.paths),
            'active': random.randint(15, 20),
            'p95': random.randint(800, 1500),
            'error': random.choice([
                'Connection refused', 'Timeout', 'Invalid credentials',
                'Resource not found', 'Permission denied'
            ]),
            'service': random.choice(['payment-gateway', 'email-service', 'sms-service']),
            'gateway': random.choice(['stripe', 'paypal', 'square']),
            'code': f'ERR_{random.randint(1000, 9999)}',
            'reason': random.choice(['invalid_token', 'expired_session', 'missing_credentials']),
            'field': random.choice(['email', 'password', 'amount', 'quantity']),
            'message': 'is required',
            'resource_type': random.choice(['user', 'product', 'order']),
            'id': random.randint(1, 10000),
            'operation': random.choice(['database_query', 'api_call', 'file_upload']),
            'timeout': random.randint(5, 30),
            'heap': random.randint(3500, 4000),
            'limit': 4096,
            'failures': random.randint(5, 20)
        }
        
        # Format message
        message = message_template
        for key, value in values.items():
            message = message.replace(f'{{{key}}}', str(value))
        
        # Generate log entry
        log_entry = {
            'log_id': str(uuid4()),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'level': level,
            'service': service,
            'environment': environment,
            'message': message,
            'host': f'{service}-{random.randint(1, 5)}.{environment}.internal',
            'thread': f'thread-{random.randint(1, 20)}',
            'ingestion_time': datetime.now(timezone.utc).isoformat()
        }
        
        # Add structured fields based on log level
        if level in ['ERROR', 'FATAL']:
            log_entry['stack_trace'] = f'at {service}.Handler.process()\\nat com.example.core.BaseHandler.handle()'
            log_entry['error_code'] = f'ERR_{random.randint(1000, 9999)}'
        
        # Add trace ID for request correlation
        if random.random() < 0.8:  # 80% of logs have trace ID
            log_entry['trace_id'] = uuid4().hex[:16]
            log_entry['span_id'] = uuid4().hex[:8]
        
        return log_entry
    
    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of log entries."""
        return [self.generate_log_entry() for _ in range(batch_size)]


def send_to_zerobus(endpoint_url: str, auth_token: str, events: List[Dict[str, Any]], 
                    max_retries: int = 3) -> bool:
    """Send events to Zerobus endpoint with retry logic."""
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(endpoint_url, headers=headers, json=events, timeout=30)
            
            if response.status_code == 200:
                return True
            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
            else:
                print(f"  Error {response.status_code}: {response.text}")
                return False
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return False
    
    return False


def main():
    parser = argparse.ArgumentParser(
        description='Application Log Event Generator for Zerobus',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--endpoint', required=True, help='Zerobus endpoint URL')
    parser.add_argument('--token', required=True, help='Authentication token')
    parser.add_argument('--services', type=int, default=10, help='Number of services (default: 10)')
    parser.add_argument('--rate', type=int, default=200, help='Log events per minute (default: 200)')
    parser.add_argument('--batch-size', type=int, default=100, help='Events per batch (default: 100)')
    parser.add_argument('--duration', type=int, default=300, help='Duration in seconds (0 = infinite, default: 300)')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("APPLICATION LOG EVENT GENERATOR")
    print("=" * 80)
    print(f"Services: {args.services}")
    print(f"Rate: {args.rate} log events/minute")
    print(f"Batch size: {args.batch_size}")
    print(f"Duration: {'infinite' if args.duration == 0 else f'{args.duration} seconds'}")
    print("=" * 80)
    
    simulator = ApplicationLogSimulator(num_services=args.services)
    
    batch_interval = 60.0 / (args.rate / args.batch_size)
    
    print(f"\nGenerating {args.batch_size} log events every {batch_interval:.1f} seconds...")
    print("Press Ctrl+C to stop\n")
    
    total_sent = 0
    total_failed = 0
    start_time = time.time()
    
    try:
        while True:
            batch_start = time.time()
            
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
            
            # Count by level in batch
            levels = {}
            for event in batch:
                levels[event['level']] = levels.get(event['level'], 0) + 1
            
            level_str = ' '.join([f"{k}:{v}" for k, v in sorted(levels.items())])
            
            print(f"{status} Sent {len(batch)} events ({level_str}) | "
                  f"Total: {total_sent} | Throughput: {throughput:.1f} events/sec")
            
            if args.duration > 0 and elapsed >= args.duration:
                break
            
            batch_duration = time.time() - batch_start
            sleep_time = max(0, batch_interval - batch_duration)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\n\nStopping generator...")
    
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
