#!/usr/bin/env python3
"""
Clickstream Event Generator

Simulates website clickstream events and sends them to a Zerobus endpoint.
Generates realistic user sessions, page views, and actions.

Usage:
    python clickstream_generator.py --endpoint URL --token TOKEN [options]

Example:
    python clickstream_generator.py \
        --endpoint https://my-workspace.databricks.com/api/2.0/zerobus/v1/ingest/catalog/schema/connection \
        --token dapi1234567890abcdef \
        --users 50 \
        --rate 120 \
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


class ClickstreamSimulator:
    """Simulates website clickstream with realistic user behavior."""
    
    def __init__(self, num_users: int = 50):
        self.num_users = num_users
        self.sessions = {}
        
        # Website structure
        self.pages = {
            'home': {'path': '/', 'avg_duration': 15},
            'products': {'path': '/products', 'avg_duration': 30},
            'product_detail': {'path': '/products/{id}', 'avg_duration': 45},
            'cart': {'path': '/cart', 'avg_duration': 20},
            'checkout': {'path': '/checkout', 'avg_duration': 60},
            'confirmation': {'path': '/order/confirmation', 'avg_duration': 10},
            'search': {'path': '/search', 'avg_duration': 25},
            'blog': {'path': '/blog/{slug}', 'avg_duration': 120},
            'contact': {'path': '/contact', 'avg_duration': 40},
            'profile': {'path': '/profile', 'avg_duration': 30}
        }
        
        self.event_types = [
            'page_view', 'click', 'scroll', 'search', 
            'add_to_cart', 'remove_from_cart', 'purchase', 
            'form_submit', 'video_play', 'download'
        ]
        
        self.referrers = [
            'google.com', 'facebook.com', 'twitter.com', 
            'linkedin.com', 'direct', 'email_campaign'
        ]
        
        self.devices = [
            ('mobile', 'iOS', 'Safari'),
            ('mobile', 'Android', 'Chrome'),
            ('desktop', 'Windows', 'Chrome'),
            ('desktop', 'Mac', 'Safari'),
            ('tablet', 'iOS', 'Safari')
        ]
        
        self.locations = [
            ('US', 'California', 'San Francisco'),
            ('US', 'New York', 'New York'),
            ('US', 'Texas', 'Austin'),
            ('UK', 'England', 'London'),
            ('CA', 'Ontario', 'Toronto'),
            ('AU', 'NSW', 'Sydney')
        ]
    
    def get_or_create_session(self, user_id: str) -> Dict[str, Any]:
        """Get existing session or create new one for user."""
        
        # Check if session exists and is still active (< 30 min old)
        if user_id in self.sessions:
            session = self.sessions[user_id]
            if time.time() - session['start_time'] < 1800:  # 30 minutes
                return session
        
        # Create new session
        device_type, os, browser = random.choice(self.devices)
        country, region, city = random.choice(self.locations)
        
        session = {
            'session_id': str(uuid4()),
            'user_id': user_id,
            'start_time': time.time(),
            'page_views': 0,
            'referrer': random.choice(self.referrers),
            'device_type': device_type,
            'os': os,
            'browser': browser,
            'country': country,
            'region': region,
            'city': city,
            'current_page': 'home'
        }
        
        self.sessions[user_id] = session
        return session
    
    def generate_page_transition(self, current_page: str) -> str:
        """Determine next page based on current page (simulate user flow)."""
        
        transitions = {
            'home': ['products', 'search', 'blog', 'products'],
            'products': ['product_detail', 'product_detail', 'search', 'home'],
            'product_detail': ['cart', 'products', 'cart', 'home'],
            'cart': ['checkout', 'products', 'home'],
            'checkout': ['confirmation', 'cart'],
            'confirmation': ['home', 'products'],
            'search': ['products', 'product_detail', 'home'],
            'blog': ['home', 'products', 'blog'],
            'contact': ['home'],
            'profile': ['home', 'products']
        }
        
        possible_next = transitions.get(current_page, ['home'])
        return random.choice(possible_next)
    
    def generate_event(self) -> Dict[str, Any]:
        """Generate a single clickstream event."""
        
        # Select random user
        user_id = f'user_{random.randint(1, self.num_users):05d}'
        session = self.get_or_create_session(user_id)
        
        # Determine event type based on current page
        if session['page_views'] == 0:
            event_type = 'page_view'
        else:
            # Weight event types based on typical behavior
            event_type = random.choices(
                ['page_view', 'click', 'scroll', 'add_to_cart', 'search'],
                weights=[40, 30, 20, 5, 5]
            )[0]
        
        # Generate page transition for page_view events
        if event_type == 'page_view':
            session['current_page'] = self.generate_page_transition(session['current_page'])
            session['page_views'] += 1
        
        page_info = self.pages[session['current_page']]
        page_path = page_info['path']
        
        # Replace placeholders in paths
        if '{id}' in page_path:
            page_path = page_path.replace('{id}', f'prod_{random.randint(1000, 9999)}')
        if '{slug}' in page_path:
            page_path = page_path.replace('{slug}', f'article-{random.randint(1, 100)}')
        
        # Generate event
        event = {
            'event_id': str(uuid4()),
            'event_type': event_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'user_id': user_id,
            'session_id': session['session_id'],
            'page_url': page_path,
            'page_title': session['current_page'].replace('_', ' ').title(),
            'referrer': session['referrer'],
            'device_type': session['device_type'],
            'os': session['os'],
            'browser': session['browser'],
            'country': session['country'],
            'region': session['region'],
            'city': session['city'],
            'ingestion_time': datetime.now(timezone.utc).isoformat()
        }
        
        # Add event-specific properties
        if event_type == 'search':
            event['search_query'] = random.choice([
                'laptop', 'phone', 'headphones', 'watch', 
                'camera', 'keyboard', 'monitor'
            ])
        elif event_type in ['add_to_cart', 'remove_from_cart']:
            event['product_id'] = f'prod_{random.randint(1000, 9999)}'
            event['product_price'] = round(random.uniform(10, 500), 2)
        elif event_type == 'purchase':
            event['order_id'] = f'order_{uuid4().hex[:12]}'
            event['order_value'] = round(random.uniform(50, 1000), 2)
            event['items_count'] = random.randint(1, 5)
        
        return event
    
    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of clickstream events."""
        return [self.generate_event() for _ in range(batch_size)]


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
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return False
        except Exception as e:
            print(f"  Exception: {str(e)}")
            return False
    
    return False


def main():
    parser = argparse.ArgumentParser(
        description='Clickstream Event Generator for Zerobus',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--endpoint', required=True, help='Zerobus endpoint URL')
    parser.add_argument('--token', required=True, help='Authentication token')
    parser.add_argument('--users', type=int, default=50, help='Number of concurrent users (default: 50)')
    parser.add_argument('--rate', type=int, default=120, help='Events per minute (default: 120)')
    parser.add_argument('--batch-size', type=int, default=50, help='Events per batch (default: 50)')
    parser.add_argument('--duration', type=int, default=300, help='Duration in seconds (0 = infinite, default: 300)')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("CLICKSTREAM EVENT GENERATOR")
    print("=" * 80)
    print(f"Concurrent users: {args.users}")
    print(f"Rate: {args.rate} events/minute")
    print(f"Batch size: {args.batch_size}")
    print(f"Duration: {'infinite' if args.duration == 0 else f'{args.duration} seconds'}")
    print("=" * 80)
    
    simulator = ClickstreamSimulator(num_users=args.users)
    
    batch_interval = 60.0 / (args.rate / args.batch_size)
    
    print(f"\nGenerating {args.batch_size} events every {batch_interval:.1f} seconds...")
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
            
            print(f"{status} Sent {len(batch)} events | "
                  f"Total: {total_sent} | Failed: {total_failed} | "
                  f"Throughput: {throughput:.1f} events/sec")
            
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
