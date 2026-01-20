#!/usr/bin/env python3
"""
Batch Event Sender

A generic utility for sending JSON events from files or stdin to Zerobus endpoints.
Supports various input formats and batch optimization.

Usage:
    # Send from JSON file
    python batch_event_sender.py --endpoint URL --token TOKEN --file events.json
    
    # Send from JSONL file (one JSON object per line)
    python batch_event_sender.py --endpoint URL --token TOKEN --file events.jsonl --format jsonl
    
    # Send from stdin
    cat events.json | python batch_event_sender.py --endpoint URL --token TOKEN
    
    # Send with custom batch size and parallelism
    python batch_event_sender.py --endpoint URL --token TOKEN --file events.json --batch-size 500 --parallel 5
"""

import argparse
import json
import sys
import time
from typing import List, Dict, Any, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests


def read_json_file(file_path: str) -> List[Dict[str, Any]]:
    """Read events from a JSON file (expects array of objects)."""
    with open(file_path, 'r') as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            raise ValueError("JSON file must contain an array or object")


def read_jsonl_file(file_path: str) -> List[Dict[str, Any]]:
    """Read events from a JSONL file (one JSON object per line)."""
    events = []
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
    return events


def read_stdin() -> List[Dict[str, Any]]:
    """Read events from stdin."""
    data = json.load(sys.stdin)
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return [data]
    else:
        raise ValueError("stdin must contain JSON array or object")


def batch_events(events: List[Dict[str, Any]], batch_size: int) -> Iterator[List[Dict[str, Any]]]:
    """Split events into batches."""
    for i in range(0, len(events), batch_size):
        yield events[i:i + batch_size]


def send_batch_to_zerobus(endpoint_url: str, auth_token: str, batch: List[Dict[str, Any]], 
                          batch_num: int, max_retries: int = 3) -> Dict[str, Any]:
    """
    Send a single batch to Zerobus with retry logic.
    
    Returns:
        Result dictionary with success status and metadata
    """
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }
    
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            response = requests.post(
                endpoint_url,
                headers=headers,
                json=batch,
                timeout=60
            )
            end_time = time.time()
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'batch_num': batch_num,
                    'event_count': len(batch),
                    'duration': end_time - start_time,
                    'attempts': attempt + 1
                }
            elif response.status_code in [408, 429, 500, 502, 503, 504]:
                # Retryable error
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    continue
            
            # Non-retryable error
            return {
                'success': False,
                'batch_num': batch_num,
                'event_count': len(batch),
                'error': f"HTTP {response.status_code}: {response.text[:200]}",
                'attempts': attempt + 1
            }
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return {
                    'success': False,
                    'batch_num': batch_num,
                    'event_count': len(batch),
                    'error': 'Request timeout',
                    'attempts': max_retries
                }
        except Exception as e:
            return {
                'success': False,
                'batch_num': batch_num,
                'event_count': len(batch),
                'error': str(e),
                'attempts': attempt + 1
            }
    
    return {
        'success': False,
        'batch_num': batch_num,
        'event_count': len(batch),
        'error': 'Max retries exceeded',
        'attempts': max_retries
    }


def send_batches_parallel(endpoint_url: str, auth_token: str, batches: List[List[Dict[str, Any]]],
                          max_workers: int = 5, verbose: bool = False) -> List[Dict[str, Any]]:
    """Send multiple batches in parallel."""
    
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batch jobs
        future_to_batch = {
            executor.submit(send_batch_to_zerobus, endpoint_url, auth_token, batch, idx): idx
            for idx, batch in enumerate(batches, 1)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_batch):
            batch_num = future_to_batch[future]
            try:
                result = future.result()
                results.append(result)
                
                if verbose or not result['success']:
                    status = "✓" if result['success'] else "✗"
                    msg = f"{status} Batch {result['batch_num']}/{len(batches)}: {result['event_count']} events"
                    if result['success']:
                        msg += f" in {result['duration']:.2f}s"
                    else:
                        msg += f" FAILED: {result['error']}"
                    print(msg)
                    
            except Exception as e:
                print(f"✗ Batch {batch_num} exception: {str(e)}")
                results.append({
                    'success': False,
                    'batch_num': batch_num,
                    'error': str(e)
                })
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Batch Event Sender for Zerobus',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send from JSON file with default settings
  python batch_event_sender.py --endpoint URL --token TOKEN --file events.json
  
  # Send JSONL with larger batches and more parallelism
  python batch_event_sender.py --endpoint URL --token TOKEN --file events.jsonl --format jsonl --batch-size 1000 --parallel 10
  
  # Read from stdin
  cat events.json | python batch_event_sender.py --endpoint URL --token TOKEN
        """
    )
    
    parser.add_argument('--endpoint', required=True, help='Zerobus endpoint URL')
    parser.add_argument('--token', required=True, help='Authentication token')
    parser.add_argument('--file', help='Input file path (omit to read from stdin)')
    parser.add_argument('--format', choices=['json', 'jsonl'], default='json',
                       help='Input file format (default: json)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Events per batch (default: 100, max: 1000)')
    parser.add_argument('--parallel', type=int, default=5,
                       help='Number of parallel threads (default: 5)')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Maximum retry attempts per batch (default: 3)')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed progress for each batch')
    parser.add_argument('--dry-run', action='store_true',
                       help='Parse events but do not send (validation only)')
    
    args = parser.parse_args()
    
    # Validate batch size
    if args.batch_size < 1 or args.batch_size > 1000:
        print("Error: batch-size must be between 1 and 1000")
        sys.exit(1)
    
    print("=" * 80)
    print("BATCH EVENT SENDER")
    print("=" * 80)
    
    # Read events
    try:
        if args.file:
            print(f"Reading events from: {args.file}")
            if args.format == 'jsonl':
                events = read_jsonl_file(args.file)
            else:
                events = read_json_file(args.file)
        else:
            print("Reading events from stdin...")
            events = read_stdin()
        
        print(f"Loaded {len(events)} events")
        
    except Exception as e:
        print(f"Error reading input: {e}")
        sys.exit(1)
    
    if len(events) == 0:
        print("No events to send")
        sys.exit(0)
    
    # Create batches
    batches = list(batch_events(events, args.batch_size))
    print(f"Split into {len(batches)} batches of up to {args.batch_size} events")
    print(f"Parallel threads: {args.parallel}")
    print("=" * 80)
    
    if args.dry_run:
        print("\nDRY RUN - Events validated but not sent")
        sys.exit(0)
    
    # Send batches
    print(f"\nSending {len(events)} events to Zerobus...\n")
    
    start_time = time.time()
    results = send_batches_parallel(
        args.endpoint,
        args.token,
        batches,
        max_workers=args.parallel,
        verbose=args.verbose
    )
    end_time = time.time()
    
    # Calculate statistics
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    total_events_sent = sum(r.get('event_count', 0) for r in successful)
    total_events_failed = sum(r.get('event_count', 0) for r in failed)
    
    duration = end_time - start_time
    throughput = total_events_sent / duration if duration > 0 else 0
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total duration: {duration:.2f} seconds")
    print(f"Total batches: {len(batches)}")
    print(f"Successful batches: {len(successful)}")
    print(f"Failed batches: {len(failed)}")
    print(f"Events sent: {total_events_sent}")
    print(f"Events failed: {total_events_failed}")
    print(f"Success rate: {total_events_sent/(total_events_sent+total_events_failed)*100:.1f}%")
    print(f"Average throughput: {throughput:.1f} events/second")
    print("=" * 80)
    
    # Show failed batches if any
    if failed:
        print(f"\n⚠️  {len(failed)} FAILED BATCHES:")
        for result in failed:
            print(f"  Batch {result['batch_num']}: {result.get('error', 'Unknown error')}")
        sys.exit(1)
    else:
        print("\n✅ All batches sent successfully!")
        sys.exit(0)


if __name__ == '__main__':
    main()
