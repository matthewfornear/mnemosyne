#!/usr/bin/env python3
"""
Async bulk initial search script with 4 workers that goes through facebook_group_urls.txt
and hits each URL with the initial search query (not pagination).
Enhanced for handling large numbers of URLs with resume capability and parallel processing.
REQUIRES Nimbleway proxy for security - will not function without proxy configuration.
"""

import requests
import json
import sys
import re
import time
import os
import asyncio
import aiohttp
from urllib.parse import unquote, urlparse, parse_qs
from datetime import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import signal
import sys
import urllib.parse

# Global proxy configuration (will be set during initialization)
PROXIES = None

# Global variables for graceful shutdown
shutdown_event = threading.Event()
processed_count = 0
successful_count = 0
failed_count = 0
skipped_count = 0
total_groups_found = 0
start_time = time.time()

# Emergency stop mechanism
worker_failure_counts = {}  # Track consecutive failures per worker
active_workers = set()      # Track which workers are still active
worker_lock = threading.Lock()  # Thread-safe access to worker tracking

def signal_handler(signum, frame):
    """Handle graceful shutdown"""
    print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
    shutdown_event.set()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def record_worker_failure(worker_id):
    """Record a failure for a worker and check if it should be dropped"""
    global worker_failure_counts, active_workers
    
    with worker_lock:
        # Initialize failure count if not exists
        if worker_id not in worker_failure_counts:
            worker_failure_counts[worker_id] = 0
        
        # Increment failure count
        worker_failure_counts[worker_id] += 1
        failure_count = worker_failure_counts[worker_id]
        
        print(f"🚨 Worker {worker_id} failure #{failure_count}/5")
        
        if failure_count >= 5:  # Increased from 3 to 5
            # Drop this worker
            if worker_id in active_workers:
                active_workers.remove(worker_id)
                print(f"💀 Worker {worker_id} DROPPED after 5 consecutive failures")
                print(f"   Active workers remaining: {len(active_workers)}")
                
                # Check if all workers are dropped
                if len(active_workers) == 0:
                    print("💀 CRITICAL: All workers have been dropped!")
                    print("🛑 Emergency stop - no workers remaining")
                    shutdown_event.set()
                    return True  # Signal that emergency stop was triggered
        
        return False

def record_worker_success(worker_id):
    """Record a success for a worker (resets failure count)"""
    global worker_failure_counts
    
    with worker_lock:
        if worker_id in worker_failure_counts and worker_failure_counts[worker_id] > 0:
            old_count = worker_failure_counts[worker_id]
            worker_failure_counts[worker_id] = 0
            print(f"✅ Worker {worker_id} failure count reset (was {old_count}/5)")

def get_active_worker_count():
    """Get the current number of active workers"""
    with worker_lock:
        return len(active_workers)

def add_worker_to_active_list(worker_id):
    """Add a worker to the active list"""
    with worker_lock:
        active_workers.add(worker_id)
        print(f"🚀 Worker {worker_id} added to active list (Total: {len(active_workers)})")

def remove_worker_from_active_list(worker_id):
    """Remove a worker from the active list (for normal completion)"""
    with worker_lock:
        if worker_id in active_workers:
            active_workers.remove(worker_id)
            print(f"✅ Worker {worker_id} completed normally (Active: {len(active_workers)})")

def load_nimbleway_settings():
    """Load Nimbleway proxy settings - REQUIRED for security"""
    global PROXIES
    
    # Always use Nimbleway proxy - no interactive selection needed
    print("🔒 Loading Nimbleway proxy settings...")
    
    # Nimbleway Proxy Integration - REQUIRED for security
    PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    NIMBLEWAY_SETTINGS_FILE = os.path.join(PARENT_DIR, "settings", "nimbleway_settings.json")
    if not os.path.exists(NIMBLEWAY_SETTINGS_FILE):
        print("❌ CRITICAL ERROR: Nimbleway settings file not found!")
        print(f"   Expected file: {NIMBLEWAY_SETTINGS_FILE}")
        print("   This is REQUIRED for security when using proxy mode.")
        print("   Please create this file with your Nimbleway credentials.")
        sys.exit(1)
    
    try:
        with open(NIMBLEWAY_SETTINGS_FILE, 'r', encoding='utf-8') as f:
            nimbleway_settings = json.load(f)
        
        # Validate required fields
        required_fields = ["accountName", "pipelineName", "pipelinePassword", "host", "port"]
        missing_fields = [field for field in required_fields if not nimbleway_settings.get(field)]
        
        if missing_fields:
            print("❌ CRITICAL ERROR: Missing required Nimbleway settings!")
            print(f"   Missing fields: {missing_fields}")
            print("   Please ensure all required fields are present in nimbleway_settings.json")
            sys.exit(1)
        
        ACCOUNT_NAME = nimbleway_settings.get("accountName")
        PIPELINE_NAME = nimbleway_settings.get("pipelineName")
        PIPELINE_PASSWORD = nimbleway_settings.get("pipelinePassword")
        NIMBLEWAY_HOST = nimbleway_settings.get("host", "ip.nimbleway.com")
        NIMBLEWAY_PORT = nimbleway_settings.get("port", "7000")
        
        # Use the correct Nimbleway format: account-accountName-pipeline-pipelineName:pipelinePassword
        # URL-encode the account name to handle spaces
        encoded_account_name = urllib.parse.quote(ACCOUNT_NAME)
        NIMBLEWAY_PROXY = f"http://account-{encoded_account_name}-pipeline-{PIPELINE_NAME}:{PIPELINE_PASSWORD}@{NIMBLEWAY_HOST}:{NIMBLEWAY_PORT}"
        
        PROXIES = {"http": NIMBLEWAY_PROXY, "https": NIMBLEWAY_PROXY}
        print("✅ Nimbleway proxy configuration loaded successfully")
        
    except json.JSONDecodeError as e:
        print("❌ CRITICAL ERROR: Invalid JSON in Nimbleway settings file!")
        print(f"   Error: {e}")
        print("   Please check the format of nimbleway_settings.json")
        sys.exit(1)
    except Exception as e:
        print("❌ CRITICAL ERROR: Failed to load Nimbleway settings!")
        print(f"   Error: {e}")
        print("   Please ensure nimbleway_settings.json is properly configured")
        sys.exit(1)

def parse_curl_command(curl_cmd):
    """Parse a curl command and extract headers, cookies, and data"""
    
    # Extract URL
    url_match = re.search(r"curl\s+['\"]([^'\"]+)['\"]", curl_cmd)
    if not url_match:
        print("❌ Could not extract URL from curl command")
        return None
    
    url = url_match.group(1)
    
    # Extract headers
    headers = {}
    header_matches = re.findall(r"-H\s+['\"]([^:]+):\s*([^'\"]+)['\"]", curl_cmd)
    for key, value in header_matches:
        headers[key] = value
    
    # Extract cookies
    cookies = {}
    cookie_match = re.search(r"-b\s+['\"]([^'\"]+)['\"]", curl_cmd)
    if cookie_match:
        cookie_string = cookie_match.group(1)
        # Parse cookie string
        for cookie in cookie_string.split('; '):
            if '=' in cookie:
                name, value = cookie.split('=', 1)
                cookies[name] = value
    
    # Extract data
    data = {}
    data_match = re.search(r"--data-raw\s+['\"]([^'\"]+)['\"]", curl_cmd)
    if data_match:
        data_string = data_match.group(1)
        # Parse form data
        for param in data_string.split('&'):
            if '=' in param:
                name, value = param.split('=', 1)
                # URL decode the value
                import urllib.parse
                data[name] = urllib.parse.unquote(value)
    
    return {
        'url': url,
        'headers': headers,
        'cookies': cookies,
        'data': data
    }

def extract_search_term_from_url(url):
    """Extract search term from Facebook search URL"""
    try:
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        search_term = query_params.get('q', [''])[0]
        # URL decode the search term
        return unquote(search_term)
    except Exception as e:
        print(f"❌ Error parsing URL {url}: {e}")
        return None

def create_initial_search_request(search_term, base_config):
    """Create an initial search request (not pagination)"""
    
    # Start with the base configuration
    data = base_config['data'].copy()
    
    # Update the search term in the variables
    if 'variables' in data:
        try:
            # Parse the variables JSON
            variables = json.loads(data['variables'])
            
            # Update the search text
            if 'args' in variables and 'text' in variables['args']:
                variables['args']['text'] = search_term
            
            # Ensure this is an initial search, not pagination
            if 'cursor' in variables:
                del variables['cursor']  # Remove cursor for initial search
            
            # Update count to get initial results
            variables['count'] = 5  # Initial batch size
            
            # Update the variables back
            data['variables'] = json.dumps(variables)
            
        except json.JSONDecodeError as e:
            print(f"⚠️  Could not parse variables JSON: {e}")
            return None
    
    return {
        'url': base_config['url'],
        'headers': base_config['headers'],
        'cookies': base_config['cookies'],
        'data': data
    }

def extract_groups_from_response(json_data, search_term):
    """Extract group information from Facebook response in the specified format"""
    groups = []
    
    try:
        if 'data' in json_data and 'serpResponse' in json_data['data']:
            edges = json_data['data']['serpResponse']['results']['edges']
            
            for edge in edges:
                if edge['node'].get('role') == 'ENTITY_GROUPS':
                    # The actual group data is nested in rendering_strategy.view_model.profile
                    if 'rendering_strategy' in edge:
                        rendering_strategy = edge['rendering_strategy']
                        
                        if 'view_model' in rendering_strategy:
                            view_model = rendering_strategy['view_model']
                            
                            if 'profile' in view_model:
                                profile = view_model['profile']
                                
                                # Check if this is actually a Group
                                if profile.get('__typename') == 'Group':
                                    # Extract group information
                                    group = {
                                        "id": str(profile.get('id', '')),
                                        "name": profile.get('name', 'Unknown'),
                                        "url": profile.get('url') or profile.get('profile_url') or f"https://www.facebook.com/groups/{profile.get('id', '')}/",
                                        "member_count": 0,  # Will be updated below
                                        "privacy": "Unknown",  # Facebook doesn't always provide this in search results
                                        "search_term": search_term,
                                        "scraped_at": datetime.now().isoformat()
                                    }
                                    
                                    # Try to extract member count from primary_snippet_text_with_entities if available
                                    if 'primary_snippet_text_with_entities' in view_model and 'text' in view_model['primary_snippet_text_with_entities']:
                                        snippet_text = view_model['primary_snippet_text_with_entities']['text']
                                        if snippet_text:
                                            # Look for numbers in the snippet (e.g., "Public · 1.2K members · 5 posts a day")
                                            member_match = re.search(r'([\d,]+(?:\.[\d]+)?[KMB]?)\s*members?', snippet_text, re.IGNORECASE)
                                            if member_match:
                                                member_str = member_match.group(1)
                                                # Convert K, M, B to actual numbers
                                                if 'K' in member_str.upper():
                                                    member_count = int(float(member_str.replace('K', '').replace(',', '')) * 1000)
                                                elif 'M' in member_str.upper():
                                                    member_count = int(float(member_str.replace('M', '').replace(',', '')) * 1000000)
                                                elif 'B' in member_str.upper():
                                                    member_count = int(float(member_str.replace('B', '').replace(',', '')) * 1000000000)
                                                else:
                                                    member_count = int(member_str.replace(',', ''))
                                                group["member_count"] = member_count
                                                
                                                # Also try to extract privacy from the snippet
                                                if 'Public' in snippet_text:
                                                    group["privacy"] = "Public"
                                                elif 'Private' in snippet_text:
                                                    group["privacy"] = "Private"
                                                elif 'Secret' in snippet_text:
                                                    group["privacy"] = "Secret"
                                    
                                    # Only add groups that have valid IDs
                                    if group["id"] and group["id"] != "":
                                        groups.append(group)
                                        print(f"      📝 Extracted group: {group['name']} (ID: {group['id']})")
                                    else:
                                        print(f"      ⚠️  Skipped group with invalid ID: {group.get('name', 'Unknown')}")
                                else:
                                    print(f"      ⚠️  Found non-Group entity: {profile.get('__typename', 'Unknown')}")
                            else:
                                print(f"      ⚠️  No profile in view_model")
                        else:
                            print(f"      ⚠️  No view_model in rendering_strategy")
                    else:
                        print(f"      ⚠️  No rendering_strategy in edge")
                else:
                    # Debug: Log what other types of edges we're seeing
                    if edge.get('node', {}).get('role'):
                        print(f"      🔍 Found edge with role: {edge['node']['role']}")
                    else:
                        print(f"      🔍 Found edge with no role")
                    
    except Exception as e:
        print(f"   ⚠️  Error extracting groups: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"      📊 Total groups extracted: {len(groups)}")
    return groups

def make_search_request(request_config, search_term):
    """Make a search request and return the results - REQUIRES proxy"""
    
    # SECURITY CHECK: Ensure proxy is loaded
    if PROXIES is None:
        print("❌ CRITICAL ERROR: Proxy not configured! Loading Nimbleway settings...")
        load_nimbleway_settings()
        if PROXIES is None:
            print("❌ Failed to load proxy settings - cannot proceed")
            return None, 0
    
    try:
        response = requests.post(
            request_config['url'],
            headers=request_config['headers'],
            cookies=request_config['cookies'],
            data=request_config['data'],
            proxies=PROXIES,  # Use Nimbleway proxy - REQUIRED for security
            timeout=30
        )
        
        # Log proxy usage for security audit
        if PROXIES:
            print(f"   🔍 Request sent through Nimbleway proxy")
        else:
            print(f"   🔍 Direct connection used (proxyless mode) - SECURITY RISK!")
        
        if response.status_code == 200:
            # Check if it's a Facebook error response
            if response.text.startswith('for (;;);'):
                return None, 0
            
            # Try to parse as JSON
            try:
                json_data = response.json()
                
                # Check if we have group data
                if 'data' in json_data and 'serpResponse' in json_data['data']:
                    edges = json_data['data']['serpResponse']['results']['edges']
                    group_count = len([e for e in edges if e['node'].get('role') == 'ENTITY_GROUPS'])
                    return json_data, group_count
                else:
                    # We got a valid response but no group data - return the response with 0 groups
                    return json_data, 0
                    
            except json.JSONDecodeError as e:
                # Invalid JSON - this is a real failure
                return None, 0
        else:
            # HTTP error - this is a real failure
            return None, 0
            
    except Exception as e:
        return None, 0

def save_groups_to_file(groups, output_file, lock):
    """Save groups to a single JSON file, appending to existing data with file locking"""
    
    with lock:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Load existing groups if file exists
        existing_groups = []
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    existing_groups = json.load(f)
            except Exception as e:
                print(f"   ⚠️  Could not load existing groups: {e}")
        
        # Add new groups
        all_groups = existing_groups + groups
        
        # Save all groups to file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_groups, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"   ❌ Error saving groups: {e}")
            return False

def get_processed_searches(output_file):
    """Get list of already processed searches to enable resume"""
    processed = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_groups = json.load(f)
            # Extract unique search terms from existing groups
            for group in existing_groups:
                if 'search_term' in group:
                    processed.add(group['search_term'])
        except Exception as e:
            print(f"⚠️  Could not read existing output file: {e}")
    return processed

def log_progress():
    """Log progress with estimated time remaining"""
    global processed_count, successful_count, failed_count, skipped_count, start_time
    
    elapsed = time.time() - start_time
    if processed_count > 0:
        rate = processed_count / elapsed
        print(f"\n📊 PROGRESS: {processed_count} cities processed")
        print(f"   ✅ Successful: {successful_count}, ❌ Failed: {failed_count}, ⏭️  Skipped: {skipped_count}")
        print(f"   👥 Total groups found: {total_groups_found}")
        print(f"   ⏱️  Elapsed: {elapsed/60:.1f} min, Rate: {rate:.1f} cities/min")
        print(f"   🏙️  Cities completed: {processed_count}")
        
        # Show active worker count
        active_count = get_active_worker_count()
        print(f"   🚀 Active workers: {active_count}")
        
        # Show worker failure status
        if worker_failure_counts:
            print(f"   🚨 Worker failures: {sum(1 for count in worker_failure_counts.values() if count >= 5)} dropped, {sum(1 for count in worker_failure_counts.values() if 0 < count < 5)} at risk")

def check_worker_health():
    """Check the health of all workers and report any issues"""
    active_count = get_active_worker_count()
    
    if active_count == 0:
        print("💀 CRITICAL: All workers have been dropped!")
        return False
    
    # Check for workers at risk (4 failures)
    at_risk_workers = [wid for wid, count in worker_failure_counts.items() if count == 4]
    if at_risk_workers:
        print(f"⚠️  WARNING: {len(at_risk_workers)} workers at risk of being dropped:")
        for wid in at_risk_workers:
            print(f"   • Worker {wid}: 4/5 failures")
    
    # Check for dropped workers
    dropped_workers = [wid for wid, count in worker_failure_counts.items() if count >= 5]
    if dropped_workers:
        print(f"💀 {len(dropped_workers)} workers have been dropped:")
        for wid in dropped_workers:
            print(f"   • Worker {wid}: DROPPED after 5 failures")
    
    return True

def worker_worker(worker_id, url_queue, result_queue, base_config, processed_searches, output_file, file_lock):
    """Worker function that processes URLs from the queue"""
    global processed_count, successful_count, failed_count, skipped_count, total_groups_found
    
    print(f"🚀 Worker {worker_id} started")
    
    # Add worker to active list
    add_worker_to_active_list(worker_id)
    
    try:
        while not shutdown_event.is_set():
            try:
                # Get URL from queue with timeout
                try:
                    url_data = url_queue.get(timeout=1)
                    if url_data is None:  # Poison pill to stop worker
                        break
                except:
                    continue
                
                url, url_index = url_data
                
                # Extract search term
                search_term = extract_search_term_from_url(url)
                if not search_term:
                    print(f"   ❌ Worker {worker_id}: Could not extract search term from {url}")
                    failed_count += 1
                    processed_count += 1
                    
                    # Record worker failure
                    if record_worker_failure(worker_id):
                        print(f"💀 Worker {worker_id} emergency stopped - shutting down")
                        break
                    
                    url_queue.task_done()
                    continue
                
                # Check if already processed
                if search_term in processed_searches:
                    print(f"   ⏭️  Worker {worker_id}: Already processed '{search_term}', skipping... (City #{processed_count + 1})")
                    skipped_count += 1
                    processed_count += 1
                    url_queue.task_done()
                    continue
                
                # Create initial search request
                request_config = create_initial_search_request(search_term, base_config)
                if not request_config:
                    print(f"   ❌ Worker {worker_id}: Could not create request config for '{search_term}'")
                    failed_count += 1
                    processed_count += 1
                    
                    # Record worker failure
                    if record_worker_failure(worker_id):
                        print(f"💀 Worker {worker_id} emergency stopped - shutting down")
                        break
                    
                    url_queue.task_done()
                    continue
                
                # Make the search request
                results, group_count = make_search_request(request_config, search_term)
                
                # Initialize groups variable to prevent undefined variable errors
                groups = []
                
                # Debug: Log what we got back
                if results:
                    print(f"   🔍 Worker {worker_id}: Got response for '{search_term}' with {group_count} group edges")
                else:
                    print(f"   🔍 Worker {worker_id}: No response for '{search_term}'")
                
                if results and group_count > 0:
                    # Extract groups from the response
                    groups = extract_groups_from_response(results, search_term)
                    
                    if groups:
                        # Save groups to the single output file
                        if save_groups_to_file(groups, output_file, file_lock):
                            total_groups_found += len(groups)
                            successful_count += 1
                            print(f"   ✅ Worker {worker_id}: Found {len(groups)} groups for '{search_term}' (City #{processed_count + 1})")
                            
                            # Record worker success (resets failure count)
                            record_worker_success(worker_id)
                        else:
                            failed_count += 1
                            print(f"   ❌ Worker {worker_id}: Failed to save groups for '{search_term}' (City #{processed_count + 1})")
                            
                            # Record worker failure
                            if record_worker_failure(worker_id):
                                print(f"💀 Worker {worker_id} emergency stopped - shutting down")
                                break
                    else:
                        print(f"   ⚠️  Worker {worker_id}: No groups extracted for '{search_term}' (City #{processed_count + 1})")
                        successful_count += 1  # Still count as successful search
                        
                        # Record worker success (resets failure count)
                        record_worker_success(worker_id)
                elif results:
                    # We got a response but no groups - this is normal, not a failure
                    print(f"   ℹ️  Worker {worker_id}: No groups found for '{search_term}' (City #{processed_count + 1})")
                    successful_count += 1  # Count as successful search
                    
                    # Record worker success (resets failure count)
                    record_worker_success(worker_id)
                else:
                    # No response at all - this is a real failure
                    failed_count += 1
                    print(f"   ❌ Worker {worker_id}: No response for '{search_term}' (City #{processed_count + 1})")
                    
                    # Record worker failure
                    if record_worker_failure(worker_id):
                        print(f"💀 Worker {worker_id} emergency stopped - shutting down")
                        break
                
                processed_count += 1
                
                # Add result to result queue for progress tracking
                result_queue.put({
                    'worker_id': worker_id,
                    'search_term': search_term,
                    'groups_found': len(groups) if groups else 0,
                    'success': bool(results)
                })
                
                url_queue.task_done()
                
                # Small delay to be respectful
                time.sleep(0.5)
                
            except Exception as e:
                print(f"   ❌ Worker {worker_id} error: {e}")
                failed_count += 1
                processed_count += 1
                
                # Record worker failure
                if record_worker_failure(worker_id):
                    print(f"💀 Worker {worker_id} emergency stopped - shutting down")
                    break
                
                url_queue.task_done()
    
    finally:
        # Remove worker from active list when it stops
        remove_worker_from_active_list(worker_id)
        print(f"🛑 Worker {worker_id} stopped")

def progress_monitor(result_queue, total_urls):
    """Monitor progress and display statistics"""
    global processed_count, successful_count, failed_count, skipped_count, total_groups_found
    
    last_log_time = time.time()
    last_health_check = time.time()
    
    while processed_count < total_urls and not shutdown_event.is_set():
        try:
            # Get result from queue with timeout
            try:
                result = result_queue.get(timeout=1)
                if result is None:  # Poison pill to stop monitor
                    break
            except:
                continue
            
            # Check for emergency stop
            if shutdown_event.is_set():
                print("🛑 Emergency stop detected in progress monitor")
                break
            
            # Check if all workers are dropped
            active_count = get_active_worker_count()
            if active_count == 0:
                print("💀 All workers dropped - emergency stop triggered")
                break
            
            # Periodic health check every 30 seconds
            current_time = time.time()
            if current_time - last_health_check >= 30:
                print(f"\n🔍 WORKER HEALTH CHECK:")
                if not check_worker_health():
                    print("💀 Critical worker health issue detected!")
                    break
                last_health_check = current_time
            
            # Log progress every 5 seconds instead of 10
            if current_time - last_log_time >= 5:
                log_progress()
                last_log_time = current_time
                
                # Show active worker count
                print(f"   🚀 Active workers: {active_count}")
            
            result_queue.task_done()
            
        except Exception as e:
            print(f"   ❌ Progress monitor error: {e}")
    
    # Final progress log
    log_progress()
    
    # Final health check
    print(f"\n🔍 FINAL WORKER HEALTH CHECK:")
    check_worker_health()
    
    # Check final status
    if shutdown_event.is_set():
        print("🛑 Progress monitor stopped due to emergency stop")
    elif get_active_worker_count() == 0:
        print("💀 Progress monitor stopped - all workers dropped")
    else:
        print("✅ Progress monitor completed normally")

def process_urls_file_async(urls_file, curl_config_file, output_file="output/initial_searches/initial_searches.json", 
                           start_from=0, max_urls=None, delay=1, num_workers=4):
    """Process all URLs in the file with async workers"""
    
    global processed_count, successful_count, failed_count, skipped_count, total_groups_found, start_time
    
    print("🚀 Facebook Bulk Initial Search (Async)")
    print("🔒 REQUIRES Nimbleway proxy for security")
    print("=" * 50)
    
    # Load Nimbleway proxy settings FIRST - REQUIRED for security
    load_nimbleway_settings()
    
    # Load the curl configuration
    try:
        with open(curl_config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        curl_cmd = config['curl_command']
        print(f"✅ Loaded curl command from {curl_config_file}")
        
    except Exception as e:
        print(f"❌ Error loading curl command: {e}")
        return
    
    # Parse the curl command
    base_config = parse_curl_command(curl_cmd)
    if not base_config:
        print("❌ Could not parse curl command")
        return
    
    print(f"📡 Base URL: {base_config['url']}")
    print(f"🍪 Cookies: {len(base_config['cookies'])} cookies")
    print(f"📋 Headers: {len(base_config['headers'])} headers")
    print(f"👥 Using {num_workers} workers")
    print(f"🔒 All requests will go through Nimbleway proxy")
    print()
    
    # Read the URLs file
    try:
        with open(urls_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        total_urls = len(urls)
        print(f"📋 Found {total_urls} cities to process")
        
        # Apply start_from and max_urls filters
        if start_from > 0:
            urls = urls[start_from:]
            print(f"⏭️  Starting from city #{start_from + 1}")
        
        if max_urls:
            urls = urls[:max_urls]
            print(f"🔢 Processing max {max_urls} cities")
        
        print(f"🎯 Will process {len(urls)} cities")
        
    except Exception as e:
        print(f"❌ Error reading URLs file: {e}")
        return
    
    # Check for already processed searches
    processed_searches = get_processed_searches(output_file)
    if processed_searches:
        print(f"📁 Found {len(processed_searches)} already processed cities")
        print("💡 Will skip already processed cities")
    
    # Create queues and locks
    url_queue = Queue()
    result_queue = Queue()
    file_lock = threading.Lock()
    
    # Fill the URL queue
    for i, url in enumerate(urls):
        url_queue.put((url, i))
    
    # Start workers
    workers = []
    for i in range(num_workers):
        worker = threading.Thread(
            target=worker_worker,
            args=(i+1, url_queue, result_queue, base_config, processed_searches, output_file, file_lock)
        )
        worker.daemon = True
        worker.start()
        workers.append(worker)
    
    # Start progress monitor
    progress_thread = threading.Thread(
        target=progress_monitor,
        args=(result_queue, len(urls))
    )
    progress_thread.daemon = True
    progress_thread.start()
    
    # Wait for all URLs to be processed
    try:
        url_queue.join()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user, waiting for workers to finish...")
        shutdown_event.set()
    
    # Wait for workers to finish
    for worker in workers:
        worker.join(timeout=5)
    
    # Check if emergency stop was triggered
    if shutdown_event.is_set():
        print("\n" + "=" * 50)
        print("🚨 EMERGENCY STOP TRIGGERED")
        print("=" * 50)
        
        # Check why emergency stop was triggered
        active_count = get_active_worker_count()
        if active_count == 0:
            print("💀 All workers were dropped due to consecutive failures")
            print("   This indicates a serious problem with the scraping process")
            print("   Possible causes:")
            print("   • Network connectivity issues")
            print("   • Facebook blocking requests")
            print("   • Proxy configuration problems")
            print("   • Rate limiting or anti-bot measures")
        else:
            print(f"🛑 Emergency stop triggered with {active_count} workers still active")
        
        print(f"\n📊 PARTIAL RESULTS SUMMARY:")
        print(f"✅ Successful searches: {successful_count}")
        print(f"❌ Failed searches: {failed_count}")
        print(f"⏭️  Skipped searches: {skipped_count}")
        print(f"👥 Total groups found: {total_groups_found}")
        print(f"🏙️  Cities processed: {processed_count}")
        print(f"📁 Partial results saved to: {output_file}")
        print(f"⏰ Time before emergency stop: {(time.time() - start_time)/60:.1f} minutes")
        print(f"🔒 All requests were made through Nimbleway proxy for security")
        
        # Show worker failure statistics
        print(f"\n🚨 WORKER FAILURE ANALYSIS:")
        for worker_id, failure_count in worker_failure_counts.items():
            status = "DROPPED" if failure_count >= 5 else f"Active ({failure_count}/5 failures)"
            print(f"   Worker {worker_id}: {status}")
        
        return  # Exit early due to emergency stop
    
    # Final summary (normal completion)
    total_time = time.time() - start_time
    print("\n" + "=" * 50)
    print("📊 FINAL SEARCH SUMMARY")
    print(f"✅ Successful searches: {successful_count}")
    print(f"❌ Failed searches: {failed_count}")
    print(f"⏭️  Skipped searches: {skipped_count}")
    print(f"👥 Total groups found: {total_groups_found}")
    print(f"🏙️  Cities processed: {processed_count}")
    print(f"📁 Results saved to: {output_file}")
    print(f"⏰ Total time: {total_time/60:.1f} minutes")
    print(f"🚀 Average rate: {len(urls)/total_time*60:.1f} cities/min")
    print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔒 All requests were made through Nimbleway proxy for security")
    
    # Show worker performance
    print(f"\n🚀 WORKER PERFORMANCE:")
    for worker_id, failure_count in worker_failure_counts.items():
        status = "DROPPED" if failure_count >= 5 else f"Completed ({failure_count} failures)"
        print(f"   Worker {worker_id}: {status}")

def main():
    """Main function with command line arguments"""
    
    parser = argparse.ArgumentParser(description='Facebook Bulk Initial Search (Async) - REQUIRES Nimbleway proxy')
    parser.add_argument('--urls-file', default='settings/facebook_group_urls.txt',
                       help='File containing Facebook search URLs')
    parser.add_argument('--curl-config', default='settings/curl/loverichards.json',
                       help='JSON file containing curl command')
    parser.add_argument('--output-file', default='output/initial_searches/initial_searches.json',
                       help='Single JSON file to save all group results')
    parser.add_argument('--start-from', type=int, default=0,
                       help='Start processing from URL number (0-based)')
    parser.add_argument('--max-urls', type=int, default=None,
                       help='Maximum number of URLs to process')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between requests in seconds (per worker)')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of worker threads to use')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from where left off (skip already processed)')
    
    args = parser.parse_args()
    
    # Check if files exist
    if not os.path.exists(args.urls_file):
        print(f"❌ URLs file not found: {args.urls_file}")
        return
    
    if not os.path.exists(args.curl_config):
        print(f"❌ Curl config file not found: {args.curl_config}")
        return
    
    # Process the URLs with async workers
    process_urls_file_async(
        args.urls_file, 
        args.curl_config, 
        args.output_file,
        args.start_from,
        args.max_urls,
        args.delay,
        args.workers
    )

if __name__ == "__main__":
    main()
