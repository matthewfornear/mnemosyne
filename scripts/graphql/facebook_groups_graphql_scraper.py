import requests
import json
import time
import os
import sys
import random
import datetime
import urllib.parse
import signal
import multiprocessing as mp
from multiprocessing import Manager, Lock
from collections import Counter
import re

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
ACCOUNTS_FILE = os.path.join(PARENT_DIR, "settings", "bought_accounts.json")
COOKIES_DIR = os.path.join(PARENT_DIR, "settings", "cookies")
OUTPUT_FILE = os.path.join(PARENT_DIR, "output", "groups_graphql.jsonl")
STATE_FILE = os.path.join(PARENT_DIR, "output", "groups_graphql_state.json")
URLS_FILE = os.path.join(PARENT_DIR, "settings", "facebook_group_urls.txt")

# --- GraphQL Call Counter and Timing ---
GRAPHQL_CALL_COUNT = 0
START_TIME = time.time()
TARGET_CALLS = 1000

# --- Rate Limiting Detection ---
RATE_LIMIT_DETECTED = False
RATE_LIMIT_ATTEMPTS = 0
MAX_RATE_LIMIT_ATTEMPTS = 3
BASE_BACKOFF_TIME = 60  # 1 minute base
LAST_RATE_LIMIT_TIME = 0

# --- Retry Configuration ---
MAX_CITY_RETRIES = 3  # Maximum retries per city before permanent failure
RETRY_BACKOFF_BASE = 5  # Base seconds for retry backoff
TEMP_FAILURE_STATUSES = ["failed", "rate_limited", "network_error", "timeout", "json_error"]
PERMANENT_FAILURE_STATUSES = ["permanently_failed", "blocked"]
COMPLETED_STATUSES = ["completed", "completed_zero_results", "completed_no_cursor", "completed_no_groups"]

# --- Account Management ---
CURRENT_ACCOUNT_INDEX = 0
ACCOUNTS = []
COOKIES = None
ACTIVE_ACCOUNTS_FILE = os.path.join(PARENT_DIR, "settings", "active_accounts.json")

def load_accounts():
    """Load accounts from bought_accounts.json that have valid cookie files"""
    if not os.path.exists(ACCOUNTS_FILE):
        raise Exception(f"Accounts file '{ACCOUNTS_FILE}' not found.")
    
    with open(ACCOUNTS_FILE, "r") as f:
        data = json.load(f)
    
    all_accounts = data.get("accounts", [])
    if not all_accounts:
        raise Exception("No accounts found in bought_accounts.json")
    
    # Filter accounts to only include ones with valid cookie files
    valid_accounts = []
    for account in all_accounts:
        account_email = account["account"]
        
        cookie_file = get_cookie_file_path(account_email)
        if os.path.exists(cookie_file):
            try:
                # Test if the cookie file is valid
                with open(cookie_file, "r") as f:
                    cookies = json.load(f)
                if cookies.get("c_user"):
                    valid_accounts.append(account)
                    print(f"✅ Account {account_email} has valid cookies")
                else:
                    print(f"⚠️  Account {account_email} has cookies but missing c_user")
            except Exception as e:
                print(f"⚠️  Account {account_email} has invalid cookie file: {e}")
        else:
            print(f"❌ Account {account_email} missing cookie file: {cookie_file}")
    
    if not valid_accounts:
        raise Exception("No accounts with valid cookie files found. Please run: python scripts/generate_cookies.py")
    
    print(f"✅ Loaded {len(valid_accounts)} accounts with valid cookies from {ACCOUNTS_FILE}")
    print(f"📋 All accounts will be tested dynamically during runtime")
    return valid_accounts

def get_cookie_file_path(account_email):
    """Get the cookie file path for a specific account"""
    safe_email = account_email.replace("@", "_at_").replace(".", "_")
    return os.path.join(COOKIES_DIR, f"{safe_email}_cookies.json")

def load_cookies_for_account(account_email):
    """Load cookies for a specific account"""
    cookie_file = get_cookie_file_path(account_email)
    
    if not os.path.exists(cookie_file):
        raise Exception(f"Cookie file not found: {cookie_file}")
    
    try:
        with open(cookie_file, "r") as f:
            cookies = json.load(f)
        
        # Convert to requests-compatible format
        if isinstance(cookies, dict) and "c_user" in cookies:
            # Ensure all cookie values are strings
            cleaned_cookies = {}
            for key, value in cookies.items():
                if isinstance(value, dict):
                    # If value is a dict, convert to JSON string
                    cleaned_cookies[key] = json.dumps(value)
                elif isinstance(value, (list, tuple)):
                    # If value is a list/tuple, convert to JSON string
                    cleaned_cookies[key] = json.dumps(value)
                elif value is None:
                    # If value is None, convert to empty string
                    cleaned_cookies[key] = ""
                else:
                    # Convert everything else to string
                    cleaned_cookies[key] = str(value)
            
            return cleaned_cookies
        else:
            raise Exception("Invalid cookie format - missing c_user")
            
    except Exception as e:
        raise Exception(f"Failed to load cookies from {cookie_file}: {e}")

def get_session_data_from_cookies(account_email):
    """Get session-specific headers and payload data from account cookies"""
    cookie_file = get_cookie_file_path(account_email)
    
    try:
        with open(cookie_file, "r") as f:
            cookie_data = json.load(f)
        
        # Check if this is the advanced cookie format with nested objects
        if "session_headers" in cookie_data and "session_payload" in cookie_data:
            print(f"   🔧 Using advanced cookie format for {account_email}")
            
            # Use the session headers from the cookie file
            session_headers = cookie_data["session_headers"].copy()
            
            # Use the session payload from the cookie file
            session_payload = cookie_data["session_payload"].copy()
            
            return session_headers, session_payload
        
        else:
            # Fallback to simple cookie format
            print(f"   🔧 Using simple cookie format for {account_email}")
            
            # Extract session-specific values from simple format
            fb_dtsg = cookie_data.get("fb_dtsg", "NAftR06AOPsyS-g4udRZQvZgBkssjaQdvXH7HH9MHNVmyuBi750iQvA:25:1754053582")
            jazoest = cookie_data.get("jazoest", "25485")
            lsd = cookie_data.get("lsd", "Z-n_XPzGloTFb9EkUVjSLE")
            
            headers = {}
            payload = {
                "fb_dtsg": fb_dtsg,
                "jazoest": jazoest,
                "lsd": lsd,
            }
            
            return headers, payload
        
    except Exception as e:
        print(f"⚠️  Could not load session data for {account_email}: {e}")
        # Return defaults if session data is not available
        return {}, {
            "fb_dtsg": "NAftR06AOPsyS-g4udRZQvZgBkssjaQdvXH7HH9MHNVmyuBi750iQvA:25:1754053582",
            "jazoest": "25485", 
            "lsd": "Z-n_XPzGloTFb9EkUVjSLE",
        }

def load_active_accounts():
    """Load active accounts status"""
    if os.path.exists(ACTIVE_ACCOUNTS_FILE):
        try:
            with open(ACTIVE_ACCOUNTS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Error loading active accounts: {e}")
    
    # Create default structure
    accounts = {}
    for account in ACCOUNTS:
        accounts[account["account"]] = {
            "status": "available",
            "last_used": None,
            "rate_limit_count": 0
        }
    
    return {"accounts": accounts}

def save_active_accounts(active_accounts):
    """Save active accounts status"""
    try:
        with open(ACTIVE_ACCOUNTS_FILE, "w") as f:
            json.dump(active_accounts, f, indent=2)
    except Exception as e:
        print(f"⚠️  Error saving active accounts: {e}")

def mark_account_busy(account_email):
    """Mark an account as busy"""
    try:
        active_accounts = load_active_accounts()
        active_accounts["accounts"][account_email] = {
            "status": "busy",
            "last_used": datetime.datetime.now().isoformat(),
            "rate_limit_count": active_accounts["accounts"].get(account_email, {}).get("rate_limit_count", 0)
        }
        save_active_accounts(active_accounts)
    except Exception as e:
        print(f"⚠️  Error marking account busy: {e}")

def mark_account_available(account_email):
    """Mark an account as available"""
    try:
        active_accounts = load_active_accounts()
        active_accounts["accounts"][account_email] = {
            "status": "available",
            "last_used": datetime.datetime.now().isoformat(),
            "rate_limit_count": active_accounts["accounts"].get(account_email, {}).get("rate_limit_count", 0)
        }
        save_active_accounts(active_accounts)
    except Exception as e:
        print(f"⚠️  Error marking account available: {e}")

def get_available_account():
    """Get the next available account"""
    try:
        active_accounts = load_active_accounts()
        
        # Find available accounts
        available = []
        for account_email, status in active_accounts["accounts"].items():
            if status.get("status") == "available":
                # Check if account exists in our loaded accounts
                for i, account in enumerate(ACCOUNTS):
                    if account["account"] == account_email:
                        available.append((i, account_email))
                        break
        
        if not available:
            print("⚠️  No available accounts found")
            # Mark all accounts as available (reset)
            for account_email in active_accounts["accounts"]:
                active_accounts["accounts"][account_email]["status"] = "available"
            save_active_accounts(active_accounts)
            
            # Return first account as fallback
            if ACCOUNTS:
                return 0, ACCOUNTS[0]["account"]
            return None, None
        
        # Sort by last used time (least recently used first)
        available.sort(key=lambda x: active_accounts["accounts"][x[1]].get("last_used", ""))
        
        return available[0]
        
    except Exception as e:
        print(f"⚠️  Error getting available account: {e}")
        if ACCOUNTS:
            return 0, ACCOUNTS[0]["account"]
        return None, None

def switch_to_available_account():
    """Switch to an available account"""
    global CURRENT_ACCOUNT_INDEX, COOKIES
    
    try:
        # Mark current account as rate limited if we have one
        if ACCOUNTS and CURRENT_ACCOUNT_INDEX < len(ACCOUNTS):
            current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]["account"]
            mark_account_available(current_account)  # Actually mark as available for now
        
        # Get next available account
        account_index, account_email = get_available_account()
        
        if account_index is None:
            print("❌ No available accounts found")
            return False
        
        # Load cookies for the new account
        try:
            new_cookies = load_cookies_for_account(account_email)
            COOKIES = new_cookies
            CURRENT_ACCOUNT_INDEX = account_index
            
            # Mark new account as busy
            mark_account_busy(account_email)
            
            print(f"🔄 Switched to account: {account_email}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to load cookies for {account_email}: {e}")
            return False
        
    except Exception as e:
        print(f"❌ Error switching accounts: {e}")
        return False

def validate_proxy_connection():
    """Test if the Nimbleway proxy is working"""
    if not PROXIES:
        return False
    
    try:
        print("🔍 Testing Nimbleway proxy connection...")
        
        # Try multiple test URLs in case some are blocked
        test_urls = [
            "https://api.ipify.org?format=json",
            "https://httpbin.org/ip",
            "https://ifconfig.me/ip"
        ]
        
        for test_url in test_urls:
            try:
                print(f"   Testing with: {test_url}")
                test_response = requests.get(
                    test_url,
                    proxies=PROXIES,
                    timeout=10
                )
                
                if test_response.status_code == 200:
                    try:
                        ip_data = test_response.json()
                        proxy_ip = ip_data.get("ip", ip_data.get("origin", "unknown"))
                    except:
                        proxy_ip = test_response.text.strip()
                    
                    print(f"✅ Proxy working - IP: {proxy_ip}")
                    return True
                else:
                    print(f"   ❌ Failed with status: {test_response.status_code}")
                    
            except Exception as e:
                print(f"   ❌ Failed: {e}")
                continue
        
        # Try Facebook specifically to see if it's allowed
        print("   Testing Facebook connection...")
        try:
            fb_response = requests.get(
                "https://www.facebook.com",
                proxies=PROXIES,
                timeout=10
            )
            if fb_response.status_code == 200:
                print("✅ Facebook connection successful")
                return True
            else:
                print(f"   ❌ Facebook failed with status: {fb_response.status_code}")
        except Exception as e:
            print(f"   ❌ Facebook connection failed: {e}")
        
        print("❌ All proxy test URLs failed")
        return False
        
    except Exception as e:
        print(f"❌ Proxy test failed: {e}")
        return False

def mark_current_account_available():
    """Mark the current account as available"""
    if ACCOUNTS and CURRENT_ACCOUNT_INDEX < len(ACCOUNTS):
        current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]["account"]
        mark_account_available(current_account)

def rotate_account():
    """Rotate to the next account"""
    return switch_to_available_account()

def is_rate_limited(response_text, status_code):
    """Detect if we're being rate limited by Facebook"""
    global RATE_LIMIT_DETECTED, LAST_RATE_LIMIT_TIME
    
    # Don't flag 200 responses as rate limited
    if status_code == 200:
        return False
    
    # Check status codes first
    if status_code in [429, 403, 503]:
        RATE_LIMIT_DETECTED = True
        LAST_RATE_LIMIT_TIME = time.time()
        return True
    
    # Check for common rate limiting indicators
    rate_limit_indicators = [
        "error:1357004",  # Common Facebook rate limit error
        "Sorry, something went wrong",
        "Please try again later",
        "Too many requests",
        "Rate limit exceeded",
        "temporarily blocked",
        "suspicious activity",
        "checkpoint_required",
        "login_required"
    ]
    
    # Check response content - ensure response_text is a string
    try:
        # Handle different types of response_text
        if response_text is None:
            return False
        elif isinstance(response_text, dict):
            # If it's a dict, convert to string for checking
            response_text_str = json.dumps(response_text)
        elif isinstance(response_text, str):
            response_text_str = response_text
        elif isinstance(response_text, bytes):
            # Handle bytes response
            response_text_str = response_text.decode('utf-8', errors='ignore')
        else:
            # Convert anything else to string
            response_text_str = str(response_text)
        
        # Convert to lowercase for case-insensitive checking
        response_lower = response_text_str.lower()
        
        # Check each indicator
        for indicator in rate_limit_indicators:
            try:
                if isinstance(indicator, str) and indicator.lower() in response_lower:
                    RATE_LIMIT_DETECTED = True
                    LAST_RATE_LIMIT_TIME = time.time()
                    return True
            except Exception as indicator_error:
                # Skip this indicator if there's an error
                print(f"⚠️  Warning: Error checking rate limit indicator '{indicator}': {indicator_error}")
                continue
                
    except Exception as e:
        # If we can't process the response text, don't fail
        print(f"⚠️  Warning: Could not check response for rate limiting: {e}")
        print(f"   Response type: {type(response_text)}")
        print(f"   Response preview: {str(response_text)[:100] if response_text else 'None'}...")
    
    return False

def handle_rate_limiting():
    """Handle rate limiting with account rotation and exponential backoff"""
    global RATE_LIMIT_DETECTED, RATE_LIMIT_ATTEMPTS
    
    RATE_LIMIT_ATTEMPTS += 1
    
    if RATE_LIMIT_ATTEMPTS >= MAX_RATE_LIMIT_ATTEMPTS:
        print(f"\n🚨 RATE LIMITING: {MAX_RATE_LIMIT_ATTEMPTS} attempts reached!")
        print(f"🔄 Trying to switch to available account...")
        
        try:
            # Mark current account as rate limited
            current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]["account"]
            active_accounts = load_active_accounts()
            active_accounts["accounts"][current_account] = {
                "status": "rate_limited",
                "last_rate_limited": datetime.datetime.now().isoformat(),
                "rate_limit_count": active_accounts["accounts"].get(current_account, {}).get("rate_limit_count", 0) + 1
            }
            save_active_accounts(active_accounts)
            
            if switch_to_available_account():
                print(f"✅ Successfully switched to available account")
                RATE_LIMIT_ATTEMPTS = 0
                RATE_LIMIT_DETECTED = False
                return
        except Exception as e:
            print(f"❌ Failed to switch account: {e}")
        
        print(f"💾 Saving progress and quitting safely...")
        save_all_progress()
        print(f"✅ Progress saved. Script stopped safely.")
        print(f"📊 Final stats: {GRAPHQL_CALL_COUNT} calls, {len(seen_ids) if 'seen_ids' in globals() else 0} groups")
        print(f"❌ Stopped due to persistent rate limiting from Facebook")
        sys.exit(0)
    
    # Calculate exponential backoff time
    backoff_time = BASE_BACKOFF_TIME * (2 ** (RATE_LIMIT_ATTEMPTS - 1))
    
    print(f"\n🚨 RATE LIMITING DETECTED! (Attempt {RATE_LIMIT_ATTEMPTS}/{MAX_RATE_LIMIT_ATTEMPTS})")
    print(f"🔄 Switching to available account...")
    
    try:
        # Mark current account as rate limited
        current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]["account"]
        active_accounts = load_active_accounts()
        active_accounts["accounts"][current_account] = {
            "status": "rate_limited",
            "last_rate_limited": datetime.datetime.now().isoformat(),
            "rate_limit_count": active_accounts["accounts"].get(current_account, {}).get("rate_limit_count", 0) + 1
        }
        save_active_accounts(active_accounts)
        
        if switch_to_available_account():
            print(f"✅ Successfully switched to available account")
            RATE_LIMIT_ATTEMPTS = 0
            RATE_LIMIT_DETECTED = False
            return
    except Exception as e:
        print(f"❌ Failed to switch account: {e}")
    
    print(f"⏸️  Waiting {backoff_time} seconds before retry...")
    print(f"💾 Saving progress...")
    
    # Save current progress
    save_all_progress()
    
    # Wait for exponential backoff
    time.sleep(backoff_time)
    
    print(f"🔄 Resuming after backoff...")
    RATE_LIMIT_DETECTED = False

def save_all_progress():
    """Save all progress data"""
    # Save state
    if 'cursor' in globals() and 'seen_ids' in globals():
        save_state(cursor, seen_ids)

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    print(f"\n\n🛑 Interrupt received! Saving progress...")
    save_all_progress()
    print(f"✅ Progress saved. Script stopped safely.")
    print(f"📊 Final stats: {GRAPHQL_CALL_COUNT} calls, {len(seen_ids) if 'seen_ids' in globals() else 0} groups")
    print(f"⏰ Runtime: {time.time() - START_TIME:.1f} seconds")
    sys.exit(0)

# Register signal handler for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)

def update_call_count():
    """Update the GraphQL call count and check if target is reached"""
    global GRAPHQL_CALL_COUNT
    GRAPHQL_CALL_COUNT += 1
    
    # Check if we've reached our target
    if GRAPHQL_CALL_COUNT >= TARGET_CALLS:
        elapsed_time = time.time() - START_TIME
        print(f"\n🎯 TARGET REACHED! {TARGET_CALLS} GraphQL calls completed in {elapsed_time:.1f} seconds")
        print(f"📊 Final stats: {len(seen_ids) if 'seen_ids' in globals() else 0} unique groups found")
        save_all_progress()
        print(f"✅ All progress saved. Script completed successfully!")
        sys.exit(0)
    
    # Progress reporting every 50 calls
    if GRAPHQL_CALL_COUNT % 50 == 0:
        elapsed_time = time.time() - START_TIME
        rate = GRAPHQL_CALL_COUNT / elapsed_time if elapsed_time > 0 else 0
        print(f"📊 Progress: {GRAPHQL_CALL_COUNT}/{TARGET_CALLS} calls ({rate:.1f} calls/sec)")

def load_url_progress(worker_id=None):
    """Load URL progress from worker-specific file"""
    if worker_id is not None:
        progress_file = os.path.join(PARENT_DIR, "output", f"url_progress_worker_{worker_id}.json")
    else:
        progress_file = os.path.join(PARENT_DIR, "output", "url_progress_graphql.json")
    
    try:
        if os.path.exists(progress_file):
            with open(progress_file, "r") as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
                    print(f"📥 Worker {worker_id if worker_id is not None else 'main'}: Loaded progress from {progress_file}")
                    return data
                else:
                    print(f"📄 Worker {worker_id if worker_id is not None else 'main'}: Empty progress file, starting fresh")
                    return {"city_progress": {}, "pagination_target": 1000}
        else:
            print(f"📄 Worker {worker_id if worker_id is not None else 'main'}: No progress file found, starting fresh")
            return {"city_progress": {}, "pagination_target": 1000}
    except Exception as e:
        print(f"⚠️  Worker {worker_id if worker_id is not None else 'main'}: Error loading progress: {e}")
        return {"city_progress": {}, "pagination_target": 1000}

def save_url_progress(progress_data, worker_id=None):
    """Save URL progress to worker-specific file"""
    if worker_id is not None:
        progress_file = os.path.join(PARENT_DIR, "output", f"url_progress_worker_{worker_id}.json")
    else:
        progress_file = os.path.join(PARENT_DIR, "output", "url_progress_graphql.json")
    
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(progress_file), exist_ok=True)
        
        with open(progress_file, "w") as f:
            json.dump(progress_data, f, indent=2)
    except Exception as e:
        print(f"⚠️  Worker {worker_id if worker_id is not None else 'main'}: Error saving progress: {e}")

def update_city_progress(progress_data, search_term, worker_id, cursor=None, pagination_count=0, new_groups=0, status="processing"):
    """Update progress for a specific city"""
    if "city_progress" not in progress_data:
        progress_data["city_progress"] = {}
    
    progress_data["city_progress"][search_term] = {
        "cursor": cursor,
        "pagination_count": pagination_count,
        "new_groups": new_groups,
        "status": status,
        "last_updated": datetime.datetime.now().isoformat(),
        "worker_id": worker_id
    }

def update_worker_progress(progress_data, worker_id, search_term, status="active"):
    """Update worker-level progress tracking"""
    if "worker_progress" not in progress_data:
        progress_data["worker_progress"] = {}
    
    progress_data["worker_progress"][worker_id] = {
        "current_city": search_term,
        "status": status,
        "last_updated": datetime.datetime.now().isoformat()
    }

def merge_all_worker_progress():
    """Merge progress from all workers into a single summary"""
    output_dir = os.path.join(PARENT_DIR, "output")
    merged_progress = {"city_progress": {}, "worker_summary": {}}
    
    # Find all worker progress files
    worker_files = []
    for filename in os.listdir(output_dir):
        if filename.startswith("url_progress_worker_") and filename.endswith(".json"):
            worker_files.append(os.path.join(output_dir, filename))
    
    print(f"🔄 Merging progress from {len(worker_files)} worker files...")
    
    for worker_file in worker_files:
        try:
            with open(worker_file, "r") as f:
                worker_data = json.load(f)
            
            # Extract worker ID from filename
            worker_id = os.path.basename(worker_file).replace("url_progress_worker_", "").replace(".json", "")
            
            # Merge city progress
            if "city_progress" in worker_data:
                for city, city_data in worker_data["city_progress"].items():
                    merged_progress["city_progress"][city] = city_data
            
            # Add worker summary
            if "worker_progress" in worker_data:
                merged_progress["worker_summary"][worker_id] = worker_data["worker_progress"].get(worker_id, {})
                
        except Exception as e:
            print(f"⚠️  Error processing {worker_file}: {e}")
    
    # Save merged progress
    merged_file = os.path.join(output_dir, "merged_progress_graphql.json")
    try:
        with open(merged_file, "w") as f:
            json.dump(merged_progress, f, indent=2)
        print(f"✅ Merged progress saved to {merged_file}")
        
        # Print summary statistics
        total_cities = len(merged_progress["city_progress"])
        completed_cities = sum(1 for city_data in merged_progress["city_progress"].values() 
                             if city_data.get("status") in COMPLETED_STATUSES)
        permanently_failed = sum(1 for city_data in merged_progress["city_progress"].values() 
                               if city_data.get("status") in PERMANENT_FAILURE_STATUSES)
        temp_failed = sum(1 for city_data in merged_progress["city_progress"].values() 
                        if city_data.get("status") in TEMP_FAILURE_STATUSES)
        processing_cities = sum(1 for city_data in merged_progress["city_progress"].values() 
                              if city_data.get("status") == "processing")
        
        # Count retry statistics
        retry_stats = Counter()
        for city_data in merged_progress["city_progress"].values():
            retry_count = city_data.get("retry_count", 0)
            if retry_count > 0:
                retry_stats[f"retry_{retry_count}"] += 1
        
        print(f"📊 Progress Summary:")
        print(f"   • Total cities: {total_cities}")
        print(f"   • Completed: {completed_cities}")
        print(f"   • Permanently failed: {permanently_failed}")
        print(f"   • Temporary failures: {temp_failed}")
        print(f"   • Processing: {processing_cities}")
        print(f"   • Remaining: {total_cities - completed_cities - permanently_failed}")
        
        if retry_stats:
            print(f"   • Retry statistics:")
            for retry_level, count in retry_stats.items():
                print(f"     - {retry_level}: {count} cities")
        
    except Exception as e:
        print(f"⚠️  Error saving merged progress: {e}")

def load_search_urls_from_file() -> list:
    """Load search URLs from the URLs file"""
    try:
        if not os.path.exists(URLS_FILE):
            print(f"⚠️  URLs file not found: {URLS_FILE}")
            return []
        
        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
        
        print(f"✅ Loaded {len(urls)} URLs from {URLS_FILE}")
        return urls
        
    except Exception as e:
        print(f"⚠️  Error loading URLs: {e}")
        return []

def extract_search_term_from_url(url: str) -> str:
    """Extract search term from Facebook Groups search URL"""
    try:
        # Parse the URL and extract the 'q' parameter
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        if 'q' in query_params:
            search_term = query_params['q'][0]
            # URL decode the search term
            search_term = urllib.parse.unquote_plus(search_term)
            return search_term
        else:
            print(f"⚠️  No 'q' parameter found in URL: {url}")
            return None
            
    except Exception as e:
        print(f"⚠️  Error extracting search term from URL {url}: {e}")
        return None

# --- CONFIGURATION ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.6",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.facebook.com",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Connection": "keep-alive",
    "Priority": "u=1, i",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
    "sec-ch-ua-full-version-list": '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.0.0", "Brave";v="138.0.0.0"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": '""',
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-platform-version": '"19.0.0"',
    "Sec-GPC": "1",
    "x-asbd-id": "359341",
    "x-fb-friendly-name": "SearchCometResultsPaginatedResultsQuery",
    "x-fb-lsd": "Z-n_XPzGloTFb9EkUVjSLE",
}
DOC_ID = "24106293015687348"
FB_API_REQ_FRIENDLY_NAME = "SearchCometResultsPaginatedResultsQuery"
SLEEP_BETWEEN_REQUESTS = (0.0, 2.0)

# Add user agent rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/119.0.0.0"
]

# --- LOAD STATE ---
if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, "r") as f:
            content = f.read().strip()
            if content:  # Only try to parse if file has content
                state = json.loads(content)
                cursor = state.get("cursor")
                seen_ids = set(state.get("seen_ids", []))
            else:
                # Empty file, use defaults
                cursor = None
                seen_ids = set()
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"⚠️  Corrupted or empty state file detected: {e}")
        print("🔄 Starting fresh state...")
        cursor = None
        seen_ids = set()
else:
    cursor = None
    seen_ids = set()

def save_state(cursor, seen_ids):
    with open(STATE_FILE, "w") as f:
        json.dump({"cursor": cursor, "seen_ids": list(seen_ids)}, f)

def append_group(group):
    # Duplicate prevention: only write if not already seen
    if group["id"] not in seen_ids:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(group, ensure_ascii=False) + "\n")
        seen_ids.add(group["id"])

# --- USER: Paste your variables JSON from DevTools below ---
USER_VARIABLES_JSON = '''
{"allow_streaming":false,"args":{"callsite":"comet:groups_search","config":{"exact_match":false,"high_confidence_config":null,"intercept_config":null,"sts_disambiguation":null,"watch_config":null},"context":{"bsid":"87ba5a8d-2e34-4534-8293-0335da876d55","tsid":null},"experience":{"client_defined_experiences":["ADS_PARALLEL_FETCH"],"encoded_server_defined_params":null,"fbid":null,"type":"GROUPS_TAB_GLOBAL"},"filters":[],"text":"test"},"count":10,"cursor":null,"feedLocation":"SEARCH","feedbackSource":23,"fetch_filters":true,"focusCommentID":null,"locale":null,"privacySelectorRenderLocation":"COMET_STREAM","renderLocation":"search_results_page","scale":1,"stream_initial_count":0,"useDefaultActor":false,"__relay_internal__pv__GHLShouldChangeAdIdFieldNamerelayprovider":false,"__relay_internal__pv__GHLShouldChangeSponsoredDataFieldNamerelayprovider":false,"__relay_internal__pv__IsWorkUserrelayprovider":false,"__relay_internal__pv__FBReels_deprecate_short_form_video_context_gkrelayprovider":true,"__relay_internal__pv__FeedDeepDiveTopicPillThreadViewEnabledrelayprovider":false,"__relay_internal__pv__FBReels_enable_view_dubbed_audio_type_gkrelayprovider":false,"__relay_internal__pv__CometImmersivePhotoCanUserDisable3DMotionrelayprovider":false,"__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider":false,"__relay_internal__pv__IsMergQAPollsrelayprovider":false,"__relay_internal__pv__FBReelsMediaFooter_comet_enable_reels_ads_gkrelayprovider":true,"__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider":false,"__relay_internal__pv__CometUFIShareActionMigrationrelayprovider":true,"__relay_internal__pv__CometUFI_dedicated_comment_routable_dialog_gkrelayprovider":false,"__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider":true,"__relay_internal__pv__FBReelsIFUTileContent_reelsIFUPlayOnHoverrelayprovider":true}
'''

# --- MAIN REQUEST FUNCTION ---
def fetch_page(cursor, search_term):
    if not COOKIES or not COOKIES.get("c_user"):
        if len(ACCOUNTS) > 1:  # Only try rotation if using multiple accounts
            current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]["account"]
            raise Exception(f"{current_account} failed! refresh cookies!")
        else:
            raise Exception("Real account cookies failed! Please refresh your cookies.")
    
    # SECURITY CHECK: Ensure proxy is used when in Nimbleway mode
    if mode != "2" and not PROXIES:
        print("❌ CRITICAL SECURITY ERROR: Nimbleway mode selected but no proxy configured!")
        print("   This would expose your requests to the open internet.")
        print("   Please ensure nimbleway_settings.json is properly configured.")
        sys.exit(1)
    
    user_id = COOKIES.get("c_user")
    variables = json.loads(USER_VARIABLES_JSON)
    variables["cursor"] = cursor
    variables["args"]["text"] = search_term  # Update search term dynamically
    
    # Add session-specific headers to look more legitimate
    current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]['account']
    session_headers, session_payload = get_session_data_from_cookies(current_account)
    
    # Use the session data from the account's cookie file if available
    headers = HEADERS.copy()
    headers.update(session_headers)
    
    # Rotate user agent
    headers["User-Agent"] = random.choice(USER_AGENTS)
    
    # Build the data payload - check for advanced format
    if "doc_id" in session_payload:
        # Use all data from session_payload for advanced format
        data = session_payload.copy()
        # Override only the variables with our search data
        data["variables"] = json.dumps(variables)
    else:
        # Use our default data structure for simple format
        data = {
            "__hslp": "09a0ri0ki0ki2K1nw5jwto2Kw5Uwm81yU0Hy1ax60k20hW0hy0nF09a04q87y0ke0SUlw4gwi80lbxO1vwgo25wCw6OwfW0h60kO0li0PU0zi3y08wwoGwRwca0ll039U4u4U6y0s60Z85G0Uo0yW0l-7o39w52w",
            "__sjsp": "hOi22wzz91e46p78F4rprV94iihFJyYiGChqQzHKayVVkUK9CF7gCEC58gx67oR3EpiDBUOpabCwIxmmjy8cp8B0-xC4KEc8bo9A0CbzE3uw8S2LxG58ek3aq4ocE8UC1qBDwFw8a11y8CnxswhwEwkEfE2VwQg-4o33wuo7S4ESfKepAVA7oK2vg1aE3IweW0cVGVEowtUbU_Vm3K2uEbFU12EcU78on704GwkUqwnqw6Xwo81rU0lbwaWfw2p83Vw2nE0yi08QwUw2L832w17W0dXw4Yw2bE7q0cjw",
            "__comet_req": "15",
            "fb_dtsg": "NAftR06AOPsyS-g4udRZQvZgBkssjaQdvXH7HH9MHNVmyuBi750iQvA:25:1754053582",
            "jazoest": "25485",
            "lsd": "Z-n_XPzGloTFb9EkUVjSLE",
            "__spin_r": "1025431519",
            "__spin_b": "trunk",
            "__spin_t": "1754094375",
            "__crn": "comet.fbweb.CometGroupsSearchRoute",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "SearchCometResultsPaginatedResultsQuery",
            "variables": json.dumps(variables),
            "server_timestamps": "true",
            "doc_id": "24106293015687348",
        }
        
        # Update with session-specific data
        data.update(session_payload)
    
    print(f"📡 Request via {current_account}")
    
    # SECURITY LOG: Log proxy usage for audit
    if PROXIES:
        print(f"🔒 Using Nimbleway proxy")
    else:
        print(f"⚠️  Direct connection (proxyless mode)")
    
    # Convert data dictionary to URL-encoded string for requests.post()
    data_encoded = urllib.parse.urlencode(data)
    
    resp = requests.post(
        "https://www.facebook.com/api/graphql/",
        headers=headers,
        cookies=COOKIES,
        data=data_encoded,  # Use URL-encoded string instead of dict
        proxies=PROXIES,  # <-- Use Nimble proxy if set
        timeout=30
    )
    
    # Check if proxy was actually used
    if hasattr(resp, 'request') and hasattr(resp.request, 'proxy'):
        print(f"🔍 Proxy used: {resp.request.proxy}")
    else:
        # Since the test script shows the proxy works, we'll assume it's working
        # if we get a response, even if the proxy attribute isn't set
        print(f"🔍 Proxy status: Working (response received)")
    
    # Check for rate limiting
    response_text = resp.text
    if is_rate_limited(response_text, resp.status_code):
        print(f"⚠️  Rate limiting detected!")
        handle_rate_limiting()
        return None
    
    try:
        return resp.json()
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON response. Status: {resp.status_code}")
        print(f"Response preview: {resp.text[:500]}")
        if resp.status_code == 200:
            print("⚠️  Got 200 but invalid JSON - might be a parsing issue")
        return None

def parse_snippet(snippet):
    # Example: "Public · 1.2K members · 10+ posts a day"
    privacy = "Unknown"
    member_count = "Unknown"
    posts_per_day = "Unknown"
    
    if snippet:
        parts = snippet.split(" · ")
        if len(parts) >= 3:
            privacy = parts[0].strip()
            member_count = parts[1].strip()
            posts_per_day = parts[2].strip()

    return privacy, member_count, posts_per_day

def extract_groups(response, search_term=None):
    try:
        edges = response["data"]["serpResponse"]["results"]["edges"]
        for edge in edges:
            node = edge.get("rendering_strategy", {}).get("view_model", {}).get("profile", {})
            if node.get("__typename") == "Group":
                # Parse city and state from search term
                city_state = None
                if search_term:
                    # Extract city and state from search term (e.g., "Dallas, TX" -> city="Dallas", state="TX")
                    parts = search_term.split(',')
                    if len(parts) >= 2:
                        city = parts[0].strip()
                        state = parts[1].strip()
                        city_state = f"{city}, {state}"
                    else:
                        city_state = search_term
                
                group_data = {
                    "id": node.get("id"),
                    "name": node.get("name"),
                    "url": node.get("url") or node.get("profile_url"),
                }
                
                # Add city/state information if available
                if city_state:
                    group_data["city_state"] = city_state
                
                yield group_data
    except KeyError as e:
        print(f"❌ KeyError extracting groups: {e}")
        print(f"📄 Response structure:")
        if "data" in response:
            print(f"   data keys: {list(response['data'].keys())}")
            if "serpResponse" not in response["data"]:
                print(f"   ❌ serpResponse missing from data")
                print(f"   📄 Available data keys: {list(response['data'].keys())}")
        else:
            print(f"   ❌ 'data' field missing from response")
            print(f"   📄 Response keys: {list(response.keys())}")
        
        # Check if this is an error response
        if "error" in response:
            print(f"⚠️  Facebook error detected: {response['error']}")
        elif "errors" in response:
            print(f"⚠️  Facebook errors detected: {response['errors']}")
        
        return []
    except Exception as e:
        print(f"❌ Error extracting groups: {e}")
        print(f"📄 Response structure: {list(response.keys()) if isinstance(response, dict) else type(response)}")
        return []

def get_next_cursor(response):
    try:
        return response["data"]["serpResponse"]["results"]["page_info"]["end_cursor"]
    except Exception:
        return None

def has_graphql_errors(response):
    """Check if the response contains GraphQL errors that should halt processing"""
    if not isinstance(response, dict):
        return False
    
    # Check for explicit error fields
    if "error" in response:
        print(f"🚨 GraphQL error detected: {response['error']}")
        return True
    elif "errors" in response:
        print(f"🚨 GraphQL errors detected: {response['errors']}")
        return True
    
    # Check for missing required data structure (indicates authentication/permission error)
    if "data" not in response:
        print(f"🚨 GraphQL response missing 'data' field - authentication issue")
        return True
    
    if "data" in response and response["data"] is None:
        print(f"🚨 GraphQL response has null data - authentication issue")
        return True
    
    return False

# --- WORKER FUNCTION FOR PARALLEL PROCESSING ---
def worker_process(account_index, city_start_index, city_end_index, shared_state, output_lock, remaining_terms_queue=None, cities_to_process=None):
    """Worker process to handle a specific account and city range with GraphQL-only approach"""
    
    try:
        # Load accounts and get the specific account for this worker
        accounts = load_accounts()
        if account_index >= len(accounts):
            print(f"❌ Worker {account_index}: Account index out of range")
            return
        
        account = accounts[account_index]
        account_email = account["account"]
        
        print(f"🚀 Worker {account_index}: Starting with account {account_email}")
        print(f"   Cities {city_start_index} to {city_end_index}")
        
        # Load URLs and get the city range for this worker
        urls = load_search_urls_from_file()
        if not urls:
            print(f"❌ Worker {account_index}: No URLs found")
            return
        
        # Load progress tracking for this specific worker
        progress_data = load_url_progress(account_index)
        pagination_target = progress_data.get("pagination_target", 1000)
        
        # Extract search terms for this worker's city range
        search_terms = []
        for i in range(city_start_index, min(city_end_index, len(urls))):
            url = urls[i]
            search_term = extract_search_term_from_url(url)
            if search_term:
                search_terms.append(search_term)
        
        print(f"📋 Worker {account_index}: Processing {len(search_terms)} search terms")
        print(f"🎯 Strategy: GraphQL pagination until 3 consecutive zero results")
        
        # Load cookies for this account
        try:
            cookies = load_cookies_for_account(account_email)
            print(f"✅ Worker {account_index}: Loaded cookies for {account_email}")
        except Exception as e:
            print(f"❌ Worker {account_index}: Failed to load cookies for {account_email}: {e}")
            return
        
        # Track groups seen by this worker
        worker_seen_ids = set()
        graphql_calls = 0
        
        # Process each search term
        for search_term in search_terms:
            try:
                print(f"\n🌟 Worker {account_index}: Starting city '{search_term}'")
                
                # Load city progress
                city_progress = progress_data.get("city_progress", {})
                city_data = city_progress.get(search_term, {})
                
                # Check if city is already completed
                status = city_data.get("status", "new")
                if status in ["completed", "completed_zero_results", "completed_no_cursor", "completed_no_groups"]:
                    print(f"✅ Worker {account_index}: City '{search_term}' already completed with status: {status}")
                    continue
                
                # Check if city should be retried using new retry logic
                if not should_retry_city(city_data):
                    status = city_data.get("status", "new")
                    retry_count = city_data.get("retry_count", 0)
                    print(f"⏭️  Worker {account_index}: Skipping '{search_term}' (status: {status}, retries: {retry_count})")
                    continue
                
                # Initialize cursor and pagination tracking
                cursor = city_data.get("cursor")
                pagination_count = city_data.get("pagination_count", 0)
                retry_count = city_data.get("retry_count", 0)
                consecutive_zero_results = 0
                search_completed = False
                
                if retry_count > 0:
                    print(f"🔄 Worker {account_index}: Retrying '{search_term}' (attempt #{retry_count + 1}/{MAX_CITY_RETRIES})")
                else:
                    print(f"📊 Worker {account_index}: Starting GraphQL pagination for '{search_term}'")
                print(f"   Initial cursor: {cursor}")
                print(f"   Pagination count: {pagination_count}")
                
                # Update worker progress with new retry-aware function
                update_worker_progress(progress_data, account_index, search_term, "processing")
                update_city_progress_with_retry(progress_data, search_term, account_index, cursor, pagination_count, 0, "processing")
                save_url_progress(progress_data, account_index)
                
                max_consecutive_zeros = 3
                
                print(f"🔄 Worker {account_index}: Starting GraphQL pagination until {max_consecutive_zeros} consecutive zero results")
                
                while not search_completed and consecutive_zero_results < max_consecutive_zeros:
                    try:
                        print(f"🔍 Worker {account_index}: GraphQL pagination {pagination_count + 1} for '{search_term}' (zeros: {consecutive_zero_results}/{max_consecutive_zeros})")
                        response = fetch_page_worker(cursor, search_term, account_email, cookies)
                        
                        # Increment pagination count BEFORE processing response
                        pagination_count += 1
                        graphql_calls += 1
                        
                        if response is None:
                            print(f"⚠️  Worker {account_index}: Rate limiting detected for {search_term}")
                            # Update progress with rate limited status and current pagination count
                            update_city_progress_with_retry(progress_data, search_term, account_index, cursor, pagination_count, 0, "rate_limited", "Rate limiting detected")
                            save_url_progress(progress_data, account_index)
                            
                            # Wait longer for rate limiting
                            rate_limit_wait = random.uniform(30.0, 60.0)
                            print(f"⏱️  Worker {account_index}: Waiting {rate_limit_wait:.1f} seconds for rate limit...")
                            time.sleep(rate_limit_wait)
                            continue
                        
                        # Check for GraphQL errors that should halt processing
                        if has_graphql_errors(response):
                            print(f"🚨 Worker {account_index}: GraphQL errors detected - HALTING WORKER")
                            print(f"💾 Saving progress and stopping worker...")
                            update_city_progress_with_retry(progress_data, search_term, account_index, cursor, pagination_count, 0, "failed", "GraphQL errors detected")
                            save_url_progress(progress_data, account_index)
                            print(f"❌ Worker {account_index} stopped due to GraphQL errors")
                            return  # Exit the worker entirely
                        
                        # Process response
                        groups = extract_groups(response, search_term)
                        new_groups = 0
                        
                        for group in groups:
                            if group["id"] not in worker_seen_ids:
                                # Use lock to safely append to shared output file
                                with output_lock:
                                    append_group_safe(group)
                                worker_seen_ids.add(group["id"])
                                new_groups += 1
                        
                        # Update progress with current results
                        update_city_progress_with_retry(progress_data, search_term, account_index, cursor, pagination_count, new_groups, "processing")
                        save_url_progress(progress_data, account_index)
                        
                        print(f"📊 Worker {account_index}: Found {new_groups} new groups in pagination {pagination_count}")
                        
                        # Check for zero results
                        if new_groups == 0:
                            consecutive_zero_results += 1
                            print(f"🔄 Worker {account_index}: Zero results #{consecutive_zero_results}/{max_consecutive_zeros}")
                        else:
                            consecutive_zero_results = 0  # Reset counter
                        
                        # Get next cursor
                        cursor = get_next_cursor(response)
                        if not cursor:
                            print(f"✅ Worker {account_index}: No more pages available for '{search_term}'")
                            update_city_progress_with_retry(progress_data, search_term, account_index, cursor, pagination_count, new_groups, "completed_no_cursor")
                            save_url_progress(progress_data, account_index)
                            search_completed = True
                            break
                        
                        # Random delay between requests
                        delay = random.uniform(0.5, 2.0)
                        time.sleep(delay)
                        
                    except Exception as e:
                        print(f"❌ Worker {account_index}: Error in pagination for '{search_term}': {e}")
                        error_msg = str(e)
                        update_city_progress_with_retry(progress_data, search_term, account_index, cursor, pagination_count, 0, "failed", error_msg)
                        save_url_progress(progress_data, account_index)
                        break
                
                # Mark completion
                if consecutive_zero_results >= max_consecutive_zeros:
                    print(f"✅ Worker {account_index}: Completed '{search_term}' after {max_consecutive_zeros} consecutive zero results")
                    update_city_progress_with_retry(progress_data, search_term, account_index, cursor, pagination_count, 0, "completed_zero_results")
                    save_url_progress(progress_data, account_index)
                
            except Exception as e:
                print(f"❌ Worker {account_index}: Error processing '{search_term}': {e}")
                error_msg = str(e)
                update_city_progress_with_retry(progress_data, search_term, account_index, None, 0, 0, "failed", error_msg)
                save_url_progress(progress_data, account_index)
                continue
        
        print(f"🏁 Worker {account_index}: Completed all assigned cities")
        print(f"📊 Worker {account_index}: Total GraphQL calls: {graphql_calls}")
        print(f"📊 Worker {account_index}: Total groups found: {len(worker_seen_ids)}")
        
        # Update worker status to completed
        update_worker_progress(progress_data, account_index, "ALL_COMPLETED", "completed")
        save_url_progress(progress_data, account_index)
        
    except Exception as e:
        print(f"❌ Worker {account_index}: Critical error: {e}")
        import traceback
        traceback.print_exc()

def fetch_page_worker(cursor, search_term, account_email, cookies):
    """Worker version of fetch_page that doesn't use global variables"""
    if not cookies or not cookies.get("c_user"):
        raise Exception(f"Invalid cookies for {account_email}")
    
    user_id = cookies.get("c_user")
    variables = json.loads(USER_VARIABLES_JSON)
    variables["cursor"] = cursor
    variables["args"]["text"] = search_term
    
    # Get session data for this account
    session_headers, session_payload = get_session_data_from_cookies(account_email)
    
    headers = HEADERS.copy()
    headers.update(session_headers)
    headers["User-Agent"] = random.choice(USER_AGENTS)
    
    # Build the data payload - check for advanced format
    if "doc_id" in session_payload:
        # Use all data from session_payload for advanced format
        data = session_payload.copy()
        # Override only the variables with our search data
        data["variables"] = json.dumps(variables)
    else:
        # Use our default data structure for simple format
        data = {
            "__hslp": "09a0ri0ki0ki2K1nw5jwto2Kw5Uwm81yU0Hy1ax60k20hW0hy0nF09a04q87y0ke0SUlw4gwi80lbxO1vwgo25wCw6OwfW0h60kO0li0PU0zi3y08wwoGwRwca0ll039U4u4U6y0s60Z85G0Uo0yW0l-7o39w52w",
            "__sjsp": "hOi22wzz91e46p78F4rprV94iihFJyYiGChqQzHKayVVkUK9CF7gCEC58gx67oR3EpiDBUOpabCwIxmmjy8cp8B0-xC4KEc8bo9A0CbzE3uw8S2LxG58ek3aq4ocE8UC1qBDwFw8a11y8CnxswhwEwkEfE2VwQg-4o33wuo7S4ESfKepAVA7oK2vg1aE3IweW0cVGVEowtUbU_Vm3K2uEbFU12EcU78on704GwkUqwnqw6Xwo81rU0lbwaWfw2p83Vw2nE0yi08QwUw2L832w17W0dXw4Yw2bE7q0cjw",
            "__comet_req": "15",
            "fb_dtsg": "NAftR06AOPsyS-g4udRZQvZgBkssjaQdvXH7HH9MHNVmyuBi750iQvA:25:1754053582",
            "jazoest": "25485",
            "lsd": "Z-n_XPzGloTFb9EkUVjSLE",
            "__spin_r": "1025431519",
            "__spin_b": "trunk",
            "__spin_t": "1754094375",
            "__crn": "comet.fbweb.CometGroupsSearchRoute",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "SearchCometResultsPaginatedResultsQuery",
            "variables": json.dumps(variables),
            "server_timestamps": "true",
            "doc_id": "24106293015687348",
        }
        
        data.update(session_payload)
    
    print(f"📡 Worker request via {account_email} for city '{search_term}'")
    
    # Convert data dictionary to URL-encoded string
    data_encoded = urllib.parse.urlencode(data)
    
    # Use global PROXIES if available
    proxies = globals().get('PROXIES', None)
    
    resp = requests.post(
        "https://www.facebook.com/api/graphql/",
        headers=headers,
        cookies=cookies,
        data=data_encoded,
        proxies=proxies,
        timeout=30
    )
    
    # Check for rate limiting
    response_text = resp.text
    if is_rate_limited(response_text, resp.status_code):
        print(f"⚠️  Worker rate limiting detected for city '{search_term}'")
        return None
    
    try:
        response_data = resp.json()
        
        # Check for GraphQL errors in the response
        if has_graphql_errors(response_data):
            print(f"🚨 GraphQL errors in response for '{search_term}' - this will halt the worker")
            # Return the error response so the worker can detect it and halt
            return response_data
        
        return response_data
    except json.JSONDecodeError:
        print(f"❌ Worker: Invalid JSON response for '{search_term}'. Status: {resp.status_code}")
        # Return None but let the calling function handle the error categorization
        return None

def append_group_safe(group):
    """Thread-safe version of append_group"""
    try:
        # Duplicate prevention: only write if not already seen
        global seen_ids
        if group["id"] not in seen_ids:
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(group, ensure_ascii=False) + "\n")
            seen_ids.add(group["id"])
    except Exception as e:
        print(f"⚠️  Error appending group: {e}")

def deduplicate_output_file():
    """Remove duplicates from the output file"""
    try:
        print("🧹 Deduplicating output file...")
        
        seen_ids = set()
        unique_groups = []
        
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        group = json.loads(line.strip())
                        if group["id"] not in seen_ids:
                            unique_groups.append(group)
                            seen_ids.add(group["id"])
                    except json.JSONDecodeError:
                        continue
            
            # Rewrite file with unique groups
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                for group in unique_groups:
                    f.write(json.dumps(group, ensure_ascii=False) + "\n")
            
            print(f"✅ Deduplicated: {len(unique_groups)} unique groups kept")
        else:
            print("⚠️  Output file does not exist yet")
            
    except Exception as e:
        print(f"⚠️  Error deduplicating: {e}")

def test_account_working(account_email, cookies):
    """Test if an account can successfully make GraphQL requests"""
    try:
        print(f"🧪 Testing account {account_email}...")
        
        # Test with a simple search term
        test_search_term = "test"
        variables = json.loads(USER_VARIABLES_JSON)
        variables["cursor"] = None
        variables["args"]["text"] = test_search_term
        
        # Get session data for this account
        session_headers, session_payload = get_session_data_from_cookies(account_email)
        
        headers = HEADERS.copy()
        headers.update(session_headers)
        headers["User-Agent"] = random.choice(USER_AGENTS)
        
        # Build the data payload
        # Check if we have doc_id in session_payload (advanced format)
        if "doc_id" in session_payload:
            print(f"   🎯 Using doc_id from cookie: {session_payload['doc_id']}")
            # Use all data from session_payload for advanced format
            data = session_payload.copy()
            # Override only the variables with our test data
            data["variables"] = json.dumps(variables)
        else:
            # Use our default data structure for simple format
            data = {
                "__hslp": "09a0ri0ki0ki2K1nw5jwto2Kw5Uwm81yU0Hy1ax60k20hW0hy0nF09a04q87y0ke0SUlw4gwi80lbxO1vwgo25wCw6OwfW0h60kO0li0PU0zi3y08wwoGwRwca0ll039U4u4U6y0s60Z85G0Uo0yW0l-7o39w52w",
                "__sjsp": "hOi22wzz91e46p78F4rprV94iihFJyYiGChqQzHKayVVkUK9CF7gCEC58gx67oR3EpiDBUOpabCwIxmmjy8cp8B0-xC4KEc8bo9A0CbzE3uw8S2LxG58ek3aq4ocE8UC1qBDwFw8a11y8CnxswhwEwkEfE2VwQg-4o33wuo7S4ESfKepAVA7oK2vg1aE3IweW0cVGVEowtUbU_Vm3K2uEbFU12EcU78on704GwkUqwnqw6Xwo81rU0lbwaWfw2p83Vw2nE0yi08QwUw2L832w17W0dXw4Yw2bE7q0cjw",
                "__comet_req": "15",
                "fb_dtsg": "NAftR06AOPsyS-g4udRZQvZgBkssjaQdvXH7HH9MHNVmyuBi750iQvA:25:1754053582",
                "jazoest": "25485",
                "lsd": "Z-n_XPzGloTFb9EkUVjSLE",
                "__spin_r": "1025431519",
                "__spin_b": "trunk",
                "__spin_t": "1754094375",
                "__crn": "comet.fbweb.CometGroupsSearchRoute",
                "fb_api_caller_class": "RelayModern",
                "fb_api_req_friendly_name": "SearchCometResultsPaginatedResultsQuery",
                "variables": json.dumps(variables),
                "server_timestamps": "true",
                "doc_id": "24106293015687348",
            }
            
            # Update with session-specific data
            data.update(session_payload)
        
        # Convert data dictionary to URL-encoded string
        data_encoded = urllib.parse.urlencode(data)
        
        # Use global PROXIES if available
        proxies = globals().get('PROXIES', None)
        
        resp = requests.post(
            "https://www.facebook.com/api/graphql/",
            headers=headers,
            cookies=cookies,
            data=data_encoded,
            proxies=proxies,
            timeout=30
        )
        
        # Get response text safely
        try:
            response_text = resp.text
        except Exception as e:
            print(f"❌ Account {account_email}: Could not get response text - {e}")
            return False, "response_text_error"
        
        # Check for rate limiting or errors with better error handling
        try:
            if is_rate_limited(response_text, resp.status_code):
                print(f"❌ Account {account_email}: Rate limited")
                return False, "rate_limited"
        except Exception as e:
            print(f"❌ Account {account_email}: Error checking rate limiting - {e}")
            # Continue anyway - might still be a valid response
        
        # Check status code first
        if resp.status_code != 200:
            print(f"❌ Account {account_email}: HTTP {resp.status_code}")
            return False, f"http_{resp.status_code}"
        
        # Check if we got a valid JSON response
        try:
            response_data = resp.json()
            
            # Check for Facebook errors
            if "error" in response_data:
                print(f"❌ Account {account_email}: Facebook error - {response_data['error']}")
                return False, "facebook_error"
            elif "errors" in response_data:
                print(f"❌ Account {account_email}: Facebook errors - {response_data['errors']}")
                return False, "facebook_errors"
            
            # Check if we have the expected response structure
            if "data" in response_data and "serpResponse" in response_data["data"]:
                print(f"✅ Account {account_email}: Working!")
                return True, "working"
            else:
                print(f"❌ Account {account_email}: Unexpected response structure")
                # Print first 200 chars of response to debug
                response_preview = str(response_data)[:200] if response_data else "None"
                print(f"   Response preview: {response_preview}...")
                return False, "invalid_structure"
                
        except json.JSONDecodeError as e:
            print(f"❌ Account {account_email}: Invalid JSON response - {e}")
            # Print response details for debugging
            print(f"   Status code: {resp.status_code}")
            print(f"   Response headers: {dict(resp.headers)}")
            print(f"   Response length: {len(response_text)} characters")
            if response_text:
                response_preview = response_text[:500] if len(response_text) > 500 else response_text
                print(f"   Response content: '{response_preview}'")
                
                # Check if it's HTML (login page, etc.)
                if response_text.strip().startswith('<!DOCTYPE') or response_text.strip().startswith('<html'):
                    print(f"   ⚠️  Response appears to be HTML - account may need login")
                    # Look for specific indicators
                    if 'login' in response_text.lower():
                        print(f"   🔍 Login page detected - cookies likely expired")
                        return False, "login_required"
                    elif 'checkpoint' in response_text.lower():
                        print(f"   🔍 Checkpoint page detected - account may be restricted")
                        return False, "checkpoint_required"
                    else:
                        print(f"   🔍 HTML page returned - unexpected response")
                        return False, "html_response"
                elif not response_text.strip():
                    print(f"   ⚠️  Response is completely empty")
                    return False, "empty_response"
                else:
                    print(f"   🔍 Non-JSON response received")
                    return False, "invalid_json"
            else:
                print(f"   ⚠️  No response content received")
                return False, "no_content"
            
    except Exception as e:
        print(f"❌ Account {account_email}: Test failed - {e}")
        # Print the full traceback for debugging
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False, "exception"

def get_working_accounts():
    """Test all accounts and return only the working ones"""
    print("\n🧪 Testing all accounts with actual GraphQL requests...")
    print("   This will identify which accounts can successfully make requests")
    
    working_accounts = []
    failed_accounts = []
    
    for i, account in enumerate(ACCOUNTS):
        account_email = account["account"]
        
        try:
            print(f"🔧 Loading cookies for {account_email}...")
            # Load cookies for this account
            cookies = load_cookies_for_account(account_email)
            print(f"   ✅ Cookies loaded successfully ({len(cookies)} cookies)")
            
            # Test the account
            is_working, status = test_account_working(account_email, cookies)
            
            if is_working:
                working_accounts.append((i, account, cookies))
                print(f"   ✅ {account_email}: Added to working accounts")
            else:
                failed_accounts.append((account_email, status))
                print(f"   ❌ {account_email}: Failed ({status})")
                
        except Exception as e:
            error_msg = f"setup_error: {e}"
            failed_accounts.append((account_email, error_msg))
            print(f"   ❌ {account_email}: Setup failed - {e}")
            
            # If it's a cookie loading error, provide more details
            if "Failed to load cookies" in str(e):
                cookie_file = get_cookie_file_path(account_email)
                if os.path.exists(cookie_file):
                    try:
                        with open(cookie_file, "r") as f:
                            raw_cookies = json.load(f)
                        print(f"      Cookie file exists with {len(raw_cookies)} entries")
                        print(f"      Sample keys: {list(raw_cookies.keys())[:5]}")
                        # Check for problematic cookie values
                        problem_cookies = []
                        for key, value in raw_cookies.items():
                            if isinstance(value, (dict, list)):
                                problem_cookies.append(f"{key}: {type(value).__name__}")
                        if problem_cookies:
                            print(f"      Problematic cookie types: {problem_cookies[:3]}")
                    except Exception as cookie_debug_error:
                        print(f"      Could not debug cookie file: {cookie_debug_error}")
                else:
                    print(f"      Cookie file does not exist: {cookie_file}")
        
        # Small delay between tests to avoid overwhelming Facebook
        time.sleep(random.uniform(1.0, 3.0))
    
    print(f"\n📊 Account Testing Results:")
    print(f"   ✅ Working accounts: {len(working_accounts)}")
    print(f"   ❌ Failed accounts: {len(failed_accounts)}")
    
    if failed_accounts:
        print(f"   Failed account details:")
        for email, status in failed_accounts:
            print(f"     - {email}: {status}")
    
    if not working_accounts:
        print("❌ No working accounts found! Cannot proceed.")
        print("   Please check your accounts and cookies.")
        print("   Common issues:")
        print("   1. Cookie files may have expired")
        print("   2. Cookie files may have invalid format")
        print("   3. Accounts may be rate limited or banned")
        print("   4. Network/proxy issues")
        sys.exit(1)
    
    return working_accounts

def categorize_failure(error_msg, response_status=None):
    """Categorize failure as temporary or permanent"""
    error_lower = str(error_msg).lower()
    
    # Network/timeout errors - temporary
    if any(term in error_lower for term in ["timeout", "connection", "network", "proxy", "ssl"]):
        return "network_error"
    
    # JSON parsing errors - could be temporary
    if any(term in error_lower for term in ["json", "decode", "parse"]):
        return "json_error"
    
    # Rate limiting - temporary
    if any(term in error_lower for term in ["rate limit", "too many requests", "429"]):
        return "rate_limited"
    
    # Account issues - potentially permanent
    if any(term in error_lower for term in ["login_required", "checkpoint", "blocked", "banned", "suspended"]):
        return "blocked"
    
    # HTTP errors
    if response_status:
        if response_status in [401, 403]:
            return "blocked"
        elif response_status in [500, 502, 503, 504]:
            return "network_error"
        elif response_status == 429:
            return "rate_limited"
    
    # Default to temporary failure
    return "failed"

def should_retry_city(city_data):
    """Determine if a city should be retried based on its failure history"""
    status = city_data.get("status", "new")
    retry_count = city_data.get("retry_count", 0)
    
    # Never retry permanently failed or completed cities
    if status in PERMANENT_FAILURE_STATUSES + COMPLETED_STATUSES:
        return False
    
    # Retry temporary failures if under retry limit
    if status in TEMP_FAILURE_STATUSES:
        return retry_count < MAX_CITY_RETRIES
    
    # Retry new or processing cities
    return True

def calculate_retry_delay(retry_count):
    """Calculate exponential backoff delay for retries"""
    return RETRY_BACKOFF_BASE * (2 ** retry_count) + random.uniform(1, 5)

def update_city_progress_with_retry(progress_data, search_term, worker_id, cursor=None, pagination_count=0, new_groups=0, status="processing", error_msg=None):
    """Update progress for a specific city with retry tracking"""
    if "city_progress" not in progress_data:
        progress_data["city_progress"] = {}
    
    # Get existing city data
    existing_data = progress_data["city_progress"].get(search_term, {})
    retry_count = existing_data.get("retry_count", 0)
    
    # If this is a failure, categorize it and potentially increment retry count
    if status == "failed" and error_msg:
        status = categorize_failure(error_msg)
        if status in TEMP_FAILURE_STATUSES:
            retry_count += 1
            # Check if we've exceeded retry limit
            if retry_count >= MAX_CITY_RETRIES:
                status = "permanently_failed"
                print(f"🚨 Worker {worker_id}: City '{search_term}' permanently failed after {retry_count} retries")
    
    progress_data["city_progress"][search_term] = {
        "cursor": cursor,
        "pagination_count": pagination_count,
        "new_groups": new_groups,
        "status": status,
        "retry_count": retry_count,
        "last_updated": datetime.datetime.now().isoformat(),
        "worker_id": worker_id,
        "last_error": error_msg if error_msg else existing_data.get("last_error")
    }

def get_cities_needing_retry(progress_data, search_terms):
    """Get cities that need retry with appropriate delays"""
    cities_to_retry = []
    current_time = time.time()
    
    city_progress = progress_data.get("city_progress", {})
    
    for search_term in search_terms:
        city_data = city_progress.get(search_term, {})
        
        if not should_retry_city(city_data):
            continue
        
        status = city_data.get("status", "new")
        retry_count = city_data.get("retry_count", 0)
        last_updated = city_data.get("last_updated")
        
        # For new cities or processing cities, add immediately
        if status in ["new", "processing"]:
            cities_to_retry.append(search_term)
            continue
        
        # For failed cities, check if enough time has passed for retry
        if status in TEMP_FAILURE_STATUSES and last_updated:
            try:
                last_attempt_time = datetime.datetime.fromisoformat(last_updated).timestamp()
                retry_delay = calculate_retry_delay(retry_count)
                
                if current_time - last_attempt_time >= retry_delay:
                    cities_to_retry.append(search_term)
                    print(f"🔄 City '{search_term}' ready for retry #{retry_count + 1} after {retry_delay:.1f}s delay")
                else:
                    remaining_time = retry_delay - (current_time - last_attempt_time)
                    print(f"⏳ City '{search_term}' retry in {remaining_time:.1f}s (attempt #{retry_count + 1})")
            except Exception as e:
                # If we can't parse the timestamp, retry anyway
                cities_to_retry.append(search_term)
        
        # Add cities that haven't been processed yet
        elif search_term not in city_progress:
            cities_to_retry.append(search_term)
    
    return cities_to_retry

# --- MAIN LOOP ---
if __name__ == "__main__":
    print(f"Starting Facebook Groups GraphQL Scraper - Target: {TARGET_CALLS} GraphQL calls")
    print(f"Start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 GraphQL-Only Mode: No DOM scraping, pure GraphQL pagination")
    print(f"🚨 HALT-ON-ERROR: Workers will stop immediately if GraphQL errors are encountered")
    print("-" * 50)
    
    # Choose proxy mode
    print("Choose proxy mode: [1] Nimbleway proxy (REQUIRED) [2] Proxyless")
    mode = input("Enter 1 or 2 (default 1): ").strip()
    if mode == "2":
        PROXIES = None
        print("Running proxyless (direct connection)...")
    else:
        # Nimbleway Proxy Integration - REQUIRED for security
        NIMBLEWAY_SETTINGS_FILE = os.path.join(PARENT_DIR, "settings", "nimbleway_settings.json")
        if not os.path.exists(NIMBLEWAY_SETTINGS_FILE):
            print("❌ CRITICAL ERROR: Nimbleway settings file not found!")
            print(f"   Expected file: {NIMBLEWAY_SETTINGS_FILE}")
            print("   This is REQUIRED for security when using proxy mode.")
            print("   Please create this file with your Nimbleway credentials.")
            sys.exit(1)
        
        try:
            with open(NIMBLEWAY_SETTINGS_FILE, "r", encoding="utf-8") as f:
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
            # URL-encode the account name to handle spaces (like in test script)
            encoded_account_name = urllib.parse.quote(ACCOUNT_NAME)
            NIMBLEWAY_PROXY = f"http://account-{encoded_account_name}-pipeline-{PIPELINE_NAME}:{PIPELINE_PASSWORD}@{NIMBLEWAY_HOST}:{NIMBLEWAY_PORT}"
            
            PROXIES = {"http": NIMBLEWAY_PROXY, "https": NIMBLEWAY_PROXY}
            print(f"✅ Using Nimbleway proxy: {NIMBLEWAY_PROXY}")
            print("🔒 SECURITY: All requests will go through Nimbleway proxy")
            
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
    
    # Load accounts with parallel processing
    try:
        ACCOUNTS.extend(load_accounts())
        print(f"✅ Loaded {len(ACCOUNTS)} accounts")
    except Exception as e:
        print(f"❌ Failed to initialize accounts: {e}")
        print("Please run: python scripts/generate_cookies.py")
        sys.exit(1)
    
    # Test proxy connection if using Nimbleway mode
    if mode != "2":
        print("⚠️  Skipping proxy validation test (pipeline may block test sites)")
        print("   Will test proxy with actual Facebook requests...")
        
        # Validate proxy configuration
        if not PROXIES:
            print("❌ CRITICAL ERROR: No proxy configuration found!")
            print("   Proxy mode is enabled but no proxy is configured.")
            sys.exit(1)
        
        print(f"🔍 Proxy configuration: {PROXIES}")
        print("✅ Proxy configuration loaded - proceeding with account testing")
    
    # Test accounts and get only working ones
    working_accounts = get_working_accounts()
    
    # Load URLs
    urls = load_search_urls_from_file()
    if not urls:
        print("❌ No URLs found. Using hardcoded search term.")
        search_terms = ["arlington tx"]  # Fallback
    else:
        print(f"✅ Loaded {len(urls)} URLs for parallel processing")
    
    # Extract search terms from URLs
    search_terms = []
    for url in urls:
        search_term = extract_search_term_from_url(url)
        if search_term:
            search_terms.append(search_term)
    
    if not search_terms:
        print("❌ No valid search terms extracted from URLs. Using fallback.")
        search_terms = ["arlington tx"]
    
    print(f"📋 Processing {len(search_terms)} search terms with GraphQL-only approach")
    
    # Load progress and determine which cities need processing
    progress_data = load_url_progress()
    city_progress = progress_data.get("city_progress", {})
    
    # Print progress summary using new retry-aware logic
    total_cities = len(search_terms)
    completed_cities = len([city for city in search_terms if city_progress.get(city, {}).get("status") in COMPLETED_STATUSES])
    permanently_failed = len([city for city in search_terms if city_progress.get(city, {}).get("status") in PERMANENT_FAILURE_STATUSES])
    temp_failed = len([city for city in search_terms if city_progress.get(city, {}).get("status") in TEMP_FAILURE_STATUSES])
    
    print(f"📊 Progress Summary:")
    print(f"   • Total cities: {total_cities}")
    print(f"   • Completed: {completed_cities}")
    print(f"   • Permanently failed: {permanently_failed}")
    print(f"   • Temporary failures (will retry): {temp_failed}")
    print(f"   • Remaining: {total_cities - completed_cities - permanently_failed}")
    
    # Count by status with retry info
    status_counts = Counter()
    retry_counts = Counter()
    for city in search_terms:
        city_data = city_progress.get(city, {})
        status = city_data.get("status", "new")
        retry_count = city_data.get("retry_count", 0)
        status_counts[status] += 1
        if retry_count > 0:
            retry_counts[f"retry_{retry_count}"] += 1
    
    print(f"   • Status breakdown:")
    for status, count in status_counts.items():
        print(f"     - {status}: {count} cities")
    
    if retry_counts:
        print(f"   • Retry breakdown:")
        for retry_status, count in retry_counts.items():
            print(f"     - {retry_status}: {count} cities")
    
    # Get cities that need processing using new retry logic
    cities_to_process = get_cities_needing_retry(progress_data, search_terms)
    
    print(f"🎯 Cities to process: {len(cities_to_process)}")
    if len(cities_to_process) < len(search_terms):
        print(f"   📝 Note: {len(search_terms) - len(cities_to_process)} cities are completed or permanently failed")
    
    if not cities_to_process:
        print("✅ All cities are completed or permanently failed!")
        print(f"   📊 Final Summary: {completed_cities} completed, {permanently_failed} permanently failed")
        sys.exit(0)
    
    # Use all working accounts as workers automatically
    num_workers = len(working_accounts)
    print(f"\n🚀 Starting {num_workers} workers using all working accounts")
    print(f"📊 Workers will process cities using PURE GRAPHQL approach:")
    print(f"   • STEP 1: Start directly with GraphQL pagination (no DOM)")
    print(f"   • STEP 2: Continue until 3 consecutive zero results")
    print(f"   • STEP 3: Move to next URL only after current URL is fully exhausted")
    print(f"   • 🔄 RESTART-SAFE: Interrupted cities restart from last cursor")
    print(f"   • 🎯 EFFICIENT: Pure GraphQL pagination for maximum speed")
    print(f"📁 Each worker will save progress to their own file (url_progress_worker_X.json)")
    
    # Distribute cities among workers
    cities_per_worker = len(cities_to_process) // num_workers
    remainder = len(cities_to_process) % num_workers
    
    # Create shared state and locks for multiprocessing
    manager = Manager()
    output_lock = manager.Lock()
    shared_state = manager.dict()
    
    # Start worker processes
    processes = []
    current_index = 0
    
    for worker_id in range(num_workers):
        # Get the working account for this worker
        account_index, account, cookies = working_accounts[worker_id]
        account_email = account["account"]
        
        # Calculate city range for this worker
        worker_cities = cities_per_worker + (1 if worker_id < remainder else 0)
        city_start = current_index
        city_end = current_index + worker_cities
        current_index = city_end
        
        # Select cities for this worker
        worker_city_list = cities_to_process[city_start:city_end]
        
        print(f"📋 Worker {worker_id} ({account_email}): Processing {len(worker_city_list)} cities ({city_start}-{city_end-1})")
        
        # Convert city list to indices in the original search_terms list
        city_start_index = 0
        city_end_index = len(search_terms)
        if worker_city_list:
            try:
                city_start_index = search_terms.index(worker_city_list[0])
                city_end_index = search_terms.index(worker_city_list[-1]) + 1
            except ValueError:
                pass  # Use full range if cities not found
        
        # Start worker process
        p = mp.Process(
            target=worker_process,
            args=(account_index, city_start_index, city_end_index, shared_state, output_lock, None, worker_city_list)
        )
        p.start()
        processes.append(p)
        
        # Small delay between starting workers
        time.sleep(1)
    
    print(f"🏁 All {num_workers} workers started!")
    print("⏱️  Monitoring worker progress... (Ctrl+C to stop safely)")
    print("🚨 NOTE: Workers will halt immediately upon encountering GraphQL errors")
    
    try:
        # Wait for all workers to complete
        for i, p in enumerate(processes):
            p.join()
            if p.exitcode == 0:
                print(f"✅ Worker {i} completed normally")
            else:
                print(f"⚠️  Worker {i} exited early (may have encountered GraphQL errors)")
        
        print("\n🎉 All workers completed!")
        print("📋 Check progress files for any workers that stopped due to GraphQL errors")
        
        # Merge progress from all workers
        merge_all_worker_progress()
        
        # Deduplicate the output file
        deduplicate_output_file()
        
        # Final statistics
        final_count = 0
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                final_count = sum(1 for line in f if line.strip())
        
        elapsed_time = time.time() - START_TIME
        print(f"\n📊 FINAL STATISTICS:")
        print(f"   • Total runtime: {elapsed_time:.1f} seconds")
        print(f"   • Total groups found: {final_count}")
        print(f"   • Workers used: {num_workers}")
        print(f"   • Cities processed: {len(cities_to_process)}")
        print(f"✅ GraphQL-only scraping completed successfully!")
        
    except KeyboardInterrupt:
        print(f"\n\n🛑 Interrupt received! Stopping workers...")
        
        # Terminate all worker processes
        for p in processes:
            if p.is_alive():
                p.terminate()
        
        # Wait for processes to stop
        for p in processes:
            p.join(timeout=5)
        
        # Merge any available progress
        try:
            merge_all_worker_progress()
            deduplicate_output_file()
        except:
            pass
        
        print(f"✅ Workers stopped safely. Progress has been saved.")
        sys.exit(0) 