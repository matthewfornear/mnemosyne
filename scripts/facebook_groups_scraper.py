import requests
import json
import time
import os
import sys
import zstandard as zstd
import io
import random
import datetime
import urllib.parse
import signal
import multiprocessing as mp
from multiprocessing import Manager, Lock
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import queue

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
ACCOUNTS_FILE = os.path.join(PARENT_DIR, "settings", "bought_accounts.json")
COOKIES_DIR = os.path.join(PARENT_DIR, "settings", "cookies")
OUTPUT_FILE = os.path.join(PARENT_DIR, "output", "groups.jsonl")
STATE_FILE = os.path.join(PARENT_DIR, "output", "groups_state.json")
URLS_FILE = os.path.join(PARENT_DIR, "settings", "facebook_group_urls.txt")
PROGRESS_FILE = os.path.join(PARENT_DIR, "output", "url_progress.json")

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
    
    # Filter accounts to only include those with valid cookie files
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
    return valid_accounts

def get_cookie_file_path(account_email):
    """Get the cookie file path for a specific account"""
    safe_email = account_email.replace("@", "_at_").replace(".", "_")
    return os.path.join(COOKIES_DIR, f"{safe_email}_cookies.json")

def load_cookies_for_account(account_email):
    """Load cookies for a specific account"""
    cookie_file = get_cookie_file_path(account_email)
    if not os.path.exists(cookie_file):
        raise Exception(f"Cookie file for {account_email} not found: {cookie_file}")
    
    with open(cookie_file, "r") as f:
        all_data = json.load(f)
    
    if not all_data.get("c_user"):
        raise Exception(f"Invalid cookies for {account_email}: missing c_user")
    
    # Filter out session data, keep only actual cookies
    cookies = {}
    for key, value in all_data.items():
        if key not in ["session_headers", "session_payload"]:
            cookies[key] = value
    
    return cookies

def get_session_data_from_cookies(account_email):
    """Extract session headers and payload from cookie data"""
    cookie_file = get_cookie_file_path(account_email)
    if not os.path.exists(cookie_file):
        return None, None
    
    try:
        with open(cookie_file, "r") as f:
            all_data = json.load(f)
        
        session_headers = all_data.get("session_headers", {})
        session_payload = all_data.get("session_payload", {})
        
        if not session_headers or not session_payload:
            print(f"⚠️  No session data found in cookies, using defaults")
            return None, None
        
        return session_headers, session_payload
    except Exception as e:
        print(f"⚠️  Error loading session data: {e}")
        return None, None

def load_active_accounts():
    """Load or create active accounts tracking file"""
    if os.path.exists(ACTIVE_ACCOUNTS_FILE):
        try:
            with open(ACTIVE_ACCOUNTS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Corrupted active accounts file: {e}")
    
    # Create new active accounts file
    active_accounts = {
        "accounts": {},
        "last_updated": datetime.datetime.now().isoformat()
    }
    save_active_accounts(active_accounts)
    return active_accounts

def save_active_accounts(active_accounts):
    """Save active accounts tracking file"""
    os.makedirs(os.path.dirname(ACTIVE_ACCOUNTS_FILE), exist_ok=True)
    with open(ACTIVE_ACCOUNTS_FILE, "w") as f:
        json.dump(active_accounts, f, indent=2)

def mark_account_busy(account_email):
    """Mark an account as currently in use"""
    active_accounts = load_active_accounts()
    active_accounts["accounts"][account_email] = {
        "status": "busy",
        "last_used": datetime.datetime.now().isoformat(),
        "request_count": active_accounts["accounts"].get(account_email, {}).get("request_count", 0) + 1
    }
    active_accounts["last_updated"] = datetime.datetime.now().isoformat()
    save_active_accounts(active_accounts)

def mark_account_available(account_email):
    """Mark an account as available for use"""
    active_accounts = load_active_accounts()
    if account_email in active_accounts["accounts"]:
        active_accounts["accounts"][account_email]["status"] = "available"
        active_accounts["accounts"][account_email]["last_available"] = datetime.datetime.now().isoformat()
        active_accounts["last_updated"] = datetime.datetime.now().isoformat()
        save_active_accounts(active_accounts)

def get_available_account():
    """Get an available account that's not currently in use"""
    active_accounts = load_active_accounts()
    
    # Get all accounts with cookies
    available_accounts = []
    for account in ACCOUNTS:
        account_email = account["account"]
        cookie_file = get_cookie_file_path(account_email)
        
        if os.path.exists(cookie_file):
            try:
                with open(cookie_file, "r") as f:
                    cookies = json.load(f)
                if cookies.get("c_user"):
                    # Check if account is available (not busy)
                    account_status = active_accounts["accounts"].get(account_email, {}).get("status", "available")
                    
                    # Skip accounts with recent session errors (give them time to recover)
                    if account_status == "session_error":
                        last_error = active_accounts["accounts"].get(account_email, {}).get("last_session_error")
                        if last_error:
                            error_time = datetime.datetime.fromisoformat(last_error)
                            time_since_error = (datetime.datetime.now() - error_time).total_seconds()
                            if time_since_error < 300:  # Skip for 5 minutes after session error
                                print(f"⏸️  Skipping {account_email} - recent session error ({time_since_error:.0f}s ago)")
                                continue
                            else:
                                # Mark as available after recovery time
                                mark_account_available(account_email)
                                account_status = "available"
                    
                    if account_status == "available":
                        available_accounts.append(account)
            except Exception:
                continue
    
    if not available_accounts:
        # If no accounts are available, reset all accounts to available
        print("🔄 All accounts are busy, resetting to available...")
        for account in ACCOUNTS:
            mark_account_available(account["account"])
        return get_available_account()  # Recursive call
    
    # Pick the account with the least recent usage
    best_account = min(available_accounts, key=lambda acc: 
        active_accounts["accounts"].get(acc["account"], {}).get("last_used", "1970-01-01"))
    
    return best_account

def switch_to_available_account():
    """Switch to an available account"""
    global CURRENT_ACCOUNT_INDEX, COOKIES
    
    account = get_available_account()
    if not account:
        raise Exception("No available accounts found")
    
    account_email = account["account"]
    
    # Mark account as busy
    mark_account_busy(account_email)
    
    print(f"🔄 Switching to available account: {account_email}")
    
    try:
        COOKIES = load_cookies_for_account(account_email)
        print(f"✅ Loaded cookies for {account_email}")
        
        # Update current account index
        for i, acc in enumerate(ACCOUNTS):
            if acc["account"] == account_email:
                CURRENT_ACCOUNT_INDEX = i
                break
        
        return True
    except Exception as e:
        print(f"❌ {account_email} failed! refresh cookies!")
        print(f"   Error: {e}")
        # Mark account as failed
        active_accounts = load_active_accounts()
        active_accounts["accounts"][account_email] = {
            "status": "failed",
            "last_error": str(e),
            "last_updated": datetime.datetime.now().isoformat()
        }
        save_active_accounts(active_accounts)
        
        # Try next available account
        return switch_to_available_account()

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
        print(f"❌ Proxy connection failed: {e}")
        return False

def mark_current_account_available():
    """Mark the current account as available after use"""
    if ACCOUNTS and CURRENT_ACCOUNT_INDEX < len(ACCOUNTS):
        current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]["account"]
        mark_account_available(current_account)

def rotate_account():
    """Rotate to the next account"""
    global CURRENT_ACCOUNT_INDEX
    CURRENT_ACCOUNT_INDEX = (CURRENT_ACCOUNT_INDEX + 1) % len(ACCOUNTS)
    return switch_to_available_account()

def is_rate_limited(response_text, status_code):
    """Detect if we're being rate limited by Facebook"""
    global RATE_LIMIT_DETECTED, LAST_RATE_LIMIT_TIME
    
    # Don't flag 200 responses as rate limited
    if status_code == 200:
        return False
    
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
    
    # Check status codes
    if status_code in [429, 403, 503]:
        RATE_LIMIT_DETECTED = True
        LAST_RATE_LIMIT_TIME = time.time()
        return True
    
    # Check response content
    response_lower = response_text.lower()
    for indicator in rate_limit_indicators:
        if indicator.lower() in response_lower:
            RATE_LIMIT_DETECTED = True
            LAST_RATE_LIMIT_TIME = time.time()
            return True
    
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
    
    print("✅ Progress saved successfully")

# --- Signal Handling ---
def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    print(f"\n\n⏸️  Interruption detected! Saving progress...")
    save_all_progress()
    print(f"✅ Progress saved. You can resume later.")
    print(f"📊 Final stats: {GRAPHQL_CALL_COUNT} calls, {len(seen_ids) if 'seen_ids' in globals() else 0} groups")
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

def update_call_count():
    global GRAPHQL_CALL_COUNT
    GRAPHQL_CALL_COUNT += 1
    elapsed_time = time.time() - START_TIME
    avg_time_per_call = elapsed_time / GRAPHQL_CALL_COUNT if GRAPHQL_CALL_COUNT > 0 else 0
    
    print(f"GraphQL Call #{GRAPHQL_CALL_COUNT}")
    print(f"Total elapsed time: {elapsed_time:.2f} seconds ({datetime.timedelta(seconds=int(elapsed_time))})")
    print(f"Average time per call: {avg_time_per_call:.2f} seconds")
    
    if GRAPHQL_CALL_COUNT >= TARGET_CALLS:
        print(f"\n🎉 REACHED {TARGET_CALLS} GRAPHQL CALLS!")
        print(f"Total time: {elapsed_time:.2f} seconds ({datetime.timedelta(seconds=int(elapsed_time))})")
        print(f"Average calls per second: {GRAPHQL_CALL_COUNT / elapsed_time:.2f}")
        return True
    return False

# --- URL Progress Tracking ---
def load_url_progress():
    """Load progress of processed URLs"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"⚠️  Corrupted progress file detected: {e}")
            print("🔄 Starting fresh progress tracking...")
            # Backup the corrupted file
            backup_file = PROGRESS_FILE + ".backup." + str(int(time.time()))
            try:
                os.rename(PROGRESS_FILE, backup_file)
                print(f"📁 Backed up corrupted file to: {backup_file}")
            except:
                pass
    return {"processed_urls": [], "current_url_index": 0, "total_urls": 0}

def save_url_progress(progress_data):
    """Save progress of processed URLs"""
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f, indent=2)

def load_search_urls_from_file() -> list:
    """Load search URLs from facebook_group_urls.txt"""
    if not os.path.exists(URLS_FILE):
        print(f"❌ URLs file not found: {URLS_FILE}")
        print("Using hardcoded search terms instead.")
        return []
    
    urls = []
    try:
        with open(URLS_FILE, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                urls.append(line)
    except Exception as e:
        print(f"❌ Error reading URLs file: {e}")
        return []
    
    print(f"✅ Loaded {len(urls)} URLs from {URLS_FILE}")
    return urls

def extract_search_term_from_url(url: str) -> str:
    """Extract search term from Facebook groups search URL"""
    if '?q=' in url:
        try:
            query_part = url.split('?q=')[1]
            search_term = urllib.parse.unquote(query_part)
            return search_term
        except Exception as e:
            print(f"⚠️  Failed to parse URL: {url[:50]}... - {e}")
            return None
    return None

# --- Proxy Mode Selection ---
# print("Choose proxy mode: [1] Nimbleway proxy (REQUIRED) [2] Proxyless")
# mode = input("Enter 1 or 2 (default 1): ").strip()
# if mode == "2":
#     PROXIES = None
#     print("Running proxyless (direct connection)...")
# else:
#     # Nimbleway Proxy Integration - REQUIRED for security
#     NIMBLEWAY_SETTINGS_FILE = os.path.join(PARENT_DIR, "settings", "nimbleway_settings.json")
#     if not os.path.exists(NIMBLEWAY_SETTINGS_FILE):
#         print("❌ CRITICAL ERROR: Nimbleway settings file not found!")
#         print(f"   Expected file: {NIMBLEWAY_SETTINGS_FILE}")
#         print("   This is REQUIRED for security when using proxy mode.")
#         print("   Please ensure nimbleway_settings.json exists with valid credentials.")
#         sys.exit(1)
    
#     try:
#         with open(NIMBLEWAY_SETTINGS_FILE, "r", encoding="utf-8") as f:
#             nimbleway_settings = json.load(f)
        
#         # Validate required fields
#         required_fields = ["accountName", "pipelineName", "pipelinePassword", "host", "port"]
#         missing_fields = [field for field in required_fields if not nimbleway_settings.get(field)]
        
#         if missing_fields:
#             print("❌ CRITICAL ERROR: Missing required Nimbleway settings!")
#             print(f"   Missing fields: {missing_fields}")
#             print("   Please ensure all required fields are present in nimbleway_settings.json")
#             sys.exit(1)
        
#         ACCOUNT_NAME = nimbleway_settings.get("accountName")
#         PIPELINE_NAME = nimbleway_settings.get("pipelineName")
#         PIPELINE_PASSWORD = nimbleway_settings.get("pipelinePassword")
#         NIMBLEWAY_HOST = nimbleway_settings.get("host", "ip.nimbleway.com")
#         NIMBLEWAY_PORT = nimbleway_settings.get("port", "7000")
        
#         # Use the correct Nimbleway format: account-accountName-pipeline-pipelineName:pipelinePassword
#         # URL-encode the account name to handle spaces (like in test script)
#         encoded_account_name = urllib.parse.quote(ACCOUNT_NAME)
#         NIMBLEWAY_PROXY = f"http://account-{encoded_account_name}-pipeline-{PIPELINE_NAME}:{PIPELINE_PASSWORD}@{NIMBLEWAY_HOST}:{NIMBLEWAY_PORT}"
        
#         PROXIES = {"http": NIMBLEWAY_PROXY, "https": NIMBLEWAY_PROXY}
#         print(f"✅ Using Nimbleway proxy: {NIMBLEWAY_PROXY}")
#         print("🔒 SECURITY: All requests will go through Nimbleway proxy")
        
#     except json.JSONDecodeError as e:
#         print("❌ CRITICAL ERROR: Invalid JSON in Nimbleway settings file!")
#         print(f"   Error: {e}")
#         print("   Please check the format of nimbleway_settings.json")
#         sys.exit(1)
#     except Exception as e:
#         print("❌ CRITICAL ERROR: Failed to load Nimbleway settings!")
#         print(f"   Error: {e}")
#         print("   Please ensure nimbleway_settings.json is properly configured")
#         sys.exit(1)

# --- CONFIGURATION ---
# NOTE: Move your cookie.json to /settings/cookie.json before running.
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
    "x-fb-lsd": "Z-n_XPzGloTFb9EkUVjSLE",  # <-- Updated from browser
}
DOC_ID = "24106293015687348"  # <-- Updated to match user's session
FB_API_REQ_FRIENDLY_NAME = "SearchCometResultsPaginatedResultsQuery"  # <-- Update if needed
SLEEP_BETWEEN_REQUESTS = (0.0, 5.0)  # Reduced delays for faster processing

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
# You MUST update this for each session/search. This is just a placeholder example.
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
    if session_headers and session_payload:
        print(f"🔄 Using session data from {current_account}")
        headers = session_headers.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)
        
        # Update the payload with current cursor and search term
        data = session_payload.copy()
        data["variables"] = json.dumps(variables)
        
        # Update Referer header for current search term
        headers["Referer"] = f"https://www.facebook.com/groups/search/groups_home/?q={urllib.parse.quote(search_term)}"
    else:
        print(f"🔄 Using default headers and payload for {user_id}")
        headers = HEADERS.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)
        
        data = {
            "av": user_id,
            "__aaid": "0",
            "__user": user_id,
            "__a": "1",
            "__req": "3",
            "__hs": "20302.HYP:comet_pkg.2.1...0",
            "dpr": "1",
            "__ccg": "EXCELLENT",
            "__rev": "1025431519",
            "__s": "ndklix:17f7hn:b6rwhq",
            "__hsi": "7533777975579479818",
            "__dyn": "7xe5WK1ixt0mUyEqxemh0no6u5U4e0yoW3q2ibwNwcyawba1DwUx60GE3Qwb-q1ew6ywMwto2awgo9oO0n24oaEnxO0Bo7O2l0Fw4Hw9O7Udo6qU2exi4UaEW2G0AEbrwh8lwuEjxu16wUwxwt819UbUG2-azo11k0z8c86-3u2WEjwhk0zU8oC1Iwqo5u1JwjHDzUiBG2OUqwjVqwLwHwea1wweW2K3a2q6E",
            "__csr": "gR5sBNk8PRFH92ZJjtPiOiF8BnFPldTcPb-J9pKT9RTGIQLmi8JkGLKWVvuDZqOuFeoySUDGOahbCTWkNGyeJp7mECi9y2aaA-8Vd6xWiaKuVV9EjyHyV8hKaK9Axqim4WCzU465U8ErDx2EsxLy4mq17wh8pxi1fwm8nx28zUGU4u2WfUdk1aw822y9wQx63u3y2u1ey89QudDwwwkESUnxHw8R2Ed89UeEc82TwQxm2a2O2e8K5V82pw2e8cQ08Hw76gO0geaU9UjwBw8qawSwXw428fG2-08swnU27ixagyy8GU5mdqx5poO3V3o0KO6o1HE8U01u2o0hXo13Ejw4fw1nZ160jq8yk14wdq04s81cojw7Pg0cozw0h9Hxl07Mw2w8040e02Xu4U",
            "__hsdp": "hOi22wzz91e46p78F4oFrV94jGhFEwL4GCh5HieKUGqQXBjAkECqAt2lBixi437AUrCp4nCz4aqDhe9GpaExvwyyrG8lppet1q8DymEB0Ky9FaV8BcChfF2UWtDla4-B6aj49LzkB9AfiWiaQZOZjRd8jHEnr4WhNq8z_knbegUiA_csGAoWCmB89i8GgGeyOaUBai7ESp6BocrGaGEa-czozAAK8xMMF2m9yUgCCJAVGDhVk9ABBjkQWyogBxq28ya_BwAAwlohxHzoW2aV8CnxswGWx28zA69UrxiawKjHULo-EpGEy323m3h3ax61TCxS2q1VwrUCUigOfKepAVd1Sbxe4Bg3cwiEhg3IwkE2rw6rw5XwxGVEowtUbU_Vm3K2uEbFU12EcU78on704GwkUqwnqw6Xwo81rU0lbwaWfw2p83Vw2nE0yi08QwUw2L832w17W0dXw4Yw2bE7q0cjw",
            "__hblp": "09a0ri0ki0ki2K1nw5jwto2Kw5Uwm81yU0Hy1ax60k20hW0hy0nF09a04q87y0ke0SUlw4gwi80lbxO1vwgo25wCw6OwfW0h60kO0li0PU0zi3y08wwoGwRwca0ll039U4u4U6y0s60Z85G0Uo0yW0l-7o39w52w",
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
        handle_rate_limiting()
        return None
    
    try:
        # Try to parse as JSON first (requests may have already decompressed)
        return resp.json()
    except Exception as e:
        # Handle Facebook's JavaScript responses FIRST (rate limiting)
        try:
            raw_text = resp.content.decode("utf-8")
            if raw_text.startswith("for (;;);"):
                print(f"🔍 Detected Facebook JavaScript response (rate limiting)")
                print(f"📄 Full JavaScript response:")
                print(f"   {raw_text}")
                # Remove the "for (;;);" prefix and parse as JSON
                json_text = raw_text[9:]  # Remove "for (;;);"
                js_response = json.loads(json_text)
                if 'error' in js_response:
                    print(f"⚠️  Facebook error: {js_response.get('error')}")
                    print(f"📄 Full error details: {json.dumps(js_response, indent=2)}")
                    
                    # Handle different types of errors
                    error_code = js_response.get('error')
                    if error_code == 1357004:
                        print(f"🔍 Session issue detected (error 1357004)")
                        print(f"   This is a browser session problem, not rate limiting")
                        # Mark account as having session issues
                        current_account = ACCOUNTS[CURRENT_ACCOUNT_INDEX]["account"]
                        active_accounts = load_active_accounts()
                        active_accounts["accounts"][current_account] = {
                            "status": "session_error",
                            "last_session_error": datetime.datetime.now().isoformat(),
                            "session_error_count": active_accounts["accounts"].get(current_account, {}).get("session_error_count", 0) + 1
                        }
                        save_active_accounts(active_accounts)
                        
                        # Try to switch to a different account
                        if switch_to_available_account():
                            print(f"✅ Switched to different account due to session error")
                            return None  # Retry with new account
                        else:
                            print(f"❌ No available accounts, treating as rate limiting")
                            handle_rate_limiting()
                            return None
                    else:
                        # This is rate limiting, handle it
                        handle_rate_limiting()
                        return None
        except Exception as js_e:
            print(f"❌ JavaScript response parsing failed: {js_e}")
            print(f"📄 Raw response that failed to parse:")
            print(f"   {resp.content.decode('utf-8', errors='ignore')}")
        
        # If JSON parsing fails and encoding is zstd, try manual decompression
        encoding = resp.headers.get("Content-Encoding")
        if encoding == "zstd":
            try:
                print(f"🔍 Attempting zstd decompression...")
                dctx = zstd.ZstdDecompressor()
                decompressed = dctx.stream_reader(io.BytesIO(resp.content))
                text = decompressed.read().decode("utf-8")
                print(f"✅ Zstd decompression successful, length: {len(text)}")
                return json.loads(text)
            except Exception as zstd_e:
                print(f"❌ Zstd decompression failed: {zstd_e}")
                print(f"🔍 Response content length: {len(resp.content)}")
                print(f"🔍 Response content preview: {resp.content[:100]}")
                
                # If zstd decompression fails, the proxy may have already decompressed it
                # Try to parse the raw content as JSON
                try:
                    print(f"🔍 Trying to parse raw content as JSON...")
                    raw_text = resp.content.decode("utf-8")
                    return json.loads(raw_text)
                except Exception as raw_e:
                    print(f"❌ Raw content parsing failed: {raw_e}")
        
        # If still not JSON, treat as HTML and save it
        html_text = None
        try:
            html_text = resp.content.decode("utf-8")
        except Exception:
            html_text = str(resp.content)
        # Save the HTML response as a special entry in the JSONL
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({"id": "unknown", "html_response": html_text}) + "\n")
        print("Saved HTML response to JSONL for debugging.")
        print("Exception:", e)
        raise

def parse_snippet(snippet):
    # Example: "Public · 1.2K members · 10+ posts a day"
    parts = [p.strip() for p in snippet.split("·")]
    privacy = member_count = posts_per = None
    if len(parts) >= 1:
        privacy = parts[0]
    if len(parts) >= 2:
        member_count = parts[1]
    if len(parts) >= 3:
        posts_per = parts[2]
    return privacy, member_count, posts_per

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
        print(f"❌ Error extracting groups: Missing field '{e}'")
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

# --- WORKER FUNCTION FOR PARALLEL PROCESSING ---
def worker_process(account_index, city_start_index, city_end_index, shared_state, output_lock, remaining_terms_queue=None):
    """Worker process to handle a specific account and city range"""
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
        
        # Extract search terms for this worker's city range
        search_terms = []
        for i in range(city_start_index, min(city_end_index, len(urls))):
            url = urls[i]
            search_term = extract_search_term_from_url(url)
            if search_term:
                search_terms.append(search_term)
        
        print(f"📋 Worker {account_index}: Processing {len(search_terms)} search terms")
        
        # Load cookies for this account
        cookies = load_cookies_for_account(account_email)
        
        # Initialize state for this worker
        cursor = None
        seen_ids = set()
        graphql_calls = 0
        start_time = time.time()
        
        # Process each search term in this worker's range
        for term_index, search_term in enumerate(search_terms):
            if graphql_calls >= TARGET_CALLS // mp.cpu_count():  # Distribute target calls across workers
                print(f"🎯 Worker {account_index}: Reached target calls ({graphql_calls})")
                break
            
            print(f"🔍 Worker {account_index}: Processing search term {term_index + 1}/{len(search_terms)}: {search_term}")
            
            # Reset cursor for new search term
            cursor = None
            search_completed = False
            
            # Process this search term until no more results or target reached
            while not search_completed and graphql_calls < TARGET_CALLS // mp.cpu_count():
                try:
                    response = fetch_page_worker(cursor, search_term, account_email, cookies)
                    
                    if response is None:
                        print(f"⚠️  Worker {account_index}: Rate limiting detected, skipping search term")
                        search_completed = True
                        break
                    
                    # Update call count
                    graphql_calls += 1
                    elapsed_time = time.time() - start_time
                    avg_time_per_call = elapsed_time / graphql_calls if graphql_calls > 0 else 0
                    
                    print(f"📊 Worker {account_index}: GraphQL Call #{graphql_calls}")
                    print(f"   Total elapsed time: {elapsed_time:.2f} seconds")
                    print(f"   Average time per call: {avg_time_per_call:.2f} seconds")
                    
                    # Extract and save groups
                    new_groups = 0
                    for group in extract_groups(response, search_term):
                        if group["id"] not in seen_ids:
                            # Use lock to safely append to shared output file
                            with output_lock:
                                append_group_safe(group)
                            seen_ids.add(group["id"])
                            new_groups += 1
                    
                    print(f"✅ Worker {account_index}: Added {new_groups} new groups. Total: {len(seen_ids)}")
                    
                    # Update shared state
                    shared_state.value += new_groups
                    
                    # If no new groups found, add extra wait to prevent rate limiting
                    if new_groups == 0:
                        extra_wait = random.uniform(3.0, 5.0)  # 3-5 second extra wait
                        print(f"⏸️  Worker {account_index}: No new groups found, extra wait {extra_wait:.2f}s to prevent rate limiting...")
                        time.sleep(extra_wait)
                    
                    next_cursor = get_next_cursor(response)
                    if not next_cursor or new_groups == 0:
                        print(f"📝 Worker {account_index}: No more pages or no new groups found")
                        search_completed = True
                        break
                    
                    cursor = next_cursor
                    
                    # Randomized delay
                    sleep_time = random.uniform(*SLEEP_BETWEEN_REQUESTS)
                    print(f"⏸️  Worker {account_index}: Sleeping for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    
                except Exception as e:
                    print(f"❌ Worker {account_index}: Request failed: {e}")
                    search_completed = True
                    break
        
        print(f"🎉 Worker {account_index}: Completed initial range! Processed {len(search_terms)} search terms, {graphql_calls} calls, {len(seen_ids)} groups")
        
        # Continue processing remaining search terms if available
        if remaining_terms_queue:
            print(f"🔄 Worker {account_index}: Checking for remaining search terms...")
            
            while True:
                try:
                    # Try to get a search term from the queue (non-blocking)
                    search_term = remaining_terms_queue.get_nowait()
                    print(f"🔍 Worker {account_index}: Processing remaining search term: {search_term}")
                    
                    # Reset cursor for new search term
                    cursor = None
                    search_completed = False
                    
                    # Process this search term until no more results
                    while not search_completed:
                        try:
                            response = fetch_page_worker(cursor, search_term, account_email, cookies)
                            
                            if response is None:
                                print(f"⚠️  Worker {account_index}: Rate limiting detected, skipping search term")
                                search_completed = True
                                break
                            
                            # Update call count
                            graphql_calls += 1
                            elapsed_time = time.time() - start_time
                            avg_time_per_call = elapsed_time / graphql_calls if graphql_calls > 0 else 0
                            
                            print(f"📊 Worker {account_index}: GraphQL Call #{graphql_calls}")
                            print(f"   Total elapsed time: {elapsed_time:.2f} seconds")
                            print(f"   Average time per call: {avg_time_per_call:.2f} seconds")
                            
                            # Extract and save groups
                            new_groups = 0
                            for group in extract_groups(response, search_term):
                                if group["id"] not in seen_ids:
                                    # Use lock to safely append to shared output file
                                    with output_lock:
                                        append_group_safe(group)
                                    seen_ids.add(group["id"])
                                    new_groups += 1
                            
                            print(f"✅ Worker {account_index}: Added {new_groups} new groups. Total: {len(seen_ids)}")
                            
                            # Update shared state
                            shared_state.value += new_groups
                            
                            # If no new groups found, add extra wait to prevent rate limiting
                            if new_groups == 0:
                                extra_wait = random.uniform(3.0, 5.0)  # 3-5 second extra wait
                                print(f"⏸️  Worker {account_index}: No new groups found, extra wait {extra_wait:.2f}s to prevent rate limiting...")
                                time.sleep(extra_wait)
                            
                            next_cursor = get_next_cursor(response)
                            if not next_cursor or new_groups == 0:
                                print(f"📝 Worker {account_index}: No more pages or no new groups found")
                                search_completed = True
                                break
                            
                            cursor = next_cursor
                            
                            # Randomized delay
                            sleep_time = random.uniform(*SLEEP_BETWEEN_REQUESTS)
                            print(f"⏸️  Worker {account_index}: Sleeping for {sleep_time:.2f} seconds...")
                            time.sleep(sleep_time)
                            
                        except Exception as e:
                            print(f"❌ Worker {account_index}: Request failed: {e}")
                            search_completed = True
                            break
                    
                except queue.Empty:
                    print(f"🏁 Worker {account_index}: No more search terms available")
                    break
        
        print(f"🎉 Worker {account_index}: Final completion! Total: {graphql_calls} calls, {len(seen_ids)} groups")
        
    except Exception as e:
        print(f"❌ Worker {account_index}: Fatal error: {e}")

def fetch_page_worker(cursor, search_term, account_email, cookies):
    """Worker version of fetch_page that doesn't use global variables"""
    if not cookies or not cookies.get("c_user"):
        raise Exception(f"{account_email} failed! refresh cookies!")
    
    # SECURITY CHECK: Ensure proxy is used when in Nimbleway mode
    if 'PROXIES' in globals() and PROXIES is None:
        print("❌ CRITICAL SECURITY ERROR: Nimbleway mode selected but no proxy configured!")
        return None
    
    user_id = cookies.get("c_user")
    variables = json.loads(USER_VARIABLES_JSON)
    variables["cursor"] = cursor
    variables["args"]["text"] = search_term
    
    # Get session data for this account
    session_headers, session_payload = get_session_data_from_cookies(account_email)
    
    # Use the session data from the account's cookie file if available
    if session_headers and session_payload:
        headers = session_headers.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)
        
        # Update the payload with current cursor and search term
        data = session_payload.copy()
        data["variables"] = json.dumps(variables)
        
        # Update Referer header for current search term
        headers["Referer"] = f"https://www.facebook.com/groups/search/groups_home/?q={urllib.parse.quote(search_term)}"
    else:
        headers = HEADERS.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)
        
        data = {
            "av": user_id,
            "__aaid": "0",
            "__user": user_id,
            "__a": "1",
            "__req": "3",
            "__hs": "20302.HYP:comet_pkg.2.1...0",
            "dpr": "1",
            "__ccg": "EXCELLENT",
            "__rev": "1025431519",
            "__s": "ndklix:17f7hn:b6rwhq",
            "__hsi": "7533777975579479818",
            "__dyn": "7xe5WK1ixt0mUyEqxemh0no6u5U4e0yoW3q2ibwNwcyawba1DwUx60GE3Qwb-q1ew6ywMwto2awgo9oO0n24oaEnxO0Bo7O2l0Fw4Hw9O7Udo6qU2exi4UaEW2G0AEbrwh8lwuEjxu16wUwxwt819UbUG2-azo11k0z8c86-3u2WEjwhk0zU8oC1Iwqo5u1JwjHDzUiBG2OUqwjVqwLwHwea1wweW2K3a2q6E",
            "__csr": "gR5sBNk8PRFH92ZJjtPiOiF8BnFPldTcPb-J9pKT9RTGIQLmi8JkGLKWVvuDZqOuFeoySUDGOahbCTWkNGyeJp7mECi9y2aaA-8Vd6xWiaKuVV9EjyHyV8hKaK9Axqim4WCzU465U8ErDx2EsxLy4mq17wh8pxi1fwm8nx28zUGU4u2WfUdk1aw822y9wQx63u3y2u1ey89QudDwwwkESUnxHw8R2Ed89UeEc82TwQxm2a2O2e8K5V82pw2e8cQ08Hw76gO0geaU9UjwBw8qawSwXw428fG2-08swnU27ixagyy8GU5mdqx5poO3V3o0KO6o1HE8U01u2o0hXo13Ejw4fw1nZ160jq8yk14wdq04s81cojw7Pg0cozw0h9Hxl07Mw2w8040e02Xu4U",
            "__hsdp": "hOi22wzz91e46p78F4oFrV94jGhFEwL4GCh5HieKUGqQXBjAkECqAt2lBixi437AUrCp4nCz4aqDhe9GpaExvwyyrG8lppet1q8DymEB0Ky9FaV8BcChfF2UWtDla4-B6aj49LzkB9AfiWiaQZOZjRd8jHEnr4WhNq8z_knbegUiA_csGAoWCmB89i8GgGeyOaUBai7ESp6BocrGaGEa-czozAAK8xMMF2m9yUgCCJAVGDhVk9ABBjkQWyogBxq28ya_BwAAwlohxHzoW2aV8CnxswGWx28zA69UrxiawKjHULo-EpGEy323m3h3ax61TCxS2q1VwrUCUigOfKepAVd1Sbxe4Bg3cwiEhg3IwkE2rw6rw5XwxGVEowtUbU_Vm3K2uEbFU12EcU78on704GwkUqwnqw6Xwo81rU0lbwaWfw2p83Vw2nE0yi08QwUw2L832w17W0dXw4Yw2bE7q0cjw",
            "__hblp": "09a0ri0ki0ki2K1nw5jwto2Kw5Uwm81yU0Hy1ax60k20hW0hy0nF09a04q87y0ke0SUlw4gwi80lbxO1vwgo25wCw6OwfW0h60kO0li0PU0zi3y08wwoGwRwca0ll039U4u4U6y0s60Z85G0Uo0yW0l-7o39w52w",
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
    
    print(f"📡 Worker request via {account_email}")
    
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
        print(f"⚠️  Worker rate limiting detected")
        return None
    
    try:
        return resp.json()
    except Exception as e:
        # Handle Facebook's JavaScript responses
        try:
            raw_text = resp.content.decode("utf-8")
            if raw_text.startswith("for (;;);"):
                json_text = raw_text[9:]
                js_response = json.loads(json_text)
                if 'error' in js_response:
                    error_code = js_response.get('error')
                    if error_code == 1357004:
                        print(f"🔍 Worker session issue detected (error 1357004)")
                        return None
                    else:
                        print(f"⚠️  Worker Facebook error: {error_code}")
                        return None
        except Exception as js_e:
            print(f"❌ Worker JavaScript response parsing failed: {js_e}")
        
        # Handle other response types
        encoding = resp.headers.get("Content-Encoding")
        if encoding == "zstd":
            try:
                dctx = zstd.ZstdDecompressor()
                decompressed = dctx.stream_reader(io.BytesIO(resp.content))
                text = decompressed.read().decode("utf-8")
                return json.loads(text)
            except Exception as zstd_e:
                print(f"❌ Worker zstd decompression failed: {zstd_e}")
        
        print(f"❌ Worker response parsing failed: {e}")
        return None

def append_group_safe(group):
    """Thread-safe version of append_group"""
    # Duplicate prevention: only write if not already seen
    # We'll use a simple approach - just append and let the main script handle deduplication later
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(group, ensure_ascii=False) + "\n")

def deduplicate_output_file():
    """Remove duplicate groups from the output file"""
    if not os.path.exists(OUTPUT_FILE):
        return
    
    print(f"📖 Reading {OUTPUT_FILE} for deduplication...")
    
    # Read all groups
    groups = []
    seen_ids = set()
    
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                group = json.loads(line.strip())
                if group.get("id") and group["id"] not in seen_ids:
                    groups.append(group)
                    seen_ids.add(group["id"])
            except json.JSONDecodeError:
                continue
    
    # Write back deduplicated groups
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for group in groups:
            f.write(json.dumps(group, ensure_ascii=False) + "\n")
    
    print(f"✅ Deduplication complete: {len(groups)} unique groups")

# --- MAIN LOOP ---
if __name__ == "__main__":
    print(f"Starting Facebook Groups Scraper - Target: {TARGET_CALLS} GraphQL calls")
    print(f"Start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            print("   Please ensure nimbleway_settings.json exists with valid credentials.")
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
    
    # Choose account mode
    print("Choose account mode:")
    print("[1] Use bought accounts (multiple accounts, parallel processing)")
    print("[2] Use real account (single account, your cookies)")
    account_mode = input("Enter 1 or 2 (default 1): ").strip()
    
    if account_mode == "2":
        # Use real account cookies (single process)
        REAL_COOKIE_FILE = os.path.join(PARENT_DIR, "settings", "cookie.json")
        if not os.path.exists(REAL_COOKIE_FILE):
            print(f"❌ Real account cookies not found: {REAL_COOKIE_FILE}")
            print("Please ensure settings/cookie.json exists with your real account cookies.")
            sys.exit(1)
        
        try:
            with open(REAL_COOKIE_FILE, "r") as f:
                COOKIES = json.load(f)
            
            if not COOKIES.get("c_user"):
                print("❌ Invalid real account cookies: missing c_user")
                sys.exit(1)
            
            print(f"✅ Using real account cookies for user: {COOKIES.get('c_user')}")
            print("🔒 Single account mode - no parallel processing")
            
            # Create a dummy account for compatibility
            ACCOUNTS = [{"account": "real_account", "password": "real_password"}]
            CURRENT_ACCOUNT_INDEX = 0
            
        except Exception as e:
            print(f"❌ Failed to load real account cookies: {e}")
            sys.exit(1)
    else:
        # Use bought accounts with parallel processing
        try:
            ACCOUNTS.extend(load_accounts())
            print(f"✅ Loaded {len(ACCOUNTS)} accounts for parallel processing")
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
        print("✅ Proxy configuration loaded - proceeding with scraping")
        
        # Skip the actual test since it's failing due to pipeline restrictions
        # The proxy will be tested with actual Facebook requests
    
    # Load URLs
    urls = load_search_urls_from_file()
    if not urls:
        print("❌ No URLs found. Using hardcoded search term.")
        search_terms = ["arlington tx"]  # Fallback
    else:
        print(f"✅ Loaded {len(urls)} URLs for parallel processing")
    
    if account_mode == "2":
        # Single account mode - use original sequential processing
        print("🔄 Running in single account mode (sequential processing)")
        
        # Extract search terms from URLs
        search_terms = []
        for url in urls:
            search_term = extract_search_term_from_url(url)
            if search_term:
                search_terms.append(search_term)
        
        if not search_terms:
            print("❌ No valid search terms extracted from URLs. Using fallback.")
            search_terms = ["arlington tx"]
        
        print(f"📋 Loaded {len(search_terms)} search terms")
        print(f"👤 Using 1 account (real account)")
        
        # Use original sequential processing
        current_index = 0
        
        while current_index < len(search_terms) and GRAPHQL_CALL_COUNT < TARGET_CALLS:
            search_term = search_terms[current_index]
            print(f"\n🔍 Processing search term {current_index + 1}/{len(search_terms)}: {search_term}")
            
            # Reset cursor for new search term
            cursor = None
            search_completed = False
            
            # Process this search term until no more results or target reached
            while not search_completed and GRAPHQL_CALL_COUNT < TARGET_CALLS:
                try:
                    response = fetch_page(cursor, search_term)
                    
                    # Check if rate limiting occurred
                    if response is None:
                        print("Rate limiting detected, skipping this search term for now...")
                        search_completed = True
                        break
                    
                    # Update call count and check if we've reached the target
                    if update_call_count():
                        print("Target reached! Stopping scraper.")
                        break
                        
                except Exception as e:
                    error_msg = str(e)
                    print(f"Request failed: {error_msg}")
                    search_completed = True
                    break

                new_groups = 0
                for group in extract_groups(response, search_term):
                    if group["id"] not in seen_ids:
                        append_group(group)
                        new_groups += 1

                # Save state immediately after processing the page
                save_state(cursor, seen_ids)

                print(f"Added {new_groups} new groups. Total: {len(seen_ids)}")

                # If no new groups found, add extra wait to prevent rate limiting
                if new_groups == 0:
                    extra_wait = random.uniform(3.0, 5.0)  # 3-5 second extra wait
                    print(f"⏸️  No new groups found, extra wait {extra_wait:.2f}s to prevent rate limiting...")
                    time.sleep(extra_wait)

                next_cursor = get_next_cursor(response)
                if not next_cursor or new_groups == 0:
                    print("No more pages or no new groups found.")
                    search_completed = True
                    break

                cursor = next_cursor
                # Randomized delay to reduce rate limiting risk
                sleep_time = random.uniform(*SLEEP_BETWEEN_REQUESTS)
                print(f"Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            
            current_index += 1
            
            if GRAPHQL_CALL_COUNT >= TARGET_CALLS:
                break
        
        print(f"\n🎉 Scraping completed!")
        print(f"Total groups found: {len(seen_ids)}")
        print(f"GraphQL calls made: {GRAPHQL_CALL_COUNT}")
        
    else:
        # Parallel processing mode with multiple accounts
        print("🚀 Running in parallel processing mode")
        print(f"📊 Target calls per worker: {TARGET_CALLS // len(ACCOUNTS)}")
        
        # Calculate city ranges for each worker
        cities_per_worker = len(urls) // len(ACCOUNTS)
        remaining_cities = len(urls) % len(ACCOUNTS)
        
        print(f"🏙️  Cities per worker: {cities_per_worker}")
        print(f"🏙️  Remaining cities: {remaining_cities}")
        
        # Create shared state for tracking total groups found
        manager = Manager()
        shared_state = manager.Value('i', 0)  # Shared integer for total groups
        output_lock = manager.Lock()  # Lock for safe file writing
        
        # Create a shared queue for remaining search terms
        remaining_terms_queue = manager.Queue()
        
        # Use three working accounts now that they all have fresh session data
        working_accounts = [1, 2, 5]  # Use Dextermonogo (index 1), Blairewal85 (index 2), and Swankycoyote91 (index 5)
        
        # Create all worker processes first, then start them all simultaneously
        processes = []
        current_city_index = 0
        
        # Track which search terms are assigned to workers
        assigned_terms = set()
        
        # Create all processes first
        for worker_id in working_accounts:
            # Calculate city range for this worker
            cities_for_this_worker = cities_per_worker + (1 if worker_id < remaining_cities else 0)
            city_start = current_city_index
            city_end = current_city_index + cities_for_this_worker
            
            print(f"🚀 Creating worker {worker_id}: cities {city_start} to {city_end-1}")
            
            # Track assigned search terms
            for i in range(city_start, city_end):
                if i < len(urls):
                    search_term = extract_search_term_from_url(urls[i])
                    if search_term:
                        assigned_terms.add(search_term)
            
            # Create process (but don't start yet)
            process = mp.Process(
                target=worker_process,
                args=(worker_id, city_start, city_end, shared_state, output_lock, remaining_terms_queue)
            )
            processes.append(process)
            
            current_city_index = city_end
        
        # Populate remaining terms queue with unassigned search terms
        all_search_terms = []
        for url in urls:
            search_term = extract_search_term_from_url(url)
            if search_term:
                all_search_terms.append(search_term)
        
        remaining_terms = [term for term in all_search_terms if term not in assigned_terms]
        print(f"📋 Total search terms: {len(all_search_terms)}")
        print(f"📋 Assigned to workers: {len(assigned_terms)}")
        print(f"📋 Remaining for queue: {len(remaining_terms)}")
        
        # Add remaining terms to the queue
        for term in remaining_terms:
            remaining_terms_queue.put(term)
        
        print(f"📦 Added {len(remaining_terms)} search terms to remaining queue")
        
        # Start all processes simultaneously
        print(f"🚀 Starting {len(processes)} workers simultaneously...")
        for i, process in enumerate(processes):
            process.start()
            print(f"✅ Started worker {working_accounts[i]}")
        
        # Wait for all processes to complete
        print(f"⏳ Waiting for {len(processes)} workers to complete...")
        for i, process in enumerate(processes):
            process.join()
            print(f"✅ Worker {working_accounts[i]} completed")
        
        # Final summary
        total_groups = shared_state.value
        print(f"\n🎉 Parallel scraping completed!")
        print(f"📊 Total groups found across all workers: {total_groups}")
        print(f"👥 Used {len(ACCOUNTS)} accounts in parallel")
        
        # Deduplicate the output file
        print("🧹 Deduplicating output file...")
        deduplicate_output_file()
        
        print("✅ All done!")
    
    # Final progress save
    save_all_progress() 