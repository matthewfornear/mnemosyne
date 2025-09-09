import json
import re
import requests
import urllib.parse
import shlex
import time
import random
from typing import Dict, List, Optional, Tuple
import os
import datetime
import glob
import sys
import logging
import multiprocessing as mp
from multiprocessing import Manager
from dataclasses import dataclass, asdict
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
MAX_CONCURRENT_SEARCHES_PER_WORKER = 4  # Number of search terms to process in parallel per worker

# File paths
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PARENT_DIR, "output", "curl")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "groups_output_curl.json")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "curl_scraper_progress.json")
URL_PROGRESS_FILE = os.path.join(OUTPUT_DIR, "url_progress_curl.json")
# New enhanced progress tracking files
URL_DETAILED_PROGRESS_FILE = os.path.join(OUTPUT_DIR, "url_detailed_progress.json")
CITY_PROGRESS_FILE = os.path.join(OUTPUT_DIR, "city_progress.json")
LOG_FILE = os.path.join(OUTPUT_DIR, "curl_scraper.log")
URLS_FILE = os.path.join(PARENT_DIR, "settings", "facebook_group_urls.txt")
CURL_DIR = os.path.join(PARENT_DIR, "settings", "curl")

# Global proxy configuration (will be set during initialization)
PROXIES = None

@dataclass
class URLProgress:
    """Track progress for each individual URL"""
    url: str
    search_term: str
    city: str
    completed_accounts: List[str]  # List of account names that completed this URL
    failed_accounts: List[str]     # List of account names that failed this URL
    last_cursor: Optional[str]     # Last pagination cursor
    total_groups_found: int        # Total groups found for this URL
    zero_result_count: int         # Consecutive zero-result pages
    last_updated: str              # ISO timestamp
    status: str                    # "pending", "in_progress", "completed", "failed"
    groups_found: List[str]        # List of group IDs found for this URL

@dataclass
class CityProgress:
    """Track progress for each unique city"""
    city: str
    urls_processed: List[str]     # List of URLs processed for this city
    total_groups_found: int        # Total groups found across all URLs for this city
    unique_groups: List[str]       # List of unique group IDs found for this city
    last_updated: str              # ISO timestamp
    status: str                    # "active", "completed"

@dataclass
class SearchProgress:
    """Track progress for each search term (legacy support)"""
    search_term: str
    url: str
    completed_accounts: List[str]  # List of account names that completed this search
    failed_accounts: List[str]     # List of account names that failed
    last_cursor: Optional[str]     # Last pagination cursor
    total_groups_found: int        # Total groups found for this search
    zero_result_count: int         # Consecutive zero-result pages
    last_updated: str              # ISO timestamp
    status: str                    # "pending", "in_progress", "completed", "failed"

def load_nimbleway_settings():
    """Load Nimbleway proxy settings"""
    global PROXIES
    
    print("🔒 Loading Nimbleway proxy settings...")
    
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

class CurlParser:
    """Parse cURL commands and extract components"""
    
    @staticmethod
    def parse_curl_command(curl_command: str) -> Dict:
        """Parse a cURL command and extract URL, headers, cookies, and data"""
        # Remove curl command and clean up
        curl_command = curl_command.strip()
        if curl_command.startswith('curl '):
            curl_command = curl_command[5:]
        
        # Split the command while preserving quoted strings
        try:
            parts = shlex.split(curl_command)
        except ValueError:
            # Fallback for malformed quotes
            parts = curl_command.split()
        
        url = None
        headers = {}
        cookies = {}
        data = None
        
        i = 0
        while i < len(parts):
            part = parts[i]
            
            if part.startswith('http'):
                url = part.strip("'\"")
            elif part == '-H' and i + 1 < len(parts):
                header_line = parts[i + 1]
                if ':' in header_line:
                    key, value = header_line.split(':', 1)
                    headers[key.strip()] = value.strip()
                i += 1
            elif part == '-b' and i + 1 < len(parts):
                cookie_line = parts[i + 1]
                cookies.update(CurlParser.parse_cookie_string(cookie_line))
                i += 1
            elif part == '--data-raw' and i + 1 < len(parts):
                data = parts[i + 1]
                i += 1
            
            i += 1
        
        return {
            'url': url,
            'headers': headers,
            'cookies': cookies,
            'data': data
        }
    
    @staticmethod
    def parse_cookie_string(cookie_string: str) -> Dict:
        """Parse cookie string into dictionary"""
        cookies = {}
        for cookie in cookie_string.split(';'):
            cookie = cookie.strip()
            if '=' in cookie:
                key, value = cookie.split('=', 1)
                cookies[key.strip()] = value.strip()
        return cookies

def load_curl_files() -> List[Dict]:
    """Load all cURL files from /settings/curl/ directory"""
    curl_files = []
    
    if not os.path.exists(CURL_DIR):
        print(f"❌ cURL directory not found: {CURL_DIR}")
        return curl_files
    
    # Find all JSON files in curl directory
    pattern = os.path.join(CURL_DIR, "*.json")
    files = glob.glob(pattern)
    
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                curl_data = json.load(f)
                
            if 'curl_command' in curl_data:
                account_name = curl_data.get('account_name', Path(file_path).stem)
                curl_files.append({
                    'account_name': account_name,
                    'curl_command': curl_data['curl_command'],
                    'file_path': file_path
                })
                print(f"✅ Loaded cURL for account: {account_name}")
            else:
                print(f"⚠️  Invalid cURL file format: {file_path}")
                
        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
    
    print(f"📊 Total cURL accounts loaded: {len(curl_files)}")
    return curl_files

def load_search_urls_from_file() -> List[str]:
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
        # Look for q= parameter in URL
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        
        if 'q' in params:
            search_term = params['q'][0]
            # URL decode the search term
            search_term = urllib.parse.unquote_plus(search_term)
            return search_term
        else:
            # Fallback: try to extract from URL path or use generic term
            print(f"⚠️  No search term found in URL: {url}")
            return "default"
            
    except Exception as e:
        print(f"⚠️  Error extracting search term from URL {url}: {e}")
        return "default"

def load_progress() -> Dict[str, SearchProgress]:
    """Load scraping progress from file"""
    if not os.path.exists(PROGRESS_FILE):
        return {}
    
    try:
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        progress = {}
        for key, item in data.items():
            progress[key] = SearchProgress(**item)
        
        print(f"📊 Loaded progress for {len(progress)} search terms")
        return progress
        
    except Exception as e:
        print(f"⚠️  Error loading progress: {e}")
        return {}

def save_progress(progress: Dict[str, SearchProgress], output_lock=None):
    """Save scraping progress to file"""
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        data = {key: asdict(prog) for key, prog in progress.items()}
        
        # Use lock if provided (for multiprocessing safety)
        if output_lock:
            with output_lock:
                with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
        else:
            with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"⚠️  Error saving progress: {e}")

def load_url_detailed_progress() -> Dict[str, URLProgress]:
    """Load detailed URL progress from file"""
    if not os.path.exists(URL_DETAILED_PROGRESS_FILE):
        return {}
    
    try:
        with open(URL_DETAILED_PROGRESS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        progress = {}
        for key, item in data.items():
            progress[key] = URLProgress(**item)
        
        print(f"📊 Loaded detailed URL progress for {len(progress)} URLs")
        return progress
        
    except Exception as e:
        print(f"⚠️  Error loading detailed URL progress: {e}")
        return {}

def save_url_detailed_progress(progress: Dict[str, URLProgress], output_lock=None):
    """Save detailed URL progress to file"""
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        data = {key: asdict(prog) for key, prog in progress.items()}
        
        # Use lock if provided (for multiprocessing safety)
        if output_lock:
            with output_lock:
                with open(URL_DETAILED_PROGRESS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
        else:
            with open(URL_DETAILED_PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"⚠️  Error saving detailed URL progress: {e}")

def load_city_progress() -> Dict[str, CityProgress]:
    """Load city progress from file"""
    if not os.path.exists(CITY_PROGRESS_FILE):
        return {}
    
    try:
        with open(CITY_PROGRESS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        progress = {}
        for key, item in data.items():
            progress[key] = CityProgress(**item)
        
        print(f"📊 Loaded city progress for {len(progress)} cities")
        return progress
        
    except Exception as e:
        print(f"⚠️  Error loading city progress: {e}")
        return {}

def save_city_progress(progress: Dict[str, CityProgress], output_lock=None):
    """Save city progress to file"""
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        data = {key: asdict(prog) for key, prog in progress.items()}
        
        # Use lock if provided (for multiprocessing safety)
        if output_lock:
            with output_lock:
                with open(CITY_PROGRESS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
        else:
            with open(CITY_PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"⚠️  Error saving city progress: {e}")

def extract_city_from_search_term(search_term: str) -> str:
    """Extract city from search term like 'Bettles, AK' -> 'Bettles'"""
    if not search_term or search_term == "Unknown":
        return "Unknown"
    
    # Split by comma and take the first part (city)
    parts = search_term.split(',')
    if len(parts) >= 1:
        return parts[0].strip()
    return search_term.strip()

def ensure_output_directory():
    """Ensure the output directory exists"""
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"📁 Output directory ready: {OUTPUT_DIR}")
    except Exception as e:
        print(f"❌ Failed to create output directory {OUTPUT_DIR}: {e}")
        sys.exit(1)

def setup_logging():
    """Set up logging to capture all output to log file"""
    try:
        # Ensure output directory exists first
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Configure logging with UTF-8 encoding
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Get the logger
        logger = logging.getLogger()
        
        # Override print function to also log (with emoji filtering for file output)
        def log_print(*args, **kwargs):
            try:
                # Convert all arguments to strings and join them
                message = ' '.join(str(arg) for arg in args)
                # Remove problematic Unicode characters for file logging
                safe_message = message.encode('ascii', errors='ignore').decode('ascii')
                # Log the safe message
                logger.info(safe_message)
            except Exception as e:
                # Fallback to original print if logging fails
                original_print = getattr(__builtins__, 'print', print)
                original_print(*args, **kwargs)
                original_print(f"Logging error: {e}")
        
        # Replace the built-in print function
        import builtins
        builtins.print = log_print
        
        print(f"Logging initialized - output will be saved to: {LOG_FILE}")
        
    except Exception as e:
        # If logging setup fails, continue without logging
        print(f"Failed to setup logging: {e}")
        print("Continuing without log file...")

def load_url_progress() -> Dict[str, bool]:
    """Load URL completion progress"""
    if not os.path.exists(URL_PROGRESS_FILE):
        return {}
    
    try:
        with open(URL_PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  Error loading URL progress: {e}")
        return {}

def save_url_progress(url_progress: Dict[str, bool]):
    """Save URL completion progress"""
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(URL_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(url_progress, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️  Error saving URL progress: {e}")

def mark_url_completed(url: str, url_progress: Dict[str, bool]):
    """Mark a URL as completed and save progress"""
    url_progress[url] = True
    save_url_progress(url_progress)
    
    # Extract search term for better logging
    search_term = extract_search_term_from_url(url)
    print(f"✅ Marked URL as completed: {search_term} ({url[:50]}...)")
    
    # Log to debug file for tracking
    debug_file = os.path.join(OUTPUT_DIR, "url_completion_debug.txt")
    try:
        with open(debug_file, 'a', encoding='utf-8') as f:
            timestamp = datetime.datetime.now().isoformat()
            f.write(f"{timestamp} - URL completed: {search_term} - {url}\n")
    except Exception as e:
        print(f"⚠️  Could not write to debug file: {e}")

def get_worker_output_file(worker_id: Optional[int] = None) -> str:
    """Get output file path for specific worker or main process"""
    if worker_id is not None:
        return os.path.join(OUTPUT_DIR, f"groups_output_curl_worker_{worker_id}.json")
    else:
        return OUTPUT_FILE

def append_group_safe(group: Dict, worker_id: Optional[int] = None):
    """Safely append a group to the output file"""
    try:
        output_file = get_worker_output_file(worker_id)
        
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Append group to worker-specific file
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(group, ensure_ascii=False) + '\n')
        
        # Debug: Write to debug file
        if worker_id is not None:
            debug_file = os.path.join(OUTPUT_DIR, f"worker_{worker_id}_debug.txt")
            with open(debug_file, 'a', encoding='utf-8') as f:
                f.write(f"Saved group: {group.get('name', 'Unknown')} (ID: {group.get('id', 'Unknown')})\n")
    
    except Exception as e:
        # Write error to debug file
        if worker_id is not None:
            debug_file = os.path.join(OUTPUT_DIR, f"worker_{worker_id}_debug.txt")
            with open(debug_file, 'a', encoding='utf-8') as f:
                f.write(f"ERROR saving group: {e}\n")

def merge_worker_output_files():
    """Merge all worker output files into main output file with proper deduplication"""
    print("🔄 Merging worker output files...")
    
    all_groups = []
    seen_ids = set()
    
    # First, load existing main output file if it exists
    existing_groups = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                if isinstance(existing_data, list):
                    existing_groups = existing_data
                    print(f"📁 Loaded {len(existing_groups)} existing groups from main output")
                else:
                    print("⚠️  Main output file is not in expected array format, starting fresh")
        except Exception as e:
            print(f"⚠️  Error reading existing main output: {e}, starting fresh")
    
    # Add existing groups to our collection
    for group in existing_groups:
        group_id = group.get("id")
        if group_id and group_id not in seen_ids:
            all_groups.append(group)
            seen_ids.add(group_id)
    
    print(f"📊 Starting with {len(all_groups)} groups from existing main output")
    
    # Find all worker output files
    worker_files = glob.glob(os.path.join(OUTPUT_DIR, "groups_output_curl_worker_*.json"))
    print(f"📁 Found {len(worker_files)} worker files to merge")
    
    new_groups_from_workers = 0
    
    for worker_file in worker_files:
        try:
            worker_groups = 0
            with open(worker_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            group = json.loads(line)
                            group_id = group.get("id")
                            if group_id and group_id not in seen_ids:
                                all_groups.append(group)
                                seen_ids.add(group_id)
                                new_groups_from_workers += 1
                                worker_groups += 1
                        except json.JSONDecodeError:
                            continue
            
            print(f"   📁 {os.path.basename(worker_file)}: {worker_groups} new groups")
            
        except Exception as e:
            print(f"⚠️  Error reading worker file {worker_file}: {e}")
    
    # Save merged results
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_groups, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Merge complete:")
    print(f"   • Total groups: {len(all_groups):,}")
    print(f"   • Existing groups: {len(existing_groups):,}")
    print(f"   • New groups from workers: {new_groups_from_workers:,}")
    print(f"   • Worker files processed: {len(worker_files)}")
    
    # Worker files are kept separate and never merged
    print(f"💾 Worker files kept separate (no merging)")
    print(f"   • Each worker file contains results from that specific account")
    print(f"   • Worker files: {len(worker_files)} files")
    print(f"   • Main output file: {OUTPUT_FILE} (may be empty or contain old data)")
    print(f"   • To clean up worker files later, run: python -c \"import glob, os; [os.remove(f) for f in glob.glob('output/curl/groups_output_curl_worker_*.json')]\"")

class FacebookGraphQLScraper:
    """Facebook GraphQL scraper using cURL commands with advanced tracking"""
    
    def __init__(self, worker_id: Optional[int] = None, output_lock=None):
        self.call_count = 0
        self.seen_groups = set()
        self.curl_templates = []
        self.progress = load_progress()  # Legacy search progress
        # New enhanced progress tracking
        self.url_progress = load_url_detailed_progress()  # Detailed URL progress
        self.city_progress = load_city_progress()  # City progress
        self.worker_id = worker_id
        self.account_failure_counts = {}  # Track consecutive failures per account
        self.failed_accounts = set()  # Track accounts that have failed 3+ times
        self.output_lock = output_lock  # Store the lock for thread-safe operations
        
    def add_curl_template(self, curl_data: Dict):
        """Add a cURL command as a template for requests"""
        parsed = CurlParser.parse_curl_command(curl_data['curl_command'])
        parsed['account_name'] = curl_data['account_name']
        parsed['file_path'] = curl_data['file_path']
        
        self.curl_templates.append(parsed)
        
        # Initialize failure count for this account
        account_name = curl_data['account_name']
        if account_name not in self.account_failure_counts:
            self.account_failure_counts[account_name] = 0
        
        # Extract and display key info
        c_user = parsed['cookies'].get('c_user', 'Unknown')
        xs_pattern = parsed['cookies'].get('xs', 'Unknown')
        if ':' in xs_pattern:
            pattern = xs_pattern.split(':')[1] if len(xs_pattern.split(':')) > 1 else 'Unknown'
            print(f"   Account ID: {c_user}, xs pattern: :{pattern}:")
    
    def record_account_failure(self, account_name: str, reason: str):
        """Record a failure for an account and remove if it reaches 3 consecutive failures"""
        self.account_failure_counts[account_name] = self.account_failure_counts.get(account_name, 0) + 1
        failure_count = self.account_failure_counts[account_name]
        
        print(f"⚠️  ACCOUNT FAILURE {failure_count}/3: {account_name} - {reason}")
        
        if failure_count >= 3:
            self.failed_accounts.add(account_name)
            print(f"🚨 ACCOUNT REMOVED: {account_name} has been removed from active use after 3 consecutive failures")
            print(f"   Failed accounts this session: {len(self.failed_accounts)}")
            
            # Remove the account from curl_templates to prevent further use
            self.curl_templates = [template for template in self.curl_templates 
                                 if template['account_name'] != account_name]
            
            print(f"   Active accounts remaining: {len(self.curl_templates)}")
        else:
            print(f"   Account will be removed after {3 - failure_count} more consecutive failures")
    
    def record_account_success(self, account_name: str):
        """Record a successful request for an account (resets failure count)"""
        if account_name in self.account_failure_counts and self.account_failure_counts[account_name] > 0:
            old_count = self.account_failure_counts[account_name]
            self.account_failure_counts[account_name] = 0
            print(f"✅ SUCCESS: {account_name} failure count reset (was {old_count}/3)")
    
    def update_city_progress(self, city: str, url: str, group_id: str, search_term: str):
        """Update city progress tracking when a new group is found"""
        if city not in self.city_progress:
            self.city_progress[city] = CityProgress(
                city=city,
                urls_processed=[],
                total_groups_found=0,
                unique_groups=[],
                last_updated=datetime.datetime.now().isoformat(),
                status="active"
            )
        
        city_prog = self.city_progress[city]
        
        # Add URL if not already processed
        if url not in city_prog.urls_processed:
            city_prog.urls_processed.append(url)
        
        # Add group ID if not already found
        if group_id not in city_prog.unique_groups:
            city_prog.unique_groups.append(group_id)
            city_prog.total_groups_found += 1
        
        # Update timestamp
        city_prog.last_updated = datetime.datetime.now().isoformat()
        
        # Save city progress
        save_city_progress(self.city_progress, self.output_lock)
    
    def update_url_progress(self, url: str, search_term: str, city: str, group_id: str, account_name: str):
        """Update detailed URL progress tracking"""
        if url not in self.url_progress:
            self.url_progress[url] = URLProgress(
                url=url,
                search_term=search_term,
                city=city,
                completed_accounts=[],
                failed_accounts=[],
                last_cursor=None,
                total_groups_found=0,
                zero_result_count=0,
                last_updated=datetime.datetime.now().isoformat(),
                status="pending",
                groups_found=[]
            )
        
        url_prog = self.url_progress[url]
        
        # Add group ID if not already found
        if group_id not in url_prog.groups_found:
            url_prog.groups_found.append(group_id)
            url_prog.total_groups_found += 1
        
        # Update timestamp
        url_prog.last_updated = datetime.datetime.now().isoformat()
        
        # Save URL progress
        save_url_detailed_progress(self.url_progress, self.output_lock)
    
    def get_working_accounts(self) -> List[str]:
        """Get list of accounts that haven't failed 3+ times yet"""
        return [template['account_name'] for template in self.curl_templates 
                if template['account_name'] not in self.failed_accounts]
    
    def create_search_variables(self, search_term: str, cursor: str = None) -> Dict:
        """Create GraphQL variables for search"""
        variables = {
            "allow_streaming": False,
            "args": {
                "callsite": "comet:groups_search",
                "config": {
                    "exact_match": False,
                    "high_confidence_config": None,
                    "intercept_config": None,
                    "sts_disambiguation": None,
                    "watch_config": None
                },
                "context": {
                    "bsid": f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}",
                    "tsid": None
                },
                "experience": {
                    "client_defined_experiences": ["ADS_PARALLEL_FETCH"],
                    "encoded_server_defined_params": None,
                    "fbid": None,
                    "type": "GROUPS_TAB_GLOBAL"
                },
                "filters": [],
                "text": search_term
            },
            "count": 5,
            "feedLocation": "SEARCH",
            "feedbackSource": 23,
            "fetch_filters": True,
            "focusCommentID": None,
            "locale": None,
            "privacySelectorRenderLocation": "COMET_STREAM",
            "renderLocation": "search_results_page",
            "scale": 1,
            "stream_initial_count": 0,
            "useDefaultActor": False,
            "__relay_internal__pv__GHLShouldChangeAdIdFieldNamerelayprovider": True,
            "__relay_internal__pv__GHLShouldChangeSponsoredDataFieldNamerelayprovider": True,
            "__relay_internal__pv__IsWorkUserrelayprovider": False,
            "__relay_internal__pv__FBReels_deprecate_short_form_video_context_gkrelayprovider": True,
            "__relay_internal__pv__FeedDeepDiveTopicPillThreadViewEnabledrelayprovider": False,
            "__relay_internal__pv__FBReels_enable_view_dubbed_audio_type_gkrelayprovider": False,
            "__relay_internal__pv__CometImmersivePhotoCanUserDisable3DMotionrelayprovider": False,
            "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False,
            "__relay_internal__pv__IsMergQAPollsrelayprovider": False,
            "__relay_internal__pv__FBReelsMediaFooter_comet_enable_reels_ads_gkrelayprovider": True,
            "__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider": False,
            "__relay_internal__pv__CometUFIShareActionMigrationrelayprovider": True,
            "__relay_internal__pv__CometUFI_dedicated_comment_routable_dialog_gkrelayprovider": False,
            "__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider": True,
            "__relay_internal__pv__FBReelsIFUTileContent_reelsIFUPlayOnHoverrelayprovider": True
        }
        
        if cursor:
            variables["cursor"] = cursor
        
        return variables
    
    def make_request(self, search_term: str, cursor: str = None, account_name: str = None) -> Optional[Dict]:
        """Make a GraphQL request using specific account's cURL template"""
        # SECURITY CHECK: Ensure proxy is loaded
        if PROXIES is None:
            print("❌ CRITICAL ERROR: Proxy not configured! Loading Nimbleway settings...")
            load_nimbleway_settings()
            if PROXIES is None:
                print("❌ Failed to load proxy settings - cannot proceed")
                return None
        
        # Find the template for this account
        template = None
        for tmpl in self.curl_templates:
            if tmpl['account_name'] == account_name:
                template = tmpl
                break
        
        if not template:
            print(f"❌ No cURL template found for account: {account_name}")
            return None
        
        print(f"📡 Making request via {account_name} for '{search_term}' (using Nimbleway proxy)")
        
        # Parse existing data to extract current values
        current_data = urllib.parse.parse_qs(template['data'])
        
        # Extract the original variables from the cURL command
        original_variables_str = current_data.get('variables', [''])[0]
        if original_variables_str:
            try:
                original_variables = json.loads(urllib.parse.unquote(original_variables_str))
                
                # Only update the search term and cursor, preserve everything else
                if 'args' in original_variables and 'text' in original_variables['args']:
                    original_variables['args']['text'] = search_term
                
                if cursor:
                    original_variables['cursor'] = cursor
                elif 'cursor' in original_variables:
                    # Remove cursor for initial request
                    original_variables.pop('cursor', None)
                
                variables = original_variables
            except (json.JSONDecodeError, ValueError) as e:
                print(f"⚠️  Could not parse original variables for {account_name}, using fallback: {e}")
                variables = self.create_search_variables(search_term, cursor)
        else:
            print(f"⚠️  No original variables found for {account_name}, using fallback")
            variables = self.create_search_variables(search_term, cursor)
        
        # Build data payload by updating only the variables
        data_dict = {}
        for key, value_list in current_data.items():
            data_dict[key] = value_list[0] if value_list else ''
        
        # Update variables
        data_dict['variables'] = json.dumps(variables)
        
        # Update timestamps to be current
        current_time = int(time.time())
        if '__spin_t' in data_dict:
            data_dict['__spin_t'] = str(current_time)
        
        # Encode data
        data_encoded = urllib.parse.urlencode(data_dict)
        
        try:
            # Make request with proxy support
            response = requests.post(
                template['url'],
                headers=template['headers'],
                cookies=template['cookies'],
                data=data_encoded,
                proxies=PROXIES,  # Use Nimbleway proxy if configured
                timeout=30
            )
            
            # Log proxy usage for security audit
            if PROXIES:
                print(f"🔍 Request sent through Nimbleway proxy")
            else:
                print(f"🔍 Direct connection used (proxyless mode)")
            
            # Check for errors
            if response.status_code != 200:
                error_msg = f"HTTP {response.status_code} error"
                print(f"❌ {error_msg} with account {account_name}: {response.text[:200]}")
                self.record_account_failure(account_name, error_msg)
                return None
            
            # Parse JSON
            try:
                result = response.json()
                self.call_count += 1
                print(f"✅ Request {self.call_count} successful")
                self.record_account_success(account_name) # Record success
                return result
            except json.JSONDecodeError:
                error_msg = "Invalid JSON response"
                print(f"❌ {error_msg} from account {account_name}: {response.text[:200]}")
                self.record_account_failure(account_name, error_msg)
                return None
                
        except requests.RequestException as e:
            error_msg = f"Request exception: {str(e)}"
            print(f"❌ Request failed with account {account_name}: {e}")
            self.record_account_failure(account_name, error_msg)
            return None
    
    def extract_groups(self, response: Dict, search_term: str) -> List[Dict]:
        """Extract group data from GraphQL response"""
        groups = []
        
        try:
            # Check for GraphQL errors first
            if "errors" in response:
                print(f"❌ GraphQL errors: {response['errors']}")
                return groups
            
            # Navigate the response structure
            serp_response = response.get("data", {}).get("serpResponse", {})
            results = serp_response.get("results", {})
            edges = results.get("edges", [])
            
            for edge in edges:
                node = edge.get("node", {})
                
                # Handle direct Group nodes
                if node.get("__typename") == "Group":
                    group = self.parse_group_node(node, search_term)
                    if group and group["id"] not in self.seen_groups:
                        groups.append(group)
                        self.seen_groups.add(group["id"])
                
                # Handle SearchRenderable nodes (newer Facebook structure)
                elif node.get("__typename") == "SearchRenderable":
                    # Look for groups in rendering_strategy
                    rendering_strategy = edge.get("rendering_strategy", {})
                    if rendering_strategy:
                        view_model = rendering_strategy.get("view_model", {})
                        if view_model:
                            # Check if this is a group profile
                            profile = view_model.get("profile", {})
                            if profile and profile.get("__typename") == "Group":
                                group = self.parse_group_node(profile, search_term)
                                if group and group["id"] not in self.seen_groups:
                                    groups.append(group)
                                    self.seen_groups.add(group["id"])
                            
                            # Also check for groups in other locations within view_model
                            elif "group" in view_model:
                                group_data = view_model["group"]
                                if isinstance(group_data, dict) and group_data.get("__typename") == "Group":
                                    group = self.parse_group_node(group_data, search_term)
                                    if group and group["id"] not in self.seen_groups:
                                        groups.append(group)
                                        self.seen_groups.add(group["id"])
            
        except Exception as e:
            print(f"❌ Error extracting groups: {e}")
            import traceback
            traceback.print_exc()
        
        return groups
    
    def parse_group_node(self, node: Dict, search_term: str) -> Optional[Dict]:
        """Parse a group node from the GraphQL response"""
        try:
            # Extract basic info
            group_id = node.get("id")
            name = node.get("name", "")
            url = node.get("url", "")
            
            # Extract member count and privacy
            group_privacy = "Unknown"
            member_count = 0
            
            # Try to get privacy from different locations
            if "group_privacy" in node:
                group_privacy = node["group_privacy"]
            elif "privacy" in node:
                group_privacy = node["privacy"]
            
            # Try to get member count
            if "member_count" in node:
                member_count = node["member_count"]
            elif "members" in node and isinstance(node["members"], dict):
                member_count = node["members"].get("count", 0)
            
            # Build group object
            group = {
                "id": group_id,
                "name": name,
                "url": url,
                "member_count": member_count,
                "privacy": group_privacy,
                "search_term": search_term,
                "scraped_at": datetime.datetime.now().isoformat(),
            }
            
            return group
            
        except Exception as e:
            print(f"❌ Error parsing group node: {e}")
            return None
    
    def get_next_cursor(self, response: Dict) -> Optional[str]:
        """Extract next page cursor from response"""
        try:
            page_info = response.get("data", {}).get("serpResponse", {}).get("results", {}).get("page_info", {})
            return page_info.get("end_cursor")
        except:
            return None
    
    def scrape_search_term_with_account(self, search_term: str, url: str, account_name: str) -> bool:
        """
        Scrape a search term with a specific account until exhausted
        Returns True if successful, False if failed
        """
        print(f"\n🔍 Starting comprehensive scrape: '{search_term}' via {account_name}")
        
        # Get or create progress entry
        progress_key = f"{search_term}::{account_name}"
        if progress_key not in self.progress:
            self.progress[progress_key] = SearchProgress(
                search_term=search_term,
                url=url,
                completed_accounts=[],
                failed_accounts=[],
                last_cursor=None,
                total_groups_found=0,
                zero_result_count=0,
                last_updated=datetime.datetime.now().isoformat(),
                status="pending"
            )
        
        progress = self.progress[progress_key]
        
        # Check if this account already completed this search
        if account_name in progress.completed_accounts:
            print(f"✅ Account {account_name} already completed '{search_term}'")
            return True
        
        # Check if this account already failed this search
        if account_name in progress.failed_accounts:
            print(f"❌ Account {account_name} previously failed '{search_term}'")
            return False
        
        progress.status = "in_progress"
        cursor = progress.last_cursor
        page = 1
        groups_found_this_session = 0
        consecutive_zero_results = progress.zero_result_count
        
        while consecutive_zero_results < 3:
            print(f"📄 Page {page} (cursor: {'initial' if not cursor else 'continuing'})")
            
            # Make request
            response = self.make_request(search_term, cursor, account_name)
            if not response:
                print(f"❌ Failed to get response for page {page} using account {account_name}")
                progress.failed_accounts.append(account_name)
                progress.status = "failed"
                save_progress(self.progress, self.output_lock)
                return False
            
            # Check for GraphQL errors
            if "errors" in response:
                error_msg = f"GraphQL errors: {response['errors']}"
                print(f"❌ {error_msg} with account {account_name}")
                self.record_account_failure(account_name, error_msg)
                progress.failed_accounts.append(account_name)
                progress.status = "failed"
                save_progress(self.progress, self.output_lock)
                return False
            
            # Extract groups
            groups = self.extract_groups(response, search_term)
            groups_count = len(groups)
            groups_found_this_session += groups_count
            progress.total_groups_found += groups_count
            
            print(f"📊 Found {groups_count} new groups on page {page}")
            
            # Update zero result counter
            if groups_count == 0:
                consecutive_zero_results += 1
                print(f"⚠️  Zero results page {consecutive_zero_results}/3")
            else:
                consecutive_zero_results = 0  # Reset counter
            
            # Save groups immediately
            if groups:
                self.save_groups(groups, url, search_term)
            
            # Get next cursor
            next_cursor = self.get_next_cursor(response)
            if not next_cursor:
                print("✅ No more pages available (reached end)")
                break
            
            # Update progress
            progress.last_cursor = next_cursor
            progress.zero_result_count = consecutive_zero_results
            progress.last_updated = datetime.datetime.now().isoformat()
            save_progress(self.progress, self.output_lock)
            
            cursor = next_cursor
            page += 1
            
            # Wait between requests (reduced by 80%)
            time.sleep(random.uniform(0.02, 0.2))
        
        # Mark as completed or failed
        if consecutive_zero_results >= 3:
            print(f"✅ Completed '{search_term}' via {account_name}: 3 consecutive zero-result pages")
            progress.completed_accounts.append(account_name)
            progress.status = "completed"
            print(f"📊 Total groups found: {groups_found_this_session}")
            save_progress(self.progress, self.output_lock)
            return True
        else:
            print(f"🔄 Paused '{search_term}' via {account_name}: reached end of pagination")
            save_progress(self.progress, self.output_lock)
            return True  # Not a failure, just reached end
    
    def save_groups(self, groups: List[Dict], url: str, search_term: str):
        """Save groups to JSON file immediately with enhanced progress tracking"""
        if not groups:
            return
            
        new_groups_count = 0
        
        for group in groups:
            try:
                # Groups are already filtered in extract_groups, so save all of them
                append_group_safe(group, self.worker_id)
                new_groups_count += 1
                
                # Update enhanced progress tracking
                group_id = group.get('id', '')
                city = extract_city_from_search_term(search_term)
                
                if group_id and city != 'Unknown':
                    # Update city progress
                    self.update_city_progress(city, url, group_id, search_term)
                    
                    # Update URL progress
                    self.update_url_progress(url, search_term, city, group_id, "current_account")
                    
                    # Note: We don't have the full URL here, but we can still track city progress
                    # The URL progress will be updated when we have the full context
                    
            except Exception as e:
                print(f"❌ CRITICAL ERROR saving group {group.get('id', 'N/A')}: {e}")
        
        if new_groups_count > 0:
            print(f"💾 Worker {self.worker_id}: Immediately saved {new_groups_count} new groups to file")
            print(f"📊 Worker {self.worker_id}: Total unique groups seen: {len(self.seen_groups)}")
            print(f"🏙️  Updated progress tracking for cities found in this batch")
        else:
            print(f"📊 Worker {self.worker_id}: No new groups in this batch")
    
    def get_incomplete_searches(self, search_terms: List[Tuple[str, str]]) -> List[Tuple[str, str, str]]:
        """Get list of (search_term, url, account_name) combinations that need to be completed"""
        incomplete = []
        url_progress = load_url_progress()
        
        # First pass: Mark URLs as completed if any account has already completed the search term
        for search_term, url in search_terms:
            # Check if any account has already completed this search term
            search_completed_by_any_account = False
            for template in self.curl_templates:
                test_account = template['account_name']
                progress_key = f"{search_term}::{test_account}"
                if progress_key in self.progress:
                    progress = self.progress[progress_key]
                    if test_account in progress.completed_accounts:
                        search_completed_by_any_account = True
                        break
            
            # If already completed by any account, mark URL as complete immediately
            if search_completed_by_any_account:
                if not url_progress.get(url, False):
                    mark_url_completed(url, url_progress)
                    print(f"✅ Marked URL as completed (search term already done): {search_term}")
                continue
        
        # Second pass: Now check for incomplete searches, skipping completed URLs
        for search_term, url in search_terms:
            # Skip URLs that have already been completed
            if url_progress.get(url, False):
                print(f"⏭️  Skipping completed URL: {search_term}")
                continue
            
            # Add incomplete combinations for this search term
            for template in self.curl_templates:
                account_name = template['account_name']
                progress_key = f"{search_term}::{account_name}"
                
                # Check if this combination needs work
                if progress_key not in self.progress:
                    # Never attempted
                    incomplete.append((search_term, url, account_name))
                else:
                    progress = self.progress[progress_key]
                    if (account_name not in progress.completed_accounts and 
                        account_name not in progress.failed_accounts):
                        # In progress but not completed
                        incomplete.append((search_term, url, account_name))
        
        return incomplete
    
    def process_search_term_parallel(self, search_term: str, url: str, account_name: str) -> Tuple[bool, str]:
        """Process a single search term and return success status and message"""
        try:
            success = self.scrape_search_term_with_account(search_term, url, account_name)
            if success:
                return True, f"SUCCESS: {search_term} completed by {account_name}"
            else:
                return False, f"FAILED: {search_term} failed with {account_name}"
        except Exception as e:
            return False, f"EXCEPTION: Error processing '{search_term}' with {account_name}: {e}"

    def get_comprehensive_progress_stats(self) -> Dict:
        """Get comprehensive progress statistics including URL and city tracking"""
        stats = {
            'legacy_search_progress': {
                'total_entries': len(self.progress),
                'completed': sum(1 for p in self.progress.values() if p.status == 'completed'),
                'failed': sum(1 for p in self.progress.values() if p.status == 'failed'),
                'in_progress': sum(1 for p in self.progress.values() if p.status == 'in_progress'),
                'pending': sum(1 for p in self.progress.values() if p.status == 'pending')
            },
            'url_progress': {
                'total_urls': len(self.url_progress),
                'total_groups_found': sum(p.total_groups_found for p in self.url_progress.values()),
                'urls_with_groups': sum(1 for p in self.url_progress.values() if p.total_groups_found > 0)
            },
            'city_progress': {
                'total_cities': len(self.city_progress),
                'total_groups_found': sum(p.total_groups_found for p in self.city_progress.values()),
                'cities_with_groups': sum(1 for p in self.city_progress.values() if p.total_groups_found > 0),
                'total_unique_groups': sum(len(p.unique_groups) for p in self.city_progress.values())
            }
        }
        
        # Calculate unique cities from search terms
        unique_cities = set()
        for progress in self.progress.values():
            city = extract_city_from_search_term(progress.search_term)
            if city != 'Unknown':
                unique_cities.add(city)
        
        stats['legacy_search_progress']['unique_cities'] = len(unique_cities)
        
        return stats
    
    def print_progress_summary(self):
        """Print a comprehensive progress summary"""
        stats = self.get_comprehensive_progress_stats()
        
        print(f"\n📊 COMPREHENSIVE PROGRESS SUMMARY:")
        print("=" * 60)
        
        print(f"🔍 LEGACY SEARCH PROGRESS:")
        print(f"   • Total entries: {stats['legacy_search_progress']['total_entries']:,}")
        print(f"   • Completed: {stats['legacy_search_progress']['completed']:,}")
        print(f"   • Failed: {stats['legacy_search_progress']['failed']:,}")
        print(f"   • In progress: {stats['legacy_search_progress']['in_progress']:,}")
        print(f"   • Pending: {stats['legacy_search_progress']['pending']:,}")
        print(f"   • Unique cities: {stats['legacy_search_progress']['unique_cities']:,}")
        
        print(f"\n🌐 URL PROGRESS:")
        print(f"   • Total URLs tracked: {stats['url_progress']['total_urls']:,}")
        print(f"   • URLs with groups: {stats['url_progress']['urls_with_groups']:,}")
        print(f"   • Total groups found: {stats['url_progress']['total_groups_found']:,}")
        
        print(f"\n🏙️  CITY PROGRESS:")
        print(f"   • Total cities tracked: {stats['city_progress']['total_cities']:,}")
        print(f"   • Cities with groups: {stats['city_progress']['cities_with_groups']:,}")
        print(f"   • Total groups found: {stats['city_progress']['total_groups_found']:,}")
        print(f"   • Total unique groups: {stats['city_progress']['total_unique_groups']:,}")
        
        print("=" * 60)

def worker_process(worker_id: int, account_name: str, search_terms: List[Tuple[str, str]], output_lock):
    """Worker process to handle a specific account and search terms"""
    debug_file = os.path.join(OUTPUT_DIR, f"worker_{worker_id}_debug.txt")
    
    try:
        # Create a debug file to verify worker is running
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(f"Worker {worker_id} started with account {account_name}\n")
            f.write(f"Processing {len(search_terms)} search terms\n")
        
        print(f"🚀 Worker {worker_id}: Starting with account {account_name}")
        print(f"   Processing {len(search_terms)} search terms")
        
        # Load Nimbleway proxy settings for this worker process
        try:
            load_nimbleway_settings()
            with open(debug_file, 'a', encoding='utf-8') as f:
                f.write("Nimbleway proxy settings loaded successfully\n")
        except Exception as e:
            with open(debug_file, 'a', encoding='utf-8') as f:
                f.write(f"ERROR loading Nimbleway settings: {e}\n")
            print(f"❌ Worker {worker_id}: Failed to load proxy settings: {e}")
            return
        
        # Create worker-specific scraper
        scraper = FacebookGraphQLScraper(worker_id, output_lock)
        
        # Load cURL files and find the template for this account
        curl_files = load_curl_files()
        account_template = None
        for curl_data in curl_files:
            if curl_data['account_name'] == account_name:
                scraper.add_curl_template(curl_data)
                account_template = curl_data
                break
        
        if not account_template:
            error_msg = f"No cURL template found for account: {account_name}"
            print(f"❌ Worker {worker_id}: {error_msg}")
            with open(debug_file, 'a', encoding='utf-8') as f:
                f.write(f"ERROR: {error_msg}\n")
            return
        
        with open(debug_file, 'a', encoding='utf-8') as f:
            f.write(f"cURL template loaded for account: {account_name}\n")
        
        # Load URL progress for this worker and filter out already completed URLs
        url_progress = load_url_progress()
        worker_seen_ids = set()
        completed_count = 0
        failed_count = 0
        
        # Filter out already completed URLs before starting work
        pending_search_terms = []
        for search_term, url in search_terms:
            if url_progress.get(url, False):
                print(f"⏭️  Worker {worker_id}: Skipping already completed URL: {search_term}")
                with open(debug_file, 'a', encoding='utf-8') as f:
                    f.write(f"Skipped already completed URL: {search_term}\n")
                continue
            pending_search_terms.append((search_term, url))
        
        if not pending_search_terms:
            print(f"✅ Worker {worker_id}: All assigned URLs are already completed, nothing to do")
            return
        
        print(f"📋 Worker {worker_id}: Processing {len(pending_search_terms)} pending search terms in PARALLEL")
        
        # Filter out URLs that were completed by other workers
        final_pending_terms = []
        for search_term, url in pending_search_terms:
            if url_progress.get(url, False):
                print(f"⏭️  Worker {worker_id}: URL was completed by another worker, skipping: {search_term}")
                with open(debug_file, 'a', encoding='utf-8') as f:
                    f.write(f"Skipped URL completed by another worker: {search_term}\n")
                continue
            final_pending_terms.append((search_term, url))
        
        if not final_pending_terms:
            print(f"✅ Worker {worker_id}: All assigned URLs were completed by other workers")
            return
        
        print(f"🚀 Worker {worker_id}: Starting PARALLEL processing of {len(final_pending_terms)} search terms")
        
        # Use ThreadPoolExecutor for true parallel processing within the worker
        max_workers = min(MAX_CONCURRENT_SEARCHES_PER_WORKER, len(final_pending_terms))  # Process up to MAX_CONCURRENT_SEARCHES_PER_WORKER search terms concurrently
        progress_save_counter = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all search terms for parallel processing
            future_to_term = {
                executor.submit(scraper.process_search_term_parallel, search_term, url, account_name): (search_term, url)
                for search_term, url in final_pending_terms
            }
            
            # Process completed futures as they finish
            for future in as_completed(future_to_term):
                search_term, url = future_to_term[future]
                
                # Check if account is still available
                if account_name in scraper.failed_accounts:
                    print(f"🚨 Worker {worker_id}: Account {account_name} removed after 3 consecutive failures, stopping worker")
                    with open(debug_file, 'a', encoding='utf-8') as f:
                        f.write(f"WORKER STOPPED: Account {account_name} removed after 3 consecutive failures\n")
                    break
                
                try:
                    success, message = future.result()
                    
                    if success:
                        completed_count += 1
                        # Check if this search term is now complete and immediately mark URL as completed
                        progress_key = f"{search_term}::{account_name}"
                        if progress_key in scraper.progress:
                            progress = scraper.progress[progress_key]
                            if account_name in progress.completed_accounts:
                                # Immediately mark URL as completed to prevent other workers from processing it
                                with output_lock:
                                    mark_url_completed(url, url_progress)
                                    print(f"✅ Worker {worker_id}: Immediately marked URL as completed: {search_term}")
                        
                        print(f"✅ Worker {worker_id}: {message}")
                    else:
                        failed_count += 1
                        print(f"❌ Worker {worker_id}: {message}")
                        
                        # Check if this was due to account-specific issues
                        progress_key = f"{search_term}::{account_name}"
                        if progress_key in scraper.progress:
                            progress = scraper.progress[progress_key]
                            if account_name in progress.failed_accounts:
                                print(f"🚨 Worker {worker_id}: Account {account_name} failed for '{search_term}' - possible account issue")
                    
                    # Write to debug file
                    with open(debug_file, 'a', encoding='utf-8') as f:
                        f.write(f"{message}\n")
                    
                    # Save progress every 5 search terms instead of every single one
                    progress_save_counter += 1
                    if progress_save_counter % 5 == 0:
                        save_progress(scraper.progress, output_lock)
                        print(f"💾 Worker {worker_id}: Saved progress after {progress_save_counter} search terms")
                
                except Exception as e:
                    failed_count += 1
                    error_msg = f"Error processing '{search_term}' with {account_name}: {e}"
                    print(f"❌ Worker {worker_id}: {error_msg}")
                    with open(debug_file, 'a', encoding='utf-8') as f:
                        f.write(f"EXCEPTION: {error_msg}\n")
                    import traceback
                    with open(debug_file, 'a', encoding='utf-8') as f:
                        f.write(f"Traceback: {traceback.format_exc()}\n")
        
        # Final progress save
        save_progress(scraper.progress, output_lock)
        
        result_msg = f"Worker {worker_id} completed: {completed_count} successful, {failed_count} failed"
        print(f"✅ {result_msg}")
        with open(debug_file, 'a', encoding='utf-8') as f:
            f.write(f"FINAL: {result_msg}\n")
        
    except Exception as e:
        error_msg = f"Worker {worker_id} critical error: {e}"
        print(f"❌ {error_msg}")
        try:
            with open(debug_file, 'a', encoding='utf-8') as f:
                f.write(f"CRITICAL ERROR: {error_msg}\n")
                import traceback
                f.write(f"Traceback: {traceback.format_exc()}\n")
        except:
            pass
        import traceback
        traceback.print_exc()

def check_for_race_conditions(url_progress: Dict[str, bool], search_terms: List[Tuple[str, str]], progress: Dict[str, SearchProgress], curl_files: List[Dict]):
    """Check for any race conditions where multiple workers might be processing the same URLs"""
    print("\n🔍 Checking for potential race conditions...")
    
    race_conditions = []
    
    for search_term, url in search_terms:
        if url_progress.get(url, False):
            continue  # Skip completed URLs
            
        # Check if multiple accounts are working on the same search term
        working_accounts = []
        for curl_data in curl_files:
            account_name = curl_data['account_name']
            progress_key = f"{search_term}::{account_name}"
            if progress_key in progress:
                search_progress = progress[progress_key]
                if (account_name not in search_progress.completed_accounts and 
                    account_name not in search_progress.failed_accounts and
                    search_progress.status == "in_progress"):
                    working_accounts.append(account_name)
        
        if len(working_accounts) > 1:
            race_conditions.append({
                'search_term': search_term,
                'url': url,
                'working_accounts': working_accounts
            })
    
    if race_conditions:
        print(f"⚠️  Found {len(race_conditions)} potential race conditions:")
        for race in race_conditions:
            print(f"   • '{race['search_term']}': {len(race['working_accounts'])} accounts working simultaneously")
            print(f"     Accounts: {', '.join(race['working_accounts'])}")
        
        # Resolve race conditions by marking URLs as completed if any account has finished
        resolved_count = 0
        for race in race_conditions:
            search_term = race['search_term']
            url = race['url']
            
            # Check if any account has completed this search term
            for account_name in race['working_accounts']:
                progress_key = f"{search_term}::{account_name}"
                if progress_key in progress:
                    search_progress = progress[progress_key]
                    if account_name in search_progress.completed_accounts:
                        # Mark URL as completed to stop other workers
                        mark_url_completed(url, url_progress)
                        resolved_count += 1
                        print(f"✅ Resolved race condition: {search_term} completed by {account_name}")
                        break
        
        if resolved_count > 0:
            print(f"📊 Resolved {resolved_count} race conditions")
        else:
            print("📊 No race conditions could be automatically resolved")
    else:
        print("✅ No race conditions detected")
    
    return race_conditions

def pre_mark_completed_urls(search_terms: List[Tuple[str, str]], progress: Dict[str, SearchProgress], curl_files: List[Dict]):
    """Pre-check and mark URLs as completed if any account has already completed the search term"""
    print("\n🔍 Pre-checking for already completed search terms...")
    
    url_progress = load_url_progress()
    newly_marked = 0
    
    for search_term, url in search_terms:
        # Skip if URL is already marked as completed
        if url_progress.get(url, False):
            continue
            
        # Check if any account has already completed this search term
        search_completed_by_any_account = False
        for curl_data in curl_files:
            account_name = curl_data['account_name']
            progress_key = f"{search_term}::{account_name}"
            if progress_key in progress:
                search_progress = progress[progress_key]
                if account_name in search_progress.completed_accounts:
                    search_completed_by_any_account = True
                    break
        
        # If already completed by any account, mark URL as complete immediately
        if search_completed_by_any_account:
            if not url_progress.get(url, False):
                mark_url_completed(url, url_progress)
                newly_marked += 1
                print(f"✅ Pre-marked URL as completed: {search_term}")
    
    if newly_marked > 0:
        print(f"📊 Pre-marked {newly_marked} URLs as completed")
    else:
        print("📊 No new URLs to pre-mark as completed")
    
    return url_progress

def test_all_workers(curl_files):
    """Test all cURL accounts before starting main scraping process"""
    print("\n🧪 TESTING ALL WORKERS BEFORE STARTING...")
    print("=" * 60)
    
    working_accounts = []
    failed_accounts = []
    
    for curl_data in curl_files:
        account_name = curl_data['account_name']
        print(f"\n�� Testing account: {account_name}")
        
        # For pre-flight testing, we'll be more lenient and allow 1 retry
        max_test_attempts = 2
        test_passed = False
        last_error = None
        
        for attempt in range(max_test_attempts):
            try:
                # Create a temporary scraper for testing
                test_scraper = FacebookGraphQLScraper(worker_id=f"test_{account_name}", output_lock=None)
                test_scraper.add_curl_template(curl_data)
                
                # Test with a simple search term
                test_search_term = "test"
                if attempt > 0:
                    print(f"   🔄 Retry attempt {attempt + 1}/{max_test_attempts}...")
                else:
                    print(f"   📡 Making test request for '{test_search_term}'...")
                
                response = test_scraper.make_request(test_search_term, cursor=None, account_name=account_name)
                
                if response is None:
                    last_error = 'No response received - possible authentication or connection issue'
                    print(f"   ❌ Attempt {attempt + 1} failed: No response received")
                    continue
                
                # Check for GraphQL errors
                if "errors" in response:
                    last_error = f"GraphQL errors: {response['errors']}"
                    print(f"   ❌ Attempt {attempt + 1} failed: GraphQL errors")
                    continue
                
                # Check for valid response structure
                if "data" not in response:
                    last_error = 'Invalid response structure - possible authentication issue'
                    print(f"   ❌ Attempt {attempt + 1} failed: Invalid response structure")
                    continue
                
                # If we get here, the test passed
                test_passed = True
                print(f"   ✅ SUCCESS: Account {account_name} is working properly")
                break
                
            except Exception as e:
                last_error = f"Exception: {str(e)}"
                print(f"   ❌ Attempt {attempt + 1} failed: Exception: {str(e)}")
                continue
        
        # Determine if account should be included
        if test_passed:
            working_accounts.append(curl_data)
        else:
            failed_accounts.append({
                'account_name': account_name,
                'error': f"{last_error} (failed {max_test_attempts} test attempts)",
                'file_path': curl_data['file_path']
            })
            print(f"   ❌ FAILED: Account {account_name} - Failed all {max_test_attempts} test attempts")
    
    # Print summary
    print(f"\n📊 WORKER TESTING SUMMARY:")
    print("=" * 60)
    print(f"✅ Working accounts: {len(working_accounts)}")
    print(f"❌ Failed accounts: {len(failed_accounts)} (excluded from session)")
    print(f"📈 Success rate: {len(working_accounts)}/{len(curl_files)} ({(len(working_accounts)/len(curl_files)*100):.1f}%)")
    
    if working_accounts:
        print(f"\n✅ WORKING ACCOUNTS:")
        for account in working_accounts:
            print(f"   • {account['account_name']}")
    
    if failed_accounts:
        print(f"\n❌ FAILED ACCOUNTS (EXCLUDED FROM SESSION):")
        for account in failed_accounts:
            print(f"   • {account['account_name']}: {account['error']}")
            print(f"     File: {account['file_path']}")
        print(f"\n⚠️  During scraping, accounts need 3 consecutive failures to be removed")
    
    # Ask user if they want to continue with only working accounts
    if failed_accounts and working_accounts:
        print(f"\n⚠️  WARNING: {len(failed_accounts)} accounts failed testing but {len(working_accounts)} accounts are working.")
        user_input = input("Continue with working accounts only? (y/n): ").strip().lower()
        if user_input != 'y':
            print("❌ User chose to abort. Please fix failed accounts and try again.")
            sys.exit(1)
    elif not working_accounts:
        print(f"\n❌ CRITICAL ERROR: No working accounts found!")
        print("   Please check your cURL files and network connectivity.")
        sys.exit(1)
    else:
        print(f"\n🎉 All accounts passed testing! Proceeding with full scraping...")
    
    print("=" * 60)
    return working_accounts

def analyze_worker_performance():
    """Analyze debug files to show which accounts are failing most"""
    print("\n📊 ANALYZING WORKER PERFORMANCE...")
    print("=" * 60)
    
    account_stats = {}
    
    # Find all debug files
    debug_files = glob.glob(os.path.join(OUTPUT_DIR, "worker_*_debug.txt"))
    
    for debug_file in debug_files:
        worker_id = os.path.basename(debug_file).replace("worker_", "").replace("_debug.txt", "")
        
        try:
            # Try multiple encodings to handle corrupted files
            content = None
            for encoding in ['utf-8', 'cp1252', 'latin1']:
                try:
                    with open(debug_file, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                print(f"Could not read {debug_file} with any encoding - skipping")
                continue
                
            # Extract account name from first few lines
            lines = content.split('\n')
            account_name = "unknown"
            for line in lines[:10]:
                if "with account" in line:
                    account_name = line.split("with account ")[-1].strip()
                    break
            
            if account_name not in account_stats:
                account_stats[account_name] = {
                    'worker_id': worker_id,
                    'successes': 0,
                    'failures': 0,
                    'account_failures': 0,
                    'exceptions': 0
                }
            
            # Count different types of events
            account_stats[account_name]['successes'] += content.count('SUCCESS:')
            account_stats[account_name]['failures'] += content.count('FAILED:')
            account_stats[account_name]['account_failures'] += content.count('ACCOUNT FAILURE:')
            account_stats[account_name]['exceptions'] += content.count('EXCEPTION:')
            
        except Exception as e:
            print(f"Error reading {debug_file}: {e}")
    
    if not account_stats:
        print("⚠️  No debug files found for analysis")
        return
    
    print(f"📈 ACCOUNT PERFORMANCE SUMMARY:")
    print("-" * 80)
    print(f"{'Account':<15} {'Worker':<8} {'Success':<8} {'Failed':<8} {'Acct Fail':<10} {'Exceptions':<12} {'Success %':<10}")
    print("-" * 80)
    
    # Sort by success rate
    sorted_accounts = sorted(account_stats.items(), 
                           key=lambda x: x[1]['successes'] / max(1, x[1]['successes'] + x[1]['failures']) * 100, 
                           reverse=True)
    
    total_successes = 0
    total_failures = 0
    
    for account_name, stats in sorted_accounts:
        total_attempts = stats['successes'] + stats['failures']
        success_rate = (stats['successes'] / max(1, total_attempts)) * 100
        
        total_successes += stats['successes']
        total_failures += stats['failures']
        
        print(f"{account_name:<15} {stats['worker_id']:<8} {stats['successes']:<8} {stats['failures']:<8} "
              f"{stats['account_failures']:<10} {stats['exceptions']:<12} {success_rate:<10.1f}%")
    
    print("-" * 80)
    overall_success_rate = (total_successes / max(1, total_successes + total_failures)) * 100
    print(f"{'TOTAL':<15} {'ALL':<8} {total_successes:<8} {total_failures:<8} {'N/A':<10} {'N/A':<12} {overall_success_rate:<10.1f}%")
    
    # Identify problematic accounts
    problematic_accounts = []
    for account_name, stats in account_stats.items():
        total_attempts = stats['successes'] + stats['failures']
        if total_attempts > 0:
            success_rate = (stats['successes'] / total_attempts) * 100
            if success_rate < 50 or stats['account_failures'] > 0:
                problematic_accounts.append((account_name, success_rate, stats))
    
    if problematic_accounts:
        print(f"\n🚨 PROBLEMATIC ACCOUNTS (Success rate < 50% or account failures):")
        for account_name, success_rate, stats in problematic_accounts:
            print(f"   • {account_name}: {success_rate:.1f}% success rate")
            if stats['account_failures'] > 0:
                print(f"     - {stats['account_failures']} account-specific failures")
            if stats['exceptions'] > 0:
                print(f"     - {stats['exceptions']} exceptions")
            print(f"     - Recommendation: Check cURL file at worker_{stats['worker_id']}_debug.txt")
    
    print("=" * 60)

def show_final_status(search_terms: List[Tuple[str, str]], curl_files: List[Dict]):
    """Show final status of all URLs and search terms"""
    print("\n📊 FINAL STATUS SUMMARY:")
    print("=" * 80)
    
    url_progress = load_url_progress()
    progress_data = load_progress()
    
    # Group by completion status
    completed_urls = []
    pending_urls = []
    failed_searches = []
    
    for search_term, url in search_terms:
        if url_progress.get(url, False):
            completed_urls.append((search_term, url))
        else:
            # Check if any account failed this search
            any_failed = False
            for curl_data in curl_files:
                account_name = curl_data['account_name']
                progress_key = f"{search_term}::{account_name}"
                if progress_key in progress_data:
                    search_progress = progress_data[progress_key]
                    if account_name in search_progress.failed_accounts:
                        any_failed = True
                        break
            
            if any_failed:
                failed_searches.append((search_term, url))
            else:
                pending_urls.append((search_term, url))
    
    print(f"✅ Completed URLs: {len(completed_urls)}")
    print(f"⏳ Pending URLs: {len(pending_urls)}")
    print(f"❌ Failed searches: {len(failed_searches)}")
    print(f"📋 Total URLs: {len(search_terms)}")
    
    if completed_urls:
        print(f"\n✅ COMPLETED URLs:")
        for search_term, url in completed_urls[:10]:  # Show first 10
            print(f"   • {search_term}")
        if len(completed_urls) > 10:
            print(f"   ... and {len(completed_urls) - 10} more")
    
    if pending_urls:
        print(f"\n⏳ PENDING URLs:")
        for search_term, url in pending_urls[:10]:  # Show first 10
            print(f"   • {search_term}")
        if len(pending_urls) > 10:
            print(f"   ... and {len(pending_urls) - 10} more")
    
    if failed_searches:
        print(f"\n❌ FAILED SEARCHES:")
        for search_term, url in failed_searches:
            print(f"   • {search_term}")
            # Show which accounts failed
            failed_accounts = []
            for curl_data in curl_files:
                account_name = curl_data['account_name']
                progress_key = f"{search_term}::{account_name}"
                if progress_key in progress_data:
                    search_progress = progress_data[progress_key]
                    if account_name in search_progress.failed_accounts:
                        failed_accounts.append(account_name)
            if failed_accounts:
                print(f"     Failed accounts: {', '.join(failed_accounts)}")
    
    print("=" * 80)

def main():
    print("🚀 Facebook Groups GraphQL Scraper - Advanced cURL Edition")
    print("🎯 Workers will process ALL URLs until completion")
    print("🔒 Nimbleway proxy support included for security")
    print("-" * 50)
    
    # Ensure output directory exists
    ensure_output_directory()
    
    # Setup logging
    setup_logging()
    
    # Load Nimbleway proxy settings first
    load_nimbleway_settings()
    
    # Proxy is now always loaded - no validation needed
    print("🔒 SECURITY: All requests will go through Nimbleway proxy")
    
    # Load all cURL files from /settings/curl/
    curl_files = load_curl_files()
    if not curl_files:
        print("❌ No cURL files loaded. Please add cURL files to /settings/curl/")
        return
    
    print(f"📊 Total cURL accounts loaded: {len(curl_files)}")
    
    # Test all workers before starting main scraping
    working_curl_files = test_all_workers(curl_files)
    
    # Update the curl_files to only include working accounts
    curl_files = working_curl_files
    print(f"📊 Proceeding with {len(curl_files)} working accounts")
    
    # Load URLs from file and extract search terms
    urls = load_search_urls_from_file()
    if not urls:
        print("❌ No URLs found. Using fallback search terms.")
        search_terms = [("tx", "fallback"), ("california", "fallback")]
    else:
        # Load URL progress to skip completed URLs
        url_progress = load_url_progress()
        print(f"📊 Loaded URL progress: {len(url_progress)} URLs tracked")
        
        # Filter out completed URLs
        pending_urls = [url for url in urls if not url_progress.get(url, False)]
        completed_urls = [url for url in urls if url_progress.get(url, False)]
        
        print(f"📊 URLs status: {len(completed_urls)} completed, {len(pending_urls)} pending")
        
        # Extract search terms from pending URLs only
        search_terms = []
        for url in pending_urls:
            search_term = extract_search_term_from_url(url)
            if search_term and search_term != "default":
                search_terms.append((search_term, url))
        
        # Note: Processing ALL URLs (no deduplication by search term)
        # Each URL might have different parameters, filters, or contexts
        # even if the search term appears similar
    
    print(f"📋 Found {len(search_terms)} URLs to process")
    if len(search_terms) > 0:
        print(f"📝 Sample search terms: {[t[0] for t in search_terms[:5]]}...")
    
    if not search_terms:
        print("✅ No URLs to process!")
        return
    
    # Pre-mark URLs as completed if any account has already completed them
    url_progress = pre_mark_completed_urls(search_terms, load_progress(), curl_files)
    
    # Check for any remaining race conditions
    progress_data = load_progress()
    check_for_race_conditions(url_progress, search_terms, progress_data, curl_files)
    
    # Re-filter search terms after pre-marking to remove newly completed URLs
    final_search_terms = []
    for search_term, url in search_terms:
        if not url_progress.get(url, False):
            final_search_terms.append((search_term, url))
        else:
            print(f"⏭️  Skipping newly marked completed URL: {search_term}")
    
    if not final_search_terms:
        print("✅ All URLs are now completed after pre-checking!")
        return
    
    print(f"📋 Final URLs to process: {len(final_search_terms)} (after pre-checking)")
    
    # Set up parallel processing
    num_workers = len(curl_files)
    print(f"\n🚀 Starting {num_workers} parallel workers using all cURL accounts")
    print(f"📊 Workers will process search terms in TRUE PARALLEL:")
    print(f"   • Each worker uses a different Facebook account")
    print(f"   • Each worker processes up to {MAX_CONCURRENT_SEARCHES_PER_WORKER} search terms simultaneously")
    print(f"   • Workers process different subsets of search terms")
    print(f"   • Each worker processes assigned URLs to completion (3 consecutive zero results)")
    print(f"   • Separate output files prevent file locking")
    print(f"   • Results are kept in separate worker files (no merging)")
    print(f"   • No artificial limits - workers run until all URLs are exhausted")
    print(f"   • OPTIMIZED: 80% faster with reduced delays and parallel processing")
    
    # Distribute search terms among workers
    terms_per_worker = len(final_search_terms) // num_workers
    remainder = len(final_search_terms) % num_workers
    
    # Create shared state and locks for multiprocessing
    manager = Manager()
    output_lock = manager.Lock()
    
    # Start worker processes
    processes = []
    current_index = 0
    
    for worker_id in range(num_workers):
        account_name = curl_files[worker_id]['account_name']
        
        # Calculate search terms for this worker
        worker_terms = terms_per_worker + (1 if worker_id < remainder else 0)
        term_start = current_index
        term_end = current_index + worker_terms
        current_index = term_end
        
        # Get search terms for this worker
        worker_search_terms = final_search_terms[term_start:term_end]
        
        print(f"📋 Worker {worker_id} ({account_name}): Processing {len(worker_search_terms)} search terms ({term_start}-{term_end-1})")
        
        # Start worker process
        p = mp.Process(
            target=worker_process,
            args=(worker_id, account_name, worker_search_terms, output_lock)
        )
        p.start()
        processes.append(p)
        
        # Small delay between starting workers (reduced by 80%)
        time.sleep(0.2)
    
    print(f"🏁 All {num_workers} workers started!")
    print("⏱️  Monitoring worker progress... (Ctrl+C to stop safely)")
    
    try:
        # Monitor workers and periodically check for race conditions
        last_race_check = time.time()
        race_check_interval = 300  # Check every 5 minutes (reduced frequency)
        
        while any(p.is_alive() for p in processes):
            current_time = time.time()
            
            # Periodic race condition check
            if current_time - last_race_check >= race_check_interval:
                print("\n🔍 Periodic race condition check...")
                current_progress = load_progress()
                current_url_progress = load_url_progress()
                check_for_race_conditions(current_url_progress, final_search_terms, current_progress, curl_files)
                last_race_check = current_time
            
            # Small delay to prevent excessive CPU usage (reduced by 80%)
            time.sleep(1)
        
        # Wait for all workers to complete
        for i, p in enumerate(processes):
            p.join()
            if p.exitcode == 0:
                print(f"✅ Worker {i} completed normally")
            else:
                print(f"⚠️  Worker {i} exited with code {p.exitcode}")
        
        print("\n🎉 All workers completed!")
        
        # Final race condition check
        print("\n🔍 Final race condition check...")
        final_progress = load_progress()
        final_url_progress = load_url_progress()
        check_for_race_conditions(final_url_progress, final_search_terms, final_progress, curl_files)
        
        # Skip merging worker output files - keep them separate
        print("📁 Worker output files kept separate (no merging)")
        print(f"   • Worker files: {len(glob.glob(os.path.join(OUTPUT_DIR, 'groups_output_curl_worker_*.json')))} files")
        print(f"   • Each worker file contains results from that specific account")
        print(f"   • Main output file: {OUTPUT_FILE} (may be empty or contain old data)")
        
        # Calculate total groups from all worker files
        worker_files = glob.glob(os.path.join(OUTPUT_DIR, "groups_output_curl_worker_*.json"))
        total_groups = 0
        worker_stats = []
        
        for worker_file in worker_files:
            try:
                with open(worker_file, 'r', encoding='utf-8') as f:
                    worker_groups = 0
                    for line in f:
                        if line.strip():
                            worker_groups += 1
                    total_groups += worker_groups
                    worker_stats.append((os.path.basename(worker_file), worker_groups))
            except Exception as e:
                print(f"⚠️  Error reading {worker_file}: {e}")
        
        print(f"\n📊 FINAL STATISTICS:")
        print(f"   • Total groups found across all workers: {total_groups}")
        print(f"   • Workers used: {num_workers}")
        print(f"   • URLs processed: {len(search_terms)}")
        print(f"💾 Results saved to separate worker files:")
        for worker_file, count in worker_stats:
            print(f"   • {worker_file}: {count} groups")
        print(f"📊 Progress saved to: {PROGRESS_FILE}")
        print(f"📊 URL progress saved to: {URL_PROGRESS_FILE}")
        print(f"📝 Session log saved to: {LOG_FILE}")
        
        # Show URL completion statistics
        url_progress = load_url_progress()
        total_completed_urls = sum(1 for completed in url_progress.values() if completed)
        print(f"📈 URLs completed: {total_completed_urls}/{len(url_progress)} tracked URLs")
        
        print("🔒 All requests were made through Nimbleway proxy for security")
        print("✅ Parallel cURL scraping completed successfully!")
        
        # Analyze worker performance to identify failing accounts
        analyze_worker_performance()

        # Show final status of all URLs and search terms
        show_final_status(search_terms, curl_files)
        
        # Show comprehensive progress summary
        print(f"\n🔍 COMPREHENSIVE PROGRESS ANALYSIS:")
        print("=" * 60)
        
        # Create a temporary scraper instance to get progress stats
        temp_scraper = FacebookGraphQLScraper(output_lock=None)
        temp_scraper.print_progress_summary()
        
        print(f"\n💡 PROGRESS TRACKING IMPROVEMENTS:")
        print(f"   • Enhanced URL tracking: {URL_DETAILED_PROGRESS_FILE}")
        print(f"   • City-level tracking: {CITY_PROGRESS_FILE}")
        print(f"   • Legacy search tracking: {PROGRESS_FILE}")
        print(f"   • URL completion tracking: {URL_PROGRESS_FILE}")
        print(f"   • All progress files now track individual URLs and cities")
        print(f"   • City counts should now match between progress and output files")
        
    except KeyboardInterrupt:
        print(f"\n\n🛑 Interrupt received! Stopping workers...")
        
        # Terminate all worker processes
        for p in processes:
            if p.is_alive():
                p.terminate()
        
        # Wait for processes to stop
        for p in processes:
            p.join(timeout=5)
        
        # Skip merging worker output files - keep them separate
        print("📁 Worker output files kept separate (no merging after interrupt)")
        worker_files = glob.glob(os.path.join(OUTPUT_DIR, "groups_output_curl_worker_*.json"))
        print(f"   • Worker files available: {len(worker_files)} files")
        
        # Show final status even when interrupted
        try:
            show_final_status(search_terms, curl_files)
        except:
            pass
        
        print(f"✅ Workers stopped safely. Progress has been saved.")
        sys.exit(0)

if __name__ == "__main__":
    main() 