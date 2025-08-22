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

# Configuration
# TARGET_CALLS = 10000  # Removed - workers should process all assigned URLs

# File paths
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PARENT_DIR, "output", "curl")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "groups_output_curl.json")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "curl_scraper_progress.json")
URL_PROGRESS_FILE = os.path.join(OUTPUT_DIR, "url_progress_curl.json")
LOG_FILE = os.path.join(OUTPUT_DIR, "curl_scraper.log")
URLS_FILE = os.path.join(PARENT_DIR, "settings", "facebook_group_urls.txt")
CURL_DIR = os.path.join(PARENT_DIR, "settings", "curl")

# Global proxy configuration (will be set during initialization)
PROXIES = None

@dataclass
class SearchProgress:
    """Track progress for each search term"""
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
    
    # Always use Nimbleway proxy - no interactive selection needed
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

def save_progress(progress: Dict[str, SearchProgress]):
    """Save scraping progress to file"""
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        data = {key: asdict(prog) for key, prog in progress.items()}
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"⚠️  Error saving progress: {e}")

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
    print(f"✅ Marked URL as completed: {url}")

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
    """Merge all worker output files into main output file"""
    print("🔄 Merging worker output files...")
    
    all_groups = []
    seen_ids = set()
    
    # Find all worker output files
    worker_files = glob.glob(os.path.join(OUTPUT_DIR, "groups_output_curl_worker_*.json"))
    
    for worker_file in worker_files:
        try:
            with open(worker_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            group = json.loads(line)
                            if group.get("id") and group["id"] not in seen_ids:
                                all_groups.append(group)
                                seen_ids.add(group["id"])
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"⚠️  Error reading worker file {worker_file}: {e}")
    
    # Save merged results
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_groups, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Merged {len(all_groups)} unique groups from {len(worker_files)} worker files")
    
    # Clean up worker files
    for worker_file in worker_files:
        try:
            os.remove(worker_file)
        except:
            pass

class FacebookGraphQLScraper:
    """Facebook GraphQL scraper using cURL commands with advanced tracking"""
    
    def __init__(self, worker_id: Optional[int] = None):
        self.call_count = 0
        self.seen_groups = set()
        self.curl_templates = []
        self.progress = load_progress()
        self.worker_id = worker_id
        self.account_failure_counts = {}  # Track consecutive failures per account
        self.failed_accounts = set()  # Track accounts that have failed 3+ times
        
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
                save_progress(self.progress)
                return False
            
            # Check for GraphQL errors
            if "errors" in response:
                error_msg = f"GraphQL errors: {response['errors']}"
                print(f"❌ {error_msg} with account {account_name}")
                self.record_account_failure(account_name, error_msg)
                progress.failed_accounts.append(account_name)
                progress.status = "failed"
                save_progress(self.progress)
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
                self.save_groups(groups)
            
            # Get next cursor
            next_cursor = self.get_next_cursor(response)
            if not next_cursor:
                print("✅ No more pages available (reached end)")
                break
            
            # Update progress
            progress.last_cursor = next_cursor
            progress.zero_result_count = consecutive_zero_results
            progress.last_updated = datetime.datetime.now().isoformat()
            save_progress(self.progress)
            
            cursor = next_cursor
            page += 1
            
            # Wait between requests
            time.sleep(random.uniform(0, 1))
        
        # Mark as completed or failed
        if consecutive_zero_results >= 3:
            print(f"✅ Completed '{search_term}' via {account_name}: 3 consecutive zero-result pages")
            progress.completed_accounts.append(account_name)
            progress.status = "completed"
            print(f"📊 Total groups found: {groups_found_this_session}")
            save_progress(self.progress)
            return True
        else:
            print(f"🔄 Paused '{search_term}' via {account_name}: reached end of pagination")
            save_progress(self.progress)
            return True  # Not a failure, just reached end
    
    def save_groups(self, groups: List[Dict]):
        """Save groups to JSON file immediately"""
        if not groups:
            return
            
        new_groups_count = 0
        
        for group in groups:
            try:
                # Groups are already filtered in extract_groups, so save all of them
                append_group_safe(group, self.worker_id)
                new_groups_count += 1
            except Exception as e:
                print(f"❌ CRITICAL ERROR saving group {group.get('id', 'N/A')}: {e}")
        
        if new_groups_count > 0:
            print(f"💾 Worker {self.worker_id}: Immediately saved {new_groups_count} new groups to file")
            print(f"📊 Worker {self.worker_id}: Total unique groups seen: {len(self.seen_groups)}")
        else:
            print(f"📊 Worker {self.worker_id}: No new groups in this batch")
    
    def get_incomplete_searches(self, search_terms: List[Tuple[str, str]]) -> List[Tuple[str, str, str]]:
        """Get list of (search_term, url, account_name) combinations that need to be completed"""
        incomplete = []
        url_progress = load_url_progress()
        
        for search_term, url in search_terms:
            # Skip URLs that have already been completed
            if url_progress.get(url, False):
                print(f"⏭️  Skipping completed URL: {search_term}")
                continue
                
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
            
            # If already completed by any account, mark URL as complete and skip
            if search_completed_by_any_account:
                if not url_progress.get(url, False):
                    mark_url_completed(url, url_progress)
                print(f"⏭️  Skipping search term already completed: {search_term}")
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
        scraper = FacebookGraphQLScraper(worker_id)
        
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
        
        # Load URL progress for this worker
        url_progress = load_url_progress()
        worker_seen_ids = set()
        completed_count = 0
        failed_count = 0
        
        for i, (search_term, url) in enumerate(search_terms):
            # Check if URL is already completed
            if url_progress.get(url, False):
                print(f"⏭️  Worker {worker_id}: Skipping completed URL: {search_term}")
                with open(debug_file, 'a', encoding='utf-8') as f:
                    f.write(f"Skipped completed URL: {search_term}\n")
                continue
            
            # Check if the account is still available (not marked as failed)
            if account_name in scraper.failed_accounts:
                print(f"🚨 Worker {worker_id}: Account {account_name} has been removed after 3 consecutive failures, stopping worker")
                with open(debug_file, 'a', encoding='utf-8') as f:
                    f.write(f"WORKER STOPPED: Account {account_name} removed after 3 consecutive failures\n")
                break
            
            print(f"\n🎯 Worker {worker_id}: Processing '{search_term}' ({i+1}/{len(search_terms)})")
            with open(debug_file, 'a', encoding='utf-8') as f:
                f.write(f"Starting search term: {search_term}\n")
            
            # Process this search term with this account
            try:
                success = scraper.scrape_search_term_with_account(search_term, url, account_name)
                
                if success:
                    completed_count += 1
                    # Check if this search term is now complete
                    progress_key = f"{search_term}::{account_name}"
                    if progress_key in scraper.progress:
                        progress = scraper.progress[progress_key]
                        if account_name in progress.completed_accounts:
                            # Mark URL as completed
                            with output_lock:
                                mark_url_completed(url, url_progress)
                    
                    with open(debug_file, 'a', encoding='utf-8') as f:
                        f.write(f"SUCCESS: {search_term} completed by {account_name}\n")
                else:
                    failed_count += 1
                    with open(debug_file, 'a', encoding='utf-8') as f:
                        f.write(f"FAILED: {search_term} failed with {account_name}\n")
                    
                    # Check if this was due to account-specific issues
                    progress_key = f"{search_term}::{account_name}"
                    if progress_key in scraper.progress:
                        progress = scraper.progress[progress_key]
                        if account_name in progress.failed_accounts:
                            print(f"🚨 Worker {worker_id}: Account {account_name} failed for '{search_term}' - possible account issue")
                            with open(debug_file, 'a', encoding='utf-8') as f:
                                f.write(f"ACCOUNT FAILURE: {account_name} failed for {search_term}\n")
                
            except Exception as e:
                failed_count += 1
                error_msg = f"Error processing '{search_term}' with {account_name}: {e}"
                print(f"❌ Worker {worker_id}: {error_msg}")
                with open(debug_file, 'a', encoding='utf-8') as f:
                    f.write(f"EXCEPTION: {error_msg}\n")
                import traceback
                with open(debug_file, 'a', encoding='utf-8') as f:
                    f.write(f"Traceback: {traceback.format_exc()}\n")
            
            # Save progress after each search term
            save_progress(scraper.progress)
            
            # Small delay between search terms
            time.sleep(random.uniform(0.5, 1.5))
        
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
                test_scraper = FacebookGraphQLScraper(worker_id=f"test_{account_name}")
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
    
    # Set up parallel processing
    num_workers = len(curl_files)
    print(f"\n🚀 Starting {num_workers} parallel workers using all cURL accounts")
    print(f"📊 Workers will process search terms in parallel:")
    print(f"   • Each worker uses a different Facebook account")
    print(f"   • Workers process different subsets of search terms")
    print(f"   • Each worker processes assigned URLs to completion (3 consecutive zero results)")
    print(f"   • Separate output files prevent file locking")
    print(f"   • Results are merged at the end")
    print(f"   • No artificial limits - workers run until all URLs are exhausted")
    
    # Distribute search terms among workers
    terms_per_worker = len(search_terms) // num_workers
    remainder = len(search_terms) % num_workers
    
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
        worker_search_terms = search_terms[term_start:term_end]
        
        print(f"📋 Worker {worker_id} ({account_name}): Processing {len(worker_search_terms)} search terms ({term_start}-{term_end-1})")
        
        # Start worker process
        p = mp.Process(
            target=worker_process,
            args=(worker_id, account_name, worker_search_terms, output_lock)
        )
        p.start()
        processes.append(p)
        
        # Small delay between starting workers
        time.sleep(1)
    
    print(f"🏁 All {num_workers} workers started!")
    print("⏱️  Monitoring worker progress... (Ctrl+C to stop safely)")
    
    try:
        # Wait for all workers to complete
        for i, p in enumerate(processes):
            p.join()
            if p.exitcode == 0:
                print(f"✅ Worker {i} completed normally")
            else:
                print(f"⚠️  Worker {i} exited with code {p.exitcode}")
        
        print("\n🎉 All workers completed!")
        
        # Merge worker output files
        merge_worker_output_files()
        
        # Final statistics
        final_count = 0
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                final_count = len(data)
        
        print(f"\n📊 FINAL STATISTICS:")
        print(f"   • Total groups found: {final_count}")
        print(f"   • Workers used: {num_workers}")
        print(f"   • URLs processed: {len(search_terms)}")
        print(f"💾 Results saved to: {OUTPUT_FILE}")
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
        
    except KeyboardInterrupt:
        print(f"\n\n🛑 Interrupt received! Stopping workers...")
        
        # Terminate all worker processes
        for p in processes:
            if p.is_alive():
                p.terminate()
        
        # Wait for processes to stop
        for p in processes:
            p.join(timeout=5)
        
        # Merge any available results
        try:
            merge_worker_output_files()
        except:
            pass
        
        print(f"✅ Workers stopped safely. Progress has been saved.")
        sys.exit(0)

if __name__ == "__main__":
    main() 