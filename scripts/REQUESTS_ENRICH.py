import json
import time
import random
import requests
import os
import sys
import urllib.parse
import multiprocessing as mp
from multiprocessing import Manager, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root - focusing on cURL output files
CURL_OUTPUT_DIR = os.path.join(PARENT_DIR, "output", "curl")
OUTPUT_FILE = os.path.join(CURL_OUTPUT_DIR, "groups_output_enriched.jsonl")
COOKIES_DIR = os.path.join(PARENT_DIR, "settings", "cookies")
HOVERCARD_DOC_ID = "24093351840274783"  # Updated from new session

SLEEP_BETWEEN_REQUESTS = (0.5, 2.0)  # 0.5-2 second random wait
WORKERS_PER_SESSION = 4  # Number of workers per working session

# --- Proxy Mode Selection ---
print("Choose proxy mode: [1] Nimbleway proxy [2] Proxyless")
mode = input("Enter 1 or 2 (default 1): ").strip()
if mode == "2":
    PROXIES = None
    print("Running proxyless (direct connection)...")
else:
    # Nimbleway Proxy Integration
    NIMBLEWAY_SETTINGS_FILE = os.path.join(PARENT_DIR, "settings", "nimbleway_settings.json")
    if os.path.exists(NIMBLEWAY_SETTINGS_FILE):
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
        print(f"✅ Using Nimbleway proxy: {NIMBLEWAY_PROXY}")
        print("🔒 SECURITY: All requests will go through Nimbleway proxy")
    else:
        PROXIES = None
        print("Nimbleway settings not found, running proxyless.")

# --- Load all cURL worker output files ---
def load_curl_worker_files():
    """Load all groups from cURL worker output files"""
    all_groups = []
    worker_files = glob.glob(os.path.join(CURL_OUTPUT_DIR, "groups_output_curl_worker_*.json"))
    
    # Also check for the main output file
    main_output_file = os.path.join(CURL_OUTPUT_DIR, "groups_output_curl.json")
    if os.path.exists(main_output_file):
        worker_files.append(main_output_file)
    
    if not worker_files:
        print(f"❌ No cURL worker output files found in {CURL_OUTPUT_DIR}")
        return []
    
    print(f"🔍 Found {len(worker_files)} cURL output files to process:")
    
    for file_path in worker_files:
        print(f"  📁 {os.path.basename(file_path)}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    # Handle both JSON array and JSONL formats
                    if content.startswith('['):
                        # JSON array format
                        groups = json.loads(content)
                        all_groups.extend(groups)
                        print(f"    ✅ Loaded {len(groups)} groups (JSON array format)")
                    else:
                        # JSONL format
                        file_groups = []
                        for line in content.split('\n'):
                            if line.strip():
                                try:
                                    group = json.loads(line)
                                    file_groups.append(group)
                                except json.JSONDecodeError:
                                    continue
                        all_groups.extend(file_groups)
                        print(f"    ✅ Loaded {len(file_groups)} groups (JSONL format)")
                else:
                    print(f"    ⚠️  File is empty")
        except Exception as e:
            print(f"    ❌ Error reading {file_path}: {e}")
    
    # Remove duplicates based on group ID
    unique_groups = {}
    for group in all_groups:
        if "id" in group:
            unique_groups[str(group["id"])] = group
    
    print(f"📊 Total groups loaded: {len(all_groups)}")
    print(f"📊 Unique groups (after deduplication): {len(unique_groups)}")
    
    return list(unique_groups.values())

# --- Load all cookie files ---
def load_all_cookie_files():
    """Load all cookie files from the cookies directory"""
    cookie_files = glob.glob(os.path.join(COOKIES_DIR, "*_cookies.json"))
    loaded_sessions = []
    
    for cookie_file in cookie_files:
        try:
            with open(cookie_file, "r", encoding="utf-8") as f:
                all_data = json.load(f)
            
            # Extract cookie data (exclude session_headers and session_payload)
            cookies = {}
            for key, value in all_data.items():
                if key not in ["session_headers", "session_payload"] and isinstance(value, str):
                    cookies[key] = value
            
            # Get session data
            session_headers = all_data.get("session_headers", {})
            session_payload = all_data.get("session_payload", {})
            
            session_name = os.path.basename(cookie_file).replace("_cookies.json", "")
            
            loaded_sessions.append({
                "name": session_name,
                "cookies": cookies,
                "headers": session_headers,
                "payload": session_payload,
                "file_path": cookie_file
            })
            print(f"✅ Loaded session: {session_name}")
            
        except Exception as e:
            print(f"❌ Failed to load {cookie_file}: {e}")
    
    return loaded_sessions

# --- Test basic connectivity first ---
def test_basic_connectivity(proxies):
    """Test if we can reach Facebook at all"""
    try:
        session = requests.Session()
        resp = session.get(
            "https://www.facebook.com",
            proxies=proxies,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        if resp.status_code == 200:
            return True, "Basic connectivity OK"
        else:
            return False, f"HTTP {resp.status_code} when accessing Facebook"
    except Exception as e:
        return False, f"Basic connectivity failed: {str(e)}"

# --- Test session validity ---
def test_session_validity(session_data, proxies):
    """Test if a session is still valid by making a simple request"""
    try:
        session = requests.Session()
        session.cookies.update(session_data["cookies"])
        
        # First, try a simple GET request to see if the session can access Facebook
        print("   🔍 Testing basic session access...")
        basic_headers = {
            "User-Agent": session_data["headers"].get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": session_data["headers"].get("Accept-Language", "en-US,en;q=0.9"),
        }
        
        try:
            basic_resp = session.get(
                "https://www.facebook.com",
                headers=basic_headers,
                proxies=proxies,
                timeout=15
            )
            
            if basic_resp.status_code == 200:
                # Check if we're logged in (not redirected to login page)
                if "login" not in basic_resp.url.lower() and "checkpoint" not in basic_resp.text.lower():
                    print("   ✅ Basic session access OK")
                else:
                    print("   ⚠️  Session redirected to login/checkpoint")
            else:
                print(f"   ⚠️  Basic access returned HTTP {basic_resp.status_code}")
        except Exception as e:
            print(f"   ⚠️  Basic access test failed: {e}")
        
        # Now try the GraphQL API test
        print("   🔍 Testing GraphQL API access...")
        
        # Create test headers - use the session's own headers as much as possible
        test_headers = {
            "User-Agent": session_data["headers"].get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": session_data["headers"].get("Accept-Language", "en-US,en;q=0.9"),
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.facebook.com",
            "Referer": session_data["headers"].get("Referer", "https://www.facebook.com/search/groups/?q=test"),
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "X-FB-Friendly-Name": "CometHovercardQueryRendererQuery",
            "x-fb-lsd": session_data["headers"].get("x-fb-lsd", ""),
            "x-asbd-id": "359341",
        }
        
        # Add browser-specific headers if available - be more inclusive
        for header_key in ["sec-ch-prefers-color-scheme", "sec-ch-ua", "sec-ch-ua-full-version-list", 
                          "sec-ch-ua-mobile", "sec-ch-ua-model", "sec-ch-ua-platform", 
                          "sec-ch-ua-platform-version", "Priority", "Connection", "TE", "Sec-GPC"]:
            if header_key in session_data["headers"]:
                test_headers[header_key] = session_data["headers"][header_key]
        
        # Use the session's own doc_id if available, otherwise fall back to the hardcoded one
        doc_id = session_data["payload"].get("doc_id", HOVERCARD_DOC_ID)
        
        # Create test payload using session data - be more conservative
        test_payload = session_data["payload"].copy()
        test_payload.update({
            "variables": json.dumps({
                "actionBarRenderLocation": "WWW_COMET_HOVERCARD",
                "context": "DEFAULT", 
                "entityID": "123456789",  # Test group ID
                "scale": "1",
                "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False
            }, ensure_ascii=False, separators=(',', ':')),
            "doc_id": doc_id,
        })
        
        # Add a small delay before testing to avoid rate limiting
        time.sleep(random.uniform(0.5, 1.5))
        
        resp = session.post(
            "https://www.facebook.com/api/graphql/",
            headers=test_headers,
            data=test_payload,
            proxies=proxies,
            timeout=20,  # Increased timeout
        )
        
        # Check response status first
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
        
        # Check if response is JSON and doesn't contain obvious error indicators
        try:
            result = resp.json()
            
            # Look for common error patterns
            if "errors" in result:
                error_msg = str(result['errors'])[:200]
                return False, f"GraphQL errors: {error_msg}"
            
            # Check if we got a valid response structure
            if "data" in result and "node" in result["data"]:
                return True, "Session valid - got valid response structure"
            elif "data" in result:
                return True, "Session valid - got data response"
            else:
                # If we got JSON but no data, the session might still be working
                return True, "Session valid - got JSON response"
                
        except json.JSONDecodeError as e:
            # If response is not JSON, check if it's an HTML error page
            if "text/html" in resp.headers.get("content-type", ""):
                if "checkpoint" in resp.text.lower() or "security" in resp.text.lower():
                    return False, "Session blocked - security checkpoint detected"
                elif "login" in resp.text.lower() or "password" in resp.text.lower():
                    return False, "Session expired - login required"
                else:
                    return False, f"HTML response (not JSON): {resp.text[:200]}"
            else:
                # Check if it's a rate limiting or other non-JSON response that might still indicate a working session
                if "rate" in resp.text.lower() or "limit" in resp.text.lower():
                    return True, "Session valid - rate limited (still working)"
                elif "too many requests" in resp.text.lower():
                    return True, "Session valid - too many requests (still working)"
                elif resp.status_code == 429:  # Too Many Requests
                    return True, "Session valid - HTTP 429 (rate limited but working)"
                else:
                    # For now, be more lenient and consider non-JSON responses as potentially valid
                    # This helps with sessions that might work but return unexpected response formats
                    # Also log the actual response for debugging
                    print(f"   🔍 Non-JSON response (status {resp.status_code}): {resp.text[:300]}")
                    return True, f"Session potentially valid - non-JSON response (status {resp.status_code})"
            
    except requests.exceptions.Timeout:
        return False, "Request timeout"
    except requests.exceptions.ProxyError:
        return False, "Proxy connection error"
    except Exception as e:
        return False, f"Request failed: {str(e)}"

# --- Initialize working sessions and create worker pool ---
def initialize_working_sessions():
    """Load all sessions, test their validity, and create worker assignments"""
    print("🔍 Loading and testing all available sessions...")
    
    # Test basic connectivity first
    print("🌐 Testing basic connectivity to Facebook...")
    connectivity_ok, connectivity_msg = test_basic_connectivity(PROXIES)
    if connectivity_ok:
        print(f"✅ {connectivity_msg}")
    else:
        print(f"❌ {connectivity_msg}")
        print("⚠️  Basic connectivity failed - this may affect session testing")
    
    all_sessions = load_all_cookie_files()
    working_sessions = []
    
    for session in all_sessions:
        print(f"🧪 Testing session: {session['name']}...")
        
        # Show some debug info about the session
        doc_id = session["payload"].get("doc_id", "Not found")
        has_lsd = "x-fb-lsd" in session["headers"] and session["headers"]["x-fb-lsd"]
        user_agent = session["headers"].get("User-Agent", "Unknown")
        browser_type = "Chrome/Edge" if "AppleWebKit" in user_agent else "Firefox" if "Gecko" in user_agent else "Other"
        print(f"   📋 Session info: doc_id={doc_id}, has_lsd={has_lsd}, browser={browser_type}")
        
        # Show key headers for debugging
        key_headers = ["x-fb-lsd", "x-asbd-id", "Referer", "Accept-Language"]
        header_info = []
        for header in key_headers:
            if header in session["headers"]:
                header_info.append(f"{header}={session['headers'][header][:30]}...")
        if header_info:
            print(f"   🔍 Key headers: {', '.join(header_info)}")
        
        is_valid, message = test_session_validity(session, PROXIES)
        
        if is_valid:
            working_sessions.append(session)
            print(f"✅ Session {session['name']} is working: {message}")
        else:
            print(f"❌ Session {session['name']} failed: {message}")
    
    if not working_sessions:
        print("❌ No working sessions found!")
        print("\n🔍 Debugging tips:")
        print("   - Check if cookies are expired")
        print("   - Verify proxy settings")
        print("   - Try running without proxy first")
        print("   - Check if Facebook is blocking the IP")
        print("   - Firefox sessions may need different validation approach")
        
        # Ask user if they want to proceed anyway with untested sessions
        print("\n⚠️  WARNING: Proceeding with untested sessions may cause failures during processing.")
        proceed_anyway = input("Do you want to proceed anyway? (y/N): ").strip().lower()
        
        if proceed_anyway in ['y', 'yes']:
            print("🔄 Proceeding with all sessions (untested)...")
            working_sessions = all_sessions
        else:
            print("❌ Exiting as requested.")
            sys.exit(1)
    
    # Create worker assignments - each session gets WORKERS_PER_SESSION workers
    worker_assignments = []
    for session in working_sessions:
        for worker_num in range(WORKERS_PER_SESSION):
            worker_assignments.append({
                "session": session,
                "worker_id": f"{session['name']}_worker_{worker_num + 1}"
            })
    
    total_workers = len(worker_assignments)
    print(f"\n🎯 Found {len(working_sessions)} working sessions")
    print(f"⚡ Created {total_workers} total workers ({WORKERS_PER_SESSION} workers per session)")
    for session in working_sessions:
        browser_type = "Chrome/Edge" if "AppleWebKit" in session["headers"].get("User-Agent", "") else "Firefox" if "Gecko" in session["headers"].get("User-Agent", "") else "Other"
        print(f"  ✓ {session['name']} ({browser_type}) → {WORKERS_PER_SESSION} workers")
    
    return worker_assignments

# --- Hovercard Query ---
def make_hovercard_variables(group_id):
    variables = {
        "actionBarRenderLocation": "WWW_COMET_HOVERCARD",
        "context": "DEFAULT",
        "entityID": group_id,
        "scale": "1",
        "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False
    }
    return json.dumps(variables, ensure_ascii=False, separators=(',', ':'))

# --- Extract fields from hovercard response ---
def extract_hovercard_fields(resp):
    try:
        group = resp["data"]["node"]["comet_hovercard_renderer"]["group"]
        return {
            "hovercard_name": group.get("name"),
            "hovercard_url": group.get("url"),
            "hovercard_member_count": group.get("group_member_profiles", {}).get("formatted_count_text"),
            "hovercard_privacy": group.get("privacy_info", {}).get("title", {}).get("text"),
        }
    except Exception as e:
        print("Error extracting hovercard fields:", e)
        return {}

# --- Worker Function for Parallel Processing ---
def process_group_worker(group_data, worker_assignment, proxies, doc_id_param):
    """Worker function to process a single group with hovercard enrichment"""
    try:
        group = group_data['group']
        group_id = str(group["id"])
        session_data = worker_assignment['session']
        worker_id = worker_assignment['worker_id']
        
        # Create session for this worker
        session = requests.Session()
        session.cookies.update(session_data["cookies"])
        
        # Create headers from session data
        headers = {
            "User-Agent": session_data["headers"].get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": session_data["headers"].get("Accept-Language", "en-US,en;q=0.9"),
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.facebook.com",
            "Referer": session_data["headers"].get("Referer", "https://www.facebook.com/search/groups/?q=texas"),
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "X-FB-Friendly-Name": "CometHovercardQueryRendererQuery",
            "x-fb-lsd": session_data["headers"].get("x-fb-lsd", ""),
            "x-asbd-id": "359341",
        }
        
        # Add browser-specific headers if available - be more inclusive
        for header_key in ["sec-ch-prefers-color-scheme", "sec-ch-ua", "sec-ch-ua-full-version-list", 
                          "sec-ch-ua-mobile", "sec-ch-ua-model", "sec-ch-ua-platform", 
                          "sec-ch-ua-platform-version", "Priority", "Connection", "TE", "Sec-GPC"]:
            if header_key in session_data["headers"]:
                headers[header_key] = session_data["headers"][header_key]
        
        # Make hovercard request using session payload data
        variables = make_hovercard_variables(group_id)
        data = session_data["payload"].copy()
        
        # Use the session's own doc_id if available, otherwise fall back to the hardcoded one
        doc_id = session_data["payload"].get("doc_id", HOVERCARD_DOC_ID)
        
        data.update({
            "variables": variables,
            "doc_id": doc_id,
        })
        
        resp = session.post(
            "https://www.facebook.com/api/graphql/",
            headers=headers,
            data=data,
            proxies=proxies,
            timeout=30,
        )
        
        try:
            result = resp.json()
        except Exception:
            return {"success": False, "group_id": group_id, "error": "Non-JSON response", "worker_id": worker_id}
        
        # Check for errors
        if "errors" in result:
            return {"success": False, "group_id": group_id, "error": f"GraphQL errors: {result['errors']}", "worker_id": worker_id}
        
        hovercard = extract_hovercard_fields(result)
        enriched_group = group.copy()
        enriched_group.update(hovercard)
        
        # Random wait between calls
        time.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS))
        
        return {
            "success": True, 
            "group_id": group_id, 
            "enriched_group": enriched_group,
            "worker_id": worker_id
        }
        
    except Exception as e:
        return {"success": False, "group_id": group_id, "error": str(e), "worker_id": worker_assignment['worker_id']}

# --- Load already enriched IDs ---
def load_enriched_ids():
    enriched_ids = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    group = json.loads(line)
                    if "id" in group:
                        # Ensure ID is converted to string for consistent comparison
                        enriched_ids.add(str(group["id"]))
                except Exception:
                    continue
    return enriched_ids

# --- Main enrichment loop ---
def main():
    print("🚀 cURL Groups Hovercard Enrichment Script")
    print("=" * 60)
    
    # Load all groups from cURL worker files
    print("\n📂 Loading groups from cURL worker output files...")
    all_groups = load_curl_worker_files()
    
    if not all_groups:
        print("❌ No groups found in cURL output files.")
        return
    
    # Initialize working sessions and worker assignments
    print("\n🔧 Initializing sessions and workers...")
    worker_assignments = initialize_working_sessions()
    num_workers = len(worker_assignments)
    
    # Load already enriched IDs
    enriched_ids = load_enriched_ids()
    print(f"📊 Skipping {len(enriched_ids)} groups already enriched.")
    
    # Filter out already enriched groups
    groups_to_process = [group for group in all_groups if str(group["id"]) not in enriched_ids]
    print(f"📊 Processing {len(groups_to_process)} groups that need enrichment.")
    print(f"📊 Total groups available: {len(all_groups)}")
    print(f"📊 Already enriched: {len(enriched_ids)}")
    
    if not groups_to_process:
        print("✅ No groups to process. All groups are already enriched.")
        return
    
    # Create output file lock for thread-safe writing
    output_lock = Lock()
    
    # Ensure output directory exists
    os.makedirs(CURL_OUTPUT_DIR, exist_ok=True)
    
    # Process groups in parallel
    print(f"\n🚀 Starting parallel processing with {num_workers} total workers...")
    print(f"📁 Output will be saved to: {OUTPUT_FILE}")
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks, cycling through worker assignments
        future_to_group = {}
        for idx, group in enumerate(groups_to_process):
            worker_assignment = worker_assignments[idx % num_workers]  # Round-robin assignment
            future = executor.submit(
                process_group_worker, 
                {"group": group, "index": idx}, 
                worker_assignment,
                PROXIES, 
                None  # doc_id will be determined by each session
            )
            future_to_group[future] = {
                "group": group, 
                "index": idx, 
                "worker_id": worker_assignment['worker_id']
            }
        
        # Process completed tasks
        completed = 0
        successful = 0
        failed = 0
        # Track stats by session name (not worker ID)
        session_stats = {}
        for worker_assignment in worker_assignments:
            session_name = worker_assignment['session']['name']
            if session_name not in session_stats:
                session_stats[session_name] = {"success": 0, "failed": 0}
        
        for future in as_completed(future_to_group):
            group_info = future_to_group[future]
            group = group_info["group"]
            idx = group_info["index"]
            worker_id = group_info["worker_id"]
            
            try:
                result = future.result()
                completed += 1
                
                # Extract session name from worker_id
                session_name = '_'.join(worker_id.split('_')[:-2])  # Remove _worker_X suffix
                
                if result["success"]:
                    successful += 1
                    session_stats[session_name]["success"] += 1
                    enriched_group = result["enriched_group"]
                    
                    # Thread-safe writing to output file
                    with output_lock:
                        with open(OUTPUT_FILE, "a", encoding="utf-8") as out:
                            out.write(json.dumps(enriched_group, ensure_ascii=False) + "\n")
                    
                    # Show enrichment details
                    search_term = group.get('search_term', 'Unknown')
                    group_name = enriched_group.get('name', enriched_group.get('hovercard_name', 'Unknown'))
                    member_count = enriched_group.get('hovercard_member_count', 'Unknown')
                    privacy = enriched_group.get('hovercard_privacy', 'Unknown')
                    
                    print(f"[{completed}/{len(groups_to_process)}] {result['worker_id']} enriched group {result['group_id']}: {group_name} | {member_count} members | {privacy} | Search: {search_term}")
                else:
                    failed += 1
                    session_stats[session_name]["failed"] += 1
                    print(f"[{completed}/{len(groups_to_process)}] {result['worker_id']} failed group {result['group_id']}: {result.get('error', 'Unknown error')}")
                
                # Progress update every 25 completions
                if completed % 25 == 0:
                    print(f"\n📊 Progress: {completed}/{len(groups_to_process)} completed ({successful} successful, {failed} failed)")
                    print("📈 Session stats:")
                    for sess_name, stats in session_stats.items():
                        total = stats["success"] + stats["failed"]
                        if total > 0:
                            success_rate = (stats["success"] / total) * 100
                            print(f"  {sess_name}: {stats['success']}/{total} ({success_rate:.1f}% success)")
                    print()
                
            except Exception as e:
                failed += 1
                # Extract session name from worker_id for error stats
                session_name = '_'.join(worker_id.split('_')[:-2])
                if session_name in session_stats:
                    session_stats[session_name]["failed"] += 1
                print(f"[{completed}/{len(groups_to_process)}] {worker_id} exception: {e}")
    
    print(f"\n✅ Parallel processing completed!")
    print(f"📊 Final stats: {completed} total, {successful} successful, {failed} failed")
    print("\n📈 Final session performance:")
    for sess_name, stats in session_stats.items():
        total = stats["success"] + stats["failed"]
        if total > 0:
            success_rate = (stats["success"] / total) * 100
            print(f"  {sess_name}: {stats['success']}/{total} ({success_rate:.1f}% success)")
    print(f"\n📁 Enriched data written to {OUTPUT_FILE}")
    print(f"🎉 cURL enrichment completed successfully!")

if __name__ == "__main__":
    main() 