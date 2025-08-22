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

# Paths relative to project root
INPUT_FILE = os.path.join(PARENT_DIR, "output", "groups_graphql.jsonl")
OUTPUT_FILE = os.path.join(PARENT_DIR, "output", "groups_enriched_curl.jsonl")
COOKIES_DIR = os.path.join(PARENT_DIR, "settings", "cookies")
HOVERCARD_DOC_ID = "24093351840274783"  # <-- updated from new session

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

# --- Test session validity ---
def test_session_validity(session_data, proxies):
    """Test if a session is still valid by making a simple request"""
    try:
        session = requests.Session()
        session.cookies.update(session_data["cookies"])
        
        # Create test headers
        test_headers = {
            "User-Agent": session_data["headers"].get("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.facebook.com",
            "Referer": "https://www.facebook.com/search/groups/?q=test",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "X-FB-Friendly-Name": "CometHovercardQueryRendererQuery",
            "x-fb-lsd": session_data["headers"].get("x-fb-lsd", ""),
            "x-asbd-id": "359341",
        }
        
        # Create test payload using session data
        test_payload = session_data["payload"].copy()
        test_payload.update({
            "variables": json.dumps({
                "actionBarRenderLocation": "WWW_COMET_HOVERCARD",
                "context": "DEFAULT", 
                "entityID": "123456789",  # Test group ID
                "scale": "1",
                "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False
            }, ensure_ascii=False, separators=(',', ':')),
            "doc_id": HOVERCARD_DOC_ID,
        })
        
        resp = session.post(
            "https://www.facebook.com/api/graphql/",
            headers=test_headers,
            data=test_payload,
            proxies=proxies,
            timeout=15,
        )
        
        # Check if response is JSON and doesn't contain obvious error indicators
        try:
            result = resp.json()
            # Look for common error patterns
            if "errors" in result:
                return False, f"GraphQL errors: {result['errors']}"
            if resp.status_code != 200:
                return False, f"HTTP {resp.status_code}"
            return True, "Session valid"
        except:
            return False, "Non-JSON response"
            
    except Exception as e:
        return False, f"Request failed: {str(e)}"

# --- Initialize working sessions and create worker pool ---
def initialize_working_sessions():
    """Load all sessions, test their validity, and create worker assignments"""
    print("🔍 Loading and testing all available sessions...")
    all_sessions = load_all_cookie_files()
    working_sessions = []
    
    for session in all_sessions:
        print(f"🧪 Testing session: {session['name']}...")
        is_valid, message = test_session_validity(session, PROXIES)
        
        if is_valid:
            working_sessions.append(session)
            print(f"✅ Session {session['name']} is working: {message}")
        else:
            print(f"❌ Session {session['name']} failed: {message}")
    
    if not working_sessions:
        print("❌ No working sessions found! Cannot proceed.")
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
        print(f"  ✓ {session['name']} → {WORKERS_PER_SESSION} workers")
    
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
            "name": group.get("name"),
            "url": group.get("url"),
            "member_count": group.get("group_member_profiles", {}).get("formatted_count_text"),
            "privacy": group.get("privacy_info", {}).get("title", {}).get("text"),
        }
    except Exception as e:
        print("Error extracting hovercard fields:", e)
        return {}

# --- Worker Function for Parallel Processing ---
def process_group_worker(group_data, worker_assignment, proxies, doc_id):
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
        
        # Add browser-specific headers if available
        for header_key in ["sec-ch-prefers-color-scheme", "sec-ch-ua", "sec-ch-ua-full-version-list", 
                          "sec-ch-ua-mobile", "sec-ch-ua-model", "sec-ch-ua-platform", 
                          "sec-ch-ua-platform-version", "Priority", "Connection", "TE"]:
            if header_key in session_data["headers"]:
                headers[header_key] = session_data["headers"][header_key]
        
        # Make hovercard request using session payload data
        variables = make_hovercard_variables(group_id)
        data = session_data["payload"].copy()
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

# --- Main enrichment loop ---
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found.")
        return
    
    # Initialize working sessions and worker assignments
    worker_assignments = initialize_working_sessions()
    num_workers = len(worker_assignments)
    
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        groups = [json.loads(line) for line in f if line.strip()]
    
    # Filter groups to only those with city_state
    groups_with_city_state = [group for group in groups if "city_state" in group]
    print(f"Found {len(groups_with_city_state)} groups with city_state out of {len(groups)} total groups")
    
    enriched_ids = load_enriched_ids()
    print(f"Skipping {len(enriched_ids)} groups already enriched.")
    
    # COMPREHENSIVE DEBUG: Let's figure out this discrepancy
    enriched_with_city_state = 0
    enriched_total_lines = 0
    enriched_unique_ids = set()
    
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    enriched_total_lines += 1
                    try:
                        group = json.loads(line)
                        if "id" in group:
                            enriched_unique_ids.add(str(group["id"]))
                        if "city_state" in group:
                            enriched_with_city_state += 1
                    except:
                        continue
    
    # Count main file IDs for comparison
    main_file_ids = set()
    main_with_city_state_ids = set()
    for group in groups:
        if "id" in group:
            main_file_ids.add(str(group["id"]))
        if "city_state" in group and "id" in group:
            main_with_city_state_ids.add(str(group["id"]))
    
    # Find overlapping IDs
    overlapping_ids = main_file_ids.intersection(enriched_unique_ids)
    main_only_ids = main_file_ids - enriched_unique_ids
    enriched_only_ids = enriched_unique_ids - main_file_ids
    
    print(f"\n🔍 COMPREHENSIVE DEBUG ANALYSIS:")
    print(f"=" * 60)
    print(f"📁 MAIN FILE (groups.jsonl):")
    print(f"   • Total lines/groups: {len(groups):,}")
    print(f"   • Groups with city_state: {len(groups_with_city_state):,}")
    print(f"   • Unique IDs in main file: {len(main_file_ids):,}")
    print(f"   • IDs with city_state: {len(main_with_city_state_ids):,}")
    
    print(f"\n📁 ENRICHED FILE (groups_enriched.jsonl):")
    print(f"   • Total lines in file: {enriched_total_lines:,}")
    print(f"   • Lines with city_state: {enriched_with_city_state:,}")
    print(f"   • Unique IDs in enriched: {len(enriched_unique_ids):,}")
    print(f"   • IDs loaded by script: {len(enriched_ids):,}")
    
    print(f"\n🔗 ID OVERLAP ANALYSIS:")
    print(f"   • IDs in both files: {len(overlapping_ids):,}")
    print(f"   • IDs only in main: {len(main_only_ids):,}")
    print(f"   • IDs only in enriched: {len(enriched_only_ids):,}")
    
    print(f"\n📊 MATH CHECK:")
    print(f"   • Expected remaining (simple): {len(groups_with_city_state)} - {len(enriched_ids)} = {len(groups_with_city_state) - len(enriched_ids):,}")
    print(f"   • Expected remaining (by ID): {len(main_with_city_state_ids)} - {len(overlapping_ids)} = {len(main_with_city_state_ids) - len(overlapping_ids):,}")
    
    # Show sample IDs for investigation
    if enriched_only_ids:
        sample_enriched_only = list(enriched_only_ids)[:5]
        print(f"\n🔍 SAMPLE IDs ONLY IN ENRICHED: {sample_enriched_only}")
    
    if main_only_ids:
        sample_main_only = list(main_only_ids)[:5]
        print(f"🔍 SAMPLE IDs ONLY IN MAIN: {sample_main_only}")
    
    print(f"=" * 60)
    
    # Filter out already enriched groups
    groups_to_process = [group for group in groups_with_city_state if str(group["id"]) not in enriched_ids]
    print(f"Processing {len(groups_to_process)} groups with city_state that need enrichment.")
    
    if not groups_to_process:
        print("No groups to process. All groups are already enriched.")
        return
    
    # Create output file lock for thread-safe writing
    output_lock = Lock()
    
    # Process groups in parallel
    print(f"🚀 Starting parallel processing with {num_workers} total workers...")
    
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
                HOVERCARD_DOC_ID
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
                    
                    print(f"[{completed}/{len(groups_to_process)}] {result['worker_id']} enriched group {result['group_id']}: {enriched_group.get('name')} (city_state: {group.get('city_state')})")
                else:
                    failed += 1
                    session_stats[session_name]["failed"] += 1
                    print(f"[{completed}/{len(groups_to_process)}] {result['worker_id']} failed group {result['group_id']}: {result.get('error', 'Unknown error')}")
                
                # Progress update every 50 completions
                if completed % 50 == 0:
                    print(f"📊 Progress: {completed}/{len(groups_to_process)} completed ({successful} successful, {failed} failed)")
                    print("📈 Session stats:")
                    for sess_name, stats in session_stats.items():
                        total = stats["success"] + stats["failed"]
                        if total > 0:
                            success_rate = (stats["success"] / total) * 100
                            print(f"  {sess_name}: {stats['success']}/{total} ({success_rate:.1f}% success)")
                
            except Exception as e:
                failed += 1
                # Extract session name from worker_id for error stats
                session_name = '_'.join(worker_id.split('_')[:-2])
                if session_name in session_stats:
                    session_stats[session_name]["failed"] += 1
                print(f"[{completed}/{len(groups_to_process)}] {worker_id} exception: {e}")
    
    print(f"✅ Parallel processing completed!")
    print(f"📊 Final stats: {completed} total, {successful} successful, {failed} failed")
    print("📈 Final session performance:")
    for sess_name, stats in session_stats.items():
        total = stats["success"] + stats["failed"]
        if total > 0:
            success_rate = (stats["success"] / total) * 100
            print(f"  {sess_name}: {stats['success']}/{total} ({success_rate:.1f}% success)")
    print(f"📁 Enriched data written to {OUTPUT_FILE}")

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

if __name__ == "__main__":
    main() 