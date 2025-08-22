import json
import time
import random
import subprocess
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
CURL_OUTPUT_DIR = os.path.join(PARENT_DIR, "output", "curl")
OUTPUT_FILE = os.path.join(CURL_OUTPUT_DIR, "groups_output_enriched_curl.jsonl")
CURL_HOVER_DIR = os.path.join(PARENT_DIR, "settings", "curl_hover")

SLEEP_BETWEEN_REQUESTS = (1.0, 3.0)  # 1-3 second random wait
WORKERS_PER_CURL = 4  # Number of workers per curl file

# --- Proxy Mode Selection ---
print("Choose proxy mode: [1] Nimbleway proxy [2] Proxyless")
mode = input("Enter 1 or 2 (default 1): ").strip()
if mode == "2":
    PROXIES = None
    NIMBLEWAY_PROXY = None
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
        encoded_account_name = urllib.parse.quote(ACCOUNT_NAME)
        NIMBLEWAY_PROXY = f"http://account-{encoded_account_name}-pipeline-{PIPELINE_NAME}:{PIPELINE_PASSWORD}@{NIMBLEWAY_HOST}:{NIMBLEWAY_PORT}"
        
        # Set PROXIES variable for compatibility
        PROXIES = {"http": NIMBLEWAY_PROXY, "https": NIMBLEWAY_PROXY}
        
        print(f"✅ Using Nimbleway proxy: {NIMBLEWAY_PROXY}")
        print("🔒 SECURITY: All requests will go through Nimbleway proxy")
    else:
        print("❌ Nimbleway settings not found!")
        sys.exit(1)

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

# --- Load curl hovercard files ---
def load_curl_hovercard_files():
    """Load all curl hovercard files from the curl_hover directory"""
    curl_files = glob.glob(os.path.join(CURL_HOVER_DIR, "*.curl"))
    loaded_curls = []
    
    for curl_file in curl_files:
        try:
            with open(curl_file, 'r', encoding='utf-8') as f:
                curl_content = f.read().strip()
            
            # Parse the curl command to extract components
            parsed_curl = parse_curl_command(curl_content)
            
            session_name = os.path.basename(curl_file).replace(".curl", "")
            
            loaded_curls.append({
                "name": session_name,
                "curl_content": curl_content,
                "parsed": parsed_curl,
                "file_path": curl_file
            })
            print(f"✅ Loaded curl: {session_name}")
            
        except Exception as e:
            print(f"❌ Failed to load {curl_file}: {e}")
    
    return loaded_curls

# --- Parse curl command ---
def parse_curl_command(curl_content):
    """Parse curl command to extract headers, data, and other components"""
    parsed = {
        "url": "",
        "headers": {},
        "data": "",
        "method": "POST"
    }
    
    lines = curl_content.split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('curl '):
            # Extract URL
            url_start = line.find('"') + 1
            url_end = line.find('"', url_start)
            if url_start > 0 and url_end > url_start:
                parsed["url"] = line[url_start:url_end]
        elif line.startswith('-H '):
            # Extract header
            header_start = line.find('"') + 1
            header_end = line.find('"', header_start)
            if header_start > 0 and header_end > header_start:
                header_line = line[header_start:header_end]
                if ':' in header_line:
                    key, value = header_line.split(':', 1)
                    parsed["headers"][key.strip()] = value.strip()
        elif line.startswith('--data-raw '):
            # Extract data
            data_start = line.find('"') + 1
            data_end = line.find('"', data_start)
            if data_start > 0 and data_end > data_start:
                parsed["data"] = line[data_start:data_end]
    
    return parsed

# --- Test curl validity ---
def test_curl_validity(curl_data, proxies):
    """Test if a curl command is still valid by making a simple request"""
    try:
        print(f"   🔍 Testing curl: {curl_data['name']}...")
        
        # Create a test request with a dummy group ID
        test_data = curl_data["parsed"]["data"].replace(
            '"entityID":"695830943815404"',
            '"entityID":"123456789"'
        )
        
        # Build curl command with proxy
        if proxies:
            proxy_arg = f"--proxy {NIMBLEWAY_PROXY}"
        else:
            proxy_arg = ""
        
        # Create test curl command - remove --compressed to avoid zstd issues
        test_curl = f'curl "{curl_data["parsed"]["url"]}" {proxy_arg} -X POST'
        
        # Add headers
        for key, value in curl_data["parsed"]["headers"].items():
            test_curl += f' -H "{key}: {value}"'
        
        # Add test data
        test_curl += f' --data-raw "{test_data}"'
        
        # Execute test
        result = subprocess.run(
            test_curl,
            shell=True,
            capture_output=True,
            text=False,  # Don't decode text to avoid Unicode issues
            timeout=30
        )
        
        if result.returncode == 0:
            try:
                # Check if response is empty
                if not result.stdout or len(result.stdout) == 0:
                    return False, "Empty response from curl"
                
                # Try to decode as UTF-8 first
                try:
                    stdout_text = result.stdout.decode('utf-8', errors='ignore')
                except UnicodeDecodeError:
                    # If UTF-8 fails, try other encodings
                    try:
                        stdout_text = result.stdout.decode('latin-1', errors='ignore')
                    except:
                        stdout_text = str(result.stdout)
                
                # Check if it looks like HTML (error page)
                if stdout_text.strip().startswith('<') or 'html' in stdout_text.lower():
                    if 'checkpoint' in stdout_text.lower() or 'security' in stdout_text.lower():
                        return False, "Security checkpoint detected"
                    elif 'login' in stdout_text.lower() or 'password' in stdout_text.lower():
                        return False, "Login required - session expired"
                    else:
                        return False, f"HTML response (likely error page): {stdout_text[:100]}"
                
                # Check if it's compressed data (binary/garbled)
                if len(stdout_text) > 0 and not stdout_text.isprintable():
                    # Try to find any readable text
                    readable_chars = sum(1 for c in stdout_text if c.isprintable())
                    if readable_chars < len(stdout_text) * 0.3:  # Less than 30% readable
                        # This might be compressed data - try to proceed anyway
                        # Facebook sometimes sends compressed responses even without compression headers
                        return True, "Compressed response detected - proceeding anyway"
                
                # Try to parse response as JSON
                try:
                    response_data = json.loads(stdout_text)
                    if "data" in response_data:
                        return True, "Curl valid - got data response"
                    elif "errors" in response_data:
                        return False, f"Curl returned errors: {response_data['errors']}"
                    else:
                        return True, "Curl valid - got response"
                except json.JSONDecodeError:
                    # Check if it's a rate limiting response
                    if "rate" in stdout_text.lower() or "limit" in stdout_text.lower():
                        return True, "Curl valid - rate limited (still working)"
                    elif "too many requests" in stdout_text.lower():
                        return True, "Curl valid - too many requests (still working)"
                    else:
                        # If it's compressed data, we might still be able to work with it
                        if len(stdout_text) > 0 and not stdout_text.isprintable():
                            return True, "Compressed response - will attempt processing"
                        else:
                            return False, f"Non-JSON response: {stdout_text[:200]}"
                        
            except Exception as e:
                return False, f"Error processing response: {str(e)}"
        else:
            stderr_text = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""
            return False, f"Curl failed with return code {result.returncode}: {stderr_text}"
            
    except subprocess.TimeoutExpired:
        return False, "Curl request timeout"
    except Exception as e:
        return False, f"Curl test failed: {str(e)}"

# --- Initialize working curls and create worker pool ---
def initialize_working_curls():
    """Load all curls, test their validity, and create worker assignments"""
    print("🔍 Loading and testing all available curl commands...")
    
    all_curls = load_curl_hovercard_files()
    working_curls = []
    
    for curl_data in all_curls:
        is_valid, message = test_curl_validity(curl_data, PROXIES)
        
        if is_valid:
            working_curls.append(curl_data)
            print(f"✅ Curl {curl_data['name']} is working: {message}")
        else:
            print(f"❌ Curl {curl_data['name']} failed: {message}")
    
    if not working_curls:
        print("❌ No working curls found!")
        print("\n🔍 Debugging tips:")
        print("   - Check if curl commands are valid")
        print("   - Verify proxy settings")
        print("   - Check if Facebook is blocking the IP")
        
        # Ask user if they want to proceed anyway with untested curls
        print("\n⚠️  WARNING: Proceeding with untested curls may cause failures during processing.")
        proceed_anyway = input("Do you want to proceed anyway? (y/N): ").strip().lower()
        
        if proceed_anyway in ['y', 'yes']:
            print("🔄 Proceeding with all curls (untested)...")
            working_curls = all_curls
        else:
            print("❌ Exiting as requested.")
            sys.exit(1)
    
    # Create worker assignments - each curl gets WORKERS_PER_CURL workers
    worker_assignments = []
    for curl_data in working_curls:
        for worker_num in range(WORKERS_PER_CURL):
            worker_assignments.append({
                "curl": curl_data,
                "worker_id": f"{curl_data['name']}_worker_{worker_num + 1}"
            })
    
    total_workers = len(worker_assignments)
    print(f"\n🎯 Found {len(working_curls)} working curls")
    print(f"⚡ Created {total_workers} total workers ({WORKERS_PER_CURL} workers per curl)")
    for curl_data in working_curls:
        print(f"  ✓ {curl_data['name']} → {WORKERS_PER_CURL} workers")
    
    return worker_assignments

# --- Worker Function for Parallel Processing ---
def process_group_worker(group_data, worker_assignment, proxies):
    """Worker function to process a single group with curl hovercard enrichment"""
    try:
        group = group_data['group']
        group_id = str(group["id"])
        curl_data = worker_assignment['curl']
        worker_id = worker_assignment['worker_id']
        
        # Replace the entityID in the data with the actual group ID
        modified_data = curl_data["parsed"]["data"].replace(
            '"entityID":"695830943815404"',
            f'"entityID":"{group_id}"'
        )
        
        # Build curl command with proxy
        if proxies:
            proxy_arg = f"--proxy {NIMBLEWAY_PROXY}"
        else:
            proxy_arg = ""
        
        # Create curl command - remove --compressed to avoid zstd issues
        curl_cmd = f'curl "{curl_data["parsed"]["url"]}" {proxy_arg} -X POST'
        
        # Add headers
        for key, value in curl_data["parsed"]["headers"].items():
            curl_cmd += f' -H "{key}: {value}"'
        
        # Add modified data
        curl_cmd += f' --data-raw "{modified_data}"'
        
        # Execute curl
        result = subprocess.run(
            curl_cmd,
            shell=True,
            capture_output=True,
            text=False,  # Don't decode text to avoid Unicode issues
            timeout=30
        )
        
        if result.returncode != 0:
            stderr_text = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""
            return {"success": False, "group_id": group_id, "error": f"Curl failed: {stderr_text}", "worker_id": worker_id}
        
        try:
            stdout_text = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
            if not stdout_text.strip():
                return {"success": False, "group_id": group_id, "error": "Empty response from curl", "worker_id": worker_id}
            
            # Check if response is compressed/binary data
            if len(stdout_text) > 0 and not stdout_text.isprintable():
                readable_chars = sum(1 for c in stdout_text if c.isprintable())
                if readable_chars < len(stdout_text) * 0.3:  # Less than 30% readable
                    # This is compressed data - we can't process it without decompression
                    return {"success": False, "group_id": group_id, "error": "Compressed response (zstd) - cannot process", "worker_id": worker_id}
            
            response_data = json.loads(stdout_text)
        except json.JSONDecodeError:
            # Check if it's compressed data
            if len(stdout_text) > 0 and not stdout_text.isprintable():
                return {"success": False, "group_id": group_id, "error": "Compressed response (zstd) - cannot process", "worker_id": worker_id}
            else:
                return {"success": False, "group_id": group_id, "error": "Non-JSON response", "worker_id": worker_id}
        
        # Check for errors
        if "errors" in response_data:
            return {"success": False, "group_id": group_id, "error": f"GraphQL errors: {response_data['errors']}", "worker_id": worker_id}
        
        # Extract hovercard fields
        hovercard = extract_hovercard_fields(response_data)
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

# --- Load already enriched IDs ---
def load_enriched_ids():
    enriched_ids = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    group = json.loads(line)
                    if "id" in group:
                        enriched_ids.add(str(group["id"]))
                except Exception:
                    continue
    return enriched_ids

# --- Main enrichment loop ---
def main():
    print("🚀 cURL Groups Hovercard Enrichment Script (CURL-based)")
    print("=" * 70)
    
    # Load all groups from cURL worker files
    print("\n📂 Loading groups from cURL worker output files...")
    all_groups = load_curl_worker_files()
    
    if not all_groups:
        print("❌ No groups found in cURL output files.")
        return
    
    # Initialize working curls and worker assignments
    print("\n🔧 Initializing curls and workers...")
    worker_assignments = initialize_working_curls()
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
            worker_assignment = worker_assignments[idx % num_workers]
            future = executor.submit(
                process_group_worker, 
                {"group": group, "index": idx}, 
                worker_assignment,
                PROXIES
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
        curl_stats = {}
        for worker_assignment in worker_assignments:
            curl_name = worker_assignment['curl']['name']
            if curl_name not in curl_stats:
                curl_stats[curl_name] = {"success": 0, "failed": 0}
        
        for future in as_completed(future_to_group):
            group_info = future_to_group[future]
            group = group_info["group"]
            idx = group_info["index"]
            worker_id = group_info["worker_id"]
            
            try:
                result = future.result()
                completed += 1
                
                # Extract curl name from worker_id
                curl_name = '_'.join(worker_id.split('_')[:-2])
                
                if result["success"]:
                    successful += 1
                    curl_stats[curl_name]["success"] += 1
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
                    curl_stats[curl_name]["failed"] += 1
                    print(f"[{completed}/{len(groups_to_process)}] {result['worker_id']} failed group {result['group_id']}: {result.get('error', 'Unknown error')}")
                
                # Progress update every 25 completions
                if completed % 25 == 0:
                    print(f"\n📊 Progress: {completed}/{len(groups_to_process)} completed ({successful} successful, {failed} failed)")
                    print("📈 Curl performance:")
                    for curl_name, stats in curl_stats.items():
                        total = stats["success"] + stats["failed"]
                        if total > 0:
                            success_rate = (stats["success"] / total) * 100
                            print(f"  {curl_name}: {stats['success']}/{total} ({success_rate:.1f}% success)")
                    print()
                
            except Exception as e:
                failed += 1
                curl_name = '_'.join(worker_id.split('_')[:-2])
                if curl_name in curl_stats:
                    curl_stats[curl_name]["failed"] += 1
                print(f"[{completed}/{len(groups_to_process)}] {worker_id} exception: {e}")
    
    print(f"\n✅ Parallel processing completed!")
    print(f"📊 Final stats: {completed} total, {successful} successful, {failed} failed")
    print("\n📈 Final curl performance:")
    for curl_name, stats in curl_stats.items():
        total = stats["success"] + stats["failed"]
        if total > 0:
            success_rate = (stats["success"] / total) * 100
            print(f"  {curl_name}: {stats['success']}/{total} ({success_rate:.1f}% success)")
    print(f"\n📁 Enriched data written to {OUTPUT_FILE}")
    print(f"🎉 cURL-based enrichment completed successfully!")

if __name__ == "__main__":
    main()
