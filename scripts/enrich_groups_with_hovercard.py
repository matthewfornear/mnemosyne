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

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
INPUT_FILE = os.path.join(PARENT_DIR, "output", "groups.jsonl")
OUTPUT_FILE = os.path.join(PARENT_DIR, "output", "groups_enriched.jsonl")
COOKIE_FILE = os.path.join(PARENT_DIR, "settings", "cookie.json")
HOVERCARD_DOC_ID = "30440693392243377"  # <-- updated from new session

# You must update this with the correct headers and POST fields from your browser session for the hovercard call
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.7",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.facebook.com",
    "Referer": "https://www.facebook.com/groups/search/groups_home/?q=test",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "X-FB-Friendly-Name": "CometHovercardQueryRendererQuery",
    "x-fb-lsd": "IvdgU3Ww4tkD7REF0TTmQj",  # <-- updated from new session
    "x-asbd-id": "359341",
}

SLEEP_BETWEEN_REQUESTS = (0.0, 5.0)  # 0-5 second random wait

# --- Parallel Processing Configuration ---
NUM_WORKERS = 4

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

# --- Load cookies ---
def load_cookies():
    with open(COOKIE_FILE, "r", encoding="utf-8") as f:
        all_data = json.load(f)
    
    # Only return actual cookie fields (exclude session_headers and session_payload)
    cookies = {}
    for key, value in all_data.items():
        if key not in ["session_headers", "session_payload"] and isinstance(value, str):
            cookies[key] = value
    
    return cookies

COOKIES = load_cookies()

# --- Hovercard Query ---
def make_hovercard_variables(group_id):
    variables = {
        "actionBarRenderLocation": "WWW_COMET_HOVERCARD",
        "context": "DEFAULT",
        "entityID": group_id,
        "scale": "1",  # Convert to string
        "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False
    }
    # Use ensure_ascii=False and separators to match JavaScript JSON format
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
def process_group_worker(group_data, worker_id, cookies, proxies, headers, doc_id):
    """Worker function to process a single group with hovercard enrichment"""
    try:
        group = group_data['group']
        group_id = str(group["id"])
        
        # Create session for this worker
        session = requests.Session()
        session.cookies.update(cookies)
        
        # Make hovercard request
        variables = make_hovercard_variables(group_id)
        data = {
            "av": cookies.get("c_user"),
            "__aaid": "0",
            "__user": cookies.get("c_user"),
            "__a": "1",
            "__req": "32",
            "__hs": "20303.HYP:comet_pkg.2.1...0",
            "dpr": "1",
            "__ccg": "EXCELLENT",
            "__rev": "1025464452",
            "__s": "prnyzx:10euyl:841441",
            "__hsi": "7534420000307952224",
            "__dyn": "7xeUjGU9k9wxxt0koC8G6Ejh941twWwIxu13wFw_DyUJ3odF8vyUco2qwJyEiwsobo6u3y4o27wywfi0LVEtwMw6ywMwto88422y11wBz822wtU4a3a4oaEnxO0Bo4O2-2l2UtwxwhU31wiE4u9x-3m1mzXw8W58jwGzEjxq1jxS6Fobrwh8lwUwOzEjUlDw-wUwxwhFVovUaU3VwLyEbUGdG1QwVwwwOg2cwMwhA4UjyUaUbGxe6Uak0zU8oC1hxB0qo4e4UO2m3zwxK2yVU-4FqwIK6E4-mEbUaU2wwgo620XEaUcEK6Eqw",
            "__csr": "g4f2QaY4ZT97Ni2y2iYuwLklPIL2cImxQr5h7sQhvjd7POihRsBn8lLvqilHl8LnvFElnkOiQAijDHPXjTf_v9jnlqfiRp8BDHB_EzV26J-OdajjykSykQGN9aiiqdR8BK8uuSAi-QABDhSAADCAqVSgygyEOmmGjGilmJ2JkDK5aGFFbAGahQ-F5mhaiGye9B-J29Z2QQdJa5lAKGiiBiG9yFuUPKAGheh5XhZ6iAGEgF16eByoRKh2pU-H-nxfFioW8hUKBh4VqVbVoKaQ9FaXBmiECeGHgCaoSiEWVHKqii5EmhUBUjDBGAeFp98y5UWaHiDyaAKqmHgZ1DGmBhovBAVUO9yK8Gtx3BXzFp8G7WCgCdGFkWQhe8gsGq8XVpaByei9wIAx2q5FajykK9Qbzqy8iDwBG5WHwKK2OrAh8OLDAzulz998nyVul125FA5e2fyF8CES4EmKuhopxy7AbDAyAbwzVElAxmGx10WxvxfGd_Ax-V4Ugxu4FXxi4pe4UC9Ah9oO9HzEC2K0XEnDJ5nwXG4WwSg8oy3u2258vzki0xueByEiwxz42mq2HUybJ2k9UsgbEWiEO1hxWah6Q5EbUkw8Ex9UkQ2p5UF28GQfypS6o8Q8y8bonho6l0Pxu2d3m1Xy9Edm2u22Eb8aUC68O9wYx2uexK8Kjwu8W1gCBmA9xycpA15yk5ocF8khF4UyU-9wKxe4Upw0y3w9e0IE3btWyl5N81DEcU1SFC05KWRy83Xwd9GEnBfoyawnag0iyw42UaU5uJCHgOm1Ug2vg0XC18h83Og6WFU5C0hS220nO880bHw0qANd2kcyo3Cg095m0U2y40yo087U0iEwr8CgK4RKaz83wwaG3YErwOwDwfTwFa0So7a0DWxl1wR7oC0bkwaS08nw4Vxmexa9wam1qQ088RxDxHwPxajw9J02b4781X20Ew2qA13wabz83uwGwEwda3mp1x0b52o5B0ADiO5xh0KwJw2aC6bCxG8zZx2cwfm2a0-kjK7o1fRqDgqwNDg2cw8N02dmElgpe9g3bwdOcwnU6u1uy83kIM0Me9y82sw4M77zrc0280w0EW0PO073whqyECmA04FV2w2lU3rgzxtwqo8E7uaz81288E1nEfo0sdwdu0SEG9FFU2XK0DUG0aAFw4rFu1IBK9yo0Q20-8AwoxGU4u2G6o",
            "__hsdp": "g4de82hx91z48fJF8GA3AyiitE98oQwwwQsGugG39kWismcejiOH8B8xSzFd4cACn4p2Sgy4d8YSRihRaj9_NmPeuiidxyVyzaV4qEFObYFW8i68b8x6hkEa4iVLzEMwhyGQm54p2AqV4_Q4EGrGm79pH8q-9GvBhAO1F8woQexmroG8G6k6A44QK8HyA6aplF4h4uJkFC8i8jWVozeXUW49bZlEwgQ28PmWIFUunHx4whzVCbz925gQwB3BhCy7BCG8hSaFKugsEnoSbwgppqolyk44ngy6kVF94UW6-UB4x5i0YwRgaAcglDw8G3J1RzEd9QmVswyqgMwIzwMgc9ryXJ0CmED56rCF0ibwIQt09y1koOay9o-2u3G1owa20v90Xxm0Zv6wnA3G1DGx4w4x2m6U4u3kMpEl50m821818wai1qw7Fw7Ng1Xo1iO04PwnE0Qu0XU4W19wa6485efw-wi8",
            "__hblp": "0uUB28iHwkoeENxm1gwYxC5UbUx1y1Kxi1DzodE9UfGxi22E2Xxi5k1PDwhU6aUnxe0y9pojx-58SfwSwTxqUW8g-2yu2i1ZxS48524VQq4XwQCDwAjyonCG17xbyFVU2EgbEcp82jxW4ocrg9awYxu2a3arKdx69w-Cx22CcxK8zEaFEv8axC1swlHwHyU4i0y8O1Xg560zoS2W3S2m2G1XzoeV8eaxq0M8K6F82BBwQxqEe8txq2PocUoyoswnUc8kxW3Wi1SBwACwBAwWyXxS2O2ybx66E2zwHw_wUy8rxK2-2edCwJJ0rEG2W48y6Wxy3668a8G6E6W1sDyEqye2udwQhU723S2q5kE8o72m2edwlEaEswwxi68G1uwTwEw-wPy9EXwxz8rx-2abg66cgdU5y2S49awoUfofUvw-gpzoO2idxm13wzwoVbyES3y2K7EC8xmdwTzUhyp8gwGJ1edwGwpoaUlwcqcyocoaUd8d8sCwIDwFgS9VpXwzU8u5Vo-68a8S5oO7omw-wGwPxe3W3Z16i6oW4UepEO2e3S362q3Xx22SfJ2UpzA8CwFK48aUK48b-ay98xCyF8S3CfwIxW4oC22fx23a7UaE4O4Uco8U4u262SfGi6UKbGUZa48Wl0WVKaxebwood9azUSm4U98O4ooxq12Gi9gpCwr20QCwde5GwooWeBwYwzypUC15wMwNQ16yoO0Tum0DFE98kKeg9oV5K2C9zuF84WnghDAADz9E4W5A5Uix14xaFFHAw9-3i22cwlUy7ahIwbaAxei2mt0Ly9859J0",
            "__sjsp": "g4de82hx91z48fJF8GA3AyiitE98oQwwwQsGugG39kWismcejiOH8B8xSzFd4cACn4p2Sgy4d8YSRihRaj9_NmPeuiidxyVyzaV4qEFObYFW8i68b8x6hkEa4iVLzEMwhyGQm54p2AqV4_Q4EGrGm79pH8q-9GvBhAO1F8woQexmroG8G6k6A44QK8HyA6aplF4h4uJkFC8i8jWVozeXUW49bZlEwgQ28PmWIFUunHx4whzVCbz925gQwB3BhCy7BCG8hSaFKugsEnoSbwgppqolyk44ngy6kVF94UW6-UB4x5i0YwRgaAcglDw8G3J1RzEd9QmVswyqgMwIzwMgc9ryXJ0CmED56rCF0ibwIQt09y1koOay9o-2u3G1owa20v90Xxm0Zv6wnA3G1DGx4w4x2m6U4u3kMpEl50m821818wai1qw7Fw7Ng1Xo1iO04PwnE0Qu0XU4W19wa6485efw-wi8",
            "__comet_req": "15",
            "fb_dtsg": "NAfu4uUDCagfwqo-di8Ec1NjODkdp8uQlgl4AuYnp8MjibylvVCzCqw:10:1754243856",
            "jazoest": "25788",
            "lsd": "IvdgU3Ww4tkD7REF0TTmQj",
            "__spin_r": "1025464452",
            "__spin_b": "trunk",
            "__spin_t": "1754243858",
            "__crn": "comet.fbweb.CometGroupsSearchRoute",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "CometHovercardQueryRendererQuery",
            "variables": variables,
            "server_timestamps": "true",
            "doc_id": doc_id,
        }
        
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
            return {"success": False, "group_id": group_id, "error": "Non-JSON response"}
        
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
        return {"success": False, "group_id": group_id, "error": str(e), "worker_id": worker_id}

# --- Main enrichment loop ---
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found.")
        return
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        groups = [json.loads(line) for line in f if line.strip()]
    
    # Filter groups to only those with city_state
    groups_with_city_state = [group for group in groups if "city_state" in group]
    print(f"Found {len(groups_with_city_state)} groups with city_state out of {len(groups)} total groups")
    
    enriched_ids = load_enriched_ids()
    print(f"Skipping {len(enriched_ids)} groups already enriched.")
    
    # Filter out already enriched groups
    groups_to_process = [group for group in groups_with_city_state if str(group["id"]) not in enriched_ids]
    print(f"Processing {len(groups_to_process)} groups with city_state that need enrichment.")
    
    if not groups_to_process:
        print("No groups to process. All groups are already enriched.")
        return
    
    # Prepare shared data for workers
    cookies = COOKIES
    proxies = PROXIES
    headers = HEADERS
    doc_id = HOVERCARD_DOC_ID
    
    # Create output file lock for thread-safe writing
    output_lock = Lock()
    
    # Process groups in parallel
    print(f"🚀 Starting parallel processing with {NUM_WORKERS} workers...")
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all tasks
        future_to_group = {}
        for idx, group in enumerate(groups_to_process):
            worker_id = idx % NUM_WORKERS
            future = executor.submit(
                process_group_worker, 
                {"group": group, "index": idx}, 
                worker_id, 
                cookies, 
                proxies, 
                headers, 
                doc_id
            )
            future_to_group[future] = {"group": group, "index": idx, "worker_id": worker_id}
        
        # Process completed tasks
        completed = 0
        successful = 0
        failed = 0
        
        for future in as_completed(future_to_group):
            group_info = future_to_group[future]
            group = group_info["group"]
            idx = group_info["index"]
            worker_id = group_info["worker_id"]
            
            try:
                result = future.result()
                completed += 1
                
                if result["success"]:
                    successful += 1
                    enriched_group = result["enriched_group"]
                    
                    # Thread-safe writing to output file
                    with output_lock:
                        with open(OUTPUT_FILE, "a", encoding="utf-8") as out:
                            out.write(json.dumps(enriched_group, ensure_ascii=False) + "\n")
                    
                    print(f"[{completed}/{len(groups_to_process)}] Worker {worker_id} enriched group {result['group_id']}: {enriched_group.get('name')} (city_state: {group.get('city_state')})")
                else:
                    failed += 1
                    print(f"[{completed}/{len(groups_to_process)}] Worker {worker_id} failed group {result['group_id']}: {result.get('error', 'Unknown error')}")
                
                # Progress update every 100 completions
                if completed % 100 == 0:
                    print(f"📊 Progress: {completed}/{len(groups_to_process)} completed ({successful} successful, {failed} failed)")
                
            except Exception as e:
                failed += 1
                print(f"[{completed}/{len(groups_to_process)}] Worker {worker_id} exception: {e}")
    
    print(f"✅ Parallel processing completed!")
    print(f"📊 Final stats: {completed} total, {successful} successful, {failed} failed")
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