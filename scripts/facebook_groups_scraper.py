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

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
COOKIE_FILE = os.path.join(PARENT_DIR, "settings", "cookie.json")
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
RATE_LIMIT_COOLDOWN = 300  # 5 minutes cooldown
LAST_RATE_LIMIT_TIME = 0

def is_rate_limited(response_text, status_code):
    """Detect if we're being rate limited by Facebook"""
    global RATE_LIMIT_DETECTED, LAST_RATE_LIMIT_TIME
    
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
    """Handle rate limiting by saving progress and waiting"""
    global RATE_LIMIT_DETECTED
    
    print(f"\n🚨 RATE LIMITING DETECTED!")
    print(f"⏸️  Pausing for {RATE_LIMIT_COOLDOWN} seconds...")
    print(f"💾 Saving progress...")
    
    # Save current progress
    save_all_progress()
    
    # Wait for cooldown
    time.sleep(RATE_LIMIT_COOLDOWN)
    
    print(f"🔄 Resuming after rate limit cooldown...")
    RATE_LIMIT_DETECTED = False

def save_all_progress():
    """Save all progress data"""
    # Save URL progress
    if 'progress' in globals():
        save_url_progress(progress)
    
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
print("Choose proxy mode: [1] Nimbleway proxy [2] Proxyless")
mode = input("Enter 1 or 2 (default 1): ").strip()
if mode == "2":
    PROXIES = None
    print("Running proxyless (direct connection)...")
else:
    # Nimble Proxy Integration
    NIMBLE_SETTINGS_FILE = os.path.join(PARENT_DIR, "settings", "nimble_settings.json")
    if os.path.exists(NIMBLE_SETTINGS_FILE):
        with open(NIMBLE_SETTINGS_FILE, "r", encoding="utf-8") as f:
            nimble_settings = json.load(f)
        NIMBLE_USERNAME = nimble_settings.get("username")
        NIMBLE_PASSWORD = nimble_settings.get("password")
        NIMBLE_HOST = nimble_settings.get("host", "ip.nimbleway.com")
        NIMBLE_PORT = nimble_settings.get("port", "7000")
        NIMBLE_PROXY = f"http://{NIMBLE_USERNAME}:{NIMBLE_PASSWORD}@{NIMBLE_HOST}:{NIMBLE_PORT}"
        PROXIES = {"http": NIMBLE_PROXY, "https": NIMBLE_PROXY}
        print(f"Using Nimbleway proxy: {NIMBLE_PROXY}")
    else:
        PROXIES = None
        print("Nimble settings not found, running proxyless.")

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
SLEEP_BETWEEN_REQUESTS = (1.5, 4.0)  # seconds, randomized range

# --- LOAD COOKIES ---
def load_cookies():
    if not os.path.exists(COOKIE_FILE):
        raise Exception(f"Cookie file '{COOKIE_FILE}' not found. Please create it with your Facebook cookies.")
    with open(COOKIE_FILE, "r") as f:
        cookies = json.load(f)
    return cookies

COOKIES = load_cookies()

# --- LOAD STATE ---
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
    cursor = state.get("cursor")
    seen_ids = set(state.get("seen_ids", []))
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
    if not COOKIES.get("c_user"):
        raise Exception("You must include your Facebook user ID as 'c_user' in the cookie file.")
    user_id = COOKIES.get("c_user")
    variables = json.loads(USER_VARIABLES_JSON)
    variables["cursor"] = cursor
    variables["args"]["text"] = search_term  # Update search term dynamically
    
    data = {
        "av": user_id,
        "__aaid": "0",
        "__user": user_id,
        "__a": "1",
        "__req": "3",  # Updated from browser
        "__hs": "20302.HYP:comet_pkg.2.1...0",  # Updated from browser
        "dpr": "1",
        "__ccg": "EXCELLENT",
        "__rev": "1025431519",  # Updated from browser
        "__s": "ndklix:17f7hn:b6rwhq",  # Updated from browser
        "__hsi": "7533777975579479818",  # Updated from browser
        "__dyn": "7xe5WK1ixt0mUyEqxemh0no6u5U4e0yoW3q2ibwNwcyawba1DwUx60GE3Qwb-q1ew6ywMwto2awgo9oO0n24oaEnxO0Bo7O2l0Fw4Hw9O7Udo6qU2exi4UaEW2G0AEbrwh8lwuEjxu16wUwxwt819UbUG2-azo11k0z8c86-3u2WEjwhk0zU8oC1Iwqo5u1JwjHDzUiBG2OUqwjVqwLwHwea1wweW2K3a2q6E",  # Updated from browser
        "__csr": "gR5sBNk8PRFH92ZJjtPiOiF8BnFPldTcPb-J9pKT9RTGIQLmi8JkGLKWVvuDZqOuFeoySUDGOahbCTWkNGyeJp7mECi9y2aaA-8Vd6xWiaKuVV9EjyHyV8hKaK9Axqim4WCzU465U8ErDx2EsxLy4mq17wh8pxi1fwm8nx28zUGU4u2WfUdk1aw822y9wQx63u3y2u1ey89QudDwwwkESUnxHw8R2Ed89UeEc82TwQxm2a2O2e8K5V82pw2e8cQ08Hw76gO0geaU9UjwBw8qawSwXw428fG2-08swnU27ixagyy8GU5mdqx5poO3V3o0KO6o1HE8U01u2o0hXo13Ejw4fw1nZ160jq8yk14wdq04s81cojw7Pg0cozw0h9Hxl07Mw2w8040e02Xu4U",  # Updated from browser
        "__hsdp": "hOi22wzz91e46p78F4oFrV94jGhFEwL4GCh5HieKUGqQXBjAkECqAt2lBixi437AUrCp4nCz4aqDhe9GpaExvwyyrG8lppet1q8DymEB0Ky9FaV8BcChfF2UWtDla4-B6aj49LzkB9AfiWiaQZOZjRd8jHEnr4WhNq8z_knbegUiA_csGAoWCmB89i8GgGeyOaUBai7ESp6BocrGaGEa-czozAAK8xMMF2m9yUgCCJAVGDhVk9ABBjkQWyogBxq28ya_BwAAwlohxHzoW2aV8CnxswGWx28zA69UrxiawKjHULo-EpGEy323m3h3ax61TCxS2q1VwrUCUigOfKepAVd1Sbxe4Bg3cwiEhg3IwkE2rw6rw5XwxGVEowtUbU_Vm3K2uEbFU12EcU78on704GwkUqwnqw6Xwo81rU0lbwaWfw2p83Vw2nE0yi08QwUw2L832w17W0dXw4Yw2bE7q0cjw",  # Updated from browser
        "__hblp": "09a0ri0ki0ki2K1nw5jwto2Kw5Uwm81yU0Hy1ax60k20hW0hy0nF09a04q87y0ke0SUlw4gwi80lbxO1vwgo25wCw6OwfW0h60kO0li0PU0zi3y08wwoGwRwca0ll039U4u4U6y0s60Z85G0Uo0yW0l-7o39w52w",  # Updated from browser
        "__sjsp": "hOi22wzz91e46p78F4rprV94iihFJyYiGChqQzHKayVVkUK9CF7gCEC58gx67oR3EpiDBUOpabCwIxmmjy8cp8B0-xC4KEc8bo9A0CbzE3uw8S2LxG58ek3aq4ocE8UC1qBDwFw8a11y8CnxswhwEwkEfE2VwQg-4o33wuo7S4ESfKepAVA7oK2vg1aE3IweW0cVGVEowtUbU_Vm3K1pDw4-wsxxss0iG02qy0czw0jpE0-a",  # Updated from browser
        "__comet_req": "15",
        "fb_dtsg": "NAftR06AOPsyS-g4udRZQvZgBkssjaQdvXH7HH9MHNVmyuBi750iQvA:25:1754053582",  # Updated from browser
        "jazoest": "25485",  # Updated from browser
        "lsd": HEADERS["x-fb-lsd"],
        "__spin_r": "1025431519",  # Updated from browser
        "__spin_b": "trunk",
        "__spin_t": "1754094375",  # Updated from browser
        "__crn": "comet.fbweb.CometGroupsSearchRoute",
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": "SearchCometResultsPaginatedResultsQuery",
        "variables": json.dumps(variables),
        "server_timestamps": "true",
        "doc_id": "24106293015687348",  # Updated from browser
    }
    
    # Update Referer header for current search term
    headers = HEADERS.copy()
    headers["Referer"] = f"https://www.facebook.com/groups/search/groups_home/?q={urllib.parse.quote(search_term)}"
    
    print("POST data:", data)
    print("HEADERS:", headers)
    print("COOKIES:", COOKIES)
    resp = requests.post(
        "https://www.facebook.com/api/graphql/",
        headers=headers,
        cookies=COOKIES,
        data=data,
        proxies=PROXIES,  # <-- Use Nimble proxy if set
    )
    encoding = resp.headers.get("Content-Encoding")
    print("Content-Encoding:", encoding)
    
    # Check for rate limiting
    response_text = resp.text
    if is_rate_limited(response_text, resp.status_code):
        handle_rate_limiting()
        return None
    
    try:
        # Try to parse as JSON first (requests may have already decompressed)
        return resp.json()
    except Exception as e:
        # If JSON parsing fails and encoding is zstd, try manual decompression
        if encoding == "zstd":
            print("First 100 bytes of raw response:", resp.content[:100])
            try:
                dctx = zstd.ZstdDecompressor()
                decompressed = dctx.stream_reader(io.BytesIO(resp.content))
                text = decompressed.read().decode("utf-8")
                return json.loads(text)
            except Exception as zstd_e:
                print("Zstd decompression also failed:", zstd_e)
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

def extract_groups(response):
    try:
        edges = response["data"]["serpResponse"]["results"]["edges"]
        for edge in edges:
            node = edge.get("rendering_strategy", {}).get("view_model", {}).get("profile", {})
            if node.get("__typename") == "Group":
                yield {
                    "id": node.get("id"),
                    "name": node.get("name"),
                    "url": node.get("url") or node.get("profile_url"),
                }
    except Exception as e:
        print("Error extracting groups:", e)
        return []

def get_next_cursor(response):
    try:
        return response["data"]["serpResponse"]["results"]["page_info"]["end_cursor"]
    except Exception:
        return None

# --- MAIN LOOP ---
if __name__ == "__main__":
    print(f"Starting Facebook Groups Scraper - Target: {TARGET_CALLS} GraphQL calls")
    print(f"Start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    # Load URLs and progress
    urls = load_search_urls_from_file()
    progress = load_url_progress()
    
    if not urls:
        print("❌ No URLs found. Using hardcoded search term.")
        search_terms = ["arlington tx"]  # Fallback
    else:
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
    print(f"📊 Progress: {progress['current_url_index']}/{len(search_terms)} URLs processed")
    
    # Start from where we left off
    current_index = progress['current_url_index']
    
    while current_index < len(search_terms) and GRAPHQL_CALL_COUNT < TARGET_CALLS:
        search_term = search_terms[current_index]
        print(f"\n🔍 Processing search term {current_index + 1}/{len(search_terms)}: {search_term}")
        
        # Reset cursor for new search term
        cursor = None
        search_completed = False
        
        # Process this search term until no more results or target reached
        while not search_completed and GRAPHQL_CALL_COUNT < TARGET_CALLS:
            print(f"Fetching page with cursor: {cursor}")
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
                print("Request failed:", e)
                search_completed = True
                break

            new_groups = 0
            for group in extract_groups(response):
                if group["id"] not in seen_ids:
                    append_group(group)
                    new_groups += 1

            # Save state immediately after processing the page
            save_state(cursor, seen_ids)

            print(f"Added {new_groups} new groups. Total: {len(seen_ids)}")

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
        
        # Mark this URL as processed and save progress
        progress['processed_urls'].append(search_term)
        progress['current_url_index'] = current_index + 1
        save_url_progress(progress)
        
        current_index += 1
        
        # Save progress every 10 search terms
        if current_index % 10 == 0:
            print(f"💾 Saving progress checkpoint...")
            save_all_progress()
        
        if GRAPHQL_CALL_COUNT >= TARGET_CALLS:
            break
    
    print(f"\n🎉 Scraping completed!")
    print(f"Processed {len(progress['processed_urls'])} URLs")
    print(f"Total groups found: {len(seen_ids)}")
    print(f"GraphQL calls made: {GRAPHQL_CALL_COUNT}")
    
    # Final progress save
    save_all_progress() 