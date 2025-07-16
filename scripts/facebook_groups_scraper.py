import requests
import json
import time
import os
import sys
import zstandard as zstd
import io
import random

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
COOKIE_FILE = os.path.join(PARENT_DIR, "settings", "cookie.json")
OUTPUT_FILE = os.path.join(PARENT_DIR, "output", "groups.jsonl")
STATE_FILE = os.path.join(PARENT_DIR, "output", "groups_state.json")

# Optional: Use Nimble proxy if NIMBLE_PROXY env var is set
NIMBLE_PROXY = os.getenv('NIMBLE_PROXY')  # e.g., "http://user:pass@gw.nimbleway.com:XXXX"
PROXIES = {"http": NIMBLE_PROXY, "https": NIMBLE_PROXY} if NIMBLE_PROXY else None

# --- CONFIGURATION ---
# NOTE: Move your cookie.json to /settings/cookie.json before running.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.5",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.facebook.com",
    "Referer": "https://www.facebook.com/groups/search/groups_home/?q=Dallas%2C%20TX",
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
    "x-fb-friendly-name": "SearchCometResultsPaginatedResultsQuery",  # <-- Update if needed
    "x-fb-lsd": "whxXrn___FJH5N9W4OZODD",  # <-- Update this value from your session
}
DOC_ID = "9960465747398298"  # <-- Updated to match user's session
FB_API_REQ_FRIENDLY_NAME = "SearchCometResultsPaginatedResultsQuery"  # <-- Update if needed
SEARCH_TEXT = os.getenv('SEARCH_TEXT', 'Dallas, TX')
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
{"allow_streaming":false,"args":{"callsite":"comet:groups_search","config":{"exact_match":false,"high_confidence_config":null,"intercept_config":null,"sts_disambiguation":null,"watch_config":null},"context":{"bsid":"97e85077-b9d2-4f94-9a3b-c1926420c4e5","tsid":null},"experience":{"client_defined_experiences":["ADS_PARALLEL_FETCH"],"encoded_server_defined_params":null,"fbid":null,"type":"GROUPS_TAB_GLOBAL"},"filters":[],"text":"Dallas, TX"},"count":5,"cursor":null,"feedLocation":"SEARCH","feedbackSource":23,"fetch_filters":true,"focusCommentID":null,"locale":null,"privacySelectorRenderLocation":"COMET_STREAM","renderLocation":"search_results_page","scale":1,"stream_initial_count":0,"useDefaultActor":false,"__relay_internal__pv__GHLShouldChangeAdIdFieldNamerelayprovider":false,"__relay_internal__pv__GHLShouldChangeSponsoredDataFieldNamerelayprovider":false,"__relay_internal__pv__IsWorkUserrelayprovider":false,"__relay_internal__pv__FBReels_deprecate_short_form_video_context_gkrelayprovider":true,"__relay_internal__pv__FeedDeepDiveTopicPillThreadViewEnabledrelayprovider":false,"__relay_internal__pv__CometImmersivePhotoCanUserDisable3DMotionrelayprovider":false,"__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider":false,"__relay_internal__pv__IsMergQAPollsrelayprovider":false,"__relay_internal__pv__FBReelsMediaFooter_comet_enable_reels_ads_gkrelayprovider":true,"__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider":false,"__relay_internal__pv__CometUFIShareActionMigrationrelayprovider":true,"__relay_internal__pv__CometUFI_dedicated_comment_routable_dialog_gkrelayprovider":false,"__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider":true,"__relay_internal__pv__FBReelsIFUTileContent_reelsIFUPlayOnHoverrelayprovider":true}
'''

# --- MAIN REQUEST FUNCTION ---
def fetch_page(cursor):
    if not COOKIES.get("c_user"):
        raise Exception("You must include your Facebook user ID as 'c_user' in the cookie file.")
    user_id = COOKIES.get("c_user")
    variables = json.loads(USER_VARIABLES_JSON)
    variables["cursor"] = cursor
    data = {
        "av": user_id,
        "__aaid": "0",
        "__user": user_id,
        "__a": "1",
        "__req": "9",
        "__hs": "20285.HYP:comet_pkg.2.1...0",
        "dpr": "1",
        "__ccg": "EXCELLENT",
        "__rev": "1024785322",
        "__s": "7h5qe5:gm7xfb:f8aoy1",
        "__hsi": "7527480471665268846",
        "__dyn": "7xeUjGU5a5Q1ryaxG4Vp41twpUnwgU29zEdE98K360CEboG0IE6u3y4o2Gwfi0LVE4W0qa321Rw8G11wBz81s8hwGxu782lwv89k2C0iK1awhUC7Udo5qfK0zEkxe2GewGwkUe9obrwh8lwuEjxuu3W3y261kx-0iu2-awLyES0gl08O321LwTwKG4UrwFg2fwxyo6J0qo5u1JwjHDzUiBG2OUqwjVqwLwHwea1wweW2K3a2q",
        "__csr": "gB3c8hQBE8OZEl5gxdbibnv6d9jd5idFikN7lpf9bl9jp5QXGybJBTIxuhkjmGl7eGiLGJ5BmiJmiN7y9W-4poSEyEOUCRiVXBDz8b8l-rxGE-5RAgG8xm5UaUWqeyUry8nwxxi8wCV8qwCxq1TxyEaECuUjwgo4C3W25a1NUpx21kgfUO4Uy4U6idzUgwwwNwRU4e2W15xO48qxqE2rwyw964poy4oeu2KbwKDwWwmU2lw5OG090zUN049c0uibw19S0vG0u2m1eyIy835w3Z8075N5hk00_5E15U1gkb83u0GE2Ow7gw0QDQ051Ekw1wS0mitm0byP011R0q606LUiwf20ky02qm4E0aGo0Cm046Q0qu065o0gsw4SK0kO0uW8m",
        "__hsdp": "gqMB2isG86VUgh0wky98PaVVHxe5SuCjSO9Cle6jel9288Q44i4QEsG8gIwJa13DDG88LxGhrXkUyEOAfx2lGdAmQHx3yER68-kLqbsiijhbAJAAmazGVzkHQa4Fa8GKQOy9JABjAraZjlkRlFbW58RfaxREp4aNtEaIV2FdAySA4t8wEx2i7Ozkp11aiaCy6eBGmdxuax67SEtyVGz8gBxuS8zGx2iAbByKunBDAVrzl8EG6oyWBQhqPwIGfiGaGaAJi124WoSAi68O44cx2ExAEqnyWymu2cwlxam5UG4UG2C5o9U8olwyyU7-0yF4exSQ12wgQ6UsUGufxIxp5gjDxC1-wAwBwaq2S9BHU188cE5G0si0n60TQqp3aoix-fweK3a78147apmaf9orweO1Hwpoeo2GwcS1xwn80Im04zA3u0nu0k60ME2gx28x61Rz82cwRw3Fo7S0ve08Jw3dE1H80hSw",
        "__hblp": "053wUw5uwGw2E9U30w4RwnESawh86K0MU7a0oq1Bw7gw2CE1LV80Eq7Q1zw13G0GE3dws84S0kO0ny0V834w8K09oxm0rm0iq0wEfU2Gw9i0z8doiwTw4kwaO0py1Zw7Pw2bo6mfxq0aFg1GU0Au3K0va0Xo880A5028E",
        "__sjsp": "gplvl8I99GHUrDx259opwGxCuCjD8CpkUpJe9gb42K7WwAwhVVULixWhoNm2S5CEScG589rVpoWewCwwwfqq585V12do5K58mxu0IrocbwgoqwgE5O7o4h0YxS2CeyEy2cwlw4FwvU2awOJ08p0UUGu2IwFk4U8o2zwcO2S9BHU188cE0V60TQqp3aoiwLweK0m1OClyzOm0gy062o0ieg1HU0Fm48qwto",
        "__comet_req": "15",
        "fb_dtsg": "NAfsLMAoygtUMjjy79WCqgvelx-UTNq-pgw9OCueXPBPXVa04ebBe9A:43:1750474603",
        "jazoest": "25567",
        "lsd": HEADERS["x-fb-lsd"],
        "__spin_r": "1024785322",
        "__spin_b": "trunk",
        "__spin_t": "1752628123",
        "__crn": "comet.fbweb.CometGroupsSearchRoute",
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": "SearchCometResultsPaginatedResultsQuery",
        "variables": json.dumps(variables),
        "server_timestamps": "true",
        "doc_id": DOC_ID,
    }
    print("POST data:", data)
    print("HEADERS:", HEADERS)
    print("COOKIES:", COOKIES)
    resp = requests.post(
        "https://www.facebook.com/api/graphql/",
        headers=HEADERS,
        cookies=COOKIES,
        data=data,
        proxies=PROXIES,  # <-- Use Nimble proxy if set
    )
    encoding = resp.headers.get("Content-Encoding")
    print("Content-Encoding:", encoding)
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
    while True:
        print(f"Fetching page with cursor: {cursor}")
        try:
            response = fetch_page(cursor)
        except Exception as e:
            print("Request failed:", e)
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
            break

        cursor = next_cursor
        # Randomized delay to reduce rate limiting risk
        sleep_time = random.uniform(*SLEEP_BETWEEN_REQUESTS)
        print(f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time) 