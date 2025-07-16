import json
import time
import random
import requests
import os
import sys

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
INPUT_FILE = os.path.join(PARENT_DIR, "output", "groups.jsonl")
OUTPUT_FILE = os.path.join(PARENT_DIR, "output", "groups_enriched.jsonl")
COOKIE_FILE = os.path.join(PARENT_DIR, "settings", "cookie.json")
HOVERCARD_DOC_ID = "24553182484278857"

# You must update this with the correct headers and POST fields from your browser session for the hovercard call
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
    "X-FB-Friendly-Name": "CometHovercardQueryRendererQuery",
    "x-fb-lsd": "84EMFCpbSO1IO3zao5ViuJ",  # <-- update this each session
    "x-asbd-id": "359341",
}

SLEEP_BETWEEN_REQUESTS = (1.5, 4.0)

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

# --- Load cookies ---
def load_cookies():
    with open(COOKIE_FILE, "r", encoding="utf-8") as f:
        cookies = json.load(f)
    return cookies

COOKIES = load_cookies()

# --- Hovercard Query ---
def make_hovercard_variables(group_id):
    return json.dumps({
        "actionBarRenderLocation": "WWW_COMET_HOVERCARD",
        "context": "DEFAULT",
        "entityID": group_id,
        "scale": 1,
        "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False
    })

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

# --- Main enrichment loop ---
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file {INPUT_FILE} not found.")
        return
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        groups = [json.loads(line) for line in f if line.strip()]
    enriched_ids = load_enriched_ids()
    print(f"Skipping {len(enriched_ids)} groups already enriched.")
    session = requests.Session()
    session.cookies.update(COOKIES)
    enriched = []
    for idx, group in enumerate(groups):
        if group["id"] in enriched_ids:
            continue
        group_id = group["id"]
        variables = make_hovercard_variables(group_id)
        data = {
            "av": COOKIES.get("c_user"),
            "__aaid": "0",
            "__user": COOKIES.get("c_user"),
            "__a": "1",
            "__req": "1q",  # <-- update per session if needed
            "__hs": "20285.HYP:comet_pkg.2.1...0",
            "dpr": "1",
            "__ccg": "EXCELLENT",
            "__rev": "1024785322",
            "__s": "19rbhi:cf27t6:tmmyxk",  # <-- update per session
            "__hsi": "7527498299079297119",  # <-- update per session
            "__dyn": "7xeUjGU5a5Q1ryaxG4Vp41twWwIxu13wFwkUKewSwAyUco2qwJyE2OwpUe8hwaG0Z82_CxS320qa321Rwwwqo462mcw5Mx62G5Usw9m1YwBgK7o6C0Mo4G17yovwRwlE-U2exi4UaEW2G1jwUBwJK14xm1Wxfxmu3W3y261eBx_wHwfC2-awLyESE2KwkQ0z8c86-bwHwKG4UrwFg2fwxyo6J0qo4e4UO2m3G1eKufxamEbbxG1fBG2-2K0E8461wweW2K3abxG",  # <-- update per session
            "__csr": "gB1P4gBbNY55N4Yp8xauCzlf6Olsj3b6nOHYCIGcLWky9ncojF9OlQG9vEAZYhqWaRlFai-IyBGFKAGi_VXlbAHuVaX-AQqXC-EC9XB_jVGGHCGfgmmh5AKiaAAgCdyUvzFJ5mAaDy8G8x6m8xymeAKm23WxmiU8Ghbyuq2OubwDByUOEaEx7K4Uhxa78gzEaoboLG58Na2m326Kcz9VqwLwAmbwNz44Ey4U9VUd8Sq8gZe6U98O3nwLxe2Wayoc8sx26EmG1zwTwywMwLwMx6m8x6i3jxWcyUbFUa8hAwh8hw9m1Twg82VG08CgozUN0uU2dc0uibwai0VE28wd62Ne0ju4oGWa2i5oB0lE1wEdoBie1fw56BwhQmaO8wcm3G0eVw0nI8co0Ge0tl5hk025Hg03qUwc4Wxi6CEnCw4WgIwdawaG0IE1Q814qiyEIk02tC640vHg0k6gzy80odw5ADlw2UIM3dyE0Qh0q60hLw1lW4EbUiw9R1u3C0ky09Rg0h5w2W8iw0BVK0iK09Bw2zooy5o1pA0qu3O05S80-wwc81dHw5cw7Ky5w",  # <-- update per session
            "__hsdp": "gbqsG86VUgh0xEG9azaAuqUjxsxG8x4OpzkhZi4sWLih4AikwR5gSAi4QFFEWExyy5i8xYaeMUnGGAIJWAacV5LikVaucF5yoyulGUx4mTKEFuyt8Jd8gBBbSyT5ejhahbp95yEWKWdiLpa6nhWHJcsHajJGhEQdjFbR5hj3q6hexBEb8gOF8HCAqgSZO4qagAFIFKhIG2B4jhExKahaHt5LmuXS8jBpoxck8qpF4eCjCGcF8xALc4a88GiDmVkmiAbiUH8j_qjFdrXDBkBESi4Q8IynjvkUBVGW45QKdayCF24iD8p6UCQiCdEVWgBgEp8Nby9WGlAEqnyWymp2h0i8qhaGzWh8nyFoSag8pEQicBP2Gg88lwxpWwKx2pAaS7WqwMwmroW7rg4a13grzETy6XzUBcFOI8CKVVpoTyobEvAxGVouyZ7zCm64bBcAgNla5obUOWxmEJKNDw4doZ0OwhUiF8E2-yoiwk82bg3Ew8y2QMc8vglwh4qWhB5167UZ0du4k2pyoswjAdwasxtOClyzOm6Ukx-0JU6K1BwVwaG0G898661swpo27Cw4Lxa1gwYxKh040w8i0IE1YE410Axa0nvxS0i61-wgU2gx28x6awqEO0z8do0wS0pu1Zw7Pw9i11w5mwi8ow2R81H83uw6ow5gwaS0VE1oE0Cu0lO17w",  # <-- update per session
            "__hblp": "04sxKawUwoU2dxudG13wGy8nw2vVU6u223u1-wuEeU4y5oSmiu2OdUaefyU9833wsE1xE6m0T86i0zE4N0naw5awQwrawcq6Q1gw8uQ0qe0UEvg-1cCwb-2S2q581b8jxa0AU1a82GwcS1Mwjo1ibwp83mwYw9W15wWw8vyo21wADwdm0lm5o-0Po3nw4Cw8a3-0GE2kw8O3m4EdU1585q7EdA3O0yE3fwSwhEb8dE8E621fwa26E5q3S11w8q0PU4y6bzU-Hw9-15wOwRw5poeo1iU9o1ro3pwXwb-0jaUc84y1ECwww4dz8vgvwae11g1nV9o5G1sx6689827gyaw",  # <-- update per session
            "__sjsp": "gbqsG86VUgh0xF5giKuqUjxsxG8x4OpzkhZi4sWLih4AikwR5ggh8jgC5qyEExkCgt5N3Ie5AGEwKqgwTAmZ5c9xa48ydqzpXuWyBW9Xbji8WmlbzfpHzUF2EuVEGZxaUiJd2VU2fA98iewh8jLp96Ad888vyA4rHBjDyE-iml2uE-E522ifi8qsMgEwyuCKVkaKbx38cjAGh2VVkKA3eGz8jG6ZafSyyoCmAEB6UCi6z2Q9kOOWCigO8DG8xLyWymp2h0i8biBzWgboS0SobE520yEcHg26gee8rK2EHkpCKUjzu1twgULg6d0mbG2qX6u0gRzQ3a0iW0tC0y86e3q14hHF6kk4obU3ox50Cg1e7apmaf9o520JU0WC0xVE1bUiw8S6V40g20cGwg40q_xS0q20R8gxGawqE0eS80KC",  # <-- update per session
            "__comet_req": "15",
            "fb_dtsg": "NAftck539DhlFUznehfoGAYC5PKDIIPh_3fX30jwthhEmP-sXtj9LcA:43:1750474603",  # <-- update per session
            "jazoest": "25456",  # <-- update per session
            "lsd": "84EMFCpbSO1IO3zao5ViuJ",  # <-- update per session
            "__spin_r": "1024785322",
            "__spin_b": "trunk",
            "__spin_t": "1752632274",  # <-- update per session
            "__crn": "comet.fbweb.CometGroupsSearchRoute",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "CometHovercardQueryRendererQuery",
            "variables": variables,
            "server_timestamps": "true",
            "doc_id": HOVERCARD_DOC_ID,
        }
        try:
            resp = session.post(
                "https://www.facebook.com/api/graphql/",
                headers=HEADERS,
                data=data,
                proxies=PROXIES,  # <-- Use Nimble proxy if set
                timeout=30,
            )
            try:
                result = resp.json()
            except Exception:
                print(f"Non-JSON response for group {group_id}, stopping script.")
                print(f"HTTP status: {resp.status_code}")
                print("First 500 bytes of response:")
                print(resp.content[:500])
                print("\n--- END OF RESPONSE ---\n")
                print("Check your doc_id, variables, headers, and session/cookies. Script will now exit to avoid fingerprinting.")
                return
            hovercard = extract_hovercard_fields(result)
            group.update(hovercard)
            enriched.append(group)
            with open(OUTPUT_FILE, "a", encoding="utf-8") as out:
                out.write(json.dumps(group, ensure_ascii=False) + "\n")
            print(f"[{idx+1}/{len(groups)}] Enriched group {group_id}: {group.get('name')}")
        except Exception as e:
            print(f"Error processing group {group_id}: {e}")
        time.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS))
    print(f"Done. Enriched data written to {OUTPUT_FILE}")

def load_enriched_ids():
    enriched_ids = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    group = json.loads(line)
                    if "id" in group:
                        enriched_ids.add(group["id"])
                except Exception:
                    continue
    return enriched_ids

if __name__ == "__main__":
    main() 