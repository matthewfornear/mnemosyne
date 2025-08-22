#!/usr/bin/env python3
"""
Debug script to test individual Facebook sessions and identify why they're failing
"""

import json
import os
import sys
import requests
import urllib.parse

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths
COOKIES_DIR = os.path.join(PARENT_DIR, "settings", "cookies")
HOVERCARD_DOC_ID = "24093351840274783"

def load_cookie_file(file_path):
    """Load a single cookie file"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Failed to load {file_path}: {e}")
        return None

def test_session_detailed(session_data, session_name):
    """Test a session with detailed debugging"""
    print(f"\n🧪 Testing session: {session_name}")
    print("=" * 60)
    
    # Show session info
    doc_id = session_data["payload"].get("doc_id", "Not found")
    has_lsd = "x-fb-lsd" in session_data["headers"] and session_data["headers"]["x-fb-lsd"]
    user_agent = session_data["headers"].get("User-Agent", "Unknown")
    browser_type = "Chrome/Edge" if "AppleWebKit" in user_agent else "Firefox" if "Gecko" in user_agent else "Other"
    
    print(f"📋 Session info:")
    print(f"   - doc_id: {doc_id}")
    print(f"   - has_lsd: {has_lsd}")
    print(f"   - browser: {browser_type}")
    print(f"   - User-Agent: {user_agent[:80]}...")
    
    # Show key headers
    print(f"\n🔍 Key headers:")
    key_headers = ["x-fb-lsd", "x-asbd-id", "Referer", "Accept-Language", "Accept"]
    for header in key_headers:
        if header in session_data["headers"]:
            value = session_data["headers"][header]
            print(f"   - {header}: {value[:50]}{'...' if len(value) > 50 else ''}")
        else:
            print(f"   - {header}: MISSING")
    
    # Show key payload fields
    print(f"\n📦 Key payload fields:")
    key_payload = ["fb_dtsg", "jazoest", "lsd", "__spin_r", "__spin_t"]
    for field in key_payload:
        if field in session_data["payload"]:
            value = session_data["payload"][field]
            print(f"   - {field}: {value[:50]}{'...' if len(value) > 50 else ''}")
        else:
            print(f"   - {field}: MISSING")
    
    # Test basic connectivity
    print(f"\n🌐 Testing basic connectivity...")
    try:
        session = requests.Session()
        session.cookies.update(session_data["cookies"])
        
        basic_headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": session_data["headers"].get("Accept-Language", "en-US,en;q=0.9"),
        }
        
        basic_resp = session.get(
            "https://www.facebook.com",
            headers=basic_headers,
            timeout=15
        )
        
        print(f"   ✅ Basic access: HTTP {basic_resp.status_code}")
        print(f"   📍 Final URL: {basic_resp.url}")
        
        if "login" in basic_resp.url.lower():
            print("   ⚠️  Redirected to login page")
        elif "checkpoint" in basic_resp.url.lower():
            print("   ⚠️  Redirected to checkpoint")
        else:
            print("   ✅ No login/checkpoint redirect")
            
    except Exception as e:
        print(f"   ❌ Basic access failed: {e}")
        return False
    
    # Test GraphQL API
    print(f"\n🔍 Testing GraphQL API...")
    try:
        # Create test headers
        test_headers = {
            "User-Agent": user_agent,
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
        
        # Add browser-specific headers
        for header_key in ["sec-ch-prefers-color-scheme", "sec-ch-ua", "sec-ch-ua-full-version-list", 
                          "sec-ch-ua-mobile", "sec-ch-ua-model", "sec-ch-ua-platform", 
                          "sec-ch-ua-platform-version", "Priority", "Connection", "TE", "Sec-GPC"]:
            if header_key in session_data["headers"]:
                test_headers[header_key] = session_data["headers"][header_key]
        
        # Create test payload
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
        
        print(f"   📤 Sending GraphQL request...")
        print(f"   📋 Headers count: {len(test_headers)}")
        print(f"   📦 Payload fields: {len(test_payload)}")
        
        resp = session.post(
            "https://www.facebook.com/api/graphql/",
            headers=test_headers,
            data=test_payload,
            timeout=20,
        )
        
        print(f"   📥 Response: HTTP {resp.status_code}")
        print(f"   📋 Content-Type: {resp.headers.get('content-type', 'Unknown')}")
        print(f"   📏 Response size: {len(resp.text)} characters")
        
        # Try to parse JSON
        try:
            result = resp.json()
            print(f"   ✅ Response is valid JSON")
            
            if "errors" in result:
                print(f"   ❌ GraphQL errors: {result['errors']}")
                return False
            elif "data" in result:
                print(f"   ✅ Got data response")
                if "node" in result["data"]:
                    print(f"   ✅ Valid response structure")
                    return True
                else:
                    print(f"   ⚠️  Response has data but no node")
                    return True
            else:
                print(f"   ⚠️  Response has no data field")
                print(f"   📋 Response keys: {list(result.keys())}")
                return True
                
        except json.JSONDecodeError as e:
            print(f"   ❌ Response is not JSON: {e}")
            
            # Check response content
            if "text/html" in resp.headers.get("content-type", ""):
                if "checkpoint" in resp.text.lower():
                    print(f"   ❌ Security checkpoint detected")
                    return False
                elif "login" in resp.text.lower():
                    print(f"   ❌ Login required")
                    return False
                else:
                    print(f"   ⚠️  HTML response (not JSON)")
                    print(f"   📋 HTML preview: {resp.text[:200]}...")
                    return False
            else:
                print(f"   📋 Response preview: {resp.text[:300]}...")
                return False
                
    except Exception as e:
        print(f"   ❌ GraphQL test failed: {e}")
        return False
    
    return False

def main():
    print("🔍 Facebook Session Debug Script")
    print("=" * 60)
    
    # Find all cookie files
    cookie_files = []
    for file in os.listdir(COOKIES_DIR):
        if file.endswith("_cookies.json"):
            cookie_files.append(file)
    
    if not cookie_files:
        print("❌ No cookie files found!")
        return
    
    print(f"📁 Found {len(cookie_files)} cookie files:")
    for file in cookie_files:
        print(f"  - {file}")
    
    # Test each session
    working_sessions = []
    failed_sessions = []
    
    for cookie_file in cookie_files:
        file_path = os.path.join(COOKIES_DIR, cookie_file)
        session_data = load_cookie_file(file_path)
        
        if session_data:
            session_name = cookie_file.replace("_cookies.json", "")
            is_working = test_session_detailed(session_data, session_name)
            
            if is_working:
                working_sessions.append(session_name)
            else:
                failed_sessions.append(session_name)
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"✅ Working sessions ({len(working_sessions)}): {', '.join(working_sessions)}")
    print(f"❌ Failed sessions ({len(failed_sessions)}): {', '.join(failed_sessions)}")
    
    if failed_sessions:
        print(f"\n🔍 Common issues to check:")
        print(f"   - Expired cookies")
        print(f"   - Missing required headers")
        print(f"   - Browser fingerprinting differences")
        print(f"   - Facebook security measures")

if __name__ == "__main__":
    main()
