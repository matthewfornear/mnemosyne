#!/usr/bin/env python3
"""
Debug script to examine account responses and understand why they're failing
"""

import requests
import json
import re
from urllib.parse import unquote

def parse_curl_command(curl_cmd):
    """Parse a curl command and extract headers, cookies, and data"""
    
    # Handle different curl command formats
    curl_cmd = curl_cmd.strip().replace('\r\n', '\n').replace('\r', '\n')
    
    # Extract URL
    url_patterns = [
        r"curl\s+['\"]([^'\"]+)['\"]",
        r"curl\s+\^?['\"]([^'\"]+)\^?['\"]",
        r"curl\s+([^\s]+)",
    ]
    
    url = None
    for pattern in url_patterns:
        url_match = re.search(pattern, curl_cmd)
        if url_match:
            url = url_match.group(1)
            break
    
    if not url:
        return None
    
    # Extract headers
    headers = {}
    header_patterns = [
        r"-H\s+['\"]([^:]+):\s*([^'\"]+)['\"]",
        r"-H\s+\^?['\"]([^:]+):\s*([^'\"]+)\^?['\"]",
        r"-H\s+([^:]+):\s*([^\s]+)",
    ]
    
    for pattern in header_patterns:
        header_matches = re.findall(pattern, curl_cmd)
        for key, value in header_matches:
            key = key.strip()
            value = value.strip()
            if key and value:
                headers[key] = value
    
    # Extract cookies
    cookies = {}
    cookie_patterns = [
        r"-b\s+['\"]([^'\"]+)['\"]",
        r"-b\s+\^?['\"]([^'\"]+)\^?['\"]",
        r"--cookie\s+['\"]([^'\"]+)['\"]",
        r"--cookie\s+\^?['\"]([^'\"]+)\^?['\"]",
    ]
    
    for pattern in cookie_patterns:
        cookie_match = re.search(pattern, curl_cmd)
        if cookie_match:
            cookie_string = cookie_match.group(1)
            for cookie in cookie_string.split('; '):
                if '=' in cookie:
                    name, value = cookie.split('=', 1)
                    cookies[name.strip()] = value.strip()
            break
    
    # Extract data
    data = {}
    data_patterns = [
        r"--data-raw\s+['\"]([^'\"]+)['\"]",
        r"--data-raw\s+\^?['\"]([^'\"]+)\^?['\"]",
        r"--data\s+['\"]([^'\"]+)['\"]",
        r"--data\s+\^?['\"]([^'\"]+)\^?['\"]",
    ]
    
    for pattern in data_patterns:
        data_match = re.search(pattern, curl_cmd)
        if data_match:
            data_string = data_match.group(1)
            data_string = data_string.replace('^%^', '%')
            for param in data_string.split('&'):
                if '=' in param:
                    name, value = param.split('=', 1)
                    data[name.strip()] = unquote(value.strip())
            break
    
    return {
        'url': url,
        'headers': headers,
        'cookies': cookies,
        'data': data
    }

def create_initial_search_request(search_term, base_config):
    """Create an initial search request (not pagination)"""
    
    data = base_config['data'].copy()
    
    if 'variables' in data:
        try:
            variables = json.loads(data['variables'])
            
            if 'args' in variables and 'text' in variables['args']:
                variables['args']['text'] = search_term
            
            if 'cursor' in variables:
                del variables['cursor']
            
            variables['count'] = 5
            data['variables'] = json.dumps(variables)
            
        except json.JSONDecodeError as e:
            print(f"⚠️  Could not parse variables JSON: {e}")
            return None
    else:
        print(f"⚠️  No 'variables' found in data. Available keys: {list(data.keys())}")
        return None
    
    return {
        'url': base_config['url'],
        'headers': base_config['headers'],
        'cookies': base_config['cookies'],
        'data': data
    }

def debug_account_response(account_name, curl_config):
    """Debug account response to understand what's happening"""
    
    print(f"🔍 Debugging account: {account_name}")
    
    try:
        # Parse the curl command
        base_config = parse_curl_command(curl_config)
        if not base_config:
            print(f"   ❌ Could not parse curl command for {account_name}")
            return
        
        print(f"   📡 URL: {base_config['url']}")
        print(f"   🍪 Cookies: {len(base_config['cookies'])} cookies")
        print(f"   📋 Headers: {len(base_config['headers'])} headers")
        print(f"   📊 Data keys: {list(base_config['data'].keys())}")
        
        # Test with a simple search term
        test_search_term = "test"
        request_config = create_initial_search_request(test_search_term, base_config)
        if not request_config:
            print(f"   ❌ Could not create request config for {account_name}")
            return
        
        # Make a test request
        response = requests.post(
            request_config['url'],
            headers=request_config['headers'],
            cookies=request_config['cookies'],
            data=request_config['data'],
            timeout=30
        )
        
        print(f"   📥 Status: {response.status_code}")
        print(f"   📏 Size: {len(response.content)} bytes")
        print(f"   🔒 Content-Type: {response.headers.get('content-type', 'Unknown')}")
        
        if response.status_code == 200:
            if response.text.startswith('for (;;);'):
                print(f"   ❌ Facebook error response")
                error_text = response.text.replace('for (;;);', '')
                try:
                    error_data = json.loads(error_text)
                    print(f"   📝 Error: {error_data.get('errorSummary', 'Unknown error')}")
                except:
                    print(f"   📝 Error text: {error_text[:200]}...")
            else:
                # Check if it's HTML
                if response.text.strip().startswith('<!DOCTYPE') or response.text.strip().startswith('<html'):
                    print(f"   ⚠️  HTML response (likely redirected to login)")
                    print(f"   📝 First 200 chars: {response.text[:200]}...")
                else:
                    try:
                        json_data = response.json()
                        print(f"   ✅ JSON response")
                        if 'data' in json_data and 'serpResponse' in json_data['data']:
                            print(f"   🎯 Contains expected data structure")
                        else:
                            print(f"   ⚠️  Unexpected JSON structure: {list(json_data.keys())}")
                    except json.JSONDecodeError:
                        print(f"   ❌ Not JSON, first 200 chars: {response.text[:200]}...")
        else:
            print(f"   ❌ Request failed with status {response.status_code}")
            print(f"   📝 Response text: {response.text[:200]}...")
            
    except Exception as e:
        print(f"   ❌ Error debugging {account_name}: {e}")

def main():
    """Main function"""
    
    # Load curl configs
    configs = {}
    curl_dir = "settings/curl"
    
    for filename in os.listdir(curl_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(curl_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    account_name = config.get('account_name', filename.replace('.json', ''))
                    configs[account_name] = config['curl_command']
            except Exception as e:
                print(f"⚠️  Error loading {filename}: {e}")
    
    print(f"📁 Loaded {len(configs)} curl configurations")
    print("=" * 60)
    
    # Debug each account
    for account_name, curl_cmd in configs.items():
        debug_account_response(account_name, curl_cmd)
        print()

if __name__ == "__main__":
    import os
    main()
