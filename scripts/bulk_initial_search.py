#!/usr/bin/env python3
"""
Bulk initial search script that goes through facebook_group_urls.txt
and hits each URL with the initial search query (not pagination).
Enhanced for handling large numbers of URLs with resume capability.
"""

import requests
import json
import sys
import re
import time
import os
from urllib.parse import unquote, urlparse, parse_qs
from datetime import datetime
import argparse

def parse_curl_command(curl_cmd):
    """Parse a curl command and extract headers, cookies, and data"""
    
    # Extract URL
    url_match = re.search(r"curl\s+['\"]([^'\"]+)['\"]", curl_cmd)
    if not url_match:
        print("❌ Could not extract URL from curl command")
        return None
    
    url = url_match.group(1)
    
    # Extract headers
    headers = {}
    header_matches = re.findall(r"-H\s+['\"]([^:]+):\s*([^'\"]+)['\"]", curl_cmd)
    for key, value in header_matches:
        headers[key] = value
    
    # Extract cookies
    cookies = {}
    cookie_match = re.search(r"-b\s+['\"]([^'\"]+)['\"]", curl_cmd)
    if cookie_match:
        cookie_string = cookie_match.group(1)
        # Parse cookie string
        for cookie in cookie_string.split('; '):
            if '=' in cookie:
                name, value = cookie.split('=', 1)
                cookies[name] = value
    
    # Extract data
    data = {}
    data_match = re.search(r"--data-raw\s+['\"]([^'\"]+)['\"]", curl_cmd)
    if data_match:
        data_string = data_match.group(1)
        # Parse form data
        for param in data_string.split('&'):
            if '=' in param:
                name, value = param.split('=', 1)
                # URL decode the value
                import urllib.parse
                data[name] = urllib.parse.unquote(value)
    
    return {
        'url': url,
        'headers': headers,
        'cookies': cookies,
        'data': data
    }

def extract_search_term_from_url(url):
    """Extract search term from Facebook search URL"""
    try:
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        search_term = query_params.get('q', [''])[0]
        # URL decode the search term
        return unquote(search_term)
    except Exception as e:
        print(f"❌ Error parsing URL {url}: {e}")
        return None

def create_initial_search_request(search_term, base_config):
    """Create an initial search request (not pagination)"""
    
    # Start with the base configuration
    data = base_config['data'].copy()
    
    # Update the search term in the variables
    if 'variables' in data:
        try:
            # Parse the variables JSON
            variables = json.loads(data['variables'])
            
            # Update the search text
            if 'args' in variables and 'text' in variables['args']:
                variables['args']['text'] = search_term
            
            # Ensure this is an initial search, not pagination
            if 'cursor' in variables:
                del variables['cursor']  # Remove cursor for initial search
            
            # Update count to get initial results
            variables['count'] = 5  # Initial batch size
            
            # Update the variables back
            data['variables'] = json.dumps(variables)
            
        except json.JSONDecodeError as e:
            print(f"⚠️  Could not parse variables JSON: {e}")
            return None
    
    return {
        'url': base_config['url'],
        'headers': base_config['headers'],
        'cookies': base_config['cookies'],
        'data': data
    }

def extract_groups_from_response(json_data, search_term):
    """Extract group information from Facebook response in the specified format"""
    groups = []
    
    try:
        if 'data' in json_data and 'serpResponse' in json_data['data']:
            edges = json_data['data']['serpResponse']['results']['edges']
            print(f"      🔍 Found {len(edges)} edges in response")
            
            for i, edge in enumerate(edges):
                print(f"      📋 Edge {i}: role={edge['node'].get('role')}, typename={edge['node'].get('__typename')}")
                
                if edge['node'].get('role') == 'ENTITY_GROUPS':
                    # The actual group data is nested in rendering_strategy.view_model.profile
                    if 'rendering_strategy' in edge:
                        rendering_strategy = edge['rendering_strategy']
                        print(f"      📁 Found rendering_strategy")
                        
                        if 'view_model' in rendering_strategy:
                            view_model = rendering_strategy['view_model']
                            print(f"      📁 Found view_model")
                            
                            if 'profile' in view_model:
                                profile = view_model['profile']
                                print(f"      📁 Found profile: {profile.get('__typename')} - {profile.get('name', 'Unknown')}")
                                
                                # Check if this is actually a Group
                                if profile.get('__typename') == 'Group':
                                    # Extract group information
                                    group = {
                                        "id": str(profile.get('id', '')),
                                        "name": profile.get('name', 'Unknown'),
                                        "url": profile.get('url') or profile.get('profile_url') or f"https://www.facebook.com/groups/{profile.get('id', '')}/",
                                        "member_count": 0,  # Will be updated below
                                        "privacy": "Unknown",  # Facebook doesn't always provide this in search results
                                        "search_term": search_term,
                                        "scraped_at": datetime.now().isoformat()
                                    }
                                    
                                    # Try to extract member count from primary_snippet_text_with_entities if available
                                    if 'primary_snippet_text_with_entities' in view_model and 'text' in view_model['primary_snippet_text_with_entities']:
                                        snippet_text = view_model['primary_snippet_text_with_entities']['text']
                                        if snippet_text:
                                            # Look for numbers in the snippet (e.g., "Public · 1.2K members · 5 posts a day")
                                            member_match = re.search(r'([\d,]+(?:\.[\d]+)?[KMB]?)\s*members?', snippet_text, re.IGNORECASE)
                                            if member_match:
                                                member_str = member_match.group(1)
                                                # Convert K, M, B to actual numbers
                                                if 'K' in member_str.upper():
                                                    member_count = int(float(member_str.replace('K', '').replace(',', '')) * 1000)
                                                elif 'M' in member_str.upper():
                                                    member_count = int(float(member_str.replace('M', '').replace(',', '')) * 1000000)
                                                elif 'B' in member_str.upper():
                                                    member_count = int(float(member_str.replace('B', '').replace(',', '')) * 1000000000)
                                                else:
                                                    member_count = int(member_str.replace(',', ''))
                                                group["member_count"] = member_count
                                                
                                                # Also try to extract privacy from the snippet
                                                if 'Public' in snippet_text:
                                                    group["privacy"] = "Public"
                                                elif 'Private' in snippet_text:
                                                    group["privacy"] = "Private"
                                                elif 'Secret' in snippet_text:
                                                    group["privacy"] = "Secret"
                                    
                                    # Only add groups that have valid IDs
                                    if group["id"] and group["id"] != "":
                                        groups.append(group)
                                        print(f"      ✅ Added group: {group['name']} (ID: {group['id']}) - {group['member_count']} members")
                                    else:
                                        print(f"      ⚠️  Skipping group with empty ID: {group['name']}")
                                else:
                                    print(f"      ⚠️  Profile is not a Group: {profile.get('__typename')}")
                            else:
                                print(f"      ⚠️  No profile in view_model")
                        else:
                            print(f"      ⚠️  No view_model in rendering_strategy")
                    else:
                        print(f"      ⚠️  No rendering_strategy in edge")
                    
    except Exception as e:
        print(f"   ⚠️  Error extracting groups: {e}")
        import traceback
        traceback.print_exc()
    
    return groups

def make_search_request(request_config, search_term):
    """Make a search request and return the results"""
    
    try:
        print(f"🔍 Searching for: {search_term}")
        
        response = requests.post(
            request_config['url'],
            headers=request_config['headers'],
            cookies=request_config['cookies'],
            data=request_config['data'],
            timeout=30
        )
        
        print(f"   📥 Status: {response.status_code}, Size: {len(response.content)} bytes")
        
        if response.status_code == 200:
            # Check if it's a Facebook error response
            if response.text.startswith('for (;;);'):
                print(f"   ❌ Facebook error for '{search_term}'")
                return None
            
            # Try to parse as JSON
            try:
                json_data = response.json()
                
                # Check if we have group data
                if 'data' in json_data and 'serpResponse' in json_data['data']:
                    edges = json_data['data']['serpResponse']['results']['edges']
                    group_count = len([e for e in edges if e['node'].get('role') == 'ENTITY_GROUPS'])
                    print(f"   ✅ Found {group_count} groups for '{search_term}'")
                    return json_data
                else:
                    print(f"   ⚠️  No group data found for '{search_term}'")
                    return None
                    
            except json.JSONDecodeError as e:
                print(f"   ❌ JSON parse error for '{search_term}': {e}")
                return None
        else:
            print(f"   ❌ Request failed for '{search_term}': {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   ❌ Error searching for '{search_term}': {e}")
        return None

def save_groups_to_file(groups, output_file):
    """Save groups to a single JSON file, appending to existing data"""
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Load existing groups if file exists
    existing_groups = []
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_groups = json.load(f)
            print(f"   📁 Loaded {len(existing_groups)} existing groups from {os.path.basename(output_file)}")
        except Exception as e:
            print(f"   ⚠️  Could not load existing groups: {e}")
    
    # Add new groups
    all_groups = existing_groups + groups
    
    # Save all groups to file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_groups, f, indent=2, ensure_ascii=False)
        print(f"   💾 Saved {len(groups)} new groups to {os.path.basename(output_file)} (total: {len(all_groups)})")
        return True
    except Exception as e:
        print(f"   ❌ Error saving groups: {e}")
        return False

def get_processed_searches(output_file):
    """Get list of already processed searches to enable resume"""
    processed = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_groups = json.load(f)
            # Extract unique search terms from existing groups
            for group in existing_groups:
                if 'search_term' in group:
                    processed.add(group['search_term'])
        except Exception as e:
            print(f"⚠️  Could not read existing output file: {e}")
    return processed

def log_progress(current, total, successful, failed, start_time):
    """Log progress with estimated time remaining"""
    elapsed = time.time() - start_time
    if current > 0:
        rate = current / elapsed
        remaining = (total - current) / rate if rate > 0 else 0
        
        print(f"\n📊 PROGRESS: {current}/{total} ({current/total*100:.1f}%)")
        print(f"   ✅ Successful: {successful}, ❌ Failed: {failed}")
        print(f"   ⏱️  Elapsed: {elapsed/60:.1f} min, Remaining: {remaining/60:.1f} min")
        print(f"   🚀 Rate: {rate:.1f} searches/min")

def process_urls_file(urls_file, curl_config_file, output_file="output/initial_searches/initial_searches.json", 
                     start_from=0, max_urls=None, delay=1):
    """Process all URLs in the file and make initial search requests"""
    
    print("🚀 Facebook Bulk Initial Search")
    print("=" * 50)
    
    # Load the curl configuration
    try:
        with open(curl_config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        curl_cmd = config['curl_command']
        print(f"✅ Loaded curl command from {curl_config_file}")
        
    except Exception as e:
        print(f"❌ Error loading curl command: {e}")
        return
    
    # Parse the curl command
    base_config = parse_curl_command(curl_cmd)
    if not base_config:
        print("❌ Could not parse curl command")
        return
    
    print(f"📡 Base URL: {base_config['url']}")
    print(f"🍪 Cookies: {len(base_config['cookies'])} cookies")
    print(f"📋 Headers: {len(base_config['headers'])} headers")
    print()
    
    # Read the URLs file
    try:
        with open(urls_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        total_urls = len(urls)
        print(f"📋 Found {total_urls} URLs to process")
        
        # Apply start_from and max_urls filters
        if start_from > 0:
            urls = urls[start_from:]
            print(f"⏭️  Starting from URL #{start_from + 1}")
        
        if max_urls:
            urls = urls[:max_urls]
            print(f"🔢 Processing max {max_urls} URLs")
        
        print(f"🎯 Will process {len(urls)} URLs")
        
    except Exception as e:
        print(f"❌ Error reading URLs file: {e}")
        return
    
    # Check for already processed searches
    processed_searches = get_processed_searches(output_file)
    if processed_searches:
        print(f"📁 Found {len(processed_searches)} already processed searches")
        print("💡 Use --resume to skip already processed searches")
    
    # Process each URL
    successful_searches = 0
    failed_searches = 0
    skipped_searches = 0
    total_groups_found = 0
    start_time = time.time()
    
    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] Processing: {url}")
        
        # Extract search term
        search_term = extract_search_term_from_url(url)
        if not search_term:
            print("   ❌ Could not extract search term, skipping...")
            failed_searches += 1
            continue
        
        # Check if already processed
        if search_term in processed_searches:
            print(f"   ⏭️  Already processed '{search_term}', skipping...")
            skipped_searches += 1
            continue
        
        # Create initial search request (not pagination)
        request_config = create_initial_search_request(search_term, base_config)
        if not request_config:
            print("   ❌ Could not create request config, skipping...")
            failed_searches += 1
            continue
        
        # Make the search request
        results = make_search_request(request_config, search_term)
        
        if results:
            # Extract groups from the response
            groups = extract_groups_from_response(results, search_term)
            
            if groups:
                # Save groups to the single output file
                if save_groups_to_file(groups, output_file):
                    total_groups_found += len(groups)
                    successful_searches += 1
                else:
                    failed_searches += 1
            else:
                print(f"   ⚠️  No groups extracted for '{search_term}'")
                successful_searches += 1  # Still count as successful search
        else:
            failed_searches += 1
        
        # Log progress every 10 searches
        if i % 10 == 0:
            log_progress(i, len(urls), successful_searches, failed_searches, start_time)
        
        # Add a delay to be respectful
        if i < len(urls):  # Don't delay after the last request
            time.sleep(delay)
    
    # Final summary
    total_time = time.time() - start_time
    print("\n" + "=" * 50)
    print("📊 FINAL SEARCH SUMMARY")
    print(f"✅ Successful searches: {successful_searches}")
    print(f"❌ Failed searches: {failed_searches}")
    print(f"⏭️  Skipped searches: {skipped_searches}")
    print(f"👥 Total groups found: {total_groups_found}")
    print(f"📁 Results saved to: {output_file}")
    print(f"⏰ Total time: {total_time/60:.1f} minutes")
    print(f"🚀 Average rate: {len(urls)/total_time*60:.1f} searches/min")
    print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main function with command line arguments"""
    
    parser = argparse.ArgumentParser(description='Facebook Bulk Initial Search')
    parser.add_argument('--urls-file', default='settings/facebook_group_urls.txt',
                       help='File containing Facebook search URLs')
    parser.add_argument('--curl-config', default='settings/curl/loverichards.json',
                       help='JSON file containing curl command')
    parser.add_argument('--output-file', default='output/initial_searches/initial_searches.json',
                       help='Single JSON file to save all group results')
    parser.add_argument('--start-from', type=int, default=0,
                       help='Start processing from URL number (0-based)')
    parser.add_argument('--max-urls', type=int, default=None,
                       help='Maximum number of URLs to process')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between requests in seconds')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from where left off (skip already processed)')
    
    args = parser.parse_args()
    
    # Check if files exist
    if not os.path.exists(args.urls_file):
        print(f"❌ URLs file not found: {args.urls_file}")
        return
    
    if not os.path.exists(args.curl_config):
        print(f"❌ Curl config file not found: {args.curl_config}")
        return
    
    # Process the URLs
    process_urls_file(
        args.urls_file, 
        args.curl_config, 
        args.output_file,
        args.start_from,
        args.max_urls,
        args.delay
    )

if __name__ == "__main__":
    main()
