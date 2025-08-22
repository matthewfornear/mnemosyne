#!/usr/bin/env python3
"""
cURL Group Parse - Fork of group_parse.py
Analyzes Facebook Groups data from cURL scraper output and provides comprehensive statistics.
Handles both individual worker files and merged output files.
"""

import json
import re
import os
import sys
import argparse
import glob
from collections import Counter, defaultdict
from urllib.parse import quote

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Default file paths for cURL output
CURL_OUTPUT_DIR = os.path.join(PARENT_DIR, "output", "curl")
DEFAULT_MERGED_FILE = os.path.join(CURL_OUTPUT_DIR, "groups_output_curl.json")
WORKER_FILES_PATTERN = os.path.join(CURL_OUTPUT_DIR, "groups_output_curl_worker_*.json")

def parse_member_count(member_count_str):
    """Parse member count string to numeric value"""
    if not member_count_str:
        return 0
    
    # Extract numbers and multipliers (K, M)
    match = re.search(r'([\d,.]+)\s*([KM]?)', str(member_count_str), re.IGNORECASE)
    if match:
        num_str = match.group(1).replace(',', '')
        multiplier = match.group(2).upper()
        
        try:
            num = float(num_str)
            if multiplier == 'K':
                return int(num * 1000)
            elif multiplier == 'M':
                return int(num * 1000000)
            else:
                return int(num)
        except ValueError:
            return 0
    
    return 0

def load_curl_worker_files():
    """Load all groups from cURL worker output files"""
    all_groups = []
    seen_ids = set()
    worker_files = glob.glob(WORKER_FILES_PATTERN)
    
    print(f"🔍 Found {len(worker_files)} worker files")
    
    for worker_file in worker_files:
        worker_id = os.path.basename(worker_file).replace("groups_output_curl_worker_", "").replace(".json", "")
        print(f"📁 Loading worker {worker_id} file: {worker_file}")
        
        try:
            with open(worker_file, 'r', encoding='utf-8') as f:
                # Handle JSONL format (one JSON object per line)
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:
                        try:
                            group = json.loads(line)
                            if group.get("id") and group["id"] not in seen_ids:
                                all_groups.append(group)
                                seen_ids.add(group["id"])
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"⚠️  Error reading {worker_file}: {e}")
    
    return all_groups

def load_merged_file(filename):
    """Load groups from merged JSON file"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            # Try to load as JSON array first
            try:
                data = json.load(f)
                if isinstance(data, list):
                    return data
            except json.JSONDecodeError:
                # If that fails, try JSONL format
                f.seek(0)
                groups = []
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:
                        try:
                            group = json.loads(line)
                            groups.append(group)
                        except json.JSONDecodeError:
                            continue
                return groups
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return []

def analyze_curl_groups(use_worker_files=False, filename=None):
    """Analyze the cURL groups data and return comprehensive statistics"""
    
    # Load groups data
    if use_worker_files:
        print("📊 Analyzing individual worker files...")
        groups_data = load_curl_worker_files()
        source_info = f"from {len(glob.glob(WORKER_FILES_PATTERN))} worker files"
    else:
        filename = filename or DEFAULT_MERGED_FILE
        print(f"📊 Analyzing merged file: {filename}")
        if not os.path.exists(filename):
            print(f"❌ File not found: {filename}")
            return None
        groups_data = load_merged_file(filename)
        source_info = f"from {filename}"
    
    if not groups_data:
        print("❌ No groups data found")
        return None
    
    print(f"✅ Loaded {len(groups_data):,} groups {source_info}")
    print("=" * 80)
    
    # Data structures for analysis
    groups_by_city = defaultdict(list)
    city_group_counts = Counter()
    privacy_stats = Counter()
    member_count_stats = []
    total_members = 0
    groups_with_member_count = 0
    id_lengths = Counter()
    search_terms = set()
    
    total_groups = len(groups_data)
    
    for group_num, group in enumerate(groups_data, 1):
        try:
            # Extract search term / city_state
            search_term = group.get('search_term', '').replace('+', ' ').strip()
            if not search_term:
                # Fallback: try to extract from other fields
                search_term = group.get('city_state', '').replace('+', ' ').strip()
            if not search_term:
                search_term = 'Unknown'
            
            search_terms.add(search_term)
            
            # Store group data
            group_data = {
                'id': group.get('id', ''),
                'name': group.get('name', 'Unknown'),
                'url': group.get('url', ''),
                'member_count': group.get('member_count', ''),
                'member_count_numeric': parse_member_count(group.get('member_count', '')),
                'privacy': group.get('privacy', ''),
                'search_term': search_term,
                'scraped_at': group.get('scraped_at', '')
            }
            
            groups_by_city[search_term].append(group_data)
            city_group_counts[search_term] += 1
            
            # Privacy statistics
            privacy_stats[group_data['privacy']] += 1
            
            # Member count statistics
            if group_data['member_count_numeric'] > 0:
                member_count_stats.append(group_data['member_count_numeric'])
                total_members += group_data['member_count_numeric']
                groups_with_member_count += 1
            
            # ID length analysis
            if group_data['id']:
                id_lengths[len(group_data['id'])] += 1
            
            # Progress indicator
            if group_num % 5000 == 0:
                print(f"📈 Processed {group_num:,} groups...")
        
        except Exception as e:
            print(f"⚠️  Error processing group {group_num}: {e}")
            continue
    
    return {
        'total_groups': total_groups,
        'unique_search_terms': len(search_terms),
        'groups_by_city': dict(groups_by_city),
        'city_group_counts': city_group_counts,
        'privacy_stats': privacy_stats,
        'member_count_stats': member_count_stats,
        'total_members': total_members,
        'groups_with_member_count': groups_with_member_count,
        'id_lengths': id_lengths,
        'search_terms': search_terms
    }

def analyze_completion_status():
    """Analyze completion status from progress files"""
    progress_file = os.path.join(CURL_OUTPUT_DIR, "url_progress_curl.json")
    
    completion_stats = {
        'total_urls_tracked': 0,
        'completed_urls': 0,
        'pending_urls': 0,
        'completion_rate': 0.0
    }
    
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                url_progress = json.load(f)
            
            completion_stats['total_urls_tracked'] = len(url_progress)
            completion_stats['completed_urls'] = sum(1 for completed in url_progress.values() if completed)
            completion_stats['pending_urls'] = completion_stats['total_urls_tracked'] - completion_stats['completed_urls']
            
            if completion_stats['total_urls_tracked'] > 0:
                completion_stats['completion_rate'] = (completion_stats['completed_urls'] / completion_stats['total_urls_tracked']) * 100
            
        except Exception as e:
            print(f"⚠️  Error reading progress file: {e}")
    
    return completion_stats

def display_curl_statistics(data):
    """Display comprehensive statistics for cURL scraper output"""
    
    total_groups = data['total_groups']
    groups_by_city = data['groups_by_city']
    city_group_counts = data['city_group_counts']
    unique_search_terms = data['unique_search_terms']
    
    print(f"\n📊 FACEBOOK GROUPS CURL SCRAPER ANALYSIS")
    print("=" * 80)
    
    # Completion Status
    completion_stats = analyze_completion_status()
    print(f"\n🎯 SCRAPING COMPLETION STATUS")
    print("-" * 50)
    print(f"📈 URLs tracked: {completion_stats['total_urls_tracked']:,}")
    print(f"✅ URLs completed: {completion_stats['completed_urls']:,}")
    print(f"⏳ URLs pending: {completion_stats['pending_urls']:,}")
    print(f"📊 Completion rate: {completion_stats['completion_rate']:.1f}%")
    
    # Overall Statistics
    print(f"\n📈 OVERALL STATISTICS")
    print("-" * 40)
    print(f"📋 Total groups found: {total_groups:,}")
    print(f"🌆 Unique cities/search terms: {unique_search_terms:,}")
    print(f"📊 Average groups per city: {total_groups / unique_search_terms:.2f}")
    
    # Groups per City/Search Term Distribution
    print(f"\n📊 GROUPS PER CITY/SEARCH TERM DISTRIBUTION")
    print("-" * 60)
    
    distribution = Counter()
    for city, count in city_group_counts.items():
        distribution[count] += 1
    
    print(f"{'Groups Found':<15} {'Cities':<15} {'Percentage':<12}")
    print("-" * 42)
    
    for group_count in sorted(distribution.keys()):
        location_count = distribution[group_count]
        percentage = (location_count / unique_search_terms) * 100
        print(f"{group_count:<15} {location_count:<15} {percentage:<12.2f}%")
    
    # Zero-result cities
    zero_result_cities = [city for city, count in city_group_counts.items() if count == 0]
    if zero_result_cities:
        print(f"\n🚫 ZERO-RESULT CITIES")
        print("-" * 40)
        print(f"Cities with no groups found: {len(zero_result_cities):,}")
        if len(zero_result_cities) <= 20:
            for city in sorted(zero_result_cities):
                print(f"   • {city}")
        else:
            for city in sorted(zero_result_cities)[:20]:
                print(f"   • {city}")
            print(f"   ... and {len(zero_result_cities) - 20} more")
    
    # Top performing cities
    print(f"\n🏆 TOP 20 CITIES BY GROUP COUNT")
    print("-" * 60)
    
    for i, (city, count) in enumerate(city_group_counts.most_common(20), 1):
        # Create Facebook search URL
        search_query = quote(city)
        search_url = f"https://www.facebook.com/groups/search/groups_home/?q={search_query}"
        
        print(f"{i:2d}. {city:<35} {count:,} groups")
        print(f"    🔍 Search: {search_url}")
    
    # Privacy Statistics
    if data['privacy_stats']:
        print(f"\n🔒 PRIVACY SETTINGS")
        print("-" * 40)
        
        for privacy, count in data['privacy_stats'].most_common():
            percentage = (count / total_groups) * 100
            privacy_display = privacy if privacy else 'Unknown'
            print(f"{privacy_display:<15} {count:,} groups ({percentage:.1f}%)")
    
    # Member Statistics
    if data['groups_with_member_count'] > 0:
        print(f"\n👥 MEMBERSHIP STATISTICS")
        print("-" * 40)
        
        member_stats = sorted(data['member_count_stats'])
        avg_members = data['total_members'] / data['groups_with_member_count']
        median_members = member_stats[len(member_stats)//2]
        
        print(f"📊 Groups with member data: {data['groups_with_member_count']:,} / {total_groups:,}")
        print(f"📊 Total members across all groups: {data['total_members']:,}")
        print(f"📊 Average members per group: {avg_members:,.0f}")
        print(f"📊 Median members per group: {median_members:,}")
        print(f"📊 Smallest group: {min(member_stats):,} members")
        print(f"📊 Largest group: {max(member_stats):,} members")
    
    # Recent scraping activity
    recent_groups = []
    for city_groups in groups_by_city.values():
        for group in city_groups:
            if group.get('scraped_at'):
                recent_groups.append((group['scraped_at'], group['search_term'], group['name']))
    
    if recent_groups:
        print(f"\n⏰ RECENT SCRAPING ACTIVITY")
        print("-" * 50)
        
        # Sort by scraped_at timestamp
        recent_groups.sort(key=lambda x: x[0], reverse=True)
        
        print("Latest 10 groups scraped:")
        for i, (scraped_at, search_term, group_name) in enumerate(recent_groups[:10], 1):
            print(f"{i:2d}. {search_term} - {group_name[:50]}{'...' if len(group_name) > 50 else ''}")
            print(f"    📅 {scraped_at}")
    
    # Summary
    print(f"\n✅ ANALYSIS COMPLETE")
    print("-" * 40)
    print(f"📊 Cities with groups: {len([c for c in city_group_counts.values() if c > 0]):,}")
    print(f"📊 Cities with no groups: {len([c for c in city_group_counts.values() if c == 0]):,}")
    print(f"📋 Total groups analyzed: {total_groups:,}")
    print(f"🎯 Scraping completion: {completion_stats['completion_rate']:.1f}%")

def main():
    parser = argparse.ArgumentParser(description='Analyze Facebook Groups cURL scraper output')
    parser.add_argument('--workers', action='store_true',
                       help='Analyze individual worker files instead of merged file')
    parser.add_argument('--file', 
                       help=f'Specific file to analyze (default: {DEFAULT_MERGED_FILE})')
    parser.add_argument('--reset', action='store_true',
                       help='Reset/delete the specified file (use with caution!)')
    
    args = parser.parse_args()
    
    if args.reset:
        target_file = args.file or DEFAULT_MERGED_FILE
        if os.path.exists(target_file):
            confirm = input(f"⚠️  WARNING: This will DELETE {target_file}!\nAre you ABSOLUTELY SURE? (type 'DELETE' to confirm): ")
            if confirm == 'DELETE':
                os.remove(target_file)
                print(f"🗑️  File {target_file} has been deleted.")
            else:
                print("❌ Reset cancelled.")
        else:
            print(f"❌ File {target_file} does not exist.")
        return
    
    print("📊 Facebook Groups cURL Scraper Analysis Tool")
    print("=" * 60)
    
    # Analyze the data
    data = analyze_curl_groups(use_worker_files=args.workers, filename=args.file)
    if not data:
        sys.exit(1)
    
    # Display statistics
    display_curl_statistics(data)

if __name__ == "__main__":
    main() 