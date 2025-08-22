#!/usr/bin/env python3
"""
Python version of groupParse.php
Analyzes Facebook Groups JSONL data and provides comprehensive statistics.
"""

import json
import re
import os
import sys
import argparse
from collections import Counter, defaultdict
from urllib.parse import quote

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Default file path
DEFAULT_GROUPS_FILE = os.path.join(PARENT_DIR, "output", "groups_graphql.jsonl")

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

def analyze_groups_file(filename):
    """Analyze the groups JSONL file and return comprehensive statistics"""
    
    if not os.path.exists(filename):
        print(f"❌ Groups file not found: {filename}")
        return None
    
    print(f"📊 Analyzing groups file: {filename}")
    print("=" * 80)
    
    # Data structures for analysis
    groups_by_city = defaultdict(list)
    city_group_counts = Counter()
    privacy_stats = Counter()
    member_count_stats = []
    total_members = 0
    groups_with_member_count = 0
    id_lengths = Counter()
    
    total_groups = 0
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    group = json.loads(line)
                    total_groups += 1
                    
                    # Extract city_state
                    city_state = group.get('city_state', '').replace('+', ' ').strip()
                    if not city_state:
                        city_state = 'Unknown'
                    
                    # Store group data
                    group_data = {
                        'id': group.get('id', ''),
                        'name': group.get('name', 'Unknown'),
                        'url': group.get('url', ''),
                        'member_count': group.get('member_count', ''),
                        'member_count_numeric': parse_member_count(group.get('member_count', '')),
                        'privacy': group.get('privacy', '')
                    }
                    
                    groups_by_city[city_state].append(group_data)
                    city_group_counts[city_state] += 1
                    
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
                    if total_groups % 5000 == 0:
                        print(f"📈 Processed {total_groups:,} groups...")
                
                except json.JSONDecodeError as e:
                    print(f"⚠️  JSON decode error on line {line_num}: {e}")
                    continue
                except Exception as e:
                    print(f"⚠️  Error processing line {line_num}: {e}")
                    continue
    
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None
    
    return {
        'total_groups': total_groups,
        'groups_by_city': dict(groups_by_city),
        'city_group_counts': city_group_counts,
        'privacy_stats': privacy_stats,
        'member_count_stats': member_count_stats,
        'total_members': total_members,
        'groups_with_member_count': groups_with_member_count,
        'id_lengths': id_lengths
    }

def display_statistics(data):
    """Display comprehensive statistics similar to the PHP version"""
    
    total_groups = data['total_groups']
    groups_by_city = data['groups_by_city']
    city_group_counts = data['city_group_counts']
    total_city_states = len(city_group_counts)
    
    print(f"\n📊 FACEBOOK GROUPS ANALYSIS")
    print("=" * 80)
    
    # Overall Statistics
    print(f"\n📈 OVERALL STATISTICS")
    print("-" * 40)
    print(f"📋 Total groups parsed: {total_groups:,}")
    print(f"🌆 Total unique city/state locations: {total_city_states:,}")
    if total_city_states > 0:
        print(f"📊 Average groups per city/state: {total_groups / total_city_states:.2f}")
    
    # Groups per City/State Distribution
    print(f"\n📊 GROUPS PER CITY/STATE DISTRIBUTION")
    print("-" * 50)
    
    distribution = Counter()
    for city, count in city_group_counts.items():
        distribution[count] += 1
    
    print(f"{'Number of Groups':<20} {'Number of Locations':<20} {'Percentage':<12}")
    print("-" * 52)
    
    for group_count in sorted(distribution.keys()):
        location_count = distribution[group_count]
        percentage = (location_count / total_city_states) * 100
        print(f"{group_count:<20} {location_count:<20} {percentage:<12.2f}%")
    
    # Distribution Histogram
    print(f"\n📊 DISTRIBUTION HISTOGRAM")
    print("-" * 40)
    
    max_bar_length = 50
    max_location_count = max(distribution.values()) if distribution else 1
    
    for group_count in sorted(distribution.keys()):
        location_count = distribution[group_count]
        bar_length = int((location_count / max_location_count) * max_bar_length)
        bar = '█' * bar_length
        print(f"{group_count:3d} groups: {bar:<{max_bar_length}} {location_count} locations")
    
    # Sample Examples by Group Count
    print(f"\n📋 SAMPLE EXAMPLES BY GROUP COUNT")
    print("-" * 80)
    
    # Show examples for different group counts
    seen_counts = set()
    sorted_cities = sorted(city_group_counts.items(), key=lambda x: (x[1], x[0]))
    
    for city_state, group_count in sorted_cities:
        if group_count not in seen_counts and len(seen_counts) < 20:  # Show max 20 different counts
            seen_counts.add(group_count)
            
            print(f"\n🏙️  {city_state} ({group_count} groups):")
            
            # Show groups for this city (limit to 10 for readability)
            city_groups = groups_by_city[city_state]
            display_groups = city_groups[:10] if len(city_groups) > 10 else city_groups
            
            for i, group in enumerate(display_groups, 1):
                member_info = f" ({group['member_count']})" if group['member_count'] else ""
                print(f"   {i:2d}. {group['name'][:60]}{'...' if len(group['name']) > 60 else ''}{member_info}")
                print(f"       🔗 {group['url']}")
            
            if len(city_groups) > 10:
                print(f"       ... and {len(city_groups) - 10} more groups")
    
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
    
    # Top 10 Cities/States by Group Count
    print(f"\n🏆 TOP 10 CITIES/STATES BY GROUP COUNT")
    print("-" * 50)
    
    for i, (city_state, count) in enumerate(city_group_counts.most_common(10), 1):
        # Create Facebook search URL
        search_query = quote(city_state)
        search_url = f"https://www.facebook.com/groups/search/groups_home/?q={search_query}"
        
        print(f"{i:2d}. {city_state:<30} {count:,} groups")
        print(f"    🔍 Search: {search_url}")
    
    # Group ID Analysis
    if data['id_lengths']:
        print(f"\n🆔 GROUP ID ANALYSIS")
        print("-" * 40)
        
        print("Group ID length distribution:")
        for id_length in sorted(data['id_lengths'].keys()):
            count = data['id_lengths'][id_length]
            print(f"{id_length} characters: {count:,} groups")
    
    # One-Group Searches (Clickable Links)
    one_group_cities = [city for city, count in city_group_counts.items() if count == 1]
    
    if one_group_cities:
        print(f"\n🔍 ONE-GROUP SEARCHES")
        print("-" * 40)
        print("Cities/states with exactly 1 group (Facebook search links):")
        
        # Display in columns for better readability
        for i, city_state in enumerate(sorted(one_group_cities)):
            search_query = quote(city_state)
            search_url = f"https://www.facebook.com/groups/search/groups_home/?q={search_query}"
            print(f"🔗 {city_state:<25} -> {search_url}")
            
            # Limit display for very long lists
            if i >= 50:  # Show max 50 one-group cities
                remaining = len(one_group_cities) - 51
                if remaining > 0:
                    print(f"... and {remaining} more cities with 1 group each")
                break
        
        print(f"\nTotal locations with exactly 1 group: {len(one_group_cities):,}")
    
    # Complete City/State Listing
    print(f"\n📋 COMPLETE CITY/STATE LISTING (Top 50)")
    print("-" * 80)
    print("All locations ordered by number of groups (highest to lowest):")
    print(f"{'Rank':<6} {'City/State':<35} {'Groups':<10} {'Facebook Search URL'}")
    print("-" * 80)
    
    for i, (city_state, count) in enumerate(city_group_counts.most_common(50), 1):
        search_query = quote(city_state)
        search_url = f"https://www.facebook.com/groups/search/groups_home/?q={search_query}"
        
        print(f"{i:<6} {city_state:<35} {count:<10,} {search_url}")
    
    if len(city_group_counts) > 50:
        print(f"\n... and {len(city_group_counts) - 50:,} more locations")
    
    print(f"\nTotal unique locations: {len(city_group_counts):,}")
    
    # Summary
    print(f"\n✅ PROCESSING COMPLETE")
    print("-" * 40)
    print("All data has been successfully analyzed.")
    print(f"📊 Total unique cities found: {len(city_group_counts):,}")
    print(f"📋 Total groups processed: {total_groups:,}")

def main():
    parser = argparse.ArgumentParser(description='Analyze Facebook Groups JSONL data')
    parser.add_argument('filename', nargs='?', default=DEFAULT_GROUPS_FILE,
                       help=f'JSONL file to analyze (default: {DEFAULT_GROUPS_FILE})')
    parser.add_argument('--reset', action='store_true',
                       help='Reset/delete the specified file (use with caution!)')
    
    args = parser.parse_args()
    
    if args.reset:
        if os.path.exists(args.filename):
            confirm = input(f"⚠️  WARNING: This will DELETE {args.filename}!\nAre you ABSOLUTELY SURE? (type 'DELETE' to confirm): ")
            if confirm == 'DELETE':
                os.remove(args.filename)
                print(f"🗑️  File {args.filename} has been deleted.")
            else:
                print("❌ Reset cancelled.")
        else:
            print(f"❌ File {args.filename} does not exist.")
        return
    
    print("📊 Facebook Groups Analysis Tool (Python Version)")
    print("=" * 60)
    
    # Analyze the file
    data = analyze_groups_file(args.filename)
    if not data:
        sys.exit(1)
    
    # Display statistics
    display_statistics(data)

if __name__ == "__main__":
    main() 