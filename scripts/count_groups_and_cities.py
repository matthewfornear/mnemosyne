#!/usr/bin/env python3
"""
Count Groups and Cities - Comprehensive count from all sources
Counts every group in every JSONL file and returns deduplicated counts
"""

import json
from pathlib import Path
from collections import Counter, defaultdict

# Configuration
PARENT_DIR = Path(__file__).parent.parent
OUTPUT_DIR = PARENT_DIR / "output" / "curl"
ENRICHED_GROUPS_FILE = OUTPUT_DIR / "groups_output_enriched.jsonl"

def find_worker_output_files():
    """Find all worker output files"""
    pattern = "groups_output_curl_worker_*.json"
    worker_files = list(OUTPUT_DIR.glob(pattern))
    return worker_files

def extract_city_from_search_term(search_term):
    """Extract city from search term like 'Bettles, AK' -> 'Bettles'"""
    if not search_term or search_term == "Unknown":
        return "Unknown"
    
    # Split by comma and take the first part (city)
    parts = search_term.split(',')
    if len(parts) >= 1:
        return parts[0].strip()
    return search_term.strip()

def count_from_worker_files():
    """Count groups and cities from all worker files"""
    worker_files = find_worker_output_files()
    total_groups = 0
    unique_group_ids = set()
    unique_cities = set()
    city_groups = defaultdict(int)
    
    print(f"🔍 Found {len(worker_files)} worker output files")
    
    for worker_file in worker_files:
        try:
            worker_id = worker_file.stem.replace('groups_output_curl_worker_', '')
            worker_groups = 0
            worker_cities = set()
            
            with open(worker_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            group = json.loads(line)
                            worker_groups += 1
                            total_groups += 1
                            
                            # Track unique group IDs
                            group_id = group.get('id')
                            if group_id:
                                unique_group_ids.add(group_id)
                            
                            # Track unique cities
                            search_term = group.get('search_term', 'Unknown')
                            city = extract_city_from_search_term(search_term)
                            if city != "Unknown":
                                unique_cities.add(city)
                                worker_cities.add(city)
                                city_groups[city] += 1
                                
                        except json.JSONDecodeError:
                            continue
            
            print(f"   📁 Worker {worker_id}: {worker_groups:,} groups, {len(worker_cities):,} unique cities")
                    
        except Exception as e:
            print(f"   ❌ Error reading worker {worker_id}: {e}")
    
    return total_groups, len(unique_group_ids), len(unique_cities), city_groups

def count_from_enriched_file():
    """Count groups and cities from enriched file"""
    if not ENRICHED_GROUPS_FILE.exists():
        print("❌ Enriched groups file not found")
        return 0, 0, 0, {}
    
    try:
        total_groups = 0
        unique_group_ids = set()
        unique_cities = set()
        city_groups = defaultdict(int)
        
        with open(ENRICHED_GROUPS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        group = json.loads(line)
                        total_groups += 1
                        
                        # Track unique group IDs
                        group_id = group.get('id')
                        if group_id:
                            unique_group_ids.add(group_id)
                        
                        # Track unique cities
                        search_term = group.get('search_term', 'Unknown')
                        city = extract_city_from_search_term(search_term)
                        if city != "Unknown":
                            unique_cities.add(city)
                            city_groups[city] += 1
                            
                    except json.JSONDecodeError:
                        continue
        
        print(f"📁 Enriched file: {total_groups:,} groups, {len(unique_cities):,} unique cities")
        return total_groups, len(unique_group_ids), len(unique_cities), city_groups
        
    except Exception as e:
        print(f"❌ Error reading enriched groups: {e}")
        return 0, 0, 0, {}

def count_all_sources():
    """Count from all sources and combine results"""
    print("🔍 Counting from all sources...")
    
    # Count from worker files
    worker_groups, worker_unique_groups, worker_unique_cities, worker_city_groups = count_from_worker_files()
    
    print()
    
    # Count from enriched file
    enriched_groups, enriched_unique_groups, enriched_unique_cities, enriched_city_groups = count_from_enriched_file()
    
    print()
    
    # Combine all data
    all_groups = worker_groups + enriched_groups
    
    # Combine unique group IDs (need to load actual IDs to deduplicate)
    print("🔄 Combining and deduplicating data...")
    all_unique_group_ids = set()
    all_unique_cities = set()
    all_city_groups = defaultdict(int)
    
    # Process worker files for deduplication
    worker_files = find_worker_output_files()
    for worker_file in worker_files:
        try:
            with open(worker_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            group = json.loads(line)
                            group_id = group.get('id')
                            if group_id:
                                all_unique_group_ids.add(group_id)
                            
                            search_term = group.get('search_term', 'Unknown')
                            city = extract_city_from_search_term(search_term)
                            if city != "Unknown":
                                all_unique_cities.add(city)
                                all_city_groups[city] += 1
                                
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            continue
    
    # Process enriched file for deduplication
    if ENRICHED_GROUPS_FILE.exists():
        try:
            with open(ENRICHED_GROUPS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            group = json.loads(line)
                            group_id = group.get('id')
                            if group_id:
                                all_unique_group_ids.add(group_id)
                            
                            search_term = group.get('search_term', 'Unknown')
                            city = extract_city_from_search_term(search_term)
                            if city != "Unknown":
                                all_unique_cities.add(city)
                                all_city_groups[city] += 1
                                
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            pass
    
    return all_groups, len(all_unique_group_ids), len(all_unique_cities), dict(all_city_groups)

def print_top_cities(city_groups, limit=20):
    """Print top cities by group count"""
    if not city_groups:
        return
    
    print(f"\n🏙️  TOP {limit} CITIES BY GROUP COUNT:")
    print("-" * 50)
    
    sorted_cities = sorted(city_groups.items(), key=lambda x: x[1], reverse=True)
    for i, (city, count) in enumerate(sorted_cities[:limit], 1):
        print(f"   {i:2d}. {city}: {count:,} groups")
    
    if len(sorted_cities) > limit:
        print(f"   ... and {len(sorted_cities) - limit} more cities")

def main():
    print("🚀 COMPREHENSIVE GROUPS AND CITIES COUNT")
    print("=" * 60)
    
    # Count from all sources
    total_groups, unique_groups, unique_cities, city_groups = count_all_sources()
    
    print()
    print("🎯 FINAL RESULTS:")
    print("=" * 40)
    print(f"📊 Total groups found: {total_groups:,}")
    print(f"🆔 Unique groups (deduplicated): {unique_groups:,}")
    print(f"🏙️  Unique cities processed: {unique_cities:,}")
    
    if total_groups > 0:
        duplicate_rate = ((total_groups - unique_groups) / total_groups * 100)
        print(f"🔄 Group duplicate rate: {duplicate_rate:.1f}%")
    
    # Show top cities
    print_top_cities(city_groups, 20)
    
    print()
    print("📋 SUMMARY:")
    print(f"   • Total groups across all sources: {total_groups:,}")
    print(f"   • Unique groups (no duplicates): {unique_groups:,}")
    print(f"   • Unique cities processed: {unique_cities:,}")
    print(f"   • Average groups per city: {unique_groups / max(unique_cities, 1):.1f}")

if __name__ == "__main__":
    main() 