import json
import os
from collections import Counter

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Path to groups file
GROUPS_FILE = os.path.join(PARENT_DIR, "output", "groups.jsonl")

def count_cities():
    """Count unique cities from groups.jsonl file"""
    
    if not os.path.exists(GROUPS_FILE):
        print(f"❌ Groups file not found: {GROUPS_FILE}")
        return
    
    print(f"📁 Reading groups from: {GROUPS_FILE}")
    
    cities = set()
    total_groups = 0
    
    try:
        with open(GROUPS_FILE, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        group = json.loads(line)
                        total_groups += 1
                        
                        # Extract city_state if present
                        if "city_state" in group:
                            city_state = group["city_state"]
                            cities.add(city_state)
                        else:
                            # Try to extract from name or other fields
                            name = group.get("name", "")
                            if name:
                                # Look for city patterns in name
                                if "," in name:
                                    parts = name.split(",")
                                    if len(parts) >= 2:
                                        city_part = parts[0].strip()
                                        state_part = parts[1].strip()
                                        cities.add(f"{city_part}, {state_part}")
                        
                    except json.JSONDecodeError as e:
                        print(f"⚠️  Error parsing line {line_num}: {e}")
                        continue
                    except Exception as e:
                        print(f"⚠️  Error processing line {line_num}: {e}")
                        continue
    
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Sort cities for better display
    sorted_cities = sorted(list(cities))
    
    print(f"\n📊 CITY COUNT RESULTS:")
    print(f"🏙️  Total unique cities: {len(cities)}")
    print(f"📦 Total groups processed: {total_groups}")
    print(f"📈 Average groups per city: {total_groups / len(cities) if cities else 0:.1f}")
    
    print(f"\n📋 CITIES FOUND ({len(cities)} total):")
    for i, city in enumerate(sorted_cities, 1):
        print(f"  {i:3d}. {city}")
    
    # Show some statistics
    if cities:
        print(f"\n📈 STATISTICS:")
        print(f"   • Cities with 'TX': {len([c for c in cities if 'TX' in c])}")
        print(f"   • Cities with 'NE': {len([c for c in cities if 'NE' in c])}")
        print(f"   • Cities with 'OH': {len([c for c in cities if 'OH' in c])}")
        print(f"   • Cities with 'CA': {len([c for c in cities if 'CA' in c])}")
        print(f"   • Cities with 'NY': {len([c for c in cities if 'NY' in c])}")
        
        # Count by state
        state_counts = Counter()
        for city in cities:
            if "," in city:
                state = city.split(",")[1].strip()
                state_counts[state] += 1
        
        print(f"\n🗺️  TOP STATES:")
        for state, count in state_counts.most_common(10):
            print(f"   • {state}: {count} cities")
    
    # FINAL SUMMARY - Make this very prominent
    print(f"\n" + "="*60)
    print(f"🎯 FINAL SUMMARY")
    print(f"="*60)
    print(f"📊 TOTAL CITIES PARSED: {len(cities):,}")
    print(f"📦 TOTAL GROUPS PROCESSED: {total_groups:,}")
    print(f"📈 AVERAGE GROUPS PER CITY: {total_groups / len(cities) if cities else 0:.1f}")
    print(f"="*60)

if __name__ == "__main__":
    count_cities() 