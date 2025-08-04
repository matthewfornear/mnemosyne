import json
import os
from collections import Counter

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths to files
ENRICHED_FILE = os.path.join(PARENT_DIR, "output", "groups_enriched.jsonl")
SAMPLE_FILE = os.path.join(PARENT_DIR, "output", "groups_enriched_sample.jsonl")

def create_sample():
    """Create a sample file with groups from the 3 most populous cities"""
    
    if not os.path.exists(ENRICHED_FILE):
        print(f"❌ Enriched file not found: {ENRICHED_FILE}")
        return
    
    print(f"📁 Reading enriched groups from: {ENRICHED_FILE}")
    
    # Load all groups and count by city
    groups_by_city = {}
    all_groups = []
    
    try:
        with open(ENRICHED_FILE, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        group = json.loads(line)
                        all_groups.append(group)
                        
                        # Extract city_state if present
                        if "city_state" in group:
                            city_state = group["city_state"]
                            if city_state not in groups_by_city:
                                groups_by_city[city_state] = []
                            groups_by_city[city_state].append(group)
                        else:
                            # Try to extract from name or other fields
                            name = group.get("name", "")
                            if name and "," in name:
                                parts = name.split(",")
                                if len(parts) >= 2:
                                    city_part = parts[0].strip()
                                    state_part = parts[1].strip()
                                    city_state = f"{city_part}, {state_part}"
                                    if city_state not in groups_by_city:
                                        groups_by_city[city_state] = []
                                    groups_by_city[city_state].append(group)
                        
                    except json.JSONDecodeError as e:
                        print(f"⚠️  Error parsing line {line_num}: {e}")
                        continue
                    except Exception as e:
                        print(f"⚠️  Error processing line {line_num}: {e}")
                        continue
    
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Count groups per city
    city_counts = {city: len(groups) for city, groups in groups_by_city.items()}
    
    # Find the 3 most populous cities
    top_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    
    print(f"\n📊 ANALYSIS RESULTS:")
    print(f"🏙️  Total cities found: {len(city_counts)}")
    print(f"📦 Total groups processed: {len(all_groups)}")
    
    print(f"\n🏆 TOP 3 MOST POPULOUS CITIES:")
    for i, (city, count) in enumerate(top_cities, 1):
        print(f"  {i}. {city}: {count} groups")
    
    # Collect all groups from the top 3 cities
    sample_groups = []
    for city, count in top_cities:
        sample_groups.extend(groups_by_city[city])
        print(f"  📋 Added {count} groups from {city}")
    
    # Write sample file
    try:
        with open(SAMPLE_FILE, "w", encoding="utf-8") as f:
            for group in sample_groups:
                f.write(json.dumps(group, ensure_ascii=False) + "\n")
        
        print(f"\n✅ SAMPLE FILE CREATED:")
        print(f"📁 File: {SAMPLE_FILE}")
        print(f"📦 Total groups in sample: {len(sample_groups)}")
        print(f"🏙️  Cities included: {len(top_cities)}")
        
        # Show some statistics about the sample
        print(f"\n📈 SAMPLE STATISTICS:")
        total_members = 0
        groups_with_members = 0
        
        for group in sample_groups:
            member_count = group.get("member_count", "")
            if member_count and "K" in member_count:
                try:
                    # Extract number from "1.2K members" format
                    num_str = member_count.split("K")[0]
                    num = float(num_str) * 1000
                    total_members += num
                    groups_with_members += 1
                except:
                    pass
            elif member_count and "members" in member_count:
                try:
                    # Extract number from "1200 members" format
                    num_str = member_count.split(" ")[0].replace(",", "")
                    num = int(num_str)
                    total_members += num
                    groups_with_members += 1
                except:
                    pass
        
        if groups_with_members > 0:
            avg_members = total_members / groups_with_members
            print(f"   • Groups with member count: {groups_with_members}")
            print(f"   • Total members: {total_members:,.0f}")
            print(f"   • Average members per group: {avg_members:,.0f}")
        
        # Count privacy types
        privacy_counts = Counter()
        for group in sample_groups:
            privacy = group.get("privacy", "Unknown")
            privacy_counts[privacy] += 1
        
        print(f"   • Privacy breakdown:")
        for privacy, count in privacy_counts.most_common():
            print(f"     - {privacy}: {count} groups")
        
    except Exception as e:
        print(f"❌ Error writing sample file: {e}")
        return

if __name__ == "__main__":
    create_sample() 