#!/usr/bin/env python3
"""
URL Generator for Facebook Groups Scraper

This script helps generate the facebook_group_urls.txt file from search terms.
"""

import os
import sys
import urllib.parse
from pathlib import Path

# Add the scripts directory to the path
SCRIPT_DIR = Path(__file__).parent
PARENT_DIR = SCRIPT_DIR.parent
URLS_FILE = PARENT_DIR / "settings" / "facebook_group_urls.txt"

def generate_url_from_search_term(search_term: str) -> str:
    """Generate Facebook groups search URL from search term"""
    # URL encode the search term
    encoded_term = urllib.parse.quote(search_term)
    return f"https://www.facebook.com/groups/search/groups_home/?q={encoded_term}"

def generate_urls_from_search_terms(search_terms: list) -> list:
    """Generate URLs from a list of search terms"""
    urls = []
    for term in search_terms:
        url = generate_url_from_search_term(term)
        urls.append(url)
    return urls

def save_urls_to_file(urls: list, filename: str = None):
    """Save URLs to file"""
    if filename is None:
        filename = URLS_FILE
    
    os.makedirs(filename.parent, exist_ok=True)
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("# Facebook Groups Search URLs\n")
        f.write("# Generated automatically - one URL per line\n")
        f.write("# Format: https://www.facebook.com/groups/search/groups_home/?q=City%2C+State\n\n")
        
        for url in urls:
            f.write(f"{url}\n")
    
    print(f"✅ Saved {len(urls)} URLs to {filename}")

def extract_search_terms_from_urls(urls: list) -> list:
    """Extract search terms from existing URLs"""
    search_terms = []
    
    for url in urls:
        if '?q=' in url:
            try:
                query_part = url.split('?q=')[1]
                search_term = urllib.parse.unquote(query_part)
                search_terms.append(search_term)
            except Exception as e:
                print(f"⚠️  Failed to parse URL: {url[:50]}... - {e}")
                continue
        else:
            print(f"⚠️  Invalid URL format: {url[:50]}...")
    
    return search_terms

def generate_sample_search_terms() -> list:
    """Generate sample search terms for testing"""
    cities = [
        "Dallas", "Houston", "Austin", "San Antonio", "Fort Worth", "Arlington", "Plano", "Irving",
        "Frisco", "McKinney", "Denton", "Garland", "Grand Prairie", "Mesquite", "Carrollton"
    ]
    
    states = ["TX", "CA", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI"]
    
    search_terms = []
    
    # Generate city + state combinations
    for city in cities:
        for state in states:
            search_terms.append(f"{city}, {state}")
    
    # Add some variations
    variations = ["neighborhood", "community", "local", "area"]
    for city in cities[:5]:
        for variation in variations:
            search_terms.append(f"{city} {variation}")
    
    return search_terms

def main():
    """Main function with interactive menu"""
    print("🔗 Facebook Groups URL Generator")
    print("=" * 40)
    
    while True:
        print("\n📋 Menu:")
        print("1. Generate URLs from search terms")
        print("2. Generate sample URLs")
        print("3. Convert existing URLs to search terms")
        print("4. View current URLs file")
        print("5. Exit")
        
        choice = input("\nSelect option (1-5): ").strip()
        
        if choice == "1":
            print("\nEnter search terms (one per line, press Enter twice to finish):")
            search_terms = []
            while True:
                term = input().strip()
                if not term:
                    break
                search_terms.append(term)
            
            if search_terms:
                urls = generate_urls_from_search_terms(search_terms)
                save_urls_to_file(urls)
            else:
                print("❌ No search terms entered")
        
        elif choice == "2":
            print("\nGenerating sample URLs...")
            search_terms = generate_sample_search_terms()
            urls = generate_urls_from_search_terms(search_terms)
            save_urls_to_file(urls)
            print(f"Generated {len(urls)} sample URLs")
        
        elif choice == "3":
            print("\nEnter URLs (one per line, press Enter twice to finish):")
            urls = []
            while True:
                url = input().strip()
                if not url:
                    break
                urls.append(url)
            
            if urls:
                search_terms = extract_search_terms_from_urls(urls)
                print(f"\nExtracted {len(search_terms)} search terms:")
                for term in search_terms:
                    print(f"  - {term}")
            else:
                print("❌ No URLs entered")
        
        elif choice == "4":
            if URLS_FILE.exists():
                print(f"\n📄 Current URLs file ({URLS_FILE}):")
                print("-" * 60)
                try:
                    with open(URLS_FILE, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        for i, line in enumerate(lines[:20], 1):  # Show first 20 lines
                            print(f"{i:3d}. {line.strip()}")
                        if len(lines) > 20:
                            print(f"... and {len(lines) - 20} more lines")
                except Exception as e:
                    print(f"❌ Error reading file: {e}")
            else:
                print(f"❌ URLs file not found: {URLS_FILE}")
        
        elif choice == "5":
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid option")

if __name__ == "__main__":
    main() 