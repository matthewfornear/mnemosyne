#!/usr/bin/env python3
"""
Parallel Facebook Groups Scraper Launcher

This script launches the parallel scraper with configuration management.
"""

import json
import os
import sys
import time
from pathlib import Path

# Add the scripts directory to the path
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

def load_config():
    """Load configuration from parallel_config.json"""
    config_file = SCRIPT_DIR / "parallel_config.json"
    if not config_file.exists():
        print(f"Configuration file not found: {config_file}")
        print("Please create parallel_config.json with your settings.")
        return None
    
    with open(config_file, 'r') as f:
        return json.load(f)

def validate_config(config):
    """Validate the configuration"""
    required_sections = ['scaling', 'rate_limiting', 'proxy', 'output', 'search_terms', 'facebook']
    
    for section in required_sections:
        if section not in config:
            print(f"Missing required configuration section: {section}")
            return False
    
    # Validate scaling settings
    scaling = config['scaling']
    if scaling['total_searches'] <= 0:
        print("total_searches must be greater than 0")
        return False
    
    if scaling['target_calls_per_search'] <= 0:
        print("target_calls_per_search must be greater than 0")
        return False
    
    if scaling['max_concurrent_searches'] <= 0:
        print("max_concurrent_searches must be greater than 0")
        return False
    
    return True

def print_summary(config):
    """Print a summary of the configuration"""
    scaling = config['scaling']
    rate_limiting = config['rate_limiting']
    
    print("=== Parallel Facebook Groups Scraper Configuration ===")
    print(f"Total searches: {scaling['total_searches']:,}")
    print(f"Calls per search: {scaling['target_calls_per_search']:,}")
    print(f"Max concurrent searches: {scaling['max_concurrent_searches']}")
    print(f"Rate limit delay: {rate_limiting['rate_limit_delay_min']}-{rate_limiting['rate_limit_delay_max']} seconds")
    print(f"Search cooldown: {rate_limiting['search_cooldown_min']}-{rate_limiting['search_cooldown_max']} seconds")
    print(f"Proxy enabled: {config['proxy']['enabled']}")
    print(f"Output directory: {config['output']['output_directory']}")
    print()
    
    # Calculate estimated time
    total_calls = scaling['total_searches'] * scaling['target_calls_per_search']
    avg_delay = (rate_limiting['rate_limit_delay_min'] + rate_limiting['rate_limit_delay_max']) / 2
    concurrent_factor = scaling['max_concurrent_searches']
    
    estimated_seconds = (total_calls * avg_delay) / concurrent_factor
    estimated_hours = estimated_seconds / 3600
    estimated_days = estimated_hours / 24
    
    print(f"Estimated total calls: {total_calls:,}")
    print(f"Estimated time: {estimated_days:.1f} days ({estimated_hours:.1f} hours)")
    print()

def main():
    """Main launcher function"""
    print("Facebook Groups Parallel Scraper Launcher")
    print("=" * 50)
    
    # Load configuration
    config = load_config()
    if not config:
        return 1
    
    # Validate configuration
    if not validate_config(config):
        return 1
    
    # Print summary
    print_summary(config)
    
    # Ask for confirmation
    response = input("Do you want to start the parallel scraper? (y/N): ")
    if response.lower() != 'y':
        print("Scraper cancelled.")
        return 0
    
    # Import and run the parallel scraper
    try:
        from facebook_groups_scraper_parallel import run_parallel_scraper
        
        print("\nStarting parallel scraper...")
        print("Press Ctrl+C to stop the scraper (it will save progress)")
        print("-" * 50)
        
        start_time = time.time()
        results = run_parallel_scraper()
        end_time = time.time()
        
        print("\n" + "=" * 50)
        print("SCRAPER COMPLETED!")
        print(f"Total time: {(end_time - start_time) / 3600:.2f} hours")
        print(f"Results saved to: {config['output']['output_directory']}")
        print("=" * 50)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\nScraper interrupted by user. Progress has been saved.")
        return 0
    except Exception as e:
        print(f"\nError running scraper: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 