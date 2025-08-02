#!/usr/bin/env python3
"""
Multi-Account Parallel Facebook Groups Scraper Launcher

This script launches the multi-account parallel scraper with configuration management.
"""

import json
import os
import sys
import time
from pathlib import Path

# Add the scripts directory to the path
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

def check_account_setup():
    """Check if accounts are properly set up"""
    accounts_file = Path(SCRIPT_DIR).parent / "settings" / "accounts.json"
    urls_file = Path(SCRIPT_DIR).parent / "settings" / "facebook_group_urls.txt"
    
    if not accounts_file.exists():
        print("❌ Accounts file not found!")
        print(f"Please create: {accounts_file}")
        print("\n📋 Setup Instructions:")
        print("1. Run: python scripts/manage_accounts.py")
        print("2. Use option 2 to add your Facebook accounts")
        print("3. Use option 5 to create cookie templates")
        print("4. Update cookie files with your Facebook session data")
        return False
    
    # Check if URLs file exists
    if not urls_file.exists():
        print(f"⚠️  URLs file not found: {urls_file}")
        print("The scraper will use hardcoded search terms instead.")
        print("To use custom search URLs, create this file with one URL per line.")
        print("Example URL format: https://www.facebook.com/groups/search/groups_home/?q=City%2C+State")
    else:
        print(f"✅ URLs file found: {urls_file}")
    
    with open(accounts_file, 'r') as f:
        config = json.load(f)
    
    accounts = config.get("accounts", [])
    if not accounts:
        print("❌ No accounts configured!")
        print("Please add accounts using: python scripts/manage_accounts.py")
        return False
    
    missing_cookies = []
    ready_accounts = []
    
    for account in accounts:
        cookie_file = Path(SCRIPT_DIR).parent / "settings" / "accounts" / account.get("cookie_file", "")
        if not cookie_file.exists():
            missing_cookies.append(account.get("cookie_file", "unknown"))
        else:
            try:
                with open(cookie_file, 'r') as f:
                    cookies = json.load(f)
                if cookies.get('c_user') and not cookies.get('c_user').startswith('YOUR_'):
                    ready_accounts.append(account)
                else:
                    missing_cookies.append(f"{account.get('cookie_file')} (needs updating)")
            except:
                missing_cookies.append(f"{account.get('cookie_file')} (invalid)")
    
    if missing_cookies:
        print("❌ Missing or incomplete cookie files:")
        for cookie in missing_cookies:
            print(f"   - {cookie}")
        print(f"\nPlease create/update cookie files in: settings/accounts/")
        print("Use: python scripts/manage_accounts.py (option 5)")
        return False
    
    print(f"✅ Account setup looks good! {len(ready_accounts)} accounts ready for scraping.")
    return True

def print_account_summary():
    """Print summary of account configuration"""
    accounts_file = Path(SCRIPT_DIR).parent / "settings" / "accounts.json"
    urls_file = Path(SCRIPT_DIR).parent / "settings" / "facebook_group_urls.txt"
    
    if not accounts_file.exists():
        return
    
    with open(accounts_file, 'r') as f:
        config = json.load(f)
    
    accounts = config.get("accounts", [])
    settings = config.get("settings", {})
    
    print("\n=== Multi-Account Configuration ===")
    print(f"Total accounts configured: {len(accounts)}")
    print(f"Max concurrent accounts: {settings.get('max_concurrent_accounts', 5)}")
    print(f"Max concurrent searches per account: {settings.get('max_concurrent_searches_per_account', 3)}")
    print(f"Rate limit delay: {settings.get('rate_limit_delay_min', 1.0)}-{settings.get('rate_limit_delay_max', 3.0)} seconds")
    print(f"Search cooldown: {settings.get('search_cooldown_min', 10.0)}-{settings.get('search_cooldown_max', 20.0)} seconds")
    
    # Check URLs file
    if urls_file.exists():
        try:
            with open(urls_file, 'r', encoding='utf-8') as f:
                url_count = sum(1 for line in f if line.strip() and not line.startswith('#'))
            print(f"Search URLs loaded: {url_count} URLs from {urls_file.name}")
        except:
            print("Search URLs: Error reading file")
    else:
        print("Search URLs: Using hardcoded search terms")
    
    # Calculate performance estimates
    total_workers = len(accounts) * settings.get('max_concurrent_searches_per_account', 3)
    avg_delay = (settings.get('rate_limit_delay_min', 1.0) + settings.get('rate_limit_delay_max', 3.0)) / 2
    
    print(f"\n=== Performance Estimates ===")
    print(f"Total concurrent workers: {total_workers}")
    print(f"Estimated calls per second: {total_workers / avg_delay:.1f}")
    
    # For 34,000 searches with 1,000 calls each
    total_calls = 34000 * 1000
    estimated_seconds = total_calls / (total_workers / avg_delay)
    estimated_hours = estimated_seconds / 3600
    estimated_days = estimated_hours / 24
    
    print(f"Estimated time for 34M calls: {estimated_days:.1f} days ({estimated_hours:.1f} hours)")
    print()

def validate_proxy_config():
    """Validate proxy configuration"""
    accounts_file = Path(SCRIPT_DIR).parent / "settings" / "accounts.json"
    
    if not accounts_file.exists():
        return False
    
    with open(accounts_file, 'r') as f:
        config = json.load(f)
    
    for account in config.get("accounts", []):
        proxy_config = account.get("proxy_config", {})
        if proxy_config.get("enabled", False):
            required_fields = ["host", "port", "username", "password"]
            for field in required_fields:
                if not proxy_config.get(field):
                    print(f"❌ Missing {field} in proxy config for account {account.get('account_id')}")
                    return False
    
    return True

def main():
    """Main launcher function"""
    print("🚀 Multi-Account Facebook Groups Parallel Scraper")
    print("=" * 60)
    
    # Check account setup
    if not check_account_setup():
        print("\n📝 To set up accounts:")
        print("1. Run: python scripts/manage_accounts.py")
        print("2. Add your Facebook accounts (option 2)")
        print("3. Create cookie templates (option 5)")
        print("4. Update cookie files with your Facebook session data")
        print("5. Run this script again")
        return 1
    
    # Validate proxy config
    if not validate_proxy_config():
        print("\n❌ Proxy configuration incomplete!")
        print("Please update your proxy credentials in settings/accounts.json")
        return 1
    
    # Print account summary
    print_account_summary()
    
    # Ask for confirmation
    response = input("Do you want to start the multi-account scraper? (y/N): ")
    if response.lower() != 'y':
        print("Scraper cancelled.")
        return 0
    
    # Import and run the multi-account scraper
    try:
        from multi_account_parallel_scraper import run_multi_account_parallel_scraper
        
        print("\n🚀 Starting multi-account parallel scraper...")
        print("Press Ctrl+C to stop the scraper (it will save progress)")
        print("-" * 60)
        
        start_time = time.time()
        results = run_multi_account_parallel_scraper()
        end_time = time.time()
        
        print("\n" + "=" * 60)
        print("🎉 MULTI-ACCOUNT SCRAPER COMPLETED!")
        print(f"⏱️  Total time: {(end_time - start_time) / 3600:.2f} hours")
        print(f"📁 Results saved to: output/")
        print("=" * 60)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⏸️  Scraper interrupted by user. Progress has been saved.")
        return 0
    except Exception as e:
        print(f"\n❌ Error running scraper: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 