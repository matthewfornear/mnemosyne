#!/usr/bin/env python3
"""
Facebook Account Manager

This script helps you manage multiple Facebook accounts for the multi-account scraper.
"""

import json
import os
import sys
from pathlib import Path
from typing import List, Dict

# Add the scripts directory to the path
SCRIPT_DIR = Path(__file__).parent
PARENT_DIR = SCRIPT_DIR.parent
ACCOUNTS_FILE = PARENT_DIR / "settings" / "accounts.json"
ACCOUNTS_DIR = PARENT_DIR / "settings" / "accounts"

def load_accounts() -> Dict:
    """Load accounts configuration"""
    if not ACCOUNTS_FILE.exists():
        print(f"❌ Accounts file not found: {ACCOUNTS_FILE}")
        print("Creating new accounts file...")
        return {"accounts": [], "settings": {
            "max_concurrent_accounts": 5,
            "max_concurrent_searches_per_account": 3,
            "rate_limit_delay_min": 1.0,
            "rate_limit_delay_max": 3.0,
            "search_cooldown_min": 10.0,
            "search_cooldown_max": 20.0
        }}
    
    with open(ACCOUNTS_FILE, 'r') as f:
        return json.load(f)

def save_accounts(accounts_data: Dict):
    """Save accounts configuration"""
    os.makedirs(ACCOUNTS_FILE.parent, exist_ok=True)
    with open(ACCOUNTS_FILE, 'w') as f:
        json.dump(accounts_data, f, indent=4)

def add_account(email: str, password: str, facebook_email: str = None, facebook_password: str = None, account_id: str = None, proxy_config: Dict = None):
    """Add a new Facebook account"""
    accounts_data = load_accounts()
    
    if account_id is None:
        account_id = f"account_{len(accounts_data['accounts']) + 1}"
    
    # Use facebook credentials if provided, otherwise use regular email/password
    if facebook_email is None:
        facebook_email = email
    if facebook_password is None:
        facebook_password = password
    
    if proxy_config is None:
        proxy_config = {
            "enabled": True,
            "host": f"ip{len(accounts_data['accounts']) + 1}.nimbleway.com",
            "port": str(7000 + len(accounts_data['accounts'])),
            "username": f"your_nimble_username_{len(accounts_data['accounts']) + 1}",
            "password": f"your_nimble_password_{len(accounts_data['accounts']) + 1}"
        }
    
    new_account = {
        "account_id": account_id,
        "email": email,
        "password": password,
        "facebook_email": facebook_email,
        "facebook_password": facebook_password,
        "cookie_file": f"cookie_{len(accounts_data['accounts']) + 1}.json",
        "proxy_config": proxy_config
    }
    
    accounts_data['accounts'].append(new_account)
    save_accounts(accounts_data)
    
    print(f"✅ Added account: {account_id}")
    print(f"   Email: {email}")
    print(f"   Facebook Email: {facebook_email}")
    return new_account

def list_accounts():
    """List all configured accounts"""
    accounts_data = load_accounts()
    
    if not accounts_data['accounts']:
        print("📝 No accounts configured yet.")
        return
    
    print(f"\n📋 Configured Accounts ({len(accounts_data['accounts'])}):")
    print("-" * 80)
    
    for i, account in enumerate(accounts_data['accounts'], 1):
        print(f"{i}. {account['account_id']}")
        print(f"   Email: {account['email']}")
        print(f"   Facebook Email: {account['facebook_email']}")
        print(f"   Cookie file: {account['cookie_file']}")
        print(f"   Proxy: {account['proxy_config']['host']}:{account['proxy_config']['port']}")
        print()

def remove_account(account_id: str):
    """Remove an account by ID"""
    accounts_data = load_accounts()
    
    for i, account in enumerate(accounts_data['accounts']):
        if account['account_id'] == account_id:
            removed = accounts_data['accounts'].pop(i)
            save_accounts(accounts_data)
            print(f"✅ Removed account: {account_id}")
            return True
    
    print(f"❌ Account not found: {account_id}")
    return False

def update_account(account_id: str, **kwargs):
    """Update account fields"""
    accounts_data = load_accounts()
    
    for account in accounts_data['accounts']:
        if account['account_id'] == account_id:
            for key, value in kwargs.items():
                if key in account:
                    account[key] = value
                    print(f"✅ Updated {account_id}.{key} = {value}")
                elif key == 'proxy_config':
                    account['proxy_config'].update(value)
                    print(f"✅ Updated {account_id}.proxy_config")
            
            save_accounts(accounts_data)
            return True
    
    print(f"❌ Account not found: {account_id}")
    return False

def create_cookie_template(account_id: str):
    """Create a cookie template file for an account"""
    accounts_data = load_accounts()
    
    for account in accounts_data['accounts']:
        if account['account_id'] == account_id:
            cookie_file = ACCOUNTS_DIR / account['cookie_file']
            os.makedirs(ACCOUNTS_DIR, exist_ok=True)
            
            template = {
                "c_user": f"YOUR_FACEBOOK_USER_ID_{account_id.upper()}",
                "datr": f"YOUR_DATR_COOKIE_{account_id.upper()}",
                "oo": f"YOUR_OO_COOKIE_{account_id.upper()}",
                "presence": f"YOUR_PRESENCE_COOKIE_{account_id.upper()}",
                "ps_l": "1",
                "ps_n": "1",
                "sb": f"YOUR_SB_COOKIE_{account_id.upper()}",
                "wd": "1132x1311",
                "xs": f"YOUR_XS_COOKIE_{account_id.upper()}"
            }
            
            with open(cookie_file, 'w') as f:
                json.dump(template, f, indent=4)
            
            print(f"✅ Created cookie template: {cookie_file}")
            print(f"📝 Please update the cookie values with your Facebook session data")
            print(f"   Account: {account_id}")
            print(f"   Facebook Email: {account['facebook_email']}")
            return True
    
    print(f"❌ Account not found: {account_id}")
    return False

def bulk_add_accounts(accounts_list: List[Dict]):
    """Add multiple accounts at once"""
    accounts_data = load_accounts()
    
    for account_info in accounts_list:
        account_id = account_info.get('account_id', f"account_{len(accounts_data['accounts']) + 1}")
        email = account_info['email']
        password = account_info['password']
        facebook_email = account_info.get('facebook_email', email)
        facebook_password = account_info.get('facebook_password', password)
        
        proxy_config = account_info.get('proxy_config', {
            "enabled": True,
            "host": f"ip{len(accounts_data['accounts']) + 1}.nimbleway.com",
            "port": str(7000 + len(accounts_data['accounts'])),
            "username": f"your_nimble_username_{len(accounts_data['accounts']) + 1}",
            "password": f"your_nimble_password_{len(accounts_data['accounts']) + 1}"
        })
        
        new_account = {
            "account_id": account_id,
            "email": email,
            "password": password,
            "facebook_email": facebook_email,
            "facebook_password": facebook_password,
            "cookie_file": f"cookie_{len(accounts_data['accounts']) + 1}.json",
            "proxy_config": proxy_config
        }
        
        accounts_data['accounts'].append(new_account)
        print(f"✅ Added account: {account_id} ({email})")
    
    save_accounts(accounts_data)
    print(f"\n🎉 Added {len(accounts_list)} accounts successfully!")

def get_accounts_for_scraping() -> List[Dict]:
    """Get accounts ready for scraping (with cookies)"""
    accounts_data = load_accounts()
    ready_accounts = []
    
    for account in accounts_data['accounts']:
        cookie_file = ACCOUNTS_DIR / account['cookie_file']
        if cookie_file.exists():
            try:
                with open(cookie_file, 'r') as f:
                    cookies = json.load(f)
                if cookies.get('c_user') and not cookies.get('c_user').startswith('YOUR_'):
                    ready_accounts.append(account)
                else:
                    print(f"⚠️  {account['account_id']}: Cookie file exists but needs updating")
            except:
                print(f"⚠️  {account['account_id']}: Invalid cookie file")
        else:
            print(f"❌ {account['account_id']}: Missing cookie file")
    
    return ready_accounts

def main():
    """Main function with interactive menu"""
    print("🔐 Facebook Account Manager")
    print("=" * 40)
    
    while True:
        print("\n📋 Menu:")
        print("1. List accounts")
        print("2. Add account")
        print("3. Remove account")
        print("4. Update account")
        print("5. Create cookie template")
        print("6. Check ready accounts")
        print("7. Bulk add accounts")
        print("8. Exit")
        
        choice = input("\nSelect option (1-8): ").strip()
        
        if choice == "1":
            list_accounts()
        
        elif choice == "2":
            email = input("Email: ").strip()
            password = input("Password: ").strip()
            facebook_email = input("Facebook Email (optional, press Enter to use same as email): ").strip() or None
            facebook_password = input("Facebook Password (optional, press Enter to use same as password): ").strip() or None
            account_id = input("Account ID (optional): ").strip() or None
            add_account(email, password, facebook_email, facebook_password, account_id)
        
        elif choice == "3":
            list_accounts()
            account_id = input("Account ID to remove: ").strip()
            remove_account(account_id)
        
        elif choice == "4":
            list_accounts()
            account_id = input("Account ID to update: ").strip()
            field = input("Field to update (email/password/facebook_email/facebook_password/proxy_config): ").strip()
            value = input("New value: ").strip()
            
            if field == "proxy_config":
                print("Enter proxy config (JSON format):")
                try:
                    value = json.loads(input())
                except:
                    print("❌ Invalid JSON")
                    continue
            
            update_account(account_id, **{field: value})
        
        elif choice == "5":
            list_accounts()
            account_id = input("Account ID for cookie template: ").strip()
            create_cookie_template(account_id)
        
        elif choice == "6":
            ready_accounts = get_accounts_for_scraping()
            print(f"\n✅ Ready accounts: {len(ready_accounts)}")
            for account in ready_accounts:
                print(f"   - {account['account_id']} ({account['facebook_email']})")
        
        elif choice == "7":
            print("Enter accounts in JSON format:")
            print('Example: [{"email": "user1@example.com", "password": "pass1", "facebook_email": "fb1@example.com", "facebook_password": "fbpass1"}, ...]')
            try:
                accounts_list = json.loads(input())
                bulk_add_accounts(accounts_list)
            except:
                print("❌ Invalid JSON format")
        
        elif choice == "8":
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid option")

if __name__ == "__main__":
    main() 