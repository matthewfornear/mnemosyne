#!/usr/bin/env python3
"""
Enhanced Browser Profile Manager
Creates realistic browser fingerprints for each Facebook account
"""

import os
import json
import time
import random
import shutil
import psutil
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Get the script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths
ACCOUNTS_FILE = os.path.join(PARENT_DIR, "settings", "bought_accounts.json")
BROWSER_PROFILES_DIR = os.path.join(PARENT_DIR, "browser_profiles")
FINGERPRINT_CONFIG_FILE = os.path.join(PARENT_DIR, "settings", "browser_fingerprints.json")

def load_accounts():
    """Load accounts from bought_accounts.json"""
    with open(ACCOUNTS_FILE, "r") as f:
        data = json.load(f)
    return data.get("accounts", [])

def get_profile_dir(account_email):
    """Get browser profile directory for an account"""
    safe_email = account_email.replace("@", "_at_").replace(".", "_")
    return os.path.join(BROWSER_PROFILES_DIR, safe_email)

def create_realistic_fingerprint():
    """Create a realistic browser fingerprint"""
    # Common screen resolutions
    resolutions = [
        (1920, 1080), (1366, 768), (1440, 900), (1536, 864),
        (1280, 720), (1600, 900), (1680, 1050), (2560, 1440)
    ]
    
    # Common user agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/119.0.0.0"
    ]
    
    # Common timezones
    timezones = [
        "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
        "America/Phoenix", "America/Anchorage", "Pacific/Honolulu"
    ]
    
    # Common languages
    languages = [
        "en-US,en;q=0.9", "en-US,en;q=0.8", "en-US,en;q=0.7",
        "en-US,en;q=0.9,es;q=0.8", "en-US,en;q=0.9,fr;q=0.8"
    ]
    
    return {
        "resolution": random.choice(resolutions),
        "user_agent": random.choice(user_agents),
        "timezone": random.choice(timezones),
        "language": random.choice(languages),
        "color_depth": random.choice([24, 32]),
        "pixel_ratio": random.choice([1, 1.25, 1.5, 2]),
        "hardware_concurrency": random.choice([4, 6, 8, 12, 16]),
        "device_memory": random.choice([4, 8, 16, 32])
    }

def create_enhanced_browser_session(account_email):
    """Create an enhanced browser session with realistic fingerprint"""
    profile_dir = get_profile_dir(account_email)
    
    # Clean up existing profile
    if os.path.exists(profile_dir):
        try:
            shutil.rmtree(profile_dir)
            print(f"🧹 Cleaned up existing profile: {profile_dir}")
        except Exception as e:
            print(f"⚠️  Could not clean up profile: {e}")
    
    os.makedirs(profile_dir, exist_ok=True)
    
    # Create realistic fingerprint
    fingerprint = create_realistic_fingerprint()
    
    chrome_options = Options()
    chrome_options.add_argument(f"--user-data-dir={profile_dir}")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Set realistic window size
    width, height = fingerprint["resolution"]
    chrome_options.add_argument(f"--window-size={width},{height}")
    
    # Set user agent
    chrome_options.add_argument(f"--user-agent={fingerprint['user_agent']}")
    
    # Add timezone
    chrome_options.add_argument(f"--timezone={fingerprint['timezone']}")
    
    # Add language
    chrome_options.add_argument(f"--lang={fingerprint['language'].split(',')[0]}")
    
    # Disable automation detection
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-features=TranslateUI")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    
    # Add unique debugging port
    chrome_options.add_argument(f"--remote-debugging-port={random.randint(9222, 9999)}")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        # Execute scripts to make browser look more human
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        driver.execute_script("Object.defineProperty(navigator, 'deviceMemory', {get: () => " + str(fingerprint["device_memory"]) + "})")
        driver.execute_script("Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => " + str(fingerprint["hardware_concurrency"]) + "})")
        
        # Set screen properties
        driver.execute_script(f"Object.defineProperty(screen, 'width', {{get: () => {fingerprint['resolution'][0]}}})")
        driver.execute_script(f"Object.defineProperty(screen, 'height', {{get: () => {fingerprint['resolution'][1]}}})")
        driver.execute_script(f"Object.defineProperty(screen, 'colorDepth', {{get: () => {fingerprint['color_depth']}}})")
        driver.execute_script(f"Object.defineProperty(window, 'devicePixelRatio', {{get: () => {fingerprint['pixel_ratio']}}})")
        
        print(f"🔧 Created enhanced browser session for {account_email}")
        print(f"   Resolution: {fingerprint['resolution']}")
        print(f"   User Agent: {fingerprint['user_agent'][:50]}...")
        print(f"   Timezone: {fingerprint['timezone']}")
        
        return driver
    except Exception as e:
        print(f"❌ Failed to create enhanced browser session for {account_email}: {e}")
        return None

def setup_enhanced_account_session(account_email):
    """Set up an enhanced browser session for an account"""
    print(f"🔧 Setting up enhanced browser session for {account_email}")
    
    driver = create_enhanced_browser_session(account_email)
    if not driver:
        return False
    
    try:
        # Navigate to Facebook
        driver.get("https://www.facebook.com")
        time.sleep(3)
        
        # Check if already logged in
        if "login" not in driver.current_url.lower():
            print(f"✅ {account_email} already has an active session")
            return True
        
        print(f"📝 {account_email} needs to login")
        return True
        
    except Exception as e:
        print(f"❌ Error setting up session for {account_email}: {e}")
        return False
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def main():
    """Main function to set up enhanced browser sessions for all accounts"""
    print("🔧 Enhanced Browser Session Manager")
    print("=" * 50)
    
    # Kill any existing Chrome processes
    print("🔪 Cleaning up existing Chrome processes...")
    try:
        for proc in psutil.process_iter(['pid', 'name']):
            if 'chrome' in proc.info['name'].lower():
                try:
                    proc.kill()
                    print(f"🔪 Killed Chrome process: {proc.info['pid']}")
                except:
                    pass
        time.sleep(2)
    except Exception as e:
        print(f"⚠️  Could not kill Chrome processes: {e}")
    
    accounts = load_accounts()
    print(f"📋 Found {len(accounts)} accounts")
    
    successful = 0
    failed = 0
    
    for account in accounts:
        email = account["account"]
        print(f"\n🔧 Setting up enhanced session for {email}")
        
        if setup_enhanced_account_session(email):
            successful += 1
            print(f"✅ Enhanced session setup successful for {email}")
        else:
            failed += 1
            print(f"❌ Enhanced session setup failed for {email}")
        
        # Add delay between accounts
        time.sleep(3)
    
    print(f"\n{'='*50}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📁 Enhanced browser profiles saved to: {BROWSER_PROFILES_DIR}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main() 