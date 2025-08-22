import os
import sys
import time
import json
import random
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.firefox import GeckoDriverManager
import re
import hashlib

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
ACCOUNTS_FILE = os.path.join(PARENT_DIR, "settings", "bought_accounts.json")
COOKIES_DIR = os.path.join(PARENT_DIR, "settings", "cookies")
URLS_FILE = os.path.join(PARENT_DIR, "settings", "facebook_group_urls.txt")
HTML_OUTPUT_DIR = os.path.join(PARENT_DIR, "output", "html")

# Ensure output directory exists
os.makedirs(HTML_OUTPUT_DIR, exist_ok=True)

def load_accounts():
    """Load accounts from bought_accounts.json that have valid cookie files"""
    if not os.path.exists(ACCOUNTS_FILE):
        raise Exception(f"Accounts file '{ACCOUNTS_FILE}' not found.")
    
    with open(ACCOUNTS_FILE, "r") as f:
        data = json.load(f)
    
    all_accounts = data.get("accounts", [])
    if not all_accounts:
        raise Exception("No accounts found in bought_accounts.json")
    
    # Filter accounts to only include ones with valid cookie files
    valid_accounts = []
    for account in all_accounts:
        account_email = account["account"]
        
        cookie_file = get_cookie_file_path(account_email)
        if os.path.exists(cookie_file):
            try:
                # Test if the cookie file is valid
                with open(cookie_file, "r") as f:
                    cookies = json.load(f)
                if cookies.get("c_user"):
                    valid_accounts.append(account)
                    print(f"✅ Account {account_email} has valid cookies")
                else:
                    print(f"⚠️  Account {account_email} has cookies but missing c_user")
            except Exception as e:
                print(f"⚠️  Account {account_email} has invalid cookie file: {e}")
        else:
            print(f"❌ Account {account_email} missing cookie file: {cookie_file}")
    
    if not valid_accounts:
        raise Exception("No accounts with valid cookie files found. Please run: python scripts/generate_cookies.py")
    
    print(f"✅ Loaded {len(valid_accounts)} accounts with valid cookies")
    return valid_accounts

def get_cookie_file_path(account_email):
    """Get the cookie file path for a specific account"""
    safe_email = account_email.replace("@", "_at_").replace(".", "_")
    return os.path.join(COOKIES_DIR, f"{safe_email}_cookies.json")

def load_cookies_for_account(account_email):
    """Load cookies for a specific account"""
    cookie_file = get_cookie_file_path(account_email)
    
    if not os.path.exists(cookie_file):
        raise Exception(f"Cookie file not found: {cookie_file}")
    
    try:
        with open(cookie_file, "r") as f:
            cookies = json.load(f)
        return cookies
    except Exception as e:
        raise Exception(f"Failed to load cookies from {cookie_file}: {e}")

def load_search_urls_from_file():
    """Load search URLs from the URLs file"""
    try:
        if not os.path.exists(URLS_FILE):
            print(f"⚠️  URLs file not found: {URLS_FILE}")
            return []
        
        with open(URLS_FILE, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
        
        print(f"✅ Loaded {len(urls)} URLs from {URLS_FILE}")
        return urls
        
    except Exception as e:
        print(f"⚠️  Error loading URLs: {e}")
        return []

def extract_search_term_from_url(url):
    """Extract search term from Facebook Groups search URL"""
    try:
        # Parse the URL and extract the 'q' parameter
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        if 'q' in query_params:
            search_term = query_params['q'][0]
            # URL decode the search term
            search_term = urllib.parse.unquote_plus(search_term)
            return search_term
        else:
            print(f"⚠️  No 'q' parameter found in URL: {url}")
            return None
            
    except Exception as e:
        print(f"⚠️  Error extracting search term from URL {url}: {e}")
        return None

def generate_filename_from_url(url):
    """Generate a safe filename from the URL"""
    try:
        search_term = extract_search_term_from_url(url)
        if search_term:
            # Clean the search term for filename
            safe_filename = re.sub(r'[^\w\-_\. ]', '_', search_term)
            safe_filename = safe_filename.replace(' ', '_').replace('__', '_').strip('_')
            return f"{safe_filename}.html"
        else:
            # Fallback: use URL hash
            url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
            return f"facebook_groups_{url_hash}.html"
    except Exception as e:
        print(f"⚠️  Error generating filename: {e}")
        # Ultimate fallback
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        return f"facebook_groups_{url_hash}.html"

def setup_firefox_driver(account_email, cookies):
    """Setup Firefox driver with cookies for a specific account"""
    try:
        print(f"🔧 Setting up Firefox driver for {account_email}...")
        
        # Firefox options
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Use headless mode for efficiency (comment out to see browser)
        options.add_argument("--headless")
        
        # Setup Firefox service
        service = Service(GeckoDriverManager().install())
        
        # Create driver
        driver = webdriver.Firefox(service=service, options=options)
        
        # Set timeouts
        driver.implicitly_wait(2)
        driver.set_page_load_timeout(10)
        
        # Navigate to Facebook first
        print(f"   📍 Navigating to Facebook...")
        driver.get("https://www.facebook.com")
        
        # Wait a moment for the page to load
        time.sleep(1)
        
        # Add cookies
        print(f"   🍪 Adding cookies...")
        for cookie_name, cookie_value in cookies.items():
            if cookie_name not in ["session_headers", "session_payload", "bootstrap_headers", "bootstrap_payload"]:
                try:
                    # Ensure cookie value is a string
                    if isinstance(cookie_value, dict):
                        cookie_value = json.dumps(cookie_value)
                    elif not isinstance(cookie_value, str):
                        cookie_value = str(cookie_value)
                    
                    driver.add_cookie({
                        "name": cookie_name,
                        "value": cookie_value,
                        "domain": ".facebook.com"
                    })
                except Exception as e:
                    print(f"   ⚠️  Could not add cookie {cookie_name}: {e}")
                    continue
        
        print(f"   ✅ Firefox driver setup complete")
        return driver
        
    except Exception as e:
        print(f"❌ Error setting up Firefox driver: {e}")
        raise

def wait_for_graphql_requests(driver, timeout=5):
    """Wait for GraphQL requests to complete by monitoring network activity"""
    try:
        print(f"   ⏳ Waiting for GraphQL requests to complete...")
        
        # Wait for groups to appear on the page
        wait = WebDriverWait(driver, timeout)
        
        # Try to find group elements - various selectors Facebook might use
        group_selectors = [
            "[data-pagelet*='SearchResult']",
            "[role='article']",
            "[data-testid*='search-result']", 
            ".x1yztbdb",  # Common Facebook class for search results
            "[data-testid='groups_search_results']",
            ".search_result"
        ]
        
        groups_found = False
        for selector in group_selectors:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                print(f"   ✅ Found groups using selector: {selector}")
                groups_found = True
                break
            except TimeoutException:
                continue
        
        if not groups_found:
            print(f"   ⚠️  No groups found with standard selectors, waiting for page load...")
            # Fallback: just wait for the page to be in a ready state
            WebDriverWait(driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
        
        # Additional wait for any remaining async content
        print(f"   ⏱️  Waiting additional time for async content...")
        time.sleep(1)
        
        # Check if we can find any text that suggests groups are loaded
        page_source = driver.page_source.lower()
        if any(keyword in page_source for keyword in ["group", "member", "public", "private"]):
            print(f"   ✅ Page appears to contain group content")
        else:
            print(f"   ⚠️  Page may not have loaded group content properly")
        
        return True
        
    except TimeoutException:
        print(f"   ⚠️  Timeout waiting for GraphQL requests")
        return False
    except Exception as e:
        print(f"   ❌ Error waiting for GraphQL requests: {e}")
        return False

def scrape_url_html(driver, url, output_filename):
    """Scrape a single URL and save the HTML"""
    try:
        print(f"\n🌐 Loading URL: {url}")
        print(f"📄 Output file: {output_filename}")
        
        # Navigate to the URL
        driver.get(url)
        
        # Wait for GraphQL requests to complete
        wait_for_graphql_requests(driver)
        
        # Get the page source
        html_content = driver.page_source
        
        # Save to file
        output_path = os.path.join(HTML_OUTPUT_DIR, output_filename)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        print(f"✅ Saved HTML ({len(html_content):,} characters) to {output_path}")
        
        # Add a small delay between requests
        delay = random.uniform(0.5, 1.5)
        print(f"⏱️  Waiting {delay:.1f} seconds before next request...")
        time.sleep(delay)
        
        return True
        
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return False

def main():
    """Main scraper function"""
    print("🚀 Starting Facebook Groups HTML Scraper")
    print("=" * 50)
    
    # Load accounts
    try:
        accounts = load_accounts()
        if not accounts:
            print("❌ No valid accounts found")
            return
        
        # Use the first account for simplicity
        account = accounts[0]
        account_email = account["account"]
        print(f"📧 Using account: {account_email}")
        
    except Exception as e:
        print(f"❌ Error loading accounts: {e}")
        return
    
    # Load cookies
    try:
        cookies = load_cookies_for_account(account_email)
        print(f"🍪 Loaded {len(cookies)} cookies for {account_email}")
    except Exception as e:
        print(f"❌ Error loading cookies: {e}")
        return
    
    # Load URLs
    urls = load_search_urls_from_file()
    if not urls:
        print("❌ No URLs found to scrape")
        return
    
    print(f"📋 Found {len(urls)} URLs to scrape")
    
    # Setup Firefox driver
    driver = None
    try:
        driver = setup_firefox_driver(account_email, cookies)
        
        # Process each URL
        successful = 0
        failed = 0
        
        for i, url in enumerate(urls, 1):
            print(f"\n📍 Processing URL {i}/{len(urls)}")
            
            # Generate filename
            filename = generate_filename_from_url(url)
            output_path = os.path.join(HTML_OUTPUT_DIR, filename)
            
            # Skip if file already exists
            if os.path.exists(output_path):
                print(f"⏭️  Skipping {filename} (already exists)")
                continue
            
            # Scrape the URL
            if scrape_url_html(driver, url, filename):
                successful += 1
            else:
                failed += 1
        
        # Print summary
        print(f"\n📊 Scraping Summary:")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📁 HTML files saved to: {HTML_OUTPUT_DIR}")
        
    except Exception as e:
        print(f"❌ Critical error: {e}")
    finally:
        # Clean up
        if driver:
            print(f"🧹 Closing browser...")
            driver.quit()

if __name__ == "__main__":
    main() 