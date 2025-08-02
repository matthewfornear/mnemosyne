import requests
import json
import time
import os
import sys
import zstandard as zstd
import io
import random
import datetime
import threading
import queue
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import List, Dict, Optional, Tuple
import hashlib
from dataclasses import dataclass
from itertools import cycle
import urllib.parse

# Ensure script directory is the working directory for relative paths
os.chdir(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))

# Paths relative to project root
ACCOUNTS_DIR = os.path.join(PARENT_DIR, "settings", "accounts")
OUTPUT_DIR = os.path.join(PARENT_DIR, "output")
STATE_DIR = os.path.join(PARENT_DIR, "output", "states")

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(ACCOUNTS_DIR, exist_ok=True)

# --- Configuration ---
TOTAL_SEARCHES = 34000
TARGET_CALLS_PER_SEARCH = 1000
MAX_CONCURRENT_ACCOUNTS = 5  # Number of Facebook accounts to use simultaneously
MAX_CONCURRENT_SEARCHES_PER_ACCOUNT = 3  # Searches per account
MAX_CONCURRENT_REQUESTS_PER_SEARCH = 2  # GraphQL calls per search
RATE_LIMIT_DELAY = (1.0, 3.0)  # Reduced delay since we have multiple accounts
SEARCH_COOLDOWN = (10.0, 20.0)  # Reduced cooldown
ACCOUNT_ROTATION_INTERVAL = 50  # Calls before rotating accounts

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'multi_account_scraper.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Account Management ---
@dataclass
class FacebookAccount:
    account_id: str
    cookies: Dict
    headers: Dict
    proxy: Optional[Dict]
    session_params: Dict
    call_count: int = 0
    last_used: float = 0.0
    lock: threading.Lock = None
    
    def __post_init__(self):
        if self.lock is None:
            self.lock = threading.Lock()

class AccountManager:
    def __init__(self):
        self.accounts = []
        self.account_cycle = None
        self.load_accounts()
        self.account_cycle = cycle(self.accounts)
        logger.info(f"Loaded {len(self.accounts)} Facebook accounts")
    
    def load_accounts(self):
        """Load multiple Facebook accounts from settings/accounts.json"""
        # Load account configurations
        accounts_file = os.path.join(PARENT_DIR, "settings", "accounts.json")
        
        if os.path.exists(accounts_file):
            with open(accounts_file, "r") as f:
                accounts_config = json.load(f)
        else:
            # Create sample accounts config
            accounts_config = {
                "accounts": [
                    {
                        "account_id": "account_1",
                        "email": "your_email_1@example.com",
                        "password": "your_password_1",
                        "cookie_file": "cookie_1.json",
                        "proxy_config": {
                            "enabled": True,
                            "host": "ip1.nimbleway.com",
                            "port": "7000",
                            "username": "user1",
                            "password": "pass1"
                        }
                    },
                    {
                        "account_id": "account_2", 
                        "email": "your_email_2@example.com",
                        "password": "your_password_2",
                        "cookie_file": "cookie_2.json",
                        "proxy_config": {
                            "enabled": True,
                            "host": "ip2.nimbleway.com",
                            "port": "7001",
                            "username": "user2",
                            "password": "pass2"
                        }
                    }
                ],
                "settings": {
                    "max_concurrent_accounts": 5,
                    "max_concurrent_searches_per_account": 3,
                    "rate_limit_delay_min": 1.0,
                    "rate_limit_delay_max": 3.0,
                    "search_cooldown_min": 10.0,
                    "search_cooldown_max": 20.0
                }
            }
            
            # Save sample config
            with open(accounts_file, "w") as f:
                json.dump(accounts_config, f, indent=4)
        
        # Load each account
        for account_config in accounts_config["accounts"]:
            account = self.load_single_account(account_config)
            if account:
                self.accounts.append(account)
    
    def load_single_account(self, account_config: Dict) -> Optional[FacebookAccount]:
        """Load a single Facebook account"""
        try:
            account_id = account_config["account_id"]
            email = account_config.get("email", "unknown@example.com")
            password = account_config.get("password", "")
            facebook_email = account_config.get("facebook_email", email)
            facebook_password = account_config.get("facebook_password", password)
            cookie_file = os.path.join(ACCOUNTS_DIR, account_config["cookie_file"])
            
            if not os.path.exists(cookie_file):
                logger.warning(f"Cookie file not found for {account_id}: {cookie_file}")
                logger.info(f"Account {account_id} ({facebook_email}) - Please create cookie file")
                return None
            
            # Load cookies
            with open(cookie_file, "r") as f:
                cookies = json.load(f)
            
            # Check if cookies are properly configured
            if not cookies.get("c_user") or cookies.get("c_user").startswith("YOUR_"):
                logger.warning(f"Account {account_id} ({facebook_email}) - Cookie file needs updating")
                return None
            
            # Generate headers
            headers = self.generate_headers(account_id)
            
            # Setup proxy
            proxy = None
            if account_config.get("proxy_config", {}).get("enabled", False):
                proxy_config = account_config["proxy_config"]
                proxy_url = f"http://{proxy_config['username']}:{proxy_config['password']}@{proxy_config['host']}:{proxy_config['port']}"
                proxy = {"http": proxy_url, "https": proxy_url}
            
            # Generate session parameters (these should be unique per account)
            session_params = self.generate_session_params(account_id)
            
            account = FacebookAccount(
                account_id=account_id,
                cookies=cookies,
                headers=headers,
                proxy=proxy,
                session_params=session_params
            )
            
            logger.info(f"Loaded account {account_id} ({facebook_email}) with proxy: {proxy is not None}")
            return account
            
        except Exception as e:
            logger.error(f"Failed to load account {account_config.get('account_id', 'unknown')}: {e}")
            return None
    
    def generate_headers(self, account_id: str) -> Dict:
        """Generate headers for an account"""
        # Add some randomization to headers to make accounts look different
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        ]
        
        return {
            "User-Agent": random.choice(user_agents),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.7",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.facebook.com",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Connection": "keep-alive",
            "Priority": "u=1, i",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
            "sec-ch-ua-full-version-list": '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.0.0", "Brave";v="138.0.0.0"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": '""',
            "sec-ch-ua-platform": '"Windows"',
            "sec-ch-ua-platform-version": '"19.0.0"',
            "Sec-GPC": "1",
            "x-asbd-id": "359341",
            "x-fb-friendly-name": "SearchCometResultsPaginatedResultsQuery",
            "x-fb-lsd": "QtHMiXCRHid0cuO5v247AY",  # This should be unique per account
        }
    
    def generate_session_params(self, account_id: str) -> Dict:
        """Generate unique session parameters for each account"""
        # Base parameters that should be unique per account
        base_params = {
            "x_fb_lsd": "QtHMiXCRHid0cuO5v247AY",
            "fb_dtsg": "NAfu9CKvo5n9HQ7oc2uuNcExCweYm0zG6-AzKsJLuPk1O6V7LzEXk4g:43:1750474603",
            "jazoest": "25410",
            "__req": "6",
            "__rev": "1025363761",
            "__s": "u71sjo:oem9po:9r0wa0",
            "__hsi": "7533292342787910145",
            "doc_id": "24258477577126445"
        }
        
        # Add some variation based on account_id
        account_hash = hash(account_id) % 1000
        base_params["jazoest"] = str(25410 + account_hash)
        base_params["__req"] = str(6 + (account_hash % 5))
        
        return base_params
    
    def get_account(self) -> FacebookAccount:
        """Get next available account with load balancing"""
        with threading.Lock():
            # Simple round-robin for now, could be enhanced with load balancing
            account = next(self.account_cycle)
            
            # Update account usage
            with account.lock:
                account.call_count += 1
                account.last_used = time.time()
            
            return account
    
    def get_accounts_for_concurrent_use(self, count: int) -> List[FacebookAccount]:
        """Get multiple accounts for concurrent use"""
        accounts = []
        for _ in range(count):
            accounts.append(self.get_account())
        return accounts

# --- Global Statistics ---
class GlobalStats:
    def __init__(self):
        self.total_calls = 0
        self.total_groups_found = 0
        self.searches_completed = 0
        self.accounts_used = set()
        self.start_time = time.time()
        self.lock = threading.Lock()
    
    def update(self, calls: int = 0, groups: int = 0, searches: int = 0, account_id: str = None):
        with self.lock:
            self.total_calls += calls
            self.total_groups_found += groups
            self.searches_completed += searches
            if account_id:
                self.accounts_used.add(account_id)
    
    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            return {
                'total_calls': self.total_calls,
                'total_groups': self.total_groups_found,
                'searches_completed': self.searches_completed,
                'accounts_used': len(self.accounts_used),
                'elapsed_time': elapsed,
                'calls_per_second': self.total_calls / elapsed if elapsed > 0 else 0
            }

global_stats = GlobalStats()
account_manager = AccountManager()

# --- Search Terms Generator ---
def load_search_urls_from_file() -> List[str]:
    """Load search URLs from facebook_group_urls.txt and extract search terms"""
    urls_file = os.path.join(PARENT_DIR, "settings", "facebook_group_urls.txt")
    
    if not os.path.exists(urls_file):
        logger.warning(f"URLs file not found: {urls_file}")
        logger.info("Falling back to hardcoded search terms")
        return generate_search_terms()
    
    search_terms = []
    try:
        with open(urls_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # Extract search term from URL
                # URL format: https://www.facebook.com/groups/search/groups_home/?q=Adak%2C+AK
                if '?q=' in line:
                    try:
                        # Extract the query parameter
                        query_part = line.split('?q=')[1]
                        # URL decode the search term
                        search_term = urllib.parse.unquote(query_part)
                        search_terms.append(search_term)
                    except Exception as e:
                        logger.warning(f"Failed to parse URL on line {line_num}: {line[:50]}... - {e}")
                        continue
                else:
                    logger.warning(f"Invalid URL format on line {line_num}: {line[:50]}...")
                    continue
                
                # Limit to TOTAL_SEARCHES to avoid memory issues
                if len(search_terms) >= TOTAL_SEARCHES:
                    logger.info(f"Reached limit of {TOTAL_SEARCHES} search terms")
                    break
                    
    except Exception as e:
        logger.error(f"Error reading URLs file: {e}")
        logger.info("Falling back to hardcoded search terms")
        return generate_search_terms()
    
    logger.info(f"Loaded {len(search_terms)} search terms from {urls_file}")
    return search_terms

def generate_search_terms() -> List[str]:
    """Generate 34,000 search terms for different cities/locations (fallback)"""
    cities = [
        "Dallas", "Houston", "Austin", "San Antonio", "Fort Worth", "Arlington", "Plano", "Irving",
        "Frisco", "McKinney", "Denton", "Garland", "Grand Prairie", "Mesquite", "Carrollton",
        "McAllen", "Waco", "Amarillo", "Lubbock", "El Paso", "Corpus Christi", "Laredo",
        "Brownsville", "Killeen", "Beaumont", "Abilene", "Odessa", "Midland", "Tyler", "Wichita Falls"
    ]
    
    states = ["TX", "CA", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI", "NJ", "VA", "WA", "AZ", "MA"]
    
    search_terms = []
    
    # Generate city + state combinations
    for city in cities:
        for state in states:
            search_terms.append(f"{city}, {state}")
    
    # Generate city + "TX" combinations for Texas cities
    for city in cities:
        search_terms.append(f"{city}, TX")
    
    # Add more variations
    variations = [
        "neighborhood", "community", "local", "area", "region", "district", "zone"
    ]
    
    for city in cities[:10]:  # Limit to avoid too many combinations
        for variation in variations:
            search_terms.append(f"{city} {variation}")
    
    # Add specific area searches
    areas = ["downtown", "uptown", "midtown", "suburbs", "metro", "greater", "north", "south", "east", "west"]
    for city in cities[:5]:
        for area in areas:
            search_terms.append(f"{city} {area}")
    
    # If we don't have enough, add numbered variations
    if len(search_terms) < TOTAL_SEARCHES:
        for i in range(TOTAL_SEARCHES - len(search_terms)):
            city = cities[i % len(cities)]
            search_terms.append(f"{city} area {i+1}")
    
    return search_terms[:TOTAL_SEARCHES]

def get_search_terms() -> List[str]:
    """Get search terms from file or generate fallback"""
    # Try to load from file first
    search_terms = load_search_urls_from_file()
    
    if not search_terms:
        logger.warning("No search terms loaded from file, using fallback")
        search_terms = generate_search_terms()
    
    return search_terms

# --- Multi-Account Search Worker ---
class MultiAccountSearchWorker:
    def __init__(self, search_term: str, worker_id: int, account: FacebookAccount):
        self.search_term = search_term
        self.worker_id = worker_id
        self.account = account
        self.call_count = 0
        self.groups_found = 0
        self.cursor = None
        self.seen_ids = set()
        
        # Create search-specific files
        self.output_file = os.path.join(OUTPUT_DIR, f"groups_{worker_id}_{self.account.account_id}_{hashlib.md5(search_term.encode()).hexdigest()[:8]}.jsonl")
        self.state_file = os.path.join(STATE_DIR, f"state_{worker_id}_{self.account.account_id}_{hashlib.md5(search_term.encode()).hexdigest()[:8]}.json")
        
        # Load state if exists
        self.load_state()
        
        # Update headers for this search
        self.headers = self.account.headers.copy()
        self.headers["Referer"] = f"https://www.facebook.com/groups/search/groups_home/?q={self.search_term.replace(' ', '%20')}"
        
        logger.info(f"Worker {worker_id} (Account: {self.account.account_id}) initialized for search: {search_term}")
    
    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                state = json.load(f)
            self.cursor = state.get("cursor")
            self.seen_ids = set(state.get("seen_ids", []))
            self.call_count = state.get("call_count", 0)
            logger.info(f"Worker {self.worker_id} loaded state: {self.call_count} calls, {len(self.seen_ids)} groups")
    
    def save_state(self):
        with open(self.state_file, "w") as f:
            json.dump({
                "cursor": self.cursor,
                "seen_ids": list(self.seen_ids),
                "call_count": self.call_count
            }, f)
    
    def append_group(self, group):
        if group["id"] not in self.seen_ids:
            with open(self.output_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(group, ensure_ascii=False) + "\n")
            self.seen_ids.add(group["id"])
            self.groups_found += 1
    
    def fetch_page(self) -> Optional[Dict]:
        if not self.account.cookies.get("c_user"):
            raise Exception(f"Account {self.account.account_id} must include Facebook user ID as 'c_user' in cookie file.")
        
        user_id = self.account.cookies.get("c_user")
        
        # Generate variables for this search
        variables = {
            "allow_streaming": False,
            "args": {
                "callsite": "comet:groups_search",
                "config": {
                    "exact_match": False,
                    "high_confidence_config": None,
                    "intercept_config": None,
                    "sts_disambiguation": None,
                    "watch_config": None
                },
                "context": {
                    "bsid": "6fe9abaf-7938-4245-b9b8-6e0c515920ce",
                    "tsid": None
                },
                "experience": {
                    "client_defined_experiences": ["ADS_PARALLEL_FETCH"],
                    "encoded_server_defined_params": None,
                    "fbid": None,
                    "type": "GROUPS_TAB_GLOBAL"
                },
                "filters": [],
                "text": self.search_term
            },
            "count": 5,
            "cursor": self.cursor,
            "feedLocation": "SEARCH",
            "feedbackSource": 23,
            "fetch_filters": True,
            "focusCommentID": None,
            "locale": None,
            "privacySelectorRenderLocation": "COMET_STREAM",
            "renderLocation": "search_results_page",
            "scale": 1,
            "stream_initial_count": 0,
            "useDefaultActor": False,
            "__relay_internal__pv__GHLShouldChangeAdIdFieldNamerelayprovider": False,
            "__relay_internal__pv__GHLShouldChangeSponsoredDataFieldNamerelayprovider": False,
            "__relay_internal__pv__IsWorkUserrelayprovider": False,
            "__relay_internal__pv__FBReels_deprecate_short_form_video_context_gkrelayprovider": True,
            "__relay_internal__pv__FeedDeepDiveTopicPillThreadViewEnabledrelayprovider": False,
            "__relay_internal__pv__FBReels_enable_view_dubbed_audio_type_gkrelayprovider": False,
            "__relay_internal__pv__CometImmersivePhotoCanUserDisable3DMotionrelayprovider": False,
            "__relay_internal__pv__WorkCometIsEmployeeGKProviderrelayprovider": False,
            "__relay_internal__pv__IsMergQAPollsrelayprovider": False,
            "__relay_internal__pv__FBReelsMediaFooter_comet_enable_reels_ads_gkrelayprovider": True,
            "__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider": False,
            "__relay_internal__pv__CometUFIShareActionMigrationrelayprovider": True,
            "__relay_internal__pv__CometUFI_dedicated_comment_routable_dialog_gkrelayprovider": False,
            "__relay_internal__pv__StoriesArmadilloReplyEnabledrelayprovider": True,
            "__relay_internal__pv__FBReelsIFUTileContent_reelsIFUPlayOnHoverrelayprovider": True
        }
        
        # Use account-specific session parameters
        session_params = self.account.session_params
        
        data = {
            "av": user_id,
            "__aaid": "0",
            "__user": user_id,
            "__a": "1",
            "__req": session_params["__req"],
            "__hs": "20300.HYP:comet_pkg.2.1...0",
            "dpr": "1",
            "__ccg": "EXCELLENT",
            "__rev": session_params["__rev"],
            "__s": session_params["__s"],
            "__hsi": session_params["__hsi"],
            "__dyn": "7xeUjGU5a5Q1ryaxG4Vp41twpUnwgU29zEdE98K360CEboG0IE6u3y4o2Gwfi0LVE4W0qa321Rw8G11wBz81s8hwGxu782lwv89k2C0iK1awhUC7Udo5qfK0zEkxe2GewGwkUe9obrwh8lwuEjxuu3W3y261kx-0iu2-awLyES0gl08O321LwTwKG4UrwFg2fwxyo6J0qo5u1JwjHDzUiBG2OUqwjVqwLwHwea1wweW2K3a2q6E",
            "__csr": "ggTst2LtFisRn92k8vblnfsl9jlitZnnNBkFdi9lWELKnKiBv9IwLLLHHJdzqruWiBGmAOkLcF4j_jRiriV2Gh5zp9p4uaF4KlQqaDKu6lypaKdx2cyVEoLzUaFEWmegaqwBK48nhA7-2a8zp8R0NBBwEw8Ku13AwholzEC5EeE-484y6odoC7Gg6e0LUbA8ybxx0jofkfwlEjxqfwmE665VE8E2jxO3G6EO3W2O0FUKewRwzw8m0Moqw8CU7G0pG3y5U4Z162WmJ5xm0vm0Yo2kw4Wg1P8hwyojw8eu0H8Do4e1by2e2W0hp6Dx2212E4Gbw05PNg1e80p2w5rw0zno1TVC2G06MC05Xzw3CU0nXDw0Wkw2Go0buU081aw7gw7axK0bEFw",
            "__hsdp": "gwwyA2ic4NgN2P6yaeAiCGBA5yaUwoqpkQAIV5fKqHykyCBDi8qgwmyppSFGG6Yxbgx4zHihFA8iDiQciVvByaAgF2QukEwzoF19G9heVXosghBCSlpWKF8zAkBe4LgFVanoAXgMNabcHEGkTB6Tlhu_kIQnEiVQIWcF2sm9yNpsWQuySSKcBGFlai4aHHGFqAUsCz4iepUvKmcwCBzUpzWzUmGW8FzKq8ry9UiDuaJ6h4AQ8yVBq51AEIEymAciKAm9wOxlU9eiawAzo-6dbzpmb6CKqUHAp8oy21ZxG494qcyrhpHh8kKcxG1zgW22exG688o6CdwvpEuga89UfEhx2Q48W2i9z658fRh8mwZx-9xq3yfwfi783kw7SwGwbqFA0SXDh66E9k0w8uwqE1aSwOcwEwo40M88E198y0iW0mq0hO0A814o0Oe1Bzo0vzwuo13E0Ba0su0tO0e2w2EU0ZO0la",
            "__hblp": "08S4U1Ho1CU10EnwNw6owGwqo19o7i17w7ow2BUmwUzU0N60OA2G0me0cpwo80wC0nq0_oao1782gwt82sw38U6m0aAwvE1PU3NwBwdK0gC1Vw4ew2kE1xGwZw7sw5Dx2089w5hwBxS6E3Owoo0TG0la3W0jh0fS",
            "__sjsp": "gwwyE9oYRYwN2P6yb9F4FG8B8y28Qoqpp4AgKnG8CGUoByapVA5ECfzUugCfxp6CykELgsByV8pDxm-agnzUhwNz8G1dwgK1Hx62a1swfK3O1jG2Rxi4EC1jx-2-u4UcEpwHwv8colV88i0wgaU9Unwah0LzE5m0Zo9Ea89U52547U98Ccokwgd4xq3S7U1uU3kw2SaCg3rKt4oqwau0yo1aSMMcwEw4ywyw0CDwpoS",
            "__comet_req": "15",
            "fb_dtsg": session_params["fb_dtsg"],
            "jazoest": session_params["jazoest"],
            "lsd": self.headers["x-fb-lsd"],
            "__spin_r": session_params["__rev"],
            "__spin_b": "trunk",
            "__spin_t": "1753981305",
            "__crn": "comet.fbweb.CometGroupsSearchRoute",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "SearchCometResultsPaginatedResultsQuery",
            "variables": json.dumps(variables),
            "server_timestamps": "true",
            "doc_id": session_params["doc_id"],
        }
        
        try:
            resp = requests.post(
                "https://www.facebook.com/api/graphql/",
                headers=self.headers,
                cookies=self.account.cookies,
                data=data,
                proxies=self.account.proxy,
                timeout=30
            )
            
            encoding = resp.headers.get("Content-Encoding")
            
            try:
                return resp.json()
            except Exception as e:
                if encoding == "zstd":
                    try:
                        dctx = zstd.ZstdDecompressor()
                        decompressed = dctx.stream_reader(io.BytesIO(resp.content))
                        text = decompressed.read().decode("utf-8")
                        return json.loads(text)
                    except Exception as zstd_e:
                        logger.error(f"Worker {self.worker_id} Zstd decompression failed: {zstd_e}")
                
                logger.error(f"Worker {self.worker_id} JSON parsing failed: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id} Request failed: {e}")
            return None
    
    def extract_groups(self, response):
        try:
            edges = response["data"]["serpResponse"]["results"]["edges"]
            for edge in edges:
                node = edge.get("rendering_strategy", {}).get("view_model", {}).get("profile", {})
                if node.get("__typename") == "Group":
                    yield {
                        "id": node.get("id"),
                        "name": node.get("name"),
                        "url": node.get("url") or node.get("profile_url"),
                        "search_term": self.search_term,
                        "worker_id": self.worker_id,
                        "account_id": self.account.account_id
                    }
        except Exception as e:
            logger.error(f"Worker {self.worker_id} Error extracting groups: {e}")
            return []
    
    def get_next_cursor(self, response):
        try:
            return response["data"]["serpResponse"]["results"]["page_info"]["end_cursor"]
        except Exception:
            return None
    
    def run_search(self):
        """Run the search until target calls reached or no more results"""
        logger.info(f"Worker {self.worker_id} (Account: {self.account.account_id}) starting search for: {self.search_term}")
        
        while self.call_count < TARGET_CALLS_PER_SEARCH:
            try:
                response = self.fetch_page()
                if not response:
                    logger.warning(f"Worker {self.worker_id} No response, stopping search")
                    break
                
                self.call_count += 1
                new_groups = 0
                
                for group in self.extract_groups(response):
                    if group["id"] not in self.seen_ids:
                        self.append_group(group)
                        new_groups += 1
                
                # Update global stats
                global_stats.update(calls=1, groups=new_groups, account_id=self.account.account_id)
                
                # Save state periodically
                if self.call_count % 10 == 0:
                    self.save_state()
                
                logger.info(f"Worker {self.worker_id} (Account: {self.account.account_id}) Call #{self.call_count}, found {new_groups} new groups")
                
                # Check if we should stop
                next_cursor = self.get_next_cursor(response)
                if not next_cursor or new_groups == 0:
                    logger.info(f"Worker {self.worker_id} No more pages or no new groups found")
                    break
                
                self.cursor = next_cursor
                
                # Rate limiting
                sleep_time = random.uniform(*RATE_LIMIT_DELAY)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Worker {self.worker_id} Error in search loop: {e}")
                break
        
        # Final state save
        self.save_state()
        global_stats.update(searches=1)
        
        logger.info(f"Worker {self.worker_id} (Account: {self.account.account_id}) completed search: {self.call_count} calls, {self.groups_found} groups")
        return self.call_count, self.groups_found

# --- Main Multi-Account Parallel Scraper ---
def run_multi_account_parallel_scraper():
    """Main function to run the multi-account parallel scraper"""
    logger.info("Starting Multi-Account Parallel Facebook Groups Scraper")
    logger.info(f"Target: {TOTAL_SEARCHES} searches, {TARGET_CALLS_PER_SEARCH} calls per search")
    logger.info(f"Max concurrent accounts: {MAX_CONCURRENT_ACCOUNTS}")
    logger.info(f"Max concurrent searches per account: {MAX_CONCURRENT_SEARCHES_PER_ACCOUNT}")
    
    if not account_manager.accounts:
        logger.error("No Facebook accounts loaded. Please set up accounts in settings/accounts/")
        return []
    
    # Generate search terms
    search_terms = get_search_terms()
    logger.info(f"Generated {len(search_terms)} search terms")
    
    # Create search queue
    search_queue = queue.Queue()
    for i, term in enumerate(search_terms):
        search_queue.put((i, term))
    
    # Results storage
    results = []
    
    def worker_function(worker_id: int, search_term: str, account: FacebookAccount):
        """Worker function for each search"""
        try:
            worker = MultiAccountSearchWorker(search_term, worker_id, account)
            calls, groups = worker.run_search()
            return {
                'worker_id': worker_id,
                'search_term': search_term,
                'account_id': account.account_id,
                'calls': calls,
                'groups': groups,
                'success': True
            }
        except Exception as e:
            logger.error(f"Worker {worker_id} failed: {e}")
            return {
                'worker_id': worker_id,
                'search_term': search_term,
                'account_id': account.account_id,
                'calls': 0,
                'groups': 0,
                'success': False,
                'error': str(e)
            }
    
    # Calculate total workers needed
    total_workers = MAX_CONCURRENT_ACCOUNTS * MAX_CONCURRENT_SEARCHES_PER_ACCOUNT
    logger.info(f"Total concurrent workers: {total_workers}")
    
    # Run searches with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        
        # Submit initial batch of searches
        for _ in range(min(total_workers, search_queue.qsize())):
            if not search_queue.empty():
                worker_id, search_term = search_queue.get()
                account = account_manager.get_account()
                future = executor.submit(worker_function, worker_id, search_term, account)
                futures.append(future)
        
        # Process completed searches and submit new ones
        while futures or not search_queue.empty():
            # Wait for any future to complete
            done, not_done = as_completed(futures, timeout=1), futures
            
            for future in done:
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Log progress
                    stats = global_stats.get_stats()
                    logger.info(f"Completed search {result['search_term']} (Account: {result['account_id']}): {result['calls']} calls, {result['groups']} groups")
                    logger.info(f"Global stats: {stats['total_calls']} total calls, {stats['searches_completed']} searches completed, {stats['accounts_used']} accounts used")
                    
                    # Submit new search if queue not empty
                    if not search_queue.empty():
                        worker_id, search_term = search_queue.get()
                        account = account_manager.get_account()
                        new_future = executor.submit(worker_function, worker_id, search_term, account)
                        futures.append(new_future)
                    
                    # Cooldown between searches
                    time.sleep(random.uniform(*SEARCH_COOLDOWN))
                    
                except Exception as e:
                    logger.error(f"Future processing error: {e}")
            
            # Update futures list
            futures = [f for f in futures if not f.done()]
    
    # Final summary
    stats = global_stats.get_stats()
    logger.info("=== FINAL SUMMARY ===")
    logger.info(f"Total searches completed: {stats['searches_completed']}")
    logger.info(f"Total GraphQL calls: {stats['total_calls']}")
    logger.info(f"Total groups found: {stats['total_groups']}")
    logger.info(f"Accounts used: {stats['accounts_used']}")
    logger.info(f"Total time: {stats['elapsed_time']:.2f} seconds")
    logger.info(f"Average calls per second: {stats['calls_per_second']:.2f}")
    
    # Save results summary
    with open(os.path.join(OUTPUT_DIR, 'multi_account_scraper_summary.json'), 'w') as f:
        json.dump({
            'results': results,
            'final_stats': stats
        }, f, indent=2)
    
    return results

if __name__ == "__main__":
    run_multi_account_parallel_scraper() 