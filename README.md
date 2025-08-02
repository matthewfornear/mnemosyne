# Facebook Groups Scraper

A powerful, multi-account Facebook Groups scraper with support for parallel processing, proxy rotation, and URL-based search terms.


facebook-groups-scraper/
├── scripts/
│   ├── facebook_groups_scraper.py          # Single-threaded scraper
│   ├── multi_account_parallel_scraper.py   # Multi-account parallel scraper
│   ├── run_multi_account_scraper.py        # Multi-account launcher
│   ├── manage_accounts.py                  # Account management tool
│   └── generate_urls.py                    # URL generation tool
├── settings/
│   ├── cookie.json                         # Single account cookies
│   ├── accounts.json                       # Multi-account configuration
│   ├── accounts/                           # Account-specific files
│   │   ├── cookie_1.json
│   │   ├── cookie_2.json
│   │   └── ...
│   ├── facebook_group_urls.txt             # Search URLs
│   └── nimble_settings.json                # Proxy configuration
├── output/
│   ├── groups.jsonl                        # Scraped groups data
│   ├── states/                             # Progress state files
│   └── logs/                               # Log files
└── README.md
```

## 🛠️ Installation

### **Prerequisites**
```bash
pip install requests zstandard
```

### **Setup Steps**

#### **1. Single Account Setup**
```bash
# Copy your Facebook cookies to settings/cookie.json
# Format:
{
    "c_user": "YOUR_USER_ID",
    "datr": "YOUR_DATR_COOKIE",
    "xs": "YOUR_XS_COOKIE",
    # ... other cookies
}
```

#### **2. Multi-Account Setup**
```bash
# Run account manager
python scripts/manage_accounts.py

# Add accounts (option 2)
# Enter email, password, Facebook email, Facebook password

# Create cookie templates (option 5)
# Update cookie files with real session data
```

#### **3. URL Setup**
```bash
# Generate sample URLs
python scripts/generate_urls.py
# Choose option 2 for sample URLs

# Or create custom URLs file
# Edit settings/facebook_group_urls.txt
```

## 📋 Usage

### **Single Account Scraper**
```bash
python scripts/facebook_groups_scraper.py
```

**Features:**
- Uses `facebook_group_urls.txt` for search terms
- Tracks progress across URLs
- Resumes from last processed URL
- Target: 1000 GraphQL calls

### **Multi-Account Parallel Scraper**
```bash
python scripts/run_multi_account_scraper.py
```

**Features:**
- Multiple accounts with different IPs
- Parallel processing (15+ concurrent workers)
- Smart rate limiting per account
- Massive scale: 34,000 searches × 1,000 calls each

## ⚙️ Configuration

### **Account Configuration** (`settings/accounts.json`)
```json
{
    "accounts": [
        {
            "account_id": "account_1",
            "email": "user1@example.com",
            "password": "password1",
            "facebook_email": "fb1@example.com",
            "facebook_password": "fbpass1",
            "cookie_file": "cookie_1.json",
            "proxy_config": {
                "enabled": true,
                "host": "ip1.nimbleway.com",
                "port": "7000",
                "username": "nimble_user",
                "password": "nimble_pass"
            }
        }
    ],
    "settings": {
        "max_concurrent_accounts": 5,
        "max_concurrent_searches_per_account": 3,
        "rate_limit_delay_min": 1.0,
        "rate_limit_delay_max": 3.0
    }
}
```

### **URL Configuration** (`settings/facebook_group_urls.txt`)
```
https://www.facebook.com/groups/search/groups_home/?q=Dallas%2C+TX
https://www.facebook.com/groups/search/groups_home/?q=Houston%2C+TX
https://www.facebook.com/groups/search/groups_home/?q=Austin%2C+TX
```

### **Proxy Configuration** (`settings/nimble_settings.json`)
```json
{
    "username": "your_nimble_username",
    "password": "your_nimble_password",
    "host": "ip.nimbleway.com",
    "port": "7000"
}
```

## 🛠️ Tools

### **Account Manager** (`scripts/manage_accounts.py`)
```bash
python scripts/manage_accounts.py
```

**Features:**
- Add/remove accounts
- Update credentials
- Create cookie templates
- Bulk account import
- Check account readiness

### **URL Generator** (`scripts/generate_urls.py`)
```bash
python scripts/generate_urls.py
```

**Features:**
- Generate URLs from search terms
- Convert existing URLs to search terms
- Generate sample URLs
- View current URLs file

## 📊 Performance

### **Single Account**
- **~0.3 calls/second** with rate limiting
- **Target: 1000 calls** (~1 hour)
- **Memory efficient** - processes URLs line by line

### **Multi-Account (5 accounts)**
- **~5-8 calls/second** with parallel processing
- **15 concurrent workers** (5 accounts × 3 searches)
- **Estimated time: 3-5 days** for 34M calls
- **15x faster** than single account

### **Scalability**
- **Add more accounts** for increased speed
- **Configure proxy IPs** for better distribution
- **Adjust concurrency** based on rate limits
- **Monitor progress** with detailed logging

## 🔧 Advanced Configuration

### **Rate Limiting**
```python
# In multi_account_parallel_scraper.py
RATE_LIMIT_DELAY = (1.0, 3.0)  # seconds between requests
SEARCH_COOLDOWN = (10.0, 20.0)  # seconds between searches
```

### **Concurrency Settings**
```python
MAX_CONCURRENT_ACCOUNTS = 5
MAX_CONCURRENT_SEARCHES_PER_ACCOUNT = 3
# Total workers = 5 × 3 = 15 concurrent searches
```

### **Search Terms**
- **URL-based**: Load from `facebook_group_urls.txt`
- **Fallback**: Hardcoded search terms
- **Dynamic**: Extract from Facebook search URLs
- **Flexible**: Support any location or search term

## 📈 Monitoring & Logging

### **Progress Tracking**
- **URL progress**: `output/url_progress.json`
- **Account state**: `output/states/state_*.json`
- **Detailed logs**: `output/multi_account_scraper.log`
- **Summary reports**: `output/multi_account_scraper_summary.json`

### **Log Levels**
- **INFO**: Normal operation
- **WARNING**: Rate limiting, parsing errors
- **ERROR**: Request failures, account issues

## 🛡️ Anti-Detection Features

### **Account Diversity**
- **Unique User-Agents** per account
- **Different session parameters** per account
- **Individual cookie management**
- **Account rotation** with load balancing

### **Request Patterns**
- **Randomized delays** between requests
- **Dynamic Referer headers** per search term
- **Proper request sequencing**
- **Session parameter updates**

### **Proxy Management**
- **Multiple IP addresses** via Nimbleway
- **Proxy rotation** per account
- **Fallback mechanisms** for proxy failures
- **Load distribution** across IPs

## 🚨 Troubleshooting

### **Common Issues**

#### **Rate Limiting**
```
Error: 1357004 - Sorry, something went wrong
```
**Solution:**
- Increase delays between requests
- Rotate accounts more frequently
- Update session parameters
- Check proxy configuration

#### **Cookie Issues**
```
Error: Cookie file needs updating
```
**Solution:**
- Update cookies with fresh session data
- Check cookie format and values
- Ensure all required cookies are present

#### **Proxy Problems**
```
Error: Connection timeout
```
**Solution:**
- Verify proxy credentials
- Check proxy server status
- Enable fallback to direct connection
- Test proxy connectivity

### **Debug Mode**
```bash
# Enable detailed logging
export DEBUG=1
python scripts/run_multi_account_scraper.py
```

## 📝 Examples

### **Generate Sample URLs**
```bash
python scripts/generate_urls.py
# Choose option 2
# Creates 150+ sample URLs for testing
```

### **Add Multiple Accounts**
```bash
python scripts/manage_accounts.py
# Choose option 7
# Enter JSON array of accounts:
[
    {"email": "user1@example.com", "password": "pass1"},
    {"email": "user2@example.com", "password": "pass2"}
]
```

### **Custom Search Terms**
```bash
# Edit settings/facebook_group_urls.txt
# Add your specific locations:
https://www.facebook.com/groups/search/groups_home/?q=New+York%2C+NY
https://www.facebook.com/groups/search/groups_home/?q=Los+Angeles%2C+CA
https://www.facebook.com/groups/search/groups_home/?q=Chicago%2C+IL
```

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**
3. **Make your changes**
4. **Test thoroughly**
5. **Submit a pull request**

## 📄 License

This project is for educational purposes only. Please respect Facebook's Terms of Service and rate limits.

## ⚠️ Disclaimer

- **Use responsibly** and respect rate limits
- **Don't overload** Facebook's servers
- **Comply with** Facebook's Terms of Service
- **Monitor** your account status regularly
- **Backup** your data and progress frequently

---

**Happy Scraping! 🚀** 