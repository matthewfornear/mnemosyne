# Facebook Groups Scraper with Account Rotation

This updated Facebook Groups Scraper now supports multiple account rotation to avoid rate limiting and increase scraping efficiency.

## Features

- **Multi-Account Support**: Uses accounts from `settings/bought_accounts.json`
- **Automatic Account Rotation**: Switches accounts when rate limited
- **Cookie Generation**: Automated cookie generation for all accounts
- **Progress Tracking**: Saves progress and can resume from where it left off
- **Rate Limiting Detection**: Automatically detects and handles Facebook rate limiting

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Install Chrome WebDriver

The cookie generator requires Chrome WebDriver. You can install it automatically:

```bash
pip install webdriver-manager
```

Or download it manually from: https://chromedriver.chromium.org/

**Important**: Make sure you have Google Chrome installed on your system.

### 3. Configure Your Accounts

Your accounts are already configured in `settings/bought_accounts.json`. The file should look like this:

```json
{
    "accounts": [
        {
      "account": "your_email@gmail.com",
      "password": "your_password"
    },
    {
      "account": "another_email@gmail.com", 
      "password": "another_password"
    }
  ]
}
```

**Account Setup Tips:**
- Use real Facebook accounts (not fake/bot accounts)
- Ensure accounts have been active for at least a few days
- Avoid accounts that have been recently created
- Make sure accounts don't have any restrictions or warnings
- Consider using accounts from different IP addresses if possible

### 4. Generate Cookies for All Accounts

Before running the scraper, you need to generate cookies for each account:

```bash
python scripts/generate_cookies.py
```

**What this script does:**
- Opens Chrome browser for each account
- Navigates to Facebook login page
- Automatically enters email and password
- Clicks login button
- Waits for login to complete
- Extracts all cookies from the browser
- Saves cookies to `settings/cookies/` directory

**Important Notes:**
- The browser will open visibly (not headless) so you can see the login process
- If an account has 2FA enabled, you'll need to manually complete the verification when the browser opens
- If login fails, the script will continue with the next account
- Cookies are saved as `{email}_cookies.json` files (email with @ and . replaced with _at_ and _)
- The script waits 10 seconds between accounts to avoid triggering security measures

**Troubleshooting Cookie Generation:**

1. **Login Failed - Check Credentials**
   ```
   ❌ Login failed for email@gmail.com: The email address or mobile number you entered isn't connected to an account
   ```
   - Verify the email and password are correct
   - Check if the account exists and is active
   - Try logging in manually to Facebook first

2. **2FA Required**
   - When the browser opens, you'll see a 2FA prompt
   - Enter your 2FA code manually
   - The script will wait for you to complete the verification
   - After successful 2FA, the script will continue automatically

3. **Account Locked/Temporarily Blocked**
   ```
   ❌ Login failed for email@gmail.com: Your account has been temporarily locked
   ```
   - Wait a few hours before trying again
   - Try logging in manually to Facebook first
   - Consider using a different account

4. **Chrome Driver Issues**
   ```
   ❌ Error: ChromeDriver not found
   ```
   - Install ChromeDriver: `pip install webdriver-manager`
   - Or download manually from: https://chromedriver.chromium.org/
   - Make sure Chrome browser is installed

5. **Browser Opens But Doesn't Login**
   - Check if Facebook's login page structure has changed
   - Try running the script again
   - Check your internet connection

### 5. Test Your Cookies

After generating cookies, test them to make sure they work:

```bash
python scripts/test_cookies.py
```

This script will:
- Load all generated cookie files
- Test each account's cookies by making a request to Facebook
- Verify that the cookies allow access to Facebook
- Show you which accounts are working and which need attention

**Expected Output:**
```
🧪 Facebook Cookie Tester
==================================================
📋 Found 8 accounts

🔍 Testing cookies for: Lanekimmonogo@gmail.com
✅ Cookies working for Lanekimmonogo@gmail.com
   User ID: 123456789

🔍 Testing cookies for: Dextermonogo@gmail.com
✅ Cookies working for Dextermonogo@gmail.com
   User ID: 987654321

==================================================
🎯 Test Results:
✅ Working cookies: 8
❌ Failed cookies: 0
📊 Success rate: 100.0%
==================================================

✅ You can now run the scraper:
   python scripts/facebook_groups_scraper.py
```

### 6. Configure Search URLs (Optional)

Create `settings/facebook_group_urls.txt` with your search URLs, one per line:

```
https://www.facebook.com/groups/search/groups_home/?q=arlington%20tx
https://www.facebook.com/groups/search/groups_home/?q=dallas%20groups
https://www.facebook.com/groups/search/groups_home/?q=houston%20business
```

**URL Format:**
- Use Facebook's search URL format
- Encode spaces as `%20` or `+`
- One URL per line
- The scraper will extract search terms from these URLs

### 7. Run the Scraper

```bash
python scripts/facebook_groups_scraper.py
```

## How It Works

### Account Rotation
- The scraper starts with the first account
- When rate limiting is detected, it automatically switches to the next account
- After trying all accounts, it waits with exponential backoff
- If all accounts are rate limited, it saves progress and stops

### Cookie Management
- Cookies are stored in `settings/cookies/` directory
- Each account has its own cookie file: `{email}_cookies.json`
- The scraper loads cookies for the current account automatically
- If cookies are invalid or missing, it skips to the next account

### Progress Tracking
- Progress is saved in `output/url_progress.json`
- State is saved in `output/groups_state.json`
- You can stop and resume the scraper at any time
- Groups are saved to `output/groups.jsonl`

## File Structure

```
facebook/
├── settings/
│   ├── bought_accounts.json      # Your account credentials
│   ├── cookies/                  # Generated cookies for each account
│   │   ├── Lanekimmonogo_at_gmail_com_cookies.json
│   │   ├── Dextermonogo_at_gmail_com_cookies.json
│   │   └── ...
│   ├── facebook_group_urls.txt   # Search URLs (optional)
│   └── nimble_settings.json      # Proxy settings (optional)
├── output/
│   ├── groups.jsonl              # Scraped groups
│   ├── groups_state.json         # Scraper state
│   └── url_progress.json         # URL processing progress
├── scripts/
│   ├── facebook_groups_scraper.py    # Main scraper
│   ├── generate_cookies.py           # Cookie generator
│   └── test_cookies.py               # Cookie tester
└── requirements.txt
```

## Troubleshooting

### Cookie Generation Issues

1. **Login Failed**: Check if the account credentials are correct
2. **2FA Required**: Complete 2FA manually when the browser opens
3. **Account Locked**: Some accounts may be temporarily locked - try again later
4. **Chrome Driver Issues**: Update Chrome or ChromeDriver to latest version
5. **Browser Not Opening**: Make sure Chrome is installed and accessible
6. **Script Hangs**: Check your internet connection and Facebook's status

### Scraper Issues

1. **No Cookies Found**: Run `generate_cookies.py` first
2. **All Accounts Rate Limited**: Wait a few hours before trying again
3. **Invalid Cookies**: Re-generate cookies for the problematic account
4. **Script Crashes**: Check the console output for error messages

### Rate Limiting

The scraper handles rate limiting by:
- Detecting rate limit responses
- Automatically switching to the next account
- Using exponential backoff when all accounts are limited
- Saving progress before stopping

**Common Rate Limit Signs:**
- HTTP 429 (Too Many Requests)
- "Sorry, something went wrong" messages
- "Please try again later" responses
- Account checkpoint requirements

## Configuration Options

### Target Calls
Change `TARGET_CALLS` in the scraper to set how many GraphQL calls to make:

```python
TARGET_CALLS = 1000  # Default: 1000 calls
```

### Sleep Between Requests
Adjust the sleep time between requests:

```python
SLEEP_BETWEEN_REQUESTS = (1.5, 4.0)  # Random range in seconds
```

### Rate Limiting Settings
Modify rate limiting behavior:

```python
MAX_RATE_LIMIT_ATTEMPTS = 3  # How many attempts before stopping
BASE_BACKOFF_TIME = 60        # Base wait time in seconds
```

## Best Practices

### Account Management
- **Use Real Accounts**: Avoid fake or bot accounts
- **Age Matters**: Older accounts are less likely to be flagged
- **Activity**: Accounts should have some legitimate activity
- **Diversity**: Use accounts from different sources/IPs if possible
- **Rotation**: Don't use the same account too frequently

### Cookie Management
- **Fresh Cookies**: Generate new cookies every few days
- **Test Regularly**: Use `test_cookies.py` to verify cookies work
- **Backup**: Keep backup copies of working cookies
- **Cleanup**: Remove cookies for non-working accounts

### Scraping Strategy
- **Start Small**: Begin with fewer calls to test the system
- **Monitor**: Watch for rate limiting signs
- **Pause**: Stop if all accounts get rate limited
- **Resume**: The scraper can resume from where it left off

## Security Notes

- Keep your `bought_accounts.json` file secure
- Don't commit cookies to version control
- Use proxies if available to reduce detection
- Consider using residential proxies for better success rates
- Respect Facebook's Terms of Service
- Don't overload Facebook's servers

## Support

If you encounter issues:
1. Check the console output for error messages
2. Verify all dependencies are installed
3. Ensure Chrome/ChromeDriver are up to date
4. Try regenerating cookies for problematic accounts
5. Test cookies individually with `test_cookies.py`
6. Check if Facebook's login page structure has changed

## Quick Start Checklist

- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Verify Chrome is installed
- [ ] Check `settings/bought_accounts.json` has your accounts
- [ ] Generate cookies: `python scripts/generate_cookies.py`
- [ ] Test cookies: `python scripts/test_cookies.py`
- [ ] Optional: Add search URLs to `settings/facebook_group_urls.txt`
- [ ] Run scraper: `python scripts/facebook_groups_scraper.py` 