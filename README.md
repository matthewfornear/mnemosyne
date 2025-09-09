# Facebook Groups Scraping Project

A comprehensive Python-based system for scraping Facebook groups using GraphQL API with multi-account management, proxy integration, and data enrichment capabilities.

## 🚀 Overview

This project is designed to systematically discover and collect Facebook groups by searching for location-based terms. It uses Facebook's GraphQL API to perform searches, extract group information, and enrich the data with additional details like member counts, descriptions, and hovercard information.

## 📁 Project Structure

```
facebook/
├── scripts/                          # Main Python scripts
│   ├── GRAPHQL_Initial_Curl_Scraper.py    # Initial group discovery and enrichment
│   ├── GRAPHQL_Hovercard_Curl_Enricher.py # Data enrichment with hovercard info
│   ├── GRAPHQL_Pagination_Curl_Scraper.py # Advanced pagination scraper
│   └── php/                         # PHP-based scrapers (alternative implementation)
│       ├── facebook_groups_scraper.php
│       └── enrich_groups_with_hovercard.php
├── settings/                         # Configuration files
│   ├── bought_accounts.json        # Facebook account credentials
│   ├── nimbleway_settings.json    # Proxy configuration
│   ├── curl/                       # cURL session data per account
│   └── cookies/                    # Browser cookies per account
├── output/                          # Generated data files
│   ├── curl/                       # Main scraping outputs
│   ├── initial_searches/           # Initial search results
│   └── super/                      # Combined final datasets
└── temp/                           # Temporary files and browser profiles
```

## 🔧 Core Components

### 1. Account Management
- **Multi-Account System**: Manages multiple Facebook accounts with rotation
- **Session Management**: Maintains authenticated sessions using cURL commands
- **Account Health Monitoring**: Tracks banned/OTP-problem accounts
- **Cookie Management**: Stores and rotates browser cookies per account

### 2. Scraping Engine
- **GraphQL API Integration**: Uses Facebook's internal GraphQL endpoints
- **Location-Based Search**: Searches for groups by location terms
- **Pagination Handling**: Automatically handles multi-page results
- **Rate Limiting**: Implements intelligent delays between requests

### 3. Data Processing
- **JSONL Format**: Stores data in line-delimited JSON for large datasets
- **Deduplication**: Prevents duplicate group entries
- **Progress Tracking**: Resumable operations with state persistence
- **Data Enrichment**: Adds hovercard data (member counts, descriptions)

### 4. Proxy Integration
- **Proxy Support**: Rotating IP addresses for anonymity
- **Proxyless Mode**: Direct connection option for testing
- **Session Persistence**: Maintains proxy sessions across requests

## 🛠️ Setup and Installation

### Prerequisites
- Python 3.11+ (for Python scripts)
- PHP 7.4+ (for PHP scripts, optional)
- Facebook accounts
- Proxy service account (optional)

### Installation
1. Clone the repository
2. Install Python dependencies:
   ```bash
   pip install requests urllib3 multiprocessing
   ```
3. Configure accounts in `settings/bought_accounts.json`
4. Set up proxy settings in `settings/nimbleway_settings.json` (optional)

## 🚀 Usage

### Python Scripts

#### 1. Initial Group Discovery and Enrichment
```bash
python scripts/GRAPHQL_Initial_Curl_Scraper.py
```
- Searches for groups using location terms
- Enriches data with hovercard information
- Outputs to `output/initial_searches/initial_searches_enriched.jsonl`
- Uses multiple accounts with load balancing

#### 2. Data Enrichment (cURL Output)
```bash
python scripts/GRAPHQL_Hovercard_Curl_Enricher.py
```
- Enriches existing group data with additional information
- Adds member counts, descriptions, and metadata
- Outputs to `output/curl/groups_output_enriched.jsonl`

#### 3. Advanced Pagination Scraper
```bash
python scripts/GRAPHQL_Pagination_Curl_Scraper.py
```
- Advanced scraper with comprehensive pagination handling
- Processes large datasets with progress tracking
- Outputs to `output/curl/groups_output_curl.json`

### PHP Scripts (Alternative Implementation)

#### 1. PHP Group Scraper
```bash
php scripts/php/facebook_groups_scraper.php
```
- PHP port of the main scraper functionality
- Alternative implementation for different environments

#### 2. PHP Data Enrichment
```bash
php scripts/php/enrich_groups_with_hovercard.php
```
- PHP port of the data enrichment functionality
- Adds hovercard data to existing group records

## 📊 Data Output

### Group Data Structure
Each group entry contains:
```json
{
  "id": "group_id",
  "name": "Group Name",
  "url": "https://facebook.com/groups/group_id",
  "member_count": 1234,
  "description": "Group description",
  "location": "City, State",
  "search_term": "search_term_used",
  "discovered_via": "initial_search|missing_search",
  "enriched": true
}
```

### Output Files
- `initial_searches_enriched.jsonl`: Initial search results with enrichment
- `groups_output_enriched.jsonl`: Main enriched dataset from cURL scraper
- `groups_output_curl.json`: Advanced pagination scraper output
- `SUPER_GROUPS_MISSING_ADDED.jsonl`: Combined final dataset
- Progress files: Track completion status for resumability

## ⚙️ Configuration

### Proxy Settings
Configure in `settings/nimbleway_settings.json`:
```json
{
  "accountName": "your_account",
  "pipelineName": "pipeline_name",
  "pipelinePassword": "password",
  "host": "ip.nimbleway.com",
  "port": "7000"
}
```

### Scraping Parameters
- **Sleep Between Requests**: 0.5-2.0 seconds (randomized)
- **Workers Per Session**: 4 concurrent workers
- **Max Retries**: 3 attempts per request
- **Timeout**: 30 seconds per request

## 🔍 Key Features

### 1. Resilient Operation
- **Resumable**: Can restart from last completed location
- **Error Handling**: Graceful handling of network/API errors
- **Account Rotation**: Automatically switches to healthy accounts
- **Progress Tracking**: Real-time progress monitoring

### 2. Scalability
- **Multiprocessing**: Parallel processing across multiple workers
- **Dynamic Load Balancing**: Distributes work based on account health
- **Memory Efficient**: Processes large datasets without memory issues
- **Incremental Updates**: Only processes new/changed data

### 3. Data Quality
- **Deduplication**: Prevents duplicate entries
- **Validation**: Ensures data integrity
- **Enrichment**: Adds comprehensive group metadata
- **Format Consistency**: Standardized JSONL output

## 📈 Performance

### Typical Performance Metrics
- **Groups Discovered**: Thousands of unique groups
- **Processing Speed**: ~100-200 groups per minute
- **Success Rate**: 95%+ with proper account health
- **Data Completeness**: 90%+ with enrichment data

### Resource Usage
- **Memory**: Varies based on dataset size
- **CPU**: Multi-core utilization
- **Network**: Moderate bandwidth usage
- **Storage**: Varies based on dataset size

## 🚨 Important Notes

### Legal and Ethical Considerations
- **Terms of Service**: Ensure compliance with Facebook's ToS
- **Rate Limiting**: Respect Facebook's rate limits
- **Data Usage**: Use scraped data responsibly
- **Account Safety**: Monitor account health to avoid bans

### Best Practices
- **Account Rotation**: Use multiple accounts to distribute load
- **Proxy Usage**: Use proxies for anonymity and IP rotation
- **Regular Updates**: Keep session data and cookies fresh
- **Monitoring**: Watch for account bans or OTP issues

## 🔧 Troubleshooting

### Common Issues
1. **Account Bans**: Rotate to healthy accounts
2. **OTP Problems**: Update account status in settings
3. **Proxy Issues**: Switch to proxyless mode for testing
4. **Memory Issues**: Process data in smaller chunks

### Debug Mode
Enable debug logging by modifying the logging level in scripts:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 📝 License

This project is for educational and research purposes. Please ensure compliance with Facebook's Terms of Service and applicable laws.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review logs in `output/curl/` directory
3. Verify account and proxy configurations
4. Check Facebook account health status

---

**⚠️ Disclaimer**: This tool is for educational purposes only. Users are responsible for ensuring compliance with Facebook's Terms of Service and applicable laws. The authors are not responsible for any misuse of this software.
