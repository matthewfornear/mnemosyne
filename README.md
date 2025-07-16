# Facebook Groups Scraper & Enricher

## Overview
This project allows you to scrape Facebook group search results and enrich them with additional group info using Facebook's internal GraphQL APIs. It is designed for robustness, deduplication, resumability, and can be run through the Nimble proxy network for privacy or rate limit avoidance.

---

## Requirements
- Python 3.8+
- Facebook account (for cookies)
- `requests` and `zstandard` Python packages

Install dependencies:
```sh
pip install -r requirements.txt
```

---

## Setup
1. **Get your Facebook cookies**
   - Log in to Facebook in your browser
   - Open DevTools > Application > Cookies > Copy all cookies as JSON
   - Save as `settings/cookie.json`

2. **Create output directories**
   - The scripts will use `output/` for all results
   - The scripts will use `settings/cookie.json` for cookies

   Example `settings/cookies_example.json`:
   ```json
   {
       "c_user": "YOUR_C_USER_HERE",
       "datr": "YOUR_DATR_HERE",
       "oo": "YOUR_OO_HERE",
       "presence": "YOUR_PRESENCE_HERE",
       "ps_l": "YOUR_PS_L_HERE",
       "ps_n": "YOUR_PS_N_HERE",
       "sb": "YOUR_SB_HERE",
       "wd": "YOUR_WD_HERE",
       "xs": "YOUR_XS_HERE"
   }
   ```

---

## Usage

### 1. Scrape Groups
Run the main scraper to collect group IDs, names, and URLs:
```sh
python scripts/facebook_groups_scraper.py
```
- Output: `output/groups.jsonl`
- State: `output/groups_state.json` (for resumability)

### 2. Enrich Groups
Run the enrichment script to fetch member count and privacy for each group:
```sh
python scripts/enrich_groups_with_hovercard.py
```
- Output: `output/groups_enriched.jsonl`
- Skips already-enriched groups for safe resumption

---

## Using Nimble Proxy
All requests should be routed through Nimble for privacy and reliability:
1. Get your Nimble proxy URL (e.g., `http://user:pass@gw.nimbleway.com:XXXX`)
2. Set the environment variable before running either script:
   ```sh
   export NIMBLE_PROXY="http://user:pass@gw.nimbleway.com:XXXX"
   python scripts/facebook_groups_scraper.py
   # or
   python scripts/enrich_groups_with_hovercard.py
   ```
   On Windows PowerShell:
   ```powershell
   $env:NIMBLE_PROXY="http://user:pass@gw.nimbleway.com:XXXX"
   python scripts/facebook_groups_scraper.py
   ```

---

## Notes
- You must update session-specific POST fields and headers in the scripts for each new session (see comments in code).
- The enrichment script does **not** fetch group descriptions, as these are not available in the hovercard API.
- For large jobs, both scripts are safe to stop and resume.

---

## Troubleshooting
- If you get HTML or login pages instead of JSON, your cookies or session fields are likely expired or incorrect.
- If you hit rate limits, increase the random sleep interval in the scripts.
- For more fields (like group description), you will need to adapt the enrichment script to use a different Facebook GraphQL query or scrape HTML.

---

## License
MIT 