# Facebook Groups Scraper & Enricher

## Quick Start (macOS/Linux)

### Install dependencies
```sh
pip install -r requirements.txt
```

### Run the main scraper
```sh
python scripts/facebook_groups_scraper.py
```

### Run the enrichment script
```sh
python scripts/enrich_groups_with_hovercard.py
```

---

## Nimble Proxy Setup

To use Nimble proxies, create a file at `settings/nimble_settings.json` with your credentials:

```json
{
    "username": "account-your_account-pipeline-your_pipeline",
    "password": "YOUR_NIMBLE_PASSWORD",
    "host": "ip.nimbleway.com",
    "port": "7000"
}
```
- **username**: Your full Nimble pipeline username (from the Nimble dashboard)
- **password**: Your Nimble pipeline password
- **host**: Should be `ip.nimbleway.com` (per Nimble documentation)
- **port**: Usually `7000` (check your Nimble dashboard)

The scripts will automatically use this file for all proxy requests. No environment variables are needed.

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

## Troubleshooting Nimble Proxy
- If you see DNS errors (e.g., `Failed to resolve 'ip.nimbleway.com'`), check your DNS settings or try a different network.
- If you see `402 Payment Required`, your Nimble account may be out of credit or not authorized for the pipeline.
- Double-check your `nimble_settings.json` for typos and correct credentials.
- You can test DNS with:
  ```sh
  nslookup ip.nimbleway.com
  ```

---

## Notes
- You must update session-specific POST fields and headers in the scripts for each new session (see comments in code).
- The enrichment script does **not** fetch group descriptions, as these are not available in the hovercard API.
- For large jobs, both scripts are safe to stop and resume.

---

## Windows PowerShell Usage

### Install dependencies
```powershell
pip install -r requirements.txt
```

### Run the main scraper
```powershell
python scripts/facebook_groups_scraper.py
```

### Run the enrichment script
```powershell
python scripts/enrich_groups_with_hovercard.py
```

### Test DNS
```powershell
nslookup ip.nimbleway.com
```

---

## License
MIT 