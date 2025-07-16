import os
import sys
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_DIR = os.path.join(SCRIPT_DIR, 'settings')
SCRAPER_PATH = os.path.join(SCRIPT_DIR, 'scripts', 'facebook_groups_scraper.py')
URLS_FILE = os.path.join(SETTINGS_DIR, 'facebook_group_urls.txt')

# Helper to run the scraper with a given search term
def run_scraper(search_term):
    print(f"[main.py] Scraping for search: {search_term}")
    # Pass the search term as an environment variable
    env = os.environ.copy()
    env['SEARCH_TEXT'] = search_term
    subprocess.run([sys.executable, SCRAPER_PATH], env=env)

# Main logic
if os.path.exists(URLS_FILE):
    with open(URLS_FILE, 'r', encoding='utf-8') as f:
        search_terms = [line.strip() for line in f if line.strip()]
    for term in search_terms:
        run_scraper(term)
else:
    print(f"Could not find {URLS_FILE}.")
    search_term = input("Enter a Facebook group search term or search URL: ").strip()
    if search_term:
        run_scraper(search_term)
    else:
        print("No search term provided. Exiting.") 