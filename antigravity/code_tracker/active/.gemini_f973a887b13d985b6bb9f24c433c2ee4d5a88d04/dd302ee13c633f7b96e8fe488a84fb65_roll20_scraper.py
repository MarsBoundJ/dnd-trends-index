±&from curl_cffi import requests
from bs4 import BeautifulSoup
import datetime
from google.cloud import bigquery
import time
import random


import html
from google.cloud import bigquery

# Config
API_URL = "https://marketplace.roll20.net/browse/search"
MAX_ITEMS = 100
PROJECT_ID = "dnd-trends-index"
TABLE_ID = "commercial_data.roll20_rankings"

def scrape_roll20():
    all_items = []
    page = 1
    
    # Session to keep cookies/headers
    s = requests.Session()
    
    # Headers to look like the browser's AJAX request
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://marketplace.roll20.net",
        "Referer": "https://marketplace.roll20.net/browse/search?category=itemtype:Games&sortby=popular"
    }
    
    while len(all_items) < MAX_ITEMS:
        print(f"Fetching Page {page}...")
        
        # Payload based on reverse-engineering marketplace-search.js
        # filters[category][]=itemtype:Games
        payload = {
            "page": str(page),
            "sortby": "popular",
            "keywords": "",
            "filters[category][]": "itemtype:Games" 
        }
        
        try:
            resp = s.post(API_URL, data=payload, headers=headers, impersonate="chrome120")
            
            if resp.status_code != 200:
                print(f"Failed to fetch page {page}: {resp.status_code}")
                # Dump content for debug if it fails
                print(resp.text[:500]) 
                break
                
            try:
                data = resp.json()
            except Exception as e:
                print(f"Failed to parse JSON: {e}")
                print(resp.text[:500])
                break

            results = data.get('results', [])
            if not results:
                print("No more results.")
                break
                
            for res in results:
                if len(all_items) >= MAX_ITEMS:
                    break
                    
                rank = len(all_items) + 1
                
                # Check price logic from JS
                price = res.get('cost', 'N/A')
                # If discount/sale, they might override cost, but cost seems to be the display string
                
                link = res.get('url', '')
                if link and not link.startswith('http'):
                    link = f"https://marketplace.roll20.net{link}"
                
                title = html.unescape(res.get('name', 'Unknown'))
                publisher = html.unescape(res.get('author', 'Unknown'))
                
                item = {
                    "snapshot_date": datetime.date.today().isoformat(),
                    "rank": rank,
                    "title": title,
                    "publisher": publisher,
                    "category": "Games",
                    "price": price,
                    "url": link
                }
                all_items.append(item)
            
            # Pagination check
            total_pages = data.get('totalpages', 1)
            if page >= total_pages:
                print("Reached last page.")
                break
                
            page += 1
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"Error on page {page}: {e}")
            break

    print(f"Scraped {len(all_items)} items.")
    return all_items

if __name__ == "__main__":
    items = scrape_roll20()
    
    if items:
        # Push to BigQuery
        try:
            client = bigquery.Client() # Implicitly uses GOOGLE_APPLICATION_CREDENTIALS or default env
            # But user might need prompt to login? Usually this env works if `setup_roll20` worked.
            # Assuming env is set up.
            
            # Insert
            errors = client.insert_rows_json(TABLE_ID, items)
            if errors:
                print(f"BigQuery Insert Errors: {errors}")
            else:
                print(f"Successfully inserted {len(items)} rows into {TABLE_ID}")
                
        except Exception as e:
             print(f"BigQuery Push Failed: {e}")

def push_to_bigquery(items):
    if not items:
        print("No data to push.")
        return

    client = bigquery.Client()
    try:
        errors = client.insert_rows_json(TABLE_ID, items)
        if errors:
            print(f"BigQuery Insert Errors: {errors}")
        else:
            print(f"Successfully inserted {len(items)} rows into {TABLE_ID}")
    except Exception as e:
        print(f"BigQuery Push Failed: {e}")

if __name__ == "__main__":
    items = scrape_roll20()
    if items:
        # Print sample
        print("Sample Item:", items[0])
        push_to_bigquery(items)
±&"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0420file:///c:/Users/Yorri/.gemini/roll20_scraper.py:file:///c:/Users/Yorri/.gemini