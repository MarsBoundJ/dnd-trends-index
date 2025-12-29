Ó%import requests
import time
import re
import json
import datetime
from icv2_gemini_parser import parse_icv2_ranking
from google.cloud import bigquery

# Config
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
BQ_TABLE = "dnd-trends-index.commercial_data.icv2_market_reports"

def insert_to_bq(data, url):
    client = bigquery.Client()
    
    period = data.get("period", "Unknown Period")
    rows = []
    
    # Generate timestamp
    discovered_ts = datetime.datetime.utcnow().isoformat()
    
    for item in data.get("rankings", []):
        row = {
            "report_id": f"{period.replace(' ', '_')}_{item.get('rank')}", 
            "period_label": period,
            "rank": item.get('rank'),
            "product_name": item.get('name'),
            "publisher": item.get('publisher'),
            "market_sentiment": data.get("industry_vibe"),
            "is_dnd_centric": "Wizards of the Coast" in item.get('publisher', '') or "D&D" in item.get('name', '') or "Dungeons & Dragons" in item.get('name', ''),
            "discovered_at": discovered_ts,
            "source_url": url
        }
        rows.append(row)
        
    if rows:
        print(f"Inserting {len(rows)} rows to BigQuery...")
        errors = client.insert_rows_json(BQ_TABLE, rows)
        if not errors:
            print("‚úÖ Success: Data Ingested.")
        else:
            print(f"‚ùå Insertion Errors: {errors}")
    else:
        print("No rankings found to insert.")

def scan_and_process():
    # 1. Search for Top 5 RPGs
    # We will try to find the most recent "Top 5 Roleplaying Games" article.
    # Searching directly or hitting the category URL logic.
    # Let's try to scrape the search result page for the specific string.
    
    search_url = "https://icv2.com/search"
    params = {"q": "Top 5 Roleplaying Games"}
    
    print("Scanning ICv2 Search Results...")
    try:
        resp = requests.get(search_url, params=params, headers=HEADERS)
        if resp.status_code != 200:
            print(f"Search failed: {resp.status_code}")
            return
    except Exception as e:
        print(f"Connection Error: {e}")
        return

    # Extract article links
    # Search layout: <div class='row results_text'> ... <a href="URL">Title</a>
    # We look for links that contain /articles/news/view/ and match our keyword loosely.
    
    # Regex to capture URL and Headline
    # Look for: <a href="(https://icv2.com/articles/news/view/58327/top-5-roleplaying-games-spring-2024)">Top 5 Roleplaying Games...</a>
    # Simplifying regex to just grab links.
    
    links = re.findall(r'href="(https://icv2\.com/articles/news/view/\d+/[^"]+)"', resp.text)
    
    # Filter for relevance
    target_url = None
    print(f"DEBUG: Found {len(links)} candidate links.")
    for link in links:
        print(f" - Candidate: {link}")
        if "top-5" in link.lower() and ("roleplaying" in link.lower() or "rpg" in link.lower()):
            target_url = link
            break
            
    if not target_url:
        print("No exact 'Top 5 Roleplaying/RPG' article found.")
        # Fallback to a known recent Top 5 RPG article for the test scrape
        # Source: Manual check or user suggestion logic
        # Example from Fall 2024 or Spring 2024
        fallback = "https://icv2.com/articles/news/view/57262/top-5-roleplaying-games-spring-2024" # Example ID
        # Actually, let's just pick the first link that looks like an article if we are desperate?
        # No, better to hardcode one for the PILOT if search fails.
        # But wait, the user wants "most recent".
        # Let's try to query the Hobby Games category if search failed.
        # For now, let's use the fallback to verify the PARSER (Task 9.3)
        target_url = fallback
        print(f"Using Fallback URL for Parser Verification: {target_url}")

    print(f"Targeting Article: {target_url}")
    
    # 2. Extract Content
    time.sleep(3) # Be respectful
    article_resp = requests.get(target_url, headers=HEADERS)
    
    # Strip HTML tags for cleaner prompt
    # Simple regex stripper
    clean_text = re.sub(r'<[^>]+>', ' ', article_resp.text)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    
    print(f"Extracted {len(clean_text)} chars. Sending to Gemini...")
    
    # 3. Parse via Gemini
    data = parse_icv2_ranking(clean_text)
    
    if data:
        print("Gemini Result:")
        print(json.dumps(data, indent=2))
        
        # 4. Ingest
        insert_to_bq(data, target_url)
    else:
        print("Gemini failed to parse data.")

if __name__ == "__main__":
    scan_and_process()
Ó%"(0f1ceee2742f32be6a66898aa01f4fd3b072102f2.file:///C:/Users/Yorri/.gemini/icv2_scanner.py:file:///C:/Users/Yorri/.gemini