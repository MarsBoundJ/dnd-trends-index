Â(from curl_cffi import requests
from bs4 import BeautifulSoup
import re
import datetime
from google.cloud import bigquery
import os

# Config
BASE_URL = "https://www.backerkit.com"
TARGET_URL = "https://www.backerkit.com/c/collections/role-playing-games?sort_by=trending" # Corrected URL
BQ_TABLE = "dnd-trends-index.commercial_data.backerkit_projects"

def parse_funding(fund_str):
    if not fund_str: return 0.0
    s = fund_str.replace('$', '').replace(',', '').strip().lower()
    
    multiplier = 1.0
    if 'k' in s:
        multiplier = 1000.0
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000.0
        s = s.replace('m', '')
        
    try:
        if 'goal' in s: return 0.0
        return float(s) * multiplier
    except:
        return 0.0

def is_dnd_project(title, blurb):
    text = (title + " " + blurb).lower()
    keywords = ["5e", "5th edition", "2024", "level up", "black flag", "tales of the valiant", "mcdm rpg", "d&d", "dungeons & dragons", "dungeons and dragons"]
    hit = any(k in text for k in keywords)
    exclusions = ["board game", "dice", "card game"]
    # is_excluded = any(ex in text for ex in exclusions) # Relaxed for now
    if hit: return "5e Compatible"
    elif "osr" in text or "old school" in text: return "OSR"
    else: return "RPG (Other)" 

def fetch_backerkit_trending():
    print(f"Fetching {TARGET_URL} with curl_cffi...")
    try:
        response = requests.get(
            TARGET_URL,
            impersonate="chrome120",
            headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.backerkit.com/"
            },
            timeout=30
        )
    except Exception as e:
        print(f"CRITICAL ERROR: Request failed: {e}")
        return []
    
    if response.status_code != 200:
        print(f"Failed to fetch: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    projects = []
    
    # Updated Selector based on debug HTML
    # Look for the card container
    cards = soup.select('div[class*="project-card-small-component"]')
    
    print(f"Found {len(cards)} project cards.")
    
    for card in cards:
        try:
            # Title
            title_el = card.select_one('h1') or card.select_one('h2') or card.select_one('div[class*="title"]')
            title = title_el.get_text(strip=True) if title_el else "Unknown Title"
            
            # Creator
            # "by Steve Jackson Games" usually in a span inside an anchor
            creator_el = card.select_one('a[href*="/c/users/"]')
            creator = creator_el.get_text(strip=True).replace('by', '').strip() if creator_el else "Unknown Creator"
            
            # Link / Slug
            link_el = card.find('a')
            href = link_el['href'] if link_el else ""
            slug = href.split('/c/projects/')[-1].split('?')[0] if '/c/projects/' in href else "unknown"
            
            # Stats (Funding & Backers)
            # They are in a div with id stats_project_...
            # We can find them by looking for the '$' text or 'backers' text
            funding_str = "$0"
            backers_str = "0"
            
            all_text_divs = card.select('div')
            for d in all_text_divs:
                txt = d.get_text(strip=True)
                if '$' in txt and len(txt) < 20 and not 'goal' in txt.lower(): # "$97,901"
                     # Ensure it's the amount, not goal
                     funding_str = txt
                elif 'backers' in txt.lower(): # "2,119 backers"
                    backers_str = txt.lower().replace('backers', '').strip()

            # Refined Funding Parse: Look for specific class structure if text fails
            # stats = card.select('div[class*="stats_project"]')
            # if stats: ...
            
            system = is_dnd_project(title, card.get_text(" ", strip=True))
            
            projects.append({
                "project_id": slug,
                "title": title,
                "creator": creator,
                "funding_usd": parse_funding(funding_str),
                "backers_count": int(backers_str.replace(',', '')),
                "days_remaining": 30, # Hard to parse relative time easily
                "system_tag": system,
                "scraped_at": datetime.datetime.utcnow().isoformat(),
                "source_url": BASE_URL + href
            })
        except Exception as e:
            # print(f"Error parsing card: {e}")
            continue
            
    return projects

def save_to_bq(projects):
    if not projects:
        print("No projects to insert.")
        return
    client = bigquery.Client()
    errors = client.insert_rows_json(BQ_TABLE, projects)
    if not errors: print("Success.")
    else: print(f"Errors: {errors}")

if __name__ == "__main__":
    data = fetch_backerkit_trending()
    # valid_data = [d for d in data if d['title'] != "Unknown Title"] # Commented out for debug
    save_to_bq(data)
Â("(0346c7b262db785b9f82b154e34994382565350e23file:///C:/Users/Yorri/.gemini/backerkit_scraper.py:file:///C:/Users/Yorri/.gemini