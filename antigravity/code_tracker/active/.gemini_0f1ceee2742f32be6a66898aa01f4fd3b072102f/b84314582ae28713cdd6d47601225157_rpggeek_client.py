–$import requests
import time
import defusedxml.ElementTree as ET
from urllib.parse import quote

# Config
BASE_URL = "https://boardgamegeek.com/xmlapi2"
# BGG requires a user-agent to avoid default python-requests block
HEADERS = {"User-Agent": "DndTrendsIndex/1.0 (contact: admin@dndtrends.com)"}

def safe_get_xml(endpoint, params=None):
    """
    Strictly rate-limited fetcher for BGG API.
    Sleeps 5 seconds BEFORE every request to ensure safety.
    """
    print(f"DEBUG: Sleeping 5s before request to {endpoint}...")
    time.sleep(5) # CRITICAL: Do not remove
    
    url = f"{BASE_URL}/{endpoint}"
    
    # Auth Logic
    import os
    api_key = os.getenv("BGG_API_KEY")
    req_headers = HEADERS.copy()
    if api_key:
        req_headers["Authorization"] = f"Bearer {api_key}"
    
    try:
        response = requests.get(url, params=params, headers=req_headers, timeout=30)
        
        if response.status_code == 200:
            return ET.fromstring(response.content)
        elif response.status_code == 202:
            print("Status 202 (Accepted/Queued). Sleeping 10s and retrying...")
            time.sleep(10)
            return safe_get_xml(endpoint, params)
        elif response.status_code == 429:
            print("Rate limit hit (429)! Sleeping 60s...")
            time.sleep(60)
            return safe_get_xml(endpoint, params)
        else:
            print(f"Error {response.status_code}: {url}")
            return None
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

def search_rpg_items(keyword):
    """
    Task 7.2: Discovery Loop
    """
    params = {
        "type": "rpgitem",
        "query": keyword
    }
    print(f"Searching for: {keyword}")
    root = safe_get_xml("search", params)
    items = []
    
    if root is not None:
        # DEBUG: Print raw XML to see what's happening
        try:
             import defusedxml.ElementTree as ET
             print(f"Raw XML: {ET.tostring(root, encoding='unicode')[:500]}...") 
        except: pass
        
        for item in root.findall("item"):
            # Refinement: Capture ID and Name
            i_id = item.get("id")
            name_el = item.find("name")
            name = name_el.get("value") if name_el is not None else "Unknown"
            
            # Basic fuzzy match check could go here, but for now capturing all
            items.append({"id": i_id, "name": name})
    else:
        print("DEBUG: Root was None (Request Failed?)")
            
    return items

def get_item_stats(item_id):
    """
    Task 7.3: The 'Stats' Fetcher
    """
    # stats=1 to get ownership data
    params = {"id": item_id, "stats": 1, "type": "rpgitem"}
    print(f"Fetching stats for ID: {item_id}")
    root = safe_get_xml("thing", params)
    
    if root is not None:
        item = root.find("item")
        if item is None: return None
        
        # Parse Stats
        stats_node = item.find("statistics")
        ratings = stats_node.find("ratings") if stats_node is not None else None
        
        owned = 0
        rating = 0.0
        
        if ratings is not None:
            usersowned = ratings.find("usersowned")
            owned = int(usersowned.get("value")) if usersowned is not None else 0
            
            avg = ratings.find("average")
            rating = float(avg.get("value")) if avg is not None else 0.0

        # Family Check
        is_dnd = False
        description = item.find("description").text if item.find("description") is not None else ""
        
        for link in item.findall("link"):
            if link.get("type") == "rpgfamily":
                val = link.get("value").lower()
                if "dungeons & dragons" in val or "d&d" in val or "5th edition" in val:
                    is_dnd = True

        return {
            "item_id": int(item_id),
            "title": item.find("name").get("value"),
            "owned_count": owned,
            "rating": rating,
            "is_dnd": is_dnd
        }
    return None

if __name__ == "__main__":
    # Test Run
    print("--- Starting Test Run ---")
    
    # 1. Search
    hits = search_rpg_items("Player's Handbook")
    print(f"Found {len(hits)} hits.")
    
    if hits:
        # 2. Stats for first hit
        target = hits[0]
        print(f"Fetching stats for {target['name']} (ID: {target['id']})...")
        stats = get_item_stats(target['id'])
        print("Result:")
        print(stats)
–$"(0f1ceee2742f32be6a66898aa01f4fd3b072102f20file:///C:/Users/Yorri/.gemini/rpggeek_client.py:file:///C:/Users/Yorri/.gemini