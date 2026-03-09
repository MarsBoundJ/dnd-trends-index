import requests
import datetime
import time
from bs4 import BeautifulSoup
from google.cloud import bigquery
import statistics

# Configuration
WIKI_BASE = "https://dnd5e.fandom.com"
API_BASE = f"{WIKI_BASE}/api/v1/Articles"
CATEGORIES = ['Monster', 'Spell', 'Magic Item', 'Class', 'Race', 'Deity', 'Location', 'NPC']
# Note: 'Magic Item' might need to be 'Magic_Item' or 'Category:Magic_Items' depending on API. 
# Fandom API usually handles spaces or we might need to be precise. 
# Attempting "Top" with category expansion.

BQ_CLIENT = bigquery.Client()
TARGET_TABLE = "dnd-trends-index.dnd_trends_categorized.crunch_coefficients"

def get_articles_for_category(category, limit=30):
    """
    Fetches articles by scraping the Fandom Category page directly.
    """
    # Manual Category Mapping
    # Debug results show Singular names are standard: Category:Spell, Category:Class, Category:Race
    cat_map = {
        'Monster': 'Monster',       # Guessing Singular based on pattern
        'Spell': 'Spell',           # Confirmed
        'Magic Item': 'Magic_Item', # Guessing
        'Class': 'Class',           # Confirmed
        'Race': 'Race',             # Confirmed
        'Deity': 'Deity',           # Guessing
        'Location': 'Location',     # Guessing
        'NPC': 'NPC'                # Guessing
    }
    
    # "Locations" check: https://dnd5e.fandom.com/wiki/Category:Locations
    # "Places" check: https://dnd5e.fandom.com/wiki/Category:Places
    # I'll stick to 'Locations' but fallback if needed. Actually 'Places' is often a subcat.
    # We will use 'Locations'.
    
    cat_slug = cat_map.get(category, category) # Default to as-is (Singular)
    url = f"{WIKI_BASE}/wiki/Category:{cat_slug}"
    
    print(f"Scraping list from {url}...")
    
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"Failed to load category page: {r.status_code}")
            return []
            
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # Fandom Category Members are usually in div.category-page__members
        # links are a.category-page__member-link
        links = soup.find_all("a", class_="category-page__member-link")
        
        items = []
        for link in links[:limit]:
            title = link.get_text().strip()
            href = link.get('href') # e.g. /wiki/Fireball
            if href and not "Category:" in title:
                items.append({'title': title, 'url': href})
                
        print(f"  Found {len(items)} items.")
        return items
        
    except Exception as e:
        print(f"Error scraping category list for {category}: {e}")
        return []

def scrape_and_calculate(article_url):
    """
    Fetches HTML, parses, and returns counts.
    """
    full_url = f"{WIKI_BASE}{article_url}"
    try:
        r = requests.get(full_url, timeout=10)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # 1. Crunch Mass (Infobox + Tables + Lists)
        crunch_text = ""
        
        # Infoboxes (class 'portable-infobox')
        infoboxes = soup.find_all("aside", class_="portable-infobox")
        for box in infoboxes:
            crunch_text += box.get_text()
            
        # Tables
        tables = soup.find_all("table")
        for tbl in tables:
            crunch_text += tbl.get_text()

        # Lists (Feature Lists, Stat breakdowns) - NEW for V2
        # ul, ol, and dl (Definition Lists are huge for Class Features)
        lists = soup.find_all(["ul", "ol", "dl"])
        for lst in lists:
            # Avoid double counting if lists are nested?
            # get_text() on parent includes children. 
            # If we find all 'ul', we might find nested 'ul'.
            # Better to find top-level lists? 
            # Simpler: Just find all list items 'li', 'dt', 'dd'.
            pass
            
        # Refined List logic: Count content of list items directly
        list_items = soup.find_all(["li", "dt", "dd"])
        for item in list_items:
            crunch_text += item.get_text()
            
        # 2. Fluff Mass (Paragraphs)
        fluff_text = ""
        # Get 'mw-parser-output' content usually
        content_div = soup.find("div", class_="mw-parser-output")
        if content_div:
            # We want paragraphs that are NOT inside tables or infoboxes
            # But 'p' tags are rarely inside tables in MediaWiki (usually td).
            # 'p' inside 'li'? Possible.
            # If we count 'li' as crunch, we should NOT count 'p' inside 'li' as fluff.
            # So we only want direct-ish p?
            # Or just all 'p'. If a 'p' is inside an 'li', it counts towards Fluff here?
            # If we count 'li' text as Crunch, and 'p' inside it as Fluff, we dilute the Crunch.
            
            # Implementation: Iterate all P. If parent is not a list/table/infobox, count as fluff.
            all_ps = content_div.find_all("p")
            for p in all_ps:
                # Check parents
                if not p.find_parent(["table", "aside", "li", "dd", "dt"]):
                    fluff_text += p.get_text()
        else:
            # Fallback
            all_ps = soup.find_all("p")
            for p in all_ps:
                 if not p.find_parent(["table", "aside", "li", "dd", "dt"]):
                    fluff_text += p.get_text()

                
        crunch_len = len(crunch_text)
        fluff_len = len(fluff_text)
        
        return crunch_len, fluff_len
        
    except Exception as e:
        # print(f"Error scraping {article_url}: {e}")
        return 0, 0

def run_calibration():
    results = []
    
    for cat in CATEGORIES:
        articles = get_articles_for_category(cat, limit=30)
        if not articles:
            print(f"Skipping {cat} (No articles found)")
            continue
            
        ratios = []
        print(f"  Analyzing {len(articles)} sample pages for {cat}...")
        
        for item in articles:
            url = item.get('url')
            if not url: continue
            
            c_len, f_len = scrape_and_calculate(url)
            total = c_len + f_len
            
            if total > 50: # Ignore stubs/empty
                ratio = c_len / total
                ratios.append(ratio)
            
            time.sleep(0.1) # Be nice
            
        if ratios:
            avg_crunch = statistics.mean(ratios)
            print(f"  > {cat}: {avg_crunch:.4f} (Samples: {len(ratios)})")
            
            results.append({
                "category": cat,
                "sample_size": len(ratios),
                "crunch_factor": round(avg_crunch, 4),
                "last_updated": datetime.date.today().isoformat()
            })
        else:
            print(f"  > {cat}: No valid data.")

    # Write to BigQuery
    if results:
        # Define Schema
        schema = [
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sample_size", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("crunch_factor", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("last_updated", "DATE", mode="REQUIRED")
        ]
        
        # Create table if not exists
        try:
            table = bigquery.Table(TARGET_TABLE, schema=schema)
            BQ_CLIENT.create_table(table)
            print(f"Created table {TARGET_TABLE}")
        except Exception:
            pass # Exists
            
        # Truncate and Insert (Refresh methodology)
        # Actually, let's just insert new rows. 
        # But for 'calibration' usually we want the current truth.
        # Let's DELETE existing for today? Or just append.
        # User output requirement: "Query the results... ORDER BY crunch_factor DESC".
        # Appending is safer.
        
        errors = BQ_CLIENT.insert_rows_json(TARGET_TABLE, results)
        if errors:
            print(f"BQ Errors: {errors}")
        else:
            print("Successfully uploaded coefficients.")

if __name__ == "__main__":
    run_calibration()
