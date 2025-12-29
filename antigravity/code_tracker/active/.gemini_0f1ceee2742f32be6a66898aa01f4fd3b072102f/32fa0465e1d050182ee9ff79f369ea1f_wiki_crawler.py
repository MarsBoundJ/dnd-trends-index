Ü#
import requests
import time
import datetime
from google.cloud import bigquery

# Config
API_URL = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "DndTrendsIndexBot/1.0 (luckys-story-garden.com)"}
PROJECT_ID = "dnd-trends-index"
TABLE_ID = "social_data.wikipedia_article_registry"
ROOT_CATEGORY = "Category:Dungeons_&_Dragons"
MAX_DEPTH = 2

def get_category_members(category_name):
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": category_name,
        "cmlimit": "500",
        "format": "json",
        "cmtype": "page|subcat" 
    }
    try:
        r = requests.get(API_URL, params=params, headers=HEADERS)
        data = r.json()
        return data.get("query", {}).get("categorymembers", [])
    except Exception as e:
        print(f"Error fetching {category_name}: {e}")
        return []

def filter_wiki_page(title, ns):
    """
    Returns True if page should be kept, False otherwise.
    ns 0 = Article, ns 14 = Category
    """
    if ns == 14:
        return True # Follow all subcats (logic handled in loop)
    
    if ns != 0:
        return False # generic non-article page
        
    # Exclusions
    lower_title = title.lower()
    if lower_title.startswith("list of"):
        return False
    if lower_title.startswith("template:"):
        return False
    if lower_title.startswith("portal:"):
        return False
    
    # Generic term exclusion (if needed)
    generics = ["role-playing game", "tabletop game"]
    if lower_title in generics:
        return False
        
    return True

def crawl_wikipedia_tree(root_category, max_depth):
    visited = set()
    queue = [(root_category, 0)] # Tuple: (CategoryName, CurrentDepth)
    all_pages = [] # List of dicts

    print(f"Starting crawl from {root_category} (Max Depth: {max_depth})")

    while queue:
        current_cat, depth = queue.pop(0)
        
        if current_cat in visited:
            continue
        
        # Stop digging if too deep, but allow processing of current queue items if they are cats?
        # No, if depth > max, we don't fetch members.
        if depth > max_depth:
            continue

        print(f"[{len(all_pages)} items] Crawling: {current_cat} (Depth {depth})")
        visited.add(current_cat)
        
        # Throttling
        time.sleep(0.5) 
        
        members = get_category_members(current_cat)
        
        for member in members:
            title = member['title']
            ns = member['ns'] 
            
            # Simple Filter
            if not filter_wiki_page(title, ns):
                continue

            if ns == 0: # Article
                # Dedup check could happen here or in BQ. 
                # Let's just collect all and dedup by title later.
                
                # Tag logic: Use current_cat as tag
                tag = current_cat.replace("Category:", "").replace("_", " ")
                
                all_pages.append({
                    "article_title": title.replace(" ", "_"), # Wikipedia format favors underscores
                    "parent_category": tag,
                    "discovery_date": datetime.date.today().isoformat(),
                    "is_tracked": True
                })
            
            elif ns == 14: # Subcategory
                # Add to queue if not visited
                if title not in visited:
                    queue.append((title, depth + 1))
    
    return all_pages

def push_to_bigquery(pages):
    if not pages:
        print("No pages found to push.")
        return

    client = bigquery.Client()
    
    # Simple Dedup by title (keeping first occurrence which is usually highest level/first found)
    unique_pages = {}
    for p in pages:
        if p['article_title'] not in unique_pages:
            unique_pages[p['article_title']] = p
    
    final_list = list(unique_pages.values())
    print(f"Pushing {len(final_list)} unique articles to BigQuery...")
    
    # BQ Schema expects: article_title, parent_category, discovery_date, is_tracked
    try:
        errors = client.insert_rows_json(TABLE_ID, final_list)
        if errors:
            print(f"Errors: {errors}")
        else:
            print("Success!")
    except Exception as e:
        print(f"BQ Error: {e}")

if __name__ == "__main__":
    articles = crawl_wikipedia_tree(ROOT_CATEGORY, MAX_DEPTH)
    push_to_bigquery(articles)
Ü#"(0f1ceee2742f32be6a66898aa01f4fd3b072102f2.file:///C:/Users/Yorri/.gemini/wiki_crawler.py:file:///C:/Users/Yorri/.gemini