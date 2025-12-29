Î from googleapiclient.discovery import build
from google.cloud import bigquery
import os
import datetime

# --- Configuration ---
# NOTE: Keys provided by user
API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"
CX_ID = "e1a6d54ae95c1451d"
BQ_TABLE_ID = "dnd-trends-index.commercial_data.google_shopping_snapshots"

# Safety Cap (Free Tier = 100/day)
MAX_QUERIES = 95 

def get_trending_keywords():
    client = bigquery.Client()
    # Placeholder: Fetch top 50 trending keywords (Velocity 24h)
    # Adjust logic to match actual trend table
    query = """
    SELECT search_term as keyword
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
    WHERE date = (SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`)
    ORDER BY interest DESC
    LIMIT 5
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        return [row.keyword for row in results]
    except Exception as e:
        print(f"BigQuery Fetch Failed: {e}")
        return []

def search_shopping_data(keyword):
    if not API_KEY or not CX_ID:
        print("CRITICAL: Missing API_KEY or CX_ID env vars.")
        return []

    service = build("customsearch", "v1", developerKey=API_KEY)
    
    # Query optimized for product finding
    query = f"{keyword} dnd 5e product -site:reddit.com -site:wikipedia.org"
    print(f"Searching: {query}...")
    
    try:
        res = service.cse().list(
            q=query,
            cx=CX_ID,
            num=5, # Reduce to 5 for debug
            safe="off"
        ).execute()
        
        products = []
        for item in res.get('items', []):
            pagemap = item.get('pagemap', {})
            metatags = pagemap.get('metatags', [{}])[0]
            
            # 1. Schema.org Offer (Best)
            offers = pagemap.get('offer', [])
            price = None
            currency = 'USD'
            
            if offers:
                price_str = offers[0].get('price')
                currency = offers[0].get('pricecurrency', 'USD')
                try: price = float(price_str)
                except: pass
            
            # 2. Availability Signals (Fallbacks)
            is_product = False
            if price: 
                is_product = True
            elif metatags.get('og:type') == 'product':
                is_product = True
            elif 'shop' in item['link'] or 'marketplace' in item['link'] or 'store' in item['link']:
                is_product = True
            
            # 3. Capture
            if is_product:
                products.append({
                    "snapshot_date": datetime.date.today().isoformat(),
                    "keyword": keyword,
                    "product_title": item['title'],
                    "price": price,
                    "currency": currency,
                    "retailer": metatags.get('og:site_name', item.get('displayLink')),
                    "link": item['link']
                })
                    
        return products

    except Exception as e:
        print(f"Shopping Search Failed for {keyword}: {e}")
        return []

def save_to_bq(rows):
    if not rows: return
    client = bigquery.Client()
    errors = client.insert_rows_json(BQ_TABLE_ID, rows)
    if errors:
        print(f"BQ Insert Errors: {errors}")
    else:
        print(f"Inserted {len(rows)} shopping records.")

def run_sampler():
    keywords = get_trending_keywords()
    print(f"Found {len(keywords)} trending keywords from BQ.")
    keywords.insert(0, "Vecna Eve of Ruin") # TEST INJECTION
    
    total_queries = 0
    all_products = []
    
    for kw in keywords:
        if total_queries >= MAX_QUERIES:
            print("Daily safety limit reached.")
            break
            
        products = search_shopping_data(kw)
        print(f"  -> Found {len(products)} products for '{kw}'")
        total_queries += 1
        all_products.extend(products)
    
    save_to_bq(all_products)

if __name__ == "__main__":
    run_sampler()
Î "(0f1ceee2742f32be6a66898aa01f4fd3b072102f22file:///C:/Users/Yorri/.gemini/shopping_sampler.py:file:///C:/Users/Yorri/.gemini