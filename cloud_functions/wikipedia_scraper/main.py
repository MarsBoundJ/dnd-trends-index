import functions_framework
import requests
import datetime
import time
import json
from google.cloud import bigquery

# Config
PROJECT_ID = "dnd-trends-index"
REGISTRY_TABLE = "social_data.wikipedia_article_registry"
VIEWS_TABLE = "social_data.wikipedia_daily_views"
USER_AGENT = "DndTrendsIndexBot/1.0 (luckys-story-garden.com)"

# Wikimedia API
BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user"

def get_tracked_articles(client):
    query = f"""
        SELECT article_title, parent_category 
        FROM `{PROJECT_ID}.{REGISTRY_TABLE}`
        WHERE is_tracked = TRUE
        ORDER BY article_title ASC
        LIMIT 500
    """
    return client.query(query).result()

def fetch_daily_views(article_title, start_date, end_date):
    s_str = start_date.strftime("%Y%m%d")
    e_str = end_date.strftime("%Y%m%d")
    
    safe_title = article_title.replace(" ", "_").replace("/", "%2F")
    url = f"{BASE_URL}/{safe_title}/daily/{s_str}/{e_str}"
    headers = {"User-Agent": USER_AGENT}
    
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 429:
            print("Rate limit hit. Sleeping...")
            time.sleep(5)
            # Retry once
            r = requests.get(url, headers=headers)
        
        if r.status_code != 200:
            return []
            
        return r.json().get("items", [])
    except Exception as e:
        print(f"Error fetching {article_title}: {e}")
        return []

@functions_framework.http
def wikipedia_scraper_http(request):
    """
    HTTP entry point for Wikipedia Scraper.
    """
    print("🚀 Starting Wikipedia Scraper...")
    from watermark import HighWatermark
    
    watermark = HighWatermark("wikipedia")
    start_time, end_time = watermark.get_range(default_lookback_hours=168) # 7 days
    
    # Wiki API uses YYYYMMDD
    # start_time is ISO string (e.g. 2023-10-27T...)
    start_dt = datetime.datetime.fromisoformat(start_time).date()
    end_dt = datetime.datetime.fromisoformat(end_time).date()
    
    print(f"🕒 Target Date Range: {start_dt} -> {end_dt}")

    try:
        client = bigquery.Client()
        
        # 1. Get List
        articles = list(get_tracked_articles(client))
        print(f"Found {len(articles)} tracked articles.")
        
        all_rows = []
        total_rows_inserted = 0
        
        for i, row in enumerate(articles):
            title = row.article_title
            category = row.parent_category
            
            # Simple logging
            if i % 50 == 0:
                print(f"Processing {i}/{len(articles)}: {title}")
                
            views_data = fetch_daily_views(title, start_dt, end_dt)
            
            for day in views_data:
                # day['timestamp'] is YYYYMMDD00
                ds = day['timestamp'][:8]
                dt = datetime.datetime.strptime(ds, "%Y%m%d").date()
                params = {
                    "date": dt.isoformat(),
                    "article_title": title,
                    "views": day['views'],
                    "category": category
                }
                all_rows.append(params)
                
            if len(all_rows) >= 50:
                errors = client.insert_rows_json(VIEWS_TABLE, all_rows)
                if not errors:
                    total_rows_inserted += len(all_rows)
                else:
                    print(f"Batch Insert Errors: {errors}")
                all_rows = []
                
            time.sleep(0.05) 

        # Push remaining
        if all_rows:
            errors = client.insert_rows_json(VIEWS_TABLE, all_rows)
            if not errors:
                total_rows_inserted += len(all_rows)
            else:
                print(f"Final Batch Insert Errors: {errors}")
        
        result_stats = {
            "articles_scanned": len(articles),
            "rows_inserted": total_rows_inserted,
            "status": "success"
        }
        
        if result_stats['status'] == 'success':
            watermark.update_marker(end_time)
            
        return json.dumps(result_stats), 200

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500
