
import requests
import datetime
import time
from google.cloud import bigquery

# Config
PROJECT_ID = "dnd-trends-index"
REGISTRY_TABLE = "social_data.wikipedia_article_registry"
VIEWS_TABLE = "social_data.wikipedia_daily_views"
USER_AGENT = "DndTrendsIndexBot/1.0 (luckys-story-garden.com)"

# Wikimedia API (User Agent Policy Required)
# endpoint: /metrics/pageviews/per-article/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}
BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user"

import argparse

def get_tracked_articles(client, limit=None, new_only=False):
    today_str = datetime.date.today().isoformat()
    query = f"""
        SELECT article_title, parent_category 
        FROM `{PROJECT_ID}.{REGISTRY_TABLE}`
        WHERE is_tracked = TRUE
    """
    if new_only:
        query += f" AND discovery_date = '{today_str}'"
        
    if limit:
        query += f" LIMIT {limit}"
        
    return client.query(query).result()

def fetch_daily_views(article_title, start_date, end_date):
    # API expects YYYYMMDD
    s_str = start_date.strftime("%Y%m%d")
    e_str = end_date.strftime("%Y%m%d")
    
    # URL Encoding for titles with spaces/special chars (wiki titles usually underscores)
    safe_title = article_title.replace(" ", "_").replace("/", "%2F")
    
    url = f"{BASE_URL}/{safe_title}/daily/{s_str}/{e_str}"
    headers = {"User-Agent": USER_AGENT}
    
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 429:
            print("Rate limit hit. Sleeping...")
            time.sleep(5)
            return []
        
        if r.status_code != 200:
            # print(f"Failed {article_title}: {r.status_code}")
            return []
            
        return r.json().get("items", [])
    except Exception as e:
        print(f"Error fetching {article_title}: {e}")
        return []

def run_scraper(limit=None, new_only=False):
    client = bigquery.Client()
    
    # 1. Get List
    articles = list(get_tracked_articles(client, limit=limit, new_only=new_only))
    print(f"Found {len(articles)} tracked articles (Limit={limit}, NewOnly={new_only}).")
    
    # 2. Define Date Range (Last 30 days)
    today = datetime.date.today()
    end_date = today
    start_date = today - datetime.timedelta(days=30)
    
    all_rows = []
    
    for i, row in enumerate(articles):
        title = row.article_title
        category = row.parent_category
        
        if i % 10 == 0:
            print(f"Processing {i}/{len(articles)}: {title}")
            
        views_data = fetch_daily_views(title, start_date, end_date)
        
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
            print(f"Pushing batch of {len(all_rows)}...")
            client.insert_rows_json(VIEWS_TABLE, all_rows)
            all_rows = []
            
        time.sleep(0.05) 

    # Push remaining
    if all_rows:
        print(f"Pushing final {len(all_rows)}...")
        client.insert_rows_json(VIEWS_TABLE, all_rows)
    else:
        print("No view data collected.")

def create_views_table(client):
    dataset_ref = client.dataset("social_data")
    table_ref = dataset_ref.table("wikipedia_daily_views")
    
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("article_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("views", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE")
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    table.clustering_fields = ["category", "article_title"]
    
    try:
        client.create_table(table)
        print("Created views table.")
    except Exception:
        pass # Exists

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--new-only", action="store_true")
    args = parser.parse_args()
    run_scraper(limit=args.limit, new_only=args.new_only)
