import os
import datetime
import cloudscraper
from bs4 import BeautifulSoup
from google.cloud import bigquery
import json

PROJECT_ID = "dnd-trends-index"
PROXY_URL = os.environ.get("PROXY_URL")
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

def fetch_jams_html():
    url = "https://itch.io/jams?filter=physical"
    print(f"Fetching Jams from {url} via proxy...")
    
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    response = scraper.get(url, proxies=PROXIES, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text

def parse_relative_date(date_str):
    """
    Itch returns strings like 'Starts in 4 days' or 'Ended 2 months ago'.
    Returns an ISO timestamp or None.
    """
    date_str = date_str.lower().strip()
    now = datetime.datetime.utcnow()
    
    try:
        if 'in ' in date_str:
            num = int(''.join(filter(str.isdigit, date_str)))
            if 'day' in date_str:
                return (now + datetime.timedelta(days=num)).isoformat()
            elif 'month' in date_str:
                return (now + datetime.timedelta(days=num * 30)).isoformat()
            elif 'hour' in date_str:
                return (now + datetime.timedelta(hours=num)).isoformat()
        elif 'ago' in date_str:
            num = int(''.join(filter(str.isdigit, date_str)))
            if 'day' in date_str:
                return (now - datetime.timedelta(days=num)).isoformat()
            elif 'month' in date_str:
                return (now - datetime.timedelta(days=num * 30)).isoformat()
            elif 'year' in date_str:
                return (now - datetime.timedelta(days=num * 365)).isoformat()
    except Exception:
        pass
    
    return None

import re
def extract_jams(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    rows = []
    
    # Itch Jams are rendered via React in a script block that calls R.Jam.FilteredJamCalendar
    script_match = re.search(r'R\.Jam\.FilteredJamCalendar\(\{"jams":(\[.*?\])\}\)', html_content)
    
    if script_match:
        try:
            jams_data = json.loads(script_match.group(1))
            for j in jams_data:
                jam_id = str(j.get('id', ''))
                jam_title = j.get('title', 'Unknown Jam')
                sub_count = j.get('joined', 0)
                
                # BigQuery requires valid TIMESTAMP strings or NULL
                start_date = j.get('start_date')
                end_date = j.get('end_date')
                
                rows.append({
                    "jam_id": jam_id,
                    "jam_title": jam_title,
                    "theme_keywords": [],
                    "submission_count": sub_count,
                    "start_date": start_date if start_date else None,
                    "end_date": end_date if end_date else None
                })
        except json.JSONDecodeError as e:
            print(f"Failed to decode React JSON: {e}")
            
    return rows

def merge_jams_to_bq(client, rows):
    if not rows:
        print("No jams found to merge.")
        return
        
    table_ref = f"{PROJECT_ID}.dnd_trends_raw.itchio_jams"
    
    query = f"""
    MERGE `{table_ref}` T
    USING (
      SELECT 
        JSON_VALUE(p, '$.jam_id') AS jam_id,
        JSON_VALUE(p, '$.jam_title') AS jam_title,
        ARRAY(SELECT JSON_VALUE(x) FROM UNNEST(JSON_EXTRACT_ARRAY(p, '$.theme_keywords')) AS x) AS theme_keywords,
        CAST(JSON_VALUE(p, '$.submission_count') AS INT64) AS submission_count,
        IF(JSON_VALUE(p, '$.start_date') IS NOT NULL, CAST(JSON_VALUE(p, '$.start_date') AS TIMESTAMP), NULL) AS start_date,
        IF(JSON_VALUE(p, '$.end_date') IS NOT NULL, CAST(JSON_VALUE(p, '$.end_date') AS TIMESTAMP), NULL) AS end_date
      FROM UNNEST(JSON_EXTRACT_ARRAY(PARSE_JSON(@records_json))) AS p
    ) S
    ON T.jam_id = S.jam_id
    WHEN MATCHED THEN
      UPDATE SET 
        jam_title = S.jam_title,
        submission_count = S.submission_count,
        end_date = S.end_date
    WHEN NOT MATCHED THEN
      INSERT (jam_id, jam_title, theme_keywords, submission_count, start_date, end_date)
      VALUES (S.jam_id, S.jam_title, S.theme_keywords, S.submission_count, S.start_date, S.end_date)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("records_json", "STRING", json.dumps(rows))
        ]
    )
    
    client.query(query, job_config=job_config).result()
    print(f"✅ Successfully MERGED {len(rows)} jams into {table_ref}.")

if __name__ == "__main__":
    client = get_bq_client()
    try:
        html = fetch_jams_html()
        jams = extract_jams(html)
        print(f"Extracted {len(jams)} jams from HTML.")
        
        for j in jams[:3]:
            print(f"- {j['jam_title']} (Submissions: {j['submission_count']})")
            
        merge_jams_to_bq(client, jams)
        
    except Exception as e:
        import traceback
        print(f"Error scraping jams: {e}")
        traceback.print_exc()
