import asyncio
import json
import datetime
import random
import time
import os
import sys

from curl_cffi import requests as c_requests
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from google.cloud import bigquery

# Authentication Details
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"c:\Users\Yorri\dnd-trends\dnd-key.json"
PROXY_SOCKS = "socks5://lcbaurkt-US-rotate:q8aa993piq8h@p.webshare.io:9999"

# Core Keywords for Test
TARGET_KEYWORDS = ['Fighter', 'Monk', 'Wizard', 'Fireball', 'Dragon']
TABLE_ID = "dnd-trends-index.dnd_trends_categorized.trend_data_pilot"

def get_bq_client():
    return bigquery.Client()

def get_max_date_for_keyword(client, keyword):
    query = f"""
        SELECT MAX(date) as max_date
        FROM `{TABLE_ID}`
        WHERE search_term = @keyword
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("keyword", "STRING", keyword)
        ]
    )
    results = client.query(query, job_config=job_config).result()
    for row in results:
        if row.max_date:
            return row.max_date
    return datetime.date(2026, 2, 1) # Fallback if no data

async def human_mimicry(page):
    """Simulate human interaction."""
    print("  [*] Performing human mimicry (Jiggle & Scroll)...")
    await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
    await asyncio.sleep(random.uniform(0.5, 1.5))
    await page.mouse.wheel(0, random.randint(200, 600))
    await asyncio.sleep(random.uniform(1, 3))

async def fetch_trends_data(keyword, start_date, end_date):
    url = f"https://trends.google.com/trends/explore?q={keyword}&date={start_date.isoformat()}%20{end_date.isoformat()}&geo=US"
    print(f"[*] Targeting: {keyword}")
    print(f"[*] URL: {url}")
    
    cookies_to_add = []
    print("  [*] Phase 1: The Scout (curl_cffi TLS Mimicry)")
    try:
        c_session = c_requests.Session(
            impersonate="firefox",
            proxies={"http": PROXY_SOCKS, "https": PROXY_SOCKS}
        )
        c_res = c_session.get(url, timeout=30)
        print(f"  [curl_cffi] Status: {c_res.status_code}")
        for name, value in c_session.cookies.get_dict().items():
            cookies_to_add.append({
                "name": name,
                "value": value,
                "domain": ".google.com",
                "path": "/",
                "secure": True,
                "httpOnly": False
            })
    except Exception as ce:
        print(f"  [curl_cffi Error] {ce}")
        return None
        
    if not cookies_to_add:
        print("  [-] Failed to acquire scout cookies. Aborting target.")
        return None

    print(f"  [+] Acquired {len(cookies_to_add)} scout cookies. Initiating Handoff...")

    async with async_playwright() as p:
        browser = await p.firefox.launch(
            headless=True,
            proxy={
                "server": "socks5://p.webshare.io:9999",
                "username": "lcbaurkt-US-rotate",
                "password": "q8aa993piq8h"
            }
        )
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0"
        )
        
        await context.add_cookies(cookies_to_add)
        
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        captured_data = []

        async def handle_response(response):
            if "trends.google.com/trends/api/widgetdata/multiline" in response.url:
                print(f"    [INTERCEPT] {response.status} | {response.url[:100]}...")
                try:
                    if response.status == 200:
                        text = await response.text()
                        if text.startswith(")]}'"): text = text[5:]
                        data = json.loads(text)
                        timeline = data.get("default", {}).get("timelineData", [])
                        for entry in timeline:
                            dt = datetime.datetime.fromtimestamp(int(entry["time"]))
                            captured_data.append({
                                "date": dt.date().isoformat(),
                                "interest": entry["value"][0],
                                "is_partial": bool(entry.get("isPartial", False))
                            })
                except Exception as e: 
                    print(f"    [INTERCEPT ERROR] {e}")

        page.on("response", handle_response)
        
        print("  [*] Phase 2: Playwright Execution")
        response = await page.goto(url, wait_until="load", timeout=90000)
        
        if response:
            print(f"  [HTTP] Main Page Status: {response.status}")
        
        await human_mimicry(page)
        
        print("  [*] Waiting up to 30s for chart and API pulse...")
        for _ in range(30):
            if captured_data:
                break
            await asyncio.sleep(1)
            
        await browser.close()
        return captured_data

def process_keyword(client, keyword):
    max_date = get_max_date_for_keyword(client, keyword)
    today = datetime.date.today()
    
    # We want to fetch from max_date to today
    # But Google Trends range is inclusive, so we fetch max_date - 1 day to ensure overlap context, then filter
    start_date = max_date - datetime.timedelta(days=1)
    
    print(f"\n[{keyword}] Gap Analysis: Max Date is {max_date}. Fetching from {start_date} to {today}.")
    
    if max_date >= today:
        print(f"[{keyword}] Data is already current. Skipping.")
        return
        
    raw_data = asyncio.run(fetch_trends_data(keyword, start_date, today))
    
    if not raw_data:
        print(f"[{keyword}] Failed to retrieve data.")
        return
        
    # The Havdalah Fill (Extract missing dates)
    rows_to_insert = []
    seen_dates = set()
    for row in raw_data:
        row_date_str = row['date']
        row_date = datetime.date.fromisoformat(row_date_str)
        # Only take data strictly AFTER the current max_date in BQ
        if row_date > max_date and row_date_str not in seen_dates:
            rows_to_insert.append({
                "search_term": keyword,
                "date": row_date_str,
                "interest": row['interest'],
                "is_partial": row['is_partial'],
                "fetch_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "batch_id": "ORACLE_BRIDGE_1"
            })
            seen_dates.add(row_date_str)
            
    if rows_to_insert:
        print(f"[{keyword}] The Havdalah Fill: Pushing {len(rows_to_insert)} days of missing data to BigQuery...")
        errors = client.insert_rows_json(TABLE_ID, rows_to_insert)
        if errors:
            print(f"[{keyword}] BIGQUERY ERRORS: {errors}")
        else:
            print(f"[{keyword}] Successfully pushed to BigQuery.")
    else:
        print(f"[{keyword}] No new valid data points found in response to fill gap.")

if __name__ == "__main__":
    client = get_bq_client()
    print("="*60)
    print("[INITIATING LOCAL ORACLE BRIDGE]")
    print("="*60)
    for kw in TARGET_KEYWORDS:
        process_keyword(client, kw)
        # Sleep slightly between keywords
        time.sleep(random.uniform(2, 5))
    print("="*60)
    print("[ORACLE BRIDGE RUN COMPLETE]")
    print("="*60)
