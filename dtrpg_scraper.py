import asyncio
import datetime
import os
from playwright.async_api import async_playwright
from google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.commercial_data'
VELOCITY_TABLE = f'{DATASET_ID}.dtrpg_velocity'
INVENTORY_TABLE = f'{DATASET_ID}.dtrpg_inventory'

# Task 5.1: Medal Weights & Filters
MEDAL_MAP = {
    'copper': 51,
    'silver': 101,
    'electrum': 251,
    'gold': 501,
    'platinum': 1001,
    'mithral': 2501,
    'adamantine': 5001
}

# URLs
DTRPG_HOTTEST = "https://www.drivethrurpg.com/hottest.php"

# Task 5.3: Ruleset Classification
def classify_ruleset(title, description, ruleset_filter=""):
    # Normalize
    text_blob = (str(title) + " " + str(description)).lower()
    
    if "2024" in text_blob or "revised 5e" in text_blob or "5.5e" in text_blob:
        return "5e_2024"
    
    # Heuristic: If explicitly filtered for 5e Legacy (Filter ID 44830) OR just general '5e' without 2024 markers
    if "5e" in ruleset_filter or "fifth edition" in text_blob: 
        return "5e_Legacy"
        
    if "osr" in ruleset_filter or "osr" in text_blob:
        return "OSR"
        
    return "Generic/Unknown"

# Task 5.4: Category Logic
def categorize_product(breadcrumbs_text, description):
    b_text = str(breadcrumbs_text).lower()
    d_text = str(description).lower()
    
    if "adventure" in b_text or "module" in b_text:
        return "Adventure"
    if "rulebook" in b_text or "core" in b_text:
        return "Core Rules"
    if "map" in b_text and "battle" in b_text:
        return "Maps"
    if "monster" in b_text or "bestiary" in b_text:
        return "Monsters"
    if "class" in b_text or "archetype" in b_text:
        return "Player Options"
    if "dm" in b_text or "gamemaster" in b_text:
        return "DM Resources"
        
    # Fallback Heuristics from Description
    if "new spells" in d_text: return "Player Options"
    if "stat blocks" in d_text: return "Monsters"
    
    return "General"

class DtrpgScraper:
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.today = datetime.date.today().isoformat()

    async def scrape_velocity(self):
        target_url = "https://www.drivethrurpg.com/metal.php"
        print(f"Refueling scraper... Target: {target_url}")
        velocity_rows = []

        async with async_playwright() as p:
            # Launch with anti-detect args
            browser = await p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            try:
                await page.goto(target_url, timeout=90000)
                
                # DEBUG: Just wait for 'body' then screenshot/dump
                await page.wait_for_selector('body', timeout=30000)
                await asyncio.sleep(5) # Give it a moment to settle
                
                content = await page.content()
                with open("debug_page_source.html", "w", encoding="utf-8") as f:
                    f.write(content)
                
                # Check if we are still on a challenge page
                if "challenge" in await page.title():
                    print("Still stuck on Cloudflare Challenge page.")
                    
                # Try to find the cells loosely
                cells = await page.query_selector_all('td.smallText, th.smallText')
                print(f"Found {len(cells)} product cells. data mining...")

                rank = 1
                for cell in cells:
                    if rank > 100: break # Limit
                    
                    # 1. Link & Title & ID
                    link_el = await cell.query_selector('a[href*="product/"]')
                    if not link_el: continue
                    
                    href = await link_el.get_attribute('href')
                    title = await link_el.inner_text()
                    
                    # Extract ID: .../product/12345/...
                    import re
                    pid_match = re.search(r'product/(\d+)', href)
                    prod_id = pid_match.group(1) if pid_match else f"unknown_{rank}"
                    
                    # 2. Price (Full text of cell usually ends with price)
                    cell_text = await cell.inner_text()
                    # simplistic extraction: find last $... or "Pay What You Want"
                    price = 0.0
                    matches = re.findall(r'\$(\d+\.\d{2})', cell_text)
                    if matches:
                        price = float(matches[-1]) # Take the last one (usually sale price)
                    
                    # 3. Publisher matches (image alt or text? Hard in this view)
                    publisher = "Unknown" 
                    
                    # 4. Ruleset & Category
                    ruleset = classify_ruleset(title, cell_text)
                    category = categorize_product("", title)
                    
                    # 5. Medal Inference (based on rank roughly?)
                    # Top 10 = Adamantine-ish? 
                    # For now, we leave medal as "Metal Page" to indicate source.
                    medal = "Metal List"

                    v_row = {
                        "date": self.today,
                        "product_id": prod_id,
                        "product_name": title,
                        "publisher": publisher,
                        "medal_level": medal,
                        "category": category,
                        "ruleset": ruleset,
                        "rank": rank,
                        "price": price
                    }
                    velocity_rows.append(v_row)
                    rank += 1
                    
                print(f"Scraped {len(velocity_rows)} items.")
                self.insert_bq(velocity_rows, VELOCITY_TABLE)
                
            except Exception as e:
                print(f"Error during scraping: {e}")
                await page.screenshot(path="debug_error.png")
            
            await browser.close()

    def insert_bq(self, rows, table_id):
        if not rows: return
        errors = self.bq_client.insert_rows_json(table_id, rows)
        if errors == []:
            print(f"✅ Successfully inserted {len(rows)} rows into {table_id}.")
        else:
            print(f"❌ Errors inserting: {errors}")

if __name__ == "__main__":
    scraper = DtrpgScraper()
    asyncio.run(scraper.scrape_velocity())
