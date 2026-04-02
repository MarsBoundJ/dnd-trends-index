import functions_framework
import requests
import json
import html as html_lib
from datetime import date
from google.cloud import bigquery

# Config
BQ_TABLE = "dnd-trends-index.commercial_data.roll20_rankings"
API_URL = "https://marketplace.roll20.net/browse/search"
MAX_ITEMS = 200


@functions_framework.http
def roll20_harvester_http(request):
    print("🚀 Starting Roll20 Harvest...")

    all_items = []
    page = 1
    snapshot_date = date.today().isoformat()

    while len(all_items) < MAX_ITEMS:
        try:
            resp = requests.post(
                API_URL,
                data={
                    "page": str(page),
                    "sortby": "popular",
                    "keywords": "",
                    "filters[category][]": "itemtype:Games",
                },
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": "https://marketplace.roll20.net/browse/search?category=itemtype:Games&sortby=popular",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"❌ Page {page} failed: {e}")
            break

        results = data.get("results", [])
        if not results:
            print("No more results.")
            break

        for res in results:
            if len(all_items) >= MAX_ITEMS:
                break
            link = res.get("url", "")
            if link and not link.startswith("http"):
                link = f"https://marketplace.roll20.net{link}"

            all_items.append({
                "snapshot_date": snapshot_date,
                "rank": len(all_items) + 1,
                "title": html_lib.unescape(res.get("name", "Unknown")),
                "publisher": html_lib.unescape(res.get("author", "Unknown")),
                "category": ", ".join(res.get("categories", ["Games"])),
                "price": str(res.get("cost", "N/A")),
                "url": link,
            })

        total_pages = data.get("totalpages", 1)
        if page >= total_pages:
            print("Reached last page.")
            break
        page += 1

    print(f"Scraped {len(all_items)} items across {page} pages.")

    if not all_items:
        return json.dumps({"status": "warning", "message": "No data"}), 200

    client = bigquery.Client()
    errors = client.insert_rows_json(BQ_TABLE, all_items)
    if errors:
        print(f"❌ BQ errors: {errors}")
        return json.dumps({"status": "error", "details": str(errors)}), 500

    msg = f"✅ Inserted {len(all_items)} Roll20 rankings."
    print(msg)
    return json.dumps({"status": "success", "message": msg}), 200
