"""
dndbeyond_harvester — Track WotC's digital product strategy on D&D Beyond.

Scrapes the DDB marketplace (marketplace.dndbeyond.com) to monitor:
  - Product catalog (titles, prices, formats, SKUs)
  - New product launches and price changes
  - Featured/promoted products on homepage
  - Digital-only vs book+digital bundles vs physical

Method: Server-rendered HTML scraping of product pages. The marketplace is
a React SPA (Hasbro Pulse) but product detail pages include structured data
in the initial HTML load. Category pages require JS rendering, so we maintain
a registry of known product SKUs and check each individually.

Intelligence Value: Track WotC's digital pivot — when digital-only products
increase, when bundle pricing shifts, when new product types appear.
"""

import os
import re
import json
import datetime
import time
import functions_framework
import requests
from google.cloud import bigquery

PROJECT = "dnd-trends-index"
RAW_DATASET = f"{PROJECT}.dnd_trends_raw"
CATALOG_TABLE = f"{RAW_DATASET}.dndbeyond_catalog"

MARKETPLACE_BASE = "https://marketplace.dndbeyond.com"
REQUEST_DELAY = 2  # Be polite to DDB servers

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

# Known D&D Beyond product SKUs — expanded over time as we discover new ones
# Seeded from the marketplace homepage + known major releases
KNOWN_PRODUCT_SKUS = [
    # 2024/2025 Core Rules
    "6015000",   # Player's Handbook 2024
    "6017000",   # Dungeon Master's Guide 2024
    "6019000",   # Monster Manual 2025
    "6018000",   # Core Rulebooks Bundle
    # Bundles & Featured
    "CBR4VNLFT", # Ravenloft: The Horrors Within Ultimate Bundle
    "328N2NP",   # Featured product
    "CBUHIKXOD", # Featured product
    "528M8ZF",   # Featured product
    "CBFS8K0Y7", # Featured product
    "EIP64LD",   # Featured product
    "Q4AXQ3W",   # Featured product
    # Adventures
    "8XTUXUM",   # Product from category
    # Classic sourcebooks (will be discovered dynamically)
]


def _extract_product_data(html, sku):
    """
    Extract product metadata from a DDB marketplace product page HTML.

    The page embeds product data in the server-rendered HTML as part of
    the React state hydration. We extract key fields via regex patterns.
    """
    product = {"sku": sku}

    # Product name — appears in <title> and in structured data
    name_match = re.search(r'"name":"([^"]+)"', html)
    if name_match:
        # Skip generic names like "D&D Beyond", "Localhost", country names
        skip_names = {"D&D Beyond", "Localhost", "United States", "United Kingdom",
                      "France", "Germany", "Spain", "Italy", "Language"}
        for m in re.finditer(r'"name":"([^"]+)"', html):
            name = m.group(1)
            if name not in skip_names and len(name) > 3:
                product["title"] = name
                break

    # Price — first meaningful price in the page
    prices = re.findall(r'"price":(\d+\.?\d*)', html)
    if prices:
        # Filter out 0 and very high prices (bundles)
        valid_prices = [float(p) for p in prices if 0 < float(p) < 500]
        if valid_prices:
            product["price"] = min(valid_prices)  # Lowest = base price
            product["bundle_price"] = max(valid_prices) if len(valid_prices) > 1 else None

    # Description
    desc_match = re.search(r'"description":"((?:[^"\\]|\\.){10,500})"', html)
    if desc_match:
        desc = desc_match.group(1)
        # Clean HTML tags from description
        desc = re.sub(r'<[^>]+>', ' ', desc)
        desc = re.sub(r'\\[rn]', ' ', desc)
        desc = re.sub(r'\s+', ' ', desc).strip()[:500]
        product["description"] = desc

    # SKU confirmation
    sku_match = re.search(r'"sku":"([^"]+)"', html)
    if sku_match:
        product["sku"] = sku_match.group(1)

    # Product ID (internal)
    pid_match = re.search(r'"productId":"([^"]+)"', html)
    if pid_match:
        product["product_id"] = pid_match.group(1)

    # Detect format type from page content
    html_lower = html.lower()
    if "beyond digital" in html_lower or "digital only" in html_lower:
        product["format"] = "digital_only"
    elif "book + digital" in html_lower or "bundle" in html_lower:
        product["format"] = "book_and_digital"
    elif "physical" in html_lower:
        product["format"] = "physical"
    else:
        product["format"] = "unknown"

    return product


def _discover_homepage_products(session):
    """
    Scrape the marketplace homepage for featured product SKUs.
    Returns list of SKU strings found in the homepage HTML.
    """
    try:
        resp = session.get(f"{MARKETPLACE_BASE}/", timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Extract product arrays from homepage
        matches = re.findall(r'"products":\["([^]]+)\]', html)
        skus = set()
        for match in matches:
            for sku in re.findall(r'([^",]+)', match):
                skus.add(sku.strip('"'))

        # Also find SKUs linked in the page
        for m in re.finditer(r'/product/([A-Za-z0-9]+)', html):
            skus.add(m.group(1))

        print(f"  Discovered {len(skus)} product SKUs from homepage")
        return list(skus)

    except Exception as e:
        print(f"  Homepage discovery error: {e}")
        return []


class DnDBeyondHarvester:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.today = datetime.date.today()

    def run(self):
        """Fetch product data for all known SKUs + newly discovered ones."""
        print("--- D&D Beyond Catalog Harvester ---")

        # Step 1: Discover any new products from homepage
        discovered = _discover_homepage_products(self.session)
        time.sleep(REQUEST_DELAY)

        # Combine known + discovered, deduplicate
        all_skus = list(set(KNOWN_PRODUCT_SKUS + discovered))
        print(f"  Total SKUs to check: {len(all_skus)}")

        # Step 2: Fetch each product page
        rows = []
        featured_skus = set(discovered)  # Products on homepage are "featured"

        for sku in all_skus:
            try:
                resp = self.session.get(
                    f"{MARKETPLACE_BASE}/product/{sku}",
                    timeout=30,
                )

                if resp.status_code == 404:
                    print(f"  {sku}: not found (404)")
                    continue

                if resp.status_code == 429:
                    print(f"  RATE LIMITED. Backing off 30s...")
                    time.sleep(30)
                    resp = self.session.get(
                        f"{MARKETPLACE_BASE}/product/{sku}",
                        timeout=30,
                    )

                resp.raise_for_status()
                product = _extract_product_data(resp.text, sku)

                if not product.get("title"):
                    print(f"  {sku}: could not extract title")
                    continue

                row = {
                    "sku": product.get("sku", sku),
                    "product_id": product.get("product_id"),
                    "title": product.get("title", ""),
                    "description": product.get("description", "")[:500],
                    "price": product.get("price"),
                    "bundle_price": product.get("bundle_price"),
                    "format": product.get("format", "unknown"),
                    "is_featured": sku in featured_skus,
                    "fetch_date": self.today.isoformat(),
                }
                rows.append(row)

                print(f"  {product.get('title', sku)}: ${product.get('price', '?')} "
                      f"({product.get('format', '?')})"
                      f"{' [FEATURED]' if sku in featured_skus else ''}")

            except Exception as e:
                print(f"  {sku}: error - {e}")

            time.sleep(REQUEST_DELAY)

        # Step 3: Insert to BigQuery
        if rows:
            errors = self.bq.insert_rows_json(CATALOG_TABLE, rows)
            if errors:
                print(f"BQ insert errors: {errors}")
            else:
                print(f"Inserted {len(rows)} products into dndbeyond_catalog.")

        # Summary
        digital_only = sum(1 for r in rows if r["format"] == "digital_only")
        bundles = sum(1 for r in rows if r["format"] == "book_and_digital")
        featured = sum(1 for r in rows if r["is_featured"])

        return {
            "status": "success",
            "products_tracked": len(rows),
            "digital_only": digital_only,
            "book_and_digital": bundles,
            "featured_count": featured,
            "skus_checked": len(all_skus),
        }


@functions_framework.http
def dndbeyond_harvester_http(request):
    """HTTP entry point for D&D Beyond Catalog Harvester."""
    from shabbat_gate import is_shabbat, shabbat_skip_response
    if is_shabbat():
        return shabbat_skip_response()

    print("Starting D&D Beyond Catalog Harvester...")
    try:
        harvester = DnDBeyondHarvester()
        result = harvester.run()
        print(f"Complete: {json.dumps(result)}")
        return json.dumps(result), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
