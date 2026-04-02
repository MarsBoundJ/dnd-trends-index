import json
import asyncio
from datetime import date, datetime, timezone, timedelta
from collections import defaultdict
from google.cloud import bigquery
from playwright.async_api import async_playwright

# --- CONFIGURATION ---
BQ_CLIENT = bigquery.Client()
METRICS_TABLE = "dnd-trends-index.dnd_trends_raw.fandom_daily_metrics"

WIKI_REGISTRY = {
    "dnd5e": "D&D 5e (Core Mechanics)",
    "forgottenrealms": "Forgotten Realms (Primary Lore)",
    "criticalrole": "Critical Role (Influencer)",
    "dungeonsdragons": "D&D General Lore (Aggregator)",
    "5point5": "D&D 2024 (New Rules)",
    "eberron": "Eberron (Steampunk/Pulp)",
    "ravenloft": "Ravenloft (Horror)",
    "dragonlance": "Dragonlance (High Fantasy)",
    "spelljammer": "Spelljammer (Sci-Fi/Astral)",
    "planescape": "Planescape (Multiverse)",
    "greyhawk": "Greyhawk (Classic/Gygax)",
    "darksun": "Dark Sun (Survival)",
    "mystara": "Mystara (OSR/Retro)"
}

EXCLUDED_TITLE_PREFIXES = ["User:", "File:", "Talk:", "Category:", "Template:", "Blog:", "Forum:"]
EXCLUDED_TITLES = [
    "Main Page", "Wiki_Activity", "Special:Search",
    "Special:Random", "Home", "Dungeons_&_Dragons_Wiki",
    "Community_Corner", "D&D_Wiki"
]


async def fetch_view_counts(page, wiki_slug):
    """Use Playwright to fetch top articles with view counts (bypasses Cloudflare JS challenge)."""
    print(f"📡 Fetching view counts for {wiki_slug}...")
    url = f"https://{wiki_slug}.fandom.com/api/v1/Articles/Top?expand=1&limit=100"

    try:
        # Navigate and wait for Cloudflare challenge to resolve
        response = await page.goto(url, wait_until="networkidle", timeout=30000)
        content = await page.content()

        # If Cloudflare challenge is still active, page content will be HTML not JSON
        # Wait a moment and try to get the JSON directly via evaluate
        try:
            data = await page.evaluate("() => JSON.parse(document.body.innerText)")
        except Exception:
            # Try fetching via page.request once cookies are set
            api_response = await page.request.get(url)
            data = await api_response.json()

    except Exception as e:
        print(f"❌ Error fetching {wiki_slug}: {e}")
        return {}

    view_map = {}
    for item in data.get("items", []):
        title = item.get("title", "").strip()
        if any(title.startswith(p) for p in EXCLUDED_TITLE_PREFIXES):
            continue
        if title in EXCLUDED_TITLES:
            continue
        view_map[title] = item.get("views", 0)

    print(f"✅ {wiki_slug}: {len(view_map)} articles with view counts")
    return view_map


def update_view_counts(wiki_slug, view_map):
    """UPSERT view_count into today's rows.

    If a row already exists (from the edit-count scraper), updates view_count.
    If no row exists yet, inserts a new row so view counts are captured
    regardless of whether the edit scraper ran first.
    """
    if not view_map:
        return

    today = date.today().isoformat()
    selects = []
    for rank, (title, views) in enumerate(view_map.items(), start=1):
        safe_title = title.replace("'", "\\'")
        url_path = f"/wiki/{title.replace(' ', '_')}"
        hype = round(1.01 - (rank * 0.01), 2)
        selects.append(
            f"SELECT '{wiki_slug}' as slug, '{safe_title}' as title, "
            f"{views} as vc, {rank} as rk, {hype} as hs, '{url_path}' as up"
        )

    using_sql = " UNION ALL ".join(selects)

    query = f"""
    MERGE `{METRICS_TABLE}` t
    USING ({using_sql}) s
    ON t.wiki_slug = s.slug
       AND t.article_title = s.title
       AND t.extraction_date = '{today}'
    WHEN MATCHED THEN
      UPDATE SET view_count = s.vc
    WHEN NOT MATCHED THEN
      INSERT (extraction_date, wiki_slug, article_title, rank_position,
              hype_score, view_count, edit_count, url_path)
      VALUES ('{today}', s.slug, s.title, s.rk, s.hs, s.vc, NULL, s.up)
    """

    try:
        job = BQ_CLIENT.query(query)
        job.result()
        print(f"✅ {wiki_slug}: upserted {len(view_map)} view counts in BQ")
    except Exception as e:
        print(f"❌ BQ upsert failed for {wiki_slug}: {e}")


async def run_fetcher():
    results = {}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for slug in WIKI_REGISTRY:
            view_map = await fetch_view_counts(page, slug)
            update_view_counts(slug, view_map)
            results[slug] = len(view_map)

        await browser.close()

    return results


def fandom_view_fetcher_http(request):
    """Cloud Run HTTP entry point."""
    print("🚀 Starting Fandom View Count Fetcher (Playwright)...")
    results = asyncio.run(run_fetcher())
    total = sum(results.values())
    msg = f"✅ Done. Updated view counts for {total} articles across {len(results)} wikis."
    print(msg)
    return json.dumps({"status": "success", "message": msg, "per_wiki": results}), 200


if __name__ == "__main__":
    import os
    from flask import Flask, request as flask_request
    app = Flask(__name__)

    @app.route("/", methods=["POST", "GET"])
    def index():
        return fandom_view_fetcher_http(flask_request)

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
