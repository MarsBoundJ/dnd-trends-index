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

# Step 9.9 Chunk C — Universes Beyond candidate wikis. Mirrored from
# scripts/seed_ub_candidate_ips.py::FANDOM_WIKI_SLUGS (canonical
# source). Keep these two lists in sync; the seed list is the source
# of truth because the gold view JOINs on fandom_wiki_slug. When
# adding an IP to the seed list with a fandom_wiki_slug, add the
# corresponding entry here too.
#
# Values are free-form labels for logs. Keys must exactly match the
# slug used by the Fandom subdomain (e.g. "dungeon-meshi" not
# "delicious-in-dungeon"; "tardis" not "doctorwho" — Fandom's slugs
# are owner-chosen, not algorithmic).
UB_CANDIDATE_WIKIS = {
    "cyberpunk": "Cyberpunk 2077 (UB candidate)",
    "eldenring": "Elden Ring (UB candidate)",
    "witcher": "The Witcher (UB candidate)",
    "warhammer40k": "Warhammer 40K (UB candidate)",
    "dune": "Dune (UB candidate)",
    "severance": "Severance (UB candidate)",
    "arcane": "Arcane / League of Legends (UB candidate)",
    "strangerthings": "Stranger Things (UB candidate)",
    "gameofthrones": "GoT / House of the Dragon (UB candidate)",
    "lotr": "Lord of the Rings (UB candidate)",
    "starwars": "Star Wars (UB candidate — covers Andor + Mandalorian)",
    "tardis": "Doctor Who (UB candidate — slug is 'tardis')",
    "cowboybebop": "Cowboy Bebop (UB candidate)",
    # dungeon-meshi omitted — returned 0 top-articles on the Apr 20 smoke
    # test. Keep the IP scored (Reddit/YouTube carry the trajectory) but
    # don't scrape a wiki that yields empty data. See scripts/seed_ub_candidate_ips.py.
    "bluelock": "Blue Lock (UB candidate)",
    "frieren": "Frieren (UB candidate)",
    "jujutsu-kaisen": "Jujutsu Kaisen (UB candidate)",
    "attackontitan": "Attack on Titan (UB candidate)",
    "onepiece": "One Piece (UB candidate)",
    "chainsawman": "Chainsaw Man (UB candidate)",
    "stormlightarchive": "Stormlight Archive (UB candidate)",
    "kpop-demon-hunters": "Kpop Demon Hunters (UB candidate)",
    "godzilla": "Godzilla (UB candidate)",
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

    Pre-9.9 this built the MERGE via Python string concatenation of the
    article titles — which silently failed on ~15 of the 36 wikis because
    certain title characters (straight + curly apostrophes, quotes, etc.)
    broke the quoted SQL literals with "concatenated string literals must
    be separated by whitespace" errors. Elden Ring, Cyberpunk, LotR, Dune,
    Star Wars, etc. all failed historically. Apr 20 fix: parameterize the
    MERGE via ArrayQueryParameter so BQ handles all escaping.
    """
    if not view_map:
        return

    today = date.today().isoformat()

    # Parallel arrays — simpler than ArrayQueryParameter of STRUCT
    # (which the Python SDK requires as StructQueryParameter-wrapped
    # values, awkward for this row shape). MERGE uses UNNEST over
    # matching-index arrays: assumes @titles[i], @views[i], @ranks[i],
    # @hypes[i], @urls[i] describe one article.
    titles = []
    views_arr = []
    ranks = []
    hypes = []
    urls = []
    for rank, (title, views) in enumerate(view_map.items(), start=1):
        titles.append(title)
        views_arr.append(int(views or 0))
        ranks.append(rank)
        hypes.append(round(1.01 - (rank * 0.01), 2))
        urls.append(f"/wiki/{title.replace(' ', '_')}")

    query = f"""
    MERGE `{METRICS_TABLE}` t
    USING (
      SELECT
        @titles[OFFSET(i)] AS title,
        @views[OFFSET(i)]  AS views,
        @ranks[OFFSET(i)]  AS rank,
        @hypes[OFFSET(i)]  AS hype,
        @urls[OFFSET(i)]   AS url
      FROM UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(@titles) - 1)) AS i
    ) s
    ON t.wiki_slug = @slug
       AND t.article_title = s.title
       AND t.extraction_date = @date
    WHEN MATCHED THEN
      UPDATE SET view_count = s.views
    WHEN NOT MATCHED THEN
      INSERT (extraction_date, wiki_slug, article_title, rank_position,
              hype_score, view_count, edit_count, url_path)
      VALUES (@date, @slug, s.title, s.rank, s.hype, s.views, NULL, s.url)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("titles", "STRING", titles),
            bigquery.ArrayQueryParameter("views", "INT64", views_arr),
            bigquery.ArrayQueryParameter("ranks", "INT64", ranks),
            bigquery.ArrayQueryParameter("hypes", "FLOAT64", hypes),
            bigquery.ArrayQueryParameter("urls", "STRING", urls),
            bigquery.ScalarQueryParameter("slug", "STRING", wiki_slug),
            bigquery.ScalarQueryParameter("date", "DATE", today),
        ]
    )

    try:
        job = BQ_CLIENT.query(query, job_config=job_config)
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

        # D&D-registry wikis + UB-candidate wikis (Step 9.9 Chunk C).
        # Merged at runtime so the UB list lives next to its label
        # source (seed_ub_candidate_ips.py) rather than inflating
        # WIKI_REGISTRY with non-D&D entries. Any slug that 404s
        # returns an empty view_map and is logged — low-risk extension.
        all_slugs = list(WIKI_REGISTRY.keys()) + list(UB_CANDIDATE_WIKIS.keys())
        for slug in all_slugs:
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
