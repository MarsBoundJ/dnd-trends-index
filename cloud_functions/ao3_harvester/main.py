"""
ao3_harvester — Track D&D/BG3 narrative engagement on Archive of Our Own.

AO3 is the largest fanfiction platform. Tag work counts serve as a proxy
for cultural obsession — characters with 50,000 stories are ones WotC
should feature in supplements, comics, and campaigns.

Tracks:
  - BG3 companion characters (Astarion, Shadowheart, Gale, etc.)
  - Classic D&D characters (Drizzt, Strahd, Jarlaxle, Vecna)
  - Fandom tags (Baldur's Gate, D&D)
  - Top relationship tags (for crossover engagement signals)

Method: Scrapes AO3 tag pages — no official API, but HTML is clean.
AO3's ToS allows scraping at reasonable rates (5-second delay between requests).
"""

import os
import json
import re
import time
import datetime
import functions_framework
import requests
from google.cloud import bigquery

PROJECT = "dnd-trends-index"
RAW_DATASET = f"{PROJECT}.dnd_trends_raw"
AO3_TABLE = f"{RAW_DATASET}.ao3_tag_counts"

AO3_BASE = "https://archiveofourown.org"
REQUEST_DELAY = 5  # AO3 ToS: be polite, minimum 5s between requests

# User-Agent identifying our bot (AO3 community best practice)
HEADERS = {
    "User-Agent": "DnD-Trends-Index/1.0 (academic research; contact: dnd-trends@example.com)",
    "Accept": "text/html",
}

# ── Tags to track ──────────────────────────────────────────────────────
# Format: (tag_name, tag_type, ao3_tag_slug)
# AO3 tag pages: https://archiveofourown.org/tags/{slug}/works

TRACKED_TAGS = [
    # BG3 Companion Characters
    ("Astarion", "character", "Astarion (Baldur's Gate)"),
    ("Shadowheart", "character", "Shadowheart (Baldur's Gate)"),
    ("Gale Dekarios", "character", "Gale (Baldur's Gate)"),
    ("Karlach", "character", "Karlach (Baldur's Gate)"),
    ("Lae'zel", "character", "Lae'zel (Baldur's Gate)"),
    ("Wyll Ravengard", "character", "Wyll Ravengard"),
    ("Halsin", "character", "Halsin (Baldur's Gate)"),
    ("Minthara", "character", "Minthara (Baldur's Gate)"),
    ("The Dark Urge", "character", "Dark Urge (Baldur's Gate)"),
    ("Raphael", "character", "Raphael (Baldur's Gate)"),

    # Classic D&D Characters
    ("Drizzt Do'Urden", "character", "Drizzt Do'Urden"),
    ("Strahd von Zarovich", "character", "Strahd von Zarovich"),
    ("Jarlaxle Baenre", "character", "Jarlaxle Baenre"),
    ("Vecna", "character", "Vecna"),

    # Fandom Tags (aggregate interest)
    ("Baldur's Gate (Video Games)", "fandom", "Baldur's Gate (Video Games)"),
    ("Dungeons & Dragons (Roleplaying Game)", "fandom", "Dungeons *a* Dragons (Roleplaying Game)"),
    ("Forgotten Realms", "fandom", "Forgotten Realms"),
    ("Critical Role", "fandom", "Critical Role (Web Series)"),

    # Top Relationship Tags (crossover engagement signal)
    # AO3 relationship tags: first character without disambiguator, *s* separator
    ("Astarion/Tav", "relationship", "Astarion*s*Tav (Baldur's Gate)"),
    ("Shadowheart/Tav", "relationship", "Shadowheart*s*Tav (Baldur's Gate)"),
    ("Karlach/Tav", "relationship", "Karlach*s*Tav (Baldur's Gate)"),
    ("Gale/Tav", "relationship", "Gale*s*Tav (Baldur's Gate)"),
    ("Astarion/Gale", "relationship", "Astarion*s*Gale (Baldur's Gate)"),
]


def _encode_tag_for_url(slug):
    """
    Encode an AO3 tag slug for use in a URL path.
    AO3 uses *s* for / in relationship tags.
    Most special chars need URL encoding, but AO3 handles spaces as %20.
    """
    # Replace / with *s* (AO3 convention for relationship tags)
    encoded = slug.replace("/", "*s*")
    # URL-encode remaining special characters (but not *s*)
    # AO3 tag URLs use the tag name directly in the path
    encoded = requests.utils.quote(encoded, safe="*")
    return encoded


def _scrape_tag_work_count(session, tag_name, tag_slug):
    """
    Scrape the work count from an AO3 tag page.

    AO3 tag pages show "X Works" in the heading. We parse this from HTML
    rather than using BeautifulSoup to avoid the dependency — the pattern
    is simple and stable.

    Returns: work_count (int) or None on failure.
    """
    url = f"{AO3_BASE}/tags/{_encode_tag_for_url(tag_slug)}/works"

    try:
        resp = session.get(url, timeout=30)

        # AO3 returns 429 when rate limited
        if resp.status_code == 429:
            print(f"  RATE LIMITED on {tag_name}. Backing off 30s...")
            time.sleep(30)
            resp = session.get(url, timeout=30)

        if resp.status_code == 404:
            print(f"  Tag not found: {tag_name} (slug: {tag_slug})")
            return None

        resp.raise_for_status()
        html = resp.text

        # Pattern 1: "X Works in TAG" heading (tag landing page with works)
        # e.g., "2,847 Works in Astarion (Baldur's Gate)"
        match = re.search(r'([\d,]+)\s+Work', html)
        if match:
            count = int(match.group(1).replace(",", ""))
            return count

        # Pattern 2: Check if it's a tag with 0 works
        if "No works found" in html or "0 Works" in html:
            return 0

        print(f"  Could not parse work count for {tag_name}")
        return None

    except Exception as e:
        print(f"  Error scraping {tag_name}: {e}")
        return None


class AO3Harvester:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.today = datetime.date.today()

    def run(self):
        """Scrape work counts for all tracked tags."""
        print(f"--- AO3 Harvester: Scraping {len(TRACKED_TAGS)} tags ---")

        rows = []
        for tag_name, tag_type, tag_slug in TRACKED_TAGS:
            count = _scrape_tag_work_count(self.session, tag_name, tag_slug)

            if count is not None:
                rows.append({
                    "tag_name": tag_name,
                    "tag_type": tag_type,
                    "ao3_tag_slug": tag_slug,
                    "work_count": count,
                    "fetch_date": self.today.isoformat(),
                })
                print(f"  {tag_name} ({tag_type}): {count:,} works")
            else:
                print(f"  {tag_name}: FAILED to scrape")

            # Respect AO3's rate expectations
            time.sleep(REQUEST_DELAY)

        # Insert to BigQuery
        if rows:
            errors = self.bq.insert_rows_json(AO3_TABLE, rows)
            if errors:
                print(f"BQ insert errors: {errors}")
            else:
                print(f"Inserted {len(rows)} tag counts into ao3_tag_counts.")

        # Summary stats
        characters = [r for r in rows if r["tag_type"] == "character"]
        if characters:
            characters.sort(key=lambda r: r["work_count"], reverse=True)
            print("\n--- Top Characters by Work Count ---")
            for c in characters[:5]:
                print(f"  {c['tag_name']}: {c['work_count']:,}")

        fandoms = [r for r in rows if r["tag_type"] == "fandom"]
        if fandoms:
            fandoms.sort(key=lambda r: r["work_count"], reverse=True)
            print("\n--- Fandom Totals ---")
            for f in fandoms:
                print(f"  {f['tag_name']}: {f['work_count']:,}")

        return {
            "status": "success",
            "tags_scraped": len(rows),
            "tags_failed": len(TRACKED_TAGS) - len(rows),
            "top_character": characters[0]["tag_name"] if characters else None,
            "top_character_works": characters[0]["work_count"] if characters else 0,
        }


@functions_framework.http
def ao3_harvester_http(request):
    """HTTP entry point for AO3 Harvester."""
    from shabbat_gate import is_shabbat, shabbat_skip_response
    if is_shabbat():
        return shabbat_skip_response()

    print("Starting AO3 Harvester...")
    try:
        harvester = AO3Harvester()
        result = harvester.run()
        print(f"Complete: {json.dumps(result)}")
        return json.dumps(result), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
