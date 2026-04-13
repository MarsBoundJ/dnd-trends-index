"""
nexus_harvester — Track BG3 community mods on Nexus Mods.

Nexus Mods is the largest PC modding community. BG3 has a massive presence.
Collects: trending mods, new mods, download counts, endorsements, categories.

Complements the official mod.io data — Nexus has the deeper community history
and higher download volumes.
"""

import os
import json
import datetime
import time
import functions_framework
import requests
from google.cloud import bigquery, secretmanager

PROJECT = "dnd-trends-index"
RAW_DATASET = f"{PROJECT}.dnd_trends_raw"
MODS_TABLE = f"{RAW_DATASET}.nexus_mods"
NEXUS_SECRET_NAME = f"projects/{PROJECT}/secrets/nexus-api-key/versions/latest"

NEXUS_API_BASE = "https://api.nexusmods.com/v1"
GAME_DOMAIN = "baldursgate3"


class NexusHarvester:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT)
        self.api_key = self._load_api_key()
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "apikey": self.api_key,
        })
        self.today = datetime.date.today()

    def _load_api_key(self):
        """Load Nexus Mods API key from environment or Secret Manager."""
        key = os.environ.get("NEXUS_API_KEY", "")
        if key:
            return key
        try:
            sm = secretmanager.SecretManagerServiceClient()
            key = sm.access_secret_version(name=NEXUS_SECRET_NAME).payload.data.decode("utf-8")
            print("Loaded Nexus Mods API key from Secret Manager.")
            return key
        except Exception as e:
            print(f"Could not load Nexus API key: {e}")
            return ""

    def _fetch_mod_list(self, endpoint, label):
        """Fetch a mod list endpoint (trending, latest_added, latest_updated)."""
        try:
            resp = self.session.get(
                f"{NEXUS_API_BASE}/games/{GAME_DOMAIN}/mods/{endpoint}.json",
                timeout=15,
            )
            resp.raise_for_status()
            mods = resp.json()
            print(f"  {label}: {len(mods)} mods")
            return mods
        except Exception as e:
            print(f"  {label} error: {e}")
            return []

    def _fetch_mod_details(self, mod_id):
        """Fetch detailed stats for a single mod."""
        try:
            resp = self.session.get(
                f"{NEXUS_API_BASE}/games/{GAME_DOMAIN}/mods/{mod_id}.json",
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"    Mod {mod_id} detail error: {e}")
            return None

    def fetch_all(self, enrich_top_n=25):
        """
        Fetch trending + latest_added + latest_updated mod lists.
        Enrich the top N trending mods with full detail (download counts, endorsements).
        """
        if not self.api_key:
            print("No Nexus API key available.")
            return []

        # Fetch the three list endpoints
        trending = self._fetch_mod_list("trending", "Trending")
        latest = self._fetch_mod_list("latest_added", "Latest Added")
        updated = self._fetch_mod_list("latest_updated", "Latest Updated")

        # Combine and deduplicate by mod_id
        seen_ids = set()
        all_mods = []
        for source_label, mod_list in [("trending", trending), ("latest_added", latest), ("latest_updated", updated)]:
            for mod in mod_list:
                mid = mod.get("mod_id")
                if mid in seen_ids:
                    continue
                seen_ids.add(mid)
                all_mods.append((source_label, mod))

        print(f"  Total unique mods: {len(all_mods)}")

        # Enrich trending mods with full details (download counts, etc.)
        # The list endpoints don't include download/endorsement counts
        enriched_ids = set()
        rows = []
        for source_label, mod in all_mods:
            mid = mod.get("mod_id")
            detail = None

            # Enrich top trending mods with full stats
            if source_label == "trending" and len(enriched_ids) < enrich_top_n:
                detail = self._fetch_mod_details(mid)
                enriched_ids.add(mid)
                time.sleep(0.2)  # Stay well within rate limits

            row = self._mod_to_row(mod, source_label, detail)
            rows.append(row)

        return rows

    def _mod_to_row(self, mod, source, detail=None):
        """Convert a mod API response to a BQ row."""
        d = detail or mod  # Use detail if we enriched, otherwise basic info
        return {
            "mod_id": mod.get("mod_id"),
            "mod_name": mod.get("name", ""),
            "author": mod.get("author", ""),
            "category_name": mod.get("category_id", ""),  # Numeric ID from list
            "summary": (mod.get("summary", "") or "")[:500],
            "source_list": source,
            "downloads_total": d.get("mod_downloads", 0) if detail else None,
            "unique_downloads": d.get("mod_unique_downloads", 0) if detail else None,
            "endorsement_count": d.get("endorsement_count", 0) if detail else None,
            "created_time": mod.get("created_time"),
            "updated_time": mod.get("updated_time"),
            "fetch_date": self.today.isoformat(),
        }

    def run(self, enrich_top_n=25):
        """Fetch mods and insert into BQ."""
        print("--- Fetching Nexus Mods (BG3) ---")
        rows = self.fetch_all(enrich_top_n=enrich_top_n)

        if not rows:
            return {"status": "success", "message": "No mods fetched.", "mods": 0}

        # Log top trending with download counts
        enriched = [r for r in rows if r["downloads_total"] is not None]
        enriched.sort(key=lambda r: r["downloads_total"] or 0, reverse=True)
        print("--- Top Trending by Downloads ---")
        for m in enriched[:10]:
            print(f"  {m['mod_name']}: {m['downloads_total']:,} downloads, "
                  f"{m['endorsement_count']:,} endorsements")

        errors = self.bq.insert_rows_json(MODS_TABLE, rows)
        if errors:
            print(f"BQ insert errors: {errors}")
        else:
            print(f"Inserted {len(rows)} mods into nexus_mods.")

        return {
            "status": "success",
            "mods_tracked": len(rows),
            "enriched_with_stats": len(enriched),
            "top_mod": enriched[0]["mod_name"] if enriched else None,
            "top_downloads": enriched[0]["downloads_total"] if enriched else 0,
        }


@functions_framework.http
def nexus_harvester_http(request):
    """HTTP entry point for Nexus Mods Harvester."""
    from shabbat_gate import is_shabbat, shabbat_skip_response
    if is_shabbat():
        return shabbat_skip_response()

    print("Starting Nexus Mods Harvester...")
    try:
        body = {}
        if request.content_type == "application/json":
            body = request.get_json(silent=True) or {}

        enrich = body.get("enrich_top_n", 25)

        harvester = NexusHarvester()
        result = harvester.run(enrich_top_n=enrich)
        print(f"Complete: {json.dumps(result)}")
        return json.dumps(result), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
