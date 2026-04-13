"""
modio_harvester — Track BG3 mod ecosystem on mod.io.

BG3 uses mod.io as its official cross-platform mod platform (not Steam Workshop).
Collects: trending mods, download counts, subscriber counts, ratings, tags.
Over 100M mod downloads as of January 2025.
"""

import os
import json
import datetime
import functions_framework
import requests
from google.cloud import bigquery, secretmanager

PROJECT = "dnd-trends-index"
RAW_DATASET = f"{PROJECT}.dnd_trends_raw"
MODS_TABLE = f"{RAW_DATASET}.modio_mods"

# mod.io API — game-specific subdomain required since Jan 2025
# Format: https://g-{game_id}.modapi.io/v1  (old api.mod.io is deprecated)
MODIO_API_BASE = "https://g-6715.modapi.io/v1"
MODIO_SECRET_NAME = f"projects/{PROJECT}/secrets/modio-api-key/versions/latest"

# BG3 game ID on mod.io (Larian Studios / Baldur's Gate 3)
BG3_GAME_ID = 6715


class ModioHarvester:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT)
        self.api_key = self._load_api_key()
        self.session = requests.Session()
        self.today = datetime.date.today()

    def _load_api_key(self):
        """Load mod.io API key from environment or Secret Manager."""
        key = os.environ.get("MODIO_API_KEY", "")
        if key:
            return key
        try:
            sm = secretmanager.SecretManagerServiceClient()
            key = sm.access_secret_version(name=MODIO_SECRET_NAME).payload.data.decode("utf-8")
            print("Loaded mod.io API key from Secret Manager.")
            return key
        except Exception as e:
            print(f"Could not load mod.io API key: {e}")
            return ""

    def fetch_mods(self, sort="_downloads", limit=100):
        """
        Fetch mods from mod.io for BG3.

        Sort options:
          _downloads — most downloaded
          _popular — trending/popular
          _rating — highest rated
          date_added — newest
        """
        if not self.api_key:
            print("No mod.io API key available.")
            return []

        all_mods = []
        offset = 0

        while offset < limit:
            page_size = min(100, limit - offset)
            try:
                resp = self.session.get(
                    f"{MODIO_API_BASE}/games/{BG3_GAME_ID}/mods",
                    params={
                        "api_key": self.api_key,
                        "_sort": sort,
                        "_limit": page_size,
                        "_offset": offset,
                    },
                    timeout=20,
                )
                resp.raise_for_status()
                data = resp.json()
                mods = data.get("data", [])
                if not mods:
                    break

                for mod in mods:
                    tags = [t.get("name", "") for t in mod.get("tags", [])]
                    all_mods.append({
                        "mod_id": mod["id"],
                        "mod_name": mod.get("name", ""),
                        "game_id": BG3_GAME_ID,
                        "game_name": "Baldur's Gate 3",
                        "summary": (mod.get("summary", "") or "")[:500],
                        "downloads_total": mod.get("stats", {}).get("downloads_total", 0),
                        "subscribers_total": mod.get("stats", {}).get("subscribers_total", 0),
                        "popularity_rank": mod.get("stats", {}).get("popularity", {}).get("rank_position", 0),
                        "rating_total": mod.get("stats", {}).get("ratings_total", 0),
                        "rating_positive": mod.get("stats", {}).get("ratings_positive", 0),
                        "rating_negative": mod.get("stats", {}).get("ratings_negative", 0),
                        "date_added": datetime.datetime.utcfromtimestamp(
                            mod.get("date_added", 0)
                        ).isoformat() if mod.get("date_added") else None,
                        "date_updated": datetime.datetime.utcfromtimestamp(
                            mod.get("date_updated", 0)
                        ).isoformat() if mod.get("date_updated") else None,
                        "tags": tags,
                        "fetch_date": self.today.isoformat(),
                    })

                offset += len(mods)
                if len(mods) < page_size:
                    break

            except Exception as e:
                print(f"mod.io fetch error at offset {offset}: {e}")
                break

        print(f"Fetched {len(all_mods)} mods from mod.io (sort={sort})")
        return all_mods

    def run(self, top_n=100):
        """Fetch top mods by downloads and insert into BQ."""
        mods = self.fetch_mods(sort="_downloads", limit=top_n)

        if not mods:
            return {"status": "success", "message": "No mods fetched.", "mods": 0}

        # Log top 10 for visibility
        print("--- Top 10 BG3 Mods by Downloads ---")
        for m in mods[:10]:
            print(f"  {m['mod_name']}: {m['downloads_total']:,} downloads, "
                  f"{m['subscribers_total']:,} subs")

        errors = self.bq.insert_rows_json(MODS_TABLE, mods)
        if errors:
            print(f"BQ insert errors: {errors}")
        else:
            print(f"Inserted {len(mods)} mods into modio_mods.")

        return {
            "status": "success",
            "mods_tracked": len(mods),
            "top_mod": mods[0]["mod_name"] if mods else None,
            "top_downloads": mods[0]["downloads_total"] if mods else 0,
        }


@functions_framework.http
def modio_harvester_http(request):
    """HTTP entry point for mod.io Harvester."""
    print("Starting mod.io Harvester...")
    try:
        body = {}
        if request.content_type == "application/json":
            body = request.get_json(silent=True) or {}

        top_n = body.get("top_n", 100)

        harvester = ModioHarvester()
        result = harvester.run(top_n=top_n)
        print(f"Complete: {json.dumps(result)}")
        return json.dumps(result), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
