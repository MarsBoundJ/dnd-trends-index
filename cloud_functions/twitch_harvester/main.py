"""
twitch_harvester — Track D&D streaming viewership on Twitch.

Collects: viewer counts, stream counts, and stream tags for D&D game categories.
Unique signal: VTT market share from stream tags (Roll20, Foundry, D&D Beyond).

Categories tracked:
  - Baldur's Gate 3
  - Dungeons & Dragons
  - Solasta: Crown of the Magister
  - Idle Champions of the Forgotten Realms
"""

import os
import json
import datetime
import functions_framework
import requests
from google.cloud import bigquery, secretmanager
from collections import Counter

PROJECT = "dnd-trends-index"
RAW_DATASET = f"{PROJECT}.dnd_trends_raw"
VIEWERSHIP_TABLE = f"{RAW_DATASET}.twitch_viewership"

TWITCH_CLIENT_ID_SECRET = f"projects/{PROJECT}/secrets/twitch-client-id/versions/latest"
TWITCH_CLIENT_SECRET_SECRET = f"projects/{PROJECT}/secrets/twitch-client-secret/versions/latest"

HELIX_BASE = "https://api.twitch.tv/helix"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# D&D-relevant Twitch categories to track
# We look these up by name to get their game_ids dynamically
DND_CATEGORIES = [
    "Baldur's Gate 3",
    "Dungeons & Dragons",
    "Solasta: Crown of the Magister",
    "Idle Champions of the Forgotten Realms",
    "Neverwinter",
]


class TwitchHarvester:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT)
        self.client_id, self.client_secret = self._load_credentials()
        self.access_token = self._get_app_token()
        self.session = requests.Session()
        self.session.headers.update({
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.access_token}",
        })
        self.now = datetime.datetime.utcnow()
        self.today = datetime.date.today()

    def _load_credentials(self):
        """Load Twitch client_id and client_secret from env or Secret Manager."""
        cid = os.environ.get("TWITCH_CLIENT_ID", "")
        csec = os.environ.get("TWITCH_CLIENT_SECRET", "")
        if cid and csec:
            return cid, csec
        try:
            sm = secretmanager.SecretManagerServiceClient()
            cid = sm.access_secret_version(name=TWITCH_CLIENT_ID_SECRET).payload.data.decode("utf-8")
            csec = sm.access_secret_version(name=TWITCH_CLIENT_SECRET_SECRET).payload.data.decode("utf-8")
            print("Loaded Twitch credentials from Secret Manager.")
            return cid, csec
        except Exception as e:
            print(f"Could not load Twitch credentials: {e}")
            return "", ""

    def _get_app_token(self):
        """Get an OAuth2 app access token (client credentials flow)."""
        if not self.client_id or not self.client_secret:
            print("No Twitch credentials available.")
            return ""
        try:
            resp = requests.post(TOKEN_URL, params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            }, timeout=10)
            resp.raise_for_status()
            token = resp.json().get("access_token", "")
            print("Obtained Twitch app access token.")
            return token
        except Exception as e:
            print(f"Twitch token error: {e}")
            return ""

    def _lookup_game_ids(self):
        """Look up Twitch game IDs for our category names."""
        game_map = {}
        # Twitch allows up to 100 names per request
        params = [("name", cat) for cat in DND_CATEGORIES]
        try:
            resp = self.session.get(f"{HELIX_BASE}/games", params=params, timeout=10)
            resp.raise_for_status()
            for game in resp.json().get("data", []):
                game_map[game["id"]] = game["name"]
                print(f"  Category: {game['name']} (id={game['id']})")
        except Exception as e:
            print(f"Game lookup error: {e}")
        return game_map

    def _get_streams_for_category(self, game_id, category_name):
        """
        Paginate through all live streams for a game category.
        Returns: total viewers, stream count, and top tag counts.
        """
        total_viewers = 0
        stream_count = 0
        tag_counter = Counter()
        cursor = None

        while True:
            params = {"game_id": game_id, "first": 100}
            if cursor:
                params["after"] = cursor

            try:
                resp = self.session.get(f"{HELIX_BASE}/streams", params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                streams = data.get("data", [])

                for stream in streams:
                    total_viewers += stream.get("viewer_count", 0)
                    stream_count += 1
                    for tag in stream.get("tags", []):
                        tag_counter[tag] += 1

                # Check for more pages
                pagination = data.get("pagination", {})
                cursor = pagination.get("cursor")
                if not cursor or not streams:
                    break

            except Exception as e:
                print(f"  Stream fetch error for {category_name}: {e}")
                break

        return total_viewers, stream_count, tag_counter

    def run(self):
        """Fetch viewership data for all D&D categories."""
        if not self.access_token:
            return {"status": "error", "message": "No Twitch access token."}

        print("--- Looking up Twitch categories ---")
        game_map = self._lookup_game_ids()
        if not game_map:
            return {"status": "error", "message": "No categories found."}

        print("--- Fetching live streams ---")
        rows = []
        total_all_viewers = 0

        for game_id, category_name in game_map.items():
            viewers, streams, tags = self._get_streams_for_category(game_id, category_name)
            total_all_viewers += viewers

            # Get top 20 tags for this category
            top_tags = [tag for tag, _ in tags.most_common(20)]

            rows.append({
                "category_name": category_name,
                "game_id": game_id,
                "total_streams": streams,
                "total_viewers": viewers,
                "top_tags": top_tags,
                "snapshot_date": self.today.isoformat(),
                "snapshot_ts": self.now.isoformat(),
            })

            print(f"  {category_name}: {viewers:,} viewers across {streams} streams")
            if top_tags[:5]:
                print(f"    Top tags: {', '.join(top_tags[:5])}")

        # Insert to BQ
        if rows:
            errors = self.bq.insert_rows_json(VIEWERSHIP_TABLE, rows)
            if errors:
                print(f"BQ insert errors: {errors}")
            else:
                print(f"Inserted {len(rows)} category snapshots into twitch_viewership.")

        return {
            "status": "success",
            "categories_tracked": len(rows),
            "total_viewers_all_categories": total_all_viewers,
        }


@functions_framework.http
def twitch_harvester_http(request):
    """HTTP entry point for Twitch Harvester."""
    from shabbat_gate import is_shabbat, shabbat_skip_response
    if is_shabbat():
        return shabbat_skip_response()

    print("Starting Twitch Harvester...")
    try:
        harvester = TwitchHarvester()
        result = harvester.run()
        print(f"Complete: {json.dumps(result)}")
        return json.dumps(result), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
