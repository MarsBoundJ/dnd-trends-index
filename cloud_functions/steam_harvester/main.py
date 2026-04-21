"""
steam_harvester — Track D&D video game metrics from Steam.

Collects three data streams per game:
  1. Concurrent player counts (every run)
  2. Review summary stats (daily)
  3. Achievement unlock percentages (weekly)

Games tracked: BG3, Neverwinter, Solasta, Idle Champions, Dark Alliance.
"""

import os
import json
import datetime
import functions_framework
import requests
from google.cloud import bigquery, secretmanager

PROJECT = "dnd-trends-index"
RAW_DATASET = f"{PROJECT}.dnd_trends_raw"
PLAYER_TABLE = f"{RAW_DATASET}.steam_player_counts"
REVIEW_TABLE = f"{RAW_DATASET}.steam_reviews"
ACHIEVEMENT_TABLE = f"{RAW_DATASET}.steam_achievements"

STEAM_SECRET_NAME = f"projects/{PROJECT}/secrets/steam-api-key/versions/latest"


def _load_steam_api_key():
    """Load Steam API key from environment or Secret Manager."""
    key = os.environ.get("STEAM_API_KEY", "")
    if key:
        return key
    try:
        sm = secretmanager.SecretManagerServiceClient()
        key = sm.access_secret_version(name=STEAM_SECRET_NAME).payload.data.decode("utf-8")
        print("Loaded Steam API key from Secret Manager.")
        return key
    except Exception as e:
        print(f"Could not load Steam API key: {e}")
        return ""


STEAM_API_KEY = _load_steam_api_key()

# D&D games to track: {app_id: display_name}
DND_GAMES = {
    1086940: "Baldur's Gate 3",
    109600: "Neverwinter",
    1096530: "Solasta: Crown of the Magister",
    627690: "Idle Champions of the Forgotten Realms",
    623280: "Dark Alliance",
}

# Step 9.9 Chunk D.1 — Universes Beyond candidate games. Mirrored from
# scripts/seed_ub_candidate_ips.py::STEAM_APP_IDS (canonical source).
# Kept here as a mirror (not imported) because Cloud Run deploys only
# this directory — cross-directory imports would require restructuring.
# When STEAM_APP_IDS changes in the seed list, mirror the change here
# and redeploy. UB games participate in player_counts ONLY (trajectory
# signal is the gold view's Steam input); reviews + achievements stay
# DND_GAMES-scoped to keep those loops fast and focused.
UB_GAMES = {
    # Winners — fast-moving / high-player-count IPs
    1245620: "Elden Ring",
    1091500: "Cyberpunk 2077",
    553850: "Helldivers 2",
    377160: "Fallout 4",                   # franchise flagship
    2515020: "Final Fantasy XVI",
    1687950: "Persona 5 Royal",
    292030: "The Witcher 3",
    1145360: "Hades",
    2246340: "Monster Hunter Wilds",
    # Soulslike / FromSoft
    374320: "Dark Souls III",               # franchise flagship
    814380: "Sekiro: Shadows Die Twice",
    1627720: "Lies of P",
    1325200: "Nioh 2",
    # Action-RPG / live-service
    2694490: "Path of Exile 2",
    1085660: "Destiny 2",
    230410: "Warframe",
    # Indie narrative / cult
    632470: "Disco Elysium",
    1205520: "Pentiment",
    1608700: "Citizen Sleeper",
    1221250: "Norco",
    1262350: "Signalis",
    312520: "Rain World",
    333640: "Caves of Qud",
    1092790: "Inscryption",
    # Strategy / tactics
    268500: "XCOM 2",
    1142710: "Total War: Warhammer 3",
    2186680: "Warhammer 40,000: Rogue Trader",
    1158310: "Crusader Kings 3",
    1850510: "Triangle Strategy",   # corrected Apr 20 after 2170800 failed Steam API lookup
    # Survival / sandbox
    892970: "Valheim",
    548430: "Deep Rock Galactic",
    1172620: "Sea of Thieves",
    294100: "Rimworld",
    975370: "Dwarf Fortress",
    # CRPG
    1184370: "Pathfinder: Wrath of the Righteous",
    560130: "Pillars of Eternity II: Deadfire",
    362960: "Tyranny",
    435150: "Divinity: Original Sin 2",
    # Platformer
    367520: "Hollow Knight",
    # FPS / extraction
    594650: "Hunt: Showdown",
    # Co-op / cult
    1966720: "Lethal Company",
    2379780: "Balatro",
    # Narrative indie
    1155970: "Roadwarden",
    391540: "Undertale",
}
# BG3 (app_id 1086940) intentionally omitted from UB_GAMES — it's
# already tracked in DND_GAMES. The seed list counts it as a UB IP
# (steam_app_id=1086940 in STEAM_APP_IDS) but harvesting through
# DND_GAMES is sufficient; adding it to both dicts would duplicate
# API calls without changing the data we write.
PLAYER_COUNTS_TRACKED = {**UB_GAMES, **DND_GAMES}

STEAM_API_BASE = "https://api.steampowered.com"
STORE_API_BASE = "https://store.steampowered.com"


class SteamHarvester:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT)
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.today = datetime.date.today()
        self.now = datetime.datetime.utcnow()

    # ------------------------------------------------------------------
    # 1. Concurrent Player Counts
    # ------------------------------------------------------------------
    def fetch_player_counts(self):
        """Fetch current concurrent players for every tracked game
        (D&D roster + UB candidate roster). Reviews + achievements
        loops stay DND_GAMES-only — player_count is the trajectory
        signal the UB Matrix gold view joins on."""
        rows = []
        for app_id, name in PLAYER_COUNTS_TRACKED.items():
            try:
                url = f"{STEAM_API_BASE}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
                resp = self.session.get(url, params={"appid": app_id}, timeout=10)
                resp.raise_for_status()
                data = resp.json().get("response", {})
                count = data.get("player_count", 0)
                rows.append({
                    "app_id": app_id,
                    "app_name": name,
                    "concurrent_players": count,
                    "snapshot_date": self.today.isoformat(),
                    "snapshot_ts": self.now.isoformat(),
                })
                print(f"  {name}: {count:,} concurrent players")
            except Exception as e:
                print(f"  {name}: player count error — {e}")
        return rows

    # ------------------------------------------------------------------
    # 2. Review Summary
    # ------------------------------------------------------------------
    def fetch_reviews(self):
        """Fetch aggregate review stats for each game."""
        rows = []
        for app_id, name in DND_GAMES.items():
            try:
                url = f"{STORE_API_BASE}/appreviews/{app_id}"
                resp = self.session.get(url, params={
                    "json": 1,
                    "language": "all",
                    "purchase_type": "all",
                    "num_per_page": 0,  # We only need query_summary
                }, timeout=15)
                resp.raise_for_status()
                summary = resp.json().get("query_summary", {})
                total_pos = summary.get("total_positive", 0)
                total_neg = summary.get("total_negative", 0)
                total = total_pos + total_neg
                pct = round(total_pos / total * 100, 1) if total > 0 else 0.0
                rows.append({
                    "app_id": app_id,
                    "app_name": name,
                    "total_positive": total_pos,
                    "total_negative": total_neg,
                    "total_reviews": total,
                    "review_score_pct": pct,
                    "review_score_desc": summary.get("review_score_desc", ""),
                    "recent_positive": summary.get("recent_positive", 0),
                    "recent_negative": summary.get("recent_negative", 0),
                    "fetch_date": self.today.isoformat(),
                })
                print(f"  {name}: {total_pos:,}+ / {total_neg:,}- ({pct}% positive)")
            except Exception as e:
                print(f"  {name}: review fetch error — {e}")
        return rows

    # ------------------------------------------------------------------
    # 3. Achievement Unlock Percentages
    # ------------------------------------------------------------------
    def fetch_achievements(self):
        """Fetch global achievement unlock percentages."""
        if not STEAM_API_KEY:
            print("  No STEAM_API_KEY set, skipping achievements.")
            return []

        rows = []
        for app_id, name in DND_GAMES.items():
            try:
                # Get achievement percentages
                url = f"{STEAM_API_BASE}/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/"
                resp = self.session.get(url, params={"gameid": app_id}, timeout=10)
                resp.raise_for_status()
                achievements = resp.json().get("achievementpercentages", {}).get("achievements", [])

                # Get achievement display names (schema)
                schema_url = f"{STEAM_API_BASE}/ISteamUserStats/GetSchemaForGame/v2/"
                schema_resp = self.session.get(schema_url, params={
                    "key": STEAM_API_KEY, "appid": app_id
                }, timeout=10)
                display_names = {}
                if schema_resp.ok:
                    stats = schema_resp.json().get("game", {}).get("availableGameStats", {})
                    for a in stats.get("achievements", []):
                        display_names[a["name"]] = a.get("displayName", a["name"])

                for ach in achievements:
                    api_name = ach.get("name", "")
                    rows.append({
                        "app_id": app_id,
                        "app_name": name,
                        "achievement_api_name": api_name,
                        "achievement_display_name": display_names.get(api_name, api_name),
                        "unlock_pct": round(float(ach.get("percent", 0)), 2),
                        "fetch_date": self.today.isoformat(),
                    })

                print(f"  {name}: {len(achievements)} achievements tracked")
            except Exception as e:
                print(f"  {name}: achievement fetch error — {e}")
        return rows

    # ------------------------------------------------------------------
    # Insert helpers
    # ------------------------------------------------------------------
    def _insert(self, table, rows, label):
        if not rows:
            return
        errors = self.bq.insert_rows_json(table, rows)
        if errors:
            print(f"BQ insert errors ({label}): {errors}")
        else:
            print(f"Inserted {len(rows)} rows into {label}.")

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------
    def run(self, include_achievements=False):
        """Run the harvester. Achievements are weekly — controlled by caller."""
        print("--- Player Counts ---")
        player_rows = self.fetch_player_counts()
        self._insert(PLAYER_TABLE, player_rows, "steam_player_counts")

        print("--- Reviews ---")
        review_rows = self.fetch_reviews()
        self._insert(REVIEW_TABLE, review_rows, "steam_reviews")

        achievement_count = 0
        if include_achievements:
            print("--- Achievements ---")
            ach_rows = self.fetch_achievements()
            self._insert(ACHIEVEMENT_TABLE, ach_rows, "steam_achievements")
            achievement_count = len(ach_rows)

        return {
            "status": "success",
            "games_tracked": len(PLAYER_COUNTS_TRACKED),
            "games_dnd": len(DND_GAMES),
            "games_ub": len(UB_GAMES),
            "player_snapshots": len(player_rows),
            "review_snapshots": len(review_rows),
            "achievements_tracked": achievement_count,
        }


@functions_framework.http
def steam_harvester_http(request):
    """HTTP entry point for Steam Harvester."""
    from shabbat_gate import is_shabbat, shabbat_skip_response
    if is_shabbat():
        return shabbat_skip_response()

    print("Starting Steam Harvester...")
    try:
        body = {}
        if request.content_type == "application/json":
            body = request.get_json(silent=True) or {}

        include_achievements = body.get("achievements", False)

        harvester = SteamHarvester()
        result = harvester.run(include_achievements=include_achievements)
        print(f"Complete: {json.dumps(result)}")
        return json.dumps(result), 200
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(e)}), 500
