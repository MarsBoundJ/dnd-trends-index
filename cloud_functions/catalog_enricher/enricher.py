"""
cloud_functions/catalog_enricher/enricher.py
Arcane Analytics — GCP project: dnd-trends-index

Gemini-powered enrichment for catalog_supply rows.

Flow:
  1. Load distinct (title, source, snippet) from catalog_supply not yet in catalog_enrichment.
  2. Send batches of 15 to Gemini-2.5-flash for classification.
  3. Write enrichment rows to dnd_trends_raw.catalog_enrichment.

Enrichment fields added per product:
  - product_type  (Adventure / Setting Guide / Rules Supplement / Player Options /
                   Maps & Assets / Monster/NPC Compendium / Solo Adventure /
                   Accessory / Fiction / Other)
  - game_system   (D&D 5e (2014) / D&D 5e (2024) / OSR / Pathfinder 1e /
                   Pathfinder 2e / System Agnostic / Other)
  - setting       (Forgotten Realms / Eberron / Ravenloft / Greyhawk / Dark Sun /
                   Spelljammer / Planescape / Dragonlance / Mystara /
                   Original Setting / System Agnostic / Unknown)
  - theme         (Horror / Exploration / Combat-Heavy / Social/Political /
                   Mystery / Comedy / High Fantasy / Cosmic/Planar / Mixed)
"""

import json
import logging
import time
from datetime import datetime, timezone

from google import genai
from google.genai import types as genai_types
from google.cloud import bigquery, secretmanager

log = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
BATCH_SIZE = 15
ENRICHMENT_TABLE = f"{PROJECT_ID}.dnd_trends_raw.catalog_enrichment"
SUPPLY_TABLE = f"{PROJECT_ID}.dnd_trends_raw.catalog_supply"

# ---------- BigQuery setup helper ----------

SETUP_SQL = f"""
CREATE TABLE IF NOT EXISTS `{ENRICHMENT_TABLE}` (
    title             STRING NOT NULL,
    source            STRING NOT NULL,
    product_type      STRING,
    game_system       STRING,
    setting           STRING,
    theme             STRING,
    enrichment_model  STRING,
    enriched_at       TIMESTAMP
)
"""

GOLD_VIEW_SQL = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.commercial_data.catalog_enriched` AS
SELECT
    cs.collected_date,
    cs.source,
    cs.title,
    cs.publisher,
    cs.seller_tier,
    cs.price,
    cs.snippet,
    ce.product_type,
    ce.game_system,
    ce.setting,
    ce.theme,
    ce.enriched_at
FROM `{SUPPLY_TABLE}` cs
LEFT JOIN `{ENRICHMENT_TABLE}` ce
    ON LOWER(cs.title) = LOWER(ce.title)
    AND cs.source = ce.source
"""


def _ensure_table(bq: bigquery.Client) -> None:
    try:
        bq.query(SETUP_SQL).result()
        log.info("catalog_enrichment table ready")
    except Exception as e:
        log.warning("Table setup warning (may already exist): %s", e)


def _ensure_gold_view(bq: bigquery.Client) -> None:
    try:
        bq.query(GOLD_VIEW_SQL).result()
        log.info("catalog_enriched gold view ready")
    except Exception as e:
        log.warning("Gold view setup warning: %s", e)


# ---------- Gemini client ----------

def _make_gemini_client() -> genai.Client:
    sm = secretmanager.SecretManagerServiceClient()
    api_key = sm.access_secret_version(name=GEMINI_SECRET_NAME).payload.data.decode("utf-8")
    log.info("Gemini API key loaded from Secret Manager")
    return genai.Client(api_key=api_key)


# ---------- Data loading ----------

def _load_unenriched(bq: bigquery.Client, limit: int) -> list[dict]:
    """
    Returns distinct (title, source, snippet) from catalog_supply
    that do not yet have a row in catalog_enrichment.
    """
    query = f"""
        SELECT DISTINCT
            cs.title,
            cs.source,
            MAX(cs.snippet) AS snippet
        FROM `{SUPPLY_TABLE}` cs
        WHERE NOT EXISTS (
            SELECT 1 FROM `{ENRICHMENT_TABLE}` ce
            WHERE LOWER(ce.title) = LOWER(cs.title)
              AND ce.source = cs.source
        )
        GROUP BY cs.title, cs.source
        ORDER BY cs.title
        LIMIT {limit}
    """
    rows = list(bq.query(query).result())
    log.info("Found %d unenriched catalog items", len(rows))
    return [dict(r) for r in rows]


# ---------- Gemini prompts ----------

SYSTEM_PROMPT = """You are an expert in tabletop RPG products, specifically products sold on DMs Guild and DriveThruRPG. You classify products using a controlled vocabulary.

Respond ONLY with a valid JSON array — no markdown, no explanation, no trailing text.

PRODUCT_TYPE values (pick the single best fit):
  "Adventure"               — scenario, module, campaign, dungeon, heist, one-shot
  "Setting Guide"           — worldbuilding, campaign setting, gazetteers, lore compendium
  "Rules Supplement"        — mechanics, rules expansion, optional rules, variant rules
  "Player Options"          — character options, subclasses, races/species, spells, feats, backgrounds
  "Maps & Assets"           — battle maps, tokens, handouts, VTT assets
  "Monster/NPC Compendium"  — creature statblocks, bestiary, NPC collections
  "Solo Adventure"          — designed explicitly for solo play
  "Accessory"               — DM screens, trackers, toolkits, non-content utility items
  "Fiction"                 — novels, short stories, in-world fiction
  "Other"                   — does not fit any above

GAME_SYSTEM values (pick the single best fit):
  "D&D 5e (2014)"     — Fifth Edition, 2014 core books, SRD 5.1
  "D&D 5e (2024)"     — One D&D, 2024 revised Player's Handbook
  "OSR"               — Old-school revival, retroclone, Basic, 1e/2e-inspired, OSRIC, Labyrinth Lord
  "Pathfinder 1e"
  "Pathfinder 2e"
  "System Agnostic"   — explicitly compatible with multiple systems or any system
  "Other"             — specific named system not listed (Starfinder, Shadowrun, etc.)

SETTING values (pick the single best fit):
  "Forgotten Realms"   — Faerûn, Waterdeep, Baldur's Gate, Neverwinter, Candlekeep
  "Eberron"            — magitech, noir, Khorvaire, Sharn
  "Ravenloft"          — Domains of Dread, gothic horror, Barovia, Strahd
  "Greyhawk"           — Oerth, classic Gary Gygax setting, Flanaess
  "Dark Sun"           — Athas, post-apocalyptic desert, psionics
  "Spelljammer"        — wildspace, crystal spheres, space D&D
  "Planescape"         — Sigil, outer planes, Lady of Pain
  "Dragonlance"        — Krynn, War of the Lance, Kender
  "Mystara"            — Known World, classic Basic D&D
  "Original Setting"   — author's own world not listed above
  "System Agnostic"    — no specific setting, usable anywhere
  "Unknown"            — cannot determine from available information

THEME values (pick the single best fit):
  "Horror"            — dark, gothic, fear, psychological terror
  "Exploration"       — wilderness, hex crawl, discovery, travel
  "Combat-Heavy"      — dungeon crawl, tactical battles, war
  "Social/Political"  — intrigue, diplomacy, faction play, NPC-driven
  "Mystery"           — investigation, whodunnit, puzzles
  "Comedy"            — humorous, parody, lighthearted
  "High Fantasy"      — epic, heroic, world-saving, chosen ones
  "Cosmic/Planar"     — outer planes, elder gods, reality-bending, existential
  "Mixed"             — clearly blends two or more themes equally"""


def _build_user_prompt(batch: list[dict]) -> str:
    items = []
    for i, row in enumerate(batch):
        items.append({
            "id": i,
            "title": row["title"],
            "source": row["source"],
            "snippet": (row.get("snippet") or "")[:300] or "(no description available)"
        })
    return "Classify these TTRPG products:\n\n" + json.dumps(items, indent=2)


# ---------- Gemini call ----------

def _call_gemini(client: genai.Client, user_prompt: str, attempt: int = 1) -> list[dict]:
    if attempt > 3:
        raise RuntimeError("Gemini failed after 3 attempts")
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                temperature=0.1,
                max_output_tokens=8192,
                response_mime_type="application/json",
            ),
        )
        raw = response.text.strip()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning("JSON parse error on attempt %d: %s", attempt, e)
        time.sleep(2 ** attempt)
        return _call_gemini(client, user_prompt, attempt + 1)
    except Exception as e:
        log.warning("Gemini error on attempt %d: %s", attempt, e)
        time.sleep(2 ** attempt)
        return _call_gemini(client, user_prompt, attempt + 1)


# ---------- Response parsing ----------

VALID_PRODUCT_TYPES = {
    "Adventure", "Setting Guide", "Rules Supplement", "Player Options",
    "Maps & Assets", "Monster/NPC Compendium", "Solo Adventure",
    "Accessory", "Fiction", "Other"
}
VALID_GAME_SYSTEMS = {
    "D&D 5e (2014)", "D&D 5e (2024)", "OSR", "Pathfinder 1e",
    "Pathfinder 2e", "System Agnostic", "Other"
}
VALID_SETTINGS = {
    "Forgotten Realms", "Eberron", "Ravenloft", "Greyhawk", "Dark Sun",
    "Spelljammer", "Planescape", "Dragonlance", "Mystara",
    "Original Setting", "System Agnostic", "Unknown"
}
VALID_THEMES = {
    "Horror", "Exploration", "Combat-Heavy", "Social/Political",
    "Mystery", "Comedy", "High Fantasy", "Cosmic/Planar", "Mixed"
}


def _validate(value: str | None, valid_set: set, default: str) -> str:
    if value and value in valid_set:
        return value
    return default


def _parse_results(batch: list[dict], gemini_response: list[dict]) -> list[dict]:
    result_map = {r.get("id"): r for r in gemini_response if isinstance(r, dict)}
    enriched = []
    for i, row in enumerate(batch):
        r = result_map.get(i, {})
        enriched.append({
            "title": row["title"],
            "source": row["source"],
            "product_type": _validate(r.get("product_type"), VALID_PRODUCT_TYPES, "Other"),
            "game_system":  _validate(r.get("game_system"),  VALID_GAME_SYSTEMS,  "Other"),
            "setting":      _validate(r.get("setting"),       VALID_SETTINGS,      "Unknown"),
            "theme":        _validate(r.get("theme"),         VALID_THEMES,        "Mixed"),
            "enrichment_model": GEMINI_MODEL,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        })
    return enriched


# ---------- BigQuery write ----------

def _write_enrichment(bq: bigquery.Client, rows: list[dict]) -> None:
    if not rows:
        return
    errors = bq.insert_rows_json(ENRICHMENT_TABLE, rows)
    if errors:
        log.error("BigQuery insert errors: %s", errors)
        raise RuntimeError(f"BQ insert failed: {errors}")
    log.info("Wrote %d enrichment rows", len(rows))


# ---------- Main ----------

def run_catalog_enrichment(bq: bigquery.Client, limit: int = 500) -> dict:
    """
    Runs one enrichment pass. Returns stats dict.
    """
    _ensure_table(bq)
    _ensure_gold_view(bq)

    pending = _load_unenriched(bq, limit=limit)
    if not pending:
        log.info("Nothing to enrich.")
        return {"items_enriched": 0, "batches": 0}

    gemini = _make_gemini_client()
    total_enriched = 0
    batches = 0

    for i in range(0, len(pending), BATCH_SIZE):
        batch = pending[i : i + BATCH_SIZE]
        log.info("Batch %d: enriching %d items", batches + 1, len(batch))

        user_prompt = _build_user_prompt(batch)
        gemini_response = _call_gemini(gemini, user_prompt)
        enriched_rows = _parse_results(batch, gemini_response)
        _write_enrichment(bq, enriched_rows)

        total_enriched += len(enriched_rows)
        batches += 1

        # Small pause between batches to stay within rate limits
        if i + BATCH_SIZE < len(pending):
            time.sleep(1)

    return {
        "items_enriched": total_enriched,
        "batches": batches,
    }
