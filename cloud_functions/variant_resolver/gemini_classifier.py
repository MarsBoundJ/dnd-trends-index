"""
cloud_functions/variant_resolver/gemini_classifier.py
Arcane Analytics — GCP project: dnd-trends-index

Gemini Classification — Stage 2 of the variant resolution pipeline
--------------------------------------------------------------------
Reads rows from variant_staging WHERE review_status = 'pending_gemini',
calls Gemini with a structured 4-bucket prompt, and routes each term to
its destination table based on bucket and confidence score.

Gemini returns a JSON decision per term:
  {
    "term":              "kali dungeons and dragons",
    "bucket":            "concept" | "insight" | "competitor" | "noise",
    "signal_category":   "new_player_acquisition" | ...   (insight bucket only),
    "game_system":       "pathfinder" | ...               (competitor bucket only),
    "native_category":   "Ancestry"                       (competitor bucket only),
    "dnd_analog":        "Race" | null                    (competitor bucket only),
    "variant_of":        "Dungeons and Dragons" | null    (concept bucket, variant only),
    "is_new_concept":    true | false                     (concept bucket only),
    "category":          "Class" | ...                    (concept/competitor bucket),
    "confidence":        0.87,                            (float 0.0-1.0)
    "reasoning":         "One sentence explanation."
  }

Routing by confidence threshold (CONFIDENCE_THRESHOLD = 0.80):
  >= threshold:
    concept  + variant_of set  → concept_variations
    concept  + is_new_concept  → concept_library
    insight                    → insight_library
    competitor                 → competitor_concepts
    noise                      → variant_staging (review_status = 'noise', no further action)
  < threshold (any bucket)     → review_queue (human review in Library Clerk)

Deduplication: terms already present in any destination table are skipped
(Option A first-sight policy — no re-classification unless manually triggered).

This module is called by main.py after run_fuzzy_match().
"""

import json
import time
import uuid
import random
import logging
import datetime

from google import genai
from google.genai import types as genai_types
from google.cloud import bigquery, secretmanager

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID           = "dnd-trends-index"
GEMINI_MODEL         = "gemini-2.5-flash"
GEMINI_SECRET_NAME   = f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"
BATCH_SIZE           = 10
RETRY_LIMIT          = 3
RETRY_BACKOFF        = 5.0
CONFIDENCE_THRESHOLD = 0.80   # Below this → review_queue

# ---------------------------------------------------------------------------
# Table references
# ---------------------------------------------------------------------------
STAGING_TABLE      = f"{PROJECT_ID}.dnd_trends_categorized.variant_staging"
VARIATIONS_TABLE   = f"{PROJECT_ID}.dnd_trends_categorized.concept_variations"
LIBRARY_TABLE      = f"{PROJECT_ID}.dnd_trends_categorized.concept_library"
INSIGHT_TABLE      = f"{PROJECT_ID}.dnd_trends_categorized.insight_library"
COMPETITOR_TABLE   = f"{PROJECT_ID}.dnd_trends_categorized.competitor_concepts"
REVIEW_QUEUE_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.review_queue"
SEEDS_TABLE        = f"{PROJECT_ID}.dnd_trends_categorized.seeds"


# ---------------------------------------------------------------------------
# Gemini API key (loaded once from Secret Manager at cold-start)
# ---------------------------------------------------------------------------

def _make_gemini_client() -> genai.Client:
    """Fetch the Gemini API key from Secret Manager and return a genai Client."""
    sm = secretmanager.SecretManagerServiceClient()
    api_key = sm.access_secret_version(name=GEMINI_SECRET_NAME).payload.data.decode("utf-8")
    log.info("Gemini API key loaded from Secret Manager")
    return genai.Client(api_key=api_key)


# ---------------------------------------------------------------------------
# Prompt vocabulary
# ---------------------------------------------------------------------------

_KNOWN_CATEGORIES = [
    "Class", "Subclass", "Spell", "Feat", "Monster", "Species & Lineage",
    "Magic Item", "Rules", "AI Art", "Actual Play", "Background", "Convention",
    "Deity", "Equipment", "Faction", "Homebrew Class", "Homebrew Subclass",
    "Influencer", "Location", "Lore", "NPC", "Party Role", "Rulebooks",
    "Setting", "TTRPG System", "UA", "Videogame BG3", "VTT Platform",
    "YouTube", "Other",
]

_SIGNAL_CATEGORIES = [
    "new_player_acquisition",   # Queries from people new to D&D (e.g. "how do you play dnd")
    "product_dissatisfaction",  # Searches suggesting problems with WotC products (e.g. "dnd beyond alternative")
    "media_crossover",          # D&D in movies, TV, or streaming (e.g. "honor among thieves streaming")
    "ip_crossover",             # D&D crossed with another IP (e.g. "mtg secret lair dnd")
    "competitive_intel",        # Competitor TTRPG signals that don't warrant a concept entry
    "other",                    # Meaningful insight that doesn't fit the above categories
]


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

def _build_system_prompt() -> str:
    return """You are a D&D search intelligence classifier for Arcane Analytics.

You classify Google Trends search queries into one of four buckets:

BUCKET: concept
  The term is a D&D concept — either a variant of an existing one, or a new one worth tracking.
  Examples: "dnd ranger build" (variant of Ranger), "lazy dungeon master" (new concept)

BUCKET: insight
  The term is not a D&D concept, but carries strategic signal about the D&D ecosystem.
  These are valuable to WotC stakeholders even though they don't belong in the concept library.
  Sub-types (signal_category):
    new_player_acquisition   — queries from people new to D&D (e.g. "how do you play dnd")
    product_dissatisfaction  — dissatisfaction with WotC products (e.g. "dnd beyond alternative")
    media_crossover          — D&D in movies/TV/streaming (e.g. "honor among thieves streaming")
    ip_crossover             — D&D crossed with another IP (e.g. "mtg secret lair dnd")
    competitive_intel        — competitor TTRPG ecosystem signals without a trackable concept
    other                    — meaningful insight not fitting the above

BUCKET: competitor
  The term refers to a concept from a non-D&D TTRPG system (Pathfinder, Call of Cthulhu, etc).
  These go into a separate competitor tracking table.
  Fields required: game_system, native_category (that system's own term), dnd_analog (nullable).
  Examples:
    "pathfinder druid"       → game_system: "pathfinder", native_category: "Class", dnd_analog: "Class"
    "call of cthulhu keeper" → game_system: "call_of_cthulhu", native_category: "Game Facilitator", dnd_analog: "DM"
    "blades in the dark crew"→ game_system: "blades_in_the_dark", native_category: "Crew", dnd_analog: null

BUCKET: noise
  Genuinely irrelevant. No meaningful signal, no D&D connection, no strategic value.
  Use sparingly — when in doubt, prefer "insight" with signal_category "other".

Key rules:
- If the term contains a video game reference (Baldur's Gate, BG3, Larian) → concept, category "Videogame BG3"
- If the term introduces a creator, show, or platform → concept, appropriate category
- Homebrew content with a known creator → concept, category "Homebrew Class" or "Homebrew Subclass"
- NEVER mark "noise" for a term with clear D&D intent
- confidence is a float 0.0–1.0. Use < 0.80 for genuine borderline cases only.

Return ONLY a JSON array. No preamble, no markdown, no text outside the JSON.
""".strip()


def _build_user_prompt(batch: list[dict], known_variants: dict[str, list[str]]) -> str:
    categories_str     = ", ".join(_KNOWN_CATEGORIES)
    signal_cats_str    = ", ".join(_SIGNAL_CATEGORIES)

    lines = [
        f"Available D&D categories: {categories_str}",
        f"Available signal_category values: {signal_cats_str}",
        "",
        "Classify each term below. Return a JSON array — one object per term.",
        "Required keys for every object:",
        "  term, bucket, signal_category, game_system, native_category, dnd_analog,",
        "  variant_of, is_new_concept, category, confidence, reasoning",
        "Set fields to null when not applicable to the bucket.",
        "",
        "---",
        "",
    ]

    for item in batch:
        term        = item["term"]
        seed        = item["seed_keyword"]
        fuzzy_match = item.get("fuzzy_match_concept")
        fuzzy_score = item.get("fuzzy_match_score", 0.0)
        is_breakout = item.get("is_breakout", False)
        search_val  = item.get("search_value")

        lines.append(f"TERM: {term}")
        lines.append(f"  Seed keyword:  {seed}")
        if is_breakout:
            lines.append("  Breakout:      YES (>5000% growth spike)")
        if search_val:
            lines.append(f"  Search value:  {search_val}")

        if fuzzy_match and fuzzy_score and fuzzy_score > 0.0:
            lines.append(f"  Fuzzy match:   {fuzzy_match} (score: {fuzzy_score:.2f})")
            variants = known_variants.get(fuzzy_match.lower(), [])
            if variants:
                vstr = ", ".join(f'"{v}"' for v in variants[:8])
                lines.append(f"  Known variants of {fuzzy_match}: {vstr}")
        else:
            lines.append("  Fuzzy match:   none")

        lines.append("")

    lines.append("---")
    lines.append("Return ONLY a JSON array. Example objects:")
    lines.append(json.dumps([
        {
            "term": "dnd ranger build", "bucket": "concept", "signal_category": None,
            "game_system": None, "native_category": None, "dnd_analog": None,
            "variant_of": "Ranger", "is_new_concept": False, "category": "Class",
            "confidence": 0.92, "reasoning": "Standard build-guide phrasing for the Ranger class.",
        },
        {
            "term": "dnd beyond alternative", "bucket": "insight", "signal_category": "product_dissatisfaction",
            "game_system": None, "native_category": None, "dnd_analog": None,
            "variant_of": None, "is_new_concept": False, "category": None,
            "confidence": 0.95, "reasoning": "Users seeking alternatives to WotC digital toolset — product dissatisfaction signal.",
        },
        {
            "term": "pathfinder druid", "bucket": "competitor", "signal_category": None,
            "game_system": "pathfinder", "native_category": "Class", "dnd_analog": "Class",
            "variant_of": None, "is_new_concept": True, "category": "Class",
            "confidence": 0.90, "reasoning": "Pathfinder's Druid class — direct competitor analog to D&D Druid.",
        },
    ], indent=2))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# BigQuery helpers — read
# ---------------------------------------------------------------------------

def _load_pending_gemini(client: bigquery.Client, run_id: str | None = None) -> list[dict]:
    """Load variant_staging rows awaiting Gemini classification.

    Reads both 'pending_gemini' (fuzzy match ambiguous/no-match) and
    'pending_claude' (fuzzy match high-confidence candidate) rows — both
    need Gemini AI classification in this pipeline.
    """
    run_filter = f"AND run_id = '{run_id}'" if run_id else ""
    query = f"""
        SELECT
            staging_id, run_id, term, seed_keyword,
            fuzzy_match_concept, fuzzy_match_score,
            is_breakout, search_value
        FROM `{STAGING_TABLE}`
        WHERE review_status IN ('pending_gemini', 'pending_claude')
        {run_filter}
        ORDER BY created_at DESC
    """
    rows = list(client.query(query).result())
    log.info("Loaded %d rows from variant_staging (pending_gemini + pending_claude)", len(rows))
    return [dict(r) for r in rows]


def _load_known_variants(client: bigquery.Client) -> dict[str, list[str]]:
    """Load all active variants keyed by lowercase concept_id."""
    query = f"""
        SELECT concept_id, variant_string
        FROM `{VARIATIONS_TABLE}`
        WHERE status = 'active'
        ORDER BY concept_id, is_best_variant DESC
    """
    result: dict[str, list[str]] = {}
    for row in client.query(query).result():
        key = row["concept_id"].lower()
        result.setdefault(key, []).append(row["variant_string"])
    log.info("Loaded known variants for %d concepts", len(result))
    return result


def _load_already_classified_terms(client: bigquery.Client) -> set[str]:
    """
    First-sight deduplication gate (Option A).
    Returns lowercase set of terms already routed to a FINAL destination table
    (concept_library, insight_library, competitor_concepts).

    review_queue is intentionally excluded — items there are pending human
    review and should be re-classifiable if Gemini runs again (e.g. after a
    previous failed run dumped everything there as fallback).
    """
    query = f"""
        SELECT LOWER(concept_name) AS term FROM `{LIBRARY_TABLE}` WHERE is_active = TRUE
        UNION DISTINCT
        SELECT LOWER(term)          FROM `{INSIGHT_TABLE}`
        UNION DISTINCT
        SELECT LOWER(concept_name)  FROM `{COMPETITOR_TABLE}` WHERE is_active = TRUE
    """
    seen = {row["term"] for row in client.query(query).result()}
    log.info("Dedup gate: %d terms already in final destination tables", len(seen))
    return seen


# ---------------------------------------------------------------------------
# BigQuery helpers — re-route stranded auto_routed rows
# ---------------------------------------------------------------------------

def _load_stranded_auto_routed(
    client: bigquery.Client,
    already_classified: set[str],
) -> list[dict]:
    """
    Load variant_staging rows with review_status='auto_routed' that are NOT
    yet in any destination table. These are rows whose staging status was
    successfully updated (Gemini classified them) but the _route_update call
    crashed or timed out before the destination table INSERT completed.

    Returns them as update dicts compatible with _route_update().
    """
    query = f"""
        SELECT
            staging_id, run_id, term, seed_keyword,
            is_breakout, search_value,
            gemini_bucket, gemini_decision, gemini_parent,
            gemini_category, gemini_confidence_score, gemini_reasoning,
            gemini_signal_category, gemini_game_system,
            gemini_native_category, gemini_dnd_analog
        FROM `{STAGING_TABLE}`
        WHERE review_status = 'auto_routed'
    """
    stranded = []
    for row in client.query(query).result():
        r = dict(row)
        if r["term"].lower() in already_classified:
            continue   # Already routed in a previous attempt
        stranded.append({
            "staging_id":          r["staging_id"],
            "run_id":              r.get("run_id"),
            "term":                r["term"],
            "seed_keyword":        r.get("seed_keyword"),
            "is_breakout":         r.get("is_breakout"),
            "search_value":        r.get("search_value"),
            "gemini_bucket":       r.get("gemini_bucket", "noise"),
            "gemini_decision":     r.get("gemini_decision"),
            "confidence":          float(r.get("gemini_confidence_score") or 0.0),
            "gemini_reasoning":    r.get("gemini_reasoning"),
            # Routing fields (names _route_update expects)
            "variant_of":          r.get("gemini_parent"),
            "is_new_concept":      r.get("gemini_decision") == "new_concept",
            "category":            r.get("gemini_category"),
            "signal_category":     r.get("gemini_signal_category"),
            "game_system":         r.get("gemini_game_system"),
            "native_category":     r.get("gemini_native_category"),
            "dnd_analog":          r.get("gemini_dnd_analog"),
            "next_review_status":  "auto_routed",   # Already set; not re-written
        })
    log.info("Found %d stranded auto_routed rows to re-route", len(stranded))
    return stranded


# ---------------------------------------------------------------------------
# BigQuery helpers — write
# ---------------------------------------------------------------------------

def _update_staging_rows(client: bigquery.Client, updates: list[dict]) -> None:
    """Write Gemini decisions back to variant_staging."""
    now = datetime.datetime.utcnow().isoformat()
    for u in updates:
        def _esc(s):
            return s.replace("'", "\\'") if s else ""

        def _sql_str(val):
            return f"'{_esc(val)}'" if val else "NULL"

        def _sql_bool(val):
            return "TRUE" if val else "FALSE"

        query = f"""
            UPDATE `{STAGING_TABLE}`
            SET
                gemini_decision        = {_sql_str(u.get("gemini_decision"))},
                gemini_parent          = {_sql_str(u.get("gemini_parent"))},
                gemini_category        = {_sql_str(u.get("gemini_category"))},
                gemini_confidence      = {_sql_str(u.get("gemini_confidence_label"))},
                gemini_reasoning       = {_sql_str(u.get("gemini_reasoning"))},
                gemini_bucket          = {_sql_str(u.get("gemini_bucket"))},
                gemini_signal_category = {_sql_str(u.get("gemini_signal_category"))},
                gemini_game_system     = {_sql_str(u.get("gemini_game_system"))},
                gemini_native_category = {_sql_str(u.get("gemini_native_category"))},
                gemini_dnd_analog      = {_sql_str(u.get("gemini_dnd_analog"))},
                gemini_confidence_score = {u.get("confidence", 0.0)},
                review_status          = {_sql_str(u.get("next_review_status", "pending_claude"))},
                updated_at             = '{now}'
            WHERE staging_id = '{u["staging_id"]}'
        """
        client.query(query).result()

    log.info("Updated %d variant_staging rows with Gemini decisions", len(updates))


def _insert_concept_library(client: bigquery.Client, row: dict) -> None:
    """Route a high-confidence new concept to concept_library."""
    now = datetime.datetime.utcnow().isoformat()
    record = {
        "concept_name":      row["term"],
        "category":          row.get("category") or "Other",
        "source_book":       None,
        "ruleset":           None,
        "tags":              [],
        "last_processed_at": now,
        "is_active":         True,
        "canonical_parent":  None,
        "sub_tags":          [],
        "persona_target":    None,
        "publisher":         None,
        "is_breakout":       row.get("is_breakout") or False,
        "seed_promoted":     False,
    }
    errors = client.insert_rows_json(LIBRARY_TABLE, [record])
    if errors:
        log.error("concept_library insert error for '%s': %s", row["term"], errors)
    else:
        log.info("Inserted new concept '%s' (category: %s)", row["term"], record["category"])


def _insert_concept_variation(client: bigquery.Client, row: dict) -> None:
    """Route a high-confidence variant to concept_variations."""
    now = datetime.datetime.utcnow().isoformat()
    record = {
        "variation_id":  str(uuid.uuid4()),
        "concept_id":    row["variant_of"],
        "variant_string": row["term"],
        "is_best_variant": False,
        "source":        "google_trends",
        "status":        "active",
        "date_added":    now,
        "added_by":      "gemini_classifier",
        "trend_score":   int(row["search_value"]) if row.get("search_value", "").isdigit() else None,
        "notes":         row.get("gemini_reasoning"),
    }
    errors = client.insert_rows_json(VARIATIONS_TABLE, [record])
    if errors:
        log.error("concept_variations insert error for '%s': %s", row["term"], errors)
    else:
        log.info("Inserted variant '%s' → parent '%s'", row["term"], row["variant_of"])


def _insert_insight_library(client: bigquery.Client, row: dict) -> None:
    """Route a high-confidence insight to insight_library."""
    now = datetime.datetime.utcnow().isoformat()
    record = {
        "term":            row["term"],
        "canonical_name":  None,
        "signal_category": row.get("signal_category") or "other",
        "search_value":    row.get("search_value"),
        "is_breakout":     row.get("is_breakout") or False,
        "source_seed":     row.get("seed_keyword"),
        "run_id":          row.get("run_id"),
        "is_active":       True,
        "notes":           row.get("gemini_reasoning"),
        "added_at":        now,
    }
    errors = client.insert_rows_json(INSIGHT_TABLE, [record])
    if errors:
        log.error("insight_library insert error for '%s': %s", row["term"], errors)
    else:
        log.info("Inserted insight '%s' (category: %s)", row["term"], record["signal_category"])


def _insert_competitor_concept(client: bigquery.Client, row: dict) -> None:
    """Route a high-confidence competitor term to competitor_concepts."""
    now = datetime.datetime.utcnow().isoformat()
    record = {
        "concept_name":      row["term"],
        "category":          row.get("category") or None,
        "source_book":       None,
        "ruleset":           None,
        "tags":              [],
        "last_processed_at": now,
        "is_active":         True,
        "canonical_parent":  None,
        "sub_tags":          [],
        "persona_target":    None,
        "publisher":         None,
        "game_system":       row.get("game_system"),
        "native_category":   row.get("native_category"),
        "dnd_analog":        row.get("dnd_analog"),
        "source_seed":       row.get("seed_keyword"),
        "search_value":      row.get("search_value"),
        "is_breakout":       row.get("is_breakout") or False,
        "run_id":            row.get("run_id"),
        "added_at":          now,
    }
    errors = client.insert_rows_json(COMPETITOR_TABLE, [record])
    if errors:
        log.error("competitor_concepts insert error for '%s': %s", row["term"], errors)
    else:
        log.info("Inserted competitor '%s' (%s / %s)", row["term"],
                 row.get("game_system"), row.get("native_category"))


def _insert_review_queue(client: bigquery.Client, row: dict) -> None:
    """Route a low-confidence term to the review_queue for human review."""
    now = datetime.datetime.utcnow().isoformat()
    record = {
        "review_id":               str(uuid.uuid4()),
        "term":                    row["term"],
        "suggested_bucket":        row.get("gemini_bucket"),
        "suggested_signal_category": row.get("signal_category"),
        "suggested_native_category": row.get("native_category"),
        "suggested_dnd_analog":    row.get("dnd_analog"),
        "suggested_game_system":   row.get("game_system"),
        "suggested_variant_of":    row.get("variant_of"),
        "is_new_concept":          row.get("is_new_concept"),
        "confidence":              row.get("confidence", 0.0),
        "reasoning":               row.get("gemini_reasoning"),
        "search_value":            row.get("search_value"),
        "is_breakout":             row.get("is_breakout") or False,
        "source_seed":             row.get("seed_keyword"),
        "run_id":                  row.get("run_id"),
        "status":                  "pending",
        "reviewer_notes":          None,
        "reviewed_at":             None,
        "added_at":                now,
    }
    errors = client.insert_rows_json(REVIEW_QUEUE_TABLE, [record])
    if errors:
        log.error("review_queue insert error for '%s': %s", row["term"], errors)
    else:
        log.info("Queued for review: '%s' (confidence: %.2f, bucket: %s)",
                 row["term"], row.get("confidence", 0.0), row.get("gemini_bucket"))


def _maybe_promote_seed(client: bigquery.Client, concept_name: str) -> None:
    """
    Auto-promote a newly approved concept to the seeds table so the next
    scrape run will use it as a seed.
    Marks the concept as seed_promoted=TRUE in concept_library.
    """
    now = datetime.datetime.utcnow().isoformat()
    seed_row = {
        "term":           concept_name,
        "added_by":       "auto",
        "source_concept": concept_name,
        "is_active":      True,
        "added_at":       now,
        "last_used_at":   None,
        "notes":          "Auto-promoted from Gemini classification.",
    }
    errors = client.insert_rows_json(SEEDS_TABLE, [seed_row])
    if errors:
        log.warning("seeds insert error for '%s': %s", concept_name, errors)
        return

    # Mark concept_library row as seed_promoted.
    # Wrapped in try/except: if the concept was just streaming-inserted this
    # run, BigQuery blocks DML UPDATE on streaming-buffer rows. The seed insert
    # above already succeeded; the mark can be retried on the next run.
    safe_name = concept_name.replace("'", "\\'")
    mark_sql = f"""
        UPDATE `{LIBRARY_TABLE}`
        SET seed_promoted = TRUE
        WHERE LOWER(concept_name) = LOWER('{safe_name}')
    """
    try:
        client.query(mark_sql).result()
        log.info("Auto-promoted '%s' to seeds table", concept_name)
    except Exception as exc:
        log.warning("Could not mark seed_promoted for '%s' (streaming buffer — will retry next run): %s",
                    concept_name, exc)


# ---------------------------------------------------------------------------
# Gemini call
# ---------------------------------------------------------------------------

def _call_gemini(
    client: genai.Client,
    system_prompt: str,
    user_prompt: str,
    attempt: int = 1,
) -> list[dict]:
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.1,
                max_output_tokens=16384,
                response_mime_type="application/json",
            ),
        )
        raw = response.text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        decisions = json.loads(raw)
        if not isinstance(decisions, list):
            raise ValueError(f"Expected JSON array, got {type(decisions)}")
        return decisions

    except Exception as exc:
        log.warning("Gemini call failed (attempt %d/%d): %s", attempt, RETRY_LIMIT, exc)
        if attempt >= RETRY_LIMIT:
            log.error("Gemini permanently failed — skipping batch")
            return []
        time.sleep(RETRY_BACKOFF * attempt + random.uniform(0, 2))
        return _call_gemini(client, system_prompt, user_prompt, attempt + 1)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _confidence_label(score: float) -> str:
    """Convert numeric confidence to legacy label (for backward compat)."""
    if score >= 0.85:
        return "high"
    if score >= 0.65:
        return "medium"
    return "low"


def _parse_and_enrich(
    batch: list[dict],
    gemini_response: list[dict],
) -> list[dict]:
    """
    Match Gemini response items back to staging rows by term.
    Enriches each update dict with staging context (run_id, seed_keyword,
    is_breakout, search_value) needed for destination table inserts.
    """
    term_to_row = {row["term"].lower(): row for row in batch}
    updates = []

    for item in gemini_response:
        term       = item.get("term", "").lower()
        source_row = term_to_row.get(term)
        if not source_row:
            log.warning("Gemini returned unknown term '%s' — skipping", term)
            continue

        bucket     = item.get("bucket", "noise")
        confidence = float(item.get("confidence", 0.0))

        if bucket not in ("concept", "insight", "competitor", "noise"):
            log.warning("Invalid bucket '%s' for '%s' — defaulting to noise", bucket, term)
            bucket = "noise"
            confidence = min(confidence, 0.5)

        # Determine next review_status for variant_staging
        if confidence >= CONFIDENCE_THRESHOLD and bucket != "noise":
            next_status = "auto_routed"
        elif bucket == "noise" and confidence >= CONFIDENCE_THRESHOLD:
            next_status = "noise"
        else:
            next_status = "pending_review"

        update = {
            # Staging identifiers
            "staging_id":          source_row["staging_id"],
            "run_id":              source_row["run_id"],
            "term":                source_row["term"],   # original case
            "seed_keyword":        source_row.get("seed_keyword"),
            "is_breakout":         source_row.get("is_breakout"),
            "search_value":        source_row.get("search_value"),
            # Gemini decisions
            "gemini_bucket":       bucket,
            "gemini_decision":     "variant" if item.get("variant_of") else ("new_concept" if item.get("is_new_concept") else bucket),
            "gemini_parent":       item.get("variant_of"),
            "gemini_category":     item.get("category"),
            "gemini_confidence_label": _confidence_label(confidence),
            "gemini_reasoning":    item.get("reasoning", ""),
            "gemini_signal_category": item.get("signal_category"),
            "gemini_game_system":  item.get("game_system"),
            "gemini_native_category": item.get("native_category"),
            "gemini_dnd_analog":   item.get("dnd_analog"),
            "confidence":          confidence,
            # Routing fields
            "signal_category":     item.get("signal_category"),
            "game_system":         item.get("game_system"),
            "native_category":     item.get("native_category"),
            "dnd_analog":          item.get("dnd_analog"),
            "variant_of":          item.get("variant_of"),
            "is_new_concept":      item.get("is_new_concept", False),
            "category":            item.get("category"),
            "next_review_status":  next_status,
        }
        updates.append(update)

    # Any terms Gemini didn't return — send to review queue as low confidence
    returned_terms = {item.get("term", "").lower() for item in gemini_response}
    for row in batch:
        if row["term"].lower() not in returned_terms:
            log.warning("No Gemini response for '%s' — queuing for review", row["term"])
            updates.append({
                "staging_id":          row["staging_id"],
                "run_id":              row["run_id"],
                "term":                row["term"],
                "seed_keyword":        row.get("seed_keyword"),
                "is_breakout":         row.get("is_breakout"),
                "search_value":        row.get("search_value"),
                "gemini_bucket":       "noise",
                "gemini_decision":     "noise",
                "gemini_parent":       None,
                "gemini_category":     None,
                "gemini_confidence_label": "low",
                "gemini_reasoning":    "No Gemini response received.",
                "gemini_signal_category": None,
                "gemini_game_system":  None,
                "gemini_native_category": None,
                "gemini_dnd_analog":   None,
                "confidence":          0.0,
                "signal_category":     None,
                "game_system":         None,
                "native_category":     None,
                "dnd_analog":          None,
                "variant_of":          None,
                "is_new_concept":      False,
                "category":            None,
                "next_review_status":  "pending_review",
            })

    return updates


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

def _route_update(client: bigquery.Client, u: dict) -> str:
    """
    Route a single classified term to its destination table.
    Returns the destination name for stats tracking.
    """
    bucket     = u.get("gemini_bucket", "noise")
    confidence = u.get("confidence", 0.0)

    # Low confidence → review queue regardless of bucket
    if confidence < CONFIDENCE_THRESHOLD:
        _insert_review_queue(client, u)
        return "review_queue"

    if bucket == "concept":
        if u.get("variant_of"):
            _insert_concept_variation(client, u)
            return "concept_variations"
        elif u.get("is_new_concept"):
            _insert_concept_library(client, u)
            _maybe_promote_seed(client, u["term"])
            return "concept_library"
        else:
            # Gemini said concept but didn't set variant_of or is_new_concept — queue it
            _insert_review_queue(client, u)
            return "review_queue"

    elif bucket == "insight":
        _insert_insight_library(client, u)
        return "insight_library"

    elif bucket == "competitor":
        _insert_competitor_concept(client, u)
        return "competitor_concepts"

    elif bucket == "noise":
        # noise at high confidence — just update staging status, no destination insert
        return "noise"

    else:
        _insert_review_queue(client, u)
        return "review_queue"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_gemini_classification(
    client: bigquery.Client,
    run_id: str | None = None,
) -> dict:
    """
    Full Gemini classification pass.

    1. Load already-classified terms (first-sight dedup gate)
    2. Load pending_gemini rows from variant_staging
    3. Load known variants for context
    4. Batch → Gemini → parse decisions
    5. Write decisions back to variant_staging
    6. Route each term to its destination table
    7. Return summary stats

    Returns:
        {
            "terms_processed":    int,
            "concept_library":    int,
            "concept_variations": int,
            "insight_library":    int,
            "competitor_concepts":int,
            "review_queue":       int,
            "noise":              int,
            "skipped_dedup":      int,
            "batches":            int,
        }
    """
    gemini_client      = _make_gemini_client()
    system_prompt      = _build_system_prompt()

    already_classified = _load_already_classified_terms(client)
    pending            = _load_pending_gemini(client, run_id)
    known_variants     = _load_known_variants(client)

    stats = {
        "terms_processed": 0, "concept_library": 0, "concept_variations": 0,
        "insight_library": 0, "competitor_concepts": 0, "review_queue": 0,
        "noise": 0, "skipped_dedup": 0, "batches": 0,
        "re_routed": 0,
    }

    # --- Re-route any stranded auto_routed rows from a previous partial run ---
    stranded = _load_stranded_auto_routed(client, already_classified)
    for u in stranded:
        destination = _route_update(client, u)
        if destination in stats:
            stats[destination] += 1
        stats["re_routed"] += 1
        # Refresh already_classified so these terms are excluded from new pass
        already_classified.add(u["term"].lower())

    if not pending:
        log.info("No pending_gemini rows to process (re_routed: %d)", stats["re_routed"])
        return stats

    # Apply first-sight dedup gate
    fresh   = [r for r in pending if r["term"].lower() not in already_classified]
    skipped = len(pending) - len(fresh)
    stats["skipped_dedup"] = skipped
    if skipped:
        log.info("Dedup gate skipped %d already-classified terms", skipped)

    all_updates = []

    for i in range(0, len(fresh), BATCH_SIZE):
        batch = fresh[i: i + BATCH_SIZE]
        stats["batches"] += 1
        log.info("Processing batch %d (%d terms)", stats["batches"], len(batch))

        user_prompt     = _build_user_prompt(batch, known_variants)
        gemini_response = _call_gemini(gemini_client, system_prompt, user_prompt)
        updates         = _parse_and_enrich(batch, gemini_response)
        all_updates.extend(updates)

        if i + BATCH_SIZE < len(fresh):
            time.sleep(random.uniform(1, 3))

    # Write all decisions back to variant_staging
    if all_updates:
        _update_staging_rows(client, all_updates)

    # Route each update to its destination table
    for u in all_updates:
        destination = _route_update(client, u)
        if destination in stats:
            stats[destination] += 1

    stats["terms_processed"] = len(fresh)
    log.info("Gemini classification complete: %s", stats)
    return stats
