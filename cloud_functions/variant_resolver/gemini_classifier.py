"""
cloud_functions/variant_resolver/gemini_classifier.py
Arcane Analytics — GCP project: dnd-trends-index

Gemini Classification — Stage 2 of the variant resolution pipeline
--------------------------------------------------------------------
Reads rows from variant_staging WHERE review_status = 'pending_gemini',
calls Gemini with a structured prompt that includes:
  - The candidate term
  - The seed keyword that surfaced it
  - The fuzzy match suggestion (if any) and its score
  - Known variants for the suggested concept (behavioral fingerprint)
  - The full category list from concept_library

Gemini returns a JSON decision:
  {
    "decision":    "variant" | "new_concept" | "noise",
    "parent":      "Ranger"  (if decision=variant),
    "category":    "Class"   (if decision=new_concept),
    "confidence":  "high" | "medium" | "low",
    "reasoning":   "One sentence explanation"
  }

Results are written back to variant_staging, updating:
  - gemini_decision, gemini_parent, gemini_category
  - gemini_confidence, gemini_reasoning
  - review_status → "pending_claude"

This module is called by main.py after run_fuzzy_match().
"""

import json
import time
import random
import logging
import datetime

import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.cloud import bigquery

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID     = "dnd-trends-index"
LOCATION       = "us-central1"
GEMINI_MODEL   = "gemini-1.5-flash"   # Fast + cheap for classification tasks
BATCH_SIZE     = 20                    # Terms per Gemini call
RETRY_LIMIT    = 3
RETRY_BACKOFF  = 5.0                   # seconds

STAGING_TABLE    = f"{PROJECT_ID}.dnd_trends_categorized.variant_staging"
VARIATIONS_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.concept_variations"
LIBRARY_TABLE    = f"{PROJECT_ID}.dnd_trends_categorized.concept_library"


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

# The full category list — used in the prompt so Gemini picks from real values
_KNOWN_CATEGORIES = [
    "Class", "Subclass", "Spell", "Feat", "Monster", "Species & Lineage",
    "Magic Item", "Rules", "AI Art", "Actual Play", "Background", "Convention",
    "Deity", "Equipment", "Faction", "Homebrew Class", "Homebrew Subclass",
    "Influencer", "Location", "Lore", "NPC", "Party Role", "Rulebooks",
    "Setting", "TTRPG System", "UA", "Videogame BG3", "VTT Platform",
    "YouTube", "Other",
]


def _build_system_prompt() -> str:
    return """You are a D&D search intelligence classifier for Arcane Analytics.

Your task: classify candidate search terms surfaced by Google Trends related_queries() 
against an existing D&D concept library.

For each term you will receive:
- The candidate term (a Google search string real users typed)
- The seed keyword that surfaced it  
- A fuzzy match suggestion (concept name + similarity score), if one was found
- Known search variants already associated with the fuzzy-matched concept
- The full category list

You must classify each term as one of:
  "variant"     — This term is just another way people search for an existing concept.
                  It belongs in concept_variations for that concept.
                  Example: "dnd ranger build" → variant of "Ranger"
  
  "new_concept" — This term represents a genuinely distinct D&D topic not yet tracked.
                  It deserves its own row in concept_library.
                  Example: "lazy dungeon master" → new concept, category "Influencer"
  
  "noise"       — Not a meaningful D&D search signal. Generic, ambiguous, or irrelevant.
                  Example: "how to play games online" → noise

Key rules:
- If the term contains a video game reference (Baldur's Gate, BG3, Larian) → "new_concept", category "Videogame BG3" UNLESS the non-BG3 meaning is clearly primary
- If the term is clearly a variant phrasing of the fuzzy-matched concept AND the score is above 0.50 → prefer "variant"  
- If the term introduces a creator name, show name, or platform → "new_concept" with appropriate category
- Homebrew content with a known creator → "new_concept", category "Homebrew Class" or "Homebrew Subclass"
- When uncertain → "new_concept" with confidence "low" (Claude will review)
- NEVER return "noise" for a term that has clear D&D intent, even if it's not in the library yet

Return ONLY a JSON array. No preamble, no markdown, no explanation outside the JSON.
""".strip()


def _build_user_prompt(batch: list[dict], known_variants: dict[str, list[str]]) -> str:
    """
    Build the per-batch user prompt.
    
    batch: list of variant_staging rows (pending_gemini)
    known_variants: {concept_name: [variant_string, ...]} from concept_variations
    """
    categories_str = ", ".join(_KNOWN_CATEGORIES)

    lines = [
        f"Available categories: {categories_str}",
        "",
        "Classify each of the following terms. Return a JSON array with one object per term.",
        "Each object must have exactly these keys: term, decision, parent, category, confidence, reasoning",
        "Set parent=null if decision != 'variant'. Set category=null if decision != 'new_concept'.",
        "",
        "---",
        "",
    ]

    for item in batch:
        term         = item["term"]
        seed         = item["seed_keyword"]
        fuzzy_match  = item.get("fuzzy_match_concept")
        fuzzy_score  = item.get("fuzzy_match_score", 0.0)

        lines.append(f"TERM: {term}")
        lines.append(f"  Seed keyword: {seed}")

        if fuzzy_match and fuzzy_score and fuzzy_score > 0.0:
            lines.append(f"  Fuzzy match suggestion: {fuzzy_match} (score: {fuzzy_score:.2f})")
            variants = known_variants.get(fuzzy_match.lower(), [])
            if variants:
                variants_str = ", ".join(f'"{v}"' for v in variants[:8])
                lines.append(f"  Known variants of {fuzzy_match}: {variants_str}")
            else:
                lines.append(f"  Known variants of {fuzzy_match}: (none yet)")
        else:
            lines.append("  Fuzzy match suggestion: none")

        lines.append("")

    lines.append("---")
    lines.append("Return ONLY a JSON array. Example format:")
    lines.append("""[
  {"term": "dnd ranger build", "decision": "variant", "parent": "Ranger", "category": null, "confidence": "high", "reasoning": "Standard build-guide phrasing for the Ranger class."},
  {"term": "lazy dungeon master", "decision": "new_concept", "parent": null, "category": "Influencer", "confidence": "high", "reasoning": "Matt Colville's influential DM philosophy brand with distinct search identity."}
]""")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _load_pending_gemini(client: bigquery.Client, run_id: str | None = None) -> list[dict]:
    """Load variant_staging rows awaiting Gemini classification."""
    run_filter = f"AND run_id = '{run_id}'" if run_id else ""
    query = f"""
        SELECT
            staging_id,
            run_id,
            term,
            seed_keyword,
            fuzzy_match_concept,
            fuzzy_match_score
        FROM `{STAGING_TABLE}`
        WHERE review_status = 'pending_gemini'
        {run_filter}
        ORDER BY created_at DESC
    """
    rows = list(client.query(query).result())
    log.info("Loaded %d pending_gemini rows from variant_staging", len(rows))
    return [dict(r) for r in rows]


def _load_known_variants(client: bigquery.Client) -> dict[str, list[str]]:
    """
    Load all active variants from concept_variations keyed by lowercase concept_id.
    Used to give Gemini behavioral fingerprints.
    """
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


def _update_staging_rows(client: bigquery.Client, updates: list[dict]) -> None:
    """
    Write Gemini decisions back to variant_staging.
    Uses individual UPDATE statements via the BQ query API.
    (Stream insert can't UPDATE — we need DML here.)
    """
    now = datetime.datetime.utcnow().isoformat()
    for update in updates:
        # Escape single quotes in text fields
        def _esc(s):
            return s.replace("'", "\\'") if s else ""

        decision   = _esc(update.get("gemini_decision", ""))
        parent     = _esc(update.get("gemini_parent", ""))
        category   = _esc(update.get("gemini_category", ""))
        confidence = _esc(update.get("gemini_confidence", ""))
        reasoning  = _esc(update.get("gemini_reasoning", ""))
        staging_id = update["staging_id"]

        parent_sql   = f"'{parent}'"   if parent   else "NULL"
        category_sql = f"'{category}'" if category else "NULL"

        query = f"""
            UPDATE `{STAGING_TABLE}`
            SET
                gemini_decision   = '{decision}',
                gemini_parent     = {parent_sql},
                gemini_category   = {category_sql},
                gemini_confidence = '{confidence}',
                gemini_reasoning  = '{reasoning}',
                review_status     = 'pending_claude',
                updated_at        = '{now}'
            WHERE staging_id = '{staging_id}'
        """
        client.query(query).result()

    log.info("Updated %d variant_staging rows with Gemini decisions", len(updates))


# ---------------------------------------------------------------------------
# Gemini call
# ---------------------------------------------------------------------------

def _call_gemini(
    model: GenerativeModel,
    system_prompt: str,
    user_prompt: str,
    attempt: int = 1,
) -> list[dict]:
    """
    Call Gemini with the classification prompt.
    Returns a list of decision dicts.
    Falls back to empty list on persistent failure.
    """
    try:
        response = model.generate_content(
            [user_prompt],
            generation_config=GenerationConfig(
                temperature=0.1,      # Low temp — we want consistent classification
                max_output_tokens=4096,
                response_mime_type="application/json",
            ),
        )
        raw = response.text.strip()

        # Strip markdown fences if present (safety net)
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
            log.error("Gemini permanently failed for this batch — skipping")
            return []
        time.sleep(RETRY_BACKOFF * attempt + random.uniform(0, 2))
        return _call_gemini(model, system_prompt, user_prompt, attempt + 1)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_decisions(
    batch: list[dict],
    gemini_response: list[dict],
) -> list[dict]:
    """
    Match Gemini response items back to staging rows by term.
    Returns a list of update dicts ready for _update_staging_rows().
    """
    # Build a lookup from term → staging_id
    term_to_staging = {row["term"].lower(): row["staging_id"] for row in batch}
    updates = []

    for item in gemini_response:
        term = item.get("term", "").lower()
        staging_id = term_to_staging.get(term)

        if not staging_id:
            log.warning("Gemini returned unknown term '%s' — skipping", term)
            continue

        decision   = item.get("decision", "noise")
        parent     = item.get("parent")
        category   = item.get("category")
        confidence = item.get("confidence", "low")
        reasoning  = item.get("reasoning", "")

        # Validation
        if decision not in ("variant", "new_concept", "noise"):
            log.warning("Invalid decision '%s' for term '%s' — defaulting to noise", decision, term)
            decision = "noise"

        updates.append({
            "staging_id":       staging_id,
            "gemini_decision":  decision,
            "gemini_parent":    parent if decision == "variant" else None,
            "gemini_category":  category if decision == "new_concept" else None,
            "gemini_confidence": confidence,
            "gemini_reasoning": reasoning,
        })

    # Any batch terms Gemini didn't return — mark as noise with low confidence
    returned_terms = {item.get("term", "").lower() for item in gemini_response}
    for row in batch:
        if row["term"].lower() not in returned_terms:
            log.warning("Gemini did not return result for '%s' — defaulting to noise", row["term"])
            updates.append({
                "staging_id":        row["staging_id"],
                "gemini_decision":   "noise",
                "gemini_parent":     None,
                "gemini_category":   None,
                "gemini_confidence": "low",
                "gemini_reasoning":  "No Gemini response received — defaulting to noise for Claude review.",
            })

    return updates


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_gemini_classification(
    client: bigquery.Client,
    run_id: str | None = None,
) -> dict:
    """
    Full Gemini classification pass.

    1. Load pending_gemini rows from variant_staging
    2. Load known variants for context
    3. Batch terms and call Gemini
    4. Write decisions back to variant_staging (review_status → pending_claude)
    5. Return summary stats

    Returns:
        {
            "terms_processed": int,
            "variant":         int,
            "new_concept":     int,
            "noise":           int,
            "batches":         int,
        }
    """
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    model = GenerativeModel(
        GEMINI_MODEL,
        system_instruction=_build_system_prompt(),
    )

    pending       = _load_pending_gemini(client, run_id)
    known_variants = _load_known_variants(client)

    if not pending:
        log.info("No pending_gemini rows to process")
        return {"terms_processed": 0, "variant": 0,
                "new_concept": 0, "noise": 0, "batches": 0}

    stats = {"variant": 0, "new_concept": 0, "noise": 0, "batches": 0}
    all_updates = []

    # Process in batches
    for i in range(0, len(pending), BATCH_SIZE):
        batch = pending[i: i + BATCH_SIZE]
        stats["batches"] += 1
        log.info("Processing batch %d (%d terms)", stats["batches"], len(batch))

        user_prompt = _build_user_prompt(batch, known_variants)
        gemini_response = _call_gemini(model, _build_system_prompt(), user_prompt)
        updates = _parse_decisions(batch, gemini_response)
        all_updates.extend(updates)

        # Tally stats
        for u in updates:
            decision = u.get("gemini_decision", "noise")
            if decision in stats:
                stats[decision] += 1

        # Polite delay between batches
        if i + BATCH_SIZE < len(pending):
            time.sleep(random.uniform(1, 3))

    # Write all updates to BigQuery
    if all_updates:
        _update_staging_rows(client, all_updates)

    stats["terms_processed"] = len(pending)
    log.info("Gemini classification complete: %s", stats)
    return stats
