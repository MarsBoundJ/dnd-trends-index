"""
cloud_functions/variant_resolver/fuzzy_match.py
Arcane Analytics — GCP project: dnd-trends-index

Fuzzy Match Pre-Filter
-----------------------
Stage 1 of the variant resolution pipeline.

Reads PENDING rows from dnd_trends_raw.emerging_terms, runs token-overlap
fuzzy matching against concept_library.concept_name, and writes results to
dnd_trends_categorized.variant_staging with:

  - fuzzy_match_concept  : best matching concept_name (if any)
  - fuzzy_match_score    : similarity score 0.0-1.0
  - fuzzy_match_method   : "token_overlap"
  - review_status        : "pending_claude" (high confidence match)
                           "pending_gemini" (low confidence, needs AI)

Rows with score >= HIGH_CONFIDENCE_THRESHOLD are pre-tagged as variant
candidates. Rows below are passed to Gemini (Stage 2) with no suggestion.

This module is called by main.py (Cloud Function entry point).
"""

import re
import hashlib
import datetime
import logging
from dataclasses import dataclass

from google.cloud import bigquery

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
HIGH_CONFIDENCE_THRESHOLD = 0.72   # Score >= this → strong variant candidate
LOW_CONFIDENCE_THRESHOLD  = 0.30   # Score < this  → likely new concept or noise

# ---------------------------------------------------------------------------
# BigQuery table references
# ---------------------------------------------------------------------------
PROJECT_ID      = "dnd-trends-index"
EMERGING_TABLE  = f"{PROJECT_ID}.dnd_trends_raw.emerging_terms"
LIBRARY_TABLE   = f"{PROJECT_ID}.dnd_trends_categorized.concept_library"
STAGING_TABLE   = f"{PROJECT_ID}.dnd_trends_categorized.variant_staging"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class ConceptRecord:
    concept_name: str
    category: str
    ruleset: str | None
    tokens: set[str]          # Pre-tokenized for fast matching


@dataclass
class FuzzyResult:
    term: str
    seed_keyword: str
    run_id: str
    term_id: str
    data_type: str
    value: int | None
    is_breakout: bool
    best_concept: str | None
    best_score: float
    best_category: str | None
    review_status: str        # "pending_claude" | "pending_gemini"


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------

# Words that add no signal for matching — stripped before comparison
_STOPWORDS = {
    "dnd", "d&d", "5e", "2024", "2014", "the", "a", "an", "of", "and",
    "to", "in", "for", "how", "best", "guide", "class", "spell",
    "feat", "monster", "subclass", "species", "homebrew", "one", "what",
    "is", "are", "do", "does", "can", "my", "your", "with", "vs", "or",
    "official", "character", "player", "dungeon", "master", "dm", "pc",
    "s",   # apostrophe fragments e.g. "Baldur's" → "baldur" + "s"
}


def _tokenize(text: str) -> set[str]:
    """
    Lowercase, strip punctuation, split on whitespace, remove stopwords.
    Returns a set of meaningful tokens.

    Examples:
        "dnd ranger build 2024"  → {"ranger", "build"}
        "Blood Hunter 5e guide"  → {"blood", "hunter"}
        "Baldur's Gate 3 Druid"  → {"baldur", "gate", "3", "druid"}
    """
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)   # punctuation → space
    tokens = set(text.split())
    return tokens - _STOPWORDS


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _token_overlap_score(candidate_tokens: set[str], concept_tokens: set[str]) -> float:
    """
    Jaccard-style overlap weighted toward the concept side.

    We weight toward the concept because a short concept name like "Ranger"
    (1 token after stopword removal) should score highly against
    "dnd ranger build" (tokens: {"ranger", "build"}) — the concept token
    appears, even though the candidate has extra tokens.

    Formula: |intersection| / |concept_tokens|
    Capped at 1.0, falls back to standard Jaccard if concept_tokens is empty.
    """
    if not concept_tokens or not candidate_tokens:
        return 0.0

    intersection = candidate_tokens & concept_tokens
    if not intersection:
        return 0.0

    # Concept-weighted score: what fraction of concept tokens appear in candidate
    concept_score = len(intersection) / len(concept_tokens)

    # Standard Jaccard: penalizes candidates with many extra unrelated tokens
    union = candidate_tokens | concept_tokens
    jaccard = len(intersection) / len(union)

    # Weighted blend: 60% concept-weighted, 40% Jaccard
    # Higher Jaccard weight penalizes candidates with many unrelated tokens
    # e.g. "baldurs gate 3 ranger" vs "Ranger" — ranger token matches but
    # the candidate has 3 other unrelated tokens, Jaccard pulls score down
    return round((0.6 * concept_score) + (0.4 * jaccard), 4)


def find_best_match(
    term: str,
    concepts: list[ConceptRecord],
) -> tuple[ConceptRecord | None, float]:
    """
    Find the best matching concept for a candidate term.
    Returns (best_concept, score) or (None, 0.0) if no match found.
    """
    candidate_tokens = _tokenize(term)
    if not candidate_tokens:
        return None, 0.0

    best_concept = None
    best_score = 0.0

    for concept in concepts:
        if not concept.tokens:
            continue
        score = _token_overlap_score(candidate_tokens, concept.tokens)
        if score > best_score:
            best_score = score
            best_concept = concept

    return best_concept, best_score


# ---------------------------------------------------------------------------
# BigQuery I/O
# ---------------------------------------------------------------------------

def _load_concept_library(client: bigquery.Client) -> list[ConceptRecord]:
    """
    Pull all active concepts from concept_library and pre-tokenize them.
    """
    query = f"""
        SELECT
            concept_name,
            category,
            ruleset
        FROM `{LIBRARY_TABLE}`
        WHERE is_active = TRUE
          AND concept_name IS NOT NULL
    """
    records = []
    for row in client.query(query).result():
        tokens = _tokenize(row["concept_name"])
        records.append(ConceptRecord(
            concept_name = row["concept_name"],
            category     = row["category"] or "",
            ruleset      = row["ruleset"],
            tokens       = tokens,
        ))
    log.info("Loaded %d active concepts from concept_library", len(records))
    return records


def _load_pending_terms(client: bigquery.Client, run_id: str | None = None) -> list[dict]:
    """
    Pull PENDING rows from emerging_terms.
    If run_id is provided, filter to that run only.
    """
    run_filter = f"AND run_id = '{run_id}'" if run_id else ""
    query = f"""
        SELECT
            run_id,
            term_id,
            term,
            seed_keyword,
            data_type,
            value,
            is_breakout
        FROM `{EMERGING_TABLE}`
        WHERE review_status = 'PENDING'
        {run_filter}
        ORDER BY flagged_at DESC
    """
    rows = list(client.query(query).result())
    log.info("Loaded %d PENDING emerging terms", len(rows))
    return [dict(r) for r in rows]


def _already_staged(client: bigquery.Client, term_ids: list[str]) -> set[str]:
    """
    Return set of term_ids already present in variant_staging
    to avoid duplicate staging rows across runs.
    """
    if not term_ids:
        return set()
    ids_str = ", ".join(f"'{t}'" for t in term_ids)
    query = f"""
        SELECT DISTINCT term_id
        FROM `{STAGING_TABLE}`
        WHERE term_id IN ({ids_str})
    """
    return {row["term_id"] for row in client.query(query).result()}


def _build_staging_row(result: FuzzyResult, run_id: str) -> dict:
    """Convert a FuzzyResult into a variant_staging insert row."""
    now = datetime.datetime.utcnow().isoformat()
    staging_id = hashlib.md5(
        f"{run_id}|{result.term}".encode()
    ).hexdigest()

    return {
        "staging_id":          staging_id,
        "run_id":              run_id,
        "term":                result.term,
        "seed_keyword":        result.seed_keyword,
        "fuzzy_match_concept": result.best_concept,
        "fuzzy_match_score":   result.best_score,
        "fuzzy_match_method":  "token_overlap" if result.best_score > 0 else "none",
        "gemini_decision":     None,
        "gemini_parent":       None,
        "gemini_category":     None,
        "gemini_confidence":   None,
        "gemini_reasoning":    None,
        "claude_decision":     None,
        "claude_override":     None,
        "claude_notes":        None,
        "review_status":       result.review_status,
        "phil_notes":          None,
        "created_at":          now,
        "updated_at":          now,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_fuzzy_match(
    client: bigquery.Client,
    run_id: str | None = None,
) -> dict:
    """
    Full fuzzy match pass.

    1. Load concept_library
    2. Load PENDING emerging_terms
    3. Score each term against all concepts
    4. Write results to variant_staging
    5. Return summary stats

    Returns:
        {
            "terms_processed": int,
            "high_confidence": int,   # score >= HIGH_CONFIDENCE_THRESHOLD
            "low_confidence":  int,   # score between thresholds
            "no_match":        int,   # score < LOW_CONFIDENCE_THRESHOLD
            "skipped":         int,   # already in staging
        }
    """
    concepts = _load_concept_library(client)
    pending  = _load_pending_terms(client, run_id)

    if not pending:
        log.info("No PENDING terms to process")
        return {"terms_processed": 0, "high_confidence": 0,
                "low_confidence": 0, "no_match": 0, "skipped": 0}

    # Dedup check
    all_term_ids   = [r["term_id"] for r in pending]
    already_staged = _already_staged(client, all_term_ids)

    staging_rows = []
    stats = {"high_confidence": 0, "low_confidence": 0,
             "no_match": 0, "skipped": 0}

    for row in pending:
        if row["term_id"] in already_staged:
            stats["skipped"] += 1
            continue

        best_concept, best_score = find_best_match(row["term"], concepts)

        # Determine review routing
        if best_score >= HIGH_CONFIDENCE_THRESHOLD:
            review_status = "pending_claude"   # Strong match — Claude reviews
            stats["high_confidence"] += 1
        elif best_score >= LOW_CONFIDENCE_THRESHOLD:
            review_status = "pending_gemini"   # Ambiguous — Gemini classifies
            stats["low_confidence"] += 1
        else:
            review_status = "pending_gemini"   # No match — Gemini decides
            stats["no_match"] += 1

        result = FuzzyResult(
            term          = row["term"],
            seed_keyword  = row["seed_keyword"],
            run_id        = row["run_id"],
            term_id       = row["term_id"],
            data_type     = row["data_type"],
            value         = row["value"],
            is_breakout   = row["is_breakout"],
            best_concept  = best_concept.concept_name if best_concept else None,
            best_score    = best_score,
            best_category = best_concept.category if best_concept else None,
            review_status = review_status,
        )
        staging_rows.append(_build_staging_row(result, row["run_id"]))

    # Write to variant_staging
    if staging_rows:
        errors = client.insert_rows_json(STAGING_TABLE, staging_rows)
        if errors:
            log.error("BQ insert errors: %s", errors)
            raise RuntimeError(f"variant_staging insert failed: {errors}")
        log.info("Wrote %d rows to variant_staging", len(staging_rows))

    stats["terms_processed"] = len(pending)
    return stats
