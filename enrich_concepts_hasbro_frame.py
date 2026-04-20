"""
Arcane Analytics — Hasbro 2026 concept enrichment (Step 9.8, Chunk C).

For each concept in `dnd_trends_categorized.concept_library` (~11,977
rows, ~11,910 active), scores the concept against Hasbro's interpretive
taxonomies and writes the result to
`frames/hasbro-2026/concept_mappings/{slug}`.

The concept library itself stays vendor-neutral — this side-table is the
Hasbro-specific overlay, so switching to `frames/paizo-2026` later means
a different enrichment pass, not a concept-library rewrite.

Fields per concept:
    {
        "concept_name": str,           # exact library concept_name
        "concept_slug": str,           # doc ID (slug of name + category)
        "category": str | None,
        # ─── Hasbro portfolio tier ────────────────────────────────────
        "portfolio_tier": "Grow" | "Optimize" | "Reinvent",
        "portfolio_tier_confidence": float 0..1,
        # ─── GEM² four binary dimensions ──────────────────────────────
        "gem2_gamified": bool,
        "gem2_entertainment_driven": bool,
        "gem2_multi_purchase": bool,
        "gem2_multi_generational": bool,
        "gem2_confidence": float 0..1,
        # ─── Audit trail ──────────────────────────────────────────────
        "reasoning": str,              # one-line justification
        "model": str,                  # "gemini-2.5-flash"
        "enriched_at": server timestamp
    }

Args:
    --dry-run          Don't write to Firestore; print results to stdout.
    --limit N          Stop after N concepts (useful with --sample for
                       a 50-concept dry-run).
    --sample           Select concepts spread across categories
                       deterministically via FARM_FINGERPRINT.
    --batch-size N     Gemini batch size (default 20).
    --force            Re-score concepts that already have a mapping.
    --categories c1,c2 Only enrich these categories (comma-sep list).

Resumability:
    On each run, existing Firestore docs in concept_mappings are read up
    front and those concept_slugs are skipped (unless --force).

Cost:
    Gemini 2.5-flash is roughly $0.30/1M input tokens + $2.50/1M output
    tokens. Per-concept usage is small (~100 input + ~120 output tokens).
    Estimated full-library cost: well under $50 (< $10 at current
    pricing). A --dry-run on 50 concepts is effectively free.

Auth:
    - BigQuery: ADC (`gcloud auth application-default login`).
    - Firestore: ADC.
    - Gemini: GEMINI_API_KEY env var (preferred for local), else fetched
      from Secret Manager projects/dnd-trends-index/secrets/gemini-api-key.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any

from google import genai
from google.genai import types as genai_types
from google.cloud import bigquery, firestore, secretmanager
from google.api_core import exceptions as gexc

PROJECT_ID = "dnd-trends-index"
CONCEPT_LIBRARY_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.concept_library"
FRAMES_COLLECTION = "frames"
FRAME_ID = "hasbro-2026"
MAPPINGS_SUBCOLLECTION = "concept_mappings"

GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_SECRET_NAME = f"projects/{PROJECT_ID}/secrets/gemini-api-key/versions/latest"

DEFAULT_BATCH_SIZE = 20
RETRY_LIMIT = 3
RETRY_BACKOFF_SEC = 5.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─── Gemini setup ─────────────────────────────────────────────────────────

def _get_gemini_api_key() -> str:
    key = os.environ.get("GEMINI_API_KEY")
    if key:
        log.info("Using GEMINI_API_KEY from environment")
        return key
    log.info("Fetching Gemini API key from Secret Manager")
    sm = secretmanager.SecretManagerServiceClient()
    resp = sm.access_secret_version(name=GEMINI_SECRET_NAME)
    return resp.payload.data.decode("utf-8")


def _build_gemini_client() -> genai.Client:
    return genai.Client(api_key=_get_gemini_api_key())


# ─── Prompt ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a concept-enrichment classifier for Arcane Analytics, scoring
D&D-ecosystem concepts against Hasbro's strategic taxonomies. Your job
is taxonomy tagging, not strategy — apply the definitions literally.

PORTFOLIO TIER (pick exactly one):
  Grow      — concepts aligned with Hasbro's Grow Brand tier
              (MAGIC: THE GATHERING, DUNGEONS & DRAGONS, MONOPOLY, NERF,
              TRANSFORMERS, PLAY-DOH). Core D&D SRD + official 5e/2024
              content + flagship monsters/classes/spells belong here.
              Third-party content adjacent to official D&D also Grow
              when it drives engagement with the D&D brand.
  Optimize  — concepts tied to mature, still-profitable formats with
              stable demand but limited growth trajectory. Examples in
              this library would be legacy editions (AD&D, 2e), niche
              supplements, long-tail SKUs without active marketing pull.
  Reinvent  — concepts tied to areas Hasbro wants to transform or exit:
              channels/formats being restructured, competitor systems
              Hasbro doesn't publish (Pathfinder, Call of Cthulhu,
              generic indie TTRPGs), orphaned third-party product
              lines. Also: AI-generated content categories.

GEM² STRUCTURAL DIMENSIONS (independent booleans — a concept can hit
zero, one, two, three, or all four):
  gamified                — plays like a game with win states, scoring,
                            or structured competition.
  entertainment_driven    — primarily consumed as spectator entertainment
                            (actual-play streams, lore shows, adaptations).
  multi_purchase          — users buy more than once (supplements,
                            expansions, miniatures, variant art).
  multi_generational      — meaningful appeal across age cohorts (13+
                            through adult), not kid-only.

CONFIDENCE:
  Score 0.0–1.0 for both portfolio_tier_confidence and gem2_confidence.
  Use 0.9+ only when the tier/flags are unambiguous. Use 0.5–0.7 when
  you're inferring from category + canonical_source without hard
  evidence. Never below 0.3 — if it's that uncertain, pick the safer
  default (Grow for official-adjacent, Optimize otherwise) and note
  the uncertainty in `reasoning`.

REASONING: one sentence, under 25 words. Cite the field that drove
your decision (category, canonical_source, canonical_parent).

Return ONE object per input concept in the same order as received.
"""


# Schema for structured output. Gemini validates against this.
CONCEPT_ITEM_SCHEMA = genai_types.Schema(
    type=genai_types.Type.OBJECT,
    required=[
        "concept_name",
        "portfolio_tier",
        "portfolio_tier_confidence",
        "gem2_gamified",
        "gem2_entertainment_driven",
        "gem2_multi_purchase",
        "gem2_multi_generational",
        "gem2_confidence",
        "reasoning",
    ],
    properties={
        "concept_name": genai_types.Schema(type=genai_types.Type.STRING),
        "portfolio_tier": genai_types.Schema(
            type=genai_types.Type.STRING,
            enum=["Grow", "Optimize", "Reinvent"],
        ),
        "portfolio_tier_confidence": genai_types.Schema(
            type=genai_types.Type.NUMBER
        ),
        "gem2_gamified": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "gem2_entertainment_driven": genai_types.Schema(
            type=genai_types.Type.BOOLEAN
        ),
        "gem2_multi_purchase": genai_types.Schema(type=genai_types.Type.BOOLEAN),
        "gem2_multi_generational": genai_types.Schema(
            type=genai_types.Type.BOOLEAN
        ),
        "gem2_confidence": genai_types.Schema(type=genai_types.Type.NUMBER),
        "reasoning": genai_types.Schema(type=genai_types.Type.STRING),
    },
)

BATCH_SCHEMA = genai_types.Schema(
    type=genai_types.Type.ARRAY,
    items=CONCEPT_ITEM_SCHEMA,
)


def _build_user_prompt(batch: list[dict]) -> str:
    """Format a batch of concepts as the user prompt."""
    lines = [
        "Classify each concept below. Return a JSON array in the same order.",
        "",
    ]
    for c in batch:
        lines.append(f"CONCEPT: {c['concept_name']}")
        lines.append(f"  category: {c.get('category') or '(none)'}")
        if c.get("canonical_parent"):
            lines.append(f"  canonical_parent: {c['canonical_parent']}")
        if c.get("canonical_source"):
            lines.append(f"  canonical_source: {c['canonical_source']}")
        if c.get("publisher"):
            lines.append(f"  publisher: {c['publisher']}")
        if c.get("ruleset"):
            lines.append(f"  ruleset: {c['ruleset']}")
        if c.get("origin"):
            lines.append(f"  origin: {c['origin']}")
        lines.append("")
    return "\n".join(lines)


# ─── BigQuery load ───────────────────────────────────────────────────────

def load_concepts(
    bq: bigquery.Client,
    limit: int | None,
    sample: bool,
    categories: list[str] | None,
) -> list[dict]:
    where = ["is_active = TRUE"]
    if categories:
        cat_list = ", ".join(f"'{c}'" for c in categories)
        where.append(f"category IN ({cat_list})")
    order = (
        "FARM_FINGERPRINT(CONCAT(concept_name, IFNULL(category, '')))"
        if sample
        else "concept_name"
    )
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
        SELECT
            concept_name, category, canonical_parent, canonical_source,
            publisher, ruleset, origin
        FROM `{CONCEPT_LIBRARY_TABLE}`
        WHERE {" AND ".join(where)}
        ORDER BY {order}
        {limit_clause}
    """
    log.info("Loading concepts from BigQuery...")
    return [dict(r) for r in bq.query(query).result()]


# ─── Firestore helpers ──────────────────────────────────────────────────

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def make_slug(concept_name: str, category: str | None) -> str:
    """Deterministic Firestore doc ID. Collisions across categories are
    rare but we include category in the slug to be safe."""
    parts = [concept_name.lower(), (category or "none").lower()]
    return "__".join(_SLUG_RE.sub("_", p).strip("_") for p in parts)[:150]


def load_existing_slugs(db: firestore.Client) -> set[str]:
    coll = (
        db.collection(FRAMES_COLLECTION)
        .document(FRAME_ID)
        .collection(MAPPINGS_SUBCOLLECTION)
    )
    slugs = {doc.id for doc in coll.list_documents()}
    log.info("Firestore already has %d mapping docs", len(slugs))
    return slugs


# ─── Gemini call ────────────────────────────────────────────────────────

@dataclass
class UsageStats:
    prompt_tokens: int = 0
    response_tokens: int = 0
    total_tokens: int = 0
    batches: int = 0
    errors: int = 0

    def add_response(self, resp: Any) -> None:
        try:
            meta = resp.usage_metadata
            self.prompt_tokens += getattr(meta, "prompt_token_count", 0) or 0
            self.response_tokens += (
                getattr(meta, "candidates_token_count", 0) or 0
            )
            self.total_tokens += getattr(meta, "total_token_count", 0) or 0
        except Exception:
            pass
        self.batches += 1


def classify_batch(
    client: genai.Client,
    batch: list[dict],
    usage: UsageStats,
) -> list[dict]:
    """Classify one batch. Returns list of dicts keyed by Gemini output
    (same order as input). Raises on terminal failure."""
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=_build_user_prompt(batch),
                config=genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=BATCH_SCHEMA,
                    temperature=0.2,
                ),
            )
            usage.add_response(resp)
            raw = resp.text or "[]"
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                raise ValueError(f"Gemini returned non-list: {type(parsed)}")
            return parsed
        except (gexc.ResourceExhausted, gexc.ServiceUnavailable,
                gexc.DeadlineExceeded) as e:
            if attempt >= RETRY_LIMIT:
                usage.errors += 1
                raise
            wait = RETRY_BACKOFF_SEC * attempt
            log.warning(
                "Gemini transient error (%s); retrying in %.1fs (attempt %d/%d)",
                type(e).__name__, wait, attempt, RETRY_LIMIT,
            )
            time.sleep(wait)
        except Exception as e:
            usage.errors += 1
            log.error("Gemini batch failed: %s", e)
            raise
    return []  # unreachable


# ─── Result persistence ─────────────────────────────────────────────────

def merge_result_with_input(
    concept: dict, result: dict, slug: str
) -> dict:
    """Shape a Firestore doc from the Gemini result + input metadata."""
    return {
        "concept_name": concept["concept_name"],
        "concept_slug": slug,
        "category": concept.get("category"),
        "canonical_parent": concept.get("canonical_parent"),
        "canonical_source": concept.get("canonical_source"),
        "publisher": concept.get("publisher"),
        "portfolio_tier": result["portfolio_tier"],
        "portfolio_tier_confidence": float(
            result["portfolio_tier_confidence"]
        ),
        "gem2_gamified": bool(result["gem2_gamified"]),
        "gem2_entertainment_driven": bool(
            result["gem2_entertainment_driven"]
        ),
        "gem2_multi_purchase": bool(result["gem2_multi_purchase"]),
        "gem2_multi_generational": bool(result["gem2_multi_generational"]),
        "gem2_confidence": float(result["gem2_confidence"]),
        "reasoning": result["reasoning"],
        "model": GEMINI_MODEL,
        "enriched_at": firestore.SERVER_TIMESTAMP,
    }


def write_batch(
    db: firestore.Client, docs: list[tuple[str, dict]]
) -> None:
    coll = (
        db.collection(FRAMES_COLLECTION)
        .document(FRAME_ID)
        .collection(MAPPINGS_SUBCOLLECTION)
    )
    batch = db.batch()
    for slug, doc in docs:
        batch.set(coll.document(slug), doc, merge=True)
    batch.commit()


# ─── Cost estimation ────────────────────────────────────────────────────

# Gemini 2.5 Flash pricing (as of 2026 Q1): input $0.30/1M tokens, output
# $2.50/1M tokens. If rates change, update here.
PRICE_INPUT_PER_1M = 0.30
PRICE_OUTPUT_PER_1M = 2.50


def estimate_cost_usd(usage: UsageStats) -> float:
    return (
        usage.prompt_tokens / 1_000_000 * PRICE_INPUT_PER_1M
        + usage.response_tokens / 1_000_000 * PRICE_OUTPUT_PER_1M
    )


# ─── Main ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich concept library with Hasbro-frame tags."
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--categories",
        type=str,
        default=None,
        help="Comma-separated list of category names to restrict to.",
    )
    args = parser.parse_args()

    categories = (
        [c.strip() for c in args.categories.split(",") if c.strip()]
        if args.categories
        else None
    )

    bq = bigquery.Client(project=PROJECT_ID)
    db = firestore.Client(project=PROJECT_ID)

    concepts = load_concepts(
        bq, limit=args.limit, sample=args.sample, categories=categories
    )
    log.info("Loaded %d concepts from concept_library", len(concepts))

    if not args.force:
        existing = load_existing_slugs(db)
        before = len(concepts)
        concepts = [
            c for c in concepts
            if make_slug(c["concept_name"], c.get("category")) not in existing
        ]
        log.info(
            "Skipping %d already-enriched concepts; %d remain",
            before - len(concepts), len(concepts),
        )

    if not concepts:
        log.info("Nothing to do. Exiting.")
        return

    client = _build_gemini_client()
    usage = UsageStats()
    results_written = 0
    results_failed = 0

    for batch_start in range(0, len(concepts), args.batch_size):
        batch = concepts[batch_start : batch_start + args.batch_size]
        batch_idx = batch_start // args.batch_size + 1
        total_batches = (len(concepts) + args.batch_size - 1) // args.batch_size
        log.info(
            "Batch %d/%d — %d concepts", batch_idx, total_batches, len(batch)
        )
        try:
            raw_results = classify_batch(client, batch, usage)
        except Exception as e:
            log.error("Batch %d failed terminally: %s — skipping", batch_idx, e)
            results_failed += len(batch)
            continue

        # Align Gemini output to input by concept_name (defensive —
        # Gemini is supposed to preserve order but we don't trust it).
        by_name = {r["concept_name"]: r for r in raw_results if "concept_name" in r}
        docs_to_write: list[tuple[str, dict]] = []
        for c in batch:
            result = by_name.get(c["concept_name"])
            if result is None:
                log.warning(
                    "Gemini did not return result for '%s' — skipping",
                    c["concept_name"],
                )
                results_failed += 1
                continue
            slug = make_slug(c["concept_name"], c.get("category"))
            try:
                doc = merge_result_with_input(c, result, slug)
            except Exception as e:
                log.warning(
                    "Shape failure for '%s': %s — skipping",
                    c["concept_name"], e,
                )
                results_failed += 1
                continue
            docs_to_write.append((slug, doc))

        if args.dry_run:
            for slug, doc in docs_to_write:
                # Drop the sentinel for readable print
                printable = {k: v for k, v in doc.items() if k != "enriched_at"}
                print(json.dumps(printable, ensure_ascii=False))
            results_written += len(docs_to_write)
        else:
            try:
                write_batch(db, docs_to_write)
                results_written += len(docs_to_write)
            except Exception as e:
                log.error(
                    "Firestore batch write failed (batch %d): %s",
                    batch_idx, e,
                )
                results_failed += len(docs_to_write)

    # Summary
    log.info("=" * 50)
    log.info("Done.")
    log.info("  concepts scored: %d", results_written)
    log.info("  failures:        %d", results_failed)
    log.info("  batches:         %d", usage.batches)
    log.info(
        "  tokens:          %d in / %d out (%d total)",
        usage.prompt_tokens, usage.response_tokens, usage.total_tokens,
    )
    log.info("  estimated cost:  $%.4f", estimate_cost_usd(usage))
    if args.dry_run:
        log.info("  DRY-RUN: no Firestore writes performed")


if __name__ == "__main__":
    main()
