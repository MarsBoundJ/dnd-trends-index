"""
Phase 1 Schema — dnd-trends-index
Creates four new tables and alters concept_library.
Run once: python scripts/phase1_schema.py
"""

from google.cloud import bigquery

PROJECT = "dnd-trends-index"
DATASET = "dnd_trends_categorized"

client = bigquery.Client(project=PROJECT)
ds = f"{PROJECT}.{DATASET}"


def run_ddl(label: str, sql: str):
    print(f"  >> {label}... ", end="", flush=True)
    job = client.query(sql)
    job.result()
    print("done")


# ─────────────────────────────────────────────
# 1. insight_library
# ─────────────────────────────────────────────
run_ddl(
    "CREATE insight_library",
    f"""
    CREATE TABLE IF NOT EXISTS `{ds}.insight_library` (
        term              STRING  NOT NULL,
        canonical_name    STRING,
        signal_category   STRING,   -- new_player_acquisition | product_dissatisfaction |
                                    -- media_crossover | ip_crossover | competitor_comparison | other
        search_value      STRING,   -- numeric string or "Breakout"
        is_breakout       BOOL,
        source_seed       STRING,
        run_id            STRING,
        is_active         BOOL,
        notes             STRING,
        added_at          TIMESTAMP NOT NULL
    )
    OPTIONS (description = 'Non-concept D&D-ecosystem signals discovered via Google Trends related queries.')
    """,
)

# ─────────────────────────────────────────────
# 2. competitor_concepts  (full mirror of concept_library + competitor fields)
# ─────────────────────────────────────────────
run_ddl(
    "CREATE competitor_concepts",
    f"""
    CREATE TABLE IF NOT EXISTS `{ds}.competitor_concepts` (
        -- ── mirrored from concept_library ──────────────────────────────────
        concept_name        STRING,
        category            STRING,
        source_book         STRING,
        ruleset             STRING,
        tags                ARRAY<STRING>,
        last_processed_at   TIMESTAMP,
        is_active           BOOL,
        canonical_parent    STRING,
        sub_tags            ARRAY<STRING>,
        persona_target      STRING,
        publisher           STRING,
        -- ── competitor-specific fields ─────────────────────────────────────
        game_system         STRING,   -- pathfinder | call_of_cthulhu | blades_in_the_dark | etc.
        native_category     STRING,   -- that game's own term (e.g. "Ancestry", "Keeper", "Crew")
        dnd_analog          STRING,   -- closest D&D category equivalent; nullable
        -- ── provenance ────────────────────────────────────────────────────
        source_seed         STRING,
        search_value        STRING,
        is_breakout         BOOL,
        run_id              STRING,
        added_at            TIMESTAMP
    )
    OPTIONS (description = 'Competitor TTRPG concepts discovered via Google Trends. Mirrors concept_library schema with game_system / native_category / dnd_analog layers for apples-to-apples comparison.')
    """,
)

# ─────────────────────────────────────────────
# 3. review_queue
# ─────────────────────────────────────────────
run_ddl(
    "CREATE review_queue",
    f"""
    CREATE TABLE IF NOT EXISTS `{ds}.review_queue` (
        review_id               STRING  NOT NULL,   -- UUID assigned at insert time
        term                    STRING  NOT NULL,   -- raw search query from Google Trends
        -- ── Gemini suggestions ─────────────────────────────────────────────
        suggested_bucket        STRING,             -- concept | insight | competitor | noise
        suggested_signal_category STRING,           -- for insight bucket
        suggested_native_category STRING,           -- for competitor bucket
        suggested_dnd_analog    STRING,             -- for competitor bucket; nullable
        suggested_game_system   STRING,             -- for competitor bucket
        suggested_variant_of    STRING,             -- for concept bucket; existing canonical name
        is_new_concept          BOOL,               -- TRUE if Gemini says this is a brand-new concept
        confidence              FLOAT64,            -- 0.0–1.0; items below threshold land here
        reasoning               STRING,             -- Gemini's one-line explanation
        -- ── source context ────────────────────────────────────────────────
        search_value            STRING,             -- numeric or "Breakout"
        is_breakout             BOOL,
        source_seed             STRING,
        run_id                  STRING,
        -- ── review workflow ───────────────────────────────────────────────
        status                  STRING  NOT NULL,   -- pending | approved | rejected
        reviewer_notes          STRING,
        reviewed_at             TIMESTAMP,
        added_at                TIMESTAMP NOT NULL
    )
    OPTIONS (description = 'Holding queue for low-confidence Gemini classifications awaiting human review in the Library Clerk.')
    """,
)

# ─────────────────────────────────────────────
# 4. seeds
# ─────────────────────────────────────────────
run_ddl(
    "CREATE seeds",
    f"""
    CREATE TABLE IF NOT EXISTS `{ds}.seeds` (
        term            STRING  NOT NULL,
        added_by        STRING  NOT NULL,   -- auto | manual
        source_concept  STRING,             -- concept_name that triggered auto-add; null for manual
        is_active       BOOL    NOT NULL,
        added_at        TIMESTAMP NOT NULL,
        last_used_at    TIMESTAMP,
        notes           STRING
    )
    OPTIONS (description = 'Managed seed list for the discover-related-queries Cloud Function. Replaces hardcoded DEFAULT_SEEDS. Auto-populated when concepts are approved; supports manual injection.')
    """,
)

# ─────────────────────────────────────────────
# 5. ALTER concept_library  — add is_breakout, seed_promoted
# ─────────────────────────────────────────────
run_ddl(
    "ALTER concept_library — add is_breakout",
    f"""
    ALTER TABLE `{ds}.concept_library`
    ADD COLUMN IF NOT EXISTS is_breakout   BOOL
                             OPTIONS (description = 'TRUE if this concept was first observed as a Google Trends Breakout query (>5000% growth).')
    """,
)

run_ddl(
    "ALTER concept_library — add seed_promoted",
    f"""
    ALTER TABLE `{ds}.concept_library`
    ADD COLUMN IF NOT EXISTS seed_promoted BOOL
                             OPTIONS (description = 'TRUE once this concept has been added to the seeds table for future scraping.')
    """,
)

# ─────────────────────────────────────────────
# 6. Pre-populate seeds with current hardcoded list
# ─────────────────────────────────────────────
INITIAL_SEEDS = [
    ("dungeons and dragons", "The flagship D&D seed"),
    ("dnd 5e",               "Core edition seed"),
    ("dnd beyond",           "WotC's official digital toolset"),
    ("pathfinder 2e",        "Primary competitor; routes competitor_concepts"),
    ("ttrpg",                "Genre-level seed"),
    ("dnd 2024",             "2024 revised ruleset seed"),
    ("one dnd",              "Legacy pre-release name; currently 0 results — kept for monitoring"),
]

rows = [
    {
        "term":           term,
        "added_by":       "manual",
        "source_concept": None,
        "is_active":      True,
        "added_at":       "2026-03-29T00:00:00Z",
        "last_used_at":   None,
        "notes":          note,
    }
    for term, note in INITIAL_SEEDS
]

print("  >> Pre-populating seeds table... ", end="", flush=True)
errors = client.insert_rows_json(f"{ds}.seeds", rows)
if errors:
    print(f"ERRORS: {errors}")
else:
    print("done")

print("\n[OK] Phase 1 schema complete.")
