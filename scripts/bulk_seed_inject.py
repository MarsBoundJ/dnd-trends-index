"""
bulk_seed_inject.py
-------------------
Injects high-value concepts from concept_library into the seeds table
for the Google Trends related_queries_discovery pipeline.

Usage:
    python scripts/bulk_seed_inject.py [--dry-run] [--limit 250]

Requires: google-cloud-bigquery (pip install google-cloud-bigquery)
Auth:     GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default login
"""

import argparse
import datetime
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DS = f"{PROJECT_ID}.dnd_trends_categorized"

# High-value categories for seeding — these produce the best related queries
# because they are specific, searchable D&D terms with real Google Trends volume.
HIGH_VALUE_CATEGORIES = [
    "Class",         # ~50 — core game mechanics
    "Subclass",      # ~460 — very specific, great for discovery
    "Spell",         # ~600 — named entities with high search volume
    "Monster",       # ~2800 — largest category, sample the top ones
    "Feat",          # ~217 — specific rules terms
    "Species & Lineage",  # ~104 — races/species
    "Background",    # ~90 — character backgrounds
    "Deity",         # ~157 — named entities
    "Location",      # ~1100 — named D&D places
    "TTRPG System",  # ~26 — competitors (Daggerheart, PF2e, etc.)
    "VTT Platform",  # ~4 — digital platforms
    "Influencer",    # ~132 — content creators
]

# For very large categories (Monster, Location), only take a sample
# to avoid flooding the seeds table with low-signal terms
CATEGORY_LIMITS = {
    "Monster": 60,
    "Location": 40,
    "MagicItem": 30,
    "Spell": 50,
    "Subclass": 40,
    "Influencer": 20,
}

DEFAULT_LIMIT = 250  # Total cap across all categories


def get_existing_seeds(client):
    """Fetch all current seed terms to avoid duplicates."""
    query = f"SELECT LOWER(TRIM(term)) AS term FROM `{DS}.seeds`"
    return {row["term"] for row in client.query(query).result()}


def get_candidate_concepts(client, existing_seeds, total_limit):
    """
    Fetch concepts from high-value categories, excluding those already seeded.
    Prioritizes smaller categories first (Class, VTT, TTRPG System) then samples
    from larger categories up to their per-category limit.
    """
    candidates = []

    for category in HIGH_VALUE_CATEGORIES:
        cat_limit = CATEGORY_LIMITS.get(category, 100)
        query = f"""
            SELECT concept_name, category
            FROM `{DS}.concept_library`
            WHERE is_active = TRUE
              AND category = '{category}'
              AND LOWER(TRIM(concept_name)) NOT IN (
                  SELECT LOWER(TRIM(term)) FROM `{DS}.seeds`
              )
            ORDER BY concept_name
            LIMIT {cat_limit}
        """
        rows = list(client.query(query).result())
        for row in rows:
            term_lower = row["concept_name"].lower().strip()
            if term_lower not in existing_seeds:
                candidates.append({
                    "concept_name": row["concept_name"],
                    "category": row["category"],
                })

        if len(candidates) >= total_limit:
            break

    return candidates[:total_limit]


def build_seed_rows(candidates):
    """Convert candidate concepts into seed table rows."""
    now = datetime.datetime.utcnow().isoformat()
    return [
        {
            "term": c["concept_name"],
            "added_by": "auto",
            "source_concept": c["concept_name"],
            "is_active": True,
            "added_at": now,
            "last_used_at": None,
            "notes": f"Bulk seeded from concept_library ({c['category']})",
        }
        for c in candidates
    ]


def main():
    parser = argparse.ArgumentParser(description="Bulk inject seeds from concept_library")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be inserted without writing")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max seeds to inject")
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT_ID)

    print(f"Fetching existing seeds...")
    existing = get_existing_seeds(client)
    print(f"  {len(existing)} seeds already in table")

    print(f"Finding candidates from {len(HIGH_VALUE_CATEGORIES)} categories (limit: {args.limit})...")
    candidates = get_candidate_concepts(client, existing, args.limit)
    print(f"  {len(candidates)} new candidates found")

    if not candidates:
        print("Nothing to inject — all high-value concepts already seeded.")
        return

    # Show category breakdown
    from collections import Counter
    cat_counts = Counter(c["category"] for c in candidates)
    print("\nCategory breakdown:")
    for cat, count in cat_counts.most_common():
        print(f"  {cat}: {count}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would inject {len(candidates)} seeds. Sample:")
        for c in candidates[:20]:
            print(f"  {c['concept_name']} ({c['category']})")
        if len(candidates) > 20:
            print(f"  ... and {len(candidates) - 20} more")
        return

    rows = build_seed_rows(candidates)
    print(f"\nInserting {len(rows)} seeds...")
    errors = client.insert_rows_json(f"{DS}.seeds", rows)
    if errors:
        print(f"ERROR: {errors}")
    else:
        print(f"Successfully injected {len(rows)} seeds!")
        print(f"Total seeds now: {len(existing) + len(rows)}")


if __name__ == "__main__":
    main()
