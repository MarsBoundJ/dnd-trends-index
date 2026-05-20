"""
YT Transcript POC — glossary v2 (sim-feed precision).

Iteration 1 used `concept_library` as a flat ~11k wide net → flooded the
speech-only pass with ambient vocabulary ("Action / Armor Class / Buff /
Adventure / Background / Adept ..."), and generic tokens ("Reaction",
"Subclasses", "Spell") leaked into primary. The fix tiers the source by
the categories concept_library already carries.

Tiers (sim-feed first):
  Tier-1   precision-trusted:
           - game_registry.subclasses canonical+aliases (purpose-built)
           - concept_library WHERE category IN (Spell, Feat, MagicItem,
             Monster, Equipment, Background, Species & Lineage,
             Invocations and Pacts)
           - concept_library WHERE category='Mechanic'  AND multi-word
             (multi-word filter is the precision lever — excludes bare
             "Action / Adventure / Background" while keeping "Bonus
             Action / Action Economy / passive perception / fall damage")
  Tier-2   jargon: vocabulary_lexicon (DPR/eHP/nova/...).
  DROPPED  Location / NPC / Faction / Villain / Deity / Influencer /
           Convention / AI Tool / Art / Third Party / Youtube / Build /
           UA Content / Other / Subclass (use registry instead). Lore
           and community concepts are noise for mechanics extraction.

Also: hardcoded AMBIENT_STOPLIST for tokens that leak no matter the
tier ("reaction" / "spell" / "level" / "class" / "subclass" alone).
"""

from __future__ import annotations

import json

from google.cloud import bigquery

try:
    from . import config
except ImportError:  # pragma: no cover
    import config  # type: ignore

_BQ = None

# Sim-feed Tier-1 categories from concept_library.
TIER1_CONCEPT_CATEGORIES = (
    "Spell", "Feat", "MagicItem", "Monster", "Equipment",
    "Background", "Species & Lineage", "Invocations and Pacts",
)
MECHANIC_CATEGORY = "Mechanic"  # multi-word filter applied below

# The 13 5e core classes — central sim/optimizer entities, not in
# concept_library's Tier-1 categories. Tiny curated inline list so
# primary doesn't show `0` on a video that's clearly "about the Wizard".
CORE_CLASSES = (
    "Barbarian", "Bard", "Cleric", "Druid", "Fighter", "Monk", "Paladin",
    "Ranger", "Rogue", "Sorcerer", "Warlock", "Wizard", "Artificer",
)

# Ambient tokens that leak as "entities" regardless of source — generic
# nouns that match within their own trigger context. Hardcoded denylist;
# we never emit these as canonical entities. Compounds like "Bonus
# Action" / "Action Surge" / "Attack Action" still match — this only
# blocks the BARE single-word form.
AMBIENT_STOPLIST = {
    "reaction", "subclass", "subclasses", "spell", "spells", "level",
    "levels", "class", "classes", "feat", "feats", "creature", "creatures",
    "concept", "build", "builds", "character sheet", "campaign",
    "creation", "die", "dice", "stat", "stats", "score", "scores",
    "ability", "abilities", "round", "rounds", "turn", "turns",
    "encounter", "encounters", "rule", "rules", "feature", "features",
    "case", "cases", "moment", "use", "uses", "way", "ways",
}

TRIGGER_WORDS = [
    "cast", "casting", "casts", "use", "uses", "using", "take", "takes",
    "pick", "picks", "build", "subclass", "feat", "level",
    "concentration", "bonus action", "reaction", "action",
]


def _bq(sql: str) -> list[dict]:
    global _BQ
    if _BQ is None:
        _BQ = bigquery.Client(project=config.BQ_PROJECT)
    return [dict(r) for r in _BQ.query(sql).result()]


def _split_slash(term: str) -> list[str]:
    """ "WoF / Wall of Force" -> ["WoF", "Wall of Force"] (both alias the
    same canonical). Common in concept_library."""
    if not term:
        return []
    if " / " in term:
        return [p.strip() for p in term.split(" / ") if p.strip()]
    return [term.strip()]


def _is_multi_word(t: str) -> bool:
    return " " in t.strip()


def _is_list_b(term: str) -> bool:
    """Multi-word -> List A (safe). Short single tokens -> List B
    (collision-risk, trigger-gated only)."""
    t = term.strip().lower()
    if not t:
        return True
    if " " in t:
        return False
    return len(t) <= 6


def build(refresh: bool = False) -> dict:
    config.ensure_dirs()
    if config.GLOSSARY_CACHE.exists() and not refresh:
        return json.loads(config.GLOSSARY_CACHE.read_text(encoding="utf-8"))

    terms: dict[str, dict] = {}     # lower -> {canonical, kind, tier}
    tier1_canon: set[str] = set()   # for comparative + rule-ambiguity passes

    def add(term: str, canonical: str, kind: str, tier: str):
        t = (term or "").strip()
        if len(t) < 3:
            return
        if t.lower() in AMBIENT_STOPLIST:
            return
        terms.setdefault(t.lower(), {
            "canonical": canonical or t, "kind": kind, "tier": tier,
        })
        if tier == "tier1" and canonical:
            tier1_canon.add(canonical)

    # 1) Registry subclasses (canonical + aliases) — purpose-built.
    for row in _bq(
        "SELECT canonical_name, alias FROM "
        "`dnd-trends-index.game_registry.subclasses`, "
        "UNNEST(aliases) AS alias"
    ):
        for piece in _split_slash(row["alias"]):
            add(piece, row["canonical_name"], "subclass", "tier1")
        add(row["canonical_name"], row["canonical_name"], "subclass", "tier1")

    # 2) concept_library Tier-1 categories.
    cats_sql = ", ".join(f"'{c}'" for c in TIER1_CONCEPT_CATEGORIES)
    for row in _bq(
        f"SELECT DISTINCT concept_name, category FROM "
        f"`dnd-trends-index.dnd_trends_categorized.concept_library` "
        f"WHERE concept_name IS NOT NULL AND category IN ({cats_sql})"
    ):
        for piece in _split_slash(row["concept_name"]):
            add(piece, row["concept_name"], row["category"].lower(), "tier1")

    # 3) Mechanic: multi-word ONLY (precision filter; bare single-word
    #    Mechanic terms are mostly ambient — those that matter live in
    #    vocabulary_lexicon).
    for row in _bq(
        "SELECT DISTINCT concept_name FROM "
        "`dnd-trends-index.dnd_trends_categorized.concept_library` "
        f"WHERE category = '{MECHANIC_CATEGORY}' AND concept_name IS NOT NULL"
    ):
        for piece in _split_slash(row["concept_name"]):
            if _is_multi_word(piece):
                add(piece, row["concept_name"], "mechanic", "tier1")

    # 4) Core classes — curated, central sim/optimizer entities.
    for c in CORE_CLASSES:
        add(c, c, "class", "tier1")

    # 5) Jargon (Tier-2).
    for row in _bq(
        "SELECT term FROM `dnd-trends-index.game_registry.vocabulary_lexicon`"
    ):
        add(row["term"], row["term"], "jargon", "tier2")

    list_a, list_b = {}, {}
    for t, meta in terms.items():
        (list_b if _is_list_b(t) else list_a)[t] = meta
    # Core classes are proper-noun + sim-central — force List A regardless
    # of their short length, so "Wizard Flagship Tier 3" yields `Wizard`
    # without needing an adjacent trigger word in the title.
    for c in CORE_CLASSES:
        cl = c.lower()
        if cl in list_b:
            list_a[cl] = list_b.pop(cl)

    gloss = {
        "built_terms": len(terms),
        "tier1_canonicals": sorted(tier1_canon),
        "list_a_rare": list_a,
        "list_b_collisions": list_b,
        "trigger_words": TRIGGER_WORDS,
        "ambient_stoplist": sorted(AMBIENT_STOPLIST),
    }
    config.GLOSSARY_CACHE.write_text(
        json.dumps(gloss, ensure_ascii=False), encoding="utf-8")
    print(f"[glossary] v2 built: terms={len(terms)} "
          f"listA={len(list_a)} listB={len(list_b)} "
          f"tier1_canon={len(tier1_canon)} -> {config.GLOSSARY_CACHE}")
    return gloss


if __name__ == "__main__":
    import sys
    build(refresh="--refresh" in sys.argv)
