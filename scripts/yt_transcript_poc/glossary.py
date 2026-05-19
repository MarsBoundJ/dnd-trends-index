"""
YT Transcript POC — glossary loader (the closed, curated term sets).

Pulls (read-only) from BQ and PARTITIONS per the red-team-hardened design
(project_yt_transcript_poc.md):

  List A — rare / fantasy proper nouns (Aarakocra, Mordenkainen, Bigby,
           "Path of the World Tree"): safe to match directly / phonetically.
  List B — D&D terms that are ALSO common English words (Shield, Wish,
           Bane, Bless, Hide, Rage, Command, Slow, Fear, Light, Hold...):
           NEVER bare-match — require a trigger-word gate (see extract.py),
           else false-positive rate approaches 100% on stance-heavy speech.

Sources: game_registry.subclasses (canonical + aliases), game_registry.
vocabulary_lexicon (jargon), dnd_trends_categorized.concept_library (~11k
wide net). Cached to GLOSSARY_CACHE; refresh with --refresh.

Uses the `bq` CLI (proven robust this session) — no python BQ client dep.
"""

from __future__ import annotations

import json

from google.cloud import bigquery

try:
    from . import config
except ImportError:  # pragma: no cover
    import config  # type: ignore

_BQ = None

# D&D terms that collide with ordinary English. Hand-curated for the POC
# (the partition heuristic also catches single common words, but these
# are the load-bearing collisions called out in the red-team).
COMMON_ENGLISH_COLLISIONS = {
    "shield", "wish", "bane", "bless", "hide", "rage", "command", "dash",
    "slow", "fear", "hold", "light", "darkness", "guidance", "sleep",
    "haste", "bond", "guard", "hunter", "fighter", "ranger", "thief",
    "barbarian", "champion", "grave", "life", "war", "light", "knowledge",
    "nature", "tempest", "death", "order", "peace", "twilight", "forge",
    "blade", "shadow", "dawn", "moon", "land", "sea", "stars", "open hand",
    "berserker", "assassin", "soul knife", "hunter", "beast master",
    "true strike", "find", "aid", "heal", "longstrider", "enlarge",
}

TRIGGER_WORDS = [
    "cast", "casting", "casts", "spell", "use", "uses", "using", "take",
    "takes", "taking", "pick", "picks", "build", "subclass", "feat",
    "the ", "a ", "an ", "your ", "this ", "level", "concentration",
    "bonus action", "reaction", "action",
]


def _bq_csv(sql: str) -> list[dict]:
    """Read-only BQ via the Python client (cross-platform; the `bq` CLI
    is a .CMD wrapper Python subprocess can't resolve on Windows). Uses
    the ambient ADC creds already configured in this environment."""
    global _BQ
    if _BQ is None:
        _BQ = bigquery.Client(project=config.BQ_PROJECT)
    return [dict(row) for row in _BQ.query(sql).result()]


def _is_list_b(term: str) -> bool:
    t = term.strip().lower()
    if not t:
        return True
    if t in COMMON_ENGLISH_COLLISIONS:
        return True
    # single short common-looking word -> treat as collision-risk (List B);
    # multi-word or long/rare tokens -> List A.
    if " " not in t and len(t) <= 6:
        return True
    return False


def build(refresh: bool = False) -> dict:
    config.ensure_dirs()
    if config.GLOSSARY_CACHE.exists() and not refresh:
        return json.loads(config.GLOSSARY_CACHE.read_text(encoding="utf-8"))

    terms: dict[str, dict] = {}  # lower term -> {canonical, kind}

    def add(term: str, canonical: str, kind: str):
        t = (term or "").strip()
        if len(t) < 3:
            return
        terms.setdefault(t.lower(), {"canonical": canonical or t, "kind": kind})

    # 1) subclasses: canonical + aliases (purpose-built this session)
    for row in _bq_csv(
        "SELECT canonical_name, alias FROM "
        "`dnd-trends-index.game_registry.subclasses`, "
        "UNNEST(aliases) AS alias"
    ):
        add(row["canonical_name"], row["canonical_name"], "subclass")
        add(row["alias"], row["canonical_name"], "subclass")

    # 2) jargon lexicon (DPR/eHP/nova/Striker...)
    for row in _bq_csv(
        "SELECT term FROM `dnd-trends-index.game_registry.vocabulary_lexicon`"
    ):
        add(row["term"], row["term"], "jargon")

    # 3) concept_library wide net (~11k)
    try:
        for row in _bq_csv(
            "SELECT DISTINCT concept_name FROM "
            "`dnd-trends-index.dnd_trends_categorized.concept_library` "
            "WHERE concept_name IS NOT NULL"
        ):
            add(row["concept_name"], row["concept_name"], "concept")
    except Exception as e:  # wide net optional for POC
        print(f"[glossary] concept_library skipped: {e}")

    list_a, list_b = {}, {}
    for t, meta in terms.items():
        (list_b if _is_list_b(t) else list_a)[t] = meta

    gloss = {
        "built_terms": len(terms),
        "list_a_rare": list_a,            # direct/phonetic-safe
        "list_b_collisions": list_b,      # trigger-gated only
        "trigger_words": TRIGGER_WORDS,
    }
    config.GLOSSARY_CACHE.write_text(
        json.dumps(gloss, ensure_ascii=False), encoding="utf-8")
    print(f"[glossary] terms={len(terms)} listA={len(list_a)} "
          f"listB={len(list_b)} -> {config.GLOSSARY_CACHE}")
    return gloss


if __name__ == "__main__":
    import sys
    build(refresh="--refresh" in sys.argv)
