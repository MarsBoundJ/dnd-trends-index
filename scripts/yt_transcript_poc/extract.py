"""
YT Transcript POC — deterministic extractor v2 (sim-feed precision).

Iteration 2 fixes the two precision problems iter-1 surfaced on live
Treantmonk data, AND adds two new sim-direct deterministic passes the
previous pass didn't have.

Fixes vs v1:
  1. Glossary tiered (see glossary.py): bare ambient nouns dropped,
     Mechanic-category constrained to multi-word.
  2. List-B trigger gate now requires the trigger to live OUTSIDE the
     matched term's own character span (window_before / window_after
     instead of a single window through the match) — kills the
     self-trigger leak that put "Subclasses" / "Reaction" / "Spell"
     into primary in v1.

NEW passes (Tier-1 entities only; high-precision, sim-direct):
  - comparative_pairs : two Tier-1 entities adjacent (≤60 chars apart)
    with a comparative trigger BETWEEN them ("vs / better than / worse
    than / unlike / instead of / stronger than / weaker than /
    compared to"). The "X is worse than Y" structure the cadre
    red-team flagged as the highest-value speech-only signal.
    Overlapping substring matches are collapsed to the longest span
    first (see _collapse_overlaps) so one comparison is one pair.
  - rule_ambiguity_flags : an ambiguity marker ("RAW / RAI / Crawford
    said / ask your DM / unclear / ambiguous / depends how / errata /
    sage advice") within ±80 chars of a Tier-1 entity. Directly feeds
    the spine's open combinatorial-config question (which rules are
    ambiguous in practice).
"""

from __future__ import annotations

import glob
import json
import re

try:
    from . import config, glossary as glossary_mod
except ImportError:  # pragma: no cover
    import config  # type: ignore
    import glossary as glossary_mod  # type: ignore


# NOTE: bare " like " was removed (iter-3). In spoken English it is
# three different words — preposition, discourse-filler, verb — and is
# ungateable without POS tagging the POC deliberately does not build.
# On live Treantmonk data every " like "-triggered pair was noise
# (filler "like", word-fragments). Precision >> recall (Principle #0).
# " more like " is kept: a low-frequency 2-word n-gram, far safer.
COMPARATIVE_TRIGGERS = [
    " vs ", " versus ", "better than", "worse than", "compared to",
    "compared with", "instead of", "more like", " unlike ",
    "as good as", "as bad as", "stronger than", "weaker than",
    "preferable to", "outperforms", "outclasses",
]

# NOTE: bare "raw" was removed (iter-4). It is an ordinary English word
# Treantmonk uses constantly in a NON-ruling sense ("raw damage", "raw
# numbers", "raw DPR") — even word-bounded it is a false-positive magnet.
# "rai" is kept: it is NOT an English word, so word-bounded (see the
# _wb() fix in _rule_ambiguity) it safely catches only the genuine RAI
# abbreviation. The multi-word "rules as written/intended" and "raw vs
# rai / rai vs raw" forms carry the spelled-out RAW/RAI discourse.
AMBIGUITY_MARKERS = [
    "rai", "rules as written", "rules as intended",
    "jeremy crawford", "crawford said", "crawford clarified",
    "ask your dm", "dm allows", "dm ruling", "dm's discretion",
    "dm rules", "depends how", "depends on the dm", "unclear",
    "ambiguous", "interpretation", "errata", "sage advice",
    "raw vs rai", "rai vs raw",
]


def _wb(term: str) -> re.Pattern:
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])")


def _match_list_a(text_l: str, list_a: dict) -> dict:
    """Multi-word / rare terms: direct word-boundary match — safe."""
    hits = {}
    for term, meta in list_a.items():
        m = _wb(term).search(text_l)
        if m:
            hits[meta["canonical"]] = {
                "canonical": meta["canonical"], "kind": meta["kind"],
                "tier": meta.get("tier", "tier1"),
                "matched": term, "method": "listA_wordboundary",
                "span": text_l[max(0, m.start() - 40):m.end() + 40],
            }
    return hits


def _match_list_b(text_l: str, list_b: dict, triggers: list[str]) -> dict:
    """List B (common-collision singles): match only when a trigger
    sits in the window BEFORE or AFTER the term, NEVER inside the
    term's own span (the self-trigger fix). Term and trigger must also
    differ — `subclass` cannot trigger `subclasses`."""
    hits = {}
    for term, meta in list_b.items():
        for m in _wb(term).finditer(text_l):
            window_before = text_l[max(0, m.start() - 24):m.start()]
            window_after = text_l[m.end():m.end() + 24]
            ok = False
            for tw in triggers:
                tw_l = tw.strip().lower()
                if not tw_l or tw_l == term:
                    continue  # trigger must differ from the term itself
                if tw_l in term or term in tw_l:
                    continue  # nor be a substring overlap
                if tw_l in window_before or tw_l in window_after:
                    ok = True
                    break
            if ok:
                hits[meta["canonical"]] = {
                    "canonical": meta["canonical"], "kind": meta["kind"],
                    "tier": meta.get("tier", "tier1"),
                    "matched": term, "method": "listB_trigger_gated",
                    "span": text_l[max(0, m.start() - 40):m.end() + 40],
                }
                break
    return hits


def _collapse_overlaps(
    positions: list[tuple[int, int, str]],
) -> list[tuple[int, int, str]]:
    """When several entity occurrences cover overlapping character spans
    — e.g. "leather" ⊂ "studded leather" ⊂ "studded leather armor" all
    matching the same words — keep only the longest (most specific) one.

    Without this, one sentence ("studded leather armor ... better than
    light armor") yields a cartesian product of fragment pairs: every
    {Leather|Studded Leather|Studded Leather Armor} × {Light|Light
    Armor} combination. Collapsing to the longest span per text region
    is loss-free for distinct entities (distinct non-nested entities
    never share characters) and kills the substring explosion."""
    kept: list[tuple[int, int, str]] = []
    # Longest spans first; a shorter span is dropped if it overlaps one
    # already kept (i.e. it is a fragment of a more specific match).
    for s, e, c in sorted(positions, key=lambda p: p[1] - p[0], reverse=True):
        if any(s < ke and ks < e for ks, ke, _ in kept):
            continue
        kept.append((s, e, c))
    kept.sort()
    return kept


def _comparative_pairs(text_l: str, tier1_canonicals: list[str]) -> list[dict]:
    """Find Tier-1 entity pairs (X, Y) with a comparative trigger
    BETWEEN them in the text. Only Tier-1 endpoints to keep precision
    high. Caps results to avoid quadratic blowup on long transcripts."""
    # First, locate every Tier-1 entity occurrence and its span. Use
    # lowercase canonical because text_l is lowercase.
    positions: list[tuple[int, int, str]] = []  # (start, end, canonical)
    for canon in tier1_canonicals:
        for m in _wb(canon.lower()).finditer(text_l):
            positions.append((m.start(), m.end(), canon))
    # Collapse nested/overlapping matches to the longest span — kills the
    # substring-fragment cartesian explosion before pairing.
    positions = _collapse_overlaps(positions)

    pairs: list[dict] = []
    seen: set[tuple[str, str, str]] = set()  # case-insensitive
    for i, (sa, ea, ca) in enumerate(positions):
        for sb, eb, cb in positions[i + 1:]:
            if sb - ea > 60:  # too far apart -> not a comparison
                break
            cal, cbl = ca.lower(), cb.lower()
            if cal == cbl or cal in cbl or cbl in cal:
                # Self-pair via case variant, OR a term compared to a
                # sub/superstring of itself ("Light Armor" vs "Armor",
                # "Resistance" vs "damage resistance"). _collapse_overlaps
                # only merges same-position matches; this catches the
                # same substring noise when the two occurrences sit at
                # different positions in the sentence.
                continue
            between = text_l[ea:sb]
            trig_hit = next((t.strip() for t in COMPARATIVE_TRIGGERS
                             if t.strip() in between), None)
            if not trig_hit:
                continue
            # Unordered dedup: (a,b) == (b,a) for same trigger.
            key = (*sorted([ca.lower(), cb.lower()]), trig_hit)
            if key in seen:
                continue
            seen.add(key)
            pairs.append({
                "a": ca, "b": cb, "trigger": trig_hit,
                "span": text_l[max(0, sa - 30):eb + 30],
            })
            if len(pairs) >= 50:
                return pairs
    return pairs


def _rule_ambiguity(text_l: str, tier1_canonicals: list[str]) -> list[dict]:
    """An ambiguity marker within ±80 chars of a Tier-1 entity. Sim
    config-layer signal (which rules the community treats as unclear)."""
    flags: list[dict] = []
    # Locate marker occurrences first (markers are scarce; entities are many).
    # _wb() word-boundaries the match — iter-4 fix: bare re.finditer let
    # "rai" match inside traits/trail/raised and "raw" inside crawl,
    # which made 96% of rule-ambiguity flags substring false positives.
    marker_hits: list[tuple[int, int, str]] = []
    for marker in AMBIGUITY_MARKERS:
        for m in _wb(marker).finditer(text_l):
            marker_hits.append((m.start(), m.end(), marker))
    if not marker_hits:
        return flags

    for canon in tier1_canonicals:
        for m in _wb(canon.lower()).finditer(text_l):
            cs, ce = m.start(), m.end()
            for ms, me, marker in marker_hits:
                if abs(ms - ce) <= 80 or abs(cs - me) <= 80:
                    flags.append({
                        "feature": canon, "marker": marker,
                        "span": text_l[max(0, min(cs, ms) - 30):
                                       max(ce, me) + 30],
                    })
                    break  # one flag per (feature, location)
            if len(flags) >= 80:
                return flags
    return flags


def extract_one(norm: dict, gloss: dict) -> dict:
    list_a = gloss["list_a_rare"]
    list_b = gloss["list_b_collisions"]
    trig = gloss["trigger_words"]
    tier1 = gloss.get("tier1_canonicals", [])

    clean_l = (norm.get("clean_text") or "").lower()
    trans_l = (norm.get("transcript_text") or "").lower()

    # PRIMARY — clean uploader text.
    primary = {}
    primary.update(_match_list_a(clean_l, list_a))
    primary.update(_match_list_b(clean_l, list_b, trig))
    for h in primary.values():
        h["provenance"] = "metadata"

    # SPEECH-ONLY — transcript; only NEW Tier-1 entities (no echo of
    # primary; the orthogonal speech-only relational/comparative layer).
    seen = set(primary.keys())
    speech = {}
    if trans_l:
        for canon, h in _match_list_a(trans_l, list_a).items():
            if canon not in seen:
                h["provenance"] = "speech"
                speech[canon] = h
        for canon, h in _match_list_b(trans_l, list_b, trig).items():
            if canon not in seen and canon not in speech:
                h["provenance"] = "speech"
                speech[canon] = h

    # NEW deterministic sim-feeders (Tier-1 canonicals only).
    comparatives = _comparative_pairs(trans_l, tier1) if trans_l else []
    rule_flags = _rule_ambiguity(trans_l, tier1) if trans_l else []

    return {
        "video_id": norm["video_id"],
        "title": norm["title"],
        "caption_source": norm.get("caption_source"),
        "transcript_ok": norm.get("transcript_ok"),
        "primary_entities": sorted(primary.values(),
                                   key=lambda x: x["canonical"]),
        "speech_only_entities": sorted(speech.values(),
                                       key=lambda x: x["canonical"]),
        "comparative_pairs": comparatives,
        "rule_ambiguity_flags": rule_flags,
        "counts": {
            "primary": len(primary),
            "speech_only": len(speech),
            "comparative_pairs": len(comparatives),
            "rule_ambiguity_flags": len(rule_flags),
        },
    }


def run() -> list[dict]:
    config.ensure_dirs()
    gloss = glossary_mod.build(refresh=False)
    out = []
    for f in sorted(glob.glob(str(config.DERIVED_DIR / "*.norm.json"))):
        norm = json.loads(open(f, encoding="utf-8").read())
        ext = extract_one(norm, gloss)
        dest = config.DERIVED_DIR / f"{ext['video_id']}.entities.json"
        dest.write_text(json.dumps(ext, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        out.append(ext)
    print(f"[extract] v2: {len(out)} videos -> {config.DERIVED_DIR}")
    return out


if __name__ == "__main__":
    run()
