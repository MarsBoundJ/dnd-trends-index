"""
YT Transcript POC — deterministic entity extractor (no API, no hallucination).

Principle #0 (red-team): conservative-to-a-fault. Precision >> recall.
Every entity carries a provenance tag and the matched span.

Two passes:
  1. PRIMARY (provenance=metadata): match the glossary against the CLEAN
     uploader text. High trust.
  2. SPEECH-ONLY (provenance=speech): match against the transcript, but
     ONLY for glossary terms NOT already found in metadata — this is the
     comparison/relational layer the red-team flagged as speech-only and
     high-value ("unlike Twilight Cleric...", "worse than Hexblade").

Partitioned matching:
  - List A (rare/fantasy nouns): word-boundary substring match. Safe.
  - List B (common-English collisions): match ONLY when a trigger word
    is adjacent (cast/use/take/the/subclass/spell...). Otherwise dropped
    — refusing to invent `spell_shield` from "he acts as a shield".
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


def _wb(term: str) -> re.Pattern:
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])")


def _match_list_a(text_l: str, list_a: dict) -> dict:
    hits = {}
    for term, meta in list_a.items():
        m = _wb(term).search(text_l)
        if m:
            hits[meta["canonical"]] = {
                "canonical": meta["canonical"], "kind": meta["kind"],
                "matched": term, "method": "listA_wordboundary",
                "span": text_l[max(0, m.start() - 40):m.end() + 40],
            }
    return hits


def _match_list_b(text_l: str, list_b: dict, triggers: list[str]) -> dict:
    """List B only counts when a trigger word sits within ~24 chars of
    the term — the gate that stops common-English false positives."""
    hits = {}
    for term, meta in list_b.items():
        for m in _wb(term).finditer(text_l):
            window = text_l[max(0, m.start() - 24):m.end() + 24]
            if any(tw in window for tw in triggers):
                hits[meta["canonical"]] = {
                    "canonical": meta["canonical"], "kind": meta["kind"],
                    "matched": term, "method": "listB_trigger_gated",
                    "span": text_l[max(0, m.start() - 40):m.end() + 40],
                }
                break
    return hits


def extract_one(norm: dict, gloss: dict) -> dict:
    list_a = gloss["list_a_rare"]
    list_b = gloss["list_b_collisions"]
    trig = gloss["trigger_words"]

    clean_l = (norm.get("clean_text") or "").lower()
    trans_l = (norm.get("transcript_text") or "").lower()

    # PRIMARY — clean uploader text
    primary = {}
    primary.update(_match_list_a(clean_l, list_a))
    primary.update(_match_list_b(clean_l, list_b, trig))
    for h in primary.values():
        h["provenance"] = "metadata"

    # SPEECH-ONLY — transcript, terms NOT already in metadata
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

    return {
        "video_id": norm["video_id"],
        "title": norm["title"],
        "caption_source": norm.get("caption_source"),
        "transcript_ok": norm.get("transcript_ok"),
        "primary_entities": sorted(primary.values(),
                                   key=lambda x: x["canonical"]),
        "speech_only_entities": sorted(speech.values(),
                                       key=lambda x: x["canonical"]),
        "counts": {"primary": len(primary), "speech_only": len(speech)},
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
    print(f"[extract] {len(out)} videos -> {config.DERIVED_DIR}")
    return out


if __name__ == "__main__":
    run()
