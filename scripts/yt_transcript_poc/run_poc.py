"""
YT Transcript POC — orchestrator + eyeball summary.

  python scripts/yt_transcript_poc/run_poc.py           # normalize+extract+summary
  python scripts/yt_transcript_poc/run_poc.py --llm     # also run gated LLM step

Acquisition (pull.py) is run SEPARATELY and deliberately (it is
rate-limited; see README). This orchestrates the no-API deterministic
pipeline + the gated LLM step, then prints a summary an operator who
knows the channel can sanity-check (the POC's actual purpose).
"""

from __future__ import annotations

import sys

try:
    from . import normalize, extract, parse_llm
except ImportError:  # pragma: no cover
    import normalize, extract, parse_llm  # type: ignore


def main(run_llm: bool = False) -> None:
    norms = normalize.run()
    exts = extract.run()
    by_id = {e["video_id"]: e for e in exts}

    print("\n================ EYEBALL SUMMARY ================")
    ok = [n for n in norms if n.get("transcript_ok")]
    print(f"videos={len(norms)}  transcript_ok={len(ok)}  "
          f"blocked={len(norms)-len(ok)}")
    for n in norms:
        e = by_id.get(n["video_id"], {})
        c = e.get("counts", {})
        flag = "" if n.get("transcript_ok") else "  [NO TRANSCRIPT/blocked]"
        print(f"\n• {n['title'][:66]}{flag}")
        print(f"    cap={n.get('caption_source')} "
              f"views={n.get('view_count')} "
              f"primary={c.get('primary',0)} "
              f"speech_only={c.get('speech_only',0)} "
              f"comp_pairs={c.get('comparative_pairs',0)} "
              f"rule_flags={c.get('rule_ambiguity_flags',0)}")
        prim = [x["canonical"] for x in e.get("primary_entities", [])][:10]
        spch = [x["canonical"] for x in e.get("speech_only_entities", [])][:10]
        if prim:
            print(f"    primary[meta]: {', '.join(prim)}")
        if spch:
            print(f"    speech-only  : {', '.join(spch)}")
        comps = e.get("comparative_pairs", [])[:3]
        for cp in comps:
            print(f"    [vs]  {cp['a']} ~{cp['trigger']}~ {cp['b']}")
        rules = e.get("rule_ambiguity_flags", [])[:3]
        for rf in rules:
            print(f"    [?]   {rf['feature']}  ({rf['marker']})")

    if run_llm:
        print("\n---- LLM step ----")
        parse_llm.run()
    print("\n(LLM aspect/stance/comparison/rule-ambiguity layer is the "
          "scaffolded next step — needs GEMINI_API_KEY; the above is the "
          "deterministic, no-API, provenance-tagged core.)")


if __name__ == "__main__":
    main(run_llm="--llm" in sys.argv)
