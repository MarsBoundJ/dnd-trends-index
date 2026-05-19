"""
YT Transcript POC — LLM parser SCAFFOLD (the aspect/stance/comparison/
rule-ambiguity layer). Hardened per the red-team; runnable separately,
API-gated, graceful skip when no key (the deterministic pipeline stands
on its own without this).

Red-team constraints baked into the prompt:
  - residual span PLUS surrounding context PLUS metadata active-context
    entities (bare-residual guarantees hallucination).
  - glossary-bounded: may only attribute to entities in the supplied
    closed list.
  - "do-nothing" bias: emit nothing unless a verbatim justification span
    from the transcript supports it.
  - every record carries a justification_span + provenance.
  - conservative-to-a-fault: precision >> recall.

Output schema per video:
  aspect_stance[]      {entity, aspect(power|flavor|complexity|dm_burden),
                        stance(pos|neg|mixed|neutral), justification_span}
  comparatives[]       {a, b, relation, justification_span}
  power_tier_claims[]  {entity, claim, justification_span}
  rule_ambiguity[]     {feature, marker, justification_span}
  demand_statements[]  {want, justification_span}
"""

from __future__ import annotations

import glob
import json
import os
import textwrap

try:
    from . import config
except ImportError:  # pragma: no cover
    import config  # type: ignore

SYSTEM = textwrap.dedent("""
    You analyze a D&D YouTube transcript (auto-captioned, noisy). You are
    conservative to a fault: PRECISION OVER RECALL. Emit a record ONLY if a
    verbatim transcript span supports it. Never invent an entity. You may
    only attribute to entities in ACTIVE_CONTEXT or the supplied glossary.
    If unsure, emit nothing. Every record MUST include justification_span
    copied verbatim from the transcript. Output strict JSON only.
""").strip()

USER_TMPL = textwrap.dedent("""
    VIDEO: {title}
    ACTIVE_CONTEXT (entities already found in clean metadata — high trust):
    {active_context}

    GLOSSARY (closed set you may attribute to; do NOT attribute outside it):
    {glossary_sample}

    TRANSCRIPT (noisy auto-captions; proper nouns may be mangled):
    \"\"\"{transcript}\"\"\"

    Return JSON with keys: aspect_stance, comparatives, power_tier_claims,
    rule_ambiguity, demand_statements. Each item MUST include
    justification_span (verbatim) and, where it names an entity, that
    entity MUST be from ACTIVE_CONTEXT/GLOSSARY (canonical spelling).
""").strip()


def build_prompt(norm: dict, ents: dict, glossary_names: list[str]) -> dict:
    active = [e["canonical"] for e in ents.get("primary_entities", [])]
    return {
        "system": SYSTEM,
        "user": USER_TMPL.format(
            title=norm["title"],
            active_context=", ".join(active) or "(none)",
            glossary_sample=", ".join(sorted(set(active + glossary_names[:120]))),
            transcript=(norm.get("transcript_text") or "")[:48000],
        ),
        "model": config.LLM_MODEL,
        "response_mime_type": "application/json",
        "temperature": 0.0,
    }


def call_model(prompt: dict):
    """Pluggable. Requires GEMINI_API_KEY or GOOGLE_API_KEY + the
    google-generativeai package. Returns parsed JSON or None (skip)."""
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not key:
        print("[parse_llm] no GEMINI_API_KEY/GOOGLE_API_KEY — LLM step "
              "SKIPPED (deterministic pipeline already produced output).")
        return None
    try:
        import google.generativeai as genai  # type: ignore
    except ImportError:
        print("[parse_llm] google-generativeai not installed — skip "
              "(`pip install google-generativeai` to enable).")
        return None
    genai.configure(api_key=key)
    model = genai.GenerativeModel(prompt["model"],
                                  system_instruction=prompt["system"])
    resp = model.generate_content(
        prompt["user"],
        generation_config={"temperature": prompt["temperature"],
                           "response_mime_type": prompt["response_mime_type"]},
    )
    try:
        return json.loads(resp.text)
    except Exception as e:
        print(f"[parse_llm] model returned non-JSON: {e}")
        return None


def run() -> list[dict]:
    try:
        from . import glossary as g
    except ImportError:  # pragma: no cover
        import glossary as g  # type: ignore
    gloss = g.build(refresh=False)
    gnames = [m["canonical"] for m in gloss["list_a_rare"].values()]
    out = []
    for f in sorted(glob.glob(str(config.DERIVED_DIR / "*.entities.json"))):
        ents = json.loads(open(f, encoding="utf-8").read())
        nf = config.DERIVED_DIR / f"{ents['video_id']}.norm.json"
        if not nf.exists():
            continue
        norm = json.loads(nf.read_text(encoding="utf-8"))
        if not norm.get("transcript_ok"):
            continue
        prompt = build_prompt(norm, ents, gnames)
        parsed = call_model(prompt)
        if parsed is None:
            continue
        parsed["video_id"] = ents["video_id"]
        dest = config.DERIVED_DIR / f"{ents['video_id']}.llm.json"
        dest.write_text(json.dumps(parsed, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        out.append(parsed)
    print(f"[parse_llm] produced {len(out)} llm records "
          f"(0 = API key not set, expected for the no-API POC run).")
    return out


if __name__ == "__main__":
    run()
