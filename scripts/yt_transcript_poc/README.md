# YT Transcript POC — D&D creator-discourse puller + parser

Pilot for the creator-discourse leg of the divergence triangle
(creator-claimed power × community reception × sim eHP). Full design,
scope, and red-team hardening: memory `project_yt_transcript_poc.md`.
**This is a single-channel POC, not the stratified accuracy gate.**

Pilot channel: **Treantmonk's Temple** (optimizer / rules-analysis —
ideal for the sim/validation-oracle + expert mechanics opinion).

## Legal posture (locked)

Transcripts are the creator's copyrighted speech. **Derive-and-discard,
internal-only. Raw pulls live OUTSIDE the repo (`~/yt_poc_data`, override
`YT_POC_DATA_DIR`) and are NEVER committed. Publish aggregate signals
only — never verbatim quotes.** `.gitignore` is belt-and-suspenders.

## Pipeline

| Step | File | API? | What |
|---|---|---|---|
| Acquire | `pull.py` | no | yt-dlp metadata + youtube-transcript-api v1.x transcript → `RAW_DIR` |
| Glossary | `glossary.py` | BQ read | partitioned closed term-sets (List A rare / List B common-collisions) |
| Normalize | `normalize.py` | no | split clean uploader text vs noisy transcript |
| Extract | `extract.py` | no | deterministic, provenance-tagged entities (metadata-primary + speech-only) |
| Parse LLM | `parse_llm.py` | Gemini | **scaffold** — aspect/stance/comparison/rule-ambiguity, hardened, API-gated |
| Orchestrate | `run_poc.py` | — | normalize+extract+summary (+`--llm`) |

Run order: `pull.py` (separate, deliberate) → `run_poc.py`.

## Known finding (2026-05-19, first run): transcript endpoint rate-limits

youtube-transcript-api IP-blocks after a burst (~15 rapid fetches —
**identical to the BackerKit/DDB "burst then block" pattern**) even from
a residential IP. `pull.py` applies our own established discipline:
gentle paced fetches **and on the first hard block it STOPS** (never
retry-harder). Re-run after a cooldown — the 15 cached videos skip,
only the blocked ones retry. Confirms the spec's call that **ASR-on-
audio is the production path**; transcript-scraping is paced-POC-only.

15/20 transcripts acquired on the first run — sufficient for the POC's
two de-risk goals (extraction accuracy; creator-vs-sim divergence).

## Red-team principles enforced in code

- **Conservative-to-a-fault**: precision ≫ recall; every entity carries
  a provenance tag (`metadata` | `speech`) + matched span.
- **Route precision to clean uploader text**; transcript only for
  stance + speech-only relational entities.
- **Partitioned glossary**: List A (rare nouns) direct-match; List B
  (Shield/Wish/Bane/Rage…) ONLY trigger-word-gated — refuses to invent
  `spell_shield` from "acts as a shield".
- **LLM step**: glossary-bounded, context+justification-span required,
  do-nothing bias, separate/gated — the deterministic core stands alone.
