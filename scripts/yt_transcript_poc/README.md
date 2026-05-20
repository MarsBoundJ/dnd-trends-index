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

## Pipeline (two acquisition paths)

| Step | File | API? | What |
|---|---|---|---|
| **Acquire — POC fallback** | `pull.py` | no | yt-dlp metadata + youtube-transcript-api v1.x transcript → `RAW_DIR`. **Rate-limited** (see below); kept for low-volume / no-key smoke tests. |
| **Acquire — production (Option D)** | `audio_pull.py` → `transcribe.py` | Gemini Flash-Lite | yt-dlp `-x` downloads native m4a audio (no captions endpoint, no IP block) → Gemini 2.5 Flash-Lite ASR with the dynamic per-video micro-glossary in the system instruction → transcript written into the same `RAW_DIR/{id}.json` schema. **Cost (verified May 2026): ~$3–8 for the full 75 × 35-min Treantmonk batch.** |
| Glossary | `glossary.py` | BQ read | partitioned closed term-sets (List A rare / List B common-collisions); seeds the micro-glossary above |
| Normalize | `normalize.py` | no | split clean uploader text vs transcript |
| Extract | `extract.py` | no | deterministic, provenance-tagged entities + comparative_pairs + rule_ambiguity_flags |
| Parse LLM | `parse_llm.py` | Gemini Flash | aspect/stance/comparison schema; hardened prompt; API-gated |
| Orchestrate | `run_poc.py` | — | normalize+extract+summary (+`--llm`) |

Production run order (Option D): `audio_pull.py` → `transcribe.py` → `run_poc.py`.
Smoke run order (no key needed): `pull.py` → `run_poc.py`.

## Setting up the Gemini API key (one time)

1. Visit https://aistudio.google.com/app/apikey, sign in, "Create API key" → pick/create a Cloud project. **Enable billing** on that project so the batch isn't free-tier-throttled.
2. Copy this folder's `.env.example` → `.env` and paste the key. `.env` is gitignored — never committed.
3. `transcribe.py` auto-loads it on import; nothing else to wire.

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
