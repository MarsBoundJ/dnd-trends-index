# Step 9 Seed Prompt — Articles (Council of Writers)

Paste the block below into a fresh Claude Code session to start Step 9. Everything the session needs to load is referenced by path so it can be pulled lazily.

---

## Seed prompt (copy from here)

We're starting **Step 9** of the 16-step Arcane Analytics frontend build. Steps 1–8 are complete (last merge: PR #39, concept detail drawer). Step 9 is **Articles** — but we're replacing the legacy 3-persona journalist (Tavern Keeper / Sage / Goblin) with a **Council of 5 bylined writers** modeled on real TTRPG and business luminaries.

**Before writing any code, read these in order:**

1. `FRONTEND_BUILD_STATUS.md` — confirm where we are, mark Step 9 as in progress.
2. `FRONTEND_DESIGN_SPEC.md` — card metaphor, palette, Sage framing.
3. `docs/step-9-persona-study.md` — Yorri's full Gemini+Perplexity research (12+ luminary profiles, Perplexity Sage style guide, Gemini prototype script). Long file — skim the headers and deep-read the sections relevant to the 5 Council members below.
4. `cloud_functions/daily_journalist/main.py` — legacy backend. This is what we're refactoring.
5. `generate_daily_insight.py` — local testing version of same.
6. Memory file `project_step_9_council.md` — architectural plan, Council roster, anomaly→writer routing, data plan for Quartermaster.

**Architectural decisions already made (do not re-litigate):**

- **5-member Council v1:** The Loremaster, The Bursar, The Quartermaster, The Weaver, The Architect. See memory file for luminary lineage per member.
- **Naming convention:** Every Council member is "The [Title]" (title/byline). The conversational chatbot is just "**Sage**" (no article) — **she/her**. This asymmetry is intentional: title vs. name = column vs. conversation.
- **Sage remains singular** — she does NOT write articles. She chairs the Council in the chatbot surface and can cite/quote Council articles by name.
- **Sage voice** — Perplexity's style guide (in `docs/step-9-persona-study.md`) is the base; the memory file's "Sage voice guide (v1)" section adds five subtle texture levers to land the female voice without tropes. Read both before writing any Sage prompt text.
- **Bylines are real** — each article carries `author_name`, `author_beat`, `author_bio` in the schema and on the card.
- **Rotation guard:** no writer publishes two days in a row.
- **Legacy runs in parallel** for 3–5 days before we retire the old 3-persona prompts.

**Execute in two parts:**

### Step 9a — Backend Council

1. Refactor `cloud_functions/daily_journalist/main.py`:
   - Replace `PERSONAS` dict with 5-member Council (name, beat, bio, voice guidelines, domain system prompt). Ground each voice in the luminary research from `docs/step-9-persona-study.md`.
   - Add an anomaly → writer routing function (table in memory file).
   - Add rotation guard that queries `gold_data.daily_articles` for yesterday's author_name and excludes them.
   - Extend output JSON: `author_name`, `author_beat`, `author_bio` alongside existing `headline/hook/body_markdown/key_stat`.
2. Migrate `gold_data.daily_articles` schema to add the three new columns (backfill nulls for legacy rows, or legacy_author=TRUE flag).
3. Add Quartermaster data ingestion:
   - NEW: `cloud_functions/freight_index_harvester/` — daily snapshot of Freightos Baltic Index (FBX) China→North America lane.
   - NEW BQ table: `gold_data.freight_index_daily`.
   - Formalize DriveThruRPG shipping-zone costs into a BQ table if not already.
4. Run Council alongside legacy in production for 3–5 days. Compare side-by-side in BQ before retiring legacy.

### Step 9b — Frontend Article Cards

1. Build `/articles` page at `arcane/src/app/articles/page.tsx`.
2. Server-side data fetch via new `/api/articles` route (pattern: `/api/concept`).
3. `ArticleCard` component inside `CardChrome` shell:
   - Writer sigil (lucide icon per beat — e.g. Scroll for Loremaster, Crown for Bursar, Anchor for Quartermaster, Spline for Weaver, Compass for Architect — confirm choices with Yorri).
   - Byline row: `{author_name} · {author_beat}`.
   - Bio tooltip/expandable on hover (desktop) or tap (mobile).
   - Headline → hook → body_markdown (render via react-markdown, already installed).
   - Key stat chip, confidence pip (metal tier).
4. Update Sage system prompt in `arcane/src/lib/sage-prompts.ts` (or wherever it currently lives) with Council Chair framing. Inject the Council roster as reference block. Explicit rule: Sage cites members, does not role-play them.
5. Add navigation entry for `/articles` in the main nav.
6. Update `FRONTEND_BUILD_STATUS.md` and the `project_frontend_build_status.md` memory file when Step 9 is verified.

**Guardrails reminder (from CLAUDE.md):**
- This is Bypass Permissions mode. Pause and confirm before any of: PR merge, push to main, force-push, Cloud Run deploy, Firestore write, BigQuery destination-table write (read queries fine), GCP state-changing gcloud, recursive delete.
- Commit after every logical chunk. Don't let uncommitted state pile up.

**Start by reading `project_step_9_council.md` in memory and `docs/step-9-persona-study.md` in repo root. Then propose the Step 9a file-touch plan for my signoff before you start coding.**

---

## Why these files and not others

- `docs/step-9-persona-study.md` is the source-of-truth for voice. Without it, the Council reverts to generic-sounding AI prose.
- The memory file collapses the architectural debate into decisions so the new session doesn't rehash them.
- `cloud_functions/daily_journalist/main.py` is the minimum-viable reference for how BQ anomaly queries + Gemini prompt composition currently work — don't reinvent the pipeline.

## What to ask Yorri before touching code

1. Lucide sigil choices per Council member — confirm Scroll/Crown/Anchor/Spline/Compass or propose alternatives.
2. Freightos ingestion cadence — daily snapshot is the default; confirm that's fine vs. weekly.
3. Schema migration style — add columns with NULL backfill, or write fresh rows only with a `council_version` flag.
4. Parallel-run duration — default 3–5 days before legacy retirement; confirm or override.
