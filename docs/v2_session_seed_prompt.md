# Seed Prompt — v2 Stages 6 + 7 (sentiment-rich) Build Session

**Paste this entire document into a fresh Claude Code session to start v2 work.** Everything the new session needs is referenced by path so it can load lazily.

---

## NEW SESSION KICKOFF — v2 Stages 6 + 7 Build

Phil here. Today is 2026-04-29 (or a later date — adjust). The Licensing Expo (Hasbro at Booth G170) is **May 19-21, 2026** — that's the demo deadline.

### Mission

You're building **v2** of Stages 6 (Homebrew) and 7 (Forums) for the Universes Beyond Matrix's `community_reception_score` composite. v1 ships **presence-only** signals; v2 needs to add **sentiment classification + richer source coverage** so we can validate whether these stages move the composite scores meaningfully vs the v1 baseline.

### What's already built (locked, do not relitigate)

The full Phase 1 build is committed to main (PR #69 merged). Read these **before** writing any code:

1. **`docs/community_reception_findings.md`** (in repo) — comprehensive findings report covering all 7 stages + acquisition dimension + cross-source insights. Appendix A has the **v1 baseline snapshot** for A/B comparison.

2. **`~/.claude/projects/.../memory/project_community_reception_plan.md`** — strategic plan + locked decisions. Section "Phase 1 COMPLETE" + "Stage 6 v1 SHIPPED" + "Stage 7 v1 SHIPPED" cover the architecture and findings.

3. **`gold_views/ub_matrix_composite.sql`** — the master composite view consuming all 7 stages + acquisition dimension. v2 work feeds INTO this view via the existing `homebrew_score` and `forum_score` CTEs.

### Locked architectural decisions (do not relitigate)

These were debated thoroughly and concluded:

- **Dual-view composite** (equal-weighted + ChatGPT-weighted, both surfaced with `score_divergence`). Per-IP renormalization handles missing sources.
- **Weighted-composite weights:** Precedents 0.30 / BGG 0.20 / AO3 0.20 / Reddit 0.15 / Gemini 0.15 / Homebrew 0.10 / Forum 0.10.
- **Abstention rule:** composite NULL when `measured_sources_count < 2` (Stages 2-7 count; Gemini doesn't).
- **Reception ≠ Acquisition:** never averaged together. Sibling matrix dimensions.
- **Bookmarklet pattern** for ToS-restrictive platforms (AO3 + FFN already use this; D&D Beyond Homebrew likely will too).
- **AI Bouncer pattern** (Gemini Flash for binary disambiguation + classification) is the standard pattern for converting search results into clean per-IP signals.

---

## v2 Scope — what this session should deliver

### v2 Stage 6 — D&D Beyond + GM Binder + Homebrewery

**Goal:** Augment v1 (r/UnearthedArcana subset, 11 IPs scored) with broader / richer homebrew signal.

Three sub-sources to add, in priority order:

#### 6a. D&D Beyond Homebrew (HIGHEST priority — all 3 reviewer tools rated this #1)

Native to the D&D ecosystem. `ddb.com/homebrew/subclasses` etc. shows community-published homebrew with **"Adds to Collection"** counts — direct in-ecosystem revealed-use signal.

**Constraint:** D&D Beyond is Cloudflare-protected + ToS-restrictive. Per CLAUDE.md ethics-over-feature, automated scraping is OFF the table.

**Resolution:** Bookmarklet pattern, same as AO3 + FFN already use. Phil installs the bookmarklet, browses to D&D Beyond's homebrew search for `[IP name]`, clicks bookmarklet, count + top items POST to bouncer.

Reference: `scripts/ao3_bookmarklet.js` for the pattern. The bookmarklet should:
- Read DOM for homebrew item count + top 5-10 entries (name, "adds" count, type=subclass/spell/monster/etc.)
- Auto-attribute via `_arcane_ip=...` URL marker (same pattern as AO3)
- POST to a new bouncer endpoint `/system/homebrew/ingest-ddb`
- Show confirm modal before save

New BQ table: `dnd_trends_raw.ddb_homebrew_counts`.

**Effort estimate:** ~3-4 hours build + Phil's manual capture time (~30-60 min for ~25 priority IPs).

#### 6b. GM Binder + Homebrewery via Google Custom Search

Two markdown-to-PDF tools used to make polished homebrew docs. Many "Unofficial [IP] 5e" PDFs published here.

**Approach:** Google Custom Search API (we already have the infrastructure from Stage 7). Add gmbinder.com + homebrewery.naturalcrit.com to the existing PSE OR create a new PSE.

Per-IP query: `"[IP name]" (site:gmbinder.com OR site:homebrewery.naturalcrit.com)`

**Effort estimate:** ~2 hours build (mirrors `scripts/harvest_forum_presence.py`).

#### 6c. r/UnearthedArcana sentiment depth (extension of v1)

Currently we treat any UA mention as positive (per the AI Bouncer's `crossover_attitude='positive'`). v2 could do a **second-pass classification** specifically for homebrew posts: extract the type (subclass / spell / monster / item / setting), the IP referenced, and the upvote count to weight the score.

**Effort estimate:** ~2 hours (new SQL + classifier prompt).

### v2 Stage 7 — Forum sentiment via Playwright (+ bookmarklet fallback)

**Goal:** Augment v1 (presence-only — total result counts + top URLs) with **sentiment per top thread**. The v1 schema already captures `top_thread_urls` for exactly this purpose.

#### 7a. Playwright thread scraper

For each IP × top URL captured in `forum_presence_counts.top_thread_urls`:
1. Use Playwright to fetch the thread page
2. Extract OP + first ~20 replies' text
3. Send to AI Bouncer (Gemini Flash) for sentiment classification:
   - `is_about_ip_for_dnd` (binary disambiguation — many "Tyranny" mentions are generic)
   - `forum_attitude`: `positive` / `negative` / `divisive` / `mentions_only` / `not_about_ip_for_dnd`
   - `backlash_keywords` detected: `cash_grab`, `tone_mismatch`, `not_dnd`, `cringe`, `pandering` etc. (per Perplexity's "narrative classification" suggestion)
4. Aggregate per IP into `forum_sentiment_score` and combine with v1 `forum_presence_score` into a final `forum_proxy_score`.

**Per-forum bot-detection prediction (untested):**
- EN World (XenForo): likely scrapable
- GitP (vBulletin): very likely scrapable (older tech)
- RPG.net (XenForo): possible Cloudflare; bookmarklet fallback if blocked
- Dragonsfoot (phpBB): likely scrapable

**Effort estimate:** ~4-6 hours build, plus per-forum debugging if any need bookmarklet fallback.

#### 7b. Bookmarklet fallback for forums where Playwright fails

Same pattern as AO3 + FFN bookmarklets. Phil clicks through top URLs, captures sentiment manually via bookmarklet. v1 captured top URLs specifically for this case.

**Effort estimate:** ~2-3 hours per problematic forum.

#### 7c. Backlash narrative extraction (Perplexity's suggestion, lower priority)

Beyond binary positive/negative, classify forum threads by argument type:
- `cash_grab_narrative`
- `tone_mismatch_narrative`
- `not_dnd_narrative`
- `pandering_narrative`
- `system_design_critique`
- `worldbuilding_endorsement`

Useful for the data trail UI (showing reviewers WHY the community rejects an IP, not just THAT they reject it). Could be a column on the `forum_sentiment_classified` table populated by an additional Gemini classification pass.

**Effort estimate:** ~1-2 hours additional classifier prompt.

---

## v1 baseline (for A/B comparison)

The full v1 baseline is in **`docs/community_reception_findings.md` Appendix A**. Key IPs to A/B against post-v2:

| IP | v1 rec_eq | v1 sources | v2 expected change |
|---|---|---|---|
| **Berserk** | 0.90 | 4 (Gemini, BGG, Reddit, Homebrew, Forum) | minimal — already saturated positive |
| **BG3** | 0.86 | 4 | minimal — BG3 is D&D, ceiling effect |
| **Dungeon Crawler Carl** | 0.84 | 2 (BGG, Forum) | moderate — DDB Homebrew likely adds signal |
| **Stranger Things** | 0.70 | 5 (most measured sources) | moderate — forum sentiment may differ from presence (lots of mentions but mixed reception) |
| **Spy x Family** | 0.25 | 3 (BGG, AO3, Forum) | likely DEEPENS the negative — forum sentiment probably negative-skewed |
| **Tyranny** | thin (only forum 0.95) | 1 | **BIGGEST EXPECTED MOVEMENT** — forum sentiment classification will catch the false-positive "tyranny" word inflation; expect v2 to drop dramatically |
| **Cthulhu Mythos** | thin (forum 0.90) | 1 | similar to Tyranny — name is widely referenced beyond the IP |
| **The Boys** | thin (forum 0.83) | 1 | likely retains signal — show is genuinely discussed on RPG.net |
| **Hollow Knight** | 0.70 | 2 (Reddit, Forum) | DDB Homebrew may add signal (HK fans build for D&D) |
| **Magnus Archives** | 0.78 | 2 (Homebrew, Forum) | minimal change expected |

**The Tyranny case is the killer test.** If v2 sentiment-classifies forum threads and Tyranny drops from "thin_evidence forum 0.95" to "thin_evidence forum 0.20" or similar, that proves v2 is meaningfully more accurate than v1 presence-only.

## Available infrastructure (don't rebuild)

### Secrets (already in Secret Manager)
- `gemini-api-key` — Gemini Flash for AI Bouncer
- `reddit-client-id`, `reddit-client-secret`, `reddit-user-agent` — PRAW (existing)
- `google-cse-api-key`, `google-cse-id` — Google Custom Search (Stage 7)

### BGG/Reddit/AO3/Stage 7 patterns to mirror
- **Bookmarklet:** `scripts/ao3_bookmarklet.js` + `.txt` + registry in `arcane/src/lib/bookmarklets.ts`. Bouncer endpoint pattern: `bouncer/main.py` route at `/system/<area>/<action>`.
- **Google CSE harvester:** `scripts/harvest_forum_presence.py` — copy-and-adapt for GMBinder/Homebrewery.
- **AI Bouncer:** `scripts/classify_reddit_ub_mentions.py` — Gemini Flash batch classifier with structured output schema.
- **Gold view abstention pattern:** `gold_views/forum_presence_proxy.sql` — log-scale normalize + per-forum bucketing + abstention thresholds.

### BQ tables that already exist
- `dnd_trends_raw.forum_presence_counts` — has `top_thread_urls` ARRAY for v2 sentiment scraping
- `dnd_trends_raw.reddit_ub_classified_mentions` — has the existing UA classifications for v1 homebrew_creation_score

## Guardrails (CLAUDE.md, important)

Phil runs Bypass Permissions but expects pause-and-confirm for:
- Merging PRs (`gh pr merge`)
- Pushing to main (don't bypass — PRs only)
- Force-pushing anywhere
- Deploying to Cloud Run (`gcloud run deploy`, `gcloud functions deploy`)
- Writing to Firestore (mutations)
- Writing/altering BigQuery (CREATE OR REPLACE VIEW, INSERT, DELETE, schema changes, `bq mk`)
- gcloud commands changing GCP state
- Recursive deletes

**Pause-and-ask** means: state what you're about to do in one sentence, then wait for explicit "yes" before running.

## Recommended order of work

1. **Read the findings doc + memory file** — understand what v1 produced + the demo-grade insights to preserve
2. **Pause-and-confirm scope with Phil** — scope can grow or shrink based on his time
3. **6b Google CSE for GMBinder/Homebrewery first** — easiest, no bookmarklet UX
4. **7a Playwright forum scraper second** — biggest signal lift (Tyranny test case)
5. **6a DDB Homebrew bookmarklet third** — most novel data but requires bookmarklet build + Phil's manual capture
6. **7b Bookmarklet fallback as needed** — only if specific forums block Playwright
7. **6c + 7c — additional classification depth** — only if time permits

## Ask Phil before starting

1. Re-read these locked decisions and confirm none should change
2. Confirm the v2 scope ordering above
3. Confirm time budget (Expo is May 19-21 — how much runway pre-Expo?)
4. Confirm whether to also do the UI work (Phase B in the original plan) — this seed prompt assumes UI is **deferred to AFTER v2**

Then start with 6b (cheapest, most automated).

## Where to find the v1 baseline data

When you want to compare a v2 score against v1 baseline:

```sql
-- v1 snapshot is implicitly captured in the existing view's last refresh.
-- BUT: v2 work will modify the same view. Before running v2 deploys,
-- consider creating a snapshot table:
CREATE TABLE `dnd-trends-index.dnd_trends_raw.matrix_v1_baseline_snapshot`
AS SELECT * FROM `dnd-trends-index.gold_data.ub_matrix_composite`;

-- Then post-v2:
SELECT v1.ip_name,
       v1.community_reception_score_equal AS rec_v1,
       v2.community_reception_score_equal AS rec_v2,
       ROUND(v2.community_reception_score_equal - v1.community_reception_score_equal, 3) AS delta
FROM `dnd-trends-index.dnd_trends_raw.matrix_v1_baseline_snapshot` v1
LEFT JOIN `dnd-trends-index.gold_data.ub_matrix_composite` v2 USING (ip_name)
ORDER BY ABS(delta) DESC NULLS LAST LIMIT 30;
```

**Recommend creating the v1 baseline snapshot table FIRST** before any v2 deploys. It locks in the comparison baseline so we can show "Stage 7 v2 sentiment moved Tyranny from 0.95 to 0.20" type findings concretely.

---

## End of seed prompt

When the new session is unsure about decisions or context, the authoritative sources in priority order:

1. `docs/community_reception_findings.md` (in repo) — strategic findings + v1 baseline
2. `gold_views/ub_matrix_composite.sql` — the master composite contract
3. `~/.claude/projects/.../memory/project_community_reception_plan.md` — locked decisions
4. `~/.claude/projects/.../memory/project_hasbro_pitch_problems_solutions.md` — pitch positioning context

Total Phase 1 build cost so far: ~$0.71 across all Gemini batches + Google CSE. v2 will likely add similar amounts (Gemini Flash classification across forum threads + DDB items). Cost ceiling unlikely to exceed $5 for full v2 build.
