# Seed Prompt — Stage 7a-ii Playwright Forum Thread Bodies

**Paste this entire document into a fresh Claude session to start v4 work.** PR #70 should be merged before this session begins so you start on a clean main branch.

---

## NEW SESSION KICKOFF — Stage 7a-ii Playwright Forum Thread Scrape

Phil here. The Licensing Expo is **May 19-21, 2026** — the demo deadline. Today is 2026-04-30 (or later — adjust). Plenty of runway.

### Mission

Build **Stage 7a-ii** — the deferred-from-Stage-7a Playwright forum-thread-body scraper. Stage 7a-i (already shipped) classifies forum top URLs from title+snippet only. Stage 7a-ii replaces that thin input with full thread content (OP + first 20 replies), which will dramatically improve **Stage 7c (backlash narrative classification)** which currently shows 99% constructive results because title+snippet doesn't have enough text to detect cash_grab/tone_mismatch/not_dnd rhetoric.

This is the last big lift on the Stage 7 forum work. After this, the Hasbro pitch deck has near-complete community_reception data trail.

### What's already shipped (do NOT relitigate)

The full v2 + v3 community_reception build is committed to main (PR #70 merged). Read the authoritative source:

1. **`docs/community_reception_findings.md`** (in repo) — comprehensive findings doc with the v3-final section covering everything shipped through Apr 30. Includes the v1 baseline snapshot (Appendix A) for A/B comparison.

2. **`gold_views/ub_matrix_composite.sql`** — the master composite view consuming all 7 stages. Stage 7 forum data flows through `gold_data.forum_presence_proxy`.

3. **`gold_views/forum_presence_proxy.sql`** — current Stage 7a-i sentiment-weighted view. Stage 7a-ii will deepen the input but the output schema should stay backward-compatible.

4. **Memory file `project_community_reception_plan.md`** — strategic plan + locked decisions. The v3-final section captures what we just shipped.

### Locked architectural decisions (do NOT relitigate)

These were debated thoroughly and concluded:

- **Two-layer disambiguation pattern is mandatory** for any IP-name-based source. `feedback_disambiguation_pattern.md` memo. 51/142 IPs are `ambiguity_flag=TRUE`.
- **AI Bouncer pattern**: Gemini Flash for binary disambiguation + classification. Mirror existing classifier scripts.
- **Dual-view composite** (equal-weighted + ChatGPT-weighted, both surfaced with `score_divergence`).
- **Per-IP renormalization** for missing sources.
- **Abstention rule:** composite NULL when `measured_sources_count < 2`.
- **Reception ≠ Acquisition**: never averaged. Sibling matrix dimensions.
- **The Tyranny test holds**: across 5 versions, the canary correctly classifies as thin_evidence. Don't break this.

---

## What this session should deliver

### Stage 7a-ii — Playwright thread body scrape + sentiment refinement

**Goal:** Replace the thin title+snippet input to Stage 7a-i + Stage 7c classifiers with full forum thread content (OP + first ~20 replies), then re-classify both attitude AND narratives with the richer input.

**Input:** Existing `dnd_trends_raw.forum_presence_counts.top_thread_urls` — already captured in v1 forum harvest. ~704 URLs across 142 IPs across 4 forums (EN World, GitP, RPG.net, Dragonsfoot).

**Output:** New BQ table `dnd_trends_raw.forum_thread_bodies` with one row per URL (ip_name, url, op_text, replies_text_combined, scrape_status, scraped_at). Then re-run the existing `classify_forum_top_urls.py` and `classify_forum_narratives.py` scripts with the body text appended to the existing title+snippet input.

**Per-forum bot-detection prediction (untested):**
- EN World (XenForo): likely scrapable
- GitP (vBulletin): very likely scrapable (older tech)
- RPG.net (XenForo): possible Cloudflare; bookmarklet fallback if blocked → Stage 7b
- Dragonsfoot (phpBB): likely scrapable

**Cost estimate:**
- Playwright runtime: ~3 sec/URL × 704 URLs = ~35 min (assuming no rate limits + no Cloudflare blocks)
- Reasonable rate-limit pacing: 5 sec/URL = ~60 min
- Re-classification with longer input: ~$0.30 (prompt tokens scale with body length)

**Effort estimate per the v2 seed prompt:** ~4-6 hours build, plus per-forum debugging if any need bookmarklet fallback.

### Build steps (recommended order)

1. **Verify Playwright already available in repo**: cloud_functions/google_trends_scraper/browser_trends.py and fandom_view_fetcher/main.py both use Playwright. Pattern to mirror. The new scraper should NOT live in cloud_functions/ — it's a one-off harvester run from Phil's machine, mirroring the pattern of `scripts/harvest_*.py`.

2. **Test scraping ONE thread per forum** before bulk run. Confirm the OP + replies extract cleanly. Report selectors back for the per-forum extraction logic. Different forum software needs different selectors:
   - XenForo: `.message-body` / `.bbCodeBlock`
   - vBulletin: `.postcontent`
   - phpBB: `.postbody`

3. **Build `scripts/scrape_forum_thread_bodies.py`**:
   - Read URLs from `forum_presence_counts.top_thread_urls`
   - Group by forum_domain → use forum-specific Playwright selectors
   - Capture OP + first 20 replies as text (truncate each to ~2000 chars to bound payload)
   - Write to `forum_thread_bodies` table
   - Idempotent: skip URLs already scraped (LEFT JOIN check)
   - Rate-limit per forum (e.g. 5 sec/req); randomize User-Agent within reasonable browser strings
   - Handle Cloudflare gracefully — if HTTP 403 or interstitial detected, log + skip + flag

4. **Update `scripts/classify_forum_top_urls.py`** to accept body text as additional input. Append body content to the existing title+snippet prompt. Re-classify with `--force` to refresh attitudes with the richer signal.

5. **Update `scripts/classify_forum_narratives.py`** to also accept body text. The narrative classifier benefits more from this than the attitude classifier — backlash narratives need real argument text to detect.

6. **Update gold view if needed** — backwards-compat columns mostly preserved; may want a new `body_scrape_coverage` column showing fraction of top_thread_urls that got body-scraped per IP.

7. **A/B vs the Apr 30 baseline** — Stage 7c showed only 5 backlash narratives across all forums; Stage 7a-ii should surface dozens more if the body content is informative. Pre-and-post counts in findings doc.

### Decision points to flag for Phil before building

- **Replies depth**: 20 replies recommended. Top of thread tends to set tone; deeper replies dilute. Could go 10 to be safer with rate limits.
- **Forum-by-forum start**: build for ONE forum first (probably GitP — vBulletin is oldest/simplest), prove the pattern, then add the other 3.
- **Cloudflare on RPG.net**: if it's blocked, falls back to Stage 7b bookmarklet (per the original v2 plan). Don't pre-build the bookmarklet; only do it if Playwright actually fails.
- **Re-classification scope**: only the threads where new body data exists, OR all 704 with --force? Recommend force-all for consistency.

---

## Available infrastructure (don't rebuild)

### BQ tables in scope
- `dnd_trends_raw.forum_presence_counts` — has `top_thread_urls` ARRAY for v2 (has been there since Stage 7 v1 shipped)
- `dnd_trends_raw.forum_top_urls_classified` — Stage 7a-i first-pass classifications
- `dnd_trends_raw.forum_narratives_classified` — Stage 7c narrative classifications
- `dnd_trends_raw.ub_ip_alias_library` — disambiguation contracts (51 ambiguity-flagged IPs)

### Existing patterns to mirror
- **Playwright cloud functions**: `cloud_functions/google_trends_scraper/browser_trends.py` and `cloud_functions/fandom_view_fetcher/main.py` show the pattern. Probably want `playwright.async_api`. Browsers installed via `playwright install chromium`.
- **Forum harvester**: `scripts/harvest_forum_presence.py` for the IP-loop + alias-library + per-forum pattern (just no Playwright there; CSE-only).
- **AI Bouncer classifiers**: `scripts/classify_forum_top_urls.py` + `scripts/classify_forum_narratives.py` for the Gemini Flash prompt + schema pattern.

### Secrets in Secret Manager
- `gemini-api-key` — for the re-classification pass
- `google-cse-api-key` / `google-cse-id` — not needed for Stage 7a-ii (we have URLs already)
- No new secrets required for Playwright (no Itch.io API, no DDB-API; just browser fetches with no auth)

### Forum URL distribution (for sizing)
Run this query to see the per-forum URL distribution so you can size the Playwright work:

```sql
SELECT t.forum_domain, COUNT(*) AS url_count
FROM `dnd-trends-index.dnd_trends_raw.forum_presence_counts` p,
     UNNEST(p.top_thread_urls) AS t
GROUP BY t.forum_domain
ORDER BY url_count DESC;
```

Expected breakdown roughly: enworld.org and rpg.net each ~250 URLs, giantitp.com ~150, dragonsfoot.org ~50.

---

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

**Pause-and-ask** means: state what you're about to do in one sentence, then wait for explicit "yes" before running. A plain "yes" or "ok" is enough.

**Stage 7a-ii doesn't need Cloud Run / Firestore / IAM changes** — it's a local script + BQ inserts + view re-deploys. Pause-and-confirm only on:
- `bq mk` for the new `forum_thread_bodies` table
- `bq query` deploying any updated gold view
- The actual scrape run (~$0 cost but ~35-60 min runtime + ethics — confirm before kicking off the full sweep)

---

## Recommended order of work

1. **Read the findings doc** (especially v3-final section) — understand current state.
2. **Pause-and-confirm scope with Phil** — scope can grow or shrink based on his time.
3. **Test on ONE thread per forum** — sanity check Playwright extraction + selectors per forum software.
4. **Build the scraper** — mirror existing patterns; idempotent skip-already-scraped logic.
5. **Pause-confirm full bulk run** — flag Cloudflare risk on RPG.net.
6. **Run full sweep** — log per-forum success/fail counts.
7. **Re-classify both attitude + narratives** — `--force` re-run with body text appended.
8. **Verify A/B** — Stage 7c narrative counts before/after; should see real backlash narratives surface where the title+snippet pass missed.
9. **Update findings doc + memory** — capture v4 numbers + any per-forum gotchas + Cloudflare verdict.
10. **(If time) Stage 7b** — bookmarklet fallback for any forum Playwright couldn't reach.

---

## Demo-grade findings to preserve / build on

Already-shipped findings the new session shouldn't regress:

1. **Tyranny test passes across 5 versions** — abstention math is correct.
2. **GMBinder/Homebrewery as ebook-piracy hosts** finding — caught by `is_5e_homebrew` axis.
3. **DDB two-form-generation finding** — `filter-name` for newer 5e-2024 sections, `filter-search` for older content sections.
4. **Hades-Demigod disambiguation** — Layer 2 AI Bouncer correctly distinguishes universally-popular generic from IP-specific.
5. **TTRPG forums = 99% constructive DMs framing** — validates Gemini's "Reddit is Players, AO3 is Fans, Forums are DMs" plan-level framing. Stage 7a-ii data may shift this — if backlash narratives surface in body text where they didn't in title+snippet, the framing softens. That's a valid update; just document it.
6. **Wuthering Waves + Discworld + Delicious in Dungeon backlash signals** — small but validating cases.

The new session SHOULD update finding #5 if the deeper Playwright body data shows more backlash narratives than the cheap-path title+snippet did. That's a legitimate refinement of the framing, not a regression.

---

## What could go wrong

- **Cloudflare interstitials on RPG.net**: most likely failure mode. Bookmarklet fallback (Stage 7b) exists in the plan for this case. Don't sink hours debugging Cloudflare before falling back.
- **Forum schema changes since v1 harvest**: forum URLs from Apr 29 may have moved. Treat 404s as "skip this URL" rather than "halt the run".
- **Rate-limited bans**: if any forum starts returning 429s, slow down + restart. Don't get the IP blocked.
- **Per-forum HTML differences**: different forum software has different selectors. Test ONE thread per forum first.
- **Cost overrun on re-classification**: 704 threads × longer body input could hit ~$1+ in Gemini Flash. Pause-confirm before running classifier with --force on full set.

Total Stage 7a-ii budget: ~$0.50 - $1.50 expected.

---

## Where to find context

In priority order:
1. `docs/community_reception_findings.md` v3-final section — what shipped + what skipped
2. `gold_views/forum_presence_proxy.sql` — current Stage 7a-i view that 7a-ii feeds
3. `scripts/classify_forum_top_urls.py` + `scripts/classify_forum_narratives.py` — classifiers to update
4. `scripts/harvest_forum_presence.py` — IP-loop + alias-library pattern
5. `cloud_functions/google_trends_scraper/browser_trends.py` — existing Playwright pattern in repo
6. `~/.claude/projects/.../memory/project_community_reception_plan.md` v3-final section
7. `~/.claude/projects/.../memory/feedback_disambiguation_pattern.md` — the two-layer playbook (relevant for any IP-name-based work)

---

## Total v2 + v3 spend so far: ~$1.05

The v2+v3 community_reception build cost ~$1.05 across CSE quota + Gemini Flash classifications. Stage 7a-ii will likely add ~$0.50-$1.50 (mostly the re-classification pass with longer body input). Total budget for the entire community_reception build through Expo: well under $5.

---

## End of seed prompt
