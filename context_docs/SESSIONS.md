# Arcane Analytics — Session Log
**GCP Project:** `dnd-trends-index`
**Format:** Newest session at top. Each entry appended by Claude at session end, committed by Antigravity.

---

## Session: 2026-03-22 (continued — afternoon)

**Topics covered:** concept_variations schema; variant_staging schema; BigQuery deployment

### What was built
- `concept_variations_schema.sql` — DDL for two new BigQuery tables, saved to repo root
- `concept_variations` table — live in `dnd_trends_categorized`, partitioned by date_added, clustered by concept_id/status
- `variant_staging` table — live in `dnd_trends_categorized`, partitioned by created_at, clustered by review_status/gemini_decision

### Table purposes
- `concept_variations` — permanent record of all search string variants per concept; `is_best_variant` flag marks the string currently driving leaderboard scoring
- `variant_staging` — review workspace for the variant resolution pipeline; captures fuzzy match score, Gemini decision + reasoning, Claude override if any, Phil final approval; nothing here is live until Phil approves

### review_status flow
`pending_claude` → `pending_phil` → `approved` | `rejected`

### Next session: pick up here
1. Build fuzzy match pre-filter (token overlap against concept_library keywords)
2. Design Gemini classification prompt — must include: candidate term, seed keyword, existing concept, known variants array from concept_variations
3. Build the Cloud Function or script that runs: emerging_terms → fuzzy match → Gemini → writes to variant_staging
4. Build Claude review report template (reads variant_staging WHERE review_status = 'pending_claude')
5. Deploy related_queries_discovery Cloud Function (currently scaffolded but not deployed)
6. Decide: add related_queries_discovery to dnd-fast-lane workflow or give it its own schedule

---

## Session: 2026-03-22 (morning)

**Topics covered:** Related queries discovery pipeline design; Chaldean Cycle scheduler diagnosis and repair; context document creation

### What was built
- `cloud_functions/related_queries_discovery/` — complete Cloud Function scaffolded and delivered (main.py, requirements.txt, deploy.sh, sql/schema.sql, README.md)
- `context_docs/` — CONTEXT.md, ARCHITECTURE.md, CONCEPT_LIBRARY.md, SESSIONS.md, COMMIT_INSTRUCTIONS.md — all committed to GitHub

### What was fixed
**Chaldean Cycle scheduler — fully repaired.** Root cause: single typo in `utils/schedule_manager.py` line 9. `workflow-fast-lane` → `dnd-fast-lane`. Every daily scrape job since ~March 1 was firing at a non-existent workflow. Four specific fixes:
1. `utils/schedule_manager.py` line 9 — corrected workflow name (future jobs)
2. `scrape-2026-03-23` — manually patched already-created tomorrow job
3. `caldean-master-trigger` — fixed OIDC → OAuth auth (Workflow Executions API requires OAuth)
4. Verified live end-to-end test: `status: {}` (success)

**Key lesson:** Cloud Scheduler jobs targeting `workflowexecutions.googleapis.com` must use `--oauth-service-account-email` with `cloud-platform` scope. OIDC returns 401.

**Health dashboard diagnosis:** `bouncer/main.py` `/system/health` endpoint returns hardcoded arithmetic — not connected to real pipeline data. Known issue, deferred.

### Key design decisions made
- Related queries variant resolution: four-stage pipeline (fuzzy match → Gemini → Claude staging review → Phil approval)
- `concept_variations` — separate table, not array column in `concept_library` (avoids DML rewrites)
- Gemini receives variants array as context — gives behavioral fingerprint of each concept
- BG3, homebrew, UA: category isolation over exclusion
- Context documents pattern established: Claude writes at session end, Antigravity commits

### Outstanding issues (not yet addressed)
- `trigger-daily-journalist` returning code 13 (INTERNAL) — not investigated
- Health dashboard shows mocked data — deferred
- Verify Havdalah catchup ran correctly Saturday night for Friday + Saturday data gap

---

*Previous sessions not yet logged — this is the first entry in this document.*
*For context on earlier work, see userMemories in Claude's system context.*
