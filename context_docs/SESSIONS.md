# Arcane Analytics — Session Log
**GCP Project:** `dnd-trends-index`
**Format:** Newest session at top. Each entry appended by Claude at session end, committed by Antigravity.

---

## Session: 2026-03-22

**Topics covered:** Related queries discovery pipeline design; Chaldean Cycle scheduler diagnosis and repair; context document creation

### What was built
- `cloud_functions/related_queries_discovery/` — complete Cloud Function scaffolded and delivered (main.py, requirements.txt, deploy.sh, sql/schema.sql, README.md)
- `context_docs/` — this document set (CONTEXT.md, ARCHITECTURE.md, CONCEPT_LIBRARY.md, SESSIONS.md)

### What was fixed
**Chaldean Cycle scheduler — fully repaired.** Root cause: single typo in `utils/schedule_manager.py` line 9. `workflow-fast-lane` → `dnd-fast-lane`. Every daily scrape job since ~March 1 was firing at a non-existent workflow. Four specific fixes:
1. `utils/schedule_manager.py` line 9 — corrected workflow name (future jobs)
2. `scrape-2026-03-23` — manually patched already-created tomorrow job
3. `caldean-master-trigger` — fixed OIDC → OAuth auth (Workflow Executions API requires OAuth)
4. Verified live end-to-end test: `status: {}` (success)

**Key lesson:** Cloud Scheduler jobs targeting `workflowexecutions.googleapis.com` must use `--oauth-service-account-email` with `cloud-platform` scope. OIDC returns 401.

**Health dashboard diagnosis:** `bouncer/main.py` `/system/health` endpoint returns hardcoded arithmetic (`now - 4h`, `now + 20h`) — not connected to real pipeline data. Known issue, deferred.

### Key design decisions made
- Related queries variant resolution: four-stage pipeline (fuzzy match → Gemini → Claude staging review → Phil approval)
- `concept_variations` — separate table, not array column in `concept_library` (avoids DML rewrites, Antigravity reliability concern)
- Gemini receives variants array as context — gives behavioral fingerprint of each concept, dramatically improves its ability to classify whether new related query terms are variants or new concepts
- BG3, homebrew, UA: category isolation over exclusion
- Context documents: Claude writes at session end, Antigravity commits, pattern established

### Outstanding issues noted
- `trigger-daily-journalist` returning code 13 (INTERNAL) — not investigated
- Health dashboard shows mocked data — deferred
- `scrape-2026-03-21` gap — no job exists for March 21, data missing for that day (Saturday — likely correct Shabbat skip, worth verifying)

### Next session: pick up here
Resume related queries discovery pipeline — specifically:
1. Create `concept_variations` table DDL and deploy
2. Build fuzzy match pre-filter
3. Design Gemini classification prompt with variants-array context
4. Build staging table + Claude review report template
5. Wire variant resolution into `related_queries_discovery` post-processing flow
6. Schedule `related-queries-discovery` Cloud Function (add to `dnd-fast-lane` workflow or separate schedule)

---

*Previous sessions not yet logged — this is the first entry in this document.*
*For context on earlier work, see userMemories in Claude's system context.*
