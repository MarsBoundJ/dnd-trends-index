# Arcane Analytics — Master Context Document
**GCP Project:** `dnd-trends-index`
**Last Updated:** 2026-03-22
**Maintained by:** Claude (written at session end, committed by Antigravity)

---

## What This Project Is

Arcane Analytics is an internal market intelligence platform analyzing the D&D/TTRPG ecosystem. It measures search demand, supply signals, and cultural trends across D&D content categories using Google Trends as its primary data source, supplemented by Reddit, YouTube, Fandom, Wikipedia, BGG, Itch.io, Kickstarter, and Roll20.

**Primary users:** Content creators, publishers, and marketers who want to know what D&D topics are growing, peaking, or declining in audience interest.

**Core output:** A multi-study dashboard with leaderboards showing relative search interest across categories (classes, spells, monsters, feats, etc.), trend velocity, and emerging terms.

---

## The People

- **Phil** — Owner, lead decision-maker, D&D domain expert, astrology practitioner
- **AI Studio** — Lead Software Developer role
- **Antigravity (Antigrav)** — Build agent / DevContainer. Has a known pattern of silently reporting success without verifying. All claimed changes require independent verification via BigQuery reads or gcloud describe commands.
- **Claude** — Architecture advisor, code reviewer, session planner, context maintainer

---

## Current Project Status (as of 2026-03-22)

| System | Status | Notes |
|---|---|---|
| Chaldean Cycle scheduler | ✅ FIXED TODAY | Was broken since ~March 1 due to workflow naming mismatch |
| Google Trends scraper | ✅ Operational | Runs via `google-trends-job` Cloud Run job inside `dnd-fast-lane` workflow |
| Reddit harvester | ✅ Operational | |
| Fandom scraper | ✅ Operational | |
| YouTube listener | ✅ Operational | |
| Wikipedia scraper | ✅ Operational | |
| Itch.io RSS harvester | ✅ Operational | |
| BGG/RPGGeek harvester | ✅ Operational | |
| Daily journalist | ⚠️ ERROR | `trigger-daily-journalist` returning code 13 (INTERNAL) — not yet investigated |
| Health dashboard | ⚠️ MOCKED | Shows plausible times but not connected to real pipeline data |
| Related queries discovery | 🔵 IN PROGRESS | Cloud Function scaffolded, not yet deployed |
| Variant resolution pipeline | 🔵 PLANNED | Design complete, not yet built |

---

## How the Scoring System Works

1. For each keyword in `concept_library`, the Search Variant Generator tests multiple phrasings: bare keyword, keyword + "dnd", keyword + "2024", keyword + "5.5e"
2. Each variant is scored against Google Trends
3. The variant with the highest score wins and becomes that keyword's score on the leaderboard
4. Key empirical finding: users search bare class/spell names rather than creator-attributed variants; "2024" outperforms "5.5e" as a qualifier

---

## The Chaldean Cycle Scheduling System

Data collection timing is governed by a custom astronomical scheduling system:

- **Mercury hour rotation:** Each day's scrape fires at the Chaldean "hour of Mercury" — calculated using real Sha'ah Zmanit (proportional hour) math based on sunrise/sunset for the location
- **Shabbat observance:** No data collection from Friday sunset through Saturday night
- **Havdalah catchup:** Saturday night run collects Friday + Saturday data
- **Implementation:** `utils/schedule_manager.py` — calculates the correct time, creates a dated Cloud Scheduler job (`scrape-YYYY-MM-DD`) each day at midnight UTC via `caldean-daily-calculator`
- **Target:** All scrape jobs fire at `dnd-fast-lane` workflow via OAuth token auth

**Critical lesson learned (2026-03-22):** The Workflow Executions API requires OAuth tokens, not OIDC. All scheduler jobs targeting workflows must use `--oauth-service-account-email` not `--oidc-service-account-email`.

---

## Key Architectural Principles

- **BigQuery MCP is read-only** — all DML (INSERT/UPDATE/DELETE) goes through Antigravity DevContainer (authenticated via `/app/dnd-key.json`) or BigQuery Console directly
- **Antigravity verification rule** — after any claimed change, always run a read command to confirm the actual state before proceeding
- **Gemini as primary filter** — AI judgment is the main classification mechanism, not a spot-check (established with monster-classifier, carried forward to variant pipeline)
- **Staging before live tables** — nothing promoted to production tables without Phil's approval
- **Category isolation over exclusion** — BG3, homebrew, UA get their own categories rather than being filtered out

---

## Active Work Streams

### 1. Related Queries Discovery Pipeline (IN PROGRESS)
See `ARCHITECTURE.md` for technical details and `SESSIONS.md` 2026-03-22 for full design decisions.

### 2. Concept Library Enrichment (PLANNED)
- `concept_variations` table needed (separate from `concept_library` to avoid DML rewrites)
- Fuzzy match pre-filter → Gemini classification → Claude staging review → Phil approval
- Four known messy areas: homebrew classes/subclasses, Unearthed Arcana, Baldur's Gate 3, edition comparison

### 3. Health Dashboard (DEFERRED)
- Currently shows mocked last/next run times
- Should read from real pipeline execution data once other streams stabilized

---

## How to Start a New Session

1. Read `CONTEXT.md` (this file) first
2. Read `SESSIONS.md` to find the most recent session entry — that tells you exactly where we left off
3. Read the relevant sub-doc (`ARCHITECTURE.md` or `CONCEPT_LIBRARY.md`) for the work stream being resumed
4. Ask Phil to confirm the current task before writing any code
