# Trusight — Execution Plan (go-to-market + 4-stage sim roadmap)

**Owner:** Phil · **Co-driver:** Claude · **Created:** 2026-05-18 · **Horizon:** May 2026 → Q1 2027
**Calendar companion:** `trusight_plan.ics` (this folder; copy in `Downloads/`) — import into Google/Apple/Outlook for weekly/monthly views.
**Strategy rationale lives in memory:** `project_rules_substrate_architecture.md` → "STRATEGY (consolidated)" section. This file is the *dated how*; memory is the *durable why*. If they ever conflict, memory wins on principle, this file wins on dates.

> **Not rigid.** Dates are commitments to *direction and cadence*, not a whip. Slip a task, don't slip the cadence. Quarterly buffer weeks exist precisely so caretaking interruptions don't cascade.

---

## Operating constraints (baked into every date below)

- **Shabbat/observance blackout:** no work tasks Fri sunset → Sat nightfall. Saturdays are clear in the calendar; Fridays are light (review/publish only, no heavy build). Phil adjusts around Jewish holidays as they fall.
- **Caretaking resilience:** Phil is solo + home-based with caretaking duties. One **buffer/catch-up week per quarter** with zero hard deadlines. Travel ≤ 1 night, late-stage high-probability meetings only.
- **Ownership legend:** **[P]** Phil-only (sends as himself, phone, final publish click, legal counsel, money decisions) · **[C]** Claude-executable (drafting, sim build, data/SQL, tracker upkeep, calendar) · **[P+C]** joint (review, strategic checkpoints).

---

## The four-stage sim roadmap (the spine the plan serves)

1. **Stage 1 — Internal grading tool.** Sim grades WotC UA / new subclasses / spells / monsters / candidate-IP material for *internal* Trusight reports. "UA Warlock X ran 1,000 combats → power between Fiend and Genie." Lowest risk, directly serves the pitch.
2. **Stage 2 — Published analysis.** Sim outputs become public reports/articles (reception **+** power dual reading — the moat). Tone-gated, `is_srd`-gated.
3. **Stage 3 — Standalone (non)profit service.** Community-facing tool; sustainability fees. **Trigger-gated** (traction + counsel).
4. **Stage 4 — AI-DM platform.** Sim inside an AI-DM site (solo/group/modules/campaigns). **Highest legal risk; separate content architecture + real counsel before any build.** Modules are the *most* protected content — opposite end from mechanics.

Gate rule: a stage does not start until the prior stage's gate review passes. Stage 1 cannot start until the stalled combat-sim repo is un-stalled (reconcile `CONTEXT.md`/`SESSIONS.md` frozen 2026-03-30 + `pillars-reconciliation.md`).

---

## Go-to-market: layered, NOT sequential

Direct WotC channels and the quiet community track run **in parallel**. Community is not the fallback after WotC says no — it is the lever that *creates* WotC interest (the DDB-became-indispensable model). The only variable is the community track's **volume dial**: *soft* now (credibility: methodology/wants/zeitgeist), *loud* later (leverage: sim-powered reception+power), turned at the Fork-5 checkpoint.

**Direct-channel order (A → B → C, audiences not a priority ladder — different products from one substrate):**
- **A. Named licensing/creative targets** — Kenna (#1) / Earp / Ayoub. IP-translatability value prop. (Live in `Downloads/trusight_outreach_tracker.csv`.)
- **B. Design teams** — UA reception+power value prop. Target as **champions/internal advocates**, not buyers. *Sharpest tone risk: you'd be scoring the work of the people you're courting — visibly on their side.*
- **C. Marketing** — zeitgeist/timing-intelligence value prop.

**Fork-5 checkpoint criteria** (assessed 2026-09-04): for each of A/B/C, "bit" = a substantive reply, a call booked, or a stated interest in a trial. "Cold" = no substantive engagement after the full follow-up arc (≈ the quarter following the May-26 outreach start). **If ≥1 bit → stay soft, do not complicate a live thread.** **If all cold → turn the dial loud.**

---

## Phased timeline

### Phase 0 — Direct-outreach launch (now → end May)
> **Licensing Expo firmly skipped (May 18 2026).** Verified: $90 Event Planner = floor-only in-person meetings, contact gated to in-platform messaging, no virtual meetings; $500 only adds in-person invite quota — near-zero value for a non-attending remote founder. Direct LinkedIn/email to named targets is now the *near-only* WotC channel (raises the weight of the parallel community/load-bearing track). Expo's only surviving role is a *negative* timing constraint.

| Date | Task | Own |
|---|---|---|
| May 19–21 | Expo week — WotC licensing/creative in Vegas & slammed → **do NOT send outreach** (timing constraint only; we are not attending) | — |
| May 22–24 | Shavuot / Shabbat — blackout | — |
| May 26 | **Direct outreach begins** (Expo week over, targets resurface) — Tier-1 LinkedIn connects + emails (Kenna/Earp/Ayoub) per tracker | [P] |
| May 27 | Sim un-stall #1 — reconcile combat-sim `CONTEXT.md`/`SESSIONS.md` + `pillars-reconciliation.md` | [C] |
| May 29 | Week-1 tracker update | [P+C] |

### Phase 1 — Direct channels + quiet track + Sim Stage 1 (Jun → Aug)
| Date | Task | Own |
|---|---|---|
| Jun 1–5 | Publish-readiness sprint: HTML+PDF report parity, scrub legacy "Arcane Analytics"/"Truesight-with-e" strings, beta logo, surface pitch PDFs, **stand up newsletter channel** | [C] |
| Jun 8 | Sim Stage 1 — engine scaffold (SRD/Open5e ingest, clean-room functional-spec model) | [C] |
| Jun 12 | Flagship methodology piece — DRAFT ("Why one power number lies") | [C] |
| Jun 17 | Flagship — tone review (neutral-measurement discipline) | [P+C] |
| **Jun 19** | **PUBLISH #1** — flagship methodology (LinkedIn + website + newsletter launch) | [P] |
| Jun 24 | Design-team champion outreach wave (UA report as asset) | [P] |
| Jul 6 | Sim Stage 1 — first internal eHP reading produced (validation milestone) | [C] |
| Jul 10 | Quiet piece #2 — DRAFT (State of Community Wants) | [C] |
| **Jul 17** | **PUBLISH #2** — Community Wants | [P] |
| Jul 22 | Marketing-angle outreach wave (zeitgeist/timing value prop) | [P] |
| Aug 3 | **Sim Stage 1 GATE** — is internal grading rigorous & defensible? (Capability Gate 1) | [P+C] |
| **Aug 14** | **PUBLISH #3** — quarterly zeitgeist read | [P] |

### Phase 2 — Fork-5 + escalation (Sep → Nov)
| Date | Task | Own |
|---|---|---|
| **Sep 4** | **FORK-5 STRATEGIC CHECKPOINT** — assess A/B/C vs criteria; set volume dial; Stage 1→2 decision | [P+C] |
| Sep 14 | Sim Stage 2 — published-analysis-grade outputs (if dial = loud) | [C] |
| **Sep 18** | **PUBLISH #4** — first reception+power dual reading (the moat content) | [P] |
| Oct | Escalated cadence (bi-weekly if loud); YouTube channel launch *decision* (one-way-door — deliberate) | [P+C] |
| Nov 2 | **Legal-counsel pre-engagement reminder** — before ANY monetization / Stage 3 / Stage 4 | [P] |

### Phase 3 — Standalone service scoping (Dec → Q1 2027, trigger-gated, light)
| Date | Task | Own |
|---|---|---|
| Dec 1 | Stage 3 scoping *only if* traction criteria met; **legal counsel engaged before any money or Stage-4 content work** | [P] |
| Jan 8 2027 | Q1 strategic review — re-plan next horizon | [P+C] |

---

## Recurring tracks (see `.ics` for the actual repeats)

| Cadence | Task | Own |
|---|---|---|
| **Weekly — Mon** | Data maintenance: bookmarklet harvest 3 TTRPG forums (GitP / RPG.net / EN World) + Reddit/AO3/BGG flow check | [C] |
| **Weekly — Wed** | Outreach tracker review + follow-up sequencing (connect→accept→DM→email) | [P+C] |
| **Monthly — last Fri** | Mini-retro + full data-stream health audit | [P+C] |
| **Quarterly** | Strategic review + **registry/BigQuery export-integrity check** + buffer/catch-up week | [P+C] |

---

## Things you were missing (now folded in)

1. **Sim un-stall is a prerequisite, not Stage 1 itself** — stale combat-sim docs must reconcile first (scheduled May 27).
2. **Website must be publish-ready before Publish #1** — parity/scrub/newsletter sprint scheduled Jun 1–5.
3. **Content lead time** — every publish has a draft (~1 wk prior) + tone-review gate; no same-day publishing.
4. **Legal-counsel checkpoint is on the calendar** (Nov 2 reminder; hard gate before money/Stage 3/Stage 4).
5. **Data integrity ≠ data freshness** — added a quarterly registry/BQ export-integrity check on top of harvest cadence.
6. **Decision criteria for Fork-5 are defined** (above) so "all three don't bite" isn't a vibe.
7. **Buffer weeks** for caretaking resilience (quarterly, no hard deadlines).
8. **Lightweight KPIs to log at each monthly retro:** outreach response rate (A/B/C), newsletter subs + open rate, piece engagement, sim-milestone status, data-stream health. Keep in the tracker CSV / a new tab.

---

## Change log
- 2026-05-18 — v1 created (Phil + Claude). Consolidates: 4-stage sim roadmap, layered GTM, publishing menu, five forks, legal posture, data-maintenance cadence.
