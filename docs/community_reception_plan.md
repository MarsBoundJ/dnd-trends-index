# Community Reception — Strategic Plan

**Status:** Phase 1 locked. Phase 2 candidates identified. Implementation begins after Phil's review of this plan.
**Date:** 2026-04-27
**Companion fix:** Per-IP signal renormalization on `rubric_composite` (Option A — see "Composite Score Fix" section below)

---

## Why this exists

The existing 5-dimension UB rubric (`genre_fit`, `combat_translatability`, `party_dynamics_fit`, `setting_portability`, `fanbase_ttrpg_overlap`) measures **fit** — does this IP work mechanically and demographically with D&D? It does not measure **reception** — would the established D&D community embrace or reject this IP if WotC actually licensed it?

These are different things. The current rubric scores Pokemon as a strong-fit candidate (huge game-literate fanbase). But a Pokemon Universes Beyond set would be a community disaster (Magic competitor + tonal mismatch). Bridgerton would score middling on fit but reception would be far worse than the fit predicts (perceived as "WotC pandering").

The model needs a way to distinguish these cases.

This is also Hasbro's documented pain. WotC has "stepped in it" repeatedly — OGL crisis (2023), AI art controversy, the digital-pivot framing widely read as "getting rid of D&D books." The trust deficit with the established D&D community is real and unmeasured. **Arcane is positioned to build the diagnostic instrument that names the pattern.**

---

## Strategic significance

This is shaping up to be Arcane's most defensible feature.

It addresses a measurable phenomenon (community reception risk on licensing decisions) that no other vendor in WotC's stack can produce:
- **Touchstone Research** is panel-based and slow — quarterly cycles minimum
- **Kepler** is media-execution-focused, not strategic-signal
- **Hasbro AI Studio** is a single-person LLM workflow with no community-data substrate
- **Internal community management** tracks Discord/Reddit informally but doesn't quantify

Arcane uniquely has the data substrate (Reddit harvesters, Fandom data, AO3 access pattern, BGG ratings, MTG UB sentiment history) to measure community reception at scale, in close to real time.

The Hasbro pitch line:

> *"For each candidate IP, we score not just the fit (5-dimension rubric) but also the community reception risk — combining the AO3 organic-crossover signal, MTG Universes Beyond historical precedent, BGG board-game proxy reception, and live D&D-community Reddit sentiment. So when you see 'Bridgerton: high fit, low reception,' you know immediately it's a Walking Dead-tier risk before you greenlight it."*

---

## How the score is built — three breakout signals

After triangulating ChatGPT, Gemini, and Perplexity inputs, three signal sources stand out as strategically unique. **All three were Gemini contributions.**

### 1. AO3 organic crossover signal

Archive of Our Own (the largest fanfiction repository on the internet) has a public, robust tagging system. We query for `tag:"Dungeons & Dragons (Roleplaying Game)" AND tag:"[IP]"` to count how many fanfiction stories already cross those two universes.

**What this measures:** revealed preference at the deepest level. Fans don't write 4,500 Percy Jackson × D&D crossover stories because they were paid to or marketed to. They write them because the crossover *already exists in the community's imagination.* If WotC officially blesses that crossover, they're catching a wave that's already moving.

Inverse case is just as informative: zero AO3 crossover fic means "literally nobody asked for this." Massive negative-predictive signal nobody else is measuring.

**Public data, ethically clean to scrape.** ~1 day to build the scraper.

### 2. MTG Universes Beyond historical precedent

WotC has been doing UB for 3+ years. The reception of past UB sets is publicly visible and is a leading indicator for D&D crossovers:
- Walking Dead UB → community revolt
- Lord of the Rings UB → universally loved
- Final Fantasy UB → record sales
- Spongebob UB → divisive

We build a curated database of ~30 past UB sets, each scored on reception outcomes (sales data, secondary-market price stability, sentiment trajectory, follow-up-set decisions). When evaluating a new IP, we match it analogically against the closest UB precedents.

**This explicitly speaks WotC's own language.** They lived through the Walking Dead reception. Showing them an AI that learned from their own history is uniquely persuasive.

~half day to assemble the curated database.

### 3. BGG licensed-game proxy

When this IP previously got a board game adaptation, how did tabletop gamers receive it? Tabletop gamers are a closer audience proxy to D&D players than general consumers. If the *Dark Souls* board game was hated by BGG voters as a "cash grab," that same skepticism transfers to a hypothetical *Dark Souls* D&D set.

**We already have BGG data in our pipeline.** Add a query that looks up "[IP] board game" entries and pulls user ratings + comment sentiment. ~half day.

---

## Phase 1 plan (ship before Licensing Expo, May 19-21)

| Signal | Source | Effort | Strategic value |
|---|---|---|---|
| Gemini-anchored baseline | LLM rubric pass with 6th dimension | ~1 hr | Coverage — works for all 142 IPs |
| Reddit sentiment | Existing pipeline (25 subreddits) | ~2 hrs | Core discourse signal |
| **AO3 crossover signal** | New scraper (public AO3 tags) | ~1 day | **Unique — nobody else has this** |
| **MTG UB precedent database** | Hand-curated DB + analogical matching | ~half day | **Unique — leading indicator from WotC's own history** |
| **BGG licensed-game proxy** | Existing BGG data + new query | ~half day | **Unique — mechanics-literate audience proxy** |

**Total Phase 1 effort:** ~3 working days. Doable in remaining window before Expo.

### Phase 1 composition formula

Same per-IP renormalization pattern as the proposed `rubric_composite` Option A fix — if a signal is unavailable for a given IP, redistribute its weight proportionally across the available signals.

```
community_reception_score (target weights when all 5 signals present) =
    0.20 × gemini_anchored_baseline      (always available, all 142 IPs)
  + 0.25 × ao3_crossover_signal          (counts of organic crossover fic)
  + 0.20 × reddit_sentiment_quantified   (when N >= threshold per IP)
  + 0.20 × mtg_ub_precedent_match        (analogical match to closest UB sets)
  + 0.15 × bgg_licensed_game_proxy       (when prior tabletop adaptation exists)
```

When fewer than all 5 signals exist for a given IP:
- Available weights renormalize to sum = 1.0
- Each available signal's weight = `w_i / sum(w_available)`
- Surfaces in the data trail which signals contributed

Each individual sub-score is shown in the data trail. When a WotC reviewer asks "why is Bridgerton 0.42?", the drill-down reveals the contributing signals and the analogies/anchors behind each.

### Display pattern

Phil's preference: keep `rubric_composite` and `community_reception` as **two separate scores**, both surfaced on each candidate card.

```
Stormlight Archive
  Fit:        95         (mean of 5 rubric dimensions)
  Reception:  87         (community_reception_score)
  License-fit: 91        (composite, weighted blend)
```

The user weighs the two in their head rather than collapsing the information into a single ranking number.

---

## Phase 2 candidates (post-Expo, or pulled forward if time permits)

| Signal | Source | Effort | Notes |
|---|---|---|---|
| YouTube comment sentiment | New scraper on existing 57-channel registry | ~2 days | Influencer sentiment layer |
| Narrative classification | LLM topic-classify on Reddit + YouTube text | ~1 day | "Cash grab" / "tone mismatch" / "doesn't feel like D&D" labels |
| Kickstarter revealed preference | Existing pipeline | ~half day | "Are similar IPs funding well right now?" |
| D&D Beyond public-page sampling | Cautious narrow scraper, robots-respecting | ~1 day | Paying-customer sentiment, less cynical than Reddit |

**Most valuable Phase 2 candidate to pull forward:** narrative classification. It's the "WHY of negative reception" — labeling each negative comment as "cash grab" / "tone mismatch" / "doesn't feel like D&D" / etc. Enormous pitch value for ~1 day of work.

---

## Phase 3 (long-term / post-trial / partnership-driven)

These all face access constraints (legal/ethical/operational) that prevent default automation:

- **EN World / RPG.net** forums via partnership ask. Robots.txt explicitly blocks crawlers (per Perplexity's review). Don't bulk-scrape. Path forward: direct outreach to ENWorld's owner about a research partnership.
- **Hand-curated grognard tier** — manual analyst notes on selected EN World / RPG.net threads. Slowest but highest-purity signal.
- **Discord** via opt-in survey bots. Privacy + ToS constraints prevent default scraping. Only viable if a partner community wants this.

---

## Composite Score Fix (Option A) — companion work item

**Tracked as a separate but linked work item, NOT to be lost while community_reception work proceeds.**

The current `gold_views/universes_beyond_candidates.sql` view applies fixed 0.60 / 0.30 / 0.10 weights to rubric / fandom / steam respectively, with `NULL` signals treated as 0 in the composite. This **structurally penalizes IPs without applicable signal**.

Concrete observed problem: literature IPs (Stormlight, Murderbot Diaries, Foundation) get docked 10 points because they can't have Steam data — books don't go on Steam. Stormlight Archive observed at `license_fit_score = 0.87` instead of the ~0.97 it should be at.

**Approved fix is Option A — per-IP signal renormalization:** redistribute the weight of unavailable signals proportionally across available ones, so each IP is scored using only what applies, with weights always summing to 1.0.

**Same renormalization pattern as the community_reception composition above.** Once both ship, `rubric_composite` and `community_reception_score` will both use this fair-weighting approach. A single PR can ship the composite fix; pairs naturally with the community_reception work.

---

## Locked decisions

### 1. Two scores per IP, both surfaced — LOCKED Apr 27, 2026

Each candidate displays:
- **Fit score** = `rubric_composite` (mean of the 5 fit dimensions: `genre_fit`, `combat_translatability`, `party_dynamics_fit`, `setting_portability`, `fanbase_ttrpg_overlap`)
- **Reception score** = `community_reception_score` (the new multi-source composite — Gemini baseline + AO3 + MTG UB precedent + BGG proxy + Reddit, per-IP renormalized)
- Both shown side-by-side. **NOT averaged together.** The reviewer (WotC exec, Phil during demo, etc.) weighs the two in their own head.

**Why locked, not deferred:** collapsing "fit" and "reception" into a single ranking number destroys exactly the information community_reception was built to surface. A high-fit / low-reception IP (Pokemon, Bridgerton) requires a completely different licensing decision than a low-fit / high-reception IP — but both might score the same blended number under any single-score system. The whole strategic premise of community_reception is that the **gap between fit and reception is the actionable signal.** Hiding that gap behind a composite would defeat the feature.

**Rejected alternatives** (recorded for context, not for revisit):
- Averaging community_reception into `rubric_composite` as a 6th dimension at 1/6 weight — collapses the fit/reception distinction
- Modifier on top of fit composite (`license_fit × (0.6 + 0.4 × reception)`) — same problem in different math

**Subject to revisit only:** the LABELS on the two scores ("Fit" / "Reception" / "License-fit") and exact UI placement. The two-scores-not-one structure is permanent.

---

## Open decisions deferred

1. **Naming:** `community_reception` or `backlash_risk`?
   Phil leaned toward neutral framing: `community_reception` with high = better, matching the polarity of the existing 5 fit dimensions.

2. **Bridgerton-tier negative anchors:** initial draft picks include Bridgerton, Stardew Valley (cozy mismatch), Pokemon (Magic competitor), reality TV / lifestyle brands. Phil to validate before the rubric prompt is finalized.

3. **Phase 2 scope before outreach:** Phil said "possibly add in Phase 2 work before we reach out." Decision deferred until we see Phase 1 progress.

---

## Triangulation source notes

This plan synthesizes inputs from three AI tools consulted on Apr 27, 2026:

- **ChatGPT** — Channel survey across YouTube, Discord, Forums, Twitter, D&D Beyond, Actual Play, Kickstarter, Google Trends. Proposed weighted CRI (Community Reception Index). Strong contribution: the **narrative detection** angle — measure WHY negative ("cash grab" / "identity mismatch" / "tone mismatch"), not just IF negative.
- **Gemini** — Surfaced the three uniquely brilliant signals that became this plan's core: AO3 fanfiction crossover counts, BGG licensed-game ratings, MTG Universes Beyond historical precedents. These three are the strategic core.
- **Perplexity** — Operational reality check via robots.txt analysis. EN World and RPG.net forums explicitly disallow general crawling. D&D Beyond has mixed posture. Forced the plan to honor scraping ethics. Without Perplexity's input, the plan would have included un-shippable scraping tactics.

Each tool contributed something the others missed. Triangulation across AI tools is itself a useful methodology pattern — preserved here as a reference for future product decisions.

---

## Cross-references

- `project_hasbro_pitch_problems_solutions.md` (memory) — pitch framing, WotC org intel, exec targeting context
- `project_tracks_frames_roadmap.md` (memory) — Step 9.9 (UB Matrix) shipped Apr 23; this plan is post-9.9 enhancement
- `project_data_quality_backlog.md` (memory) — the Stormlight 0.87 anomaly that surfaced the composite fix
- `gold_views/universes_beyond_candidates.sql` — the composite-score view that needs the Option A renormalization fix
- `scripts/enrich_ub_candidates.py` — the rubric scoring script that will need a 6th dimension added
