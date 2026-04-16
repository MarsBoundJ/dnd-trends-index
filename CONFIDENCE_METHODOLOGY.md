# Confidence Methodology — Arcane Analytics

**Version:** v1.0.1 (tuned 2026-04-15)
**Author:** Claude Opus 4.6 with direction from Yorri
**SQL source of truth:** `gold_views/concept_confidence.sql`
**Frontend source of truth:** `arcane/src/components/card-chrome.tsx` (`confidenceToTier()`)
**Spec reference:** `FRONTEND_DESIGN_SPEC.md` sections 5.1, 5.2, 5.3, 9.16, 9.17

---

## 1. What Confidence Means in Arcane Analytics

Confidence is not popularity. A concept with 100% search interest on Google
Trends and zero other data has *lower* confidence than one with moderate
search interest corroborated by Reddit, Fandom Wiki, and Roll20.

**Confidence answers:** "How much should you trust the trend signal we're
showing you?" It's a measure of data reliability, not data magnitude.

The system has two layers:

| Layer | What it scores | When it runs | Where it lives |
|---|---|---|---|
| **data_confidence** | How well-corroborated the raw data is | Daily (materialized view refresh) | `gold_data.concept_confidence` |
| **ai_grounding_confidence** | How well-grounded AI-generated text is in cited facts | At article/Sage generation time | Card-assembly layer (Step 6.5) |

For pure data cards (leaderboards, charts), the displayed confidence *is*
`data_confidence` directly. For AI cards (articles, Sage summaries), the
displayed confidence is `min(data_confidence, ai_grounding_confidence)` per
FRONTEND_DESIGN_SPEC 5.1. This document covers the data_confidence layer.

---

## 2. The Metal Ladder (Locked Thresholds)

The tier thresholds are **locked** by spec. When the distribution is wrong,
we tune the formula — never the ladder.

| Tier | Range | Meaning |
|---|---|---|
| **Copper** | 0-69 | Exploratory. Thin data, not decision-grade. |
| **Silver** | 70-79 | Acceptable. Some corroboration, rarely act on alone. |
| **Gold** | 80-89 | Decision-grade. Multiple families agree. |
| **Platinum** | 90-94 | Highly trusted. Broad cross-validation. |
| **Mithral** | 95-99 | Gold-standard consensus. Reserved for strongly corroborated concepts. |

See `FRONTEND_DESIGN_SPEC.md` 9.16 for the full design rationale on why
we chose D&D metals over MtG rarity or letter grades. The short version:
D&D metals communicate trust progression naturally ("copper is cheap, mithral
is legendary") and avoid the "A/B/C" trap where users read grades as quality
judgments rather than data-reliability signals.

---

## 3. The Formula

### 3.1 Inputs

All inputs come from `gold_data.composite_concept_index`, the materialized
table that normalizes 13 single-stream analytics views into 5 strategic
buckets. The formula never reads raw stream data directly.

| Input | Source | Range | What it captures |
|---|---|---|---|
| `concept_avg_confidence` | Per-bucket average of stream-level HIGH(1.0)/MED(0.7)/LOW(0.4) weights, then averaged across non-null buckets | 0.4-1.0 | How much raw data backs each stream's claim |
| `streams_present` | Count of distinct data streams with any data for this concept | 1-13 (theoretical max) | Breadth of coverage |
| `families_hit` | Count of the 5 families (curiosity/community/creator/ownership/commerce) with at least one stream | 1-5 | Diversity of evidence types |
| `agreement_factor` | Fraction of non-null bucket momenta on the majority side of 0.5 | 0-1 | Do different evidence families agree on the direction? |
| `velocity_factor` | Penalty for "high level but flat momentum" (evergreen/stale consensus) | 0.90-1.0 | Are we seeing active signal or a parked giant? |
| `freshness_factor` | Placeholder, hardcoded to 1.0 pending stream-level max(snapshot_date) flow-through | 1.0 | Will penalize stale snapshots when available |

### 3.2 Assembly

```
base = 45
     + 10 * avg_stream_confidence      (weight for per-stream data density)
     + 15 * streams_present / (streams_present + 2)   (saturating coverage curve)
     + 20 * families_hit / 5           (diversity bonus, linear)
     + 10 * agreement_factor           (directional consensus)

raw  = base * velocity_factor * freshness_factor

data_confidence = ROUND(
  CASE
    WHEN streams_present <= 1 THEN LEAST(raw, 79)   -- single-source cap
    ELSE LEAST(raw, 99)                              -- mithral ceiling
  END
)
```

### 3.3 Term-by-Term Design Rationale

**Base constant (45):**
We want a single-stream, low-confidence concept with no diversity and no
agreement to land at about 60 (mid-copper). Working backwards:
- avg_stream_confidence contributes ~4 (10 * 0.4)
- streams_present contributes ~5 (15 * 1/3)
- families_hit contributes ~4 (20 * 1/5)
- agreement_factor contributes ~5 (10 * 0.5)
- Total without base: ~18. 45 + 18 = 63. That's copper. Correct.

A three-family, three-stream concept with good agreement should land around
78-82 (silver-gold border), which it does:
- avg_stream_confidence: ~5 (10 * 0.5)
- streams_present: ~9 (15 * 3/5)
- families_hit: ~12 (20 * 3/5)
- agreement_factor: ~7 (10 * 0.7)
- Total: 45 + 33 = 78. Silver, on the gold cusp. Correct.

**avg_stream_confidence weight (10):**
This was originally 25 in v1.0.0 and caused the biggest calibration failure.
The problem: each stream picks its own arbitrary thresholds for HIGH/MED/LOW
(e.g., Google Trends says "10+ data points in 14 days = HIGH"). That makes
avg_stream_confidence closer to a *coverage proxy* than a genuine trust
signal — a stream with lots of data points calls itself HIGH regardless of
whether those points corroborate anything. Dropping the weight from 25 to 10
means this term contributes variance without dominating.

**streams_present (15, saturating):**
The `streams_present / (streams_present + 2)` curve is a deliberate choice
over linear scaling. Linear would give each additional stream a fixed bonus,
but the marginal trust value of the 8th stream tracking "Paladin" is much
lower than the 2nd stream. The curve:

| Streams | Value | Contribution |
|---|---|---|
| 1 | 0.33 | 5.0 |
| 2 | 0.50 | 7.5 |
| 3 | 0.60 | 9.0 |
| 5 | 0.71 | 10.7 |
| 10 | 0.83 | 12.5 |

Maximum contribution is 15, but you effectively plateau at ~12 after 5-6
streams. This reflects reality: 2 streams is much better than 1, but 10
streams vs. 8 doesn't meaningfully change how much you trust the signal.

**families_hit (20, linear):**
This is the most important structural factor and gets the highest weight
(20 out of 55 total variable points). The five families represent
fundamentally different kinds of evidence:

- **Curiosity** (Google Trends, Wikipedia, Twitch): "Are people searching for this?"
- **Community** (Reddit, Fandom Wiki): "Are people talking about this?"
- **Creator** (YouTube, Itch.io, AO3, mod.io, Nexus Mods): "Are people making content about this?"
- **Ownership** (BGG/RPGGeek, Roll20, Steam): "Are people playing/owning this?"
- **Commerce** (Crowdfunding, Amazon, DMs Guild, DriveThruRPG, DDB Catalog): "Are people buying this?"

A concept showing up in curiosity AND commerce AND ownership is genuinely
more trustworthy than one showing up only in curiosity — even if the
curiosity signal is very strong. That's the core thesis of the formula.
Linear scaling is appropriate here because each family adds a genuinely
different kind of corroboration (unlike additional streams within the same
family, where diminishing returns apply).

**agreement_factor (10):**
Measures whether the bucket momenta agree on direction. Calculated as the
fraction of non-null bucket momenta on the majority side of 0.5 (where 0.5
is neutral momentum in the PERCENT_RANK scale):

- If 3 of 3 buckets are all trending up (momentum > 0.5), agreement = 1.0
- If 2 of 3 agree, agreement = 0.667
- If all are neutral (exactly 0.5), agreement = 1.0 by convention

Weight of 10 means this swings the score by at most 10 points. That's
intentional — disagreement should pull you down from gold to silver, not
from gold to copper. It's a "yellow flag," not a disqualifier.

**velocity_factor (multiplicative, 0.90-1.0):**
Solves the "5e always maxes out" problem. Core D&D concepts (Player's
Handbook, Dungeon Master, etc.) have high trend_level (they're perennially
popular) but low trend_momentum (they're not going anywhere). Without this
penalty, they'd permanently squat in platinum/mithral by virtue of having
lots of data everywhere — but "the Player's Handbook is still the Player's
Handbook" isn't a trend insight. The penalty says: if you're very popular
(trend_level >= 0.90) and very flat (trend_momentum < 0.30), we trim your
raw score by 10%.

The v1.0.1 tune tightened the gate from `trend_level >= 0.70` (which hit
any moderately popular concept with mixed momentum) to `>= 0.90` (only
truly maxed-volume evergreens). Two tiers:

| Condition | Penalty |
|---|---|
| trend_level >= 0.90 AND momentum < 0.30 | *0.90 (10% trim) |
| trend_level >= 0.80 AND momentum < 0.25 | *0.95 (5% trim) |
| Otherwise | *1.00 (no trim) |

---

## 4. The Anti-Theater Rules

### 4.1 Single-Source Hard Cap (79 = Silver Ceiling)

If a concept has data from only one stream, its confidence is capped at 79
regardless of how high the formula scores it. This is the most important
anti-theater rule in the system.

**Why:** A single source can be wrong in ways we can't detect. Google Trends
might show 100 interest for "Paladin" — but is that D&D Paladins or
Paladin-class characters in Final Fantasy? Wikipedia might show huge
pageviews for "Beholder" — but is that the D&D monster or the 2022 horror
movie? Without a second source confirming the signal is D&D-specific, we
can't know.

**Why 79 specifically:** It's the maximum silver score. A single-source
concept should never reach gold (80+). Silver says "we have some data, but
don't make major decisions based solely on this." That's exactly right for
an uncorroborated signal.

### 4.2 Stale-Consensus Penalty

See velocity_factor in 3.3 above. The principle: high confidence should
reflect active, corroborated trend intelligence — not the mere fact that a
perennially popular concept has data in many places. "D&D is popular" is not
an insight; "D&D interest spiked 30% this month because of X" is.

### 4.3 Formula Tuning, Never Ladder Tuning

When the distribution is off (too many concepts in one tier, core concepts
misclassified), the response is always to adjust the formula constants —
never the tier thresholds. The thresholds (0-69/70-79/80-89/90-94/95-99)
are the user-facing contract. Changing them would retroactively reclassify
every concept in the system and break any user intuition about what "gold"
means. The formula is the internal machinery; the ladder is the external
promise.

---

## 5. v1.0.0 to v1.0.1: The First Calibration

### 5.1 What v1.0.0 Got Wrong

The initial formula (base=30, avg_stream_confidence weight=25) produced:

| Tier | Count | Share |
|---|---|---|
| Copper | ~73% | Way too many |
| Silver | ~27% | |
| Gold | 1 | |
| Platinum | 0 | |
| Mithral | 0 | |

Core D&D classes like Paladin and Wizard landed at 63 (copper). That's not
just a calibration miss — it actively undermines user trust. If users see
"Paladin" marked as "Exploratory / Copper" they'll conclude the system is
broken, because Paladin is one of the most data-rich concepts in D&D.

**Root cause:** 93.7% of concepts in the composite_concept_index have only
one stream. This isn't a formula bug — it's the known concept-name matching
gap (Roll20, BGG, and Amazon use product titles that don't join cleanly to
canonical concept names like "Paladin"). So the formula was technically
correct: most concepts genuinely have thin data. But it over-weighted
`avg_stream_confidence` (which measures stream-internal data density, not
cross-stream corroboration), pushing even multi-stream concepts below gold
when their single-stream confidence labels happened to be LOW or MEDIUM.

### 5.2 What v1.0.1 Changed

| Parameter | v1.0.0 | v1.0.1 | Why |
|---|---|---|---|
| Base constant | 30 | 45 | Lift the floor so single-stream concepts land at ~65-68 (honest copper) instead of ~55 (uncomfortably low copper) |
| avg_stream_confidence weight | 25 | 10 | It's a coverage proxy, not a trust signal; should contribute variance without dominating |
| velocity gate threshold | trend_level >= 0.70 | trend_level >= 0.90 | Original gate was too aggressive — any moderately popular concept with mixed momentum got trimmed |
| velocity secondary gate | (none) | trend_level >= 0.80 AND momentum < 0.25 -> 0.95 | Gentler step for high-but-not-maxed concepts |

### 5.3 v1.0.1 Results

| Tier | Count | Share | Assessment |
|---|---|---|---|
| Copper | 48% | Down from 73% | Honest — these are genuinely single-stream concepts |
| Silver | 52% | Up from 27% | The single-stream concepts with decent data now correctly sit here |
| Gold | 10 | Up from 1 | Multi-family concepts with agreement |
| Platinum | 0 | | Structurally unreachable until families_hit > 3 (needs concept-name matching fix) |
| Mithral | 0 | | Same structural ceiling |

**Spot checks:**

| Concept | Score | Tier | Binding Constraint | Notes |
|---|---|---|---|---|
| Sorcerer | 81 | Gold | thin_stream_data | 3 streams, 3 families, full agreement |
| Paladin | 78 | Silver | conflicting_signals | 3 streams, 3 families, agreement = 0.667 |
| Wizard | 78 | Silver | conflicting_signals | Same structure as Paladin |
| Fighter | 78 | Silver | conflicting_signals | Same — the 3 core classes form a natural cluster |
| Bag of Holding | 68 | Copper | single_source | 1 stream — correctly capped, correctly identified |

Sorcerer at 81 gold while Paladin/Wizard/Fighter sit at 78 silver is
genuinely interesting: Sorcerer has full directional agreement across its 3
families (all trending the same way) while the other classes have one family
trending against the other two (agreement = 0.667). That's a 3-point swing
from the agreement_factor term (10 * 1.0 vs 10 * 0.667 = 3.3 points).
The formula is distinguishing a real signal — Sorcerer's trend is more
internally consistent.

---

## 6. Known Limitations and Future Work

### 6.1 Platinum and Mithral Are Structurally Empty

No concept currently reaches platinum (90+) or mithral (95+). This is not a
formula deficiency — it's a data coverage ceiling:

- Maximum `families_hit` in the current dataset is 3 (curiosity, community,
  creator). No concept has ownership or commerce data linked because
  Roll20/BGG product titles and Amazon listings don't join to canonical
  concept names yet.
- Maximum `streams_present` is 4.
- These caps mean the formula's diversity and coverage terms can't contribute
  enough to clear the platinum threshold.

**Fix:** Close the concept-name matching gap (tracked in
`project_data_quality_backlog.md`). When Roll20's "Player's Handbook (2024)"
correctly resolves to concept "Player's Handbook" and BGG's "D&D 5th Ed."
resolves to concept "5th Edition," the multi-family concepts will naturally
rise to platinum/mithral because the formula already rewards families_hit
and streams_present.

### 6.2 Freshness Factor Is Placeholder

`freshness_factor` is hardcoded to 1.0. The intent is to penalize concepts
whose data is stale (e.g., last snapshot was 3 months ago), but
`composite_concept_index` doesn't currently surface per-stream
max(snapshot_date). When it does, multiply freshness_factor into raw without
changing other weights.

### 6.3 AI Grounding Confidence (Step 6.5)

This layer is not yet implemented. It will:
- Run at article/Sage text generation time (not at composite refresh time)
- Score how well the generated text is grounded in cited data
- Produce `ai_grounding_confidence` (0-100)
- The displayed confidence for AI cards becomes `min(data_confidence, ai_grounding_confidence)`

This ensures an AI-generated article about a gold-tier concept still shows
copper confidence if the text makes poorly-grounded claims.

### 6.4 48% Copper Is Not a Bug

Nearly half of concepts landing in copper looks wrong at first glance. It's
not. 93.7% of concepts have data from only one stream. The single-source cap
(79) limits them to silver at best, and most don't even have enough in their
one stream to reach silver (70+). The copper tier is doing its job:
transparently communicating that we have thin data for the long tail.

When the concept-name matching gap closes and more streams connect to each
concept, the distribution will naturally shift upward. The formula doesn't
need to change — the data underneath it does.

### 6.5 Category-Level Confidence

The Overview page's "Category Heat" card renders category names (Class,
Monster, Subclass, etc.), not concept names. These don't resolve against
`concept_confidence` and fall back to the silver stub (75). Category-level
confidence would need a different aggregation — possibly the median
data_confidence across all concepts in that category. This is a future
enhancement, not a current gap in the formula itself.

---

## 7. How to Tune the Formula

If a future calibration pass shows the distribution is wrong:

1. **Histogram the current distribution** across tiers. The target: bulk of
   well-corroborated concepts in silver/gold, thin-data tail in copper,
   platinum/mithral reserved for genuinely exceptional coverage.

2. **Spot-check core concepts** (Paladin, Fighter, Wizard, Sorcerer, Rogue,
   Cleric, Player's Handbook, Dungeon Master's Guide) — these should never
   be copper unless something is genuinely broken in their data.

3. **Adjust the formula constants**, not the tier thresholds:
   - Base: raises or lowers the floor for all concepts
   - Factor weights (10/15/20/10): shift emphasis between data density,
     coverage, diversity, and agreement
   - Velocity gate thresholds: widen or narrow the "stale consensus" penalty
   - Single-source cap: could be lowered to 74 (mid-silver) or raised to 84
     (gold floor) depending on whether single-source signals deserve more or
     less trust

4. **Increment `algo_version`** (currently `v1.0.1`) so the explanation
   payload in the methodology popover reflects which formula produced the
   score. This prevents confusion when comparing scores across versions.

5. **Re-run the spot checks** after tuning. The calibration is done when the
   spot-check concepts land where domain expertise says they should.

**The cardinal rule:** Tune the formula, never the ladder.

---

## 8. Architecture Reference

```
                      ┌────────────────────────┐
                      │  13 single-stream       │
                      │  analytics views        │
                      │  (google_trends,        │
                      │   reddit, youtube, ...) │
                      └───────────┬────────────┘
                                  │ UNION ALL
                                  ▼
                      ┌────────────────────────┐
                      │  composite_concept_     │
                      │  index (materialized)   │
                      │                         │
                      │  5 buckets, PERCENT_    │
                      │  RANK normalization,    │
                      │  confidence weights     │
                      └───────────┬────────────┘
                                  │ reads from
                                  ▼
                      ┌────────────────────────┐
                      │  concept_confidence     │
                      │  (VIEW)                 │
                      │                         │
                      │  formula → score, tier, │
                      │  explanation_json        │
                      └───────────┬────────────┘
                                  │ queried by
                                  ▼
                      ┌────────────────────────┐
                      │  Bouncer /confidence    │
                      │  endpoint (Option B)    │
                      │                         │
                      │  ?names=Paladin,Wizard  │
                      │  → { paladin: {...} }   │
                      └───────────┬────────────┘
                                  │ fetched by
                                  ▼
                      ┌────────────────────────┐
                      │  fetchConfidence()      │
                      │  bouncer.ts             │
                      │                         │
                      │  cardConfidence() →     │
                      │  min across card items  │
                      └───────────┬────────────┘
                                  │ drives
                                  ▼
                      ┌────────────────────────┐
                      │  CardChrome Popover     │
                      │  card-chrome.tsx        │
                      │                         │
                      │  pip color, tier label, │
                      │  methodology breakdown  │
                      └────────────────────────┘
```

---

## 9. File Index

| File | Role |
|---|---|
| `gold_views/concept_confidence.sql` | The formula. CREATE OR REPLACE VIEW on `gold_data.concept_confidence`. |
| `gold_views/composite_concept_index.sql` | The normalized foundation. Materializes all 13 streams into 5 buckets. |
| `bouncer/main.py` (path: `/confidence`) | REST endpoint: accepts `?names=...`, returns `{ name: { data_confidence, tier, explanation } }`. |
| `arcane/src/lib/bouncer.ts` | `fetchConfidence()`, `cardConfidence()`, `ConfidenceEntry` type. |
| `arcane/src/components/card-chrome.tsx` | `confidenceToTier()`, tier maps, methodology Popover UI. |
| `arcane/src/app/overview/page.tsx` | Wiring: batch-fetch, per-card min aggregation, prop threading. |
| `FRONTEND_DESIGN_SPEC.md` (sections 5.1, 5.2, 5.3, 9.16, 9.17) | Design spec sections governing confidence display. |
| `CONFIDENCE_METHODOLOGY.md` (this file) | You are here. |
