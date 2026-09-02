# AO3 Crossover Metrics — Methodology

Why the fanfic numbers are built the way they are. Written for someone reviewing
the results who wants to know what was measured, what wasn't, and where the
joints are.

Current as of **Sep 2, 2026**. Views: `gold_data.fanfic_crossover_rate`,
`fanfic_crossover_proxy`, `fanfic_capture_guard`.

---

## What we measure

Three numbers per IP, and they answer different questions:

| | Definition |
|---|---|
| **crossover works** | AO3 works tagged with *both* `Dungeons & Dragons (Roleplaying Game)` and the IP's canonical fandom tag |
| **fandom total** | All AO3 works in that IP's canonical fandom tag |
| **crossover rate** | crossover ÷ total — D&D affinity per unit of fandom |

Absolute count answers *how much D&D crossover exists*. Rate answers *what share
of this fandom reaches for D&D*. **We report both and privilege neither**,
because they rank the set almost inversely: Stranger Things is 2nd by count and
7th by rate within its tier; Attack on Titan has more crossover works than Dark
Souls and the lowest rate in the set.

---

## These are censuses, not samples

AO3 reports exactly how many works carry both tags. There is no sampling error —
the rate is exact as of fetch time.

This changes what the small-numerator problem *is*. It is not uncertainty about
the true value; it is **fragility**. Mistborn's 0.268% rests on 2 works; two
authors could double it next month. That is why the column is
`rate_evidence_tier` and not a confidence interval — a confidence interval would
imply a sampling model that does not exist here.

Tiers: **HIGH** k≥25 · **MEDIUM** k 9–24 · **LOW** k<9. Split 10 / 8 / 7.

---

## Why we tier instead of shrinking

The obvious move is to shrink small-k rates toward the population mean. We
built it, kept it as a labelled secondary column, and **did not make it the
default** — because shrinkage penalises high-rate/small-fandom IPs hardest, and
that combination is the signal most worth finding.

The prior work's headline finding was ORV at 13 crossover works against a 1,316
fandom — 0.99%, an order of magnitude above comparable IPs. Shrinking that
toward the ~0.06% population rate would have erased exactly what made it
interesting.

So: rank **within** an evidence tier. The error to avoid was never "small
numbers are wrong" — it was a 2-work estimate and an 84-work estimate competing
as equals.

---

## The denominator is a choice, and it is recorded

A franchise is not a tag. It is a *set* of tags — Dark Souls I/II/III, Witcher
games vs TV vs books, `SPY x FAMILY (Anime)` vs `(Manga)` vs `- All Media Types`.

Rules:

1. Use the **broadest AO3 umbrella for the same licensable entity**. AO3
   publishes umbrellas under two suffixes and both count: `- All Media Types`
   (257 tags) and `& Related Fandoms` (65). Their wranglers already did the
   entity resolution.
2. A parent *franchise* is not an umbrella for a property inside it.
   `Star Wars - All Media Types` is not the denominator for The Mandalorian, and
   A Song of Ice and Fire is not the denominator for House of the Dragon — those
   measure a different entity.
3. Otherwise use the single canonical tag.
4. **Never sum children.** Works carry multiple tags, so sum ≠ union — observed:
   Anime 7,177 + Manga 8,051 = 15,228 against an umbrella of 8,897.
5. Numerator and denominator use identical scope. Always.
6. **Record which level was used** (`measured_at_umbrella_level`).

Umbrellas exist for 8 of 25 IPs, so a uniform level is not achievable. Rule 6 is
what makes the numbers defensible anyway: comparability is a property of the
comparison, not of the datum. Compare within level; the column tells you when
you can't.

### The rule is applied even where the gap looks trivial

One Piece moved 99,968 → 101,017 (+1.0%). Switching it changed almost nothing,
and it was switched anyway. Applying the rule only where the gap looks large
would make the level a post-hoc judgement, and *"we used whatever we already had
unless it looked wrong"* is not a rule a reviewer can check.

That turned out to matter empirically, not just aesthetically — see below.

### A denominator gap does not predict a numerator gap

The four Sep 2, 2026 umbrella re-captures:

| IP | Denominator | Numerator |
|---|---|---|
| The Witcher | 10,538 → 42,482 (+303%) | 24 → 62 (+158%) |
| Doctor Who | 61,401 → 109,819 (+79%) | 13 → 23 (+77%) |
| One Piece | 99,968 → 101,017 (+1.0%) | 52 → 54 (+3.8%) |
| **Spy x Family** | **8,053 → 8,899 (+10.5%)** | **2 → 8 (+300%)** |

Spy x Family's fandom is almost entirely one medium, so its denominator barely
moved — but six of its eight D&D crossovers were tagged against a *different*
child. We had been measuring **25% of the crossover population** while the
denominator gap said the level was nearly irrelevant.

So the two gaps are independent: a fandom can concentrate in one medium while
its crossovers concentrate in another. **Never use the denominator gap to decide
whether the level matters.** Had the rule been applied selectively on apparent
magnitude, Spy x Family would have kept a 4× understated numerator.

### Consequences of the Sep 2 level switch

- The Witcher moves 12th → **3rd** by absolute count and is promoted MEDIUM →
  HIGH evidence tier. Its **rate falls** 0.228% → 0.146%, which is a scope
  correction and **not a decline** — an umbrella pulls in media with lower D&D
  affinity, so denominators grow faster than numerators.
- Doctor Who's rate is essentially unchanged (0.0212% → 0.0209%) despite a 79%
  larger denominator: numerator and denominator scaled together, so its D&D
  affinity is uniform across the Whoniverse.
- Spy x Family sits at 8 crossover works — one short of the MEDIUM tier
  threshold. Its tier is marginal, not settled.

### Series break

Umbrella-level captures are not comparable with earlier medium-level ones for
these four IPs. This compounds the pre-Sep-2 break recorded below.

---

## The numerator method must be stated

Two defensible methods exist and they do not agree:

- **canonical tag intersection** (what we do) — House of the Dragon: **15**
- **keyword match on crossover tags** (the May 2026 manual pass) — **44**

Same IP, same period, same denominator (34,294 vs 35,362 — ordinary growth).
Neither is wrong; they measure different things, and a rate is meaningless
unless you know which produced the numerator. Ours is always tag intersection.

---

## A zero is not a zero

Every AO3 zero we have ever recorded — nine of them — turned out to be a stale
tag or a tag AO3 cannot filter on. **None was a measured absence.**

Two failure modes produce an identical silent `0`: a tag that has become a
*synonym*, and a tag not marked *common*. Neither errors. On Sep 2 this affected
**9 of 26 seed tags (35%)**, hiding real values of 54, 24, 21, 2 and more.

So zero is treated as **unverified until the tag is re-confirmed canonical**,
and `fanfic_capture_guard` flags any zero CRITICAL. If a genuine zero ever
appears, it will need positive evidence — a tag confirmed present in AO3's
canonical fandom listing with no matching works.

---

## Guardrails

`gold_data.fanfic_capture_guard` runs the cross-checks that were missing when
two live errors survived four months:

- **metatag inflation** — a crossover count ≈ the whole fandom. BG3 recorded
  49,020 "crossover works" against a 48,997-work fandom. The filter was
  returning the entire fandom because AO3 wrangles Baldur's Gate *under* the D&D
  metatag.
- **magnitude outlier** — ≥50× the platform median. BG3 was 2,334×.
- **zero counts**, **stale captures**, **missing denominators**.

Median, not mean, deliberately: the mean is destroyed by the outlier being
hunted. BG3 would have dragged the AO3 mean to ~2,000 and hidden inside its own
distortion; the median stayed at 15.

---

## What we do not claim

- **AO3 measures fanfic-writing propensity**, not popularity or commercial
  scale. It skews toward character-driven, relationship-heavy, younger-skewing
  properties. A commercially enormous IP can have a thin AO3 presence.
- **The 26-IP set is a pitch-shaped sample**, curated as Universes Beyond
  licensing candidates — not a representative or demand-shaped one. Harry Potter
  has the strongest FFN crossover signal in the data and is not in it.
- **Cross-platform counts are not comparable.** AO3 works, FFN stories and
  Royal Road views measure different phenomena and are never normalised together.
- **Absence of a measurement is not evidence of absence.** This is the mistake
  the prior write-up made when four IPs were reported as having no D&D crossover
  community; all four were tag bugs, and one of them (Jujutsu Kaisen, 54 works)
  is the third-strongest signal in the set.

---

## Temporal comparability

**Sep 2, 2026 is the first trustworthy baseline.** Earlier AO3 rows carry the
same stale-tag zeros, and at least one tag was re-wrangled by AO3 between April
and September (The Witcher, 48 → 24 — a definition change, not a decline).

Do not compute trends across that boundary; you would be measuring our bug fix.

---

## Reproducing

```
scripts/ao3_fandom_listing.py          fandom census (denominators, canonicality)
cloud_functions/ao3_fandom_listing/    the same, scheduled weekly
scripts/print_fanfic_capture_urls.py   per-IP capture URLs
scripts/apply_gold_view.py             deploy a view from gold_views/
```

Crossover capture is human-wielded by design: AO3's ToS forbids automated
scraping of `/works?` **search** queries, so a person clicks a bookmarklet.
Browse endpoints (`/tags/…`, `/media/…/fandoms`) are cheap and cacheable and are
scraped on a schedule with a 5s delay and an identifying User-Agent. That line —
browse automatable, search not — is the one the whole collection design rests on.
