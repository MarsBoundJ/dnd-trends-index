# Data-Capture Hardening — Verification + Batch Confirm

**Status:** planned, agreed Sep 2, 2026. Next session's work.
**Scope:** the human-wielded capture path (AO3 first, pattern reusable for FFN / DDB / forums).

---

## Why this is the next piece of work

Over Sep 1–2, 2026 the capture path produced **five distinct wrong-but-plausible
results, none of which raised an error.** Every one would have shipped into the
composite as a real number.

| # | Failure | What it produced | How it was found |
|---|---|---|---|
| 1 | Unfiltered AO3 search | `10,886` for "Dungeons and Dragons" — every D&D crossover on AO3 | Eyeballed as implausible |
| 2 | Metatag inflation | BG3 `49,020` — the whole BG3 fandom, not an intersection | Compared against the plain tag page |
| 3 | Synonym tag | Silent `0` for 4 of 26 IPs — real values 54 / 24 / 21 / 2 | Manual tag-page check |
| 4 | Non-common tag | Silent `0` for Severance — actually unmeasurable | Manual tag-page check |
| 5 | Wrong-origin bookmarklet click | "No products found. Amazon layout may have changed." | Reasoning about CORS |

Three of those are AO3 returning a different wrong number each time. **Every one
was caught by a human noticing, not by the tooling.** That does not scale, and it
already cost us: Jujutsu Kaisen sat at `0` when its true value (54) makes it the
third-strongest AO3 signal in the set.

The severity multiplier is the normalisation in `gold_data.fanfic_crossover_proxy`:

```sql
LOG10(work_count + 1) / MAX(LOG10(work_count + 1)) OVER (PARTITION BY platform)
```

A single inflated row sets the denominator for the entire platform. BG3 alone was
compressing every other AO3 score ~2.4x (LotR 0.411 → 1.000 once removed). **One
bad row silently rescales the whole dimension.**

---

## The media listing pages change the shape of this plan

**Confirmed Sep 2, 2026:** `https://archiveofourown.org/media/<Category>/fandoms`
lists fandoms with work counts, and all our IPs appear. Five pages cover the set:

```
/media/Video%20Games/fandoms
/media/Anime%20*a*%20Manga/fandoms
/media/TV%20Shows/fandoms
/media/Books%20*a*%20Literature/fandoms
/media/Movies/fandoms
```

(`*a*` for `&`, AO3's usual convention.)

If those pages list **canonical fandoms only** — AO3's convention, since synonyms
redirect rather than getting their own listing — then five page loads supply both
the work item C denominators **and** a canonical-tag registry for work item A,
turning per-capture verification into a cheap pre-flight audit.

Coverage under that approach:

| Failure mode | Caught by |
|---|---|
| 1 — unfiltered search | Sep-1 `other_tag_names` guard (shipped) |
| 2 — metatag inflation | ratio ≈ 1.0 (work item C) |
| 3 — synonym tag | absent from listing (pre-flight audit) |
| 4 — non-common tag | absent from listing (pre-flight audit) |

All four, with no DOM-reading engineering. That would demote work item A from
"the plan" to belt-and-braces.

**CONFIRMED Sep 2, 2026 — canonicals only.** Neither `The Witcher (Video Games)`
nor bare `SPY x FAMILY` appears; both canonicals do. The listings are a valid
verification source, so work item A becomes a pre-flight audit.

Secondary benefit: ~5 page loads instead of 25+ is materially less load on AO3,
which matters given the human-wielded constraint below.

---

## Work item D — standardise the taxonomic level (NEW, and it affects data we
## already have)

The listing entries exposed a problem that is **already biasing the current
crossover ranking**, not just the future ratio:

```
Wiedźmin | The Witcher - All Media Types   42,482
Wiedźmin | The Witcher (Video Game)        10,539

SPY x FAMILY - All Media Types              8,897
SPY x FAMILY (Anime)                        7,177
SPY x FAMILY (Manga)                        8,051
```

We measure The Witcher at `(Video Game)` — **a quarter of its 42,482-work
franchise** — while measuring LotR at `All Media Types`, the whole franchise. The
two numbers are not at the same level, so 84 vs 24 is not a like-for-like
comparison.

`seed_fanfic_canonical_tags.py` is genuinely mixed on this:

| Level | IPs |
|---|---|
| Umbrella (`All Media Types`) | LotR, Dune, Percy Jackson |
| Medium-specific | The Witcher, Spy x Family, Stranger Things, Doctor Who, The Mandalorian, House of the Dragon, Cyberpunk 2077, Dark Souls, Bloodborne, Elden Ring, Hades, Jujutsu Kaisen, Demon Slayer |
| Bare / single-medium | Avatar, FFXIV, Persona 5, One Piece, Attack on Titan, Mistborn, Stormlight, Murderbot |

For single-medium IPs (Elden Ring) the level is moot. For multi-medium franchises
it changes the number substantially — and those are the big licensing targets.

### Recommendation

**Standardise on the `All Media Types` umbrella as the primary level.** The
licensing question is franchise-level — "is this IP a good D&D target?" — not "is
the video game specifically?" It is also the only level that exists consistently
across franchises.

Keep medium-specific as an **optional secondary** where the distinction is
analytically real. That is the generalised form of the BG3 question: separating
the video game from the TTRPG-native property.

### Critical caveat: levels overlap, they do not partition

Anime (7,177) + Manga (8,051) = 15,228 against an umbrella of 8,897. Works carry
multiple tags, so **children cannot be summed to reach a parent**, and a
medium-specific count is not a share of the umbrella. The umbrella is the only
safe cross-IP denominator.

### Consequence for existing data

Re-capturing at umbrella level changes most crossover counts, so it starts a new
series. Sep 2's counts stay valid *as medium-level measurements* but should not
be mixed with umbrella-level ones. Decide the level **before** the next capture
round, not after.

---

## Work item A — read-back filter verification

### The gap

We never verify the filter was actually *applied*. `platform_canonical` is echoed
from our own URL parameter, not read back from AO3's page:

```js
const otherTagsParam = params.get('work_search[other_tag_names]');
if (otherTagsParam) canonical = decodeURIComponent(otherTagsParam);
```

So it reads identically whether the filter worked, was ignored, or matched
nothing. The guard added Sep 1 checks the parameter is *present* — not that it
*resolved*. Failure modes 2, 3 and 4 all sailed straight through it.

### The fix

Read back what AO3 says it filtered on and compare against what we asked for.
Concretely, on the results page:

- confirm the requested IP tag appears in AO3's own applied-filter UI
- if the count is `0` **and** the tag is not echoed → report "tag not recognised —
  likely a synonym or not marked common", do **not** save a zero
- if the count is implausibly close to the primary tag's own total → warn about
  possible metatag inflation

### Open question

The exact DOM to read has not been determined — it needs a look at a live AO3
results page (filter sidebar vs. results header). **Do that first**; the rest of
the design depends on what is actually available.

---

## Work item B — batch confirm

### The friction

Current flow is ~3 interactions per IP: load page → click bookmarklet → click
confirm. At 25 IPs that is ~75 interactions, and the per-page `confirm()` modal
shows one number in isolation.

### The fix

Stash each capture in `localStorage` with a running tally; replace the 25 modals
with a single review table and one send action at the end.

- open all 25 — 1 action (Chrome bookmarks folder → "Open all")
- click the bookmarklet per tab — 25 (irreducible; this is the human-wielded part)
- review one table, send once — 1

~27 interactions instead of ~75, with **identical AO3 traffic**.

### Why it is a correctness fix, not just ergonomics

A batch table is a better review surface than 25 sequential modals. BG3's `49,020`
sitting next to LotR's `84` is obvious in a list and invisible one dialog at a
time. Outlier detection is what the confirmation step is *for*.

### Why together with A

Same code path. A produces a per-capture verification verdict; B needs somewhere
to display it. Building them separately means touching the same flow twice and
designing the stash format twice.

---

## Work item C — fandom total + D&D-affinity ratio

### The gap

The crossover count conflates two things: **how big a fandom is** and **how
D&D-affiliated it is**. LotR's 84 and Mistborn's 2 are not comparable — LotR is
one of AO3's largest fandoms, Mistborn is small. Two works out of a small fandom
may represent far stronger affinity than 84 out of an enormous one. We currently
rank on the confounded number.

### The metric

Capture the fandom total alongside the crossover count, giving three values:

| Metric | Measures |
|---|---|
| `fandom_total` | audience size / creative energy |
| `work_count` (existing) | absolute D&D affinity |
| ratio = `work_count / fandom_total` | affinity **per unit of fandom** |

Consistent with the project's dual-axis principle (a single metric is always the
trap). The ratio is a second axis, not a better version of the first — a small
high-affinity fandom and a huge low-affinity one are different licensing
propositions, not better/worse ones.

### Why it is nearly free

The fandom total lives on `/tags/<tag>/works` — **the same page work item A needs
for tag verification**. One visit yields both:

1. canonical / synonym / not-common status → work item A
2. total works → the denominator here

This is not a second capture pass. It makes A cheaper to justify, not more
expensive.

### It resolves the BG3 decision

"Track both" stops being a special case for one IP and becomes the general shape
of the metric. See the open-decision section below.

### Design rules

- **Measure the denominator with the SAME tag used in `other_tag_names`**, so
  numerator and denominator match by construction. Cross-IP comparison is still
  not perfectly level (LotR is All-Media-Types, The Witcher is games-only) —
  document that caveat rather than pretending it away.
- **Never rank on ratio alone.** Require both axes.
- **A ratio near 1.0 is a metatag-inflation detector, not a finding.** BG3 would
  read 49,020/49,020 ≈ 1.0 — that is failure mode 2, caught for free.

### Small-N handling

Observed crossover counts (24 IPs, Sep 2 2026): **2 – 84, median ~15.** These are
small counts, so the ratio is a proportion estimated from few events. Relative
standard error ≈ `1/√k`:

| k | RSE | Reading |
|---:|---:|---|
| 84 | 11% | solid |
| 30 | 18% | usable |
| 10 | 32% | shaky |
| 2 | 71% | noise |

A fandom with 40 works and 2 crossovers reads as 5% and would outrank almost
everything — on the strength of two works. Two mitigations, use both:

1. **Smooth the ratio** (additive / empirical-Bayes shrinkage toward the global
   D&D-crossover rate). Pulls small-k estimates toward the mean in proportion to
   how little evidence they carry. Nothing is discarded, and k=84 barely moves.
2. **Carry a confidence tier** derived from k, matching the existing confidence
   pattern in the codebase:
   - **HIGH** k ≥ 25 (RSE ≤ 20%) — 6 of 24 IPs today
   - **MEDIUM** k 9–24 — 10 of 24
   - **LOW** k < 9 — 8 of 24

Prefer smoothing over a hard cutoff: a k ≥ 10 threshold would discard a third of
the current set, including every literature IP.

---

## Work item E — revisit the FFN exclusion (its stated rationale is invalid)

`gold_views/fanfic_crossover_proxy.sql` excludes FFN from the score, and the
in-file rationale says:

> FFN excluded from the score (Apr 28, 2026)… when Phil first captured FFN data
> via the index page bookmarklet, only 6 of 142 IPs had ANY FFN crossover with
> D&D, and their counts maxed out at 7. Compared to **AO3's max of 47,660 works
> on a single IP**, that's a **6,800:1** ratio — too sparse for meaningful
> triangulation.

**That 47,660 is April's BG3 metatag artifact** — the row quarantined Sep 2, 2026.
It was never a real crossover count; it was the entire BG3 fandom. With the true
AO3 maximum of 84, the observed ratio on the two IPs carrying both signals is:

| IP | AO3 | FFN | ratio |
|---|---:|---:|---:|
| The Lord of the Rings | 84 | 7 | 12:1 |
| Doctor Who | 13 | 2 | 6.5:1 |

**~10:1, not 6,800:1.** FFN is a smaller archive, not a different universe — an
entirely ordinary cross-platform scale difference, well within triangulation
range. The headline argument for exclusion was an artifact of the same corrupted
row that was compressing every AO3 score.

### What still stands

Sparsity is real: only 6 IPs have any FFN data. And the log-normalisation
pathology is real — with a dataset max of 7, whichever IP holds 7 works scores
1.0 and would falsely boost anything captured on both platforms. Note this is the
*same* normalise-by-dataset-max fragility BG3 exploited on AO3, approached from
the other end: AO3 had one absurdly large max, FFN has a tiny noisy one.

### Proposed reframe

Use FFN as a **corroboration flag** — "does D&D crossover fic exist here at all?"
— rather than a scored magnitude. Presence/absence is robust at small N in a way
magnitude is not, and it sidesteps the normalisation problem entirely because
nothing is divided by a noisy max. 6 of 142 IPs having *any* FFN crossover is
itself information.

### Ordering — after A–D, not before

1. **D is blocking.** The taxonomic level must be settled before the next AO3
   capture; starting FFN leaves AO3 half-finished with a known bias in it.
2. **FFN reuses everything A–D builds** — pre-flight verification, batch confirm,
   denominators, ratio, confidence tiers. Doing it first means building twice.
3. **FFN has its own version of every bug found Sep 1–2.** Its own script warns
   index counts can be off ±1 against pair pages; fandom IDs can go stale exactly
   as AO3 tags did; and the existing 6 values have never been checked against the
   failure modes we now know to look for.

FFN is cheap when we get there — index mode captured 51 fandoms from a single
page on Sep 1, so it is closer to an afternoon than a project.

### Whatever is decided, fix the comment

Even if FFN stays excluded, the rationale in `fanfic_crossover_proxy.sql` must be
corrected. A documented decision resting on a number since proven false will
quietly mislead the next person who reads it — including us.

---

## Work item F — Wattpad: decided against, and say so in the code

Wattpad is **fully plumbed but was never captured**. `gold_data.fanfic_crossover_proxy`
carries `wattpad_work_count` and `wattpad_platform_score`, the bouncer's
`/system/fanfic/ingest-crossover-count` route accepts `'wattpad'` as a valid
platform, and the confidence scale is defined around it (`HIGH: 3 platforms
present (AO3 + FFN + Wattpad)`). The entire recorded rationale is four words:
*"no captures planned."*

Four places in the view say "future Wattpad", which reads as a roadmap item
rather than a rejected option. It should say what was actually decided.

### Assessment (Sep 2, 2026): not a viable source for this metric

The metric needs four things from a platform:

| Requirement | AO3 | Wattpad |
|---|---|---|
| Canonical fandom taxonomy | community tag-wrangled | freeform hashtags |
| Way to express an intersection | `work_search[other_tag_names]` | no real equivalent |
| Reliably exposed totals | `"N Works in X"` header | infinite scroll, no total |
| Volume of D&D crossover fic | sufficient | thin — skews teen / romance / mobile |

**Row 1 is the disqualifier, and it is exactly what Sep 1–2 taught us.** On
Wattpad `Baldur's Gate` might be `#bg3`, `#baldursgate`, `#baldursgate3`,
`#BaldursGateIII` — with **no canonical to correct toward**, because Wattpad has
no tag-wrangling process. Every failure mode we just eliminated on AO3 exists
there permanently and by design.

We could fix the AO3 synonym bug only because AO3 *told us* the canonical name,
and could audit in bulk only because `/media/<Category>/fandoms` exists. Wattpad
offers neither. Capturing it would reintroduce unverifiable counts into a
pipeline we just made verifiable.

### Where it would deserve a second look

As a **demographic reach** signal, not a crossover count — Wattpad's audience
skews younger and more female than AO3's, so presence there says something real
about market breadth. That is a different metric with a different definition, and
capturing `work_count` does not deliver it.

### Action

Documentation only. Replace "future Wattpad" phrasing with the decision and its
reasoning. Leave the columns and the bouncer's `valid_platforms` alone — they
cost nothing and keep the option open if the demographic-reach framing is ever
wanted.

---

## Work item G — `platforms_present` is degenerate (a real behaviour bug)

`per_ip_aggregated` applies the platform allow-list **before** counting
platforms:

```sql
COUNT(DISTINCT platform) AS platforms_present,
...
FROM per_platform_normalized
WHERE platform IN ('ao3')
```

So `platforms_present` is always 1. Verified against the live view: **all 24 IPs
return `platforms_present = 1`, `platforms_list = 'ao3'`.**

The documented confidence scale is:

```
HIGH:   3 platforms present (AO3 + FFN + Wattpad)
MEDIUM: 2 platforms present
LOW:    1 platform present
NONE:   0 platforms
```

**HIGH and MEDIUM are unreachable by construction.** The scale advertises four
tiers and can only ever emit two — and since every IP scores LOW, the dimension
carries no information at all.

### The fix, and the question it raises

Count platforms **captured** rather than platforms **scored**: compute
`platforms_present` over `latest_per_pair` before the allow-list, so an IP with
both AO3 and FFN data reads MEDIUM even while FFN is excluded from the score.
That is arguably what the tier was always meant to express — *how much
corroboration exists*, not *how many platforms fed the number*.

Note this interacts with work item E: if FFN returns as a corroboration flag,
MEDIUM becomes reachable without Wattpad, and the tier starts doing real work.

**Not a comments-only change** — it alters view output, so it needs deciding
deliberately rather than folding into a documentation pass.

---

## Work item H — AO3 census as an independent discovery frame ("sleepers")

Phil's idea, Sep 2, 2026.

The `/media/<Category>/fandoms` pages list **every canonical fandom with its work
count** — effectively a census of AO3 fandom sizes. Read as a ranking rather than
a lookup table, it becomes a discovery instrument.

### Why this fills a structural gap

The current pipeline can only **confirm or deny IPs someone already nominated**.
`scripts/seed_ub_candidate_ips.py` is human-curated from licensing assumptions, so
it encodes a theory of what is licensable *before any measurement happens*. Every
result is downstream of that theory.

AO3's census does not know about the seed list. It is an **independent sampling
frame**, and the only thing in the pipeline capable of surfacing an IP nobody
thought to nominate. "Sleepers no one is talking about" is precisely the class a
curated seed list structurally cannot find.

Fits the existing `gold_views/composite_blue_ocean.sql` frame rather than needing
a new one.

### Three requirements

**1. "Sleeper" is a divergence, not a rank.** Fandom size alone yields *popular*,
not *overlooked* — the top of any AO3 category is Marvel, Harry Potter, anime
megafandoms. A sleeper is **high AO3 fandom size + low attention elsewhere**, and
the second axis already exists (Google Trends, Reddit, YouTube). Same dual-axis
shape as work items C and D.

**2. A licensability filter — not optional.** AO3's largest fandoms are heavily
**RPF** (real-person fic), which is not licensable IP in any form and will
dominate raw rankings. Filter RPF, plus already-licensed properties and those with
defunct or unreachable rightsholders, before the ranking means anything.

**3. Triage.** Five pages x hundreds of canonical fandoms is 500+ candidates,
while qualifying each still costs a page load and a bookmarklet click. Discovery
outruns validation immediately, so the ranking must be good enough that the top
~20 justify the manual pass.

### Caveat — state this wherever the output lands

AO3 fandom size measures **fanfic-writing propensity**, a specific and skewed
behaviour: it favours character-driven, relationship-heavy, younger-skewing
properties. A property can be commercially enormous with a thin AO3 presence —
strategy games, sims, most sports, much hard SF.

This is a **lens, not a census of IP value.** It will find one kind of sleeper and
be structurally blind to others. Say so explicitly in any deliverable, or absence
will be read as evidence of absence — the same mistake the "zero-count negatives"
made in `community_reception_findings.md`.

### Cost

Effectively zero marginal cost: those five pages are already loaded for work items
A and C. This is a different *read* of data being fetched anyway.

---

## Constraints (non-negotiable)

- **Human-wielded by design.** AO3's ToS forbids automated scraping. The
  bookmarklet pattern is the *resolution* to that: a human clicks, AO3 renders
  the page as for any user, the bookmarklet reads the DOM the human can already
  see. No `fetch()` to AO3, no auto-iteration.
- **Verification must work inside a human-clicked page load.** Not by crawling,
  not by pre-fetching pages to validate tags.
- **Reading tabs the human already opened is fine** — zero additional AO3
  requests. *Navigating* to pages on their behalf is not.
- **Any 0 is unverified until the tag is re-checked.** AO3 re-wrangles tags
  continuously; `seed_fanfic_canonical_tags.py` now carries `ao3_verified_on`
  per entry because this will drift again.

---

## Not in scope

- Adding new data sources. The value now is trusting what we already collect.
- Automating tag verification by crawling `/tags/<name>` for all 26 IPs.
- Re-capturing April data. `gold_data.fanfic_crossover_proxy` keeps only the
  latest row per (ip, platform), so Sep 2 already supersedes it. April is
  retained as provenance but is **not a valid trend baseline** — it carries the
  same synonym zeros, and The Witcher's tag was re-wrangled to a narrower
  canonical between April and September (48 → 24 is a definition change, not a
  decline).

---

## Open decision — BG3 "track both"

The D&D × BG3 intersection appears **unmeasurable**: AO3 wrangles
`Baldur's Gate (Video Games)` under the `Dungeons & Dragons (Roleplaying Game)`
metatag, so the filter returns the whole BG3 fandom.

Phil's framing: BG3 is a grey area — a video game, but built directly on D&D
mechanics (spells, classes, subclasses, species), so some crossover attribution is
legitimate. Wants the video game distinguished from the TTRPG, possibly tracking
both.

**Work item C largely answers this.** Recording `fandom_total` for every IP makes
BG3 an ordinary row rather than a special case: its fandom total is a real,
useful measurement, and its ratio of ~1.0 flags the intersection as unmeasurable
rather than reporting a false crossover count. The two quarantined rows (49,020
Sep 2, 47,660 Apr 28) become the back-data for the fandom-total series —
preserved in `dnd_trends_raw.fanfic_crossover_quarantine`, not deleted.

What remains open is narrower: whether a *genuine* D&D × BG3 crossover number is
obtainable at all through some other tag combination, and whether BG3's D&D-native
mechanics (spells, classes, subclasses, species) warrant treating it as a
different category from ordinary crossover IPs.

---

## Current state (Sep 2, 2026)

Live AO3 set after cleanup: **24 IPs scored, max 84 (LotR), zero rows with
`work_count = 0`.** After this session there is no such thing as a zero in the AO3
dataset — every 0 we had was a bug or an unmeasurable tag.
