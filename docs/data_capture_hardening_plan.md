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

---

## Work item I — reconcile pitch deliverables against the corrected AO3 numbers

Added Sep 2, 2026, after merging the corrections in #99.

Correcting the repo does not correct what shipped. The AO3 numbers that moved feed
conclusions in pitch material, and those conclusions have already gone out.

### Exposure

- The four false-zero IPs appear in **8 pitch files**, including
  `build_ip_licensing_report.js`, `build_onepager_ip.js` and `build_docx.js` —
  i.e. the builders that generate shipped deliverables
- `arcane/public/reports/ip_deep_dive.html` references them **33 times**
- Pitch markdown carries **127** AO3 / fanfic references
- Six PDFs in `arcane/public/reports/` cannot be grepped and must be checked by hand

**Timing (confirmed by Phil, Sep 2, 2026): effectively nobody from WotC has read
these yet.** The reports are live on trusightdata.ai but have not been consumed by
a counterparty. So this is *fix before first read*, not damage control — the bugs
were caught in the window between publishing and being read.

Two consequences:

- **No retraction is needed.** Nothing has to be un-said to anyone.
- **Regenerate ONCE, after item D.** Item D will change most crossover counts
  (measuring The Witcher at `All Media Types` moves its denominator from 10,539 to
  42,482, and the numerator with it). Correcting the reports now and again after D
  means doing it twice, with the second pass contradicting the first.

The deadline is real but conditional: the reports are **publicly reachable**, so
"nobody has read them" describes the past, not next week. Correcting them is a
**prerequisite for resuming outreach**, not a follow-up to it.

### The concrete case

`pitch/report/trusight_breakdowns_scratch.md` builds a licensing recommendation on
a thin-AO3 premise:

> broader-community-conversion is thin (**AO3 0.040% — second-lowest of any IP
> measured**) … **Demon Slayer fanbase is heavily anime-watcher demographic, less
> tabletop-converting. This is the structural finding from the harvest.**

That is not a stat in a table; it drives a **negotiation-leverage recommendation**
(lower upfront fee, narrower initial scope, performance-gated expansion).

### The surprise: a second AO3 measurement path may exist

**0.040% cannot have come from `fanfic_crossover_counts`** — Demon Slayer read `0`
there, and 0/anything is 0%. Backing out the implied denominators against the
Sep 2 corrected counts:

| IP | Stated rate | Corrected count | Implied fandom total |
|---|---:|---:|---:|
| Demon Slayer | 0.040% | 21 | ~52,500 |
| One Piece | 0.048% | 5 | ~10,400 |
| FFXIV | 0.063% | 48 | ~76,200 |

~52,500 is a plausible Demon Slayer AO3 fandom, so the rate is consistent with a
count of **~21, not 0**. Something measured this correctly while the gold table
carried a false zero.

**This must be run down before work items A–C are built.** If a second measurement
path exists and it dodged the synonym bug, it is more trustworthy than the one we
just spent two days hardening, and we should understand why before designing
around the wrong one.

### Work item C is not new — align, do not fork

The breakdowns already define an **"AO3 proportional crossover rate"**, with a
documented caveat that it is unreliable for ship-fic-heavy fandoms (One Piece,
FFXIV, Demon Slayer) where Reddit + DDB become load-bearing. Item C should adopt
that definition and caveat rather than introduce a parallel metric with different
semantics. See also the proportional-rate calibration band in the breakdowns notes.

### Tasks

1. Trace the provenance of the AO3 proportional rates in the breakdowns — which
   query, which tags, when captured
2. Determine whether any breakdown conclusion **flips** under the corrected counts
   (JJK 0→54 is the most likely candidate — it is now the 3rd strongest AO3 signal)
3. Check the six PDFs by hand for the 47,660 figure and the four "no crossover" IPs
4. Reconcile item C with the existing proportional-rate definition
5. Fix the internal inconsistency at lines 1717 vs 1838 — "second-lowest" vs
   "Lowest of any IP measured"

### Priority

**Ahead of A–C.** Those build capture machinery; this decides which existing
numbers are trustworthy and whether anything already sent to a counterparty needs
correcting. Sequence it against D, which is the other blocking decision.

---

## Work item I — FINDINGS (investigated Sep 2, 2026)

### Where the 0.040% came from: manual searches, May 2026

Not a pipeline. The AO3 proportional rates were gathered by **hand in May 2026**
and typed directly into `pitch/report/build_ip_licensing_report.js` and
`pitch/report/trusight_breakdowns_scratch.md`. The report's own caption says so:

> "AO3 search, May 2026. Both IPs above 0.90 baseline fit."

No table, no script, no ingestion, not reproducible. `fanfic_crossover_counts`
never held these numbers, which is why Demon Slayer could read `0` there while the
breakdowns carried 0.040%.

### Work item C already existed — with explicit numerator/denominator pairs

The breakdowns record both halves, not just the ratio:

| IP | crossover / total | rate |
|---|---|---:|
| Omniscient Reader's Viewpoint | 13 / 1,316 | 0.99% |
| House of the Dragon | 44 / 34,294 | 0.13% |
| Monster Hunter World | 1 / 124 | 0.81% |
| Deep Rock Galactic | 2 / 67 | 2.99% |
| Dwarf Fortress | 3 / 73 | 4.11% |
| Demon Slayer | ~20 / 49,611 | 0.040% |

It also already handles the multi-tag problem ad hoc — line 2486 reads
"AO3 total works **(combined fandom)**", i.e. summing across tags for some IPs.

### The paths AGREE where the level matches — and that validates the synonym fix

| IP | May manual | Sep 2 bookmarklet | |
|---|---:|---:|---|
| Demon Slayer | ~20 (0.040% x 49,611) | **21** | match |
| House of the Dragon | **44** | **15** | ~3x apart |

Demon Slayer matching is meaningful: an independent measurement taken *before the
synonym bug was known* lands within rounding of the canonical-tag re-capture. The
Sep 2 correction is corroborated.

**The HotD divergence is work item D, not a method difference.** The report's
34,294 denominator is far larger than a `House of the Dragon (TV)` tag alone
carries — it is a broader or combined tag, consistent with the "(combined fandom)"
note. The two paths agree when they measure at the same taxonomic level and
diverge when they do not.

**Consequence: D now blocks reconciliation as well as capture.** Comparing May
numbers to Sep numbers is meaningless until the level is fixed.

### The genuinely undocumented stream: `dnd_trends_raw.ao3_tag_counts`

A deployed `cloud_functions/ao3_harvester/main.py` has been running since
**Apr 13, 2026**, last fetch **Aug 30** — 390 rows, 23 D&D-*native* tags across
20 fetch days, typed `fandom` / `character` / `relationship`:

- fandoms: Dungeons & Dragons (RPG) **69,804**, Forgotten Realms 51,024,
  Baldur's Gate (Video Games) **48,997**, Critical Role 30,585
- characters: Astarion 27,102, Gale 17,111, Shadowheart 11,784, Karlach, Wyll,
  Dark Urge, Lae'zel, Halsin, Raphael, Minthara, Strahd, Jarlaxle, Drizzt, Vecna
- relationships: Astarion/Tav 11,420, Gale/Tav, Astarion/Gale, Shadowheart/Tav

This answers a **different question** — how much fic exists about D&D itself and
its characters — and is not the rate source. Memory and
`project_digital_streams_plan` list AO3 as *planned*; it has been live for five
months.

**It independently confirms the BG3 artifact.** `Baldur's Gate (Video Games)` =
**48,997** against our "crossover" capture of 49,020. A join between
`ao3_tag_counts` and `fanfic_crossover_counts` would have caught the metatag
inflation in April. Two tables in the same dataset held the answer; nothing
compared them.

### Revised tasks

1. ~~Trace the provenance of the AO3 proportional rates~~ — **DONE**: manual, May 2026
2. Decide whether the May manual numbers are retained, re-measured, or discarded.
   They are unreproducible and level-inconsistent, but Demon Slayer corroborates.
3. Re-measure at a single settled taxonomic level (blocked on D)
4. Check the six PDFs by hand
5. Align item C with the existing proportional-rate definition, including the
   "(combined fandom)" convention
6. Fix the internal inconsistency at breakdowns lines 1717 vs 1838
7. **NEW:** document `ao3_tag_counts` as a live stream, and add a guard view that
   flags any crossover count within ~1% of that IP's fandom total — the
   metatag-inflation detector, using data already collected

---

# PLAN REVISION (Sep 2, 2026) — most of this is automatable

Investigating item I surfaced `cloud_functions/ao3_harvester/main.py`, which has
been scraping AO3 **politely and automatically since April**: 5-second delay,
identifying User-Agent with contact address, 429 backoff. That forces a useful
distinction the project had made implicitly but never written down.

## The endpoint distinction — browse vs search

| Endpoint | Nature | Status |
|---|---|---|
| `/tags/<slug>/works` | reading a public aggregate | **automatable** (the harvester already does) |
| `/media/<Category>/fandoms` | browse listing | **automatable** — same class |
| `/works?tag_id=…&other_tag_names=…` | a **search query** | **human-wielded** — expensive server-side |

*Browse* is cheap and cacheable; *search* is the thing archives guard. This is why
the crossover bookmarklet must stay human-clicked while a tag-count harvester can
run weekly — not an inconsistency, a correct line nobody had articulated.

**Verify against AO3's live `robots.txt` before scaling up.** The harvester's
docstring asserts ToS permission; that assertion should be re-checked, not
inherited.

## Consequence: A, C, D and H become one automated stream

All four read from listing/tag pages, not search:

- **A** canonicality audit — a tag absent from the listing is a synonym or
  non-common
- **C** denominators — fandom totals come straight off the listings
- **D** taxonomic level — the listings expose every level per franchise
- **H** discovery census — the listings *are* the census

**Only the crossover numerators need a human.** That collapses the manual burden
from ~142 IPs to just the numerators, and makes everything else a weekly Cloud
Function beside the existing one.

## Franchise normalisation spec

**A franchise is not a tag — it is a *set* of tags.** Dark Souls I/II/III; Witcher
games vs TV vs books; SPY x FAMILY Anime vs Manga. Rules:

1. **Prefer AO3's own umbrella** (`— All Media Types`) where it exists. AO3's tag
   wranglers already performed the entity resolution; it is free and authoritative.
2. **Where no umbrella exists, define an explicit tag set** and store it as data.
   The franchise definition becomes a reviewable row, not a judgement buried in a
   script.
3. **NEVER sum children.** Anime 7,177 + Manga 8,051 = 15,228 against an umbrella
   of 8,897. Works carry multiple tags, so **sum ≠ union**. Use the umbrella
   (which *is* the union) or measure the union directly.
4. **Numerator and denominator must use identical tag scope.** The ratio is
   meaningless otherwise.
5. **Record the level on every row** — `umbrella` / `union_of_set` /
   `single_canonical` — together with the tag set used.

Rule 5 is what makes this defensible. Across 142 franchises AO3's taxonomy is not
uniform enough to yield one perfect comparable number, and pretending otherwise is
how the Witcher-at-quarter-franchise problem happened. But every measurement can
carry its scope, so comparisons can be made **within-level** and checked.

> **Comparability is a property of the comparison, not of the datum.** An analyst
> will accept "here is the level, here are the tags, here is the count." They will
> not accept a single number whose scope varies invisibly.

The listing scrape also **solves entity resolution**: a complete canonical-fandom
list per category exposes every Dark Souls variant and every Witcher level, so
franchise grouping is a data exercise over a full list rather than hand-curation
against a partial one.

## Make the tag config table-driven

`TRACKED_TAGS` in the harvester is a hardcoded Python list — expansion currently
means a code change and a redeploy. Move it to a BigQuery config table so adding
an IP is a row insert. This is the prerequisite for scaling past the current 23
tags to the full seed list.

## Revised scope for a 142-IP pass

- **Denominators + canonicality for all 142** — ~5 listing loads, automated
- **Franchise grouping** — derived from the same scrape
- **Crossover numerators** — human-wielded, so *triage rather than exhaust*: rank
  by fandom size x fit and capture the top N by hand

Item H stops being a side idea and becomes the front of the main workflow.

---

## Work item E — RESOLVED (Sep 2, 2026)

Re-captured FFN's D&D crossover index to test whether the thin overlap with the
AO3 set was real or an artifact of a four-month-old capture.

**It is real.** FFN's *entire* D&D crossover corpus is **206 works across 51
fandoms**. For scale, LotR's AO3 crossover count alone is 84 — all of FFN is
roughly 2.4x one AO3 IP.

Overlap with the seed 26 is **3 IPs**: LotR (7), Baldur's Gate (4),
Doctor Who (2). The stored May data was accurate, not stale.

### The corroboration flag is not buildable

The proposed reframe was to use FFN as a corroboration signal — "does D&D
crossover fic exist here at all?" — rather than a scored magnitude. With 3 of 26
IPs overlapping, there is nothing to corroborate. A column resolving for 11% of
the set would look like a signal and carry none.

**FFN stays excluded from scoring.** The original rationale cited a 6,800:1
AO3:FFN ratio that was an artifact of the BG3 row (corrected in #99 — the real
ratio is ~10:1), but the *other* argument always did the real work and still
holds: sparsity, plus the normalisation pathology where a dataset max of 7 hands
that IP a platform_score of 1.0.

### FFN covers a different population — that belongs to item H

FFN's D&D crossover fandoms are Harry Potter (55), My Little Pony (19),
Buffy (11), Pokémon (9), Star Wars (6), Terminator (7), Yu-Gi-Oh (6) — classic
and older fandoms with long crossover traditions. Almost none appear in the seed
26, which was curated as Universes Beyond licensing candidates.

So FFN is a **discovery frame for a population AO3's set misses**, not a
corroboration source for the population we have. That folds into work item H
alongside the AO3 census, with the same caveat: it is a lens, not a census of
demand.

### The non-obvious finding: FFN measures what AO3 structurally cannot

**FFN gives Baldur's Gate = 4.** AO3 cannot produce a D&D x BG3 crossover count
at all — AO3 wrangles Baldur's Gate *under* the D&D metatag, so the filter
returns the whole fandom (the quarantined 49,020). FFN has no metatag hierarchy,
so its 4 is a genuine intersection.

That inverts the usual framing. FFN is not simply a smaller, weaker AO3; it has a
flatter taxonomy, and flatness is an advantage precisely where AO3's hierarchy
collapses the measurement. Worth remembering for any IP that trips
METATAG_INFLATION in `gold_data.fanfic_capture_guard`: FFN may be able to answer
the question AO3 refuses.

### Caveats on the FFN numbers

- Index counts carry a documented ±1 drift against pair-page counts
- FFN fandom IDs can go stale exactly as AO3 tags did (9 of 26 were wrong)
- Nothing has re-verified the FFN IDs since April

### Open option, not taken

The full 51-fandom index could be ingested rather than just seed-list matches,
which would give a complete FFN picture for discovery. Not done: it would change
`fanfic_crossover_counts` from seed-scoped to mixed-scope, and that is an item H
decision about the sampling frame rather than an item E fix.

---

## Work item D — DECIDED (Sep 2, 2026)

### The rule

**Measure at the broadest AO3 umbrella for the same licensable entity.**

AO3 publishes umbrellas under **two** suffixes and both count:

| Suffix | Meaning | Tags |
|---|---|---|
| `- All Media Types` | same entity, aggregated across media | 257 |
| `& Related Fandoms` | entity plus its spin-offs / related works | 65 |

A parent **franchise** is not an umbrella for a property inside it.
`Star Wars - All Media Types` is not the denominator for The Mandalorian, and
A Song of Ice and Fire is not the denominator for House of the Dragon — those
measure a different entity's affinity.

### Two corrections to this item's own premise

The section above listed **three** IPs sitting below an available umbrella
(One Piece, The Witcher, Spy x Family). Checking against the census, both halves
of that were wrong.

**1. Avatar was never below its umbrella.** It is measured at
`Avatar: The Last Airbender & Related Fandoms` (64,594) — already the broadest
tag. It only *looked* wrong because `measured_at_umbrella_level` read `false`.

**2. Doctor Who is a fourth case, and the second-largest.** Measured at
`Doctor Who (2005)` (61,401) with `Doctor Who & Related Fandoms` (109,819) sitting
unused — a **+79%** denominator gap that nobody had noticed.

Both trace to one bug: `is_umbrella` was `name.endswith(" - All Media Types")`,
so **65 of 322 umbrellas — 20% — were flagged as non-umbrella.** The flag that
existed to make level problems visible was hiding one of them.

That is the more useful lesson than the level rule itself. A verification column
that is silently wrong is worse than no column, because it converts an open
question into a settled-looking answer. `platforms_present` did the same thing
for five months.

### What changes

| IP | Was | Now | Denominator change |
|---|---|---|---|
| The Witcher | `Wiedźmin \| The Witcher (Video Game)` 10,538 | `… - All Media Types` 42,482 | **+303%** |
| Doctor Who | `Doctor Who (2005)` 61,401 | `Doctor Who & Related Fandoms` 109,819 | **+79%** |
| Spy x Family | `SPY x FAMILY (Manga)` 8,053 | `… - All Media Types` 8,899 | +10.5% |
| One Piece | `One Piece (Anime & Manga)` 99,968 | `… - All Media Types` 101,017 | +1.0% |

Already compliant: LotR, Dune, Percy Jackson, Avatar. Every other IP has no
umbrella — the level is moot.

**One Piece is switched despite a 1.0% gap.** Applying the rule only where the
gap looks large would make the level a post-hoc judgement, and *"we used what we
already had unless it looked wrong"* is not a rule anyone can check. The cost of
uniformity here is four bookmarklet clicks.

### Re-capture is required, not optional

Rule 4 of the methodology is that numerator and denominator share scope. Moving
the denominator to the umbrella while the numerator still comes from a
medium-specific filter would produce a ratio between two different populations —
a subtler version of exactly the mistake this whole plan exists to prevent.

So all four IPs need their crossover count re-captured against the umbrella tag.
Both halves move together; the view picks the newest capture per IP, so old
medium-level rows stay in the raw table and simply stop being selected.

**Expect the rates to FALL for The Witcher and Doctor Who.** An umbrella pulls in
media whose D&D affinity is lower than the flagship's, so denominators grow
faster than numerators. The Witcher's 0.228% will drop substantially. That is a
scope correction, not a decline — and it must not be read as one.

### Series break

Sep 2, 2026 umbrella-level captures are not comparable with earlier
medium-level ones for these four IPs. This compounds the break already recorded
in `docs/fanfic_methodology.md`: pre-Sep-2 AO3 data was already unusable for
trends because of the stale-tag zeros.

### No metatag-inflation risk from this change

Worth stating explicitly, since umbrellas *are* metatags and metatag inflation
was the day's worst bug. Inflation happens when the two tags being intersected
are ancestor/descendant of each other — D&D is a metatag of Baldur's Gate, so
`D&D + BG3` returned all of BG3. None of these four umbrellas has any ancestry
relationship with the D&D tag, so intersecting them is a genuine intersection.
`fanfic_capture_guard` will confirm this at capture time regardless.

---

## Work item I — audit of the PUBLISHED IP Deep Dive (Sep 2, 2026)

Audited `https://trusightdata.ai/reports/ip_deep_dive.html` — the only report on
the site with an HTML version. The other six are PDF-only and unaudited.

**Findings recorded, deliberately NOT fixed.** The four umbrella re-captures
(work item D) will move several of these numbers again, and correcting twice is
wasted work. Outreach is paused, so nothing here is urgent.

### First, what is NOT wrong

The corrupted AO3 artifacts never reached this report. No `47,660`, no `49,020`,
no BG3 row. The denominators are ordinary May snapshots and most have grown
normally into today's values — Demon Slayer 49,611 → 53,061, Hades 8,260 →
8,495, Elden Ring 6,242 → 6,503, Mistborn 682 → 746. Persona 5's rate (0.081%)
matches today's view almost exactly.

So the numbers are broadly sound. **The errors are in the prose**, which is a
harder problem, because prose is the part that does not regenerate when data is
corrected.

### Finding 1 — a pun became a methodology (3 IPs + a general claim)

One Piece's rate is explained as:

> "AO3 channel is heavily ship-fic-dominated for One Piece … the AO3 proportional
> rate is unreliable for **naval** ship-fic-dominated fandoms"

"Ship fic" means *relationship* fic. The report reads it as *ships* — boats —
and reasons from One Piece being about pirates. The rate (0.048%) was right; the
explanation is a homophone collapse.

It did not stay contained. The same explanation is applied to two IPs with no
nautical content whatsoever:

- **FFXIV** — "Same ship-fic-dominated pattern; AO3 proportional rate is
  unreliable for FFXIV"
- **Demon Slayer** — "Same ship-fic-dominated pattern as One Piece and FFXIV"

and then generalised into a standing methodological rule in the summary:

> "AO3 proportional rate is unreliable for ship-fic-heavy fandoms; Reddit + DDB
> are load-bearing."

So a reasoning error about one IP now licenses discounting the AO3 channel
across the whole report. This is the most consequential finding here, and the
one least amenable to a data fix.

### Finding 2 — three IPs each claim to be the lowest

| IP | Rate | Claim |
|---|---|---|
| One Piece | 0.048% | "Lowest of any IP measured" |
| Demon Slayer | 0.040% | "Lowest of any IP measured" |
| Sea of Thieves | 0% | "Lowest of any IP we've measured" |

At most one can be true. FFXIV compounds it: "second-lowest of any IP measured
(after One Piece's 0.048%)" at 0.063% — while Demon Slayer's 0.040% sits lower
than both.

These are superlatives typed as prose rather than computed. See the
recommendation below.

### Finding 3 — a published AO3 zero

Sea of Thieves: `AO3 D&D-crossover works 0`, `rate 0%`, presented as a finding
about the audience.

Every AO3 zero this project has ever recorded — nine of them — was a stale or
unfilterable tag, never a measured absence (`docs/fanfic_methodology.md`). The
Sea of Thieves tag has not been verified against the canonical fandom listing.

`gold_data.fanfic_capture_guard` flags zeros CRITICAL. It was not consulted,
because it did not exist when this report was written.

### Finding 4 — HotD's anchor number uses the other numerator method

> AO3 D&D-crossover works **44** · rate **0.13%** — "The empirical anchor for the
> engine-question argument"

44 is the May keyword-match count. Today's canonical tag intersection gives
**15**, and a rate of **0.042%** against a 35,364 denominator. Both methods are
defensible and they measure different things
(`docs/fanfic_methodology.md`, "The numerator method must be stated") — but the
report states neither, and HotD's verdict is explicitly built on this number.

Directionally the argument survives: 0.042% is *even lower* relative to fanbase
size, which is what the engine-question argument claims. The magnitudes and
every "~3-7× lower than the calibration cluster" comparison do not.

### Finding 5 — FFXIV's numerator moved materially

Deep dive 29 → today 48; rate 0.063% → 0.104%. FFXIV's "strongest honest-caveat
case in the breakdowns" rests partly on a low AO3 rate that is now 65% higher.
Whether this is method, a tag correction or real growth has not been
established — it needs checking before FFXIV's verdict is restated.

### Finding 6 — a factual slip

Demon Slayer is described as "third-largest after One Piece and FFXIV". Demon
Slayer (53,061) is larger than FFXIV (46,349).

### The structural recommendation

Findings 2 and 6 are **rank claims typed as sentences**. Finding 4 is a number
typed as a sentence. None of them could have survived being computed.

The fix is not proofreading. It is that a report should not be able to *state*
an unsupported superlative — "lowest of any IP measured" should be rendered from
the data, so three IPs claiming it simultaneously becomes impossible rather than
merely unnoticed. Likewise a rendered report should refuse, or visibly flag, any
IP whose `fanfic_capture_guard` row is CRITICAL — which is exactly how Sea of
Thieves' zero would have been caught before publication.

That points at the dynamic-report workflow tracked separately. Note the limit
honestly: of the six findings here, computed rendering prevents 2, 4 and 6,
catches 3 via the guard, and does **nothing** for 1 — the pun — which is the
worst of them. Data binding fixes numbers. It does not fix reasoning.

### Scope not yet audited

The six PDF-only reports. They cannot be read as text from the repo and were not
checked. Whether they repeat the ship-fic claim is unknown and worth knowing,
since the shared phrasing suggests a common source.
