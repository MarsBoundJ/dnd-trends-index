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

Realistic shape: record BG3 **fandom size** as its own labelled metric rather than
pretending it is a crossover count. The two quarantined rows (49,020 Sep 2,
47,660 Apr 28) are the back-data if that happens — preserved in
`dnd_trends_raw.fanfic_crossover_quarantine`, not deleted.

Blocked on deciding whether a crossover number is obtainable for BG3 at all.

---

## Current state (Sep 2, 2026)

Live AO3 set after cleanup: **24 IPs scored, max 84 (LotR), zero rows with
`work_count = 0`.** After this session there is no such thing as a zero in the AO3
dataset — every 0 we had was a bug or an unmeasurable tag.
