# The OneBookShelf product API

Confirmed 22 Sep 2026 against DriveThruRPG product 535790 (*Single Player Mode*,
R. Talsorian). `STATUS 200`, ~45 KB, JSON:API shape.

```
GET https://api.drivethrurpg.com/api/vBeta/products/535790?groupId=1&siteId=10
```

| Store | host | `siteId` | `groupId` |
|---|---|---|---|
| DriveThruRPG | `api.drivethrurpg.com` | 10 | 1 |
| DMs Guild | `api.dmsguild.com` | 76 | 29 |

One call returns everything the detail harvest was designed to scrape, and a
good deal more. **The DOM is no longer needed.**

## The structural discovery: facets are a tree

`relationships.filters` is an array of `Filter` ids, and `included` expands each
one with `filterId`, `descriptions` (name, slug), **`parentId`** and
**`ancestors`**.

That single field explains every "missing" value we chased:

| Value | parent |
|---|---|
| `510` Cyberpunk (genre) | `500` Science Fiction |
| `2110` Campaigns, Adventures & Modules | `2150` Supplements & Expansions |

Nothing was missing. **Browse lists show top-level nodes only.** Monsters & NPCs,
Campaigns/Adventures/Modules, Solo, GM-less, Cyberpunk-the-genre, `5e`, Descent
into Avernus — all of them are children, which is why they appeared on product
pages and in Refine panels but never in a browse sidebar.

So the curated-surface rule in `README.md` was the right conclusion from the
evidence, but the underlying reason is simpler than "curation": it is depth. Our
facet JSON files are a flat snapshot of a hierarchy, which is why they kept
coming up short.

**Consequences.** A harvester must record `parentId`/`ancestors`, not just the
id. Rolling up a child to its parent becomes possible (every Cyberpunk product
is also Science Fiction), and it is now clear why counts on those axes were
floors. The `*_facets_v1.json` files stay useful as an offline reference with
hazards and catch-alls written down, but **the API is authoritative** — it
resolves ids to names itself, so the harvester should take labels from the
response rather than from our files, and use ours as a cross-check.

## `ranking` is the medal tier

```json
"ranking": { "humanName": "adamantine", "imageUrl": "medals/adamantine.png" }
```

The tier comes straight from the API, per product, authoritative.

This is the same value the extension currently derives by reading shelf headings
on `metal.php` — the mechanism behind the V9 tier bug (#144), its V11 fix, and
146 regression tests. None of that was wasted: it repaired six months of stored
rows and it is what the Monday harvest still runs on. But going forward the
detail pass can take the tier directly, and the whole class of "which heading
was this product under" bugs disappears.

It also lifts a limit. Shelf scraping only ever sees products *on* `metal.php`.
`ranking` is present on every product, so tier becomes available for the whole
catalogue, not just the bestseller shelves.

Discovery still needs a listing — `metal.php` or a browse walk — to learn *which*
product ids exist. The API gives detail, not enumeration.

## Field inventory

Everything asked for, and several things nobody thought to ask for.

**Physical** — `pagecount` `"104"` (string, matches the DOM exactly) ·
`files[].filename` · `files[].size` (use this, not the top-level `filesize`,
which read `0`) · `scannedPDF` · `watermarked` · `disablePrint`

**Commercial** — `price` · `specialPrice` · `lowestDigitalPrice` ·
`lowestPrintPrice` · `onSale` · `isPwyw` (so PWYW is a first-class flag, exactly
as hoped) · `options[]` (Digital Format, print options)

**Reception** — `rating` `"4.8"` · `reviewCount` · `reviewRatings`
`{average, countOne…countFive}` — the **full star distribution**, not just a
mean. A 4.8 from six ratings and a 4.8 from six hundred are different facts, and
so is a bimodal split.

**Provenance** — `publisherId` · `publisher` relationship (expands to name, slug,
`productCount`, `averageRating`, `reviewCount`) · `isCommunityContent` ·
`communityContentAuthorId` / `Alias`

**Creators** — `authors[]` (3) · `artists[]` (14) · `editors[]` ·
`contributors[]`. Credited individuals, not just the publishing entity. Artist
counts alone say something about production budget.

**Dates** — `dateCreated` · `dateAvailable` · `dateModified` ·
`fileLastModified`. **This is what makes time-to-tier computable**: release date
plus a weekly tier series answers how long a product takes to reach Adamantine.

**Disclosure flags** — `ai` (false) and `handmade` (true). The storefront
records AI-generated content. Tracking that share over time is a live industry
question and it is sitting right there.

Unreliable on this sample: `sku` and `isbn` were empty strings, `edition` null,
`filesize` 0.

## What the LLM is still for

The list is now very short:

- **5e 2014 vs 2024** — still not a facet.
- **Audience (player vs GM)** — partly implied by product type.
- **Cross-store mapping** — and even this is easier now that hierarchy is visible.

Everything else that `bouncer/main.py` currently asks Gemini to infer from a
title and a truncated snippet — category, publisher, system, setting — is
available as fact. That enrichment should be retired in favour of observation,
not tuned.

## Harvest design

1. **Discover** product ids from a listing (`metal.php` today).
2. **Fetch** `/api/vBeta/products/{id}` per product, paced.
3. **Store** attributes, plus `filters` with `parentId`/`ancestors`, plus
   `ranking.humanName`, keyed on `productId`.

One JSON call per product against ~36 requests per rendered page, so pacing
costs us nothing and spares their infrastructure. `also_purchased` and
`bundle_memberships` are separate calls — worth having eventually, not in v1.

**Authentication.** The call was made from a signed-in browser with
`credentials: "include"`. Whether it works anonymously is untested; if it does,
nothing changes, and if it does not, the bookmarklet pattern already runs in the
user's signed-in session.

**Still unverified:** the DMs Guild response shape. Same platform and the same
`siteId`/`groupId` convention, so expect a match — but confirm before assuming,
which is the lesson this whole exercise has been teaching.
