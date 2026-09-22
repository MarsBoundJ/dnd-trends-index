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

- ~~5e 2014 vs 2024~~ — **it is a facet.** `5.5e` sits under `5th Edition`
  on both stores. Stated as needing inference in four separate commits; each
  rested on a browse list rather than the tree.
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

## DMs Guild confirmed — product 339645

*Claus for Concern*, B.J. Keeton, a community-created one-shot. `STATUS 200`,
same shape, same endpoints, `siteId=76&groupId=29`. **One harvester, two rows of
config**, now verified rather than assumed.

Choosing a *community* product mattered: both earlier samples were publisher
titles (WotC/Guild Adept, R. Talsorian) and had these fields null. Here they
populate, and they are the fields that separate indie creators from back
catalogue:

```
isCommunityContent          true
communityContentAuthorId    433429
communityContentAuthorAlias "B.J. Keeton"
creatorLevelId              3          <- DMs Guild only, not on DriveThruRPG
worldId                     2          <- null on the DriveThruRPG sample
```

`creatorLevelId` is new and DMs-Guild-specific. The platform grades its
creators, and that grade is queryable — publisher tiering on a storefront with
no publishers, which is exactly the analysis DMs Guild otherwise makes hard.

### The axis roots — how to classify a filter without our files

Every filter chains up to an axis root via `parentId`. From this one product:

| Root | Axis |
|---|---|
| `45341` | productType |
| `45342` | edition |
| `45343` | setting |
| `45423` | theme |
| `45468` | content |
| `45477` | languages |
| `45544` | format |

So the harvester determines a filter's axis by walking `parentId` to the root —
no lookup table, no dependence on our hand-captured JSON. Those files become
what they should be: an annotated cross-check carrying hazards and catch-alls.

It also resolves the `45462-5e` puzzle. It is not a duplicate handle for
`1000261-5th-edition`, as recorded earlier; it is its **child**. Co-occurrence is
a product tagged at two depths, which is ordinary in a tree.

### Depth reaches further than expected

```
45418  parent=45393   1st Tier (Levels 1-4)      <- under Adventures
45438  parent=45396   Magic Items                <- under Gear/Magic Items
1000142 parent=1000140 Human-Created Without AI  <- under Creation Method
```

`45418` is a **character-level band**, a market attribute nobody asked for and no
browse surface shows: what level range do bestselling adventures target? And
`Creation Method` turns out to be the AI-disclosure axis in facet form, matching
the `ai` / `handmade` booleans.

### Two data hazards

**`reviewCount` is not the rating denominator.** On this product `reviewCount`
is 28 while `reviewRatings` counts sum to **94** (0+0+2+18+74). On the
DriveThruRPG sample, 6 against 12. Consistent across both, so the reading is
that `reviewCount` counts *written reviews* and the star buckets count
*ratings*. Using `reviewCount` as the denominator of an average would be wrong
by a factor of three here. Sum the buckets.

**`pagecount` type is inconsistent.** `"104"` (string) on DriveThruRPG, `27` on
DMs Guild. Coerce on read; never compare raw.

Also varying: `sku` was `"XMAS-2020"` here and empty on DriveThruRPG;
`storefrontPrimaryFilterValues` populated there and `[]` here. Treat both as
optional.

## The last unexplored door

The DMs Guild page called **`api.dmsguild.com/api/vBeta/filters`** — an endpoint
for the facet tree itself, alongside `filters/promos`.

If it returns every filter with `parentId` and `ancestors`, it is the
authoritative taxonomy, and `dmsguild_facets_v1.json` / `drivethrurpg_facets_v1.json`
stop being hand-captured approximations and become generated artefacts with our
annotations layered on. Every "not enumerable" axis would close at once.

### Confirmed — and it is the authoritative taxonomy

```
GET https://api.dmsguild.com/api/vBeta/filters?groupId=29&siteId=76   -> 200
```

`groupIds present: 29`, and the roots come back exactly as reconstructed from
one product's ancestors: `45341` Product Type, `45342` Edition, `45343` Setting.
Children chain correctly (`45344 <- 45342` Original Edition, and so on). This is
the facet tree, from the source.

**THE PARAMS DECIDE THE VOCABULARY, NOT THE HOST.** Calling
`api.dmsguild.com/api/vBeta/filters` with no query string returns 200 and
`groupId: 1` data — DriveThruRPG's tree, including an axis named *Genre*, which
DMs Guild does not have. A harvester that omits `groupId`/`siteId` gets a
plausible, well-formed, completely wrong taxonomy with no error to warn it.
Always send both.

**Two limits on the endpoint:**

- **`pageSize` is capped at 50.** Asking for 1000 returns
  `meta: {"itemsPerPage":50,"currentPage":1}`. Not an error — silently clamped.
- **There is no total.** `meta` carries only `itemsPerPage` and `currentPage`,
  so the only way to know you have the whole tree is to walk pages until one
  comes back short.

Ordering appears to be by `filterId` ascending, which is why the first page
holds only the three lowest roots — theme (`45423`), content (`45468`),
languages (`45477`) and format (`45544`) sit on later pages.

Once walked, `*_facets_v1.json` should be **generated** from this endpoint with
our annotations layered on, rather than hand-captured. Every "not enumerable"
axis closes.

**Still unverified:** whether any of these endpoints work unauthenticated. Every
call so far ran from a signed-in browser. If they need a session, the
bookmarklet pattern already provides one.
